# Issue #2494: Subscription Timeout Root Cause Analysis

## Executive Summary

This investigation analyzed why subscription operations timeout within 60 seconds in Docker NAT environments, despite this being sufficient time for a 6-peer network. The analysis confirms several contributing factors, with the primary root cause being the **cumulative effect of multiple timeout layers** rather than any single issue.

## Key Findings

### 1. Timeout Budget Analysis

The 60-second `OPERATION_TTL` must cover multiple layers of potential delays:

| Component | Worst Case | Occurrence |
|-----------|-----------|------------|
| RTO exponential backoff | 1s → 2s → 4s → 8s → 16s → 32s → 60s cap | Per retransmit |
| Connection backoff | 5s → 10s → 20s → 40s → 80s → 160s → 300s cap | Per connection failure |
| Contract wait timeout | 2s per hop | Per intermediate node |
| HTL traversal | 10 hops max | Per subscription |

**Critical Insight**: If packet loss occurs requiring even 3 retransmissions (1s + 2s + 4s = 7s minimum), and this happens at multiple hops with contract wait timeouts (2s each), the budget is quickly exhausted:
- 3 hops × (2s contract wait + 7s packet retries) = 27s minimum
- Add NAT traversal overhead and routing delays → exceeds 60s

### 2. Subscription vs GET Routing Differences

**GET Operations (more resilient)**:
- Multi-route with fallback (`alternatives: candidates` field)
- Breadth-first search with `MAX_RETRIES = 10` and `DEFAULT_MAX_BREADTH = 3`
- Can retry to alternative peers on failure
- Tracks `tried_peers` to avoid repeating

**Subscribe Operations (single-path)**:
- Hop-by-hop routing with no failover
- If connection drops mid-subscription: `handle_abort()` fails immediately
- No alternative route mechanism
- Each intermediate node must succeed or the entire operation fails

Relevant code locations:
- GET breadth: `crates/core/src/operations/get.rs:29` - `DEFAULT_MAX_BREADTH: usize = 3`
- Subscribe abort: `crates/core/src/operations/subscribe.rs:373` - `handle_abort()`

### 3. Contract Availability Delay (2 seconds per hop)

When a subscription arrives before the contract (PUT is still propagating), each intermediate node waits up to 2 seconds:

```rust
// crates/core/src/operations/subscribe.rs:25
const CONTRACT_WAIT_TIMEOUT_MS: u64 = 2_000;
```

In a 6-peer network with HTL=10, if the subscription races ahead of the PUT, multiple nodes could each wait 2 seconds, accumulating significant delays.

### 4. Transport Layer RTO Growth

The `SentPacketTracker` implements RFC 6298 exponential backoff (`crates/core/src/transport/sent_packet_tracker.rs`):

- Initial RTO: 1 second
- Backoff: doubles on each timeout (1s → 2s → 4s → 8s → 16s → 32s)
- Cap: 60 seconds
- Backoff multiplier: up to 64x

Under packet loss conditions in Docker NAT (especially with bridge networks), a single message could take 1 + 2 + 4 + 8 + 16 = 31 seconds of retries before reaching the 60s cap.

### 5. Connection-Level Backoff

When connection attempts fail (`crates/core/src/ring/connection_backoff.rs`):

- Base interval: 5 seconds
- Pattern: 5s → 10s → 20s → 40s → 80s → 160s → 300s (capped)

If initial connection to a peer fails due to NAT issues, the backoff means subsequent attempts are significantly delayed.

### 6. Docker NAT Specifics

The tests use Docker NAT simulation (`FREENET_TEST_DOCKER_NAT=1`) which:
- Routes traffic through Docker bridge networks
- Simulates NAT traversal scenarios
- Introduces additional latency compared to localhost
- Can have higher packet loss due to network stack overhead

## Root Cause Hierarchy

1. **Primary**: Cumulative timeout budget exhaustion from multiple layers
2. **Secondary**: Single-path routing for subscriptions (no failover)
3. **Contributing**: Contract wait delays at intermediate nodes
4. **Contributing**: RTO exponential backoff under packet loss
5. **Environmental**: Docker NAT overhead

## The PR #2489 Workaround

PR #2489 (commit `bf5c878`) changed subscriptions to be asynchronous by **not** registering parent-child relationships:

```rust
// Before: Subscription blocked PUT/GET response
super::start_subscription_request(op_manager, id, key, true)

// After: Subscription runs independently
super::start_subscription_request_async(op_manager, id, key)
```

This means:
- PUT/GET responses return immediately to clients
- Subscriptions complete (or fail) in the background
- Client gets confirmation the contract was stored, subscription is best-effort

**Trade-off**: This changes the API semantics from atomic (PUT+subscribe succeeds together) to eventual (PUT succeeds, subscribe may fail silently).

## Recommendations

### Short-term (Symptom Mitigation)
1. **Increase OPERATION_TTL for subscriptions** - Consider 120s+ for subscription-specific timeout
2. **Add subscription-specific telemetry** - Track where time is spent (waiting, routing, retrying)

### Medium-term (Architectural Improvements)
1. **Add failover routing to subscriptions** - Similar to GET's `alternatives` mechanism
2. **Reduce contract wait timeout** - 2s may be excessive; 500ms might suffice with retries
3. **Tune RTO parameters for Docker NAT** - Consider faster initial retries, slower backoff

### Long-term (Design Considerations)
1. **Evaluate if async subscriptions are the right design** - The current workaround may be acceptable if:
   - Clients can detect missing subscriptions via update absence
   - Retry logic exists at the application layer
2. **Consider subscription confirmation callback** - Let clients know when subscription actually succeeds
3. **Implement subscription health monitoring** - Periodic verification that subscriptions are active

## Related Files

- `crates/core/src/operations/subscribe.rs` - Subscription operation logic
- `crates/core/src/operations/get.rs` - GET operation with failover for comparison
- `crates/core/src/config/mod.rs:40` - `OPERATION_TTL = 60 seconds`
- `crates/core/src/transport/sent_packet_tracker.rs` - RTO implementation
- `crates/core/src/ring/connection_backoff.rs` - Connection backoff logic
- `crates/core/src/node/op_state_manager.rs` - Operation timeout enforcement

## Conclusion

The subscription timeout issue is not caused by a single bug but by the cumulative effect of multiple defensive timeout mechanisms operating within a constrained 60-second budget. Under ideal conditions (no packet loss, contracts pre-propagated), subscriptions complete quickly. However, Docker NAT environments with their inherent latency and potential packet loss push the cumulative delays past the timeout threshold.

The async subscription approach in PR #2489 is a pragmatic workaround that trades atomicity for reliability. The deeper fix would require either:
1. Significantly increasing the timeout budget
2. Adding failover routing to subscriptions
3. Reducing individual timeout delays
4. Accepting async subscriptions as the intended design

Given the distributed nature of the system and the need for resilience, **option 4 (accepting async subscriptions as the design) may be the most appropriate**, with the addition of client-side monitoring for subscription health.
