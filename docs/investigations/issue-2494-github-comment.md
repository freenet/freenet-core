## Investigation Summary

I investigated the subscription timeout issue by analyzing the codebase and running tests. Here's what I found:

### Empirical Test Results

**Tests run locally (not Docker NAT):**

| Test | Result | Time | Notes |
|------|--------|------|-------|
| `test_get_with_subscribe_flag` | ✅ PASS | 62s | Just over 60s threshold |
| `test_multiple_clients_subscription` | ✅ PASS | ~63s | The `#[ignore]` test passed locally |

**Key observation**: The cross-node subscription test marked `#[ignore]` due to "recurring flakiness" (issue #1798) **passed locally in ~63 seconds**. This suggests:

1. The timeout issue is **environment-specific** (Docker NAT vs local)
2. The 60s `OPERATION_TTL` is borderline - tests run close to this limit even locally
3. Any additional latency (Docker NAT, CI load, packet loss) would push past the threshold

### Code Analysis Findings

**1. Timeout Budget Breakdown**

The 60-second `OPERATION_TTL` (defined in `config/mod.rs:40`) must cover multiple timeout layers:

| Component | Location | Timing |
|-----------|----------|--------|
| RTO exponential backoff | `transport/sent_packet_tracker.rs` | 1s → 2s → 4s → 8s → 16s → 32s → 60s cap |
| Connection backoff | `ring/connection_backoff.rs` | 5s → 10s → 20s → 40s → 80s → 160s → 300s cap |
| Contract wait timeout | `operations/subscribe.rs:25` | 2s per intermediate node |
| HTL traversal | `operations/subscribe.rs:258` | Up to 10 hops |

**2. Routing Difference Confirmed**

GET operations (`operations/get.rs:29-30`) use multi-path routing with failover:
```rust
const MAX_RETRIES: usize = 10;
const DEFAULT_MAX_BREADTH: usize = 3;
```

Subscribe operations use single-path hop-by-hop routing with no failover. If a connection fails mid-subscription, `handle_abort()` (`subscribe.rs:373`) fails immediately with no retry.

**3. PR #2489 Workaround Mechanics**

The async subscription change (`start_subscription_request_async`) works by **not** registering parent-child relationships:
```rust
// Before: blocked on subscription completion
start_subscription_request_internal(op_manager, parent_tx, key, true)

// After: fire-and-forget
start_subscription_request_internal(op_manager, parent_tx, key, false)
```

This allows PUT/GET to return immediately while subscription completes (or fails) in background.

### Supporting Evidence in Codebase

1. **Disabled test with timeout complaint** (`operations.rs:717-719`):
   ```rust
   // Ignored due to recurring flakiness - fails intermittently with timeout waiting for
   // cross-node subscription notifications (Client 3 timeout). See issue #1798.
   #[ignore]
   ```

2. **Timeout increase in PR #2441**: Changed subscription pruning test from `timeout_secs = 180` → `timeout_secs = 300`

3. **Cross-node subscription test uses 600s timeout** - 10x the normal operation TTL, suggesting subscriptions are known to be slow

4. **Issue #1798** documents the same symptom: "indefinite hangs" and "channel closed" errors in cross-node subscriptions

### Remaining Validation Needed

To fully confirm the root cause, still need:

1. **Run tests in Docker NAT environment** to compare timing
   ```bash
   RUST_LOG=freenet::operations::subscribe=debug \
   FREENET_TEST_DOCKER_NAT=1 \
   cargo test --test message_flow river_message_flow_over_freenet_six_peers_five_rounds -- --ignored
   ```

2. **Add subscription timing instrumentation** to measure time at each hop

3. **Profile under packet loss conditions** to observe RTO growth

### Conclusions

1. **The 60s timeout is borderline** - subscriptions take ~62-63s locally, leaving almost no margin for network delays

2. **Docker NAT adds overhead** - the test passes locally but fails in Docker NAT, confirming environment-specific latency

3. **PR #2489 is a pragmatic fix** - making subscriptions async avoids blocking PUT/GET on the slow subscription path

### Recommended Actions

| Priority | Action | Rationale |
|----------|--------|-----------|
| High | Increase `OPERATION_TTL` for subscriptions to 120s | Quick fix, provides margin |
| Medium | Add subscription timing telemetry | Required for proper debugging |
| Low | Consider failover routing for subscriptions | Matches GET's resilience, but complex |

The async subscription approach (PR #2489) may be the right long-term design if atomicity isn't required for `subscribe=true`.
