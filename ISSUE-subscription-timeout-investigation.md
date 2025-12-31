# Issue: Investigate root cause of subscription timeouts in Docker NAT environments

## Problem

PR #2489 introduced a workaround for subscription timeouts by making subscriptions async (not blocking PUT/GET responses). However, as noted by @iduartgomez in [comment](https://github.com/freenet/freenet-core/pull/2489#issuecomment-3701596853), this treats the symptom rather than the root cause:

> "This change defeats the purpose of implicit subscriptions... subs being flaky in the test which for 60secs in a network of 6 shouldn't be."

A 60-second timeout (`OPERATION_TTL`) should be more than sufficient for subscription propagation in a 6-peer network. The fact that subscriptions are timing out indicates an underlying issue that deserves investigation.

## Background

When `subscribe=true` is passed to PUT/GET operations:
1. The contract is stored/fetched
2. A child subscription operation is started
3. **Previously:** Response waited for subscription completion (atomic guarantee)
4. **After PR #2489:** Response sent immediately, subscription runs in background (best-effort)

This changes the API semantics - clients can no longer rely on `subscribe=true` providing atomic confirmation of subscription success.

## Potential Causes to Investigate

### 1. Subscription Routing Issues
**File:** `crates/core/src/operations/subscribe.rs:182-241`

Unlike GET operations, subscriptions have:
- Single-path routing (no alternative routes on failure)
- Fallback to any connected peer if `k_closest_potentially_caching` returns empty
- No retry mechanism if initial routing choice fails

### 2. Subscription Backoff Collision with TTL
**File:** `crates/core/src/ring/seeding.rs:17-21`

```rust
const INITIAL_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(5);
const MAX_SUBSCRIPTION_BACKOFF: Duration = Duration::from_secs(300);
```

Retry sequence: 5s → 10s → 20s → 40s → 80s...
Operations at 40s→80s boundary hit the 60s TTL mid-retry.

### 3. Network Retransmission Delays
**File:** `crates/core/src/transport/sent_packet_tracker.rs:18-28`

Under packet loss conditions (common in Docker NAT), RTO can grow exponentially: 1s → 2s → 4s → 8s → 16s → 32s → 60s

### 4. Contract Availability Wait
**File:** `crates/core/src/operations/subscribe.rs:25`

```rust
const CONTRACT_WAIT_TIMEOUT_MS: u64 = 2_000;
```

If subscription arrives before contract propagation, there's a 2-second wait that compounds other delays.

### 5. Docker NAT Specific Issues
- Variable latencies and packet loss from NAT gateway
- Connection timeouts not being recovered quickly
- Possible issues with address translation affecting routing decisions

## Known Related Issues

- Issue #1798 - Referenced in `crates/core/tests/operations.rs:714-718` as cause of test flakiness
- PR #2009 - Original parent-child atomicity tracking that PR #2489 effectively bypasses

## Investigation Plan

1. **Add detailed subscription tracing**: Log timing at each stage of subscription flow
2. **Analyze test failures**: Collect logs from `six-peer-regression` failures showing where time is spent
3. **Profile subscription routing**: Understand why `k_closest_potentially_caching` might return empty or slow candidates
4. **Test Docker network modes**: Compare behavior with different Docker networking configurations
5. **Stress test subscriptions**: Create targeted test for subscription-only timing in controlled conditions

## Success Criteria

- Identify why subscriptions take >60s in a 6-peer Docker network
- Either fix the root cause OR document why async subscriptions are the correct design
- If keeping async approach, update API documentation to reflect "best-effort" subscription semantics

## Related

- PR #2489 - The workaround that prompted this investigation
- PR #2487 - Added subscription tree telemetry (may help with analysis)
- PR #2492 - Added subscription snapshots (may help with analysis)
