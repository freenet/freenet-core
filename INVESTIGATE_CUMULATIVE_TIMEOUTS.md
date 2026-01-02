# Investigation Plan: Cumulative Timeout Hypothesis for #2494

## Hypothesis

Subscription operations are timing out because **cumulative delays** from multiple defensive mechanisms exceed the 60-second `OPERATION_TTL`, even though no individual component is failing.

## Components Contributing to Total Latency

From issue #2494 analysis:

1. **Contract wait timeout**: 2s per hop (when contract arrives before subscription)
2. **RTO exponential backoff**: 1s → 2s → 4s → 8s → 16s → 32s → 60s (under packet loss)
3. **Subscription retry backoff**: 5s → 10s → 20s → 40s → 80s
4. **Docker NAT overhead**: Unknown latency added by NAT gateway
5. **Routing delays**: Single-path routing with no failover

## Investigation Steps

---

### Step 1: Add Comprehensive Subscription Timing Instrumentation

**Goal**: Capture actual time spent in each stage of the subscription flow

**Implementation**: Add timing events at each critical point in the subscription path.

#### Code Changes

**File: `crates/core/src/operations/subscribe.rs`**

Add timing markers at:
- Subscription request start
- Contract availability check (before/after 2s wait)
- Each network hop
- Routing decisions
- Subscription completion/failure

```rust
// Add to subscribe.rs near the top
use std::time::Instant;

// In the subscription request handler
#[tracing::instrument(skip_all, fields(
    contract_key = %key,
    subscribe_start = ?Instant::now()
))]
pub async fn start_subscription_request(...) {
    let start = Instant::now();

    // At contract wait point (line ~25)
    tracing::info!(
        elapsed_ms = start.elapsed().as_millis(),
        stage = "contract_wait_start",
        timeout_ms = CONTRACT_WAIT_TIMEOUT_MS,
    );

    // After contract wait
    tracing::info!(
        elapsed_ms = start.elapsed().as_millis(),
        stage = "contract_wait_complete",
        contract_found = contract.is_some(),
    );

    // Before routing decision
    tracing::info!(
        elapsed_ms = start.elapsed().as_millis(),
        stage = "routing_decision",
        candidates_found = candidates.len(),
    );

    // At each network send
    tracing::info!(
        elapsed_ms = start.elapsed().as_millis(),
        stage = "network_send",
        target = %target_peer,
    );

    // On completion
    tracing::info!(
        elapsed_ms = start.elapsed().as_millis(),
        stage = "subscription_complete",
        total_hops = hop_count,
    );
}
```

**File: `crates/core/src/transport/sent_packet_tracker.rs`**

Add RTO timing events:

```rust
// In sent_packet_tracker.rs around line 18-28
fn calculate_rto(&self) -> Duration {
    let rto = /* existing calculation */;

    tracing::debug!(
        rto_ms = rto.as_millis(),
        rtt_ms = self.smoothed_rtt.as_millis(),
        rtt_var_ms = self.rtt_var.as_millis(),
        packet_id = self.current_packet_id,
    );

    rto
}

// When retransmitting
fn on_retransmit(&mut self) {
    tracing::warn!(
        retransmit_count = self.retransmit_count,
        backoff_rto_ms = (self.rto * 2u32.pow(self.retransmit_count)).as_millis(),
        packet_id = self.packet_id,
    );
}
```

**File: `crates/core/src/ring/seeding.rs`**

Add subscription retry backoff timing:

```rust
// Around line 17-21 where backoff is defined
tracing::info!(
    attempt = retry_count,
    backoff_ms = backoff.as_millis(),
    next_backoff_ms = (backoff * 2).min(MAX_SUBSCRIPTION_BACKOFF).as_millis(),
    contract_key = %key,
);
```

---

### Step 2: Create Instrumented Test Run

**Goal**: Collect real timing data from six-peer-regression test

**Test Script**: `scripts/measure-subscription-timing.sh`

```bash
#!/bin/bash
set -e

echo "Running instrumented subscription timing test..."

# Set detailed tracing for subscription path only
export RUST_LOG="freenet::operations::subscribe=trace,freenet::transport::sent_packet_tracker=debug,freenet::ring::seeding=info,error"

# Run six-peer-regression with Docker NAT
cd river-src
export FREENET_CORE_PATH=$(pwd)/..
export FREENET_TEST_DOCKER_NAT=1

# Capture output to file for analysis
cargo test --test message_flow river_message_flow_over_freenet_six_peers_five_rounds \
    -- --ignored --exact --nocapture 2>&1 | tee ../subscription-timing-$(date +%s).log

echo "Timing data captured to subscription-timing-*.log"
```

**Analysis Script**: `scripts/analyze-subscription-timing.py`

```python
#!/usr/bin/env python3
import re
import json
from collections import defaultdict
from datetime import datetime

def parse_timing_log(log_file):
    """Extract timing events from log file"""
    events = []

    # Match patterns like:
    # elapsed_ms=1234 stage="contract_wait_start"
    pattern = re.compile(r'elapsed_ms=(\d+)\s+stage="([^"]+)"(?:\s+(\w+)=([^\s]+))?')

    with open(log_file) as f:
        for line in f:
            match = pattern.search(line)
            if match:
                events.append({
                    'elapsed_ms': int(match.group(1)),
                    'stage': match.group(2),
                    'extra': {match.group(3): match.group(4)} if match.group(3) else {}
                })

    return events

def analyze_subscription_timeline(events):
    """Calculate time spent in each stage"""
    stages = defaultdict(list)

    for i, event in enumerate(events):
        if i > 0:
            time_in_stage = event['elapsed_ms'] - events[i-1]['elapsed_ms']
            stages[events[i-1]['stage']].append(time_in_stage)

    # Calculate statistics
    stats = {}
    for stage, times in stages.items():
        stats[stage] = {
            'count': len(times),
            'min_ms': min(times),
            'max_ms': max(times),
            'avg_ms': sum(times) / len(times),
            'total_ms': sum(times)
        }

    return stats

def main(log_file):
    events = parse_timing_log(log_file)
    stats = analyze_subscription_timeline(events)

    print("\n=== Subscription Timing Breakdown ===\n")
    print(f"{'Stage':<30} {'Count':>6} {'Min (ms)':>10} {'Avg (ms)':>10} {'Max (ms)':>10} {'Total (ms)':>12}")
    print("-" * 90)

    total_time = 0
    for stage, data in sorted(stats.items(), key=lambda x: x[1]['total_ms'], reverse=True):
        print(f"{stage:<30} {data['count']:>6} {data['min_ms']:>10.1f} {data['avg_ms']:>10.1f} {data['max_ms']:>10.1f} {data['total_ms']:>12.1f}")
        total_time += data['total_ms']

    print("-" * 90)
    print(f"{'TOTAL':<30} {' '*6} {' '*10} {' '*10} {' '*10} {total_time:>12.1f}")

    # Check if cumulative timeout is the issue
    if total_time > 60000:
        print(f"\n⚠️  CUMULATIVE TIMEOUT EXCEEDED: {total_time:.0f}ms > 60000ms")
        print("   Hypothesis CONFIRMED: Cumulative timeouts are causing failures")
    else:
        print(f"\n✓  Total time under limit: {total_time:.0f}ms < 60000ms")
        print("   Hypothesis REJECTED: Issue is not cumulative timeouts")

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Usage: analyze-subscription-timing.py <log-file>")
        sys.exit(1)
    main(sys.argv[1])
```

---

### Step 3: Model Worst-Case Timeout Scenario

**Goal**: Calculate theoretical maximum latency under worst conditions

**Analysis File**: `TIMEOUT_BUDGET_ANALYSIS.md`

```markdown
# Subscription Timeout Budget Analysis

## Constants (from codebase)

- `OPERATION_TTL`: 60,000ms (crates/core/src/operations/mod.rs)
- `CONTRACT_WAIT_TIMEOUT_MS`: 2,000ms (crates/core/src/operations/subscribe.rs:25)
- `INITIAL_SUBSCRIPTION_BACKOFF`: 5,000ms (crates/core/src/ring/seeding.rs:17)
- `MAX_SUBSCRIPTION_BACKOFF`: 300,000ms (crates/core/src/ring/seeding.rs:21)
- Initial RTO: 1,000ms (RFC 6298)
- Max RTO: ~60,000ms after 6 doublings

## Scenario 1: Normal Path (No Retries)

| Stage | Time | Cumulative |
|-------|------|------------|
| Contract wait (1 hop) | 2,000ms | 2,000ms |
| Network send + RTT | 100ms | 2,100ms |
| Response processing | 50ms | 2,150ms |
| **Total** | | **2,150ms** ✓ |

**Result**: Well under 60s limit

## Scenario 2: Contract Propagation Delay

Subscription arrives before contract at 3 intermediate hops:

| Stage | Time | Cumulative |
|-------|------|------------|
| Contract wait (hop 1) | 2,000ms | 2,000ms |
| Network hop 1 | 100ms | 2,100ms |
| Contract wait (hop 2) | 2,000ms | 4,100ms |
| Network hop 2 | 100ms | 4,200ms |
| Contract wait (hop 3) | 2,000ms | 6,200ms |
| Network hop 3 | 100ms | 6,300ms |
| Contract wait (hop 4) | 2,000ms | 8,300ms |
| Final processing | 100ms | 8,400ms |
| **Total** | | **8,400ms** ✓ |

**Result**: Still acceptable, but 3× longer than normal

## Scenario 3: Moderate Packet Loss (2 retransmissions per hop)

With 10% packet loss in Docker NAT:

| Stage | Time | Cumulative |
|-------|------|------------|
| Initial send | 100ms | 100ms |
| RTO timeout #1 | 1,000ms | 1,100ms |
| Retransmit #1 | 100ms | 1,200ms |
| RTO timeout #2 | 2,000ms | 3,200ms |
| Retransmit #2 succeeds | 100ms | 3,300ms |
| × 3 hops | × 3 | **9,900ms** ✓ |

**Result**: Getting close to 10s, but still acceptable

## Scenario 4: Subscription Retry (Routing Failure)

First subscription attempt fails at routing (no candidates):

| Stage | Time | Cumulative |
|-------|------|------------|
| Initial attempt (routing failure) | 1,000ms | 1,000ms |
| Backoff wait | 5,000ms | 6,000ms |
| Retry #1 (partial success) | 8,400ms | 14,400ms |
| Backoff wait | 10,000ms | 24,400ms |
| Retry #2 (success) | 8,400ms | 32,800ms |
| **Total** | | **32,800ms** ✓ |

**Result**: Over 30s, but still under 60s

## Scenario 5: WORST CASE - All Issues Combined

Contract delay + packet loss + subscription retry:

| Stage | Time | Cumulative |
|-------|------|------------|
| Attempt #1: Contract wait × 4 | 8,000ms | 8,000ms |
| Attempt #1: Packet loss (2 RTO) | 3,000ms | 11,000ms |
| Attempt #1: Routing failure | 1,000ms | 12,000ms |
| **Backoff wait** | **5,000ms** | **17,000ms** |
| Attempt #2: Contract wait × 4 | 8,000ms | 25,000ms |
| Attempt #2: Packet loss (3 RTO) | 7,000ms | 32,000ms |
| Attempt #2: Partial failure | 1,000ms | 33,000ms |
| **Backoff wait** | **10,000ms** | **43,000ms** |
| Attempt #3: Contract wait × 2 | 4,000ms | 47,000ms |
| Attempt #3: Packet loss (2 RTO) | 3,000ms | 50,000ms |
| Attempt #3: Success | 2,000ms | 52,000ms |
| **Total** | | **52,000ms** ⚠️ |

**Result**: Dangerously close to 60s limit!

## Scenario 6: TIMEOUT FAILURE

Same as Scenario 5, but one more retry:

| Stage | Time | Cumulative |
|-------|------|------------|
| [Scenarios 1-3 from above] | | 43,000ms |
| Attempt #3: Full contract wait path | 8,000ms | 51,000ms |
| Attempt #3: Heavy packet loss (4 RTO) | 15,000ms | 66,000ms |
| **Total** | | **66,000ms** ❌ |

**Result**: TIMEOUT! Exceeds 60s OPERATION_TTL

## Conclusion

**Hypothesis: CONFIRMED**

The cumulative timeout hypothesis is **valid**. Under realistic worst-case conditions:
- Contract propagation delays (2s × 4 hops = 8s)
- Moderate packet loss (3-7s per hop)
- Subscription retries with backoff (5s + 10s = 15s)

**Total can easily exceed 60 seconds**, especially in Docker NAT environments where packet loss is higher.

## Recommendations

### Option 1: Increase OPERATION_TTL
Change from 60s → 120s to provide more headroom

**Pros**: Simple fix, accommodates real network conditions
**Cons**: Slow failure detection, locks resources longer

### Option 2: Reduce Per-Stage Timeouts
- CONTRACT_WAIT_TIMEOUT: 2,000ms → 1,000ms
- Subscription backoff: 5s → 2s initial

**Pros**: Faster individual operations
**Cons**: More aggressive, may cause false failures

### Option 3: Adaptive Timeout Budget
Track time spent so far, reduce stage timeouts proportionally

**Pros**: Optimal use of time budget
**Cons**: Complex implementation

### Option 4: Remove Blocking (Status Quo - PR #2489)
Keep async subscriptions, accept best-effort semantics

**Pros**: Already implemented
**Cons**: Changes API guarantees

### Option 5: Parallel Multi-Path Subscription
Send subscription requests via multiple routes simultaneously

**Pros**: Resilient to single-path failures
**Cons**: More network traffic, complex deduplication
```

---

### Step 4: Isolate Docker NAT Overhead

**Goal**: Measure how much latency Docker NAT adds vs localhost

**Test Script**: `scripts/compare-docker-vs-localhost.sh`

```bash
#!/bin/bash
set -e

echo "=== Comparing Docker NAT vs Localhost Timing ==="

# Test 1: Localhost (no NAT)
echo -e "\n[1/2] Running test with localhost (no NAT)..."
export FREENET_TEST_DOCKER_NAT=0
export RUST_LOG="error"

cd river-src
export FREENET_CORE_PATH=$(pwd)/..

time cargo test --test message_flow river_message_flow_over_freenet_six_peers_five_rounds \
    -- --ignored --exact 2>&1 | tee ../localhost-timing.log

# Extract timing from logs
LOCALHOST_TIME=$(grep -oP 'test result:.* \K[\d.]+s' ../localhost-timing.log | tail -1)

# Test 2: Docker NAT
echo -e "\n[2/2] Running test with Docker NAT..."
export FREENET_TEST_DOCKER_NAT=1

time cargo test --test message_flow river_message_flow_over_freenet_six_peers_five_rounds \
    -- --ignored --exact 2>&1 | tee ../docker-nat-timing.log

DOCKER_TIME=$(grep -oP 'test result:.* \K[\d.]+s' ../docker-nat-timing.log | tail -1)

# Compare
echo -e "\n=== Results ==="
echo "Localhost:   ${LOCALHOST_TIME}s"
echo "Docker NAT:  ${DOCKER_TIME}s"

# Calculate overhead
python3 -c "
localhost = float('${LOCALHOST_TIME}'.rstrip('s'))
docker = float('${DOCKER_TIME}'.rstrip('s'))
overhead = docker - localhost
percent = (overhead / localhost) * 100
print(f'Overhead:    {overhead:.2f}s ({percent:.1f}% increase)')
print()
if overhead > 10:
    print('⚠️  Docker NAT adds significant overhead (>10s)')
    print('   This contributes to cumulative timeout issues')
else:
    print('✓  Docker NAT overhead is acceptable (<10s)')
"
```

---

### Step 5: Correlate with LEDBAT++ Changes

**Goal**: Determine if LEDBAT++ parameter changes affected timeout sensitivity

**Test**: Compare behavior before/after PR #2472

```bash
#!/bin/bash
# scripts/test-ledbat-timing.sh

echo "Testing subscription timing with different LEDBAT++ parameters..."

# Test with current LEDBAT++ params (TARGET=60ms)
echo -e "\n[1/2] Current LEDBAT++ (TARGET=60ms)..."
cargo test --test message_flow river_message_flow_over_freenet_six_peers_five_rounds \
    -- --ignored --exact 2>&1 | grep -E "(elapsed|timeout)" > ledbat-new.log

# Temporarily revert to old LEDBAT params (TARGET=100ms)
echo -e "\n[2/2] Old LEDBAT (TARGET=100ms)..."
# Would need to modify constants temporarily
git stash
git show c8834e5:crates/core/src/transport/congestion_control.rs > /tmp/old-ledbat.rs
cp /tmp/old-ledbat.rs crates/core/src/transport/congestion_control.rs
cargo build --release

cargo test --test message_flow river_message_flow_over_freenet_six_peers_five_rounds \
    -- --ignored --exact 2>&1 | grep -E "(elapsed|timeout)" > ledbat-old.log

git stash pop

# Compare
diff -u ledbat-old.log ledbat-new.log || true
```

---

### Step 6: Validate with Telemetry Data

**Goal**: Use production telemetry to see if real-world subscriptions exhibit same pattern

**Query**: If telemetry is being collected, query for subscription timing events

```bash
# If telemetry is available via API/logs
curl -s https://telemetry.freenet.org/api/events \
    -d 'event_type=SubscriptionComplete' \
    -d 'since=7days' | jq '
    .events[]
    | select(.duration_ms > 60000)
    | {peer_id, duration_ms, hop_count, failed_hops, retry_count}
    ' > subscription-timeouts.json

# Analyze
python3 scripts/analyze-telemetry-timeouts.py subscription-timeouts.json
```

---

## Execution Plan

### Phase 1: Data Collection (Week 1)
- [ ] Add timing instrumentation (Step 1)
- [ ] Run instrumented tests (Step 2)
- [ ] Collect 10+ test runs for statistical significance
- [ ] Run Docker vs localhost comparison (Step 4)

### Phase 2: Analysis (Week 2)
- [ ] Analyze timing logs with Python script
- [ ] Complete worst-case budget analysis (Step 3)
- [ ] Correlate with LEDBAT++ changes if possible (Step 5)
- [ ] Review production telemetry data (Step 6)

### Phase 3: Conclusion (Week 3)
- [ ] Synthesize findings
- [ ] Determine if cumulative timeout hypothesis is confirmed
- [ ] If confirmed: Recommend solution (Options 1-5 from Step 3)
- [ ] If rejected: Identify actual root cause from data
- [ ] Update #2494 with findings

## Success Criteria

**Hypothesis CONFIRMED if:**
- Average total subscription time in Docker NAT > 50s
- Breakdown shows time distributed across multiple stages (not one bottleneck)
- Contract wait + RTO backoff + retry backoff accounts for >80% of time

**Hypothesis REJECTED if:**
- One specific stage dominates (e.g., 55s spent in routing)
- Total time under 40s but still failing
- Different failure mode discovered (e.g., deadlock, race condition)

## Next Steps Based on Outcome

### If Confirmed ✓
1. Discuss trade-offs of Options 1-5 with team
2. Implement chosen solution
3. Validate with same instrumented tests
4. Document decision in #2494

### If Rejected ✗
1. Analyze which stage is the actual bottleneck
2. Deep dive into that specific component
3. Create new focused investigation
4. Update #2494 with new hypothesis
