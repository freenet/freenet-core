# Root Cause Investigation: Why Are Subscriptions Taking 30-60s in a 6-Peer Network?

## The Core Problem

**Expected**: Subscription in 6-peer network should take 2-3 seconds
**Actual**: Taking 30-60+ seconds, often timing out

The cumulative timeout analysis shows that timeouts CAN be exceeded, but **the real question is why the delays exist at all**.

---

## Root Cause Questions

### 1. Why Does Contract Wait (2s) Trigger Multiple Times?

**File**: `crates/core/src/operations/subscribe.rs:25`
```rust
const CONTRACT_WAIT_TIMEOUT_MS: u64 = 2_000;
```

**The Question**: Why doesn't the contract arrive before the subscription?

**Expected behavior**:
- Client does PUT (stores contract)
- Client immediately does SUBSCRIBE for same contract
- Contract should already be at gateway and propagating
- Subscription should find contract immediately (0ms wait, not 2s)

**Possible causes**:
1. **PUT/SUBSCRIBE race condition** - Subscription sent before PUT completes propagation
2. **Routing mismatch** - Subscription goes to different peers than PUT
3. **Contract eviction** - Contract gets evicted from cache before subscription arrives
4. **No contract caching** - Intermediate nodes don't cache contracts during PUT

**Investigation**:
```rust
// In subscribe.rs, add this logging
if contract.is_none() {
    tracing::warn!(
        contract_key = %key,
        waited_ms = CONTRACT_WAIT_TIMEOUT_MS,
        "Contract not found - subscription arrived before PUT completed?",
        recent_puts = self.recent_puts.contains(&key),  // Add tracking
    );
}
```

**Test hypothesis**: Add explicit delay between PUT and SUBSCRIBE
```rust
// In six-peer-regression test
client.put(contract, subscribe=false).await?;
tokio::time::sleep(Duration::from_secs(5)).await;  // Ensure PUT completes
client.subscribe(contract_key).await?;  // Should be instant now
```

If this fixes it: **Root cause is PUT/SUBSCRIBE race, not network delays**

---

### 2. Why Is There Packet Loss in Docker NAT?

**File**: `crates/core/src/transport/sent_packet_tracker.rs:18-28`

**The Question**: Is Docker NAT causing real packet loss, or is something else happening?

**Expected**: Docker NAT should have <1% packet loss on localhost network
**Suspected**: Seeing 10-20% packet loss, causing RTO backoffs

**Possible causes**:

#### 2a. UDP Buffer Overflow
Docker NAT may have small UDP buffers. LEDBAT++ can ramp up quickly, overwhelming buffers.

**Test**:
```bash
# Check Docker container UDP buffer sizes
docker exec <container> sysctl net.core.rmem_default
docker exec <container> sysctl net.core.rmem_max

# Increase if too small
docker run --sysctl net.core.rmem_max=26214400 ...
```

#### 2b. NAT Conntrack Table Saturation
NAT connection tracking table might be dropping packets when full.

**Test**:
```bash
# Check conntrack table
docker exec <nat-router> cat /proc/sys/net/netfilter/nf_conntrack_count
docker exec <nat-router> cat /proc/sys/net/netfilter/nf_conntrack_max

# Increase if count ≈ max
docker run --sysctl net.netfilter.nf_conntrack_max=131072 ...
```

#### 2c. LEDBAT++ Too Aggressive
LEDBAT++ with TARGET=60ms might be ramping up too fast for Docker's virtual network.

**File**: `crates/core/src/transport/congestion_control.rs`

**Test**: Temporarily increase TARGET delay
```rust
// Change from 60ms to 100ms (pre-LEDBAT++ value)
const TARGET: Duration = Duration::from_millis(100);
```

If packet loss drops: **LEDBAT++ is outpacing Docker NAT capacity**

#### 2d. Port Reuse Issues
Rapid connection setup/teardown might cause port reuse before NAT timeout.

**Investigation**:
```rust
// Log when we see decryption errors (from issue #2528)
tracing::error!(
    peer_addr = %addr,
    our_port = local_port,
    last_seen = ?last_connection_time,
    time_since_last = ?now.duration_since(last_connection_time),
    "Decryption error - port reuse too fast?"
);
```

---

### 3. Why Does Subscription Routing Fail?

**File**: `crates/core/src/operations/subscribe.rs:182-241`

**The Question**: Why does `k_closest_potentially_caching` return empty, triggering fallback to random peer?

**Expected**: In a 6-peer network, should always find k-closest peers
**Suspected**: Routing table incomplete or contract location prediction failing

**Possible causes**:

#### 3a. Incomplete Routing Table
Peers haven't discovered each other yet when subscription runs.

**Investigation**:
```rust
fn k_closest_potentially_caching(...) -> Vec<PeerId> {
    let candidates = self.routing_table.k_closest(key, k);

    tracing::info!(
        contract_key = %key,
        k = k,
        candidates_found = candidates.len(),
        total_peers_known = self.routing_table.len(),
        "Routing candidates for subscription",
    );

    if candidates.is_empty() {
        tracing::warn!(
            "No routing candidates - routing table incomplete?",
            known_peers = ?self.routing_table.peer_ids(),
        );
    }

    candidates
}
```

**Test**: Add explicit peer discovery phase before subscription
```rust
// In test setup
network.wait_for_full_connectivity().await?;  // Ensure all peers know each other
tokio::time::sleep(Duration::from_secs(2)).await;  // Let routing stabilize
```

#### 3b. Contract Location Prediction Wrong
The "potentially_caching" heuristic might be wrong, sending subscription to peers that don't have the contract.

**Investigation**: Compare PUT routing with SUBSCRIBE routing
```rust
// During PUT
tracing::info!(
    contract_key = %key,
    put_targets = ?selected_peers,
    "PUT routing decision"
);

// During SUBSCRIBE
tracing::info!(
    contract_key = %key,
    subscribe_targets = ?selected_peers,
    put_targets_overlap = ?put_targets.intersection(&subscribe_targets).count(),
    "SUBSCRIBE routing decision"
);
```

If overlap is 0: **Subscription going to wrong peers**

#### 3c. Single-Path Routing Fragility
Unlike GET (multi-path), subscription uses single-path routing with no retry.

**File**: `crates/core/src/operations/subscribe.rs:182-241`

**Current**:
```rust
// If k_closest returns empty, fall back to ANY connected peer
let target = self.routing_table.random_peer()
```

This is a **disaster** - in a 6-peer network, random peer might not have contract!

**Better approach**:
```rust
// Try multiple paths in parallel, first success wins
let candidates = self.routing_table.k_closest(key, 3);
let results = join_all(
    candidates.iter().map(|peer| self.send_subscribe(peer, key))
).await;

// Use first successful response
```

---

### 4. Why Are There Subscription Retries?

**File**: `crates/core/src/ring/seeding.rs:17-21`

**The Question**: What's causing subscriptions to fail and trigger 5s/10s/20s backoff?

**Expected**: First attempt should succeed in small network
**Suspected**: Some failure mode causing retry spiral

**Possible causes**:

#### 4a. Upstream Peer Selection Fails
Can't find suitable upstream peer for subscription tree.

**Investigation**:
```rust
// In seeding.rs where upstream is selected
match self.select_upstream_for_contract(key) {
    Some(upstream) => {
        tracing::info!(
            contract_key = %key,
            upstream_peer = %upstream,
            "Selected upstream for subscription"
        );
    }
    None => {
        tracing::error!(
            contract_key = %key,
            connected_peers = self.connected_peers.len(),
            "No suitable upstream peer found - retry with backoff",
            retry_count = self.retry_count,
        );
        // This triggers 5s backoff!
    }
}
```

#### 4b. Circular Reference Detection Firing
PR #2541 added circular reference detection. Is it being too aggressive?

**Investigation**:
```rust
// In seeding.rs from PR #2541
fn add_downstream(...) -> Result<(), SubscriptionError> {
    if subscriber_addr == self.own_addr {
        tracing::warn!(
            "Rejected self-reference in subscription tree",
            contract_key = %key,
        );
        return Err(SubscriptionError::SelfReference);
    }

    if self.is_in_upstream(key, subscriber_addr) {
        tracing::warn!(
            "Rejected circular reference in subscription tree",
            contract_key = %key,
            would_be_circular_with = %subscriber_addr,
        );
        return Err(SubscriptionError::CircularReference);
    }

    Ok(())
}
```

Are these errors common? They would cause subscription retry with backoff.

---

### 5. Why Does Docker NAT Make It Worse?

**The Question**: Same test passes on localhost, fails in Docker NAT. What's different?

**Possible causes**:

#### 5a. NAT State Timeout
NAT mapping times out during subscription backoff, requiring re-handshake.

**Test**: Check NAT timeout settings
```bash
# Default is usually 30s for UDP
docker exec <nat-router> cat /proc/sys/net/netfilter/nf_conntrack_udp_timeout
```

If 30s and subscription retry is 5s→10s→20s:
- After first retry (5s): NAT mapping still valid
- After second retry (10s): NAT mapping still valid
- After third retry (20s): NAT mapping might expire during operation
- Total time: 35s, NAT expires, connection lost, retry from scratch

**Fix**: Increase NAT UDP timeout
```bash
docker run --sysctl net.netfilter.nf_conntrack_udp_timeout=180 ...
```

#### 5b. Docker Network Latency
Virtual network adds latency that compounds exponential backoffs.

**Test**: Measure actual RTT in Docker vs localhost
```rust
// In transport layer
tracing::debug!(
    peer = %peer_addr,
    rtt_ms = smoothed_rtt.as_millis(),
    environment = if peer_addr.ip().is_loopback() { "localhost" } else { "docker" },
);
```

Expected localhost: <1ms
Expected Docker NAT: 5-20ms

If Docker is 20ms and RTO is 4× RTT = 80ms, that's already significant.

#### 5c. Docker Bridge MTU Issues
Fragmentation from MTU mismatch could cause packet loss.

**Test**: Check MTU
```bash
docker network inspect bridge | grep MTU
# Should be 1500

# Inside container
ip link show eth0
```

If container MTU < 1500, large packets fragment and may be dropped.

---

## Investigation Priority Order

### Phase 1: Quick Wins (1 day)

1. **Test PUT/SUBSCRIBE race** - Add 5s delay between PUT and SUBSCRIBE in test
   - If fixes it: Root cause is ordering, not network
   - Fix: Ensure PUT completion before SUBSCRIBE, or remove contract wait timeout

2. **Check Docker NAT configuration** - UDP buffers, conntrack, MTU
   - If undersized: Increase limits
   - Fix: Update Docker test setup in CI

3. **Log routing decisions** - See if subscription finding right peers
   - If wrong peers: Routing bug
   - Fix: Improve routing or add multi-path

### Phase 2: Deep Dive (3 days)

4. **Instrument subscription flow** - Add comprehensive logging
   - Identify which specific stage takes 30-60s
   - May reveal unexpected bottleneck

5. **Compare LEDBAT++ parameters** - Test with old vs new TARGET
   - If new is problematic: Tune parameters
   - Fix: Adjust TARGET or other params for Docker

6. **Analyze retry patterns** - Why are retries happening?
   - If circular ref detection: Tune validation
   - If no upstream: Fix peer discovery

### Phase 3: Architectural (1 week)

7. **Multi-path subscription** - Send via multiple routes
   - Makes subscription resilient to single-path failures
   - More work but fundamentally more robust

8. **Adaptive timeouts** - Reduce timeouts based on time budget
   - Prevents cumulative timeout issues
   - Complex but elegant solution

---

## My Hypothesis: The Real Root Cause

Based on the evidence, I suspect **combination of #1 and #3**:

### Primary: PUT/SUBSCRIBE Race + Single-Path Routing Failure

1. Client sends PUT, gets response (contract stored at gateway)
2. Client immediately sends SUBSCRIBE
3. **SUBSCRIBE arrives at gateway before PUT has propagated to other peers**
4. Gateway's `k_closest_potentially_caching` returns empty (contract not propagated yet)
5. Gateway falls back to **random peer** (disaster!)
6. Random peer doesn't have contract, triggers 2s contract wait
7. After 2s, peer still doesn't have it, subscription fails
8. Retry with 5s backoff
9. Repeat multiple times

**Why Docker NAT is worse**:
- Extra latency makes timing windows wider
- Packet loss from buffer overflow makes retries more common
- NAT timeouts during long backoffs kill connections

### Test This Hypothesis

```rust
// In six-peer-regression test, add explicit ordering
println!("1. Sending PUT...");
let response = client.put(contract, subscribe=false).await?;
println!("   PUT succeeded, contract stored");

println!("2. Waiting for propagation...");
tokio::time::sleep(Duration::from_secs(10)).await;

println!("3. Verifying contract is cached...");
let get_response = client.get(contract_key).await?;
assert!(get_response.is_ok(), "Contract should be retrievable");

println!("4. Now sending SUBSCRIBE...");
let start = Instant::now();
let subscribe_response = client.subscribe(contract_key).await?;
println!("   SUBSCRIBE completed in {:?}", start.elapsed());

// If this completes in <3s: hypothesis confirmed
// If still takes 30s: hypothesis rejected, something else is wrong
```

---

## Next Steps

1. **Run the hypothesis test** (30 minutes)
2. **Add routing decision logging** (1 hour)
3. **Check Docker NAT config** (30 minutes)
4. **Analyze results** (2 hours)

This should tell us definitively whether the issue is:
- **A) Race condition** (PUT/SUBSCRIBE ordering)
- **B) Network configuration** (Docker NAT limits)
- **C) Routing bug** (wrong peers selected)
- **D) Something else** (instrumentation will reveal)

Want me to implement the hypothesis test?
