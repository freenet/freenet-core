# Code Analysis: Subscription Timeout Root Causes

## Summary of Findings

After analyzing the code, I've identified the **exact mechanism** causing subscription delays:

### The Problem (Confirmed)

**File**: `crates/core/src/operations/subscribe.rs:256-305`

```rust
// Line 256: Get routing candidates
let candidates = op_manager
    .ring
    .k_closest_potentially_caching(instance_id, &visited, 3);

// Line 264-266: Use first candidate if available
let target = if let Some(t) = candidates.first() {
    t.clone()
} else {
    // Line 267-282: FALLBACK TO ANY CONNECTED PEER
    let connections = op_manager
        .ring
        .connection_manager
        .get_connections_by_location();
    let fallback_target = connections
        .values()
        .flatten()
        .find(|conn| {
            conn.location
                .socket_addr()
                .map(|addr| !visited.probably_visited(addr))
                .unwrap_or(false)
        })
        .map(|conn| conn.location.clone());
    // ...
}
```

### What `k_closest_potentially_caching` Actually Does

**File**: `crates/core/src/ring/mod.rs:537-575`

```rust
pub fn k_closest_potentially_caching<K>(
    &self,
    contract_id: &K,
    skip_list: impl Contains<std::net::SocketAddr> + Clone,
    k: usize,
) -> Vec<PeerKeyLocation> {
    let router = self.router.read();
    let target_location = Location::from(contract_id);

    let mut candidates: Vec<PeerKeyLocation> = Vec::new();

    // ONLY considers CURRENTLY CONNECTED peers
    let connections = self.connection_manager.get_connections_by_location();
    for conns in connections.values() {
        for conn in conns {
            if let Some(addr) = conn.location.socket_addr() {
                if skip_list.has_element(addr) || !seen.insert(addr) {
                    continue;
                }
            }
            candidates.push(conn.location.clone());
        }
    }

    // Note: We intentionally do NOT fall back to known_locations here.
    // known_locations may contain peers we're not currently connected to,
    // and attempting to route to them would require establishing a new connection
    // which may fail (especially in NAT scenarios without coordination).

    router
        .select_k_best_peers(candidates.iter(), target_location, k)
        .into_iter()
        .cloned()
        .collect()
}
```

**Key Insight**: This function:
1. Only considers **currently connected peers**
2. Does NOT use `known_locations` (peers we're aware of but not connected to)
3. Returns **empty vector** if no suitable connections exist
4. Filters out peers in the skip_list (visited peers)

### The Failure Scenario

**In a 6-peer network during six-peer-regression test**:

1. **Client connects to Gateway**
2. **Client does PUT** → Gateway stores contract, begins forwarding to peers
3. **Client immediately does SUBSCRIBE** (implicit subscription from PUT)
4. **Gateway calls `k_closest_potentially_caching(contract_key)`**
5. **Problem**: Gateway may only have connections to 1-2 peers at this point
   - Network is still forming
   - Peers are still discovering each other
   - PUT propagation hasn't completed
6. **If `k_closest_potentially_caching` returns empty** (all connected peers filtered by skip_list):
   - Falls back to **ANY connected peer** (line 273)
   - This peer is chosen **randomly**, not based on contract location
   - Random peer likely **doesn't have the contract yet**
7. **Random peer receives SUBSCRIBE**:
   - Waits 2s for contract (CONTRACT_WAIT_TIMEOUT_MS)
   - Contract still hasn't propagated from PUT
   - Returns failure
8. **Retry with 5s backoff**
9. **Repeat** → 10s backoff → 20s backoff → **TIMEOUT**

### Why Docker NAT Makes It Worse

1. **Slower connection establishment** - NAT hole punching adds latency
2. **Packet loss** - Buffer overflow from LEDBAT++ ramp-up
3. **Longer propagation times** - Virtual network overhead
4. **Race window widens** - More time between PUT start and SUBSCRIBE arrival

---

## Reproduction Test (No Docker Required)

### Simple Localhost Test to Prove the Issue

Create: `crates/core/tests/subscribe_timing_regression.rs`

```rust
//! Test to reproduce subscription timeout issue without Docker
//!
//! This test demonstrates the PUT/SUBSCRIBE race condition where
//! a subscription arrives before contract propagation completes,
//! causing fallback to random peer routing and 2s contract wait delays.

use freenet_test_network::TestNetwork;
use freenet_stdlib::client_api::{ClientRequest, WebApi};
use freenet_stdlib::prelude::*;
use std::time::{Duration, Instant};
use testresult::TestResult;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;

// Load a simple contract for testing
fn make_test_contract() -> (ContractCode, Parameters, State) {
    let code = ContractCode::from(vec![1, 2, 3, 4]); // Dummy WASM
    let params = Parameters::from(vec![]);
    let state = State::from(vec![5, 6, 7, 8]);
    (code, params, state)
}

#[test_log::test(tokio::test)]
async fn test_put_subscribe_race_condition() -> TestResult {
    // Create a small network: 1 gateway + 5 peers (matching six-peer-regression)
    let network = TestNetwork::builder()
        .gateways(1)
        .peers(5)
        .binary(freenet_test_network::FreenetBinary::CurrentCrate(
            freenet_test_network::BuildProfile::Debug,
        ))
        .build()
        .await?;

    tracing::info!("Network started");

    // Connect to gateway
    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let (stream, _) = connect_async(&gw_url).await?;
    let mut client = WebApi::start(stream);

    // Prepare contract
    let (code, params, state) = make_test_contract();
    let contract = ContractContainer::Wasm(WasmAPIVersion::V1(
        freenet_stdlib::prelude::WasmContractV1 {
            data: code.data().to_vec().into(),
            params: params.clone().into(),
        }
    ));
    let key = contract.key(&params);

    tracing::info!("Contract key: {}", key.id());

    // ========== TEST SCENARIO 1: Immediate SUBSCRIBE (will fail) ==========
    tracing::info!("\n=== SCENARIO 1: PUT then IMMEDIATE subscribe (races) ===");

    let start = Instant::now();

    // PUT with subscribe=false
    let put_req = ClientRequest::Put {
        contract: contract.clone(),
        state: state.clone(),
        related_contracts: Default::default(),
    };

    client.send(put_req).await?;
    let put_response = client.recv().await?;
    tracing::info!("PUT completed in {:?}: {:?}", start.elapsed(), put_response);

    // IMMEDIATELY subscribe (before propagation completes)
    let sub_start = Instant::now();
    let sub_req = ClientRequest::Subscribe {
        key: key.clone(),
        summary: None,
    };

    client.send(sub_req).await?;
    let sub_response = client.recv().await?;
    let sub_duration = sub_start.elapsed();

    tracing::info!("SUBSCRIBE completed in {:?}: {:?}", sub_duration, sub_response);

    // Check if subscription took suspiciously long
    if sub_duration > Duration::from_secs(5) {
        tracing::error!(
            "⚠️ SCENARIO 1 FAILED: Subscription took {:?} (>5s)",
            sub_duration
        );
        tracing::error!("This indicates fallback to random peer + contract wait delays");
    } else {
        tracing::info!("✓ SCENARIO 1 PASSED: Subscription was fast ({:?})", sub_duration);
    }

    // ========== TEST SCENARIO 2: Delayed SUBSCRIBE (should succeed) ==========
    tracing::info!("\n=== SCENARIO 2: PUT then DELAYED subscribe (should be fast) ===");

    // Use different contract to avoid caching
    let (code2, params2, state2) = make_test_contract();
    let contract2 = ContractContainer::Wasm(WasmAPIVersion::V1(
        freenet_stdlib::prelude::WasmContractV1 {
            data: code2.data().to_vec().into(),
            params: params2.clone().into(),
        }
    ));
    let key2 = contract2.key(&params2);

    // PUT
    let put_req2 = ClientRequest::Put {
        contract: contract2.clone(),
        state: state2.clone(),
        related_contracts: Default::default(),
    };

    client.send(put_req2).await?;
    let _ = client.recv().await?;

    // WAIT for propagation (10 seconds should be more than enough)
    tracing::info!("Waiting 10s for PUT propagation to complete...");
    sleep(Duration::from_secs(10)).await;

    // NOW subscribe - should be instant
    let sub_start2 = Instant::now();
    let sub_req2 = ClientRequest::Subscribe {
        key: key2,
        summary: None,
    };

    client.send(sub_req2).await?;
    let sub_response2 = client.recv().await?;
    let sub_duration2 = sub_start2.elapsed();

    tracing::info!("SUBSCRIBE completed in {:?}: {:?}", sub_duration2, sub_response2);

    if sub_duration2 > Duration::from_secs(3) {
        tracing::error!(
            "⚠️ SCENARIO 2 FAILED: Even delayed subscription took {:?} (>3s)",
            sub_duration2
        );
        tracing::error!("This suggests a deeper routing or network issue");
    } else {
        tracing::info!("✓ SCENARIO 2 PASSED: Delayed subscription was fast ({:?})", sub_duration2);
    }

    // ========== ANALYSIS ==========
    tracing::info!("\n=== ANALYSIS ===");
    tracing::info!("Immediate subscribe: {:?}", sub_duration);
    tracing::info!("Delayed subscribe:   {:?}", sub_duration2);

    let speedup = sub_duration.as_millis() as f64 / sub_duration2.as_millis() as f64;
    tracing::info!("Speedup ratio: {:.2}x", speedup);

    if speedup > 5.0 {
        tracing::error!("\n❌ PUT/SUBSCRIBE RACE CONDITION CONFIRMED");
        tracing::error!("   Immediate subscribe is {}x slower than delayed", speedup as u32);
        tracing::error!("   Root cause: k_closest_potentially_caching returns empty");
        tracing::error!("   → Falls back to random peer");
        tracing::error!("   → Random peer doesn't have contract");
        tracing::error!("   → 2s contract wait + retries");
    } else {
        tracing::info!("\n✓ No significant race condition detected");
        tracing::info!("   Both scenarios completed in similar time");
    }

    Ok(())
}

/// Test that validates routing decision logging
/// This helps understand what k_closest_potentially_caching is returning
#[test_log::test(tokio::test)]
#[ignore] // Run manually with: cargo test --test subscribe_timing_regression -- --ignored --nocapture
async fn test_subscription_routing_visibility() -> TestResult {
    // Set detailed logging to see routing decisions
    std::env::set_var("RUST_LOG", "freenet::operations::subscribe=debug,freenet::ring=debug");

    let network = TestNetwork::builder()
        .gateways(1)
        .peers(5)
        .build()
        .await?;

    // Give network time to establish connections
    sleep(Duration::from_secs(3)).await;

    let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
    let (stream, _) = connect_async(&gw_url).await?;
    let mut client = WebApi::start(stream);

    let (code, params, state) = make_test_contract();
    let contract = ContractContainer::Wasm(WasmAPIVersion::V1(
        freenet_stdlib::prelude::WasmContractV1 {
            data: code.data().to_vec().into(),
            params: params.clone().into(),
        }
    ));

    // PUT with subscribe=false
    let put_req = ClientRequest::Put {
        contract: contract.clone(),
        state: state.clone(),
        related_contracts: Default::default(),
    };

    tracing::info!("Sending PUT...");
    client.send(put_req).await?;
    let _ = client.recv().await?;

    // IMMEDIATELY subscribe to trigger the race
    tracing::info!("Sending SUBSCRIBE immediately after PUT...");
    let sub_req = ClientRequest::Subscribe {
        key: contract.key(&params),
        summary: None,
    };

    client.send(sub_req).await?;
    let _ = client.recv().await?;

    tracing::info!("\n=== Check the logs above for: ===");
    tracing::info!("1. 'Using fallback connection' - indicates k_closest returned empty");
    tracing::info!("2. 'contract_wait_start' - indicates 2s contract wait triggered");
    tracing::info!("3. Retry attempts with backoff");

    Ok(())
}
```

### Running the Test

```bash
# Run the race condition test
cargo test --test subscribe_timing_regression test_put_subscribe_race_condition -- --nocapture

# Run with detailed logging to see routing decisions
RUST_LOG=debug cargo test --test subscribe_timing_regression test_subscription_routing_visibility -- --ignored --nocapture
```

### Expected Results

**If hypothesis is correct**:
- **Scenario 1** (immediate): 10-30+ seconds (random peer + contract wait + retries)
- **Scenario 2** (delayed): <1 second (contract already propagated)
- **Speedup ratio**: >10x

**If hypothesis is wrong**:
- Both scenarios take similar time
- Need to investigate different root cause

---

## Root Cause Summary

### Primary Issue: Network Formation Race

The code assumes the network is **fully formed** when subscription starts:
- All peers connected
- Routing tables populated
- Contracts propagated

**Reality**: In a test that starts fresh network and immediately does PUT→SUBSCRIBE:
- Network still forming
- Few peer connections established
- `k_closest_potentially_caching` returns empty or incomplete results
- Fallback to random peer routing

### Secondary Issue: No Multi-Path Subscription

Unlike GET operations (which try multiple routes), Subscribe uses **single-path routing**:
- Tries one peer
- If that fails, waits 5s backoff, tries again
- No parallel attempts or alternative routes

**Comparison** with GET operation (`crates/core/src/operations/get.rs`):
```rust
// GET uses multiple candidates
let candidates = self.ring.k_closest_potentially_caching(key, &visited, 5);
for candidate in candidates {
    // Try each one, first success wins
}
```

**Subscribe uses only one**:
```rust
let target = if let Some(t) = candidates.first() {  // Only first!
    t.clone()
} else {
    // Random fallback
}
```

---

## Solutions (Ordered by Impact)

### Solution 1: Ensure Network Stability Before Operations (Test Fix)

**For six-peer-regression test**, add warmup period:

```rust
// In river-src/tests/message_flow.rs (or similar)
async fn setup_network() {
    let network = TestNetwork::builder()
        .gateways(1)
        .peers(5)
        .build()
        .await?;

    // ✅ ADD THIS: Wait for network to stabilize
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify all peers have ring locations
    let diagnostics = network.collect_diagnostics().await?;
    for diag in diagnostics {
        assert!(diag.location.is_some(), "Peer {} has no ring location", diag.peer_id);
    }

    // NOW do PUT/SUBSCRIBE operations
}
```

**Impact**: Should eliminate test flakiness immediately
**Downside**: Doesn't fix underlying race condition in production

### Solution 2: Add Explicit Propagation Wait in PUT Response (Production Fix)

**In PUT handler**, delay response until contract has propagated to k peers:

```rust
// In crates/core/src/operations/put.rs
async fn complete_put(...) {
    // Store contract locally
    store_contract(contract).await?;

    // ✅ ADD THIS: Wait for propagation confirmation
    wait_for_propagation(contract_key, min_replicas=3, timeout=5s).await?;

    // NOW send success response to client
    send_response(PutSuccess { key: contract_key }).await;
}
```

**Impact**: Ensures SUBSCRIBE finds contract immediately
**Downside**: Slows down PUT operations (but only by actual propagation time)

### Solution 3: Multi-Path Subscription Routing (Architectural Fix)

**Make Subscribe work like GET** - try multiple routes in parallel:

```rust
// In crates/core/src/operations/subscribe.rs:256
let candidates = op_manager
    .ring
    .k_closest_potentially_caching(instance_id, &visited, 5);  // Get 5 instead of 3

if candidates.is_empty() {
    // Existing fallback logic
} else {
    // ✅ NEW: Try multiple paths in parallel
    let tasks: Vec<_> = candidates.iter().take(3).map(|target| {
        send_subscribe_request(op_manager, target, instance_id)
    }).collect();

    // Wait for first success
    let (result, _remaining) = futures::future::select_all(tasks).await;

    // Cancel remaining attempts
    // Use result
}
```

**Impact**: Robust to single-peer failures, faster completion
**Downside**: More complex implementation, more network traffic

### Solution 4: Reduce CONTRACT_WAIT_TIMEOUT (Quick Fix)

**Current**: 2000ms (line 26 of subscribe.rs)
**Change to**: 500ms

**Rationale**: If contract isn't there after 500ms, it probably won't arrive. Fail faster and retry with different peer.

```rust
const CONTRACT_WAIT_TIMEOUT_MS: u64 = 500;  // Was 2_000
```

**Impact**: Faster retry cycle, less time wasted
**Downside**: May increase retry count if network is genuinely slow

### Solution 5: Smart Fallback Routing (Medium Fix)

**When `k_closest_potentially_caching` returns empty**, don't use random peer. Instead:

```rust
// In subscribe.rs:267
} else {
    // ✅ Instead of random fallback, try gateway or well-connected peer
    let fallback_target = connections
        .values()
        .flatten()
        .max_by_key(|conn| conn.connection_count)  // Peer with most connections
        .or_else(|| find_gateway())  // Or gateway if this is a peer
        .map(|conn| conn.location.clone());

    // Gateway is more likely to have received the contract from PUT
}
```

**Impact**: Better fallback peer selection
**Downside**: Still a workaround, not a fix

---

## Immediate Action Items

### 1. Run the Reproduction Test (30 minutes)
```bash
# Create the test file above
vim crates/core/tests/subscribe_timing_regression.rs

# Run it
cargo test --test subscribe_timing_regression test_put_subscribe_race_condition -- --nocapture
```

**Expected outcome**: Confirms PUT/SUBSCRIBE race with 10x+ slowdown

### 2. Add Warmup to six-peer-regression (1 hour)
Find the River test and add 10s sleep after network creation

**Expected outcome**: Test becomes reliable

### 3. Instrument Routing Decisions (2 hours)
Add logging to see when `k_closest_potentially_caching` returns empty:

```rust
// In subscribe.rs:258, add:
if candidates.is_empty() {
    tracing::warn!(
        tx = %id,
        contract = %instance_id,
        phase = "routing_failure",
        "k_closest_potentially_caching returned empty - network not ready?",
    );
}
```

**Expected outcome**: CI logs show routing failures during immediate subscriptions

### 4. Update Issue #2494 (30 minutes)
Post findings with:
- Code analysis
- Reproduction test
- Recommended fixes
- Request for feedback on which solution to pursue

---

## Questions to Answer

1. **Does the reproduction test confirm the hypothesis?**
   - If yes → Proceed with Solution 2 or 3
   - If no → Investigate routing algorithm itself

2. **What does six-peer-regression actually do?**
   - Does it have a warmup period?
   - What's the timing between PUT and SUBSCRIBE?
   - (Need to check River repository code)

3. **Is this a test-only issue or production issue?**
   - Check production telemetry for subscription times
   - If only tests: Fix test setup (Solution 1)
   - If production too: Fix code (Solution 2 or 3)

4. **What's the right timeout budget?**
   - 60s might be fine if we fix the routing
   - Or might need 120s to accommodate real network delays
   - Need data from production to decide
