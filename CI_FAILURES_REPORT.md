# CI Failures Analysis - Last 2 Weeks (Dec 19, 2025 - Jan 2, 2026)

## Executive Summary

This report documents all CI failures and stability issues encountered in the last 2 weeks, including both infrastructure problems and test flakiness issues. Multiple PRs were required to address these issues, with several still under investigation.

---

## 1. Toolchain/Compiler Issues

### 1.1 Rust 1.92.0 Internal Compiler Error (ICE)
- **Issue**: #2479 (implied)
- **PR**: #2479 - "ci: pin Rust to 1.91.0 to avoid rustc 1.92.0 ICE"
- **Date**: Dec 30, 2025
- **Status**: ✅ Fixed
- **Severity**: Critical (blocks all CI)

**Problem**: rustc 1.92.0 (released Dec 2025) has a transient ICE in monomorphization/LLVM:
```
error: rustc interrupted by SIGSEGV, printing backtrace
/home/runner/.rustup/toolchains/stable-x86_64-unknown-linux-gnu/lib/libLLVM.so.21.1-rust-1.92.0-stable
```

**Root Cause**: Upstream rustc bug in LLVM monomorphization
**Reference**: https://github.com/rust-lang/rust/issues/147164

**Solution**: Pin CI to Rust 1.91.0 in all workflow jobs
- Modified `.github/workflows/ci.yml` to use `toolchain: 1.91.0`
- Added comments explaining the pin

**Related to #2494?**: No, but caused CI failures that delayed investigation of #2494

---

### 1.2 Rust 1.91.0 Stack Exhaustion (SIGSEGV)
- **Commit**: d578683 - "build: pin Rust toolchain to 1.90.0 to avoid rustc SIGSEGV"
- **Date**: Dec 31, 2025
- **Status**: ✅ Fixed (then superseded by 1.91.0 + stack increase)
- **Severity**: Critical (blocks CI on self-hosted runner)

**Problem**: Even with 1.91.0, the six-peer-regression job failed with SIGSEGV due to stack exhaustion:
```
help: you can increase rustc's stack size by setting RUST_MIN_STACK=16777216
```

**Root Cause**: The river workspace builds many crates fresh (no cache), exhausting default rustc stack during parallel compilation

**Solution**: Set `RUST_MIN_STACK=16777216` (16MB) in all self-hosted jobs
- Applied to: `test_all`, `six_peer_regression`, `claude-ci-analysis`
- Eventually reverted the 1.90.0 pin, settled on 1.91.0 + increased stack

**Related to #2494?**: No, infrastructure issue

---

### 1.3 LLVM Linker Crashes (rust-lld)
- **Issue**: #2519 - "ci: LLVM linker crash (rust-lld segfault)"
- **PR**: #2520 - "ci: use mold linker on self-hosted runners to avoid rust-lld crashes"
- **Date**: Dec 31, 2025
- **Status**: ✅ Fixed
- **Severity**: High (transient, but wastes CI time)

**Problem**: Intermittent rust-lld crashes during test compilation:
```
rust-lld: error: relocation R_X86_64_PLT32 out of range
collect2: fatal error: ld terminated with signal 11 [Segmentation fault]
```

**Root Cause**: Known rust-lld bug with large link jobs
**Occurrence**: Transient - re-running typically succeeds

**Solution**: Switch self-hosted runners to mold linker
- Set `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=clang`
- Set `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS=-C link-arg=-fuse-ld=mold`
- Benefits: Avoids LLVM bug, 3-10x faster linking, matches local dev setup

**Related to #2494?**: No, infrastructure issue

---

## 2. Platform-Specific Build Issues

### 2.1 macOS liblzma Dynamic Linking Failure
- **PR**: #2544 - "fix(build): statically link liblzma for macOS to fix install failures"
- **Date**: Jan 1, 2026
- **Status**: ✅ Fixed
- **Severity**: Critical (users cannot install on fresh macOS)

**Problem**: Binaries crashed on fresh macOS systems with "Abort trap: 6"
```
sh: line 221: 12366 Abort trap: 6           "$install_dir/freenet" --version > /dev/null 2>&1
error: Binary verification failed. macOS may be blocking the unsigned binary.
```

**Root Cause**: Binary dynamically linked against Homebrew's liblzma:
```
/opt/homebrew/opt/xz/lib/liblzma.5.dylib
```
On systems without Homebrew/xz, the dylib is missing → crash

**Solution**: Set `LZMA_API_STATIC=1` during macOS cross-compile build
- Forces `lzma-sys` to compile liblzma from source and link statically
- Binary now portable to any macOS system

**Related to #2494?**: No, distribution/packaging issue

---

## 3. Test Infrastructure & Flakiness

### 3.1 Docker NAT Simulation - Alpine Image Missing
- **Commit**: 5259286 - "fix(ci): pull alpine image before NAT simulation test"
- **Date**: Dec 27, 2025
- **Status**: ✅ Fixed
- **Severity**: Critical (six-peer-regression blocked)

**Problem**: six-peer-regression test failed with:
```
No such image: alpine:latest
```

**Root Cause**: The test uses Docker NAT simulation which requires `alpine:latest` for NAT router containers. Docker wasn't pre-pulling the image.

**Solution**: Added docker pull step in `.github/workflows/ci.yml`:
```yaml
- name: Pull Docker images for NAT simulation
  run: docker pull alpine:latest
```

**Related to #2494?**: Yes - this test is specifically mentioned in #2494 as exposing subscription timeout issues in Docker NAT environments

---

### 3.2 Test Log Verbosity Pollution
- **PR**: #2448 - "fix(ci): reduce test log verbosity by using RUST_LOG and removing --nocapture"
- **Date**: Dec 28, 2025
- **Status**: ✅ Fixed
- **Severity**: Medium (makes debugging harder)

**Problem**: CI logs were overly verbose, making it hard to find actual failures

**Root Causes**:
1. CI set `FREENET_LOG=error` but code reads `RUST_LOG` (wrong variable)
2. `--nocapture` flag forced ALL output to display, bypassing cargo's "only show failures" behavior
3. Tests mixed `println!` (always visible) and `tracing::info!` (controllable)

**Solution**:
- Replace `FREENET_LOG` with `RUST_LOG=error` in CI
- Remove `--nocapture` flag
- Convert `println!` → `tracing::info!` in 13 test files
- Convert tests to use `#[test_log::test(tokio::test)]`

**Related to #2494?**: Indirectly - harder to debug subscription timeout issues with noisy logs

---

### 3.3 Telemetry Flooding from Tests
- **PR**: #2471 - "fix(telemetry): disable telemetry in test environments"
- **Date**: Dec 30, 2025
- **Status**: ✅ Fixed
- **Severity**: Medium (pollutes production telemetry)

**Problem**: Telemetry collector flooded with test data (127.x.x.x, 172.29.x.x addresses) from CI and simulated networks, making production analysis difficult

**Root Cause**: No mechanism to disable telemetry in test environments

**Solution**:
- Add `is_test_environment: bool` field to `TelemetryConfig`
- Set to `true` when `--id` flag is passed (existing test indicator)
- `TelemetryReporter::new()` returns `None` if test environment
- Production peers unaffected (don't use `--id`)

**Related to #2494?**: No, but affected ability to analyze subscription behavior in production

---

### 3.4 Ubertest Removal
- **Issue**: #1932 - "Debug issues with uber test"
- **PR**: #2555 - "ci: remove ubertest from CI"
- **Date**: Jan 2, 2026
- **Status**: ✅ Fixed (removed)
- **Severity**: High (persistent flakiness)

**Problem**: Ubertest was a persistent source of CI instability
- PUT timeouts, UPDATE routing failures, waker registration bugs
- Had been disabled in CI (`if: false`) due to unreliability
- Issue #1932 tracked months of debugging

**Why Removal Over Fixing**:
- six-peer-regression provides **superior** integration testing via Docker NAT
- Ubertest runs on localhost only, missing critical P2P network conditions
- Regular test suite already covers PUT/GET/SUBSCRIBE/UPDATE thoroughly
- Maintenance burden didn't justify redundant coverage

**Solution**: Deleted:
- `crates/core/tests/ubertest.rs` (775 lines)
- `crates/core/tests/UBERTEST.md` (167 lines)
- `ubertest` job from `.github/workflows/ci.yml` (43 lines)

**Related to #2494?**: No, but removed a source of timeout-related CI noise

---

## 4. Subscription & Network Protocol Issues

### 4.1 Subscription Timeouts in Docker NAT (Root Issue)
- **Issue**: #2494 - "Investigate root cause of subscription timeouts in Docker NAT environments"
- **Date**: Opened Dec 31, 2025
- **Status**: 🔍 Under Investigation
- **Severity**: High (affects test reliability and API semantics)

**Problem**: Subscriptions timing out after 60s in a 6-peer Docker NAT network - should be plenty of time

**Background**: PR #2489 introduced a workaround (async subscriptions) but @iduartgomez noted:
> "This change defeats the purpose of implicit subscriptions... subs being flaky in the test which for 60secs in a network of 6 shouldn't be."

**API Semantic Change**:
- **Before**: `subscribe=true` blocks PUT/GET response until subscription completes (atomic)
- **After PR #2489**: Response sent immediately, subscription runs in background (best-effort)
- Clients can no longer rely on atomic subscription confirmation

**Potential Root Causes** (from issue):
1. Single-path routing for subscriptions (no failover like GET)
2. Subscription backoff (5s→10s→20s→40s→80s) hits 60s TTL mid-retry
3. Network retransmission delays (RTO: 1s→2s→4s→8s→16s→32s→60s)
4. 2-second contract wait timeout compounds delays
5. Docker NAT specific packet loss / latency

**Related PRs Attempting Fixes**:
- PR #2489 (async subscription workaround)
- PR #2495 (re-enable test to verify stability)
- PR #2515, #2523, #2526 (subscription recovery mechanisms)
- PR #2541 (circular reference prevention)
- PR #2545, #2465, #2441 (timeout increases - treating symptoms)

**Status**: Investigation ongoing, multiple contributing factors identified

---

### 4.2 Subscription Blocking Workaround
- **PR**: #2489 - "fix: avoid blocking PUT/GET responses on subscription completion"
- **Date**: Dec 30, 2025
- **Status**: ✅ Merged (but see #2494 for root cause investigation)
- **Severity**: High (changes API semantics)

**Problem**: six-peer-regression timing out with "Timeout waiting for PUT response after 60 seconds"
- Contract was successfully stored
- Subscriptions appeared on peers
- But client never received PUT response

**Root Cause**: Parent-child blocking mechanism (from PR #2009)
- PUT/GET with `subscribe=true` spawns child subscription
- Parent response withheld until child completes
- Under Docker NAT delays, subscription takes >60s

**Solution**: Introduce `start_subscription_request_async()`
- Spawns subscription WITHOUT registering as blocking child
- PUT/GET response sent immediately
- Subscription completes in background

**Controversy**: @iduartgomez noted this "defeats the purpose of implicit subscriptions" - led to creation of issue #2494

**Related to #2494?**: **YES - This is the workaround that prompted #2494 investigation**

---

### 4.3 Circular Subscription Tree References
- **Issue**: #2535 - "Circular subscription references in subscription tree formation"
- **PR**: #2541 - "fix(subscription): prevent circular and self-references in subscription tree"
- **Date**: Dec 31, 2025
- **Status**: ✅ Fixed
- **Severity**: High (breaks update propagation to River UI)

**Problem**: Subscription tree developing invalid states:
1. **Self-references**: Peers had themselves in downstream subscriber list
2. **Circular references**: Gateway in both upstream AND downstream lists
3. **Disconnected trees**: Seeders not connected to gateway

Example from telemetry:
```
nkupk43JUUYTNogZ:
  upstream: CurJAAXnMTgciL6A@5.9.111.215:31337 (gateway)
  downstream: ['nkupk43JUUYTNogZ@...' (SELF!), 'CurJAAXnMTgciL6A@...' (UPSTREAM!)]
```

**Effects**: Updates echoed back to senders, lost in disconnected branches

**Root Cause**: No validation in `SeedingManager::add_downstream()` / `set_upstream()`

**Solution**: Add validation to reject:
- Self-references (subscriber address == own_addr)
- Circular references (peer already on opposite side of tree)

**Added Tests**:
- `test_add_downstream_rejects_self_reference`
- `test_set_upstream_rejects_self_reference`
- `test_add_downstream_rejects_circular_reference`
- `test_set_upstream_rejects_circular_reference`

**Related to #2494?**: Possibly - circular references could cause subscription delays or failures

---

### 4.4 Missing Unsubscribed Notifications in Tests
- **PR**: #2558 - "fix(test): send Unsubscribed notifications in testing_impl ClientDisconnected handler"
- **Date**: Dec 31, 2025
- **Status**: ✅ Fixed
- **Severity**: High (subscription tree pruning broken in tests)

**Problem**: CI failing on subscription tree pruning tests:
- `test_multiple_clients_prevent_premature_pruning`
- `test_subscription_pruning_sends_unsubscribed`
- `test_update_no_change_notification`

Tests timed out waiting for peers to be removed from subscriber list after disconnect

**Root Cause**: `testing_impl.rs` discarded return value from `remove_client_from_all_subscriptions()`:
```rust
// BROKEN
NodeEvent::ClientDisconnected { client_id } => {
    op_manager.ring.remove_client_from_all_subscriptions(client_id);
    continue;  // ❌ notifications never sent
}
```

Production code in `p2p_protoc.rs` correctly sends notifications:
```rust
let notifications = op_manager.ring.remove_client_from_all_subscriptions(client_id);
ctx.bridge.send_prune_notifications(notifications).await;
```

**Solution**: Update `testing_impl.rs` to match production behavior
- Capture notifications list
- Send `Unsubscribed` message to each upstream peer

**Why CI Didn't Catch Earlier**: Regression introduced in PR #2290 (Dec 20), but timing-dependent

**Related to #2494?**: Yes - subscription tree management issues could contribute to timeout problems

---

### 4.5 Test Re-enablement for Stability Verification
- **PR**: #2495 - "test: re-enable test_multiple_clients_subscription to verify CI stability"
- **Date**: Dec 31, 2025
- **Status**: ✅ Merged
- **Severity**: Medium (monitoring)

**Purpose**: Re-enable previously disabled test to verify CI stability after recent fixes

**Related to #2494?**: Yes - verifying subscription stability

---

### 4.6 Multiple Timeout Increases (Symptom Treatment)

#### PR #2545 - test_subscription_tree_pruning timeout
- **Date**: Jan 1, 2026
- **Problem**: Test passed on PR branch, failed on main (same code, different CI timing)
- **Error**: `deadline has elapsed` after ~49s
- **Solution**: Increase timeout 60s → 90s
- **Related**: PR #2441, #2523 (similar timeout fixes)

#### PR #2465 - test_small_network_get_failure timeout
- **Date**: Dec 29, 2025 (before 2-week window, but relevant)
- **Problem**: Second GET using 10s timeout (too aggressive)
- **Solution**: Increase second GET timeout 10s → 45s
- **Note**: Test assumed cached GET would be faster, but this doesn't hold in CI

#### PR #2441 - subscription pruning timeout (before window)
- Similar pattern: increase timeout to accommodate CI timing variability

**Related to #2494?**: **YES - These are all treating symptoms rather than root cause. Mentioned in #2494 analysis.**

---

### 4.7 Subscription Recovery Mechanisms

#### PR #2515 - Periodic subscription recovery
- **Title**: "fix: add periodic subscription recovery for orphaned seeders"
- **Date**: Dec 31, 2025
- **Purpose**: Detect and recover orphaned seeders

#### PR #2523 - Retry with backoff for peer lookup
- **Title**: "fix: add retry with backoff for peer lookup during subscription"
- **Date**: Dec 31, 2025
- **Purpose**: Handle transient peer lookup failures

#### PR #2526 - Random startup delay
- **Title**: "fix: add random startup delay to subscription recovery task"
- **Date**: Dec 31, 2025
- **Purpose**: Prevent thundering herd on recovery

**Related to #2494?**: Yes - defensive mechanisms to work around subscription unreliability

---

### 4.8 Flaky Decryption Errors
- **Issue**: #2528 - "fix: test_small_network_get_failure is flaky with decryption errors"
- **Date**: Dec 31, 2025
- **Status**: 🔍 Closed (but may indicate real bug)
- **Severity**: Medium (intermittent CI failure)

**Problem**: Test shows "decryption error" from transport layer:
```
[ERROR] Failed to establish gateway connection error=decryption error peer_addr=127.0.0.1:43711
```

**Hypotheses**:
1. Transport key handshake race
2. Key rotation during test
3. Port reuse with stale encryption state
4. Test timing sensitivity

**Impact**: Flaky in CI, may indicate real transport bug

**Related to #2494?**: Possibly - transport issues could contribute to timeout problems

---

## 5. Transport Layer Changes (Timing Impact)

### 5.1 LEDBAT++ Implementation
- **PR**: #2472 - "feat(transport): implement LEDBAT++ (draft-irtf-iccrg-ledbat-plus-plus)"
- **Date**: Dec 29, 2025
- **Status**: ✅ Merged
- **Severity**: Medium (affects network timing)

**Changes**: Upgrade from RFC 6817 LEDBAT to LEDBAT++:
1. Dynamic GAIN calculation
2. Multiplicative decrease cap
3. Periodic slowdown mechanism
4. TARGET delay: 100ms → 60ms
5. Slow start exit: 50% → 75% of target

**Impact on Tests**: PR #2489 notes "LEDBAT++ transport changes (#2472) affecting timing" as contributing to subscription timeout exposure

**Related to #2494?**: **YES - Explicitly mentioned in PR #2489 as contributing to timeout issues**

---

### 5.2 X25519 Key Exchange Replacement
- **PR**: #2533 - "perf(transport): replace RSA-2048 with X25519 for key exchange"
- **Date**: Dec 31, 2025
- **Status**: ✅ Merged
- **Severity**: Low (performance improvement)

**Change**: Replace RSA-2048 with X25519 for transport key exchange
**Impact**: Faster key exchange, less CPU usage (RSA was 36% of test CPU time per #2530)

**Note**: PR #2545 mentions test passed on X25519 PR branch but failed on main (timing sensitivity)

**Related to #2494?**: Indirectly - faster key exchange might help, but doesn't address root timeout issues

---

### 5.3 Transport Timeout and Recovery Fixes

#### PR #2549 - Slow start re-entry
- **Title**: "fix(transport): re-enter slow start after timeout for fast recovery"
- **Date**: Jan 1, 2026

#### PR #2532 - LEDBAT++ min_interval tuning
- **Title**: "fix(transport): increase LEDBAT++ min_interval from 1 RTT to 18 RTTs"
- **Date**: Dec 31, 2025

#### PR #2522 - Gateway handshake race
- **Title**: "fix(transport): eliminate race condition in gateway handshake completion"
- **Date**: Dec 31, 2025

**Related to #2494?**: Possibly - transport timing issues could contribute to subscription delays

---

## 6. Test Framework & Simulation

### 6.1 Simulation Framework Development
- **Issue**: #2497 - "Simulation Testing Framework - Determinism Layer"
- **PR**: #2536 - "feat(simulation): add deterministic simulation testing framework"
- **Date**: Dec 31, 2025
- **Status**: ✅ Merged
- **Severity**: N/A (new feature)

**Purpose**: Add deterministic simulation testing with VirtualTime, fault injection, etc.

**Follow-ups**:
- PR #2553 - "feat(simulation): add comprehensive simulation testing framework"
- PR #2557 - "fix(simulation): improve framework robustness and documentation"

**Related to #2494?**: Not directly, but provides tools for investigating timeout issues in controlled environments

---

### 6.2 Test Parametrization Refactoring
- **PR**: #2478 - "test: refactor tests with parametrization in core crate"
- **Date**: Dec 28, 2025
- **Status**: ✅ Merged
- **Severity**: Low (cleanup)

**Purpose**: DRY improvement using rstest for parametrized tests

**Related to #2494?**: No

---

## 7. Issues Related to #2494 - Summary

### Directly Related
1. **PR #2489** - Async subscription workaround (prompted #2494)
2. **PR #2495** - Re-enable subscription test for stability verification
3. **PR #2545** - Subscription timeout increase (symptom)
4. **PR #2465** - GET timeout increase (symptom)
5. **PR #2441** - Subscription pruning timeout (symptom - before window)
6. **PR #2515, #2523, #2526** - Subscription recovery mechanisms (defensive)
7. **PR #2541** - Circular subscription references (could cause delays)
8. **PR #2558** - Missing unsubscribed notifications (test infrastructure)
9. **Commit 5259286** - Alpine image pull (six-peer-regression blocked)
10. **PR #2472** - LEDBAT++ (explicitly mentioned as affecting timing)

### Possibly Related
1. **Issue #2528** - Flaky decryption errors (transport issues)
2. **PR #2549, #2532, #2522** - Transport timeout/recovery fixes
3. **PR #2448** - Log verbosity (made debugging harder)
4. **PR #2555** - Ubertest removal (timeout noise source)

### Infrastructure (Not Related)
1. **PR #2479** - Rust 1.92.0 ICE (blocked CI)
2. **Commit d578683** - Rust stack exhaustion (blocked CI)
3. **PR #2520** - mold linker (infrastructure)
4. **PR #2544** - macOS liblzma (packaging)
5. **PR #2471** - Telemetry in tests (data quality)

---

## Recommendations

### For #2494 Investigation
1. **Add detailed subscription tracing** at each stage (as suggested in issue)
2. **Profile Docker NAT overhead** vs localhost
3. **Test with simulation framework** (#2536) for deterministic reproduction
4. **Analyze cumulative timeouts** - issue notes multiple 2s delays compound
5. **Consider increasing OPERATION_TTL** if root cause is truly cumulative delays
6. **Document API semantics** - if async subscriptions are intended, update docs

### For CI Stability
1. **Monitor rustc 1.93.0** release to unpin from 1.91.0
2. **Track linker crash frequency** with mold to ensure it's fixed
3. **Review timeout values** holistically - many ad-hoc increases suggest systemic issue
4. **Use simulation framework** for deterministic network timing tests

### For Test Hygiene
1. **Avoid treating symptoms** - timeout increases should come with investigation
2. **Use telemetry** to understand actual timing distributions before setting timeouts
3. **Document why timeouts are set** to specific values (not just "increase until it passes")

---

## Timeline of CI Issues (Chronological)

| Date | Type | Issue/PR | Description |
|------|------|----------|-------------|
| Dec 27 | Infrastructure | 5259286 | Alpine image missing for Docker NAT |
| Dec 28 | Test Quality | #2448 | Log verbosity fixes |
| Dec 29 | Flaky Test | #2465 | Small network GET timeout increase |
| Dec 30 | Compiler | #2479 | Pin to Rust 1.91.0 (ICE fix) |
| Dec 30 | Subscription | #2489 | Async subscription workaround |
| Dec 30 | Telemetry | #2471 | Disable telemetry in tests |
| Dec 31 | Compiler | d578683 | Stack exhaustion fix (RUST_MIN_STACK) |
| Dec 31 | Infrastructure | #2520 | mold linker to avoid crashes |
| Dec 31 | Subscription | #2494 | Issue filed for root cause investigation |
| Dec 31 | Subscription | #2495 | Re-enable subscription test |
| Dec 31 | Subscription | #2541 | Circular reference prevention |
| Dec 31 | Test Infra | #2558 | Unsubscribed notifications in tests |
| Jan 1 | Subscription | #2545 | Subscription tree pruning timeout |
| Jan 1 | Build | #2544 | macOS static liblzma linking |
| Jan 2 | Test Infra | #2555 | Remove ubertest |

---

## Conclusion

The last 2 weeks saw significant CI instability from **three independent categories**:

1. **Infrastructure** (toolchain, linker, platform builds) - mostly resolved
2. **Test framework** (logging, telemetry, test infrastructure) - resolved
3. **Subscription timing** (the #2494 investigation) - ongoing

**Key Finding**: Multiple PRs (#2545, #2465, #2441, #2515, #2523, #2526) are treating symptoms of subscription timeouts rather than addressing the root cause. Issue #2494 correctly identifies this as a systemic problem requiring investigation.

**Most Critical Issue**: #2494 subscription timeouts - affects both test reliability and API semantics. The workaround (PR #2489) changes the API from atomic to best-effort subscriptions.
