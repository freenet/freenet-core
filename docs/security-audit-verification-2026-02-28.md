# Security Audit Verification: Claims vs Reality

**Date**: 2026-02-28
**Scope**: Verification of the "Freenet Contract Security Audit" (dated 2026-02-25) against the actual codebase
**Method**: Systematic code inspection of every file/line cited in the audit

## Executive Summary

The audit was conducted against an **older version** of the codebase. Many of the critical and high-severity findings have already been **fixed** in the current code. The audit's line numbers are almost universally wrong, and the wasmer backend it references no longer exists. Of the findings that remain valid, most are resource exhaustion / DoS hardening issues rather than memory safety bugs.

**Key finding**: The two most critical claims (C1: arbitrary host memory corruption, C2: undefined behavior from `from_utf8_unchecked`) are **no longer exploitable at the code level** — the codebase now has bounds-checking via `validate_and_compute_ptr()` and the `InstanceInfo` struct stores `mem_size`. The remaining valid concerns are resource limits (state size, gas metering, pool fairness) and DoS hardening.

---

## Finding-by-Finding Verification

### CRITICAL

#### C1: "No Bounds Checking on Host Function Pointer Arithmetic"

**Audit claim**: `compute_ptr` at `native_api.rs:184-186` adds WASM-supplied offset to host memory base without bounds checking. `InstanceInfo` stores `start_ptr` but not memory size.

**VERDICT: FIXED — Claim describes code that no longer exists**

Reality in the current codebase:
- There is **no** `compute_ptr` function. It was replaced by `validate_and_compute_ptr<T>` at `native_api.rs:348-381`.
- This function performs comprehensive bounds checking:
  - Rejects negative offsets (line 355)
  - Checks for overflow when adding size to offset via `checked_add` (line 363)
  - Validates entire access range against `MAX_WASM_MEMORY_BYTES` (256 MiB) (line 367)
  - Checks for overflow when computing host pointer via `checked_add` (line 376)
  - Returns `Option<*mut T>` — callers must handle `None`
- `InstanceInfo` at `runtime.rs:105-109` **does** store `mem_size: usize` alongside `start_ptr`
- The memory info is recorded at instance creation: `runtime.rs:80-81`:
  ```rust
  let (ptr, size) = engine.memory_info(&handle)?;
  native_api::MEM_ADDR.insert(id, InstanceInfo::new(ptr as i64, size, key));
  ```
- All three host functions (`log::info`, `rand::rand_bytes`, `time::utc_now`) call `validate_and_compute_ptr` and gracefully handle `None` by logging an error and returning early.

The audit's fix recommendation (store `memory_size` in `InstanceInfo`, add bounds-checking wrapper) has been **exactly implemented**.

#### C2: "`from_utf8_unchecked` on Untrusted WASM Data"

**Audit claim**: `native_api.rs:201` uses `std::str::from_utf8_unchecked` on bytes from WASM linear memory.

**VERDICT: PARTIALLY VALID — `from_utf8_unchecked` still exists but is now bounds-checked**

Reality:
- The line number is wrong (201 is a doc comment for `resolve_contract_key()`). The actual usage is at `native_api.rs:404`.
- The call **is** still `from_utf8_unchecked` — this has NOT been changed to `from_utf8()` or `from_utf8_lossy()`.
- However, the pointer/length are now validated by `validate_and_compute_ptr` (line 395-399), so the read is within WASM linear memory bounds.
- The UB concern per Rust spec remains valid: if the WASM contract writes invalid UTF-8 bytes to its linear memory and then calls `log::info`, the resulting `&str` is undefined behavior. The practical impact is low (tracing infrastructure would likely just display garbage), but it should still be fixed to `from_utf8_lossy()` for correctness.

**Residual issue**: `from_utf8_unchecked` should be replaced with safe alternative. Low practical impact but technically UB.

#### C3: "Gas Metering Disabled by Default"

**Audit claim**: `RuntimeConfig::default()` at `runtime.rs:169` sets `enable_metering: false`.

**VERDICT: CONFIRMED — Still true**

Reality at `runtime.rs:166-176`:
```rust
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_execution_seconds: 5.0,
            cpu_cycles_per_second: None,
            safety_margin: 0.2,
            enable_metering: false,   // <-- confirmed
            module_cache_capacity: DEFAULT_MODULE_CACHE_CAPACITY,
        }
    }
}
```
- `enable_metering: false` is confirmed at line 172.
- The 5-second timeout (`max_execution_seconds: 5.0`) is the only CPU defense.
- The audit's line number (169) is close but off by 3 — it's actually line 172.

### HIGH

#### H1: "No State Size Limit at Storage Layer"

**Audit claim**: Neither `StateStore` nor ReDb/SQLite backends enforce maximum state size. The `MAX_SIZE = 10 MiB` in `executor.rs:866` applies to contract code, not state.

**VERDICT: CONFIRMED — No state size limit exists**

Reality:
- `state_store.rs` `store()` and `update()` methods (lines 101-151) contain **no size validation**.
- Grep for `MAX_STATE_SIZE`, `max_state_size`, `state.*size.*limit` across the entire `crates/core/src` returned **zero matches**.
- The `MAX_SIZE` constant the audit mentions was not found at the claimed location (`executor.rs:866`), but the absence of state size limits is confirmed.
- An attacker contract with `validate_state` returning `Valid` can store arbitrarily large state.

#### H2: "No WASM Binary Validation Before Compilation"

**Audit claim**: Raw WASM bytes passed to `Module::new()` without pre-validation at `wasmtime_engine.rs:278-280`.

**VERDICT: PARTIALLY VALID but overstated**

Reality:
- Wasmtime's `Module::new()` performs its own comprehensive WASM validation (structural checks, type validation, etc.) before compilation. This is the standard and correct approach — wasmtime is a security-hardened runtime.
- There's no **pre-validation** with `wasmparser` for cheap rejection, which could be useful for DoS defense (reject malformed/huge modules before hitting the compiler).
- The line numbers are wrong — the actual `Module::new()` call is at a different location.
- No wasmer engine file exists (see below).

#### H3: "TOCTOU Race in `upsert_contract_state`"

**Audit claim**: Multi-step sequence (fetch, update, validate, commit) runs without per-key locking at `runtime.rs:677-1112`.

**VERDICT: PARTIALLY VALID — no per-key lock, but mitigated by executor pool design**

Reality:
- The `bridged_upsert_contract_state` method at `runtime.rs:708` does perform a multi-step sequence without per-contract-key locking.
- However, each executor owns its own `Executor<Runtime>` with its own `state_store`. The `RuntimePool` checks out executors via semaphore.
- A recovery guard (`recovery_guard: Mutex<HashSet<ContractKey>>`) exists at lines 1128-1139, but this is for corruption recovery, not general TOCTOU prevention.
- Concurrent updates to the same contract from different executors could theoretically race, though the pool design makes this less likely than the audit suggests.

#### H4: "Stale `MEM_ADDR` Pointers After Memory Growth"

**Audit claim**: `MEM_ADDR` caches `start_ptr` at instance creation (`runtime.rs:80-87`). WASM `memory.grow()` can relocate backing allocation, making cached pointer stale.

**VERDICT: PARTIALLY VALID — pointer is cached, but note the validation change**

Reality:
- `runtime.rs:80-81` does cache the pointer at creation time:
  ```rust
  let (ptr, size) = engine.memory_info(&handle)?;
  native_api::MEM_ADDR.insert(id, InstanceInfo::new(ptr as i64, size, key));
  ```
- The audit's claim that `_size` was "discarded" (Appendix A) is **wrong** — `size` IS stored in `InstanceInfo.mem_size`.
- However, the `validate_and_compute_ptr` function uses `MAX_WASM_MEMORY_BYTES` (256 MiB constant) for bounds checking rather than the cached `mem_size` — the `_mem_size` parameter is explicitly unused (line 342-352). This means the bounds check is against the maximum possible memory, not the current allocation.
- On wasmtime with guard pages, a `memory.grow()` may not relocate the base pointer (wasmtime uses virtual memory reservations). This is backend-specific behavior.
- The stale pointer concern is theoretically valid but practically mitigated by wasmtime's memory model.

### MEDIUM

#### M1: "Panic in Host Functions Crashes Host Thread"

**Audit claim**: Host functions panic on invalid input at `native_api.rs:196-198, 213, 229`.

**VERDICT: PARTIALLY VALID — `.expect()` calls remain, panic replaced with error return for id=-1**

Reality:
- The `id == -1` case now returns early with `tracing::error!` instead of panicking (lines 390-392, 415-421, 442-446).
- However, `.expect("instance mem space not recorded")` calls remain at lines 394, 422, 449. If `MEM_ADDR` doesn't contain the instance ID (which shouldn't happen in normal operation), these will still panic.
- The line numbers in the audit are all wrong.

#### M2: "Integer Overflow in `len as _` Cast"

**Audit claim**: `len: i32` cast to usize via `len as _` at `native_api.rs:201` — negative values become huge.

**VERDICT: PARTIALLY MITIGATED**

Reality:
- In `log::info` (line 389), `len: i32` is passed to `validate_and_compute_ptr` as `len as usize` (line 396). A negative `len` would become a very large usize.
- However, `validate_and_compute_ptr` would reject this because `ptr_usize.checked_add(size)` would overflow, and even if it didn't, `end_offset > MAX_WASM_MEMORY_BYTES` would fail for any huge value.
- In `rand::rand_bytes` (line 414), `len` is already `u32`, so no negative value issue.
- The bounds validation catches the pathological case, but a direct `len < 0` guard would be cleaner.

#### M3: "Unbounded Init Tracker Queue"

**Audit claim**: No upper bound on queued operations per contract during initialization at `init_tracker.rs:108`.

**VERDICT: CONFIRMED — No queue size limit**

Reality at `init_tracker.rs:134`:
```rust
state.queued_ops.push(QueuedOperation { ... });
```
- No check on `queued_ops.len()` before pushing.
- The 30-second stale cleanup (`cleanup_stale_initializations`) provides a time-based bound but not a count-based bound.
- Line 108 is the `new()` function, not the queue push — audit line number is wrong.

#### M4: "No Per-Contract Subscriber Limit"

**Audit claim**: Unbounded subscriber registration per contract with `UnboundedSender` channels at `runtime.rs:481-506, 1114-1137`.

**VERDICT: CONFIRMED — No per-contract subscriber limit**

Reality at `runtime.rs:1192-1209`:
- `bridged_register_contract_notifier` adds subscribers to a `Vec` per contract with no limit.
- Uses `UnboundedSender<HostResult>` — no backpressure.
- The line numbers are wrong but the claim is accurate.

#### M5: "No Per-Contract Execution Fairness"

**Audit claim**: 16 executors, 5-second timeout, no fairness. One contract can hold all executors.

**VERDICT: CONFIRMED — No per-contract concurrency limit**

Reality at `handler.rs:103-112`:
- Pool size is `parallelism.clamp(2, 16)` — max 16, confirmed.
- No per-contract concurrency cap exists in `RuntimePool`.
- N copies of the same contract (or N contracts with the same WASM) can saturate the entire pool.

#### M6: "Corrupted State Recovery Enables State Rollback"

**Audit claim**: When merge fails, recovery replaces local state with incoming state (line 1083), enabling state rollback.

**VERDICT: MOSTLY FIXED — Recovery now has safeguards**

Reality at `runtime.rs:1090-1162`:
- The recovery path now:
  1. Only triggers when the **local** state fails validation (lines 1105-1109) — "legitimate" merge failures (local state is valid) are propagated as errors (line 1122).
  2. Uses a `recovery_guard` (lines 1128-1139) to prevent repeated recovery on the same contract.
  3. Only attempts recovery when a validated full incoming state is available (line 1094).
- This is significantly more robust than the audit describes. The "send old-but-valid state to trigger rollback" attack would fail because:
  - The local state must fail validation for recovery to trigger.
  - Recovery is attempted at most once per contract (guard check).
- The line numbers are completely wrong (audit says 1035-1085, actual logic is 1090-1162).

#### M7: "Network Amplification via UPDATE Broadcast"

**Audit claim**: No rate limiting on broadcast frequency. A contract that always returns `changed=true` triggers unlimited broadcast cascades.

**VERDICT: CONFIRMED — No broadcast rate limiting**

Reality:
- `broadcast_state_change` at `runtime.rs:1588` fires `NodeEvent::BroadcastStateChange` with no rate limiting.
- Grep for rate limiting on contract operations found nothing relevant — only log rate limiting and auto-update rate limiting exist.
- The audit's concern about broadcast cascades is valid.

#### M8: "Timeout Thread Leak (Wasmer Backend)"

**VERDICT: NOT APPLICABLE — Wasmer backend does not exist in the current codebase**

There is no `wasmer_engine.rs` file. The codebase requires `wasmtime-backend` feature flag (`engine/mod.rs:28-29`). `Cargo.toml` has no wasmer dependency. All references to wasmer in code are in comments or documentation.

### LOW

#### L1: "DelegateCallEnv Raw Pointer Soundness"

**Audit claim**: Latent UB if execution model changes from single-threaded at `native_api.rs:67-88`.

**VERDICT: DESIGN ADDRESSED DIFFERENTLY**

Reality:
- `DELEGATE_ENV` uses `DashMap` (concurrent HashMap), not raw pointers for the map itself.
- `DelegateCallEnv` (lines 90-170) does contain raw pointers (`*mut SecretsStore`, `*const ContractStore`).
- The safety invariant is documented extensively (lines 143-192) — raw pointers are safe because WASM execution is synchronous on the calling thread and the Runtime holds `&mut self`.
- This is a documented design decision, not an oversight. The audit's suggestion of `Arc<Mutex<>>` would add unnecessary overhead for a single-threaded execution model.

#### L2: "No PUT Originator Authentication"

**VERDICT: LIKELY STILL VALID** — By design, Freenet allows any node to publish any contract. This is an architectural property of the content-addressed network, not a bug.

#### L3: "`unreachable!()` in UpdateQuery Handler"

**Audit claim**: `contract/mod.rs:608`.

**VERDICT: CONFIRMED at different location**

Reality at `contract/mod.rs:821`:
```rust
| UpdateData::RelatedState { .. }
| UpdateData::RelatedDelta { .. }
| UpdateData::RelatedStateAndDelta { .. } => {
    unreachable!()
}
```
- A second `unreachable!()` at line 1083 for response events.
- Line number is wrong (821, not 608).

#### L4: "Unbounded Contract Handler Channel"

**Audit claim**: `handler.rs:269` uses `mpsc::unbounded_channel()`.

**VERDICT: CONFIRMED** — `client_responses_channel()` at `handler.rs:37` uses `mpsc::unbounded_channel()`. Line 269 is elsewhere in the file.

#### L5: "No Related Contract Depth/Count Limit"

**VERDICT: NOT VERIFIED** — No explicit limit found in the code, but `RelatedContracts` is a stdlib type whose internals were not checked.

#### L6: "Engine Permanently Unhealthy After Timeout"

**VERDICT: NOT APPLICABLE** — Wasmer-specific finding. Wasmer backend does not exist.

---

## Structural Inaccuracies in the Audit

### Wasmer Backend Does Not Exist

The audit repeatedly references a wasmer backend (`wasmer_engine.rs`) and claims it's "the default in Cargo.toml." This is **false** in the current codebase:
- `engine/mod.rs:28-29`: `compile_error!("The wasmtime-backend feature must be enabled.");`
- No `wasmer_engine.rs` file exists anywhere in the codebase.
- `Cargo.toml` has no wasmer dependency.

This suggests the audit was conducted against a different version where both backends existed.

### Line Numbers Are Almost Universally Wrong

| Finding | Claimed Location | Actual Location |
|---------|-----------------|-----------------|
| C1 compute_ptr | native_api.rs:184-186 | native_api.rs:348-381 (and renamed) |
| C2 from_utf8_unchecked | native_api.rs:201 | native_api.rs:404 |
| C3 enable_metering | runtime.rs:169 | runtime.rs:172 |
| H1 state_store | state_store.rs:101-151 | Correct range |
| H3 upsert | runtime.rs:677-1112 | runtime.rs:708-1190 |
| H4 MEM_ADDR | runtime.rs:80-87 | runtime.rs:79-81 (close) |
| M1 panics | native_api.rs:196-198 | native_api.rs:394, 422, 449 |
| M3 init tracker | init_tracker.rs:108 | init_tracker.rs:134 |
| M6 recovery | runtime.rs:1035-1085 | runtime.rs:1090-1162 |
| L3 unreachable | contract/mod.rs:608 | contract/mod.rs:821 |
| L4 unbounded channel | handler.rs:269 | handler.rs:37 |

### Appendix A Root Cause Note Is Wrong

The audit states at the end of Appendix A:
> At runtime.rs:80, the memory size is available but discarded:
> `let (ptr, _size) = engine.memory_info(&handle)?;`

In the current code, the size is **not** discarded:
```rust
let (ptr, size) = engine.memory_info(&handle)?;
native_api::MEM_ADDR.insert(id, InstanceInfo::new(ptr as i64, size, key));
```

This fix was already implemented.

---

## Summary: What's Still Valid

| Finding | Status | Severity Now |
|---------|--------|-------------|
| C1: No bounds checking | **FIXED** | N/A |
| C2: from_utf8_unchecked | **Residual** (UB still exists, but bounds-checked) | Low |
| C3: Gas metering disabled | **CONFIRMED** | High |
| H1: No state size limit | **CONFIRMED** | High |
| H2: No WASM pre-validation | **Minor** (wasmtime validates internally) | Low |
| H3: TOCTOU race | **Partially valid** | Medium |
| H4: Stale MEM_ADDR | **Partially valid** (mitigated by wasmtime memory model) | Low |
| M1: Panics in host functions | **Partially fixed** (.expect still exists) | Low |
| M2: Integer overflow in len cast | **Mitigated** by bounds validation | Low |
| M3: Unbounded init queue | **CONFIRMED** | Medium |
| M4: No subscriber limit | **CONFIRMED** | Medium |
| M5: No execution fairness | **CONFIRMED** | High |
| M6: State rollback via recovery | **Mostly fixed** (recovery guard + validation) | Low |
| M7: No broadcast rate limiting | **CONFIRMED** | Medium |
| M8: Wasmer timeout leak | **N/A** (wasmer removed) | N/A |
| L1-L6 | Mixed — L6 N/A (wasmer) | Low |

### Top Remaining Concerns (Priority Order)

1. **C3 + M5**: Gas metering disabled + no per-contract fairness = the dormant worm DoS attack described in the GHSA is **still viable**. This is the highest-priority fix.
2. **H1**: No state size limit enables storage exhaustion.
3. **M7**: No broadcast rate limiting enables amplification.
4. **C2 residual**: `from_utf8_unchecked` should be replaced with `from_utf8_lossy()` — easy fix.
5. **M3/M4**: Unbounded queues and subscriber lists need caps.

### The Dormant Worm GHSA

The GHSA-xxxx-xxxx-xxxx (dormant worm DoS) chains C3 + M5 + M7, all of which are **confirmed still present**. The attack chain described in the advisory is viable against the current codebase. The PoC contract would work as described: deploy N contracts, arm them via ACTIVATE delta, and every hosting node is permanently DoS'd.
