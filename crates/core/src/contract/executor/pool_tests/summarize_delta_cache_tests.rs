//! Tests for the cheap state-change-detector that backs the read-only
//! `summarize_contract_state` / `get_contract_state_delta` caches.
//!
//! The optimization replaces the per-call full-state load + `DefaultHasher`
//! pass (used only to build the summary/delta cache key) with a cheap
//! per-contract change-detector kept in `StateStore`. The detector is
//! invalidated by every state write and repopulated from a freshly-loaded
//! state, so an unchanged contract summarizes/deltas without reloading or
//! rehashing the state, or re-running WASM — while a changed contract is always
//! recomputed.
//!
//! The stake is divergence: a stale summary OR delta served against changed
//! state would hand a peer the wrong bytes. The correctness tests below assert
//! freshness after an update and would FAIL if the detector missed a write.

use either::Either;
use freenet_stdlib::prelude::*;

use crate::contract::executor::mock_wasm_runtime::MockWasmRuntime;
use crate::contract::executor::{ContractExecutor, Executor};
use crate::wasm_runtime::MockStateStorage;

use super::super::mock_runtime::test::create_test_contract as test_contract;

async fn cached_executor(storage: MockStateStorage) -> Executor<MockWasmRuntime, MockStateStorage> {
    Executor::new_mock_wasm("summarize_delta_cache", storage, None, None)
        .await
        .expect("create cached MockWasmRuntime executor")
}

// =========================================================================
// CORRECTNESS (the guard against divergence)
// =========================================================================

/// PUT a contract, summarize, UPDATE it, summarize again → the second summarize
/// MUST reflect the updated state, never the stale cached summary. This fails if
/// the detector misses the write (e.g. if a write path stopped invalidating it).
#[tokio::test(flavor = "current_thread")]
async fn summarize_is_fresh_after_update_not_stale_cache() {
    let storage = MockStateStorage::new();
    let mut exec = cached_executor(storage).await;

    let contract = test_contract(b"summarize_fresh_after_update");
    let key = contract.key();
    let state_a = WrappedState::new(vec![1, 2, 3]);
    let state_b = WrappedState::new(vec![9, 8, 7, 6]);

    // Initial PUT of state A.
    exec.upsert_contract_state(
        key,
        Either::Left(state_a.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT A");

    let summary_a = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize A");
    assert_eq!(
        summary_a.as_ref(),
        blake3::hash(state_a.as_ref()).as_bytes(),
        "summary of A must be blake3(A)"
    );

    // Repeat on unchanged state: still A (exercises the fast path).
    let summary_a2 = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize A again");
    assert_eq!(summary_a2.as_ref(), summary_a.as_ref());

    // UPDATE to state B.
    exec.upsert_contract_state(
        key,
        Either::Left(state_b.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE B");

    // Summarize again: MUST be fresh (B), not the stale cached A.
    let summary_b = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize B");
    assert_eq!(
        summary_b.as_ref(),
        blake3::hash(state_b.as_ref()).as_bytes(),
        "summary must reflect updated state B, not stale cached A"
    );
    assert_ne!(
        summary_b.as_ref(),
        summary_a.as_ref(),
        "summary must change after the state changed"
    );
}

/// The delta sibling of the test above: a delta computed against a fixed peer
/// summary MUST be recomputed after the state changes, never served stale.
/// `MockWasmRuntime::get_state_delta` returns the full state as the delta, so a
/// fresh delta equals the new state bytes and a stale one would equal the old.
#[tokio::test(flavor = "current_thread")]
async fn delta_is_fresh_after_update_not_stale_cache() {
    let storage = MockStateStorage::new();
    let mut exec = cached_executor(storage).await;

    let contract = test_contract(b"delta_fresh_after_update");
    let key = contract.key();
    let state_a = WrappedState::new(vec![1, 2, 3]);
    let state_b = WrappedState::new(vec![9, 8, 7, 6]);
    // A fixed peer summary: the same value is used before and after the update so
    // ONLY the state component of the delta cache key changes.
    let peer_summary = StateSummary::from(vec![42u8; 4]);

    exec.upsert_contract_state(
        key,
        Either::Left(state_a.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT A");

    let delta_a = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta A");
    assert_eq!(
        delta_a.as_ref(),
        state_a.as_ref(),
        "mock delta is the full state A"
    );

    // Repeat on unchanged state + same peer summary: cached, identical delta.
    let delta_a2 = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta A again");
    assert_eq!(delta_a2.as_ref(), delta_a.as_ref());

    // UPDATE to state B.
    exec.upsert_contract_state(
        key,
        Either::Left(state_b.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE B");

    // Delta against the SAME peer summary: MUST be fresh (B), not stale (A).
    let delta_b = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta B");
    assert_eq!(
        delta_b.as_ref(),
        state_b.as_ref(),
        "delta must reflect updated state B, not stale cached A"
    );
    assert_ne!(
        delta_b.as_ref(),
        delta_a.as_ref(),
        "delta must change after the state changed"
    );
}

// =========================================================================
// OPTIMIZATION (the fast path skips the state load + rehash + WASM)
// =========================================================================

/// On an UNCHANGED contract, repeated summarize must NOT reload the state from
/// storage (and therefore must not re-hash it or re-run WASM). Uses an uncached
/// `StateStore` so every load is observable via `MockStateStorage::get_count`.
#[tokio::test(flavor = "current_thread")]
async fn summarize_unchanged_skips_state_load() {
    let storage = MockStateStorage::new();
    let mut exec = Executor::new_mock_wasm_uncached("summarize_opt", storage.clone())
        .await
        .expect("create uncached executor");

    let contract = test_contract(b"summarize_opt");
    let key = contract.key();
    let state = WrappedState::new(vec![1, 2, 3, 4, 5]);

    exec.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT");

    // First summarize: cold detector → slow path, loads the state.
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize 1");
    let gets_after_first = storage.get_count();
    assert!(
        gets_after_first >= 1,
        "the cold first summarize must load the state at least once"
    );

    // Second + third summarize on unchanged state: fast path, NO extra loads.
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize 2");
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize 3");
    assert_eq!(
        storage.get_count(),
        gets_after_first,
        "repeated unchanged summarize must not reload the state (fast path)"
    );

    // After an UPDATE the detector is invalidated → the next summarize reloads.
    let new_state = WrappedState::new(vec![6, 7, 8, 9, 10, 11]);
    exec.upsert_contract_state(
        key,
        Either::Left(new_state.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE");
    let gets_before_post_update = storage.get_count();
    let _ = exec
        .summarize_contract_state(key)
        .await
        .expect("summarize after update");
    assert!(
        storage.get_count() > gets_before_post_update,
        "summarize after an update must reload the changed state"
    );
}

/// Delta sibling: repeated delta on an UNCHANGED contract against the same peer
/// summary must NOT reload the state.
#[tokio::test(flavor = "current_thread")]
async fn delta_unchanged_skips_state_load() {
    let storage = MockStateStorage::new();
    let mut exec = Executor::new_mock_wasm_uncached("delta_opt", storage.clone())
        .await
        .expect("create uncached executor");

    let contract = test_contract(b"delta_opt");
    let key = contract.key();
    let state = WrappedState::new(vec![5, 4, 3, 2, 1]);
    let peer_summary = StateSummary::from(vec![7u8; 3]);

    exec.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT");

    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta 1");
    let gets_after_first = storage.get_count();
    assert!(
        gets_after_first >= 1,
        "cold first delta must load the state"
    );

    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta 2");
    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta 3");
    assert_eq!(
        storage.get_count(),
        gets_after_first,
        "repeated unchanged delta must not reload the state (fast path)"
    );

    // After an UPDATE the detector is invalidated → the next delta against the
    // SAME peer summary must reload the changed state (symmetry with
    // `summarize_unchanged_skips_state_load`).
    let new_state = WrappedState::new(vec![9, 8, 7, 6, 5, 4]);
    exec.upsert_contract_state(
        key,
        Either::Left(new_state.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE");
    let gets_before_post_update = storage.get_count();
    let _ = exec
        .get_contract_state_delta(key, peer_summary.clone())
        .await
        .expect("delta after update");
    assert!(
        storage.get_count() > gets_before_post_update,
        "delta after an update must reload the changed state"
    );
}

// =========================================================================
// SCALE (the 0.2.98 fix: cover the hosted set, not a fixed 1024)
// =========================================================================

/// At an every-hop-hosting hosted set LARGER than the historical fixed
/// 1024-entry cap, re-summarizing an UNCHANGED contract must still hit the fast
/// path (no state reload → no WASM `summarize_state` → no module recompile).
///
/// This is the regression guard for the 0.2.98 anti-entropy re-summarization
/// storm. With the old fixed `LruCache::new(1024)`, warming > 1024 distinct
/// contracts evicted the earliest summaries, so the ~5-min InterestSync
/// heartbeat re-summarize of those contracts fell to the slow path (reload +
/// WASM summarize + a possible module recompile) EVERY cycle. The cache is now
/// sized from the hosting byte budget (`summarize_cache_capacity`), so the whole
/// hosted set stays cached. `get_count` is the fast/slow-path discriminator: the
/// slow path ALWAYS loads the state before it runs WASM, so "no additional load"
/// proves "no WASM summarize" (uses an uncached `StateStore` so every load is
/// observable).
#[tokio::test(flavor = "current_thread")]
async fn summarize_unchanged_hits_fast_path_beyond_old_1024_cap() {
    use crate::contract::executor::{
        build_delta_cache, build_summary_cache, delta_cache_capacity_bytes,
        summarize_cache_capacity,
    };

    let storage = MockStateStorage::new();
    let mut exec = Executor::new_mock_wasm_uncached("summarize_scale", storage.clone())
        .await
        .expect("create uncached executor");

    // Size the SUMMARY cache from a 1 GiB hosting budget (the gateway default),
    // which derives to 8192 entries — far above both the old 1024 cap and the warm
    // set below. The delta cache is byte-sized from the same budget. Injected via
    // the SAME setter the pool uses in production.
    let capacity = summarize_cache_capacity(1024 * 1024 * 1024);
    assert!(
        capacity > 1024,
        "the gateway-default hosting budget must derive a cache larger than the old fixed cap"
    );
    exec.set_summarize_caches(
        build_summary_cache(capacity),
        build_delta_cache(delta_cache_capacity_bytes(1024 * 1024 * 1024)),
    );

    // Warm WELL past the old 1024 cap (but under the derived capacity, so nothing
    // is evicted). Each contract is PUT then summarized once (cold → slow path,
    // populating both the detector and the summary cache).
    const WARM: usize = 1500;
    let mut keys = Vec::with_capacity(WARM);
    for i in 0..WARM {
        let seed = format!("summarize-scale-{i}");
        let contract = test_contract(seed.as_bytes());
        let key = contract.key();
        keys.push(key);
        let state = WrappedState::new(vec![(i & 0xff) as u8, (i >> 8) as u8, 7, 7]);
        exec.upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("warm PUT");
        let _ = exec
            .summarize_contract_state(key)
            .await
            .expect("warm summarize");
    }

    // Re-summarize the FIRST (oldest) contract on unchanged state. With the old
    // fixed 1024-entry cache this key's summary would have been evicted by the
    // 1500 warm inserts, forcing a reload; with the hosting-budget-sized cache it
    // is still cached, so no reload (and no WASM summarize) happens.
    let gets_before = storage.get_count();
    let _ = exec
        .summarize_contract_state(keys[0])
        .await
        .expect("re-summarize oldest");
    assert_eq!(
        storage.get_count(),
        gets_before,
        "re-summarizing the oldest of {WARM} hosted contracts (> old 1024 cap) must hit the \
         fast path (no reload, no WASM summarize) with the budget-sized cache"
    );

    // Freshness-after-resize: the larger cache must NOT weaken the staleness
    // guarantee. UPDATE the oldest contract, then re-summarize — the result must
    // reflect the NEW state (the detector invalidation forces a recompute), never
    // the summary still cached in the (large) budget-sized cache.
    let new_state = WrappedState::new(vec![0xAB, 0xCD, 0xEF, 0x01]);
    exec.upsert_contract_state(
        keys[0],
        Either::Left(new_state.clone()),
        RelatedContracts::default(),
        None,
    )
    .await
    .expect("UPDATE oldest");
    let summary_after = exec
        .summarize_contract_state(keys[0])
        .await
        .expect("re-summarize after update");
    assert_eq!(
        summary_after.as_ref(),
        blake3::hash(new_state.as_ref()).as_bytes(),
        "after an UPDATE the summary must reflect the NEW state, never the stale summary \
         still held in the budget-sized cache (sizing must not weaken freshness)"
    );
}

/// Cross-executor sharing (the CORE of the 0.2.98 fix): a summary computed on one
/// pool executor must be served on the FAST path by another executor, because the
/// pool injects ONE shared summary cache (+ shared change-detector via the shared
/// `StateStore`) into every executor. The headline scale test uses a single
/// executor, so it proves sizing but NOT this cross-executor sharing; this guards
/// it behaviorally (dropping the injection is exercised by the control C).
#[tokio::test(flavor = "current_thread")]
async fn shared_summary_cache_serves_fast_path_across_executors() {
    use crate::contract::executor::{
        build_delta_cache, build_summary_cache, delta_cache_capacity_bytes,
        summarize_cache_capacity,
    };
    use crate::wasm_runtime::StateStore;

    // ONE shared uncached StateStore: its change-detector `state_hash_cache` and
    // the backing storage clone together (moka is internally Arc), exactly like the
    // production pool sharing one cloned StateStore across executors. Uncached so
    // every state LOAD hits the backing storage and is observable via get_count.
    let storage = MockStateStorage::new();
    let state_store = StateStore::new_uncached(storage.clone());

    // ONE shared summary/delta cache pair, injected into BOTH working executors —
    // exactly what RuntimePool does.
    let budget = 1024 * 1024 * 1024;
    let shared_summary = build_summary_cache(summarize_cache_capacity(budget));
    let shared_delta = build_delta_cache(delta_cache_capacity_bytes(budget));

    let mut exec_a = Executor::new_mock_wasm_uncached_with_state_store(state_store.clone())
        .await
        .expect("executor A");
    exec_a.set_summarize_caches(shared_summary.clone(), shared_delta.clone());
    let mut exec_b = Executor::new_mock_wasm_uncached_with_state_store(state_store.clone())
        .await
        .expect("executor B");
    exec_b.set_summarize_caches(shared_summary.clone(), shared_delta.clone());

    // Control executor C: SAME shared StateStore (so the shared DETECTOR is warm)
    // but a PRIVATE summary cache (it never gets set_summarize_caches) — models
    // "dropped the shared injection".
    let mut exec_c = Executor::new_mock_wasm_uncached_with_state_store(state_store.clone())
        .await
        .expect("executor C");

    let contract = test_contract(b"cross-executor-share");
    let key = contract.key();
    let state = WrappedState::new(vec![3, 1, 4, 1, 5, 9]);

    // A PUTs and summarizes K → populates the SHARED detector (cache_state_hash)
    // AND the SHARED summary cache (keyed by state hash).
    exec_a
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("A PUT");
    let _ = exec_a
        .summarize_contract_state(key)
        .await
        .expect("A summarize");

    // B has NEVER summarized K, but both halves of the fast path (detector via the
    // shared StateStore, summary via the shared cache) are populated by A, so B
    // serves it fast: no state reload (no get_count bump), no WASM summarize.
    let gets_before_b = storage.get_count();
    let summary_b = exec_b
        .summarize_contract_state(key)
        .await
        .expect("B summarize");
    assert_eq!(
        storage.get_count(),
        gets_before_b,
        "executor B must serve K from the POOL-SHARED summary cache A populated (no state \
         reload) — the cross-executor sharing the fix relies on"
    );
    assert_eq!(
        summary_b.as_ref(),
        blake3::hash(state.as_ref()).as_bytes(),
        "the shared cached summary must be correct for K's state"
    );

    // Control: C shares the warm DETECTOR but has a PRIVATE summary cache, so its
    // fast path misses on the summary half and MUST reload — proving it is the
    // SHARED summary cache (not merely the shared detector) that lets B skip the
    // load. This is exactly what dropping the injection into replacements/executors
    // would cost.
    let gets_before_c = storage.get_count();
    let _ = exec_c
        .summarize_contract_state(key)
        .await
        .expect("C summarize");
    assert!(
        storage.get_count() > gets_before_c,
        "control: an executor WITHOUT the shared summary cache must reload the state (its \
         private cache misses), proving the shared injection is load-bearing"
    );
}

/// Delta sibling of `summarize_unchanged_hits_fast_path_beyond_old_1024_cap`: at a
/// working set LARGER than the historical fixed 1024-ENTRY cap, re-computing an
/// UNCHANGED delta must still hit the fast path. The delta cache is now
/// BYTE-bounded, so 1500 tiny deltas (a few bytes each) all stay cached where the
/// old entry cap would have evicted the earliest.
#[tokio::test(flavor = "current_thread")]
async fn delta_unchanged_hits_fast_path_beyond_old_1024_cap() {
    use crate::contract::executor::{
        build_delta_cache, build_summary_cache, delta_cache_capacity_bytes,
        summarize_cache_capacity,
    };

    let storage = MockStateStorage::new();
    let mut exec = Executor::new_mock_wasm_uncached("delta_scale", storage.clone())
        .await
        .expect("create uncached executor");

    // Byte-sized delta cache from the 1 GiB gateway-default budget (64 MiB). The
    // warm set below is 1500 TINY deltas (`MockWasmRuntime` returns the ~3-byte
    // state as the delta), ~KB total, so the byte budget evicts nothing while the
    // old fixed 1024-ENTRY cap would have evicted the earliest. A fixed peer summary
    // keeps ONLY the state component of the delta cache key varying across
    // contracts.
    let budget = 1024 * 1024 * 1024;
    exec.set_summarize_caches(
        build_summary_cache(summarize_cache_capacity(budget)),
        build_delta_cache(delta_cache_capacity_bytes(budget)),
    );
    let peer_summary = StateSummary::from(vec![9u8; 3]);

    const WARM: usize = 1500;
    let mut keys = Vec::with_capacity(WARM);
    for i in 0..WARM {
        let seed = format!("delta-scale-{i}");
        let contract = test_contract(seed.as_bytes());
        let key = contract.key();
        keys.push(key);
        let state = WrappedState::new(vec![(i & 0xff) as u8, (i >> 8) as u8, 3]);
        exec.upsert_contract_state(
            key,
            Either::Left(state),
            RelatedContracts::default(),
            Some(contract),
        )
        .await
        .expect("warm PUT");
        let _ = exec
            .get_contract_state_delta(key, peer_summary.clone())
            .await
            .expect("warm delta");
    }

    // Re-delta the FIRST (oldest) contract, unchanged state, same peer summary. With
    // the old fixed 1024-ENTRY cache this entry would have been evicted by the 1500
    // warm inserts, forcing a reload; the byte-bounded cache holds all 1500 tiny
    // deltas, so no reload (no WASM get_state_delta) happens.
    let gets_before = storage.get_count();
    let _ = exec
        .get_contract_state_delta(keys[0], peer_summary.clone())
        .await
        .expect("re-delta oldest");
    assert_eq!(
        storage.get_count(),
        gets_before,
        "re-delta of the oldest of {WARM} contracts (> old 1024 ENTRY cap) must hit the fast \
         path (no reload) with the byte-bounded delta cache"
    );
}

/// REGRESSION (#4794 review): the byte weigher must FLOOR each entry's weight so
/// empty/tiny deltas still count toward eviction. moka evicts only when total
/// WEIGHT exceeds the byte budget, and a raw byte-length weigher reports 0 for the
/// EMPTY deltas some contracts return (the website / UI-container contract). A
/// stream of distinct empty-delta keys — one per UPDATE, since every UPDATE mints a
/// fresh `(key, state_hash, summary_hash)` — would then NEVER be evicted and the
/// entry count would grow unbounded, re-opening the #4565 OOM class this cache
/// closes.
///
/// This inserts FAR more distinct EMPTY-delta keys than `budget / floor` and
/// asserts the entry count stays bounded by that ratio. Without the floor every
/// entry weighs 0, nothing is ever evicted, and `entry_count` equals the number
/// inserted (here 2000, ~62x the bound) — so this test would fail.
#[tokio::test(flavor = "current_thread")]
async fn delta_cache_empty_deltas_stay_entry_bounded() {
    use crate::contract::executor::{DELTA_ENTRY_OVERHEAD_FLOOR_BYTES, build_delta_cache};

    // A deliberately tiny byte budget so the derived max entry count is small and
    // easy to exceed: 32 * floor => at most 32 floored entries fit.
    let floor = u64::from(DELTA_ENTRY_OVERHEAD_FLOOR_BYTES);
    let max_entries = 32u64;
    let capacity_bytes = max_entries * floor;
    let cache = build_delta_cache(capacity_bytes);

    // Insert MANY distinct keys, each with an EMPTY delta (byte length 0). Distinct
    // ContractKeys model the per-UPDATE churn of `(key, state_hash, summary_hash)`
    // cache keys the website contract produces with empty deltas.
    const INSERTS: usize = 2000;
    for i in 0..INSERTS {
        let seed = format!("delta-floor-{i}");
        let key = test_contract(seed.as_bytes()).key();
        cache.insert((key, 0u64, 0u64), StateDelta::from(Vec::<u8>::new()));
    }

    // Force moka's pending eviction/housekeeping so the counts are settled.
    cache.run_pending_tasks();

    let entries = cache.entry_count();
    let weighted = cache.weighted_size();
    assert!(
        entries <= max_entries,
        "delta cache must stay entry-bounded: inserted {INSERTS} distinct EMPTY deltas into a \
         {capacity_bytes}-byte cache (floor {floor} => max {max_entries} entries) but holds \
         {entries} (weighted_size {weighted}). Without the per-entry weight floor every empty \
         delta weighs 0, nothing is evicted, and this would equal {INSERTS} (#4565 OOM class)."
    );
    assert!(
        weighted <= capacity_bytes,
        "floored weighted_size ({weighted}) must stay within the byte budget ({capacity_bytes})"
    );
    // Sanity: the bound is FAR below the insert count (not merely equal to it).
    assert!(
        entries < INSERTS as u64,
        "entry count ({entries}) must not grow with the {INSERTS} inserts"
    );
}

// =========================================================================
// EDGE CASES
// =========================================================================

/// State-absent: summarizing a contract with no stored state must error (the
/// #4610 state-presence behavior), never fast-path a phantom summary.
#[tokio::test(flavor = "current_thread")]
async fn summarize_absent_contract_errors() {
    let storage = MockStateStorage::new();
    let mut exec = cached_executor(storage).await;
    let contract = test_contract(b"summarize_absent");
    let key = contract.key();

    let result = exec.summarize_contract_state(key).await;
    assert!(
        result.is_err(),
        "summarize of an absent contract must error, not return a phantom summary"
    );
}

/// Cold caches (simulating a restart or a detector/LRU eviction): a second
/// executor that shares the backing storage but has cold detector + summary
/// caches must recompute the correct summary from the on-disk state, not error
/// or return a phantom.
#[tokio::test(flavor = "current_thread")]
async fn summarize_with_cold_caches_recomputes_correctly() {
    let storage = MockStateStorage::new();
    let contract = test_contract(b"summarize_restart");
    let key = contract.key();
    let state = WrappedState::new(vec![3, 1, 4, 1, 5]);

    // Executor 1 PUTs and summarizes (warming its caches and the backing store).
    let mut e1 = cached_executor(storage.clone()).await;
    e1.upsert_contract_state(
        key,
        Either::Left(state.clone()),
        RelatedContracts::default(),
        Some(contract.clone()),
    )
    .await
    .expect("PUT");
    let s1 = e1
        .summarize_contract_state(key)
        .await
        .expect("summarize e1");
    assert_eq!(s1.as_ref(), blake3::hash(state.as_ref()).as_bytes());

    // Executor 2 shares the backing storage but its detector + summary caches are
    // cold (the StateStore detector is per-StateStore-instance). It must still
    // recompute the correct summary from disk.
    let mut e2 = cached_executor(storage.clone()).await;
    let s2 = e2
        .summarize_contract_state(key)
        .await
        .expect("summarize e2");
    assert_eq!(
        s2.as_ref(),
        blake3::hash(state.as_ref()).as_bytes(),
        "cold-cache executor must recompute the correct summary from disk"
    );
}
