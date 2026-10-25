//! Tests for the SAMPLED re-apply probe (`Executor::maybe_probe_idempotency`)
//! through the executor's merge path. The decision rule itself
//! (`runtime::reapply_probe::reapply_verdict`) has its own unit tests.
//!
//! The probe re-applies the incoming state `X` to the merge result
//! `merge(S, X)`. A difference there is a strict associativity failure on the
//! triple `(S, X, X)`, which the conformance verifier reports only when the two
//! groupings fail to heal (#5915). These tests pin that a merge that changes once
//! on re-apply and then settles is not flagged, that a merge that keeps moving
//! is, and that the probe's verdict agrees with
//! `verify_case(StateAssociativity, [S, X, X])` on every pair of four small
//! merges.
//!
//! Sampling is forced with `IDEMPOTENCY_PROBE_ALWAYS`, which is thread-local, so
//! every test that drives the merge path runs on a `current_thread` runtime.

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use super::identical_input_probe_tests::{build_executor, build_op_manager};
use crate::conformance::runtime_oracle::BorrowedRuntimeOracle;
use crate::conformance::{ConformanceCase, ConformanceProperty, PropertyOutcome, verify_case};
use crate::contract::UpsertResult;
use crate::contract::executor::mock_wasm_runtime::UpdateOverride;
use crate::contract::executor::runtime::{
    IDEMPOTENCY_PROBE_ALWAYS, IdenticalIsNoOp, REAPPLY_PROBE_TIME_BUDGET,
};
use crate::contract::executor::{ContractExecutor, Executor};
use crate::ring::{BROKEN_INVARIANT_TTL, EMITTED_FLAG_EVENTS};
use crate::wasm_runtime::MockStateStorage;

use super::super::mock_runtime::test::create_test_contract as test_contract;

/// A symmetric, idempotent merge over the one-byte states `0..=3`:
///
/// | merge | 0 | 1 | 2 | 3 |
/// |---|---|---|---|---|
/// | 0 | 0 | 0 | 0 | 0 |
/// | 1 | 0 | 1 | 0 | 0 |
/// | 2 | 0 | 0 | 2 | 1 |
/// | 3 | 0 | 0 | 1 | 3 |
///
/// It is commutative and idempotent, and its eight strictly non-associative
/// triples all heal, so the verifier accepts it. `merge(2, 3) = 1` but
/// `merge(1, 3) = 0`: re-applying `X = 3` changes the result once, and
/// `merge(0, 3) = 0` is then a fixpoint.
fn settles_after_one_reapply(a: &[u8], b: &[u8]) -> Vec<u8> {
    const TABLE: [[u8; 4]; 4] = [[0, 0, 0, 0], [0, 1, 0, 0], [0, 0, 2, 1], [0, 0, 1, 3]];
    vec![TABLE[a[0] as usize % 4][b[0] as usize % 4]]
}

/// A merge that never settles: merging any different state advances the current
/// one around `0 -> 1 -> 2 -> 0`, so re-applying the same incoming state cycles.
fn cycles_on_reapply(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a == b {
        a.to_vec()
    } else {
        vec![(a[0] + 1) % 3]
    }
}

fn force_sampling() -> impl Drop {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            IDEMPOTENCY_PROBE_ALWAYS.with(|always| always.set(false));
        }
    }
    IDEMPOTENCY_PROBE_ALWAYS.with(|always| always.set(true));
    EMITTED_FLAG_EVENTS.with(|events| events.borrow_mut().clear());
    Reset
}

fn emitted_flag_events() -> Vec<serde_json::Value> {
    EMITTED_FLAG_EVENTS.with(|events| events.borrow().clone())
}

/// Install `stored` with a PUT, then deliver `incoming` as a full state through
/// the production merge path with the probe sampling every merge.
async fn deliver(
    id: &str,
    merge: UpdateOverride,
    stored: &[u8],
    incoming: &[u8],
) -> (UpsertResult, bool, Vec<serde_json::Value>, Option<Vec<u8>>) {
    let (op_manager, _guards) = build_op_manager(id).await;
    let contract = test_contract(id.as_bytes());
    let key = contract.key();
    let mut executor = build_executor(&op_manager).await;
    executor
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), merge);

    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(stored.to_vec())),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");

    let _sampling = force_sampling();
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(incoming.to_vec())),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("full-state delivery");
    let events = emitted_flag_events();
    let (stored_after, _) = executor
        .fetch_contract(key, false)
        .await
        .expect("fetch after delivery");
    (
        result,
        op_manager.ring.is_contract_broken(&key),
        events,
        stored_after.map(|s| s.as_ref().to_vec()),
    )
}

/// A merge the verifier accepts, whose re-apply changes the state once and then
/// settles, is not flagged and commits.
#[tokio::test(flavor = "current_thread")]
async fn sampled_probe_does_not_flag_merge_that_settles_after_one_reapply() {
    let (s, x) = ([2u8], [3u8]);
    let merged = settles_after_one_reapply(&s, &x);
    let reapplied = settles_after_one_reapply(&merged, &x);
    assert_ne!(
        reapplied, merged,
        "the pair must change on re-apply, or this tests nothing"
    );
    assert_eq!(
        settles_after_one_reapply(&reapplied, &x),
        reapplied,
        "the pair must settle on the next re-apply"
    );

    let (result, broken, events, stored) = deliver(
        "probe-settles-once",
        UpdateOverride::MergeWith(settles_after_one_reapply),
        &s,
        &x,
    )
    .await;
    assert!(
        !broken,
        "a merge that settles after one re-apply must not be flagged"
    );
    assert!(
        matches!(result, UpsertResult::Updated(_)),
        "the merge must commit, got {result:?}"
    );
    assert_eq!(stored, Some(merged));
    assert!(
        events.is_empty(),
        "no flag, so no flag telemetry: {events:?}"
    );
}

/// A merge that cycles under re-apply is still flagged, the commit is
/// suppressed, and the flag is handed to telemetry.
#[tokio::test(flavor = "current_thread")]
async fn sampled_probe_flags_merge_that_cycles_and_emits_telemetry() {
    let (s, x) = ([0u8], [3u8]);
    let (result, broken, events, stored) = deliver(
        "probe-cycles",
        UpdateOverride::MergeWith(cycles_on_reapply),
        &s,
        &x,
    )
    .await;
    assert!(broken, "a merge that cycles on re-apply must be flagged");
    assert!(
        matches!(result, UpsertResult::NoChange),
        "the flagged merge must not commit, got {result:?}"
    );
    assert_eq!(stored, Some(s.to_vec()), "stored state must be untouched");

    assert_eq!(events.len(), 1, "exactly one flag event: {events:?}");
    let event = &events[0];
    assert_eq!(event["detector"], "sampled_reapply");
    assert_eq!(event["invariant"], "non_idempotent");
    assert_eq!(event["outcome"], "new");
    assert_eq!(event["verdict"], "groupings_do_not_converge");
    assert_eq!(
        event["suppression_ttl_secs"],
        BROKEN_INVARIANT_TTL.as_secs()
    );
    let contract = test_contract(b"probe-cycles");
    assert_eq!(event["contract"], contract.key().id().to_string());
}

/// The storm-shaped contract (mutates on every apply) is still caught by the
/// sampled probe on a non-identical delivery.
#[tokio::test(flavor = "current_thread")]
async fn sampled_probe_flags_merge_that_mutates_on_every_apply() {
    let counter = Arc::new(AtomicU64::new(0));
    let stored: Vec<u8> = (1u8..=32).collect();
    let incoming: Vec<u8> = (101u8..=132).collect();
    let (result, broken, events, _) = deliver(
        "probe-mutates",
        UpdateOverride::NonIdempotent(counter),
        &stored,
        &incoming,
    )
    .await;
    assert!(
        broken,
        "a merge that mutates on every apply must be flagged"
    );
    assert!(matches!(result, UpsertResult::NoChange));
    assert_eq!(events.len(), 1, "{events:?}");
}

type MergeFn = fn(&[u8], &[u8]) -> Vec<u8>;

/// Commutative and idempotent; on some pairs the reconciled state moves on a
/// re-delivered input, on others reconciliation revisits a state.
fn redelivery_and_cycle(a: &[u8], b: &[u8]) -> Vec<u8> {
    const TABLE: [[u8; 4]; 4] = [[0, 3, 0, 2], [3, 1, 3, 2], [0, 3, 2, 2], [2, 2, 2, 3]];
    vec![TABLE[a[0] as usize % 4][b[0] as usize % 4]]
}

/// Addition mod 4: commutative and associative, but it moves on every apply,
/// and `merge(x, x) != x` for `x != 0`.
fn sum_mod_4(a: &[u8], b: &[u8]) -> Vec<u8> {
    vec![(a[0] + b[0]) % 4]
}

/// The probe's verdict is the verifier's: on every pair `(S, X)` of four merges,
/// the probe flags exactly when `verify_case(StateAssociativity, [S, X, X])`
/// reports a violation, with the verifier run on an oracle that answers a merge
/// of a state into an identical copy of itself the way the node does (state
/// unchanged, no contract call). That is the only difference in what the two
/// compute: with it, the verifier's right grouping `merge(S, merge(X, X))` is
/// `merge(S, X)`, the probe's. `sum_mod_4` is where it matters: on the raw
/// contract the verifier finds it associative, while on the network every
/// re-delivery moves the state, and both must flag it.
///
/// Every state here is valid and no merge is refused, so the verifier's
/// validity checks and its refusal verdict, where the probe is deliberately
/// lenient, are not exercised; the rule's unit tests cover those arms.
#[tokio::test(flavor = "current_thread")]
async fn reapply_verdict_matches_verifier_on_s_x_x() {
    let contract = test_contract(b"probe-verdict-pin");
    let key = contract.key();
    let params = Parameters::from(vec![]);

    let merges: [(&str, MergeFn); 4] = [
        ("settles_after_one_reapply", settles_after_one_reapply),
        ("cycles_on_reapply", cycles_on_reapply),
        ("redelivery_and_cycle", redelivery_and_cycle),
        ("sum_mod_4", sum_mod_4),
    ];
    let mut verdicts_seen = std::collections::BTreeSet::new();
    let mut self_merge_differs = 0;
    for (name, merge) in merges {
        let mut executor: Executor<_, _> =
            Executor::new_mock_wasm("t", MockStateStorage::new(), None, None)
                .await
                .expect("build mock-wasm executor");
        executor
            .mock_runtime_mut()
            .update_overrides
            .insert(*key.id(), UpdateOverride::MergeWith(merge));

        let (mut differs_on_reapply, mut flagged) = (0, 0);
        for s in 0u8..4 {
            for x in 0u8..4 {
                let (s, x) = ([s], [x]);
                let merged = merge(&s, &x);
                if s == x || merged == s {
                    // The probe only runs on a merge that changed the state, and
                    // an incoming state identical to the stored one never
                    // reaches a merge (the byte-identical fast path).
                    continue;
                }
                let reapplied = merge(&merged, &x);
                if reapplied != merged {
                    differs_on_reapply += 1;
                }
                if merge(&x, &x) != x && reapplied != merged {
                    self_merge_differs += 1;
                }
                let verdict = executor.reapply_probe_verdict(
                    &key,
                    &params,
                    &WrappedState::new(s.to_vec()),
                    &x,
                    &WrappedState::new(merged.clone()),
                );
                let mut runtime =
                    BorrowedRuntimeOracle::new(executor.mock_runtime_mut(), &key, &params);
                let outcome = verify_case(
                    &mut IdenticalIsNoOp(&mut runtime),
                    &ConformanceCase::new(
                        ConformanceProperty::StateAssociativity,
                        vec![Arc::from(&s[..]), Arc::from(&x[..]), Arc::from(&x[..])],
                    ),
                );
                let violated = matches!(outcome, PropertyOutcome::Violated(_));
                assert_eq!(
                    verdict.is_some(),
                    violated,
                    "{name}: S={s:?} X={x:?}: probe verdict {verdict:?}, verifier {outcome:?}"
                );
                if let Some(verdict) = verdict {
                    verdicts_seen.insert(verdict.as_str());
                }
                flagged += usize::from(verdict.is_some());
            }
        }
        assert!(
            differs_on_reapply > 0,
            "{name}: no pair changes on re-apply, so this pins nothing"
        );
        match name {
            "settles_after_one_reapply" => assert_eq!(flagged, 0, "{name}"),
            _ => assert!(flagged > 0, "{name}: this merge must be flagged somewhere"),
        }
    }
    assert!(
        self_merge_differs > 0,
        "no changing pair has merge(X, X) != X, so the identical-merge rule is not pinned"
    );
    assert_eq!(
        verdicts_seen.into_iter().collect::<Vec<_>>(),
        vec![
            "groupings_do_not_converge",
            "reconciliation_cycles",
            "redelivery_keeps_moving"
        ],
        "every non-settling verdict must be reached and checked against the verifier"
    );
}

thread_local! {
    /// Calls of `merge([3], [3])`. On the recovery-path test's inputs that is
    /// the probe's re-apply: recovery leaves `[3]` as the result, and the probe
    /// would re-apply `X = [3]` to it.
    static REAPPLY_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Refuses to merge into the stored state `[9]`, which the contract also
/// considers invalid; otherwise the incoming state wins. Counts
/// [`REAPPLY_CALLS`].
fn refuses_corrupt_base(a: &[u8], b: &[u8]) -> Option<Vec<u8>> {
    if a == [9] {
        return None;
    }
    if a == [3] && b == [3] {
        REAPPLY_CALLS.with(|calls| calls.set(calls.get() + 1));
    }
    Some(b.to_vec())
}

/// When the stored state is invalid and the merge with it fails, the executor
/// replaces the stored state with the incoming one. That result is not
/// `merge(S, X)`, so the probe must not run on it.
#[tokio::test(flavor = "current_thread")]
async fn sampled_probe_skips_corrupted_state_recovery() {
    use crate::contract::executor::mock_wasm_runtime::ValidateOverride;

    let (op_manager, _guards) = build_op_manager("probe-recovery").await;
    let contract = test_contract(b"probe-recovery");
    let key = contract.key();
    let mut executor = build_executor(&op_manager).await;

    // Install [9] while it is still accepted, then make it invalid.
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![9])),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");
    let runtime = executor.mock_runtime_mut();
    runtime.update_overrides.insert(
        *key.id(),
        UpdateOverride::TryMergeWith(refuses_corrupt_base),
    );
    runtime
        .validate_overrides
        .insert(*key.id(), ValidateOverride::ValidWhen(|s| s != [9]));

    REAPPLY_CALLS.with(|calls| calls.set(0));
    let _sampling = force_sampling();
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![3])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("recovery replaces the corrupted state");
    assert!(
        matches!(result, UpsertResult::Updated(_)),
        "recovery must commit the incoming state, got {result:?}"
    );
    assert_eq!(
        REAPPLY_CALLS.with(std::cell::Cell::get),
        0,
        "the probe ran on the recovery result"
    );
    assert!(!op_manager.ring.is_contract_broken(&key));
}

thread_local! {
    /// Calls of [`counted_cycles`].
    static MERGE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// [`cycles_on_reapply`], counting its calls in [`MERGE_CALLS`].
fn counted_cycles(a: &[u8], b: &[u8]) -> Vec<u8> {
    MERGE_CALLS.with(|calls| calls.set(calls.get() + 1));
    cycles_on_reapply(a, b)
}

/// Once flagged, later deliveries skip the probe: each makes only its own merge
/// call, and a busy flagged contract produces one flag event, not one per
/// delivery.
#[tokio::test(flavor = "current_thread")]
async fn a_flagged_contract_is_not_probed_again_per_delivery() {
    let (op_manager, _guards) = build_op_manager("probe-once").await;
    let contract = test_contract(b"probe-once");
    let key = contract.key();
    let mut executor = build_executor(&op_manager).await;
    executor
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(counted_cycles));
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![0])),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");

    let _sampling = force_sampling();
    // Both later deliveries change the state, so each would be probed if the
    // flag did not stop it; the probe's re-apply alone is a second call.
    for (round, incoming) in [3u8, 3, 1].into_iter().enumerate() {
        if round == 1 {
            assert!(op_manager.ring.is_contract_broken(&key));
            MERGE_CALLS.with(|calls| calls.set(0));
        }
        executor
            .upsert_contract_state(
                key,
                Either::Left(WrappedState::new(vec![incoming])),
                RelatedContracts::default(),
                None,
            )
            .await
            .expect("delivery");
    }
    assert_eq!(
        MERGE_CALLS.with(std::cell::Cell::get),
        2,
        "one merge per delivery; any more is the probe running again"
    );
    assert!(op_manager.ring.is_contract_broken(&key));
    assert_eq!(
        emitted_flag_events().len(),
        1,
        "{:?}",
        emitted_flag_events()
    );
}

/// Only a batch of exactly one full state is probed. A full state with related
/// state appended, two full states and a delta are not, even when sampled; the
/// same inputs as a single full state are probed and flagged.
#[tokio::test(flavor = "current_thread")]
async fn sampled_probe_runs_only_on_a_single_state_batch() {
    let (op_manager, _guards) = build_op_manager("probe-gate").await;
    let contract = test_contract(b"probe-gate");
    let key = contract.key();
    let params = Parameters::from(vec![]);
    let mut executor = build_executor(&op_manager).await;
    executor
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(counted_cycles));

    // merge(0, 3) = 1 with the cycling merge, which the probe flags.
    let (stored, merged) = (WrappedState::new(vec![0]), WrappedState::new(vec![1]));
    let incoming = State::from(vec![3u8]);
    let related = UpdateData::RelatedState {
        related_to: ContractInstanceId::new([1; 32]),
        state: State::from(vec![7u8]),
    };
    let _sampling = force_sampling();
    for (shape, batch) in [
        (
            "State + RelatedState",
            vec![UpdateData::State(incoming.clone()), related],
        ),
        (
            "two States",
            vec![
                UpdateData::State(incoming.clone()),
                UpdateData::State(incoming.clone()),
            ],
        ),
        (
            "Delta",
            vec![UpdateData::Delta(StateDelta::from(vec![3u8]))],
        ),
    ] {
        MERGE_CALLS.with(|calls| calls.set(0));
        executor
            .maybe_probe_idempotency(&key, &params, &stored, &merged, &batch)
            .await;
        assert_eq!(
            MERGE_CALLS.with(std::cell::Cell::get),
            0,
            "{shape}: the probe ran"
        );
        assert!(!op_manager.ring.is_contract_broken(&key), "{shape}");
    }

    executor
        .maybe_probe_idempotency(
            &key,
            &params,
            &stored,
            &merged,
            &[UpdateData::State(incoming)],
        )
        .await;
    assert!(
        op_manager.ring.is_contract_broken(&key),
        "the single-State control must be probed and flagged, or this tests nothing"
    );
}

/// `merge(2, 1)` sleeps past the probe's time budget; every other call cycles
/// like [`cycles_on_reapply`]. On `(S, X) = (0, 3)` that is the heal step's first
/// merge, after the probe's clock has started.
fn slow_heal_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a == [2] && b == [1] {
        std::thread::sleep(REAPPLY_PROBE_TIME_BUDGET + Duration::from_millis(50));
    }
    cycles_on_reapply(a, b)
}

/// The node's clock reaches the probe. With `slow_heal_merge` the probe spends
/// its time budget inside the heal step, so through the merge path it is
/// inconclusive and the merge commits. The same inputs on an executor with no
/// node, and so no clock, are decided and flagged.
#[tokio::test(flavor = "current_thread")]
async fn the_probe_is_bounded_by_the_node_clock() {
    let contract = test_contract(b"probe-clock");
    let key = contract.key();
    let params = Parameters::from(vec![]);
    let mut unclocked: Executor<_, _> =
        Executor::new_mock_wasm("t", MockStateStorage::new(), None, None)
            .await
            .expect("build mock-wasm executor");
    unclocked
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), UpdateOverride::MergeWith(slow_heal_merge));
    assert!(
        unclocked
            .reapply_probe_verdict(
                &key,
                &params,
                &WrappedState::new(vec![0]),
                &[3],
                &WrappedState::new(vec![1]),
            )
            .is_some(),
        "without a clock the probe must decide these inputs, or this tests nothing"
    );

    let (result, broken, events, _) = deliver(
        "probe-clock",
        UpdateOverride::MergeWith(slow_heal_merge),
        &[0],
        &[3],
    )
    .await;
    assert!(
        !broken,
        "the probe spent its time budget, so it must not decide"
    );
    assert!(
        matches!(result, UpsertResult::Updated(_)),
        "the merge must commit, got {result:?}"
    );
    assert!(events.is_empty(), "{events:?}");
}

/// `merge(X, X)` sleeps past the probe's time budget; every other call cycles
/// like [`cycles_on_reapply`]. The merge path never makes that call (an
/// incoming state identical to the stored one returns before merging), and
/// neither does the probe.
fn slow_self_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a == b {
        std::thread::sleep(REAPPLY_PROBE_TIME_BUDGET + Duration::from_millis(50));
    }
    cycles_on_reapply(a, b)
}

/// A contract slow only on `merge(X, X)` cannot spend the probe's time budget,
/// because the probe never asks for that merge: the cycling merge is still
/// flagged through the merge path with the node's clock.
#[tokio::test(flavor = "current_thread")]
async fn a_slow_self_merge_does_not_hide_a_cycling_merge() {
    let (result, broken, events, _) = deliver(
        "probe-slow-self-merge",
        UpdateOverride::MergeWith(slow_self_merge),
        &[0],
        &[3],
    )
    .await;
    assert!(broken, "a slow merge(X, X) must not hide a cycling merge");
    assert!(matches!(result, UpsertResult::NoChange), "{result:?}");
    assert_eq!(events.len(), 1, "{events:?}");
}

/// The stored state `[0]` makes the contract ask for related state.
fn needs_related_at_zero(state: &[u8]) -> bool {
    state == [0]
}

/// When the merge needed related state fetched in a second attempt, the merge
/// result depends on that state and the probe's re-apply would not have it, so
/// the probe does not run. The contract cycles once it has a non-zero state, so
/// a probe that ran would flag it.
#[tokio::test(flavor = "current_thread")]
async fn sampled_probe_skips_a_merge_that_needed_related_state() {
    let (op_manager, _guards) = build_op_manager("probe-related-retry").await;
    let mut executor = build_executor(&op_manager).await;

    let related = test_contract(b"probe-related-dependency");
    executor
        .upsert_contract_state(
            related.key(),
            Either::Left(WrappedState::new(vec![42])),
            RelatedContracts::default(),
            Some(related.clone()),
        )
        .await
        .expect("PUT the related contract");

    let contract = test_contract(b"probe-related-retry");
    let key = contract.key();
    executor.mock_runtime_mut().update_overrides.insert(
        *key.id(),
        UpdateOverride::RequiresRelatedWhen {
            related: *related.key().id(),
            needs: needs_related_at_zero,
            merge: cycles_on_reapply,
        },
    );
    executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![0])),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");

    // Control: the probe on the same inputs, called directly, flags.
    let params = Parameters::from(vec![]);
    assert!(
        executor
            .reapply_probe_verdict(
                &key,
                &params,
                &WrappedState::new(vec![0]),
                &[3],
                &WrappedState::new(vec![1]),
            )
            .is_some(),
        "the probe must flag these inputs, or this tests nothing"
    );

    let _sampling = force_sampling();
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(WrappedState::new(vec![3])),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("delivery after fetching related state");
    assert!(
        matches!(result, UpsertResult::Updated(_)),
        "the merge must commit, got {result:?}"
    );
    assert!(
        !op_manager.ring.is_contract_broken(&key),
        "the probe ran on a merge computed with fetched related state"
    );
    assert!(emitted_flag_events().is_empty());
}
