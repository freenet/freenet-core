//! End-to-end tests for the DETERMINISTIC identical-input idempotency
//! probe (`Executor::probe_identical_input_idempotency`) and its wiring
//! into the real bridged upsert path with a real `Ring`.
//!
//! Background (production storm root cause): a non-idempotent contract —
//! one that mutates its state on every `update_state` apply, producing a
//! self-sustaining broadcast echo — was detected only by the SAMPLED
//! re-apply probe (`IDEMPOTENCY_PROBE_PROBABILITY` = 1/32, TTL-bounded,
//! per-node). Across many co-hosts somebody was always unflagged, so the
//! echo survived. The identical-input probe closes the detection half:
//! when an incoming full-`State` payload is byte-identical to the stored
//! state, `update_state(S, State(S))` must reach a FIXPOINT by CvRDT
//! lattice semantics, so re-applying (cooldown-bounded, up to
//! `IDENTITY_PROBE_MAX_APPLIES` times) and seeing the byte MULTISET
//! change on EVERY step is deterministic proof — the FIRST identical
//! re-push flags the contract, no sampling. A contract that
//! canonicalizes a raw state once and then stabilizes is NOT flagged
//! (the F3 false-positive fix).
//!
//! These tests drive `upsert_contract_state` through the production
//! `bridged_upsert_contract_state` path via `Executor<MockWasmRuntime>`
//! wired to a real `OpManager`/`Ring` (harness mirrors
//! `disk_budget_gate_tests.rs`), so the `is_contract_broken` flag the
//! probes set — and the commit suppression it gates — are the real ones.
//!
//! The probe cooldown is unit-tested with a mock clock in
//! `ring::broken_invariants::tests`; the egress gates (ResyncResponse /
//! SyncStateToPeer / handle_broadcast_state_change) are pinned in
//! `non_idempotent_detector_tests.rs`.

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::config::ConfigArgs;
use crate::contract::UpsertResult;
use crate::contract::executor::mock_wasm_runtime::UpdateOverride;
use crate::contract::executor::{ContractExecutor, Executor, OperationMode};
use crate::node::OpManager;
use crate::wasm_runtime::MockStateStorage;

use super::super::mock_runtime::test::create_test_contract as test_contract;

/// Build a real `OpManager` backed by a temp-dir `Config`, mirroring the
/// wiring in `disk_budget_gate_tests.rs::build_op_manager`.
async fn build_op_manager(id: &str) -> (Arc<OpManager>, Box<dyn std::any::Any>) {
    let config_args = ConfigArgs {
        id: Some(id.to_string()),
        mode: Some(OperationMode::Local),
        ..Default::default()
    };
    let node_config =
        crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
            .await
            .expect("build NodeConfig");

    let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
    let (ops_ch_channel, ch_channel, wait_for_event) = crate::contract::contract_handler_channel();
    let connection_manager = crate::ring::ConnectionManager::new(&node_config);
    let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
    let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();

    let op_manager = Arc::new(
        OpManager::new(
            notification_tx,
            ops_ch_channel,
            &node_config,
            crate::tracing::DynamicRegister::new(vec![]),
            connection_manager,
            result_router_tx,
            &task_monitor,
        )
        .expect("build OpManager"),
    );
    op_manager.ring.attach_op_manager(&op_manager);

    let guards: Box<dyn std::any::Any> = Box::new((
        notification_rx,
        ch_channel,
        wait_for_event,
        result_router_rx,
        task_monitor,
    ));
    (op_manager, guards)
}

/// A state with enough distinct byte values that a rotation (ReorderBytes)
/// and a counter-prefix mutation (NonIdempotent) both produce
/// byte-different output — and the multiset comparison can tell them apart.
fn distinct_bytes_state() -> WrappedState {
    WrappedState::new((1u8..=32).collect::<Vec<u8>>())
}

async fn build_executor(
    op_manager: &Arc<OpManager>,
) -> Executor<crate::contract::executor::mock_wasm_runtime::MockWasmRuntime, MockStateStorage> {
    Executor::new_mock_wasm("t", MockStateStorage::new(), None, Some(op_manager.clone()))
        .await
        .expect("build mock-wasm executor")
}

/// The storm-shaped contract (mutates on every apply, the
/// `UpdateOverride::NonIdempotent` fixture) is flagged on the FIRST
/// byte-identical re-apply — deterministically, not at 1/32 — and the
/// apply itself reports `NoChange` (nothing for the network to observe).
#[tokio::test(flavor = "current_thread")]
async fn identical_input_reapply_flags_non_idempotent_contract_on_first_apply() {
    let (op_manager, _guards) = build_op_manager("nonidem-identity-flag").await;
    let contract = test_contract(b"nonidem_identity_contract");
    let key = contract.key();
    let state = distinct_bytes_state();

    let mut executor = build_executor(&op_manager).await;
    let counter = Arc::new(AtomicU64::new(0));
    executor
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), UpdateOverride::NonIdempotent(counter));

    // Initial PUT installs the state directly (new contract, no merge).
    executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");
    assert!(
        !op_manager.ring.is_contract_broken(&key),
        "no flag before any identical re-apply — the install path proves nothing"
    );

    // FIRST identical re-apply: the probe re-merges S into itself, the
    // fixture mutates (different byte multiset) → flagged. Deterministic:
    // this assertion would only pass ~1/32 of runs under the sampled probe.
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("identical re-apply");
    assert!(
        matches!(result, UpsertResult::NoChange),
        "identical re-apply must report NoChange, got {result:?}"
    );
    assert!(
        op_manager.ring.is_contract_broken(&key),
        "identity probe must flag the non-idempotent contract on the FIRST \
         identical apply (no 1/32 sampling)"
    );
}

/// Healthy baseline: a correctly-idempotent contract (default mock merge)
/// is NOT flagged by the identical-input probe — the #4151 fast path keeps
/// returning `NoChange` and the contract keeps propagating.
#[tokio::test(flavor = "current_thread")]
async fn identical_input_reapply_does_not_flag_healthy_contract() {
    let (op_manager, _guards) = build_op_manager("nonidem-identity-healthy").await;
    let contract = test_contract(b"healthy_identity_contract");
    let key = contract.key();
    let state = distinct_bytes_state();

    let mut executor = build_executor(&op_manager).await;

    executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("identical re-apply");
    assert!(matches!(result, UpsertResult::NoChange));
    assert!(
        !op_manager.ring.is_contract_broken(&key),
        "a correctly-idempotent contract must NOT be flagged by the identity probe"
    );
}

/// The #4295 reorder exemption: a contract whose re-merge emits the SAME
/// byte multiset in a different order (non-canonical serialization, the
/// `ReorderBytes` fixture) is benign flutter — NOT flagged.
#[tokio::test(flavor = "current_thread")]
async fn identical_input_reorder_only_is_not_flagged() {
    let (op_manager, _guards) = build_op_manager("nonidem-identity-reorder").await;
    let contract = test_contract(b"reorder_identity_contract");
    let key = contract.key();
    let state = distinct_bytes_state();

    let mut executor = build_executor(&op_manager).await;
    executor
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), UpdateOverride::ReorderBytes);

    executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");

    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("identical re-apply");
    assert!(matches!(result, UpsertResult::NoChange));
    assert!(
        !op_manager.ring.is_contract_broken(&key),
        "a reordering-only re-merge (same byte multiset) is the #4295 \
         false-positive shape and must NOT be flagged"
    );
}

/// The F3 canonicalization exemption: a correct contract whose
/// `update_state` normalizes a raw (e.g. fresh-PUT-installed) state ONCE
/// — a genuine multiset change — and then stabilizes at a fixpoint must
/// NOT be flagged. Only a contract whose output NEVER stabilizes across
/// the probe's re-apply sequence is a violation.
#[tokio::test(flavor = "current_thread")]
async fn identical_input_canonicalize_once_is_not_flagged() {
    let (op_manager, _guards) = build_op_manager("nonidem-identity-canonicalize").await;
    let contract = test_contract(b"canonicalize_identity_contract");
    let key = contract.key();
    // Raw, non-canonical state: leading 0xFF markers the contract strips
    // on its first merge (a real content/multiset change), stable after.
    let mut raw = vec![0xFFu8, 0xFF];
    raw.extend(1u8..=16);
    let state = WrappedState::new(raw);

    let mut executor = build_executor(&op_manager).await;
    executor
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), UpdateOverride::CanonicalizeOnce);

    // Initial PUT installs the RAW state directly (install path does not
    // run update_state — exactly the scenario that makes the first
    // re-apply a content change).
    executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");

    // Identical re-push: the probe sees one canonicalization step, then a
    // fixpoint — benign, must NOT flag.
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("identical re-apply");
    assert!(matches!(result, UpsertResult::NoChange));
    assert!(
        !op_manager.ring.is_contract_broken(&key),
        "a canonicalize-once-then-stable contract must NOT be flagged by the \
         identity probe (F3 false-positive fix)"
    );
}

/// While flagged, a subsequent NON-identical apply is fully suppressed:
/// the merge result is neither committed nor observable — the stored
/// state stays put and the caller sees `NoChange`. (This is the existing
/// #4279 commit gate; asserting it here proves the flag set by the NEW
/// identity probe engages it end-to-end.)
#[tokio::test(flavor = "current_thread")]
async fn flagged_contract_subsequent_apply_is_suppressed() {
    let (op_manager, _guards) = build_op_manager("nonidem-identity-suppress").await;
    let contract = test_contract(b"suppressed_identity_contract");
    let key = contract.key();
    let state = distinct_bytes_state();

    let mut executor = build_executor(&op_manager).await;
    let counter = Arc::new(AtomicU64::new(0));
    executor
        .mock_runtime_mut()
        .update_overrides
        .insert(*key.id(), UpdateOverride::NonIdempotent(counter));

    executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial PUT");

    // Flag via the identity probe.
    executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("identical re-apply");
    assert!(op_manager.ring.is_contract_broken(&key));

    // A different incoming state now runs the merge, but the flagged-
    // contract gate suppresses the commit: NoChange, stored state intact.
    let different = WrappedState::new((101u8..=132).collect::<Vec<u8>>());
    let result = executor
        .upsert_contract_state(
            key,
            Either::Left(different),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("non-identical apply while flagged");
    assert!(
        matches!(result, UpsertResult::NoChange),
        "apply while flagged must be suppressed to NoChange, got {result:?}"
    );

    let (stored, _) = executor
        .fetch_contract(key, false)
        .await
        .expect("fetch after suppressed apply");
    assert_eq!(
        stored.map(|s| s.as_ref().to_vec()),
        Some(state.as_ref().to_vec()),
        "suppressed apply must not have touched the stored state"
    );
}
