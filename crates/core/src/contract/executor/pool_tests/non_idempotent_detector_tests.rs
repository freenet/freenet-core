//! Tests for the per-contract non-idempotency *fixture*. See
//! `crate::ring::broken_invariants` (tracker-level unit tests) and
//! `Executor::maybe_probe_idempotency` in `contract/executor/runtime.rs`
//! for the detector + gate code itself.
//!
//! ## What these tests cover
//!
//! 1. **Fixture self-check** — the `UpdateOverride::NonIdempotent` mock
//!    actually models the smoking-gun behavior the detector is built to
//!    catch (`update_state(update_state(S, U), U) != update_state(S, U)`).
//!    A regression that flattens the fixture into a no-op would silently
//!    pass the production detector while testing nothing.
//!
//! 2. **Healthy baseline** — the default mock IS idempotent on re-apply,
//!    so the property the probe checks doesn't false-positive on
//!    well-behaved contracts.
//!
//! ## What these tests do NOT cover (deferred)
//!
//! End-to-end "probe fires inside the executor, sets the flag via
//! `Ring::record_broken_invariant`, and the next merge / broadcast is
//! suppressed by the `is_contract_broken` gate in `commit_state_update`
//! and `broadcast_state_change`" is **NOT** exercised here — the mock
//! executor path (`Executor::new_mock_wasm`) doesn't wire a real
//! `OpManager`, so `op_manager.ring.is_contract_broken(...)` from inside
//! the executor is a no-op against an absent ring. The tracker layer
//! (record → is_broken) is covered in `ring::broken_invariants::tests`.
//! Full integration coverage requires a `SimNetwork`-shaped harness and
//! is filed as a follow-up to #4279.

use freenet_stdlib::prelude::*;

use crate::contract::executor::mock_wasm_runtime::{MockWasmRuntime, UpdateOverride};
use crate::wasm_runtime::{ContractRuntimeInterface, InMemoryContractStore};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

fn fake_key() -> ContractKey {
    // The override is keyed on instance id only; the code hash is
    // incidental for these tests. Using `from_id_and_code` to construct
    // a key without round-tripping through a full contract.
    let instance = ContractInstanceId::new([7u8; 32]);
    let code_hash = CodeHash::from(&[7u8; 32]);
    ContractKey::from_id_and_code(instance, code_hash)
}

fn make_mock_runtime(override_: UpdateOverride) -> MockWasmRuntime {
    let mut overrides = HashMap::new();
    let key = fake_key();
    overrides.insert(*key.id(), override_);
    MockWasmRuntime {
        contract_store: InMemoryContractStore::default(),
        validate_overrides: HashMap::new(),
        update_overrides: overrides,
    }
}

/// The fixture must produce different bytes for two consecutive
/// `update_state` calls with the same input — this is the *contract bug*
/// shape the in-peer detector exists to flag. If this assertion fails,
/// the mock has been flattened and detector tests downstream would pass
/// vacuously.
#[test]
fn fixture_non_idempotent_override_produces_distinct_states() {
    let counter = Arc::new(AtomicU64::new(0));
    let mut rt = make_mock_runtime(UpdateOverride::NonIdempotent(counter.clone()));
    let key = fake_key();
    let params = Parameters::from(vec![]);
    let initial = WrappedState::new(vec![0u8; 32]);
    let update = vec![UpdateData::State(initial.clone().into())];

    let first = rt
        .update_state(&key, &params, &initial, &update)
        .expect("first update_state");
    let s1 = WrappedState::new(first.new_state.expect("state").into_bytes());

    // Re-apply the same update with `s1` as the current state. A correct
    // CRDT must yield `s1` again. Our fixture intentionally yields a
    // byte-different state — the smoking-gun shape the detector catches.
    let update2 = vec![UpdateData::State(s1.clone().into())];
    let second = rt
        .update_state(&key, &params, &s1, &update2)
        .expect("second update_state");
    let s2 = WrappedState::new(second.new_state.expect("state").into_bytes());

    assert_ne!(
        s1.as_ref(),
        s2.as_ref(),
        "NonIdempotent fixture must produce byte-different states on re-apply; \
         if this fails, the mock has been flattened and any test that relies on \
         it to exercise the detector is testing nothing."
    );
    assert_eq!(
        s1.as_ref().len(),
        s2.as_ref().len(),
        "Both outputs should stay the same size (fixed-shape state, \
         matching the 464-byte production observation)."
    );
}

/// Source-grep pin: the three production gate sites in `runtime.rs`
/// (`commit_state_update` top-gate, `broadcast_state_change` gate, and
/// `bridged_upsert_contract_state` merge-suppression after probe) all
/// MUST consult `op_manager.ring.is_contract_broken(...)`. A regression
/// that removes or inverts any of these checks silently re-enables the
/// broadcast storm — this test would fail compile-time-style on string
/// match in CI before the gap reaches production.
#[test]
fn production_gate_sites_consult_is_contract_broken() {
    // Read the full source files at compile time. `include_str!` is
    // resolved relative to the file containing this macro — these tests
    // live at `crates/core/src/contract/executor/pool_tests/` so we
    // climb one level to reach the production files.
    // `is_contract_broken` call sites live in runtime/executor_impl.rs after
    // the split; both files are searched independently (not concatenated) so
    // that `#[cfg(test)]` markers in runtime.rs don't accidentally truncate
    // the executor_impl.rs search window.
    const RUNTIME_RS: &str = include_str!("../runtime.rs");
    const EXECUTOR_IMPL_RS: &str = include_str!("../runtime/executor_impl.rs");

    // Strip test modules from each file independently, then concatenate the
    // production-only slices. executor_impl.rs has no #[cfg(test)] today so
    // its split_once is a no-op, but future-proofs against one being added.
    let runtime_prod = RUNTIME_RS
        .split_once("#[cfg(test)]")
        .map(|(prod, _)| prod)
        .unwrap_or(RUNTIME_RS);
    let executor_impl_prod = EXECUTOR_IMPL_RS
        .split_once("#[cfg(test)]")
        .map(|(prod, _)| prod)
        .unwrap_or(EXECUTOR_IMPL_RS);

    let occurrences = runtime_prod.matches("is_contract_broken").count()
        + executor_impl_prod.matches("is_contract_broken").count();
    assert!(
        occurrences >= 3,
        "expected at least 3 `is_contract_broken` checks in production \
         runtime.rs (commit_state_update top-gate, broadcast_state_change \
         gate, and bridged_upsert_contract_state post-probe merge \
         suppression); found {occurrences}. A regression that removed or \
         inverted any of these silently re-enables the storm we're \
         defending against — see PR #4279."
    );
}

/// Source-grep pin for the full-state EGRESS gates (durable-quarantine
/// PR): while a contract is flagged, the executor's commit/broadcast
/// suppression is not enough — two paths still pushed full state off the
/// node and kept the broadcast echo alive network-wide:
///
/// 1. `InterestMessage::ResyncRequest` handling in node.rs served a full
///    `ResyncResponse`.
/// 2. The Summaries-mismatch heal (`handle_sync_state_to_peer` in
///    p2p_protoc/broadcast.rs) pushed full state to a "stale" peer.
///
/// Both must consult `is_contract_broken` BEFORE any state leaves the
/// node. These are pinned by source scrape because driving the interest-
/// sync handler / p2p bridge in a unit test requires the full node
/// harness (the flag predicate itself is behavior-tested in
/// `ring::broken_invariants::tests` and `identical_input_probe_tests`).
#[test]
fn egress_paths_gated_on_is_contract_broken() {
    const NODE_RS: &str = include_str!("../../../node.rs");
    const BROADCAST_RS: &str = include_str!("../../../node/network_bridge/p2p_protoc/broadcast.rs");

    // --- ResyncRequest arm: gate must precede the state fetch/serve. ---
    let arm_start = NODE_RS
        .find("InterestMessage::ResyncRequest { key } =>")
        .expect("node.rs must still have the ResyncRequest handler arm");
    let arm = &NODE_RS[arm_start..];
    let gate = arm
        .find("is_contract_broken")
        .expect("ResyncRequest arm must gate on is_contract_broken (broken-contract egress)");
    let serve = arm
        .find("get_contract_state(op_manager, &key)")
        .expect("ResyncRequest arm must still fetch state to serve");
    assert!(
        gate < serve,
        "the is_contract_broken gate (offset {gate}) must run BEFORE the \
         ResyncResponse state fetch (offset {serve}) so a flagged node never \
         serves full state"
    );

    // --- SyncStateToPeer heal: gate must precede the enqueue/send. ---
    let fn_start = BROADCAST_RS
        .find("fn handle_sync_state_to_peer")
        .expect("broadcast.rs must still have handle_sync_state_to_peer");
    let body = &BROADCAST_RS[fn_start..];
    let heal_gate = body
        .find("is_contract_broken")
        .expect("handle_sync_state_to_peer must gate on is_contract_broken");
    let heal_send = body
        .find("broadcast_queue.enqueue")
        .expect("handle_sync_state_to_peer must still enqueue the heal send");
    assert!(
        heal_gate < heal_send,
        "the is_contract_broken gate (offset {heal_gate}) must run BEFORE the \
         SyncStateToPeer enqueue (offset {heal_send}) so a flagged node never \
         heals peers with the problematic state"
    );
}

/// Source-grep pin for the durable-quarantine wiring in ring.rs:
/// `record_broken_invariant` must feed the escalation ledger (which bans
/// at threshold), and storage hydration must re-arm bans for persisted
/// offenders — the ban list itself is in-memory only, so dropping either
/// call silently reverts the quarantine to the old dampen-only behavior.
#[test]
fn ring_wires_escalation_and_restart_reban() {
    // NOTE: no `split_once("#[cfg(test)]")` truncation here — ring.rs has
    // an early cfg(test) item (line ~125) long before the wiring under
    // pin, so the truncated slice would miss it. Whole-file matching is
    // sound for these strings: they only exist at the wiring sites.
    const RING_RS: &str = include_str!("../../../ring.rs");
    assert!(
        RING_RS.contains("record_and_escalate("),
        "Ring::record_broken_invariant must route through the tracker's \
         record_and_escalate so repeated detections reach the ban list"
    );
    assert!(
        RING_RS.contains("reban_offenders("),
        "Ring::set_broken_invariants_storage must re-arm bans from the \
         persisted escalation ledger after hydration (restart durability)"
    );
}

/// Source-grep pin: the inbound wire-dispatch gates that make a ban
/// actually DROP a contract's traffic. The escalation path's whole value
/// rests on these existing sites (PUT / GET / UPDATE / SUBSCRIBE arms in
/// node.rs) continuing to consult the ban list — the ban-list predicate
/// itself is behavior-tested in `ring::contract_ban_list` and
/// `ring::broken_invariants::tests::sustained_reflagging_escalates_to_durable_ban`.
#[test]
fn dispatch_sites_consult_contract_ban_list() {
    // Whole-file matching (node.rs has an early cfg(test) item, so a
    // split-at-first-cfg(test) slice would truncate before the dispatch
    // arms). The matched string is receiver-qualified
    // (`contract_ban_list.is_banned`), which test code does not use.
    const NODE_RS: &str = include_str!("../../../node.rs");
    let count = NODE_RS.matches("contract_ban_list.is_banned").count();
    assert!(
        count >= 4,
        "expected at least 4 contract_ban_list.is_banned dispatch gates in \
         node.rs (PUT/GET/UPDATE/SUBSCRIBE inbound arms); found {count}. A \
         banned (quarantined) contract's requests must be dropped at the \
         receive boundary"
    );
}

/// A healthy fixture — `UpdateOverride` unwired — must produce byte-equal
/// state on re-apply. This is the "no false positive" pin for the probe:
/// a contract whose `update_state` is correctly idempotent must NOT be
/// flagged.
#[test]
fn healthy_mock_is_idempotent_on_reapply() {
    let mut rt = MockWasmRuntime {
        contract_store: InMemoryContractStore::default(),
        validate_overrides: HashMap::new(),
        update_overrides: HashMap::new(),
    };
    let key = fake_key();
    let params = Parameters::from(vec![]);
    let initial = WrappedState::new(vec![10u8; 16]);
    let update = vec![UpdateData::State(initial.clone().into())];

    let first = rt
        .update_state(&key, &params, &initial, &update)
        .expect("first update_state");
    let s1 = WrappedState::new(first.new_state.expect("state").into_bytes());

    let update2 = vec![UpdateData::State(s1.clone().into())];
    let second = rt
        .update_state(&key, &params, &s1, &update2)
        .expect("second update_state");
    let s2 = WrappedState::new(second.new_state.expect("state").into_bytes());

    assert_eq!(
        s1.as_ref(),
        s2.as_ref(),
        "Default mock merge must be idempotent on re-apply; otherwise the \
         healthy-baseline assumption of the probe is broken."
    );
}
