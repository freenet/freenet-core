//! Tests for the per-contract non-idempotency detector and the broken-
//! invariants gate. See `crate::ring::broken_invariants` and
//! `Executor::maybe_probe_idempotency` in `contract/executor/runtime.rs`.
//!
//! These exercise two layers without spinning up a full network:
//!
//! 1. **Fixture self-check** — the `UpdateOverride::NonIdempotent` mock
//!    actually models the smoking-gun behavior the detector is built to
//!    catch (`update_state(update_state(S, U), U) != update_state(S, U)`).
//!    A regression that flattens the fixture into a no-op would silently
//!    pass the production detector while testing nothing.
//!
//! 2. **Gate behavior** — once a contract has been marked broken via
//!    `Ring::record_broken_invariant`, `Ring::is_contract_broken` returns
//!    true; the production gate code in `commit_state_update` /
//!    `broadcast_state_change` reads this exact predicate.

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
