//! Tests for the per-contract non-idempotency *fixture*. See
//! `crate::ring::broken_invariants` (tracker-level unit tests) and
//! `Executor::maybe_probe_idempotency` in `contract/executor/runtime.rs`
//! for the detector itself.
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
//! 3. **No gate on the flag** — `broken_flag_gates_no_commit_broadcast_or_egress`
//!    pins that the flag stays diagnostic (#6190).

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
        delegate_script: Default::default(),
        delegate_calls: Default::default(),
        delegate_contexts: Default::default(),
        delegate_observations: Default::default(),
        capabilities: None,
        delegate_codes: Default::default(),
        unregistered_delegates: Default::default(),
        delegate_wrong_variant: Default::default(),
        delegate_lane: None,
        delegate_pause_after: Default::default(),
        delegate_run_hook: Default::default(),
        registered_params: Default::default(),
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

/// Source pin (#6190): the broken-invariant flag is DIAGNOSTIC ONLY.
///
/// It used to gate the commit, the broadcast and three egress paths, so a
/// flagged contract stopped advancing on this node, including the node's own
/// client UPDATEs, which were answered `state_changed: false` and silently
/// discarded. The harm the gates guarded against (a replay loop from a merge
/// that never settles) is bounded instead by the per-contract merge budget
/// (`ring::contract_merge_budget`), which applies to every contract by usage
/// alone and never touches local client writes.
///
/// Walks every production source file in the crate, so a gate added anywhere
/// fails it, including a direct read of the tracker from inside `ring`. The
/// only reads allowed are `is_contract_broken` itself in `ring.rs` and the two
/// probe prechecks in `executor_impl.rs` that skip re-probing an
/// already-flagged contract (detection cost, not a gate on state).
#[test]
fn broken_flag_gates_no_commit_broadcast_or_egress() {
    use crate::node::{collect_rs_files, strip_cfg_test_regions};
    use std::collections::BTreeMap;

    let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src_root, &mut files);
    assert!(
        files.len() > 100,
        "the source walk found {} files",
        files.len()
    );

    let mut reads: BTreeMap<String, usize> = BTreeMap::new();
    let mut probe_window_hits = 0;
    for path in &files {
        let rel = path
            .strip_prefix(&src_root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        // Whole test files are gated by a parent `#[cfg(test)] mod`, which
        // `strip_cfg_test_regions` cannot see from inside the file.
        if rel.ends_with("tests.rs") || rel.contains("/tests/") || rel.contains("pool_tests/") {
            continue;
        }
        let Ok(src) = std::fs::read_to_string(path) else {
            continue;
        };
        let prod: String = strip_cfg_test_regions(&src)
            .lines()
            .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
            .collect::<Vec<_>>()
            .join("\n");
        // `is_contract_broken(` is the public reader; `.is_broken(` and
        // `.current_ttl(` read the tracker directly, which code in `ring` can do.
        for needle in ["is_contract_broken(", ".is_broken(", ".current_ttl("] {
            let n = prod.matches(needle).count();
            if n > 0 {
                *reads.entry(format!("{rel} {needle}")).or_insert(0) += n;
            }
        }
        if rel == "contract/executor/runtime/executor_impl.rs" {
            for probe in [
                "async fn maybe_probe_idempotency(",
                "fn probe_identical_input_idempotency(",
            ] {
                let start = prod.find(probe).expect("probe function exists");
                let window = &prod[start..(start + 1500).min(prod.len())];
                if window.contains("is_contract_broken(") {
                    probe_window_hits += 1;
                }
            }
        }
    }

    let expected: BTreeMap<String, usize> = [
        // The two probe prechecks.
        (
            "contract/executor/runtime/executor_impl.rs is_contract_broken(".to_string(),
            2,
        ),
        // The definition of `is_contract_broken`, and its body.
        ("ring.rs is_contract_broken(".to_string(), 1),
        ("ring.rs .is_broken(".to_string(), 1),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        reads, expected,
        "production reads of `is_contract_broken(` by file. The flag is \
         diagnostic only (#6190); a read outside the definition and the two \
         probe prechecks is a commit/broadcast/egress gate coming back. Route \
         load protection through `ring::contract_merge_budget` instead."
    );
    assert_eq!(
        probe_window_hits, 2,
        "both probes must keep their already-flagged precheck near their top"
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
        delegate_script: Default::default(),
        delegate_calls: Default::default(),
        delegate_contexts: Default::default(),
        delegate_observations: Default::default(),
        capabilities: None,
        delegate_codes: Default::default(),
        unregistered_delegates: Default::default(),
        delegate_wrong_variant: Default::default(),
        delegate_lane: None,
        delegate_pause_after: Default::default(),
        delegate_run_hook: Default::default(),
        registered_params: Default::default(),
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
