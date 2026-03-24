//! WASM Engine ↔ Mock Conformance Tests
//!
//! Verifies that `MockWasmRuntime`'s `ContractRuntimeInterface` stubs produce
//! the same results as the production `Runtime` executing real WASM.
//!
//! ## How it works
//!
//! The `test-contract-mock-aligned` WASM contract implements the exact same
//! behavior as `MockWasmRuntime`:
//! - `validate_state` → always `Valid`
//! - `update_state` → last `State`/`Delta` from update_data wins
//! - `summarize_state` → blake3 hash of state (32 bytes)
//! - `get_state_delta` → full state as delta (pessimistic)
//!
//! By running both implementations with identical inputs and comparing outputs,
//! we detect drift between the mock stubs and what the WASM engine actually produces.
//!
//! ## When these tests break
//!
//! 1. `MockWasmRuntime` behavior changed → update `test-contract-mock-aligned`
//! 2. `test-contract-mock-aligned` changed → update `MockWasmRuntime`
//! 3. WASM serialization boundary differs from direct Rust calls → investigate
//!
//! ## Feature gate
//!
//! Requires `wasmtime-backend` feature and `wasm32-unknown-unknown` target to
//! compile the test contract. The feature gate is on the `mod` declaration in
//! `pool_tests.rs`.

use freenet_stdlib::prelude::*;

use crate::contract::executor::mock_wasm_runtime::MockWasmRuntime;
use crate::wasm_runtime::{
    ContractRuntimeInterface, ContractStore, DelegateStore, InMemoryContractStore, Runtime,
    SecretsStore,
};

const MOCK_ALIGNED_CONTRACT: &str = "test-contract-mock-aligned";

struct WasmTestSetup {
    runtime: Runtime,
    contract_key: ContractKey,
    // hold TempDir so it's not deleted while tests run
    _temp_dir: tempfile::TempDir,
}

/// Compile the mock-aligned test contract to WASM and build a production Runtime.
///
/// Uses the shared `test_utils::load_contract` helper which properly checks the
/// cargo build exit status and streams compiler output.
async fn setup_wasm_runtime() -> Result<WasmTestSetup, Box<dyn std::error::Error>> {
    let contract = tokio::task::spawn_blocking(|| {
        crate::test_utils::load_contract(MOCK_ALIGNED_CONTRACT, Parameters::from(vec![]))
    })
    .await??;

    let contract_key = contract.key();

    let temp_dir = crate::util::tests::get_temp_dir();
    let db = crate::contract::storages::Storage::new(temp_dir.path()).await?;
    let mut contract_store =
        ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())?;
    let secrets_store = SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db)?;

    contract_store.store_contract(contract)?;
    let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)?;

    Ok(WasmTestSetup {
        runtime,
        contract_key,
        _temp_dir: temp_dir,
    })
}

/// Create a MockWasmRuntime (no WASM engine needed).
fn setup_mock() -> MockWasmRuntime {
    MockWasmRuntime {
        contract_store: InMemoryContractStore::new(),
    }
}

// =========================================================================
// validate_state: both should return Valid for any state
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_validate_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    // Include a 100KB state to exercise the streaming refill path (STREAMING_BUF_CAP = 64KB)
    let large_state: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
    let test_states = [
        WrappedState::new(vec![]),
        WrappedState::new(vec![1, 2, 3]),
        WrappedState::new(vec![0; 1024]),
        WrappedState::new(b"hello world".to_vec()),
        WrappedState::new(large_state),
    ];
    let params = Parameters::from(vec![]);
    let related = RelatedContracts::default();

    for (i, state) in test_states.iter().enumerate() {
        let wasm_result =
            wasm.runtime
                .validate_state(&wasm.contract_key, &params, state, &related)?;
        let mock_result = mock.validate_state(&wasm.contract_key, &params, state, &related)?;

        assert_eq!(
            wasm_result, mock_result,
            "validate_state divergence on case {i}: wasm={wasm_result:?}, mock={mock_result:?}"
        );
        assert_eq!(wasm_result, ValidateResult::Valid);
    }

    Ok(())
}

// =========================================================================
// update_state with State: both should accept the incoming state
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_update_state_full() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let current = WrappedState::new(vec![10, 20, 30]);
    let incoming = WrappedState::new(vec![40, 50, 60]);
    let params = Parameters::from(vec![]);
    let updates = vec![UpdateData::State(incoming.clone().into())];

    let wasm_result = wasm
        .runtime
        .update_state(&wasm.contract_key, &params, &current, &updates)?;
    let mock_result = mock.update_state(&wasm.contract_key, &params, &current, &updates)?;

    assert_eq!(
        wasm_result.unwrap_valid().as_ref(),
        mock_result.unwrap_valid().as_ref(),
        "update_state(State) must produce identical output"
    );
    Ok(())
}

// =========================================================================
// update_state with Delta: both should use delta bytes as new state
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_update_state_delta() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let current = WrappedState::new(vec![1, 2, 3]);
    let delta = StateDelta::from(vec![7, 8, 9]);
    let params = Parameters::from(vec![]);
    let updates = vec![UpdateData::Delta(delta.clone())];

    let wasm_result = wasm
        .runtime
        .update_state(&wasm.contract_key, &params, &current, &updates)?;
    let mock_result = mock.update_state(&wasm.contract_key, &params, &current, &updates)?;

    let wasm_state = wasm_result.unwrap_valid();
    let mock_state = mock_result.unwrap_valid();

    assert_eq!(
        wasm_state.as_ref(),
        mock_state.as_ref(),
        "update_state(Delta) must produce identical output"
    );
    assert_eq!(wasm_state.as_ref(), delta.as_ref());
    Ok(())
}

// =========================================================================
// update_state with multiple updates: last one wins
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_update_state_last_wins() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let current = WrappedState::new(vec![0]);
    let params = Parameters::from(vec![]);
    let updates = vec![
        UpdateData::State(WrappedState::new(vec![1, 1]).into()),
        UpdateData::Delta(StateDelta::from(vec![2, 2])),
        UpdateData::State(WrappedState::new(vec![3, 3, 3]).into()),
    ];

    let wasm_result = wasm
        .runtime
        .update_state(&wasm.contract_key, &params, &current, &updates)?;
    let mock_result = mock.update_state(&wasm.contract_key, &params, &current, &updates)?;

    let wasm_state = wasm_result.unwrap_valid();
    let mock_state = mock_result.unwrap_valid();

    assert_eq!(wasm_state.as_ref(), mock_state.as_ref());
    assert_eq!(wasm_state.as_ref(), &[3, 3, 3], "Last State should win");
    Ok(())
}

// =========================================================================
// update_state with no updates: returns current state
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_update_state_empty() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let current = WrappedState::new(vec![42, 43]);
    let params = Parameters::from(vec![]);
    let updates: Vec<UpdateData<'_>> = vec![];

    let wasm_result = wasm
        .runtime
        .update_state(&wasm.contract_key, &params, &current, &updates)?;
    let mock_result = mock.update_state(&wasm.contract_key, &params, &current, &updates)?;

    assert_eq!(
        wasm_result.unwrap_valid().as_ref(),
        mock_result.unwrap_valid().as_ref(),
    );
    Ok(())
}

// =========================================================================
// summarize_state: both should return blake3 hash
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_summarize_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let test_states = [
        WrappedState::new(vec![1, 2, 3, 4, 5]),
        WrappedState::new(vec![0; 100]),
        WrappedState::new(b"conformance test data".to_vec()),
    ];
    let params = Parameters::from(vec![]);

    for (i, state) in test_states.iter().enumerate() {
        let wasm_summary = wasm
            .runtime
            .summarize_state(&wasm.contract_key, &params, state)?;
        let mock_summary = mock.summarize_state(&wasm.contract_key, &params, state)?;

        assert_eq!(
            wasm_summary.as_ref(),
            mock_summary.as_ref(),
            "summarize_state divergence on case {i}"
        );

        // Verify it's the expected blake3 hash
        let expected = blake3::hash(state.as_ref());
        assert_eq!(wasm_summary.as_ref(), expected.as_bytes());
    }

    Ok(())
}

// =========================================================================
// get_state_delta: both should return full state as delta
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_get_state_delta() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let state = WrappedState::new(vec![10, 20, 30, 40, 50]);
    let summary = StateSummary::from(vec![0; 32]);
    let params = Parameters::from(vec![]);

    let wasm_delta = wasm
        .runtime
        .get_state_delta(&wasm.contract_key, &params, &state, &summary)?;
    let mock_delta = mock.get_state_delta(&wasm.contract_key, &params, &state, &summary)?;

    assert_eq!(
        wasm_delta.as_ref(),
        mock_delta.as_ref(),
        "get_state_delta must produce identical output"
    );
    assert_eq!(wasm_delta.as_ref(), state.as_ref());
    Ok(())
}

// =========================================================================
// Round-trip: summarize → get_delta → update produces consistent results
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let state = WrappedState::new(vec![100, 200, 150]);
    let params = Parameters::from(vec![]);

    // Step 1: Summarize on both
    let wasm_summary = wasm
        .runtime
        .summarize_state(&wasm.contract_key, &params, &state)?;
    let mock_summary = mock.summarize_state(&wasm.contract_key, &params, &state)?;
    assert_eq!(wasm_summary.as_ref(), mock_summary.as_ref());

    // Step 2: Get delta from summary on both
    let wasm_delta =
        wasm.runtime
            .get_state_delta(&wasm.contract_key, &params, &state, &wasm_summary)?;
    let mock_delta = mock.get_state_delta(&wasm.contract_key, &params, &state, &mock_summary)?;
    assert_eq!(wasm_delta.as_ref(), mock_delta.as_ref());

    // Step 3: Apply delta as update on both
    let old = WrappedState::new(vec![0]);
    let wasm_updated = wasm.runtime.update_state(
        &wasm.contract_key,
        &params,
        &old,
        &[UpdateData::Delta(wasm_delta)],
    )?;
    let mock_updated = mock.update_state(
        &wasm.contract_key,
        &params,
        &old,
        &[UpdateData::Delta(mock_delta)],
    )?;

    let wasm_state = wasm_updated.unwrap_valid();
    let mock_state = mock_updated.unwrap_valid();

    assert_eq!(
        wasm_state.as_ref(),
        mock_state.as_ref(),
        "Round-trip must produce same state"
    );

    // Semantic check: applying the delta to any base state must recover the
    // original value, since get_state_delta returns the full state as the delta.
    assert_eq!(
        wasm_state.as_ref(),
        state.as_ref(),
        "Round-trip delta application must recover the original state"
    );
    Ok(())
}

// =========================================================================
// StateAndDelta: state field wins (matching mock)
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_state_and_delta() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let current = WrappedState::new(vec![0]);
    let params = Parameters::from(vec![]);
    let state_part = WrappedState::new(vec![11, 22, 33]);
    let delta_part = StateDelta::from(vec![44, 55]);

    let updates = vec![UpdateData::StateAndDelta {
        state: state_part.clone().into(),
        delta: delta_part,
    }];

    let wasm_result = wasm
        .runtime
        .update_state(&wasm.contract_key, &params, &current, &updates)?;
    let mock_result = mock.update_state(&wasm.contract_key, &params, &current, &updates)?;

    let wasm_state = wasm_result.unwrap_valid();
    let mock_state = mock_result.unwrap_valid();
    assert_eq!(wasm_state.as_ref(), mock_state.as_ref());
    assert_eq!(
        wasm_state.as_ref(),
        state_part.as_ref(),
        "StateAndDelta must use the State part"
    );
    Ok(())
}

// =========================================================================
// Empty state: edge case handled consistently
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_empty_state() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    let empty = WrappedState::new(vec![]);
    let params = Parameters::from(vec![]);

    // validate
    let w =
        wasm.runtime
            .validate_state(&wasm.contract_key, &params, &empty, &Default::default())?;
    let m = mock.validate_state(&wasm.contract_key, &params, &empty, &Default::default())?;
    assert_eq!(w, m);

    // summarize
    let ws = wasm
        .runtime
        .summarize_state(&wasm.contract_key, &params, &empty)?;
    let ms = mock.summarize_state(&wasm.contract_key, &params, &empty)?;
    assert_eq!(ws.as_ref(), ms.as_ref());

    // get_state_delta
    let summary = StateSummary::from(vec![]);
    let wd = wasm
        .runtime
        .get_state_delta(&wasm.contract_key, &params, &empty, &summary)?;
    let md = mock.get_state_delta(&wasm.contract_key, &params, &empty, &summary)?;
    assert_eq!(wd.as_ref(), md.as_ref());
    assert!(wd.as_ref().is_empty());

    Ok(())
}

// =========================================================================
// Large state (>64KB): exercises the streaming refill path
// =========================================================================

#[tokio::test(flavor = "multi_thread")]
async fn wasm_conformance_large_state_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let mut wasm = setup_wasm_runtime().await?;
    let mut mock = setup_mock();

    // 100KB state — exceeds the 64KB STREAMING_BUF_CAP, forcing at least one
    // host refill callback during contract execution.
    let large: Vec<u8> = (0u32..100_000).map(|i| (i % 251) as u8).collect();
    let state = WrappedState::new(large);
    let params = Parameters::from(vec![]);

    // validate_state with large state
    let wasm_valid =
        wasm.runtime
            .validate_state(&wasm.contract_key, &params, &state, &Default::default())?;
    let mock_valid =
        mock.validate_state(&wasm.contract_key, &params, &state, &Default::default())?;
    assert_eq!(wasm_valid, mock_valid, "validate diverged on large state");

    // summarize_state with large state
    let wasm_summary = wasm
        .runtime
        .summarize_state(&wasm.contract_key, &params, &state)?;
    let mock_summary = mock.summarize_state(&wasm.contract_key, &params, &state)?;
    assert_eq!(
        wasm_summary.as_ref(),
        mock_summary.as_ref(),
        "summarize diverged on large state"
    );

    // get_state_delta with large state
    let wasm_delta =
        wasm.runtime
            .get_state_delta(&wasm.contract_key, &params, &state, &wasm_summary)?;
    let mock_delta = mock.get_state_delta(&wasm.contract_key, &params, &state, &mock_summary)?;
    assert_eq!(
        wasm_delta.as_ref(),
        mock_delta.as_ref(),
        "get_state_delta diverged on large state"
    );

    // update_state with large delta
    let old = WrappedState::new(vec![0]);
    let wasm_updated = wasm.runtime.update_state(
        &wasm.contract_key,
        &params,
        &old,
        &[UpdateData::Delta(wasm_delta)],
    )?;
    let mock_updated = mock.update_state(
        &wasm.contract_key,
        &params,
        &old,
        &[UpdateData::Delta(mock_delta)],
    )?;

    assert_eq!(
        wasm_updated.unwrap_valid().as_ref(),
        mock_updated.unwrap_valid().as_ref(),
        "update diverged on large state"
    );

    Ok(())
}
