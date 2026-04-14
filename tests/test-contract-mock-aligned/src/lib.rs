//! Test contract whose behavior exactly mirrors `MockWasmRuntime`'s
//! `ContractRuntimeInterface` stubs.
//!
//! Used by conformance tests to verify that running the production WASM engine
//! with this contract produces the same results as `MockWasmRuntime`. If these
//! tests break, either this contract or `MockWasmRuntime` was changed and the
//! other needs to be updated to match.
//!
//! ## Behavioral specification (must match MockWasmRuntime)
//!
//! - `validate_state`: Always returns `Valid`
//! - `update_state`: Accepts the last `State` or `Delta` from update_data
//! - `summarize_state`: Returns blake3 hash of state (32 bytes)
//! - `get_state_delta`: Returns full state as delta (pessimistic)

use freenet_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        // MockWasmRuntime: always returns Ok(ValidateResult::Valid)
        Ok(ValidateResult::Valid)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        // MockWasmRuntime: accepts the last full state or delta from update_data
        let mut new_state = None;
        for ud in &data {
            match ud {
                UpdateData::State(s) => {
                    new_state = Some(State::from(s.as_ref().to_vec()));
                }
                UpdateData::Delta(delta) => {
                    new_state = Some(State::from(delta.as_ref().to_vec()));
                }
                UpdateData::StateAndDelta { state, .. } => {
                    new_state = Some(State::from(state.as_ref().to_vec()));
                }
                UpdateData::RelatedState { .. }
                | UpdateData::RelatedDelta { .. }
                | UpdateData::RelatedStateAndDelta { .. } => {
                    // Ignore related data for the merge
                }
                // `UpdateData` is `#[non_exhaustive]` since stdlib 0.6.0.
                // Test fixture only — ignore unknown variants for the merge.
                _ => {}
            }
        }
        match new_state {
            Some(s) => Ok(UpdateModification::valid(s)),
            None => Ok(UpdateModification::valid(state)),
        }
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        // MockWasmRuntime: blake3 hash of state
        Ok(StateSummary::from(
            blake3::hash(state.as_ref()).as_bytes().to_vec(),
        ))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        // MockWasmRuntime: pessimistic, always return full state as delta
        Ok(StateDelta::from(state.as_ref().to_vec()))
    }
}
