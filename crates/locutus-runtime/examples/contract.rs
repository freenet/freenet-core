//! This contract just checks that macros compile etc.
use locutus_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts,
    ) -> Result<ValidateResult, ContractError> {
        let _state_bytes = state.as_ref();
        unimplemented!()
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        _delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        unimplemented!()
    }

    fn update_state(
        _parameters: Parameters<'static>,
        mut state: State<'static>,
        _data: Vec<UpdateData>,
    ) -> Result<UpdateModification, ContractError> {
        // let new_state = state.to_mut();
        // new_state.extend(delta.as_ref());
        // Ok(UpdateModification::ValidUpdate(state))
        todo!()
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let state = state.as_ref();
        Ok(StateSummary::from(state[0..3].to_vec()))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        let state = state.as_ref();
        assert!(state.len() == 4);
        let summary = summary.as_ref();
        assert!(summary.len() == 2);
        assert!(&state[1..=2] == summary);
        Ok(StateDelta::from(state[3..=3].to_vec()))
    }
}

fn main() {}
