//! This contract just checks that macros compile etc.
use locutus_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
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
        _state: State<'static>,
        _data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        unimplemented!()
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        unimplemented!()
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        unimplemented!()
    }
}

fn main() {}
