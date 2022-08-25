use locutus_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(_parameters: Parameters<'static>, _state: State<'static>) -> bool {
        true
    }

    fn validate_delta(_parameters: Parameters<'static>, _delta: StateDelta<'static>) -> bool {
        true
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError> {
        Ok(UpdateModification::ValidUpdate(state))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
    ) -> StateSummary<'static> {
        StateSummary::from(vec![])
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> StateDelta<'static> {
        StateDelta::from(vec![])
    }
}
