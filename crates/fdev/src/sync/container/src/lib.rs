use freenet_stdlib::prelude::*;

struct SyncContainer;

#[contract]
impl ContractInterface for SyncContainer {
    fn validate_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        Ok(ValidateResult::Valid)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        for update in data {
            if let UpdateData::Delta(delta) = update {
                return Ok(UpdateModification::valid(State::from(delta.to_vec())));
            }
        }
        Err(ContractError::InvalidDelta)
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        Ok(StateSummary::from(vec![]))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        Ok(StateDelta::from(vec![]))
    }
}
