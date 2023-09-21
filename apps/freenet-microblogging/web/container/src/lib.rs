use freenet_stdlib::prelude::*;

struct Contract;

// TODO: verify that the state is signed by a pub/key pair
// ~/.freenet/secrets/keys/keypair1.json

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _related: RelatedContracts,
    ) -> Result<ValidateResult, ContractError> {
        Ok(ValidateResult::Valid)
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        _delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        Ok(true)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _data: Vec<UpdateData>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        Ok(UpdateModification::valid(state))
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
