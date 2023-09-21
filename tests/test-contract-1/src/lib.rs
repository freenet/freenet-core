use freenet_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let bytes = state.as_ref();
        if bytes.len() == 4 {
            let r = match bytes[0..4] {
                [1, 2, 3, 4] => ValidateResult::Valid,
                _ => ValidateResult::RequestRelated(vec![]),
            };
            Ok(r)
        } else if bytes.len() != 4 {
            Err(ContractError::InvalidState)
        } else {
            Ok(ValidateResult::Invalid)
        }
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        let bytes = delta.as_ref();
        if bytes.len() == 4 && bytes == [1, 2, 3, 4] {
            Ok(true)
        } else if bytes.len() != 4 {
            Err(ContractError::InvalidDelta)
        } else {
            Ok(false)
        }
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        mut data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        if let Some(UpdateData::Delta(delta)) = data.pop() {
            if delta.as_ref() == [4] && state.as_ref() == [5, 2, 3] {
                Ok(UpdateModification::valid(State::from(vec![5, 2, 3, 4])))
            } else {
                Err(ContractError::InvalidUpdate)
            }
        } else {
            Err(ContractError::InvalidUpdate)
        }
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let state = state.as_ref();
        if state.len() < 3 {
            Err(ContractError::Other("incorrect data".to_owned()))
        } else {
            Ok(StateSummary::from(state[0..3].to_vec()))
        }
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        if state.len() != 4 {
            return Err(ContractError::Other("incorrect state data".to_owned()));
        }
        if summary.len() != 2 {
            return Err(ContractError::Other("incorrect summ data".to_owned()));
        }
        if &state.as_ref()[1..=2] == summary.as_ref() {
            Ok(StateDelta::from(state[3..=3].to_vec()))
        } else {
            Err(ContractError::Other("cannot summarize".to_owned()))
        }
    }
}

#[test]
fn validate_test() -> Result<(), Box<dyn std::error::Error>> {
    let is_valid = Contract::validate_state(
        Parameters::from([].as_ref()),
        State::from(vec![1, 2, 3, 4]),
        Default::default(),
    )?;
    assert!(is_valid == ValidateResult::Valid);
    let not_valid = Contract::validate_state(
        Parameters::from([].as_ref()),
        State::from(vec![1, 0, 0, 1]),
        Default::default(),
    )?;
    assert!(matches!(not_valid, ValidateResult::RequestRelated(_)));
    Ok(())
}
