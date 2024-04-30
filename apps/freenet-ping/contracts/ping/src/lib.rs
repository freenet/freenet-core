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
        if bytes.len() % 3 == 0 {
            return Ok(ValidateResult::Valid);
        }

        Ok(ValidateResult::Invalid)
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        let bytes = delta.as_ref();
        if bytes.len() % 3 == 0 {
            Ok(true)
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
            let mut data = state.to_vec();
            data.extend(delta.as_ref());
            if data.len() >= 15_000 {
                data.truncate(15_000);
                return Ok(UpdateModification::valid(data.into()));
            }

            Ok(UpdateModification::valid(data.into()))
        } else {
            Err(ContractError::InvalidUpdate)
        }
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let state = state.as_ref();
        if state.len() > 15_000 {
            Err(ContractError::Other("incorrect data".to_owned()))
        } else {
            Ok(StateSummary::from(state.to_vec()))
        }
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        let state = state.as_ref();
        let len = state.len();
        if state.len() < 3 {
            return Ok(StateDelta::from(state.to_vec()));
        }

        Ok(StateDelta::from(state[len - 3..len].to_vec()))
    }
}

#[test]
fn validate_test() -> Result<(), Box<dyn std::error::Error>> {
    let is_valid = Contract::validate_state(
        Parameters::from([].as_ref()),
        State::from(vec![0, 0, 0]),
        Default::default(),
    )?;
    assert!(is_valid == ValidateResult::Valid);
    let not_valid = Contract::validate_state(
        Parameters::from([].as_ref()),
        State::from(vec![1, 1, 1, 4]),
        Default::default(),
    )?;
    assert!(matches!(not_valid, ValidateResult::Invalid));
    Ok(())
}

#[test]
fn test_() {}
