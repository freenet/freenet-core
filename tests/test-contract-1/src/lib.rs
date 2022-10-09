use locutus_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts,
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
        _state: State<'static>,
        _data: Vec<UpdateData>,
    ) -> Result<UpdateModification, ContractError> {
        unimplemented!()
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
