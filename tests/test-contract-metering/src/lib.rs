use freenet_stdlib::prelude::*;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct TestConditions {
    pub iterations: u64,
}

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let test_conditions: TestConditions = serde_json::from_slice(state.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        
        // Simple loop with:
        // - Loop operation: 25 cycles
        // - Addition: 1 cycle
        // Total: 26 cycles per iteration
        let mut _counter = 0;
        for _ in 0..test_conditions.iterations {
            _counter += 1;
        }

        Ok(ValidateResult::Valid)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let test_conditions: TestConditions = serde_json::from_slice(state.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        // Simple loop with:
        // - Loop operation: 25 cycles
        // - Addition: 1 cycle
        // Total: 26 cycles per iteration
        let mut _counter = 0;
        for _ in 0..test_conditions.iterations {
            _counter += 1;
        }

        Ok(UpdateModification::valid(state))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let state = state.as_ref();
        let test_conditions: TestConditions = serde_json::from_slice(state)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        // Simple loop with:
        // - Loop operation: 25 cycles
        // - Addition: 1 cycle
        // Total: 26 cycles per iteration
        let mut _counter = 0;
        for _ in 0..test_conditions.iterations {
            _counter += 1;
        }

        Ok(StateSummary::from(state.to_vec()))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        let test_conditions: TestConditions = serde_json::from_slice(state.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        // Simple loop with:
        // - Loop operation: 25 cycles
        // - Addition: 1 cycle
        // Total: 26 cycles per iteration
        let mut counter = 0;
        for _ in 0..test_conditions.iterations {
            counter += 1;
        }

        Ok(StateDelta::from(vec![counter as u8]))
    }
}
