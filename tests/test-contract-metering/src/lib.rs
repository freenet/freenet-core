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

        // Loop with:
        // - Loop operation: 25 cycles
        // - Modulo: 1 cycle
        // - Comparison: 1 cycle
        // - Addition: 1 cycle
        // Total: 28 cycles per iteration
        let mut _counter = 0;
        for i in 0..test_conditions.iterations {
            if i % 2 == 0 {
                _counter += 1;
            }
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

        // Loop with:
        // - Loop operation: 25 cycles
        // - Two modulo ops: 2 cycles
        // - Three comparisons: 3 cycles
        // - Two additions: 2 cycles
        // Total: 32 cycles per iteration
        let mut _counter = 0;
        for i in 0..test_conditions.iterations {
            if i % 2 == 0 {
                _counter += 1;
            } else if i % 3 == 0 {
                _counter += 2;
            }
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

        // Loop with:
        // - Loop operation: 25 cycles
        // - Three modulo ops: 3 cycles
        // - Four comparisons: 4 cycles
        // - Three additions: 3 cycles
        // Total: 35 cycles per iteration
        let mut counter = 0;
        for i in 0..test_conditions.iterations {
            if i % 2 == 0 {
                counter += 1;
            } else if i % 3 == 0 {
                counter += 2;
            } else if i % 5 == 0 {
                counter += 3;
            }
        }

        Ok(StateDelta::from(vec![counter as u8]))
    }
}
