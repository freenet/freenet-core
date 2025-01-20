use freenet_stdlib::prelude::*;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct TestConditions {
    pub max_iterations: u64,
}

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let bytes = state.as_ref();
        let test_conditions: TestConditions = serde_json::from_slice::<TestConditions>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        
        // Perform CPU-intensive loop based on max_iterations
        let mut sum: i32 = 0;
        for i in 0..test_conditions.max_iterations {
            sum = sum.wrapping_add(i as i32);
            if i % 2 == 0 {
                sum = sum.wrapping_mul(2);
            }
        }

        if sum % 2 == 0 {
            Ok(ValidateResult::Valid)
        } else {
            Ok(ValidateResult::RequestRelated(vec![]))
        }
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        mut data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let bytes = state.as_ref();
        let test_conditions: TestConditions = serde_json::from_slice::<TestConditions>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        // CPU-intensive loop
        let mut sum: i32 = 0;
        for i in 0..test_conditions.max_iterations {
            sum = sum.wrapping_add(i as i32);
            if i % 3 == 0 {
                sum = sum.wrapping_mul(3);
            }
        }

        let mut new_state = bytes.to_vec();
        if let Some(update) = data.pop() {
            match update {
                UpdateData::State(s) => new_state.extend_from_slice(s.as_ref()),
                UpdateData::Delta(d) => new_state.extend_from_slice(d.as_ref()),
                UpdateData::StateAndDelta { state: s, delta: d } => {
                    new_state.extend_from_slice(s.as_ref());
                    new_state.extend_from_slice(d.as_ref());
                }
                _ => {}
            }
        }
        
        Ok(UpdateModification::valid(State::from(new_state)))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let bytes = state.as_ref();
        let test_conditions: TestConditions = serde_json::from_slice::<TestConditions>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        // CPU-intensive loop
        let mut sum: i32 = 0;
        for i in 0..test_conditions.max_iterations {
            sum = sum.wrapping_add(i as i32);
            if i % 4 == 0 {
                sum = sum.wrapping_mul(4);
            }
        }

        let summary_bytes = bytes[..bytes.len().saturating_sub(1)].to_vec();
        Ok(StateSummary::from(summary_bytes))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        let bytes = state.as_ref();
        let test_conditions: TestConditions = serde_json::from_slice::<TestConditions>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        // CPU-intensive loop
        let mut sum: i32 = 0;
        for i in 0..test_conditions.max_iterations {
            sum = sum.wrapping_add(i as i32);
            if i % 5 == 0 {
                sum = sum.wrapping_mul(5);
            }
        }

        let summary_len = summary.as_ref().len();
        let delta_bytes = bytes[summary_len..].to_vec();
        Ok(StateDelta::from(delta_bytes))
    }
}
