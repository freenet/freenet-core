use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

/// Simple contract state that doesn't auto-increment version
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct SimpleState {
    value: String,
    counter: u64,
}

/// The contract will only return NoChange if the state is exactly identical
struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        // Just verify it's valid JSON
        serde_json::from_slice::<SimpleState>(state.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        Ok(ValidateResult::Valid)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        // Parse current state
        let current_state: SimpleState = serde_json::from_slice(state.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        // Process updates
        if let Some(update) = data.into_iter().next() {
            match update {
                UpdateData::State(new_state_data) => {
                    // Parse new state
                    let new_state: SimpleState = serde_json::from_slice(new_state_data.as_ref())
                        .map_err(|e| ContractError::Deser(e.to_string()))?;

                    // THIS IS THE KEY: Only update if state actually changed
                    if current_state == new_state {
                        // Return the same state to trigger NoChange in the executor
                        return Ok(UpdateModification::valid(state.clone()));
                    } else {
                        // State changed, return the new state
                        let updated_bytes = serde_json::to_vec(&new_state).map_err(|e| {
                            ContractError::Other(format!("Serialization error: {e}"))
                        })?;
                        return Ok(UpdateModification::valid(State::from(updated_bytes)));
                    }
                }
                UpdateData::Delta(_) => {
                    return Err(ContractError::InvalidUpdate);
                }
                _ => {
                    return Err(ContractError::InvalidUpdate);
                }
            }
        }

        // No updates provided - return current state
        Ok(UpdateModification::valid(state))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        // Just return the full state as delta
        Ok(StateDelta::from(state.as_ref().to_vec()))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        // Simple summary - just the length of the state
        let summary = state.as_ref().len().to_le_bytes().to_vec();
        Ok(StateSummary::from(summary))
    }
}
