use freenet_stdlib::prelude::*;
use std::collections::HashSet;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Ping {
    from: HashSet<String>,
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
        let _ = serde_json::from_slice::<Ping>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        Ok(ValidateResult::Valid)
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        let bytes = delta.as_ref();
        let _ = serde_json::from_slice::<HashSet<String>>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        Ok(true)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut ping = serde_json::from_slice::<Ping>(state.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        for ud in data {
            match ud {
                UpdateData::State(s) => {
                    let ping_state = serde_json::from_slice::<Ping>(&s)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    ping.from.extend(ping_state.from);
                }
                UpdateData::Delta(s) => {
                    let names = serde_json::from_slice::<HashSet<String>>(&s)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    ping.from.extend(names);
                }
                UpdateData::StateAndDelta { state, delta } => {
                    let ping_state = serde_json::from_slice::<Ping>(&state)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    let names = serde_json::from_slice::<HashSet<String>>(&delta)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    ping.from.extend(ping_state.from);
                    ping.from.extend(names);
                }
                _ => return Err(ContractError::InvalidUpdate),
            }
        }
        return Ok(UpdateModification::valid(State::from(
            serde_json::to_vec(&ping).map_err(|e| ContractError::Other(e.to_string()))?,
        )));
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let state = state.as_ref();
        let _ = serde_json::from_slice::<Ping>(state)
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        Ok(StateSummary::from(state.to_vec()))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        let ping = serde_json::from_slice::<Ping>(state.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        let ping_summary = serde_json::from_slice::<Ping>(summary.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        let mut delta = HashSet::new();
        for s in ping.from.difference(&ping_summary.from) {
            delta.insert(s.clone());
        }

        Ok(StateDelta::from(
            serde_json::to_vec(&delta).map_err(|e| ContractError::Other(e.to_string()))?,
        ))
    }
}
