use freenet_ping_types::Ping;
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
        // allow empty state
        if bytes.is_empty() {
            return Ok(ValidateResult::Valid);
        }
        let _ = serde_json::from_slice::<Ping>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        Ok(ValidateResult::Valid)
    }

    fn validate_delta(
        _parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        let bytes = delta.as_ref();
        // allow empty delta
        if bytes.is_empty() {
            return Ok(true);
        }
        let _ = serde_json::from_slice::<Ping>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        Ok(true)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut ping = if state.is_empty() {
            None
        } else {
            Some(serde_json::from_slice::<Ping>(state.as_ref())
                .map_err(|e| ContractError::Deser(e.to_string()))?)
        };

        for ud in data {
            match ud {
                UpdateData::State(s) => {
                    if s.is_empty() {
                        continue;
                    }
                    let ping_state = serde_json::from_slice::<Ping>(&s)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    match ping {
                        Some(ref mut p) => p.merge(ping_state),
                        None => ping = Some(ping_state),
                    }
                }
                UpdateData::Delta(s) => {
                    if s.is_empty() {
                        continue;
                    }
                    let pd = serde_json::from_slice::<Ping>(&s)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    match ping {
                        Some(ref mut p) => p.merge(pd),
                        None => ping = Some(pd),
                    }
                }
                UpdateData::StateAndDelta { state, delta } => {
                    if !state.is_empty() {
                        let np = serde_json::from_slice::<Ping>(&state)
                            .map_err(|e| ContractError::Deser(e.to_string()))?;
                        match ping {
                            Some(ref mut p) => p.merge(np),
                            None => ping = Some(np),
                        }
                    }

                    if !delta.is_empty() {
                        let pd = serde_json::from_slice::<Ping>(&delta)
                            .map_err(|e| ContractError::Deser(e.to_string()))?;
                        match ping {
                            Some(ref mut p) => p.merge(pd),
                            None => ping = Some(pd),
                        }
                    }
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
        if state.is_empty() {
            return Ok(StateSummary::from(vec![]));
        }
        let _ = serde_json::from_slice::<Ping>(state)
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        Ok(StateSummary::from(state.to_vec()))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        let ping = if state.is_empty() {
            None
        } else {
            Some(serde_json::from_slice::<Ping>(state.as_ref())
                .map_err(|e| ContractError::Deser(e.to_string()))?)
        };
        let ping_summary = if summary.is_empty() {
            None
        } else {
            Some(serde_json::from_slice::<Ping>(summary.as_ref())
                .map_err(|e| ContractError::Deser(e.to_string()))?)
        };

        match (ping, ping_summary) {
            (None, None) => Ok(StateDelta::from(vec![])),
            (Some(ping), None) => {
                Ok(StateDelta::from(
                    serde_json::to_vec(&ping).map_err(|e| ContractError::Other(e.to_string()))?,
                ))
            }
            (None, Some(ping_summary)) => {
                Ok(StateDelta::from(
                    serde_json::to_vec(&ping_summary).map_err(|e| ContractError::Other(e.to_string()))?,
                ))
            }
            (Some(mut p1), Some(p2)) => {
                p1.merge(p2);
                Ok(StateDelta::from(
                    serde_json::to_vec(&p1).map_err(|e| ContractError::Other(e.to_string()))?,
                ))
            }
        }
    }
}
