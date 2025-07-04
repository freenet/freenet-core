use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::prelude::*;

pub struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!(
            "[VALIDATE_STATE] Validating state with size: {}",
            state.as_ref().len()
        ));

        let bytes = state.as_ref();
        // allow empty state
        if bytes.is_empty() {
            #[cfg(feature = "contract")]
            freenet_stdlib::log::info("[VALIDATE_STATE] Empty state, returning Valid");

            return Ok(ValidateResult::Valid);
        }
        #[cfg(feature = "contract")]
        let ping = serde_json::from_slice::<Ping>(bytes)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!(
            "[VALIDATE_STATE] State validated successfully: {ping:?}"
        ));

        Ok(ValidateResult::Valid)
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(
            &format!(
                "[UPDATE_STATE] Ping contract update_state called with parameters: {parameters:?}, state: {state:?}, data: {data:?}"
            )
        );
        let opts = serde_json::from_slice::<PingContractOptions>(parameters.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;
        let mut ping = if state.is_empty() {
            Ping::default()
        } else {
            serde_json::from_slice::<Ping>(state.as_ref())
                .map_err(|e| ContractError::Deser(e.to_string()))?
        };

        for ud in data {
            match ud {
                UpdateData::State(s) => {
                    if s.is_empty() {
                        continue;
                    }
                    let update = serde_json::from_slice::<Ping>(&s)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    #[cfg(feature = "contract")]
                    {
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:STATE] Ping state before merge: {:?}",
                            ping.clone()
                        ));
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:STATE] Update before merge: {:?}",
                            update.clone()
                        ));
                    }
                    ping.merge(update, opts.ttl);
                    #[cfg(feature = "contract")]
                    {
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:STATE] Ping state after merge: {:?}",
                            ping.clone()
                        ));
                    }
                }
                UpdateData::Delta(s) => {
                    if s.is_empty() {
                        continue;
                    }
                    let update = serde_json::from_slice::<Ping>(&s)
                        .map_err(|e| ContractError::Deser(e.to_string()))?;
                    #[cfg(feature = "contract")]
                    {
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:DELTA] Ping delta before merge: {:?}",
                            ping.clone()
                        ));
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:DELTA] Update before merge: {:?}",
                            update.clone()
                        ));
                    }
                    ping.merge(update, opts.ttl);
                    #[cfg(feature = "contract")]
                    {
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:DELTA] Ping state after merge: {:?}",
                            ping.clone()
                        ));
                    }
                }
                UpdateData::StateAndDelta { state, delta } => {
                    #[cfg(feature = "contract")]
                    {
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:STATE_AND_DELTA] State size: {}, Delta size: {}",
                            state.len(),
                            delta.len()
                        ));
                    }

                    if !state.is_empty() {
                        let np = serde_json::from_slice::<Ping>(&state)
                            .map_err(|e| ContractError::Deser(e.to_string()))?;

                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "[UPDATE_STATE:STATE_AND_DELTA:STATE] State before merge: {:?}",
                                ping.clone()
                            ));
                            freenet_stdlib::log::info(&format!(
                                "[UPDATE_STATE:STATE_AND_DELTA:STATE] Update to merge: {:?}",
                                np.clone()
                            ));
                        }

                        ping.merge(np, opts.ttl);

                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "[UPDATE_STATE:STATE_AND_DELTA:STATE] After merge: {:?}",
                                ping.clone()
                            ));
                        }
                    }

                    if !delta.is_empty() {
                        let pd = serde_json::from_slice::<Ping>(&delta)
                            .map_err(|e| ContractError::Deser(e.to_string()))?;

                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "[UPDATE_STATE:STATE_AND_DELTA:DELTA] State before merge: {:?}",
                                ping.clone()
                            ));
                            freenet_stdlib::log::info(&format!(
                                "[UPDATE_STATE:STATE_AND_DELTA:DELTA] Update to merge: {:?}",
                                pd.clone()
                            ));
                        }

                        ping.merge(pd, opts.ttl);

                        #[cfg(feature = "contract")]
                        {
                            freenet_stdlib::log::info(&format!(
                                "[UPDATE_STATE:STATE_AND_DELTA:DELTA] After merge: {:?}",
                                ping.clone()
                            ));
                        }
                    }
                }
                _ => return Err(ContractError::InvalidUpdate),
            }
        }

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!("[UPDATE_STATE] Returning final state: {ping:?}"));

        Ok(UpdateModification::valid(State::from(
            serde_json::to_vec(&ping).map_err(|e| ContractError::Other(e.to_string()))?,
        )))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!(
            "[SUMMARIZE_STATE] State size: {}",
            state.as_ref().len()
        ));

        let state = state.as_ref();
        if state.is_empty() {
            #[cfg(feature = "contract")]
            freenet_stdlib::log::info("[SUMMARIZE_STATE] Empty state, returning empty summary");

            return Ok(StateSummary::from(vec![]));
        }
        #[cfg(feature = "contract")]
        let ping = serde_json::from_slice::<Ping>(state)
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!("[SUMMARIZE_STATE] State summarized: {ping:?}"));

        Ok(StateSummary::from(state.to_vec()))
    }

    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!(
            "[GET_STATE_DELTA] State size: {}, Summary size: {}",
            state.as_ref().len(),
            summary.as_ref().len()
        ));

        let opts = serde_json::from_slice::<PingContractOptions>(parameters.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!("[GET_STATE_DELTA] Contract options: {opts:?}"));

        let mut ping = if state.is_empty() {
            #[cfg(feature = "contract")]
            freenet_stdlib::log::info("[GET_STATE_DELTA] Empty state, using default Ping");

            Ping::default()
        } else {
            let p = serde_json::from_slice::<Ping>(state.as_ref())
                .map_err(|e| ContractError::Deser(e.to_string()))?;

            #[cfg(feature = "contract")]
            freenet_stdlib::log::info(&format!("[GET_STATE_DELTA] Loaded state: {p:?}"));

            p
        };
        let ping_summary = if summary.is_empty() {
            #[cfg(feature = "contract")]
            freenet_stdlib::log::info("[GET_STATE_DELTA] Empty summary, using default Ping");

            Ping::default()
        } else {
            let ps = serde_json::from_slice::<Ping>(summary.as_ref())
                .map_err(|e| ContractError::Deser(e.to_string()))?;

            #[cfg(feature = "contract")]
            freenet_stdlib::log::info(&format!("[GET_STATE_DELTA] Loaded summary: {ps:?}"));

            ps
        };

        ping.merge(ping_summary, opts.ttl);

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!("[GET_STATE_DELTA] Merged result: {ping:?}"));

        let result = serde_json::to_vec(&ping).map_err(|e| ContractError::Other(e.to_string()))?;

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!(
            "[GET_STATE_DELTA] Returning delta with size: {}",
            result.len()
        ));

        Ok(StateDelta::from(result))
    }
}
