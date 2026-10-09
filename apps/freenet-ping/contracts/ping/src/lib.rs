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
        freenet_stdlib::log::info(&format!(
            "[UPDATE_STATE] Ping contract update_state called with parameters: {parameters:?}, state: {state:?}, data: {data:?}"
        ));
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
                        let ping_clone = ping.clone();
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:STATE] Ping state before merge: {ping_clone:?}"
                        ));
                        let update_clone = update.clone();
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:STATE] Update before merge: {update_clone:?}"
                        ));
                    }
                    ping.merge(update, opts.ttl);
                    #[cfg(feature = "contract")]
                    {
                        let ping_clone = ping.clone();
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:STATE] Ping state after merge: {ping_clone:?}"
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
                        let ping_clone = ping.clone();
                        freenet_stdlib::log::info(&format!(
                            "[UPDATE_STATE:DELTA] Ping delta before merge: {ping_clone:?}"
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

    /// The summary is the whole state, deliberately.
    ///
    /// A summary's job is to let the far side work out exactly what we are missing,
    /// and for this contract the state IS the set of observations — there is no
    /// smaller thing that still answers "which timestamps do you not have". A
    /// contract with a compressible state (a version vector, a Merkle root, a set of
    /// per-peer high-water marks) should summarise instead of copying, and would
    /// send far less over the wire on every sync.
    ///
    /// What matters for the merge laws is that the DELTA is small, and
    /// `get_state_delta` below makes it so. Sending a large summary costs one
    /// round-trip's bandwidth; returning a large delta costs the whole state on
    /// every update, which is what the `whole_state_self_delta` diagnostic exists to
    /// flag.
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

    /// The delta is what we hold and the recipient does not — not the whole state.
    ///
    /// This used to merge the summary into our state and return the result, so every
    /// delta was a full state copy and a delta against our OWN summary was still the
    /// entire state. The conformance verifier reports that as `self_delta_empty` and
    /// `whole_state_self_delta` (#5072): both are diagnostics, so neither breaks a
    /// merge law, but "synchronisation saves nothing" is not what this contract
    /// should be teaching.
    ///
    /// See `Ping::delta_against` for why a difference is sufficient: `merge` is a
    /// union, so applying `S \ R` to `R` reaches the same state as applying `S`.
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

        // Parsed for validation only. The TTL is deliberately NOT applied here:
        // pruning is the receiving merge's job, and pruning on the sender's side
        // would be deciding what the recipient may keep on its behalf.
        // Only the log line below reads this, and that is compiled out without the
        // `contract` feature — but the parse itself is NOT decoration: a malformed
        // parameters blob must be rejected here as it is in every other entry point.
        #[cfg_attr(not(feature = "contract"), allow(unused_variables))]
        let opts = serde_json::from_slice::<PingContractOptions>(parameters.as_ref())
            .map_err(|e| ContractError::Deser(e.to_string()))?;

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!("[GET_STATE_DELTA] Contract options: {opts:?}"));

        let ping = if state.is_empty() {
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

        let Some(delta) = ping.delta_against(&ping_summary) else {
            #[cfg(feature = "contract")]
            freenet_stdlib::log::info("[GET_STATE_DELTA] Recipient is up to date, empty delta");

            // Nothing to send. An empty delta is the honest answer and is what makes
            // the `self_delta_empty` law hold; `update_state` skips empty deltas.
            return Ok(StateDelta::from(Vec::new()));
        };

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!("[GET_STATE_DELTA] Delta: {delta:?}"));

        let result = serde_json::to_vec(&delta).map_err(|e| ContractError::Other(e.to_string()))?;

        #[cfg(feature = "contract")]
        freenet_stdlib::log::info(&format!(
            "[GET_STATE_DELTA] Returning delta with size: {}",
            result.len()
        ));

        Ok(StateDelta::from(result))
    }
}
