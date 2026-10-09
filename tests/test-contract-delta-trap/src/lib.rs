//! Test contract whose `get_state_delta` always fails.
//!
//! Exists to drive leg 2 of `Executor::finalize_state_commit`
//! (`send_update_notification`) into its error path, so a test can assert that
//! the commit still succeeds and legs 3 and 4 still run.
//!
//! `send_update_notification` computes a delta for any subscriber holding a
//! cached summary and propagates a failure with `?`, which aborts the whole
//! subscriber fan-out. That is the only way to make leg 2 fail without adding a
//! production seam, and a failing `get_state_delta` is the realistic cause: a
//! contract whose delta computation traps for one client's summary is exactly
//! the case that used to fail the *whole PUT*.
//!
//! Note the delta path is only taken for a subscriber registered with a
//! `Some(summary)`. With no summary the executor sends full state and never
//! calls this function, so a test that forgets the summary silently exercises
//! nothing — assert that the subscriber received nothing, not merely that the
//! PUT succeeded.
//!
//! Everything else is deliberately permissive, so a test failure points at the
//! fan-out rather than at contract semantics.

use freenet_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        Ok(ValidateResult::Valid)
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut new_state = None;
        for update in &data {
            match update {
                UpdateData::State(s) => new_state = Some(State::from(s.as_ref().to_vec())),
                UpdateData::Delta(delta) => new_state = Some(State::from(delta.as_ref().to_vec())),
                UpdateData::StateAndDelta { state, .. } => {
                    new_state = Some(State::from(state.as_ref().to_vec()))
                }
                _ => {}
            }
        }
        Ok(UpdateModification::valid(new_state.unwrap_or(state)))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        Ok(StateSummary::from(state.as_ref().to_vec()))
    }

    /// Always fails. This is the whole point of the fixture.
    fn get_state_delta(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        Err(ContractError::Other(
            "get_state_delta fails by design (test fixture)".to_owned(),
        ))
    }
}
