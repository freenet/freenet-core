//! Test contract whose `update_state` asks for a related contract it does not
//! have, then settles once the executor supplies it.
//!
//! This exists to make the fetch-and-install branch of
//! `Executor::get_updated_state` reachable from a unit test. That branch is the
//! only place a node installs *another* contract's state while servicing an
//! UPDATE, and it owes that contract's subscribers the same post-store fan-out
//! any other install does — keyed on the RELATED contract, not on the contract
//! being updated (#5481). Nothing else in `tests/` returns
//! `UpdateModification::requires`, so without this contract that branch cannot
//! be entered at all and can only be guarded by a source scrape.
//!
//! Behaviour:
//!
//! - `validate_state` — always `Valid`. This contract is about the fan-out, not
//!   about contract semantics.
//! - `update_state` — if the update data carries no `UpdateData::State`, ask for
//!   [`WANTED_RELATED_ID`]; otherwise keep the current state unchanged. The
//!   executor pushes the fetched related state in as `UpdateData::State` before
//!   retrying, so this settles on the second pass and the loop terminates.
//! - `summarize_state` / `get_state_delta` — whole state, no cleverness.

use freenet_stdlib::prelude::*;

/// The instance id this contract asks for.
///
/// Arbitrary and fixed. It is only ever used as a lookup key: the executor asks
/// the network (in tests, a stubbed sub-op GET) for it and installs whatever
/// contract comes back under THAT contract's own key. So this constant
/// deliberately does not have to match the key of the contract the test hands
/// back — which is the point, since the property under test is that the fan-out
/// follows the installed contract's key rather than the requested id or the
/// enclosing contract's key.
const WANTED_RELATED_ID: [u8; 32] = [0xA7; 32];

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
        let related_supplied = data
            .iter()
            .any(|update| matches!(update, UpdateData::State(_)));
        if !related_supplied {
            return UpdateModification::requires(vec![RelatedContract {
                contract_instance_id: ContractInstanceId::new(WANTED_RELATED_ID),
                mode: RelatedMode::StateOnce,
            }]);
        }
        // Settle without changing anything. The commit of THIS contract is not
        // what the test observes; the related contract's install is.
        Ok(UpdateModification::valid(state))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        Ok(StateSummary::from(state.as_ref().to_vec()))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        Ok(StateDelta::from(state.as_ref().to_vec()))
    }
}
