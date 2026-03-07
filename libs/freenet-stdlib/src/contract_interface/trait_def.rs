//! Contract interface trait definition.
//!
//! This module defines the `ContractInterface` trait which all contracts must implement.

use crate::parameters::Parameters;

use super::{
    ContractError, RelatedContracts, State, StateDelta, StateSummary, UpdateData,
    UpdateModification, ValidateResult,
};

/// Trait to implement for the contract building.
///
/// Contains all necessary methods to interact with the contract.
///
/// # Examples
///
/// Implementing `ContractInterface` on a type:
///
/// ```
/// # use freenet_stdlib::prelude::*;
/// struct Contract;
///
/// #[contract]
/// impl ContractInterface for Contract {
///     fn validate_state(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///         _related: RelatedContracts
///     ) -> Result<ValidateResult, ContractError> {
///         Ok(ValidateResult::Valid)
///     }
///
///     fn update_state(
///         _parameters: Parameters<'static>,
///         state: State<'static>,
///         _data: Vec<UpdateData>,
///     ) -> Result<UpdateModification<'static>, ContractError> {
///         Ok(UpdateModification::valid(state))
///     }
///
///     fn summarize_state(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///     ) -> Result<StateSummary<'static>, ContractError> {
///         Ok(StateSummary::from(vec![]))
///     }
///
///     fn get_state_delta(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///         _summary: StateSummary<'static>,
///     ) -> Result<StateDelta<'static>, ContractError> {
///         Ok(StateDelta::from(vec![]))
///     }
/// }
/// ```
// ANCHOR: contractifce
/// # ContractInterface
///
/// This trait defines the core functionality for managing and updating a contract's state.
/// Implementations must ensure that state delta updates are *commutative*. In other words,
/// when applying multiple delta updates to a state, the order in which these updates are
/// applied should not affect the final state. Once all deltas are applied, the resulting
/// state should be the same, regardless of the order in which the deltas were applied.
///
/// Noncompliant behavior, such as failing to obey the commutativity rule, may result
/// in the contract being deprioritized or removed from the p2p network.
pub trait ContractInterface {
    /// Verify that the state is valid, given the parameters.
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError>;

    /// Update the state to account for the new data
    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError>;

    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state.
    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError>;

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError>;
}
// ANCHOR_END: contractifce
