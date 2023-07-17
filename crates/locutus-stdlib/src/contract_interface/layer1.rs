use serde::{de::DeserializeOwned, Serialize};

use crate::contract_interface as L0;

pub trait ContractAdapter {
    // type Parameters: for<'x> From<&'x [u8]>;
    // type State: for<'x> From<&'x [u8]>;
    // type StateDelta: for<'x> From<&'x [u8]>;
    // type StateSummary: for<'x> From<&'x [u8]>;
    type Parameters: Serialize + DeserializeOwned;
    type State: Serialize + DeserializeOwned;
    type StateDelta: Serialize + DeserializeOwned;
    type StateSummary: Serialize + DeserializeOwned;

    fn validate_state(
        parameters: Self::Parameters,
        state: Self::State,
        related: L0::RelatedContracts<'static>,
    ) -> Result<L0::ValidateResult, L0::ContractError>;

    /// Verifies that the given `delta` is valid, given these `parameters`.
    fn validate_delta(
        parameters: Self::Parameters,
        delta: Self::StateDelta,
    ) -> Result<bool, L0::ContractError>;

    ///
    fn update_state(
        parameters: Self::Parameters,
        state: Self::State,
        data: Vec<L0::UpdateData<'static>>,
    ) -> Result<L1UpdateModification<Self::State>, L0::ContractError>;

    fn summarize_state(
        parameters: Self::Parameters,
        state: Self::State,
    ) -> Result<Self::StateSummary, L0::ContractError>;

    fn get_state_delta(
        parameters: Self::Parameters,
        state: Self::State,
        summary: Self::StateSummary,
    ) -> Result<Self::StateDelta, L0::ContractError>;
}
pub enum UpdateData<'a, State, StateDelta>
where
    State: Serialize + DeserializeOwned,
    StateDelta: Serialize + DeserializeOwned,
{
    State(State),
    Delta(StateDelta),
    StateAndDelta(State, StateDelta),
    RelatedState {
        related_to: L0::ContractInstanceId,
        state: L0::State<'a>,
    },
    RelatedDelta {
        related_to: L0::ContractInstanceId,
        delta: L0::StateDelta<'a>,
    },
    RelatedStateAndDelta {
        related_to: L0::ContractInstanceId,
        state: L0::State<'a>,
        delta: L0::StateDelta<'a>,
    },
}

pub struct L1UpdateModification<State>
where
    State: Serialize + DeserializeOwned,
{
    pub state: State,
    pub related: Vec<L0::RelatedContract>,
}

trait Encoder {
    type Error: Into<L0::ContractError>;
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Self::Error>;
    fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, Self::Error>;
}

pub trait BincodeContract {
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode::Error> {
        bincode::deserialize(bytes)
    }
    fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(value)
    }
}

impl<C> Encoder for C
where
    C: BincodeContract,
{
    type Error = bincode::Error;
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, bincode::Error> {
        <C as BincodeContract>::deserialize(bytes)
    }
    fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
        <C as BincodeContract>::serialize(value)
    }
}

impl From<Box<bincode::ErrorKind>> for L0::ContractError {
    fn from(error: Box<bincode::ErrorKind>) -> Self {
        L0::ContractError::Deser(error.to_string())
    }
}

/*
This allows doing something like

struct MyContract;

impl locutus_stdlib::prelude::BincodeContract for MyContract {}

impl ContractAdapter for MyContract {
    type Parameter = MyParameter;
    ..

    fn validate_state(..) -> Result<ValidateResult, ContractError> { .. }
}

*/

impl<T> L0::ContractInterface for T
where
    T: Encoder + ContractAdapter,
{
    fn validate_state(
        parameters: L0::Parameters<'static>,
        state: L0::State<'static>,
        related: L0::RelatedContracts<'static>,
    ) -> Result<L0::ValidateResult, L0::ContractError> {
        let parameters: <Self as ContractAdapter>::Parameters =
            <Self as Encoder>::deserialize(parameters.as_ref()).map_err(Into::into)?;
        let state: <Self as ContractAdapter>::State =
            <Self as Encoder>::deserialize(state.as_ref()).map_err(Into::into)?;
        <Self as ContractAdapter>::validate_state(parameters, state, related)
    }

    /// Verify that a delta is valid if possible, returns false if and only delta is
    /// definitely invalid, true otherwise.
    fn validate_delta(
        parameters: L0::Parameters<'static>,
        delta: L0::StateDelta<'static>,
    ) -> Result<bool, L0::ContractError> {
        let parameters: <Self as ContractAdapter>::Parameters =
            <Self as Encoder>::deserialize(parameters.as_ref()).map_err(Into::into)?;
        let delta: <Self as ContractAdapter>::StateDelta =
            <Self as Encoder>::deserialize(delta.as_ref()).map_err(Into::into)?;
        <Self as ContractAdapter>::validate_delta(parameters, delta)
    }

    /// Update the state to account for the new data
    fn update_state(
        parameters: L0::Parameters<'static>,
        state: L0::State<'static>,
        data: Vec<L0::UpdateData<'static>>,
    ) -> Result<L0::UpdateModification<'static>, L0::ContractError> {
        let parameters: <Self as ContractAdapter>::Parameters =
            <Self as Encoder>::deserialize(parameters.as_ref()).map_err(Into::into)?;
        let state: <Self as ContractAdapter>::State =
            <Self as Encoder>::deserialize(state.as_ref()).map_err(Into::into)?;
        let data: Vec<L0::UpdateData<'static>> = data
            .into_iter()
            .map(|d| todo!())
            .collect::<Result<Vec<L0::UpdateData<'static>>, L0::ContractError>>()?;
        todo!()
    }

    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state.
    fn summarize_state(
        parameters: L0::Parameters<'static>,
        state: L0::State<'static>,
    ) -> Result<L0::StateSummary<'static>, L0::ContractError> {
        let parameters: <Self as ContractAdapter>::Parameters =
            <Self as Encoder>::deserialize(parameters.as_ref()).map_err(Into::into)?;
        let state: <Self as ContractAdapter>::State =
            <Self as Encoder>::deserialize(state.as_ref()).map_err(Into::into)?;
        todo!()
    }

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    fn get_state_delta(
        parameters: L0::Parameters<'static>,
        state: L0::State<'static>,
        summary: L0::StateSummary<'static>,
    ) -> Result<L0::StateDelta<'static>, L0::ContractError> {
        todo!()
    }
}
