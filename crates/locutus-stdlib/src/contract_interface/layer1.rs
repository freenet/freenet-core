use super::*;

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
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError>;

    /// Verifies that the given `delta` is valid, given these `parameters`.
    fn validate_delta(
        parameters: Self::Parameters,
        delta: Self::StateDelta,
    ) -> Result<bool, ContractError>;

    ///
    fn update_state(
        parameters: Self::Parameters,
        state: Self::State,
        data: Vec<UpdateData<'static>>,
    ) -> Result<L1UpdateModification<Self::State>, ContractError>;

    fn summarize_state(
        parameters: Self::Parameters,
        state: Self::State,
    ) -> Result<Self::StateSummary, ContractError>;

    fn get_state_delta(
        parameters: Self::Parameters,
        state: Self::State,
        summary: Self::StateSummary,
    ) -> Result<Self::StateDelta, ContractError>;
}

pub struct L1UpdateModification<State>
where
    State: Serialize + DeserializeOwned,
{
    pub state: State,
    pub related : Vec<RelatedContract>,
}

trait Encoder {
    type Error: Into<ContractError>;
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

impl<T> ContractInterface for T
where
    T: Encoder + ContractAdapter,
{
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let parameters: <Self as ContractAdapter>::Parameters =
            <Self as Encoder>::deserialize(parameters.as_ref()).map_err(Into::into)?;
        let state: <Self as ContractAdapter>::State =
            <Self as Encoder>::deserialize(state.as_ref()).map_err(Into::into)?;
        <Self as ContractAdapter>::validate_state(parameters, state, related)
    }

    /// Verify that a delta is valid if possible, returns false if and only delta is
    /// definitely invalid, true otherwise.
    fn validate_delta(
        parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        let parameters: <Self as ContractAdapter>::Parameters =
            <Self as Encoder>::deserialize(parameters.as_ref()).map_err(Into::into)?;
        let delta: <Self as ContractAdapter>::StateDelta =
            <Self as Encoder>::deserialize(delta.as_ref()).map_err(Into::into)?;
        <Self as ContractAdapter>::validate_delta(parameters, delta)
    }

    /// Update the state to account for the new data
    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<crate::contract_interface::UpdateModification<'static>, ContractError> {
        let parameters: <Self as ContractAdapter>::Parameters =
            <Self as Encoder>::deserialize(parameters.as_ref()).map_err(Into::into)?;
        let state: <Self as ContractAdapter>::State =
            <Self as Encoder>::deserialize(state.as_ref()).map_err(Into::into)?;
        let data: Vec<UpdateData<'static>> = data
            .into_iter()
            .map(|d| {
                let d: UpdateData<'static> =
                    <Self as Encoder>::deserialize(d.to_owned()).map_err(Into::into)?;
                Ok(d)
            })
            .collect::<Result<Vec<UpdateData<'static>>, ContractError>>()?;
        <Self as ContractAdapter>::update_state(parameters, state, data)
    }

    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state.
    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
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
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        todo!()
    }
}
