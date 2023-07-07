use std::marker::PhantomData;

use serde::{Serialize, Deserialize};

use crate::prelude::*;

/// `ContractInterfaceBincode` is a higher-level interface for interacting with contracts.
/// The trait provides an abstraction over the lower-level `ContractInterface`, by automatically
/// handling the serialization and deserialization of data using bincode.
///
/// Any type implementing this trait is assumed to handle state changes in a commutative manner.
///
/// # Examples
///
/// Let's assume we have a contract type `MyContract`.
/// ```
/// pub struct MyContract;
/// ```
///
/// We would implement `ContractInterfaceBincode` for `MyContract` like this:
/// ```
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct MyParameters {
///     // ... fields for the parameters ...
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct MyState {
///     // ... fields for the state ...
/// }
///
/// // Similar structs for `MyStateDelta` and `MyStateSummary`...
///
/// impl ContractInterfaceBincode for MyContract {
///     type Parameters = MyParameters;
///     type State = MyState;
///     // ... and so on for `StateDelta` and `StateSummary`...
///
///     fn validate_state(
///         parameters: Self::Parameters,
///         state: Self::State,
///         related: RelatedContracts<'static>,
///     ) -> Result<ValidateResult, ContractError> {
///         // Implement your contract logic here
///     }
///
///     // ... and so on for the other functions...
/// }
/// ```
/// With `ContractInterfaceBincode` implemented for `MyContract`, we can now call functions on it with
/// parameters of our custom types.
/// ```
/// let parameters = MyParameters { /* ... */ };
/// let state = MyState { /* ... */ };
/// let result = MyContract::validate_state(parameters, state, related);
/// ```
pub trait ContractInterfaceBincode {
    type Parameters: Serialize + for<'de> Deserialize<'de>;
    type State: Serialize + for<'de> Deserialize<'de>;
    type StateDelta: Serialize + for<'de> Deserialize<'de>;
    type StateSummary: Serialize + for<'de> Deserialize<'de>;

    fn validate_state(
        parameters: Self::Parameters,
        state: Self::State,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError>;

    fn validate_delta(
        parameters: Self::Parameters,
        delta: Self::StateDelta,
    ) -> Result<bool, ContractError>;

    fn update_state(
        parameters: Self::Parameters,
        state: Self::State,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError>;

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

pub struct BincodeContractAdapter<T: ContractInterfaceBincode>(PhantomData<T>);

impl<T: ContractInterfaceBincode> ContractInterface for BincodeContractAdapter<T> {
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let deserialized_params: T::Parameters = bincode::deserialize(parameters.as_ref())?;
        let deserialized_state: T::State = bincode::deserialize(state.as_ref())?;
    
        T::validate_state(deserialized_params, deserialized_state, related)
    }
    

    fn validate_delta(
        parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        todo!()
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        todo!()
    }

    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        todo!()
    }

    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        todo!()
    }
}


/* 
// Then, users can implement ContractInterfaceBincode for their types, and use
// BincodeContractAdapter to automatically get a ContractInterface implementation.
struct MyContract {
    //...
}

impl ContractInterfaceBincode for MyContract {
    //...
}

let my_contract = MyContract { /* ... */ };
let contract_interface: Box<dyn ContractInterface> = Box::new(BincodeContractAdapter { inner: my_contract });
*/

impl From<bincode::Error> for ContractError {
    fn from(error: bincode::Error) -> Self {
        ContractError::Deser(error.to_string())
    }
}
