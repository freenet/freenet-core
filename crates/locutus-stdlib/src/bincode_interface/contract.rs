use std::marker::PhantomData;

use serde::{Serialize, Deserialize};

use crate::prelude::*;

/// This trait provides an abstraction over the ContractInterface, where data types are serialized
/// and deserialized using bincode.
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
