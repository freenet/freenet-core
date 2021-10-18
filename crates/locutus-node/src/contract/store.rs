use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::{Contract, ContractError, ContractKey, ContractValue};

/// Behaviour
#[async_trait::async_trait]
pub(crate) trait ContractHandler {
    type Error;

    fn channel(&self) -> &ContractHandlerChannel<Self::Error>;

    fn contract_store(&mut self) -> &mut ContractStore;

    /// Get current contract value, if present, otherwise get none.
    async fn get_value(&self, contract: &ContractKey)
        -> Result<Option<ContractValue>, Self::Error>;

    /// Updates (or inserts) a value for the given contract. This operation is fallible:
    /// It will return an error when the value is not valid (according to the contract specification)
    /// or any other condition happened (for example the contract not being present currently,
    /// in which case it has to be stored by calling `contract_store` first).
    async fn put_value(
        &mut self,
        contract: &ContractKey,
        value: ContractValue,
    ) -> Result<ContractValue, Self::Error>;
}

/// Handle contract blob storage on the file system.
pub(crate) struct ContractStore {}

impl ContractStore {
    /// Returns a copy of the contract bytes if available, none otherwise.
    pub async fn fetch_contract<CErr>(
        &self,
        _key: &ContractKey,
    ) -> Result<Option<Contract>, ContractError<CErr>>
    where
        CErr: std::error::Error,
    {
        todo!()
    }

    /// Store a copy of the contract in the local store.
    pub async fn store_contract<CErr>(
        &mut self,
        _contract: Contract,
    ) -> Result<(), ContractError<CErr>>
    where
        CErr: std::error::Error,
    {
        todo!()
    }
}

pub struct EventId(usize);

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
pub(crate) struct ContractHandlerChannel<Err> {
    _err: PhantomData<Err>,
}

impl<Err> Clone for ContractHandlerChannel<Err> {
    fn clone(&self) -> Self {
        Self { _err: PhantomData }
    }
}

impl<Err: std::error::Error> ContractHandlerChannel<Err> {
    pub fn new() -> Self {
        // let (notification_tx, notification_channel) = mpsc::channel(100);
        // let (ch_tx, ch_listener) = mpsc::channel(10);
        Self { _err: PhantomData }
    }

    /// Send an event to the contract handler and receive a response event if succesful.
    pub async fn send_to_handler(
        &self,
        _ev: ContractHandlerEvent<Err>,
    ) -> Result<ContractHandlerEvent<Err>, ContractError<Err>> {
        todo!()
    }

    pub async fn send_to_listeners(&self, _id: EventId, _ev: ContractHandlerEvent<Err>) {
        todo!()
    }

    pub async fn recv_from_listeners(
        &self,
    ) -> Result<(EventId, ContractHandlerEvent<Err>), ContractError<Err>> {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoreResponse {
    pub value: Option<ContractValue>,
    pub contract: Option<Contract>,
}

pub(crate) enum ContractHandlerEvent<Err>
where
    Err: std::error::Error,
{
    /// Try to push/put a new value into the contract.
    PushQuery {
        key: ContractKey,
        value: ContractValue,
    },
    /// The response to a push query.
    PushResponse {
        new_value: Result<ContractValue, Err>,
    },
    /// Fetch a supposedly existing contract value in this node, and optionally the contract itself.  
    FetchQuery {
        key: ContractKey,
        fetch_contract: bool,
    },
    /// The response to a FetchQuery event
    FetchResponse {
        key: ContractKey,
        response: Result<StoreResponse, Err>,
    },
    Cache(Contract),
    /// Result of a caching operation.
    CacheResult(Result<(), ContractError<Err>>),
}
