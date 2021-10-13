use std::marker::PhantomData;

use crate::{
    contract::{Contract, ContractError, ContractKey},
    operations::put::ContractPutValue,
};

/// Behaviour
#[async_trait::async_trait]
pub(crate) trait ContractHandler {
    type Error;

    fn channel(&self) -> &ContractHandlerChannel<Self::Error>;

    /// Returns a copy of the contract bytes if available, none otherwise.
    async fn fetch_contract(&self, key: &ContractKey) -> Result<Option<Contract>, Self::Error>;

    /// Store a copy of the contract in the local store.
    async fn store_contract(&mut self, contract: Contract) -> Result<(), Self::Error>;

    /// Updates (or inserts) a value for the given contract. This operation is fallible:
    /// It will return an error when the value is not valid (from the contract pov)
    /// or any other condition happened.
    async fn put_value(
        &mut self,
        contract: &ContractKey,
        value: ContractPutValue,
    ) -> Result<ContractPutValue, Self::Error>;
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

impl<Err> ContractHandlerChannel<Err> {
    pub fn new() -> Self {
        // let (notification_tx, notification_channel) = mpsc::channel(100);
        // let (ch_tx, ch_listener) = mpsc::channel(10);
        Self { _err: PhantomData }
    }

    /// Send an event to the contract handler and receive a response event if succesful.
    pub async fn send_to_handler(
        &self,
        ev: ContractHandlerEvent<Err>,
    ) -> Result<ContractHandlerEvent<Err>, ContractError<Err>> {
        todo!()
    }

    pub async fn send_to_listeners(&self, id: EventId, ev: ContractHandlerEvent<Err>) {
        todo!()
    }

    pub async fn recv_from_listeners(
        &self,
    ) -> Result<(EventId, ContractHandlerEvent<Err>), ContractError<Err>> {
        todo!()
    }
}

pub(crate) enum ContractHandlerEvent<Err> {
    /// Fetch a supposedly existing contract in this node.  
    FetchQuery(ContractKey),
    /// The response to a FetchQuery event
    FetchResponse {
        key: ContractKey,
        contract: Result<Option<Contract>, Err>,
    },
    /// Try to push/put a new value into the contract.
    PushQuery {
        key: ContractKey,
        value: ContractPutValue,
    },
    /// The response to a push query.
    PushResponse {
        new_value: Result<ContractPutValue, Err>,
    },
    Cache(Contract),
    /// Result of a caching operation.
    CacheResult(Result<(), Err>),
}
