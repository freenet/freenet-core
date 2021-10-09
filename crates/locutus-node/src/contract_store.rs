use crate::{
    contract::{Contract, ContractError, ContractKey},
    operations::put::ContractPutValue,
};

/// Behaviour
#[async_trait::async_trait]
pub(crate) trait ContractHandler {
    type Error;

    /// Returns a copy of the contract bytes if available, none otherwise.
    async fn fetch_contract(&self, key: &ContractKey) -> Result<Option<Contract>, Self::Error>;

    /// Store a copy of the contract in the local store.
    async fn store_contract(&mut self, contract: Contract) -> Result<(), Self::Error>;

    /// Updates (or inserts) a value for the given contract. This operation is fallible:
    /// It will return an error when the value is not valid (from the contract pov)
    /// or any other condition happened.
    async fn put_value(&mut self, contract: &ContractKey) -> Result<ContractPutValue, Self::Error>;

    fn channel(&self) -> &ContractHandlerChannel;
}

pub struct EventId(usize);

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
#[derive(Clone)]
pub(crate) struct ContractHandlerChannel {}

impl ContractHandlerChannel {
    pub fn new() -> Self {
        // let (notification_tx, notification_channel) = mpsc::channel(100);
        // let (ch_tx, ch_listener) = mpsc::channel(10);
        Self {}
    }

    /// Send an event to the contract handler and receive a response event if succesful.
    pub async fn send_to_handler<Err>(
        &self,
        ev: ContractHandlerEvent<Err>,
    ) -> Result<ContractHandlerEvent<Err>, ContractError> {
        todo!()
    }

    pub async fn send_to_listeners<Err>(&self, id: EventId, ev: ContractHandlerEvent<Err>) {
        todo!()
    }

    pub async fn recv_from_listeners<Err>(
        &self,
    ) -> Result<(EventId, ContractHandlerEvent<Err>), ContractError> {
        todo!()
    }
}

pub(crate) enum ContractHandlerEvent<Err> {
    /// Fetch a supposedly existing contract in this node.  
    FetchQuery(ContractKey),
    FetchResponse {
        key: ContractKey,
        contract: Result<Option<Contract>, Err>,
    },
    /// Try to push/put a new value into the contract.
    PushQuery {
        key: ContractKey,
        value: Vec<u8>,
    },
    /// A push query was successful.
    PushResponse,
    Cache(Contract),
    /// Result of a caching operation.
    CacheResult(Result<(), Err>),
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    #[cfg(test)]
    pub(crate) struct MemoryContractHandler {
        channel: ContractHandlerChannel,
    }

    impl MemoryContractHandler {
        pub fn new(channel: ContractHandlerChannel) -> Self {
            MemoryContractHandler { channel }
        }
    }

    #[cfg(test)]
    #[async_trait::async_trait]
    impl ContractHandler for MemoryContractHandler {
        type Error = ();

        async fn fetch_contract(&self, key: &ContractKey) -> Result<Option<Contract>, ()> {
            todo!()
        }

        async fn store_contract(&mut self, contract: Contract) -> Result<(), ()> {
            todo!()
        }

        #[inline(always)]
        fn channel(&self) -> &ContractHandlerChannel {
            &self.channel
        }

        async fn put_value(
            &mut self,
            contract: &ContractKey,
        ) -> Result<ContractPutValue, Self::Error> {
            todo!()
        }
    }
}
