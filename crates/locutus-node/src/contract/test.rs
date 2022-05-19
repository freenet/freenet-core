use std::collections::HashMap;

use locutus_runtime::{ContractKey, ContractStore, RuntimeInterface, StateStorage, StateStore};

use super::handler::{CHListenerHalve, ContractHandler, ContractHandlerChannel};
use crate::{config::CONFIG, WrappedState};

pub(crate) struct MockRuntime {}

#[allow(unused_variables)]
impl RuntimeInterface for MockRuntime {
    fn validate_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'a>,
        state: &locutus_runtime::WrappedState,
    ) -> locutus_runtime::RuntimeResult<bool> {
        todo!()
    }

    fn validate_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'a>,
        delta: &locutus_runtime::StateDelta<'a>,
    ) -> locutus_runtime::RuntimeResult<bool> {
        todo!()
    }

    fn update_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'a>,
        state: &locutus_runtime::WrappedState,
        delta: &locutus_runtime::StateDelta<'a>,
    ) -> locutus_runtime::RuntimeResult<locutus_runtime::WrappedState> {
        todo!()
    }

    fn summarize_state<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'a>,
        state: &locutus_runtime::WrappedState,
    ) -> locutus_runtime::RuntimeResult<locutus_runtime::StateSummary<'a>> {
        todo!()
    }

    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'a>,
        state: &locutus_runtime::WrappedState,
        delta_to: &locutus_runtime::StateSummary<'a>,
    ) -> locutus_runtime::RuntimeResult<locutus_runtime::StateDelta<'a>> {
        todo!()
    }
}

#[derive(Default)]
pub(crate) struct MemKVStore(HashMap<ContractKey, WrappedState>);

#[async_trait::async_trait]
impl StateStorage for MemKVStore {
    type Error = String;

    async fn store(
        &mut self,
        key: ContractKey,
        state: locutus_runtime::WrappedState,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn get(
        &self,
        key: &ContractKey,
    ) -> Result<Option<locutus_runtime::WrappedState>, Self::Error> {
        todo!()
    }
}

impl MemKVStore {
    pub fn new() -> Self {
        Self::default()
    }
}

pub(crate) struct MemoryContractHandler<KVStore = MemKVStore> {
    channel: ContractHandlerChannel<SimStoreError, CHListenerHalve>,
    kv_store: StateStore<KVStore>,
    contract_store: ContractStore,
    _runtime: MockRuntime,
}

impl<KVStore> MemoryContractHandler<KVStore>
where
    KVStore: StateStorage,
    <KVStore as StateStorage>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    const MAX_MEM_CACHE: i64 = 10_000_000;

    pub fn new(
        channel: ContractHandlerChannel<SimStoreError, CHListenerHalve>,
        kv_store: KVStore,
    ) -> Self {
        MemoryContractHandler {
            channel,
            kv_store: StateStore::new(kv_store, 10_000_000).unwrap(),
            contract_store: ContractStore::new(
                CONFIG.config_paths.contracts_dir.clone(),
                Self::MAX_MEM_CACHE,
            ),
            _runtime: MockRuntime {},
        }
    }
}

impl From<ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>>
    for MemoryContractHandler
{
    fn from(
        channel: ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>,
    ) -> Self {
        let store = MemKVStore::new();
        MemoryContractHandler::new(channel, store)
    }
}

#[async_trait::async_trait]
impl ContractHandler for MemoryContractHandler {
    type Error = SimStoreError;
    type Store = MemKVStore;

    #[inline(always)]
    fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve> {
        &mut self.channel
    }

    #[inline(always)]
    fn contract_store(&mut self) -> &mut ContractStore {
        &mut self.contract_store
    }

    async fn handle_request(
        &mut self,
        req: crate::ClientRequest,
    ) -> Result<crate::HostResponse, Self::Error> {
        // async fn get_state(&self, contract: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        //     Ok(self.kv_store.get(contract).cloned())
        // }

        // async fn update_state(
        //     &mut self,
        //     contract: &ContractKey,
        //     value: WrappedState,
        // ) -> Result<WrappedState, Self::Error> {
        //     let new_val = value.clone();
        //     self.kv_store.insert(*contract, value);
        //     Ok(new_val)
        // }
        todo!()
    }

    fn state_store(&mut self) -> &mut locutus_runtime::StateStore<Self::Store> {
        &mut self.kv_store
    }
}

#[derive(Debug)]
pub(crate) struct SimStoreError(String);

impl std::fmt::Display for SimStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SimStoreError {}

impl From<std::io::Error> for SimStoreError {
    fn from(err: std::io::Error) -> Self {
        Self(format!("{err}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::WrappedContract;

    #[test]
    fn serialization() -> Result<(), anyhow::Error> {
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;

        let serialized = bincode::serialize(&contract)?;
        let deser: WrappedContract = bincode::deserialize(&serialized)?;
        assert_eq!(deser.code(), contract.code());
        assert_eq!(deser.key(), contract.key());
        Ok(())
    }
}
