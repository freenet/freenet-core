use dashmap::DashMap;
use futures::future::BoxFuture;
use locutus_runtime::{
    ContractKey, ContractRuntimeInterface, ContractStore, StateStorage, StateStore,
    UpdateModification, ValidateResult,
};
use locutus_stdlib::client_api::{ClientRequest, HostResponse};

use super::handler::{CHListenerHalve, ContractHandler, ContractHandlerChannel};
use crate::{config::CONFIG, DynError, WrappedState};

pub(crate) struct MockRuntime {}

#[allow(unused_variables)]
impl ContractRuntimeInterface for MockRuntime {
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'_>,
        state: &locutus_runtime::WrappedState,
        related: locutus_runtime::RelatedContracts,
    ) -> locutus_runtime::RuntimeResult<ValidateResult> {
        todo!()
    }

    fn validate_delta(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'_>,
        delta: &locutus_runtime::StateDelta<'_>,
    ) -> locutus_runtime::RuntimeResult<bool> {
        todo!()
    }

    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'_>,
        state: &locutus_runtime::WrappedState,
        data: &[locutus_runtime::UpdateData<'_>],
    ) -> locutus_runtime::RuntimeResult<UpdateModification<'static>> {
        todo!()
    }

    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'_>,
        state: &locutus_runtime::WrappedState,
    ) -> locutus_runtime::RuntimeResult<locutus_runtime::StateSummary<'static>> {
        todo!()
    }

    fn get_state_delta(
        &mut self,
        key: &ContractKey,
        parameters: &locutus_runtime::Parameters<'_>,
        state: &locutus_runtime::WrappedState,
        delta_to: &locutus_runtime::StateSummary<'_>,
    ) -> locutus_runtime::RuntimeResult<locutus_runtime::StateDelta<'static>> {
        todo!()
    }
}

#[derive(Default, Clone)]
pub(crate) struct MemKVStore(DashMap<ContractKey, WrappedState>);

#[async_trait::async_trait]
impl StateStorage for MemKVStore {
    type Error = String;

    async fn store(
        &mut self,
        _key: ContractKey,
        _state: locutus_runtime::WrappedState,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn get(
        &self,
        _key: &ContractKey,
    ) -> Result<Option<locutus_runtime::WrappedState>, Self::Error> {
        todo!()
    }

    async fn store_params(
        &mut self,
        _key: ContractKey,
        _state: locutus_runtime::Parameters<'static>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn get_params<'a>(
        &'a self,
        _key: &'a ContractKey,
    ) -> std::pin::Pin<
        Box<
            dyn futures::Future<
                    Output = Result<Option<locutus_runtime::Parameters<'static>>, Self::Error>,
                > + Send
                + 'a,
        >,
    > {
        todo!()
    }
}

impl MemKVStore {
    pub fn new() -> Self {
        Self::default()
    }
}

pub(crate) struct MemoryContractHandler<KVStore = MemKVStore>
where
    KVStore: StateStorage,
{
    channel: ContractHandlerChannel<CHListenerHalve>,
    kv_store: StateStore<KVStore>,
    contract_store: ContractStore,
    _runtime: MockRuntime,
}

impl<KVStore> MemoryContractHandler<KVStore>
where
    KVStore: StateStorage + Send + Sync + 'static,
    <KVStore as StateStorage>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    const MAX_MEM_CACHE: i64 = 10_000_000;

    pub fn new(channel: ContractHandlerChannel<CHListenerHalve>, kv_store: KVStore) -> Self {
        MemoryContractHandler {
            channel,
            kv_store: StateStore::new(kv_store, 10_000_000).unwrap(),
            contract_store: ContractStore::new(
                CONFIG.config_paths.contracts_dir.clone(),
                Self::MAX_MEM_CACHE,
            )
            .unwrap(),
            _runtime: MockRuntime {},
        }
    }
}

impl From<ContractHandlerChannel<CHListenerHalve>> for MemoryContractHandler {
    fn from(channel: ContractHandlerChannel<CHListenerHalve>) -> Self {
        let store = MemKVStore::new();
        MemoryContractHandler::new(channel, store)
    }
}

impl ContractHandler for MemoryContractHandler {
    type Store = MemKVStore;

    #[inline(always)]
    fn channel(&mut self) -> &mut ContractHandlerChannel<CHListenerHalve> {
        &mut self.channel
    }

    #[inline(always)]
    fn contract_store(&mut self) -> &mut ContractStore {
        &mut self.contract_store
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        _req: ClientRequest<'a>,
    ) -> BoxFuture<'static, Result<HostResponse, DynError>> {
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

    #[ignore]
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
