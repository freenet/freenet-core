use dashmap::DashMap;
use futures::{future::BoxFuture, FutureExt};
use locutus_runtime::{ContractKey, ContractStore, StateStorage, StateStore};
use locutus_stdlib::client_api::{ClientRequest, HostResponse};

use super::{
    handler::{CHListenerHalve, ContractHandler, ContractHandlerChannel},
    Executor,
};
use crate::{config::Config, DynError, WrappedContract, WrappedState};

pub(crate) struct MockRuntime {}

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

    async fn get_params<'a>(
        &'a self,
        _key: &'a ContractKey,
    ) -> Result<Option<locutus_runtime::Parameters<'static>>, Self::Error> {
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
    _kv_store: StateStore<KVStore>,
    _contract_store: ContractStore,
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
            _kv_store: StateStore::new(kv_store, 10_000_000).unwrap(),
            _contract_store: ContractStore::new(
                Config::get_static_conf().config_paths.contracts_dir.clone(),
                Self::MAX_MEM_CACHE,
            )
            .unwrap(),
            _runtime: MockRuntime {},
        }
    }
}

impl ContractHandler for MemoryContractHandler {
    type Builder = ();
    type ContractExecutor = Executor<MockRuntime>;

    fn build(
        channel: ContractHandlerChannel<CHListenerHalve>,
        _config: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        let store = MemKVStore::new();
        async move { Ok(MemoryContractHandler::new(channel, store)) }.boxed()
    }

    fn channel(&mut self) -> &mut ContractHandlerChannel<CHListenerHalve> {
        &mut self.channel
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        _req: ClientRequest<'a>,
    ) -> BoxFuture<'static, Result<HostResponse, DynError>> {
        todo!()
    }

    fn executor(&mut self) -> &mut Self::ContractExecutor {
        todo!()
    }
}

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
