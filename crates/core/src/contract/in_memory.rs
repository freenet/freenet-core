use crate::{
    client_events::ClientId,
    runtime::{ContractStore, StateStorage, StateStore},
};
use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::WrappedContract,
};
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc::UnboundedSender;

use super::{
    executor::{ExecutorHalve, ExecutorToEventLoopChannel},
    handler::{ContractHandler, ContractHandlerHalve, ContractHandlerToEventLoopChannel},
    storages::in_memory::MemKVStore,
    Executor,
};
use crate::DynError;

pub(crate) struct MockRuntime {
    pub contract_store: ContractStore,
}

pub(crate) struct MemoryContractHandler<KVStore = MemKVStore>
where
    KVStore: StateStorage,
{
    channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
    _kv_store: StateStore<KVStore>,
    runtime: Executor<MockRuntime>,
}

impl<KVStore> MemoryContractHandler<KVStore>
where
    KVStore: StateStorage + Send + Sync + 'static,
    <KVStore as StateStorage>::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    pub async fn new(
        channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
        kv_store: KVStore,
        data_dir: &str,
    ) -> Self {
        // let rt = MockRuntime {
        //     contract_store: ContractStore::new(
        //         Config::conf().contracts_dir(),
        //         Self::MAX_MEM_CACHE,
        //     )
        //     .unwrap();
        MemoryContractHandler {
            channel,
            _kv_store: StateStore::new(kv_store, 10_000_000).unwrap(),
            runtime: Executor::new_mock(data_dir).await.unwrap(),
        }
    }
}

impl ContractHandler for MemoryContractHandler {
    type Builder = String;
    type ContractExecutor = Executor<MockRuntime>;

    fn build(
        channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
        _executor_request_sender: ExecutorToEventLoopChannel<ExecutorHalve>,
        config: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        let store = MemKVStore::new();
        async move { Ok(MemoryContractHandler::new(channel, store, &config).await) }.boxed()
    }

    fn channel(&mut self) -> &mut ContractHandlerToEventLoopChannel<ContractHandlerHalve> {
        &mut self.channel
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        _req: ClientRequest<'a>,
        _client_id: ClientId,
        _updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> BoxFuture<'static, Result<HostResponse, DynError>> {
        todo!()
    }

    fn executor(&mut self) -> &mut Self::ContractExecutor {
        &mut self.runtime
    }
}

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
