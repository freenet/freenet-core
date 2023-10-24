use crate::{client_events::ClientId, runtime::ContractStore};
use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::WrappedContract,
};
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::mpsc::UnboundedSender;

use super::{
    executor::{ExecutorHalve, ExecutorToEventLoopChannel},
    handler::{ContractHandler, ContractHandlerHalve, ContractHandlerToEventLoopChannel},
    Executor,
};
use crate::DynError;

pub(crate) struct MockRuntime {
    pub contract_store: ContractStore,
}

pub(crate) struct MemoryContractHandler {
    channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
    runtime: Executor<MockRuntime>,
}

impl MemoryContractHandler {
    pub async fn new(
        channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
        data_dir: &str,
    ) -> Self {
        MemoryContractHandler {
            channel,
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
        async move { Ok(MemoryContractHandler::new(channel, &config).await) }.boxed()
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
        unreachable!()
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
