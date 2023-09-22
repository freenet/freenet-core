#![allow(unused)] // FIXME: remove this

use std::collections::{BTreeMap, VecDeque};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::{Duration, Instant};

use freenet_stdlib::client_api::{ClientError, ClientRequest, HostResponse};
use freenet_stdlib::prelude::*;
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::executor::{ExecutorHalve, ExecutorToEventLoopChannel};
use super::{
    executor::{ContractExecutor, Executor},
    ContractError,
};
use crate::client_events::HostResult;
use crate::message::Transaction;
use crate::node::OpManager;
use crate::{
    client_events::ClientId,
    node::NodeConfig,
    runtime::{ContractStore, Runtime, StateStorage, StateStore},
    DynError,
};

pub const MAX_MEM_CACHE: i64 = 10_000_000;

pub(crate) struct ClientResponses(UnboundedReceiver<(ClientId, HostResult)>);

impl ClientResponses {
    pub fn channel() -> (Self, ClientResponsesSender) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self(rx), ClientResponsesSender(tx))
    }
}

impl std::ops::Deref for ClientResponses {
    type Target = UnboundedReceiver<(ClientId, HostResult)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ClientResponses {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone)]
pub(crate) struct ClientResponsesSender(UnboundedSender<(ClientId, HostResult)>);

impl std::ops::Deref for ClientResponsesSender {
    type Target = UnboundedSender<(ClientId, HostResult)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) trait ContractHandler {
    type Builder;
    type ContractExecutor: ContractExecutor;

    fn build(
        contract_handler_channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
        executor_request_sender: ExecutorToEventLoopChannel<ExecutorHalve>,
        builder: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static;

    fn channel(&mut self) -> &mut ContractHandlerToEventLoopChannel<ContractHandlerHalve>;

    /// # Arguments
    /// - updates: channel to send back updates from contracts to whoever is subscribed to the contract.
    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
        client_id: ClientId,
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> BoxFuture<'a, Result<HostResponse, DynError>>;

    fn executor(&mut self) -> &mut Self::ContractExecutor;
}

pub(crate) struct NetworkContractHandler<R = Runtime> {
    executor: Executor<R>,
    channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
}

impl ContractHandler for NetworkContractHandler<Runtime> {
    type Builder = NodeConfig;
    type ContractExecutor = Executor<Runtime>;

    fn build(
        channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
        executor_request_sender: ExecutorToEventLoopChannel<ExecutorHalve>,
        config: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        async {
            let mut executor = Executor::from_config(config).await?;
            executor.event_loop_channel(executor_request_sender);
            Ok(Self { executor, channel })
        }
        .boxed()
    }

    fn channel(&mut self) -> &mut ContractHandlerToEventLoopChannel<ContractHandlerHalve> {
        &mut self.channel
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
        client_id: ClientId,
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> BoxFuture<'a, Result<HostResponse, DynError>> {
        async move {
            let res = self
                .executor
                .handle_request(client_id, req, updates)
                .await?;
            Ok(res)
        }
        .boxed()
    }

    fn executor(&mut self) -> &mut Self::ContractExecutor {
        &mut self.executor
    }
}

#[cfg(test)]
impl ContractHandler for NetworkContractHandler<super::MockRuntime> {
    type Builder = ();
    type ContractExecutor = Executor<super::MockRuntime>;

    fn build(
        channel: ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
        _executor_request_sender: ExecutorToEventLoopChannel<ExecutorHalve>,
        _builder: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        async {
            let executor = Executor::new_mock().await?;
            Ok(Self { executor, channel })
        }
        .boxed()
    }

    fn channel(&mut self) -> &mut ContractHandlerToEventLoopChannel<ContractHandlerHalve> {
        &mut self.channel
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
        client_id: ClientId,
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> BoxFuture<'a, Result<HostResponse, DynError>> {
        async move {
            let res = self
                .executor
                .handle_request(client_id, req, updates)
                .await?;
            Ok(res)
        }
        .boxed()
    }

    fn executor(&mut self) -> &mut Self::ContractExecutor {
        &mut self.executor
    }
}

#[derive(Eq)]
pub(crate) struct EventId {
    id: u64,
    client_id: Option<ClientId>,
}

impl EventId {
    pub fn client_id(&self) -> Option<ClientId> {
        self.client_id
    }
}

impl PartialEq for EventId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for EventId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
pub(crate) struct ContractHandlerToEventLoopChannel<End: sealed::ChannelHalve> {
    rx: mpsc::UnboundedReceiver<InternalCHEvent>,
    tx: mpsc::UnboundedSender<InternalCHEvent>,
    queue: BTreeMap<u64, ContractHandlerEvent>,
    _halve: PhantomData<End>,
}

pub(crate) struct ContractHandlerHalve;
pub(crate) struct NetEventListener;

mod sealed {
    use super::{ContractHandlerHalve, NetEventListener};
    pub(crate) trait ChannelHalve {}
    impl ChannelHalve for ContractHandlerHalve {}
    impl ChannelHalve for NetEventListener {}
}

pub(crate) fn contract_handler_channel() -> (
    ContractHandlerToEventLoopChannel<NetEventListener>,
    ContractHandlerToEventLoopChannel<ContractHandlerHalve>,
) {
    let (notification_tx, notification_channel) = mpsc::unbounded_channel();
    let (ch_tx, ch_listener) = mpsc::unbounded_channel();
    (
        ContractHandlerToEventLoopChannel {
            rx: notification_channel,
            tx: ch_tx,
            queue: BTreeMap::new(),
            _halve: PhantomData,
        },
        ContractHandlerToEventLoopChannel {
            rx: ch_listener,
            tx: notification_tx,
            queue: BTreeMap::new(),
            _halve: PhantomData,
        },
    )
}

static EV_ID: AtomicU64 = AtomicU64::new(0);

// TODO: the timeout should be derived from whatever is the worst
// case we are willing to accept for waiting out for an event;
// have to double check all events to see if any depend on external
// responses and go from there, also this may very well depend on the
// kind of event and can be optimized on a case basis
const CH_EV_RESPONSE_TIME_OUT: Duration = Duration::from_secs(300);

impl ContractHandlerToEventLoopChannel<NetEventListener> {
    /// Send an event to the contract handler and receive a response event if successful.
    pub async fn send_to_handler(
        &mut self,
        ev: ContractHandlerEvent,
        client_id: Option<ClientId>,
    ) -> Result<ContractHandlerEvent, ContractError> {
        let id = EV_ID.fetch_add(1, SeqCst);
        self.tx
            .send(InternalCHEvent { ev, id, client_id })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))?;
        if let Some(handler) = self.queue.remove(&id) {
            Ok(handler)
        } else {
            let started_op = Instant::now();
            loop {
                if started_op.elapsed() > CH_EV_RESPONSE_TIME_OUT {
                    break Err(ContractError::NoEvHandlerResponse);
                }
                while let Some(msg) = self.rx.recv().await {
                    if msg.id == id {
                        return Ok(msg.ev);
                    } else {
                        self.queue.insert(id, msg.ev); // should never be duplicates
                    }
                }
                tokio::time::sleep(Duration::from_nanos(100)).await;
            }
        }
    }

    // todo: use
    pub async fn recv_from_handler(&mut self) -> (EventId, ContractHandlerEvent) {
        todo!()
    }
}

impl ContractHandlerToEventLoopChannel<ContractHandlerHalve> {
    pub async fn send_to_event_loop(
        &self,
        id: EventId,
        ev: ContractHandlerEvent,
    ) -> Result<(), ContractError> {
        self.tx
            .send(InternalCHEvent {
                ev,
                id: id.id,
                client_id: id.client_id,
            })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))
    }

    pub async fn recv_from_event_loop(
        &mut self,
    ) -> Result<(EventId, ContractHandlerEvent), ContractError> {
        if let Some(msg) = self.rx.recv().await {
            return Ok((
                EventId {
                    id: msg.id,
                    client_id: msg.client_id,
                },
                msg.ev,
            ));
        }
        Err(ContractError::NoEvHandlerResponse)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoreResponse {
    pub state: Option<WrappedState>,
    pub contract: Option<ContractContainer>,
}

struct InternalCHEvent {
    ev: ContractHandlerEvent,
    id: u64,
    client_id: Option<ClientId>,
}

#[derive(Debug)]
pub(crate) enum ContractHandlerEvent {
    /// Try to push/put a new value into the contract.
    PutQuery {
        key: ContractKey,
        state: WrappedState,
    },
    /// The response to a push query.
    PutResponse {
        new_value: Result<WrappedState, DynError>,
    },
    /// Fetch a supposedly existing contract value in this node, and optionally the contract itself.  
    GetQuery {
        key: ContractKey,
        fetch_contract: bool,
    },
    /// The response to a FetchQuery event
    GetResponse {
        key: ContractKey,
        response: Result<StoreResponse, DynError>,
    },
    /// Store a contract in the local store.
    Cache(ContractContainer),
    /// Result of a caching operation.
    CacheResult(Result<(), ContractError>),
}

impl ContractHandlerEvent {
    pub async fn into_network_op(self, op_manager: &OpManager) -> Transaction {
        todo!()
    }
}

#[cfg(test)]
pub mod test {
    use std::sync::Arc;

    use crate::runtime::ContractStore;
    use freenet_stdlib::{
        client_api::{ClientRequest, HostResponse},
        prelude::*,
    };

    use super::*;
    use crate::{config::GlobalExecutor, contract::MockRuntime};

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_test() -> Result<(), anyhow::Error> {
        let (mut send_halve, mut rcv_halve) = contract_handler_channel();

        let h = GlobalExecutor::spawn(async move {
            let contract =
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
                    Arc::new(ContractCode::from(vec![0, 1, 2, 3])),
                    Parameters::from(vec![]),
                )));
            send_halve
                .send_to_handler(ContractHandlerEvent::Cache(contract), None)
                .await
        });

        let (id, ev) =
            tokio::time::timeout(Duration::from_millis(100), rcv_halve.recv_from_event_loop())
                .await??;

        if let ContractHandlerEvent::Cache(contract) = ev {
            let data: Vec<u8> = contract.data();
            assert_eq!(data, vec![0, 1, 2, 3]);
            let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                WrappedContract::new(Arc::new(ContractCode::from(data)), Parameters::from(vec![])),
            ));
            tokio::time::timeout(
                Duration::from_millis(100),
                rcv_halve.send_to_event_loop(id, ContractHandlerEvent::Cache(contract)),
            )
            .await??;
        } else {
            anyhow::bail!("invalid event");
        }

        if let ContractHandlerEvent::Cache(contract) = h.await?? {
            let data: Vec<u8> = contract.data();
            assert_eq!(data, vec![0, 1, 2, 3]);
        } else {
            anyhow::bail!("invalid event!");
        }

        Ok(())
    }
}
