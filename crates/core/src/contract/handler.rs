#![allow(unused)] // FIXME: remove this
use std::collections::BTreeMap;
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
use super::ExecutorError;
use super::{
    executor::{ContractExecutor, Executor},
    ContractError,
};
use crate::client_events::HostResult;
use crate::message::Transaction;
use crate::{client_events::ClientId, node::PeerCliConfig, runtime::Runtime, DynError};

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
        contract_handler_channel: ContractHandlerChannel<ContractHandlerHalve>,
        executor_request_sender: ExecutorToEventLoopChannel<ExecutorHalve>,
        builder: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static;

    fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve>;

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
    channel: ContractHandlerChannel<ContractHandlerHalve>,
}

impl ContractHandler for NetworkContractHandler<Runtime> {
    type Builder = PeerCliConfig;
    type ContractExecutor = Executor<Runtime>;

    fn build(
        channel: ContractHandlerChannel<ContractHandlerHalve>,
        executor_request_sender: ExecutorToEventLoopChannel<ExecutorHalve>,
        config: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        async {
            let mut executor = Executor::from_config(config, Some(executor_request_sender)).await?;
            Ok(Self { executor, channel })
        }
        .boxed()
    }

    fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve> {
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
    type Builder = String;
    type ContractExecutor = Executor<super::MockRuntime>;

    fn build(
        channel: ContractHandlerChannel<ContractHandlerHalve>,
        executor_request_sender: ExecutorToEventLoopChannel<ExecutorHalve>,
        identifier: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        async move {
            let executor = Executor::new_mock(&identifier, executor_request_sender).await?;
            Ok(Self { executor, channel })
        }
        .boxed()
    }

    fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve> {
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
    transaction: Option<Transaction>,
}

impl EventId {
    pub fn client_id(&self) -> Option<ClientId> {
        self.client_id
    }

    pub fn transaction(&self) -> Option<Transaction> {
        self.transaction
    }

    // FIXME: this should be used somewhere to inform than an event is pending
    // a transaction resolution
    pub fn with_transaction(&mut self, transaction: Transaction) {
        self.transaction = Some(transaction);
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
pub(crate) struct ContractHandlerChannel<End: sealed::ChannelHalve> {
    rx: mpsc::UnboundedReceiver<InternalCHEvent>,
    tx: mpsc::UnboundedSender<InternalCHEvent>,
    queue: BTreeMap<u64, ContractHandlerEvent>,
    _halve: PhantomData<End>,
}

pub(crate) struct ContractHandlerHalve;
pub(crate) struct SenderHalve;

mod sealed {
    use super::{ContractHandlerHalve, SenderHalve};
    pub(crate) trait ChannelHalve {}
    impl ChannelHalve for ContractHandlerHalve {}
    impl ChannelHalve for SenderHalve {}
}

pub(crate) fn contract_handler_channel() -> (
    ContractHandlerChannel<SenderHalve>,
    ContractHandlerChannel<ContractHandlerHalve>,
) {
    let (notification_tx, notification_channel) = mpsc::unbounded_channel();
    let (ch_tx, ch_listener) = mpsc::unbounded_channel();
    (
        ContractHandlerChannel {
            rx: notification_channel,
            tx: ch_tx,
            queue: BTreeMap::new(),
            _halve: PhantomData,
        },
        ContractHandlerChannel {
            rx: ch_listener,
            tx: notification_tx,
            queue: BTreeMap::new(),
            _halve: PhantomData,
        },
    )
}

static EV_ID: AtomicU64 = AtomicU64::new(0);

impl ContractHandlerChannel<SenderHalve> {
    // TODO: the timeout should be derived from whatever is the worst
    // case we are willing to accept for waiting out for an event;
    // have to double check all events to see if any depend on external
    // responses and go from there, also this may very well depend on the
    // kind of event and can be optimized on a case basis
    const CH_EV_RESPONSE_TIME_OUT: Duration = Duration::from_secs(300);

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
                if started_op.elapsed() > Self::CH_EV_RESPONSE_TIME_OUT {
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

    pub async fn recv_from_handler(&mut self) -> EventId {
        todo!()
    }
}

impl ContractHandlerChannel<ContractHandlerHalve> {
    pub async fn send_to_sender(
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

    pub async fn recv_from_sender(
        &mut self,
    ) -> Result<(EventId, ContractHandlerEvent), ContractError> {
        if let Some(msg) = self.rx.recv().await {
            return Ok((
                EventId {
                    id: msg.id,
                    client_id: msg.client_id,
                    transaction: None,
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
        related_contracts: RelatedContracts<'static>,
        contract: Option<ContractContainer>,
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
    CacheResult(Result<(), ExecutorError>),
}

impl std::fmt::Display for ContractHandlerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContractHandlerEvent::PutQuery { key, contract, .. } => {
                if let Some(contract) = contract {
                    write!(f, "put query {{ {key}, params: {:?} }}", contract.params())
                } else {
                    write!(f, "put query {{ {key} }}")
                }
            }
            ContractHandlerEvent::PutResponse { new_value } => match new_value {
                Ok(v) => {
                    write!(f, "put query response {{ {v} }}",)
                }
                Err(e) => {
                    write!(f, "put query failed {{ {e} }}",)
                }
            },
            ContractHandlerEvent::GetQuery {
                key,
                fetch_contract,
            } => {
                write!(f, "get query {{ {key}, fetch contract: {fetch_contract} }}",)
            }
            ContractHandlerEvent::GetResponse { key, response } => match response {
                Ok(_) => {
                    write!(f, "get query response {{ {key} }}",)
                }
                Err(_) => {
                    write!(f, "get query failed {{ {key} }}",)
                }
            },
            ContractHandlerEvent::Cache(container) => {
                write!(f, "caching {{ {} }}", container.key())
            }
            ContractHandlerEvent::CacheResult(r) => {
                write!(f, "caching result {{ {} }}", r.is_ok())
            }
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::sync::Arc;

    use freenet_stdlib::prelude::*;

    use super::*;
    use crate::config::GlobalExecutor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_test() -> Result<(), anyhow::Error> {
        let (mut send_halve, mut rcv_halve) = contract_handler_channel();

        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2, 3])),
            Parameters::from(vec![4, 5]),
        )));

        let contract_cp = contract.clone();
        let h = GlobalExecutor::spawn(async move {
            send_halve
                .send_to_handler(ContractHandlerEvent::Cache(contract_cp), None)
                .await
        });
        let (id, ev) =
            tokio::time::timeout(Duration::from_millis(100), rcv_halve.recv_from_sender())
                .await??;

        let ContractHandlerEvent::Cache(contract) = ev else {
            anyhow::bail!("invalid event");
        };
        assert_eq!(contract.data(), vec![0, 1, 2, 3]);

        tokio::time::timeout(
            Duration::from_millis(100),
            rcv_halve.send_to_sender(id, ContractHandlerEvent::Cache(contract)),
        )
        .await??;
        let ContractHandlerEvent::Cache(contract) = h.await?? else {
            anyhow::bail!("invalid event!");
        };
        assert_eq!(contract.data(), vec![0, 1, 2, 3]);

        Ok(())
    }
}
