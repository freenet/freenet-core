#![allow(unused)] // FIXME: remove this

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::FutureExt;
use locutus_runtime::{
    ContractContainer, ContractStore, Parameters, Runtime, StateStorage, StateStore,
};
use locutus_stdlib::client_api::{ClientRequest, HostResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::contract::{ContractError, ContractKey};
use crate::{ClientId, DynError, Executor, NodeConfig, WrappedState};

pub const MAX_MEM_CACHE: i64 = 10_000_000;

pub(crate) trait ContractHandler {
    type Builder;
    type Runtime;

    fn build(
        channel: ContractHandlerChannel<CHListenerHalve>,
        config: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static;

    fn channel(&mut self) -> &mut ContractHandlerChannel<CHListenerHalve>;

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
    ) -> BoxFuture<'a, Result<HostResponse, DynError>>;

    fn executor(&mut self) -> &mut Executor<Self::Runtime>;
}

pub(crate) struct NetworkContractHandler<R = Runtime> {
    executor: Executor<R>,
    channel: ContractHandlerChannel<CHListenerHalve>,
}

impl ContractHandler for NetworkContractHandler<Runtime> {
    type Builder = NodeConfig;
    type Runtime = Runtime;

    fn build(
        channel: ContractHandlerChannel<CHListenerHalve>,
        config: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        async {
            let executor = Executor::from_config(config).await?;
            Ok(Self { executor, channel })
        }
        .boxed()
    }

    fn channel(&mut self) -> &mut ContractHandlerChannel<CHListenerHalve> {
        &mut self.channel
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
    ) -> BoxFuture<'a, Result<HostResponse, DynError>> {
        async {
            // FIXME: pass the missing arguments in the fn
            let updates = None;
            let res = self
                .executor
                .handle_request(ClientId::FIRST, req, updates)
                .await?;
            Ok(res)
        }
        .boxed()
    }

    fn executor(&mut self) -> &mut Executor {
        &mut self.executor
    }
}

#[cfg(test)]
impl ContractHandler for NetworkContractHandler<super::MockRuntime> {
    type Builder = NodeConfig;
    type Runtime = super::MockRuntime;

    fn build(
        channel: ContractHandlerChannel<CHListenerHalve>,
        config: Self::Builder,
    ) -> BoxFuture<'static, Result<Self, DynError>>
    where
        Self: Sized + 'static,
    {
        // async {
        //     let executor = Executor::from_config(config).await?;
        //     Ok(Self { executor, channel })
        // }
        // .boxed()
        todo!()
    }

    fn channel(&mut self) -> &mut ContractHandlerChannel<CHListenerHalve> {
        &mut self.channel
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
    ) -> BoxFuture<'a, Result<HostResponse, DynError>> {
        // async {
        //     // FIXME: pass the missing arguments in the fn
        //     let updates = None;
        //     let res = self
        //         .executor
        //         .handle_request(ClientId::FIRST, req, updates)
        //         .await?;
        //     Ok(res)
        // }
        // .boxed()
        todo!()
    }

    fn executor(&mut self) -> &mut Executor<Self::Runtime> {
        &mut self.executor
    }
}

pub struct EventId(u64);

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
pub(crate) struct ContractHandlerChannel<End> {
    rx: mpsc::UnboundedReceiver<InternalCHEvent>,
    tx: mpsc::UnboundedSender<InternalCHEvent>,
    //TODO:  change queue to btree once pop_first is stabilized
    // (https://github.com/rust-lang/rust/issues/62924)
    queue: VecDeque<(u64, ContractHandlerEvent)>,
    _halve: PhantomData<End>,
}

pub(crate) struct CHListenerHalve;
pub(crate) struct CHSenderHalve;

mod sealed {
    use super::{CHListenerHalve, CHSenderHalve};
    pub(crate) trait ChannelHalve {}
    impl ChannelHalve for CHListenerHalve {}
    impl ChannelHalve for CHSenderHalve {}
}

pub(crate) fn contract_handler_channel() -> (
    ContractHandlerChannel<CHSenderHalve>,
    ContractHandlerChannel<CHListenerHalve>,
) {
    let (notification_tx, notification_channel) = mpsc::unbounded_channel();
    let (ch_tx, ch_listener) = mpsc::unbounded_channel();
    (
        ContractHandlerChannel {
            rx: notification_channel,
            tx: ch_tx,
            queue: VecDeque::new(),
            _halve: PhantomData,
        },
        ContractHandlerChannel {
            rx: ch_listener,
            tx: notification_tx,
            queue: VecDeque::new(),
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

impl ContractHandlerChannel<CHSenderHalve> {
    /// Send an event to the contract handler and receive a response event if successful.
    pub async fn send_to_handler(
        &mut self,
        ev: ContractHandlerEvent,
    ) -> Result<ContractHandlerEvent, ContractError> {
        let id = EV_ID.fetch_add(1, SeqCst);
        self.tx
            .send(InternalCHEvent { ev, id })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))?;
        if let Ok(pos) = self.queue.binary_search_by_key(&id, |(k, _v)| *k) {
            Ok(self.queue.remove(pos).unwrap().1)
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
                        self.queue.push_front((id, msg.ev)); // should never be duplicates
                    }
                }
                tokio::time::sleep(Duration::from_nanos(100)).await;
            }
        }
    }
}

impl ContractHandlerChannel<CHListenerHalve> {
    pub async fn send_to_listener(
        &self,
        id: EventId,
        ev: ContractHandlerEvent,
    ) -> Result<(), ContractError> {
        self.tx
            .send(InternalCHEvent { ev, id: id.0 })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))
    }

    pub async fn recv_from_listener(
        &mut self,
    ) -> Result<(EventId, ContractHandlerEvent), ContractError> {
        if let Some((id, ev)) = self.queue.pop_front() {
            return Ok((EventId(id), ev));
        }
        if let Some(msg) = self.rx.recv().await {
            return Ok((EventId(msg.id), msg.ev));
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

#[cfg(test)]
pub mod test {
    use std::sync::Arc;

    use locutus_runtime::{ContractStore, ContractWasmAPIVersion};
    use locutus_stdlib::{
        client_api::{ClientRequest, HostResponse},
        prelude::ContractCode,
    };

    use super::*;
    use crate::{
        config::GlobalExecutor,
        contract::{
            test::{MemKVStore, SimStoreError},
            MockRuntime,
        },
        WrappedContract,
    };

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
                .send_to_handler(ContractHandlerEvent::Cache(contract))
                .await
        });

        let (id, ev) =
            tokio::time::timeout(Duration::from_millis(100), rcv_halve.recv_from_listener())
                .await??;

        if let ContractHandlerEvent::Cache(contract) = ev {
            let data: Vec<u8> = contract.data();
            assert_eq!(data, vec![0, 1, 2, 3]);
            let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                WrappedContract::new(Arc::new(ContractCode::from(data)), Parameters::from(vec![])),
            ));
            tokio::time::timeout(
                Duration::from_millis(100),
                rcv_halve.send_to_listener(id, ContractHandlerEvent::Cache(contract)),
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
