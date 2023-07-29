#![allow(unused)] // FIXME: remove this

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use locutus_runtime::{ContractContainer, ContractStore, Parameters, StateStorage, StateStore};
use locutus_stdlib::client_api::{ClientRequest, HostResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::contract::{ContractError, ContractKey};
use crate::WrappedState;

pub const MAX_MEM_CACHE: i64 = 10_000_000;

pub(crate) trait ContractHandler:
    From<ContractHandlerChannel<Self::Error, CHListenerHalve>>
{
    type Error: std::error::Error;
    type Store: StateStorage;

    fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve>;

    fn contract_store(&mut self) -> &mut ContractStore;

    fn state_store(&mut self) -> &mut StateStore<Self::Store>;

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
    ) -> BoxFuture<'a, Result<HostResponse, Self::Error>>;
}

pub struct EventId(u64);

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
pub(crate) struct ContractHandlerChannel<CErr, End> {
    rx: mpsc::UnboundedReceiver<InternalCHEvent<CErr>>,
    tx: mpsc::UnboundedSender<InternalCHEvent<CErr>>,
    //TODO:  change queue to btree once pop_first is stabilized
    // (https://github.com/rust-lang/rust/issues/62924)
    queue: VecDeque<(u64, ContractHandlerEvent<CErr>)>,
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

pub(crate) fn contract_handler_channel<CErr>() -> (
    ContractHandlerChannel<CErr, CHSenderHalve>,
    ContractHandlerChannel<CErr, CHListenerHalve>,
)
where
    CErr: std::error::Error,
{
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

impl<CErr: std::error::Error> ContractHandlerChannel<CErr, CHSenderHalve> {
    /// Send an event to the contract handler and receive a response event if successful.
    pub async fn send_to_handler(
        &mut self,
        ev: ContractHandlerEvent<CErr>,
    ) -> Result<ContractHandlerEvent<CErr>, ContractError<CErr>> {
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

impl<CErr> ContractHandlerChannel<CErr, CHListenerHalve> {
    pub async fn send_to_listener(
        &self,
        id: EventId,
        ev: ContractHandlerEvent<CErr>,
    ) -> Result<(), ContractError<CErr>> {
        self.tx
            .send(InternalCHEvent { ev, id: id.0 })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))
    }

    pub async fn recv_from_listener(
        &mut self,
    ) -> Result<(EventId, ContractHandlerEvent<CErr>), ContractError<CErr>> {
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

struct InternalCHEvent<CErr> {
    ev: ContractHandlerEvent<CErr>,
    id: u64,
}

#[derive(Debug)]
pub(crate) enum ContractHandlerEvent<Err> {
    /// Try to push/put a new value into the contract.
    PushQuery {
        key: ContractKey,
        state: WrappedState,
    },
    /// The response to a push query.
    PushResponse {
        new_value: Result<WrappedState, Err>,
    },
    /// Fetch a supposedly existing contract value in this node, and optionally the contract itself.  
    FetchQuery {
        key: ContractKey,
        fetch_contract: bool,
    },
    /// The response to a FetchQuery event
    FetchResponse {
        key: ContractKey,
        response: Result<StoreResponse, Err>,
    },
    /// Store a contract in the local store.
    Cache(ContractContainer),
    /// Result of a caching operation.
    CacheResult(Result<(), ContractError<Err>>),
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
        config::{GlobalExecutor, CONFIG},
        contract::{
            test::{MemKVStore, SimStoreError},
            MockRuntime,
        },
        WrappedContract,
    };

    #[derive(Debug, thiserror::Error)]
    pub(crate) enum TestContractStoreError {
        #[error(transparent)]
        IOError(#[from] std::io::Error),
    }

    pub(crate) struct TestContractHandler {
        channel: ContractHandlerChannel<TestContractStoreError, CHListenerHalve>,
        kv_store: MemKVStore,
        contract_store: ContractStore,
        _runtime: MockRuntime,
    }

    impl TestContractHandler {
        pub fn new(
            channel: ContractHandlerChannel<TestContractStoreError, CHListenerHalve>,
        ) -> Self {
            TestContractHandler {
                channel,
                kv_store: MemKVStore::new(),
                contract_store: ContractStore::new(
                    CONFIG.config_paths.contracts_dir.clone(),
                    MAX_MEM_CACHE,
                )
                .unwrap(),
                _runtime: MockRuntime {},
            }
        }
    }

    impl From<ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>>
        for TestContractHandler
    {
        fn from(
            channel: ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>,
        ) -> Self {
            TestContractHandler::new(channel)
        }
    }

    impl ContractHandler for TestContractHandler {
        type Error = TestContractStoreError;
        type Store = MemKVStore;

        #[inline(always)]
        fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve> {
            &mut self.channel
        }

        #[inline(always)]
        fn contract_store(&mut self) -> &mut ContractStore {
            &mut self.contract_store
        }

        fn handle_request<'a, 's: 'a>(
            &'s mut self,
            _req: ClientRequest<'a>,
        ) -> BoxFuture<'a, Result<HostResponse, Self::Error>> {
            // async fn get_state(
            //     &self,
            //     contract: &ContractKey,
            // ) -> Result<Option<WrappedState>, Self::Error> {
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

        fn state_store(&mut self) -> &mut StateStore<Self::Store> {
            todo!()
        }
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_test() -> Result<(), anyhow::Error> {
        let (mut send_halve, mut rcv_halve) = contract_handler_channel::<SimStoreError>();

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
