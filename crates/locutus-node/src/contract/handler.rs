use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::{Duration, Instant};

use locutus_runtime::{
    ContractStore, Parameters, RuntimeResult, State, StateDelta, StateStore, StateSummary,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::contract::{ContractError, ContractKey};
use crate::{ClientRequest, HostResponse, WrappedContract, WrappedState};
pub(crate) use sqlite::{SQLiteContractHandler, SqlDbError};

const MAX_MEM_CACHE: i64 = 10_000_000;

#[async_trait::async_trait]
pub(crate) trait ContractHandler:
    From<ContractHandlerChannel<Self::Error, CHListenerHalve>>
{
    type Error: std::error::Error;
    type Store;

    fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve>;

    fn contract_store(&mut self) -> &mut ContractStore;

    fn state_store(&mut self) -> &mut StateStore<Self::Store>;

    async fn handle_request(&mut self, req: ClientRequest) -> Result<HostResponse, Self::Error>;
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
                        let _ = self.queue.push_front((id, msg.ev)); // should never be duplicates
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
    pub contract: Option<WrappedContract<'static>>,
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
    Cache(WrappedContract<'static>),
    /// Result of a caching operation.
    CacheResult(Result<(), ContractError<Err>>),
}

pub(in crate::contract) mod sqlite {
    use std::str::FromStr;

    use locutus_runtime::{
        ContractRuntimeError, ContractStore, RuntimeInterface, StateStorage, StateStoreError,
    };
    use once_cell::sync::Lazy;
    use sqlx::{
        sqlite::{SqliteConnectOptions, SqliteRow},
        ConnectOptions, Row, SqlitePool,
    };

    use super::*;
    use crate::{config::CONFIG, contract::test::MockRuntime};

    pub struct Pool(SqlitePool);

    impl Default for Pool {
        fn default() -> Self {
            Self(POOL.clone())
        }
    }

    #[async_trait::async_trait]
    impl StateStorage for Pool {
        type Error = sqlx::Error;

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
        ) -> Result<locutus_runtime::WrappedState, Self::Error> {
            todo!()
        }
    }

    // Is fine to clone this as it wraps by an Arc.
    static POOL: Lazy<SqlitePool> = Lazy::new(|| {
        let mut opts = if cfg!(test) {
            SqliteConnectOptions::from_str("sqlite::memory:").unwrap()
        } else {
            let conn_str = CONFIG.config_paths.db_dir.join("locutus.db");
            SqliteConnectOptions::new()
                .create_if_missing(true)
                .filename(conn_str)
        };
        opts.log_statements(log::LevelFilter::Debug);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { SqlitePool::connect_with(opts).await })
        })
        .unwrap()
    });

    #[derive(Debug, thiserror::Error)]
    pub enum SqlDbError {
        #[error("Contract not found")]
        ContractNotFound,
        #[error(transparent)]
        SqliteError(#[from] sqlx::Error),
        #[error(transparent)]
        RuntimeError(#[from] ContractRuntimeError),
        #[error(transparent)]
        IOError(#[from] std::io::Error),
        #[error(transparent)]
        StateStore(#[from] StateStoreError),
    }

    pub struct SQLiteContractHandler<R> {
        channel: ContractHandlerChannel<SqlDbError, CHListenerHalve>,
        store: ContractStore,
        runtime: R,
        state_store: StateStore<Pool>,
        pub(super) pool: SqlitePool,
    }

    impl<R> SQLiteContractHandler<R>
    where
        R: RuntimeInterface + 'static,
    {
        /// max number of values stored in memory
        const MEM_CACHE_ITEMS: usize = 10_000;
        /// number of max bytes allowed to be stored in the cache
        const MEM_SIZE: u32 = 10_000_000;

        async fn new(
            channel: ContractHandlerChannel<SqlDbError, CHListenerHalve>,
            store: ContractStore,
            runtime: R,
        ) -> Result<Self, SqlDbError> {
            let pool = POOL.clone();
            Self::create_contracts_table().await?;
            Ok(SQLiteContractHandler {
                channel,
                store,
                runtime,
                state_store: StateStore::new(Pool(POOL.clone()), Self::MEM_SIZE)?,
                pool,
            })
        }

        async fn create_contracts_table() -> Result<(), SqlDbError> {
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS contracts (
                        key             STRING PRIMARY KEY,
                        value           BLOB
                    )",
            )
            .execute(&*POOL)
            .await?;
            Ok(())
        }
    }

    impl From<ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>>
        for SQLiteContractHandler<MockRuntime>
    {
        fn from(
            channel: ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>,
        ) -> Self {
            let store =
                ContractStore::new(CONFIG.config_paths.contracts_dir.clone(), MAX_MEM_CACHE);
            let runtime = MockRuntime {};
            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current()
                    .block_on(SQLiteContractHandler::new(channel, store, runtime))
            })
            .expect("handler initialization")
        }
    }

    #[async_trait::async_trait]
    impl ContractHandler for SQLiteContractHandler<MockRuntime> {
        type Error = SqlDbError;
        type Store = Pool;

        #[inline(always)]
        fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve> {
            &mut self.channel
        }

        #[inline(always)]
        fn contract_store(&mut self) -> &mut ContractStore {
            &mut self.store
        }

        async fn handle_request(
            &mut self,
            req: ClientRequest,
        ) -> Result<HostResponse, Self::Error> {
            // async fn update_state(
            //     &mut self,
            //     contract_key: &ContractKey,
            //     value: WrappedState,
            // ) -> Result<WrappedState, Self::Error> {
            //     let old_value = match self.get_state(contract_key).await {
            //         Ok(Some(contract_value)) => contract_value,
            //         Ok(None) => value.clone(),
            //         Err(_) => value.clone(),
            //     };

            //     let value = vec![];
            //     let value: Vec<u8> = self
            //         .runtime
            //         .update_value(contract_key, &*old_value, &*value)?;
            //     let encoded_key = bs58::encode(contract_key.as_ref())
            //         .with_alphabet(bs58::Alphabet::BITCOIN)
            //         .into_string();
            //     match sqlx::query(
            //         "INSERT OR REPLACE INTO contracts (key, value) VALUES ($1, $2) \
            //              RETURNING value",
            //     )
            //     .bind(encoded_key)
            //     .bind(value)
            //     .map(|row: SqliteRow| WrappedState::new(row.get("value")))
            //     .fetch_one(&self.pool)
            //     .await
            //     {
            //         Ok(contract_value) => {
            //             let size = contract_value.len() as i64;
            //             self.value_mem_cache
            //                 .insert(*contract_key, contract_value.clone(), size)
            //                 .await;
            //             Ok(contract_value)
            //         }
            //         Err(err) => {
            //             log::error!("{}", err);
            //             Err(SqlDbError::SqliteError(err))
            //         }
            //     }
            // }

            // async fn get_state(
            //     &self,
            //     contract_key: &ContractKey,
            // ) -> Result<Option<WrappedState>, Self::Error> {
            //     let encoded_key = bs58::encode(&**contract_key)
            //         .with_alphabet(bs58::Alphabet::BITCOIN)
            //         .into_string();
            //     if let Some(value) = self.value_mem_cache.get(contract_key) {
            //         return Ok(Some(value.value().clone()));
            //     }
            //     match sqlx::query("SELECT key, value FROM contracts WHERE key = ?")
            //         .bind(encoded_key)
            //         .map(|row: SqliteRow| Some(WrappedState::new(row.get("value"))))
            //         .fetch_one(&self.pool)
            //         .await
            //     {
            //         Ok(result) => Ok(result),
            //         Err(sqlx::Error::RowNotFound) => Ok(None),
            //         Err(_) => Err(SqlDbError::ContractNotFound),
            //     }
            // }
            todo!()
        }

        fn state_store(&mut self) -> &mut StateStore<Self::Store> {
            &mut self.state_store
        }
    }

    #[cfg(test)]
    mod test {
        use std::sync::Arc;

        use locutus_stdlib::prelude::ContractCode;

        use super::sqlite::SQLiteContractHandler;
        use super::*;

        // Prepare and get handler for an in-memory sqlite db
        async fn get_handler() -> Result<SQLiteContractHandler<MockRuntime>, SqlDbError> {
            let (_, ch_handler) = contract_handler_channel();
            let store: ContractStore =
                ContractStore::new(CONFIG.config_paths.contracts_dir.clone(), MAX_MEM_CACHE);
            SQLiteContractHandler::new(ch_handler, store, MockRuntime {}).await
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn contract_handler() -> Result<(), anyhow::Error> {
            // Create a sqlite handler and initialize the database
            let mut handler = get_handler().await?;

            // Generate a contract
            let contract_bytes = b"Test contract value".to_vec();
            let contract: WrappedContract = WrappedContract::new(
                Arc::new(ContractCode::from(contract_bytes.clone())),
                Parameters::from(vec![]),
            );

            // Get contract parts
            let state = WrappedState::new(contract_bytes.clone());
            handler
                .handle_request(ClientRequest::Put {
                    contract: contract.clone(),
                    state: state.clone(),
                })
                .await?
                .unwrap_put();
            let (get_result_value, _) = handler
                .handle_request(ClientRequest::Get {
                    key: *contract.key(),
                    fetch_contract: false,
                })
                .await?
                .unwrap_get();
            assert_eq!(state, get_result_value);

            // Update the contract state with a new delta
            let delta = StateDelta::from(b"New test contract value".to_vec());
            handler
                .handle_request(ClientRequest::Update {
                    key: *contract.key(),
                    delta,
                })
                .await?;
            // let (new_get_result_value, _) = handler
            //     .handle_request(ClientRequest::Get {
            //         key: *contract.key(),
            //         contract: false,
            //     })
            //     .await?
            //     .unwrap_summary();
            // assert_eq!(delta, new_get_result_value);
            todo!("get summary and compare with delta");
        }
    }
}

#[cfg(test)]
pub mod test {
    use std::sync::Arc;

    use locutus_runtime::ContractStore;
    use locutus_stdlib::prelude::ContractCode;

    use super::*;
    use crate::{
        config::{GlobalExecutor, CONFIG},
        contract::{
            test::{MemKVStore, SimStoreError},
            MockRuntime,
        },
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
                ),
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

    #[async_trait::async_trait]
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

        async fn handle_request(
            &mut self,
            req: ClientRequest,
        ) -> Result<HostResponse, Self::Error> {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_test() -> Result<(), anyhow::Error> {
        let (mut send_halve, mut rcv_halve) = contract_handler_channel::<SimStoreError>();

        let h = GlobalExecutor::spawn(async move {
            send_halve
                .send_to_handler(ContractHandlerEvent::Cache(WrappedContract::new(
                    Arc::new(ContractCode::from(vec![0, 1, 2, 3])),
                    Parameters::from(vec![]),
                )))
                .await
        });

        let (id, ev) =
            tokio::time::timeout(Duration::from_millis(100), rcv_halve.recv_from_listener())
                .await??;

        if let ContractHandlerEvent::Cache(contract) = ev {
            let data: Vec<u8> = contract.try_into()?;
            assert_eq!(data, vec![0, 1, 2, 3]);
            tokio::time::timeout(
                Duration::from_millis(100),
                rcv_halve.send_to_listener(
                    id,
                    ContractHandlerEvent::Cache(WrappedContract::new(
                        Arc::new(ContractCode::from(data)),
                        Parameters::from(vec![]),
                    )),
                ),
            )
            .await??;
        } else {
            anyhow::bail!("invalid event");
        }

        if let ContractHandlerEvent::Cache(contract) = h.await?? {
            let data: Vec<u8> = contract.try_into()?;
            assert_eq!(data, vec![0, 1, 2, 3]);
        } else {
            anyhow::bail!("invalid event!");
        }

        Ok(())
    }
}
