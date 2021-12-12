use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::runtime::{ContractRuntime, ContractUpdateError};
use crate::contract::store::ContractStore;
use crate::contract::{Contract, ContractError, ContractKey, ContractValue};

#[async_trait::async_trait]
pub(crate) trait ContractHandler:
    From<ContractHandlerChannel<Self::Error, CHListenerHalve>>
{
    type Error: std::error::Error;

    fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve>;

    fn contract_store(&mut self) -> &mut ContractStore;

    /// Get current contract value, if present, otherwise get none.
    async fn get_value(&self, contract: &ContractKey)
        -> Result<Option<ContractValue>, Self::Error>;

    /// Updates (or inserts) a value for the given contract. This operation is fallible:
    /// It will return an error when the value is not valid (according to the contract specification)
    /// or any other condition happened (for example the contract not being present currently,
    /// in which case it has to be stored by calling `contract_store` first).
    async fn put_value(
        &mut self,
        contract: &ContractKey,
        value: ContractValue,
    ) -> Result<ContractValue, Self::Error>;
}

pub struct EventId(u64);

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
pub(crate) struct ContractHandlerChannel<SErr, End>
where
    SErr: std::error::Error,
{
    rx: mpsc::UnboundedReceiver<InternalCHEvent<SErr>>,
    tx: mpsc::UnboundedSender<InternalCHEvent<SErr>>,
    //TODO:  change queue to btree once pop_first is stabilized
    // (https://github.com/rust-lang/rust/issues/62924)
    queue: VecDeque<(u64, ContractHandlerEvent<SErr>)>,
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

pub(crate) fn contract_handler_channel<SErr>() -> (
    ContractHandlerChannel<SErr, CHSenderHalve>,
    ContractHandlerChannel<SErr, CHListenerHalve>,
)
where
    SErr: std::error::Error,
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

impl<SErr: std::error::Error> ContractHandlerChannel<SErr, CHSenderHalve> {
    /// Send an event to the contract handler and receive a response event if succesful.
    pub async fn send_to_handler(
        &mut self,
        ev: ContractHandlerEvent<SErr>,
    ) -> Result<ContractHandlerEvent<SErr>, ContractError<SErr>> {
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
                    break Err(ContractError::NoHandlerEvResponse);
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

impl<SErr: std::error::Error> ContractHandlerChannel<SErr, CHListenerHalve> {
    pub async fn send_to_listener(
        &self,
        id: EventId,
        ev: ContractHandlerEvent<SErr>,
    ) -> Result<(), ContractError<SErr>> {
        Ok(self
            .tx
            .send(InternalCHEvent { ev, id: id.0 })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))?)
    }

    pub async fn recv_from_listener(
        &mut self,
    ) -> Result<(EventId, ContractHandlerEvent<SErr>), ContractError<SErr>> {
        if let Some((id, ev)) = self.queue.pop_front() {
            return Ok((EventId(id), ev));
        }
        if let Some(msg) = self.rx.recv().await {
            return Ok((EventId(msg.id), msg.ev));
        }
        Err(ContractError::NoHandlerEvResponse)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoreResponse {
    pub value: Option<ContractValue>,
    pub contract: Option<Contract>,
}

struct InternalCHEvent<Err>
where
    Err: std::error::Error,
{
    ev: ContractHandlerEvent<Err>,
    id: u64,
}

#[derive(Debug)]
pub(crate) enum ContractHandlerEvent<Err>
where
    Err: std::error::Error,
{
    /// Try to push/put a new value into the contract.
    PushQuery {
        key: ContractKey,
        value: ContractValue,
    },
    /// The response to a push query.
    PushResponse {
        new_value: Result<ContractValue, Err>,
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
    Cache(Contract),
    /// Result of a caching operation.
    CacheResult(Result<(), ContractError<Err>>),
}

#[cfg(test)]
mod test {
    use crate::node::SimStorageError;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_test() -> Result<(), anyhow::Error> {
        let (mut send_halve, mut rcv_halve) = contract_handler_channel::<SimStorageError>();

        let h = tokio::spawn(async move {
            send_halve
                .send_to_handler(ContractHandlerEvent::Cache(Contract::new(vec![0, 1, 2, 3])))
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
                rcv_halve.send_to_listener(id, ContractHandlerEvent::Cache(Contract::new(data))),
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

mod sqlite {
    use once_cell::sync::Lazy;
    use sqlx::{sqlite::SqliteRow, Row, SqlitePool};

    use crate::contract::test_utils::MockRuntime;

    use super::*;

    // Is fine to clone this as it wraps by an Arc.
    static POOL: Lazy<SqlitePool> = Lazy::new(|| {
        tokio::task::block_in_place(|| {
            let conn_str = if cfg!(test) {
                "sqlite::memory:"
            } else {
                // FIXME: initialize this with the actual connection string
                todo!()
            };
            tokio::runtime::Handle::current()
                .block_on(async move { SqlitePool::connect(conn_str).await })
        })
        .unwrap()
    });

    #[derive(Debug, thiserror::Error)]
    pub(crate) enum DatabaseError {
        #[error("contract nor found")]
        ContractNotFound,
        #[error(transparent)]
        SqliteError(#[from] sqlx::Error),
        #[error(transparent)]
        RuntimeError(#[from] ContractUpdateError),
    }

    pub(crate) struct SQLiteContractHandler<R> {
        channel: ContractHandlerChannel<DatabaseError, CHListenerHalve>,
        store: ContractStore,
        runtime: R,
        pub(super) pool: SqlitePool,
    }

    impl<R> SQLiteContractHandler<R>
    where
        R: ContractRuntime,
    {
        pub fn new(
            channel: ContractHandlerChannel<DatabaseError, CHListenerHalve>,
            store: ContractStore,
            runtime: R,
        ) -> Self {
            let pool = POOL.clone();
            SQLiteContractHandler {
                channel,
                store,
                pool,
                runtime,
            }
        }
    }

    impl From<ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>>
        for SQLiteContractHandler<MockRuntime>
    {
        fn from(
            channel: ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>,
        ) -> Self {
            let store = ContractStore::new();
            let runtime = MockRuntime {};
            SQLiteContractHandler::new(channel, store, runtime)
        }
    }

    #[async_trait::async_trait]
    impl ContractHandler for SQLiteContractHandler<MockRuntime> {
        type Error = DatabaseError;

        #[inline(always)]
        fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve> {
            &mut self.channel
        }

        #[inline(always)]
        fn contract_store(&mut self) -> &mut ContractStore {
            &mut self.store
        }

        async fn get_value(
            &self,
            contract: &ContractKey,
        ) -> Result<Option<ContractValue>, Self::Error> {
            let encoded_key = hex::encode(contract.0);
            match sqlx::query("SELECT key, value FROM contracts WHERE key = ?")
                .bind(encoded_key)
                .map(|row: SqliteRow| Some(ContractValue::new(row.get("value"))))
                .fetch_one(&self.pool)
                .await
            {
                Ok(result) => Ok(result),
                Err(sqlx::Error::RowNotFound) => Ok(None),
                Err(_) => Err(DatabaseError::ContractNotFound),
            }
        }

        async fn put_value(
            &mut self,
            contract: &ContractKey,
            value: ContractValue,
        ) -> Result<ContractValue, Self::Error> {
            let old_value: ContractValue = match self.get_value(contract).await {
                Ok(Some(contract_value)) => contract_value,
                Ok(None) => value.clone(),
                Err(_) => value.clone(),
            };

            let value: Vec<u8> = self.runtime.update_value(&*old_value, &value.0)?;
            let encoded_key = hex::encode(contract.0);
            match sqlx::query(
                "INSERT OR REPLACE INTO contracts (key, value) VALUES (?1, ?2)\
             RETURNING value",
            )
            .bind(encoded_key)
            .bind(value)
            .map(|row: SqliteRow| ContractValue::new(row.get("value")))
            .fetch_one(&self.pool)
            .await
            {
                Ok(contract_value) => Ok(contract_value),
                Err(err) => Err(DatabaseError::SqliteError(err)),
            }
        }
    }

    #[cfg(test)]
    mod test {
        use super::sqlite::SQLiteContractHandler;
        use super::*;
        use crate::contract::Contract;

        // Prepare and get handler for an in-memory sqlite db
        async fn get_handler() -> Result<SQLiteContractHandler<MockRuntime>, sqlx::Error> {
            let (_, ch_handler) = contract_handler_channel();
            let db_pool = SqlitePool::connect("sqlite::memory:").await?;
            let store: ContractStore = ContractStore::new();
            create_test_contracts_table(&db_pool).await;
            Ok(SQLiteContractHandler::new(
                ch_handler,
                store,
                MockRuntime {},
            ))
        }

        // Create test contracts table
        async fn create_test_contracts_table(pool: &SqlitePool) {
            sqlx::query(
                "CREATE TABLE IF NOT EXISTS contracts (
                        key             STRING PRIMARY KEY,
                        value           BLOB
                    )",
            )
            .execute(pool)
            .await
            .unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn contract_handler() -> Result<(), anyhow::Error> {
            // Create a sqlite handler and initialize the database
            let mut handler = get_handler().await?;
            create_test_contracts_table(&handler.pool).await;

            // Generate a contract
            let contract_bytes = b"Test contract value".to_vec();
            let contract: Contract = Contract::new(contract_bytes.clone());

            // Get contract parts
            let contract_value = ContractValue::new(contract_bytes.clone());
            let put_result_value = handler
                .put_value(&contract.key(), contract_value.clone())
                .await?;
            let get_result_value = handler
                .get_value(&contract.key())
                .await?
                .ok_or_else(|| anyhow::anyhow!("No value found"))?;

            assert_eq!(contract_value, put_result_value);
            assert_eq!(contract_value, get_result_value);

            // Update the contract value with new one
            let new_contract_value = ContractValue::new(b"New test contract value".to_vec());
            let new_put_result_value = handler
                .put_value(&contract.key(), new_contract_value.clone())
                .await?;
            let new_get_result_value = handler
                .get_value(&contract.key())
                .await?
                .ok_or_else(|| anyhow::anyhow!("No value found"))?;

            assert_eq!(new_contract_value, new_put_result_value);
            assert_eq!(new_contract_value, new_get_result_value);

            Ok(())
        }
    }
}
