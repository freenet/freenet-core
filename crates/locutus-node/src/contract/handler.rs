use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::runtime::{ContractRuntime, ContractUpdateError};
use crate::contract::store::ContractStore;
use crate::contract::{Contract, ContractError, ContractKey, ContractValue};

/// Behaviour
#[async_trait::async_trait]
pub(crate) trait ContractHandler: From<ContractHandlerChannel<Self::Error>> {
    type Error;

    fn channel(&self) -> &ContractHandlerChannel<Self::Error>;

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

pub struct EventId(usize);

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
pub(crate) struct ContractHandlerChannel<Err> {
    _err: PhantomData<Err>,
}

// TODO: pretty much can copy this verbatim?
// https://docs.rs/tarpc/0.27.2/src/tarpc/transport/channel.rs.html#39-42

impl<Err> Clone for ContractHandlerChannel<Err> {
    fn clone(&self) -> Self {
        Self { _err: PhantomData }
    }
}

impl<Err: std::error::Error> ContractHandlerChannel<Err> {
    pub fn new() -> Self {
        // let (notification_tx, notification_channel) = mpsc::channel(100);
        // let (ch_tx, ch_listener) = mpsc::channel(10);
        Self { _err: PhantomData }
    }

    /// Send an event to the contract handler and receive a response event if succesful.
    pub async fn send_to_handler(
        &self,
        _ev: ContractHandlerEvent<Err>,
    ) -> Result<ContractHandlerEvent<Err>, ContractError<Err>> {
        todo!()
    }

    pub async fn send_to_listeners(&self, _id: EventId, _ev: ContractHandlerEvent<Err>) {
        todo!()
    }

    pub async fn recv_from_listeners(
        &self,
    ) -> Result<(EventId, ContractHandlerEvent<Err>), ContractError<Err>> {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoreResponse {
    pub value: Option<ContractValue>,
    pub contract: Option<Contract>,
}

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
    /// Get a contract from the store.
    FetchContract(ContractKey),
    /// Result of fetching a contract.
    FetchContractResult(Result<Option<Contract>, ContractError<Err>>),
}

mod sqlite {
    use once_cell::sync::Lazy;
    use sqlx::{sqlite::SqliteRow, Row, SqlitePool};

    use super::*;

    // Is fine to clone this as it wraps by an Arc.
    static POOL: Lazy<SqlitePool> = Lazy::new(|| {
        tokio::task::block_in_place(|| {
            let conn_str = if cfg!(debug_assertions) {
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

    pub(crate) struct SQLiteContractHandler {
        channel: ContractHandlerChannel<DatabaseError>,
        store: ContractStore,
        pool: SqlitePool,
    }

    impl SQLiteContractHandler {
        pub fn new(
            channel: ContractHandlerChannel<DatabaseError>,
            store: ContractStore,
            pool: SqlitePool,
        ) -> Self {
            SQLiteContractHandler {
                channel,
                store,
                pool,
            }
        }
    }

    impl From<ContractHandlerChannel<<Self as ContractHandler>::Error>> for SQLiteContractHandler {
        fn from(channel: ContractHandlerChannel<<Self as ContractHandler>::Error>) -> Self {
            let store = ContractStore::new();
            let pool = (&*POOL).clone();
            SQLiteContractHandler::new(channel, store, pool)
        }
    }

    #[async_trait::async_trait]
    impl ContractHandler for SQLiteContractHandler {
        type Error = DatabaseError;

        #[inline(always)]
        fn channel(&self) -> &ContractHandlerChannel<Self::Error> {
            &self.channel
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
                .map(|row: SqliteRow| {
                    Some(ContractValue {
                        0: row.get("value"),
                    })
                })
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

            let value: Vec<u8> = ContractRuntime::update_value(old_value.0, &value.0)?;
            let encoded_key = hex::encode(contract.0);
            match sqlx::query(
                "INSERT OR REPLACE INTO contracts (key, value) VALUES (?1, ?2)\
             RETURNING value",
            )
            .bind(encoded_key)
            .bind(value)
            .map(|row: SqliteRow| ContractValue {
                0: row.get("value"),
            })
            .fetch_one(&self.pool)
            .await
            {
                Ok(contract_value) => Ok(contract_value),
                Err(err) => Err(DatabaseError::SqliteError(err)),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use sqlx::SqlitePool;

    use super::sqlite::SQLiteContractHandler;
    use super::*;
    use crate::contract::Contract;

    // Prepare and get handler for an in-memory sqlite db
    async fn get_handler() -> Result<SQLiteContractHandler, sqlx::Error> {
        let ch_handler = ContractHandlerChannel::new();
        let db_pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store: ContractStore = ContractStore::new();
        create_test_contracts_table(&db_pool).await;
        Ok(SQLiteContractHandler::new(ch_handler, store, db_pool))
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

    #[tokio::test]
    async fn contract_handler() -> Result<(), anyhow::Error> {
        // Create a sqlite handler and initialize the database
        let mut handler: SQLiteContractHandler = get_handler().await?;
        create_test_contracts_table(&handler.pool).await;

        // Generate a contract
        let contract_value: Vec<u8> = b"Test contract value".to_vec();
        let contract: Contract = Contract::new(contract_value);

        // Get contract parts
        let contract_key = ContractKey(contract.key);
        let contract_value = ContractValue::new(contract.data);
        let contract_value_cloned = contract_value.clone();

        let put_result_value = handler.put_value(&contract_key, contract_value).await?;
        let get_result_value = handler
            .get_value(&contract_key)
            .await?
            .ok_or(anyhow::anyhow!("No value found"))?;

        assert_eq!(contract_value_cloned.0, put_result_value.0);
        assert_eq!(contract_value_cloned.0, get_result_value.0);
        assert_eq!(put_result_value.0, get_result_value.0);

        // Update the contract value with new one
        let new_contract_value: Vec<u8> = b"New test contract value".to_vec();
        let new_contract_value = ContractValue::new(new_contract_value);
        let new_contract_value_cloned = new_contract_value.clone();
        let new_put_result_value = handler.put_value(&contract_key, new_contract_value).await?;
        let new_get_result_value = handler
            .get_value(&contract_key)
            .await?
            .ok_or(anyhow::anyhow!("No value found"))?;

        assert_eq!(new_contract_value_cloned.0, new_put_result_value.0);
        assert_eq!(new_contract_value_cloned.0, new_get_result_value.0);
        assert_eq!(new_put_result_value.0, new_get_result_value.0);

        Ok(())
    }
}
