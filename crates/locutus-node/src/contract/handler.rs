use libp2p::core::Executor;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};
use crate::contract::{ContractHandler, ContractHandlerChannel, ContractKey, ContractValue};
use crate::contract::store::ContractStore;
use crate::node::SimStorageError;

pub(crate) struct SQLiteContractHandler {
    channel: ContractHandlerChannel<sqlx::Error>,
    pub pool: SqlitePool
}

impl SQLiteContractHandler {
    pub fn new(channel: ContractHandlerChannel<sqlx::Error>, pool: SqlitePool) -> Self {
        SQLiteContractHandler { channel, pool }
    }
}


#[async_trait::async_trait]
impl ContractHandler for SQLiteContractHandler {
    type Error = sqlx::Error;

    #[inline(always)]
    fn channel(&self) -> &ContractHandlerChannel<sqlx::Error> {
        &self.channel
    }

    #[inline(always)]
    fn contract_store(&mut self) -> &mut ContractStore {
        todo!()
    }

    /// Get current contract value, if present, otherwise get none.
    async fn get_value(
        &self,
        _contract: &ContractKey,
    ) -> Result<Option<ContractValue>, Self::Error> {
        let encoded_key = base64::encode(_contract.0);
        match sqlx::query(
        "SELECT key, value FROM contracts WHERE key = ?"
        )
        .bind(encoded_key)
        .map(|row: SqliteRow| {
            Some(ContractValue{ 0: row.get("value") })
        })
        .fetch_one(&self.pool).await {
            Ok(result) => Ok(result),
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(_) => Ok(None)
        }
    }

    async fn put_value(
        &mut self,
        contract: &ContractKey,
        value: ContractValue,
    ) -> Result<ContractValue, Self::Error> {



        let encoded_key = base64::encode(contract.0);

        sqlx::query(
            "INSERT OR REPLACE INTO contracts (key, value) VALUES (?1, ?2)\
             RETURNING value"
        )
        .bind(encoded_key)
        .bind(&value.0)
        .map(|row: SqliteRow| {
            ContractValue{ 0: row.get("value") }
        })
        .fetch_one(&self.pool).await
    }
}