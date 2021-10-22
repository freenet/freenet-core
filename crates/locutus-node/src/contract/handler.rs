use libp2p::core::Executor;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};
use crate::contract::{ContractHandler, ContractHandlerChannel, ContractKey, ContractValue};
use crate::contract::interface::{ContractInterface, ContractUpdateError, ContractUpdateResult};
use crate::contract::store::ContractStore;
use crate::node::SimStorageError;

pub(crate) struct TestContract {}

impl ContractInterface for TestContract {

    fn validate_value(value: &[u8]) -> bool {todo!()}

    fn update_value(value: Vec<u8>, value_update: &[u8]) -> ContractUpdateResult<Vec<u8>> {
        Ok(value_update.to_vec())
    }
    fn related_contracts(value_update: &[u8]) -> Vec<ContractKey> {todo!()}

    fn extract(extractor: Option<&[u8]>, value: &[u8]) -> Vec<u8> {todo!()}
}

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

        let old_value: ContractValue = match self.get_value(contract).await {
            Ok(Some(contract_value)) => contract_value,
            Ok(None) => value.clone(),
            Err(_) => value.clone()
        };

        let value: Vec<u8> = match TestContract::update_value(
            old_value.0, &value.0) {
            Ok(contract_value) => contract_value,
            Err(err) => {
                // err.value
                todo!()
            }
        };

        let encoded_key = base64::encode(contract.0);
        sqlx::query(
            "INSERT OR REPLACE INTO contracts (key, value) VALUES (?1, ?2)\
             RETURNING value"
        )
        .bind(encoded_key)
        .bind(&value)
        .map(|row: SqliteRow| {
            ContractValue{ 0: row.get("value") }
        })
        .fetch_one(&self.pool).await
    }
}