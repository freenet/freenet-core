use crate::contract::interface::{ContractInterface, ContractUpdateResult};
use crate::contract::store::ContractStore;
use crate::contract::{ContractHandler, ContractHandlerChannel, ContractKey, ContractValue};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatabaseError {
    #[error("contract nor found")]
    ContractNotFound,
    #[error(transparent)]
    SqliteError(#[from] sqlx::Error),
}

pub(crate) struct TestContract {}

impl ContractInterface for TestContract {
    fn validate_value(value: &[u8]) -> bool {
        todo!()
    }

    fn update_value(value: Vec<u8>, value_update: &[u8]) -> ContractUpdateResult<Vec<u8>> {
        Ok(value_update.to_vec())
    }
    fn related_contracts(value_update: &[u8]) -> Vec<ContractKey> {
        todo!()
    }

    fn extract(extractor: Option<&[u8]>, value: &[u8]) -> Vec<u8> {
        todo!()
    }
}

pub(crate) struct SQLiteContractHandler {
    channel: ContractHandlerChannel<DatabaseError>,
    pub pool: SqlitePool,
}

impl SQLiteContractHandler {
    pub fn new(channel: ContractHandlerChannel<DatabaseError>, pool: SqlitePool) -> Self {
        SQLiteContractHandler { channel, pool }
    }
}

#[async_trait::async_trait]
impl ContractHandler for SQLiteContractHandler {
    type Error = DatabaseError;
    type ContractStore = ContractStore;

    #[inline(always)]
    fn channel(&self) -> &ContractHandlerChannel<Self::Error> {
        &self.channel
    }

    #[inline(always)]
    fn contract_store(&mut self) -> &mut ContractStore {
        todo!()
    }

    async fn get_value(
        &self,
        contract: &ContractKey,
    ) -> Result<Option<ContractValue>, Self::Error> {
        let encoded_key = base64::encode(contract.0);
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

        let value: Vec<u8> = match TestContract::update_value(old_value.0, &value.0) {
            Ok(contract_value) => contract_value,
            Err(err) => {
                // err.value
                todo!()
            }
        };

        let encoded_key = base64::encode(contract.0);
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
