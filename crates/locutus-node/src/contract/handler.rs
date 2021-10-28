use crate::contract::store::ContractStore;
use crate::contract::{ContractHandler, ContractHandlerChannel, ContractKey, ContractValue};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};

use super::runtime::{ContractRuntime, ContractUpdateError};

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
    pub pool: SqlitePool,
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

        let value: Vec<u8> = ContractRuntime::update_value(old_value.0, &value.0)?;
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
#[cfg(test)]
mod test {
    use crate::contract::Contract;

    use super::*;

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
