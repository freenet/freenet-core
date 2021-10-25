use crate::contract::handler::SQLiteContractHandler;
use crate::node::SimStorageError;
use sqlx::SqlitePool;

#[cfg(test)]
use super::Contract;
use super::{
    store::{ContractHandler, ContractHandlerChannel, ContractStore},
    ContractKey, ContractValue,
};

pub(crate) struct MemoryContractHandler {
    channel: ContractHandlerChannel<SimStorageError>,
}

impl MemoryContractHandler {
    pub fn new(channel: ContractHandlerChannel<SimStorageError>) -> Self {
        MemoryContractHandler { channel }
    }
}

#[async_trait::async_trait]
impl ContractHandler for MemoryContractHandler {
    type Error = SimStorageError;
    type ContractStore = ContractStore;

    #[inline(always)]
    fn channel(&self) -> &ContractHandlerChannel<Self::Error> {
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
        todo!()
    }

    async fn put_value(
        &mut self,
        _contract: &ContractKey,
        _value: ContractValue,
    ) -> Result<ContractValue, Self::Error> {
        todo!()
    }
}

#[test]
fn serialization() -> Result<(), anyhow::Error> {
    let bytes = crate::test_utils::random_bytes_1024();
    let mut gen = arbitrary::Unstructured::new(&bytes);
    let contract: Contract = gen.arbitrary()?;

    let serialized = bincode::serialize(&contract)?;
    let deser: Contract = bincode::deserialize(&serialized)?;
    assert_eq!(deser.data, contract.data);
    assert_eq!(deser.key, contract.key);
    Ok(())
}

// Prepare and get handler for an in-memory sqlite db
async fn get_handler() -> Result<SQLiteContractHandler, sqlx::Error> {
    let ch_handler = ContractHandlerChannel::new();
    let db_pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    create_test_contracts_table(&db_pool).await;
    Ok(SQLiteContractHandler::new(ch_handler, db_pool))
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
