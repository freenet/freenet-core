use sqlx::SqlitePool;
use crate::contract::handler::SQLiteContractHandler;
use crate::contract::interface::{ContractInterface, ContractKeyResult};
use crate::node::SimStorageError;

#[cfg(test)]
use super::Contract;
use super::{
    store::{ContractHandler, ContractHandlerChannel, ContractStore},
    ContractKey, ContractValue,
};

pub(crate) struct MemoryContractHandler {
    channel: ContractHandlerChannel<SimStorageError>,
}

pub(crate) struct TestContract {}


impl ContractInterface for TestContract {

    fn validate_value(value: &[u8]) -> bool {todo!()}

    fn update_value(value: Vec<u8>, value_update: &[u8]) -> ContractKeyResult<Vec<u8>> {
        // si es correcto el update_value
        todo!()
    }

    fn related_contracts(value_update: &[u8]) -> Vec<ContractKey> {todo!()}

    fn extract(extractor: Option<&[u8]>, value: &[u8]) -> Vec<u8> {todo!()}
}

impl MemoryContractHandler {
    pub fn new(channel: ContractHandlerChannel<SimStorageError>) -> Self {
        MemoryContractHandler { channel }
    }
}

#[async_trait::async_trait]
impl ContractHandler for MemoryContractHandler {
    type Error = SimStorageError;

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
fn serialization() -> Result<(), Box<dyn std::error::Error>> {
    let bytes = crate::test_utils::random_bytes_1024();
    let mut gen = arbitrary::Unstructured::new(&bytes);
    let contract: Contract = gen.arbitrary().map_err(|_| "failed gen arb data")?;

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

// Create test cintracts table
async fn create_test_contracts_table(pool: &SqlitePool) {
    sqlx::query("CREATE TABLE IF NOT EXISTS contracts (
        key             STRING PRIMARY KEY,
        value           BLOB
        )"
    ).execute(pool).await.unwrap();
}

#[tokio::test]
async fn contract_handler() -> Result<(), Box<dyn std::error::Error>> {
    let contract_value: Vec<u8> = base64::decode_config("dmFsb3IgZGUgcHJ1ZWJhIDE=", base64::STANDARD)?;
    let contract: Contract = Contract::new(contract_value);

    let contract_key= ContractKey{0: contract.key};
    let contract_value= ContractValue::new(contract.data);
    let contract_value_cloned= contract_value.clone();

    let mut handler : SQLiteContractHandler = get_handler().await?;
    create_test_contracts_table(&handler.pool).await;
    let added_value = handler.put_value(&contract_key, contract_value).await?;
    let got_value = handler.get_value(&contract_key).await?.unwrap();

    assert_eq!(contract_value_cloned.0, added_value.0);
    assert_eq!(contract_value_cloned.0, got_value.0);
    assert_eq!(added_value.0, got_value.0);

    // Insert new contract_value at the same key
    let new_contract_value: Vec<u8> = base64::decode_config("dmFsb3IgZGUgcHJ1ZWJhIDI=", base64::STANDARD)?;
    let new_contract_value= ContractValue::new(new_contract_value);
    let new_contract_value_cloned= new_contract_value.clone();
    let new_added_value = handler.put_value(&contract_key, new_contract_value).await?;
    let new_got_value = handler.get_value(&contract_key).await?.unwrap();

    assert_eq!(new_contract_value_cloned.0, new_added_value.0);
    assert_eq!(new_contract_value_cloned.0, new_got_value.0);
    assert_eq!(new_added_value.0, new_got_value.0);


    Ok(())
}
