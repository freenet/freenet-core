use std::collections::HashMap;

use crate::node::SimStorageError;

#[cfg(test)]
use super::Contract;
use super::{
    handler::{ContractHandler, ContractHandlerChannel},
    store::ContractStore,
    ContractKey, ContractValue,
};

pub(crate) type MemKVStore = HashMap<ContractKey, ContractValue>;

pub(crate) struct MemoryContractHandler<KVStore = MemKVStore> {
    channel: ContractHandlerChannel<SimStorageError>,
    kv_store: KVStore,
    contract_store: ContractStore,
}

impl<KVStore> MemoryContractHandler<KVStore> {
    pub fn new(channel: ContractHandlerChannel<SimStorageError>, kv_store: KVStore) -> Self {
        MemoryContractHandler {
            channel,
            kv_store,
            contract_store: ContractStore::new(),
        }
    }
}

impl From<ContractHandlerChannel<<Self as ContractHandler>::Error>> for MemoryContractHandler {
    fn from(channel: ContractHandlerChannel<<Self as ContractHandler>::Error>) -> Self {
        MemoryContractHandler {
            channel,
            kv_store: MemKVStore::new(),
            contract_store: ContractStore::new(),
        }
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
        &mut self.contract_store
    }

    /// Get current contract value, if present, otherwise get none.
    async fn get_value(
        &self,
        contract: &ContractKey,
    ) -> Result<Option<ContractValue>, Self::Error> {
        Ok(self.kv_store.get(contract).cloned())
    }

    async fn put_value(
        &mut self,
        contract: &ContractKey,
        value: ContractValue,
    ) -> Result<ContractValue, Self::Error> {
        let new_val = value.clone();
        self.kv_store.insert(*contract, value);
        Ok(new_val)
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
