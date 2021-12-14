use std::collections::HashMap;

use super::{
    handler::{CHListenerHalve, ContractHandler, ContractHandlerChannel},
    runtime::{ContractRuntime, ContractUpdateResult},
    store::ContractStore,
    ContractKey, ContractValue,
};

pub(super) struct MockRuntime {}

impl ContractRuntime for MockRuntime {
    fn validate_value(&self, _value: &[u8]) -> bool {
        true
    }

    fn update_value(&self, _value: &[u8], value_update: &[u8]) -> ContractUpdateResult<Vec<u8>> {
        Ok(value_update.to_vec())
    }

    fn related_contracts(&self, _value_update: &[u8]) -> Vec<crate::contract::ContractKey> {
        todo!()
    }

    fn extract(&self, _extractor: Option<&[u8]>, _value: &[u8]) -> Vec<u8> {
        todo!()
    }
}

pub(crate) type MemKVStore = HashMap<ContractKey, ContractValue>;

pub(crate) struct MemoryContractHandler<KVStore = MemKVStore> {
    channel: ContractHandlerChannel<SimStoreError, CHListenerHalve>,
    kv_store: KVStore,
    contract_store: ContractStore,
    _runtime: MockRuntime,
}

impl<KVStore> MemoryContractHandler<KVStore> {
    pub fn new(
        channel: ContractHandlerChannel<SimStoreError, CHListenerHalve>,
        kv_store: KVStore,
    ) -> Self {
        MemoryContractHandler {
            channel,
            kv_store,
            contract_store: ContractStore::new(),
            _runtime: MockRuntime {},
        }
    }
}

impl From<ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>>
    for MemoryContractHandler
{
    fn from(
        channel: ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>,
    ) -> Self {
        let store = MemKVStore::new();
        MemoryContractHandler::new(channel, store)
    }
}

#[async_trait::async_trait]
impl ContractHandler for MemoryContractHandler {
    type Error = SimStoreError;

    #[inline(always)]
    fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve> {
        &mut self.channel
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

#[derive(Debug)]
pub(crate) struct SimStoreError(String);

impl std::fmt::Display for SimStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SimStoreError {}

#[cfg(test)]
mod test {
    use crate::contract::Contract;

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
}
