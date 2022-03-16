use std::collections::HashMap;

use locutus_runtime::{ContractKey, ContractStore, ContractValue};

use super::handler::{CHListenerHalve, ContractHandler, ContractHandlerChannel, RuntimeInterface};
use crate::config::CONFIG;

pub(crate) struct MockRuntime {}

impl RuntimeInterface for MockRuntime {}

pub(crate) type MemKVStore = HashMap<ContractKey, ContractValue>;

pub(crate) struct MemoryContractHandler<KVStore = MemKVStore> {
    channel: ContractHandlerChannel<SimStoreError, CHListenerHalve>,
    kv_store: KVStore,
    contract_store: ContractStore,
    _runtime: MockRuntime,
}

impl<KVStore> MemoryContractHandler<KVStore> {
    const MAX_MEM_CACHE: i64 = 10_000_000;

    pub fn new(
        channel: ContractHandlerChannel<SimStoreError, CHListenerHalve>,
        kv_store: KVStore,
    ) -> Self {
        MemoryContractHandler {
            channel,
            kv_store,
            contract_store: ContractStore::new(
                CONFIG.config_paths.contracts_dir.clone(),
                Self::MAX_MEM_CACHE,
            ),
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

impl From<std::io::Error> for SimStoreError {
    fn from(err: std::io::Error) -> Self {
        Self(format!("{err}"))
    }
}

#[cfg(test)]
mod tests {
    use locutus_runtime::Contract;

    #[test]
    fn serialization() -> Result<(), anyhow::Error> {
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: Contract = gen.arbitrary()?;

        let serialized = bincode::serialize(&contract)?;
        let deser: Contract = bincode::deserialize(&serialized)?;
        assert_eq!(deser.data(), contract.data());
        assert_eq!(deser.key(), contract.key());
        Ok(())
    }
}
