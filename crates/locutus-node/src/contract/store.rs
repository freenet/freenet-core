use stretto::AsyncCache;

use super::{Contract, ContractError, ContractKey};

/// Handle contract blob storage on the file system.
pub(crate) struct ContractStore {
    mem_cache: AsyncCache<ContractKey, Contract>,
}

impl ContractStore {
    const MAX_MEM_CACHE: i64 = 10_000_000;
    const MAX_NUM_CONTRACTS: usize = 10;

    pub fn new() -> Self {
        Self {
            mem_cache: AsyncCache::new(10, Self::MAX_MEM_CACHE).expect("failed to build mem cache"),
        }
    }
    /// Returns a copy of the contract bytes if available, none otherwise.
    pub async fn fetch_contract<CErr>(
        &self,
        key: &ContractKey,
    ) -> Result<Option<Contract>, ContractError<CErr>>
    where
        CErr: std::error::Error,
    {
        if let Some(contract) = self.mem_cache.get(key) {
            Ok(Some(contract.as_ref().clone()))
        } else {
            todo!("fetch from disc")
        }
    }

    /// Store a copy of the contract in the local store.
    pub async fn store_contract<CErr>(
        &mut self,
        contract: Contract,
    ) -> Result<(), ContractError<CErr>>
    where
        CErr: std::error::Error,
    {
        let key = contract.key();
        let size = contract.data.len() as i64;
        self.mem_cache.insert(key, contract, size).await;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ContractStoreError {}
