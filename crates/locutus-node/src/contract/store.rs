use std::path::PathBuf;

use locutus_runtime::Contract;
use stretto::AsyncCache;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::config::CONFIG;

use super::{ContractError, ContractKey};

/// Handle contract blob storage on the file system.
pub(crate) struct ContractStore {
    mem_cache: AsyncCache<ContractKey, Contract>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total speed used under a configured threshold

impl ContractStore {
    const MAX_MEM_CACHE: i64 = 10_000_000;

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
            let key_path: PathBuf = (*key).into();
            let key_path = CONFIG.config_paths.contracts_dir.join(key_path);
            let mut contract_file = File::open(key_path).await?;
            let mut contract_data = if let Ok(md) = contract_file.metadata().await {
                Vec::with_capacity(md.len() as usize)
            } else {
                Vec::new()
            };
            contract_file.read_to_end(&mut contract_data).await?;
            let contract = Contract::new(contract_data);
            let size = contract.data().len() as i64;
            self.mem_cache.insert(*key, contract.clone(), size).await;
            Ok(Some(contract))
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
        // insert in the memory cache
        {
            let size = contract.data().len() as i64;
            self.mem_cache.insert(key, contract.clone(), size).await;
        }
        // write to disc
        {
            let key_path: PathBuf = key.into();
            let key_path = CONFIG.config_paths.contracts_dir.join(key_path);
            if key_path.exists() {
                return Ok(());
            }
            let mut file = File::create(key_path).await?;
            file.write_all(contract.data()).await?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ContractStoreError {}
