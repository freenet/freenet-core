use std::{
    fs::File,
    io::{Read, Write},
    path::PathBuf,
};

use stretto::AsyncCache;

use crate::Contract;

use super::ContractKey;

/// Handle contract blob storage on the file system.
pub struct ContractStore {
    contracts_dir: PathBuf,
    mem_cache: AsyncCache<ContractKey, Contract>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total speed used under a configured threshold

impl ContractStore {
    pub fn new(contracts_dir: PathBuf, max_size: i64) -> Self {
        Self {
            mem_cache: AsyncCache::new(10, max_size).expect("failed to build mem cache"),
            contracts_dir,
        }
    }
    /// Returns a copy of the contract bytes if available, none otherwise.
    pub async fn fetch_contract<CErr>(&self, key: &ContractKey) -> Result<Option<Contract>, CErr>
    where
        CErr: std::error::Error + From<std::io::Error>,
    {
        if let Some(contract) = self.mem_cache.get(key) {
            Ok(Some(contract.as_ref().clone()))
        } else {
            let key_path: PathBuf = (*key).into();
            let key_path = self.contracts_dir.join(key_path);
            let mut contract_file = File::open(key_path)?;
            let mut contract_data = if let Ok(md) = contract_file.metadata() {
                Vec::with_capacity(md.len() as usize)
            } else {
                Vec::new()
            };
            contract_file.read_to_end(&mut contract_data)?;
            let contract = Contract::new(contract_data);
            let size = contract.data().len() as i64;
            self.mem_cache.insert(*key, contract.clone(), size).await;
            Ok(Some(contract))
        }
    }

    /// Store a copy of the contract in the local store.
    pub async fn store_contract<CErr>(&mut self, contract: Contract) -> Result<(), CErr>
    where
        CErr: std::error::Error + From<std::io::Error>,
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
            let key_path = self.contracts_dir.join(key_path).join(".wasm");
            if key_path.exists() {
                return Ok(());
            }
            let mut file = File::create(key_path)?;
            file.write_all(contract.data())?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ContractStoreError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
}
