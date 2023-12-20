use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use freenet_stdlib::prelude::*;
use stretto::Cache;

use super::{
    error::RuntimeInnerError,
    store::{SafeWriter, StoreFsManagement},
    RuntimeResult,
};

/// Handle contract blob storage on the file system.
pub struct ContractStore {
    contracts_dir: PathBuf,
    key_file: PathBuf,
    contract_cache: Cache<CodeHash, Arc<ContractCode<'static>>>,
    key_to_code_part: Arc<DashMap<ContractInstanceId, (u64, CodeHash)>>,
    index_file: SafeWriter<Self>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total space used under a configured threshold

impl StoreFsManagement for ContractStore {
    type MemContainer = Arc<DashMap<ContractInstanceId, (u64, CodeHash)>>;
    type Key = ContractInstanceId;
    type Value = CodeHash;

    fn insert_in_container(
        container: &mut Self::MemContainer,
        (key, offset): (Self::Key, u64),
        value: Self::Value,
    ) {
        container.insert(key, (offset, value));
    }
}

impl ContractStore {
    /// # Arguments
    /// - max_size: max size in bytes of the contracts being cached
    pub fn new(contracts_dir: PathBuf, max_size: i64) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";
        let mut key_to_code_part = Arc::new(DashMap::new());
        let key_file = contracts_dir.join("KEY_DATA");
        if !key_file.exists() {
            std::fs::create_dir_all(&contracts_dir).map_err(|err| {
                tracing::error!("error creating contract dir: {err}");
                err
            })?;
            File::create(contracts_dir.join("KEY_DATA"))?;
        } else {
            Self::load_from_file(&key_file, &mut key_to_code_part)?;
        }
        Self::watch_changes(key_to_code_part.clone(), &key_file)?;

        let index_file = SafeWriter::new(&key_file, false)?;
        Ok(Self {
            contract_cache: Cache::new(100, max_size).expect(ERR),
            contracts_dir,
            key_file,
            key_to_code_part,
            index_file,
        })
    }

    /// Returns a copy of the contract bytes if available, none otherwise.
    // todo: instead return Result<Option<_>, _> to handle IO errors upstream
    pub fn fetch_contract(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        let result = key
            .code_hash()
            .and_then(|code_hash| {
                self.contract_cache.get(code_hash).map(|data| {
                    Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                        WrappedContract::new(data.value().clone(), params.clone().into_owned()),
                    )))
                })
            })
            .flatten();
        if result.is_some() {
            return result;
        }

        self.key_to_code_part.get(key.id()).and_then(|key| {
            let code_hash = key.value().1;
            let path = code_hash.encode();
            let key_path = self.contracts_dir.join(path).with_extension("wasm");
            let ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract {
                data,
                params,
                ..
            })) = ContractContainer::try_from((&*key_path, params.clone().into_owned()))
                .map_err(|err| {
                    tracing::debug!("contract not found: {err}");
                    err
                })
                .ok()?
            else {
                unimplemented!()
            };
            // add back the contract part to the mem store
            let size = data.data().len() as i64;
            self.contract_cache.insert(code_hash, data.clone(), size);
            Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                WrappedContract::new(data, params),
            )))
        })
    }

    /// Store a copy of the contract in the local store, in case it hasn't been stored previously.
    pub fn store_contract(&mut self, contract: ContractContainer) -> RuntimeResult<()> {
        let (key, code) = match contract.clone() {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                (contract_v1.key().clone(), contract_v1.code().clone())
            }
            _ => unimplemented!(),
        };
        let code_hash = key.code_hash().ok_or_else(|| {
            tracing::warn!("trying to store partially unspecified contract `{}`", key);
            RuntimeInnerError::UnwrapContract
        })?;
        if self.contract_cache.get(code_hash).is_some() {
            return Ok(());
        }
        let key_path = code_hash.encode();
        let key_path = self.contracts_dir.join(key_path).with_extension("wasm");
        if let Ok((code, _ver)) = ContractCode::load_versioned_from_path(&key_path) {
            let size = code.data().len() as i64;
            self.contract_cache.insert(*code_hash, Arc::new(code), size);
            return Ok(());
        }

        // insert in the memory cache
        let size = code.data().len() as i64;
        let data = code.data().to_vec();
        self.contract_cache
            .insert(*code_hash, Arc::new(ContractCode::from(data)), size);

        // save on disc
        let version = APIVersion::from(contract);
        let output: Vec<u8> = code.to_bytes_versioned(version)?;
        let mut file = File::create(key_path)?;
        file.write_all(output.as_slice())?;

        // Update index
        let keys = self.key_to_code_part.entry(*key.id());
        match keys {
            dashmap::mapref::entry::Entry::Occupied(mut v) => {
                let current_version_offset = v.get().0;
                let prev_val = &mut v.get_mut().1;
                // first mark the old entry (if it exists) as removed
                Self::remove(&self.key_file, current_version_offset)?;
                let new_offset = Self::insert(&mut self.index_file, *key.id(), code_hash)?;
                *prev_val = *code_hash;
                v.get_mut().0 = new_offset;
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let offset = Self::insert(&mut self.index_file, *key.id(), code_hash)?;
                v.insert((offset, *code_hash));
            }
        }

        Ok(())
    }

    pub fn get_contract_path(&mut self, key: &ContractKey) -> RuntimeResult<PathBuf> {
        let contract_hash = match key.code_hash() {
            Some(k) => *k,
            None => self.code_hash_from_key(key).ok_or_else(|| {
                tracing::warn!("trying to get partially unspecified contract `{key}`");
                RuntimeInnerError::UnwrapContract
            })?,
        };
        let key_path = contract_hash.encode();
        Ok(self.contracts_dir.join(key_path).with_extension("wasm"))
    }

    pub fn remove_contract(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        let contract_hash = match key.code_hash() {
            Some(k) => *k,
            None => self.code_hash_from_key(key).ok_or_else(|| {
                tracing::warn!("trying to get partially unspecified contract `{key}`");
                RuntimeInnerError::UnwrapContract
            })?,
        };
        if let Some((_, (offset, _))) = self.key_to_code_part.remove(key.id()) {
            Self::remove(&self.key_file, offset)?;
        }
        let key_path = self
            .contracts_dir
            .join(contract_hash.encode())
            .with_extension("wasm");
        std::fs::remove_file(key_path)?;
        Ok(())
    }

    pub fn code_hash_from_key(&self, key: &ContractKey) -> Option<CodeHash> {
        self.key_to_code_part.get(key.id()).map(|r| r.value().1)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000)?;
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2])),
            [0, 1].as_ref().into(),
        );
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone()));
        store.store_contract(container)?;
        let f = store.fetch_contract(contract.key(), &[0, 1].as_ref().into());
        assert!(f.is_some());
        Ok(())
    }
}
