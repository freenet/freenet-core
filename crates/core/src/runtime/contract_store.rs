use std::{fs::File, io::Write, iter::FromIterator, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};
use stretto::Cache;

use super::{
    error::RuntimeInnerError,
    store::{StoreEntriesContainer, StoreFsManagement},
    RuntimeResult,
};

#[derive(Serialize, Deserialize, Default)]
struct KeyToCodeMap(Vec<(ContractKey, CodeHash)>);

impl StoreEntriesContainer for KeyToCodeMap {
    type MemContainer = Arc<DashMap<ContractKey, CodeHash>>;
    type Key = ContractKey;
    type Value = CodeHash;

    fn update(self, container: &mut Self::MemContainer) {
        for (k, v) in self.0 {
            container.insert(k, v);
        }
    }

    fn replace(container: &Self::MemContainer) -> Self {
        KeyToCodeMap::from(&**container)
    }

    fn insert(container: &mut Self::MemContainer, key: Self::Key, value: Self::Value) {
        container.insert(key, value);
    }
}

impl From<&DashMap<ContractKey, CodeHash>> for KeyToCodeMap {
    fn from(vals: &DashMap<ContractKey, CodeHash>) -> Self {
        let mut map = vec![];
        for r in vals.iter() {
            map.push((r.key().clone(), *r.value()));
        }
        Self(map)
    }
}

/// Handle contract blob storage on the file system.
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: Cache<CodeHash, Arc<ContractCode<'static>>>,
    key_to_code_part: Arc<DashMap<ContractKey, CodeHash>>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total speed used under a configured threshold

static LOCK_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();
static KEY_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();

impl StoreFsManagement<KeyToCodeMap> for ContractStore {}

impl ContractStore {
    /// # Arguments
    /// - max_size: max size in bytes of the contracts being cached
    pub fn new(contracts_dir: PathBuf, max_size: i64) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";
        let key_to_code_part;
        let _ = LOCK_FILE_PATH.try_insert(contracts_dir.join("__LOCK"));
        // if the lock file exists is from a previous execution so is safe to delete it
        let _ = std::fs::remove_file(LOCK_FILE_PATH.get().unwrap().as_path());
        let key_file = match KEY_FILE_PATH
            .try_insert(contracts_dir.join("KEY_DATA"))
            .map_err(|(e, _)| e)
        {
            Ok(f) => f,
            Err(f) => f,
        };
        if !key_file.exists() {
            std::fs::create_dir_all(&contracts_dir).map_err(|err| {
                tracing::error!("error creating contract dir: {err}");
                err
            })?;
            key_to_code_part = Arc::new(DashMap::new());
            File::create(contracts_dir.join("KEY_DATA"))?;
        } else {
            let map = Self::load_from_file(
                KEY_FILE_PATH.get().unwrap().as_path(),
                LOCK_FILE_PATH.get().unwrap().as_path(),
            )?;
            key_to_code_part = Arc::new(DashMap::from_iter(map.0));
        }
        Self::watch_changes(
            key_to_code_part.clone(),
            KEY_FILE_PATH.get().unwrap().as_path(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;
        Ok(Self {
            contract_cache: Cache::new(100, max_size).expect(ERR),
            contracts_dir,
            key_to_code_part,
        })
    }

    /// Returns a copy of the contract bytes if available, none otherwise.
    // FIXME: instead return Result<Option<_>, _> to handle IO errors upstream
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

        self.key_to_code_part.get(key).and_then(|key| {
            let code_hash = key.value();
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
            self.contract_cache.insert(*code_hash, data.clone(), size);
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

        Self::update(
            &mut self.key_to_code_part,
            key.clone(),
            *code_hash,
            KEY_FILE_PATH.get().unwrap(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;

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

        let version = APIVersion::from(contract);
        let output: Vec<u8> = code.to_bytes_versioned(version)?;
        let mut file = File::create(key_path)?;
        file.write_all(output.as_slice())?;

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
        let key_path = self
            .contracts_dir
            .join(contract_hash.encode())
            .with_extension("wasm");
        std::fs::remove_file(key_path)?;
        Ok(())
    }

    pub fn code_hash_from_key(&self, key: &ContractKey) -> Option<CodeHash> {
        self.key_to_code_part.get(key).map(|r| *r.value())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = std::env::temp_dir()
            .join("locutus-test")
            .join("contract-store-test");
        std::fs::create_dir_all(&contract_dir)?;
        let mut store = ContractStore::new(contract_dir, 10_000)?;
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
