use std::{
    fs::{self, File},
    io::{Read, Write},
    iter::FromIterator,
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};

use dashmap::DashMap;
use locutus_stdlib::prelude::{ContractCode, Parameters};
use notify::Watcher;
use serde::{Deserialize, Serialize};
use stretto::Cache;

use crate::{contract::WrappedContract, ContractRuntimeError, DynError, RuntimeResult};

use super::ContractKey;

type ContractCodeKey = [u8; 32];

#[derive(Serialize, Deserialize)]
struct KeyToCodeMap(Vec<(ContractKey, ContractCodeKey)>);

impl From<&DashMap<ContractKey, ContractCodeKey>> for KeyToCodeMap {
    fn from(vals: &DashMap<ContractKey, ContractCodeKey>) -> Self {
        let mut map = vec![];
        for r in vals.iter() {
            map.push((*r.key(), *r.value()));
        }
        Self(map)
    }
}

/// Handle contract blob storage on the file system.
#[derive(Clone)]
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: Cache<ContractCodeKey, Arc<ContractCode<'static>>>,
    key_to_code_part: Arc<DashMap<ContractKey, ContractCodeKey>>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total speed used under a configured threshold

static LOCK_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();
static KEY_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();

impl ContractStore {
    /// # Arguments
    /// - max_size: max size in bytes of the contracts being cached
    pub fn new(contracts_dir: PathBuf, max_size: i64) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";
        let key_to_code_part;
        LOCK_FILE_PATH.set(contracts_dir.join("__LOCK")).unwrap();
        KEY_FILE_PATH.set(contracts_dir.join("KEY_DATA")).unwrap();
        if !contracts_dir.exists() {
            std::fs::create_dir_all(&contracts_dir).map_err(|err| {
                tracing::error!("error creating contract dir: {err}");
                err
            })?;
            key_to_code_part = Arc::new(DashMap::new());
            File::create(contracts_dir.join("KEY_DATA"))?;
        } else {
            let map = Self::load_from_file()?;
            key_to_code_part = Arc::new(DashMap::from_iter(map.0));
        }
        Self::watch_changes(key_to_code_part.clone())?;
        Ok(Self {
            contract_cache: Cache::new(100, max_size).expect(ERR),
            contracts_dir,
            key_to_code_part,
        })
    }

    /// Returns a copy of the contract bytes if available, none otherwise.
    pub fn fetch_contract<'a>(
        &self,
        key: &ContractKey,
        params: &Parameters<'a>,
    ) -> Option<WrappedContract<'a>> {
        let contract_hash = if let Some(s) = key.code_hash() {
            s
        } else {
            tracing::warn!("requested partially unspecified contract `{key}`");
            return None;
        };
        if let Some(data) = self.contract_cache.get(contract_hash) {
            Some(WrappedContract::new(data.value().clone(), params.clone()))
        } else {
            let path = bs58::encode(contract_hash)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string()
                .to_lowercase();
            let key_path = self.contracts_dir.join(path).with_extension("wasm");
            let owned_params = Parameters::from(params.as_ref().to_owned());
            let WrappedContract { data, params, .. } =
                WrappedContract::try_from((&*key_path, owned_params))
                    .map_err(|err| {
                        tracing::debug!("contract not found: {err}");
                        err
                    })
                    .ok()?;

            // add back the contract part to the mem store
            let size = data.data().len() as i64;
            self.contract_cache
                .insert(*contract_hash, data.clone(), size);
            Some(WrappedContract::new(data, params))
        }
    }

    /// Store a copy of the contract in the local store, in case it hasn't been stored previously.
    pub fn store_contract(&mut self, contract: WrappedContract) -> RuntimeResult<()> {
        let contract_hash = contract.key().code_hash().ok_or_else(|| {
            tracing::warn!(
                "trying to store partially unspecified contract `{}`",
                contract.key()
            );
            ContractRuntimeError::UnwrapContract
        })?;
        if self.contract_cache.get(contract_hash).is_some() {
            return Ok(());
        }

        // lock the map file to insert changes
        let lock = LOCK_FILE_PATH.get().unwrap();
        while lock.exists() {
            thread::sleep(Duration::from_micros(5));
        }
        File::create(lock)?;
        self.key_to_code_part
            .insert(*contract.key(), *contract_hash);
        let map = KeyToCodeMap::from(&*self.key_to_code_part);
        let serialized = bincode::serialize(&map).map_err(|e| ContractRuntimeError::Any(e))?;
        // FIXME: make this more reliable, append to the file instead of truncating it
        let mut f = File::create(KEY_FILE_PATH.get().unwrap())?;
        f.write_all(&serialized)?;
        // release the lock
        fs::remove_file(LOCK_FILE_PATH.get().unwrap())?;

        let key_path = bs58::encode(contract_hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
            .to_lowercase();
        let key_path = self.contracts_dir.join(key_path).with_extension("wasm");
        if let Ok(code) = WrappedContract::get_data_from_fs(&key_path) {
            let size = code.data().len() as i64;
            self.contract_cache
                .insert(*contract_hash, Arc::new(code), size);
            return Ok(());
        }

        // insert in the memory cache
        let size = contract.code().data().len() as i64;
        let data = contract.code().data().to_vec();
        self.contract_cache
            .insert(*contract_hash, Arc::new(ContractCode::from(data)), size);

        let mut file = File::create(key_path)?;
        file.write_all(contract.code().data())?;

        Ok(())
    }

    pub fn get_contract_path(&mut self, key: &ContractKey) -> RuntimeResult<PathBuf> {
        let contract_hash = match key.code_hash() {
            Some(k) => *k,
            None => self.code_hash_from_key(key).ok_or_else(|| {
                tracing::warn!("trying to store partially unspecified contract `{key}`");
                ContractRuntimeError::UnwrapContract
            })?,
        };

        let key_path = bs58::encode(contract_hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
            .to_lowercase();
        Ok(self.contracts_dir.join(key_path).with_extension("wasm"))
    }

    pub fn code_hash_from_key(&self, key: &ContractKey) -> Option<ContractCodeKey> {
        self.key_to_code_part.get(key).map(|r| *r.value())
    }

    fn watch_changes(ori_map: Arc<DashMap<ContractKey, ContractCodeKey>>) -> Result<(), DynError> {
        let mut watcher = notify::recommended_watcher(
            move |res: Result<notify::Event, notify::Error>| match res {
                Ok(ev) => {
                    if let notify::EventKind::Modify(notify::event::ModifyKind::Data(_)) = ev.kind {
                        match Self::load_from_file() {
                            Err(err) => tracing::error!("{err}"),
                            Ok(map) => {
                                for (k, v) in map.0 {
                                    ori_map.insert(k, v);
                                }
                            }
                        }
                    }
                }
                Err(e) => tracing::error!("{e}"),
            },
        )?;

        watcher.watch(
            KEY_FILE_PATH.get().unwrap(),
            notify::RecursiveMode::NonRecursive,
        )?;
        Ok(())
    }

    fn load_from_file() -> RuntimeResult<KeyToCodeMap> {
        let mut buf = vec![];
        let mut f = File::open(KEY_FILE_PATH.get().unwrap())?;
        f.read_to_end(&mut buf)?;
        let map = if buf.is_empty() {
            KeyToCodeMap(vec![])
        } else {
            bincode::deserialize(&buf).map_err(|e| ContractRuntimeError::Any(e))?
        };
        Ok(map)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = ContractStore::new(
            std::env::temp_dir().join("locutus_test").join("contracts"),
            10_000,
        )?;
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2])),
            [0, 1].as_ref().into(),
        );
        store.store_contract(contract.clone())?;
        let f = store.fetch_contract(contract.key(), &([0, 1].as_ref().into()));
        assert!(f.is_some());
        Ok(())
    }
}
