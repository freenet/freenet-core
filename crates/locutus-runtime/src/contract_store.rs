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

use crate::{contract::WrappedContract, DynError, RuntimeInnerError, RuntimeResult};

use super::ContractKey;

type ContractCodeKey = [u8; 32];

#[derive(Serialize, Deserialize)]
struct KeyToCodeMap(Vec<(ContractKey, ContractCodeKey)>);

impl From<&DashMap<ContractKey, ContractCodeKey>> for KeyToCodeMap {
    fn from(vals: &DashMap<ContractKey, ContractCodeKey>) -> Self {
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
        let _ = LOCK_FILE_PATH.try_insert(contracts_dir.join("__LOCK"));
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
    ) -> Option<WrappedContract> {
        let result = key
            .code_hash()
            .and_then(|code_hash| {
                self.contract_cache.get(code_hash).map(|data| {
                    Some(WrappedContract::new(
                        data.value().clone(),
                        params.clone().into_owned(),
                    ))
                })
            })
            .flatten();
        if result.is_some() {
            return result;
        }

        self.key_to_code_part.get(key).and_then(|key| {
            let code_hash = key.value();
            let path = bs58::encode(code_hash.as_slice())
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string()
                .to_lowercase();
            let key_path = self.contracts_dir.join(path).with_extension("wasm");
            let WrappedContract { data, params, .. } =
                WrappedContract::try_from((&*key_path, params.clone().into_owned()))
                    .map_err(|err| {
                        tracing::debug!("contract not found: {err}");
                        err
                    })
                    .ok()?;
            // add back the contract part to the mem store
            let size = data.data().len() as i64;
            self.contract_cache.insert(*code_hash, data.clone(), size);
            Some(WrappedContract::new(data, params))
        })
    }

    /// Store a copy of the contract in the local store, in case it hasn't been stored previously.
    pub fn store_contract(&mut self, contract: WrappedContract) -> RuntimeResult<()> {
        let contract_hash = contract.key().code_hash().ok_or_else(|| {
            tracing::warn!(
                "trying to store partially unspecified contract `{}`",
                contract.key()
            );
            RuntimeInnerError::UnwrapContract
        })?;
        if self.contract_cache.get(contract_hash).is_some() {
            return Ok(());
        }

        Self::acquire_contract_ls_lock()?;
        self.key_to_code_part
            .insert(contract.key().clone(), *contract_hash);
        let map = KeyToCodeMap::from(&*self.key_to_code_part);
        let serialized = bincode::serialize(&map).map_err(|e| RuntimeInnerError::Any(e))?;
        // FIXME: make this more reliable, append to the file instead of truncating it
        let mut f = File::create(KEY_FILE_PATH.get().unwrap())?;
        f.write_all(&serialized)?;
        Self::release_contract_ls_lock()?;

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
                RuntimeInnerError::UnwrapContract
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
        Self::acquire_contract_ls_lock()?;
        let mut f = File::open(KEY_FILE_PATH.get().unwrap())?;
        f.read_to_end(&mut buf)?;
        Self::release_contract_ls_lock()?;
        let map = if buf.is_empty() {
            KeyToCodeMap(vec![])
        } else {
            bincode::deserialize(&buf).map_err(|e| RuntimeInnerError::Any(e))?
        };
        Ok(map)
    }

    fn acquire_contract_ls_lock() -> RuntimeResult<()> {
        let lock = LOCK_FILE_PATH.get().unwrap();
        while lock.exists() {
            thread::sleep(Duration::from_micros(5));
        }
        File::create(lock)?;
        Ok(())
    }

    fn release_contract_ls_lock() -> RuntimeResult<()> {
        match fs::remove_file(LOCK_FILE_PATH.get().unwrap()) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(other) => Err(other.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = std::env::temp_dir().join("locutus-test").join("store-test");
        std::fs::create_dir_all(&contract_dir)?;
        let mut store = ContractStore::new(contract_dir, 10_000)?;
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2])),
            [0, 1].as_ref().into(),
        );
        store.store_contract(contract.clone())?;
        let f = store.fetch_contract(contract.key(), &[0, 1].as_ref().into());
        assert!(f.is_some());
        Ok(())
    }
}
