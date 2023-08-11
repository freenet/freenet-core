use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};
use stretto::Cache;

use crate::store::{StoreEntriesContainer, StoreFsManagement};
use crate::RuntimeResult;
use locutus_stdlib::prelude::{
    APIVersion, CodeHash, Delegate, DelegateCode, DelegateContainer, DelegateKey,
    DelegateWasmAPIVersion, Parameters,
};

const DEFAULT_MAX_SIZE: i64 = 10 * 1024 * 1024 * 20;

#[derive(Serialize, Deserialize, Default)]
struct KeyToCodeMap(Vec<(DelegateKey, CodeHash)>);

impl StoreEntriesContainer for KeyToCodeMap {
    type MemContainer = Arc<DashMap<DelegateKey, CodeHash>>;
    type Key = DelegateKey;
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

impl From<&DashMap<DelegateKey, CodeHash>> for KeyToCodeMap {
    fn from(vals: &DashMap<DelegateKey, CodeHash>) -> Self {
        let mut map = vec![];
        for r in vals.iter() {
            map.push((r.key().clone(), r.value().clone()));
        }
        Self(map)
    }
}

pub struct DelegateStore {
    delegates_dir: PathBuf,
    delegate_cache: Cache<CodeHash, DelegateCode<'static>>,
    key_to_code_part: Arc<DashMap<DelegateKey, CodeHash>>,
}

static LOCK_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();
static KEY_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();

impl StoreFsManagement<KeyToCodeMap> for DelegateStore {}

impl DelegateStore {
    /// # Arguments
    /// - max_size: max size in bytes of the delegates being cached
    pub fn new(delegates_dir: PathBuf, max_size: i64) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";
        let key_to_delegate_part;
        let _ = LOCK_FILE_PATH.try_insert(delegates_dir.join("__LOCK"));
        let key_file = match KEY_FILE_PATH
            .try_insert(delegates_dir.join("KEY_DATA"))
            .map_err(|(e, _)| e)
        {
            Ok(f) => f,
            Err(f) => f,
        };
        if !key_file.exists() {
            std::fs::create_dir_all(&delegates_dir).map_err(|err| {
                tracing::error!("error creating delegate dir: {err}");
                err
            })?;
            key_to_delegate_part = Arc::new(DashMap::new());
            File::create(delegates_dir.join("KEY_DATA"))?;
        } else {
            let map = Self::load_from_file(
                KEY_FILE_PATH.get().unwrap().as_path(),
                LOCK_FILE_PATH.get().unwrap().as_path(),
            )?;
            key_to_delegate_part = Arc::new(DashMap::from_iter(map.0));
        }
        Self::watch_changes(
            key_to_delegate_part.clone(),
            KEY_FILE_PATH.get().unwrap().as_path(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;
        Ok(Self {
            delegate_cache: Cache::new(100, max_size).expect(ERR),
            delegates_dir,
            key_to_code_part: key_to_delegate_part,
        })
    }

    // Returns a copy of the delegate bytes if available, none otherwise.
    pub fn fetch_delegate(
        &self,
        key: &DelegateKey,
        params: &Parameters<'_>,
    ) -> Option<Delegate<'static>> {
        if let Some(delegate_code) = self.delegate_cache.get(key.code_hash()) {
            return Some(Delegate::from((delegate_code.value(), params)).into_owned());
        }
        self.key_to_code_part.get(key).and_then(|code_part| {
            let delegate_code_path = self
                .delegates_dir
                .join(code_part.value().encode())
                .with_extension("wasm");
            tracing::debug!("loading delegate `{key}` from {delegate_code_path:?}");
            let DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate {
                data: delegate_code,
                ..
            })) = DelegateContainer::try_from((
                delegate_code_path.as_path(),
                params.clone().into_owned(),
            ))
            .ok()? else {
                unimplemented!()
            };
            tracing::debug!("loaded `{key}` from path");
            let size = delegate_code.as_ref().len() as i64;
            let delegate = Delegate::from((&delegate_code, &params.clone().into_owned()));
            self.delegate_cache
                .insert(key.code_hash().clone(), delegate_code, size);
            Some(delegate)
        })
    }

    pub fn store_delegate(&mut self, delegate: DelegateContainer) -> RuntimeResult<()> {
        let code_hash = delegate.code_hash();
        if self.delegate_cache.get(code_hash).is_some() {
            return Ok(());
        }

        let key = delegate.key();
        Self::update(
            &mut self.key_to_code_part,
            key.clone(),
            code_hash.clone(),
            KEY_FILE_PATH.get().unwrap(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;

        let key_path = code_hash.encode();
        let delegate_path = self.delegates_dir.join(key_path).with_extension("wasm");
        if let Ok((code, _ver)) = DelegateCode::load_versioned_from_path(delegate_path.as_path()) {
            let size = delegate.code().size() as i64;
            self.delegate_cache.insert(code_hash.clone(), code, size);
            return Ok(());
        }

        // insert in the memory cache
        let data = delegate.code().as_ref();
        let code_size = data.len() as i64;
        self.delegate_cache.insert(
            code_hash.clone(),
            delegate.code().clone().into_owned(),
            code_size,
        );

        let version = APIVersion::from(delegate.clone());
        let output: Vec<u8> = delegate.code().to_bytes_versioned(version)?;
        let mut file = File::create(delegate_path)?;
        file.write_all(output.as_slice())?;

        Ok(())
    }

    pub fn remove_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()> {
        self.delegate_cache.remove(key.code_hash());
        let cmp_path = self.delegates_dir.join(key.encode()).with_extension("wasm");
        match std::fs::remove_file(cmp_path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
            Ok(_) => Ok(()),
        }
    }

    pub fn get_delegate_path(&mut self, key: &DelegateKey) -> RuntimeResult<PathBuf> {
        let key_path = key.encode().to_lowercase();
        Ok(self.delegates_dir.join(key_path).with_extension("wasm"))
    }

    pub fn code_hash_from_key(&self, key: &DelegateKey) -> Option<CodeHash> {
        self.key_to_code_part.get(key).map(|r| r.value().clone())
    }
}

impl Default for DelegateStore {
    fn default() -> Self {
        Self {
            delegates_dir: Default::default(),
            delegate_cache: Cache::new(100, DEFAULT_MAX_SIZE).unwrap(),
            key_to_code_part: Arc::new(DashMap::new()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let cdelegate_dir = std::env::temp_dir()
            .join("locutus-test")
            .join("delegates-store-test");
        std::fs::create_dir_all(&cdelegate_dir)?;
        let mut store = DelegateStore::new(cdelegate_dir, 10_000)?;
        let delegate = {
            let delegate = Delegate::from((&vec![0, 1, 2].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        store.store_delegate(delegate.clone())?;
        let f = store.fetch_delegate(delegate.key(), &vec![].into());
        assert!(f.is_some());
        Ok(())
    }
}
