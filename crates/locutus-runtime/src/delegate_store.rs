use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};
use stretto::Cache;

use crate::store::{StoreEntriesContainer, StoreFsManagement};
use crate::RuntimeResult;
use locutus_stdlib::prelude::{CodeHash, Delegate, DelegateKey};

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
    delegate_cache: Cache<CodeHash, Delegate<'static>>,
    key_to_delegate_part: Arc<DashMap<DelegateKey, CodeHash>>,
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
            key_to_delegate_part,
        })
    }

    // Returns a copy of the delegate bytes if available, none otherwise.
    pub fn fetch_delegate(&self, key: &DelegateKey) -> Option<Delegate<'static>> {
        if let Some(delegate) = self.delegate_cache.get(key.code_hash()) {
            return Some(delegate.value().clone());
        }
        self.key_to_delegate_part.get(key).and_then(|_| {
            let delegate_path = self.delegates_dir.join(key.encode()).with_extension("wasm");
            let delegate = Delegate::try_from(delegate_path.as_path()).ok()?;
            let size = delegate.as_ref().len() as i64;
            self.delegate_cache
                .insert(key.code_hash().clone(), delegate.clone(), size);
            Some(delegate)
        })
    }

    pub fn store_delegate(&mut self, delegate: Delegate<'_>) -> RuntimeResult<()> {
        let key = delegate.key();
        let delegate_hash = delegate.hash();
        if self.delegate_cache.get(delegate_hash).is_some() {
            return Ok(());
        }

        Self::update(
            &mut self.key_to_delegate_part,
            key.clone(),
            delegate_hash.clone(),
            KEY_FILE_PATH.get().unwrap(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;

        let delegate_path = self.delegates_dir.join(key.encode()).with_extension("wasm");
        if let Ok(delegate) = Delegate::try_from(delegate_path.as_path()) {
            let size = delegate.as_ref().len() as i64;
            self.delegate_cache
                .insert(delegate_hash.clone(), delegate.clone(), size);
            return Ok(());
        }

        // insert in the memory cache
        let data = delegate.as_ref();
        let code_size = data.len() as i64;
        self.delegate_cache.insert(
            delegate_hash.clone(),
            Delegate::from(data.to_vec()),
            code_size,
        );

        let mut output: Vec<u8> = Vec::with_capacity(code_size as usize);
        output.append(&mut delegate.as_ref().to_vec());
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
        self.key_to_delegate_part
            .get(key)
            .map(|r| r.value().clone())
    }
}

impl Default for DelegateStore {
    fn default() -> Self {
        Self {
            delegates_dir: Default::default(),
            delegate_cache: Cache::new(100, DEFAULT_MAX_SIZE).unwrap(),
            key_to_delegate_part: Arc::new(DashMap::new()),
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
        let delegate = Delegate::from(vec![0, 1, 2]);
        store.store_delegate(delegate.clone())?;
        let f = store.fetch_delegate(&delegate.key());
        assert!(f.is_some());
        Ok(())
    }
}
