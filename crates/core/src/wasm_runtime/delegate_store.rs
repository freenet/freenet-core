use dashmap::DashMap;
use freenet_stdlib::prelude::{
    APIVersion, CodeHash, Delegate, DelegateCode, DelegateContainer, DelegateKey,
    DelegateWasmAPIVersion, Parameters,
};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};
use stretto::Cache;

use crate::wasm_runtime::store::SafeWriter;

use super::store::StoreFsManagement;
use super::RuntimeResult;

pub struct DelegateStore {
    delegates_dir: PathBuf,
    delegate_cache: Cache<CodeHash, DelegateCode<'static>>,
    key_to_code_part: Arc<DashMap<DelegateKey, (u64, CodeHash)>>,
    index_file: SafeWriter<Self>,
    key_file: PathBuf,
}

impl StoreFsManagement for DelegateStore {
    type MemContainer = Arc<DashMap<DelegateKey, (u64, CodeHash)>>;
    type Key = DelegateKey;
    type Value = CodeHash;

    fn insert_in_container(
        container: &mut Self::MemContainer,
        (key, offset): (Self::Key, u64),
        value: Self::Value,
    ) {
        container.insert(key, (offset, value));
    }
}

impl DelegateStore {
    /// # Arguments
    /// - max_size: max size in bytes of the delegates being cached
    pub fn new(delegates_dir: PathBuf, max_size: i64) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";
        let mut key_to_code_part = Arc::new(DashMap::new());
        let key_file = delegates_dir.join("KEY_DATA");
        if !key_file.exists() {
            std::fs::create_dir_all(&delegates_dir).map_err(|err| {
                tracing::error!("error creating delegate dir: {err}");
                err
            })?;
            File::create(delegates_dir.join("KEY_DATA"))?;
        } else {
            Self::load_from_file(&key_file, &mut key_to_code_part)?;
        }
        Self::watch_changes(key_to_code_part.clone(), &key_file)?;

        let index_file = SafeWriter::new(&key_file, false)?;
        Ok(Self {
            delegate_cache: Cache::new(100, max_size).expect(ERR),
            delegates_dir,
            key_to_code_part,
            index_file,
            key_file,
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
                .join(code_part.value().1.encode())
                .with_extension("wasm");
            tracing::debug!("loading delegate `{key}` from {delegate_code_path:?}");
            let DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate {
                data: delegate_code,
                ..
            })) = DelegateContainer::try_from((
                delegate_code_path.as_path(),
                params.clone().into_owned(),
            ))
            .ok()?
            else {
                unimplemented!()
            };
            tracing::debug!("loaded `{key}` from path");
            let size = delegate_code.as_ref().len() as i64;
            let delegate = Delegate::from((&delegate_code, &params.clone().into_owned()));
            self.delegate_cache
                .insert(*key.code_hash(), delegate_code, size);
            Some(delegate)
        })
    }

    pub fn store_delegate(&mut self, delegate: DelegateContainer) -> RuntimeResult<()> {
        let code_hash = delegate.code_hash();
        if self.delegate_cache.get(code_hash).is_some() {
            return Ok(());
        }

        let key = delegate.key();

        let key_path = code_hash.encode();
        let delegate_path = self.delegates_dir.join(key_path).with_extension("wasm");
        if let Ok((code, _ver)) = DelegateCode::load_versioned_from_path(delegate_path.as_path()) {
            let size = delegate.code().size() as i64;
            self.delegate_cache.insert(*code_hash, code, size);
            return Ok(());
        }

        // insert in the memory cache
        let data = delegate.code().as_ref();
        let code_size = data.len() as i64;
        self.delegate_cache
            .insert(*code_hash, delegate.code().clone().into_owned(), code_size);

        // save on disc
        let version = APIVersion::from(delegate.clone());
        let output: Vec<u8> = delegate.code().to_bytes_versioned(version)?;
        let mut file = File::create(delegate_path)?;
        file.write_all(output.as_slice())?;

        // Update index
        let keys = self.key_to_code_part.entry(key.clone());
        match keys {
            dashmap::mapref::entry::Entry::Occupied(mut v) => {
                let current_version_offset = v.get().0;
                let prev_val = &mut v.get_mut().1;
                // first mark the old entry (if it exists) as removed
                Self::remove(&self.key_file, current_version_offset)?;
                let new_offset = Self::insert(&mut self.index_file, key.clone(), code_hash)?;
                *prev_val = *code_hash;
                v.get_mut().0 = new_offset;
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let offset = Self::insert(&mut self.index_file, key.clone(), code_hash)?;
                v.insert((offset, *code_hash));
            }
        }

        Ok(())
    }

    pub fn remove_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()> {
        self.delegate_cache.remove(key.code_hash());
        let cmp_path: PathBuf = self.delegates_dir.join(key.encode()).with_extension("wasm");
        if let Some((_, (offset, _))) = self.key_to_code_part.remove(key) {
            Self::remove(&self.key_file, offset)?;
        }
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
        self.key_to_code_part.get(key).map(|r| r.value().1)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let cdelegate_dir = std::env::temp_dir()
            .join("freenet-test")
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
