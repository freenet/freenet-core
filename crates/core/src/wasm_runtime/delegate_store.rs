use dashmap::DashMap;
use freenet_stdlib::prelude::{
    APIVersion, CodeHash, Delegate, DelegateCode, DelegateContainer, DelegateKey,
    DelegateWasmAPIVersion, Parameters,
};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};
use stretto::Cache;

use crate::contract::storages::Storage;

use super::RuntimeResult;

pub struct DelegateStore {
    delegates_dir: PathBuf,
    delegate_cache: Cache<CodeHash, DelegateCode<'static>>,
    /// In-memory index: DelegateKey -> CodeHash
    /// This is populated from ReDb on startup and kept in sync
    key_to_code_part: Arc<DashMap<DelegateKey, CodeHash>>,
    /// ReDb storage for persistent index
    db: Storage,
}

impl DelegateStore {
    /// # Arguments
    /// - delegates_dir: directory where delegate WASM files are stored
    /// - max_size: max size in bytes of the delegates being cached
    /// - db: ReDb storage for persistent index
    pub fn new(delegates_dir: PathBuf, max_size: i64, db: Storage) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";

        std::fs::create_dir_all(&delegates_dir).map_err(|err| {
            tracing::error!("error creating delegate dir: {err}");
            err
        })?;

        // Load index from ReDb
        let key_to_code_part = Arc::new(DashMap::new());
        match db.load_all_delegate_index() {
            Ok(entries) => {
                for (delegate_key, code_hash) in entries {
                    key_to_code_part.insert(delegate_key, code_hash);
                }
                tracing::debug!(
                    "Loaded {} delegate index entries from ReDb",
                    key_to_code_part.len()
                );
            }
            Err(e) => {
                tracing::warn!("Failed to load delegate index from ReDb: {e}");
            }
        }

        // Migrate from legacy KEY_DATA file if it exists and hasn't been migrated
        let key_file = delegates_dir.join("KEY_DATA");
        let migration_marker = delegates_dir.join(".migration_complete");
        if key_file.exists() && !migration_marker.exists() {
            Self::migrate_from_legacy(&key_file, &db, &key_to_code_part)?;
        }

        Ok(Self {
            delegate_cache: Cache::new(100, max_size).expect(ERR),
            delegates_dir,
            key_to_code_part,
            db,
        })
    }

    /// Migrate data from the legacy KEY_DATA file to ReDb.
    /// After successful migration, renames the file to KEY_DATA.migrated.
    fn migrate_from_legacy(
        key_file: &PathBuf,
        db: &Storage,
        key_to_code_part: &DashMap<DelegateKey, CodeHash>,
    ) -> RuntimeResult<()> {
        use super::store::StoreFsManagement;

        tracing::info!("Migrating delegate index from legacy KEY_DATA to ReDb");

        // Use a temporary DashMap for the legacy loader
        let mut legacy_container: Arc<DashMap<DelegateKey, (u64, CodeHash)>> =
            Arc::new(DashMap::new());

        // Load from legacy file format
        struct LegacyLoader;
        impl super::store::StoreFsManagement for LegacyLoader {
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

            fn clear_container(container: &mut Self::MemContainer) {
                container.clear();
            }
        }

        LegacyLoader::load_from_file(key_file, &mut legacy_container)?;

        let count = legacy_container.len();
        tracing::info!("Found {count} entries in legacy KEY_DATA file");

        // Collect entries for batch insert
        let entries: Vec<(DelegateKey, CodeHash)> = legacy_container
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().1))
            .collect();

        // Batch insert into ReDb (single transaction)
        db.store_delegate_index_batch(&entries).map_err(|e| {
            tracing::error!("Failed to migrate delegate index entries: {e}");
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
        })?;

        // Update in-memory map
        for (delegate_key, code_hash) in &entries {
            key_to_code_part.insert(delegate_key.clone(), *code_hash);
        }

        // Verify migration succeeded by reading back from ReDb
        let verified = db.load_all_delegate_index().map_err(|e| {
            tracing::error!("Failed to verify migration: {e}");
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
        })?;

        if verified.len() != count {
            let msg = format!(
                "Migration verification failed: wrote {} entries, read back {}",
                count,
                verified.len()
            );
            tracing::error!("{msg}");
            return Err(std::io::Error::new(std::io::ErrorKind::Other, msg).into());
        }

        tracing::info!("Migrated and verified {count} delegate index entries to ReDb");

        // Create marker file to prevent re-migration even if rename fails
        let marker_path = key_file.parent().unwrap().join(".migration_complete");
        if let Err(e) = std::fs::write(&marker_path, b"delegate_store") {
            tracing::warn!("Failed to create migration marker: {e}");
        }

        // Rename the legacy file to mark it as migrated
        let migrated_path = key_file.with_extension("migrated");
        if let Err(e) = std::fs::rename(key_file, &migrated_path) {
            tracing::warn!("Failed to rename KEY_DATA to .migrated: {e}");
        } else {
            tracing::info!("Renamed legacy KEY_DATA to {migrated_path:?}");
        }

        Ok(())
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
        self.key_to_code_part.get(key).and_then(|code_hash_entry| {
            let code_hash = *code_hash_entry.value();
            let delegate_code_path = self
                .delegates_dir
                .join(code_hash.encode())
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

        // CRITICAL ORDER: Write disk first, then index, then cache.
        // This ensures fetch_delegate() can always fall back to disk lookup
        // even if the cache insert is rejected by TinyLFU admission policy.
        // See issue #2306 - stretto's cache.wait() doesn't guarantee visibility.

        // Step 1: Save to disk first (ensures data is persisted)
        let version = APIVersion::from(delegate.clone());
        let output: Vec<u8> = delegate
            .code()
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&delegate_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?; // Ensure durability before updating index

        // Step 2: Update index in ReDb (persistent, crash-safe)
        self.db
            .store_delegate_index(key, code_hash)
            .map_err(|e| anyhow::anyhow!("Failed to store delegate index: {e}"))?;

        // Step 3: Update in-memory index
        self.key_to_code_part.insert(key.clone(), *code_hash);

        // Step 4: Insert into memory cache (best-effort, may be rejected by TinyLFU)
        let data = delegate.code().as_ref();
        let code_size = data.len() as i64;
        self.delegate_cache
            .insert(*code_hash, delegate.code().clone().into_owned(), code_size);
        // Wait for cache to process the insert. Even if TinyLFU rejects it,
        // the disk fallback above ensures the delegate can still be fetched.
        let _ = self.delegate_cache.wait();

        Ok(())
    }

    pub fn remove_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()> {
        self.delegate_cache.remove(key.code_hash());

        // Remove from ReDb index
        self.db
            .remove_delegate_index(key)
            .map_err(|e| anyhow::anyhow!("Failed to remove delegate index: {e}"))?;

        // Remove from in-memory index
        self.key_to_code_part.remove(key);

        let cmp_path: PathBuf = self.delegates_dir.join(key.encode()).with_extension("wasm");
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
        self.key_to_code_part.get(key).map(|r| *r.value())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    async fn create_test_db(path: &std::path::Path) -> Storage {
        Storage::new(path).await.expect("failed to create test db")
    }

    #[tokio::test]
    async fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let cdelegate_dir = temp_dir.path().join("delegates-store-test");
        std::fs::create_dir_all(&cdelegate_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(cdelegate_dir.clone(), 10_000, db)?;
        let delegate = {
            let delegate = Delegate::from((&vec![0, 1, 2].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        store.store_delegate(delegate.clone())?;
        let f = store.fetch_delegate(delegate.key(), &vec![].into());
        assert!(f.is_some());
        // Clean up after test
        let _ = std::fs::remove_dir_all(&cdelegate_dir);
        Ok(())
    }
}
