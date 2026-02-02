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

        Ok(Self {
            delegate_cache: Cache::new(100, max_size).expect(ERR),
            delegates_dir,
            key_to_code_part,
            db,
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

    /// Ensures the index mapping exists for a key, repairing it if missing.
    /// This handles crash recovery and same-code-different-params scenarios.
    fn ensure_index_entry(&mut self, key: &DelegateKey, code_hash: &CodeHash) -> RuntimeResult<()> {
        if !self.key_to_code_part.contains_key(key) {
            self.db
                .store_delegate_index(key, code_hash)
                .map_err(|e| anyhow::anyhow!("Failed to store delegate index: {e}"))?;
            self.key_to_code_part.insert(key.clone(), *code_hash);
        }
        Ok(())
    }

    pub fn store_delegate(&mut self, delegate: DelegateContainer) -> RuntimeResult<()> {
        let code_hash = delegate.code_hash();
        let key = delegate.key();

        // Early return if already in cache - but ensure index is updated
        if self.delegate_cache.get(code_hash).is_some() {
            self.ensure_index_entry(key, code_hash)?;
            return Ok(());
        }

        let key_path = code_hash.encode();
        let delegate_path = self.delegates_dir.join(key_path).with_extension("wasm");

        // Early return if file exists on disk - but ensure index is updated
        if let Ok((code, _ver)) = DelegateCode::load_versioned_from_path(delegate_path.as_path()) {
            self.ensure_index_entry(key, code_hash)?;
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

    /// Regression test for issue #2845: store_delegate returns Ok but fetch_delegate
    /// fails with "not found" because index wasn't updated in early return paths.
    ///
    /// This test simulates the scenario where:
    /// 1. Delegate file exists on disk (from previous registration)
    /// 2. Index entry is missing (simulating corruption/crash)
    /// 3. Re-registering should repair the index
    #[tokio::test]
    async fn store_repairs_missing_index_when_file_exists() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-index-repair-test");
        std::fs::create_dir_all(&delegate_dir)?;

        // Create delegate
        let delegate = {
            let delegate = Delegate::from((&vec![10, 20, 30].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();
        let code_hash = *delegate.code_hash();

        // Step 1: Write the delegate file directly to disk (simulating previous registration)
        let key_path = code_hash.encode();
        let delegate_path = delegate_dir.join(key_path).with_extension("wasm");
        let version = APIVersion::from(delegate.clone());
        let output: Vec<u8> = delegate
            .code()
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&delegate_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?;

        // Step 2: Create a fresh store with empty index (simulating lost index)
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        // Verify file exists but index is empty
        assert!(delegate_path.exists(), "Delegate file should exist on disk");
        assert!(
            store.key_to_code_part.is_empty(),
            "Index should be empty initially"
        );

        // Step 3: Without the fix, fetch would fail here
        let fetch_before = store.fetch_delegate(&key, &vec![].into());
        assert!(
            fetch_before.is_none(),
            "Fetch should fail before re-registration (index is empty)"
        );

        // Step 4: Re-register the delegate - this should repair the index
        store.store_delegate(delegate.clone())?;

        // Step 5: Verify the index was repaired
        assert!(
            store.key_to_code_part.contains_key(&key),
            "Index should now contain the key"
        );

        // Step 6: Fetch should now succeed
        let fetch_after = store.fetch_delegate(&key, &vec![].into());
        assert!(
            fetch_after.is_some(),
            "Fetch should succeed after re-registration repaired the index"
        );

        Ok(())
    }

    /// Regression test for issue #2845: Two delegates with same WASM code but different
    /// parameters should both be fetchable.
    ///
    /// Before the fix, registering a second delegate with the same code but different
    /// params would short-circuit at the cache check without updating the index for
    /// the new key.
    #[tokio::test]
    async fn store_handles_same_code_different_params() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-same-code-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        // Same WASM code
        let wasm_code: Vec<u8> = vec![100, 101, 102, 103];

        // Delegate 1: empty parameters
        let delegate1 = {
            let params1: Vec<u8> = vec![];
            let delegate = Delegate::from((&wasm_code.clone().into(), &params1.into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key1 = delegate1.key().clone();

        // Delegate 2: different parameters (same code, different key)
        let delegate2 = {
            let params2: Vec<u8> = vec![1, 2, 3];
            let delegate = Delegate::from((&wasm_code.clone().into(), &params2.into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key2 = delegate2.key().clone();

        // Keys should be different because params differ
        assert_ne!(
            key1, key2,
            "Keys should differ when params differ (even with same code)"
        );

        // Code hash should be the same
        assert_eq!(
            delegate1.code_hash(),
            delegate2.code_hash(),
            "Code hash should be the same"
        );

        // Store first delegate
        store.store_delegate(delegate1.clone())?;
        assert!(
            store.key_to_code_part.contains_key(&key1),
            "Index should have key1"
        );

        // Store second delegate - before fix, this would short-circuit
        // at cache check without adding key2 to index
        store.store_delegate(delegate2.clone())?;
        assert!(
            store.key_to_code_part.contains_key(&key2),
            "Index should have key2 after registration"
        );

        // Both should be fetchable
        let fetch1 = store.fetch_delegate(&key1, &vec![].into());
        let fetch2 = store.fetch_delegate(&key2, &vec![1, 2, 3].into());

        assert!(fetch1.is_some(), "First delegate should be fetchable");
        assert!(fetch2.is_some(), "Second delegate should be fetchable");

        Ok(())
    }
}
