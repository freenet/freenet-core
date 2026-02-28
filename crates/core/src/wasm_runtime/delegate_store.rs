use dashmap::DashMap;
use freenet_stdlib::prelude::{
    APIVersion, CodeHash, Delegate, DelegateCode, DelegateContainer, DelegateKey,
    DelegateWasmAPIVersion, Parameters,
};
use std::{fs::File, io::Write, path::PathBuf, sync::Arc};
use stretto::Cache;

use crate::contract::storages::Storage;

use super::RuntimeResult;

/// Registration record version for .reg files
const REG_FILE_VERSION: u8 = 1;

pub struct DelegateStore {
    delegates_dir: PathBuf,
    delegate_cache: Cache<CodeHash, DelegateCode<'static>>,
    /// In-memory index: DelegateKey -> CodeHash
    /// Populated from .reg files + ReDb on startup and kept in sync.
    key_to_code_part: Arc<DashMap<DelegateKey, CodeHash>>,
    /// ReDb storage for persistent index (primary runtime store)
    db: Storage,
}

impl DelegateStore {
    /// # Arguments
    /// - delegates_dir: directory where delegate WASM files and .reg records are stored
    /// - max_size: max size in bytes of the delegates being cached
    /// - db: ReDb storage for persistent index
    pub fn new(delegates_dir: PathBuf, max_size: i64, db: Storage) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";

        std::fs::create_dir_all(&delegates_dir).map_err(|err| {
            tracing::error!("error creating delegate dir: {err}");
            err
        })?;

        let key_to_code_part = Arc::new(DashMap::new());

        // Phase 1: Load index from ReDb (primary store)
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

        // Phase 2: Reconcile with .reg files (supplementary backup).
        // Any .reg entries missing from ReDb get restored, ensuring resilience
        // against ReDb corruption or lost entries.
        let mut reg_count = 0u32;
        let mut restored_count = 0u32;

        if let Ok(dir) = std::fs::read_dir(&delegates_dir) {
            for entry in dir.flatten() {
                let path = entry.path();
                if path.extension().map_or(true, |e| e != "reg") {
                    continue;
                }
                let Some(dk_encoded) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let data = match std::fs::read(&path) {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!("Failed to read .reg file {}: {e}", path.display());
                        continue;
                    }
                };
                let Some((code_hash, _params)) = parse_reg_file(&data) else {
                    tracing::warn!(
                        "Failed to parse .reg file {} (corrupt or unsupported version)",
                        path.display()
                    );
                    continue;
                };

                let dk_bytes = match bs58::decode(dk_encoded)
                    .with_alphabet(bs58::Alphabet::BITCOIN)
                    .into_vec()
                {
                    Ok(bytes) if bytes.len() == 32 => {
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&bytes);
                        arr
                    }
                    _ => {
                        tracing::warn!("Invalid delegate key encoding in filename: {dk_encoded}");
                        continue;
                    }
                };

                let delegate_key = DelegateKey::new(dk_bytes, code_hash);
                reg_count += 1;

                // Restore to ReDb + DashMap if missing
                if !key_to_code_part.contains_key(&delegate_key) {
                    if let Err(e) = db.store_delegate_index(&delegate_key, &code_hash) {
                        tracing::warn!("Failed to restore .reg entry to ReDb: {e}");
                    }
                    key_to_code_part.insert(delegate_key, code_hash);
                    restored_count += 1;
                }
            }
        }

        if restored_count > 0 {
            tracing::info!(
                "Restored {restored_count} delegate index entries from .reg files ({reg_count} total .reg files)"
            );
        }

        tracing::debug!("Total delegate index entries: {}", key_to_code_part.len());

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
    /// Updates ReDb, DashMap, and .reg file.
    fn ensure_index_entry(
        &mut self,
        key: &DelegateKey,
        code_hash: &CodeHash,
        params: &Parameters<'_>,
    ) -> RuntimeResult<()> {
        // Ensure .reg file exists (supplementary backup for crash recovery)
        write_reg_file_if_missing(&self.delegates_dir, key, code_hash, params)?;

        if !self.key_to_code_part.contains_key(key) {
            self.db
                .store_delegate_index(key, code_hash)
                .map_err(|e| anyhow::anyhow!("Failed to store delegate index: {e}"))?;
            self.key_to_code_part.insert(key.clone(), *code_hash);
        }
        Ok(())
    }

    /// Extract params from a DelegateContainer by destructuring.
    fn extract_params(delegate: &DelegateContainer) -> Parameters<'static> {
        match delegate {
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(d)) => {
                d.params().clone().into_owned()
            }
            _ => unimplemented!("unsupported delegate container version"),
        }
    }

    pub fn store_delegate(&mut self, delegate: DelegateContainer) -> RuntimeResult<()> {
        let code_hash = delegate.code_hash();
        let key = delegate.key();
        let params = Self::extract_params(&delegate);

        // Early return if already in cache - but ensure index and .reg are updated
        if self.delegate_cache.get(code_hash).is_some() {
            self.ensure_index_entry(key, code_hash, &params)?;
            return Ok(());
        }

        let key_path = code_hash.encode();
        let delegate_path = self.delegates_dir.join(key_path).with_extension("wasm");

        // Early return if file exists on disk - but ensure index and .reg are updated
        if let Ok((code, _ver)) = DelegateCode::load_versioned_from_path(delegate_path.as_path()) {
            self.ensure_index_entry(key, code_hash, &params)?;
            let size = delegate.code().size() as i64;
            self.delegate_cache.insert(*code_hash, code, size);
            return Ok(());
        }

        // CRITICAL ORDER: Write WASM first, then .reg, then ReDb index, then cache.
        // .reg files ensure the index can be rebuilt if ReDb entries are ever lost.

        // Step 1: Save WASM to disk (ensures data is persisted)
        let version = APIVersion::from(delegate.clone());
        let output: Vec<u8> = delegate
            .code()
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&delegate_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?;

        // Step 2: Write .reg registration record (supplementary backup)
        write_reg_file_if_missing(&self.delegates_dir, key, code_hash, &params)?;

        // Step 3: Update index in ReDb (primary persistent store)
        self.db
            .store_delegate_index(key, code_hash)
            .map_err(|e| anyhow::anyhow!("Failed to store delegate index: {e}"))?;

        // Step 4: Update in-memory index
        self.key_to_code_part.insert(key.clone(), *code_hash);

        // Step 5: Insert into memory cache (best-effort, may be rejected by TinyLFU)
        let data = delegate.code().as_ref();
        let code_size = data.len() as i64;
        self.delegate_cache
            .insert(*code_hash, delegate.code().clone().into_owned(), code_size);
        let _cache_result = self.delegate_cache.wait();

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

        // Remove .reg file
        let reg_path = self.delegates_dir.join(key.encode()).with_extension("reg");
        match std::fs::remove_file(&reg_path) {
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
            Ok(_) => {}
        }

        // Remove WASM file (keyed by delegate key encoding — legacy behavior)
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

/// Write a .reg registration record file if it doesn't already exist.
fn write_reg_file_if_missing(
    delegates_dir: &std::path::Path,
    key: &DelegateKey,
    code_hash: &CodeHash,
    params: &Parameters<'_>,
) -> RuntimeResult<()> {
    let reg_path = delegates_dir.join(key.encode()).with_extension("reg");
    if reg_path.exists() {
        return Ok(());
    }

    let params_bytes = params.as_ref();
    let mut reg = Vec::with_capacity(1 + 32 + 4 + params_bytes.len());
    reg.push(REG_FILE_VERSION);
    reg.extend_from_slice(code_hash.as_ref());
    reg.extend_from_slice(&(params_bytes.len() as u32).to_le_bytes());
    reg.extend_from_slice(params_bytes);

    let mut file = File::create(&reg_path)?;
    file.write_all(&reg)?;
    file.sync_all()?;

    tracing::debug!("Wrote .reg file: {}", reg_path.display());
    Ok(())
}

/// Parse a .reg registration record file.
/// Returns (code_hash, params) if valid, None if corrupt/unsupported.
fn parse_reg_file(data: &[u8]) -> Option<(CodeHash, Parameters<'static>)> {
    if data.len() < 37 {
        return None;
    }
    if data[0] != REG_FILE_VERSION {
        return None;
    }
    let mut code_hash_bytes = [0u8; 32];
    code_hash_bytes.copy_from_slice(&data[1..33]);
    let params_len = u32::from_le_bytes(data[33..37].try_into().ok()?) as usize;
    if data.len() < 37 + params_len {
        return None;
    }
    let params = Parameters::from(data[37..37 + params_len].to_vec());
    Some((CodeHash::new(code_hash_bytes), params))
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
        let _cleanup = std::fs::remove_dir_all(&cdelegate_dir);
        Ok(())
    }

    /// Regression test for issue #2845: store_delegate returns Ok but fetch_delegate
    /// fails with "not found" because index wasn't updated in early return paths.
    #[tokio::test]
    async fn store_repairs_missing_index_when_file_exists() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-index-repair-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let delegate = {
            let delegate = Delegate::from((&vec![10, 20, 30].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();
        let code_hash = *delegate.code_hash();

        // Write delegate file directly to disk (simulating previous registration)
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

        // Create a fresh store with empty index (simulating lost index)
        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        assert!(delegate_path.exists(), "Delegate file should exist on disk");
        assert!(
            store.key_to_code_part.is_empty(),
            "Index should be empty initially"
        );

        let fetch_before = store.fetch_delegate(&key, &vec![].into());
        assert!(
            fetch_before.is_none(),
            "Fetch should fail before re-registration"
        );

        store.store_delegate(delegate.clone())?;

        assert!(store.key_to_code_part.contains_key(&key));

        let fetch_after = store.fetch_delegate(&key, &vec![].into());
        assert!(
            fetch_after.is_some(),
            "Fetch should succeed after re-registration"
        );

        Ok(())
    }

    /// Regression test for issue #2845: Two delegates with same WASM code but different
    /// parameters should both be fetchable.
    #[tokio::test]
    async fn store_handles_same_code_different_params() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-same-code-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let wasm_code: Vec<u8> = vec![100, 101, 102, 103];

        let delegate1 = {
            let params1: Vec<u8> = vec![];
            let delegate = Delegate::from((&wasm_code.clone().into(), &params1.into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key1 = delegate1.key().clone();

        let delegate2 = {
            let params2: Vec<u8> = vec![1, 2, 3];
            let delegate = Delegate::from((&wasm_code.clone().into(), &params2.into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key2 = delegate2.key().clone();

        assert_ne!(key1, key2, "Keys should differ when params differ");
        assert_eq!(delegate1.code_hash(), delegate2.code_hash());

        store.store_delegate(delegate1.clone())?;
        assert!(store.key_to_code_part.contains_key(&key1));

        store.store_delegate(delegate2.clone())?;
        assert!(store.key_to_code_part.contains_key(&key2));

        assert!(store.fetch_delegate(&key1, &vec![].into()).is_some());
        assert!(store.fetch_delegate(&key2, &vec![1, 2, 3].into()).is_some());

        Ok(())
    }

    /// .reg files enable index recovery when ReDb entries are lost
    #[tokio::test]
    async fn reg_files_restore_lost_redb_entries() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-reg-restore-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let delegate = {
            let delegate = Delegate::from((&vec![42, 43, 44].into(), &vec![7, 8].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();

        // Store delegate (creates .wasm, .reg, and ReDb entry)
        let db_path = temp_dir.path().join("db1");
        std::fs::create_dir_all(&db_path)?;
        let db = create_test_db(&db_path).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;
        store.store_delegate(delegate.clone())?;

        let reg_path = delegate_dir.join(key.encode()).with_extension("reg");
        assert!(reg_path.exists(), ".reg file should exist after store");

        // Create a NEW store with a fresh (empty) ReDb — simulates lost database.
        // The .reg file should restore the missing entry.
        let db_path2 = temp_dir.path().join("db2");
        std::fs::create_dir_all(&db_path2)?;
        let db2 = create_test_db(&db_path2).await;
        let store2 = DelegateStore::new(delegate_dir.clone(), 10_000, db2)?;

        assert!(
            store2.key_to_code_part.contains_key(&key),
            "Index should be restored from .reg file"
        );

        let fetched = store2.fetch_delegate(&key, &vec![7, 8].into());
        assert!(
            fetched.is_some(),
            "Delegate should be fetchable after .reg restore"
        );

        Ok(())
    }

    /// remove_delegate removes .reg file alongside WASM and index entries
    #[tokio::test]
    async fn remove_delegate_cleans_reg_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-remove-reg-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let delegate = {
            let delegate = Delegate::from((&vec![50, 51, 52].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();
        store.store_delegate(delegate)?;

        let reg_path = delegate_dir.join(key.encode()).with_extension("reg");
        assert!(reg_path.exists());

        store.remove_delegate(&key)?;

        assert!(!reg_path.exists(), ".reg file should be removed");
        assert!(store.fetch_delegate(&key, &vec![].into()).is_none());

        Ok(())
    }

    /// Storing same delegate twice is idempotent
    #[tokio::test]
    async fn idempotent_store_preserves_reg() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let delegate_dir = temp_dir.path().join("delegates-idempotent-test");
        std::fs::create_dir_all(&delegate_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = DelegateStore::new(delegate_dir.clone(), 10_000, db)?;

        let delegate = {
            let delegate = Delegate::from((&vec![60, 61].into(), &vec![].into()));
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate))
        };
        let key = delegate.key().clone();

        store.store_delegate(delegate.clone())?;
        let reg_path = delegate_dir.join(key.encode()).with_extension("reg");
        let mtime1 = std::fs::metadata(&reg_path)?.modified()?;

        store.store_delegate(delegate)?;
        let mtime2 = std::fs::metadata(&reg_path)?.modified()?;

        assert_eq!(mtime1, mtime2, ".reg file should not be rewritten");

        Ok(())
    }

    /// parse_reg_file handles valid and invalid data correctly
    #[tokio::test]
    async fn parse_reg_file_validation() -> Result<(), Box<dyn std::error::Error>> {
        // Valid: version 1, 32-byte hash, 0-length params
        let mut valid = vec![1u8];
        valid.extend_from_slice(&[0u8; 32]);
        valid.extend_from_slice(&0u32.to_le_bytes());
        assert!(parse_reg_file(&valid).is_some());

        // Valid with params
        let mut valid_with_params = vec![1u8];
        valid_with_params.extend_from_slice(&[1u8; 32]);
        valid_with_params.extend_from_slice(&3u32.to_le_bytes());
        valid_with_params.extend_from_slice(&[10, 20, 30]);
        let (_, params) = parse_reg_file(&valid_with_params).unwrap();
        assert_eq!(params.as_ref(), &[10, 20, 30]);

        // Too short
        assert!(parse_reg_file(&[1u8; 10]).is_none());

        // Wrong version
        let mut wrong_version = vec![99u8];
        wrong_version.extend_from_slice(&[0u8; 32]);
        wrong_version.extend_from_slice(&0u32.to_le_bytes());
        assert!(parse_reg_file(&wrong_version).is_none());

        // Truncated params
        let mut truncated = vec![1u8];
        truncated.extend_from_slice(&[0u8; 32]);
        truncated.extend_from_slice(&10u32.to_le_bytes());
        truncated.extend_from_slice(&[1, 2, 3]);
        assert!(parse_reg_file(&truncated).is_none());

        Ok(())
    }
}
