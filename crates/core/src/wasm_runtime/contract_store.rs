use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use freenet_stdlib::prelude::*;
use stretto::Cache;

use crate::contract::storages::Storage;

use super::RuntimeResult;

/// Handle contract blob storage on the file system.
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: Cache<CodeHash, Arc<ContractCode<'static>>>,
    /// In-memory index: ContractInstanceId -> CodeHash
    /// This is populated from ReDb on startup and kept in sync
    key_to_code_part: Arc<DashMap<ContractInstanceId, CodeHash>>,
    /// ReDb storage for persistent index
    db: Storage,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total space used under a configured threshold

impl ContractStore {
    /// # Arguments
    /// - contracts_dir: directory where contract WASM files are stored
    /// - max_size: max size in bytes of the contracts being cached
    /// - db: ReDb storage for persistent index
    pub fn new(contracts_dir: PathBuf, max_size: i64, db: Storage) -> RuntimeResult<Self> {
        const ERR: &str = "failed to build mem cache";

        std::fs::create_dir_all(&contracts_dir).map_err(|err| {
            tracing::error!("error creating contract dir: {err}");
            err
        })?;

        // Load index from ReDb
        let key_to_code_part = Arc::new(DashMap::new());
        match db.load_all_contract_index() {
            Ok(entries) => {
                for (instance_id, code_hash) in entries {
                    key_to_code_part.insert(instance_id, code_hash);
                }
                tracing::debug!(
                    "Loaded {} contract index entries from ReDb",
                    key_to_code_part.len()
                );
            }
            Err(e) => {
                tracing::warn!("Failed to load contract index from ReDb: {e}");
            }
        }

        // Migrate from legacy KEY_DATA file if it exists
        let key_file = contracts_dir.join("KEY_DATA");
        if key_file.exists() {
            if let Err(e) = Self::migrate_from_legacy(&key_file, &db, &key_to_code_part) {
                tracing::warn!("Failed to migrate legacy KEY_DATA: {e}");
            }
        }

        Ok(Self {
            contract_cache: Cache::new(100, max_size).expect(ERR),
            contracts_dir,
            key_to_code_part,
            db,
        })
    }

    /// Migrate data from the legacy KEY_DATA file to ReDb.
    /// After successful migration, renames the file to KEY_DATA.migrated.
    fn migrate_from_legacy(
        key_file: &PathBuf,
        db: &Storage,
        key_to_code_part: &DashMap<ContractInstanceId, CodeHash>,
    ) -> RuntimeResult<()> {
        use super::store::StoreFsManagement;

        tracing::info!("Migrating contract index from legacy KEY_DATA to ReDb");

        // Use a temporary DashMap for the legacy loader
        let mut legacy_container: Arc<DashMap<ContractInstanceId, (u64, CodeHash)>> =
            Arc::new(DashMap::new());

        // Load from legacy file format
        // Note: We're using a helper struct to access the static load_from_file method
        struct LegacyLoader;
        impl super::store::StoreFsManagement for LegacyLoader {
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

            fn clear_container(container: &mut Self::MemContainer) {
                container.clear();
            }
        }

        LegacyLoader::load_from_file(key_file, &mut legacy_container)?;

        let count = legacy_container.len();
        tracing::info!("Found {count} entries in legacy KEY_DATA file");

        // Migrate each entry to ReDb
        let mut migrated = 0;
        for entry in legacy_container.iter() {
            let instance_id = entry.key();
            let code_hash = &entry.value().1;

            // Store in ReDb
            if let Err(e) = db.store_contract_index(instance_id, code_hash) {
                tracing::warn!("Failed to migrate contract index entry: {e}");
                continue;
            }

            // Update in-memory map
            key_to_code_part.insert(*instance_id, *code_hash);
            migrated += 1;
        }

        tracing::info!("Migrated {migrated}/{count} contract index entries to ReDb");

        // Rename the legacy file to mark it as migrated
        let migrated_path = key_file.with_extension("migrated");
        if let Err(e) = std::fs::rename(key_file, &migrated_path) {
            tracing::warn!("Failed to rename KEY_DATA to .migrated: {e}");
        } else {
            tracing::info!("Renamed legacy KEY_DATA to {migrated_path:?}");
        }

        Ok(())
    }

    /// Returns a copy of the contract bytes if available, none otherwise.
    // todo: instead return Result<Option<_>, _> to handle IO errors upstream
    pub fn fetch_contract(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        let code_hash = key.code_hash();
        if let Some(data) = self.contract_cache.get(code_hash) {
            return Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                WrappedContract::new(data.value().clone(), params.clone().into_owned()),
            )));
        }

        self.key_to_code_part.get(key.id()).and_then(|entry| {
            let code_hash = *entry.value();
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
                (*contract_v1.key(), contract_v1.code().clone())
            }
            _ => unimplemented!(),
        };
        let code_hash = key.code_hash();
        if self.contract_cache.get(code_hash).is_some() {
            // WASM code is cached, but we still need to ensure this instance_id is indexed.
            // Different ContractInstanceIds with the same code need their own mapping.
            // See issue #2380.
            self.ensure_key_indexed(&key)?;
            return Ok(());
        }
        let key_path = code_hash.encode();
        let key_path = self.contracts_dir.join(key_path).with_extension("wasm");
        if let Ok((code, _ver)) = ContractCode::load_versioned_from_path(&key_path) {
            // WASM file exists on disk. Add to cache AND ensure the index is updated.
            // See issue #2344 for why this is critical after crash recovery.
            self.ensure_key_indexed(&key)?;
            let size = code.data().len() as i64;
            self.contract_cache.insert(*code_hash, Arc::new(code), size);
            return Ok(());
        }

        // CRITICAL ORDER: Write disk first, then index, then cache.
        // This ensures fetch_contract() can always fall back to disk lookup
        // even if the cache insert is rejected by TinyLFU admission policy.
        // See issue #2306 - stretto's cache.wait() doesn't guarantee visibility.

        // Step 1: Save to disk first (ensures data is persisted)
        let version = APIVersion::from(contract);
        let output: Vec<u8> = code
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&key_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?; // Ensure durability before updating index

        // Step 2: Update index in ReDb (persistent, crash-safe)
        self.db
            .store_contract_index(key.id(), code_hash)
            .map_err(|e| anyhow::anyhow!("Failed to store contract index: {e}"))?;

        // Step 3: Update in-memory index
        self.key_to_code_part.insert(*key.id(), *code_hash);

        // Step 4: Insert into memory cache (best-effort, may be rejected by TinyLFU)
        let size = code.data().len() as i64;
        let data = code.data().to_vec();
        self.contract_cache
            .insert(*code_hash, Arc::new(ContractCode::from(data)), size);
        // Wait for cache to process the insert. Even if TinyLFU rejects it,
        // the disk fallback above ensures the contract can still be fetched.
        let _ = self.contract_cache.wait();

        Ok(())
    }

    pub fn get_contract_path(&mut self, key: &ContractKey) -> RuntimeResult<PathBuf> {
        let contract_hash = *key.code_hash();
        let key_path = contract_hash.encode();
        Ok(self.contracts_dir.join(key_path).with_extension("wasm"))
    }

    pub fn remove_contract(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        let contract_hash = *key.code_hash();

        // Remove from ReDb index
        self.db
            .remove_contract_index(key.id())
            .map_err(|e| anyhow::anyhow!("Failed to remove contract index: {e}"))?;

        // Remove from in-memory index
        self.key_to_code_part.remove(key.id());

        let key_path = self
            .contracts_dir
            .join(contract_hash.encode())
            .with_extension("wasm");
        std::fs::remove_file(key_path)?;
        Ok(())
    }

    pub fn code_hash_from_key(&self, key: &ContractKey) -> Option<CodeHash> {
        self.key_to_code_part.get(key.id()).map(|r| *r.value())
    }

    /// Look up the code hash for a contract given only its instance ID.
    /// Used when clients request contracts without knowing the code hash.
    pub fn code_hash_from_id(&self, id: &ContractInstanceId) -> Option<CodeHash> {
        self.key_to_code_part.get(id).map(|r| *r.value())
    }

    /// Ensures the key_to_code_part mapping exists for the given contract key.
    /// This is needed because when contract code is already cached, store_contract
    /// may not be called, but we still need to index new ContractInstanceIds that
    /// use the same code (different parameters = different rooms).
    /// See issue #2380.
    pub fn ensure_key_indexed(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        let code_hash = key.code_hash();
        if !self.key_to_code_part.contains_key(key.id()) {
            // Store in ReDb
            self.db
                .store_contract_index(key.id(), code_hash)
                .map_err(|e| anyhow::anyhow!("Failed to store contract index: {e}"))?;

            // Update in-memory map
            self.key_to_code_part.insert(*key.id(), *code_hash);

            tracing::debug!(
                contract = %key,
                instance_id = %key.id(),
                code_hash = %code_hash,
                "Indexed contract instance (same code, different params)"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    //! Tests for ContractStore
    //!
    //! Key invariant: For every contract stored, `code_hash_from_id(instance_id)`
    //! must return the correct CodeHash. This is critical because `lookup_key()`
    //! uses this to reconstruct ContractKey from just an instance ID.
    use super::*;

    async fn create_test_db(path: &std::path::Path) -> Storage {
        Storage::new(path).await.expect("failed to create test db")
    }

    #[tokio::test]
    async fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
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

    /// Test that simulates the actual contract store flow to see if
    /// contracts can be "lost" between store and fetch
    #[tokio::test]
    async fn test_contract_store_fetch_reliability() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Use realistic-ish cache size
        let mut store = ContractStore::new(contract_dir.path().into(), 100_000, db)?;

        // Store multiple contracts with varying sizes, track their keys
        let mut keys = Vec::new();
        for i in 0..10u8 {
            // Create contracts of different sizes
            let size = ((i as usize) + 1) * 1000;
            let code = vec![i; size];
            let params = Parameters::from(vec![i, i + 1]);
            let contract = WrappedContract::new(Arc::new(ContractCode::from(code)), params.clone());
            let key = *contract.key();
            keys.push((key, params));
            let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
            store.store_contract(container)?;
        }

        // Immediately try to fetch all contracts - this is the critical path
        // where issue #2306 manifests
        let mut fetch_failures = 0;
        for (key, params) in &keys {
            let fetched = store.fetch_contract(key, params);
            if fetched.is_none() {
                eprintln!("FETCH FAILED for contract {key} immediately after store!");
                fetch_failures += 1;
            }
        }

        assert_eq!(
            fetch_failures, 0,
            "Contracts should be fetchable immediately after store"
        );

        Ok(())
    }

    /// Test for issue #2344: Contract store index must be persisted to disk.
    /// This test simulates a node restart by creating a new ContractStore from
    /// the same directory, then verifies contracts are still fetchable.
    #[tokio::test]
    async fn test_index_persistence_after_restart() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1, 2, 3, 4, 5])),
            [10, 20].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [10, 20].as_ref().into();

        // Store the contract
        {
            let db = create_test_db(contract_dir.path()).await;
            let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
            let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
            store.store_contract(container)?;

            // Verify it's fetchable in the same instance
            assert!(
                store.fetch_contract(&key, &params).is_some(),
                "Contract should be fetchable immediately after store"
            );
        }
        // ContractStore dropped here - simulates process exit

        // Create a NEW ContractStore from the same directory - simulates node restart
        {
            let db = create_test_db(contract_dir.path()).await;
            let store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

            // The contract should be fetchable because both:
            // 1. The WASM file was persisted to disk
            // 2. The index (ReDb) was persisted to disk
            // Issue #2344: Before the fix, the index wasn't synced, so the contract
            // would not be found after restart.
            let fetched = store.fetch_contract(&key, &params);
            assert!(
                fetched.is_some(),
                "Contract should be fetchable after simulated restart - index must be persisted"
            );
        }

        Ok(())
    }

    /// Test for issue #2344: When WASM file exists but index entry is missing
    /// (e.g., after a crash), store_contract should add the missing index entry.
    #[tokio::test]
    async fn test_wasm_exists_but_index_missing() -> Result<(), Box<dyn std::error::Error>> {
        use std::io::Write;

        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![7, 8, 9])),
            [30, 40].as_ref().into(),
        );
        let key = *contract.key();
        let code_hash = key.code_hash();
        let params: Parameters = [30, 40].as_ref().into();

        // Manually create the WASM file on disk (simulating a crash scenario
        // where WASM was synced but index wasn't)
        let wasm_path = contract_dir
            .path()
            .join(code_hash.encode())
            .with_extension("wasm");
        {
            let code_bytes = contract
                .code()
                .to_bytes_versioned(freenet_stdlib::prelude::APIVersion::Version0_0_1)
                .unwrap();
            let mut file = std::fs::File::create(&wasm_path)?;
            file.write_all(&code_bytes)?;
            file.sync_all()?;
        }

        // Create a ContractStore - the ReDb will be empty (no index entries)
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // The contract is NOT fetchable yet because the index doesn't have the entry
        // and it's not in cache
        assert!(
            store.fetch_contract(&key, &params).is_none(),
            "Contract should NOT be fetchable when WASM exists but index entry is missing"
        );

        // Now call store_contract - this should detect the WASM file exists,
        // add the missing index entry, and add to cache
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
        store.store_contract(container)?;

        // Drop the store and create a new one to verify the index was persisted
        drop(store);
        let db = create_test_db(contract_dir.path()).await;
        let store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Now the contract should be fetchable because the fix adds the index entry
        let fetched = store.fetch_contract(&key, &params);
        assert!(
            fetched.is_some(),
            "Contract should be fetchable after store_contract adds missing index entry"
        );

        Ok(())
    }

    /// Regression test for issue #2380: Multiple contracts with same WASM code
    /// but different parameters must all be indexed correctly.
    ///
    /// This bug manifested in River when creating multiple chat rooms:
    /// - All rooms use the same room-contract WASM (same code_hash)
    /// - Different parameters (owner key) create different ContractInstanceIds
    /// - Only the first room's instance_id was indexed
    /// - Subscribe to 2nd+ rooms failed because lookup_key() returned None
    #[tokio::test]
    async fn test_multiple_contracts_same_code_different_params(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Same WASM code for all contracts (like River's room-contract)
        let shared_code = vec![1, 2, 3, 4, 5];

        // Create multiple contracts with SAME code but DIFFERENT parameters
        // This simulates creating multiple River rooms
        let mut contracts = Vec::new();
        for i in 0..5u8 {
            let params = Parameters::from(vec![i, i + 10, i + 20]); // Different params each time
            let contract = WrappedContract::new(
                Arc::new(ContractCode::from(shared_code.clone())),
                params.clone(),
            );
            contracts.push((contract.clone(), params));

            let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
            store.store_contract(container)?;
        }

        // All contracts share the same code_hash
        let expected_code_hash = contracts[0].0.key().code_hash();
        for (contract, _) in &contracts {
            assert_eq!(
                contract.key().code_hash(),
                expected_code_hash,
                "All contracts should have the same code_hash"
            );
        }

        // Critical assertion: code_hash_from_id must work for ALL instance IDs
        // This is what lookup_key() uses, and what failed before the fix
        for (i, (contract, _)) in contracts.iter().enumerate() {
            let instance_id = contract.key().id();
            let lookup_result = store.code_hash_from_id(instance_id);
            assert!(
                lookup_result.is_some(),
                "code_hash_from_id() failed for contract {i} (instance_id: {instance_id}) - \
                 this would cause Subscribe to fail!"
            );
            assert_eq!(
                lookup_result.unwrap(),
                *expected_code_hash,
                "code_hash_from_id() returned wrong hash for contract {i}"
            );
        }

        // Also verify fetch_contract works for all
        for (i, (contract, params)) in contracts.iter().enumerate() {
            let fetched = store.fetch_contract(contract.key(), params);
            assert!(
                fetched.is_some(),
                "fetch_contract() failed for contract {i}"
            );
        }

        Ok(())
    }
}
