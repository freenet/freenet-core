use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use freenet_stdlib::prelude::*;
use moka::sync::Cache as MokaCache;

use crate::contract::storages::Storage;

use super::RuntimeResult;

/// Shared in-memory contract instance index: `ContractInstanceId -> CodeHash`.
///
/// One `Arc` is owned by `RuntimePool` and cloned into every pool executor's
/// [`ContractStore`], so an instance stored / indexed / removed via one
/// executor is immediately visible to all the others. Before #4218 each
/// `ContractStore::new` built its OWN `Arc<DashMap>` and loaded it once from
/// ReDb, so pool executors diverged: `code_hash_from_id` / `fetch_contract`
/// on executor B could miss an instance stored via executor A, and a
/// removal on A left a "ghost" instance live on B until B was rebuilt.
pub type SharedContractIndex = Arc<DashMap<ContractInstanceId, CodeHash>>;

/// Handle contract blob storage on the file system.
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: MokaCache<CodeHash, Arc<ContractCode<'static>>>,
    /// In-memory index: ContractInstanceId -> CodeHash.
    /// Shared across all pool executors (see [`SharedContractIndex`]); loaded
    /// from ReDb once on first construction and kept in sync by every
    /// `store_contract` / `ensure_key_indexed` / `remove_contract`.
    key_to_code_part: SharedContractIndex,
    /// ReDb storage for persistent index
    db: Storage,
    /// Test-only hook invoked inside `store_contract` AFTER the `.wasm` blob is
    /// written and synced but BEFORE the ReDb index entry is committed. Lets the
    /// issue #4216 regression test drive a concurrent `remove_contract` into
    /// exactly that window. `None` in every non-test build.
    #[cfg(test)]
    after_blob_write_hook: std::sync::Mutex<Option<Box<dyn FnMut() + Send>>>,
}
// Eviction-driven reclamation of unused contracts now exists: the hosting
// cache evicts least-valuable contracts past its budget and the resulting
// `EvictContract` event drives `Executor::reclaim_contract_storage`, which
// calls `ContractStore::remove_contract` to delete the on-disk `.wasm` blob.

impl ContractStore {
    /// # Arguments
    /// - contracts_dir: directory where contract WASM files are stored
    /// - max_size: max size in bytes of the contracts being cached
    /// - db: ReDb storage for persistent index
    ///
    /// Builds a store with its OWN fresh (unshared) instance index. Use this
    /// for standalone / single-executor stores and tests. Pool executors that
    /// must share one live index across the pool use
    /// [`ContractStore::new_with_shared_index`] (#4218).
    pub fn new(contracts_dir: PathBuf, max_size: u64, db: Storage) -> RuntimeResult<Self> {
        Self::new_with_shared_index(contracts_dir, max_size, db, Arc::new(DashMap::new()))
    }

    /// Like [`ContractStore::new`] but wires in a caller-owned
    /// [`SharedContractIndex`] so every pool executor sees the same live
    /// `ContractInstanceId -> CodeHash` map (#4218).
    ///
    /// The ReDb index is loaded into the shared map only on FIRST construction
    /// (when the map is still empty). Subsequent pool executors and
    /// replacements pass the SAME already-populated `Arc`, so they inherit the
    /// live map (including instances stored since startup) instead of paying a
    /// redundant ReDb scan and racing the on-disk state.
    pub fn new_with_shared_index(
        contracts_dir: PathBuf,
        max_size: u64,
        db: Storage,
        key_to_code_part: SharedContractIndex,
    ) -> RuntimeResult<Self> {
        std::fs::create_dir_all(&contracts_dir).map_err(|err| {
            tracing::error!("error creating contract dir: {err}");
            err
        })?;

        // Load the index from ReDb only if this shared map hasn't been
        // populated yet (first executor in the pool). Later executors share the
        // same live `Arc` and must not clobber / re-scan it.
        if key_to_code_part.is_empty() {
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

            // Migrate any contract WASM files written under the legacy lowercased
            // Base58 name to the canonical mixed-case name (issue #4214) so the
            // fetch paths below, which use `code_hash.encode()`, still find code
            // persisted before the stdlib CodeHash::encode case-fix.
            for entry in key_to_code_part.iter() {
                super::migrate_legacy_lowercased_code_file(
                    &contracts_dir,
                    &entry.value().encode(),
                    "wasm",
                );
            }
        }

        Ok(Self {
            contract_cache: MokaCache::builder()
                .max_capacity(max_size)
                .weigher(
                    |key: &CodeHash, value: &Arc<ContractCode<'static>>| -> u32 {
                        // Saturate to u32::MAX on overflow as moka recommends.
                        // A contract WASM module larger than 4 GiB would indicate
                        // a bug in upstream size validation — log it loudly.
                        let len = value.data().len();
                        u32::try_from(len).unwrap_or_else(|_| {
                            tracing::warn!(
                                code_hash = %key,
                                size_bytes = len,
                                "Contract code exceeds u32::MAX in cache weigher; \
                                 saturating. This should be impossible."
                            );
                            u32::MAX
                        })
                    },
                )
                .build(),
            contracts_dir,
            key_to_code_part,
            db,
            #[cfg(test)]
            after_blob_write_hook: std::sync::Mutex::new(None),
        })
    }

    /// Install the test-only post-blob-write hook (issue #4216 regression test).
    #[cfg(test)]
    fn set_after_blob_write_hook(&self, hook: Box<dyn FnMut() + Send>) {
        *self.after_blob_write_hook.lock().unwrap() = Some(hook);
    }

    /// Returns a copy of the contract bytes if available, none otherwise.
    // todo: instead return Result<Option<_>, _> to handle IO errors upstream
    //
    // The `key_to_code_part` index is now SHARED across every pool executor
    // (see [`SharedContractIndex`] / #4218), so a contract stored via executor
    // A is visible to `fetch_contract` on executor B. That fixes the old
    // "stored on A, missing on B" divergence.
    //
    // The lookup is GATED on the shared index: the per-executor `contract_cache`
    // fast path is served ONLY when this instance id is still present in the
    // shared index. Without this gate, after a sibling executor's
    // `remove_contract` cleared the shared index entry, THIS executor's
    // still-warm `contract_cache` (keyed by the shared code hash) would keep
    // serving the removed instance as a "ghost". Consulting the shared index
    // first makes a removal on any executor immediately authoritative on all of
    // them (#4218 problem 2).
    pub fn fetch_contract(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        // Gate on the shared index: an instance that is no longer indexed
        // (e.g. removed via a sibling executor) must not be served from a stale
        // per-executor cache entry.
        if !self.key_to_code_part.contains_key(key.id()) {
            return None;
        }

        let code_hash = key.code_hash();
        if let Some(data) = self.contract_cache.get(code_hash) {
            return Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                WrappedContract::new(data, params.clone().into_owned()),
            )));
        }

        self.key_to_code_part.get(key.id()).and_then(|entry| {
            let code_hash = *entry.value();
            let path = code_hash.encode();
            let key_path = self.contracts_dir.join(path).with_extension("wasm");
            // Load with version prefix stripping (fixes #2924)
            // Files are stored with to_bytes_versioned() which adds a version prefix.
            // Must use load_versioned_from_path() to strip it before compilation.
            let (code, _ver) = ContractCode::load_versioned_from_path(&key_path)
                .map_err(|err| {
                    tracing::debug!("contract not found: {err}");
                    err
                })
                .ok()?;
            let params = params.clone().into_owned();
            // add back the contract part to the mem store
            self.contract_cache
                .insert(code_hash, Arc::new(code.clone()));
            Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                WrappedContract::new(Arc::new(code), params),
            )))
        })
    }

    /// Store a copy of the contract in the local store, in case it hasn't been stored previously.
    pub fn store_contract(&mut self, contract: ContractContainer) -> RuntimeResult<()> {
        // Serialize against concurrent store/remove on the SAME shared code
        // hash across sibling-executor `ContractStore`s (issue #4216). Without
        // this, a `remove_contract` on another executor can run its
        // `load_all_contract_index()` scan in the window after we write the
        // `.wasm` blob but before we commit our index entry, see no remaining
        // reference to the code hash, and delete the blob we just wrote —
        // leaving this instance indexed but blobless. Holding the shared lock
        // from before the cache/disk checks through the index commit closes
        // that window. All sections below are synchronous (no `.await`), so
        // holding a std `Mutex` across the ReDb + filesystem ops is
        // deadlock-safe.
        let blob_lock = self.db.contract_blob_lock();
        let _blob_guard = blob_lock.lock().unwrap_or_else(|e| e.into_inner());

        let (key, code) = match contract.clone() {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                (*contract_v1.key(), contract_v1.code().clone())
            }
            ContractContainer::Wasm(_) | _ => unimplemented!(),
        };
        let code_hash = key.code_hash();
        let key_path = code_hash.encode();
        let key_path = self.contracts_dir.join(key_path).with_extension("wasm");
        if self.contract_cache.get(code_hash).is_some() && key_path.exists() {
            // WASM code is cached AND the blob is still on disk: fast path. We
            // still need to ensure this instance_id is indexed (different
            // ContractInstanceIds with the same code each need their own
            // mapping — see issue #2380).
            //
            // We MUST also verify the blob is on disk: this `contract_cache` is
            // per-`ContractStore` (per pool executor), and a `remove_contract`
            // on a sibling executor can delete the shared blob without
            // invalidating our cache. Falling through to the disk-write branch
            // below restores the blob in that case so the new instance is
            // durably stored. See issue #4218 for the underlying per-executor
            // map divergence and Codex's round-4 finding.
            //
            // Use the `_locked` helper: we already hold `blob_lock` and std
            // `Mutex` is not reentrant.
            self.ensure_key_indexed_locked(&key)?;
            return Ok(());
        }
        if let Ok((code, _ver)) = ContractCode::load_versioned_from_path(&key_path) {
            // WASM file exists on disk. Add to cache AND ensure the index is updated.
            // See issue #2344 for why this is critical after crash recovery.
            self.ensure_key_indexed_locked(&key)?;
            self.contract_cache.insert(*code_hash, Arc::new(code));
            return Ok(());
        }

        // CRITICAL ORDER: Write disk first, then index, then cache.
        // This ensures fetch_contract() can always fall back to disk lookup.

        // Step 1: Save to disk first (ensures data is persisted)
        let version = APIVersion::from(contract);
        let output: Vec<u8> = code
            .to_bytes_versioned(version)
            .map_err(|e| anyhow::anyhow!(e))?;
        let mut file = File::create(&key_path)?;
        file.write_all(output.as_slice())?;
        file.sync_all()?; // Ensure durability before updating index

        // Test-only injection point for the issue #4216 race: the blob is now
        // observable on disk but the index entry below is not yet committed.
        #[cfg(test)]
        if let Some(hook) = self.after_blob_write_hook.lock().unwrap().as_mut() {
            hook();
        }

        // Step 2: Update index in ReDb (persistent, crash-safe)
        self.db
            .store_contract_index(key.id(), code_hash)
            .map_err(|e| anyhow::anyhow!("Failed to store contract index: {e}"))?;

        // Step 3: Update in-memory index
        self.key_to_code_part.insert(*key.id(), *code_hash);

        // Step 4: Insert into memory cache
        let data = code.data().to_vec();
        self.contract_cache
            .insert(*code_hash, Arc::new(ContractCode::from(data)));

        Ok(())
    }

    pub fn get_contract_path(&mut self, key: &ContractKey) -> RuntimeResult<PathBuf> {
        let contract_hash = *key.code_hash();
        let key_path = contract_hash.encode();
        Ok(self.contracts_dir.join(key_path).with_extension("wasm"))
    }

    /// Remove a contract instance from the store.
    ///
    /// Removes this instance's index entries (ReDb + in-memory) and any
    /// delegate subscriptions unconditionally. The on-disk `.wasm` blob and
    /// the code cache entry are keyed by `code_hash`, which is shared across
    /// every `ContractInstanceId` using the same code, so they are removed
    /// only once no remaining instance references that code hash. File
    /// removal is idempotent — an already-missing `.wasm` is not an error.
    ///
    /// The "still referenced?" decision is made against the **persistent
    /// ReDb `contract_index`**, not the in-memory `key_to_code_part` map.
    /// Pool executors now share one in-memory index (see
    /// [`SharedContractIndex`] / `new_with_shared_index`), but ReDb stays
    /// the authority here: it is the durable source of truth that also
    /// covers standalone stores built via [`ContractStore::new`] (which
    /// get a private index), and deciding from a possibly-incomplete
    /// in-memory view would wrongly delete a `.wasm` blob still
    /// referenced by another instance — corrupting every surviving
    /// contract that shares the code (e.g. every River room shares one
    /// room-contract WASM, see issue #2380).
    pub fn remove_contract(&mut self, key: &ContractKey) -> RuntimeResult<()> {
        let contract_hash = *key.code_hash();

        // Serialize against concurrent `store_contract` on the SAME shared code
        // hash across sibling-executor `ContractStore`s (issue #4216). Holding
        // the shared lock from before the ReDb index removal through the blob
        // delete guarantees our "is this code still referenced?" scan observes
        // any concurrent store's committed index entry (that store commits its
        // entry while holding this same lock), so we never delete a blob a
        // sibling executor just wrote for a new instance of the same code. All
        // sections below are synchronous (no `.await`), so holding a std
        // `Mutex` across the ReDb + filesystem ops is deadlock-safe.
        let blob_lock = self.db.contract_blob_lock();
        let _blob_guard = blob_lock.lock().unwrap_or_else(|e| e.into_inner());

        // Remove this instance's index entries first. The ReDb removal must
        // happen before the `load_all_contract_index()` scan below so this
        // instance's own entry is not counted as a remaining reference.
        self.db
            .remove_contract_index(key.id())
            .map_err(|e| anyhow::anyhow!("Failed to remove contract index: {e}"))?;
        self.key_to_code_part.remove(key.id());

        // Clean up any delegate subscriptions for this contract instance.
        super::DELEGATE_SUBSCRIPTIONS.remove(key.id());

        // The WASM blob on disk is keyed by code hash and shared by every
        // contract instance with the same code (e.g. all River rooms share
        // one room-contract WASM — see issue #2380). Only delete the blob
        // and its cache entry once no remaining instance references this
        // code hash, otherwise the surviving instances break on cache miss.
        //
        // Decide from the shared persistent index, NOT the per-executor
        // in-memory map (see the doc comment above for why).
        let code_still_referenced = match self.db.load_all_contract_index() {
            Ok(entries) => entries
                .iter()
                .any(|(_, code_hash)| code_hash == &contract_hash),
            Err(e) => {
                // If we cannot read the shared index we cannot prove the
                // blob is unreferenced. A leaked blob is recoverable disk
                // space; deleting a still-referenced shared blob corrupts
                // every surviving contract using that code. Fail safe:
                // keep the blob — and return an error so the caller knows
                // the code half was NOT fully reclaimed and will requeue
                // for retry rather than clearing the pending entry.
                tracing::warn!(
                    code_hash = %contract_hash,
                    error = %e,
                    "Could not load shared contract index while removing a \
                     contract; keeping the .wasm blob to avoid corrupting \
                     contracts that may still reference this code"
                );
                return Err(anyhow::anyhow!(
                    "kept WASM blob for {contract_hash}: shared contract \
                     index read failed: {e}"
                )
                .into());
            }
        };
        if !code_still_referenced {
            // Invalidate the code cache so a removed contract is never served
            // as a "ghost" (issue #3487).
            self.contract_cache.invalidate(&contract_hash);
            let key_path = self
                .contracts_dir
                .join(contract_hash.encode())
                .with_extension("wasm");
            match std::fs::remove_file(&key_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Returns true if the WASM code blob for `code_hash` is already present on
    /// the shared on-disk contract store.
    ///
    /// This is the disk-budget DEDUP probe (#4218 / #4683). It answers exactly
    /// the question the budget gate and the store-vs-index routing in the
    /// executor need: "will `store_contract` write NEW blob bytes to disk?" —
    /// which is true iff the blob is not already on disk.
    ///
    /// It is keyed by CODE HASH and checks the shared filesystem (one `.wasm`
    /// blob per code hash, shared by every instance across every pool executor),
    /// NOT `fetch_contract`, which is keyed by INSTANCE id. A second instance of
    /// already-stored code (same code hash, different params → a NEW instance
    /// id) is absent from the instance index, so an instance-keyed probe would
    /// wrongly report "not stored" and (a) charge the shared blob against the
    /// disk budget a second time (double-count) and (b) route the PUT down the
    /// store path instead of the ensure-indexed path. Disk existence is the
    /// single shared source of truth for on-disk blob occupancy, is O(1), and is
    /// consistent across executors without relying on any per-executor cache.
    ///
    /// The shared instance index is deliberately NOT consulted here: it can
    /// disagree with disk (a sibling executor deleted the blob, or a crash left
    /// an index entry without a blob), and trusting it would skip the
    /// blob-rewrite safety in `store_contract`. Disk is authoritative for "are
    /// the bytes on disk right now".
    pub fn code_blob_stored(&self, code_hash: &CodeHash) -> bool {
        self.contracts_dir
            .join(code_hash.encode())
            .with_extension("wasm")
            .exists()
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
        // Public entry point: take the shared store/remove lock (issue #4216)
        // so an external caller indexing a new instance is serialized against a
        // concurrent `remove_contract` on the same code hash, just like
        // `store_contract`. Callers already holding the lock (i.e.
        // `store_contract`) must use `ensure_key_indexed_locked` instead — std
        // `Mutex` is not reentrant.
        let blob_lock = self.db.contract_blob_lock();
        let _blob_guard = blob_lock.lock().unwrap_or_else(|e| e.into_inner());
        self.ensure_key_indexed_locked(key)
    }

    /// Unlocked body of [`ensure_key_indexed`]. The caller MUST hold the shared
    /// `contract_blob_lock` (issue #4216). Called by `store_contract`, which
    /// already holds the lock.
    fn ensure_key_indexed_locked(&mut self, key: &ContractKey) -> RuntimeResult<()> {
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
    async fn test_multiple_contracts_same_code_different_params()
    -> Result<(), Box<dyn std::error::Error>> {
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

    /// Regression test for issue #2924: Versioned contract files must be
    /// properly loaded without the version prefix before compilation.
    ///
    /// The bug:
    /// - Contracts are stored with to_bytes_versioned() which adds a version prefix
    /// - fetch_contract() was using ContractContainer::try_from which read raw bytes
    /// - The prefix caused wasmtime to fail auto-detection (no WASM magic number)
    /// - Module::new tried to parse as WAT, failed with "input bytes aren't valid utf-8"
    ///
    /// The fix:
    /// - Use ContractCode::load_versioned_from_path() which strips the prefix
    /// - The WASM magic number is now at offset 0, so Module::new works correctly
    #[tokio::test]
    async fn test_versioned_contract_loading_issue_2924() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::wasm_runtime::engine::{Engine, WasmEngine};
        use crate::wasm_runtime::runtime::RuntimeConfig;

        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Valid WASM binary (exports "memory" and "answer" function that returns 42)
        // WAT equivalent:
        // (module
        //   (memory 1)
        //   (export "memory" (memory 0))
        //   (func (export "answer") (result i32)
        //     i32.const 42
        //   )
        // )
        const VALID_WASM: &[u8] = &[
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x05, 0x01, 0x60, 0x00, 0x01,
            0x7f, 0x03, 0x02, 0x01, 0x00, 0x05, 0x03, 0x01, 0x00, 0x01, 0x07, 0x13, 0x02, 0x06,
            0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, 0x06, 0x61, 0x6e, 0x73, 0x77, 0x65,
            0x72, 0x00, 0x00, 0x0a, 0x06, 0x01, 0x04, 0x00, 0x41, 0x2a, 0x0b,
        ];

        // Verify the WASM starts with magic number
        assert_eq!(
            &VALID_WASM[0..4],
            &[0x00, 0x61, 0x73, 0x6d],
            "Test WASM should start with magic number"
        );

        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(VALID_WASM.to_vec())),
            [0, 1].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [0, 1].as_ref().into();
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone()));

        // Store the contract (this adds version prefix to disk file)
        store.store_contract(container)?;

        // Drop the store to clear the cache, forcing fetch_contract to read from disk
        drop(store);

        // Create a new store and fetch the contract from disk
        let db = create_test_db(contract_dir.path()).await;
        let store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
        let fetched = store
            .fetch_contract(&key, &params)
            .expect("Contract should be fetchable after store");

        // Extract the code bytes from the fetched contract
        let ContractContainer::Wasm(ContractWasmAPIVersion::V1(fetched_contract)) = fetched else {
            panic!("Expected WASM V1 contract");
        };

        // Verify the fetched code matches the original WASM (without version prefix)
        let fetched_bytes = fetched_contract.code().data();
        assert_eq!(
            fetched_bytes, VALID_WASM,
            "Fetched contract bytes should match original WASM without version prefix"
        );

        // Verify the WASM magic number is at the start (issue #2924 would fail here)
        assert_eq!(
            &fetched_bytes[0..4],
            &[0x00, 0x61, 0x73, 0x6d],
            "Fetched WASM should start with magic number (no version prefix)"
        );

        // Critical test: Verify the fetched contract can be compiled
        // Before the fix, this would fail with "Error when converting wat: input bytes aren't valid utf-8"
        let mut engine = Engine::new(&RuntimeConfig::default(), false)?;
        let compile_result = engine.compile(fetched_bytes);
        assert!(
            compile_result.is_ok(),
            "Contract should compile successfully without 'converting wat' error. Error: {:?}",
            compile_result.err()
        );

        Ok(())
    }

    /// Regression test for issue #3487: removed contracts must not be served from cache.
    ///
    /// Before the fix, remove_contract() deleted from disk and index but did not
    /// invalidate the cache, so fetch_contract() could still return "ghost" contracts.
    #[tokio::test]
    async fn test_remove_contract_invalidates_cache() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![5, 6, 7])),
            [0, 1].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [0, 1].as_ref().into();
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));

        // Store and verify fetchable
        store.store_contract(container)?;
        assert!(
            store.fetch_contract(&key, &params).is_some(),
            "Contract should be fetchable after store"
        );

        // Remove contract
        store.remove_contract(&key)?;

        // Must NOT be fetchable from cache
        assert!(
            store.fetch_contract(&key, &params).is_none(),
            "Removed contract must not be served from cache (ghost contract)"
        );

        Ok(())
    }

    /// Regression test for the latent shared-WASM deletion bug: removing one
    /// contract instance must NOT delete the `.wasm` blob while another
    /// instance still references the same code hash.
    ///
    /// Multiple `ContractInstanceId`s can share one code hash (e.g. every
    /// River chat room shares one room-contract WASM — see issue #2380).
    /// The on-disk blob is keyed by code hash, so an unconditional delete
    /// would break the surviving instances after a cache miss / restart.
    #[tokio::test]
    async fn test_remove_contract_keeps_shared_wasm() -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Two instances: SAME code, DIFFERENT params -> same code_hash,
        // different ContractInstanceIds.
        let shared_code = vec![1, 2, 3, 4, 5];
        let params1 = Parameters::from(vec![1, 1, 1]);
        let params2 = Parameters::from(vec![2, 2, 2]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();

        // Both instances share one code hash.
        assert_eq!(key1.code_hash(), key2.code_hash());

        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key1.code_hash().encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "WASM file should exist after store");

        // Remove the first instance — the shared WASM must survive because
        // the second instance still references the code hash.
        store.remove_contract(&key1)?;
        assert!(
            wasm_path.exists(),
            "Shared WASM file must NOT be deleted while another instance references it"
        );

        // The second instance must still be fetchable.
        assert!(
            store.fetch_contract(&key2, &params2).is_some(),
            "Surviving instance must still be fetchable after the other is removed"
        );

        Ok(())
    }

    /// Regression test for the cross-executor shared-WASM deletion bug.
    ///
    /// Each runtime-pool executor owns a SEPARATE `ContractStore` with its
    /// own in-memory `key_to_code_part` map, but they all share one ReDb
    /// `contract_index`. If `remove_contract` decided "is this code still
    /// referenced?" from its own in-memory map, an instance stored via a
    /// different executor would be invisible — and removing the locally
    /// known instance would wrongly delete the shared `.wasm` blob,
    /// corrupting the instance owned by the other executor.
    ///
    /// This test reproduces that: two `ContractStore`s sharing ONE `db`,
    /// each storing a different instance of the SAME code. Removing one
    /// instance via store A must NOT delete the blob, and the instance
    /// stored via store B must remain fetchable.
    #[tokio::test]
    async fn test_remove_contract_keeps_shared_wasm_across_executors()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Two separate ContractStores (simulating two runtime-pool
        // executors) sharing ONE db / ReDb contract_index.
        let mut store_a = ContractStore::new(contract_dir.path().into(), 10_000, db.clone())?;
        let mut store_b = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        // Same code, different params -> same code_hash, different
        // ContractInstanceIds.
        let shared_code = vec![10, 20, 30, 40, 50];
        let params1 = Parameters::from(vec![1, 1, 1]);
        let params2 = Parameters::from(vec![2, 2, 2]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        assert_eq!(key1.code_hash(), key2.code_hash());

        // Instance X1 stored via executor A, instance X2 via executor B.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;
        store_b.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key1.code_hash().encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "WASM file should exist after store");

        // Remove X1 via store A. Store A's in-memory map never saw X2, so
        // the OLD code would delete the shared blob here.
        store_a.remove_contract(&key1)?;

        assert!(
            wasm_path.exists(),
            "Shared WASM file must NOT be deleted while another executor's \
             ContractStore still references the code hash"
        );

        // X2, owned by executor B, must still be fetchable.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_some(),
            "Instance stored via another executor must survive removal of a \
             different instance sharing the same code"
        );

        Ok(())
    }

    /// Removing the last instance referencing a code hash must delete the
    /// `.wasm` blob from disk to reclaim space.
    #[tokio::test]
    async fn test_remove_contract_deletes_last_instance_wasm()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![9, 8, 7, 6])),
            [4, 2].as_ref().into(),
        );
        let key = *contract.key();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key.code_hash().encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "WASM file should exist after store");

        // Removing the only instance must delete the blob.
        store.remove_contract(&key)?;
        assert!(
            !wasm_path.exists(),
            "WASM file must be deleted when the last instance is removed"
        );

        Ok(())
    }

    /// `remove_contract` must tolerate an already-missing `.wasm` file
    /// (idempotent file removal).
    #[tokio::test]
    async fn test_remove_contract_idempotent_when_file_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![3, 3, 3])),
            [7, 7].as_ref().into(),
        );
        let key = *contract.key();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        // Manually delete the WASM file out from under the store.
        let wasm_path = contract_dir
            .path()
            .join(key.code_hash().encode())
            .with_extension("wasm");
        std::fs::remove_file(&wasm_path)?;

        // remove_contract must still succeed despite the missing file.
        store
            .remove_contract(&key)
            .expect("remove_contract must be Ok when the WASM file is already gone");

        Ok(())
    }

    /// Regression test for Codex's round-4 finding: the `contract_cache` is
    /// per-`ContractStore` (one per pool executor), but the `.wasm` blob on
    /// disk is shared. A sibling executor's `remove_contract` can delete the
    /// blob without invalidating this store's cache. A subsequent
    /// `store_contract` for a new instance with the same code hash must NOT
    /// take the cache-hit fast path silently — it must verify the blob still
    /// exists on disk and re-write it if not. Otherwise the new instance is
    /// indexed but blobless until the cache evicts and a fetch fails.
    #[tokio::test]
    async fn test_store_contract_rewrites_blob_when_cache_hit_but_disk_missing()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;
        let mut store = ContractStore::new(contract_dir.path().into(), 10_000, db)?;

        let shared_code = vec![9, 9, 9, 9];

        // Store instance X1: caches the code AND writes the blob.
        let x1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            [1, 1].as_ref().into(),
        );
        let x1_code_hash = *x1.key().code_hash();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(x1)))?;

        let wasm_path = contract_dir
            .path()
            .join(x1_code_hash.encode())
            .with_extension("wasm");
        assert!(wasm_path.exists(), "blob must exist after first store");

        // Simulate a sibling executor's `remove_contract` deleting the shared
        // blob WITHOUT invalidating this store's `contract_cache`.
        std::fs::remove_file(&wasm_path)?;
        assert!(!wasm_path.exists());
        assert!(
            store.contract_cache.get(&x1_code_hash).is_some(),
            "this store's cache still has the code (sibling did not invalidate it)"
        );

        // Store a NEW instance X2 with the SAME code hash. Pre-fix, the
        // cache-hit fast path returned Ok without re-writing the blob,
        // leaving X2 indexed but blobless. Post-fix, the disk-existence
        // check forces a re-write.
        let x2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code)),
            [2, 2].as_ref().into(),
        );
        let x2_key = *x2.key();
        let params: Parameters = [2, 2].as_ref().into();
        store.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(x2)))?;

        assert!(
            wasm_path.exists(),
            "blob must be re-written after store_contract for a new instance \
             whose shared code was in cache but missing from disk"
        );
        // And the new instance is fetchable end-to-end.
        assert!(
            store.fetch_contract(&x2_key, &params).is_some(),
            "new instance must be fetchable after rewrite"
        );

        Ok(())
    }

    /// Regression test for issue #4216: a concurrent `store_contract` and
    /// `remove_contract` on the SAME shared code hash, running on two
    /// sibling-executor `ContractStore`s over one shared ReDb, must not delete
    /// the freshly-written `.wasm` blob.
    ///
    /// The race: executor B stores a new instance X2 (writes the `.wasm` blob,
    /// then commits its index entry). Executor A removes instance X1, which
    /// scans the shared ReDb index to decide whether the blob is still
    /// referenced. If A's scan runs after B wrote the blob but before B
    /// committed X2's index entry, A sees no remaining reference and deletes
    /// the blob B just wrote — corrupting X2 on the next cache miss / restart.
    ///
    /// This test drives A's `remove_contract` into exactly that window via the
    /// test-only `after_blob_write_hook`, which fires inside B's
    /// `store_contract` after the blob write/sync but before the index commit.
    /// Pre-fix, A's remove completes inside the window and deletes the blob;
    /// post-fix, the shared blob lock makes A block until B's index commit
    /// lands, so A's scan sees X2 and keeps the blob.
    #[tokio::test]
    async fn test_concurrent_store_remove_keeps_freshly_written_wasm()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        // Two ContractStores (two runtime-pool executors) sharing ONE db.
        let mut store_a = ContractStore::new(contract_dir.path().into(), 10_000, db.clone())?;
        let mut store_b = ContractStore::new(contract_dir.path().into(), 10_000, db.clone())?;

        // Same code, different params -> same code_hash, different instances.
        let shared_code = vec![11, 22, 33, 44, 55];
        let params1 = Parameters::from(vec![1, 1, 1]);
        let params2 = Parameters::from(vec![2, 2, 2]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        assert_eq!(key1.code_hash(), key2.code_hash());

        // Store X1 (blob + index) via executor A.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;

        let wasm_path = contract_dir
            .path()
            .join(key1.code_hash().encode())
            .with_extension("wasm");
        assert!(
            wasm_path.exists(),
            "WASM file should exist after first store"
        );

        // Remove the on-disk blob so B is forced down the blob-WRITE path
        // (otherwise B would take the "blob already on disk" fast path and
        // never reach the write->commit window this race lives in). X1's index
        // entry is intentionally left in place, exactly as it would be during a
        // concurrent eviction of X1.
        std::fs::remove_file(&wasm_path)?;

        // When B writes X2's blob (before committing X2's index), spawn A's
        // remove_contract(X1) on another thread and hold the window open long
        // enough for it to reach — and, pre-fix, complete — its delete
        // decision. We do NOT join inside the hook: post-fix the remove blocks
        // on the shared lock B still holds, so joining here would deadlock.
        let join_slot = Arc::new(std::sync::Mutex::new(None));
        let join_slot_hook = Arc::clone(&join_slot);
        let mut store_a_opt = Some(store_a);
        store_b.set_after_blob_write_hook(Box::new(move || {
            let mut store_a = store_a_opt.take().expect("hook is invoked exactly once");
            let handle = std::thread::spawn(move || {
                let _ = store_a.remove_contract(&key1);
            });
            // Real wall-clock sleep: this is a genuine cross-thread race test,
            // so a deterministic TimeSource cannot model the interleaving.
            std::thread::sleep(std::time::Duration::from_millis(300));
            *join_slot_hook.lock().unwrap() = Some(handle);
        }));

        // Store X2 via executor B. The hook fires mid-store, after the blob is
        // on disk and before X2's index is committed.
        store_b.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        // B released the lock on return; let A's remove finish.
        let handle = join_slot
            .lock()
            .unwrap()
            .take()
            .expect("hook must have spawned the remove thread");
        handle.join().expect("remove thread panicked");

        // Post-fix: A's scan observed X2's committed reference and kept the blob.
        assert!(
            wasm_path.exists(),
            "shared WASM blob must survive a concurrent store/remove race (#4216)"
        );

        // And X2 is fetchable from a FRESH cold-cache store over the same db,
        // forcing the disk read that pre-fix corruption breaks.
        let fresh = ContractStore::new(contract_dir.path().into(), 10_000, db)?;
        assert!(
            fresh.fetch_contract(&key2, &params2).is_some(),
            "newly stored instance must be fetchable from disk after the race (#4216)"
        );

        Ok(())
    }

    /// Regression test for issue #4218: pool executors must share ONE contract
    /// instance index. A contract stored via executor A must be visible to
    /// `code_hash_from_id` / `fetch_contract` on executor B without rebuilding
    /// B's store.
    ///
    /// Two `ContractStore`s built with `new_with_shared_index` over the SAME
    /// shared `Arc<DashMap>` (as `RuntimePool` now wires them) model executors A
    /// and B. Pre-fix, each `ContractStore::new` built its own `Arc<DashMap>`,
    /// so B's index never saw A's instance and both lookups returned `None`.
    #[tokio::test]
    async fn test_shared_index_visible_across_executors() -> Result<(), Box<dyn std::error::Error>>
    {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let shared_index: SharedContractIndex = Arc::new(DashMap::new());
        let mut store_a = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db.clone(),
            shared_index.clone(),
        )?;
        let store_b = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db,
            shared_index,
        )?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![3, 1, 4, 1, 5])),
            [9, 9].as_ref().into(),
        );
        let key = *contract.key();
        let params: Parameters = [9, 9].as_ref().into();

        // Stored via executor A only.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract,
        )))?;

        // Executor B must resolve the instance and fetch it via the SHARED index.
        assert_eq!(
            store_b.code_hash_from_id(key.id()),
            Some(*key.code_hash()),
            "instance stored via executor A must be resolvable on executor B \
             through the shared index (#4218 problem 1)"
        );
        assert!(
            store_b.fetch_contract(&key, &params).is_some(),
            "contract stored via executor A must be fetchable on executor B \
             through the shared index (#4218 problem 1)"
        );

        Ok(())
    }

    /// Regression test for issue #4218: a removal on executor A must be
    /// immediately authoritative on executor B — B must NOT serve the removed
    /// instance as a "ghost" from its own still-warm code cache.
    ///
    /// We warm B's `contract_cache` for the shared code hash (by fetching a
    /// SECOND instance of the same code through B), then remove the first
    /// instance via A. With the shared index + the `fetch_contract` index gate,
    /// B's fetch of the removed instance returns `None` even though the code
    /// hash is still cached on B (still referenced by the surviving instance).
    #[tokio::test]
    async fn test_shared_index_remove_closes_ghost_across_executors()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let shared_index: SharedContractIndex = Arc::new(DashMap::new());
        let mut store_a = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db.clone(),
            shared_index.clone(),
        )?;
        let store_b = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db,
            shared_index,
        )?;

        // Two instances of the SAME code (same code hash, different params).
        let shared_code = vec![7, 7, 7, 7];
        let params1 = Parameters::from(vec![1, 0, 0]);
        let params2 = Parameters::from(vec![2, 0, 0]);
        let contract1 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params1.clone(),
        );
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        assert_eq!(key1.code_hash(), key2.code_hash());

        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract2,
        )))?;

        // Warm B's per-executor code cache for the shared code hash by fetching
        // the SURVIVING instance through B.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_some(),
            "surviving instance must be fetchable on B (warms B's code cache)"
        );

        // Remove instance 1 via executor A. The shared blob survives (instance 2
        // still references the code hash), but instance 1 is gone from the index.
        store_a.remove_contract(&key1)?;

        // B must NOT serve instance 1 as a ghost even though the shared code hash
        // is warm in B's cache.
        assert!(
            store_b.fetch_contract(&key1, &params1).is_none(),
            "instance removed via executor A must not be served as a ghost by B \
             from a stale warm cache (#4218 problem 2)"
        );
        // The surviving instance is unaffected.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_some(),
            "surviving instance must remain fetchable on B after the other is removed"
        );

        Ok(())
    }

    /// Regression test for issue #4218 (disk-budget double-count): the dedup
    /// probe that gates the wasm disk-budget charge must be keyed by CODE HASH,
    /// so a NEW instance of already-stored code (same code hash, different
    /// params) is recognised as "already stored" and not charged a second time
    /// — even on a DIFFERENT pool executor whose code cache is cold.
    ///
    /// This pins `code_blob_stored` (the probe used by the executor gate sites)
    /// against the exact cross-executor scenario. The commented contrast shows
    /// why the old instance-keyed `fetch_contract` probe double-counted: it
    /// returns `None` here for the second instance, so the gate would charge the
    /// shared blob twice.
    #[tokio::test]
    async fn test_dedup_probe_shared_code_not_double_charged_across_executors()
    -> Result<(), Box<dyn std::error::Error>> {
        let contract_dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(contract_dir.path())?;
        let db = create_test_db(contract_dir.path()).await;

        let shared_index: SharedContractIndex = Arc::new(DashMap::new());
        let mut store_a = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db.clone(),
            shared_index.clone(),
        )?;
        let store_b = ContractStore::new_with_shared_index(
            contract_dir.path().into(),
            10_000,
            db,
            shared_index,
        )?;

        let shared_code = vec![5, 5, 5, 5, 5];
        let params1 = Parameters::from(vec![1, 2, 3]);
        let params2 = Parameters::from(vec![4, 5, 6]);
        let contract1 =
            WrappedContract::new(Arc::new(ContractCode::from(shared_code.clone())), params1);
        let contract2 = WrappedContract::new(
            Arc::new(ContractCode::from(shared_code.clone())),
            params2.clone(),
        );
        let key1 = *contract1.key();
        let key2 = *contract2.key();
        let code_hash = *key1.code_hash();
        assert_eq!(key1.code_hash(), key2.code_hash());

        // Instance 1 stored (and blob charged) via executor A.
        store_a.store_contract(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            contract1,
        )))?;

        // The dedup probe on executor B (cold code cache) must report the shared
        // blob as ALREADY stored, so PUTting instance 2 charges nothing more.
        assert!(
            store_b.code_blob_stored(&code_hash),
            "the shared code blob must be recognised as stored on executor B \
             (dedup — no double-count of the disk budget) (#4218)"
        );

        // Contrast: the OLD instance-keyed probe returns None for the never-yet-
        // stored second instance, which is exactly what made the gate charge the
        // shared blob twice before this fix.
        assert!(
            store_b.fetch_contract(&key2, &params2).is_none(),
            "instance-keyed fetch_contract returns None for a new instance — the \
             old dedup probe would have double-charged the shared blob"
        );

        Ok(())
    }
}
