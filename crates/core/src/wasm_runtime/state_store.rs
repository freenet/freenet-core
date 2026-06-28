use core::future::Future;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use freenet_stdlib::prelude::*;
use moka::sync::Cache as MokaCache;

/// Maximum size of a single contract state blob (50 MiB).
///
/// This is a network-level protocol constant — identical across all nodes — so that any node
/// can make routing and caching decisions without prior knowledge of which peer will handle a
/// request. Making it per-node-configurable would create routing non-determinism and allow
/// an attacker to selectively target lenient nodes.
///
/// 50 MiB is conservative enough to prevent disk exhaustion by malicious contracts while
/// remaining large enough for legitimate use cases (web apps, documents, datasets). A contract
/// whose `validate_state` returns `Valid` for arbitrarily large state cannot bypass this limit.
pub(crate) const MAX_STATE_SIZE: usize = 50 * 1024 * 1024; // 50 MiB

/// Maximum number of per-contract state-hash entries retained for the cheap
/// summarize/delta change-detector (see [`StateStore::cached_state_hash`]).
///
/// Bounded so the detector can never grow without limit on a node that hosts
/// many contracts (contract count is influenced by external actors via PUT, so
/// per the channel/collection-bounding rules it must be capped). Each entry is
/// only ~40 bytes (key + `u64`), so this ceiling is well under 5 MiB. A miss
/// simply falls back to loading and hashing the state, so the cap only trades a
/// little extra work for a hard memory ceiling — never correctness.
const STATE_HASH_CACHE_CAPACITY: u64 = 100_000;

/// Hash a contract state's bytes into the `u64` change-detector key shared by
/// the summarize and delta caches.
///
/// Centralised so the hash recorded at write time (well — at the slow-path
/// populate; see [`StateStore::cache_state_hash`]) and the hash recomputed when
/// summarizing/delta-ing a loaded state are ALWAYS computed identically. If
/// these ever diverged, a fast-path cache key would never match its slow-path
/// counterpart and the optimization would silently disable itself (a perf bug,
/// not a correctness one — but worth ruling out).
pub(crate) fn state_hash(state: &WrappedState) -> u64 {
    let mut hasher = DefaultHasher::new();
    state.as_ref().hash(&mut hasher);
    hasher.finish()
}

/// A cheap, cloneable handle to a [`StateStore`]'s state-change detector, for
/// the contract-state write paths that BYPASS `StateStore` and must still
/// invalidate it — specifically the V2 delegate writes
/// (`put_contract_state_sync` / `update_contract_state_sync`), which write
/// through the raw `Storage` and never call `StateStore::{store,update}`.
///
/// Obtained via [`StateStore::change_detector`] and wired into the runtime's
/// `state_write_callback`. Cloning shares the underlying cache (moka is
/// internally `Arc`), so an invalidation through the handle is observed by every
/// executor reading the detector.
#[derive(Clone)]
pub(crate) struct StateChangeDetector {
    cache: MokaCache<ContractKey, u64>,
}

impl StateChangeDetector {
    /// Invalidate the detector entry for `key` after a state write made outside
    /// `StateStore`. Mirrors the invalidation `StateStore::{store,update,delete}`
    /// do for in-store writes; see [`StateStore::state_hash_cache`].
    pub(crate) fn invalidate(&self, key: &ContractKey) {
        self.cache.invalidate(key);
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StateStoreError {
    #[error(transparent)]
    Any(#[from] anyhow::Error),
    #[error("missing contract: {0}")]
    MissingContract(ContractKey),
    #[error("contract state too large for {key}: {size} bytes exceeds limit of {limit} bytes")]
    StateTooLarge {
        key: ContractKey,
        size: usize,
        limit: usize,
    },
}

impl From<StateStoreError> for crate::wasm_runtime::ContractError {
    fn from(value: StateStoreError) -> Self {
        match value {
            StateStoreError::Any(err) => {
                crate::wasm_runtime::ContractError::from(anyhow::format_err!(err))
            }
            err @ StateStoreError::MissingContract(_) => {
                crate::wasm_runtime::ContractError::from(anyhow::format_err!(err))
            }
            err @ StateStoreError::StateTooLarge { .. } => {
                crate::wasm_runtime::ContractError::from(anyhow::format_err!(err))
            }
        }
    }
}

pub trait StateStorage {
    type Error;
    /// Store state for a contract. Takes `&self` because implementations
    /// (like ReDb) handle internal locking for concurrent access.
    fn store(
        &self,
        key: ContractKey,
        state: WrappedState,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Store parameters for a contract. Takes `&self` because implementations
    /// handle internal locking for concurrent access.
    fn store_params(
        &self,
        key: ContractKey,
        state: Parameters<'static>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn get(
        &self,
        key: &ContractKey,
    ) -> impl Future<Output = Result<Option<WrappedState>, Self::Error>> + Send;
    fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> impl Future<Output = Result<Option<Parameters<'static>>, Self::Error>> + Send + 'a;
    /// Remove all persisted data for a contract (state and parameters).
    /// Idempotent: removing a contract that is not stored is not an error.
    fn remove(&self, key: &ContractKey) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// StateStore wraps a persistent storage backend with an optional in-memory cache.
/// It is Clone when the underlying storage S is Clone (e.g., ReDb with Arc<Database>).
///
/// For deterministic simulation testing, use `new_uncached()` to bypass the moka
/// cache which uses TinyLFU admission and may introduce non-determinism.
#[derive(Clone)]
pub struct StateStore<S: StateStorage> {
    state_mem_cache: Option<MokaCache<ContractKey, WrappedState>>,
    /// Cheap per-contract state-change detector for the read-only summarize and
    /// delta WASM caches (the `bridged_summarize_contract_state` /
    /// `bridged_get_contract_state_delta` hot path).
    ///
    /// Maps a contract to the `u64` hash of its CURRENTLY-stored state, or holds
    /// no entry when unknown. The invariant that makes this safe to read on the
    /// summarize/delta fast path WITHOUT reloading and rehashing the full state:
    ///
    ///   the entry is either ABSENT or equal to `state_hash(state on disk)`.
    ///
    /// It is maintained by three kinds of operation:
    ///   * any write through this `StateStore` — [`store`](Self::store),
    ///     [`update`](Self::update), [`delete`](Self::delete) — INVALIDATES the
    ///     entry, so a changed state can never be served against a stale hash.
    ///   * V2 delegate state writes (`put_contract_state_sync` /
    ///     `update_contract_state_sync`) write directly through the raw
    ///     `Storage`, BYPASSING `StateStore`, so they invalidate the entry via a
    ///     [`StateChangeDetector`] handle wired into the runtime's
    ///     `state_write_callback` (see `Runtime::set_state_write_callback` and
    ///     the callback installers in `executor/runtime.rs`). Without this a V2
    ///     write would leave a stale detector hash → stale summary/delta →
    ///     divergence (caught by Codex review).
    ///   * the summarize/delta slow path re-POPULATES it after loading the
    ///     current state (via [`cache_state_hash`](Self::cache_state_hash)).
    ///
    /// Because all contract-state writes and all summarize/delta reads are
    /// serialized through the single `&mut RuntimePool` contract-handling loop,
    /// no write can interleave between the slow path's load and its populate, so
    /// the populated hash always matches the state that produced the cached
    /// summary/delta. Shared across pool executors (moka is internally `Arc`),
    /// so a populate on one checked-out executor is visible to the next.
    ///
    /// Kept separate from `state_mem_cache` (and intentionally always on, even
    /// in `new_uncached` deterministic mode) because it only governs WHETHER the
    /// fast path is taken — both paths produce byte-identical summaries/deltas,
    /// so it cannot perturb simulation output or event-trace determinism.
    state_hash_cache: MokaCache<ContractKey, u64>,
    store: S,
}

impl<S> StateStore<S>
where
    S: StateStorage + Send + Sync + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    /// Create a StateStore with moka caching enabled.
    ///
    /// # Arguments
    /// - max_size: max number of bytes for the mem cache
    pub fn new(store: S, max_size: u32) -> Result<Self, StateStoreError> {
        let cache = MokaCache::builder()
            .max_capacity(max_size as u64)
            .weigher(|key: &ContractKey, value: &WrappedState| -> u32 {
                // Saturate to u32::MAX on overflow as moka recommends. This
                // should be unreachable in practice (MAX_STATE_SIZE is 50 MiB),
                // so a hit here indicates a bug elsewhere — log it loudly.
                u32::try_from(value.size()).unwrap_or_else(|_| {
                    tracing::warn!(
                        contract = %key,
                        size_bytes = value.size(),
                        "State size exceeds u32::MAX in cache weigher; saturating. \
                         This should be impossible — MAX_STATE_SIZE enforcement is broken."
                    );
                    u32::MAX
                })
            })
            .build();
        Ok(Self {
            state_mem_cache: Some(cache),
            state_hash_cache: Self::build_state_hash_cache(),
            store,
        })
    }

    /// Create a StateStore without caching for deterministic simulation.
    ///
    /// This bypasses the moka cache which uses TinyLFU admission and may
    /// introduce non-determinism in tests.
    ///
    /// Use this constructor for deterministic simulation testing under turmoil.
    pub fn new_uncached(store: S) -> Self {
        Self {
            state_mem_cache: None,
            state_hash_cache: Self::build_state_hash_cache(),
            store,
        }
    }

    /// Build the bounded state-change-detector cache (see the
    /// [`state_hash_cache`](Self::state_hash_cache) field docs). Entry-count
    /// bounded via the default weigher.
    fn build_state_hash_cache() -> MokaCache<ContractKey, u64> {
        MokaCache::builder()
            .max_capacity(STATE_HASH_CACHE_CAPACITY)
            .build()
    }

    /// A cloneable handle to this store's state-change detector, for the V2
    /// delegate write path which bypasses `StateStore` (see
    /// [`StateChangeDetector`]). Wired into the runtime's `state_write_callback`.
    pub(crate) fn change_detector(&self) -> StateChangeDetector {
        StateChangeDetector {
            cache: self.state_hash_cache.clone(),
        }
    }

    /// Cheap read of the change-detector: the hash of `key`'s currently-stored
    /// state, or `None` if not currently known (never written/populated, or
    /// evicted, or invalidated by a write since the last populate).
    ///
    /// A `Some(h)` return guarantees the on-disk state hashes to `h` (see the
    /// field invariant), so a caller holding a cached summary/delta keyed on `h`
    /// may return it WITHOUT reloading or rehashing the state.
    pub fn cached_state_hash(&self, key: &ContractKey) -> Option<u64> {
        self.state_hash_cache.get(key)
    }

    /// Record the hash of `key`'s currently-stored state so the next
    /// summarize/delta of an UNCHANGED state takes the fast path.
    ///
    /// MUST only be called with a hash freshly computed from the state that is
    /// currently on disk (i.e. the slow path's just-loaded state), so the field
    /// invariant — entry equals `state_hash(state on disk)` — is preserved.
    pub fn cache_state_hash(&self, key: ContractKey, hash: u64) {
        self.state_hash_cache.insert(key, hash);
    }

    pub async fn update(
        &mut self,
        key: &ContractKey,
        state: WrappedState,
    ) -> Result<(), StateStoreError> {
        if state.size() > MAX_STATE_SIZE {
            tracing::warn!(
                contract = %key,
                size_bytes = state.size(),
                limit_bytes = MAX_STATE_SIZE,
                "Rejecting oversized state at storage layer (update)"
            );
            return Err(StateStoreError::StateTooLarge {
                key: *key,
                size: state.size(),
                limit: MAX_STATE_SIZE,
            });
        }

        // only allow updates for existing contracts
        let cache_miss = if let Some(cache) = &self.state_mem_cache {
            cache.get(key).is_none()
        } else {
            true
        };

        if cache_miss {
            self.store
                .get(key)
                .await
                .map_err(Into::into)?
                .ok_or_else(|| StateStoreError::MissingContract(*key))?;
        }

        // Persist first, then cache. If persist fails, the cache must not
        // hold data that was never written to disk. See issue #3487.
        self.store
            .store(*key, state.clone())
            .await
            .map_err(Into::into)?;

        // The state changed: drop any stale change-detector hash so the
        // summarize/delta fast path can never serve a cached summary/delta for
        // the previous state. The slow path repopulates it on the next read.
        self.state_hash_cache.invalidate(key);

        if let Some(cache) = &self.state_mem_cache {
            cache.insert(*key, state);
        }
        Ok(())
    }

    pub async fn store(
        &mut self,
        key: ContractKey,
        state: WrappedState,
        params: Parameters<'static>,
    ) -> Result<(), StateStoreError> {
        if state.size() > MAX_STATE_SIZE {
            tracing::warn!(
                contract = %key,
                size_bytes = state.size(),
                limit_bytes = MAX_STATE_SIZE,
                "Rejecting oversized state at storage layer (store)"
            );
            return Err(StateStoreError::StateTooLarge {
                key,
                size: state.size(),
                limit: MAX_STATE_SIZE,
            });
        }

        // Persist first, then cache. If persist fails, the cache must not
        // hold data that was never written to disk. See issue #3487.
        //
        // Partial-failure window: if `store_params` fails after `store`
        // succeeds, the on-disk state will be orphaned (state without
        // params) and the cache will not be populated. This is strictly
        // better than the pre-fix behavior (which could cache phantom
        // state on any persist failure), but a transactional fix is
        // tracked separately.
        self.store
            .store(key, state.clone())
            .await
            .map_err(Into::into)?;
        self.store
            .store_params(key, params.clone())
            .await
            .map_err(Into::into)?;

        // The state changed: drop any stale change-detector hash (see `update`).
        self.state_hash_cache.invalidate(&key);

        if let Some(cache) = &self.state_mem_cache {
            cache.insert(key, state);
        }
        Ok(())
    }

    pub async fn get(&self, key: &ContractKey) -> Result<WrappedState, StateStoreError> {
        // Check cache first (if enabled)
        if let Some(cache) = &self.state_mem_cache {
            if let Some(v) = cache.get(key) {
                return Ok(v);
            }
        }
        let r = self.store.get(key).await.map_err(Into::into)?;
        r.ok_or_else(|| StateStoreError::MissingContract(*key))
    }

    /// Delete a contract's persisted state and parameters and drop it from
    /// the in-memory cache. Used to reclaim disk space when a contract is
    /// evicted from the hosting cache. Idempotent.
    pub async fn delete(&self, key: &ContractKey) -> Result<(), StateStoreError> {
        if let Some(cache) = &self.state_mem_cache {
            cache.invalidate(key);
        }
        // The state is gone: drop the change-detector hash so a later
        // summarize/delta can't fast-path against it (it must fall through to
        // the slow path, which now reports the contract as absent).
        self.state_hash_cache.invalidate(key);
        self.store.remove(key).await.map_err(Into::into)?;
        Ok(())
    }

    pub async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, StateStoreError> {
        let r = self.store.get_params(key).await.map_err(Into::into)?;
        Ok(r)
    }

    /// Persist contract parameters to the backing store, unconditionally.
    ///
    /// This is idempotent — safe to call even if params are already stored.
    /// Used to ensure params survive node restarts: the initial PUT path
    /// writes params via `store()`, but subsequent code paths (merge, update)
    /// go through `update()` which only writes state. Calling `ensure_params`
    /// closes the gap where params could be lost after restart.
    pub async fn ensure_params(
        &self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), StateStoreError> {
        self.store
            .store_params(key, params)
            .await
            .map_err(|e| StateStoreError::Any(e.into()))?;
        Ok(())
    }

    /// Get a reference to the underlying storage backend.
    /// Used for hosting metadata persistence operations.
    pub fn inner(&self) -> &S {
        &self.store
    }

    /// Get a clone of the underlying storage backend.
    /// Used when the storage needs to be shared with other components
    /// (e.g., ContractStore, DelegateStore, SecretsStore).
    pub fn storage(&self) -> S
    where
        S: Clone,
    {
        self.store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wasm_runtime::mock_state_storage::MockStateStorage;

    fn make_test_key() -> ContractKey {
        let code = ContractCode::from(vec![1, 2, 3, 4]);
        let params = Parameters::from(vec![5, 6, 7, 8]);
        ContractKey::from_params_and_code(&params, &code)
    }

    fn make_test_key_with_code(code_bytes: &[u8]) -> ContractKey {
        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![5, 6, 7, 8]);
        ContractKey::from_params_and_code(&params, &code)
    }

    fn make_test_state(data: &[u8]) -> WrappedState {
        WrappedState::new(data.to_vec())
    }

    // ============ Basic StateStore Operations ============

    /// Test basic store and retrieve through StateStore wrapper
    #[tokio::test]
    async fn test_state_store_basic_operations() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store state and params
        store
            .store(key, state.clone(), params.clone())
            .await
            .unwrap();

        // Retrieve state
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(retrieved, state);

        // Retrieve params
        let retrieved_params = store.get_params(&key).await.unwrap();
        assert_eq!(retrieved_params, Some(params));
    }

    /// Test that get returns error for non-existent contract
    #[tokio::test]
    async fn test_state_store_get_nonexistent() {
        let mock_storage = MockStateStorage::new();
        let store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let result = store.get(&key).await;

        assert!(matches!(result, Err(StateStoreError::MissingContract(_))));
    }

    /// Test that update fails for non-existent contract
    #[tokio::test]
    async fn test_state_store_update_nonexistent() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);

        let result = store.update(&key, state).await;

        assert!(matches!(result, Err(StateStoreError::MissingContract(_))));
    }

    /// Test successful update of existing contract
    #[tokio::test]
    async fn test_state_store_update_existing() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let initial_state = make_test_state(&[1, 2, 3]);
        let updated_state = make_test_state(&[4, 5, 6]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store initial state
        store.store(key, initial_state, params).await.unwrap();

        // Update state
        store.update(&key, updated_state.clone()).await.unwrap();

        // Verify updated state
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(retrieved, updated_state);
    }

    // ============ Storage Failure Scenarios ============

    /// Test that store failure propagates correctly.
    ///
    /// Scenario: PUT operation where persistent storage fails after
    /// memory cache is updated.
    #[tokio::test]
    async fn test_state_store_storage_failure_on_store() {
        let mock_storage = MockStateStorage::new();
        mock_storage.fail_next_stores(1);

        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        let result = store.store(key, state, params).await;

        // Store should fail due to injected failure
        assert!(result.is_err());
    }

    /// Test that params store failure propagates correctly.
    ///
    /// Scenario: PUT operation where state storage succeeds but params
    /// storage fails, leaving contract in inconsistent state.
    #[tokio::test]
    async fn test_state_store_params_failure_on_store() {
        let mock_storage = MockStateStorage::new();
        mock_storage.fail_next_store_params(1);

        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        let result = store.store(key, state, params).await;

        // Store should fail due to params storage failure
        assert!(result.is_err());
    }

    /// Test storage failure during update operation.
    ///
    /// Scenario: UPDATE operation where the contract exists but
    /// persistent storage fails during the update.
    #[tokio::test]
    async fn test_state_store_failure_on_update() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage.clone(), 10_000).unwrap();

        let key = make_test_key();
        let initial_state = make_test_state(&[1, 2, 3]);
        let updated_state = make_test_state(&[4, 5, 6]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store initial state successfully
        store.store(key, initial_state, params).await.unwrap();

        // Configure failure for next store operation (update uses store internally)
        mock_storage.fail_next_stores(1);

        // Update should fail
        let result = store.update(&key, updated_state).await;
        assert!(result.is_err());
    }

    /// Test get failure from persistent storage.
    ///
    /// Scenario: GET operation where memory cache doesn't have the state
    /// and persistent storage fails.
    #[tokio::test]
    async fn test_state_store_failure_on_get() {
        let mock_storage = MockStateStorage::new();
        // Seed state directly in mock (bypasses cache)
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        mock_storage.seed_state(key, state);

        // Configure failure
        mock_storage.fail_next_gets(1);

        let store = StateStore::new(mock_storage, 10_000).unwrap();

        // Get should fail since cache is empty and storage fails
        let result = store.get(&key).await;
        assert!(result.is_err());
    }

    // ============ Cache Coherence Tests ============

    /// Test that memory cache is populated during store.
    ///
    /// Verifies that after storing a contract, the state can be retrieved.
    /// This test verifies the functional correctness of cache + storage.
    #[tokio::test]
    async fn test_state_store_cache_populated_on_store() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage.clone(), 10_000).unwrap();

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store state (this should populate cache)
        store.store(key, state.clone(), params).await.unwrap();

        // Get should work (either from cache or storage)
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(retrieved, state);

        // Multiple gets should all return the same state
        for _ in 0..5 {
            let retrieved = store.get(&key).await.unwrap();
            assert_eq!(retrieved, state);
        }
    }

    /// Test that update modifies cache correctly.
    ///
    /// Verifies that after updating a contract, the cache reflects
    /// the new state immediately.
    #[tokio::test]
    async fn test_state_store_cache_update_coherence() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage.clone(), 10_000).unwrap();

        let key = make_test_key();
        let initial_state = make_test_state(&[1, 2, 3]);
        let updated_state = make_test_state(&[4, 5, 6]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store initial state
        store.store(key, initial_state, params).await.unwrap();

        // Update state
        store.update(&key, updated_state.clone()).await.unwrap();

        // Get should return updated state from cache
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(retrieved, updated_state);
    }

    // ============ Multiple Contract Tests ============

    /// Test storing and retrieving multiple contracts.
    #[tokio::test]
    async fn test_state_store_multiple_contracts() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key1 = make_test_key_with_code(&[1]);
        let key2 = make_test_key_with_code(&[2]);
        let key3 = make_test_key_with_code(&[3]);

        let state1 = make_test_state(&[10]);
        let state2 = make_test_state(&[20]);
        let state3 = make_test_state(&[30]);

        let params = Parameters::from(vec![5, 6, 7, 8]);

        // Store all contracts
        store
            .store(key1, state1.clone(), params.clone())
            .await
            .unwrap();
        store
            .store(key2, state2.clone(), params.clone())
            .await
            .unwrap();
        store.store(key3, state3.clone(), params).await.unwrap();

        // Verify all can be retrieved
        assert_eq!(store.get(&key1).await.unwrap(), state1);
        assert_eq!(store.get(&key2).await.unwrap(), state2);
        assert_eq!(store.get(&key3).await.unwrap(), state3);
    }

    /// Test that failure for one contract doesn't affect others.
    #[tokio::test]
    async fn test_state_store_isolated_failures() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage.clone(), 10_000).unwrap();

        let key1 = make_test_key_with_code(&[1]);
        let key2 = make_test_key_with_code(&[2]);

        let state1 = make_test_state(&[10]);
        let state2 = make_test_state(&[20]);
        let params = Parameters::from(vec![5, 6, 7, 8]);

        // Store first contract successfully
        store
            .store(key1, state1.clone(), params.clone())
            .await
            .unwrap();

        // Configure key2 to fail
        mock_storage.fail_for_key(key2);

        // Second contract should fail
        let result = store.store(key2, state2, params).await;
        assert!(result.is_err());

        // First contract should still be retrievable
        assert_eq!(store.get(&key1).await.unwrap(), state1);
    }

    // ============ Edge Case: Empty State ============

    /// Test storing and retrieving empty state.
    #[tokio::test]
    async fn test_state_store_empty_state() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let empty_state = make_test_state(&[]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store empty state
        store.store(key, empty_state.clone(), params).await.unwrap();

        // Retrieve should return empty state, not error
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(retrieved, empty_state);
        assert_eq!(retrieved.size(), 0);
    }

    // ============ State Size Limit Tests ============

    /// Verify that store() rejects state exceeding MAX_STATE_SIZE.
    ///
    /// Uses new_uncached() for determinism — cache admission policy
    /// is irrelevant to the size-limit check.
    #[tokio::test]
    async fn test_store_rejects_oversized_state() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new_uncached(mock_storage);
        let key = make_test_key();
        let oversized = make_test_state(&vec![0u8; MAX_STATE_SIZE + 1]);
        let params = Parameters::from(vec![1, 2, 3]);

        let result = store.store(key, oversized, params).await;

        assert!(
            matches!(result, Err(StateStoreError::StateTooLarge { .. })),
            "Expected StateTooLarge, got: {result:?}"
        );
    }

    /// Verify that update() rejects state exceeding MAX_STATE_SIZE.
    #[tokio::test]
    async fn test_update_rejects_oversized_state() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new_uncached(mock_storage);
        let key = make_test_key();
        let small = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![1, 2, 3]);

        // Store a valid initial state first (update requires an existing contract)
        store.store(key, small, params).await.unwrap();

        let oversized = make_test_state(&vec![0u8; MAX_STATE_SIZE + 1]);
        let result = store.update(&key, oversized).await;

        assert!(
            matches!(result, Err(StateStoreError::StateTooLarge { .. })),
            "Expected StateTooLarge, got: {result:?}"
        );
    }

    /// Verify that state at exactly MAX_STATE_SIZE is accepted (limit is inclusive).
    #[tokio::test]
    async fn test_store_accepts_state_at_limit() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new_uncached(mock_storage);
        let key = make_test_key();
        let at_limit = make_test_state(&vec![0u8; MAX_STATE_SIZE]);
        let params = Parameters::from(vec![1, 2, 3]);

        // State of exactly MAX_STATE_SIZE bytes should be accepted
        store
            .store(key, at_limit, params)
            .await
            .expect("State at exactly MAX_STATE_SIZE should be accepted");
    }

    // ============ Edge Case: Large State ============

    /// Test storing and retrieving large state.
    #[tokio::test]
    async fn test_state_store_large_state() {
        let mock_storage = MockStateStorage::new();
        // Use larger cache for this test
        let mut store = StateStore::new(mock_storage, 2_000_000).unwrap();

        let key = make_test_key();
        // 1MB state
        let large_data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let large_state = make_test_state(&large_data);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store large state
        store.store(key, large_state.clone(), params).await.unwrap();

        // Retrieve should work
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(retrieved, large_state);
        assert_eq!(retrieved.size(), 1_000_000);
    }

    // ============ Failure Recovery Tests ============

    /// Test that after a failed store, the state is not retrievable (uncached mode).
    ///
    /// This verifies clean failure semantics: when persistent store fails,
    /// no partial state should be visible to subsequent operations.
    ///
    /// Note: Use uncached mode for deterministic testing.
    #[tokio::test]
    async fn test_uncached_store_failure_leaves_no_state() {
        let mock_storage = MockStateStorage::new();
        mock_storage.fail_next_stores(1);

        let mut store = StateStore::new_uncached(mock_storage);

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store should fail
        let result = store.store(key, state, params).await;
        assert!(result.is_err());

        // Subsequent get should return MissingContract, not stale data
        let get_result = store.get(&key).await;
        assert!(
            matches!(get_result, Err(StateStoreError::MissingContract(_))),
            "Expected MissingContract after failed store, got {:?}",
            get_result
        );
    }

    /// Test that after a failed update, the original state is preserved (uncached mode).
    ///
    /// Verifies that update failure doesn't corrupt the existing state.
    #[tokio::test]
    async fn test_uncached_update_failure_preserves_original_state() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new_uncached(mock_storage.clone());

        let key = make_test_key();
        let original_state = make_test_state(&[1, 2, 3]);
        let updated_state = make_test_state(&[4, 5, 6]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store original state successfully
        store
            .store(key, original_state.clone(), params)
            .await
            .unwrap();

        // Configure failure for update
        mock_storage.fail_next_stores(1);

        // Update should fail
        let result = store.update(&key, updated_state).await;
        assert!(result.is_err());

        // Original state should still be retrievable
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(
            retrieved, original_state,
            "Original state should be preserved after failed update"
        );
    }

    /// Test sequential updates where intermediate ones fail (uncached mode).
    ///
    /// Verifies that failed updates don't affect successful ones.
    #[tokio::test]
    async fn test_uncached_sequential_updates_with_failures() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new_uncached(mock_storage.clone());

        let key = make_test_key();
        let state_v1 = make_test_state(&[1]);
        let state_v2 = make_test_state(&[2]);
        let state_v3 = make_test_state(&[3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store v1 successfully
        store.store(key, state_v1.clone(), params).await.unwrap();

        // Update to v2 - should succeed
        store.update(&key, state_v2.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), state_v2);

        // Update to v3 - configure to fail
        mock_storage.fail_next_stores(1);
        let result = store.update(&key, state_v3).await;
        assert!(result.is_err());

        // State should still be v2 (last successful update)
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(
            retrieved, state_v2,
            "State should be v2 after v3 update failed"
        );
    }

    // ============ ensure_params Tests ============

    /// Regression test: params must survive even when only update() is called
    /// after the initial store(). Simulates the scenario where a gateway restarts
    /// and receives BroadcastToStreaming UPDATEs for contracts whose params were
    /// only written once during the initial PUT.
    #[tokio::test]
    async fn test_ensure_params_persists_independently() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new_uncached(mock_storage);

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store state + params (initial PUT path)
        store
            .store(key, state.clone(), params.clone())
            .await
            .unwrap();
        assert_eq!(store.get_params(&key).await.unwrap(), Some(params.clone()));

        // ensure_params is idempotent — calling it again doesn't break anything
        store.ensure_params(key, params.clone()).await.unwrap();
        assert_eq!(store.get_params(&key).await.unwrap(), Some(params));
    }

    /// Test that ensure_params works for a contract that has state but no params.
    /// This is the exact bug scenario: state was stored but params were lost.
    #[tokio::test]
    async fn test_ensure_params_fills_gap_when_params_missing() {
        let mock_storage = MockStateStorage::new();
        let store = StateStore::new_uncached(mock_storage.clone());

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Simulate the bug: store state directly without params
        mock_storage.seed_state(key, state);

        // Params should be missing
        assert_eq!(store.get_params(&key).await.unwrap(), None);

        // ensure_params fills the gap
        store.ensure_params(key, params.clone()).await.unwrap();
        assert_eq!(store.get_params(&key).await.unwrap(), Some(params));
    }

    // ============ Cache Coherence on Failure (issue #3487) ============

    /// Regression test for issue #3487: failed store must not leave stale data in cache.
    ///
    /// Before the fix, cache was populated before persist. If persist failed,
    /// subsequent reads would return the "phantom" data from cache.
    #[tokio::test]
    async fn test_cached_store_failure_does_not_leave_stale_cache() {
        let mock_storage = MockStateStorage::new();
        mock_storage.fail_next_stores(1);

        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store should fail
        let result = store.store(key, state, params).await;
        assert!(result.is_err());

        // Cache must NOT serve the failed state
        let get_result = store.get(&key).await;
        assert!(
            matches!(get_result, Err(StateStoreError::MissingContract(_))),
            "Cache should not serve data that was never persisted, got {:?}",
            get_result
        );
    }

    /// Regression test for issue #3487: failed update must not corrupt cached state.
    ///
    /// After a successful store followed by a failed update, reads must return
    /// the last successfully persisted state, not the failed update's data.
    #[tokio::test]
    async fn test_cached_update_failure_preserves_original_state() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage.clone(), 10_000).unwrap();

        let key = make_test_key();
        let original_state = make_test_state(&[1, 2, 3]);
        let bad_update = make_test_state(&[9, 9, 9]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store original state successfully
        store
            .store(key, original_state.clone(), params)
            .await
            .unwrap();

        // Configure failure for update
        mock_storage.fail_next_stores(1);

        // Update should fail
        let result = store.update(&key, bad_update).await;
        assert!(result.is_err());

        // Cache must still serve the original (last persisted) state
        let retrieved = store.get(&key).await.unwrap();
        assert_eq!(
            retrieved, original_state,
            "Cache should serve last persisted state after failed update"
        );
    }

    // ============ delete() Tests ============

    /// After store() then delete(), the contract's state and params must be gone.
    #[tokio::test]
    async fn test_state_store_delete_removes_state_and_params() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        // Store state and params, confirm they are present
        store
            .store(key, state.clone(), params.clone())
            .await
            .unwrap();
        assert_eq!(store.get(&key).await.unwrap(), state);
        assert_eq!(store.get_params(&key).await.unwrap(), Some(params));

        // Delete the contract
        store.delete(&key).await.unwrap();

        // State should no longer be retrievable
        let get_result = store.get(&key).await;
        assert!(
            matches!(get_result, Err(StateStoreError::MissingContract(_))),
            "Expected MissingContract after delete, got {get_result:?}"
        );

        // Params should be gone too
        assert_eq!(store.get_params(&key).await.unwrap(), None);
    }

    /// delete() on a contract that was never stored is a no-op (idempotent).
    #[tokio::test]
    async fn test_state_store_delete_never_stored_is_idempotent() {
        let mock_storage = MockStateStorage::new();
        let store = StateStore::new(mock_storage, 10_000).unwrap();

        let key = make_test_key();

        // Deleting a contract that was never stored must succeed
        store
            .delete(&key)
            .await
            .expect("delete of a never-stored contract should be Ok");
    }

    // ============ State-change detector (summarize/delta fast path) ============

    /// `state_hash` distinguishes different state bytes and is stable for equal
    /// bytes. This is the property the whole change-detector rests on.
    #[test]
    fn state_hash_distinguishes_different_states_and_is_stable() {
        let a = make_test_state(&[1, 2, 3]);
        let a_again = make_test_state(&[1, 2, 3]);
        let b = make_test_state(&[1, 2, 4]);

        assert_eq!(
            state_hash(&a),
            state_hash(&a_again),
            "equal state bytes must hash equally"
        );
        assert_ne!(
            state_hash(&a),
            state_hash(&b),
            "different state bytes must (with overwhelming probability) hash differently"
        );
    }

    /// CORRECTNESS of the detector primitive: a fresh store has no detector
    /// entry; a populate makes it readable; and EVERY write path
    /// (store/update/delete) invalidates it so a changed state can never be
    /// served against a stale hash. This is the invariant the no-divergence
    /// guarantee depends on.
    #[tokio::test]
    async fn state_hash_cache_invalidated_by_every_write_path() {
        let mock_storage = MockStateStorage::new();
        let mut store = StateStore::new_uncached(mock_storage);
        let key = make_test_key();
        let params = Parameters::from(vec![1, 2, 3]);
        let state_a = make_test_state(&[1, 2, 3]);
        let state_b = make_test_state(&[4, 5, 6]);

        // Never populated → absent.
        assert_eq!(store.cached_state_hash(&key), None);

        // Initial store (PUT) writes state → detector must be absent (a write
        // invalidates; the summarize slow path repopulates on next read).
        store
            .store(key, state_a.clone(), params.clone())
            .await
            .unwrap();
        assert_eq!(
            store.cached_state_hash(&key),
            None,
            "store() must leave the detector absent so the next summarize recomputes"
        );

        // Simulate the summarize slow path populating it.
        let h_a = state_hash(&state_a);
        store.cache_state_hash(key, h_a);
        assert_eq!(store.cached_state_hash(&key), Some(h_a));

        // UPDATE writes new state → detector invalidated (the critical guard:
        // without this, a stale hash would serve a stale summary/delta).
        store.update(&key, state_b.clone()).await.unwrap();
        assert_eq!(
            store.cached_state_hash(&key),
            None,
            "update() must invalidate the detector"
        );

        // Repopulate with the new state's hash; it differs from the old one.
        let h_b = state_hash(&state_b);
        assert_ne!(h_a, h_b);
        store.cache_state_hash(key, h_b);
        assert_eq!(store.cached_state_hash(&key), Some(h_b));

        // delete() removes the state → detector invalidated.
        store.delete(&key).await.unwrap();
        assert_eq!(
            store.cached_state_hash(&key),
            None,
            "delete() must invalidate the detector"
        );
    }

    /// The V2-bypass handle ([`StateChangeDetector`]) invalidates the same
    /// detector the summarize/delta fast path reads. This is the mechanism the
    /// runtime's `state_write_callback` uses so V2 delegate writes — which
    /// bypass `StateStore` — cannot leave a stale detector hash.
    #[tokio::test]
    async fn change_detector_handle_invalidates_cached_hash() {
        let mock_storage = MockStateStorage::new();
        let store = StateStore::new_uncached(mock_storage);
        let key = make_test_key();

        store.cache_state_hash(key, 7);
        assert_eq!(store.cached_state_hash(&key), Some(7));

        // A write through the bypass handle must clear the entry, exactly like an
        // in-store write (store/update/delete) does.
        let detector = store.change_detector();
        detector.invalidate(&key);
        assert_eq!(
            store.cached_state_hash(&key),
            None,
            "change_detector().invalidate must clear the detector the fast path reads"
        );
    }

    /// The detector is present in cached mode too (it is independent of the moka
    /// state cache, so it works after restart / on disk-only states).
    #[tokio::test]
    async fn state_hash_cache_present_in_cached_mode() {
        let mock_storage = MockStateStorage::new();
        let store = StateStore::new(mock_storage, 10_000).unwrap();
        let key = make_test_key();

        assert_eq!(store.cached_state_hash(&key), None);
        store.cache_state_hash(key, 42);
        assert_eq!(store.cached_state_hash(&key), Some(42));
    }
}
