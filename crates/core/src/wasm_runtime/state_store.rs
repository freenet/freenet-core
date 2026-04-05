use core::future::Future;
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
}

/// StateStore wraps a persistent storage backend with an optional in-memory cache.
/// It is Clone when the underlying storage S is Clone (e.g., ReDb with Arc<Database>).
///
/// For deterministic simulation testing, use `new_uncached()` to bypass the moka
/// cache which uses TinyLFU admission and may introduce non-determinism.
#[derive(Clone)]
pub struct StateStore<S: StateStorage> {
    state_mem_cache: Option<MokaCache<ContractKey, WrappedState>>,
    store: S,
}

impl<S> StateStore<S>
where
    S: StateStorage + Send + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    /// Create a StateStore with moka caching enabled.
    ///
    /// # Arguments
    /// - max_size: max number of bytes for the mem cache
    pub fn new(store: S, max_size: u32) -> Result<Self, StateStoreError> {
        let cache = MokaCache::builder()
            .max_capacity(max_size as u64)
            .weigher(|_key: &ContractKey, value: &WrappedState| -> u32 {
                value.size().try_into().unwrap_or(u32::MAX)
            })
            .build();
        Ok(Self {
            state_mem_cache: Some(cache),
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
            store,
        }
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
        self.store
            .store(key, state.clone())
            .await
            .map_err(Into::into)?;
        self.store
            .store_params(key, params.clone())
            .await
            .map_err(Into::into)?;

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
}
