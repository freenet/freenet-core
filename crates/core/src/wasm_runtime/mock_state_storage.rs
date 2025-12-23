//! Mock implementation of StateStorage for testing.
//!
//! Provides an in-memory state storage with:
//! - Configurable failure injection
//! - Operation tracking for assertions
//! - Thread-safe access

#![allow(dead_code)] // Utility methods for future test expansion

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use freenet_stdlib::prelude::*;

use super::state_store::StateStorage;

/// Error type for MockStateStorage operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MockStorageError {
    #[error("Injected failure: {0}")]
    InjectedFailure(String),
    #[error("Storage error: {0}")]
    StorageError(String),
}

/// Configuration for failure injection.
#[derive(Debug, Clone, Default)]
pub struct FailureConfig {
    /// Fail the next N store operations
    pub fail_next_stores: usize,
    /// Fail the next N store_params operations
    pub fail_next_store_params: usize,
    /// Fail the next N get operations
    pub fail_next_gets: usize,
    /// Fail the next N get_params operations
    pub fail_next_get_params: usize,
    /// Fail only for specific keys
    pub fail_for_keys: Vec<ContractKey>,
}

/// Recorded operation for tracking.
#[derive(Debug, Clone)]
pub enum StorageOperation {
    Store { key: ContractKey },
    StoreParams { key: ContractKey },
    Get { key: ContractKey },
    GetParams { key: ContractKey },
}

/// Inner state protected by mutex.
#[derive(Debug, Default)]
struct MockStateStorageInner {
    states: HashMap<ContractKey, WrappedState>,
    params: HashMap<ContractKey, Parameters<'static>>,
    operations: Vec<StorageOperation>,
    failure_config: FailureConfig,
}

/// Mock implementation of StateStorage for testing.
///
/// Stores state in memory and provides:
/// - Failure injection via `configure_failures()`
/// - Operation tracking via `operations()` and `operation_count()`
/// - State inspection via `get_stored_state()` and `get_stored_params()`
#[derive(Debug, Clone, Default)]
pub struct MockStateStorage {
    inner: Arc<Mutex<MockStateStorageInner>>,
    store_count: Arc<AtomicUsize>,
    get_count: Arc<AtomicUsize>,
}

impl MockStateStorage {
    /// Create a new empty MockStateStorage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configure failure injection.
    pub fn configure_failures(&self, config: FailureConfig) {
        let mut inner = self.inner.lock().unwrap();
        inner.failure_config = config;
    }

    /// Set to fail the next N store operations.
    pub fn fail_next_stores(&self, count: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.failure_config.fail_next_stores = count;
    }

    /// Set to fail the next N get operations.
    pub fn fail_next_gets(&self, count: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.failure_config.fail_next_gets = count;
    }

    /// Set to fail the next N store_params operations.
    pub fn fail_next_store_params(&self, count: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.failure_config.fail_next_store_params = count;
    }

    /// Set to fail the next N get_params operations.
    pub fn fail_next_get_params(&self, count: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.failure_config.fail_next_get_params = count;
    }

    /// Add a key that should always fail.
    pub fn fail_for_key(&self, key: ContractKey) {
        let mut inner = self.inner.lock().unwrap();
        inner.failure_config.fail_for_keys.push(key);
    }

    /// Get all recorded operations.
    pub fn operations(&self) -> Vec<StorageOperation> {
        let inner = self.inner.lock().unwrap();
        inner.operations.clone()
    }

    /// Get the total count of store operations.
    pub fn store_count(&self) -> usize {
        self.store_count.load(Ordering::SeqCst)
    }

    /// Get the total count of get operations.
    pub fn get_count(&self) -> usize {
        self.get_count.load(Ordering::SeqCst)
    }

    /// Get total operation count.
    pub fn operation_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.operations.len()
    }

    /// Get stored state directly (bypassing normal get logic).
    pub fn get_stored_state(&self, key: &ContractKey) -> Option<WrappedState> {
        let inner = self.inner.lock().unwrap();
        inner.states.get(key).cloned()
    }

    /// Get stored params directly (bypassing normal get logic).
    pub fn get_stored_params(&self, key: &ContractKey) -> Option<Parameters<'static>> {
        let inner = self.inner.lock().unwrap();
        inner.params.get(key).cloned()
    }

    /// Get all stored keys.
    pub fn stored_keys(&self) -> Vec<ContractKey> {
        let inner = self.inner.lock().unwrap();
        inner.states.keys().copied().collect()
    }

    /// Clear all stored state (useful between tests).
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.states.clear();
        inner.params.clear();
        inner.operations.clear();
        self.store_count.store(0, Ordering::SeqCst);
        self.get_count.store(0, Ordering::SeqCst);
    }

    /// Pre-populate with state (useful for setting up test scenarios).
    pub fn seed_state(&self, key: ContractKey, state: WrappedState) {
        let mut inner = self.inner.lock().unwrap();
        inner.states.insert(key, state);
    }

    /// Pre-populate with params (useful for setting up test scenarios).
    pub fn seed_params(&self, key: ContractKey, params: Parameters<'static>) {
        let mut inner = self.inner.lock().unwrap();
        inner.params.insert(key, params);
    }

    /// Check if a specific key should fail.
    fn should_fail_for_key(inner: &MockStateStorageInner, key: &ContractKey) -> bool {
        inner.failure_config.fail_for_keys.contains(key)
    }
}

impl StateStorage for MockStateStorage {
    type Error = MockStorageError;

    async fn store(&self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        self.store_count.fetch_add(1, Ordering::SeqCst);

        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(StorageOperation::Store { key });

        // Check failure conditions
        if Self::should_fail_for_key(&inner, &key) {
            return Err(MockStorageError::InjectedFailure(format!(
                "store failed for key {}",
                key
            )));
        }

        if inner.failure_config.fail_next_stores > 0 {
            inner.failure_config.fail_next_stores -= 1;
            return Err(MockStorageError::InjectedFailure(
                "store operation failed (injected)".to_string(),
            ));
        }

        inner.states.insert(key, state);
        Ok(())
    }

    async fn store_params(
        &self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(StorageOperation::StoreParams { key });

        // Check failure conditions
        if Self::should_fail_for_key(&inner, &key) {
            return Err(MockStorageError::InjectedFailure(format!(
                "store_params failed for key {}",
                key
            )));
        }

        if inner.failure_config.fail_next_store_params > 0 {
            inner.failure_config.fail_next_store_params -= 1;
            return Err(MockStorageError::InjectedFailure(
                "store_params operation failed (injected)".to_string(),
            ));
        }

        inner.params.insert(key, params);
        Ok(())
    }

    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        self.get_count.fetch_add(1, Ordering::SeqCst);

        let mut inner = self.inner.lock().unwrap();
        inner.operations.push(StorageOperation::Get { key: *key });

        // Check failure conditions
        if Self::should_fail_for_key(&inner, key) {
            return Err(MockStorageError::InjectedFailure(format!(
                "get failed for key {}",
                key
            )));
        }

        if inner.failure_config.fail_next_gets > 0 {
            inner.failure_config.fail_next_gets -= 1;
            return Err(MockStorageError::InjectedFailure(
                "get operation failed (injected)".to_string(),
            ));
        }

        Ok(inner.states.get(key).cloned())
    }

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .operations
            .push(StorageOperation::GetParams { key: *key });

        // Check failure conditions
        if Self::should_fail_for_key(&inner, key) {
            return Err(MockStorageError::InjectedFailure(format!(
                "get_params failed for key {}",
                key
            )));
        }

        if inner.failure_config.fail_next_get_params > 0 {
            inner.failure_config.fail_next_get_params -= 1;
            return Err(MockStorageError::InjectedFailure(
                "get_params operation failed (injected)".to_string(),
            ));
        }

        Ok(inner.params.get(key).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_key() -> ContractKey {
        let code = ContractCode::from(vec![1, 2, 3, 4]);
        let params = Parameters::from(vec![5, 6, 7, 8]);
        ContractKey::from_params_and_code(&params, &code)
    }

    fn make_test_state(data: &[u8]) -> WrappedState {
        WrappedState::new(data.to_vec())
    }

    #[tokio::test]
    async fn test_basic_store_and_get() {
        let storage = MockStateStorage::new();
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);

        storage.store(key, state.clone()).await.unwrap();
        let retrieved = storage.get(&key).await.unwrap();

        assert_eq!(retrieved, Some(state));
        assert_eq!(storage.store_count(), 1);
        assert_eq!(storage.get_count(), 1);
    }

    #[tokio::test]
    async fn test_params_store_and_get() {
        let storage = MockStateStorage::new();
        let key = make_test_key();
        let params = Parameters::from(vec![10, 20, 30]);

        storage.store_params(key, params.clone()).await.unwrap();
        let retrieved = storage.get_params(&key).await.unwrap();

        assert_eq!(retrieved, Some(params));
    }

    #[tokio::test]
    async fn test_get_nonexistent_returns_none() {
        let storage = MockStateStorage::new();
        let key = make_test_key();

        let result = storage.get(&key).await.unwrap();
        assert!(result.is_none());

        let params_result = storage.get_params(&key).await.unwrap();
        assert!(params_result.is_none());
    }

    #[tokio::test]
    async fn test_fail_next_stores() {
        let storage = MockStateStorage::new();
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);

        storage.fail_next_stores(2);

        // First two should fail
        assert!(storage.store(key, state.clone()).await.is_err());
        assert!(storage.store(key, state.clone()).await.is_err());

        // Third should succeed
        assert!(storage.store(key, state).await.is_ok());
    }

    #[tokio::test]
    async fn test_fail_next_gets() {
        let storage = MockStateStorage::new();
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);

        storage.store(key, state).await.unwrap();
        storage.fail_next_gets(1);

        // First get should fail
        assert!(storage.get(&key).await.is_err());

        // Second get should succeed
        assert!(storage.get(&key).await.is_ok());
    }

    #[tokio::test]
    async fn test_fail_for_specific_key() {
        let storage = MockStateStorage::new();
        let key1 = make_test_key();
        let key2 = {
            let code = ContractCode::from(vec![5, 6, 7, 8]);
            let params = Parameters::from(vec![9, 10, 11, 12]);
            ContractKey::from_params_and_code(&params, &code)
        };

        storage.fail_for_key(key1);

        let state = make_test_state(&[1, 2, 3]);

        // key1 should fail
        assert!(storage.store(key1, state.clone()).await.is_err());

        // key2 should succeed
        assert!(storage.store(key2, state).await.is_ok());
    }

    #[tokio::test]
    async fn test_operation_tracking() {
        let storage = MockStateStorage::new();
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);
        let params = Parameters::from(vec![10, 20, 30]);

        storage.store(key, state).await.unwrap();
        storage.store_params(key, params).await.unwrap();
        storage.get(&key).await.unwrap();
        storage.get_params(&key).await.unwrap();

        let ops = storage.operations();
        assert_eq!(ops.len(), 4);
        assert!(matches!(ops[0], StorageOperation::Store { .. }));
        assert!(matches!(ops[1], StorageOperation::StoreParams { .. }));
        assert!(matches!(ops[2], StorageOperation::Get { .. }));
        assert!(matches!(ops[3], StorageOperation::GetParams { .. }));
    }

    #[tokio::test]
    async fn test_seed_state() {
        let storage = MockStateStorage::new();
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);

        storage.seed_state(key, state.clone());

        // Should be retrievable without store operation
        let retrieved = storage.get(&key).await.unwrap();
        assert_eq!(retrieved, Some(state));

        // Store count should still be 0 (seeding doesn't count)
        assert_eq!(storage.store_count(), 0);
    }

    #[tokio::test]
    async fn test_clear() {
        let storage = MockStateStorage::new();
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);

        storage.store(key, state).await.unwrap();
        assert_eq!(storage.store_count(), 1);
        assert!(storage.get_stored_state(&key).is_some());

        storage.clear();

        assert_eq!(storage.store_count(), 0);
        assert!(storage.get_stored_state(&key).is_none());
        assert!(storage.operations().is_empty());
    }

    #[tokio::test]
    async fn test_stored_keys() {
        let storage = MockStateStorage::new();

        let key1 = make_test_key();
        let key2 = {
            let code = ContractCode::from(vec![5, 6, 7, 8]);
            let params = Parameters::from(vec![9, 10, 11, 12]);
            ContractKey::from_params_and_code(&params, &code)
        };

        storage.store(key1, make_test_state(&[1])).await.unwrap();
        storage.store(key2, make_test_state(&[2])).await.unwrap();

        let keys = storage.stored_keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
    }
}
