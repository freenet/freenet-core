//! Error codes, and tests, for the delegate contract-state and
//! delegate-management host functions.
//!
//! A delegate has exactly one contract-state host function pair,
//! `__frnt__delegate__get_contract_state` / `..._len` (see
//! `native_api::DelegateCallEnv::local_contract_state`). It reports the state
//! THIS NODE already holds and never reaches the network. For anything else a
//! delegate emits an outbound message (`GetContractRequest`,
//! `PutContractRequest`, `UpdateContractRequest`, `SubscribeContractRequest`),
//! which the contract-handling loop serves through the executor's normal path.
//!
//! This module used to define a `DelegateApiVersion` enum separating "V1"
//! (message-based) from "V2" (host-function) delegates. That was never an API
//! version: freenet-stdlib's `DelegateWasmAPIVersion` has one variant, `V1`.
//! The split only chose between two call paths that had become identical, and
//! it went away with the contract write host functions in #5637.

/// Error codes for contract state host functions.
///
/// These extend the existing error code scheme in `native_api::error_codes`.
///
/// # Error Code Ranges
///
/// - `0`: Success (contract state read succeeded)
/// - `-1`: Not in process context
/// - `-4`: Invalid parameter (e.g., wrong instance ID length)
/// - `-6`: Output buffer too small for the state data
/// - `-7`: Contract not found in local store
/// - `-8`: Internal state store error
/// - `-9`: Memory bounds violation (WASM module passed invalid pointer)
///
/// # Memory Bounds Violations (`ERR_MEMORY_BOUNDS = -9`)
///
/// This error is returned when a WASM module attempts to access memory outside
/// its allocated linear memory region via contract state host function calls.
/// See `native_api::error_codes` documentation for details on validation logic.
#[allow(dead_code)] // Public API — error codes for host function implementations
pub mod contract_error_codes {
    /// Contract state read succeeded.
    pub const SUCCESS: i32 = 0;
    /// Called outside of a `process()` context.
    pub const ERR_NOT_IN_PROCESS: i32 = -1;
    /// Contract not found in local store.
    pub const ERR_CONTRACT_NOT_FOUND: i32 = -7;
    /// Output buffer too small for the state data.
    pub const ERR_BUFFER_TOO_SMALL: i32 = -6;
    /// Invalid parameter (e.g., wrong instance ID length).
    pub const ERR_INVALID_PARAM: i32 = -4;
    /// Internal state store error.
    pub const ERR_STORE_ERROR: i32 = -8;
    /// Memory bounds violation (pointer out of range). Returned when a WASM module
    /// attempts to access memory outside its allocated linear memory region via
    /// host function calls.
    pub const ERR_MEMORY_BOUNDS: i32 = -9;
    // -10 was ERR_CONTRACT_CODE_NOT_REGISTERED, returned only by the delegate
    // write and subscribe host functions removed in #5637. Do not reuse it: a
    // delegate built against the old ABI may still branch on it.
}

/// Error codes for delegate management host functions (create_delegate, etc.).
///
/// Uses the -20..-29 range to avoid collisions with existing error codes.
#[allow(dead_code)] // Public API — error codes for host function implementations
pub mod delegate_mgmt_error_codes {
    /// Delegate creation succeeded.
    pub const SUCCESS: i32 = 0;
    /// Called outside of a `process()` context.
    pub const ERR_NOT_IN_PROCESS: i32 = -1;
    /// Invalid parameter (e.g., negative length).
    pub const ERR_INVALID_PARAM: i32 = -4;
    /// Memory bounds violation (pointer out of range).
    pub const ERR_MEMORY_BOUNDS: i32 = -9;
    /// Delegate creation depth limit exceeded.
    pub const ERR_DEPTH_EXCEEDED: i32 = -20;
    /// Per-call delegate creation limit exceeded.
    pub const ERR_CREATIONS_EXCEEDED: i32 = -21;
    /// Per-node delegate creation limit exceeded (lifetime cap).
    pub const ERR_NODE_LIMIT_EXCEEDED: i32 = -22;
    /// Invalid WASM bytes (e.g., exceeds size limit).
    pub const ERR_INVALID_WASM: i32 = -23;
    /// Delegate store or secret store operation failed.
    pub const ERR_STORE_FAILED: i32 = -24;
}

#[cfg(test)]
mod tests {
    use crate::wasm_runtime::native_api::DelegateEnvError;

    // ============ ReDb synchronous state access tests ============

    use crate::contract::storages::Storage;
    use crate::util::tests::get_temp_dir;
    use crate::wasm_runtime::StateStorage;
    use freenet_stdlib::prelude::*;

    fn make_contract_key(seed: u8) -> (ContractKey, ContractInstanceId, CodeHash) {
        let code = ContractCode::from(vec![seed, seed + 1, seed + 2]);
        let params = Parameters::from(vec![seed + 10, seed + 11]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let id = *key.id();
        let code_hash = *key.code_hash();
        (key, id, code_hash)
    }

    /// Verify ReDb::get_state_sync returns the same data as the async get path.
    #[tokio::test]
    async fn test_redb_get_state_sync_matches_async() {
        let temp_dir = get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.unwrap();

        let (key, _, _) = make_contract_key(1);
        let state_data = vec![10, 20, 30, 40, 50];
        let state = WrappedState::new(state_data.clone());

        // Store via async path
        db.store(key, state).await.unwrap();

        // Read via sync path
        let sync_result = db.get_state_sync(&key).unwrap();
        assert!(sync_result.is_some(), "sync get should find stored state");
        assert_eq!(
            sync_result.unwrap().as_ref(),
            &state_data,
            "sync result should match stored data"
        );

        // Read via async path for comparison
        let async_result = db.get(&key).await.unwrap();
        assert!(async_result.is_some());
        assert_eq!(async_result.unwrap().as_ref(), &state_data);
    }

    /// Verify get_state_sync returns None for non-existent contracts.
    #[tokio::test]
    async fn test_redb_get_state_sync_missing() {
        let temp_dir = get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.unwrap();

        let (key, _, _) = make_contract_key(99);
        let result = db.get_state_sync(&key).unwrap();
        assert!(result.is_none(), "should return None for missing contract");
    }

    /// Verify get_state_sync handles empty state correctly.
    #[tokio::test]
    async fn test_redb_get_state_sync_empty_state() {
        let temp_dir = get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.unwrap();

        let (key, _, _) = make_contract_key(2);
        let state = WrappedState::new(vec![]);

        db.store(key, state).await.unwrap();

        let result = db.get_state_sync(&key).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().size(), 0, "empty state should have size 0");
    }

    /// Verify get_state_sync reads the latest state after updates.
    #[tokio::test]
    async fn test_redb_get_state_sync_after_update() {
        let temp_dir = get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.unwrap();

        let (key, _, _) = make_contract_key(3);

        // Store initial state
        db.store(key, WrappedState::new(vec![1, 1, 1]))
            .await
            .unwrap();

        // Overwrite with new state
        db.store(key, WrappedState::new(vec![9, 9, 9]))
            .await
            .unwrap();

        // Sync read should return the latest
        let result = db.get_state_sync(&key).unwrap().unwrap();
        assert_eq!(result.as_ref(), &[9, 9, 9]);
    }

    // ============ DelegateCallEnv contract state access tests ============

    use super::super::contract_store::ContractStore;
    use super::super::native_api::DelegateCallEnv;
    use super::super::secrets_store::SecretsStore;

    /// Helper to create a DelegateCallEnv wired to real stores.
    struct TestEnv {
        _temp_dir: tempfile::TempDir,
        contract_store: ContractStore,
        delegate_store: super::super::delegate_store::DelegateStore,
        secret_store: SecretsStore,
        db: Storage,
        /// This env's own per-node delegate state, standing in for what a real
        /// node's `Runtime` owns. Private to this `TestEnv`, so a test
        /// asserting on either one cannot observe another test's delegate
        /// creations — the coupling that made
        /// `test_create_delegate_non_attested_still_counts_toward_node_limit`
        /// flaky under a parallel suite run (#4813).
        created_delegates_count: super::super::native_api::SharedDelegateCounter,
        inherited_origins: super::super::native_api::SharedInheritedOrigins,
    }

    impl TestEnv {
        async fn new() -> Self {
            let temp_dir = get_temp_dir();
            let db = Storage::new(temp_dir.path()).await.unwrap();

            let contracts_dir = temp_dir.path().join("contracts");
            let delegates_dir = temp_dir.path().join("delegates");
            let secrets_dir = temp_dir.path().join("secrets");

            let contract_store = ContractStore::new(contracts_dir, 10_000_000, db.clone()).unwrap();
            let delegate_store =
                super::super::delegate_store::DelegateStore::new(delegates_dir, 10_000, db.clone())
                    .unwrap();
            let secret_store =
                SecretsStore::new(secrets_dir, Default::default(), db.clone()).unwrap();

            Self {
                _temp_dir: temp_dir,
                contract_store,
                delegate_store,
                secret_store,
                db,
                created_delegates_count: super::super::native_api::new_delegate_counter(),
                inherited_origins: super::super::native_api::new_inherited_origins(),
            }
        }

        /// Store a contract (code + state) so the env can find it.
        async fn store_contract(&mut self, seed: u8, state_data: &[u8]) -> ContractInstanceId {
            let code = ContractCode::from(vec![seed, seed + 1, seed + 2]);
            let params = Parameters::from(vec![seed + 10, seed + 11]);
            let key = ContractKey::from_params_and_code(&params, &code);
            let id = *key.id();

            // Register in contract store's in-memory index so code_hash_from_id works
            self.contract_store.ensure_key_indexed(&key).unwrap();

            // Store state in ReDb
            self.db
                .store(key, WrappedState::new(state_data.to_vec()))
                .await
                .unwrap();

            id
        }

        /// Create a DelegateCallEnv with access to contract stores.
        ///
        /// # Safety
        /// Caller must ensure the returned env does not outlive `self`.
        unsafe fn make_env(&mut self) -> DelegateCallEnv {
            // SAFETY: forwarded to make_env_with_depth
            unsafe { self.make_env_with_depth(0) }
        }

        /// Create a DelegateCallEnv with a specific creation depth.
        ///
        /// # Safety
        /// Caller must ensure the returned env does not outlive `self`.
        unsafe fn make_env_with_depth(&mut self, depth: u32) -> DelegateCallEnv {
            let delegate_key = DelegateKey::new([0u8; 32], CodeHash::new([0u8; 32]));
            // SAFETY: The caller guarantees the returned env does not outlive `self`,
            // which keeps the borrowed `secret_store` and `contract_store` alive.
            unsafe {
                DelegateCallEnv::new(
                    vec![],
                    &mut self.secret_store,
                    &self.contract_store,
                    Some(self.db.clone()),
                    delegate_key,
                    &mut self.delegate_store,
                    depth,
                    vec![],
                    None,
                    self.created_delegates_count.clone(),
                    self.inherited_origins.clone(),
                )
            }
        }

        /// Create a DelegateCallEnv with attested contracts for testing attestation inheritance.
        ///
        /// # Safety
        /// Caller must ensure the returned env does not outlive `self`.
        unsafe fn make_env_with_attestations(
            &mut self,
            attestations: Vec<ContractInstanceId>,
        ) -> DelegateCallEnv {
            let delegate_key = DelegateKey::new([1u8; 32], CodeHash::new([1u8; 32]));
            // SAFETY: The caller guarantees the returned env does not outlive `self`,
            // which keeps the borrowed `secret_store` and `contract_store` alive.
            unsafe {
                DelegateCallEnv::new(
                    vec![],
                    &mut self.secret_store,
                    &self.contract_store,
                    Some(self.db.clone()),
                    delegate_key,
                    &mut self.delegate_store,
                    0,
                    attestations,
                    None,
                    self.created_delegates_count.clone(),
                    self.inherited_origins.clone(),
                )
            }
        }
    }

    /// `local_contract_state` returns the state this node holds.
    #[tokio::test]
    async fn test_env_local_contract_state_found() {
        let mut env_holder = TestEnv::new().await;
        let state_data = vec![100, 200, 255];
        let contract_id = env_holder.store_contract(50, &state_data).await;

        // SAFETY: `env_holder` is alive for the duration of this test, ensuring
        // the returned references in `DelegateCallEnv` are valid.
        let env = unsafe { env_holder.make_env() };
        let result = env.local_contract_state(&contract_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(state_data));
    }

    /// `local_contract_state` is `None` for a contract this node does not hold.
    #[tokio::test]
    async fn test_env_local_contract_state_not_found() {
        let mut env_holder = TestEnv::new().await;

        // SAFETY: `env_holder` is alive for the duration of this test, ensuring
        // the returned references in `DelegateCallEnv` are valid.
        let env = unsafe { env_holder.make_env() };
        let missing_id = ContractInstanceId::new([77u8; 32]);
        let result = env.local_contract_state(&missing_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    /// `local_contract_state` can return an empty state.
    #[tokio::test]
    async fn test_env_local_contract_state_empty() {
        let mut env_holder = TestEnv::new().await;
        let contract_id = env_holder.store_contract(60, &[]).await;

        // SAFETY: `env_holder` is alive for the duration of this test, ensuring
        // the returned references in `DelegateCallEnv` are valid.
        let env = unsafe { env_holder.make_env() };
        let result = env.local_contract_state(&contract_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(vec![]));
    }

    /// `local_contract_state` keeps contracts apart.
    #[tokio::test]
    async fn test_env_local_contract_state_multiple_contracts() {
        let mut env_holder = TestEnv::new().await;
        let id1 = env_holder.store_contract(10, &[1, 1, 1]).await;
        let id2 = env_holder.store_contract(20, &[2, 2, 2]).await;
        let id3 = env_holder.store_contract(30, &[3, 3, 3]).await;

        // SAFETY: `env_holder` is alive for the duration of this test, ensuring
        // the returned references in `DelegateCallEnv` are valid.
        let env = unsafe { env_holder.make_env() };

        assert_eq!(env.local_contract_state(&id1).unwrap(), Some(vec![1, 1, 1]));
        assert_eq!(env.local_contract_state(&id2).unwrap(), Some(vec![2, 2, 2]));
        assert_eq!(env.local_contract_state(&id3).unwrap(), Some(vec![3, 3, 3]));
    }

    /// `local_contract_state` errors when no state store is configured.
    #[tokio::test]
    async fn test_env_local_contract_state_no_store() {
        let mut env_holder = TestEnv::new().await;

        let delegate_key = DelegateKey::new([0u8; 32], CodeHash::new([0u8; 32]));
        // SAFETY: `env_holder` is alive for the duration of this test, ensuring
        // the raw pointers to `secret_store` and `contract_store` remain valid.
        let env = unsafe {
            DelegateCallEnv::new(
                vec![],
                &mut env_holder.secret_store,
                &env_holder.contract_store,
                None, // No state store
                delegate_key,
                &mut env_holder.delegate_store,
                0,
                vec![],
                None,
                env_holder.created_delegates_count.clone(),
                env_holder.inherited_origins.clone(),
            )
        };

        let result = env.local_contract_state(&ContractInstanceId::new([1u8; 32]));
        assert!(matches!(result, Err(DelegateEnvError::StoreNotConfigured)));
    }

    /// `local_contract_state` can return a large (1 MB) state.
    #[tokio::test]
    async fn test_env_local_contract_state_large() {
        let mut env_holder = TestEnv::new().await;
        let large_state: Vec<u8> = (0..1_000_000u32).map(|i| (i % 256) as u8).collect();
        let contract_id = env_holder.store_contract(70, &large_state).await;

        // SAFETY: `env_holder` is alive for the duration of this test, ensuring
        // the returned references in `DelegateCallEnv` are valid.
        let env = unsafe { env_holder.make_env() };
        let result = env.local_contract_state(&contract_id).unwrap().unwrap();
        assert_eq!(result.len(), 1_000_000);
        assert_eq!(result, large_state);
    }

    // ============ ReDb store_state_sync tests ============

    /// Verify store_state_sync writes and get_state_sync reads back correctly.
    #[tokio::test]
    async fn test_redb_store_state_sync_basic() {
        let temp_dir = get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.unwrap();

        let (key, _, _) = make_contract_key(40);
        let state_data = vec![10, 20, 30, 40, 50];

        // Store via sync path
        db.store_state_sync(&key, WrappedState::new(state_data.clone()))
            .unwrap();

        // Read via sync path
        let result = db.get_state_sync(&key).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), &state_data);
    }

    /// Verify store_state_sync can overwrite existing state.
    #[tokio::test]
    async fn test_redb_store_state_sync_overwrite() {
        let temp_dir = get_temp_dir();
        let db = Storage::new(temp_dir.path()).await.unwrap();

        let (key, _, _) = make_contract_key(41);

        db.store_state_sync(&key, WrappedState::new(vec![1, 1, 1]))
            .unwrap();
        db.store_state_sync(&key, WrappedState::new(vec![9, 9, 9]))
            .unwrap();

        let result = db.get_state_sync(&key).unwrap().unwrap();
        assert_eq!(result.as_ref(), &[9, 9, 9]);
    }

    // ============ Wasmtime async host function integration tests ============

    /// Verify that a WASM module with async host functions can be called
    /// via wasmtime's async mechanism.
    ///
    /// This tests the core wasmtime pattern: `Linker::func_wrap` +
    /// `TypedFunc::call` for synchronous-style host functions.
    #[test]
    #[cfg(feature = "wasmtime-backend")]
    fn test_wasmtime_async_host_function_roundtrip() {
        use wasmtime::*;

        let wat = r#"
        (module
          (import "host" "async_get_value" (func $get_value (result i32)))
          (func (export "compute") (result i32)
            call $get_value
            i32.const 1
            i32.add))
        "#;

        let engine = Engine::default();
        let module = Module::new(&engine, wat).unwrap();
        let mut store = Store::new(&engine, ());
        let mut linker = Linker::new(&engine);

        linker
            .func_wrap("host", "async_get_value", || -> i32 { 41 })
            .unwrap();

        let instance = linker.instantiate(&mut store, &module).unwrap();
        let compute = instance
            .get_typed_func::<(), i32>(&mut store, "compute")
            .unwrap();

        let result = compute.call(&mut store, ()).unwrap();
        assert_eq!(result, 42, "async_get_value(41) + 1 should be 42");
    }

    /// Verify that sync and async host functions can coexist in the same module
    /// in wasmtime.
    #[test]
    #[cfg(feature = "wasmtime-backend")]
    fn test_wasmtime_mixed_sync_async_host_functions() {
        use wasmtime::*;

        let wat = r#"
        (module
          (import "host" "sync_add" (func $sync_add (param i32 i32) (result i32)))
          (import "host" "async_mul" (func $async_mul (param i32 i32) (result i32)))
          (func (export "compute") (param i32 i32) (result i32)
            ;; sync_add(a, b) + async_mul(a, b)
            local.get 0
            local.get 1
            call $sync_add
            local.get 0
            local.get 1
            call $async_mul
            i32.add))
        "#;

        let engine = Engine::default();
        let module = Module::new(&engine, wat).unwrap();
        let mut store = Store::new(&engine, ());
        let mut linker = Linker::new(&engine);

        linker
            .func_wrap("host", "sync_add", |a: i32, b: i32| -> i32 { a + b })
            .unwrap();
        linker
            .func_wrap("host", "async_mul", |a: i32, b: i32| -> i32 { a * b })
            .unwrap();

        let instance = linker.instantiate(&mut store, &module).unwrap();
        let compute = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "compute")
            .unwrap();

        let result = compute.call(&mut store, (3, 4)).unwrap();
        // sync_add(3, 4) = 7, async_mul(3, 4) = 12, total = 19
        assert_eq!(result, 19);
    }

    /// Verify that wasmtime stores can be reused across multiple calls.
    ///
    /// Wasmtime stores are reusable by default — no special async conversion
    /// is needed. This test verifies that pattern works correctly.
    #[test]
    #[cfg(feature = "wasmtime-backend")]
    fn test_wasmtime_store_reuse() {
        use wasmtime::*;

        let wat = r#"
        (module
          (import "host" "inc" (func $inc (param i32) (result i32)))
          (func (export "call_inc") (param i32) (result i32)
            local.get 0
            call $inc))
        "#;

        let engine = Engine::default();
        let module = Module::new(&engine, wat).unwrap();
        let mut store = Store::new(&engine, ());
        let mut linker = Linker::new(&engine);

        linker
            .func_wrap("host", "inc", |x: i32| -> i32 { x + 1 })
            .unwrap();

        let instance = linker.instantiate(&mut store, &module).unwrap();
        let call_inc = instance
            .get_typed_func::<i32, i32>(&mut store, "call_inc")
            .unwrap();

        // Multiple calls reusing the same store
        let result1 = call_inc.call(&mut store, 10).unwrap();
        assert_eq!(result1, 11);

        let result2 = call_inc.call(&mut store, 20).unwrap();
        assert_eq!(result2, 21);

        let result3 = call_inc.call(&mut store, 30).unwrap();
        assert_eq!(result3, 31);
    }

    // ============ Delegate creation resource limit tests ============

    use super::super::native_api::DelegateCreateError;
    use crate::contract::{MAX_DELEGATE_CREATION_DEPTH, MAX_DELEGATE_CREATIONS_PER_CALL};

    /// Minimal raw WASM bytes for delegate creation tests.
    /// The host function wraps raw WASM into a versioned DelegateContainer internally.
    fn minimal_delegate_wasm() -> Vec<u8> {
        b"\0asm\x01\x00\x00\x00".to_vec() // valid empty WASM module
    }

    /// create_delegate_sync rejects creation when depth limit is exceeded.
    #[tokio::test]
    async fn test_create_delegate_depth_exceeded() {
        let mut env_holder = TestEnv::new().await;

        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env_with_depth(MAX_DELEGATE_CREATION_DEPTH) };
        let wasm = minimal_delegate_wasm();
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        let result = env.create_delegate_sync(&wasm, &[], cipher, nonce);
        assert!(
            matches!(result, Err(DelegateCreateError::DepthExceeded)),
            "should reject at depth {}: {:?}",
            MAX_DELEGATE_CREATION_DEPTH,
            result
        );
    }

    /// create_delegate_sync allows creation at depth just below the limit.
    #[tokio::test]
    async fn test_create_delegate_depth_just_under_limit() {
        let mut env_holder = TestEnv::new().await;

        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env_with_depth(MAX_DELEGATE_CREATION_DEPTH - 1) };
        let wasm = minimal_delegate_wasm();
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        let result = env.create_delegate_sync(&wasm, &[], cipher, nonce);
        assert!(
            result.is_ok(),
            "should allow at depth {}: {:?}",
            MAX_DELEGATE_CREATION_DEPTH - 1,
            result
        );
    }

    /// create_delegate_sync rejects creation when per-call limit is exceeded.
    #[tokio::test]
    async fn test_create_delegate_per_call_limit_exceeded() {
        let mut env_holder = TestEnv::new().await;

        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env() };
        let wasm = minimal_delegate_wasm();
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        // Create up to the limit
        for i in 0..MAX_DELEGATE_CREATIONS_PER_CALL {
            let result = env.create_delegate_sync(&wasm, &[i as u8], cipher, nonce);
            assert!(result.is_ok(), "creation {i} should succeed: {:?}", result);
        }

        // One more should fail
        let result = env.create_delegate_sync(&wasm, &[255], cipher, nonce);
        assert!(
            matches!(result, Err(DelegateCreateError::CreationsExceeded)),
            "should reject at creation count {}: {:?}",
            MAX_DELEGATE_CREATIONS_PER_CALL,
            result
        );
    }

    /// create_delegate_sync accepts arbitrary bytes at creation time.
    /// Superseded: WASM validation moved from creation to execution time.
    /// Replaced test_create_delegate_invalid_wasm which expected InvalidWasm error.
    #[tokio::test]
    async fn test_create_delegate_accepts_any_bytes_at_creation() {
        let mut env_holder = TestEnv::new().await;

        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env() };
        let arbitrary_bytes = vec![0xFF, 0xFF, 0xFF]; // not valid WASM
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        let result = env.create_delegate_sync(&arbitrary_bytes, &[], cipher, nonce);
        assert!(
            result.is_ok(),
            "creation should accept any bytes (validation deferred to execution): {:?}",
            result
        );
    }

    /// create_delegate_sync rejects WASM bytes exceeding the size limit.
    #[tokio::test]
    async fn test_create_delegate_rejects_oversized_wasm() {
        let mut env_holder = TestEnv::new().await;

        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env() };
        let oversized = vec![0u8; DelegateCallEnv::MAX_WASM_CODE_SIZE + 1];
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        let result = env.create_delegate_sync(&oversized, &[], cipher, nonce);
        assert!(
            matches!(result, Err(DelegateCreateError::InvalidWasm(_))),
            "should reject oversized WASM: {:?}",
            result
        );
    }

    /// create_delegate_sync tracks per-call count correctly via Cell.
    #[tokio::test]
    async fn test_create_delegate_counter_tracks_correctly() {
        let mut env_holder = TestEnv::new().await;

        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env() };
        let wasm = minimal_delegate_wasm();
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        assert_eq!(env.creations_this_call.get(), 0);

        env.create_delegate_sync(&wasm, &[1], cipher, nonce)
            .unwrap();
        assert_eq!(env.creations_this_call.get(), 1);

        env.create_delegate_sync(&wasm, &[2], cipher, nonce)
            .unwrap();
        assert_eq!(env.creations_this_call.get(), 2);
    }

    /// Child delegate inherits parent's attested contracts.
    #[tokio::test]
    async fn test_create_delegate_inherits_attestations() {
        let mut env_holder = TestEnv::new().await;
        let origins = env_holder.inherited_origins.clone();

        let contract_id = ContractInstanceId::new([42u8; 32]);
        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env_with_attestations(vec![contract_id]) };
        let wasm = minimal_delegate_wasm();
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        let child_key = env
            .create_delegate_sync(&wasm, &[1], cipher, nonce)
            .unwrap();

        // Verify the child inherited the parent's attestation
        let inherited = origins.get(&child_key);
        assert!(
            inherited.is_some(),
            "child should have inherited attestations"
        );
        assert_eq!(inherited.unwrap().value().origins, vec![contract_id]);

        // No cleanup: the map is this env's own and dies with `env_holder`.
        // It used to be a process-global that this test had to hand back,
        // and the window before it did was the #4813 flake.
    }

    /// Child created by non-attested parent gets no attestation entry, but
    /// still counts toward the per-node limit.
    ///
    /// Both halves read this env's own state, so neither can be moved by a
    /// concurrent test (#4813). Note the params (`&[1]`) match those in
    /// `test_create_delegate_inherits_attestations`, so both derive the SAME
    /// child_key — when the attestation map was a process-global, that sibling's
    /// entry was visible here under that shared key and failed the `is_none()`
    /// assertion below. The keys still collide; the maps no longer do.
    #[tokio::test]
    async fn test_create_delegate_non_attested_still_counts_toward_node_limit() {
        use std::sync::atomic::Ordering;

        let mut env_holder = TestEnv::new().await;
        let counter = env_holder.created_delegates_count.clone();
        let origins = env_holder.inherited_origins.clone();
        assert_eq!(counter.load(Ordering::Relaxed), 0, "fresh env starts at 0");

        // SAFETY: env_holder is alive for the duration of this test
        let env = unsafe { env_holder.make_env() }; // no attestations
        let wasm = minimal_delegate_wasm();
        let cipher = [0u8; 32];
        let nonce = [0u8; 24];

        let child_key = env
            .create_delegate_sync(&wasm, &[1], cipher, nonce)
            .unwrap();

        // Should NOT be in inherited attestations (parent had none)
        assert!(
            origins.get(&child_key).is_none(),
            "non-attested parent should not create attestation entry"
        );

        // But SHOULD have counted toward the node limit. An exact count is
        // assertable now that the counter is this env's own; the old
        // `after > before` was a workaround for a count anything could move.
        assert_eq!(
            counter.load(Ordering::Relaxed),
            1,
            "creation by a non-attested parent must still count toward the node limit"
        );

        // No cleanup: both live in `env_holder` and die with it.
    }

    /// A node's delegate state is per node, not per process (#4813).
    ///
    /// This is the property that makes the two tests above deterministic. Both
    /// create a delegate from the same WASM with params `&[1]`, so both derive
    /// the SAME child_key — one asserting an attestation entry EXISTS under it,
    /// the other asserting it does NOT. While the map was a `static`, those two
    /// assertions raced on one entry, which is the flake in #4813. Nothing stops
    /// two tests from colliding on a key; what makes them independent is that
    /// each node's map is its own.
    ///
    /// It pins the scoping itself, which is a test-isolation property, not a
    /// production one. Both the count and the attestation map are per-*node*
    /// state that a `static` made process-wide. No production configuration is
    /// affected: a node is a process (one `RuntimePool` per node), and the
    /// simulation runner cannot create delegates at all (`MockWasmRuntime`'s
    /// `execute_delegate_request` returns "delegates not supported"). So the
    /// only context where per-node and per-process ever diverged is this test
    /// suite — which is exactly the flake in #4813.
    #[tokio::test]
    async fn test_delegate_node_state_is_per_node_not_per_process() {
        use std::sync::atomic::Ordering;

        // Two envs stand in for two nodes in one process.
        let mut node_a = TestEnv::new().await;
        let mut node_b = TestEnv::new().await;

        let a_counter = node_a.created_delegates_count.clone();
        let b_counter = node_b.created_delegates_count.clone();
        let b_origins = node_b.inherited_origins.clone();

        let contract_id = ContractInstanceId::new([42u8; 32]);
        // SAFETY: both holders are alive for the duration of this test
        let env_a = unsafe { node_a.make_env_with_attestations(vec![contract_id]) };
        // SAFETY: node_b is alive for the duration of this test
        let env_b = unsafe { node_b.make_env() }; // no attestations
        let child_key = env_a
            .create_delegate_sync(&minimal_delegate_wasm(), &[1], [0u8; 32], [0u8; 24])
            .unwrap();

        // Node B creates from the same code and params, so it derives the SAME
        // key. The collision is real and unavoidable; what keeps the two envs
        // independent is that each owns its map. Isolation comes from the map,
        // not from the keys differing.
        let b_child_key = env_b
            .create_delegate_sync(&minimal_delegate_wasm(), &[1], [0u8; 32], [0u8; 24])
            .unwrap();
        assert_eq!(
            child_key, b_child_key,
            "identical code+params must still collide on the key"
        );

        assert_eq!(
            a_counter.load(Ordering::Relaxed),
            1,
            "the creating node counts its own delegate"
        );
        assert_eq!(
            b_counter.load(Ordering::Relaxed),
            1,
            "node B counts only its own creation, not node A's"
        );
        assert!(
            b_origins.get(&child_key).is_none(),
            "node A's attestation must not be visible to node B under the shared \
             key — this map decides which contract a delegate message is \
             attributed to, hence what that delegate may access"
        );
    }

    /// Releasing a slot saturates at zero rather than wrapping.
    ///
    /// A delegate registered directly by an app was never counted, so an
    /// unregister can legitimately arrive with the count already at zero.
    #[test]
    fn test_release_created_delegate_slot_saturates_at_zero() {
        use super::super::native_api::{new_delegate_counter, release_created_delegate_slot};
        use std::sync::atomic::Ordering;

        let counter = new_delegate_counter();
        assert!(
            !release_created_delegate_slot(&counter),
            "releasing an already-empty count releases nothing"
        );
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "releasing an already-empty count must stay at 0, not wrap to usize::MAX"
        );

        counter.fetch_add(1, Ordering::Relaxed);
        assert!(
            release_created_delegate_slot(&counter),
            "the one occupied slot is released"
        );
        assert!(
            !release_created_delegate_slot(&counter),
            "a second release finds nothing left"
        );
        assert_eq!(counter.load(Ordering::Relaxed), 0, "one release per slot");
    }

    /// Concurrent releases at count == 1 cannot wrap the count.
    ///
    /// Pool executors share one node's counter, so two unregisters can land
    /// together. A `load`-then-`fetch_sub` pair (what this replaced) would let
    /// both observe `1` and both subtract, wrapping to `usize::MAX` —
    /// permanently above `MAX_CREATED_DELEGATES_PER_NODE`, which would wedge the
    /// node into rejecting every later delegate creation.
    /// A barrier lines the racers up on the same slot, and the round repeats,
    /// because a wrap is a race and one round proves little. Verified to have
    /// teeth rather than assumed: against the `load`-then-`fetch_sub` this
    /// replaced it fails immediately on every run (6/6 at this round count),
    /// with more than one racer claiming the slot and the count wrapping past
    /// zero. 200 rounds is deliberate — it is the smallest count still verified
    /// to catch that, since this suite does not need 16k more OS threads.
    #[test]
    fn test_release_created_delegate_slot_concurrent_releases_do_not_wrap() {
        use super::super::native_api::{new_delegate_counter, release_created_delegate_slot};
        use std::sync::atomic::Ordering;
        use std::sync::{Arc, Barrier};

        const RACERS: usize = 8;

        for _ in 0..200 {
            let counter = new_delegate_counter();
            counter.store(1, Ordering::Relaxed);
            // Release the racers together, so they contend for the ONE occupied
            // slot rather than arriving spread out and finding it already empty.
            let barrier = Arc::new(Barrier::new(RACERS));

            let threads: Vec<_> = (0..RACERS)
                .map(|_| {
                    let counter = counter.clone();
                    let barrier = barrier.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        release_created_delegate_slot(&counter)
                    })
                })
                .collect();
            let released = threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .filter(|&released| released)
                .count();

            assert_eq!(
                counter.load(Ordering::Relaxed),
                0,
                "concurrent releases must saturate at 0, never wrap"
            );
            assert_eq!(
                released, 1,
                "exactly one racer may claim the single occupied slot"
            );
        }
    }

    // These tests check the eviction rule directly. They build a small map by
    // hand, set each entry's timestamps relative to `base`, then ask the pruner
    // "if the time were `now`, what gets dropped?". Times are built forward
    // (`base + offset`) because subtracting days from `Instant::now()` can
    // underflow on a freshly-booted CI machine. A local map keeps them from
    // interfering with other tests.

    /// Regression (#3492): an entry left unused for longer than the TTL is
    /// dropped, even though nobody called `UnregisterDelegate` — that is the
    /// leak, since children created via `create_delegate` are never unregistered.
    #[test]
    fn test_inherited_origins_idle_ttl_evicts() {
        use super::super::native_api::{
            DELEGATE_INHERITED_ORIGINS_TTL, InheritedOriginsEntry, prune_inherited_origins_at,
        };
        use dashmap::DashMap;
        use freenet_stdlib::prelude::CodeHash;

        let base = tokio::time::Instant::now();
        let map = DashMap::new();
        let child = DelegateKey::new([0x71; 32], CodeHash::new([0x71; 32]));
        map.insert(
            child.clone(),
            InheritedOriginsEntry {
                origins: vec![ContractInstanceId::new([0x72; 32])],
                created_at: base,
                last_access: base,
            },
        );

        // 1s past the idle TTL, nothing touched it.
        let now = base + DELEGATE_INHERITED_ORIGINS_TTL + std::time::Duration::from_secs(1);
        prune_inherited_origins_at(&map, now);

        assert!(
            !map.contains_key(&child),
            "an entry idle past the TTL must be pruned even without UnregisterDelegate"
        );
    }

    /// Even an entry refreshed right now (last_access = now) is dropped once it
    /// is older than the cap. This is what stops a constantly-used entry from
    /// living forever, as the AGENTS.md cleanup rule requires.
    #[test]
    fn test_inherited_origins_absolute_cap_overrides_refresh() {
        use super::super::native_api::{
            DELEGATE_INHERITED_ORIGINS_MAX_AGE, InheritedOriginsEntry, prune_inherited_origins_at,
        };
        use dashmap::DashMap;
        use freenet_stdlib::prelude::CodeHash;

        let base = tokio::time::Instant::now();
        let map = DashMap::new();
        let child = DelegateKey::new([0x73; 32], CodeHash::new([0x73; 32]));
        // now is 1s past the cap; last_access == now (as if just refreshed).
        let now = base + DELEGATE_INHERITED_ORIGINS_MAX_AGE + std::time::Duration::from_secs(1);
        map.insert(
            child.clone(),
            InheritedOriginsEntry {
                origins: vec![ContractInstanceId::new([0x74; 32])],
                created_at: base,
                last_access: now,
            },
        );

        prune_inherited_origins_at(&map, now);

        assert!(
            !map.contains_key(&child),
            "an entry older than MAX_AGE must be pruned despite a fresh last_access"
        );
    }

    /// A delivered message refreshes the entry, so one that would otherwise be
    /// dropped for being idle is kept. This is why an active child — even one
    /// only ever reached by other delegates — keeps its origin.
    #[test]
    fn test_touch_inherited_origin_rescues_idle_entry() {
        use super::super::native_api::{
            DELEGATE_INHERITED_ORIGINS_TTL, InheritedOriginsEntry, prune_inherited_origins_at,
            touch_inherited_origin_at,
        };
        use dashmap::DashMap;
        use freenet_stdlib::prelude::CodeHash;

        let base = tokio::time::Instant::now();
        let map = DashMap::new();
        let child = DelegateKey::new([0x77; 32], CodeHash::new([0x77; 32]));
        map.insert(
            child.clone(),
            InheritedOriginsEntry {
                origins: vec![ContractInstanceId::new([0x78; 32])],
                created_at: base,
                last_access: base,
            },
        );

        // A message is delivered at the idle horizon, then the sweep runs 1s
        // later: idle is now 1s (< TTL) and age is TTL+1s (< cap), so it stays.
        let delivery = base + DELEGATE_INHERITED_ORIGINS_TTL;
        touch_inherited_origin_at(&map, &child, delivery);
        let now = delivery + std::time::Duration::from_secs(1);
        prune_inherited_origins_at(&map, now);

        assert!(
            map.contains_key(&child),
            "a touched (recently-delivered) entry must survive the idle sweep"
        );
    }

    /// A brand-new entry is not dropped by a sweep (the opposite of the eviction
    /// tests).
    #[test]
    fn test_inherited_origins_fresh_entry_survives() {
        use super::super::native_api::{
            DELEGATE_INHERITED_ORIGINS_TTL, InheritedOriginsEntry, prune_inherited_origins_at,
        };
        use dashmap::DashMap;
        use freenet_stdlib::prelude::CodeHash;

        let base = tokio::time::Instant::now();
        let map = DashMap::new();
        let child = DelegateKey::new([0x75; 32], CodeHash::new([0x75; 32]));
        map.insert(
            child.clone(),
            InheritedOriginsEntry {
                origins: vec![ContractInstanceId::new([0x76; 32])],
                created_at: base,
                last_access: base,
            },
        );

        // Well within the idle window.
        let now = base + DELEGATE_INHERITED_ORIGINS_TTL / 2;
        prune_inherited_origins_at(&map, now);

        assert!(
            map.contains_key(&child),
            "a fresh entry must survive the prune sweep"
        );
    }

    /// Exactly at the TTL the entry is dropped; one second under, it is kept.
    /// Catches an accidental `<` -> `<=` change in the comparison.
    #[test]
    fn test_inherited_origins_idle_ttl_boundary() {
        use super::super::native_api::{
            DELEGATE_INHERITED_ORIGINS_TTL, InheritedOriginsEntry, prune_inherited_origins_at,
        };
        use dashmap::DashMap;
        use freenet_stdlib::prelude::CodeHash;

        let base = tokio::time::Instant::now();
        let child = DelegateKey::new([0x79; 32], CodeHash::new([0x79; 32]));
        let make = || InheritedOriginsEntry {
            origins: vec![ContractInstanceId::new([0x7a; 32])],
            created_at: base,
            last_access: base,
        };

        // idle == TTL -> evicted (strict `<`).
        let at = DashMap::new();
        at.insert(child.clone(), make());
        prune_inherited_origins_at(&at, base + DELEGATE_INHERITED_ORIGINS_TTL);
        assert!(
            !at.contains_key(&child),
            "idle == TTL must evict (strict <)"
        );

        // idle just under TTL -> retained.
        let under = DashMap::new();
        under.insert(child.clone(), make());
        prune_inherited_origins_at(
            &under,
            base + DELEGATE_INHERITED_ORIGINS_TTL - std::time::Duration::from_secs(1),
        );
        assert!(
            under.contains_key(&child),
            "idle just under TTL must retain"
        );
    }

    /// Runs the real `prune_expired_inherited_origins` / `touch_inherited_origin`
    /// (the functions production calls; the other tests use the `_at` variants
    /// with a hand-picked time). Checks that touching a delegate with no entry
    /// does not create one, and that a fresh entry is kept. Operates on its own
    /// map, so its keys are its own business — it used to need distinct keys to
    /// avoid disturbing tests sharing the global map.
    #[test]
    fn test_inherited_origin_wrappers_smoke() {
        use super::super::native_api::{
            InheritedOriginsEntry, new_inherited_origins, prune_expired_inherited_origins,
            touch_inherited_origin,
        };
        use freenet_stdlib::prelude::CodeHash;

        let origins = new_inherited_origins();
        let child = DelegateKey::new([0x7b; 32], CodeHash::new([0x7b; 32]));
        let absent = DelegateKey::new([0x7c; 32], CodeHash::new([0x7c; 32]));
        origins.insert(
            child.clone(),
            InheritedOriginsEntry::new(vec![ContractInstanceId::new([0x7d; 32])]),
        );

        // touch must NOT create an entry for a delegate with no inherited origin.
        touch_inherited_origin(&origins, &absent);
        assert!(
            !origins.contains_key(&absent),
            "touch must be a no-op (never insert) for a key with no entry"
        );

        // touch refreshes an existing entry; the sweep keeps it fresh.
        touch_inherited_origin(&origins, &child);
        prune_expired_inherited_origins(&origins);
        assert!(
            origins.contains_key(&child),
            "prune must keep a freshly-touched entry"
        );
    }
}
