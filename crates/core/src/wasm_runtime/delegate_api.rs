//! Delegate API versioning and async context definitions.
//!
//! # Versioning
//!
//! The delegate API has evolved through two major versions:
//!
//! ## V1 (Current — request/response pattern)
//!
//! Delegates implement a synchronous `process()` function. To perform async
//! operations like fetching contract state, delegates must:
//! 1. Return an `OutboundDelegateMsg::GetContractRequest` from `process()`
//! 2. Encode their continuation state in `DelegateContext`
//! 3. Wait for the runtime to call `process()` again with a `GetContractResponse`
//! 4. Decode context and resume logic
//!
//! This round-trip pattern works but is cumbersome. Each async operation requires
//! managing serialization/deserialization of intermediate state and handling
//! multiple message types.
//!
//! ## V2 (New — async host functions for contract access)
//!
//! Delegates still implement `process()`, but the `DelegateCtx` gains new
//! **async host functions** for contract access, registered via wasmer's
//! `Function::new_typed_async` (requires `experimental-async` feature):
//!
//! ```text
//! ctx.get_contract_state(contract_id)  → Option<Vec<u8>>
//! ```
//!
//! From the WASM delegate's perspective, these calls appear synchronous — the
//! delegate simply calls the function and gets the result back immediately.
//! Behind the scenes, the host functions are registered as async and the
//! `process()` call uses `TypedFunction::call_async` with a `StoreAsync`.
//!
//! Currently the host function implementations are synchronous internally
//! (direct ReDb reads), but because they're registered as async, the
//! infrastructure is ready for truly async operations (network fetches,
//! PUT operations, subscriptions) that will genuinely yield in the future.
//!
//! ### Example: V1 vs V2
//!
//! **V1 (request/response):**
//! ```text
//! fn process(ctx, params, attested, msg) -> Vec<OutboundDelegateMsg> {
//!     match msg {
//!         ApplicationMessage(app_msg) => {
//!             // Can't get contract state inline — must return a request
//!             let state = DelegateState { pending_contract: contract_id, app };
//!             let context = DelegateContext::new(serialize(&state));
//!             vec![GetContractRequest { contract_id, context, processed: false }]
//!         }
//!         GetContractResponse(resp) => {
//!             // Resume: decode context, use state
//!             let state: DelegateState = deserialize(resp.context);
//!             let contract_state = resp.state;
//!             // ... finally do the real work ...
//!             vec![ApplicationMessage { payload, processed: true }]
//!         }
//!     }
//! }
//! ```
//!
//! **V2 (host function):**
//! ```text
//! fn process(ctx, params, attested, msg) -> Vec<OutboundDelegateMsg> {
//!     match msg {
//!         ApplicationMessage(app_msg) => {
//!             // Get contract state inline — no round-trip!
//!             let contract_state = ctx.get_contract_state(contract_id);
//!             // ... do the real work immediately ...
//!             vec![ApplicationMessage { payload, processed: true }]
//!         }
//!     }
//! }
//! ```
//!
//! ### Detection
//!
//! The runtime detects V2 delegates by checking whether they import the
//! `freenet_delegate_contracts` namespace. V1 delegates that don't use
//! contract host functions continue to work unchanged.

use std::fmt;

/// Delegate API version.
///
/// Used by the runtime to select the correct execution path and
/// determine which host functions are available to a delegate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(dead_code)] // Public API — used by consumers for version-based dispatch
pub enum DelegateApiVersion {
    /// V1: Request/response pattern for contract access.
    ///
    /// Delegates emit `GetContractRequest` / `PutContractRequest` outbound
    /// messages and receive responses via `GetContractResponse` /
    /// `PutContractResponse` inbound messages. State must be manually
    /// encoded in `DelegateContext` across round-trips.
    V1,

    /// V2: Async host function-based contract access.
    ///
    /// Delegates call `ctx.get_contract_state()` directly during `process()`.
    /// The runtime uses `TypedFunction::call_async` with a `StoreAsync` so
    /// async host functions can yield. Currently the contract access functions
    /// resolve synchronously (ReDb reads), but the infrastructure supports
    /// future async operations (network, subscriptions).
    /// No round-trip, no manual context encoding.
    V2,
}

#[allow(dead_code)] // Public API — version query methods
impl DelegateApiVersion {
    /// Returns true if this version supports direct contract state access
    /// via host functions (no request/response round-trip needed).
    pub fn has_contract_host_functions(&self) -> bool {
        matches!(self, DelegateApiVersion::V2)
    }
}

impl fmt::Display for DelegateApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DelegateApiVersion::V1 => write!(f, "v1"),
            DelegateApiVersion::V2 => write!(f, "v2"),
        }
    }
}

/// Error codes for contract state host functions.
///
/// These extend the existing error code scheme in `native_api::error_codes`.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_display() {
        assert_eq!(DelegateApiVersion::V1.to_string(), "v1");
        assert_eq!(DelegateApiVersion::V2.to_string(), "v2");
    }

    #[test]
    fn test_v1_no_contract_host_functions() {
        assert!(!DelegateApiVersion::V1.has_contract_host_functions());
    }

    #[test]
    fn test_v2_has_contract_host_functions() {
        assert!(DelegateApiVersion::V2.has_contract_host_functions());
    }

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
        secret_store: SecretsStore,
        db: Storage,
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
            // DelegateStore not stored since we only need contract_store + secret_store
            drop(delegate_store);
            let secret_store =
                SecretsStore::new(secrets_dir, Default::default(), db.clone()).unwrap();

            Self {
                _temp_dir: temp_dir,
                contract_store,
                secret_store,
                db,
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
            let delegate_key = DelegateKey::new([0u8; 32], CodeHash::new([0u8; 32]));
            DelegateCallEnv::new(
                vec![],
                &mut self.secret_store,
                &self.contract_store,
                Some(self.db.clone()),
                delegate_key,
            )
        }
    }

    /// V2 delegate can read contract state synchronously.
    #[tokio::test]
    async fn test_env_get_contract_state_found() {
        let mut env_holder = TestEnv::new().await;
        let state_data = vec![100, 200, 255];
        let contract_id = env_holder.store_contract(50, &state_data).await;

        let env = unsafe { env_holder.make_env() };
        let result = env.get_contract_state_sync(&contract_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(state_data));
    }

    /// V2 delegate gets None for a contract that isn't stored locally.
    #[tokio::test]
    async fn test_env_get_contract_state_not_found() {
        let mut env_holder = TestEnv::new().await;

        let env = unsafe { env_holder.make_env() };
        let missing_id = ContractInstanceId::new([77u8; 32]);
        let result = env.get_contract_state_sync(&missing_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    /// V2 delegate can read empty state.
    #[tokio::test]
    async fn test_env_get_contract_state_empty() {
        let mut env_holder = TestEnv::new().await;
        let contract_id = env_holder.store_contract(60, &[]).await;

        let env = unsafe { env_holder.make_env() };
        let result = env.get_contract_state_sync(&contract_id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(vec![]));
    }

    /// V2 delegate can read multiple different contracts.
    #[tokio::test]
    async fn test_env_get_multiple_contracts() {
        let mut env_holder = TestEnv::new().await;
        let id1 = env_holder.store_contract(10, &[1, 1, 1]).await;
        let id2 = env_holder.store_contract(20, &[2, 2, 2]).await;
        let id3 = env_holder.store_contract(30, &[3, 3, 3]).await;

        let env = unsafe { env_holder.make_env() };

        assert_eq!(
            env.get_contract_state_sync(&id1).unwrap(),
            Some(vec![1, 1, 1])
        );
        assert_eq!(
            env.get_contract_state_sync(&id2).unwrap(),
            Some(vec![2, 2, 2])
        );
        assert_eq!(
            env.get_contract_state_sync(&id3).unwrap(),
            Some(vec![3, 3, 3])
        );
    }

    /// V2 delegate gets an error if state store isn't configured.
    #[tokio::test]
    async fn test_env_get_contract_state_no_store() {
        let mut env_holder = TestEnv::new().await;

        let delegate_key = DelegateKey::new([0u8; 32], CodeHash::new([0u8; 32]));
        let env = unsafe {
            DelegateCallEnv::new(
                vec![],
                &mut env_holder.secret_store,
                &env_holder.contract_store,
                None, // No state store
                delegate_key,
            )
        };

        let result = env.get_contract_state_sync(&ContractInstanceId::new([1u8; 32]));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not configured"));
    }

    /// V2 delegate can read large contract state (1 MB).
    #[tokio::test]
    async fn test_env_get_large_contract_state() {
        let mut env_holder = TestEnv::new().await;
        let large_state: Vec<u8> = (0..1_000_000u32).map(|i| (i % 256) as u8).collect();
        let contract_id = env_holder.store_contract(70, &large_state).await;

        let env = unsafe { env_holder.make_env() };
        let result = env.get_contract_state_sync(&contract_id).unwrap().unwrap();
        assert_eq!(result.len(), 1_000_000);
        assert_eq!(result, large_state);
    }

    // ============ Wasmer async host function integration tests ============

    /// Verify that a WASM module with async host functions can be called
    /// via `call_async` with a `StoreAsync`.
    ///
    /// This tests the core mechanism: `Function::new_typed_async` +
    /// `Store::into_async()` + `TypedFunction::call_async()`.
    #[test]
    fn test_wasmer_async_host_function_roundtrip() {
        use wasmer::{imports, Function, Instance, Module, Store, TypedFunction};

        // Simple WAT module: calls an async host function and returns the result + 1
        let wat = r#"
        (module
          (import "host" "async_get_value" (func $get_value (result i32)))
          (func (export "compute") (result i32)
            call $get_value
            i32.const 1
            i32.add))
        "#;

        let mut store = Store::default();
        let module = Module::new(&store, wat).unwrap();

        // Register an async host function that returns 41
        let async_get_value = Function::new_typed_async(&mut store, || async move { 41_i32 });

        let import_object = imports! {
            "host" => {
                "async_get_value" => async_get_value,
            }
        };

        let instance = Instance::new(&mut store, &module, &import_object).unwrap();
        let compute: TypedFunction<(), i32> = instance
            .exports
            .get_typed_function(&store, "compute")
            .unwrap();

        // Convert Store to StoreAsync and call via call_async
        let store_async = store.into_async();
        let result = futures::executor::block_on(compute.call_async(&store_async));
        assert_eq!(result.unwrap(), 42, "async_get_value(41) + 1 should be 42");
    }

    /// Verify that sync and async host functions can coexist in the same module.
    #[test]
    fn test_wasmer_mixed_sync_async_host_functions() {
        use wasmer::{imports, Function, Instance, Module, Store, TypedFunction};

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

        let mut store = Store::default();
        let module = Module::new(&store, wat).unwrap();

        // Sync host function
        let sync_add = Function::new_typed(&mut store, |a: i32, b: i32| a + b);

        // Async host function
        let async_mul =
            Function::new_typed_async(&mut store, |a: i32, b: i32| async move { a * b });

        let import_object = imports! {
            "host" => {
                "sync_add" => sync_add,
                "async_mul" => async_mul,
            }
        };

        let instance = Instance::new(&mut store, &module, &import_object).unwrap();
        let compute: TypedFunction<(i32, i32), i32> = instance
            .exports
            .get_typed_function(&store, "compute")
            .unwrap();

        // Must use call_async since we have async imports
        let store_async = store.into_async();
        let result = futures::executor::block_on(compute.call_async(&store_async, 3, 4));
        // sync_add(3, 4) = 7, async_mul(3, 4) = 12, total = 19
        assert_eq!(result.unwrap(), 19);
    }

    /// Verify that `StoreAsync::into_store()` round-trips correctly.
    ///
    /// Note: Once async host functions are registered, ALL calls must use
    /// `call_async` — sync `call()` will trap with `YieldOutsideAsyncContext`.
    /// The `into_store()` round-trip is useful for operations that need
    /// `&mut Store` (e.g., creating new instances, growing memory).
    #[test]
    fn test_wasmer_store_async_roundtrip() {
        use wasmer::{imports, Function, Instance, Module, Store, TypedFunction};

        let wat = r#"
        (module
          (import "host" "async_inc" (func $inc (param i32) (result i32)))
          (func (export "call_inc") (param i32) (result i32)
            local.get 0
            call $inc))
        "#;

        let mut store = Store::default();
        let module = Module::new(&store, wat).unwrap();

        let async_inc = Function::new_typed_async(&mut store, |x: i32| async move { x + 1 });

        let import_object = imports! {
            "host" => {
                "async_inc" => async_inc,
            }
        };

        let instance = Instance::new(&mut store, &module, &import_object).unwrap();
        let call_inc: TypedFunction<i32, i32> = instance
            .exports
            .get_typed_function(&store, "call_inc")
            .unwrap();

        // First async call
        let store_async = store.into_async();
        let result1 = futures::executor::block_on(call_inc.call_async(&store_async, 10));
        assert_eq!(result1.unwrap(), 11);

        // Convert back to Store (needed for operations requiring &mut Store)
        let store = store_async
            .into_store()
            .expect("should convert back to Store");

        // Convert to async again and make another call
        let store_async = store.into_async();
        let result2 = futures::executor::block_on(call_inc.call_async(&store_async, 20));
        assert_eq!(result2.unwrap(), 21);

        // One more round-trip to verify stability
        let store = store_async
            .into_store()
            .expect("second round-trip should work");
        let store_async = store.into_async();
        let result3 = futures::executor::block_on(call_inc.call_async(&store_async, 30));
        assert_eq!(result3.unwrap(), 31);
    }
}
