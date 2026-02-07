//! Implementation of native API's exported and available in the WASM modules.

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey, DelegateKey, SecretsId};
use wasmer::{Function, Imports};

use std::sync::LazyLock;

use super::contract_store::ContractStore;
use super::runtime::InstanceInfo;
use super::secrets_store::SecretsStore;
use crate::contract::storages::Storage;

/// This is a map of starting addresses of the instance memory space.
///
/// A hackish way of having the information necessary to compute the address
/// at which bytes must be written when calling host functions from the WASM modules.
pub(super) static MEM_ADDR: LazyLock<DashMap<InstanceId, InstanceInfo>> =
    LazyLock::new(DashMap::default);

/// Per-instance delegate call environment.
///
/// The runtime populates this before calling the delegate's `process` function.
/// Host functions for context and secret access read/write through this.
/// After `process` returns, the runtime reads back the (possibly mutated) context
/// and removes the entry.
pub(super) static DELEGATE_ENV: LazyLock<DashMap<InstanceId, DelegateCallEnv>> =
    LazyLock::new(DashMap::default);

// Thread-local tracking of the currently-executing delegate instance ID.
// Set before a WASM process() call, cleared after. This allows the new
// context/secret host functions to find the right DelegateCallEnv without
// needing the delegate to pass an explicit ID (which requires stdlib changes).
thread_local! {
    pub(super) static CURRENT_DELEGATE_INSTANCE: std::cell::Cell<InstanceId> =
        const { std::cell::Cell::new(-1) };
}

pub(super) type InstanceId = i64;

/// Error codes returned by host functions.
/// Negative values indicate errors, non-negative values are success/data.
pub mod error_codes {
    /// Operation succeeded (or returned count/length).
    pub const SUCCESS: i32 = 0;
    /// Called outside of a process() context.
    pub const ERR_NOT_IN_PROCESS: i32 = -1;
    /// Secret not found.
    pub const ERR_SECRET_NOT_FOUND: i32 = -2;
    /// Storage operation failed.
    pub const ERR_STORAGE_FAILED: i32 = -3;
    /// Invalid parameter (e.g., negative length).
    pub const ERR_INVALID_PARAM: i32 = -4;
    /// Context too large (exceeds i32::MAX).
    pub const ERR_CONTEXT_TOO_LARGE: i32 = -5;
    /// Buffer too small to hold the secret (use get_secret_len first).
    pub const ERR_BUFFER_TOO_SMALL: i32 = -6;
}

/// State available to a delegate during a single `process()` call.
///
/// Created by the runtime before the WASM call and torn down after it returns.
/// Host functions access this through the global `DELEGATE_ENV` map.
pub(super) struct DelegateCallEnv {
    /// Mutable context bytes. The delegate reads/writes this via host functions.
    pub context: Vec<u8>,
    /// Raw mutable pointer to the runtime's SecretsStore. Valid only during the
    /// synchronous `process()` call. The WASM Singlepass compiler executes
    /// on the calling thread, so the pointer cannot outlive the borrow.
    /// Stored as *mut because set_secret and remove_secret need mutation.
    secret_store: *mut SecretsStore,
    /// The delegate key, needed to scope secret access.
    pub delegate_key: DelegateKey,
    /// Read-only pointer to the ContractStore for index lookups
    /// (ContractInstanceId → CodeHash). Valid only during synchronous process().
    contract_store: *const ContractStore,
    /// Clone of the state storage backend (ReDb). Used by V2 delegates to read
    /// contract state synchronously via host functions. ReDb is Arc<Database>
    /// internally, so cloning is cheap.
    state_store_db: Option<Storage>,
}

// SAFETY: DelegateCallEnv is only inserted into DELEGATE_ENV immediately before
// a synchronous WASM process() call and removed immediately after. The raw pointer
// to SecretsStore is valid for the entire duration because the Runtime (which owns
// SecretsStore) is alive and on the same call stack. Wasmer's Singlepass compiler
// executes WASM synchronously on the calling thread.
unsafe impl Send for DelegateCallEnv {}
unsafe impl Sync for DelegateCallEnv {}

impl DelegateCallEnv {
    /// Create a new call environment.
    ///
    /// # Safety
    /// The caller must ensure that `secret_store` and `contract_store` remain
    /// valid for the entire lifetime of this `DelegateCallEnv` (i.e., until it
    /// is removed from `DELEGATE_ENV`). The caller must also ensure exclusive
    /// access to the SecretsStore during this time (no other references exist).
    pub unsafe fn new(
        context: Vec<u8>,
        secret_store: &mut SecretsStore,
        contract_store: &ContractStore,
        state_store_db: Option<Storage>,
        delegate_key: DelegateKey,
    ) -> Self {
        Self {
            context,
            secret_store: secret_store as *mut SecretsStore,
            delegate_key,
            contract_store: contract_store as *const ContractStore,
            state_store_db,
        }
    }

    /// Access the secrets store immutably. Only safe during the synchronous process() call.
    fn secret_store(&self) -> &SecretsStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model
        unsafe { &*self.secret_store }
    }

    /// Access the secrets store mutably. Only safe during the synchronous process() call.
    ///
    /// # Safety invariants (why this lint is suppressed)
    /// This uses interior mutability through a raw pointer, which is safe because:
    /// 1. The Runtime holds `&mut self` when calling `process()`, ensuring exclusive access
    /// 2. WASM execution is synchronous on the calling thread (Singlepass compiler)
    /// 3. The `DelegateCallEnv` is created immediately before and destroyed immediately after
    ///    the WASM call, so no concurrent access is possible
    #[allow(clippy::mut_from_ref)]
    fn secret_store_mut(&self) -> &mut SecretsStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model.
        // The Runtime holds &mut self when calling process(), ensuring exclusive access.
        unsafe { &mut *self.secret_store }
    }

    /// Access the contract store for index lookups.
    /// Only safe during the synchronous process() call.
    fn contract_store(&self) -> &ContractStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model
        unsafe { &*self.contract_store }
    }

    /// Look up contract state by instance ID using the local ReDb store.
    ///
    /// Returns `Some(state_bytes)` if found, `None` if the contract is not stored locally.
    /// This is the synchronous fast path used by V2 delegates instead of the
    /// GetContractRequest/Response round-trip.
    ///
    /// Uses `ReDb::get_state_sync` — a purely synchronous ReDb read transaction
    /// with no async overhead.
    pub(super) fn get_contract_state_sync(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<Option<Vec<u8>>, String> {
        let Some(ref db) = self.state_store_db else {
            return Err("state store not available (V2 contract access not configured)".into());
        };

        // Step 1: Look up CodeHash from the contract index
        let code_hash = match self.contract_store().code_hash_from_id(instance_id) {
            Some(ch) => ch,
            None => return Ok(None),
        };

        // Step 2: Construct the full ContractKey
        let contract_key = ContractKey::from_id_and_code(*instance_id, code_hash);

        // Step 3: Read state directly from ReDb via synchronous read transaction.
        // No async wrapper, no futures::executor::block_on — just a plain read txn.
        match db.get_state_sync(&contract_key) {
            Ok(Some(wrapped_state)) => Ok(Some(wrapped_state.as_ref().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(format!("state store read error: {e}")),
        }
    }
}

/// Helper to get the current instance ID from the thread-local.
/// Returns -1 if not in a process() context.
fn current_instance_id() -> InstanceId {
    CURRENT_DELEGATE_INSTANCE.with(|c| c.get())
}

#[inline(always)]
fn compute_ptr<T>(ptr: i64, start_ptr: i64) -> *mut T {
    (start_ptr + ptr) as _
}

pub(crate) mod log {
    use super::*;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let utc_now = Function::new_typed(store, info);
        imports.register_namespace(
            "freenet_log",
            [("__frnt__logger__info".to_owned(), utc_now.into())],
        );
    }

    // TODO: this API right now is just a patch, ideally we want to impl a tracing subscriber
    // that can be used in wasm and that under the hood will just pass data to the host via
    // functions like this in a structured way
    fn info(id: i64, ptr: i64, len: i32) {
        if id == -1 {
            panic!("unset module id");
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let ptr = compute_ptr::<u8>(ptr, info.start_ptr);
        let msg =
            unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as _)) };
        tracing::info!(target: "contract", contract = %info.value().key(), "{msg}");
    }
}

pub(crate) mod rand {
    use ::rand::{rng, RngCore};

    use super::*;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let rand_bytes = Function::new_typed(store, rand_bytes);
        imports.register_namespace(
            "freenet_rand",
            [("__frnt__rand__rand_bytes".to_owned(), rand_bytes.into())],
        );
    }

    fn rand_bytes(id: i64, ptr: i64, len: u32) {
        if id == -1 {
            panic!("unset module id");
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let ptr = compute_ptr::<u8>(ptr, info.start_ptr);
        let slice = unsafe { &mut *std::ptr::slice_from_raw_parts_mut(ptr, len as usize) };
        let mut rng = rng();
        rng.fill_bytes(slice);
    }
}

pub(crate) mod time {
    use super::*;
    use chrono::{DateTime, Utc as UtcOriginal};

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let utc_now = Function::new_typed(store, utc_now);
        imports.register_namespace(
            "freenet_time",
            [("__frnt__time__utc_now".to_owned(), utc_now.into())],
        );
    }

    fn utc_now(id: i64, ptr: i64) {
        if id == -1 {
            panic!("unset module id");
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let now = UtcOriginal::now();
        let ptr = compute_ptr::<DateTime<UtcOriginal>>(ptr, info.start_ptr);
        unsafe {
            ptr.write(now);
        };
    }
}

/// Host functions for delegate mutable context access.
///
/// These allow a delegate to read/write a persistent context buffer
/// that the runtime maintains across `process()` invocations within
/// a single conversation (from initial app message to final response).
///
/// These functions use the thread-local `CURRENT_DELEGATE_INSTANCE` to find
/// the right environment, so delegates don't need to pass an instance ID.
/// The WASM module just calls these with pointer/length arguments.
///
/// ## Error Codes
/// - Returns non-negative values on success (usually byte counts)
/// - Returns `ERR_NOT_IN_PROCESS` (-1) if called outside process()
/// - Returns `ERR_INVALID_PARAM` (-4) for invalid parameters (negative length)
/// - Returns `ERR_CONTEXT_TOO_LARGE` (-5) if context exceeds i32::MAX bytes
pub(crate) mod delegate_context {
    use super::*;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let ctx_len = Function::new_typed(store, context_len);
        let ctx_read = Function::new_typed(store, context_read);
        let ctx_write = Function::new_typed(store, context_write);
        imports.register_namespace(
            "freenet_delegate_ctx",
            [
                ("__frnt__delegate__ctx_len".to_owned(), ctx_len.into()),
                ("__frnt__delegate__ctx_read".to_owned(), ctx_read.into()),
                ("__frnt__delegate__ctx_write".to_owned(), ctx_write.into()),
            ],
        );
    }

    /// Returns the current context length in bytes.
    ///
    /// ## Returns
    /// - Non-negative: context length in bytes
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_CONTEXT_TOO_LARGE`: context exceeds i32::MAX bytes
    fn context_len() -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate context_len called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let len = env.context.len();
        if len > i32::MAX as usize {
            return error_codes::ERR_CONTEXT_TOO_LARGE;
        }
        len as i32
    }

    /// Reads the context bytes into the WASM linear memory at `ptr`.
    ///
    /// ## Returns
    /// - Non-negative: number of bytes written (min of context length and `len`)
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative length)
    fn context_read(ptr: i64, len: i32) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate context_read called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        if len < 0 {
            tracing::warn!("delegate context_read called with negative length: {len}");
            return error_codes::ERR_INVALID_PARAM;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let to_copy = env.context.len().min(len as usize);
        if to_copy == 0 {
            return 0;
        }
        let dst = compute_ptr::<u8>(ptr, info.start_ptr);
        unsafe {
            std::ptr::copy_nonoverlapping(env.context.as_ptr(), dst, to_copy);
        }
        to_copy as i32
    }

    /// Writes `len` bytes from the WASM linear memory at `ptr` into the context,
    /// replacing any existing content.
    ///
    /// ## Returns
    /// - `SUCCESS` (0): write succeeded
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative length)
    fn context_write(ptr: i64, len: i32) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate context_write called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        if len < 0 {
            tracing::warn!("delegate context_write called with negative length: {len}");
            return error_codes::ERR_INVALID_PARAM;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let Some(mut env) = DELEGATE_ENV.get_mut(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        if len == 0 {
            env.context.clear();
            return error_codes::SUCCESS;
        }
        let src = compute_ptr::<u8>(ptr, info.start_ptr);
        let bytes = unsafe { std::slice::from_raw_parts(src, len as usize) };
        env.context = bytes.to_vec();
        error_codes::SUCCESS
    }
}

/// Host functions for synchronous delegate secret access.
///
/// These eliminate the GetSecretRequest/GetSecretResponse round-trip
/// by allowing delegates to read/write secrets directly during `process()`.
///
/// Like the context functions, these use the thread-local current instance ID.
///
/// ## Error Codes
/// - Returns non-negative values on success (usually byte counts)
/// - Returns `ERR_NOT_IN_PROCESS` (-1) if called outside process()
/// - Returns `ERR_SECRET_NOT_FOUND` (-2) if secret doesn't exist
/// - Returns `ERR_STORAGE_FAILED` (-3) if storage operation failed
/// - Returns `ERR_INVALID_PARAM` (-4) for invalid parameters (negative length)
/// - Returns `ERR_BUFFER_TOO_SMALL` (-6) if output buffer is too small
pub(crate) mod delegate_secrets {
    use super::*;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let get_secret = Function::new_typed(store, get_secret);
        let get_secret_len = Function::new_typed(store, get_secret_len);
        let set_secret = Function::new_typed(store, set_secret);
        let has_secret = Function::new_typed(store, has_secret);
        let remove_secret = Function::new_typed(store, remove_secret);
        imports.register_namespace(
            "freenet_delegate_secrets",
            [
                ("__frnt__delegate__get_secret".to_owned(), get_secret.into()),
                (
                    "__frnt__delegate__get_secret_len".to_owned(),
                    get_secret_len.into(),
                ),
                ("__frnt__delegate__set_secret".to_owned(), set_secret.into()),
                ("__frnt__delegate__has_secret".to_owned(), has_secret.into()),
                (
                    "__frnt__delegate__remove_secret".to_owned(),
                    remove_secret.into(),
                ),
            ],
        );
    }

    /// Get the length of a secret without retrieving its value.
    /// This allows the caller to allocate the right buffer size before calling get_secret.
    ///
    /// ## Returns
    /// - Non-negative: secret length in bytes
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_SECRET_NOT_FOUND`: secret doesn't exist
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative key length)
    fn get_secret_len(key_ptr: i64, key_len: i32) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate get_secret_len called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        if key_len < 0 {
            tracing::warn!("delegate get_secret_len called with negative key_len: {key_len}");
            return error_codes::ERR_INVALID_PARAM;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };

        // Read the secret key from WASM memory
        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        // Look up the secret
        match env.secret_store().get_secret(&env.delegate_key, &secret_id) {
            Ok(plaintext) => {
                let len = plaintext.len();
                if len > i32::MAX as usize {
                    // Secret is larger than i32::MAX, return max representable
                    i32::MAX
                } else {
                    len as i32
                }
            }
            Err(_) => error_codes::ERR_SECRET_NOT_FOUND,
        }
    }

    /// Get a secret by key. The key is read from WASM memory at `key_ptr` (length `key_len`).
    /// The secret value is written to WASM memory at `out_ptr` (max `out_len` bytes).
    ///
    /// ## Returns
    /// - Non-negative: number of bytes written
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_SECRET_NOT_FOUND`: secret doesn't exist
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative length)
    /// - `ERR_BUFFER_TOO_SMALL`: output buffer is too small (use get_secret_len first)
    fn get_secret(key_ptr: i64, key_len: i32, out_ptr: i64, out_len: i32) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate get_secret called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        if key_len < 0 || out_len < 0 {
            tracing::warn!(
                "delegate get_secret called with negative lengths: key_len={key_len}, out_len={out_len}"
            );
            return error_codes::ERR_INVALID_PARAM;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };

        // Read the secret key from WASM memory
        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        // Look up the secret
        match env.secret_store().get_secret(&env.delegate_key, &secret_id) {
            Ok(plaintext) => {
                let secret_len = plaintext.len();
                let out_len_usize = out_len as usize;

                // Check if buffer is large enough
                if secret_len > out_len_usize {
                    tracing::debug!(
                        "delegate get_secret buffer too small: need {secret_len}, have {out_len_usize}"
                    );
                    return error_codes::ERR_BUFFER_TOO_SMALL;
                }

                if secret_len == 0 {
                    return 0;
                }

                let dst = compute_ptr::<u8>(out_ptr, info.start_ptr);
                unsafe {
                    std::ptr::copy_nonoverlapping(plaintext.as_ptr(), dst, secret_len);
                }
                secret_len as i32
            }
            Err(_) => error_codes::ERR_SECRET_NOT_FOUND,
        }
    }

    /// Store a secret. The key is at `key_ptr` (length `key_len`),
    /// the value is at `val_ptr` (length `val_len`).
    ///
    /// ## Returns
    /// - `SUCCESS` (0): store succeeded
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_STORAGE_FAILED`: storage operation failed
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative length)
    fn set_secret(key_ptr: i64, key_len: i32, val_ptr: i64, val_len: i32) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate set_secret called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        if key_len < 0 || val_len < 0 {
            tracing::warn!(
                "delegate set_secret called with negative lengths: key_len={key_len}, val_len={val_len}"
            );
            return error_codes::ERR_INVALID_PARAM;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };

        // Read key and value from WASM memory
        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        let val_src = compute_ptr::<u8>(val_ptr, info.start_ptr);
        let value = unsafe { std::slice::from_raw_parts(val_src, val_len as usize) }.to_vec();

        match env
            .secret_store_mut()
            .store_secret(&env.delegate_key, &secret_id, value)
        {
            Ok(()) => error_codes::SUCCESS,
            Err(e) => {
                tracing::error!("delegate set_secret failed: {e}");
                error_codes::ERR_STORAGE_FAILED
            }
        }
    }

    /// Check if a secret exists.
    ///
    /// ## Returns
    /// - 1: secret exists
    /// - 0: secret doesn't exist
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative length)
    fn has_secret(key_ptr: i64, key_len: i32) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate has_secret called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        if key_len < 0 {
            tracing::warn!("delegate has_secret called with negative key_len: {key_len}");
            return error_codes::ERR_INVALID_PARAM;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };

        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        match env.secret_store().get_secret(&env.delegate_key, &secret_id) {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }

    /// Remove a secret by key.
    ///
    /// ## Returns
    /// - `SUCCESS` (0): remove succeeded
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_SECRET_NOT_FOUND`: secret doesn't exist
    /// - `ERR_STORAGE_FAILED`: storage operation failed
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative length)
    fn remove_secret(key_ptr: i64, key_len: i32) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate remove_secret called outside process()");
            return error_codes::ERR_NOT_IN_PROCESS;
        }
        if key_len < 0 {
            tracing::warn!("delegate remove_secret called with negative key_len: {key_len}");
            return error_codes::ERR_INVALID_PARAM;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return error_codes::ERR_NOT_IN_PROCESS;
        };

        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        match env
            .secret_store_mut()
            .remove_secret(&env.delegate_key, &secret_id)
        {
            Ok(()) => error_codes::SUCCESS,
            Err(e) => {
                tracing::debug!("delegate remove_secret failed: {e}");
                error_codes::ERR_SECRET_NOT_FOUND
            }
        }
    }
}

/// Host functions for synchronous contract state access (V2 delegate API).
///
/// These allow a V2 delegate to read contract state directly during `process()`,
/// eliminating the GetContractRequest/GetContractResponse round-trip.
///
/// The delegate passes a `ContractInstanceId` (32 bytes) and receives the
/// contract state bytes back in a pre-allocated output buffer.
///
/// ## Usage Pattern (from WASM)
///
/// 1. Call `get_contract_state_len(id_ptr, 32)` to get the state size
/// 2. Allocate a buffer of that size
/// 3. Call `get_contract_state(id_ptr, 32, out_ptr, out_len)` to read the state
///
/// ## Error Codes
/// - Non-negative values on success (byte counts)
/// - `ERR_NOT_IN_PROCESS` (-1): called outside process()
/// - `ERR_INVALID_PARAM` (-4): invalid parameter
/// - `ERR_BUFFER_TOO_SMALL` (-6): output buffer too small
/// - `ERR_CONTRACT_NOT_FOUND` (-7): contract not in local store
/// - `ERR_STORE_ERROR` (-8): internal storage error
pub(crate) mod delegate_contracts {
    use super::*;
    use crate::wasm_runtime::delegate_api::contract_error_codes;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let get_state = Function::new_typed(store, get_contract_state);
        let get_state_len = Function::new_typed(store, get_contract_state_len);
        imports.register_namespace(
            "freenet_delegate_contracts",
            [
                (
                    "__frnt__delegate__get_contract_state".to_owned(),
                    get_state.into(),
                ),
                (
                    "__frnt__delegate__get_contract_state_len".to_owned(),
                    get_state_len.into(),
                ),
            ],
        );
    }

    /// Get the size of a contract's state without retrieving it.
    ///
    /// The contract instance ID is read from WASM memory at `id_ptr` (must be 32 bytes).
    ///
    /// ## Returns
    /// - Non-negative: state size in bytes
    /// - `ERR_NOT_IN_PROCESS` (-1): called outside process()
    /// - `ERR_INVALID_PARAM` (-4): instance ID is not 32 bytes
    /// - `ERR_CONTRACT_NOT_FOUND` (-7): contract not in local store
    /// - `ERR_STORE_ERROR` (-8): internal storage error
    fn get_contract_state_len(id_ptr: i64, id_len: i32) -> i64 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate get_contract_state_len called outside process()");
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        }
        if id_len != 32 {
            tracing::warn!(
                "delegate get_contract_state_len: expected 32-byte instance ID, got {id_len}"
            );
            return contract_error_codes::ERR_INVALID_PARAM as i64;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };

        // Read the contract instance ID from WASM memory
        let id_src = compute_ptr::<u8>(id_ptr, info.start_ptr);
        let id_bytes: [u8; 32] = unsafe { std::slice::from_raw_parts(id_src, 32) }
            .try_into()
            .unwrap();
        let contract_id = ContractInstanceId::new(id_bytes);

        match env.get_contract_state_sync(&contract_id) {
            Ok(Some(state_bytes)) => {
                tracing::debug!(
                    contract = %contract_id,
                    size = state_bytes.len(),
                    "V2 delegate: get_contract_state_len succeeded"
                );
                state_bytes.len() as i64
            }
            Ok(None) => {
                tracing::debug!(
                    contract = %contract_id,
                    "V2 delegate: contract not found in local store"
                );
                contract_error_codes::ERR_CONTRACT_NOT_FOUND as i64
            }
            Err(e) => {
                tracing::error!(
                    contract = %contract_id,
                    error = %e,
                    "V2 delegate: state store error"
                );
                contract_error_codes::ERR_STORE_ERROR as i64
            }
        }
    }

    /// Read a contract's state into the WASM linear memory.
    ///
    /// The contract instance ID is read from `id_ptr` (32 bytes).
    /// State bytes are written to `out_ptr` (max `out_len` bytes).
    ///
    /// ## Returns
    /// - Non-negative: number of bytes written
    /// - `ERR_NOT_IN_PROCESS` (-1): called outside process()
    /// - `ERR_INVALID_PARAM` (-4): bad parameters
    /// - `ERR_BUFFER_TOO_SMALL` (-6): output buffer too small
    /// - `ERR_CONTRACT_NOT_FOUND` (-7): contract not in local store
    /// - `ERR_STORE_ERROR` (-8): internal storage error
    fn get_contract_state(id_ptr: i64, id_len: i32, out_ptr: i64, out_len: i64) -> i64 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate get_contract_state called outside process()");
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        }
        if id_len != 32 {
            tracing::warn!(
                "delegate get_contract_state: expected 32-byte instance ID, got {id_len}"
            );
            return contract_error_codes::ERR_INVALID_PARAM as i64;
        }
        if out_len < 0 {
            tracing::warn!("delegate get_contract_state: negative out_len={out_len}");
            return contract_error_codes::ERR_INVALID_PARAM as i64;
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };

        // Read the contract instance ID from WASM memory
        let id_src = compute_ptr::<u8>(id_ptr, info.start_ptr);
        let id_bytes: [u8; 32] = unsafe { std::slice::from_raw_parts(id_src, 32) }
            .try_into()
            .unwrap();
        let contract_id = ContractInstanceId::new(id_bytes);

        match env.get_contract_state_sync(&contract_id) {
            Ok(Some(state_bytes)) => {
                let state_len = state_bytes.len();
                let out_len_usize = out_len as usize;

                if state_len > out_len_usize {
                    tracing::debug!(
                        "delegate get_contract_state buffer too small: need {state_len}, have {out_len_usize}"
                    );
                    return contract_error_codes::ERR_BUFFER_TOO_SMALL as i64;
                }

                if state_len == 0 {
                    return 0;
                }

                let dst = compute_ptr::<u8>(out_ptr, info.start_ptr);
                unsafe {
                    std::ptr::copy_nonoverlapping(state_bytes.as_ptr(), dst, state_len);
                }

                tracing::debug!(
                    contract = %contract_id,
                    bytes_written = state_len,
                    "V2 delegate: get_contract_state succeeded"
                );
                state_len as i64
            }
            Ok(None) => {
                tracing::debug!(
                    contract = %contract_id,
                    "V2 delegate: contract not found"
                );
                contract_error_codes::ERR_CONTRACT_NOT_FOUND as i64
            }
            Err(e) => {
                tracing::error!(
                    contract = %contract_id,
                    error = %e,
                    "V2 delegate: state store error"
                );
                contract_error_codes::ERR_STORE_ERROR as i64
            }
        }
    }
}
