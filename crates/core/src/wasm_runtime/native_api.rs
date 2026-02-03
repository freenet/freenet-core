//! Implementation of native API's exported and available in the WASM modules.

use dashmap::DashMap;
use freenet_stdlib::prelude::{DelegateKey, SecretsId};
use wasmer::{Function, Imports};

use std::sync::LazyLock;

use super::runtime::InstanceInfo;
use super::secrets_store::SecretsStore;

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

type InstanceId = i64;

/// State available to a delegate during a single `process()` call.
///
/// Created by the runtime before the WASM call and torn down after it returns.
/// Host functions access this through the global `DELEGATE_ENV` map.
pub(super) struct DelegateCallEnv {
    /// Mutable context bytes. The delegate reads/writes this via host functions.
    pub context: Vec<u8>,
    /// Raw pointer to the runtime's SecretsStore. Valid only during the
    /// synchronous `process()` call. The WASM Singlepass compiler executes
    /// on the calling thread, so the pointer cannot outlive the borrow.
    secret_store: *const SecretsStore,
    /// The delegate key, needed to scope secret access.
    pub delegate_key: DelegateKey,
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
    /// The caller must ensure that `secret_store` remains valid for the entire
    /// lifetime of this `DelegateCallEnv` (i.e., until it is removed from
    /// `DELEGATE_ENV`).
    pub unsafe fn new(
        context: Vec<u8>,
        secret_store: &SecretsStore,
        delegate_key: DelegateKey,
    ) -> Self {
        Self {
            context,
            secret_store: secret_store as *const SecretsStore,
            delegate_key,
        }
    }

    /// Access the secrets store. Only safe during the synchronous process() call.
    fn secret_store(&self) -> &SecretsStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model
        unsafe { &*self.secret_store }
    }
}

/// Helper to get the current instance ID from the thread-local.
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
    fn context_len() -> i32 {
        let id = current_instance_id();
        assert!(id != -1, "delegate context_len called outside process()");
        let env = DELEGATE_ENV
            .get(&id)
            .expect("delegate call env not set for instance");
        env.context.len() as i32
    }

    /// Reads the context bytes into the WASM linear memory at `ptr`.
    /// Returns the number of bytes written (min of context length and `len`).
    fn context_read(ptr: i64, len: i32) -> i32 {
        let id = current_instance_id();
        assert!(id != -1, "delegate context_read called outside process()");
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let env = DELEGATE_ENV
            .get(&id)
            .expect("delegate call env not set for instance");
        let to_copy = env.context.len().min(len as usize);
        let dst = compute_ptr::<u8>(ptr, info.start_ptr);
        unsafe {
            std::ptr::copy_nonoverlapping(env.context.as_ptr(), dst, to_copy);
        }
        to_copy as i32
    }

    /// Writes `len` bytes from the WASM linear memory at `ptr` into the context,
    /// replacing any existing content.
    fn context_write(ptr: i64, len: i32) {
        let id = current_instance_id();
        assert!(id != -1, "delegate context_write called outside process()");
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let mut env = DELEGATE_ENV
            .get_mut(&id)
            .expect("delegate call env not set for instance");
        let src = compute_ptr::<u8>(ptr, info.start_ptr);
        let bytes = unsafe { std::slice::from_raw_parts(src, len as usize) };
        env.context = bytes.to_vec();
    }
}

/// Host functions for synchronous delegate secret access.
///
/// These eliminate the GetSecretRequest/GetSecretResponse round-trip
/// by allowing delegates to read/write secrets directly during `process()`.
///
/// Like the context functions, these use the thread-local current instance ID.
pub(crate) mod delegate_secrets {
    use super::*;

    pub(crate) fn prepare_export(store: &mut wasmer::Store, imports: &mut Imports) {
        let get_secret = Function::new_typed(store, get_secret);
        let set_secret = Function::new_typed(store, set_secret);
        let has_secret = Function::new_typed(store, has_secret);
        imports.register_namespace(
            "freenet_delegate_secrets",
            [
                ("__frnt__delegate__get_secret".to_owned(), get_secret.into()),
                ("__frnt__delegate__set_secret".to_owned(), set_secret.into()),
                ("__frnt__delegate__has_secret".to_owned(), has_secret.into()),
            ],
        );
    }

    /// Get a secret by key. The key is read from WASM memory at `key_ptr` (length `key_len`).
    /// The secret value is written to WASM memory at `out_ptr` (max `out_len` bytes).
    /// Returns the number of bytes written, or -1 if the secret was not found.
    fn get_secret(key_ptr: i64, key_len: i32, out_ptr: i64, out_len: i32) -> i32 {
        let id = current_instance_id();
        assert!(id != -1, "delegate get_secret called outside process()");
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let env = DELEGATE_ENV
            .get(&id)
            .expect("delegate call env not set for instance");

        // Read the secret key from WASM memory
        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        // Look up the secret
        let store = env.secret_store();
        match store.get_secret(&env.delegate_key, &secret_id) {
            Ok(plaintext) => {
                let to_copy = plaintext.len().min(out_len as usize);
                let dst = compute_ptr::<u8>(out_ptr, info.start_ptr);
                unsafe {
                    std::ptr::copy_nonoverlapping(plaintext.as_ptr(), dst, to_copy);
                }
                to_copy as i32
            }
            Err(_) => -1,
        }
    }

    /// Store a secret. The key is at `key_ptr` (length `key_len`),
    /// the value is at `val_ptr` (length `val_len`).
    /// Returns 0 on success, -1 on error.
    fn set_secret(key_ptr: i64, key_len: i32, val_ptr: i64, val_len: i32) -> i32 {
        let id = current_instance_id();
        assert!(id != -1, "delegate set_secret called outside process()");
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let env = DELEGATE_ENV
            .get(&id)
            .expect("delegate call env not set for instance");

        // Read key and value from WASM memory
        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        let val_src = compute_ptr::<u8>(val_ptr, info.start_ptr);
        let value = unsafe { std::slice::from_raw_parts(val_src, val_len as usize) }.to_vec();

        // SAFETY: Single-threaded access during synchronous WASM call.
        // The Runtime holds &mut self when calling process(), so no other
        // code can access the SecretsStore.
        let store = unsafe { &mut *(env.secret_store as *mut SecretsStore) };
        match store.store_secret(&env.delegate_key, &secret_id, value) {
            Ok(()) => 0,
            Err(e) => {
                tracing::error!("delegate set_secret failed: {e}");
                -1
            }
        }
    }

    /// Check if a secret exists. Returns 1 if it exists, 0 if not.
    fn has_secret(key_ptr: i64, key_len: i32) -> i32 {
        let id = current_instance_id();
        assert!(id != -1, "delegate has_secret called outside process()");
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let env = DELEGATE_ENV
            .get(&id)
            .expect("delegate call env not set for instance");

        let key_src = compute_ptr::<u8>(key_ptr, info.start_ptr);
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        let store = env.secret_store();
        match store.get_secret(&env.delegate_key, &secret_id) {
            Ok(_) => 1,
            Err(_) => 0,
        }
    }
}
