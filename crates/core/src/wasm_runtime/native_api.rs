//! Implementation of native API's exported and available in the WASM modules.

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey, DelegateKey, SecretsId};

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;

use super::contract_store::ContractStore;
use super::delegate_store::DelegateStore;
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

/// Global registry of delegate subscriptions to contracts.
///
/// When a V2 delegate calls `subscribe_contract()`, the (contract, delegate) pair is
/// recorded here. When `commit_state_update()` persists a new contract state, it checks
/// this registry and sends notifications to subscribed delegates.
pub(crate) static DELEGATE_SUBSCRIPTIONS: LazyLock<
    DashMap<ContractInstanceId, HashSet<DelegateKey>>,
> = LazyLock::new(DashMap::default);

/// Tracks message origins inherited by child delegates from their parent.
/// When a delegate with an origin contract creates a child, the child inherits
/// the same origin so it can interact with the same app context.
/// DelegateKey (child) → Vec<ContractInstanceId> (inherited origins)
pub(crate) static DELEGATE_INHERITED_ORIGINS: LazyLock<
    DashMap<DelegateKey, Vec<ContractInstanceId>>,
> = LazyLock::new(DashMap::default);

/// Global counter of all delegates created via the `create_delegate` host function.
/// Used to enforce `MAX_CREATED_DELEGATES_PER_NODE` regardless of attestation status.
/// Decremented on `UnregisterDelegate` to allow reuse of slots.
pub(crate) static CREATED_DELEGATES_COUNT: AtomicUsize = AtomicUsize::new(0);

// Thread-local tracking of the currently-executing delegate instance ID.
// Set before a WASM process() call, cleared after. This allows the new
// context/secret host functions to find the right DelegateCallEnv without
// needing the delegate to pass an explicit ID (which requires stdlib changes).
thread_local! {
    pub(super) static CURRENT_DELEGATE_INSTANCE: std::cell::Cell<InstanceId> =
        const { std::cell::Cell::new(-1) };
}

pub(super) type InstanceId = i64;

// ---------------------------------------------------------------------------
// Contract I/O: streaming refill buffers
// ---------------------------------------------------------------------------

/// Pending data for the streaming refill pattern.
/// The host writes an initial chunk into the WASM buffer; the remainder
/// is stored here and fed to the contract on demand via `fill_buffer_impl`.
pub(super) struct PendingContractData {
    pub data: Vec<u8>,
    pub cursor: usize,
}

/// Per-instance, per-buffer pending data.
/// Key: (instance_id, buffer_builder_ptr_offset_in_wasm).
pub(super) static CONTRACT_IO: LazyLock<DashMap<(InstanceId, i64), PendingContractData>> =
    LazyLock::new(DashMap::default);

/// Host-side fill callback. Resets buffer to position 0, copies next chunk.
///
/// Called by the contract's `StreamingBuffer::read` when the buffer is exhausted.
/// Returns the number of bytes written into the buffer, or 0 for EOF.
pub(super) fn fill_buffer_impl(instance_id: InstanceId, buf_ptr_offset: i64) -> u32 {
    use freenet_stdlib::memory::buf::{compute_ptr, BufferBuilder};
    use freenet_stdlib::memory::WasmLinearMem;

    let Some(mut pending) = CONTRACT_IO.get_mut(&(instance_id, buf_ptr_offset)) else {
        return 0; // No pending data — all data fit in the initial buffer
    };
    if pending.cursor >= pending.data.len() {
        return 0; // EOF
    }
    let Some(info) = MEM_ADDR.get(&instance_id) else {
        tracing::warn!(
            instance_id,
            "fill_buffer_impl: MEM_ADDR missing for instance — returning EOF (possible data truncation)"
        );
        return 0;
    };
    // SAFETY: `start_ptr` and `mem_size` come from the live wasmtime Caller's memory
    // export, refreshed by `refresh_mem_addr_from_caller` before this function is called.
    let linear_mem =
        unsafe { WasmLinearMem::new(info.start_ptr as *const u8, info.mem_size as u64) };

    // Get the BufferBuilder in WASM memory
    let builder_ptr = compute_ptr(buf_ptr_offset as *mut BufferBuilder, &linear_mem);
    // SAFETY: `builder_ptr` was computed from the WASM-side buffer offset and the
    // validated linear memory base. The buffer was allocated by `initiate_buffer`.
    let builder = unsafe { &mut *builder_ptr };
    let capacity = builder.capacity();

    // SAFETY: The read/write pointers and data buffer are within the WASM linear
    // memory region validated above. We reset them to 0 and write the next chunk.
    unsafe {
        // Reset read/write pointers to 0
        let read_ptr = compute_ptr(builder.last_read_ptr(), &linear_mem);
        let write_ptr = compute_ptr(builder.last_write_ptr(), &linear_mem);
        *read_ptr = 0;
        *write_ptr = 0;

        // Copy next chunk from pending data into the buffer
        let remaining = &pending.data[pending.cursor..];
        let chunk_size = remaining.len().min(capacity);
        let buf_data_ptr = compute_ptr(builder.start(), &linear_mem);
        std::ptr::copy_nonoverlapping(remaining.as_ptr(), buf_data_ptr, chunk_size);

        // Update write pointer to reflect bytes written
        let write_ptr = compute_ptr(builder.last_write_ptr(), &linear_mem);
        *write_ptr = chunk_size as u32;

        pending.cursor += chunk_size;
        chunk_size as u32
    }
}

/// Error codes returned by host functions.
///
/// Negative values indicate errors, non-negative values are success/data.
///
/// # Error Code Ranges
///
/// - `0`: Success (or returned count/length)
/// - `-1`: Not in process context (host function called outside delegate execution)
/// - `-2`: Secret not found
/// - `-3`: Storage operation failed
/// - `-4`: Invalid parameter (e.g., negative length)
/// - `-5`: Context too large (exceeds i32::MAX)
/// - `-6`: Buffer too small (use length query functions first)
/// - `-7`: Memory bounds violation (WASM module passed invalid pointer)
///
/// # Memory Bounds Violations (`ERR_MEMORY_BOUNDS = -7`)
///
/// This error is returned when a WASM module attempts to access memory outside
/// its allocated linear memory region via host function calls. This can occur when:
///
/// - A negative offset is passed (would access before WASM memory)
/// - Pointer arithmetic overflows (offset + size exceeds usize::MAX)
/// - Access range exceeds the instance's current allocated memory size
///
/// The validation uses the live memory size (refreshed from the wasmtime Caller
/// before each host call) rather than the theoretical maximum, making the bounds
/// check as tight as the actual allocation at call time.
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
    /// Memory bounds violation (pointer out of range). Returned when a WASM module
    /// attempts to access memory outside its allocated linear memory region via
    /// host function calls.
    pub const ERR_MEMORY_BOUNDS: i32 = -7;
}

/// State available to a delegate during a single `process()` call.
///
/// Created by the runtime before the WASM call and torn down after it returns.
/// Host functions access this through the global `DELEGATE_ENV` map.
pub(super) struct DelegateCallEnv {
    /// Mutable context bytes. The delegate reads/writes this via host functions.
    pub context: Vec<u8>,
    /// Interior-mutable pointer to the runtime's SecretsStore. Valid only during
    /// the synchronous `process()` call. Uses `UnsafeCell` to make the interior
    /// mutability explicit rather than hiding it behind `#[allow(clippy::mut_from_ref)]`.
    secret_store: std::cell::UnsafeCell<*mut SecretsStore>,
    /// The delegate key, needed to scope secret access.
    pub delegate_key: DelegateKey,
    /// Read-only pointer to the ContractStore for index lookups
    /// (ContractInstanceId → CodeHash). Valid only during synchronous process().
    contract_store: *const ContractStore,
    /// Clone of the state storage backend (ReDb). Used by V2 delegates to read
    /// contract state synchronously via host functions. ReDb is Arc<Database>
    /// internally, so cloning is cheap.
    state_store_db: Option<Storage>,
    /// Interior-mutable pointer to the runtime's DelegateStore. Valid only during
    /// synchronous `process()` call. Used for creating child delegates.
    delegate_store: std::cell::UnsafeCell<*mut DelegateStore>,
    /// Current delegate creation chain depth (0 for top-level calls).
    pub(super) creation_depth: u32,
    /// Number of delegates created so far in this process() call.
    pub(super) creations_this_call: std::cell::Cell<u32>,
    /// Origin contract IDs for this delegate (from parent or direct registration).
    origin_contracts: Vec<ContractInstanceId>,
}

// SAFETY: DelegateCallEnv is only inserted into DELEGATE_ENV immediately before
// a synchronous WASM process() call and removed immediately after. The raw pointer
// to SecretsStore is valid for the entire duration because the Runtime (which owns
// SecretsStore) is alive and on the same call stack. Wasmer's Singlepass compiler
// executes WASM synchronously on the calling thread.
unsafe impl Send for DelegateCallEnv {}
// SAFETY: Same rationale as Send above -- single-threaded synchronous WASM execution
// means DelegateCallEnv is never accessed from multiple threads concurrently.
unsafe impl Sync for DelegateCallEnv {}

/// Typed errors from `DelegateCallEnv` contract operations.
///
/// Replaces string-based error matching so `_impl` host functions can map
/// errors to the correct error code without fragile `contains()` checks.
#[derive(Debug)]
#[allow(dead_code)] // StorageError(String) is used for error logging via Debug
pub(super) enum DelegateEnvError {
    /// State store (ReDb) is not configured on this runtime.
    StoreNotConfigured,
    /// Contract code hash not found in the ContractStore index.
    ContractCodeNotRegistered,
    /// No existing state for this contract (required by UPDATE).
    NoExistingState,
    /// ReDb read/write error.
    StorageError(String),
}

/// Errors that can occur during delegate creation via `create_delegate_sync`.
#[derive(Debug)]
pub(super) enum DelegateCreateError {
    /// Delegate creation depth limit exceeded.
    DepthExceeded,
    /// Per-call delegate creation limit exceeded.
    CreationsExceeded,
    /// Per-node delegate creation limit exceeded.
    NodeLimitExceeded,
    /// Invalid WASM bytes (e.g., exceeds size limit).
    InvalidWasm(String),
    /// Delegate store or secret store operation failed.
    StoreFailed(String),
}

impl DelegateCallEnv {
    /// Create a new call environment.
    ///
    /// # Safety
    /// The caller must ensure that `secret_store` and `contract_store` remain
    /// valid for the entire lifetime of this `DelegateCallEnv` (i.e., until it
    /// is removed from `DELEGATE_ENV`). The caller must also ensure exclusive
    /// access to the SecretsStore during this time (no other references exist).
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn new(
        context: Vec<u8>,
        secret_store: &mut SecretsStore,
        contract_store: &ContractStore,
        state_store_db: Option<Storage>,
        delegate_key: DelegateKey,
        delegate_store: &mut DelegateStore,
        creation_depth: u32,
        origin_contracts: Vec<ContractInstanceId>,
    ) -> Self {
        Self {
            context,
            secret_store: std::cell::UnsafeCell::new(secret_store as *mut SecretsStore),
            delegate_key,
            contract_store: contract_store as *const ContractStore,
            state_store_db,
            delegate_store: std::cell::UnsafeCell::new(delegate_store as *mut DelegateStore),
            creation_depth,
            creations_this_call: std::cell::Cell::new(0),
            origin_contracts,
        }
    }

    /// Access the secrets store immutably. Only safe during the synchronous process() call.
    fn secret_store(&self) -> &SecretsStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model
        unsafe { &**self.secret_store.get() }
    }

    /// Access the secrets store mutably. Only safe during the synchronous process() call.
    ///
    /// # Safety invariants
    /// Interior mutability via `UnsafeCell` is safe because:
    /// 1. The Runtime holds `&mut self` when calling `process()`, ensuring exclusive access
    /// 2. WASM execution is synchronous on the calling thread (Singlepass compiler)
    /// 3. The `DelegateCallEnv` is created immediately before and destroyed immediately after
    ///    the WASM call, so no concurrent access is possible
    #[allow(clippy::mut_from_ref)]
    fn secret_store_mut(&self) -> &mut SecretsStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model.
        // The Runtime holds &mut self when calling process(), ensuring exclusive access.
        unsafe { &mut **self.secret_store.get() }
    }

    /// Access the contract store for index lookups.
    /// Only safe during the synchronous process() call.
    fn contract_store(&self) -> &ContractStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model
        unsafe { &*self.contract_store }
    }
    /// Access the delegate store mutably. Only safe during the synchronous process() call.
    #[allow(clippy::mut_from_ref)]
    fn delegate_store_mut(&self) -> &mut DelegateStore {
        // SAFETY: guaranteed by the caller of `new()` and the synchronous WASM execution model.
        // The Runtime holds &mut self when calling process(), ensuring exclusive access.
        unsafe { &mut **self.delegate_store.get() }
    }

    /// Maximum WASM code size (10 MiB) accepted by `create_delegate`.
    /// Prevents a single process() call from allocating excessive host-side memory.
    pub(crate) const MAX_WASM_CODE_SIZE: usize = 10 * 1024 * 1024;

    /// Create a new delegate synchronously during process() execution.
    ///
    /// Validates resource limits, constructs a DelegateContainer from the raw WASM bytes,
    /// registers it in the delegate store and secret store, and records creator attestation.
    ///
    /// Note: WASM bytes are stored as-is. Validation is deferred to execution time
    /// (when the delegate is actually invoked). Depth tracking is per-call only — child
    /// delegates invoked later via ApplicationMessages start at depth 0.
    ///
    /// # Arguments
    /// * `wasm_bytes` - Raw WASM code bytes (wrapped into versioned DelegateContainer internally)
    /// * `params` - Parameter bytes for the new delegate
    /// * `cipher_bytes` - 32-byte XChaCha20Poly1305 cipher key
    /// * `nonce_bytes` - 24-byte XNonce
    ///
    /// # Returns
    /// The new delegate's `DelegateKey` on success, or a `DelegateCreateError`.
    pub(super) fn create_delegate_sync(
        &self,
        wasm_bytes: &[u8],
        params: &[u8],
        cipher_bytes: [u8; 32],
        nonce_bytes: [u8; 24],
    ) -> Result<DelegateKey, DelegateCreateError> {
        use crate::contract::{
            MAX_CREATED_DELEGATES_PER_NODE, MAX_DELEGATE_CREATIONS_PER_CALL,
            MAX_DELEGATE_CREATION_DEPTH,
        };
        use chacha20poly1305::{KeyInit, XChaCha20Poly1305, XNonce};
        use freenet_stdlib::prelude::{
            Delegate, DelegateCode, DelegateContainer, DelegateWasmAPIVersion, Parameters,
        };

        // Check per-call creation limit
        let current = self.creations_this_call.get();
        if current >= MAX_DELEGATE_CREATIONS_PER_CALL {
            return Err(DelegateCreateError::CreationsExceeded);
        }

        // Check creation depth (per-call only; child delegates start at depth 0 when
        // invoked via ApplicationMessages in a separate process() call)
        if self.creation_depth >= MAX_DELEGATE_CREATION_DEPTH {
            return Err(DelegateCreateError::DepthExceeded);
        }

        // Check per-node creation limit (counts ALL created delegates, not just origin)
        if CREATED_DELEGATES_COUNT.load(Ordering::Relaxed) >= MAX_CREATED_DELEGATES_PER_NODE {
            return Err(DelegateCreateError::NodeLimitExceeded);
        }

        // Reject oversized WASM to prevent excessive host-side allocations
        if wasm_bytes.len() > Self::MAX_WASM_CODE_SIZE {
            return Err(DelegateCreateError::InvalidWasm(format!(
                "WASM code size {} exceeds maximum {} bytes",
                wasm_bytes.len(),
                Self::MAX_WASM_CODE_SIZE
            )));
        }

        // Construct DelegateContainer from raw WASM bytes and params.
        // The caller provides raw WASM; we wrap it into the versioned DelegateContainer.
        let params_owned = Parameters::from(params.to_vec());
        let delegate_code = DelegateCode::from(wasm_bytes.to_vec());
        let delegate = Delegate::from((&delegate_code, &params_owned));
        let container = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate));

        let child_key = container.key().clone();

        // Build cipher and nonce
        let cipher = XChaCha20Poly1305::new((&cipher_bytes).into());
        let nonce = XNonce::from(nonce_bytes);

        // Register in secret store
        self.secret_store_mut()
            .register_delegate(child_key.clone(), cipher, nonce)
            .map_err(|e| DelegateCreateError::StoreFailed(e.to_string()))?;

        // Store in delegate store; rollback secret store on failure
        if let Err(e) = self.delegate_store_mut().store_delegate(container) {
            // Rollback: remove the secret store cipher we just registered
            self.secret_store_mut().remove_delegate_cipher(&child_key);
            return Err(DelegateCreateError::StoreFailed(e.to_string()));
        }

        // Increment per-call counter and global node counter
        self.creations_this_call.set(current + 1);
        CREATED_DELEGATES_COUNT.fetch_add(1, Ordering::Relaxed);

        // Propagate app attestation to child
        if !self.origin_contracts.is_empty() {
            DELEGATE_INHERITED_ORIGINS.insert(child_key.clone(), self.origin_contracts.clone());
        }

        tracing::info!(
            parent = %self.delegate_key,
            child = %child_key,
            depth = self.creation_depth,
            creations = current + 1,
            "Delegate created child delegate"
        );

        Ok(child_key)
    }

    /// Resolve a ContractInstanceId to a ContractKey using the contract index.
    fn resolve_contract_key(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<ContractKey, DelegateEnvError> {
        let code_hash = self
            .contract_store()
            .code_hash_from_id(instance_id)
            .ok_or(DelegateEnvError::ContractCodeNotRegistered)?;
        Ok(ContractKey::from_id_and_code(*instance_id, code_hash))
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
    ) -> Result<Option<Vec<u8>>, DelegateEnvError> {
        let Some(ref db) = self.state_store_db else {
            return Err(DelegateEnvError::StoreNotConfigured);
        };

        // Look up CodeHash; return None (not error) if the instance is unknown
        let code_hash = match self.contract_store().code_hash_from_id(instance_id) {
            Some(ch) => ch,
            None => return Ok(None),
        };

        let contract_key = ContractKey::from_id_and_code(*instance_id, code_hash);

        match db.get_state_sync(&contract_key) {
            Ok(Some(wrapped_state)) => Ok(Some(wrapped_state.as_ref().to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(DelegateEnvError::StorageError(e.to_string())),
        }
    }

    /// Store (PUT) contract state by instance ID.
    ///
    /// The contract's code hash must already be registered in the ContractStore
    /// index (i.e., the contract code was previously stored). This writes the
    /// state to ReDb synchronously.
    ///
    /// Returns `Ok(())` on success.
    pub(super) fn put_contract_state_sync(
        &self,
        instance_id: &ContractInstanceId,
        state: Vec<u8>,
    ) -> Result<(), DelegateEnvError> {
        let Some(ref db) = self.state_store_db else {
            return Err(DelegateEnvError::StoreNotConfigured);
        };

        let contract_key = self.resolve_contract_key(instance_id)?;

        db.store_state_sync(
            &contract_key,
            freenet_stdlib::prelude::WrappedState::new(state),
        )
        .map_err(|e| DelegateEnvError::StorageError(e.to_string()))
    }

    /// Update contract state by instance ID.
    ///
    /// Like PUT, but only succeeds if the contract already has stored state.
    /// Returns an error if no prior state exists.
    pub(super) fn update_contract_state_sync(
        &self,
        instance_id: &ContractInstanceId,
        state: Vec<u8>,
    ) -> Result<(), DelegateEnvError> {
        let Some(ref db) = self.state_store_db else {
            return Err(DelegateEnvError::StoreNotConfigured);
        };

        let contract_key = self.resolve_contract_key(instance_id)?;

        // Atomic check-and-write in a single ReDb write transaction.
        match db.update_state_sync(
            &contract_key,
            freenet_stdlib::prelude::WrappedState::new(state),
        ) {
            Ok(true) => Ok(()),
            Ok(false) => Err(DelegateEnvError::NoExistingState),
            Err(e) => Err(DelegateEnvError::StorageError(e.to_string())),
        }
    }

    /// Register a subscription interest for a contract.
    ///
    /// Validates the contract is known and records the (contract, delegate) pair in
    /// the global subscription registry. When `commit_state_update()` persists a new
    /// state for this contract, it will send a `ContractNotification` to the delegate.
    pub(super) fn subscribe_contract_sync(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Result<(), DelegateEnvError> {
        // Validate the contract is known
        let _contract_key = self.resolve_contract_key(instance_id)?;

        // Register in global subscription registry
        DELEGATE_SUBSCRIPTIONS
            .entry(*instance_id)
            .or_default()
            .insert(self.delegate_key.clone());

        Ok(())
    }
}

/// Helper to get the current instance ID from the thread-local.
/// Returns -1 if not in a process() context.
fn current_instance_id() -> InstanceId {
    CURRENT_DELEGATE_INSTANCE.with(|c| c.get())
}

/// Validates memory bounds and computes a pointer within WASM linear memory.
///
/// # Safety
/// This function validates that the pointer arithmetic is safe. It checks for:
/// - Negative offsets (which would access memory before WASM linear memory)
/// - Overflow when adding offset + size
/// - Access exceeding the instance's current allocated memory size
/// - Overflow when adding start_ptr + ptr
///
/// The `mem_size` parameter reflects the live memory size at the moment of the call.
/// Every host function calls `refresh_mem_addr_from_caller` before invoking this helper,
/// which reads the current memory extent directly from the wasmtime `Caller`. This
/// ensures `mem_size` is always up to date, even after `memory.grow` instructions.
/// Using the live size (rather than the engine's theoretical maximum of 256 MiB) makes
/// the bounds check as tight as possible for the current allocation.
///
/// # Arguments
/// * `ptr` - Offset from the start of WASM memory (provided by WASM module)
/// * `start_ptr` - Base address of WASM linear memory (from InstanceInfo)
/// * `size` - Size of the data to be accessed (in bytes)
/// * `mem_size` - Current live memory size in bytes (refreshed before each host call)
///
/// # Returns
/// * `Some(*mut T)` - Valid pointer within current memory bounds
/// * `None` - Pointer would be out of bounds or overflow
#[inline(always)]
fn validate_and_compute_ptr<T>(
    ptr: i64,
    start_ptr: i64,
    size: usize,
    mem_size: usize,
) -> Option<*mut T> {
    // Check for negative offset (invalid - would access before WASM memory)
    if ptr < 0 {
        tracing::warn!("Memory bounds violation: negative offset {ptr}");
        return None;
    }

    let ptr_usize = ptr as usize;

    // Check for overflow when adding size to offset
    let end_offset = ptr_usize.checked_add(size)?;

    // Verify the entire access range is within the instance's current allocated memory.
    // `mem_size` is the live size refreshed from the wasmtime Caller immediately before
    // this call, so it accurately reflects any memory.grow that occurred during execution.
    // As a defence-in-depth, mem_size is also bounded by the engine's ResourceLimiter
    // at 256 MiB (DEFAULT_MAX_MEMORY_PAGES × WASM_PAGE_SIZE), so this check is always
    // at least as tight as the theoretical maximum.
    if end_offset > mem_size {
        tracing::warn!(
            "Memory bounds violation: access range [{ptr_usize}, {end_offset}) exceeds current memory size {mem_size}"
        );
        return None;
    }

    // Check for overflow when computing the host pointer
    // start_ptr is the host address (i64), ptr is the WASM offset (i64)
    let host_ptr = start_ptr.checked_add(ptr)?;

    // Safe to return the computed pointer
    Some(host_ptr as *mut T)
}

pub(super) mod log {
    use super::*;

    // TODO: this API right now is just a patch, ideally we want to impl a tracing subscriber
    // that can be used in wasm and that under the hood will just pass data to the host via
    // functions like this in a structured way
    pub(crate) fn info(id: i64, ptr: i64, len: i32) {
        if id == -1 {
            tracing::error!("freenet_log::info called with unset module id");
            return;
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let Some(ptr) =
            validate_and_compute_ptr::<u8>(ptr, info.start_ptr, len as usize, info.mem_size)
        else {
            tracing::error!("Memory bounds violation in freenet_log::info");
            return;
        };
        // SAFETY: `ptr` was validated by `validate_and_compute_ptr` to be within the
        // WASM linear memory bounds. The bytes come from an untrusted WASM contract and
        // may not be valid UTF-8, so we use from_utf8_lossy which replaces invalid byte
        // sequences with U+FFFD rather than invoking undefined behavior.
        let bytes = unsafe { std::slice::from_raw_parts(ptr, len as usize) };
        let msg = String::from_utf8_lossy(bytes);
        tracing::info!(target: "contract", contract = %info.value().key(), "{msg}");
    }
}

pub(super) mod rand {
    use ::rand::{rng, RngCore};

    use super::*;

    pub(crate) fn rand_bytes(id: i64, ptr: i64, len: u32) {
        if id == -1 {
            tracing::error!(
                "freenet_rand::rand_bytes called with unset module id; \
                 output buffer NOT written — caller will read uninitialized memory"
            );
            return;
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let Some(ptr) =
            validate_and_compute_ptr::<u8>(ptr, info.start_ptr, len as usize, info.mem_size)
        else {
            tracing::error!("Memory bounds violation in freenet_rand::rand_bytes");
            return;
        };
        // SAFETY: `ptr` was validated by `validate_and_compute_ptr` to point to `len`
        // bytes within the WASM linear memory, so constructing a mutable slice is valid.
        let slice = unsafe { &mut *std::ptr::slice_from_raw_parts_mut(ptr, len as usize) };
        let mut rng = rng();
        rng.fill_bytes(slice);
    }
}

pub(super) mod time {
    use super::*;
    use chrono::{DateTime, Utc as UtcOriginal};

    pub(crate) fn utc_now(id: i64, ptr: i64) {
        if id == -1 {
            tracing::error!(
                "freenet_time::utc_now called with unset module id; \
                 output buffer NOT written — caller will read uninitialized memory"
            );
            return;
        }
        let info = MEM_ADDR.get(&id).expect("instance mem space not recorded");
        let now = UtcOriginal::now();
        let Some(ptr) = validate_and_compute_ptr::<DateTime<UtcOriginal>>(
            ptr,
            info.start_ptr,
            std::mem::size_of::<DateTime<UtcOriginal>>(),
            info.mem_size,
        ) else {
            tracing::error!("Memory bounds violation in freenet_time::utc_now");
            return;
        };
        // SAFETY: `ptr` was validated by `validate_and_compute_ptr` to point to a region
        // large enough for `DateTime<Utc>` within the WASM linear memory.
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
pub(super) mod delegate_context {
    use super::*;

    /// Returns the current context length in bytes.
    ///
    /// ## Returns
    /// - Non-negative: context length in bytes
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_CONTEXT_TOO_LARGE`: context exceeds i32::MAX bytes
    pub(crate) fn context_len() -> i32 {
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
    pub(crate) fn context_read(ptr: i64, len: i32) -> i32 {
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
        let Some(dst) = validate_and_compute_ptr::<u8>(ptr, info.start_ptr, to_copy, info.mem_size)
        else {
            tracing::error!("Memory bounds violation in delegate context_read");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `dst` was validated by `validate_and_compute_ptr` to be within WASM
        // linear memory bounds, and `env.context` is a valid `Vec<u8>` with at least
        // `to_copy` bytes.
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
    pub(crate) fn context_write(ptr: i64, len: i32) -> i32 {
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
        let Some(src) =
            validate_and_compute_ptr::<u8>(ptr, info.start_ptr, len as usize, info.mem_size)
        else {
            tracing::error!("Memory bounds violation in delegate context_write");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `src` was validated by `validate_and_compute_ptr` to point to `len`
        // bytes within the WASM linear memory.
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
pub(super) mod delegate_secrets {
    use super::*;

    /// Get the length of a secret without retrieving its value.
    /// This allows the caller to allocate the right buffer size before calling get_secret.
    ///
    /// ## Returns
    /// - Non-negative: secret length in bytes
    /// - `ERR_NOT_IN_PROCESS`: called outside process()
    /// - `ERR_SECRET_NOT_FOUND`: secret doesn't exist
    /// - `ERR_INVALID_PARAM`: invalid parameter (negative key length)
    pub(crate) fn get_secret_len(key_ptr: i64, key_len: i32) -> i32 {
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
        let Some(key_src) = validate_and_compute_ptr::<u8>(
            key_ptr,
            info.start_ptr,
            key_len as usize,
            info.mem_size,
        ) else {
            tracing::error!("Memory bounds violation in delegate get_secret_len");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `key_src` was validated by `validate_and_compute_ptr` to point to
        // `key_len` bytes within the WASM linear memory.
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
            Err(e) => {
                tracing::debug!(
                    delegate = %env.delegate_key,
                    secret_id = ?secret_id,
                    error = %e,
                    "get_secret_len: secret not found or storage error"
                );
                error_codes::ERR_SECRET_NOT_FOUND
            }
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
    pub(crate) fn get_secret(key_ptr: i64, key_len: i32, out_ptr: i64, out_len: i32) -> i32 {
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
        let Some(key_src) = validate_and_compute_ptr::<u8>(
            key_ptr,
            info.start_ptr,
            key_len as usize,
            info.mem_size,
        ) else {
            tracing::error!("Memory bounds violation in delegate get_secret (key)");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `key_src` was validated by `validate_and_compute_ptr` to point to
        // `key_len` bytes within the WASM linear memory.
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

                let Some(dst) = validate_and_compute_ptr::<u8>(
                    out_ptr,
                    info.start_ptr,
                    secret_len,
                    info.mem_size,
                ) else {
                    tracing::error!("Memory bounds violation in delegate get_secret (output)");
                    return error_codes::ERR_MEMORY_BOUNDS;
                };
                // SAFETY: `dst` was validated by `validate_and_compute_ptr` to point to
                // `secret_len` bytes within WASM linear memory, and `plaintext` is a
                // valid byte slice of that length.
                unsafe {
                    std::ptr::copy_nonoverlapping(plaintext.as_ptr(), dst, secret_len);
                }
                secret_len as i32
            }
            Err(e) => {
                tracing::debug!(
                    delegate = %env.delegate_key,
                    secret_id = ?secret_id,
                    error = %e,
                    "get_secret: secret not found or storage error"
                );
                error_codes::ERR_SECRET_NOT_FOUND
            }
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
    pub(crate) fn set_secret(key_ptr: i64, key_len: i32, val_ptr: i64, val_len: i32) -> i32 {
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
        let Some(key_src) = validate_and_compute_ptr::<u8>(
            key_ptr,
            info.start_ptr,
            key_len as usize,
            info.mem_size,
        ) else {
            tracing::error!("Memory bounds violation in delegate set_secret (key)");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `key_src` was validated by `validate_and_compute_ptr` to point to
        // `key_len` bytes within the WASM linear memory.
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        let Some(val_src) = validate_and_compute_ptr::<u8>(
            val_ptr,
            info.start_ptr,
            val_len as usize,
            info.mem_size,
        ) else {
            tracing::error!("Memory bounds violation in delegate set_secret (value)");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `val_src` was validated by `validate_and_compute_ptr` to point to
        // `val_len` bytes within the WASM linear memory.
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
    pub(crate) fn has_secret(key_ptr: i64, key_len: i32) -> i32 {
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

        let Some(key_src) = validate_and_compute_ptr::<u8>(
            key_ptr,
            info.start_ptr,
            key_len as usize,
            info.mem_size,
        ) else {
            tracing::error!("Memory bounds violation in delegate has_secret");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `key_src` was validated by `validate_and_compute_ptr` to point to
        // `key_len` bytes within the WASM linear memory.
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        match env.secret_store().get_secret(&env.delegate_key, &secret_id) {
            Ok(_) => 1,
            Err(e) => {
                tracing::debug!(
                    delegate = %env.delegate_key,
                    secret_id = ?secret_id,
                    error = %e,
                    "has_secret: secret not found or storage error"
                );
                0
            }
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
    pub(crate) fn remove_secret(key_ptr: i64, key_len: i32) -> i32 {
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

        let Some(key_src) = validate_and_compute_ptr::<u8>(
            key_ptr,
            info.start_ptr,
            key_len as usize,
            info.mem_size,
        ) else {
            tracing::error!("Memory bounds violation in delegate remove_secret");
            return error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: `key_src` was validated by `validate_and_compute_ptr` to point to
        // `key_len` bytes within the WASM linear memory.
        let key_bytes = unsafe { std::slice::from_raw_parts(key_src, key_len as usize) };
        let secret_id = SecretsId::new(key_bytes.to_vec());

        match env
            .secret_store_mut()
            .remove_secret(&env.delegate_key, &secret_id)
        {
            Ok(()) => error_codes::SUCCESS,
            Err(e) => {
                tracing::debug!(
                    delegate = %env.delegate_key,
                    secret_id = ?secret_id,
                    error = %e,
                    "remove_secret: secret not found or storage error"
                );
                error_codes::ERR_SECRET_NOT_FOUND
            }
        }
    }
}

/// Host functions for async contract state access (V2 delegate API).
///
/// These allow a V2 delegate to read contract state directly during `process()`,
/// eliminating the GetContractRequest/GetContractResponse round-trip.
///
/// **Async host functions**: These are registered via `Function::new_typed_async`
/// so they can be called with `call_async` on a `StoreAsync`. This establishes
/// the async execution pattern needed for future operations (network fetches,
/// PUT operations, subscriptions) that will genuinely yield.
///
/// Currently the implementations are synchronous internally (ReDb reads), but
/// because they're registered as async, the delegate execution path must use
/// `call_async` via a `StoreAsync`.
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
pub(super) mod delegate_contracts {
    use super::*;
    use crate::wasm_runtime::delegate_api::contract_error_codes;

    /// Implementation of get_contract_state_len.
    ///
    /// Extracted as a free function so it can be called from the async wrapper.
    /// Accesses only global statics (MEM_ADDR, DELEGATE_ENV) which are Send + 'static.
    pub(crate) fn get_contract_state_len_impl(id_ptr: i64, id_len: i32) -> i64 {
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
        let Some(id_src) =
            validate_and_compute_ptr::<u8>(id_ptr, info.start_ptr, 32, info.mem_size)
        else {
            tracing::error!("Memory bounds violation in delegate get_contract_state_len");
            return contract_error_codes::ERR_MEMORY_BOUNDS as i64;
        };
        // SAFETY: `id_src` was validated by `validate_and_compute_ptr` to point to
        // 32 bytes within the WASM linear memory.
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
                    error = ?e,
                    "V2 delegate: state store error"
                );
                delegate_env_error_to_code(&e)
            }
        }
    }

    /// Map a `DelegateEnvError` to the appropriate contract error code.
    fn delegate_env_error_to_code(err: &DelegateEnvError) -> i64 {
        match err {
            DelegateEnvError::StoreNotConfigured => contract_error_codes::ERR_STORE_ERROR as i64,
            DelegateEnvError::ContractCodeNotRegistered => {
                contract_error_codes::ERR_CONTRACT_CODE_NOT_REGISTERED as i64
            }
            DelegateEnvError::NoExistingState => {
                contract_error_codes::ERR_CONTRACT_NOT_FOUND as i64
            }
            DelegateEnvError::StorageError(_) => contract_error_codes::ERR_STORE_ERROR as i64,
        }
    }

    /// Read a 32-byte contract instance ID from WASM memory.
    ///
    /// Shared helper for all contract host functions. Validates the instance ID
    /// pointer and length, returning the ContractInstanceId on success.
    /// Only acquires and drops the MEM_ADDR guard to read WASM memory metadata.
    fn read_instance_id(id_ptr: i64, id_len: i32) -> Result<ContractInstanceId, i64> {
        let id = current_instance_id();
        if id == -1 {
            return Err(contract_error_codes::ERR_NOT_IN_PROCESS as i64);
        }
        if id_len != 32 {
            tracing::warn!("delegate contract host fn: expected 32-byte instance ID, got {id_len}");
            return Err(contract_error_codes::ERR_INVALID_PARAM as i64);
        }
        let Some(info) = MEM_ADDR.get(&id) else {
            return Err(contract_error_codes::ERR_NOT_IN_PROCESS as i64);
        };
        let start_ptr = info.start_ptr;
        let mem_size = info.mem_size;
        drop(info);

        let Some(id_src) = validate_and_compute_ptr::<u8>(id_ptr, start_ptr, 32, mem_size) else {
            return Err(contract_error_codes::ERR_MEMORY_BOUNDS as i64);
        };
        // SAFETY: `id_src` was validated by `validate_and_compute_ptr` to point to
        // 32 bytes within the WASM linear memory.
        let id_bytes: [u8; 32] = unsafe { std::slice::from_raw_parts(id_src, 32) }
            .try_into()
            .unwrap();
        Ok(ContractInstanceId::new(id_bytes))
    }

    /// Implementation of get_contract_state.
    ///
    /// Extracted as a free function so it can be called from the async wrapper.
    pub(crate) fn get_contract_state_impl(
        id_ptr: i64,
        id_len: i32,
        out_ptr: i64,
        out_len: i64,
    ) -> i64 {
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
        let Some(id_src) =
            validate_and_compute_ptr::<u8>(id_ptr, info.start_ptr, 32, info.mem_size)
        else {
            tracing::error!("Memory bounds violation in delegate get_contract_state (id)");
            return contract_error_codes::ERR_MEMORY_BOUNDS as i64;
        };
        // SAFETY: `id_src` was validated by `validate_and_compute_ptr` to point to
        // 32 bytes within the WASM linear memory.
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

                let Some(dst) = validate_and_compute_ptr::<u8>(
                    out_ptr,
                    info.start_ptr,
                    state_len,
                    info.mem_size,
                ) else {
                    tracing::error!(
                        "Memory bounds violation in delegate get_contract_state (output)"
                    );
                    return contract_error_codes::ERR_MEMORY_BOUNDS as i64;
                };
                // SAFETY: `dst` was validated by `validate_and_compute_ptr` to point to
                // `state_len` bytes within WASM linear memory, and `state_bytes` is a
                // valid `Vec<u8>` of that length.
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
                    error = ?e,
                    "V2 delegate: state store error"
                );
                delegate_env_error_to_code(&e)
            }
        }
    }

    /// Implementation of put_contract_state.
    ///
    /// Writes contract state to the local ReDb store. Requires the contract's
    /// code hash to be registered in the ContractStore index.
    ///
    /// ## Returns
    /// - `0`: success
    /// - Negative error code on failure
    pub(crate) fn put_contract_state_impl(
        id_ptr: i64,
        id_len: i32,
        state_ptr: i64,
        state_len: i64,
    ) -> i64 {
        let contract_id = match read_instance_id(id_ptr, id_len) {
            Ok(id) => id,
            Err(code) => return code,
        };

        if state_len < 0 {
            tracing::warn!("delegate put_contract_state: negative state_len={state_len}");
            return contract_error_codes::ERR_INVALID_PARAM as i64;
        }

        let id = current_instance_id();
        let Some(info) = MEM_ADDR.get(&id) else {
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };

        // Read state bytes from WASM memory
        let state_bytes = if state_len == 0 {
            vec![]
        } else {
            let Some(src) = validate_and_compute_ptr::<u8>(
                state_ptr,
                info.start_ptr,
                state_len as usize,
                info.mem_size,
            ) else {
                tracing::error!("Memory bounds violation in delegate put_contract_state (state)");
                return contract_error_codes::ERR_MEMORY_BOUNDS as i64;
            };
            // SAFETY: `src` was validated by `validate_and_compute_ptr` to point to
            // `state_len` bytes within the WASM linear memory.
            unsafe { std::slice::from_raw_parts(src, state_len as usize) }.to_vec()
        };

        match env.put_contract_state_sync(&contract_id, state_bytes) {
            Ok(()) => {
                tracing::debug!(
                    contract = %contract_id,
                    "V2 delegate: put_contract_state succeeded"
                );
                contract_error_codes::SUCCESS as i64
            }
            Err(ref e) => {
                tracing::debug!(
                    contract = %contract_id,
                    error = ?e,
                    "V2 delegate: put_contract_state failed"
                );
                delegate_env_error_to_code(e)
            }
        }
    }

    /// Implementation of update_contract_state.
    ///
    /// Like PUT, but only succeeds if the contract already has stored state.
    ///
    /// ## Returns
    /// - `0`: success
    /// - `ERR_CONTRACT_NOT_FOUND (-7)`: no existing state to update
    /// - Negative error code on other failures
    pub(crate) fn update_contract_state_impl(
        id_ptr: i64,
        id_len: i32,
        state_ptr: i64,
        state_len: i64,
    ) -> i64 {
        let contract_id = match read_instance_id(id_ptr, id_len) {
            Ok(id) => id,
            Err(code) => return code,
        };

        if state_len < 0 {
            tracing::warn!("delegate update_contract_state: negative state_len={state_len}");
            return contract_error_codes::ERR_INVALID_PARAM as i64;
        }

        let id = current_instance_id();
        let Some(info) = MEM_ADDR.get(&id) else {
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };
        let Some(env) = DELEGATE_ENV.get(&id) else {
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };

        // Read state bytes from WASM memory
        let state_bytes = if state_len == 0 {
            vec![]
        } else {
            let Some(src) = validate_and_compute_ptr::<u8>(
                state_ptr,
                info.start_ptr,
                state_len as usize,
                info.mem_size,
            ) else {
                tracing::error!(
                    "Memory bounds violation in delegate update_contract_state (state)"
                );
                return contract_error_codes::ERR_MEMORY_BOUNDS as i64;
            };
            // SAFETY: `src` was validated by `validate_and_compute_ptr` to point to
            // `state_len` bytes within the WASM linear memory.
            unsafe { std::slice::from_raw_parts(src, state_len as usize) }.to_vec()
        };

        match env.update_contract_state_sync(&contract_id, state_bytes) {
            Ok(()) => {
                tracing::debug!(
                    contract = %contract_id,
                    "V2 delegate: update_contract_state succeeded"
                );
                contract_error_codes::SUCCESS as i64
            }
            Err(ref e) => {
                tracing::debug!(
                    contract = %contract_id,
                    error = ?e,
                    "V2 delegate: update_contract_state failed"
                );
                delegate_env_error_to_code(e)
            }
        }
    }

    /// Implementation of subscribe_contract.
    ///
    /// Validates that the contract is known (code hash resolvable) and registers
    /// subscription interest in the global `DELEGATE_SUBSCRIPTIONS` registry.
    /// When the subscribed contract's state changes via `commit_state_update()`,
    /// a `ContractNotification` is delivered to this delegate.
    ///
    /// ## Returns
    /// - `0`: success (contract is known, subscription registered)
    /// - `ERR_CONTRACT_CODE_NOT_REGISTERED (-10)`: unknown contract
    /// - Negative error code on other failures
    pub(crate) fn subscribe_contract_impl(id_ptr: i64, id_len: i32) -> i64 {
        let contract_id = match read_instance_id(id_ptr, id_len) {
            Ok(id) => id,
            Err(code) => return code,
        };

        let id = current_instance_id();
        let Some(env) = DELEGATE_ENV.get(&id) else {
            return contract_error_codes::ERR_NOT_IN_PROCESS as i64;
        };

        match env.subscribe_contract_sync(&contract_id) {
            Ok(()) => {
                tracing::debug!(
                    contract = %contract_id,
                    "V2 delegate: subscribe_contract succeeded"
                );
                contract_error_codes::SUCCESS as i64
            }
            Err(ref e) => {
                tracing::debug!(
                    contract = %contract_id,
                    error = ?e,
                    "V2 delegate: subscribe_contract failed"
                );
                delegate_env_error_to_code(e)
            }
        }
    }
}

/// Host functions for delegate management (creating child delegates).
///
/// Provides the `create_delegate` host function that allows V2 delegates to
/// spawn new delegates inline during `process()` execution.
pub(super) mod delegate_management {
    use super::*;
    use crate::wasm_runtime::delegate_api::delegate_mgmt_error_codes;

    /// Implementation of create_delegate host function.
    ///
    /// Reads WASM bytes, params, cipher, and nonce from WASM memory,
    /// calls `env.create_delegate_sync()`, and writes the resulting
    /// DelegateKey back to WASM memory.
    ///
    /// # Parameters (from WASM)
    /// * `wasm_ptr`, `wasm_len` - Pointer and length of WASM code bytes
    /// * `params_ptr`, `params_len` - Pointer and length of parameter bytes
    /// * `cipher_ptr` - Pointer to 32-byte cipher key
    /// * `nonce_ptr` - Pointer to 24-byte nonce
    /// * `out_key_ptr` - Pointer to 32-byte output buffer for delegate key hash
    /// * `out_hash_ptr` - Pointer to 32-byte output buffer for code hash
    ///
    /// # Returns
    /// 0 on success, negative error code on failure.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_delegate_impl(
        wasm_ptr: i64,
        wasm_len: i64,
        params_ptr: i64,
        params_len: i64,
        cipher_ptr: i64,
        nonce_ptr: i64,
        out_key_ptr: i64,
        out_hash_ptr: i64,
    ) -> i32 {
        let id = current_instance_id();
        if id == -1 {
            tracing::warn!("delegate create_delegate called outside process()");
            return delegate_mgmt_error_codes::ERR_NOT_IN_PROCESS;
        }

        // Validate lengths
        if wasm_len < 0 || params_len < 0 {
            tracing::warn!("delegate create_delegate: negative length");
            return delegate_mgmt_error_codes::ERR_INVALID_PARAM;
        }
        let wasm_len_usize = wasm_len as usize;
        let params_len_usize = params_len as usize;

        let Some(info) = MEM_ADDR.get(&id) else {
            tracing::warn!("instance mem space not recorded for {id}");
            return delegate_mgmt_error_codes::ERR_NOT_IN_PROCESS;
        };
        let start_ptr = info.start_ptr;
        let mem_size = info.mem_size;
        drop(info);

        // Read WASM bytes from WASM memory
        let Some(wasm_src) =
            validate_and_compute_ptr::<u8>(wasm_ptr, start_ptr, wasm_len_usize, mem_size)
        else {
            tracing::error!("Memory bounds violation reading WASM bytes in create_delegate");
            return delegate_mgmt_error_codes::ERR_MEMORY_BOUNDS;
        };
        // SAFETY: validated by validate_and_compute_ptr
        let wasm_bytes = unsafe { std::slice::from_raw_parts(wasm_src, wasm_len_usize) };

        // Read params from WASM memory
        let params_bytes = if params_len_usize > 0 {
            let Some(params_src) =
                validate_and_compute_ptr::<u8>(params_ptr, start_ptr, params_len_usize, mem_size)
            else {
                tracing::error!("Memory bounds violation reading params in create_delegate");
                return delegate_mgmt_error_codes::ERR_MEMORY_BOUNDS;
            };
            // SAFETY: validated by validate_and_compute_ptr
            unsafe { std::slice::from_raw_parts(params_src, params_len_usize) }
        } else {
            &[]
        };

        // Read 32-byte cipher key
        let Some(cipher_src) = validate_and_compute_ptr::<u8>(cipher_ptr, start_ptr, 32, mem_size)
        else {
            tracing::error!("Memory bounds violation reading cipher in create_delegate");
            return delegate_mgmt_error_codes::ERR_MEMORY_BOUNDS;
        };
        let mut cipher_bytes = [0u8; 32];
        // SAFETY: cipher_src points to 32 validated bytes (validate_and_compute_ptr above),
        // cipher_bytes is a stack-local [u8; 32], and they do not overlap.
        unsafe { std::ptr::copy_nonoverlapping(cipher_src, cipher_bytes.as_mut_ptr(), 32) };

        // Read 24-byte nonce
        let Some(nonce_src) = validate_and_compute_ptr::<u8>(nonce_ptr, start_ptr, 24, mem_size)
        else {
            tracing::error!("Memory bounds violation reading nonce in create_delegate");
            return delegate_mgmt_error_codes::ERR_MEMORY_BOUNDS;
        };
        let mut nonce_bytes = [0u8; 24];
        // SAFETY: nonce_src points to 24 validated bytes (validate_and_compute_ptr above),
        // nonce_bytes is a stack-local [u8; 24], and they do not overlap.
        unsafe { std::ptr::copy_nonoverlapping(nonce_src, nonce_bytes.as_mut_ptr(), 24) };

        // Validate output buffers
        let Some(out_key_dst) =
            validate_and_compute_ptr::<u8>(out_key_ptr, start_ptr, 32, mem_size)
        else {
            tracing::error!("Memory bounds violation for out_key_ptr in create_delegate");
            return delegate_mgmt_error_codes::ERR_MEMORY_BOUNDS;
        };
        let Some(out_hash_dst) =
            validate_and_compute_ptr::<u8>(out_hash_ptr, start_ptr, 32, mem_size)
        else {
            tracing::error!("Memory bounds violation for out_hash_ptr in create_delegate");
            return delegate_mgmt_error_codes::ERR_MEMORY_BOUNDS;
        };

        // Get the delegate env and perform creation
        let Some(env) = DELEGATE_ENV.get(&id) else {
            tracing::warn!("delegate call env not set for instance {id}");
            return delegate_mgmt_error_codes::ERR_NOT_IN_PROCESS;
        };

        match env.create_delegate_sync(wasm_bytes, params_bytes, cipher_bytes, nonce_bytes) {
            Ok(child_key) => {
                // Write the 32-byte key hash to out_key_ptr
                let key_bytes = child_key.bytes();
                // SAFETY: out_key_dst validated above
                unsafe {
                    std::ptr::copy_nonoverlapping(key_bytes.as_ptr(), out_key_dst, 32);
                }
                // Write the 32-byte code hash to out_hash_ptr
                let code_hash_bytes = child_key.code_hash().as_ref();
                // SAFETY: out_hash_dst validated above
                unsafe {
                    std::ptr::copy_nonoverlapping(code_hash_bytes.as_ptr(), out_hash_dst, 32);
                }
                delegate_mgmt_error_codes::SUCCESS
            }
            Err(DelegateCreateError::DepthExceeded) => {
                tracing::warn!("delegate create_delegate: depth limit exceeded");
                delegate_mgmt_error_codes::ERR_DEPTH_EXCEEDED
            }
            Err(DelegateCreateError::CreationsExceeded) => {
                tracing::warn!("delegate create_delegate: per-call creation limit exceeded");
                delegate_mgmt_error_codes::ERR_CREATIONS_EXCEEDED
            }
            Err(DelegateCreateError::NodeLimitExceeded) => {
                tracing::warn!("delegate create_delegate: per-node creation limit exceeded");
                delegate_mgmt_error_codes::ERR_NODE_LIMIT_EXCEEDED
            }
            Err(DelegateCreateError::InvalidWasm(msg)) => {
                tracing::error!("delegate create_delegate: invalid WASM: {msg}");
                delegate_mgmt_error_codes::ERR_INVALID_WASM
            }
            Err(DelegateCreateError::StoreFailed(msg)) => {
                tracing::error!("delegate create_delegate: store failed: {msg}");
                delegate_mgmt_error_codes::ERR_STORE_FAILED
            }
        }
    }
}
