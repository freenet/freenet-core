//! Host function API for delegates.
//!
//! This module provides synchronous access to delegate context, secrets, and
//! contract state via host functions, eliminating the need for message round-trips.
//!
//! # Example
//!
//! ```ignore
//! use freenet_stdlib::prelude::*;
//!
//! #[delegate]
//! impl DelegateInterface for MyDelegate {
//!     fn process(
//!         ctx: &mut DelegateCtx,
//!         _params: Parameters<'static>,
//!         _attested: Option<&'static [u8]>,
//!         message: InboundDelegateMsg,
//!     ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
//!         // Read/write temporary context
//!         let data = ctx.read();
//!         ctx.write(b"new state");
//!
//!         // Access persistent secrets
//!         if let Some(key) = ctx.get_secret(b"private_key") {
//!             // use key...
//!         }
//!         ctx.set_secret(b"new_secret", b"value");
//!
//!         // V2: Direct contract access (no round-trips!)
//!         let contract_id = [0u8; 32]; // your contract instance ID
//!         if let Some(state) = ctx.get_contract_state(&contract_id) {
//!             // process state...
//!         }
//!         ctx.put_contract_state(&contract_id, b"new state");
//!
//!         Ok(vec![])
//!     }
//! }
//! ```
//!
//! # Context vs Secrets vs Contracts
//!
//! - **Context** (`read`/`write`): Temporary state within a single message batch.
//!   Reset between separate runtime calls. Use for intermediate processing state.
//!
//! - **Secrets** (`get_secret`/`set_secret`): Persistent encrypted storage.
//!   Survives across all delegate invocations. Use for private keys, tokens, etc.
//!
//! - **Contracts** (`get_contract_state`/`put_contract_state`/`update_contract_state`/
//!   `subscribe_contract`): V2 host functions for direct contract state access.
//!   Synchronous local reads/writes — no request/response round-trips.
//!
//! # Error Codes
//!
//! Host functions return negative values to indicate errors:
//!
//! | Code | Meaning |
//! |------|---------|
//! | 0    | Success |
//! | -1   | Called outside process() context |
//! | -2   | Secret not found |
//! | -3   | Storage operation failed |
//! | -4   | Invalid parameter (e.g., negative length) |
//! | -5   | Context too large (exceeds i32::MAX) |
//! | -6   | Buffer too small |
//! | -7   | Contract not found in local store |
//! | -8   | Internal state store error |
//! | -9   | WASM memory bounds violation |
//! | -10  | Contract code not registered |
//!
//! The wrapper methods in [`DelegateCtx`] handle these error codes and present
//! a more ergonomic API.

/// Error codes returned by host functions.
///
/// Negative values indicate errors, non-negative values indicate success
/// (usually the number of bytes read/written).
pub mod error_codes {
    /// Operation succeeded.
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
    /// Buffer too small to hold the data.
    pub const ERR_BUFFER_TOO_SMALL: i32 = -6;
    /// Contract not found in local store.
    pub const ERR_CONTRACT_NOT_FOUND: i32 = -7;
    /// Internal state store error.
    pub const ERR_STORE_ERROR: i32 = -8;
    /// WASM memory bounds violation (pointer/length out of range).
    pub const ERR_MEMORY_BOUNDS: i32 = -9;
    /// Contract code not registered in the index.
    pub const ERR_CONTRACT_CODE_NOT_REGISTERED: i32 = -10;

    // Delegate management error codes (-20..-29)

    /// Delegate creation depth limit exceeded.
    pub const ERR_DEPTH_EXCEEDED: i32 = -20;
    /// Per-call delegate creation limit exceeded.
    pub const ERR_CREATIONS_EXCEEDED: i32 = -21;
    // -22 reserved for future per-node limit
    /// Invalid WASM bytes for delegate creation.
    pub const ERR_INVALID_WASM: i32 = -23;
    /// Delegate store operation failed.
    pub const ERR_DELEGATE_STORE_FAILED: i32 = -24;
}

// ============================================================================
// Host function declarations (WASM only)
// ============================================================================

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_ctx")]
extern "C" {
    /// Returns the current context length in bytes, or negative error code.
    fn __frnt__delegate__ctx_len() -> i32;
    /// Reads context into the buffer at `ptr` (max `len` bytes). Returns bytes written, or negative error code.
    fn __frnt__delegate__ctx_read(ptr: i64, len: i32) -> i32;
    /// Writes `len` bytes from `ptr` into the context, replacing existing content. Returns 0 on success, or negative error code.
    fn __frnt__delegate__ctx_write(ptr: i64, len: i32) -> i32;
}

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_secrets")]
extern "C" {
    /// Get a secret. Returns bytes written to `out_ptr`, or negative error code.
    fn __frnt__delegate__get_secret(key_ptr: i64, key_len: i32, out_ptr: i64, out_len: i32) -> i32;
    /// Get secret length without fetching value. Returns length, or negative error code.
    fn __frnt__delegate__get_secret_len(key_ptr: i64, key_len: i32) -> i32;
    /// Store a secret. Returns 0 on success, or negative error code.
    fn __frnt__delegate__set_secret(key_ptr: i64, key_len: i32, val_ptr: i64, val_len: i32) -> i32;
    /// Check if a secret exists. Returns 1 if yes, 0 if no, or negative error code.
    fn __frnt__delegate__has_secret(key_ptr: i64, key_len: i32) -> i32;
    /// Remove a secret. Returns 0 on success, or negative error code.
    fn __frnt__delegate__remove_secret(key_ptr: i64, key_len: i32) -> i32;
}

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_contracts")]
extern "C" {
    /// Get contract state length. Returns byte count, or negative error code (i64).
    fn __frnt__delegate__get_contract_state_len(id_ptr: i64, id_len: i32) -> i64;
    /// Get contract state. Returns byte count written, or negative error code (i64).
    fn __frnt__delegate__get_contract_state(
        id_ptr: i64,
        id_len: i32,
        out_ptr: i64,
        out_len: i64,
    ) -> i64;
    /// Put (store) contract state. Returns 0 on success, or negative error code (i64).
    fn __frnt__delegate__put_contract_state(
        id_ptr: i64,
        id_len: i32,
        state_ptr: i64,
        state_len: i64,
    ) -> i64;
    /// Update contract state (requires existing state). Returns 0 on success, or negative error code (i64).
    fn __frnt__delegate__update_contract_state(
        id_ptr: i64,
        id_len: i32,
        state_ptr: i64,
        state_len: i64,
    ) -> i64;
    /// Subscribe to contract updates. Returns 0 on success, or negative error code (i64).
    fn __frnt__delegate__subscribe_contract(id_ptr: i64, id_len: i32) -> i64;
}

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_management")]
extern "C" {
    /// Create a new delegate from WASM code + parameters.
    /// Returns 0 on success, negative error code on failure.
    /// On success, writes 32 bytes to out_key_ptr and 32 bytes to out_hash_ptr.
    fn __frnt__delegate__create_delegate(
        wasm_ptr: i64,
        wasm_len: i64,
        params_ptr: i64,
        params_len: i32,
        cipher_ptr: i64,
        nonce_ptr: i64,
        out_key_ptr: i64,
        out_hash_ptr: i64,
    ) -> i32;
}

// ============================================================================
// DelegateCtx - Unified handle to context, secrets, and contracts
// ============================================================================

/// Opaque handle to the delegate's execution environment.
///
/// Provides access to:
/// - **Temporary context**: State shared within a single message batch (reset between calls)
/// - **Persistent secrets**: Encrypted storage that survives across all invocations
/// - **Contract state** (V2): Direct synchronous access to local contract state
///
/// # Context Methods
/// - [`read`](Self::read), [`write`](Self::write), [`len`](Self::len), [`clear`](Self::clear)
///
/// # Secret Methods
/// - [`get_secret`](Self::get_secret), [`set_secret`](Self::set_secret),
///   [`has_secret`](Self::has_secret), [`remove_secret`](Self::remove_secret)
///
/// # Contract Methods (V2)
/// - [`get_contract_state`](Self::get_contract_state),
///   [`put_contract_state`](Self::put_contract_state),
///   [`update_contract_state`](Self::update_contract_state),
///   [`subscribe_contract`](Self::subscribe_contract)
///
/// # Delegate Management Methods (V2)
/// - [`create_delegate`](Self::create_delegate)
#[derive(Default)]
#[repr(transparent)]
pub struct DelegateCtx {
    _private: (),
}

impl DelegateCtx {
    /// Creates the context handle.
    ///
    /// # Safety
    ///
    /// This should only be called by macro-generated code when the runtime
    /// has set up the delegate execution environment.
    #[doc(hidden)]
    pub unsafe fn __new() -> Self {
        Self { _private: () }
    }

    // ========================================================================
    // Context methods (temporary state within a batch)
    // ========================================================================

    /// Returns the current context length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        #[cfg(target_family = "wasm")]
        {
            let len = unsafe { __frnt__delegate__ctx_len() };
            if len < 0 {
                0
            } else {
                len as usize
            }
        }
        #[cfg(not(target_family = "wasm"))]
        {
            0
        }
    }

    /// Returns `true` if the context is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Read the current context bytes.
    ///
    /// Returns an empty `Vec` if no context has been written.
    pub fn read(&self) -> Vec<u8> {
        #[cfg(target_family = "wasm")]
        {
            let len = unsafe { __frnt__delegate__ctx_len() };
            if len <= 0 {
                return Vec::new();
            }
            let mut buf = vec![0u8; len as usize];
            let read = unsafe { __frnt__delegate__ctx_read(buf.as_mut_ptr() as i64, len) };
            buf.truncate(read.max(0) as usize);
            buf
        }
        #[cfg(not(target_family = "wasm"))]
        {
            Vec::new()
        }
    }

    /// Read context into a provided buffer.
    ///
    /// Returns the number of bytes actually read.
    pub fn read_into(&self, buf: &mut [u8]) -> usize {
        #[cfg(target_family = "wasm")]
        {
            let read =
                unsafe { __frnt__delegate__ctx_read(buf.as_mut_ptr() as i64, buf.len() as i32) };
            read.max(0) as usize
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = buf;
            0
        }
    }

    /// Write new context bytes, replacing any existing content.
    ///
    /// Returns `true` on success, `false` on error.
    pub fn write(&mut self, data: &[u8]) -> bool {
        #[cfg(target_family = "wasm")]
        {
            let result =
                unsafe { __frnt__delegate__ctx_write(data.as_ptr() as i64, data.len() as i32) };
            result == 0
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = data;
            false
        }
    }

    /// Clear the context.
    #[inline]
    pub fn clear(&mut self) {
        self.write(&[]);
    }

    // ========================================================================
    // Secret methods (persistent encrypted storage)
    // ========================================================================

    /// Get the length of a secret without retrieving its value.
    ///
    /// Returns `None` if the secret does not exist.
    pub fn get_secret_len(&self, key: &[u8]) -> Option<usize> {
        #[cfg(target_family = "wasm")]
        {
            let result =
                unsafe { __frnt__delegate__get_secret_len(key.as_ptr() as i64, key.len() as i32) };
            if result < 0 {
                None
            } else {
                Some(result as usize)
            }
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = key;
            None
        }
    }

    /// Get a secret by key.
    ///
    /// Returns `None` if the secret does not exist.
    pub fn get_secret(&self, key: &[u8]) -> Option<Vec<u8>> {
        #[cfg(target_family = "wasm")]
        {
            // First get the length to allocate the right buffer size
            let len = self.get_secret_len(key)?;

            if len == 0 {
                return Some(Vec::new());
            }

            let mut out = vec![0u8; len];
            let result = unsafe {
                __frnt__delegate__get_secret(
                    key.as_ptr() as i64,
                    key.len() as i32,
                    out.as_mut_ptr() as i64,
                    out.len() as i32,
                )
            };
            if result < 0 {
                None
            } else {
                out.truncate(result as usize);
                Some(out)
            }
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = key;
            None
        }
    }

    /// Store a secret.
    ///
    /// Returns `true` on success, `false` on error.
    pub fn set_secret(&mut self, key: &[u8], value: &[u8]) -> bool {
        #[cfg(target_family = "wasm")]
        {
            let result = unsafe {
                __frnt__delegate__set_secret(
                    key.as_ptr() as i64,
                    key.len() as i32,
                    value.as_ptr() as i64,
                    value.len() as i32,
                )
            };
            result == 0
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = (key, value);
            false
        }
    }

    /// Check if a secret exists.
    pub fn has_secret(&self, key: &[u8]) -> bool {
        #[cfg(target_family = "wasm")]
        {
            let result =
                unsafe { __frnt__delegate__has_secret(key.as_ptr() as i64, key.len() as i32) };
            result == 1
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = key;
            false
        }
    }

    /// Remove a secret.
    ///
    /// Returns `true` if the secret was removed, `false` if it didn't exist.
    pub fn remove_secret(&mut self, key: &[u8]) -> bool {
        #[cfg(target_family = "wasm")]
        {
            let result =
                unsafe { __frnt__delegate__remove_secret(key.as_ptr() as i64, key.len() as i32) };
            result == 0
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = key;
            false
        }
    }

    // ========================================================================
    // Contract methods (V2 — direct synchronous access)
    // ========================================================================

    /// Get contract state by instance ID.
    ///
    /// Returns `Some(state_bytes)` if the contract exists locally,
    /// `None` if not found or on error.
    ///
    /// Uses a two-step protocol: first queries the state length, then reads
    /// the state bytes into an allocated buffer.
    pub fn get_contract_state(&self, instance_id: &[u8; 32]) -> Option<Vec<u8>> {
        #[cfg(target_family = "wasm")]
        {
            // Step 1: Get the state length
            let len = unsafe {
                __frnt__delegate__get_contract_state_len(instance_id.as_ptr() as i64, 32)
            };
            if len < 0 {
                return None;
            }
            let len = len as usize;
            if len == 0 {
                return Some(Vec::new());
            }

            // Step 2: Read the state bytes
            let mut buf = vec![0u8; len];
            let read = unsafe {
                __frnt__delegate__get_contract_state(
                    instance_id.as_ptr() as i64,
                    32,
                    buf.as_mut_ptr() as i64,
                    buf.len() as i64,
                )
            };
            if read < 0 {
                None
            } else {
                buf.truncate(read as usize);
                Some(buf)
            }
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = instance_id;
            None
        }
    }

    /// Store (PUT) contract state by instance ID.
    ///
    /// The contract's code must already be registered in the runtime's contract
    /// store. Returns `true` on success, `false` on error.
    pub fn put_contract_state(&mut self, instance_id: &[u8; 32], state: &[u8]) -> bool {
        #[cfg(target_family = "wasm")]
        {
            let result = unsafe {
                __frnt__delegate__put_contract_state(
                    instance_id.as_ptr() as i64,
                    32,
                    state.as_ptr() as i64,
                    state.len() as i64,
                )
            };
            result == 0
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = (instance_id, state);
            false
        }
    }

    /// Update contract state by instance ID.
    ///
    /// Like `put_contract_state`, but only succeeds if the contract already has
    /// stored state. This performs a full state replacement (not a delta-based
    /// update through the contract's `update_state` logic). Returns `true` on
    /// success, `false` if no prior state exists or on other errors.
    pub fn update_contract_state(&mut self, instance_id: &[u8; 32], state: &[u8]) -> bool {
        #[cfg(target_family = "wasm")]
        {
            let result = unsafe {
                __frnt__delegate__update_contract_state(
                    instance_id.as_ptr() as i64,
                    32,
                    state.as_ptr() as i64,
                    state.len() as i64,
                )
            };
            result == 0
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = (instance_id, state);
            false
        }
    }

    /// Subscribe to contract updates by instance ID.
    ///
    /// Registers interest in receiving notifications when the contract's state
    /// changes. Currently validates that the contract is known and returns success;
    /// actual notification delivery is a follow-up.
    ///
    /// Returns `true` on success, `false` if the contract is unknown or on error.
    pub fn subscribe_contract(&mut self, instance_id: &[u8; 32]) -> bool {
        #[cfg(target_family = "wasm")]
        {
            let result =
                unsafe { __frnt__delegate__subscribe_contract(instance_id.as_ptr() as i64, 32) };
            result == 0
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = instance_id;
            false
        }
    }

    // ========================================================================
    // Delegate management methods (V2 — create child delegates)
    // ========================================================================

    /// Create a new delegate from WASM code and parameters.
    ///
    /// The new delegate is registered on the local node with the given cipher
    /// and nonce for secret encryption. The WASM bytes should be versioned
    /// delegate code (same format as file-based delegates).
    ///
    /// Returns `Ok((key_bytes, code_hash_bytes))` on success, where:
    /// - `key_bytes`: 32-byte delegate key hash
    /// - `code_hash_bytes`: 32-byte code hash
    ///
    /// Returns `Err(error_code)` on failure. See [`error_codes`] for values.
    pub fn create_delegate(
        &mut self,
        wasm_code: &[u8],
        params: &[u8],
        cipher: &[u8; 32],
        nonce: &[u8; 24],
    ) -> Result<([u8; 32], [u8; 32]), i32> {
        #[cfg(target_family = "wasm")]
        {
            let mut key_buf = [0u8; 32];
            let mut hash_buf = [0u8; 32];
            let result = unsafe {
                __frnt__delegate__create_delegate(
                    wasm_code.as_ptr() as i64,
                    wasm_code.len() as i64,
                    params.as_ptr() as i64,
                    params.len() as i32,
                    cipher.as_ptr() as i64,
                    nonce.as_ptr() as i64,
                    key_buf.as_mut_ptr() as i64,
                    hash_buf.as_mut_ptr() as i64,
                )
            };
            if result == 0 {
                Ok((key_buf, hash_buf))
            } else {
                Err(result)
            }
        }
        #[cfg(not(target_family = "wasm"))]
        {
            let _ = (wasm_code, params, cipher, nonce);
            Err(error_codes::ERR_NOT_IN_PROCESS)
        }
    }
}

impl std::fmt::Debug for DelegateCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegateCtx")
            .field("context_len", &self.len())
            .finish_non_exhaustive()
    }
}
