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
//! ## V2 (New — synchronous host functions for async operations)
//!
//! Delegates still implement `process()`, but the `DelegateCtx` gains new host
//! functions for contract access:
//!
//! ```text
//! ctx.get_contract_state(contract_id)  → Option<Vec<u8>>
//! ```
//!
//! From the WASM delegate's perspective, these calls are synchronous — the
//! delegate simply calls the function and gets the result back immediately.
//! Behind the scenes, the host runtime reads from the local state store
//! (ReDb) synchronously on the calling thread.
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

    /// V2: Host function-based contract access.
    ///
    /// Delegates call `ctx.get_contract_state()` directly during `process()`.
    /// The runtime handles the state lookup synchronously via the local store.
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
}
