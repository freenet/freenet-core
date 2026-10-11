//! The contract entry points the verifier is allowed to use.
//!
//! The verifier talks to contracts only through [`ConformanceOracle`]. That buys two
//! things. First, the property logic is testable without WASM: a pure-Rust fake
//! oracle can be made to violate exactly one law, which is how we prove each check
//! actually fires (a check that cannot fail is worse than no check). Second, the
//! node-side and `fdev`-side paths cannot drift, because there is only one
//! implementation of the laws and it does not know which one it is running under.

use freenet_stdlib::prelude::{RelatedContracts, UpdateData, UpdateModification, ValidateResult};

/// What broke when a contract call failed.
///
/// The distinction is only used for reporting: all three map to
/// [`Inconclusive`](super::Inconclusive), because none of them is a merge-law proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OracleErrorKind {
    /// The contract itself returned an error (rejected the input).
    Contract,
    /// Fuel, memory or time budget exhausted.
    Resource,
    /// The host or the WASM module failed (trap, missing export, store error).
    Runtime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleError {
    pub kind: OracleErrorKind,
    pub message: String,
}

impl OracleError {
    pub fn contract(message: impl Into<String>) -> Self {
        Self {
            kind: OracleErrorKind::Contract,
            message: message.into(),
        }
    }

    pub fn resource(message: impl Into<String>) -> Self {
        Self {
            kind: OracleErrorKind::Resource,
            message: message.into(),
        }
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self {
            kind: OracleErrorKind::Runtime,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for OracleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for OracleError {}

/// One deployed contract, pinned to its exact code and parameters, callable
/// repeatedly on arbitrary states.
///
/// Implementations must be *pure with respect to the node*: nothing a verifier does
/// through this trait may touch hosted state, emit network traffic, or otherwise be
/// observable outside the check. Conformance checking is untrusted auxiliary work.
pub trait ConformanceOracle {
    /// Does the contract consider these bytes a valid state?
    fn validate_state(
        &mut self,
        state: &[u8],
        related: &RelatedContracts<'_>,
    ) -> Result<ValidateResult, OracleError>;

    /// Apply updates to a state. `merge(a, b)` is this call with
    /// `[UpdateData::State(b)]`.
    fn update_state(
        &mut self,
        state: &[u8],
        updates: &[UpdateData<'_>],
    ) -> Result<UpdateModification<'static>, OracleError>;

    fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError>;

    fn get_state_delta(&mut self, state: &[u8], summary: &[u8]) -> Result<Vec<u8>, OracleError>;

    /// Drop any cached WASM instance so the next call starts from a fresh one.
    ///
    /// The default is a no-op, and for the production runtime that is correct:
    /// measured on this runtime, each contract call gets a fresh instance from a
    /// cached *module*, so nothing leaks between calls to reset. It follows that a
    /// contract cannot be nondeterministic across calls from its own internal state
    /// alone — a pure function of `(parameters, state)` has nothing to vary on. The
    /// determinism checks are therefore aimed at **host-provided** nondeterminism,
    /// of which the clock (`freenet_stdlib::time::now`) is the realistic instance,
    /// and which is exactly the #4857 class.
    ///
    /// The hook stays because a fake oracle can and does carry state between calls,
    /// and because a future runtime that pools instances would make within-instance
    /// leakage reachable again. If that happens, this is where it gets handled, and
    /// the determinism checks start covering a second class of bug for free.
    fn reset_instance(&mut self) {}
}
