//! Contract conformance: the shared verifier and evidence model (RFC #5320).
//!
//! Freenet contract state must form a join-semilattice: merging is associative,
//! commutative and idempotent, and because delivery is at-least-once, applying the
//! same delta twice must be harmless. A contract that violates these laws leaves
//! honest peers permanently divergent and burns unbounded network resources trying
//! to repair a state that cannot converge. #5153 measured deployed contracts doing
//! exactly that.
//!
//! This module is the *single* implementation of what "conformance" means. The
//! offline `fdev conformance` harness and (later) the node-side checker both call
//! [`verify_case`], so there is no way for the developer-facing answer and the
//! network-facing answer to drift apart.
//!
//! # Structure
//!
//! - [`property`] — the laws themselves, and what an outcome of checking one looks like.
//! - [`oracle`] — the four contract entry points, behind a trait so the verifier can be
//!   tested against pure-Rust fakes with no WASM in the loop.
//! - [`verifier`] — executes one property against one oracle. Pure and replayable.
//! - [`runtime_oracle`] — the production oracle: a real wasmtime runtime over real WASM.
//! - [`evidence`] — the self-contained, bounded reproducer that travels between peers.
//! - [`bundle`] — the offline replay corpus format.
//! - [`generator`] — turns a corpus of observed states into cases to check.
//!
//! # The bias toward `Inconclusive`
//!
//! Every check has three outcomes, not two. A contract that errors, asks for a
//! related contract, or is handed a state it considers invalid produces
//! [`PropertyOutcome::Inconclusive`] — never a violation. This is deliberate and it
//! is the most important design decision in the module: the only enforcement
//! mechanism ever shipped for this class of problem (#4295) had a 100% false-positive
//! rate in production, and a violation here is eventually meant to justify deleting a
//! contract. A missed violation costs bandwidth. A false violation deletes a working
//! application. The asymmetry is not close, so anything short of "both sides ran to
//! completion and the canonical bytes differ" is inconclusive.

pub mod bundle;
pub mod evidence;
pub mod generator;
pub mod oracle;
pub mod property;
pub mod runtime_oracle;
pub mod verifier;

#[cfg(test)]
mod tests;

pub use bundle::ReplayBundle;
pub use evidence::{ConformanceEvidence, EvidenceId, EvidenceRejected};
pub use generator::{GeneratorConfig, generate_cases};
pub use oracle::{ConformanceOracle, OracleError, OracleErrorKind};
pub use property::{
    ConformanceProperty, Inconclusive, OutputDigest, PropertyOutcome, Severity, Violation,
};
pub use runtime_oracle::RuntimeOracle;
pub use verifier::{ConformanceCase, verify_case};
