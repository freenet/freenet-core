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
//! - [`minimize`] — shrinks a failing case to the smallest witness that still fails,
//!   so evidence fits its size bound and reads as a usable bug report.
//! - [`sampler`] — the bounded, restart-safe store of states a peer observed.
//! - [`focus`] — which contracts a peer watches, and when it moves on.
//! - [`policy`] — what a peer is permitted to do about a finding. Deletion is the
//!   last step of the RFC's deployment plan, and this is where that ordering is
//!   enforced and tested rather than merely intended.
//!
//! # Relationship to the existing probe
//!
//! `contract::executor::runtime::executor_impl::maybe_probe_idempotency` already
//! samples one merge in 32 and checks re-apply idempotence. Two differences matter.
//!
//! It compares with `byte_multiset_eq` rather than byte equality, because states
//! were not guaranteed to have a canonical encoding, so a strict comparison would
//! have flagged contracts that merely reordered their own output. The RFC makes
//! canonical serialization an explicit platform requirement, which is what lets this
//! module compare exact bytes and therefore say something much stronger.
//!
//! It also checks exactly one law. Commutativity, associativity and reconciliation
//! are where the measured production damage actually came from (#5153), and a
//! re-apply probe cannot see any of them.
//!
//! This module does not replace or disable that probe. Nothing here is wired into
//! the node yet.
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
pub mod focus;
pub mod generator;
pub mod minimize;
pub mod oracle;
pub mod policy;
pub mod property;
pub mod runtime_oracle;
pub mod sampler;
pub mod verifier;

#[cfg(test)]
mod sampler_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod wasm_tests;

pub use bundle::ReplayBundle;
pub use evidence::{ConformanceEvidence, EvidenceId, EvidenceRejected};
pub use focus::FocusSelector;
pub use generator::{GeneratorConfig, generate_cases};
pub use minimize::{MinimizeConfig, MinimizeReport, minimize};
pub use oracle::{ConformanceOracle, OracleError, OracleErrorKind};
pub use policy::{ConformanceAction, EnforcementMode, decide};
pub use property::{
    ConformanceProperty, Inconclusive, OutputDigest, PropertyOutcome, Severity, Violation,
};
pub use runtime_oracle::{OracleBuildError, RuntimeOracle};
pub use sampler::{Admission, ContractSampler, SamplerConfig, Stratum};
pub use verifier::{ConformanceCase, verify_case};
