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
//! offline `fdev verify-merge` harness and (later) the node-side checker both call
//! [`verify_case`], so the developer-facing answer and the network-facing answer
//! cannot disagree.
//!
//! That is a property of who calls what, not something the type system enforces:
//! [`Violation`]'s fields are public, so a future node-side integration could in
//! principle construct one directly instead of going through [`verify_case`]. Today
//! nothing does, and nothing should — a second construction site is the exact shape
//! the drift would take.
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
//! - [`capture`] — operator-enabled recording of real contract traffic for replay.
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
//! application. The asymmetry is not close.
//!
//! For the eight byte-comparison laws — the merge laws and the determinism checks —
//! that means anything short of "both sides ran to completion and the canonical
//! bytes differ" is inconclusive. Three properties reach a verdict another way and
//! are worth naming rather than glossing: [`ConformanceProperty::EmittedStateValidity`]
//! fires on the contract's own `Invalid` verdict, the two self-delta checks fire on a
//! size threshold (and are [`Severity::Diagnostic`], so they can never justify
//! removal), and [`ConformanceProperty::ReconciliationCycle`] fires on a repeated
//! state pair across simulated rounds.
//!
//! A violation is also required to reproduce: [`verify_case`] re-runs any failing
//! check and, if the second run disagrees, re-reports under
//! [`ConformanceProperty::UpdateDeterminism`] rather than under the law the first
//! run suspected — see [`verify_case`] for why silence would be worse.
//!
//! Be precise about what that buys, because it is easy to over-read. It stops a
//! contract being accused of breaking the WRONG law when the real problem is that
//! its output varies. It does NOT exonerate a clock-reading contract:
//!
//! - If the clock asymmetry is REPRODUCIBLE — the in-tree ping contract filters
//!   expired entries out of the incoming side but not its own, so `merge(A, B)` and
//!   `merge(B, A)` differ permanently — both runs agree and the finding stands as a
//!   commutativity violation. That is correct: the merge really is order-dependent.
//!   The re-run was never going to save it.
//! - If the clock straddle is a one-off, the second run disagrees and the finding
//!   becomes `UpdateDeterminism`, which is itself enforceable. So a contract that
//!   stamps `now()` into merged state is flagged either way; only the name changes.
//!
//! The honest summary is that the re-run protects the ACCURACY of the accusation,
//! not the contract.

pub mod bundle;
pub mod capture;
pub mod evidence;
pub mod focus;
pub mod generator;
pub mod minimize;
pub mod oracle;
pub mod policy;
pub mod property;
pub mod runtime_oracle;
pub mod sampler;
pub mod shadow;
pub mod verifier;

#[cfg(test)]
mod sampler_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod wasm_tests;

pub use bundle::ReplayBundle;
pub use capture::{CaptureHandle, Observation};
pub use evidence::{ConformanceEvidence, EVIDENCE_SCHEMA_VERSION, EvidenceId, EvidenceRejected};
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
