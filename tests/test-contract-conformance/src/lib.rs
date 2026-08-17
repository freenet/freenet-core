//! Deliberately-defective conformance test contract (RFC #5320).
//!
//! One WASM contract whose merge behaviour is selected by its single-byte
//! `Parameters`, so the shared conformance verifier
//! (`crates/core/src/conformance/`) can be proven against real wasmtime
//! execution, not only against the pure-Rust fakes in
//! `crates/core/src/conformance/tests.rs`. Using one crate with a behaviour
//! parameter (rather than five crates) is deliberate: it also exercises that
//! the same WASM code under different parameters is a different contract
//! instance, which the evidence model depends on.
//!
//! Modes, selected by `Parameters[0]`:
//!
//!   0 CONFORMING              — a genuine join-semilattice (set union). Baseline.
//!   1 LAST_WRITE_WINS         — merge(A, B) discards A and keeps B outright.
//!                                One of the two production shapes behind #5153.
//!   2 MUTUAL_REJECTION        — merge(A, B) == A and merge(B, A) == B: neither
//!                                side ever adopts the other. The other #5153 shape.
//!   3 NON_IDEMPOTENT_DELTA    — applying the same delta twice keeps growing the
//!                                state instead of the second application being a
//!                                no-op, which at-least-once delivery makes fatal.
//!   4 NONDETERMINISTIC_SUMMARY — summarize_state depends on something other than
//!                                the state: real wall-clock time, the #4857 class.
//!   5 CAPPED_SET               — merge caps the collection at N entries, evicting
//!                                by something other than the merge's own ordering.
//!                                Commutative and idempotent; only a three-state
//!                                case reveals it. The one mode the pairwise laws
//!                                cannot catch.
//!
//! State is always a canonical byte set: sorted, strictly ascending, no
//! duplicates. `validate_state` accepts exactly that canonical form.
//!
//! ## Why mode 4 uses the host clock, not a WASM-local counter
//!
//! An earlier version of this contract tried to fake nondeterminism with a
//! `thread_local` call counter, on the theory that "WASM has no clock or RNG, so
//! that's the only source available." That is wrong in a way worth recording: the
//! runtime creates a brand-new WASM instance for every single host call
//! (`prepare_contract_call` / `drop_running_instance` in
//! `crates/core/src/wasm_runtime/runtime.rs` — the compiled *module* is cached,
//! the *instance* never is), so any counter or static local to the module resets
//! to its initial value on every call. Two calls to `summarize_state` with
//! byte-identical `(parameters, state)` are then providably byte-identical too —
//! there is nothing left for the nondeterminism to come from. `freenet_stdlib`
//! does expose a real host clock (`freenet_stdlib::time::now`), which is the one
//! genuine source of "something other than the declared inputs" available to a
//! contract, and it is also the realistic shape of the bug: an author reaching
//! for a timestamp to break ties in a summary.

use freenet_stdlib::prelude::*;

const CONFORMING: u8 = 0;
const LAST_WRITE_WINS: u8 = 1;
const MUTUAL_REJECTION: u8 = 2;
const NON_IDEMPOTENT_DELTA: u8 = 3;
const NONDETERMINISTIC_SUMMARY: u8 = 4;
const CAPPED_SET: u8 = 5;

/// How many entries [`CAPPED_SET`] keeps. Small so a three-state case overflows it.
const CAP: usize = 3;

struct Contract;

fn mode(parameters: &Parameters<'static>) -> u8 {
    parameters.as_ref().first().copied().unwrap_or(CONFORMING)
}

fn is_canonical(state: &[u8]) -> bool {
    state.windows(2).all(|w| w[0] < w[1])
}

fn union(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = a.iter().chain(b.iter()).copied().collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn difference(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().copied().filter(|x| !b.contains(x)).collect()
}

/// `merge(current, incoming)` for a full-state `UpdateData::State` input.
fn merge_state(m: u8, current: &[u8], incoming: &[u8]) -> Vec<u8> {
    match m {
        LAST_WRITE_WINS => incoming.to_vec(),
        MUTUAL_REJECTION => current.to_vec(),
        // Union, then evict down to CAP entries.
        //
        // This is the "cap the collection at N" rule real applications write, and
        // it is why associativity needs a three-state check of its own rather
        // than being assumed to follow from the pairwise laws.
        //
        // The eviction has to pick its victim from the whole set, not from the
        // ordering the merge is defined over. Keeping the largest N would be
        // associative — a discarded entry can never re-enter the top N, so the
        // order of merging cannot matter. It is when the thing evicted by
        // (arrival order, a timestamp, a hash) is independent of the merge's own
        // ordering that dropping an entry early destroys information a different
        // merge order would have kept. Here the victim's index is derived from
        // the set's contents, which stands in for that.
        //
        // The pairwise laws still hold: the result depends only on the union, so
        // it is commutative, and a state already at or under the cap merged with
        // itself is unchanged, so it is idempotent. Only the triple reveals it.
        CAPPED_SET => {
            let mut out = union(current, incoming);
            while out.len() > CAP {
                let sum: usize = out.iter().map(|b| *b as usize).sum();
                out.remove(sum % out.len());
            }
            out
        }
        // CONFORMING, NON_IDEMPOTENT_DELTA and NONDETERMINISTIC_SUMMARY all
        // merge full-state inputs conformingly: their defects are scoped to
        // delta application and summarization respectively, so isolating each
        // mode to exactly one broken operation is what lets the verifier
        // attribute a violation to the right property instead of several
        // properties firing at once for the same underlying bug.
        _ => union(current, incoming),
    }
}

/// `apply(current, delta)` for an `UpdateData::Delta` input.
fn apply_delta(m: u8, current: &[u8], delta: &[u8]) -> Vec<u8> {
    match m {
        MUTUAL_REJECTION => current.to_vec(),
        NON_IDEMPOTENT_DELTA => {
            // Deliberately not deduplicated/sorted: re-delivering the same
            // delta appends it again instead of being a no-op the second time.
            // This is a realistic defect shape, not a contrived one — a
            // contract that treats a delta as "append these bytes" rather
            // than "add these bytes to the set" emits non-canonical state.
            let mut out = current.to_vec();
            out.extend_from_slice(delta);
            out
        }
        _ => union(current, delta),
    }
}

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        if is_canonical(state.as_ref()) {
            Ok(ValidateResult::Valid)
        } else {
            Ok(ValidateResult::Invalid)
        }
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let m = mode(&parameters);
        let mut current = state.as_ref().to_vec();
        for update in data {
            current = match update {
                UpdateData::State(incoming) => merge_state(m, &current, incoming.as_ref()),
                UpdateData::Delta(delta) => apply_delta(m, &current, delta.as_ref()),
                UpdateData::StateAndDelta { .. }
                | UpdateData::RelatedState { .. }
                | UpdateData::RelatedDelta { .. }
                | UpdateData::RelatedStateAndDelta { .. } => {
                    return Err(ContractError::InvalidUpdate);
                }
                // `UpdateData` is `#[non_exhaustive]`: a future variant this
                // contract does not know about is handled the same as the
                // other unsupported shapes above.
                _ => return Err(ContractError::InvalidUpdate),
            };
        }
        Ok(UpdateModification::valid(State::from(current)))
    }

    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let mut summary = state.as_ref().to_vec();
        if mode(&parameters) == NONDETERMINISTIC_SUMMARY {
            // Real wall-clock time genuinely differs between two calls, unlike
            // anything computed purely from `(parameters, state)` in a freshly
            // instantiated WASM module — see the module doc comment.
            //
            // Append the FULL nanosecond timestamp, not a single truncated byte:
            // a truncated byte gives two calls a ~1-in-256 chance of colliding,
            // and the determinism check repeats the call and compares, so a
            // collision on every repeat makes the check silently miss its own
            // planted defect (~1 run in 65,000). The full i64 makes two calls
            // separated by an entire WASM instantiation collide only if the
            // host clock does not advance at all between them, which does not
            // happen on Linux at this granularity.
            let now = freenet_stdlib::time::now();
            summary.extend_from_slice(&now.timestamp_nanos_opt().unwrap_or_default().to_le_bytes());
        }
        Ok(StateSummary::from(summary))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        Ok(StateDelta::from(difference(
            state.as_ref(),
            summary.as_ref(),
        )))
    }
}
