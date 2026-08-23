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
//!   6 NEVER_SETTLES            — merge rewrites the state on every apply, so
//!                                merge(A, A) never reaches a fixpoint. Idempotence
//!                                and associativity break; commutativity still HOLDS
//!                                here, because the rewrite is applied to a union and
//!                                a union is symmetric. Modelled on a live contract
//!                                whose states were all a fixed 3488 bytes and whose
//!                                content changed on every re-apply — that one broke
//!                                commutativity too, for reasons this arm does not
//!                                reproduce.
//!   7 REQUIRES_RELATED         — validate_state cannot decide without another
//!                                contract's state and asks for it. The merge itself
//!                                conforms. Proves the verifier declines rather than
//!                                accuses when the state is missing, and reaches a
//!                                real verdict once it is supplied.
//!   8 PATH_DISAGREEMENT        — the two write paths use different combinators on a
//!                                key collision: the delta path takes the last write,
//!                                the merge path the first. Each path is a perfectly
//!                                good semilattice ON ITS OWN, so every existing law
//!                                holds and only `path_agreement` can see it. The
//!                                #5394 shape.
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

//! ## Why mode 8 needs a different state shape from the rest
//!
//! Every other mode's state is a plain byte SET, and a set has no way to express
//! "two writes to the same slot" — which is the only situation in which a
//! last-write-wins and a first-write-wins rule can disagree. So mode 8 reads each
//! byte as a key/value pair (high nibble / low nibble) and its state is a MAP: at
//! most one byte per key. That is the same shape as the real defect, a map keyed by
//! a client-chosen sequence number, and the collision is two ops carrying the same
//! sequence number with different payloads.
//!
//! The two rules are the two halves of the real bug written in Rust's own idiom:
//! `entry(key).or_insert(value)` keeps whatever is already there (first write wins)
//! and is what the merge path used; `insert(key, value)` overwrites it (last write
//! wins) and is what the direct-apply path used.

use freenet_stdlib::prelude::*;

const CONFORMING: u8 = 0;
const LAST_WRITE_WINS: u8 = 1;
const MUTUAL_REJECTION: u8 = 2;
const NON_IDEMPOTENT_DELTA: u8 = 3;
const NONDETERMINISTIC_SUMMARY: u8 = 4;
const CAPPED_SET: u8 = 5;
const NEVER_SETTLES: u8 = 6;
const REQUIRES_RELATED: u8 = 7;
const PATH_DISAGREEMENT: u8 = 8;

/// The contract [`REQUIRES_RELATED`] insists on knowing about, as a fixed id so a
/// test can supply its state without deriving anything.
const RELATED_ID: [u8; 32] = [7; 32];

/// How many entries [`CAPPED_SET`] keeps. Small so a three-state case overflows it.
const CAP: usize = 3;

struct Contract;

fn mode(parameters: &Parameters<'static>) -> u8 {
    parameters.as_ref().first().copied().unwrap_or(CONFORMING)
}

fn is_canonical(state: &[u8]) -> bool {
    state.windows(2).all(|w| w[0] < w[1])
}

/// The key half of a [`PATH_DISAGREEMENT`] entry. The low nibble is the value.
fn key(entry: u8) -> u8 {
    entry >> 4
}

/// A [`PATH_DISAGREEMENT`] state is a MAP: canonical, and at most one entry per key.
///
/// Without the second condition a state could carry two writes to the same key at
/// once, which is not a state any of the paths below can produce and not the shape
/// the real defect had.
fn is_canonical_map(state: &[u8]) -> bool {
    is_canonical(state) && state.windows(2).all(|w| key(w[0]) != key(w[1]))
}

/// Collapse key collisions in a byte set, keeping either the first or the last
/// entry for each key as the set is walked in ascending order.
///
/// `keep_last == false` is `entry(key).or_insert(value)`: whatever is already
/// there stays. `keep_last == true` is `insert(key, value)`: the newcomer wins.
/// Both are deterministic and depend only on the union of the two inputs, which is
/// what leaves every existing law intact — see the mode's comment in `merge_state`.
fn collapse_by_key(entries: &[u8], keep_last: bool) -> Vec<u8> {
    let mut sorted: Vec<u8> = entries.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut out: Vec<u8> = Vec::new();
    for entry in sorted {
        match out.last().copied() {
            Some(previous) if key(previous) == key(entry) => {
                if keep_last {
                    let last = out.len() - 1;
                    out[last] = entry;
                }
            }
            _ => out.push(entry),
        }
    }
    out
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
        // Rewrite in place on every apply: same length, different content, and no
        // fixpoint. Deterministic, so it is not mistaken for nondeterminism — the
        // shape seen in the wild, where a contract that keeps a derived field
        // (a running digest, a sequence number) recomputes it on every merge.
        //
        // At-least-once delivery makes this fatal rather than untidy: the same state
        // redelivered mutates the result again, so the peer never stops producing
        // "new" state to gossip and can never agree with anyone.
        //
        // Adding one is a bijection on the byte alphabet, so the rotation can never
        // collapse two entries into one and the state never settles — EXCEPT for the
        // two sets that are invariant under a 256-cycle: the empty set (returned
        // early below) and the full 0..=255 alphabet. Neither is reachable from the
        // small states this fixture is driven with, but anyone reusing this arm with
        // a generated corpus should know the second one exists.
        NEVER_SETTLES => {
            let mut out = union(current, incoming);
            if out.is_empty() {
                return out;
            }
            // Rotate the byte values within the set's own alphabet, keeping the
            // state canonical (sorted, deduplicated) and the same length, so the
            // change cannot be dismissed as a serialization artifact.
            let rotated: Vec<u8> = out.iter().map(|b| b.wrapping_add(1)).collect();
            out = rotated;
            out.sort_unstable();
            out.dedup();
            out
        }
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
        // First write wins on a key collision — `entry(key).or_insert(value)`.
        //
        // Resolving against the UNION rather than against "whichever argument the
        // value came from" is what makes this mode isolate `path_agreement` alone,
        // and it is not a softening of the real defect: the real merge folded the
        // incoming ops into a map with `or_insert`, so the surviving op was decided
        // by iteration order over the combined set, not by which peer supplied it.
        //
        // The consequence is that this merge is min-per-key, which is a genuine
        // meet-semilattice: commutative, associative and idempotent. Every existing
        // law therefore HOLDS here, which is the entire point of the mode — the gap
        // #5394 describes is a contract that satisfies the whole property set and
        // still diverges, and a fixture that also broke commutativity would prove
        // nothing about the gap.
        PATH_DISAGREEMENT => collapse_by_key(&union(current, incoming), false),
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
        // Last write wins on a key collision — `insert(key, value)`.
        //
        // Max-per-key, which is just as sound a semilattice as the merge path's
        // min-per-key: applying deltas is idempotent and order-independent, so
        // `delta_idempotence` and `delta_permutation_invariance` both hold. The two
        // paths are individually impeccable and disagree with each other, which no
        // property that compares merge-to-merge or delta-to-delta can see.
        PATH_DISAGREEMENT => collapse_by_key(&union(current, delta), true),
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
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        // Mode 7 cannot decide alone: it needs another contract's state, as a
        // contract enforcing an authorization or membership relation does. Asking is
        // the honest answer, and it is what makes the contract unjudgeable until the
        // capture path carries that state.
        if mode(&parameters) == REQUIRES_RELATED {
            let wanted = ContractInstanceId::new(RELATED_ID);
            let supplied = related
                .states()
                .any(|(id, state)| *id == wanted && state.is_some());
            if !supplied {
                return Ok(ValidateResult::RequestRelated(vec![wanted]));
            }
        }

        // Mode 8's state is a map, not a set: see the module comment.
        let valid = if mode(&parameters) == PATH_DISAGREEMENT {
            is_canonical_map(state.as_ref())
        } else {
            is_canonical(state.as_ref())
        };
        if valid {
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
