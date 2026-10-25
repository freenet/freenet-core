//! The decision rule of the sampled re-apply probe
//! (`Executor::maybe_probe_idempotency`).
//!
//! The probe holds a stored state `S`, an incoming full state `X` and the merge
//! the node just accepted, `merged = merge(S, X)`. It re-applies `X` and asks
//! whether the contract settles. `merge(merged, X)` is the left grouping of the
//! triple `(S, X, X)`. The right grouping is `merge(S, merge(X, X))`, and since
//! the node's merge path answers a merge of a state into an identical copy of
//! itself without calling the contract (the byte-identical fast path),
//! `merge(X, X)` is `X` on the network and the right grouping is `merged`. A content change on re-apply
//! is therefore a strict associativity failure on that triple. The conformance
//! verifier reports such a failure only when the groupings do not heal (#5915),
//! and this rule uses the verifier's own heal step ([`associativity_heals`]) to
//! decide the same question.
//!
//! # Every flag rests on states the network produces
//!
//! Every merge this rule asks for is one the node would make, and no merge of a
//! state into an identical copy of itself reaches the contract: the probe answers
//! those as the node's merge path does, with the state unchanged. The heal step starts from
//! `merged` (the state the node just committed) and the re-apply (what a node
//! holding `merged` computes when `X` is delivered again), and merges in `S` and
//! `X`. Every state it touches is one of those or a merge of them, and every call
//! is a merge two peers holding those states would make. Among merge results the
//! contract accepts, a non-settling verdict is a non-settling merge on the
//! network, and how the contract would answer a call the merge path never makes
//! cannot change it. The probe does not validate the re-apply or the heal-step
//! results, which the node would refuse to commit if invalid.
//!
//! # Where this differs from `verify_case(StateAssociativity, [S, X, X])`
//!
//! - A re-apply that leaves `merged` unchanged, or only reorders its bytes (same
//!   byte multiset, the #4295 class), ends the probe.
//! - The verifier calls the contract for `merge(X, X)` and for any merge of a
//!   state into an identical copy inside the heal step; this rule answers those
//!   with the state unchanged. The two agree for any contract whose merge is
//!   idempotent on identical inputs. Otherwise the rule can be stricter (an
//!   additive merge, which moves on every apply, is decided "settles" by the
//!   verifier when `merge(X, X)` adds `X` to itself) or more lenient, and the
//!   skipped call can also steer the heal step to a cycle where the verifier
//!   would heal. In every case the rule follows what the node does.
//! - The verifier checks that the stored state and the groupings are valid. The
//!   stored state was validated when the node stored it, and the merge path has
//!   validated `merged`, so this rule does not ask again; it does not check the
//!   re-apply's validity either, and does not flag for it.
//! - A heal step the contract refuses, and running out of heal passes, are
//!   inconclusive here. The verifier reports a refusal as a violation.
//! - The work is bounded by [`REAPPLY_PROBE_MAX_CALLS`] and
//!   [`REAPPLY_PROBE_TIME_BUDGET`]; past either the rule is inconclusive.
//!
//! `reapply_verdict_matches_verifier_on_s_x_x` pins that the rule agrees with
//! the verifier run on an oracle that answers identical merges the same way.
//!
//! The heal step compares states in exact bytes, as the verifier does, so a
//! contract whose encoding depends on argument order (an insertion-ordered map)
//! and that also fails strict associativity can be flagged for byte order alone.
//! Only the first re-apply comparison tolerates a reordering.
//!
//! # Cost
//!
//! A re-apply that leaves the state unchanged or reordered costs one contract
//! call, or none when `merged` is `X` itself. When it changes the content, the
//! probe makes up to [`REAPPLY_PROBE_MAX_CALLS`] calls. Only the re-apply is
//! untimed; [`REAPPLY_PROBE_TIME_BUDGET`] covers the heal step and is checked
//! before each call, so the last call may run past it by up to the per-call
//! execution limit (`max_execution_seconds`). A contract that changes once on
//! re-apply and then settles (bounded retention) typically pays up to eight
//! calls on a sampled probe that changes its content: the re-apply, two
//! convergence merges and up to five absorbing merges.
//!
//! # Residual: a contract whose calls are slow
//!
//! A contract that spends at least [`REAPPLY_PROBE_TIME_BUDGET`] in heal-step
//! merges is never decided here, on any sampled probe. This is not only a
//! contract that is slow on purpose. Groupings that do not converge are reported
//! after two timed calls, so that verdict is reachable while a merge costs less
//! than about 250 ms. Reporting that the groupings heal takes up to seven timed
//! calls (fewer when the reconciled state equals an input it absorbs), so it is
//! reached only while merges cost less than about 41 ms each, and a moving
//! re-delivery or a cycle can take more. A violator decided by neither is caught
//! only by the identical-input probe, which needs an echo re-push. The node still
//! pays for those merges, and they are charged to the contract on the
//! `ExecCpuMicros` axis, so cost-aware eviction (#4861) is what bounds a
//! contract that burns executor time this way.
//!
//! # Residual: inconclusive outcomes are not counted
//!
//! An inconclusive verdict is logged at DEBUG, which release builds compile out,
//! and is not counted anywhere. How often the probe cannot decide (budget, heal
//! refusals, call failures) is therefore not visible in production; adding a
//! counter needs a telemetry field that does not exist yet.

use std::time::Duration;

use tokio::time::Instant;

use freenet_stdlib::prelude::{RelatedContracts, UpdateData, UpdateModification, ValidateResult};

use super::byte_multiset_eq;
use crate::conformance::oracle::{ConformanceOracle, OracleError};
use crate::conformance::verifier::{Heal, MAX_HEAL_PASSES, associativity_heals};
use crate::util::time_source::TimeSource;

/// Contract calls one probe may make, the re-apply included. This is the rule's
/// worst case: the re-apply, then the heal step's two convergence merges and at
/// most nine merges per pass. A pass merges in `L`, `R`, `S`, `X` and `X` again,
/// and a step that moves the state costs two merges (the step and its
/// re-delivery). The second `X` repeats the call that ended the step before it,
/// so for a deterministic contract it never moves; the verifier's general bound
/// of ten per pass assumes five distinct inputs. Derived from
/// `MAX_HEAL_PASSES`, so raising that raises the cap rather than turning late
/// verdicts into budget exhaustion; the longest path is pinned by
/// `the_longest_path_uses_exactly_the_call_cap`.
pub(crate) const REAPPLY_PROBE_MAX_CALLS: usize = 1 + 2 + 9 * MAX_HEAL_PASSES;

/// Wall-clock time the heal step may spend. The re-apply before it is one merge,
/// the cost every probe pays, and is not timed. Checked before each later call,
/// so the last call may run past it by up to the per-call execution limit; what
/// this bounds is the number of calls made after the budget is spent, not the
/// probe's total time (see the module doc's cost section). Under simulated time
/// contract calls take no time, so only [`REAPPLY_PROBE_MAX_CALLS`] binds there.
pub(crate) const REAPPLY_PROBE_TIME_BUDGET: Duration = Duration::from_millis(250);

/// Why the merge is known not to settle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonSettling {
    /// The two groupings do not reach one state in one exchange.
    NotConverged,
    /// Re-delivering an input the reconciled state had just absorbed moved it
    /// again.
    Redelivery,
    /// Reconciliation revisited a state before reaching a fixpoint.
    Cycle,
}

impl NonSettling {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            NonSettling::NotConverged => "groupings_do_not_converge",
            NonSettling::Redelivery => "redelivery_keeps_moving",
            NonSettling::Cycle => "reconciliation_cycles",
        }
    }
}

/// Why the rule could not decide. None of these flags a contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Inconclusive {
    /// The re-apply failed (contract error, trap, resource limit).
    ReapplyFailed,
    /// The re-apply returned no state.
    NoOutputState,
    /// [`REAPPLY_PROBE_MAX_CALLS`] or [`REAPPLY_PROBE_TIME_BUDGET`] was spent.
    BudgetExhausted,
    /// The contract refused a heal-step merge.
    HealRefused,
    /// The heal step ran out of passes without a fixpoint or a repeat.
    OutOfPasses,
    /// A heal-step merge failed other than by the contract refusing it
    /// (runtime error, resource limit, or a merge needing related state).
    CallFailed,
}

/// The rule's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReapplyVerdict {
    /// The re-apply left the state byte-identical.
    Unchanged,
    /// The re-apply only reordered the bytes.
    Reordered,
    /// The re-apply changed the content and the groupings heal.
    Settles,
    /// The merge does not settle. The only verdict that flags.
    NonSettling(NonSettling),
    Inconclusive(Inconclusive),
}

/// Decide whether re-applying `incoming` to `merged = merge(stored, incoming)`
/// shows a merge that does not settle. `clock` bounds the time spent; without
/// one only the call count is bounded.
pub(crate) fn reapply_verdict<O: ConformanceOracle + ?Sized>(
    oracle: &mut O,
    clock: Option<&dyn TimeSource>,
    stored: &[u8],
    incoming: &[u8],
    merged: &[u8],
) -> ReapplyVerdict {
    let mut oracle = Budgeted::new(oracle, clock);
    let oracle = &mut oracle;

    // Re-delivering `X` to a node that holds `X` is the byte-identical fast
    // path: no contract call, no change.
    if merged == incoming {
        return ReapplyVerdict::Unchanged;
    }
    let reapplied = match oracle.update_state(merged, &[state_update(incoming)]) {
        Ok(UpdateModification {
            new_state: Some(state),
            ..
        }) => state.into_bytes(),
        Ok(_) => return ReapplyVerdict::Inconclusive(Inconclusive::NoOutputState),
        Err(_) => return ReapplyVerdict::Inconclusive(Inconclusive::ReapplyFailed),
    };
    if reapplied.as_slice() == merged {
        return ReapplyVerdict::Unchanged;
    }
    if byte_multiset_eq(merged, &reapplied) {
        return ReapplyVerdict::Reordered;
    }
    oracle.start_clock();

    let mut heal_oracle = IdenticalIsNoOp(oracle);
    match associativity_heals(
        &mut heal_oracle,
        &reapplied,
        merged,
        [stored, incoming, incoming],
    ) {
        Ok(Heal::Healed) => ReapplyVerdict::Settles,
        Ok(Heal::NotConverged) => ReapplyVerdict::NonSettling(NonSettling::NotConverged),
        Ok(Heal::Redelivery) => ReapplyVerdict::NonSettling(NonSettling::Redelivery),
        Ok(Heal::Cycle) => ReapplyVerdict::NonSettling(NonSettling::Cycle),
        Ok(Heal::Refused(_)) => ReapplyVerdict::Inconclusive(Inconclusive::HealRefused),
        Ok(Heal::OutOfPasses) => ReapplyVerdict::Inconclusive(Inconclusive::OutOfPasses),
        Err(_) => oracle.failed(),
    }
}

fn state_update(bytes: &[u8]) -> UpdateData<'static> {
    UpdateData::State(freenet_stdlib::prelude::State::from(bytes.to_vec()))
}

/// Answers a merge of a full state into an identical copy of itself with the
/// state unchanged, without calling the contract, as the node's byte-identical
/// fast path does. Every other call goes to the inner oracle.
pub(crate) struct IdenticalIsNoOp<'a, O: ?Sized>(pub(crate) &'a mut O);

impl<O: ConformanceOracle + ?Sized> ConformanceOracle for IdenticalIsNoOp<'_, O> {
    fn validate_state(
        &mut self,
        state: &[u8],
        related: &RelatedContracts<'_>,
    ) -> Result<ValidateResult, OracleError> {
        self.0.validate_state(state, related)
    }

    fn update_state(
        &mut self,
        state: &[u8],
        updates: &[UpdateData<'_>],
    ) -> Result<UpdateModification<'static>, OracleError> {
        if let [UpdateData::State(other)] = updates {
            if other.as_ref() == state {
                return Ok(UpdateModification::valid(state.to_vec().into()));
            }
        }
        self.0.update_state(state, updates)
    }

    fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
        self.0.summarize_state(state)
    }

    fn get_state_delta(&mut self, state: &[u8], summary: &[u8]) -> Result<Vec<u8>, OracleError> {
        self.0.get_state_delta(state, summary)
    }
}

/// Counts calls and elapsed time, and refuses calls past the budget.
struct Budgeted<'a, O: ?Sized> {
    inner: &'a mut O,
    clock: Option<&'a dyn TimeSource>,
    started: Option<Instant>,
    calls: usize,
    exhausted: bool,
}

impl<'a, O: ConformanceOracle + ?Sized> Budgeted<'a, O> {
    fn new(inner: &'a mut O, clock: Option<&'a dyn TimeSource>) -> Self {
        Self {
            inner,
            clock,
            started: None,
            calls: 0,
            exhausted: false,
        }
    }

    fn charge(&mut self) -> Result<(), OracleError> {
        let over_time = match (self.clock, self.started) {
            (Some(clock), Some(started)) => {
                clock.now().saturating_duration_since(started) >= REAPPLY_PROBE_TIME_BUDGET
            }
            _ => false,
        };
        if self.exhausted || over_time || self.calls >= REAPPLY_PROBE_MAX_CALLS {
            self.exhausted = true;
            return Err(OracleError::resource("reapply probe budget exhausted"));
        }
        self.calls += 1;
        Ok(())
    }

    /// Start timing: [`REAPPLY_PROBE_TIME_BUDGET`] covers calls from here on.
    fn start_clock(&mut self) {
        self.started = self.clock.map(|c| c.now());
    }

    /// The verdict for a failed call: the budget if it was spent, else a call failure.
    fn failed(&self) -> ReapplyVerdict {
        if self.exhausted {
            ReapplyVerdict::Inconclusive(Inconclusive::BudgetExhausted)
        } else {
            ReapplyVerdict::Inconclusive(Inconclusive::CallFailed)
        }
    }
}

impl<O: ConformanceOracle + ?Sized> ConformanceOracle for Budgeted<'_, O> {
    fn validate_state(
        &mut self,
        state: &[u8],
        related: &RelatedContracts<'_>,
    ) -> Result<ValidateResult, OracleError> {
        self.charge()?;
        self.inner.validate_state(state, related)
    }

    fn update_state(
        &mut self,
        state: &[u8],
        updates: &[UpdateData<'_>],
    ) -> Result<UpdateModification<'static>, OracleError> {
        self.charge()?;
        self.inner.update_state(state, updates)
    }

    fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
        self.charge()?;
        self.inner.summarize_state(state)
    }

    fn get_state_delta(&mut self, state: &[u8], summary: &[u8]) -> Result<Vec<u8>, OracleError> {
        self.charge()?;
        self.inner.get_state_delta(state, summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    type MergeFn = Box<dyn Fn(&[u8], &[u8]) -> Result<Vec<u8>, OracleError>>;
    type ValidFn = Box<dyn Fn(&[u8]) -> bool>;

    /// A contract given as a merge function and a validity predicate. Each call
    /// optionally advances `clock` by `call_cost`, to model slow merges.
    struct Fake {
        merge: MergeFn,
        valid: ValidFn,
        clock: Option<(SharedMockTimeSource, Duration)>,
        calls: usize,
    }

    impl Fake {
        fn new(merge: impl Fn(&[u8], &[u8]) -> Result<Vec<u8>, OracleError> + 'static) -> Self {
            Self {
                merge: Box::new(merge),
                valid: Box::new(|_| true),
                clock: None,
                calls: 0,
            }
        }

        fn table(table: [[u8; 4]; 4]) -> Self {
            Self::new(move |a, b| Ok(vec![table[a[0] as usize][b[0] as usize]]))
        }

        fn validating(mut self, valid: impl Fn(&[u8]) -> bool + 'static) -> Self {
            self.valid = Box::new(valid);
            self
        }

        fn tick(&mut self) {
            self.calls += 1;
            if let Some((clock, cost)) = &self.clock {
                clock.advance_time(*cost);
            }
        }
    }

    impl ConformanceOracle for Fake {
        fn validate_state(
            &mut self,
            state: &[u8],
            _related: &RelatedContracts<'_>,
        ) -> Result<ValidateResult, OracleError> {
            self.tick();
            Ok(if (self.valid)(state) {
                ValidateResult::Valid
            } else {
                ValidateResult::Invalid
            })
        }

        fn update_state(
            &mut self,
            state: &[u8],
            updates: &[UpdateData<'_>],
        ) -> Result<UpdateModification<'static>, OracleError> {
            self.tick();
            let [UpdateData::State(other)] = updates else {
                panic!("the probe only merges single full states");
            };
            (self.merge)(state, other.as_ref()).map(|s| UpdateModification::valid(s.into()))
        }

        fn summarize_state(&mut self, _state: &[u8]) -> Result<Vec<u8>, OracleError> {
            unreachable!("the probe never summarizes")
        }

        fn get_state_delta(&mut self, _: &[u8], _: &[u8]) -> Result<Vec<u8>, OracleError> {
            unreachable!("the probe never asks for a delta")
        }
    }

    /// Run the rule on `(stored, incoming)`, computing `merged` with the fake.
    fn verdict(fake: &mut Fake, stored: &[u8], incoming: &[u8]) -> ReapplyVerdict {
        let merged = (fake.merge)(stored, incoming).expect("the first merge succeeds");
        assert_ne!(
            merged, stored,
            "the probe only runs on a merge that changed state"
        );
        fake.calls = 0;
        reapply_verdict(fake, None, stored, incoming, &merged)
    }

    /// Commutative and idempotent. On `(S, X) = (0, 1)` the reconciled state
    /// moves on a re-delivered input; on `(3, 0)` reconciliation revisits a state.
    const REDELIVERY_AND_CYCLE: [[u8; 4]; 4] =
        [[0, 3, 0, 2], [3, 1, 3, 2], [0, 3, 2, 2], [2, 2, 2, 3]];

    /// Changes once on re-applying `X = 3` to `merge(2, 3)` and then settles;
    /// its strictly non-associative triples all heal.
    const SETTLES: [[u8; 4]; 4] = [[0, 0, 0, 0], [0, 1, 0, 0], [0, 0, 2, 1], [0, 0, 1, 3]];

    /// Merging any different state advances `0 -> 1 -> 2 -> 0`.
    fn cycling(a: &[u8], b: &[u8]) -> Result<Vec<u8>, OracleError> {
        Ok(if a == b {
            a.to_vec()
        } else {
            vec![(a[0] + 1) % 3]
        })
    }

    #[test]
    fn the_three_non_settling_verdicts_are_told_apart() {
        assert_eq!(
            verdict(&mut Fake::new(cycling), &[0], &[3]),
            ReapplyVerdict::NonSettling(NonSettling::NotConverged)
        );
        assert_eq!(
            verdict(&mut Fake::table(REDELIVERY_AND_CYCLE), &[0], &[1]),
            ReapplyVerdict::NonSettling(NonSettling::Redelivery)
        );
        assert_eq!(
            verdict(&mut Fake::table(REDELIVERY_AND_CYCLE), &[3], &[0]),
            ReapplyVerdict::NonSettling(NonSettling::Cycle)
        );
    }

    #[test]
    fn a_change_that_heals_settles() {
        assert_eq!(
            verdict(&mut Fake::table(SETTLES), &[2], &[3]),
            ReapplyVerdict::Settles
        );
    }

    #[test]
    fn unchanged_and_reordered_reapplies_end_the_probe_after_one_call() {
        // Bitwise or: re-applying X to S | X leaves it alone.
        let mut fake = Fake::new(|a, b| Ok(vec![a[0] | b[0]]));
        assert_eq!(verdict(&mut fake, &[1], &[2]), ReapplyVerdict::Unchanged);
        assert_eq!(fake.calls, 1);

        // Merging into [9] gives [2, 1]; any other merge rotates the current
        // state's bytes, so the re-apply gives [1, 2]: the same multiset.
        let mut fake = Fake::new(|a, b| {
            if a == [9] {
                Ok(vec![b[1], b[0]])
            } else {
                let mut out = a.to_vec();
                out.rotate_left(1);
                Ok(out)
            }
        });
        assert_eq!(verdict(&mut fake, &[9], &[1, 2]), ReapplyVerdict::Reordered);
        assert_eq!(fake.calls, 1);
    }

    /// When the merge result is `X` itself, re-delivering `X` is the node's
    /// byte-identical fast path. The probe answers it the same way and does
    /// not ask the contract, even if the contract would change the state on
    /// an identical merge.
    #[test]
    fn a_reapply_onto_x_itself_is_the_fast_path() {
        let mut fake = Fake::new(|a, b| Ok(if a == b { vec![a[0] + 100] } else { b.to_vec() }));
        assert_eq!(verdict(&mut fake, &[1], &[2]), ReapplyVerdict::Unchanged);
        assert_eq!(fake.calls, 0);
    }

    #[test]
    fn a_heal_step_the_contract_refuses_is_inconclusive() {
        // The cycling merge's heal step starts with merge(2, 1); refuse it.
        let mut fake = Fake::new(|a, b| {
            if (a, b) == (&[2][..], &[1][..]) {
                return Err(OracleError::contract("refused"));
            }
            cycling(a, b)
        });
        assert_eq!(
            verdict(&mut fake, &[0], &[3]),
            ReapplyVerdict::Inconclusive(Inconclusive::HealRefused)
        );
    }

    #[test]
    fn a_failed_reapply_is_inconclusive() {
        let mut fake = Fake::new(|a, b| {
            if a == [1] && b == [3] {
                return Err(OracleError::runtime("trap"));
            }
            cycling(a, b)
        });
        assert_eq!(
            verdict(&mut fake, &[0], &[3]),
            ReapplyVerdict::Inconclusive(Inconclusive::ReapplyFailed)
        );
    }

    #[test]
    fn a_heal_that_never_repeats_and_never_settles_is_inconclusive() {
        // S = 1, X = 2, merge(S, X) = 3, re-apply gives 4, and the groupings
        // 4 and 3 converge to 100. From there each input other than the last
        // one absorbed moves the state up by one, and re-delivering the same
        // input is absorbed: it keeps moving without repeating, so the heal
        // step runs out of passes.
        fn merge(a: &[u8], b: &[u8]) -> Result<Vec<u8>, OracleError> {
            let (a, b) = (a[0], b[0]);
            let out = match (a, b) {
                _ if a == b => a,
                (1, 2) => 3,
                (3, 2) => 4,
                (3, 4) | (4, 3) => 100,
                (c, o) if c >= 100 => {
                    let index = match o {
                        4 => 0,
                        3 => 1,
                        1 => 2,
                        2 => 3,
                        _ => return Ok(vec![c]),
                    };
                    if (c - 100) % 4 == index { c + 1 } else { c }
                }
                _ => a,
            };
            Ok(vec![out])
        }
        let mut fake = Fake::new(merge);
        assert_eq!(
            verdict(&mut fake, &[1], &[2]),
            ReapplyVerdict::Inconclusive(Inconclusive::OutOfPasses)
        );
        assert!(
            fake.calls <= REAPPLY_PROBE_MAX_CALLS,
            "{} calls",
            fake.calls
        );
    }

    #[test]
    fn spending_the_time_budget_is_inconclusive() {
        let clock = SharedMockTimeSource::new();
        let mut fake = Fake::new(cycling);
        fake.clock = Some((clock.clone(), REAPPLY_PROBE_TIME_BUDGET));
        let verdict = reapply_verdict(&mut fake, Some(&clock), &[0], &[3], &[1]);
        assert_eq!(
            verdict,
            ReapplyVerdict::Inconclusive(Inconclusive::BudgetExhausted)
        );
        // The re-apply is not timed. The heal step's first merge spends the
        // budget and its second is refused.
        assert_eq!(fake.calls, 2, "no call is made once the budget is spent");

        // The same input with time to spare is decided.
        let mut fake = Fake::new(cycling);
        assert_eq!(
            reapply_verdict(&mut fake, Some(&clock), &[0], &[3], &[1]),
            ReapplyVerdict::NonSettling(NonSettling::NotConverged)
        );
    }

    /// The first re-apply is one merge, the cost every probe pays; a slow one
    /// does not use up the budget for the steps that follow.
    #[test]
    fn a_slow_first_reapply_does_not_spend_the_budget() {
        struct SlowFirst {
            inner: Fake,
            clock: SharedMockTimeSource,
        }
        impl ConformanceOracle for SlowFirst {
            fn validate_state(
                &mut self,
                state: &[u8],
                related: &RelatedContracts<'_>,
            ) -> Result<ValidateResult, OracleError> {
                self.inner.validate_state(state, related)
            }
            fn update_state(
                &mut self,
                state: &[u8],
                updates: &[UpdateData<'_>],
            ) -> Result<UpdateModification<'static>, OracleError> {
                if self.inner.calls == 0 {
                    self.clock.advance_time(REAPPLY_PROBE_TIME_BUDGET * 4);
                }
                self.inner.update_state(state, updates)
            }
            fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
                self.inner.summarize_state(state)
            }
            fn get_state_delta(&mut self, s: &[u8], d: &[u8]) -> Result<Vec<u8>, OracleError> {
                self.inner.get_state_delta(s, d)
            }
        }
        let clock = SharedMockTimeSource::new();
        let mut slow = SlowFirst {
            inner: Fake::new(cycling),
            clock: clock.clone(),
        };
        assert_eq!(
            reapply_verdict(&mut slow, Some(&clock), &[0], &[3], &[1]),
            ReapplyVerdict::NonSettling(NonSettling::NotConverged)
        );
    }

    /// The call cap admits exactly [`REAPPLY_PROBE_MAX_CALLS`] calls.
    #[test]
    fn the_call_cap_admits_the_worst_case_and_no_more() {
        let mut fake = Fake::new(cycling);
        let mut budgeted = Budgeted::new(&mut fake, None);
        for call in 0..REAPPLY_PROBE_MAX_CALLS {
            assert!(
                budgeted.update_state(&[0], &[state_update(&[1])]).is_ok(),
                "call {call} refused below the cap"
            );
        }
        assert!(budgeted.update_state(&[0], &[state_update(&[1])]).is_err());
        assert!(budgeted.exhausted);
        assert_eq!(
            budgeted.failed(),
            ReapplyVerdict::Inconclusive(Inconclusive::BudgetExhausted)
        );
    }

    /// The longest path through the rule: the groupings converge, and every heal
    /// pass moves the state on each input it can (all but the repeated `X`)
    /// without ever repeating a state, until the passes run out. It must take
    /// exactly [`REAPPLY_PROBE_MAX_CALLS`] calls and still reach the heal step's
    /// own verdict, so the cap neither cuts the rule short nor allows more than
    /// the rule can use.
    #[test]
    fn the_longest_path_uses_exactly_the_call_cap() {
        // Appends a tag for the merged-in state unless the state already ends
        // with it: a re-delivery is absorbed, a different input moves the state,
        // and states only grow, so none repeats.
        fn tag(state: &[u8]) -> u8 {
            state[0] + 10 * state.len() as u8
        }
        fn append(a: &[u8], b: &[u8]) -> Vec<u8> {
            if a.last() == Some(&tag(b)) {
                a.to_vec()
            } else {
                [a, &[tag(b)]].concat()
            }
        }
        let (stored, incoming, merged) = (vec![1u8], vec![2u8], vec![3u8]);
        let left = append(&merged, &incoming);
        let right = merged.clone();
        let tags = [&left, &right, &stored, &incoming].map(|s| tag(s));
        assert!(
            tags.windows(2).all(|w| w[0] != w[1]) && tags[0] != tags[3],
            "adjacent heal inputs must move the state: {tags:?}"
        );

        let (l, r) = (left.clone(), right.clone());
        let mut fake = Fake::new(move |a, b| {
            Ok(if (a == l && b == r) || (a == r && b == l) {
                vec![50]
            } else {
                append(a, b)
            })
        });
        assert_eq!(
            reapply_verdict(&mut fake, None, &stored, &incoming, &merged),
            ReapplyVerdict::Inconclusive(Inconclusive::OutOfPasses)
        );
        assert_eq!(fake.calls, REAPPLY_PROBE_MAX_CALLS);
    }

    #[test]
    fn a_reapply_with_no_output_state_is_inconclusive() {
        struct NoState;
        impl ConformanceOracle for NoState {
            fn validate_state(
                &mut self,
                _: &[u8],
                _: &RelatedContracts<'_>,
            ) -> Result<ValidateResult, OracleError> {
                Ok(ValidateResult::Valid)
            }
            fn update_state(
                &mut self,
                _: &[u8],
                _: &[UpdateData<'_>],
            ) -> Result<UpdateModification<'static>, OracleError> {
                let related = freenet_stdlib::prelude::RelatedContract {
                    contract_instance_id: freenet_stdlib::prelude::ContractInstanceId::new([1; 32]),
                    mode: freenet_stdlib::prelude::RelatedMode::StateOnce,
                };
                Ok(UpdateModification::requires(vec![related]).expect("one related contract"))
            }
            fn summarize_state(&mut self, _: &[u8]) -> Result<Vec<u8>, OracleError> {
                unreachable!()
            }
            fn get_state_delta(&mut self, _: &[u8], _: &[u8]) -> Result<Vec<u8>, OracleError> {
                unreachable!()
            }
        }
        assert_eq!(
            reapply_verdict(&mut NoState, None, &[0], &[3], &[1]),
            ReapplyVerdict::Inconclusive(Inconclusive::NoOutputState)
        );
    }

    /// The stored state is not validated again: it was validated when the
    /// node stored it. A contract whose validation of `S` asks for related
    /// contracts, or rejects it, is still decided.
    #[test]
    fn the_stored_state_is_not_validated_again() {
        /// Asks for a related contract when validating anything.
        struct NeedsRelated(Fake);
        impl ConformanceOracle for NeedsRelated {
            fn validate_state(
                &mut self,
                _: &[u8],
                _: &RelatedContracts<'_>,
            ) -> Result<ValidateResult, OracleError> {
                Ok(ValidateResult::RequestRelated(vec![
                    freenet_stdlib::prelude::ContractInstanceId::new([1; 32]),
                ]))
            }
            fn update_state(
                &mut self,
                state: &[u8],
                updates: &[UpdateData<'_>],
            ) -> Result<UpdateModification<'static>, OracleError> {
                self.0.update_state(state, updates)
            }
            fn summarize_state(&mut self, _: &[u8]) -> Result<Vec<u8>, OracleError> {
                unreachable!()
            }
            fn get_state_delta(&mut self, _: &[u8], _: &[u8]) -> Result<Vec<u8>, OracleError> {
                unreachable!()
            }
        }
        assert_eq!(
            reapply_verdict(
                &mut NeedsRelated(Fake::new(cycling)),
                None,
                &[0],
                &[3],
                &[1]
            ),
            ReapplyVerdict::NonSettling(NonSettling::NotConverged)
        );

        let mut rejects_stored = Fake::new(cycling).validating(|s| s != [0]);
        assert_eq!(
            verdict(&mut rejects_stored, &[0], &[3]),
            ReapplyVerdict::NonSettling(NonSettling::NotConverged)
        );
    }

    /// Answers `on_identical` when asked to merge a state into an identical copy
    /// of itself; every other call goes to `inner`.
    type OnIdentical = fn() -> Result<UpdateModification<'static>, OracleError>;

    struct IdenticalMergeMisbehaves {
        inner: Fake,
        on_identical: OnIdentical,
    }
    impl ConformanceOracle for IdenticalMergeMisbehaves {
        fn validate_state(
            &mut self,
            state: &[u8],
            related: &RelatedContracts<'_>,
        ) -> Result<ValidateResult, OracleError> {
            self.inner.validate_state(state, related)
        }
        fn update_state(
            &mut self,
            state: &[u8],
            updates: &[UpdateData<'_>],
        ) -> Result<UpdateModification<'static>, OracleError> {
            if let [UpdateData::State(other)] = updates {
                if other.as_ref() == state {
                    self.inner.calls += 1;
                    return (self.on_identical)();
                }
            }
            self.inner.update_state(state, updates)
        }
        fn summarize_state(&mut self, _: &[u8]) -> Result<Vec<u8>, OracleError> {
            unreachable!()
        }
        fn get_state_delta(&mut self, _: &[u8], _: &[u8]) -> Result<Vec<u8>, OracleError> {
            unreachable!()
        }
    }

    fn requires() -> Result<UpdateModification<'static>, OracleError> {
        let related = freenet_stdlib::prelude::RelatedContract {
            contract_instance_id: freenet_stdlib::prelude::ContractInstanceId::new([1; 32]),
            mode: freenet_stdlib::prelude::RelatedMode::StateOnce,
        };
        Ok(UpdateModification::requires(vec![related]).expect("one related contract"))
    }

    /// The node's merge path never asks the contract to merge a state into an
    /// identical copy of itself (other paths, such as replaying queued updates
    /// after initialization, can), and neither does the probe: whatever the contract would do
    /// there (trap, run out of execution time, answer `requires()`), every
    /// verdict stays reachable.
    #[test]
    fn misbehaving_on_identical_merges_does_not_change_the_verdict() {
        let failures: [(&str, OnIdentical); 3] = [
            ("trap", || Err(OracleError::runtime("trap"))),
            ("out of execution time", || {
                Err(OracleError::resource("max compute time exceeded"))
            }),
            ("requires()", requires),
        ];
        for (name, on_identical) in failures {
            for (stored, incoming, expected) in [
                (0u8, 1u8, NonSettling::Redelivery),
                (3, 0, NonSettling::Cycle),
            ] {
                let merged = REDELIVERY_AND_CYCLE[stored as usize][incoming as usize];
                let mut oracle = IdenticalMergeMisbehaves {
                    inner: Fake::table(REDELIVERY_AND_CYCLE),
                    on_identical,
                };
                assert_eq!(
                    reapply_verdict(&mut oracle, None, &[stored], &[incoming], &[merged]),
                    ReapplyVerdict::NonSettling(expected),
                    "{name}: S={stored} X={incoming}"
                );
            }
            let mut oracle = IdenticalMergeMisbehaves {
                inner: Fake::new(cycling),
                on_identical,
            };
            assert_eq!(
                reapply_verdict(&mut oracle, None, &[0], &[3], &[1]),
                ReapplyVerdict::NonSettling(NonSettling::NotConverged),
                "{name}: cycling"
            );
        }
    }

    /// The same holds for a merge that settles: with identical merges trapping,
    /// the change-once-then-settle table is still decided as settling.
    #[test]
    fn a_settling_merge_that_traps_on_identical_merges_still_settles() {
        let mut oracle = IdenticalMergeMisbehaves {
            inner: Fake::table(SETTLES),
            on_identical: || Err(OracleError::runtime("trap")),
        };
        // merge(2, 3) = 1, and re-applying 3 gives 0.
        assert_eq!(
            reapply_verdict(&mut oracle, None, &[2], &[3], &[1]),
            ReapplyVerdict::Settles
        );
    }

    /// Adds the incoming state to the current one, so it moves on every apply.
    fn additive(a: &[u8], b: &[u8]) -> Result<Vec<u8>, OracleError> {
        Ok(vec![a[0].wrapping_add(b[0])])
    }

    /// An additive merge is associative, so `merge(merged, X)` equals
    /// `merge(S, merge(X, X))` when the contract is asked for `merge(X, X)`.
    /// The node never asks for it, and on the network the re-delivered input
    /// keeps moving the state.
    #[test]
    fn an_additive_merge_is_flagged() {
        assert_eq!(
            verdict(&mut Fake::new(additive), &[1], &[2]),
            ReapplyVerdict::NonSettling(NonSettling::Redelivery)
        );
    }

    /// A contract that answers `merge(Y, Y)` with a tagged state `T`, accepts
    /// `T` as valid, and computes `merge(S, T)` as `merge(merge(S, Y), Y)`,
    /// while every other merge adds. A rule that consulted `merge(X, X)` would
    /// find the groupings equal; this one never asks, and flags.
    #[test]
    fn a_tagged_self_merge_does_not_hide_an_additive_merge() {
        fn value(state: &[u8]) -> u8 {
            if state[0] >= 200 {
                2 * (state[0] - 200)
            } else {
                state[0]
            }
        }
        let mut fake = Fake::new(|a, b| {
            Ok(if a == b {
                if a[0] >= 200 {
                    a.to_vec()
                } else {
                    vec![200 + a[0]]
                }
            } else {
                vec![value(a) + value(b)]
            })
        });
        assert_eq!(
            fake.update_state(&[1], &[state_update(&[202])])
                .expect("merge")
                .new_state
                .expect("state")
                .as_ref(),
            &[5],
            "merge(S, tag(X)) must equal the re-apply, or this tests nothing"
        );
        fake.calls = 0;
        assert_eq!(
            reapply_verdict(&mut fake, None, &[1], &[2], &[3]),
            ReapplyVerdict::NonSettling(NonSettling::Redelivery)
        );
    }
}
