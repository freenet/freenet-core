// Consumers (the meter-driven reaper tick, the executor's contract-
// receive boundary, the dashboard's snapshot endpoint) land in
// subsequent commits. This commit ships the data model and state-
// transition logic in isolation so each piece is reviewable
// independently.
#![allow(dead_code)]

//! Per-contract governance scoring and state machine.
//!
//! Consumes the shared MAD-based outlier detector in `crate::governance`
//! and turns it into per-contract decisions (Normal / Borderline /
//! WouldEvict / Evicted / Banned). Reads cost data from the per-contract
//! attribution wired into the `Meter` in PR #1 and demand data from
//! `HostingManager`'s existing subscription tracking.
//!
//! ## Authoritative principle
//!
//! See `docs/design/contract-hardening.md` — "Design principle:
//! dashboard reflects back-end, not the other way around." This module
//! is the back-end. Every state, transition, and number that appears
//! on the dashboard must originate from data this module computes
//! from real meter samples and real subscription events. The dashboard
//! does not invent fields; this module does not invent state.
//!
//! ## What's here in this commit
//!
//! Data model and state-machine logic, no I/O wiring yet:
//!
//! * [`GovernanceState`] — the five lifecycle states a contract can be
//!   in under this node's view.
//! * [`ContractScore`] — running cost/benefit aggregates per contract,
//!   plus the contract's current state and transition history.
//! * [`StateTransition`] / [`TransitionReason`] — the entries that
//!   feed the dashboard's Decision History panel.
//! * [`GovernanceMode`] — off / dry-run / enforce, gating whether
//!   state transitions actually evict.
//!
//! Wiring (read costs from `Meter`, read demand from `HostingManager`,
//! drive the reaper tick, emit `EvictContract` events) lands in
//! subsequent commits. The data model is committed first so each
//! subsequent commit is small enough to review independently.

use std::time::Duration;

use tokio::time::Instant;

/// The five states a contract can be in under per-contract governance.
///
/// Transitions are driven by the reaper tick comparing each contract's
/// log(cost/benefit) ratio against the MAD-derived threshold from the
/// network distribution.
///
/// **No operator-initiated state.** None of these states represent an
/// operator marking a contract; every transition is automatic and
/// based on observed cost/benefit. The dashboard surfaces these
/// states; it does not invoke them.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub(crate) enum GovernanceState {
    /// Within network norms. The default state for any healthy contract.
    Normal,
    /// Cost/benefit ratio has crossed +3 standard deviations from
    /// typical but is below the eviction threshold. Watching; no
    /// action yet.
    Borderline,
    /// Cost/benefit ratio has crossed the eviction threshold. In
    /// dry-run mode this is logged but no eviction occurs; in enforce
    /// mode the reaper acts.
    WouldEvict,
    /// Actively evicted by this node. Disk reclamation done;
    /// `SubscribeMsg::Cancelled` sent to downstream peers (Phase 5).
    Evicted,
    /// Re-evicted within the ban TTL window (Phase 7). This node
    /// refuses to host or process this contract for the remainder
    /// of the ban window.
    Banned,
}

impl GovernanceState {
    /// Whether this state is "flagged" — anything that would show up
    /// in the dashboard's verdict block as worth surfacing.
    pub(crate) fn is_flagged(self) -> bool {
        !matches!(self, GovernanceState::Normal)
    }

    /// Whether this state actively blocks new operations on the
    /// contract (PUT / UPDATE / SUBSCRIBE rejected at the receive
    /// boundary). Only `Banned` does this.
    pub(crate) fn blocks_operations(self) -> bool {
        matches!(self, GovernanceState::Banned)
    }
}

/// Why a contract transitioned between states. Surfaces as the human-
/// readable string in the dashboard's Decision History panel — the
/// translation happens at render time, not here. This enum stays
/// expressive and code-shaped; the dashboard chooses operator-facing
/// language.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TransitionReason {
    /// Contract first observed on this node. Score initialised; no
    /// demand or cost yet recorded.
    FirstSeen,
    /// Cost/benefit ratio crossed into the borderline zone
    /// (median + 3·MAD ≤ log-ratio < threshold).
    BorderlineEntered,
    /// Cost/benefit ratio crossed the eviction threshold.
    ThresholdCrossed,
    /// Reaper actioned the eviction. In dry-run this transition is
    /// recorded but the contract isn't actually evicted.
    Evicted,
    /// Re-evicted within the ban TTL window — Phase 7's repeat-
    /// offender mechanism.
    BanTriggered,
    /// Score decayed below the borderline threshold. Contract
    /// returned to Normal.
    Recovered,
    /// Ban TTL expired. Contract returns to Normal and may be
    /// re-accepted (or re-flagged) based on subsequent activity.
    BanLifted,
}

/// A recorded state transition. The dashboard's Decision History
/// panel iterates the contract's transition list and renders each as
/// a row.
///
/// Bounded length is enforced at insertion (see [`ContractScore::record_transition`])
/// so this never grows without bound on a long-lived contract.
#[derive(Clone, Debug)]
pub(crate) struct StateTransition {
    pub at: Instant,
    pub from: GovernanceState,
    pub to: GovernanceState,
    pub reason: TransitionReason,
}

/// Maximum number of transitions retained per contract. Older entries
/// are dropped; the head of `history` always holds `FirstSeen` so the
/// dashboard can show "first observed at" without unbounded growth.
pub(crate) const MAX_TRANSITIONS_PER_CONTRACT: usize = 32;

/// Per-contract running aggregate. The dashboard's per-contract row
/// reads from this; the reaper compares `cost_used / benefit_score`
/// against the network's MAD-derived threshold to drive transitions.
///
/// Both `cost_used` and `benefit_score` decay over a rolling window
/// (handled by [`ContractScore::decay`] called from the reaper tick).
/// Without decay, a once-flagged contract would stay flagged forever
/// even if its activity calmed down.
#[derive(Clone, Debug)]
pub(crate) struct ContractScore {
    /// Sum of weighted resource samples attributed to this contract.
    /// Sourced from `Meter` entries keyed on
    /// `AttributionSource::Contract(_)`.
    pub cost_used: f64,
    /// Sum of weighted demand events. Local subscriptions weigh more
    /// than forwarded (Sybil resistance — see design doc).
    pub benefit_score: f64,
    /// Current state.
    pub state: GovernanceState,
    /// Wall-clock-equivalent (via `TimeSource`) of first observation.
    /// Used to gate the ramp-up window — a brand-new contract isn't
    /// eligible for flagging while it's still accumulating its first
    /// few demand signals.
    pub first_seen: Instant,
    /// When the last state transition happened. Used by the ban-TTL
    /// check to know "did we evict this contract within the window?"
    pub last_transition: Instant,
    /// State history, capped at `MAX_TRANSITIONS_PER_CONTRACT`. Always
    /// preserves the `FirstSeen` entry as the head; older transitions
    /// in the middle are dropped if the cap is exceeded.
    pub history: Vec<StateTransition>,
}

impl ContractScore {
    /// Create a new score in the `Normal` state and record the
    /// `FirstSeen` transition. Cost and benefit start at zero.
    pub(crate) fn new(now: Instant) -> Self {
        let first = StateTransition {
            at: now,
            from: GovernanceState::Normal,
            to: GovernanceState::Normal,
            reason: TransitionReason::FirstSeen,
        };
        Self {
            cost_used: 0.0,
            benefit_score: 0.0,
            state: GovernanceState::Normal,
            first_seen: now,
            last_transition: now,
            history: vec![first],
        }
    }

    /// Compute the log10(cost/benefit) ratio used by the MAD detector.
    /// Returns `None` if benefit_score is too small to produce a
    /// stable ratio (avoids division-by-near-zero pulling the
    /// distribution toward infinity for new contracts).
    pub(crate) fn log_ratio(&self) -> Option<f64> {
        if self.benefit_score <= f64::EPSILON {
            return None;
        }
        let ratio = self.cost_used / self.benefit_score;
        if ratio <= 0.0 {
            return None;
        }
        Some(ratio.log10())
    }

    /// Record a state transition into the history, capped at
    /// `MAX_TRANSITIONS_PER_CONTRACT`. Always preserves the
    /// `FirstSeen` entry — if the cap is exceeded, drops the
    /// second-oldest entry instead.
    pub(crate) fn record_transition(
        &mut self,
        now: Instant,
        to: GovernanceState,
        reason: TransitionReason,
    ) {
        let from = self.state;
        if from == to {
            // No-op transitions (same state, just a re-check) don't
            // pollute history. The reaper tick may call us with the
            // same state on every pass for a healthy contract.
            return;
        }
        let transition = StateTransition {
            at: now,
            from,
            to,
            reason,
        };
        self.state = to;
        self.last_transition = now;

        if self.history.len() < MAX_TRANSITIONS_PER_CONTRACT {
            self.history.push(transition);
            return;
        }
        // Cap exceeded: keep FirstSeen at index 0, drop the next-
        // oldest entry, append the new one.
        if self.history.len() >= 2 {
            self.history.remove(1);
        }
        self.history.push(transition);
    }

    /// Apply exponential decay to cost and benefit. Called once per
    /// reaper tick. `half_life` is the duration over which a sample
    /// loses half its weight; with `tick_interval` smaller than
    /// `half_life` the decay per tick is gentle.
    ///
    /// Cost decays the same way benefit does, so a contract that goes
    /// quiet has its ratio held constant by symmetric decay — the
    /// state stays where the algorithm last placed it until new
    /// samples arrive.
    pub(crate) fn decay(&mut self, tick_interval: Duration, half_life: Duration) {
        if tick_interval.is_zero() || half_life.is_zero() {
            return;
        }
        let factor = 0.5f64.powf(tick_interval.as_secs_f64() / half_life.as_secs_f64());
        self.cost_used *= factor;
        self.benefit_score *= factor;
    }
}

/// Operating mode for the governance system. Plan defaults to
/// `DryRun` for one release after Phase 4 lands — operators see what
/// would be evicted, dashboards reflect intended actions, but no
/// contracts are actually evicted. After calibration, the operator
/// (or release default) flips to `Enforce`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum GovernanceMode {
    /// Disabled. No state computation, no transitions.
    Off,
    /// Compute state and record transitions, but do not evict. The
    /// dashboard shows `WouldEvict` / `Evicted` states reflecting
    /// what the system would do under `Enforce`.
    DryRun,
    /// Compute state, record transitions, and act — `Evicted` /
    /// `Banned` cause real eviction and real refusal.
    Enforce,
}

impl GovernanceMode {
    /// Whether this mode actually evicts contracts. Both `Off` and
    /// `DryRun` answer false.
    pub(crate) fn evicts(self) -> bool {
        matches!(self, GovernanceMode::Enforce)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instant_t(offset_ms: u64) -> Instant {
        // Tests use `Instant::now() + offset`; the offset is
        // monotonic and the comparisons in this module only ever
        // look at relative ordering, so the absolute time doesn't
        // matter for the assertions here.
        Instant::now() + Duration::from_millis(offset_ms)
    }

    #[test]
    fn new_score_starts_normal_with_first_seen_history() {
        let now = instant_t(0);
        let s = ContractScore::new(now);
        assert_eq!(s.state, GovernanceState::Normal);
        assert_eq!(s.cost_used, 0.0);
        assert_eq!(s.benefit_score, 0.0);
        assert_eq!(s.history.len(), 1);
        assert!(matches!(s.history[0].reason, TransitionReason::FirstSeen));
        assert_eq!(s.first_seen, now);
        assert_eq!(s.last_transition, now);
    }

    #[test]
    fn log_ratio_none_when_no_benefit() {
        let mut s = ContractScore::new(instant_t(0));
        // No benefit yet → undefined ratio.
        assert_eq!(s.log_ratio(), None);
        s.cost_used = 5.0;
        // Cost without benefit is still undefined — a brand-new
        // contract has no demand baseline yet.
        assert_eq!(s.log_ratio(), None);
    }

    #[test]
    fn log_ratio_none_when_cost_zero() {
        let mut s = ContractScore::new(instant_t(0));
        s.benefit_score = 10.0;
        // No cost → ratio is 0, log(0) is undefined.
        assert_eq!(s.log_ratio(), None);
    }

    #[test]
    fn log_ratio_computes_log10_of_cost_over_benefit() {
        let mut s = ContractScore::new(instant_t(0));
        s.cost_used = 10.0;
        s.benefit_score = 1.0;
        // log10(10) = 1
        assert!((s.log_ratio().unwrap() - 1.0).abs() < 1e-9);

        s.cost_used = 0.1;
        s.benefit_score = 10.0;
        // log10(0.01) = -2
        assert!((s.log_ratio().unwrap() - (-2.0)).abs() < 1e-9);
    }

    #[test]
    fn record_transition_updates_state_and_history() {
        let mut s = ContractScore::new(instant_t(0));
        s.record_transition(
            instant_t(100),
            GovernanceState::Borderline,
            TransitionReason::BorderlineEntered,
        );
        assert_eq!(s.state, GovernanceState::Borderline);
        assert_eq!(s.history.len(), 2);
        let last = s.history.last().unwrap();
        assert_eq!(last.from, GovernanceState::Normal);
        assert_eq!(last.to, GovernanceState::Borderline);
        assert!(matches!(last.reason, TransitionReason::BorderlineEntered));
    }

    #[test]
    fn record_transition_skips_no_op_same_state() {
        let mut s = ContractScore::new(instant_t(0));
        s.record_transition(
            instant_t(100),
            GovernanceState::Borderline,
            TransitionReason::BorderlineEntered,
        );
        let initial_len = s.history.len();
        // Re-asserting the same state is a no-op — the reaper tick
        // will call this every cycle, history must not bloat.
        s.record_transition(
            instant_t(200),
            GovernanceState::Borderline,
            TransitionReason::BorderlineEntered,
        );
        assert_eq!(s.history.len(), initial_len);
    }

    #[test]
    fn history_capped_preserves_first_seen() {
        let mut s = ContractScore::new(instant_t(0));
        // Force the history to exceed the cap by alternating
        // transitions.
        let mut state_toggle = false;
        for i in 1..(MAX_TRANSITIONS_PER_CONTRACT + 10) {
            state_toggle = !state_toggle;
            let to = if state_toggle {
                GovernanceState::Borderline
            } else {
                GovernanceState::Normal
            };
            let reason = if state_toggle {
                TransitionReason::BorderlineEntered
            } else {
                TransitionReason::Recovered
            };
            s.record_transition(instant_t(i as u64 * 100), to, reason);
        }
        // FirstSeen is preserved as the head — that's load-bearing
        // for the dashboard's "first observed at" display.
        assert!(matches!(s.history[0].reason, TransitionReason::FirstSeen));
        assert_eq!(s.history.len(), MAX_TRANSITIONS_PER_CONTRACT);
    }

    #[test]
    fn decay_reduces_cost_and_benefit_symmetrically() {
        let mut s = ContractScore::new(instant_t(0));
        s.cost_used = 100.0;
        s.benefit_score = 10.0;
        let initial_ratio = s.cost_used / s.benefit_score;
        s.decay(Duration::from_secs(60), Duration::from_secs(60));
        // After one half-life, both should be half. Ratio is invariant
        // — a contract that goes quiet doesn't drift through states.
        assert!((s.cost_used - 50.0).abs() < 1e-9);
        assert!((s.benefit_score - 5.0).abs() < 1e-9);
        let new_ratio = s.cost_used / s.benefit_score;
        assert!((new_ratio - initial_ratio).abs() < 1e-9);
    }

    #[test]
    fn decay_zero_intervals_no_op() {
        let mut s = ContractScore::new(instant_t(0));
        s.cost_used = 100.0;
        s.benefit_score = 10.0;
        s.decay(Duration::ZERO, Duration::from_secs(60));
        assert_eq!(s.cost_used, 100.0);
        s.decay(Duration::from_secs(60), Duration::ZERO);
        assert_eq!(s.cost_used, 100.0);
    }

    #[test]
    fn governance_state_predicates() {
        assert!(!GovernanceState::Normal.is_flagged());
        assert!(GovernanceState::Borderline.is_flagged());
        assert!(GovernanceState::WouldEvict.is_flagged());
        assert!(GovernanceState::Evicted.is_flagged());
        assert!(GovernanceState::Banned.is_flagged());

        // Only Banned blocks new operations — Evicted contracts can
        // be re-PUT immediately (and might trigger a re-eviction,
        // which is Phase 7's ban-TTL trigger).
        assert!(!GovernanceState::Normal.blocks_operations());
        assert!(!GovernanceState::Borderline.blocks_operations());
        assert!(!GovernanceState::WouldEvict.blocks_operations());
        assert!(!GovernanceState::Evicted.blocks_operations());
        assert!(GovernanceState::Banned.blocks_operations());
    }

    #[test]
    fn governance_mode_evicts_only_in_enforce() {
        assert!(!GovernanceMode::Off.evicts());
        assert!(!GovernanceMode::DryRun.evicts());
        assert!(GovernanceMode::Enforce.evicts());
    }
}
