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
//! emit `EvictContract` events) lands in subsequent commits.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::governance::{OutlierConfig, OutlierResult, SkipReason, detect_outliers};
use crate::util::time_source::TimeSource;

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
    /// preserves the `FirstSeen` entry as the head[0]; once the cap is
    /// reached, each new entry drops the OLDEST non-FirstSeen entry
    /// (i.e. `history[1]`) — a sliding window of the most recent
    /// `MAX_TRANSITIONS_PER_CONTRACT - 1` transitions plus the
    /// permanent FirstSeen anchor.
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

/// Tunable parameters for the governance manager. Defaults reflect
/// the design doc; tests can override.
#[derive(Clone, Debug)]
pub(crate) struct GovernanceConfig {
    /// Operating mode.
    pub mode: GovernanceMode,
    /// MAD detector config (k, min_samples, trim_fraction).
    pub outlier: OutlierConfig,
    /// How long after first-seen a contract is exempt from flagging.
    /// New contracts haven't accumulated enough demand for the ratio
    /// to be meaningful; flagging them would punish growth.
    pub ramp_up: Duration,
    /// Half-life of the cost/benefit decay applied per tick. Larger
    /// half-life = sample-history sticks around longer = state more
    /// stable but slower to recover.
    pub decay_half_life: Duration,
    /// Window measured from a contract's original `Evicted` transition
    /// during which a SECOND eviction escalates to `Banned`. Must be
    /// strictly greater than [`evicted_ttl`] so there is a window
    /// between recovery and `ban_window` expiry where a re-eviction
    /// can fire (otherwise the contract recovers to Normal and the
    /// `recently_evicted` check immediately falls outside the window).
    pub ban_window: Duration,
    /// How long an `Evicted` contract stays Evicted before the TTL
    /// sweep transitions it back to `Normal` (via
    /// [`TransitionReason::Recovered`]). Should be shorter than
    /// [`ban_window`] so a contract that gets re-flagged shortly after
    /// recovery has its first eviction still within the ban window —
    /// that is the "repeat offender" path that triggers `Banned`.
    pub evicted_ttl: Duration,
    /// How long Banned status persists before transitioning back to
    /// Normal automatically.
    pub ban_ttl: Duration,
    /// `+3·MAD` borderline threshold expressed in MAD-units from the
    /// median. Below the eviction threshold but above this enters
    /// Borderline.
    pub borderline_mad_units: f64,
    /// Capacity ceiling for the MAD detector in log-space. If the
    /// threshold would exceed this, it's clamped. Sourced from
    /// hardware capacity; placeholder value here.
    pub capacity_ceiling_log: f64,
}

impl Default for GovernanceConfig {
    fn default() -> Self {
        Self {
            mode: GovernanceMode::DryRun,
            outlier: OutlierConfig::default(),
            ramp_up: Duration::from_secs(15 * 60), // 15 minutes
            decay_half_life: Duration::from_secs(60 * 60), // 1 hour
            // ban_window > evicted_ttl is required for the
            // repeat-offender path to fire. See field docs.
            ban_window: Duration::from_secs(60 * 60), // 1 hour
            evicted_ttl: Duration::from_secs(15 * 60), // 15 minutes
            ban_ttl: Duration::from_secs(60 * 60),    // 1 hour
            borderline_mad_units: 3.0,
            capacity_ceiling_log: 4.0, // log10 ceiling = 10000× typical
        }
    }
}

/// One decision emitted by the reaper tick. The caller (executor wiring)
/// reads these and decides what to do — in `Enforce` mode emit an
/// `EvictContract` event; in `DryRun` mode just log. The decision is
/// always recorded in the contract's `history`, regardless of mode.
#[derive(Clone, Debug)]
pub(crate) struct ReaperDecision {
    pub key: ContractInstanceId,
    pub from: GovernanceState,
    pub to: GovernanceState,
    pub reason: TransitionReason,
    pub at: Instant,
    /// `true` if the system should actually act on this transition.
    /// `false` in `DryRun` (or in `Off`, though `Off` never produces
    /// decisions). Lets the caller treat dry-run logging uniformly
    /// without re-checking the mode.
    pub actionable: bool,
}

/// Summary of a single reaper tick. The dashboard reads this for the
/// network-norms panel (median, MAD, threshold, sample size).
#[derive(Clone, Debug)]
pub(crate) struct ReaperTickResult {
    /// Decisions to act on this tick.
    pub decisions: Vec<ReaperDecision>,
    /// Median log-ratio across the population, or None if the
    /// detector skipped (insufficient sample / mad collapsed / etc).
    pub median_log_ratio: Option<f64>,
    /// MAD value.
    pub mad: Option<f64>,
    /// Threshold = median + k·MAD, clamped by capacity ceiling.
    pub threshold: Option<f64>,
    /// True if the threshold was clamped at the capacity ceiling
    /// (surface in dashboard as a warning).
    pub capacity_ceiling_binding: bool,
    /// Number of contracts that produced a usable log-ratio. Less
    /// than total population if some had no demand yet.
    pub sample_size: usize,
    /// Why the detector skipped, when applicable.
    pub skip_reason: Option<SkipReason>,
}

/// Per-contract governance scoring + reaper tick.
///
/// **Authoritative for what the dashboard shows.** Every per-contract
/// state and every network-level statistic in the dashboard is read
/// from this manager (or from data it commands). No fields exist on
/// the dashboard that aren't computed here.
///
/// The time source is a trait object so the manager slots into `Ring`
/// without generic bound propagation through every consumer. Tests
/// wrap a `MockTimeSource` in `Arc<dyn TimeSource + Send + Sync>` and
/// pass it through unchanged.
pub(crate) struct GovernanceManager {
    /// Per-contract scoring state. `DashMap` chosen per code-style
    /// rule: fine-grained shard locking lets the meter + the reaper
    /// tick + the receive-boundary check all read/write
    /// independently without serializing on a global lock.
    scores: DashMap<ContractInstanceId, ContractScore>,
    /// Configuration; cloned cheaply when the reaper tick reads its
    /// fields.
    config: GovernanceConfig,
    /// Time source — every timestamp goes through this for DST.
    time_source: Arc<dyn TimeSource + Send + Sync>,
    /// Most recent reaper-tick stats (median / MAD / threshold /
    /// sample size / skip reason). Stored so the dashboard snapshot
    /// builder can read network-norms without re-running detection.
    /// `parking_lot::RwLock` for cheap reads — the snapshot builder
    /// hits this every dashboard refresh.
    latest_tick: parking_lot::RwLock<Option<NetworkNormsCache>>,
}

/// Internal cache of the network-norms portion of a `ReaperTickResult`.
/// Excludes the per-decision list (those are logged at tick time;
/// the dashboard reads transitions from each contract's `history`).
#[derive(Clone, Debug)]
pub(crate) struct NetworkNormsCache {
    pub median_log_ratio: Option<f64>,
    pub mad: Option<f64>,
    pub threshold: Option<f64>,
    pub capacity_ceiling_binding: bool,
    pub sample_size: usize,
    pub skip_reason: Option<SkipReason>,
    pub at: Instant,
}

impl GovernanceManager {
    pub(crate) fn new(
        config: GovernanceConfig,
        time_source: Arc<dyn TimeSource + Send + Sync>,
    ) -> Self {
        Self {
            scores: DashMap::new(),
            config,
            time_source,
            latest_tick: parking_lot::RwLock::new(None),
        }
    }

    /// Operating mode, for the dashboard snapshot.
    pub(crate) fn mode(&self) -> GovernanceMode {
        self.config.mode
    }

    /// MAD detector's `min_samples` threshold — the number of
    /// contracts needed for outlier scoring to activate. Surfaced
    /// on the dashboard so the empty-state message can read
    /// "Observed N / min_samples contracts needed" without
    /// hard-coding the threshold at the render site.
    pub(crate) fn outlier_min_samples(&self) -> usize {
        self.config.outlier.min_samples
    }

    /// How long a `Banned` state persists before `BanLifted` fires.
    /// Surfaced for the Phase 7 ban-list wiring: when the reaper
    /// emits a `BanTriggered` decision, the receive-boundary ban
    /// entry is given `now + ban_ttl()` as its expiry so the two
    /// timers converge.
    pub(crate) fn ban_ttl(&self) -> Duration {
        self.config.ban_ttl
    }

    /// Read the latest network-norms snapshot, if any tick has run.
    /// Used by the dashboard snapshot builder.
    pub(crate) fn latest_norms(&self) -> Option<NetworkNormsCache> {
        self.latest_tick.read().clone()
    }

    /// Iterate per-contract scores. Yields cloned snapshots so the
    /// caller doesn't need to hold the DashMap shard guards.
    ///
    /// Note: at 10k+ contracts this clones the entire state set per
    /// call. Use [`iter_flagged_scores`] from the dashboard hot path
    /// when only flagged contracts are needed.
    pub(crate) fn iter_scores(&self) -> Vec<(ContractInstanceId, ContractScore)> {
        self.scores
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect()
    }

    /// Iterate per-contract scores, returning ONLY flagged entries
    /// (Borderline / WouldEvict / Evicted / Banned). The dashboard
    /// Contract Governance card hides Normal contracts at render
    /// time; using this filter avoids cloning thousands of entries
    /// per refresh on a busy node with mostly-healthy contracts.
    /// Code-first reviewer of #4270 raised the clone-the-world cost
    /// as a major concern at 10k+ contracts.
    pub(crate) fn iter_flagged_scores(&self) -> Vec<(ContractInstanceId, ContractScore)> {
        self.scores
            .iter()
            .filter(|e| e.value().state.is_flagged())
            .map(|e| (*e.key(), e.value().clone()))
            .collect()
    }

    /// Add a cost sample to a contract's `cost_used`. Called from the
    /// Meter wiring (subsequent commit) on every per-contract resource
    /// report. If the contract is new, creates a `Normal`-state score.
    pub(crate) fn ingest_cost(&self, key: ContractInstanceId, amount: f64) {
        if !amount.is_finite() || amount < 0.0 {
            return;
        }
        let now = self.time_source.now();
        let mut entry = self
            .scores
            .entry(key)
            .or_insert_with(|| ContractScore::new(now));
        entry.cost_used += amount;
    }

    /// Add a demand event to a contract's `benefit_score`. Called
    /// from the HostingManager wiring (subsequent commit) on every
    /// observed local/forwarded GET / SUBSCRIBE / client-attach.
    /// Weight comes from the caller (local vs forwarded weighting
    /// applied at the call site).
    pub(crate) fn ingest_demand(&self, key: ContractInstanceId, weight: f64) {
        if !weight.is_finite() || weight <= 0.0 {
            return;
        }
        let now = self.time_source.now();
        let mut entry = self
            .scores
            .entry(key)
            .or_insert_with(|| ContractScore::new(now));
        entry.benefit_score += weight;
    }

    /// Look up the current score for a contract, for dashboard reads.
    /// Returns a cloned snapshot; the dashboard doesn't need (and
    /// shouldn't have) write access.
    pub(crate) fn score_snapshot(&self, key: &ContractInstanceId) -> Option<ContractScore> {
        self.scores.get(key).map(|s| s.clone())
    }

    /// Total number of contracts being tracked.
    pub(crate) fn len(&self) -> usize {
        self.scores.len()
    }

    /// Run one reaper tick. Applies decay, runs the MAD detector
    /// across all contracts past the ramp-up window, drives state
    /// transitions, and returns the result for the caller to act on.
    ///
    /// `tick_interval` is the time since the previous tick; used for
    /// decay. If this is the first tick, pass any reasonable value
    /// (e.g. the same value `decay_half_life` is configured with);
    /// decay applied once at startup is a no-op anyway because all
    /// scores are zero.
    pub(crate) fn tick(&self, tick_interval: Duration) -> ReaperTickResult {
        if matches!(self.config.mode, GovernanceMode::Off) {
            return ReaperTickResult {
                decisions: Vec::new(),
                median_log_ratio: None,
                mad: None,
                threshold: None,
                capacity_ceiling_binding: false,
                sample_size: 0,
                skip_reason: None,
            };
        }

        let now = self.time_source.now();

        // 1. Apply decay to every score AND check for ban-TTL expiry
        //    + evicted-window expiry.
        //
        // Banned → Normal transition is unconditional after `ban_ttl`
        // passes since the BanTriggered transition (recorded in
        // `last_transition`).
        //
        // Evicted → Normal works on the same shape but with the
        // `ban_window` TTL — once that window has elapsed without
        // re-eviction, the contract is allowed to recover (skeptical
        // reviewer flagged that without an explicit TTL sweep, Evicted
        // could either flap back to Normal via decay-driven score
        // recovery — see the stickiness rule in the transition loop —
        // OR stay Evicted forever, neither of which we want).
        let mut ban_lifted: Vec<ContractInstanceId> = Vec::new();
        let mut evicted_lifted: Vec<ContractInstanceId> = Vec::new();
        for mut entry in self.scores.iter_mut() {
            entry.decay(tick_interval, self.config.decay_half_life);
            match entry.state {
                GovernanceState::Banned => {
                    let elapsed = now.saturating_duration_since(entry.last_transition);
                    if elapsed >= self.config.ban_ttl {
                        ban_lifted.push(*entry.key());
                    }
                }
                GovernanceState::Evicted => {
                    let elapsed = now.saturating_duration_since(entry.last_transition);
                    if elapsed >= self.config.evicted_ttl {
                        evicted_lifted.push(*entry.key());
                    }
                }
                GovernanceState::Normal
                | GovernanceState::Borderline
                | GovernanceState::WouldEvict => {}
            }
        }

        // 2. Collect the log-ratio sample. Contracts inside the ramp-up
        //    window are excluded — a new contract whose benefit hasn't
        //    accumulated yet would skew the distribution and might be
        //    flagged for being new rather than for being abusive.
        let actionable_samples: HashMap<ContractInstanceId, f64> = self
            .scores
            .iter()
            .filter_map(|entry| {
                let age = now.saturating_duration_since(entry.first_seen);
                if age < self.config.ramp_up {
                    return None;
                }
                // Banned AND Evicted contracts are excluded from the
                // distribution computation:
                // - Banned: their extreme ratio would drag the threshold.
                // - Evicted: same retained score keeps getting reprocessed
                //   tick-after-tick; without this filter the second tick
                //   sees `flagged.contains(key)` AND `recently_evicted ==
                //   true`, escalating Evicted → Banned just because the
                //   meter still carries the pre-eviction cost. That is
                //   not "repeat offender", it is double-counting the
                //   same eviction event. Codex reviewer of #4270 caught
                //   this blocker. With Evicted excluded, the existing
                //   stickiness rule in the main loop suffices: Evicted
                //   stays Evicted until either:
                //   (a) the `evicted_lifted` TTL sweep recovers it after
                //       `ban_window`, OR
                //   (b) a NEW eviction event fires after the contract
                //       has recovered to Normal AND been re-evicted —
                //       which is the real "repeat offender" path that
                //       should trigger Banned.
                if matches!(
                    entry.state,
                    GovernanceState::Banned | GovernanceState::Evicted
                ) {
                    return None;
                }
                entry.log_ratio().map(|r| (*entry.key(), r))
            })
            .collect();

        // 3. Run MAD detection on the sample.
        let outlier_result: OutlierResult<ContractInstanceId> = detect_outliers(
            &actionable_samples,
            |&r| Some(r),
            &self.config.outlier,
            self.config.capacity_ceiling_log,
        );

        // 4. Drive state transitions. For each contract in the sample,
        //    decide its new state based on where its log-ratio falls:
        //    - Above threshold → WouldEvict (or Evicted in Enforce)
        //    - Above median + N·MAD (borderline) → Borderline
        //    - Otherwise → Normal (or stay where it is if recently
        //      transitioned and recovering)
        let mut decisions: Vec<ReaperDecision> = Vec::new();
        let flagged: std::collections::HashSet<ContractInstanceId> =
            outlier_result.flagged.iter().cloned().collect();
        let actionable = self.config.mode.evicts();
        // Borderline cutoff is only meaningful when MAD is non-zero.
        // A collapsed MAD means the population is too homogeneous to
        // distinguish "elevated" from "normal"; falling back to
        // median-only would flag every contract slightly above
        // median, which is wrong.
        // Scale MAD by `MAD_GAUSSIAN_CONSISTENCY` (≈1.4826) so
        // `borderline_mad_units` is interpretable as standard
        // deviations under a normal honest population — matching the
        // semantics of the eviction threshold computed by
        // `detect_outliers`, which uses scaled MAD too. Without this
        // scaling, `borderline_mad_units = 3.0` would correspond to
        // ~2.0σ, not the "+3 standard deviations" the
        // `GovernanceState::Borderline` docstring claims. Codex
        // reviewer of #4270 caught this inconsistency.
        let borderline_cutoff = match (outlier_result.median_log_ratio, outlier_result.mad) {
            (Some(m), Some(mad)) if mad > f64::EPSILON => Some(
                m + self.config.borderline_mad_units
                    * crate::governance::MAD_GAUSSIAN_CONSISTENCY
                    * mad,
            ),
            _ => None,
        };

        // Stickiness + MAD-collapse rules (skeptical-reviewer
        // findings on #4270):
        //
        // - Once a contract is Evicted (Enforce mode), it stays
        //   Evicted for the duration of the ban_window. Decay-driven
        //   score recovery is NOT enough to undo an eviction — the
        //   on-disk state is gone, and shrinking `cost_used` only
        //   tells us "the cost signal faded", not "the contract is
        //   fine now". Recovery happens via the explicit TTL sweep
        //   in `evicted_lifted` below the main loop, the same shape
        //   already used for Banned → Normal.
        //
        // - When MAD collapses (the population is too homogeneous
        //   to compute `borderline_cutoff`), a previously-Borderline
        //   contract MUST NOT auto-recover to Normal. Doing so logs
        //   a spurious `Recovered` transition each time MAD
        //   collapses and a fresh `BorderlineEntered` each time it
        //   recomputes — pure flap. Skip the transition for that
        //   tick and let the next tick re-evaluate.
        let mad_collapsed = borderline_cutoff.is_none();
        for (key, log_ratio) in actionable_samples.iter() {
            let Some(mut entry) = self.scores.get_mut(key) else {
                continue;
            };
            let from = entry.state;
            let next = if flagged.contains(key) {
                // Past threshold. In Enforce we'd move to Evicted; in
                // DryRun we mark WouldEvict so the dashboard reflects
                // "the system would act on this." Repeat-eviction
                // ban (Phase 7) escalates Evicted → Banned only if
                // a previous eviction happened within the ban window.
                if actionable {
                    let recently_evicted = entry.history.iter().rev().any(|t| {
                        matches!(t.reason, TransitionReason::Evicted)
                            && now.saturating_duration_since(t.at) <= self.config.ban_window
                    });
                    if recently_evicted {
                        GovernanceState::Banned
                    } else {
                        GovernanceState::Evicted
                    }
                } else {
                    GovernanceState::WouldEvict
                }
            } else if from == GovernanceState::Evicted {
                // Evicted is sticky — see comment above the loop.
                continue;
            } else if mad_collapsed {
                // No reliable signal — see comment above the loop.
                continue;
            } else if borderline_cutoff.is_some_and(|c| *log_ratio >= c) {
                GovernanceState::Borderline
            } else {
                GovernanceState::Normal
            };

            if next == from {
                continue;
            }
            let reason = match (from, next) {
                (_, GovernanceState::Borderline) => TransitionReason::BorderlineEntered,
                (_, GovernanceState::WouldEvict) => TransitionReason::ThresholdCrossed,
                (_, GovernanceState::Evicted) => TransitionReason::Evicted,
                (_, GovernanceState::Banned) => TransitionReason::BanTriggered,
                (_, GovernanceState::Normal) => TransitionReason::Recovered,
            };
            entry.record_transition(now, next, reason);
            // `actionable` means "the executor wiring should emit an
            // `EvictContract` event for this decision" — true only for
            // transitions INTO an actively-enforced state (Evicted or
            // Banned). Borderline/WouldEvict/Normal transitions are
            // observation-only regardless of mode. Codex reviewer of
            // #4270 caught that the previous `actionable = mode.evicts()`
            // marked every transition in Enforce mode (including
            // Recovered and BorderlineEntered) as actionable, which
            // violates the field's documented contract.
            let actionable_decision =
                actionable && matches!(next, GovernanceState::Evicted | GovernanceState::Banned);
            decisions.push(ReaperDecision {
                key: *key,
                from,
                to: next,
                reason,
                at: now,
                actionable: actionable_decision,
            });
        }

        // 5. Process ban TTLs that expired during the decay walk.
        //
        // `actionable` mirrors the per-mode semantics: a ban lifted in
        // DryRun was never actionable to begin with, so its lift
        // isn't either. Skeptical-reviewer caught the earlier
        // hardcoded `true` here would let a future actor act on a
        // DryRun-lifted "ban" that never enforced.
        for key in ban_lifted {
            if let Some(mut entry) = self.scores.get_mut(&key) {
                if entry.state == GovernanceState::Banned {
                    let from = entry.state;
                    entry.record_transition(
                        now,
                        GovernanceState::Normal,
                        TransitionReason::BanLifted,
                    );
                    decisions.push(ReaperDecision {
                        key,
                        from,
                        to: GovernanceState::Normal,
                        reason: TransitionReason::BanLifted,
                        at: now,
                        actionable,
                    });
                }
            }
        }

        // 5b. Process evicted-window expiries the same way. A contract
        // that has been Evicted for at least `evicted_ttl` AND is not
        // currently being re-flagged is allowed to return to Normal.
        //
        // The currently-flagged filter is critical: without it, a
        // contract whose `evicted_ttl` just elapsed AND is still
        // actively abusive (still produces flagging samples) would get
        // a free Recovered transition this tick — short-circuiting
        // through `next == from` in the main loop, then unconditionally
        // recovered here. Net effect: an abuser cycles "Evicted →
        // Recovered → Normal → Evicted" indefinitely without ever
        // triggering Banned. Skeptical-reviewer caught this as a
        // high-severity bug. Filter against `flagged` so recovery
        // only fires for contracts whose behavior has actually
        // calmed down.
        for key in evicted_lifted {
            if flagged.contains(&key) {
                // Still actively flagged — let the main transition
                // loop's `recently_evicted` check escalate on the
                // next tick (or, if `ban_window` has also elapsed
                // and the abuser keeps flagging, the next tick after
                // recovery will start the Evicted clock fresh).
                continue;
            }
            if let Some(mut entry) = self.scores.get_mut(&key) {
                if entry.state == GovernanceState::Evicted {
                    let from = entry.state;
                    entry.record_transition(
                        now,
                        GovernanceState::Normal,
                        TransitionReason::Recovered,
                    );
                    decisions.push(ReaperDecision {
                        key,
                        from,
                        to: GovernanceState::Normal,
                        reason: TransitionReason::Recovered,
                        at: now,
                        actionable,
                    });
                }
            }
        }

        // Cache the network-norms for the dashboard snapshot builder.
        *self.latest_tick.write() = Some(NetworkNormsCache {
            median_log_ratio: outlier_result.median_log_ratio,
            mad: outlier_result.mad,
            threshold: outlier_result.threshold,
            capacity_ceiling_binding: outlier_result.capacity_ceiling_binding,
            sample_size: outlier_result.sample_size,
            skip_reason: outlier_result.skip_reason,
            at: now,
        });

        ReaperTickResult {
            decisions,
            median_log_ratio: outlier_result.median_log_ratio,
            mad: outlier_result.mad,
            threshold: outlier_result.threshold,
            capacity_ceiling_binding: outlier_result.capacity_ceiling_binding,
            sample_size: outlier_result.sample_size,
            skip_reason: outlier_result.skip_reason,
        }
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

    // ============================================================
    // GovernanceManager tests
    // ============================================================

    use crate::util::time_source::MockTimeSource;
    use freenet_stdlib::prelude::ContractInstanceId;

    fn mk_key(seed: u8) -> ContractInstanceId {
        ContractInstanceId::new([seed; 32])
    }

    /// Mutex-wrapping the time source for tests where we need to
    /// advance time after construction. Wraps `MockTimeSource` in a
    /// type that implements `TimeSource` by locking and reading.
    #[derive(Debug)]
    struct SharedTs(std::sync::Mutex<MockTimeSource>);
    impl SharedTs {
        fn new() -> Arc<Self> {
            Arc::new(Self(std::sync::Mutex::new(MockTimeSource::new(
                Instant::now(),
            ))))
        }
        fn advance(&self, d: Duration) {
            self.0.lock().unwrap().advance_time(d);
        }
    }
    impl TimeSource for SharedTs {
        fn now(&self) -> Instant {
            self.0.lock().unwrap().now()
        }
    }

    fn mk_mgr_shared(mode: GovernanceMode) -> (GovernanceManager, Arc<SharedTs>) {
        let ts = SharedTs::new();
        let outlier = OutlierConfig {
            min_samples: 5,
            trim_fraction: 0.0,
            ..Default::default()
        };
        let config = GovernanceConfig {
            mode,
            outlier,
            ramp_up: Duration::from_secs(1),
            ..Default::default()
        };
        let ts_dyn: Arc<dyn TimeSource + Send + Sync> = ts.clone();
        let mgr = GovernanceManager::new(config, ts_dyn);
        (mgr, ts)
    }

    #[test]
    fn new_manager_is_empty() {
        let (mgr, _ts) = mk_mgr_shared(GovernanceMode::DryRun);
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn ingest_cost_creates_score_on_first_observation() {
        let (mgr, _ts) = mk_mgr_shared(GovernanceMode::DryRun);
        let k = mk_key(1);
        mgr.ingest_cost(k, 10.0);
        assert_eq!(mgr.len(), 1);
        let s = mgr.score_snapshot(&k).unwrap();
        assert_eq!(s.cost_used, 10.0);
        assert_eq!(s.benefit_score, 0.0);
        assert_eq!(s.state, GovernanceState::Normal);
        // FirstSeen is recorded.
        assert_eq!(s.history.len(), 1);
    }

    #[test]
    fn ingest_demand_creates_score_on_first_observation() {
        let (mgr, _ts) = mk_mgr_shared(GovernanceMode::DryRun);
        let k = mk_key(1);
        mgr.ingest_demand(k, 1.5);
        let s = mgr.score_snapshot(&k).unwrap();
        assert_eq!(s.benefit_score, 1.5);
        assert_eq!(s.cost_used, 0.0);
    }

    #[test]
    fn ingest_rejects_non_finite_or_negative_amounts() {
        let (mgr, _ts) = mk_mgr_shared(GovernanceMode::DryRun);
        let k = mk_key(1);
        mgr.ingest_cost(k, -5.0); // negative ignored
        mgr.ingest_cost(k, f64::NAN);
        mgr.ingest_cost(k, f64::INFINITY);
        mgr.ingest_demand(k, 0.0); // zero ignored (no demand)
        mgr.ingest_demand(k, -1.0);
        // No score created from invalid inputs.
        assert_eq!(mgr.len(), 0);
    }

    #[test]
    fn off_mode_tick_produces_no_decisions() {
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::Off);
        let k = mk_key(1);
        mgr.ingest_cost(k, 1000.0);
        mgr.ingest_demand(k, 0.1);
        ts.advance(Duration::from_secs(60));
        let result = mgr.tick(Duration::from_secs(1));
        assert!(result.decisions.is_empty());
        assert_eq!(result.sample_size, 0);
    }

    #[test]
    fn ramp_up_excludes_new_contracts_from_detection() {
        let (mgr, _ts) = mk_mgr_shared(GovernanceMode::DryRun);
        // Synthesize a clearly-abusive contract immediately on creation.
        for i in 0..10 {
            mgr.ingest_cost(mk_key(i), 1.0);
            mgr.ingest_demand(mk_key(i), 1.0);
        }
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100_000.0);
        mgr.ingest_demand(abuser, 0.1);
        // Tick immediately — all contracts are inside ramp-up.
        let result = mgr.tick(Duration::from_secs(1));
        assert!(result.decisions.is_empty());
        // The detector saw zero usable samples because everything was
        // ramp-up gated.
        assert_eq!(result.sample_size, 0);
    }

    #[test]
    fn detects_outlier_after_ramp_up() {
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::DryRun);
        // Bulk: 30 honest contracts clustered around log-ratio = -1.
        for i in 0..30 {
            // Tiny jitter so MAD doesn't collapse to zero; keeps the
            // population recognisably honest but gives the detector
            // a real distribution to work with.
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        // One abuser with cost ratio = 100 (log10 = 2).
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);

        // Past ramp-up window so all contracts are eligible.
        ts.advance(Duration::from_secs(2));
        let result = mgr.tick(Duration::from_millis(100));
        // Sample includes everything (31 total).
        assert_eq!(result.sample_size, 31);
        // The abuser should be flagged.
        let flagged_keys: Vec<_> = result.decisions.iter().map(|d| d.key).collect();
        assert!(flagged_keys.contains(&abuser));
        // Honest contracts shouldn't be flagged.
        assert!(!flagged_keys.contains(&mk_key(0)));
    }

    #[test]
    fn dry_run_marks_would_evict_not_evicted() {
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::DryRun);
        for i in 0..30 {
            // Tiny jitter so MAD doesn't collapse to zero; keeps the
            // population recognisably honest but gives the detector
            // a real distribution to work with.
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(2));
        let result = mgr.tick(Duration::from_millis(100));
        let abuser_decision = result.decisions.iter().find(|d| d.key == abuser).unwrap();
        assert_eq!(abuser_decision.to, GovernanceState::WouldEvict);
        // Dry-run: not actionable.
        assert!(!abuser_decision.actionable);
    }

    #[test]
    fn enforce_marks_evicted_first_time() {
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::Enforce);
        for i in 0..30 {
            // Tiny jitter so MAD doesn't collapse to zero; keeps the
            // population recognisably honest but gives the detector
            // a real distribution to work with.
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(2));
        let result = mgr.tick(Duration::from_millis(100));
        let abuser_decision = result.decisions.iter().find(|d| d.key == abuser).unwrap();
        assert_eq!(abuser_decision.to, GovernanceState::Evicted);
        assert!(abuser_decision.actionable);
    }

    #[test]
    fn second_eviction_within_ban_window_triggers_ban() {
        // Real repeat-offender path post-#4270 review:
        //   1. Contract gets Evicted at T=0.
        //   2. evicted_ttl elapses (default 15min); the contract is
        //      not currently flagged → TTL sweep recovers to Normal.
        //   3. Fresh cost arrives → contract is flagged again.
        //   4. Main loop's recently_evicted check finds the original
        //      Evicted within ban_window (default 1h) → Banned.
        //
        // The earlier version of this test re-fed the SAME tick after
        // eviction. Codex reviewer caught that this double-counted the
        // same eviction event; the fix excludes Evicted contracts from
        // the distribution so the next-tick re-feed path is unreachable.
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::Enforce);
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(2));

        // Tick 1: first eviction.
        let r1 = mgr.tick(Duration::from_millis(100));
        assert_eq!(
            r1.decisions.iter().find(|d| d.key == abuser).unwrap().to,
            GovernanceState::Evicted
        );

        // Advance past evicted_ttl (15min default). Re-establish the
        // honest distribution so MAD stays well-defined during recovery.
        ts.advance(Duration::from_secs(15 * 60 + 1));
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        let recovery = mgr.tick(Duration::from_millis(100));
        let recovered = recovery
            .decisions
            .iter()
            .find(|d| d.key == abuser)
            .expect("evicted_lifted sweep must recover the abuser");
        assert_eq!(recovered.from, GovernanceState::Evicted);
        assert_eq!(recovered.to, GovernanceState::Normal);
        assert!(matches!(recovered.reason, TransitionReason::Recovered));

        // Re-feed abuser. The original Evicted transition is still
        // within ban_window (we advanced 15min+1s, well under 1h).
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(1));

        // Tick 3: recently_evicted check fires → Banned, not Evicted.
        let r2 = mgr.tick(Duration::from_millis(100));
        let second = r2.decisions.iter().find(|d| d.key == abuser).unwrap();
        assert_eq!(second.to, GovernanceState::Banned);
        assert!(matches!(second.reason, TransitionReason::BanTriggered));
    }

    #[test]
    fn ban_ttl_expires_back_to_normal() {
        // After the Banned state's ban_ttl elapses, BanLifted fires.
        // Setup mirrors `second_eviction_within_ban_window_triggers_ban`:
        // evict → recover → re-feed → Banned, then advance past ban_ttl.
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::Enforce);
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(2));

        // Tick 1: Evicted.
        mgr.tick(Duration::from_millis(100));

        // Advance past evicted_ttl + re-establish honest distribution.
        ts.advance(Duration::from_secs(15 * 60 + 1));
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        mgr.tick(Duration::from_millis(100)); // → Normal via Recovered.

        // Re-feed → Banned (recently_evicted in window).
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(1));
        mgr.tick(Duration::from_millis(100));

        // Now advance past ban_ttl (1h default).
        ts.advance(Duration::from_secs(60 * 60 + 1));
        let result = mgr.tick(Duration::from_millis(100));
        let lifted = result.decisions.iter().find(|d| d.key == abuser).unwrap();
        assert_eq!(lifted.from, GovernanceState::Banned);
        assert_eq!(lifted.to, GovernanceState::Normal);
        assert!(matches!(lifted.reason, TransitionReason::BanLifted));
    }

    #[test]
    fn borderline_state_for_contract_above_borderline_below_threshold() {
        // Construct a distribution where the test contract MUST land
        // in Borderline — specifically, between `+borderline_mad_units
        // × 1.4826 × MAD` (the borderline cutoff) and `k × 1.4826 ×
        // MAD` (the eviction threshold). With default config
        // (borderline_mad_units=3, k=5), that's roughly 4.4σ–7.4σ.
        //
        // Codex reviewer of #4270 flagged that the previous version of
        // this test (a) over-permitted WouldEvict, and (b) silently
        // passed when no decision was logged. Both fixed here:
        //   - The assertion is unconditional (no `if let Some`).
        //   - It pins `Borderline` specifically, not the union.
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::DryRun);
        // 30 honest contracts with enough spread to give MAD a
        // meaningful magnitude. cost=0.1 ± slight jitter → log-ratio ≈ -1.
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.02;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0);
        }
        // Borderline test contract: cost ratio designed to land at
        // ~5σ above median, which is between the borderline cutoff
        // (≈4.4σ) and the eviction threshold (≈7.4σ). Calibrated
        // empirically against the test fixture's MAD.
        let borderline = mk_key(99);
        mgr.ingest_cost(borderline, 0.15);
        mgr.ingest_demand(borderline, 1.0);
        ts.advance(Duration::from_secs(2));
        let result = mgr.tick(Duration::from_millis(100));
        let decision = result
            .decisions
            .iter()
            .find(|d| d.key == borderline)
            .unwrap_or_else(|| {
                panic!(
                    "borderline contract MUST produce a decision, got: {:?}",
                    result.decisions
                )
            });
        assert_eq!(
            decision.to,
            GovernanceState::Borderline,
            "expected Borderline (not WouldEvict / Normal); got {:?}. \
             If this drifts to WouldEvict, the test fixture's MAD has \
             tightened beyond the test's design and the cost value \
             needs recalibration.",
            decision.to
        );
        assert!(matches!(
            decision.reason,
            TransitionReason::BorderlineEntered
        ));
    }

    #[test]
    fn snapshot_returns_clone() {
        let (mgr, _ts) = mk_mgr_shared(GovernanceMode::DryRun);
        let k = mk_key(1);
        mgr.ingest_cost(k, 10.0);
        let s1 = mgr.score_snapshot(&k).unwrap();
        // Modify the manager's score after snapshot — snapshot
        // should be unaffected (it's a clone).
        mgr.ingest_cost(k, 5.0);
        let s2 = mgr.score_snapshot(&k).unwrap();
        assert_eq!(s1.cost_used, 10.0);
        assert_eq!(s2.cost_used, 15.0);
    }

    #[test]
    fn missing_key_returns_none() {
        let (mgr, _ts) = mk_mgr_shared(GovernanceMode::DryRun);
        assert!(mgr.score_snapshot(&mk_key(42)).is_none());
    }

    /// Stickiness pin: once Evicted, a contract MUST NOT transition
    /// back to Normal via decay-driven score recovery in the main loop.
    /// It can only recover via the explicit ban_window TTL sweep.
    ///
    /// Skeptical reviewer of PR #4270 flagged this as the
    /// Normal → Evicted → Normal flapping bug: an evicted contract is
    /// still in `scores` and still receives demand events, so decay
    /// shrinks `cost_used` faster than `benefit_score` builds, and the
    /// ratio falls below threshold → "Recovered" transition is logged
    /// while the on-disk state is already gone. Confusing at best,
    /// state-machine vs data-plane divergence at worst.
    #[test]
    fn evicted_is_sticky_to_decay_driven_recovery() {
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::Enforce);
        // Build an honest distribution + one abuser.
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(2));

        // First tick → abuser Evicted.
        let r1 = mgr.tick(Duration::from_millis(100));
        let evicted_decision = r1.decisions.iter().find(|d| d.key == abuser).unwrap();
        assert_eq!(evicted_decision.to, GovernanceState::Evicted);

        // Now flip the abuser's signal to "low cost, high benefit" —
        // dragging its log-ratio FAR below threshold and below
        // borderline_cutoff too. Without the stickiness rule, the next
        // tick would log a `Recovered` transition (Evicted → Normal)
        // even though the on-disk state is already gone.
        for _ in 0..20 {
            mgr.ingest_demand(abuser, 1.0);
        }
        // Re-feed honest contracts so the distribution still has a
        // sensible MAD.
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0);
        }
        ts.advance(Duration::from_secs(60));
        let r2 = mgr.tick(Duration::from_millis(100));

        // Critical assertion: NO `Recovered` (or any `Normal`) transition
        // logged for the abuser. It's allowed to escalate to Banned
        // (repeat-eviction inside ban_window — also a valid transition
        // out of Evicted) but never recover via decay.
        assert!(
            !r2.decisions
                .iter()
                .any(|d| d.key == abuser && d.to == GovernanceState::Normal),
            "Evicted contract must NOT auto-recover to Normal via decay — \
             found decision: {:?}",
            r2.decisions
                .iter()
                .filter(|d| d.key == abuser)
                .collect::<Vec<_>>(),
        );

        // Snapshot confirms the abuser is still Evicted or escalated to
        // Banned — anything BUT Normal.
        let snap_state = mgr.score_snapshot(&abuser).unwrap().state;
        assert!(
            matches!(
                snap_state,
                GovernanceState::Evicted | GovernanceState::Banned
            ),
            "abuser snapshot must still report Evicted/Banned, got {:?}",
            snap_state
        );
    }

    /// After the ban_window has elapsed, an Evicted contract IS allowed
    /// to return to Normal via the explicit TTL sweep (the same shape
    /// already used for Banned → Normal). This pins that the TTL recovery
    /// path actually fires.
    #[test]
    fn evicted_lifts_back_to_normal_after_ban_window() {
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::Enforce);
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.01;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter * 0.05);
            mgr.ingest_demand(mk_key(i), 1.0 + jitter);
        }
        let abuser = mk_key(99);
        mgr.ingest_cost(abuser, 100.0);
        mgr.ingest_demand(abuser, 1.0);
        ts.advance(Duration::from_secs(2));

        // First tick → Evicted.
        let r1 = mgr.tick(Duration::from_millis(100));
        assert!(
            r1.decisions
                .iter()
                .any(|d| d.key == abuser && d.to == GovernanceState::Evicted)
        );

        // Advance past ban_window without re-eviction.
        ts.advance(Duration::from_secs(60 * 60 + 1));
        let r = mgr.tick(Duration::from_millis(100));
        let lift = r.decisions.iter().find(|d| d.key == abuser).unwrap();
        assert_eq!(lift.from, GovernanceState::Evicted);
        assert_eq!(lift.to, GovernanceState::Normal);
        assert!(matches!(lift.reason, TransitionReason::Recovered));
    }

    /// MAD-collapse pin: when MAD collapses (the population is too
    /// homogeneous to compute a borderline cutoff), a previously-
    /// Borderline contract MUST NOT auto-recover to Normal. Doing so
    /// would log a spurious `Recovered` transition each time MAD
    /// collapses and a fresh `BorderlineEntered` each time it recomputes.
    ///
    /// Skeptical reviewer of PR #4270 flagged this as a dashboard
    /// flapping risk: a homogeneous tick spuriously "recovers" every
    /// Borderline contract, then the next tick with two outliers
    /// flips them back to Borderline.
    #[test]
    fn mad_collapse_does_not_recover_borderline_to_normal() {
        let (mgr, ts) = mk_mgr_shared(GovernanceMode::DryRun);
        // Step 1: build a distribution with one Borderline contract.
        for i in 0..30 {
            let jitter = (i as f64 - 15.0) * 0.001;
            mgr.ingest_cost(mk_key(i), 0.1 + jitter);
            mgr.ingest_demand(mk_key(i), 1.0);
        }
        let borderline = mk_key(99);
        mgr.ingest_cost(borderline, 1.0);
        mgr.ingest_demand(borderline, 1.0);
        ts.advance(Duration::from_secs(2));
        let _r1 = mgr.tick(Duration::from_millis(100));
        // The borderline contract may have ended Borderline or
        // WouldEvict — both flagged states. Confirm via the snapshot.
        let after_first_tick = mgr.score_snapshot(&borderline).unwrap().state;
        // Borderline or flagged is what we want — anything but Normal.
        assert_ne!(
            after_first_tick,
            GovernanceState::Normal,
            "test fixture must flag the contract on first tick; got {:?}",
            after_first_tick
        );

        // Step 2: replace the honest population with identical values
        // (MAD collapse). Re-feeding identical cost/benefit doesn't
        // immediately replace the running averages but moves them
        // toward homogeneity. Skip this — instead, simulate MAD
        // collapse by NOT advancing time and feeding everyone the
        // same value to drag the average to zero spread.
        for i in 0..30 {
            // Heavy injection to dominate the running average.
            for _ in 0..100 {
                mgr.ingest_cost(mk_key(i), 0.1);
                mgr.ingest_demand(mk_key(i), 1.0);
            }
        }
        ts.advance(Duration::from_secs(60));
        let r2 = mgr.tick(Duration::from_millis(100));

        // The critical assertion: even if MAD collapsed and the
        // borderline contract's score sample is no longer extracted
        // (or extracts but can't be classified), there is NO
        // `Recovered` transition logged. The state stays whatever it
        // was after r1.
        assert!(
            !r2.decisions
                .iter()
                .any(|d| d.key == borderline && d.to == GovernanceState::Normal),
            "MAD-collapse must NOT recover Borderline to Normal — found: {:?}",
            r2.decisions
                .iter()
                .filter(|d| d.key == borderline)
                .collect::<Vec<_>>()
        );
        // Snapshot confirms state unchanged.
        assert_eq!(
            mgr.score_snapshot(&borderline).unwrap().state,
            after_first_tick,
            "borderline contract state must persist through MAD-collapse tick"
        );
    }
}
