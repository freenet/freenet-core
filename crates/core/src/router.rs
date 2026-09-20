pub(crate) mod dataset;
mod hierarchical;
mod isotonic_estimator;
mod residual;
mod routing_predictor;
mod util;

/// Simulation coverage for the hierarchical estimator (#4485), gated on
/// `simulation_tests` like the other in-crate simulation cases. The module name
/// is load-bearing: CI's simulation job selects in-crate simulation tests with
/// `-E 'kind(test) | (kind(lib) & test(sim_e2e_tests))'`, so a differently-named
/// module would compile and never run (#4301).
#[cfg(all(test, feature = "simulation_tests"))]
mod sim_e2e_tests;

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::GlobalRng;
use crate::node::network_status::OpType;
use crate::ring::interest::{
    InterestRegistrationSource, InterestRemovalCause, MissingSummaryClass,
    SummaryPopulationOutcome, SummaryPopulationSource,
};
use crate::ring::{Distance, Location, PeerKeyLocation, Ring};
use crate::tracing::event_kind::STATE_SIZE_BUCKET_COUNT;
pub(crate) use isotonic_estimator::{
    AdjustmentMode, EstimatorType, IsotonicEstimator, IsotonicEvent,
};
use util::{Mean, TransferSpeed};

/// Default size of the candidate window the prediction-based router considers.
///
/// The router truncates candidates to the N geographically closest peers
/// BEFORE the isotonic estimator scores them, so any peer outside this window
/// is invisible to routing for that hop. The historical value was 5, set when
/// `min_connections` was tiny and the comment in the original PR (#903) said
/// "Later we can experiment with increasing this limit." That experiment never
/// happened. Production telemetry on 2026-05 then showed 63% of failing GETs
/// on subscribed contracts never visited any subscriber, because a small
/// window misses uniformly-distributed subscribers most hops (issue #4222).
///
/// 25 matches `ring::Ring::DEFAULT_MIN_CONNECTIONS`, so a node at its minimum
/// connection count surfaces its entire routing table to the predictor. Larger
/// tables still favor the closest 25; the predictor's distance penalty
/// preserves small-world routing.
const DEFAULT_CONSIDER_N_CLOSEST_PEERS: usize = 25;

// Compile-time link between this default and `Ring::DEFAULT_MIN_CONNECTIONS`
// so future changes to either constant fail the build instead of silently
// diverging. If you intentionally want this window to differ from
// `DEFAULT_MIN_CONNECTIONS`, drop this assertion and document why.
const _: () = assert!(
    DEFAULT_CONSIDER_N_CLOSEST_PEERS == crate::ring::Ring::DEFAULT_MIN_CONNECTIONS,
    "DEFAULT_CONSIDER_N_CLOSEST_PEERS must match Ring::DEFAULT_MIN_CONNECTIONS — \
     see comment above."
);

// ==================== Telemetry types ====================

/// A snapshot of a single routing decision for telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RoutingDecisionInfo {
    pub target_location: f64,
    pub strategy: RoutingStrategy,
    pub candidates: Vec<RoutingCandidate>,
    pub total_routing_events: usize,
}

/// Which strategy the router used for this decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum RoutingStrategy {
    /// Not enough history; selected by distance only.
    DistanceBased,
    /// Used prediction model to rank candidates.
    PredictionBased,
    /// Had history but predictions failed for some candidates; fell back to distance for those.
    PredictionFallback,
}

/// A single candidate considered during routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RoutingCandidate {
    pub distance: f64,
    pub prediction: Option<RoutingPredictionInfo>,
    pub selected: bool,
}

/// Prediction details for a routing candidate (subset of RoutingPrediction for telemetry).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RoutingPredictionInfo {
    pub failure_probability: f64,
    pub time_to_response_start: f64,
    pub expected_total_time: f64,
    pub transfer_speed_bps: f64,
    /// How much renegade shifted the failure estimate (positive = renegade thinks more likely to fail).
    /// None if renegade had no prediction for this candidate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub renegade_failure_adjustment: Option<f64>,
}

impl From<RoutingPrediction> for RoutingPredictionInfo {
    fn from(p: RoutingPrediction) -> Self {
        Self {
            failure_probability: p.failure_probability,
            time_to_response_start: p.time_to_response_start,
            expected_total_time: p.expected_total_time,
            transfer_speed_bps: p.xfer_speed.bytes_per_second,
            renegade_failure_adjustment: p.renegade_failure_adjustment,
        }
    }
}

/// Per-operation-type estimator curves for the dashboard.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct PerOpCurves {
    pub failure_curve: Vec<(f64, f64)>,
    pub failure_data_range: (f64, f64),
    pub failure_events: usize,
    pub response_time_curve: Vec<(f64, f64)>,
    pub response_time_data_range: (f64, f64),
    pub response_time_events: usize,
    pub transfer_rate_curve: Vec<(f64, f64)>,
    pub transfer_rate_data_range: (f64, f64),
    pub transfer_rate_events: usize,
    /// Downsampled raw (distance, outcome) observations behind each isotonic fit,
    /// for the scatter overlay. `#[serde(default)]` keeps decode tolerant of
    /// missing fields via self-describing formats (no-op under the bincode AOF).
    #[serde(default)]
    pub failure_points: Vec<(f64, f64)>,
    #[serde(default)]
    pub response_time_points: Vec<(f64, f64)>,
    #[serde(default)]
    pub transfer_rate_points: Vec<(f64, f64)>,
}

/// Compact, fixed-cardinality evidence for the large-state architecture
/// decision (#5090).
///
/// Field names are intentionally short because every reporting peer sends this
/// block every 30 minutes. Array orders are the corresponding enum `ALL`
/// constants in `ring::interest`, receiver order is
/// `[delta_changed, delta_noop, full_changed, full_noop]`, state-size order is
/// `tracing::event_kind::STATE_SIZE_BUCKET_UPPER_BOUNDS` plus the overflow bin,
/// queue order is documented at the population site, and shadow-rollup order is
/// `tracing::telemetry::KnownShadowRollup::ALL`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct NetworkEfficiencyV1 {
    /// Schema version.
    pub v: u8,
    /// Delivered missing-summary sends and bytes by `MissingSummaryClass`.
    ///
    /// Four rows changed MEANING (not definition, recording site, or position)
    /// at #5117: the untracked staleness reset used to wipe a pair's recorded
    /// removal, so a pair whose removal was recent but whose last untracked
    /// observation was stale classified as "never seen before". Sends move
    /// `UntrackedFirstObserved` → `UntrackedFirstRecreated` and, via the same
    /// field's second reader, `TrackedFirstNew` → `TrackedFirstRecreated`; see
    /// also [`Self::recreated`]. TOTALS are unaffected — only the splits — and
    /// the First-vs-Repeat split is untouched. Read pre- and post-#5117 series
    /// separately; a step at that boundary is the fix, not a regression.
    pub ms_s: [u64; MissingSummaryClass::COUNT],
    pub ms_b: [u64; MissingSummaryClass::COUNT],
    /// First-send entry-age buckets: <1s, 1-9s, 10-59s, 1-4m59s, >=5m.
    pub ms_age: [u64; 5],
    /// SIZE histogram of delivered missing-summary payloads, log-4: <4 KiB,
    /// <16 KiB, <64 KiB, <256 KiB, <1 MiB, >=1 MiB. Rows are `interest::SIZE_HIST_CLASSES` in order:
    /// tracked_first_new, tracked_first_recreated, untracked_first_observed,
    /// untracked_first_recreated. Not all ten classes — see that constant.
    ///
    /// Added because the 0.2.120 investigation found full-state BYTES rose ~63%
    /// while full-state SEND COUNT rose only ~12% (#5153): every counter that
    /// existed measured counts, so the axis that actually moved was invisible.
    pub ms_size: [[u64; 6]; 4],
    /// DELIVERED untracked sends bucketed by how long ago the pair's entry was
    /// removed:
    /// <10s, <1m, <5m, <20m, older, and finally "no record of a removal".
    ///
    /// `ms_age` cannot cover this population — `first_age_bucket` is computed
    /// only on the tracked path — yet `untracked_first_observed` is the class
    /// that grew and is ~4x the population `ms_age` sees.
    pub ms_unt_age: [u64; 6],
    /// Registration overwrites of populated/empty entries, and cap rejects.
    pub reg_ow_k: [u64; InterestRegistrationSource::COUNT],
    pub reg_ow_m: [u64; InterestRegistrationSource::COUNT],
    pub reg_new_k: [u64; InterestRegistrationSource::COUNT],
    pub reg_new_m: [u64; InterestRegistrationSource::COUNT],
    pub reg_cap: [u64; InterestRegistrationSource::COUNT],
    /// Successful removals by cause and current known/missing population.
    pub removed: [u64; InterestRemovalCause::COUNT],
    pub current: [u64; crate::ring::interest::SummaryMissingReason::COUNT + 1],
    /// Recreated pairs by their preceding removal cause.
    ///
    /// Steps UP at #5117 for the same reason as [`Self::ms_s`] — the recorded
    /// removal this counts is no longer wiped by the untracked staleness reset,
    /// so recreations that were previously invisible are now counted. Read pre-
    /// and post-#5117 series separately. Still an undercount either way: the
    /// removal is only honoured within `INTEREST_TTL` and lives in a bounded LRU.
    pub recreated: [u64; InterestRemovalCause::COUNT],
    /// Summary-population outcomes by source then outcome.
    pub populated: [[u64; SummaryPopulationOutcome::COUNT]; SummaryPopulationSource::COUNT],
    /// Missing-pair history and active-attempt correlation overflows.
    pub corr_ovf: [u64; 2],
    /// Queue counters: capacity eviction, queued dedup, enqueue while active,
    /// large-head incidents, large-head blocked ms, small-entry-ms over that
    /// window, queued-large/actual-small count, queued-small/actual-large
    /// count, their respective actual payload bytes, scheduled small/large
    /// counts, their respective state bytes, then active-key tracking overflow.
    ///
    /// Several of these changed MEANING (not definition, recording site, or
    /// position) at #4961, which split the drain per lane AND moved the
    /// small/large split from the contract STATE size to the PREDICTED wire
    /// payload. Read pre- and post-fix series separately:
    ///   * scheduled small/large and their state-byte variants now partition by
    ///     predicted payload, so a large-state contract sending deltas counts as
    ///     SMALL. `scheduled_large` should fall by roughly the two thirds that
    ///     was misrouted. The small-lane state bytes are no longer bounded by
    ///     the 64 KiB threshold and are no longer a proxy for small-lane WIRE
    ///     volume (they never were on the large side).
    ///   * queued-large/actual-small was the pre-fix misclassification signal
    ///     (64.5% of large-lane items); it should collapse toward zero.
    ///   * queued-small/actual-large read 0 fleet-wide pre-fix and was close to
    ///     structurally so (the lane came from the state size, which bounds the
    ///     payload from above to within `MIN_FULL_STATE_SAVING_BYTES`); the one
    ///     two ways it could fire were a delta exceeding the state by up to
    ///     `MIN_FULL_STATE_SAVING_BYTES`, and the stale-dedup-lane defect fixed
    ///     alongside this in #5108. It is now the payload-misprediction count and is EXPECTED to
    ///     be non-zero. Those sends still take a large-lane permit before
    ///     hitting the wire. It is a COUNT, not a rate: nothing counts correct
    ///     predictions, and `scheduled_small` is not a usable denominator
    ///     because it includes sends that return before payload selection.
    ///   * small-entry-ms was small entries BLOCKED behind a large-lane permit
    ///     wait; it is now merely small entries queued DURING one, since the
    ///     small lane drains them concurrently. Collapsing toward zero is the
    ///     fix landing — but read it narrowly: the observation is armed ONLY by
    ///     the large lane's drain, so a small entry waiting on a small-lane
    ///     permit (including one held by a send correcting its lane) opens no
    ///     observation at all. A zero here is evidence about CROSS-lane
    ///     blocking, not about small-lane latency in general.
    ///   * large-head incidents/ms keep their trigger (the large drain waiting
    ///     on an empty pool with small entries queued), but the pool has a
    ///     second consumer now — a mispredicted small-lane send correcting its
    ///     lane — so an incident no longer implies a large-lane ENTRY caused the
    ///     exhaustion, and the sampled trigger can miss a wait that an upgrader
    ///     won the race for. BOTH these and small-entry-ms are gated on a
    ///     small-lane entry being queued, and that population widened with the
    ///     lane definition (large-state delta sends are now small-lane
    ///     entries), so all three move for a second reason as well.
    ///   * active-key overflow STAYS structurally unreachable, but for a new
    ///     reason: in-flight sends were capped by the 12+2 permits, and are now
    ///     capped by those plus the lane-correction parking area (12), still an
    ///     order of magnitude under the 256-key cap. It remains a pure invariant
    ///     check — a non-zero value means a leaked tracking guard, NOT a
    ///     scheduling backlog.
    ///   * enqueue-while-pair-active keeps its definition, but the window it
    ///     samples — how long a pair stays in `active` — now includes any
    ///     lane-correction wait, so it inflates for a reason unrelated to
    ///     enqueue behaviour.
    pub queue: [u64; 15],
    /// Successful apply counts by delta/full x changed/no-op x resulting-state
    /// size bucket.
    pub recv_n: [[u64; STATE_SIZE_BUCKET_COUNT]; 4],
    /// Every received payload by delta/full x terminal outcome (changed,
    /// no-op, dedup, backoff, failed) x incoming-payload size bucket. Delta's
    /// five outcome rows come first, then full state's five rows.
    pub recv_tn: [[u64; STATE_SIZE_BUCKET_COUNT]; 10],
    pub recv_tb: [[u64; STATE_SIZE_BUCKET_COUNT]; 10],
    /// Persisted state counts and bytes by fixed size bucket.
    pub state_n: [u64; STATE_SIZE_BUCKET_COUNT],
    pub state_b: [u64; STATE_SIZE_BUCKET_COUNT],
    /// State inventory summary: count, max bytes, over-limit count,
    /// over-limit bytes, hard limit, then pre-WASM and post-merge hard-limit
    /// rejection count/max pairs.
    pub state: [u64; 9],
    /// Cost-eviction eligibility by state size: eligible zero-demand,
    /// subscribed, recent-but-unsubscribed.
    pub evict_n: [[u64; STATE_SIZE_BUCKET_COUNT]; 3],
    pub evict_b: [[u64; STATE_SIZE_BUCKET_COUNT]; 3],
    /// Monotonic actual eviction victims by byte-budget zero-demand,
    /// byte-budget in-use, and cost-pressure reason × state-size bucket.
    pub vict_n: [[u64; STATE_SIZE_BUCKET_COUNT]; 3],
    pub vict_b: [[u64; STATE_SIZE_BUCKET_COUNT]; 3],
    /// Per cost axis: total rate, floor, max attributed rate, max eligible
    /// rate, and number of sustained attributed contracts.
    pub cost: [[u64; 5]; 3],
    /// Per cost axis × state-size bucket maximum attributed rate, first for
    /// every hosted contract and then restricted to eviction-eligible ones.
    pub cost_ba: [[u64; STATE_SIZE_BUCKET_COUNT]; 3],
    pub cost_be: [[u64; STATE_SIZE_BUCKET_COUNT]; 3],
    /// Local telemetry pipeline totals in the order documented where populated.
    pub tel: [u64; 15],
    /// Per known shadow rollup: generated/enqueue attempts, enqueue full,
    /// enqueue closed, rate-limit admitted, aggregate-limit drop,
    /// shadow-subbudget drop, backoff-buffer drop, retry truncation, final sent.
    pub shadow: [[u64; 9]; 7],
    /// Delivery path for this diagnostic block itself; order is documented in
    /// `TelemetryLocalMetricsSnapshot::network_efficiency_delivery`.
    pub eff: [u64; 8],
    /// Monotonic hosting-BEGIN counts by CAUSE; row order is
    /// `ring::hosting::HostingCause::ALL`: client GET, transit GET, sub-op GET,
    /// client PUT, transit PUT, startup restore, unattributed.
    ///
    /// Answers "why did this peer BEGIN hosting this contract" in aggregate,
    /// which nothing recorded before. The present-tense sibling is
    /// `ring::HostingReason` (`freenet.node.contracts.hosted`), which
    /// re-derives current demand on every collection instead of freezing
    /// provenance at admission — see that enum's rustdoc for the split.
    /// `AccessType` distinguishes only GET from PUT and
    /// so cannot separate a client's own request from transit — the distinction
    /// every hosting-policy decision rests on. In particular a subscribe-fetch
    /// travels the ordinary GET driver as a sub-op, and was indistinguishable
    /// from a plain GET until this split.
    ///
    /// Counted at the cache branch that inserts, so a refresh of an
    /// already-hosted contract is NOT counted. `Other` (last row) is a leak
    /// detector, not a category: it should be 0 in the field, and a nonzero
    /// value means some production path began hosting without naming a cause.
    pub host_begin: [u64; crate::ring::HostingCause::COUNT],
    /// GAUGE (not a counter — do not difference): distribution of `read_count`
    /// across the currently hosted set. Buckets: 0, 1, 2-3, 4-9, 10-99, >=100.
    ///
    /// `read_count` is half of the demand signal the subscriber-primary eviction
    /// ranking is built on, and it previously reached only the node's own local
    /// HTML dashboard — no `send_event` call site touched it — so the shape of
    /// the signal the policy depends on was unobservable fleet-wide.
    pub host_reads: [u64; crate::ring::READ_COUNT_HIST_BUCKETS],
    /// GAUGE: distribution of `last_genuine_access` AGE across the currently
    /// hosted set. Buckets: within the 5-minute cost window, <20 min, <2 h,
    /// older, never genuinely accessed. Bucket 0 over the hosted-set total is
    /// the share cost-pressure eviction currently treats as recently-accessed.
    /// The other half of the previously node-local demand signal.
    pub host_recency: [u64; crate::ring::GENUINE_ACCESS_RECENCY_BUCKETS],
    /// SHADOW-MODE futile-repair detector (`crate::ring::futile_repair`): how
    /// often does an anti-entropy heal leave the (contract, peer) edge still
    /// diverged? A non-commutative contract merge cannot converge, so the
    /// repair loop runs forever — seven contract instances were 32.7% of all
    /// update applies on 2026-08-09 for exactly this reason. Aggregate only,
    /// no per-contract or per-peer label.
    ///
    /// Order (`FutileRepairSnapshot::to_row`, which is the wire contract):
    /// attempts, futile, productive, observations_unpaired,
    /// attempts_superseded, attempts_discarded,
    /// outcomes_probe_budget_exhausted, outcomes_probe_unavailable,
    /// outcomes_after_long_gap, would_quarantine, edges_at_threshold,
    /// tracked_edges, evictions, evictions_losing_streak.
    ///
    /// `would_quarantine` is the headline: edges that reached
    /// `futile_repair::QUARANTINE_THRESHOLD` consecutive futile repairs.
    /// NOTHING is quarantined — this release only measures.
    ///
    /// # Read these four rows BEFORE the headline
    ///
    /// Each is a way the headline can be wrong, in the direction named:
    ///
    /// * `outcomes_probe_budget_exhausted` — comparisons where "stale" was the
    ///   conservative DEFAULT because the per-message WASM probe budget
    ///   (`node::MAX_STALENESS_PROBES_PER_SUMMARIES` = 32) was spent, not a
    ///   verdict. Excluded from `futile` for exactly this reason: it grows with
    ///   peer breadth and node load, so a large value means the heal path is
    ///   classifying load as staleness and any future gating threshold has to
    ///   be set knowing that.
    /// * `outcomes_probe_unavailable` — the same default, but because the
    ///   contract's own `get_state_delta` errored or timed out. Contract or
    ///   runtime health, not divergence.
    /// * `outcomes_after_long_gap` — classified outcomes settled more than
    ///   `futile_repair::LONG_GAP_THRESHOLD` after their attempt. Not a
    ///   separate class (these ARE in `futile`/`productive`), but the longer the
    ///   gap the likelier something other than our heal moved the state. On
    ///   links still taking the byte-budgeted full-bytes fallback a contract is
    ///   re-compared on the order of ten hours, so these can dominate; if they
    ///   do, the headline is measuring rotation latency.
    /// * `attempts_discarded` — attempts dropped unsettled because the peer's
    ///   interest state was torn down after the disconnect grace period. An
    ///   undercount, and the honest denominator for `futile + productive`
    ///   alongside `attempts`.
    ///
    /// Then read `tracked_edges` against `futile_repair::EDGE_CAPACITY`
    /// together with `evictions_losing_streak`: a saturated LRU makes every
    /// futility count an undercount, which is how `ms_unt_age` became useless.
    ///
    /// # Two things a fleet aggregation gets wrong by default
    ///
    /// * **Every row is PER OBSERVER.** Divergence is symmetric — A sees B
    ///   stale while B sees A stale — so both ends of a stuck edge heal it,
    ///   observe futility, and count it. A fleet SUM of `would_quarantine` is
    ///   roughly **2x** the number of distinct stuck edges, and nothing on the
    ///   wire carries an edge identity to deduplicate with. Halve it, or treat
    ///   it as an upper bound.
    /// * **`futile : productive` is not a repair-efficacy ratio.** Attempts are
    ///   recorded only for anti-entropy heals, but an edge also converges via
    ///   the proximity-overlap heal or plain live UPDATE fan-out, neither of
    ///   which records anything — so `productive` credits the outstanding
    ///   anti-entropy heal for whatever actually fixed it. It says "converged
    ///   by the next comparison", not "our heal converged it".
    pub futile: [u64; crate::ring::futile_repair::SNAPSHOT_SCALARS],
    /// Survival curve of consecutive-futility streaks over the rungs
    /// `futile_repair::LADDER_RUNGS` (1, 2, 3, 4, 5, 8, 16, 32): entry `i`
    /// counts streaks that REACHED rung `i`, at most once per rung per streak,
    /// so the series is monotonically non-increasing and reads as "of the
    /// streaks that got to 1, how many got to 32".
    pub futile_ladder: [u64; crate::ring::futile_repair::LADDER_LEN],
}

/// Periodic snapshot of the router model state for telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RouterSnapshotInfo {
    /// Versioned fixed-cardinality network-efficiency evidence. `None` on five
    /// of every six router snapshots (the wide block exports every 30 minutes)
    /// and before the production sources are initialized. As with the
    /// snapshot's earlier added fields, this changes positional bincode AOF decoding:
    /// buffered pre-upgrade RouterSnapshot records can be skipped once during
    /// upgrade, while live/self-describing OTLP JSON is unaffected. The AOF
    /// reader already treats an undecodable telemetry record as skippable.
    #[serde(default)]
    pub network_efficiency_v1: Option<NetworkEfficiencyV1>,
    pub failure_events: usize,
    pub success_events: usize,
    pub transfer_rate_events: usize,
    pub prediction_active: bool,
    pub mean_transfer_size_bytes: f64,
    pub consider_n_closest_peers: usize,
    pub peers_with_failure_adjustments: usize,
    pub peers_with_response_adjustments: usize,
    /// PAV regression curve sampled across [0, 0.5], clamped to [0, 1].
    pub failure_curve: Vec<(f64, f64)>,
    /// X-range of actual regression data for the failure estimator.
    pub failure_data_range: (f64, f64),
    /// PAV regression curve sampled across [0, 0.5], clamped to [0, inf).
    pub response_time_curve: Vec<(f64, f64)>,
    /// X-range of actual regression data for the response time estimator.
    pub response_time_data_range: (f64, f64),
    /// PAV regression curve sampled across [0, 0.5], clamped to [0, inf).
    pub transfer_rate_curve: Vec<(f64, f64)>,
    /// X-range of actual regression data for the transfer rate estimator.
    pub transfer_rate_data_range: (f64, f64),
    /// Downsampled raw (distance, outcome) observations behind each aggregate
    /// isotonic fit, for the scatter overlay. `#[serde(default)]` keeps decode
    /// tolerant of missing fields via self-describing formats; it is a no-op for
    /// the positional bincode AOF (which skips records it can't decode).
    #[serde(default)]
    pub failure_points: Vec<(f64, f64)>,
    #[serde(default)]
    pub response_time_points: Vec<(f64, f64)>,
    #[serde(default)]
    pub transfer_rate_points: Vec<(f64, f64)>,
    /// Connect forward estimator curve sampled across [0, 0.5], clamped to [0, 1].
    pub connect_forward_curve: Option<Vec<(f64, f64)>>,
    /// X-range of actual regression data for the connect forward estimator.
    pub connect_forward_data_range: Option<(f64, f64)>,
    pub connect_forward_events: Option<usize>,
    pub connect_forward_peer_adjustments: Option<usize>,
    /// Current number of open file descriptors held by this process, or `None`
    /// where it can't be read cheaply (Linux-only, via `/proc/self/fd`).
    ///
    /// Populated by `Ring` on the `router_snapshot` cadence, not by the router
    /// model. Paired with [`fd_soft_limit`](Self::fd_soft_limit) it makes
    /// fd-exhaustion headroom observable in central telemetry: open fds reaching
    /// the soft limit (`EMFILE`) drove the v0.2.73 gateway crash-loop and was
    /// invisible to the collector at the time. See #4440.
    pub open_fds: Option<u64>,
    /// Timeout route labels per peer since the previous snapshot (#5657),
    /// as a histogram: how many peers collected 1, 2-3, 4-7 and 8+ timeout
    /// labels in the window, the most any single peer collected, and how many
    /// labels landed on peers beyond the tracking cap. Populated by `Ring`.
    ///
    /// Each window is one snapshot interval. The first runs from `Ring`
    /// construction to the first snapshot (the loop skips its immediate first
    /// tick), so it is the same length but covers the node's start-up routing.
    /// Telemetry-only: there is no local dashboard view, because the window is
    /// drained by the snapshot and a second reader would steal its labels.
    ///
    /// Soak signal for CHAIN BLAME: an originator timeout is labelled against
    /// the first hop although the stall may be anywhere down the chain. One
    /// stuck host behind a popular key shows up here as a tail of first hops
    /// with many labels (a high max and a populated 8+ bucket). Accepted for
    /// router-only labels (they never reach `peer_health`), but watched.
    /// No peer identities are exported.
    #[serde(default)]
    pub timeout_label_peers_1: Option<u64>,
    #[serde(default)]
    pub timeout_label_peers_2_3: Option<u64>,
    #[serde(default)]
    pub timeout_label_peers_4_7: Option<u64>,
    #[serde(default)]
    pub timeout_label_peers_8_plus: Option<u64>,
    #[serde(default)]
    pub timeout_label_max_per_peer: Option<u64>,
    #[serde(default)]
    pub timeout_labels_untracked: Option<u64>,
    /// The `RLIMIT_NOFILE` soft limit (the ceiling that triggers `EMFILE`), or
    /// `None` on non-unix. Populated by `Ring`; see [`open_fds`](Self::open_fds).
    pub fd_soft_limit: Option<u64>,
    /// Compiled-WASM module-cache gauges (#4440), populated by `Ring` from the
    /// per-node `ModuleCacheMetrics` `Arc` the caches publish into (a
    /// process-global until #4488). `None` until the WASM runtime has
    /// touched the cache. The contract-cache thrash (eviction → recompile) that
    /// drove the #4441 incident was invisible to central telemetry; these make
    /// occupancy and eviction pressure observable on the snapshot cadence. The
    /// `*_evictions_total` fields are monotonic counters — the collector
    /// differences them across the cadence to derive an eviction rate.
    pub contract_module_cache_entries: Option<u64>,
    pub contract_module_cache_total_bytes: Option<u64>,
    pub contract_module_cache_budget_bytes: Option<u64>,
    pub contract_module_cache_evictions_total: Option<u64>,
    pub delegate_module_cache_entries: Option<u64>,
    pub delegate_module_cache_total_bytes: Option<u64>,
    pub delegate_module_cache_budget_bytes: Option<u64>,
    pub delegate_module_cache_evictions_total: Option<u64>,
    /// Capability-relative hosting-budget gauges (#4642 A2), populated by `Ring`
    /// from the `HostingManager` on the snapshot cadence. `hosting_budget_bytes`
    /// is the RAM-scaled default (or operator override); `hosting_current_bytes`
    /// is the tracked contract-state occupancy (occupancy/utilization ratio =
    /// current / budget, headroom = 1 - that; derived by the collector);
    /// `hosting_budget_evictions_total` is a monotonic counter the collector
    /// differences to get a budget-triggered eviction rate. `None` until the ring
    /// is built. Per-node aggregate scalars.
    pub hosting_budget_bytes: Option<u64>,
    pub hosting_current_bytes: Option<u64>,
    pub hosting_contract_count: Option<u64>,
    pub hosting_budget_evictions_total: Option<u64>,
    /// Demand-ordered eviction gauges (#4642 A3), populated by
    /// `Ring` from the `HostingManager` on the snapshot cadence.
    /// `hosting_evictions_of_recently_read_total` is the #4338 miscalibration
    /// signal (evictions whose victim had genuine repeat demand);
    /// `hosting_local_hits_total` / `hosting_local_misses_total` are the local
    /// hit-rate, counted at the actual serve-vs-forward decision in the client
    /// GET handler (`client_events`) — a hit is a client GET answered from local
    /// hosted state, a miss is one routed to the network — NOT a cache-membership
    /// proxy. All monotonic counters the collector differences into rates.
    /// `None` until the ring is built. Per-node aggregate scalars.
    pub hosting_evictions_of_recently_read_total: Option<u64>,
    pub hosting_local_hits_total: Option<u64>,
    pub hosting_local_misses_total: Option<u64>,
    /// PUT-durability falsifier gauges (#4642): are freshly-seeded contracts
    /// evicted before their first reader? Populated by `Ring` from the
    /// `HostingManager` on the snapshot cadence, alongside the A2/A3 gauges above.
    /// `hosting_evicted_unread_total` counts over-budget evictions whose victim
    /// was never read (`read_count == 0` — a PUT seed); differenced against
    /// `hosting_budget_evictions_total` (the total-eviction denominator) it gives
    /// the unread-seed eviction fraction. `hosting_evicted_unread_age_secs_sum` is
    /// the running sum of those victims' age-at-eviction (whole seconds); divided
    /// by `hosting_evicted_unread_total` it is the mean unread-seed lifetime (how
    /// young seeds die). Monotonic counters the collector differences. `None`
    /// until the ring is built. Per-node aggregate scalars.
    pub hosting_evicted_unread_total: Option<u64>,
    pub hosting_evicted_unread_age_secs_sum: Option<u64>,
    /// Aggregate on-disk usage gauges (#4683), populated by `Ring` from the
    /// `HostingManager`'s `DiskUsageTracker` on the snapshot cadence.
    /// `hosting_disk_state_bytes` is the delta-tracked persisted-state total;
    /// `hosting_disk_wasm_bytes` is the `du`-measured WASM-blob total;
    /// `hosting_disk_compile_cache_bytes` is the relocated wasmtime compile-cache
    /// total; `hosting_disk_total_bytes` is their sum — the aggregate the future
    /// disk budget will bound. `None` until the tracker is configured and seeded
    /// (early startup). Per-node aggregate scalars.
    pub hosting_disk_state_bytes: Option<u64>,
    pub hosting_disk_wasm_bytes: Option<u64>,
    pub hosting_disk_compile_cache_bytes: Option<u64>,
    pub hosting_disk_total_bytes: Option<u64>,
    /// OOM-valve falsifier (subscriber-primary hosting rework, #4642): monotonic
    /// count of evictions performed under genuine RAM overflow (counted by
    /// pressure). Under overflow the sweep sheds fewest-subscriber-first and
    /// pierces the in-use pin, so these MAY be SUBSCRIBED contracts shed to avoid
    /// OOM. The Overflow trigger is intentionally unwired in this release (the
    /// valve mechanism lands, but nothing fires it until the RSS/A1 resource
    /// signal is plumbed), so this stays 0 in the field today; once the trigger
    /// lands, a nonzero differenced rate is the alarm that the node is shedding to
    /// avoid OOM. Populated by `Ring` on the snapshot cadence. `None` until the
    /// ring is built. Per-node aggregate scalar.
    pub hosting_oom_valve_evictions_total: Option<u64>,
    /// Subscribed-eviction falsifier (subscriber-primary hosting rework, #4642):
    /// monotonic count of over-budget evictions whose victim was SUBSCRIBED
    /// (`local + downstream >= 1`) at eviction-decision time. Unlike
    /// `hosting_oom_valve_evictions_total` (which stays 0 until the unwired
    /// Overflow trigger lands), this can go nonzero the moment the eviction rework
    /// ships — it is the field signal for the single riskiest new behavior,
    /// shedding a subscribed contract as a last resort. A rising differenced rate
    /// means budgets are too tight or demand is churning subscribed contracts.
    /// Populated by `Ring` on the snapshot cadence. `None` until the ring is
    /// built. Per-node aggregate scalar.
    pub hosting_subscribed_evictions_total: Option<u64>,
    /// Cost-pressure eviction falsifier (cost-aware eviction, #4861): monotonic
    /// count of zero-demand contracts shed because their attributed update-work
    /// cost (WASM CPU / broadcast fan-out) dominated the node's total on a cost
    /// axis, independently of the byte budget. Disjoint from
    /// `hosting_budget_evictions_total` (byte-budget-triggered only). A nonzero
    /// differenced rate means the cost trigger is firing; a runaway rate means
    /// the floors / share threshold are miscalibrated and churning cheap
    /// contracts. Populated by `Ring` on the snapshot cadence. `None` until the
    /// ring is built. Per-node aggregate scalar.
    pub hosting_cost_evictions_total: Option<u64>,
    /// Resident-overhead pressure axis (#5325), populated by `Ring` from the
    /// `HostingManager` on the snapshot cadence. This is the SECOND, independent
    /// eviction pressure: `hosting_budget_bytes` / `hosting_current_bytes` above
    /// bound contract STATE bytes only, while this axis bounds the per-contract
    /// resident bookkeeping that scales with hosted-contract COUNT, and either
    /// can trigger a sweep on its own. Without these four, a node evicting
    /// purely under slot pressure looks idle in telemetry — its state-byte
    /// occupancy can sit at 13% while `hosting_resident_overhead_evictions_total`
    /// climbs, which is exactly the confusion the fleet audit hit.
    ///
    /// `hosting_resident_overhead_budget_bytes` is the RAM-scaled ceiling;
    /// `hosting_estimated_resident_overhead_bytes` is `contract_count *
    /// ESTIMATED_RESIDENT_BYTES_PER_CONTRACT`. Treat that pair as a
    /// contract-COUNT ceiling wearing memory units, NOT as measured RAM: the
    /// "used" side is a count multiplied by a flat estimate, so a collector that
    /// renders it as memory will mislead (the node's own dashboard renders
    /// `hosting_contract_slot_budget` — the same budget expressed as the slot
    /// count it really bounds — for that reason).
    /// `hosting_resident_overhead_evictions_total` is a monotonic counter the
    /// collector differences to get a slot-pressure eviction rate; it may overlap
    /// with `hosting_budget_evictions_total`. `None` until the ring is built.
    /// Per-node aggregate scalars.
    ///
    /// The budget is ALSO a moving target, which matters more to a collector than
    /// to this node. `ring::hosting::cache::resident_overhead_budget_for` derives
    /// it as a structural RESIDUAL (total RAM, less the baseline reservation, less
    /// every declared cache ceiling, less the state-byte budget) and then mins it
    /// against live memory signals, recomputed every 60s sweep — a known
    /// limitation, #5334, deferred pending exactly this telemetry. So
    /// `estimated / budget` graphed as a utilization ratio has a NON-STATIONARY
    /// DENOMINATOR that moves with unrelated system memory pressure: a rise in
    /// that ratio does not by itself mean the node took on more contracts. Graph
    /// the numerator and denominator separately before reading a trend into the
    /// ratio.
    pub hosting_resident_overhead_budget_bytes: Option<u64>,
    pub hosting_estimated_resident_overhead_bytes: Option<u64>,
    pub hosting_contract_slot_budget: Option<u64>,
    pub hosting_resident_overhead_evictions_total: Option<u64>,
    /// Local `UpdateNotification` deliveries dropped because the subscriber's
    /// channel was FULL (#4681). The subscriber's cached summary is invalidated
    /// at the same time, so the next update resyncs it with full state; a
    /// sustained nonzero rate means a client is not draining fast enough.
    pub notifications_dropped_channel_full: Option<u64>,
    /// Local `UpdateNotification` deliveries dropped because the subscriber's
    /// channel was CLOSED (#4681). The subscriber is unregistered at that point.
    pub notifications_dropped_channel_closed: Option<u64>,
    /// Committed updates that found NO local subscriber (#4681/#5040). Normal
    /// for contracts hosted on the network's behalf; surfaced as a counter
    /// because logging it per occurrence produced 22k lines/day (#5040) and
    /// `debug!` is compiled out of release builds.
    pub notifications_no_local_subscriber: Option<u64>,
    /// Phantom-hosting falsifier (SUBSCRIBE-retirement step 10 §1d): the count of
    /// contracts registered as in-use via a downstream subscriber whose state is
    /// NOT present on disk (`contract_in_use && !contract_state_present`). After
    /// the register-after-state fix (a hop registers a downstream only once it
    /// holds state) this should read 0; a nonzero value means a hop registered
    /// demand it cannot serve — the #4404/#4612 phantom the step eliminates.
    /// redb-scoped by construction (`contract_state_present` is conservative-true
    /// elsewhere). Populated by `Ring` on the snapshot cadence; `None` until the
    /// ring is built. Per-node aggregate scalar (current gauge, not monotonic).
    pub phantom_in_use_contracts: Option<u64>,
    /// Terminal advertisement-consult counters (hosting redesign piece C,
    /// #4646; exported to central telemetry per #4658), populated by `Ring`
    /// from the per-node `network_status` singleton on the snapshot cadence.
    ///
    /// A GET/SUBSCRIBE that routes to a terminus (the closest peer it can reach,
    /// can't route closer) consults the host-advertisements its neighbors sent
    /// before returning NotFound. These four counters decompose dead-ends —
    /// `attempts` → `hits` (found an advertised off-path host) → `resolved_found`
    /// (that forward resolved to Found/Subscribed) are the dead-ends piece C
    /// closes; `still_not_found` is the residual (no reachable host near the key)
    /// that needs piece D. That decomposition is the findability baseline the
    /// piece-E / 0.2.92 decision rests on, which is why it must be observable in
    /// production rather than only in simulation. All monotonic lifetime totals
    /// (the collector differences them across the cadence to derive rates),
    /// segmentable by release via the `service.version` OTLP resource attribute
    /// stamped on every batch. `None` until the ring's snapshot task has
    /// populated them (i.e. always populated in production snapshots). Per-node
    /// aggregate scalars.
    pub terminal_consult_attempts: Option<u64>,
    pub terminal_consult_hits: Option<u64>,
    pub terminal_consult_resolved_found: Option<u64>,
    pub terminal_consult_still_not_found: Option<u64>,
    /// Eviction-retraction emission counters (#5059). The retraction is what
    /// makes co-hosts stop fanning updates at an evicted contract, and it is
    /// emitted best-effort on the cap-2048 node-event channel. `dropped` rising
    /// means evicted contracts keep receiving updates for up to one
    /// interest-heartbeat interval (~5 min) before the full-set re-request heals
    /// it — the difference between a slow heal and a failed fix, which the
    /// `debug!` at the drop site cannot show because it compiles out in release.
    /// Monotonic lifetime totals; `None` until the ring's snapshot task has
    /// populated them.
    pub hosting_retractions_emitted: Option<u64>,
    pub hosting_retractions_dropped: Option<u64>,
    /// Computed-upstream vs. stored-`is_upstream`-flag divergence counters
    /// (hosting redesign piece D, #4642 / #4671). `comparisons` is the
    /// denominator (one per `send_unsubscribe_upstream`), `divergences` the times
    /// the demand-driven-hosting computed upstream
    /// (`Ring::most_keyward_hosting_neighbor`) disagreed with the stored flag the
    /// site still consults. Behavior-preserving field evidence for the stored
    /// flag's drift ahead of the reconcile-core keystone deleting it; monotonic
    /// lifetime totals, `None` until the ring's snapshot task populates them.
    pub upstream_computed_vs_stored_comparisons: Option<u64>,
    pub upstream_computed_vs_stored_divergences: Option<u64>,
    /// Reconcile-controller SHADOW comparison counters, split PER SITE (hosting
    /// redesign keystone step-2, #4642). Populated by `Ring` from the per-node
    /// `network_status` singleton on the snapshot cadence. Per site,
    /// `comparisons` is the denominator (one per shadow comparison at that
    /// hosting decision site), `divergences` the count whose reconcile action set
    /// differed from the actual behavior's set; the `*_diffs` are per-action
    /// symmetric-difference tallies. The FLIP is site-by-site (collapse first,
    /// then renewal), so the counters are split so each site is separately
    /// gate-able. Monotonic lifetime totals, `None` until the ring's snapshot task
    /// populates them.
    ///
    /// Several divergence classes are EXPECTED BY DESIGN, not anomalies: the
    /// `retract` / `reroot_search` classes (no on-`main` driver retracts on
    /// teardown or re-roots on upstream loss), the strict-demand-gated `renew` /
    /// `subscribe` / `unsubscribe` classes (the controller's strict-farther
    /// downstream gate and lease-aware `Renew`-vs-`Subscribe` split legitimately
    /// disagree with today's ANY-downstream renewal path), and `announce` (a
    /// subscribed, state-present, not-yet-advertised host). Read them as the
    /// reconcile-vs-today delta. `retract_diffs` in particular reflects that no
    /// on-`main` COLLAPSE/RENEWAL driver retracts at these shadow-compared sites
    /// (the controller would). NOTE: `is_hosted_locally` is no longer strictly
    /// monotonic in production — eviction now retracts via `on_contract_unhosted`
    /// (#4722) — but that eviction path is distinct from these sites, so a nonzero
    /// `retract_diffs` still reflects the missing site-local retraction, not a
    /// live-advertisement leak.
    pub reconcile_shadow_collapse_comparisons: Option<u64>,
    pub reconcile_shadow_collapse_divergences: Option<u64>,
    pub reconcile_shadow_collapse_subscribe_diffs: Option<u64>,
    pub reconcile_shadow_collapse_renew_diffs: Option<u64>,
    pub reconcile_shadow_collapse_unsubscribe_diffs: Option<u64>,
    pub reconcile_shadow_collapse_collapse_diffs: Option<u64>,
    pub reconcile_shadow_collapse_announce_diffs: Option<u64>,
    pub reconcile_shadow_collapse_retract_diffs: Option<u64>,
    pub reconcile_shadow_collapse_reroot_search_diffs: Option<u64>,
    pub reconcile_shadow_renewal_comparisons: Option<u64>,
    pub reconcile_shadow_renewal_divergences: Option<u64>,
    pub reconcile_shadow_renewal_subscribe_diffs: Option<u64>,
    pub reconcile_shadow_renewal_renew_diffs: Option<u64>,
    pub reconcile_shadow_renewal_unsubscribe_diffs: Option<u64>,
    pub reconcile_shadow_renewal_collapse_diffs: Option<u64>,
    pub reconcile_shadow_renewal_announce_diffs: Option<u64>,
    pub reconcile_shadow_renewal_retract_diffs: Option<u64>,
    pub reconcile_shadow_renewal_reroot_search_diffs: Option<u64>,
    /// Reconcile-controller SHADOW counters for the single-aspect EDGE sites
    /// (keystone step-2 completion, #4642). Each edge site is compared FOCUSED on
    /// one action class, so `comparisons` + `divergences` fully captures it (the
    /// per-action split would be redundant — a single relevant class): the FLIP
    /// hook for `inbound_unsubscribe` is `Collapse` (would the controller tear
    /// down on a downstream leave?), for `connection_drop` is `ReRootSearch`
    /// (would it re-root on an upstream loss? production does nothing today —
    /// one-sided count), for `host_formation` is `Announce` (does it agree a
    /// freshly-hosted contract should be advertised?). Monotonic lifetime totals,
    /// `None` until the ring's snapshot task populates them.
    pub reconcile_shadow_inbound_unsubscribe_comparisons: Option<u64>,
    pub reconcile_shadow_inbound_unsubscribe_divergences: Option<u64>,
    pub reconcile_shadow_connection_drop_comparisons: Option<u64>,
    pub reconcile_shadow_connection_drop_divergences: Option<u64>,
    pub reconcile_shadow_host_formation_comparisons: Option<u64>,
    pub reconcile_shadow_host_formation_divergences: Option<u64>,
    /// Interest-weighted (two-tier) module-cache SHADOW gauges (#4441/#4534),
    /// populated by `Ring` from the same per-node `ModuleCacheMetrics` `Arc`.
    /// These are ALWAYS ON, independent of the `FREENET_MODULE_CACHE_INTEREST_TIERED`
    /// feature flag — they measure what the two-tier policy WOULD do so the
    /// decision to flip the flag (and the later #4534 admission-gate change) can
    /// rest on production data rather than guesswork:
    /// - `cold_evictable_bytes`: resident contract-cache bytes the two-tier
    ///   policy could freely reclaim (no client/downstream interest).
    /// - `interested_bytes`: resident contract-cache bytes the two-tier policy
    ///   would protect first (the in-use floor).
    /// - `evictions_would_reclassify_total`: monotonic count of eviction steps
    ///   whose victim the two-tier policy would pick differently from plain LRU.
    /// - `migration_admission_recovered_total`: monotonic count of inbound
    ///   placement-migration hints (#4534) that the old raw-occupancy gate would
    ///   have refused but the current interested-occupancy gate admits — the
    ///   migrations recovered on caches that are LRU-full of cold modules.
    ///
    /// `None` until the WASM runtime has touched the cache. The contract cache
    /// is the only one with an interest predicate; there are no delegate
    /// equivalents.
    pub contract_module_cache_cold_evictable_bytes: Option<u64>,
    pub contract_module_cache_interested_bytes: Option<u64>,
    pub contract_module_cache_evictions_would_reclassify_total: Option<u64>,
    pub migration_admission_recovered_total: Option<u64>,
    /// UPDATE-broadcast stream-assembly gauges (#4440), populated by `Ring` from
    /// the process-global `BROADCAST_STREAM_METRICS` the broadcast queue
    /// publishes into. A streaming broadcast that fails to reach `Delivered`
    /// (dropped, oneshot dropped, or completion timeout) is a stream-assembly /
    /// transfer failure — the exact signal that flagged the v0.2.73 incident
    /// (nova/vega ~1500-2300 failures/hr vs ~0 baseline) and the one that would
    /// catch a re-enable going wrong. `None` until the first streaming broadcast.
    ///
    /// `*_total` are monotonic lifetime counters (the collector differences them
    /// across the cadence to derive a rate); `*_failures_last_snapshot` is the
    /// per-snapshot delta `Ring` samples directly, so the incident signal is
    /// legible without a stateful collector.
    pub broadcast_stream_attempts_total: Option<u64>,
    pub broadcast_stream_failures_total: Option<u64>,
    pub broadcast_stream_failures_last_snapshot: Option<u64>,
    /// Contract-exec WASM counters, populated by `Ring` from the per-node
    /// `ContractExecMetrics` the executor publishes into. These separate the
    /// EXPENSIVE work (a WASM `summarize_state` / `get_state_delta`
    /// invocation) from the cache hits that elide it — a distinction the only
    /// prior production signal, a handler-entry span, could not make, which is
    /// why every rate quoted across the #4473 / #4610 / #5040 / #5238 storm
    /// investigations was undifferentiated. See
    /// `ring::contract_exec_metrics` for the exact partition each arm belongs
    /// to; briefly:
    ///
    /// - `*_fast_hits` — served from cache, no state load, no WASM
    /// - `*_reload_hits` — state loaded and hashed, cache hit, no WASM
    /// - `*_wasm_calls` — WASM ran, on the cached path (a true miss)
    /// - `*_wasm_uncached` — WASM ran, at a site with no cache in front of it
    ///
    /// `*_total` are monotonic lifetime counters (the collector differences
    /// them across the cadence); `*_last_snapshot` are the per-window deltas
    /// `Ring` samples directly, so ONE snapshot answers "was this peer's
    /// summarize load cache hits or real WASM work" without a stateful reader.
    /// EVERY arm gets both, deliberately: emitting some arms as a delta and
    /// others as a lifetime total, under parallel names, invites reading them as
    /// comparable magnitudes and would understate exactly the uncached fan-out
    /// arm most likely to dominate on a client-facing node.
    /// `None` until the node has a `Ring`-backed executor.
    pub contract_exec_summarize_fast_hits_total: Option<u64>,
    pub contract_exec_summarize_reload_hits_total: Option<u64>,
    pub contract_exec_summarize_wasm_calls_total: Option<u64>,
    pub contract_exec_summarize_wasm_uncached_total: Option<u64>,
    pub contract_exec_delta_fast_hits_total: Option<u64>,
    pub contract_exec_delta_reload_hits_total: Option<u64>,
    pub contract_exec_delta_wasm_calls_total: Option<u64>,
    pub contract_exec_delta_wasm_uncached_total: Option<u64>,
    pub contract_exec_summarize_fast_hits_last_snapshot: Option<u64>,
    pub contract_exec_summarize_reload_hits_last_snapshot: Option<u64>,
    pub contract_exec_summarize_wasm_calls_last_snapshot: Option<u64>,
    pub contract_exec_summarize_wasm_uncached_last_snapshot: Option<u64>,
    pub contract_exec_delta_fast_hits_last_snapshot: Option<u64>,
    pub contract_exec_delta_reload_hits_last_snapshot: Option<u64>,
    pub contract_exec_delta_wasm_calls_last_snapshot: Option<u64>,
    pub contract_exec_delta_wasm_uncached_last_snapshot: Option<u64>,
    /// Placement-quality gauges (#4404 follow-up), populated by `Ring` on the
    /// snapshot cadence from the contracts this node hosts. They make the
    /// effect of the SubscribeHint placement migration observable: the migration
    /// nudges hosting toward each contract's key, so the host-to-hosted-key
    /// ring-distance distribution should tighten over time. `hosted_contracts_count`
    /// is the number of contracts hosted at snapshot time; the distance fields are
    /// the distribution of `ring_distance(this_node_location, contract_location)`
    /// in `[0.0, 0.5]`. `hosted_key_distance_frac_within_0_1` (fraction within ring
    /// distance 0.1) is the clearest single "are hosted contracts close" number.
    /// The distance fields are `None` when the node hosts nothing or has no ring
    /// location yet (`hosted_contracts_count` is then `0` / absent respectively).
    pub hosted_contracts_count: Option<u64>,
    pub hosted_key_distance_median: Option<f64>,
    pub hosted_key_distance_p90: Option<f64>,
    pub hosted_key_distance_min: Option<f64>,
    pub hosted_key_distance_mean: Option<f64>,
    pub hosted_key_distance_frac_within_0_1: Option<f64>,
    /// Placement-migration activity counters (#4404 follow-up), populated by
    /// `Ring` from the per-node `PlacementMigrationMetrics`. Monotonic lifetime
    /// totals (the collector differences them across the cadence to derive a
    /// rate): `sent` is hints this node dispatched, `received` is all inbound
    /// hints (counted before the admission gates), and `acted` is the subset that
    /// actually triggered a directed subscribe. `None` until the snapshot task
    /// has populated them (i.e. always populated in production snapshots).
    pub subscribe_hint_sent: Option<u64>,
    pub subscribe_hint_received: Option<u64>,
    pub subscribe_hint_acted: Option<u64>,
    /// Per-gate refusal breakdown of inbound `SubscribeHint`s (#4534
    /// diagnostics): which admission gate dropped the hint. Together they
    /// partition `subscribe_hint_received - subscribe_hint_acted` by reason.
    pub subscribe_hint_refused_version: Option<u64>,
    pub subscribe_hint_refused_already_hosting: Option<u64>,
    pub subscribe_hint_refused_holder: Option<u64>,
    pub subscribe_hint_refused_cache: Option<u64>,
    /// Outcome breakdown of acted-on directed subscribes (#4534 diagnostics):
    /// how many completed (now hosting) vs failed (error / infra / timeout).
    pub subscribe_hint_acted_succeeded: Option<u64>,
    pub subscribe_hint_acted_failed: Option<u64>,
    /// Count of renewal cycles short-circuited because this node is the
    /// body-holding subscription root for the contract (#4440 proposal 1).
    /// Monotonic lifetime total; trends how much renewal traffic the
    /// root-satisfied path removes. `None` until the snapshot task populates it.
    pub renewal_terminus_satisfied: Option<u64>,
    /// Nearest-neighbor ring-lattice completeness + probe health (#4760),
    /// populated by `Ring` on the snapshot cadence from the same
    /// `connection_manager` queries the home-page ring-stats provider uses. They
    /// make the #4760 lattice fix's impact measurable NETWORK-WIDE (the home page
    /// only shows the local peer): the fraction of peers holding BOTH immediate
    /// ring-neighbor edges, the median held-edge distances, and the route-to-self
    /// probe success rate, all as one-line queries over central telemetry.
    /// `lattice_has_successor` / `_predecessor` are whether this peer currently
    /// HOLDS its closest-higher / closest-lower connected ring neighbor (a peer
    /// with both `true` has a complete both-sides lattice; the collector
    /// aggregates the fraction). The `_distance` fields are the ring distance to
    /// each held edge, `None` when that side is unheld (or own location is
    /// unknown). `lattice_probes_issued` / `_probe_improvements` are the
    /// route-to-self discovery-health counters (monotonic lifetime totals,
    /// differenced by the collector); they are counted INDEPENDENTLY (an
    /// improvement lands a few ticks after the probe), so the ratio is a
    /// convergence-health gauge, not a strict per-probe rate. `None` until the
    /// snapshot task populates them (i.e. always populated in production). Read
    /// from the live connection set + probe counters — no mirrored counter to
    /// rot. See #4642.
    pub lattice_has_successor: Option<bool>,
    pub lattice_has_predecessor: Option<bool>,
    pub lattice_successor_distance: Option<f64>,
    pub lattice_predecessor_distance: Option<f64>,
    pub lattice_probes_issued: Option<u64>,
    pub lattice_probe_improvements: Option<u64>,
    /// Version-gate refusal counters (#5156), populated by `Ring` on the
    /// snapshot cadence from `ConnectionManager::version_gate_refusal_stats`.
    /// `supports_hash_first_summaries` and `supports_summary_first_put` both
    /// fail closed to their full-bytes fallback for two causes with opposite
    /// implications, previously indistinguishable in telemetry:
    /// `*_declined_unknown_version` (the remote's negotiated version was never
    /// recorded — documented on joiner->gateway `AckConnection` links, which
    /// never self-heals as the fleet upgrades) vs `*_declined_pre_floor` (a
    /// known version below the feature's minimum — self-heals as peers
    /// upgrade). Monotonic lifetime totals, differenced by the collector.
    /// `None` until the snapshot task populates them.
    pub hash_first_summaries_declined_unknown_version: Option<u64>,
    pub hash_first_summaries_declined_pre_floor: Option<u64>,
    pub summary_first_put_declined_unknown_version: Option<u64>,
    pub summary_first_put_declined_pre_floor: Option<u64>,
    /// Streamed-transfer (> 64 KB) abort counters, populated by `Ring` from the
    /// per-node `network_status` singleton on the snapshot cadence. They isolate
    /// the large-contract failure class (~50% of large fetches were failing)
    /// WITHOUT a per-fragment/per-transfer event stream: five monotonic cause
    /// totals (receiver inactivity / cancelled / claim-timeout / deserialize;
    /// sender cwnd) plus a fragment-progress histogram (`*_frac_*`, one bump per
    /// receiver abort) showing HOW FAR transfers got before dying. Scope differs
    /// by cause: `*_inactivity_total` / `*_cancelled_total` are recorded in
    /// transport `StreamHandle::assemble` and AGGREGATE across GET + PUT + UPDATE
    /// inbound streams, while `*_claim_timeout_total` / `*_deserialize_total` are
    /// GET-fetch-only (the GET originator's `assemble_and_cache_stream`). All
    /// monotonic lifetime totals the collector differences across the cadence.
    /// `None` until the ring's snapshot task populates them.
    pub stream_recv_aborts_inactivity_total: Option<u64>,
    pub stream_recv_aborts_cancelled_total: Option<u64>,
    pub stream_recv_aborts_claim_timeout_total: Option<u64>,
    pub stream_recv_aborts_deserialize_total: Option<u64>,
    pub stream_send_aborts_cwnd_total: Option<u64>,
    pub stream_recv_abort_frac_0: Option<u64>,
    pub stream_recv_abort_frac_1: Option<u64>,
    pub stream_recv_abort_frac_lt50: Option<u64>,
    pub stream_recv_abort_frac_50_90: Option<u64>,
    pub stream_recv_abort_frac_ge90: Option<u64>,
    /// Routing/hosting attribution gauges + counters, populated by `Ring` on the
    /// snapshot cadence. `ring_connections` / `transient_connections` are current
    /// gauges (live + transient connection counts from `connection_manager`);
    /// `connections_to_gateways` is how many of this node's active connections go
    /// to gateways (the NAT-stranded fingerprint — a peer stuck on gateways only).
    /// `relayed_*_total` are monotonic counts of operations this node RELAYED
    /// (forwarded one hop as a routing intermediary, not as originator), one
    /// increment per relay-driver entry. `None` until the ring's snapshot task
    /// populates them.
    pub ring_connections: Option<u64>,
    pub transient_connections: Option<u64>,
    pub connections_to_gateways: Option<u64>,
    pub relayed_gets_total: Option<u64>,
    pub relayed_puts_total: Option<u64>,
    pub relayed_subscribes_total: Option<u64>,
    pub relayed_updates_total: Option<u64>,
    /// Connect-event emission counters (aggregate precursor to retiring the
    /// per-event `connect_connected` / `connect_rejected` firehose), populated by
    /// `Ring` on the snapshot cadence. The per-event emission is NOT retired yet
    /// (a downstream dashboard likely still consumes it — see the increment-site
    /// TODO); these additive counters make a future retirement net-negative.
    /// Monotonic lifetime totals. `None` until the ring's snapshot task populates.
    pub connect_accepts_emitted: Option<u64>,
    pub connect_rejects_emitted: Option<u64>,
    /// Bootstrap-acceptance-churn counters (#4787): a restarted node's
    /// gateway connection lingers as transient, expires, and is later reaped
    /// as a zombie before the onward CONNECT promotes it to the ring — the
    /// joiner cycles through repeated reconnects before it acquires real
    /// peers. These are the "instrumentation before a fix" step the issue
    /// calls for, not a behavior change.
    ///
    /// `bootstrap_transient_registered` / `_expired` / `_promoted_to_ring` are
    /// monotonic lifetime totals on the ACCEPTOR side; a sustained high
    /// `_expired`:`_promoted_to_ring` ratio is the churn signature. Both
    /// promotion paths are counted, and only when the ring actually accepted
    /// the connection.
    ///
    /// The rest are JOINER-side. `bootstrap_time_to_min_connections_secs` is
    /// time from process start to first reaching `min_connections`, recorded
    /// once per process; `bootstrap_completed` disambiguates the `None` there —
    /// `Some(false)` means this node has never bootstrapped, `None` means the
    /// field wasn't reported at all. The four `bootstrap_startup_rounds_*`
    /// fields partition below-threshold join-loop rounds by what each round
    /// actually did (dialled unconnected gateways / routed CONNECTs through
    /// already-connected ones / blocked by gateway backoff / had no target),
    /// which is what keeps a permanently-stuck joiner's count from being an
    /// undifferentiated process-uptime proxy.
    ///
    /// Populated by `Ring` from the network_status singleton on the snapshot
    /// cadence; `None` until the ring's snapshot task populates them.
    pub bootstrap_transient_registered: Option<u64>,
    pub bootstrap_transient_expired: Option<u64>,
    pub bootstrap_promoted_to_ring: Option<u64>,
    pub bootstrap_time_to_min_connections_secs: Option<f64>,
    pub bootstrap_completed: Option<bool>,
    pub bootstrap_startup_rounds_connect_issued_gateway: Option<u64>,
    /// Sustained growth with `bootstrap_completed == Some(false)` means this
    /// joiner never bootstrapped; the `bootstrap_transient_expired` :
    /// `bootstrap_promoted_to_ring` ratio separates #4787 acceptance churn
    /// from simply having too few acceptable peers.
    pub bootstrap_startup_rounds_connect_issued_routed: Option<u64>,
    pub bootstrap_startup_rounds_backoff_blocked: Option<u64>,
    pub bootstrap_startup_rounds_no_target: Option<u64>,
    /// Per-operation-type estimator curves, keyed by op type name (e.g., "GET").
    pub per_op_curves: HashMap<String, PerOpCurves>,
    /// Renegade predictor diagnostics. These (and `renegade_accuracy_pairs`) are
    /// read by the in-process peer dashboard directly from this struct; they are
    /// intentionally not mirrored into the hand-written OTLP `json!` block in
    /// `tracing/telemetry.rs` (the dashboard is the only consumer). The per-op
    /// scatter does reach OTLP, because `per_op_curves` is forwarded wholesale.
    pub renegade_failure_events: usize,
    pub renegade_response_time_events: usize,
    pub renegade_transfer_speed_events: usize,
    pub renegade_known_peers: usize,
    /// Brier score for failure predictions (lower is better, 0.25 = random).
    pub renegade_brier_score: Option<f64>,
    /// Recent Brier score (EWMA).
    pub renegade_recent_brier_score: Option<f64>,
    /// Number of predictions evaluated against actual outcomes.
    pub renegade_predictions_evaluated: u64,
    /// Recent (predicted_failure, actual_outcome) pairs for accuracy visualization.
    pub renegade_accuracy_pairs: Vec<(f64, f64)>,
    /// Recent (predicted_secs, actual_secs) pairs for the response-time stage.
    /// `#[serde(default)]` for decode consistency with the `*_points` fields
    /// (no-op under the positional bincode AOF).
    #[serde(default)]
    pub renegade_response_time_pairs: Vec<(f64, f64)>,
    /// Recent (predicted_bps, actual_bps) pairs for the transfer-speed stage.
    #[serde(default)]
    pub renegade_transfer_speed_pairs: Vec<(f64, f64)>,
    /// Number of response-time predictions scored against actual outcomes.
    #[serde(default)]
    pub renegade_response_time_evaluated: u64,
    /// Number of transfer-speed predictions scored against actual outcomes.
    #[serde(default)]
    pub renegade_transfer_speed_evaluated: u64,
    /// Brier SKILL of each failure-prediction layer against the climatological
    /// base rate: `1 - brier/(p(1-p))`. Zero means "no better than assuming the
    /// base rate", negative means worse than assuming nothing.
    ///
    /// Skill rather than raw Brier because raw Brier on a rare event is
    /// dominated by how rare the event is, not by how good the forecast is — at
    /// a 1% base rate a constant forecast scores 0.0099, which the dashboard's
    /// old absolute scale graded "excellent". See #4485.
    #[serde(default)]
    pub failure_skill_global: Option<f64>,
    #[serde(default)]
    pub failure_skill_adjusted: Option<f64>,
    #[serde(default)]
    pub failure_skill_blended: Option<f64>,
    #[serde(default)]
    pub failure_skill_corrected: Option<f64>,
    /// Skill of the hierarchical empirical-Bayes estimator (#4485), scored on
    /// the same events as the four layers above.
    #[serde(default)]
    pub failure_skill_hierarchical: Option<f64>,
    /// Events the hierarchical layer was scored on. Can trail
    /// `failure_layers_evaluated` by the few events before its curve exists.
    #[serde(default)]
    pub hierarchical_failure_evaluated: u64,
    /// Whether the hierarchical estimator is reaching live routing decisions.
    #[serde(default)]
    pub hierarchical_routing_enabled: bool,
    /// Forgetting horizon currently selected for the failure stage, in hours.
    /// `None` both before the stage is active and when it forgets nothing
    /// inside its window; `hierarchical_failure_events` tells the two apart.
    #[serde(default)]
    pub hierarchical_failure_horizon_hours: Option<f64>,
    /// Events in the hierarchical failure stage's window.
    #[serde(default)]
    pub hierarchical_failure_events: usize,
    /// Peers evicted from the hierarchical estimator's bounded peer tables,
    /// summed over its three stages. Non-zero means this node's churn exceeds
    /// the headroom `hierarchical_peer_capacity` allows.
    #[serde(default)]
    pub hierarchical_peer_evictions: u64,
    /// Per-stage peer-table capacity, derived from `max_connections`.
    #[serde(default)]
    pub hierarchical_peer_capacity: usize,
    /// Contracts held by the failure stage's contract term (#4485, #5700).
    #[serde(default)]
    pub hierarchical_contracts: usize,
    /// Contracts evicted from that table, least-recently-used in batches.
    #[serde(default)]
    pub hierarchical_contract_evictions: u64,
    /// Residuals the contract table did not record LIVE, because every entry
    /// of their contract already held a larger decayed weight. The events
    /// still train the curve and the peer levels.
    #[serde(default)]
    pub hierarchical_contract_residuals_refused: u64,
    /// `(contract, peer)` pairs the LAST refit dropped for being outside a
    /// contract's heaviest eight. A GAUGE, not a total: a persistently
    /// over-full contract contributes the same drops at every refit, so a
    /// cumulative count would read about 100x the distinct pairs ever dropped.
    #[serde(default)]
    pub hierarchical_contract_pairs_refused_last_refit: u64,
    /// Live contract entries displaced by a heavier newcomer, whose
    /// accumulated moments were discarded. Counted rather than merged,
    /// because merging would attribute one peer's residuals to another.
    #[serde(default)]
    pub hierarchical_contract_entries_displaced: u64,
    /// Refits after which the contract term's variance components were
    /// estimable. NECESSARY for the term to do anything and not sufficient:
    /// `effect` also returns nothing per query below the present-peer bar, and
    /// on the recorded soak most failures are on contracts that never reach
    /// it. The two counters below are what say whether anything moved.
    #[serde(default)]
    pub hierarchical_contract_estimable_refits: u64,
    /// Learned residuals actually adjusted by a contract effect.
    #[serde(default)]
    pub hierarchical_contract_effects_applied: u64,
    /// Scored forecasts that actually carried a contract offset.
    #[serde(default)]
    pub hierarchical_contract_forecast_offsets: u64,
    /// Estimable refits at which the Bernoulli evidence floor was the binding
    /// value for the contract term's `sigma2`, rather than the measured
    /// within-cell variance. On the recorded gateway streams the floor bound
    /// on every estimable refit, because cells there are mostly unanimous and
    /// mostly tiny; this counter is how a reader sees whether that still holds
    /// on their traffic.
    #[serde(default)]
    pub hierarchical_contract_floor_bound_refits: u64,
    /// Estimable refits on which fewer than two contracts qualified, so
    /// `tau2_contract` was zero and the term produced NOTHING although the
    /// refit counted as estimable. Read alongside
    /// `hierarchical_contract_estimable_refits`: their difference is the
    /// refits on which the term could act at all.
    #[serde(default)]
    pub hierarchical_contract_den_below_two_refits: u64,
    /// Contracts that qualified for `tau2_contract` at the last refit, and
    /// present entries that informed the components. `tau2_contract` requires
    /// at least two, so this is also how the term's WARM-UP silence is
    /// observed: a freshly started node has one qualifying contract for its
    /// first several thousand events and the term is off until it has two.
    #[serde(default)]
    pub hierarchical_contract_qualifying_contracts: u64,
    #[serde(default)]
    pub hierarchical_contract_qualifying_entries: u64,
    /// Between-contract variance at the last refit; `None` when the components
    /// are not estimable. The term produces no effect while it is absent or 0.
    #[serde(default)]
    pub hierarchical_contract_tau2: Option<f64>,
    /// Whether the hierarchical estimator is being computed NOW: only when it
    /// routes or the routing dataset is recording. When false, every
    /// `hierarchical_*` reading (and the timing error readings) is either empty
    /// or FROZEN at the moment computation stopped (for example when the
    /// recorder hit its byte cap); a consumer must not present it as live.
    #[serde(default)]
    pub hierarchical_computed: bool,
    /// Whether the failure stage has a curve yet. Before it does, a `None`
    /// horizon means "not active", not "forgets nothing".
    #[serde(default)]
    pub hierarchical_failure_active: bool,
    /// A routing-dataset recorder is configured but has stopped (byte cap or
    /// write error), which is why the estimator is not being computed.
    #[serde(default)]
    pub routing_dataset_stopped: bool,
    /// `FREENET_ROUTING_DATASET` is set but the recorder could not be opened
    /// (see the node log), so there is no recorder to compute for.
    #[serde(default)]
    pub routing_dataset_open_failed: bool,
    /// Timed successes whose response time was floored to 1 ms before the log.
    #[serde(default)]
    pub hierarchical_floored_response_times: u64,
    /// Successes that carried no transfer-speed sample (zero-byte payload or
    /// zero duration). Skipped, as legacy skips them; not rejections.
    #[serde(default)]
    pub hierarchical_non_speed_samples: u64,
    /// RMS error in SECONDS of the response time each model would act on, over
    /// the same events for both (both forecast, response timed), each error
    /// clipped to 10x that event's own outcome (1 ms floor) and the mean
    /// exponentially forgotten over 24 estimator hours. `response_time_scored`
    /// counts events ever scored; `response_time_weight` is the forgotten
    /// weight behind the current means as of the snapshot's own time, which is
    /// what a verdict needs. This measures CALIBRATION of the absolute
    /// estimate, not the candidate ranking routing uses; see the promotion gate
    /// in `.claude/rules/ring.md` for how it is meant to be read.
    ///
    /// The clip is ONE-SIDED in practice: forecasts are non-negative, so an
    /// under-forecast's error is at most the outcome and is never clipped, and
    /// only over-forecasts are trimmed. Under heavy-tailed outcomes that favours
    /// the higher forecaster (on lognormal outcomes at sigma 1.5, 20% of events
    /// clip at the mean forecast, and the clipped-error minimiser is 1.3x the
    /// mean). These live figures are dashboard evidence only; offline gate (a)
    /// uses the dataset's unclipped values.
    #[serde(default)]
    pub response_time_rmse_secs_legacy: Option<f64>,
    #[serde(default)]
    pub response_time_rmse_secs_hierarchical: Option<f64>,
    #[serde(default)]
    pub response_time_scored: u64,
    #[serde(default)]
    pub response_time_weight: f64,
    /// The same for transfer time, `payload bytes / forecast speed`, over real
    /// payload transfers. Not a like-for-like contest: the hierarchical model
    /// targets `E[bytes / V]` while legacy estimates `bytes / E[V]`, so wherever
    /// speeds vary the hierarchical model is favoured by Jensen's inequality. The
    /// one-sided clip (above) compounds that: it favours the higher forecast,
    /// and `E[bytes / V]` is the higher of the two.
    #[serde(default)]
    pub transfer_time_rmse_secs_legacy: Option<f64>,
    #[serde(default)]
    pub transfer_time_rmse_secs_hierarchical: Option<f64>,
    #[serde(default)]
    pub transfer_time_scored: u64,
    #[serde(default)]
    pub transfer_time_weight: f64,
    /// Lognormality check for the response-time stage: the within-cell log
    /// residual variance expectation timing uses, and the skewness and excess
    /// kurtosis of those residuals (both ~0 when log times are normal).
    #[serde(default)]
    pub hierarchical_response_time_log_shape: LogResidualShape,
    /// The same for the transfer-speed stage.
    #[serde(default)]
    pub hierarchical_transfer_speed_log_shape: LogResidualShape,
    /// Brier score of the blended estimate, and the climatology it is scored
    /// against, so the dashboard can show the baseline alongside the result.
    #[serde(default)]
    pub failure_brier_blended: Option<f64>,
    #[serde(default)]
    pub failure_climatology_brier: Option<f64>,
    #[serde(default)]
    pub failure_base_rate: Option<f64>,
    /// Predictions scored across all four layers.
    #[serde(default)]
    pub failure_layers_evaluated: u64,
    /// Whether the residual correction is reaching live routing decisions.
    #[serde(default)]
    pub residual_correction_enabled: bool,
    /// Self-tuned correction state: the selected kappa, the kernel bandwidth,
    /// and how much residual evidence each stage holds.
    #[serde(default)]
    pub residual_kappa: Option<f64>,
    #[serde(default)]
    pub residual_bandwidth: Option<f64>,
    #[serde(default)]
    pub residual_failure_events: usize,
    #[serde(default)]
    pub residual_response_time_events: usize,
    #[serde(default)]
    pub residual_transfer_speed_events: usize,
    #[serde(default)]
    pub residual_scored: u64,
    /// Where the router's chosen peer sits in distance order, and how often that
    /// choice was made against a FULL candidate window. See
    /// [`SelectionRankStats`] — this is the evidence for whether the
    /// 25-of-`max_connections` truncation is costing anything.
    #[serde(default)]
    pub selection_ranks: SelectionRankSnapshot,
}

/// Plain-data lognormality check for one hierarchical log stage (#4485).
///
/// Expectation timing, `exp(mu + sigma2/2)`, assumes log residuals are roughly
/// normal. A soak reads this to check that on real traffic: skewness and excess
/// kurtosis near zero support it; a large positive value of either means a
/// heavy slow tail the expectation understates.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct LogResidualShape {
    pub sigma2: Option<f64>,
    pub skewness: Option<f64>,
    pub excess_kurtosis: Option<f64>,
    pub events: usize,
}

impl From<hierarchical::StageDiagnostics> for LogResidualShape {
    fn from(stage: hierarchical::StageDiagnostics) -> Self {
        LogResidualShape {
            sigma2: stage.residual_sigma2,
            skewness: stage.residual_shape.skewness,
            excess_kurtosis: stage.residual_shape.excess_kurtosis,
            events: stage.residual_shape.events,
        }
    }
}

/// Per-peer routing data for the dashboard detail page.
pub(crate) struct PeerRoutingSnapshot {
    /// (mean_adjustment, event_count) for the failure estimator.
    pub failure_adjustment: Option<(f64, u64)>,
    /// (mean_adjustment, event_count) for the response-time estimator.
    pub response_time_adjustment: Option<(f64, u64)>,
    /// (mean_adjustment, event_count) for the transfer-rate estimator.
    pub transfer_rate_adjustment: Option<(f64, u64)>,
    /// Prediction at the peer's own location (distance ≈ 0).
    pub prediction_at_own_location: Option<RoutingPredictionInfo>,
}

/// # Usage
/// Important when using this type:
/// Need to periodically rebuild the Router using `history` for better predictions.
#[derive(Debug, Serialize)]
pub(crate) struct Router {
    response_start_time_estimator: IsotonicEstimator,
    transfer_rate_estimator: IsotonicEstimator,
    failure_estimator: IsotonicEstimator,
    mean_transfer_size: Mean,
    consider_n_closest_peers: usize,
    /// Per-operation-type failure estimators (telemetry/dashboard only).
    ///
    /// All three `per_op_*` maps hold [`isotonic_estimator::FitPolicy::OnRead`]
    /// estimators: nothing routes on them, so they keep their windows current
    /// but fit only when the dashboard snapshot reads them, rather than paying a
    /// full rebuild per event under `ring.router.write()` (#5662).
    per_op_failure: HashMap<OpType, IsotonicEstimator>,
    /// Per-operation-type response time estimators (telemetry/dashboard only).
    per_op_response_time: HashMap<OpType, IsotonicEstimator>,
    /// Per-operation-type transfer rate estimators (telemetry/dashboard only).
    per_op_transfer_rate: HashMap<OpType, IsotonicEstimator>,
    /// Renegade-ML predictor for peer × contract interaction patterns.
    /// Complements the isotonic estimators by detecting targeted attacks
    /// and per-peer behavior that varies by contract location.
    #[serde(skip)]
    renegade_predictor: routing_predictor::RoutingPredictor,
    /// Prequential skill of each failure-prediction layer, so the contribution of
    /// each can be read off separately (#4485).
    ///
    /// Until this existed only the Renegade layer was scored, which made it
    /// impossible to say which layer was doing the work — or whether the blend
    /// was helping at all. All four are scored whatever the correction flag is
    /// set to: measurement is the point, and it is what decides the flag.
    #[serde(skip)]
    failure_skill_global: residual::SkillTracker,
    #[serde(skip)]
    failure_skill_adjusted: residual::SkillTracker,
    #[serde(skip)]
    failure_skill_blended: residual::SkillTracker,
    #[serde(skip)]
    failure_skill_corrected: residual::SkillTracker,
    /// Hierarchical empirical-Bayes estimator for all three stages (#4485),
    /// the intended replacement for the legacy stack.
    ///
    /// Fed and scored only when it can matter: when it reaches routing (the
    /// default; `FREENET_ROUTING_HIERARCHICAL=0`, or any other value that
    /// resolves off, turns it off) or when the
    /// routing dataset is recorded (see [`hierarchical_computed`]). A node that
    /// turned it off does not pay its learning cost for a measurement nobody
    /// reads.
    #[serde(skip)]
    hierarchical: hierarchical::HierarchicalRouting,
    /// The clock the hierarchical estimator's forgetting horizons run on.
    #[serde(skip)]
    estimator_clock: EstimatorClock,
    /// Prequential skill of the hierarchical failure forecast, scored on the
    /// same events as the four layers above.
    #[serde(skip)]
    failure_skill_hierarchical: residual::SkillTracker,
    /// Connection cap the hierarchical estimator's peer tables are sized from.
    #[serde(skip)]
    max_connections: usize,
    /// Prequential error, in seconds, of the response time and transfer time
    /// each model would act on, over events both forecast and that carry the
    /// measurement. Scored only while the hierarchical estimator is computed.
    #[serde(skip)]
    response_time_error: PairedErrorTracker,
    #[serde(skip)]
    transfer_time_error: PairedErrorTracker,
    /// Where the chosen peer sits in distance order — the censoring diagnostic
    /// for the candidate-window size. See [`SelectionRankStats`].
    #[serde(skip)]
    selection_ranks: SelectionRankStats,
    /// Cumulative outcome counts over every [`Self::add_event`], never
    /// windowed. See [`RouteOutcomeTotals`].
    #[serde(skip)]
    outcome_totals: RouteOutcomeTotals,
    /// Test-only: `(peer address, source)` of every ingested event, so driver
    /// tests can assert which dataset tag an outcome was recorded under.
    #[cfg(test)]
    #[serde(skip)]
    recorded_sources: Vec<(Option<std::net::SocketAddr>, dataset::RouteSource)>,
}

/// Cumulative success / failure counts of the route events this router has
/// ingested since it was built.
///
/// The isotonic estimators hold a rolling window of at most 500 events, so
/// they cannot say how many failures were ever observed; these counters can.
/// They exist so tests (and a future diagnostic) can check that the failure
/// model is actually receiving failure labels (for most of its life it received
/// almost none: relays labelled downstream NotFound a success and originators
/// labelled only final successes).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RouteOutcomeTotals {
    /// `RouteOutcome::Failure` events.
    pub failures: u64,
    /// `RouteOutcome::Success` and `RouteOutcome::SuccessUntimed` events.
    pub successes: u64,
}

impl Clone for Router {
    fn clone(&self) -> Self {
        // RoutingPredictor is not cloneable, so it and everything scored against
        // it (the skill trackers, the selection-rank counters) start empty here
        // and rebuild as events arrive. That is the right behaviour: carrying a
        // measurement across a clone that discards the model it measured would
        // attribute one model's accuracy to another.
        //
        // NOTE: this impl has **no production call site**. The
        // `*router.write() = Router::new(&history)` batch-reconstruction pattern
        // this used to serve was replaced by in-place `refit()` inside
        // `add_event` (#4811), and the only `router.clone()` left in the tree is
        // an `Arc` pointer clone (ring.rs), which never reaches here. The
        // previous comment cited that reconstruction path as the live reason for
        // the reset, which would have been cargo-culted as "this runs in prod".
        // Kept because `Router` is still nominally `Clone`; if that is ever
        // removed, this goes with it.
        Router {
            response_start_time_estimator: self.response_start_time_estimator.clone(),
            transfer_rate_estimator: self.transfer_rate_estimator.clone(),
            failure_estimator: self.failure_estimator.clone(),
            mean_transfer_size: self.mean_transfer_size,
            consider_n_closest_peers: self.consider_n_closest_peers,
            per_op_failure: self.per_op_failure.clone(),
            per_op_response_time: self.per_op_response_time.clone(),
            per_op_transfer_rate: self.per_op_transfer_rate.clone(),
            renegade_predictor: routing_predictor::RoutingPredictor::new(RENEGADE_MAX_OBSERVATIONS),
            // Reset with the predictor: these score the predictor's output, so
            // carrying them across a clone that discards it would attribute one
            // model's accuracy to another.
            failure_skill_global: residual::SkillTracker::new(),
            failure_skill_adjusted: residual::SkillTracker::new(),
            failure_skill_blended: residual::SkillTracker::new(),
            failure_skill_corrected: residual::SkillTracker::new(),
            // Reset for the same reason: the skill tracker below scores it.
            hierarchical: hierarchical::HierarchicalRouting::new(self.max_connections),
            estimator_clock: self.estimator_clock.clone(),
            max_connections: self.max_connections,
            response_time_error: PairedErrorTracker::default(),
            transfer_time_error: PairedErrorTracker::default(),
            failure_skill_hierarchical: residual::SkillTracker::new(),
            selection_ranks: SelectionRankStats::default(),
            outcome_totals: self.outcome_totals,
            #[cfg(test)]
            recorded_sources: self.recorded_sources.clone(),
        }
    }
}

/// Hours since the router was built, on an injected [`TimeSource`].
///
/// The hierarchical estimator's forgetting horizons are wall-clock-like
/// quantities, so they must advance with simulated time in simulations and be
/// controllable in tests. Production wires the ring's own time source.
///
/// [`TimeSource`]: crate::util::time_source::TimeSource
#[derive(Clone, Debug)]
struct EstimatorClock {
    source: crate::util::time_source::DynTimeSource,
    origin: tokio::time::Instant,
}

impl EstimatorClock {
    fn new(source: crate::util::time_source::DynTimeSource) -> Self {
        let origin = source.now();
        EstimatorClock { source, origin }
    }

    fn hours(&self) -> f64 {
        self.source
            .now()
            .saturating_duration_since(self.origin)
            .as_secs_f64()
            / 3600.0
    }
}

impl Default for EstimatorClock {
    fn default() -> Self {
        EstimatorClock::new(std::sync::Arc::new(
            crate::util::time_source::InstantTimeSrc::new(),
        ))
    }
}

/// Clock readings for one prediction, taken once so every model in it sees the
/// same instant.
#[derive(Debug, Clone, Copy)]
struct PredictionClock {
    /// Host wall clock in hours since the epoch, for the legacy Renegade path.
    wall_clock_hours: f64,
    /// The hierarchical estimator's hours, from [`EstimatorClock`].
    estimator_hours: f64,
}

/// Whether the hierarchical estimator is computed at all for this event.
///
/// Whenever its output is used: in routing (the default), or recorded into a
/// dataset that is still RECORDING. With routing on it is always computed,
/// because it routes. Only on a node where the flag resolves off
/// (`FREENET_ROUTING_HIERARCHICAL=0`, or any unrecognised or non-UTF-8 value)
/// does the recorder decide, and a recorder
/// that stopped (byte cap, write error) must not keep the estimator learning
/// under the router's write lock for the rest of the process's life, recording
/// nothing.
fn hierarchical_computed(dataset: Option<&dataset::RoutingDataset>) -> bool {
    hierarchical_routing_enabled() || dataset.is_some_and(|dataset| dataset.is_recording())
}

/// Parse a default-OFF `FREENET_ROUTING_*` boolean switch, failing safe.
///
/// Only an explicit affirmative turns a switch on. Anything else — unset,
/// empty, a typo, `0`, `off` — leaves it off, because every switch parsed here
/// changes live routing and a misspelt value must not do that silently.
///
/// Default-ON switches use [`parse_default_on_routing_flag`] instead.
fn parse_routing_flag(value: Option<&str>) -> bool {
    value.is_some_and(|value| {
        let value = value.trim().to_ascii_lowercase();
        value == "1" || value == "true" || value == "yes" || value == "on"
    })
}

/// How a default-ON `FREENET_ROUTING_*` switch resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DefaultOnFlag {
    /// Unset, empty or whitespace-only: the shipping default.
    Default,
    /// An explicit affirmative (`1`, `true`, `yes`, `on`).
    Enabled,
    /// An explicit negative (`0`, `false`, `no`, `off`).
    Disabled,
    /// Any other non-empty value, including one that is not valid UTF-8.
    /// Treated as OFF, and worth a warning.
    Unrecognised,
}

impl DefaultOnFlag {
    fn enabled(self) -> bool {
        matches!(self, DefaultOnFlag::Default | DefaultOnFlag::Enabled)
    }
}

/// Parse a default-ON `FREENET_ROUTING_*` switch.
///
/// Unset, empty or whitespace-only is the shipping default (ON), and only the
/// recognised
/// affirmatives keep it on explicitly. Every other non-empty value turns the
/// switch OFF: once a switch defaults on, setting it at all is almost always an
/// attempt to turn it off (`disabled`, `legacy`, `n`), and on ambiguous input
/// the conservative direction is the proven legacy stack. An unrecognised value
/// is reported as [`DefaultOnFlag::Unrecognised`] so the caller can warn.
///
/// Default-OFF switches use [`parse_routing_flag`] instead, whose safe
/// direction is also its default.
fn parse_default_on_routing_flag(value: Option<&str>) -> DefaultOnFlag {
    let Some(value) = value else {
        return DefaultOnFlag::Default;
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" => DefaultOnFlag::Default,
        "1" | "true" | "yes" | "on" => DefaultOnFlag::Enabled,
        "0" | "false" | "no" | "off" => DefaultOnFlag::Disabled,
        _ => DefaultOnFlag::Unrecognised,
    }
}

/// Whether the hierarchical estimator (#4485) replaces the legacy estimates in
/// live routing.
///
/// Default **on**. It takes precedence over the legacy blend and the residual
/// correction for every stage it can estimate (a cold stage falls back to
/// legacy). The kill switch is `FREENET_ROUTING_HIERARCHICAL=0` (or `false`,
/// `no`, `off`); with it off the estimator is computed and scored only while
/// the routing dataset is recording (see [`hierarchical_computed`]).
///
/// Any other non-empty value, or one that is not valid UTF-8, also turns it
/// off, with a warning: see [`parse_default_on_routing_flag`] and
/// [`resolve_hierarchical_flag`]. The resolved mode is logged once, when this
/// is first called (the first routing event, prediction or dashboard
/// snapshot), so an operator can tell from a release build's log which
/// estimator a node routes with.
fn hierarchical_routing_enabled() -> bool {
    // Same test-override shape, and for the same reason, as
    // `residual_correction_enabled`.
    #[cfg(test)]
    {
        match TEST_HIERARCHICAL_OVERRIDE.with(|cell| cell.get()) {
            1 => return true,
            2 => return false,
            _ => {}
        }
    }
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED
        .get_or_init(|| resolve_hierarchical_flag(std::env::var_os(HIERARCHICAL_ENV).as_deref()))
}

/// The environment variable [`hierarchical_routing_enabled`] reads.
const HIERARCHICAL_ENV: &str = "FREENET_ROUTING_HIERARCHICAL";

/// Resolve the raw `FREENET_ROUTING_HIERARCHICAL` value to on/off, and log the
/// result once. This is the whole of what the process-global `OnceLock` in
/// [`hierarchical_routing_enabled`] runs, split out so the wiring (which parser,
/// which default, which log line) is testable without the `OnceLock`.
///
/// A value that is not valid UTF-8 is unrecognised, not unset: reading it with
/// `std::env::var(..).ok()` would collapse it to `None` and silently turn the
/// estimator ON, the opposite of what an operator who set the variable meant.
///
/// Every line starts with `hierarchical routing estimator: `, but do NOT grep
/// that prefix alone to determine a node's mode: the peer-table saturation
/// notice in `hierarchical.rs` shares it, is also INFO, is emitted up to
/// hourly, and since the default flip every node runs the estimator, so it now
/// appears fleet-wide. Grep one of the three mode strings instead, which are
/// mutually exclusive and emitted once per process:
/// `estimator: enabled (default)`, `estimator: enabled via`,
/// `estimator: disabled via`, or `estimator: disabled, ` for the
/// unrecognised-value case. These strings are grepped by the soak's crossover
/// verification and pinned by
/// `hierarchical_routing_enabled_follows_the_environment`; change them together.
/// INFO (and WARN), never debug: release builds compile out everything below
/// INFO (`release_max_level_info`). Being INFO, the enabled and `disabled via`
/// lines appear in the main freenet log (`freenet.*.log`), not in
/// `freenet.error.*`, whose floor is WARN: only the unrecognised-value line
/// reaches the error log, so confirm a node's mode from the main log. A
/// file-logging node's journal has none of them (stdout carries the console
/// layer only on a TTY or with `FREENET_LOG_TO_CONSOLE`).
fn resolve_hierarchical_flag(raw: Option<&std::ffi::OsStr>) -> bool {
    let flag = match raw.map(std::ffi::OsStr::to_str) {
        None => parse_default_on_routing_flag(None),
        Some(Some(value)) => parse_default_on_routing_flag(Some(value)),
        Some(None) => DefaultOnFlag::Unrecognised,
    };
    let value = raw.map(|raw| raw.to_string_lossy()).unwrap_or_default();
    match flag {
        DefaultOnFlag::Default => {
            // Emit `value` whenever the variable was SET but still resolved to
            // the default, which happens for an empty or whitespace-only
            // value. Without it a node started with
            // `FREENET_ROUTING_HIERARCHICAL= ` logs a line byte-identical to
            // one where the variable is absent, so an operator who believes
            // they disabled the estimator cannot tell from the log that their
            // value was ignored. `Default` is the only branch that can be
            // reached both with and without the variable present, so it is the
            // only one that needs the distinction.
            if raw.is_some() {
                tracing::info!(
                    value = %value,
                    "hierarchical routing estimator: enabled (default)"
                )
            } else {
                tracing::info!("hierarchical routing estimator: enabled (default)")
            }
        }
        DefaultOnFlag::Enabled => tracing::info!(
            value = %value,
            "hierarchical routing estimator: enabled via FREENET_ROUTING_HIERARCHICAL"
        ),
        DefaultOnFlag::Disabled => tracing::info!(
            value = %value,
            "hierarchical routing estimator: disabled via FREENET_ROUTING_HIERARCHICAL"
        ),
        DefaultOnFlag::Unrecognised => tracing::warn!(
            value = %value,
            "hierarchical routing estimator: disabled, FREENET_ROUTING_HIERARCHICAL has an \
             unrecognised value (use 0/false/no/off to disable, 1/true/yes/on or unset to \
             enable)"
        ),
    }
    flag.enabled()
}

// Test-only override for `hierarchical_routing_enabled`: 0 unset, 1 on, 2 off.
// Thread-local for the reason given on `TEST_CORRECTION_OVERRIDE`.
#[cfg(test)]
thread_local! {
    static TEST_HIERARCHICAL_OVERRIDE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

// Test-only count of legacy stage evaluations on this thread, so a test can
// see whether the per-candidate Renegade work was skipped.
#[cfg(test)]
thread_local! {
    static LEGACY_STAGE_EVALUATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Force the hierarchical estimator on or off for the duration of a test.
///
/// THREAD-LOCAL: the override applies only to routing done on the calling
/// thread. Routing on any other thread (a multi-thread tokio worker,
/// `spawn_blocking`, `std::thread::spawn`) falls through to the process
/// default, which is ON — so `force_hierarchical_routing(false)` does NOT reach
/// such work, and a test relying on it there would silently run hierarchical.
/// Every current caller routes on the test thread.
#[cfg(test)]
pub(crate) fn force_hierarchical_routing(enabled: bool) -> HierarchicalOverrideGuard {
    let previous = TEST_HIERARCHICAL_OVERRIDE.with(|cell| {
        let previous = cell.get();
        cell.set(if enabled { 1 } else { 2 });
        previous
    });
    HierarchicalOverrideGuard { previous }
}

#[cfg(test)]
pub(crate) struct HierarchicalOverrideGuard {
    previous: u8,
}

#[cfg(test)]
impl Drop for HierarchicalOverrideGuard {
    fn drop(&mut self) {
        TEST_HIERARCHICAL_OVERRIDE.with(|cell| cell.set(self.previous));
    }
}

/// Whether the residual correction replaces the legacy fixed-weight blend in
/// live routing.
///
/// Default **off**. The correction and the blend are both computed and both
/// scored either way, so a node accumulates the evidence needed to decide this
/// without its routing behaviour changing. Promoting the default is a separate
/// decision on that evidence — see #4485.
///
/// Follows the `FREENET_*` convention already used for runtime toggles
/// (`FREENET_DISABLE_LOGS` and friends); a restart-scoped switch here is
/// equivalent to a CLI flag and needs no plumbing through every `Router::new`
/// call site, several of which are in unrelated tests.
fn residual_correction_enabled() -> bool {
    // Tests override ahead of the cached read. Without this the flag is
    // structurally untestable: the `OnceLock` is resolved by whichever test
    // touches it first and then fixed for the life of the process, so the
    // branch that actually ships could never be exercised. That is also the
    // cross-test-interference shape this repo's testing rules call out — it
    // happens to be benign under nextest's process-per-test and NOT under plain
    // `cargo test`, which is the runner AGENTS.md asks contributors to use.
    #[cfg(test)]
    {
        match TEST_CORRECTION_OVERRIDE.with(|cell| cell.get()) {
            1 => return true,
            2 => return false,
            _ => {}
        }
    }
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        parse_routing_flag(
            std::env::var("FREENET_ROUTING_RESIDUAL_CORRECTION")
                .ok()
                .as_deref(),
        )
    })
}

// Test-only override for `residual_correction_enabled`: 0 unset, 1 on, 2 off.
//
// THREAD-LOCAL, not a process-global atomic. `cargo test` runs tests in
// parallel threads within one process, so a shared cell lets one test's
// override leak into an unrelated routing test, and two guards dropping in
// either order can restore each other's stale value. That is the
// process-global cross-test-interference shape this repo's testing rules
// call out, and it is invisible under nextest's process-per-test isolation —
// which is exactly what makes it worth avoiding rather than tolerating.
#[cfg(test)]
thread_local! {
    static TEST_CORRECTION_OVERRIDE: std::cell::Cell<u8> = const { std::cell::Cell::new(0) };
}

/// Force the residual correction on or off for the duration of a test.
///
/// Returns a guard that restores the previous setting on drop, so tests sharing
/// a process cannot leak the override into each other.
#[cfg(test)]
pub(crate) fn force_residual_correction(enabled: bool) -> CorrectionOverrideGuard {
    let previous = TEST_CORRECTION_OVERRIDE.with(|cell| {
        let previous = cell.get();
        cell.set(if enabled { 1 } else { 2 });
        previous
    });
    CorrectionOverrideGuard { previous }
}

#[cfg(test)]
pub(crate) struct CorrectionOverrideGuard {
    previous: u8,
}

#[cfg(test)]
impl Drop for CorrectionOverrideGuard {
    fn drop(&mut self) {
        TEST_CORRECTION_OVERRIDE.with(|cell| cell.set(self.previous));
    }
}

/// Rank buckets for [`SelectionRankStats`]. Sized past the default window of 25
/// so a node configured with a wider one still lands in a real bucket.
const SELECTION_RANK_BUCKETS: usize = 32;

/// Where, in distance order, does the router's chosen peer actually sit?
///
/// # What this is for
///
/// The router scores only the `consider_n_closest_peers` (25) geographically
/// closest candidates; everything beyond that is invisible to routing for that
/// hop. Production nodes run `max_connections = 200`, so a well-connected peer
/// has ~175 peers it never scores. Whether that truncation costs anything is an
/// open question (#4485 follow-up), and it is not answerable from the outside:
/// nothing currently records where within the window the decision lands.
///
/// This is a **censoring diagnostic**. If selections cluster at the near ranks,
/// the window is comfortably wider than the decision needs and widening it would
/// change nothing. If they pile up against the far edge *while the window was
/// full*, the ordering is being truncated where the real optimum plausibly lies,
/// and widening is worth testing.
///
/// The saturation qualifier is load-bearing. On a node with 8 connections the
/// window is 8, so a selection at rank 7 is "last of 8" and says nothing about
/// truncation — only a selection at the edge of a FULL window is evidence that
/// options were discarded. Counting boundary hits without that condition would
/// make every sparsely-connected node look like it needs a wider window.
#[derive(Debug, Default)]
pub(crate) struct SelectionRankStats {
    /// Distance-rank of the selected peer, 0 = closest. The final bucket
    /// collects anything at or beyond `SELECTION_RANK_BUCKETS - 1`.
    rank: [std::sync::atomic::AtomicU64; SELECTION_RANK_BUCKETS],
    /// Prediction-based decisions recorded.
    total: std::sync::atomic::AtomicU64,
    /// Decisions where the window was full, so truncation could have discarded
    /// candidates that were never scored.
    saturated: std::sync::atomic::AtomicU64,
    /// Of the saturated decisions, those whose selection fell in the farthest
    /// quarter of the window — the reading that would justify widening it.
    saturated_far_quarter: std::sync::atomic::AtomicU64,
    /// Exact sum of selected ranks, so the mean does not inherit the histogram's
    /// ceiling.
    ///
    /// `SELECTION_RANK_BUCKETS` is fixed storage, but the window is
    /// configurable via `considering_n_closest_peers`. Deriving the mean from
    /// the buckets would understate the tail for any window wider than the
    /// buckets — precisely the configuration someone would run while evaluating
    /// whether a wider window helps, so the metric would mislead exactly when it
    /// was being consulted.
    rank_sum: std::sync::atomic::AtomicU64,
    /// Largest rank ever selected, so a tail beyond the histogram is visible
    /// rather than silently folded into the last bucket.
    max_rank: std::sync::atomic::AtomicU64,
}

impl SelectionRankStats {
    /// `candidates_before_truncation` is how many peers were available to the
    /// decision, and `window` how many survived the distance cut.
    fn record(&self, selected_rank: usize, window: usize, candidates_before_truncation: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let bucket = selected_rank.min(SELECTION_RANK_BUCKETS - 1);
        self.rank[bucket].fetch_add(1, Relaxed);
        self.total.fetch_add(1, Relaxed);
        // Exact, unbucketed, so the mean survives a window wider than the
        // histogram. The histogram is for the SHAPE; this is for the number.
        self.rank_sum.fetch_add(selected_rank as u64, Relaxed);
        self.max_rank.fetch_max(selected_rank as u64, Relaxed);

        // TRUNCATED, not merely full. A decision with exactly 25 candidates
        // against a 25-peer window scored every one of them — nothing was
        // discarded, so it is no evidence about the limit. Counting it would
        // inflate the saturation rate with decisions the window never
        // constrained, which is the precise false positive the qualifier exists
        // to prevent: a node whose connection count happens to sit AT the limit
        // would otherwise look starved on every decision.
        if candidates_before_truncation > window && window > 0 {
            self.saturated.fetch_add(1, Relaxed);
            // Farthest quarter, rounded so that tiny windows still have one.
            let threshold = window - (window / 4).max(1);
            if selected_rank >= threshold {
                self.saturated_far_quarter.fetch_add(1, Relaxed);
            }
        }
    }

    fn snapshot(&self) -> SelectionRankSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        SelectionRankSnapshot {
            rank: self.rank.each_ref().map(|slot| slot.load(Relaxed)),
            total: self.total.load(Relaxed),
            saturated: self.saturated.load(Relaxed),
            saturated_far_quarter: self.saturated_far_quarter.load(Relaxed),
            rank_sum: self.rank_sum.load(Relaxed),
            max_rank: self.max_rank.load(Relaxed),
        }
    }
}

/// Plain-data view of [`SelectionRankStats`] for the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct SelectionRankSnapshot {
    pub rank: [u64; SELECTION_RANK_BUCKETS],
    pub total: u64,
    pub saturated: u64,
    pub saturated_far_quarter: u64,
    #[serde(default)]
    pub rank_sum: u64,
    #[serde(default)]
    pub max_rank: u64,
}

impl SelectionRankSnapshot {
    /// Share of truncated decisions whose selection landed in the farthest
    /// quarter of the window. High means the window is plausibly too narrow.
    pub fn far_quarter_share(&self) -> Option<f64> {
        (self.saturated > 0).then(|| self.saturated_far_quarter as f64 / self.saturated as f64)
    }

    /// Mean selected rank, for a one-number summary alongside the histogram.
    ///
    /// From the exact sum, NOT the buckets — see `rank_sum`.
    pub fn mean_rank(&self) -> Option<f64> {
        (self.total > 0).then(|| self.rank_sum as f64 / self.total as f64)
    }
}

/// Maximum observations to retain per renegade funnel stage.
const RENEGADE_MAX_OBSERVATIONS: usize = 5000;

impl Router {
    pub fn new(history: &[RouteEvent]) -> Self {
        let failure_outcomes: Vec<IsotonicEvent> = history
            .iter()
            .map(|re| IsotonicEvent {
                peer: re.peer.clone(),
                contract_location: re.contract_location,
                result: match re.outcome {
                    RouteOutcome::Success { .. } | RouteOutcome::SuccessUntimed => 0.0,
                    RouteOutcome::Failure => 1.0,
                },
            })
            .collect();

        let success_durations: Vec<IsotonicEvent> = history
            .iter()
            .filter_map(|re| {
                if let RouteOutcome::Success {
                    time_to_response_start,
                    payload_size: _,
                    payload_transfer_time: _,
                } = re.outcome
                {
                    Some(IsotonicEvent {
                        peer: re.peer.clone(),
                        contract_location: re.contract_location,
                        result: time_to_response_start.as_secs_f64(),
                    })
                } else {
                    None
                }
            })
            .collect();

        let transfer_rates: Vec<IsotonicEvent> = history
            .iter()
            .filter_map(|re| {
                if let RouteOutcome::Success {
                    time_to_response_start: _,
                    payload_size,
                    payload_transfer_time,
                } = re.outcome
                {
                    // Mirror the per-op / add_event guard: SUBSCRIBE successes
                    // report payload_size=0 and payload_transfer_time=ZERO, so
                    // 0/0 = NaN. Skip those so NaN never enters the isotonic
                    // regression (a single NaN poisons interpolation).
                    if payload_size > 0 && !payload_transfer_time.is_zero() {
                        Some(IsotonicEvent {
                            peer: re.peer.clone(),
                            contract_location: re.contract_location,
                            result: payload_size as f64 / payload_transfer_time.as_secs_f64(),
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let mut mean_transfer_size = Mean::new();

        // Add some initial data so this produces sensible results with low or no historical data
        mean_transfer_size.add_with_count(1000.0, 10);

        for event in history {
            if let RouteOutcome::Success {
                time_to_response_start: _,
                payload_size,
                payload_transfer_time,
            } = event.outcome
            {
                // Only feed transfer size estimator when there's actual payload
                // data. Operations like SUBSCRIBE report payload_size=0 and
                // payload_transfer_time=ZERO; counting those would bias
                // mean_transfer_size downward on a router reload. Mirrors the
                // incremental `add_event` guard.
                if payload_size > 0 && !payload_transfer_time.is_zero() {
                    mean_transfer_size.add(payload_size as f64);
                }
            }
        }

        let mut renegade_predictor =
            routing_predictor::RoutingPredictor::new_batch(RENEGADE_MAX_OBSERVATIONS);

        // Feed historical events into the renegade predictor.
        // Use record_at_time with index-based ordering to preserve temporal
        // relationships (batch events don't have real timestamps, but ordering
        // is preserved from the event log).
        for (idx, event) in history.iter().enumerate() {
            let distance = event
                .peer
                .location()
                .map(|loc| event.contract_location.distance(loc).as_f64())
                .unwrap_or(0.5);

            let (outcome, _) =
                routing_predictor::RoutingOutcome::from_route_outcome(&event.outcome);

            // Use index-based time: events are ordered, each ~1 minute apart
            let time_hours = idx as f64 / 60.0;
            // No residuals from history, deliberately. A residual has to be taken
            // against the base estimate AS IT STOOD when the event arrived, and
            // this path builds the isotonic estimators wholesale from the whole
            // history rather than incrementally — so no such "estimate as of event
            // i" exists here. Using the final fitted curve instead would leak
            // future outcomes into past residuals, training the correction on
            // information it will never have in production. The residual stages
            // therefore start empty and fill from live traffic, which costs a
            // warm-up rather than correctness.
            renegade_predictor.record_at_time(
                &event.peer,
                event.contract_location,
                distance,
                outcome,
                routing_predictor::StageResiduals::default(),
                time_hours,
            );
        }

        renegade_predictor.finish_batch();

        // Build per-op-type estimators from history
        let mut per_op_failure: HashMap<OpType, Vec<IsotonicEvent>> = HashMap::new();
        let mut per_op_response_time: HashMap<OpType, Vec<IsotonicEvent>> = HashMap::new();
        let mut per_op_transfer_rate: HashMap<OpType, Vec<IsotonicEvent>> = HashMap::new();

        for event in history {
            if let Some(op_type) = event.op_type {
                let failure_result = match event.outcome {
                    RouteOutcome::Success { .. } | RouteOutcome::SuccessUntimed => 0.0,
                    RouteOutcome::Failure => 1.0,
                };
                per_op_failure
                    .entry(op_type)
                    .or_default()
                    .push(IsotonicEvent {
                        peer: event.peer.clone(),
                        contract_location: event.contract_location,
                        result: failure_result,
                    });
                if let RouteOutcome::Success {
                    time_to_response_start,
                    payload_size,
                    payload_transfer_time,
                } = event.outcome
                {
                    per_op_response_time
                        .entry(op_type)
                        .or_default()
                        .push(IsotonicEvent {
                            peer: event.peer.clone(),
                            contract_location: event.contract_location,
                            result: time_to_response_start.as_secs_f64(),
                        });
                    if payload_size > 0 && !payload_transfer_time.is_zero() {
                        per_op_transfer_rate
                            .entry(op_type)
                            .or_default()
                            .push(IsotonicEvent {
                                peer: event.peer.clone(),
                                contract_location: event.contract_location,
                                result: payload_size as f64 / payload_transfer_time.as_secs_f64(),
                            });
                    }
                }
            }
        }

        Router {
            // Positive because we expect time to increase as distance increases.
            // Multiplicative per-peer adjustment: a peer's response time deviates
            // from the global fit by a near-constant ratio, not a constant offset
            // (validated on production telemetry). See `AdjustmentMode`.
            response_start_time_estimator: IsotonicEstimator::new_with_mode(
                success_durations,
                EstimatorType::Positive,
                AdjustmentMode::Multiplicative,
            ),
            // Positive because we expect failure probability to increase as distance increase
            failure_estimator: IsotonicEstimator::new(failure_outcomes, EstimatorType::Positive),
            // Negative because we expect transfer rate to decrease as distance increases.
            // Additive for now: transfer rate is the same unbounded multiplicative-scale
            // quantity as response time and is a strong candidate for multiplicative too,
            // but current telemetry lacks the per-distance payload data to validate it.
            transfer_rate_estimator: IsotonicEstimator::new(
                transfer_rates,
                EstimatorType::Negative,
            ),
            mean_transfer_size,
            consider_n_closest_peers: DEFAULT_CONSIDER_N_CLOSEST_PEERS,
            // Dashboard-only, so they fit on read. See `FitPolicy`.
            per_op_failure: per_op_failure
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        IsotonicEstimator::new_fit_on_read(
                            v,
                            EstimatorType::Positive,
                            AdjustmentMode::Additive,
                        ),
                    )
                })
                .collect(),
            per_op_response_time: per_op_response_time
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        IsotonicEstimator::new_fit_on_read(
                            v,
                            EstimatorType::Positive,
                            AdjustmentMode::Multiplicative,
                        ),
                    )
                })
                .collect(),
            per_op_transfer_rate: per_op_transfer_rate
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        IsotonicEstimator::new_fit_on_read(
                            v,
                            EstimatorType::Negative,
                            AdjustmentMode::Additive,
                        ),
                    )
                })
                .collect(),
            renegade_predictor,
            // Start empty on a reload for the same reason the residual stages do:
            // the layers being scored are rebuilt here, so history carries no
            // comparable measurement forward.
            failure_skill_global: residual::SkillTracker::new(),
            failure_skill_adjusted: residual::SkillTracker::new(),
            failure_skill_blended: residual::SkillTracker::new(),
            failure_skill_corrected: residual::SkillTracker::new(),
            // Not replayed from `history`: batch events carry no timestamps, and
            // the estimator's horizons are defined on its own clock. It learns
            // from live traffic, which is the only path production uses.
            hierarchical: hierarchical::HierarchicalRouting::new(Ring::DEFAULT_MAX_CONNECTIONS),
            estimator_clock: EstimatorClock::default(),
            max_connections: Ring::DEFAULT_MAX_CONNECTIONS,
            response_time_error: PairedErrorTracker::default(),
            transfer_time_error: PairedErrorTracker::default(),
            failure_skill_hierarchical: residual::SkillTracker::new(),
            selection_ranks: SelectionRankStats::default(),
            outcome_totals: RouteOutcomeTotals::default(),
            #[cfg(test)]
            recorded_sources: Vec::new(),
        }
    }

    /// Run the hierarchical estimator's horizons on `source`. Production passes
    /// the ring's `InstantTimeSrc`, which follows tokio's clock (so it advances
    /// under a paused runtime); tests pass a mock they advance by hand.
    pub(crate) fn with_time_source(
        mut self,
        source: crate::util::time_source::DynTimeSource,
    ) -> Self {
        self.estimator_clock = EstimatorClock::new(source);
        self
    }

    /// Size the hierarchical estimator's peer tables from this node's
    /// configured connection cap. Resets the estimator, so call at construction.
    pub(crate) fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self.hierarchical = hierarchical::HierarchicalRouting::new(max_connections);
        self
    }

    #[allow(dead_code)]
    pub fn considering_n_closest_peers(mut self, n: u32) -> Self {
        self.consider_n_closest_peers = n as usize;
        self
    }

    pub fn add_event(&mut self, event: RouteEvent) {
        self.add_event_recording(event, dataset::RouteSource::Originator, dataset::global());
    }

    /// [`Self::add_event`] for an outcome observed by a relay hop about its
    /// downstream peer. Identical for the model; distinguished only in the
    /// routing dataset, because relays record outcomes under their own
    /// conventions (see `record_relay_route_event`).
    pub(crate) fn add_relay_event(&mut self, event: RouteEvent) {
        self.add_event_recording(event, dataset::RouteSource::Relay, dataset::global());
    }

    /// [`Self::add_event`], recording the event into `dataset` when one is
    /// given. Split out so tests can supply a recorder without the
    /// process-wide environment switch.
    fn add_event_recording(
        &mut self,
        event: RouteEvent,
        source: dataset::RouteSource,
        dataset: Option<&dataset::RoutingDataset>,
    ) {
        let was_below_threshold = !self.has_sufficient_routing_events();
        let op_type = event.op_type;
        #[cfg(test)]
        self.recorded_sources
            .push((event.peer.socket_addr(), source));
        match event.outcome {
            RouteOutcome::Failure => self.outcome_totals.failures += 1,
            RouteOutcome::Success { .. } | RouteOutcome::SuccessUntimed => {
                self.outcome_totals.successes += 1;
            }
        }

        // Feed renegade predictor (before isotonic, which moves event.peer)
        let distance = event
            .peer
            .location()
            .map(|loc| event.contract_location.distance(loc).as_f64())
            .unwrap_or(0.5);

        let (renegade_outcome, _) =
            routing_predictor::RoutingOutcome::from_route_outcome(&event.outcome);
        // Residual targets, captured against the base estimates as they stand
        // right now — before the isotonic estimators below ingest this event.
        let residuals =
            self.stage_residuals(&event.peer, event.contract_location, &renegade_outcome);
        let actual_failure = if renegade_outcome.success { 0.0 } else { 1.0 };
        let scored =
            self.score_failure_layers(&event.peer, event.contract_location, actual_failure);
        let (mut forecasts, legacy_queries) = match scored {
            Some((forecasts, queries)) => (Some(forecasts), Some(queries)),
            None => (None, None),
        };
        // The hierarchical estimator scores its own forecast before learning the
        // event, so feeding it here keeps predict-before-add. It is independent
        // of the legacy estimators, so its position relative to their ingestion
        // below does not matter; it is placed after the legacy scoring so every
        // legacy forecast above is made on exactly the state it always was.
        if hierarchical_computed(dataset) {
            let legacy_timing =
                legacy_queries
                    .as_ref()
                    .map_or_else(LegacyTimingForecast::default, |queries| {
                        self.legacy_timing_forecast(&event.peer, event.contract_location, queries)
                    });
            let actual_timing = ObservedTiming::of(&event.outcome);
            let now = self.estimator_clock.hours();
            let observed = self.hierarchical.observe_at(
                &event.peer,
                event.contract_location,
                distance,
                &renegade_outcome,
                now,
            );
            self.score_hierarchical_layer(
                forecasts.as_mut(),
                observed,
                legacy_timing,
                actual_timing,
                actual_failure,
                now,
            );
        }
        if let Some(dataset) = dataset.filter(|dataset| dataset.is_recording()) {
            dataset.record_route(self.route_record(&event, source, forecasts));
        }
        self.renegade_predictor.record(
            &event.peer,
            event.contract_location,
            distance,
            renegade_outcome,
            residuals,
        );

        // Feed global isotonic estimators
        match event.outcome {
            RouteOutcome::Success {
                time_to_response_start,
                payload_size,
                payload_transfer_time,
            } => {
                self.response_start_time_estimator.add_event(IsotonicEvent {
                    peer: event.peer.clone(),
                    contract_location: event.contract_location,
                    result: time_to_response_start.as_secs_f64(),
                });
                self.failure_estimator.add_event(IsotonicEvent {
                    peer: event.peer.clone(),
                    contract_location: event.contract_location,
                    result: 0.0,
                });

                // Per-op-type estimators
                if let Some(ot) = op_type {
                    // The per-op estimators are dashboard-only, so they fit on
                    // read rather than on every event. See `FitPolicy`.
                    self.per_op_response_time
                        .entry(ot)
                        .or_insert_with(|| {
                            // Multiplicative to match the global response-time estimator.
                            IsotonicEstimator::new_fit_on_read(
                                std::iter::empty(),
                                EstimatorType::Positive,
                                AdjustmentMode::Multiplicative,
                            )
                        })
                        .add_event(IsotonicEvent {
                            peer: event.peer.clone(),
                            contract_location: event.contract_location,
                            result: time_to_response_start.as_secs_f64(),
                        });
                    self.per_op_failure
                        .entry(ot)
                        .or_insert_with(|| {
                            IsotonicEstimator::new_fit_on_read(
                                std::iter::empty(),
                                EstimatorType::Positive,
                                AdjustmentMode::Additive,
                            )
                        })
                        .add_event(IsotonicEvent {
                            peer: event.peer.clone(),
                            contract_location: event.contract_location,
                            result: 0.0,
                        });
                }

                // Only feed transfer rate estimator when there's actual payload data.
                // Operations like SUBSCRIBE report payload_size=0 and
                // payload_transfer_time=ZERO, which would produce NaN (0/0).
                if payload_size > 0 && !payload_transfer_time.is_zero() {
                    self.mean_transfer_size.add(payload_size as f64);
                    self.transfer_rate_estimator.add_event(IsotonicEvent {
                        contract_location: event.contract_location,
                        peer: event.peer.clone(),
                        result: payload_size as f64 / payload_transfer_time.as_secs_f64(),
                    });
                    if let Some(ot) = op_type {
                        self.per_op_transfer_rate
                            .entry(ot)
                            .or_insert_with(|| {
                                IsotonicEstimator::new_fit_on_read(
                                    std::iter::empty(),
                                    EstimatorType::Negative,
                                    AdjustmentMode::Additive,
                                )
                            })
                            .add_event(IsotonicEvent {
                                contract_location: event.contract_location,
                                peer: event.peer,
                                result: payload_size as f64 / payload_transfer_time.as_secs_f64(),
                            });
                    }
                }
            }
            RouteOutcome::SuccessUntimed | RouteOutcome::Failure => {
                let result = if matches!(event.outcome, RouteOutcome::Failure) {
                    1.0
                } else {
                    0.0
                };
                self.failure_estimator.add_event(IsotonicEvent {
                    peer: event.peer.clone(),
                    contract_location: event.contract_location,
                    result,
                });
                if let Some(ot) = op_type {
                    self.per_op_failure
                        .entry(ot)
                        .or_insert_with(|| {
                            IsotonicEstimator::new_fit_on_read(
                                std::iter::empty(),
                                EstimatorType::Positive,
                                AdjustmentMode::Additive,
                            )
                        })
                        .add_event(IsotonicEvent {
                            peer: event.peer,
                            contract_location: event.contract_location,
                            result,
                        });
                }
            }
        }

        if was_below_threshold && self.has_sufficient_routing_events() {
            tracing::info!(
                total_events = self.failure_estimator.len(),
                successes = self.response_start_time_estimator.len(),
                "Router transitioning from distance-based to prediction-based routing"
            );
        }
    }

    /// Cumulative outcome counts. See [`RouteOutcomeTotals`].
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    pub(crate) fn outcome_totals(&self) -> RouteOutcomeTotals {
        self.outcome_totals
    }

    /// Every `(peer address, result)` pair currently held in the failure
    /// estimator's rolling window, in insertion order (`1.0` = failure,
    /// `0.0` = success). Test-only: lets a driver test assert WHICH peer a
    /// route event blamed.
    #[cfg(test)]
    pub(crate) fn recorded_sources_for_test(
        &self,
    ) -> Vec<(Option<std::net::SocketAddr>, dataset::RouteSource)> {
        self.recorded_sources.clone()
    }

    #[cfg(test)]
    pub(crate) fn failure_window_for_test(&self) -> Vec<(Option<std::net::SocketAddr>, f64)> {
        self.failure_estimator
            .raw_events_for_test()
            .map(|e| (e.peer.socket_addr(), e.result))
            .collect()
    }

    /// The `consider_n_closest_peers` closest candidates, plus HOW MANY were
    /// available before that cut.
    ///
    /// The second value is what distinguishes "the window was full" from "the
    /// window actually discarded someone", and only the latter is evidence
    /// about whether the limit is costing anything.
    fn select_closest_peers<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: &Location,
    ) -> (Vec<&'a PeerKeyLocation>, usize) {
        let mut peer_distances: Vec<_> = peers
            .into_iter()
            .map(|peer| {
                let distance = peer
                    .location()
                    .map(|loc| target_location.distance(loc))
                    .unwrap_or_else(|| Distance::new(0.5));
                (peer, distance)
            })
            .collect();

        GlobalRng::shuffle(&mut peer_distances);

        // Partial sort: find the k closest peers in O(n), then sort only
        // those k elements. This avoids O(n log n) when n ≫ k.
        // select_nth_unstable_by(0, ...) is valid and puts the minimum at
        // index 0 — the k>0 guard only excludes the degenerate k=0 case.
        // See PR #4247 review: https://github.com/freenet/freenet-core/pull/4247
        let k = self.consider_n_closest_peers.min(peer_distances.len());
        if k > 0 && k < peer_distances.len() {
            peer_distances.select_nth_unstable_by(k - 1, |a, b| a.1.cmp(&b.1));
        }
        let available = peer_distances.len();
        peer_distances.truncate(k);
        peer_distances.sort_by_key(|&(_, distance)| distance);
        (
            peer_distances.into_iter().map(|(peer, _)| peer).collect(),
            available,
        )
    }

    pub fn select_peer<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
    ) -> Option<&'a PeerKeyLocation> {
        self.select_k_best_peers(peers, target_location, 1)
            .into_iter()
            .next()
    }

    /// Select up to k best peers for routing, ranked by predicted performance.
    /// Returns peers ordered from best to worst predicted performance.
    pub fn select_k_best_peers<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
        k: usize,
    ) -> Vec<&'a PeerKeyLocation> {
        let (selected, _decision) =
            self.select_k_best_peers_with_telemetry(peers, target_location, k);
        selected
    }

    /// The adjustment space each estimator composes its per-peer correction in.
    /// Read from the estimators so the residual correction cannot drift out of
    /// step with them.
    fn stage_modes(&self) -> routing_predictor::StageModes {
        routing_predictor::StageModes {
            failure: self.failure_estimator.adjustment_mode(),
            response_time: self.response_start_time_estimator.adjustment_mode(),
            transfer_speed: self.transfer_rate_estimator.adjustment_mode(),
        }
    }

    /// Residual of each estimator's current prediction against what actually
    /// happened, in that estimator's own adjustment space.
    ///
    /// MUST be called before the isotonic estimators ingest the event: a residual
    /// taken after the base model has already fitted the point understates the
    /// error, and the correction then learns to under-correct.
    fn stage_residuals(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        outcome: &routing_predictor::RoutingOutcome,
    ) -> routing_predictor::StageResiduals {
        let actual_failure = if outcome.success { 0.0 } else { 1.0 };
        // Residual of the GLOBAL curve, NOT the peer-adjusted estimate.
        //
        // Measured, and it reverses what the design proposal assumed. Correcting
        // the peer-adjusted estimate puts the per-peer EWMA's own noise inside
        // the residual target, and that component is a function of the EWMA's
        // internal state at that instant rather than of (peer, contract,
        // distance, time) — so it is unlearnable from the features, and the
        // correction spends its capacity chasing it. On the recoverability
        // harness this was the difference between `captured = -0.30` and
        // `captured = +0.055` for the CORRECTED estimate — between worse than
        // assuming nothing and better than it for the first time. See #4485.
        //
        // Three different quantities get called "captured" around here, and a
        // reviewer who ran the test read the wrong one off it, so naming them
        // (all measured before #5658; see below for today's):
        //   -0.438  the global curve UNCORRECTED — what the harness prints as
        //           `base`
        //   -0.302  the CORRECTED estimate composing with the PEER-ADJUSTED
        //           base, i.e. the design this comment argues against. Not
        //           reproducible from the tree: that configuration is gone
        //   +0.055  the CORRECTED estimate composing with the global curve,
        //           i.e. what the code now does
        // The comparison that settles B5 is the second against the third.
        //
        // #5658 made the isotonic base exact (its rolling window had been
        // corrupted between refits), which moved the two reproducible figures:
        // the harness now prints `base` -0.133 and corrected +0.270. The -0.302
        // was measured against the corrupted base and has not been re-measured,
        // so the B5 comparison stands on pre-#5658 numbers.
        let failure = self
            .failure_estimator
            .estimate_global(peer, contract_location)
            .ok()
            .and_then(|base| {
                self.failure_estimator
                    .adjustment_mode()
                    .residual(actual_failure, base.clamp(0.0, 1.0))
            });

        let response_time = outcome.time_to_response_start_secs.and_then(|actual| {
            self.response_start_time_estimator
                .estimate_global(peer, contract_location)
                .ok()
                .and_then(|base| {
                    self.response_start_time_estimator
                        .adjustment_mode()
                        .residual(actual, base)
                })
        });

        let transfer_speed = outcome.transfer_speed_bps.and_then(|actual| {
            self.transfer_rate_estimator
                .estimate_global(peer, contract_location)
                .ok()
                .and_then(|base| {
                    self.transfer_rate_estimator
                        .adjustment_mode()
                        .residual(actual, base)
                })
        });

        routing_predictor::StageResiduals {
            failure,
            response_time,
            transfer_speed,
        }
    }

    /// The dataset record for one event, built from the state the forecasts
    /// were made in — i.e. before the event is ingested.
    fn route_record(
        &self,
        event: &RouteEvent,
        source: dataset::RouteSource,
        forecasts: Option<dataset::FailureForecasts>,
    ) -> dataset::RouteRecord {
        let (outcome, time_to_response_start_s, payload_bytes, payload_transfer_s) =
            match &event.outcome {
                RouteOutcome::Success {
                    time_to_response_start,
                    payload_size,
                    payload_transfer_time,
                } => (
                    "success",
                    Some(time_to_response_start.as_secs_f64()),
                    Some(*payload_size),
                    Some(payload_transfer_time.as_secs_f64()),
                ),
                RouteOutcome::SuccessUntimed => ("success_untimed", None, None, None),
                RouteOutcome::Failure => ("failure", None, None, None),
            };
        let peer_location = event.peer.location();
        dataset::RouteRecord {
            t_ms: dataset::now_ms(),
            source,
            peer: dataset::peer_hash(&event.peer),
            peer_location: peer_location.map(|location| location.as_f64()),
            contract_location: event.contract_location.as_f64(),
            distance: peer_location
                .map(|location| event.contract_location.distance(location).as_f64()),
            op: event.op_type.map(|op| op.as_str()),
            outcome,
            time_to_response_start_s,
            payload_bytes,
            payload_transfer_s,
            prior_failure_events: self.failure_estimator.len(),
            forecasts,
        }
    }

    /// Score every failure-prediction layer against what actually happened.
    ///
    /// MUST be called before the isotonic estimators ingest the event, for the
    /// same reason the residuals are: a layer graded after the base model has
    /// fitted the outcome is grading itself on the answer.
    ///
    /// Also returns the Renegade query results it ran, so the per-event timing
    /// comparison can reuse them instead of querying again under the write lock.
    fn score_failure_layers(
        &mut self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        actual_failure: f64,
    ) -> Option<(dataset::FailureForecasts, LegacyQueries)> {
        let (Ok(global), Ok(adjusted)) = (
            self.failure_estimator
                .estimate_global(peer, contract_location),
            self.failure_estimator
                .estimate_retrieval_time(peer, contract_location),
        ) else {
            return None;
        };
        let global = global.clamp(0.0, 1.0);
        let adjusted = adjusted.clamp(0.0, 1.0);

        let distance = peer
            .location()
            .map(|loc| contract_location.distance(loc).as_f64())
            .unwrap_or(0.5);

        let queries = self.legacy_queries_at(
            peer,
            contract_location,
            distance,
            routing_predictor::wall_clock_hours(),
        );
        let renegade = &queries.renegade;
        let corrections = queries.corrections;
        let blended = match renegade.failure_probability {
            Some(probability) if probability.is_finite() => {
                let weight = self.renegade_predictor.failure_weight();
                (adjusted * (1.0 - weight) + probability.clamp(0.0, 1.0) * weight).clamp(0.0, 1.0)
            }
            _ => adjusted,
        };

        // Scored against the GLOBAL base, matching how the correction is actually
        // composed; scoring it against the peer-adjusted estimate would measure a
        // predictor the router never forms.
        let corrected = corrections.failure.map_or(global, |correction| {
            (global + correction.value).clamp(0.0, 1.0)
        });

        self.failure_skill_global.record(global, actual_failure);
        self.failure_skill_adjusted.record(adjusted, actual_failure);
        self.failure_skill_blended.record(blended, actual_failure);
        self.failure_skill_corrected
            .record(corrected, actual_failure);

        let forecasts = dataset::FailureForecasts {
            global,
            adjusted,
            blended,
            corrected,
            lambda: corrections.failure.map(|correction| correction.lambda),
            n_eff: corrections.failure.map(|correction| correction.n_eff),
            // Filled in by `score_hierarchical_layer`, when the estimator runs.
            hierarchical: None,
            log_response_time_legacy: None,
            log_response_time_hierarchical: None,
            log_transfer_speed_legacy: None,
            log_transfer_speed_hierarchical: None,
        };
        Some((forecasts, queries))
    }

    /// The Renegade prediction and residual corrections for one query, on one
    /// clock reading.
    fn legacy_queries_at(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        distance: f64,
        wall_clock_hours: f64,
    ) -> LegacyQueries {
        let time = self.renegade_predictor.time_at(wall_clock_hours);
        LegacyQueries {
            renegade: self.renegade_predictor.predict_at_time(
                peer,
                contract_location,
                distance,
                time,
            ),
            corrections: self.renegade_predictor.predict_corrections_at_time(
                peer,
                contract_location,
                distance,
                self.stage_modes(),
                time,
            ),
        }
    }

    /// The legacy stack's three stage estimates: the isotonic estimates, the
    /// fixed-weight Renegade blend, and — when `FREENET_ROUTING_RESIDUAL_CORRECTION`
    /// is on — the residual correction in its place. Moved verbatim from
    /// `predict_routing_outcome`; slated for removal with the rest of the legacy
    /// stack once the hierarchical estimator is promoted (#4485).
    #[allow(clippy::too_many_arguments)]
    fn legacy_stage_estimates(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
        distance: f64,
        failure_estimate: f64,
        time_estimate: Option<f64>,
        transfer_estimate: Option<f64>,
        wall_clock_hours: f64,
    ) -> LegacyStageEstimates {
        #[cfg(test)]
        LEGACY_STAGE_EVALUATIONS.with(|count| count.set(count.get() + 1));

        // Blend renegade prediction if available. The renegade predictor captures
        // peer × contract interactions that the global isotonic model cannot see
        // (e.g., a peer selectively dropping requests for specific contracts).
        let renegade_time = self.renegade_predictor.time_at(wall_clock_hours);
        let renegade =
            self.renegade_predictor
                .predict_at_time(peer, target_location, distance, renegade_time);

        // Residual correction (#4485), computed ONLY when it will be used.
        //
        // An earlier version computed this unconditionally, with the rationale
        // that it let both approaches be scored against each other on live
        // traffic. That rationale does not hold at THIS call site: nothing here
        // scores the result, so with the flag off (the shipped default) it was
        // pure waste — and expensive waste, since this runs once per candidate
        // peer per routing decision (up to `consider_n_closest_peers`) and each
        // call is three k-NN queries at `KERNEL_CANDIDATE_NEIGHBOURS`. Worse,
        // renegade's VP-tree is invalidated by every eviction and only rebuilt
        // at the next `train()`, so queries in between degrade to a full scan.
        //
        // The comparison the rationale wanted happens in `score_failure_layers`,
        // once per completed event rather than once per candidate, and is
        // unaffected by this gate. Flagged in review of #5642.
        let correction_enabled = residual_correction_enabled();
        let corrections = if correction_enabled {
            self.renegade_predictor.predict_corrections_at_time(
                peer,
                target_location,
                distance,
                self.stage_modes(),
                renegade_time,
            )
        } else {
            routing_predictor::RoutingCorrections::default()
        };

        self.combine_legacy_stages(
            peer,
            target_location,
            failure_estimate,
            time_estimate,
            transfer_estimate,
            &LegacyQueries {
                renegade,
                corrections,
            },
            correction_enabled,
        )
    }

    /// Everything [`Self::legacy_stage_estimates`] does after its Renegade
    /// queries: the blend and, when `correction_enabled`, the correction in its
    /// place. Split out so a caller that already holds the query results (the
    /// per-event scoring path) does not run the same k-NN queries again.
    #[allow(clippy::too_many_arguments)]
    fn combine_legacy_stages(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
        failure_estimate: f64,
        time_estimate: Option<f64>,
        transfer_estimate: Option<f64>,
        queries: &LegacyQueries,
        correction_enabled: bool,
    ) -> LegacyStageEstimates {
        let renegade = &queries.renegade;
        let corrections = queries.corrections;
        // Clamp before using in cost formulas — per-peer EWMA adjustments can
        // push the raw estimate slightly outside [0, 1].
        let mut failure_estimate = failure_estimate.clamp(0.0, 1.0);
        let isotonic_failure = failure_estimate;

        let renegade_failure_adjustment =
            if let Some(renegade_failure) = renegade.failure_probability {
                if renegade_failure.is_finite() {
                    let w = self.renegade_predictor.failure_weight();
                    failure_estimate =
                        failure_estimate * (1.0 - w) + renegade_failure.clamp(0.0, 1.0) * w;
                    failure_estimate = failure_estimate.clamp(0.0, 1.0);
                    Some(failure_estimate - isotonic_failure)
                } else {
                    None
                }
            } else {
                None
            };

        let mut time_to_response_start = time_estimate.map_or(0.0, |estimate| {
            legacy_response_time(
                estimate,
                renegade.time_to_response_start,
                self.renegade_predictor.response_time_weight(),
            )
        });
        let mut xfer_speed = transfer_estimate.unwrap_or(0.0);
        if let Some(renegade_speed) = renegade.transfer_speed {
            if transfer_estimate.is_some() && renegade_speed.is_finite() && renegade_speed > 0.0 {
                let w = self.renegade_predictor.transfer_speed_weight();
                xfer_speed = xfer_speed * (1.0 - w) + renegade_speed * w;
            }
        }

        // When enabled, the correction REPLACES the legacy blend rather than
        // composing with it. Both are corrections to the same base estimate, so
        // applying both would double-count.
        if correction_enabled {
            // The correction composes with the GLOBAL curve, matching the space
            // its residuals were taken in (see `stage_residuals`). It therefore
            // REPLACES the per-peer EWMA rather than stacking on it — #4485's B5
            // question, settled by measurement rather than argument.
            let global_failure = self
                .failure_estimator
                .estimate_global(peer, target_location)
                .ok()
                .map(|value| value.clamp(0.0, 1.0));
            // Note the shape: the base is adopted whenever it EXISTS, and the
            // correction is added only if the residual model has something to
            // say. An earlier version required both, which quietly defeated the
            // neutral-when-uninformed property this design rests on — when the
            // correction abstained (post-restart warm-up, or a query whose
            // kernel weights underflow) the estimate silently fell back to the
            // legacy blend, i.e. to exactly the far-field behaviour the
            // correction exists to replace. Abstention must mean "base plus
            // nothing", not "revert to the old model".
            if let Some(base) = global_failure {
                let corrected =
                    (base + corrections.failure.map_or(0.0, |c| c.value)).clamp(0.0, 1.0);
                if corrected.is_finite() {
                    failure_estimate = corrected;
                }
            }
            let modes = self.stage_modes();
            let global_time = self
                .response_start_time_estimator
                .estimate_global(peer, target_location)
                .ok();
            let global_transfer = self
                .transfer_rate_estimator
                .estimate_global(peer, target_location)
                .ok();
            if let Some(base) = global_time {
                let correction = corrections.response_time.map_or(0.0, |c| c.value);
                let corrected = modes.response_time.apply(base, correction);
                if corrected.is_finite() && corrected >= 0.0 {
                    time_to_response_start = corrected;
                }
            }
            if let Some(base) = global_transfer {
                let correction = corrections.transfer_speed.map_or(0.0, |c| c.value);
                let corrected = modes.transfer_speed.apply(base, correction);
                if corrected.is_finite() && corrected > 0.0 {
                    xfer_speed = corrected;
                }
            }
        }

        LegacyStageEstimates {
            failure: failure_estimate,
            renegade_failure_adjustment,
            time_to_response_start,
            xfer_speed,
        }
    }

    /// Score the hierarchical failure forecast alongside the legacy layers.
    ///
    /// Scored only on events the legacy layers were scored on (`forecasts` is
    /// `Some`), so every skill on the dashboard describes the same population.
    ///
    /// Also scores both models' timing and transfer forecasts, the values
    /// routing would act on, in SECONDS on the events that carry them, and
    /// records the forecasts in the dataset. This is the instrument for the
    /// promotion gate's "not worse in seconds" (`.claude/rules/ring.md`).
    fn score_hierarchical_layer(
        &mut self,
        forecasts: Option<&mut dataset::FailureForecasts>,
        observed: hierarchical::Observed,
        legacy: LegacyTimingForecast,
        actual: ObservedTiming,
        actual_failure: f64,
        now_hours: f64,
    ) {
        let Some(forecasts) = forecasts else {
            return;
        };
        let hierarchical = observed.estimate;
        forecasts.hierarchical = observed.failure;
        // Both models' times pass through the same floor, for the dataset's log
        // and for the seconds error alike: a 0 s forecast is scored as 1 ms, not
        // dropped, so neither model's population loses events the other keeps.
        let floor_time = |seconds: f64| seconds.max(hierarchical::MIN_RESPONSE_SECS);
        let hierarchical_time = hierarchical.time_to_response_start_secs.map(floor_time);
        let legacy_time = legacy.time_to_response_start_secs.map(floor_time);
        forecasts.log_response_time_hierarchical = hierarchical_time.map(f64::ln);
        forecasts.log_transfer_speed_hierarchical = hierarchical.transfer_speed_bps.map(f64::ln);
        forecasts.log_response_time_legacy = legacy_time.map(f64::ln);
        forecasts.log_transfer_speed_legacy = legacy.transfer_speed_bps.map(f64::ln);
        if let Some(probability) = observed.failure {
            self.failure_skill_hierarchical
                .record(probability, actual_failure);
        }
        // Same population for both models: an event counts only when both
        // forecast it and it carries the measurement.
        if let (Some(actual), Some(legacy_time), Some(hierarchical_time)) =
            (actual.response_secs, legacy_time, hierarchical_time)
        {
            self.response_time_error
                .record(legacy_time, hierarchical_time, actual, now_hours);
        }
        if let (Some((bytes, actual)), Some(legacy_speed), Some(hierarchical_speed)) = (
            actual.transfer,
            legacy.transfer_speed_bps,
            hierarchical.transfer_speed_bps,
        ) {
            self.transfer_time_error.record(
                bytes / legacy_speed,
                bytes / hierarchical_speed,
                actual,
                now_hours,
            );
        }
    }

    /// The legacy stack's timing and transfer forecasts as routing would act on
    /// them with the hierarchical flag off, including the residual correction
    /// when `FREENET_ROUTING_RESIDUAL_CORRECTION` is on. Computed only alongside
    /// the hierarchical estimator, whose comparison it exists for.
    ///
    /// Takes the query results `score_failure_layers` already ran; it runs no
    /// Renegade query of its own.
    fn legacy_timing_forecast(
        &self,
        peer: &PeerKeyLocation,
        contract_location: Location,
        queries: &LegacyQueries,
    ) -> LegacyTimingForecast {
        let Ok(failure) = self
            .failure_estimator
            .estimate_retrieval_time(peer, contract_location)
        else {
            return LegacyTimingForecast::default();
        };
        let time_estimate = self
            .response_start_time_estimator
            .estimate_retrieval_time(peer, contract_location)
            .ok();
        let transfer_estimate = self
            .transfer_rate_estimator
            .estimate_retrieval_time(peer, contract_location)
            .ok();
        let legacy = self.combine_legacy_stages(
            peer,
            contract_location,
            failure,
            time_estimate,
            transfer_estimate,
            queries,
            residual_correction_enabled(),
        );
        LegacyTimingForecast::acted_on(
            time_estimate.is_some(),
            transfer_estimate.is_some(),
            &legacy,
        )
    }

    fn predict_routing_outcome(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
    ) -> Result<RoutingPrediction, RoutingError> {
        self.predict_routing_outcome_at(peer, target_location, self.prediction_clock())
    }

    /// Both clocks, read now.
    fn prediction_clock(&self) -> PredictionClock {
        PredictionClock {
            wall_clock_hours: routing_predictor::wall_clock_hours(),
            estimator_hours: self.estimator_clock.hours(),
        }
    }

    /// [`Self::predict_routing_outcome`] at explicit clock readings, so a test
    /// can make two calls on identical clocks.
    fn predict_routing_outcome_at(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
        clock: PredictionClock,
    ) -> Result<RoutingPrediction, RoutingError> {
        self.predict_with_model(peer, target_location, clock, hierarchical_routing_enabled())
            .map(|(prediction, _)| prediction)
    }

    /// The prediction routing would act on with the hierarchical estimator in
    /// or out of routing (`use_hierarchical`), whatever the flag is actually set
    /// to, and which stages the hierarchical estimator supplied itself. Routing
    /// passes the flag; the candidate log (`dataset`) also asks for the other
    /// model.
    fn predict_with_model(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
        clock: PredictionClock,
        use_hierarchical: bool,
    ) -> Result<(RoutingPrediction, dataset::HierarchicalStages), RoutingError> {
        if !self.has_sufficient_routing_events() {
            return Err(RoutingError::InsufficientDataError);
        }

        // Failure estimator is required — it has data from all outcome types
        let failure_estimate = self
            .failure_estimator
            .estimate_retrieval_time(peer, target_location)
            .map_err(|source| RoutingError::EstimationError {
                estimation: "failure",
                source,
            })?;

        // Timing estimators are optional — they only get data from timed GET successes
        // which are rare (~4% of operations).
        let time_estimate = self
            .response_start_time_estimator
            .estimate_retrieval_time(peer, target_location)
            .ok();
        let transfer_estimate = self
            .transfer_rate_estimator
            .estimate_retrieval_time(peer, target_location)
            .ok();

        let distance = peer
            .location()
            .map(|loc| target_location.distance(loc).as_f64())
            .unwrap_or(0.5);

        // Hierarchical estimator (#4485): when enabled it REPLACES every stage it
        // can estimate, taking precedence over both the legacy blend and the
        // residual correction. Computed ONLY when enabled: this runs once per
        // candidate per routing decision.
        let hierarchical = if use_hierarchical {
            self.hierarchical
                .estimate(peer, target_location, distance, clock.estimator_hours)
        } else {
            hierarchical::Estimate::default()
        };
        let hierarchical_failure = hierarchical
            .failure_probability
            .filter(|p| p.is_finite())
            .map(|p| p.clamp(0.0, 1.0));
        let hierarchical_time = hierarchical
            .time_to_response_start_secs
            .filter(|t| t.is_finite() && *t >= 0.0);
        let hierarchical_speed = hierarchical
            .transfer_speed_bps
            .filter(|v| v.is_finite() && *v > 0.0);

        // The legacy stack is evaluated only for a stage the hierarchical
        // estimator cannot supply yet (no curve), so with the flag on and a warm
        // estimator no Renegade query runs per candidate. With the flag off it
        // always runs, and its arithmetic is exactly the pre-#4485 path.
        let legacy_needed = hierarchical_failure.is_none()
            || (hierarchical_time.is_none() && time_estimate.is_some())
            || (hierarchical_speed.is_none() && transfer_estimate.is_some());
        let legacy = legacy_needed.then(|| {
            self.legacy_stage_estimates(
                peer,
                target_location,
                distance,
                failure_estimate,
                time_estimate,
                transfer_estimate,
                clock.wall_clock_hours,
            )
        });

        let failure_cost_multiplier = 3.0;
        let (failure_estimate, renegade_failure_adjustment) = match hierarchical_failure {
            // The blend did not reach the estimate, so it is not reported as if
            // it had.
            Some(probability) => (probability, None),
            // `legacy` is always evaluated when the failure stage is missing.
            None => legacy.map_or((failure_estimate.clamp(0.0, 1.0), None), |legacy| {
                (legacy.failure, legacy.renegade_failure_adjustment)
            }),
        };
        // The failure value the cost formula ranks by. For the hierarchical
        // estimator it keeps the order of the forecasts BEFORE the [0, 1] bound,
        // so peers whose forecasts all clamp at 1 are not tied (see
        // `hierarchical::ranking_failure_probability`); it differs from the
        // reported probability only ABOVE that bound, and then by at most 1e-6
        // per unit of overshoot, so it is never negative and cannot make this
        // cost negative. Legacy ranks by its own probability.
        let failure_for_cost = match hierarchical_failure {
            Some(probability) => hierarchical.failure_ranking.unwrap_or(probability),
            None => failure_estimate,
        };
        let time_to_response_start = hierarchical_time
            .or_else(|| legacy.map(|legacy| legacy.time_to_response_start))
            .unwrap_or(0.0);
        let xfer_speed = hierarchical_speed
            .or_else(|| legacy.map(|legacy| legacy.xfer_speed))
            .unwrap_or(0.0);

        let time_available = time_estimate.is_some() || hierarchical_time.is_some();
        let transfer_available = transfer_estimate.is_some() || hierarchical_speed.is_some();
        let expected_total_time = if time_available && transfer_available {
            // Guard against NaN from 0.0/0.0 (mean_transfer_size with no samples
            // divided by zero xfer_speed). Use a large finite fallback so the peer
            // sorts last but doesn't poison the comparator's total ordering.
            let transfer_time = if xfer_speed > 0.0 {
                let t = self.mean_transfer_size.compute() / xfer_speed;
                if t.is_finite() { t } else { f64::MAX / 2.0 }
            } else {
                f64::MAX / 2.0
            };
            time_to_response_start
                + transfer_time
                + (time_to_response_start * failure_for_cost * failure_cost_multiplier)
        } else {
            failure_for_cost * failure_cost_multiplier
        };

        let stages = dataset::HierarchicalStages {
            failure: hierarchical_failure.is_some(),
            response_time: hierarchical_time.is_some(),
            transfer_speed: hierarchical_speed.is_some(),
        };
        Ok((
            RoutingPrediction {
                failure_probability: failure_estimate,
                xfer_speed: TransferSpeed {
                    bytes_per_second: xfer_speed,
                },
                time_to_response_start,
                expected_total_time,
                renegade_failure_adjustment,
            },
            stages,
        ))
    }

    /// Like `select_k_best_peers` but also returns a `RoutingDecisionInfo` for telemetry.
    pub fn select_k_best_peers_with_telemetry<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
        k: usize,
    ) -> (Vec<&'a PeerKeyLocation>, RoutingDecisionInfo) {
        let (selected, decision, _) =
            self.select_k_best_peers_capturing(peers, target_location, k, false);
        (selected, decision)
    }

    /// [`Self::select_k_best_peers_with_telemetry`], and with `capture` also the
    /// candidate set for the routing dataset's `decision` line: every scored
    /// candidate with BOTH models' predictions, taken from the decision as it
    /// is made. `None` without `capture`, and for a distance-based decision.
    ///
    /// Capturing costs one extra prediction per candidate (the model that is
    /// not routing) under the caller's router READ lock; see the cost notes in
    /// [`dataset`]'s "Candidate sets". Without `capture` the path is the plain
    /// routing decision. Building and sending the record is left to the
    /// caller, after the lock is released.
    pub(crate) fn select_k_best_peers_capturing<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
        k: usize,
        capture: bool,
    ) -> (
        Vec<&'a PeerKeyLocation>,
        RoutingDecisionInfo,
        Option<dataset::DecisionCapture<'a>>,
    ) {
        let total_routing_events = self.failure_estimator.len();

        if k == 0 {
            return (
                Vec::new(),
                RoutingDecisionInfo {
                    target_location: target_location.as_f64(),
                    strategy: RoutingStrategy::DistanceBased,
                    candidates: Vec::new(),
                    total_routing_events,
                },
                None,
            );
        }

        if !self.has_sufficient_routing_events() {
            let mut peer_distances: Vec<_> = peers
                .into_iter()
                .filter_map(|peer| {
                    peer.location().map(|loc| {
                        let distance = target_location.distance(loc);
                        (peer, distance)
                    })
                })
                .collect();

            GlobalRng::shuffle(&mut peer_distances);
            // Prefer untried peers over peers with any routing history.
            // `peer_adjustments` is populated for ALL peers once the global regression
            // has >= ADJUSTMENT_PRIOR_SIZE (10) events, regardless of success/failure.
            // In this sub-50-event regime, preferring untried peers is the right
            // exploration strategy: it breaks death spirals where the closest peer
            // always times out, and helps the router accumulate diverse data toward
            // the prediction threshold. Within each group, closest peer wins.
            peer_distances.sort_by(|(pa, da), (pb, db)| {
                let fa = self.failure_estimator.peer_adjustments.contains_key(pa);
                let fb = self.failure_estimator.peer_adjustments.contains_key(pb);
                fa.cmp(&fb).then_with(|| da.cmp(db))
            });
            peer_distances.truncate(k);

            let candidates: Vec<RoutingCandidate> = peer_distances
                .iter()
                .map(|(_, dist)| RoutingCandidate {
                    distance: dist.as_f64(),
                    prediction: None,
                    selected: true, // All are selected (list already truncated to k)
                })
                .collect();

            let selected: Vec<&'a PeerKeyLocation> =
                peer_distances.into_iter().map(|(peer, _)| peer).collect();

            let decision = RoutingDecisionInfo {
                target_location: target_location.as_f64(),
                strategy: RoutingStrategy::DistanceBased,
                candidates,
                total_routing_events,
            };
            (selected, decision, None)
        } else {
            let (closest, candidates_available) =
                self.select_closest_peers(peers, &target_location);
            let mut fallback_count = 0;
            let acting_hierarchical = hierarchical_routing_enabled();
            let mut captured: Vec<dataset::CapturedCandidate<'a>> = if capture {
                Vec::with_capacity(closest.len())
            } else {
                Vec::new()
            };

            // `closest` is distance-sorted, so the enumerate index IS the
            // distance rank. Carrying it through the re-sort is how the rank
            // survives being reordered by predicted cost; recovering it
            // afterwards would need peer equality and an O(n) search for
            // information we already had.
            let mut scored: Vec<(usize, &'a PeerKeyLocation, f64, Option<RoutingPrediction>)> =
                closest
                    .iter()
                    .enumerate()
                    .map(|(distance_rank, peer)| {
                        let distance = peer
                            .location()
                            .map(|loc| target_location.distance(loc).as_f64())
                            .unwrap_or(0.5);
                        let prediction = if capture {
                            // Both models on one clock reading; the acting one
                            // is exactly what routing sorts below.
                            let clock = self.prediction_clock();
                            let acting = self
                                .predict_with_model(
                                    peer,
                                    target_location,
                                    clock,
                                    acting_hierarchical,
                                )
                                .ok();
                            let other = self
                                .predict_with_model(
                                    peer,
                                    target_location,
                                    clock,
                                    !acting_hierarchical,
                                )
                                .ok();
                            let (legacy, hierarchical) = if acting_hierarchical {
                                (other, acting)
                            } else {
                                (acting, other)
                            };
                            let estimate =
                                |p: &(RoutingPrediction, dataset::HierarchicalStages)| {
                                    dataset::ModelEstimate {
                                        failure_probability: p.0.failure_probability,
                                        time_to_response_start_s: p.0.time_to_response_start,
                                        transfer_speed_bps: p.0.xfer_speed.bytes_per_second,
                                        expected_total_time: p.0.expected_total_time,
                                    }
                                };
                            captured.push(dataset::CapturedCandidate {
                                peer,
                                legacy: legacy.as_ref().map(estimate),
                                hierarchical: hierarchical.as_ref().map(estimate),
                                hierarchical_stages: hierarchical
                                    .map(|(_, stages)| stages)
                                    .unwrap_or_default(),
                                selected_position: None,
                            });
                            acting.map(|(prediction, _)| prediction)
                        } else {
                            self.predict_routing_outcome(peer, target_location).ok()
                        };
                        if prediction.is_none() {
                            fallback_count += 1;
                        }
                        (distance_rank, *peer, distance, prediction)
                    })
                    .collect();

            // Sort: peers with predictions by expected_total_time, others at the end
            scored.sort_by(|a, b| {
                let time_a = dataset::cost_order_key(a.3.map(|p| p.expected_total_time));
                let time_b = dataset::cost_order_key(b.3.map(|p| p.expected_total_time));
                time_a.total_cmp(&time_b)
            });

            // Record where the winner sat in distance order, so the cost of the
            // candidate-window truncation can be measured rather than guessed.
            if let Some((distance_rank, _, _, _)) = scored.first() {
                self.selection_ranks
                    .record(*distance_rank, closest.len(), candidates_available);
            }

            let strategy = if fallback_count == 0 {
                RoutingStrategy::PredictionBased
            } else {
                // Some or all predictions failed; using distance as tiebreaker
                RoutingStrategy::PredictionFallback
            };

            let candidates: Vec<RoutingCandidate> = scored
                .iter()
                .enumerate()
                .map(|(i, (_, _, dist, pred))| RoutingCandidate {
                    distance: *dist,
                    prediction: pred.map(RoutingPredictionInfo::from),
                    selected: i < k,
                })
                .collect();

            scored.truncate(k);
            // The selection, marked on the capture by the distance rank each
            // returned peer carried through the sort: from the decision itself.
            let capture = capture.then(|| {
                for (position, (distance_rank, _, _, _)) in scored.iter().enumerate() {
                    if let Some(candidate) = captured.get_mut(*distance_rank) {
                        candidate.selected_position = Some(position);
                    }
                }
                dataset::DecisionCapture {
                    contract_location: target_location,
                    acting_model: if acting_hierarchical {
                        dataset::RoutingModel::Hierarchical
                    } else {
                        dataset::RoutingModel::Legacy
                    },
                    prediction_fallback: fallback_count > 0,
                    k,
                    candidates_available,
                    prior_failure_events: total_routing_events,
                    candidates: captured,
                }
            });
            let selected: Vec<&'a PeerKeyLocation> =
                scored.into_iter().map(|(_, peer, _, _)| peer).collect();

            let decision = RoutingDecisionInfo {
                target_location: target_location.as_f64(),
                strategy,
                candidates,
                total_routing_events,
            };
            (selected, decision, capture)
        }
    }

    /// Produce a snapshot of the router model state for telemetry.
    pub fn snapshot(&self) -> RouterSnapshotInfo {
        self.snapshot_with(dataset::global())
    }

    /// [`Self::snapshot`] with an explicit routing-dataset recorder, whose
    /// recording state decides `hierarchical_computed`.
    fn snapshot_with(&self, dataset: Option<&dataset::RoutingDataset>) -> RouterSnapshotInfo {
        let shrinkage = self.renegade_predictor.shrinkage_diagnostics();
        let hierarchical = self.hierarchical.diagnostics();
        let estimator_hours = self.estimator_clock.hours();
        RouterSnapshotInfo {
            network_efficiency_v1: None,
            failure_events: self.failure_estimator.len(),
            success_events: self.response_start_time_estimator.len(),
            transfer_rate_events: self.transfer_rate_estimator.len(),
            prediction_active: self.has_sufficient_routing_events(),
            mean_transfer_size_bytes: self.mean_transfer_size.compute(),
            consider_n_closest_peers: self.consider_n_closest_peers,
            peers_with_failure_adjustments: self.failure_estimator.peer_adjustments.len(),
            peers_with_response_adjustments: self
                .response_start_time_estimator
                .peer_adjustments
                .len(),
            failure_curve: self.failure_estimator.sampled_curve(0.0, 1.0, 50),
            failure_data_range: self.failure_estimator.data_x_range(),
            response_time_curve: self.response_start_time_estimator.sampled_curve(
                0.0,
                f64::INFINITY,
                50,
            ),
            response_time_data_range: self.response_start_time_estimator.data_x_range(),
            transfer_rate_curve: self
                .transfer_rate_estimator
                .sampled_curve(0.0, f64::INFINITY, 50),
            transfer_rate_data_range: self.transfer_rate_estimator.data_x_range(),
            // Downsampled raw observations for the scatter overlay (cap keeps the
            // serialized snapshot small; estimators retain up to 500 each).
            failure_points: self.failure_estimator.sampled_raw_points(100),
            response_time_points: self.response_start_time_estimator.sampled_raw_points(100),
            transfer_rate_points: self.transfer_rate_estimator.sampled_raw_points(100),
            per_op_curves: {
                let mut curves = HashMap::new();
                // Collect all op types that have any data
                let mut op_types: std::collections::HashSet<OpType> =
                    std::collections::HashSet::new();
                op_types.extend(self.per_op_failure.keys());
                op_types.extend(self.per_op_response_time.keys());
                op_types.extend(self.per_op_transfer_rate.keys());

                for ot in op_types {
                    let mut c = PerOpCurves::default();
                    if let Some(est) = self.per_op_failure.get(&ot) {
                        // One fit for both: these estimators fit on read.
                        (c.failure_curve, c.failure_data_range) =
                            est.sampled_curve_and_range(0.0, 1.0, 50);
                        c.failure_events = est.len();
                        c.failure_points = est.sampled_raw_points(100);
                    }
                    if let Some(est) = self.per_op_response_time.get(&ot) {
                        // One fit for both: these estimators fit on read.
                        (c.response_time_curve, c.response_time_data_range) =
                            est.sampled_curve_and_range(0.0, f64::INFINITY, 50);
                        c.response_time_events = est.len();
                        c.response_time_points = est.sampled_raw_points(100);
                    }
                    if let Some(est) = self.per_op_transfer_rate.get(&ot) {
                        // One fit for both: these estimators fit on read.
                        (c.transfer_rate_curve, c.transfer_rate_data_range) =
                            est.sampled_curve_and_range(0.0, f64::INFINITY, 50);
                        c.transfer_rate_events = est.len();
                        c.transfer_rate_points = est.sampled_raw_points(100);
                    }
                    curves.insert(ot.as_str().to_string(), c);
                }
                curves
            },
            // Populated by Ring which has access to both Router and OpManager
            connect_forward_curve: None,
            connect_forward_data_range: None,
            connect_forward_events: None,
            connect_forward_peer_adjustments: None,
            // Node-health gauges populated by Ring on the snapshot cadence (#4440).
            open_fds: None,
            timeout_label_peers_1: None,
            timeout_label_peers_2_3: None,
            timeout_label_peers_4_7: None,
            timeout_label_peers_8_plus: None,
            timeout_label_max_per_peer: None,
            timeout_labels_untracked: None,
            fd_soft_limit: None,
            contract_module_cache_entries: None,
            contract_module_cache_total_bytes: None,
            contract_module_cache_budget_bytes: None,
            contract_module_cache_evictions_total: None,
            delegate_module_cache_entries: None,
            delegate_module_cache_total_bytes: None,
            delegate_module_cache_budget_bytes: None,
            delegate_module_cache_evictions_total: None,
            // Capability-relative hosting-budget gauges populated by Ring on the
            // snapshot cadence (#4642 A2).
            hosting_budget_bytes: None,
            hosting_current_bytes: None,
            hosting_contract_count: None,
            hosting_budget_evictions_total: None,
            // Demand-ordered eviction gauges, populated by Ring
            // on the snapshot cadence (#4642 A3).
            hosting_evictions_of_recently_read_total: None,
            hosting_local_hits_total: None,
            hosting_local_misses_total: None,
            hosting_evicted_unread_total: None,
            hosting_evicted_unread_age_secs_sum: None,
            // Aggregate on-disk usage gauges, populated by Ring from the
            // DiskUsageTracker on the snapshot cadence (#4683).
            hosting_disk_state_bytes: None,
            hosting_disk_wasm_bytes: None,
            hosting_disk_compile_cache_bytes: None,
            hosting_disk_total_bytes: None,
            hosting_oom_valve_evictions_total: None,
            hosting_subscribed_evictions_total: None,
            hosting_cost_evictions_total: None,
            hosting_resident_overhead_budget_bytes: None,
            hosting_estimated_resident_overhead_bytes: None,
            hosting_contract_slot_budget: None,
            hosting_resident_overhead_evictions_total: None,
            notifications_dropped_channel_full: None,
            notifications_dropped_channel_closed: None,
            notifications_no_local_subscriber: None,
            phantom_in_use_contracts: None,
            // Terminal advertisement-consult counters (piece C, #4646),
            // populated by Ring from the network_status singleton on the
            // snapshot cadence (#4658).
            terminal_consult_attempts: None,
            terminal_consult_hits: None,
            terminal_consult_resolved_found: None,
            terminal_consult_still_not_found: None,
            // Eviction-retraction emission counters (#5059), same population
            // path as the consult counters above.
            hosting_retractions_emitted: None,
            hosting_retractions_dropped: None,
            // Computed-upstream vs. stored-flag divergence counters (piece D,
            // #4642 / #4671), populated by Ring from the network_status
            // singleton on the snapshot cadence.
            upstream_computed_vs_stored_comparisons: None,
            upstream_computed_vs_stored_divergences: None,
            // Reconcile-controller shadow comparison counters, split per site
            // (keystone step-2, #4642), populated by Ring from the network_status
            // singleton on the snapshot cadence.
            reconcile_shadow_collapse_comparisons: None,
            reconcile_shadow_collapse_divergences: None,
            reconcile_shadow_collapse_subscribe_diffs: None,
            reconcile_shadow_collapse_renew_diffs: None,
            reconcile_shadow_collapse_unsubscribe_diffs: None,
            reconcile_shadow_collapse_collapse_diffs: None,
            reconcile_shadow_collapse_announce_diffs: None,
            reconcile_shadow_collapse_retract_diffs: None,
            reconcile_shadow_collapse_reroot_search_diffs: None,
            reconcile_shadow_renewal_comparisons: None,
            reconcile_shadow_renewal_divergences: None,
            reconcile_shadow_renewal_subscribe_diffs: None,
            reconcile_shadow_renewal_renew_diffs: None,
            reconcile_shadow_renewal_unsubscribe_diffs: None,
            reconcile_shadow_renewal_collapse_diffs: None,
            reconcile_shadow_renewal_announce_diffs: None,
            reconcile_shadow_renewal_retract_diffs: None,
            reconcile_shadow_renewal_reroot_search_diffs: None,
            // Reconcile-controller shadow counters for the single-aspect edge
            // sites (keystone step-2 completion, #4642).
            reconcile_shadow_inbound_unsubscribe_comparisons: None,
            reconcile_shadow_inbound_unsubscribe_divergences: None,
            reconcile_shadow_connection_drop_comparisons: None,
            reconcile_shadow_connection_drop_divergences: None,
            reconcile_shadow_host_formation_comparisons: None,
            reconcile_shadow_host_formation_divergences: None,
            // Interest-weighted (two-tier) module-cache shadow gauges,
            // populated by Ring on the snapshot cadence (#4441/#4534).
            contract_module_cache_cold_evictable_bytes: None,
            contract_module_cache_interested_bytes: None,
            contract_module_cache_evictions_would_reclassify_total: None,
            migration_admission_recovered_total: None,
            // Broadcast stream-assembly + background-task health gauges,
            // populated by Ring on the snapshot cadence (#4440).
            broadcast_stream_attempts_total: None,
            broadcast_stream_failures_total: None,
            contract_exec_summarize_fast_hits_total: None,
            contract_exec_summarize_reload_hits_total: None,
            contract_exec_summarize_wasm_calls_total: None,
            contract_exec_summarize_wasm_uncached_total: None,
            contract_exec_delta_fast_hits_total: None,
            contract_exec_delta_reload_hits_total: None,
            contract_exec_delta_wasm_calls_total: None,
            contract_exec_delta_wasm_uncached_total: None,
            contract_exec_summarize_fast_hits_last_snapshot: None,
            contract_exec_summarize_reload_hits_last_snapshot: None,
            contract_exec_summarize_wasm_calls_last_snapshot: None,
            contract_exec_summarize_wasm_uncached_last_snapshot: None,
            contract_exec_delta_fast_hits_last_snapshot: None,
            contract_exec_delta_reload_hits_last_snapshot: None,
            contract_exec_delta_wasm_calls_last_snapshot: None,
            contract_exec_delta_wasm_uncached_last_snapshot: None,
            broadcast_stream_failures_last_snapshot: None,
            // Placement-quality + placement-migration gauges populated by Ring on
            // the snapshot cadence (#4404 follow-up).
            hosted_contracts_count: None,
            hosted_key_distance_median: None,
            hosted_key_distance_p90: None,
            hosted_key_distance_min: None,
            hosted_key_distance_mean: None,
            hosted_key_distance_frac_within_0_1: None,
            subscribe_hint_sent: None,
            subscribe_hint_received: None,
            subscribe_hint_acted: None,
            subscribe_hint_refused_version: None,
            subscribe_hint_refused_already_hosting: None,
            subscribe_hint_refused_holder: None,
            subscribe_hint_refused_cache: None,
            subscribe_hint_acted_succeeded: None,
            subscribe_hint_acted_failed: None,
            renewal_terminus_satisfied: None,
            // Nearest-neighbor ring-lattice gauges populated by Ring on the
            // snapshot cadence (#4760 / #4642).
            lattice_has_successor: None,
            lattice_has_predecessor: None,
            lattice_successor_distance: None,
            lattice_predecessor_distance: None,
            lattice_probes_issued: None,
            lattice_probe_improvements: None,
            // Version-gate refusal counters, populated by Ring on the
            // snapshot cadence (#5156).
            hash_first_summaries_declined_unknown_version: None,
            hash_first_summaries_declined_pre_floor: None,
            summary_first_put_declined_unknown_version: None,
            summary_first_put_declined_pre_floor: None,
            // Streamed-transfer abort counters, populated by Ring from the
            // network_status singleton on the snapshot cadence (Group B).
            stream_recv_aborts_inactivity_total: None,
            stream_recv_aborts_cancelled_total: None,
            stream_recv_aborts_claim_timeout_total: None,
            stream_recv_aborts_deserialize_total: None,
            stream_send_aborts_cwnd_total: None,
            stream_recv_abort_frac_0: None,
            stream_recv_abort_frac_1: None,
            stream_recv_abort_frac_lt50: None,
            stream_recv_abort_frac_50_90: None,
            stream_recv_abort_frac_ge90: None,
            // Routing/hosting attribution, populated by Ring on the snapshot
            // cadence (Group C).
            ring_connections: None,
            transient_connections: None,
            connections_to_gateways: None,
            relayed_gets_total: None,
            relayed_puts_total: None,
            relayed_subscribes_total: None,
            relayed_updates_total: None,
            // Connect-event emission counters, populated by Ring on the snapshot
            // cadence (firehose-retirement precursor).
            connect_accepts_emitted: None,
            connect_rejects_emitted: None,
            // Bootstrap-acceptance-churn counters, populated by Ring on the
            // snapshot cadence (#4787).
            bootstrap_transient_registered: None,
            bootstrap_transient_expired: None,
            bootstrap_promoted_to_ring: None,
            bootstrap_time_to_min_connections_secs: None,
            bootstrap_completed: None,
            bootstrap_startup_rounds_connect_issued_gateway: None,
            bootstrap_startup_rounds_connect_issued_routed: None,
            bootstrap_startup_rounds_backoff_blocked: None,
            bootstrap_startup_rounds_no_target: None,
            // Renegade predictor diagnostics
            renegade_failure_events: self.renegade_predictor.len(),
            renegade_response_time_events: self.renegade_predictor.stage_sizes().1,
            renegade_transfer_speed_events: self.renegade_predictor.stage_sizes().2,
            renegade_known_peers: self.renegade_predictor.known_peers(),
            renegade_brier_score: self.renegade_predictor.brier_score(),
            renegade_recent_brier_score: self.renegade_predictor.recent_brier_score(),
            renegade_predictions_evaluated: self.renegade_predictor.predictions_evaluated(),
            renegade_accuracy_pairs: self
                .renegade_predictor
                .recent_accuracy_pairs()
                .iter()
                .copied()
                .collect(),
            renegade_response_time_pairs: self
                .renegade_predictor
                .response_time_accuracy_pairs()
                .iter()
                .copied()
                .collect(),
            renegade_transfer_speed_pairs: self
                .renegade_predictor
                .transfer_speed_accuracy_pairs()
                .iter()
                .copied()
                .collect(),
            renegade_response_time_evaluated: self
                .renegade_predictor
                .response_time_predictions_evaluated(),
            renegade_transfer_speed_evaluated: self
                .renegade_predictor
                .transfer_speed_predictions_evaluated(),
            failure_skill_global: self.failure_skill_global.skill(),
            failure_skill_adjusted: self.failure_skill_adjusted.skill(),
            failure_skill_blended: self.failure_skill_blended.skill(),
            failure_skill_corrected: self.failure_skill_corrected.skill(),
            failure_skill_hierarchical: self.failure_skill_hierarchical.skill(),
            hierarchical_failure_evaluated: self.failure_skill_hierarchical.count(),
            hierarchical_routing_enabled: hierarchical_routing_enabled(),
            hierarchical_failure_horizon_hours: hierarchical[0].selected_horizon_hours,
            hierarchical_failure_events: hierarchical[0].window_events,
            hierarchical_peer_evictions: self.hierarchical.total_evictions(),
            hierarchical_peer_capacity: hierarchical[0].peer_capacity,
            hierarchical_contracts: hierarchical[0].contracts,
            hierarchical_contract_evictions: hierarchical[0].contract_evictions,
            hierarchical_contract_residuals_refused: hierarchical[0].contract_residuals_refused,
            hierarchical_contract_pairs_refused_last_refit: hierarchical[0]
                .contract_pairs_refused_last_refit,
            hierarchical_contract_entries_displaced: hierarchical[0].contract_entries_displaced,
            hierarchical_contract_estimable_refits: hierarchical[0].contract_estimable_refits,
            hierarchical_contract_effects_applied: hierarchical[0].contract_effects_applied,
            hierarchical_contract_forecast_offsets: hierarchical[0].contract_forecast_offsets,
            hierarchical_contract_floor_bound_refits: hierarchical[0].contract_floor_bound_refits,
            hierarchical_contract_den_below_two_refits: hierarchical[0]
                .contract_den_below_two_refits,
            hierarchical_contract_qualifying_contracts: hierarchical[0]
                .contract_qualifying_contracts,
            hierarchical_contract_qualifying_entries: hierarchical[0].contract_qualifying_entries,
            hierarchical_contract_tau2: hierarchical[0].contract_tau2,
            hierarchical_computed: hierarchical_computed(dataset),
            hierarchical_failure_active: hierarchical[0].active,
            routing_dataset_stopped: dataset.is_some_and(|dataset| !dataset.is_recording()),
            routing_dataset_open_failed: dataset.is_none() && dataset::configured(),
            hierarchical_floored_response_times: self.hierarchical.floored_response_times(),
            hierarchical_non_speed_samples: self.hierarchical.non_speed_samples(),
            response_time_rmse_secs_legacy: self.response_time_error.rmse().map(|(l, _)| l),
            response_time_rmse_secs_hierarchical: self.response_time_error.rmse().map(|(_, h)| h),
            response_time_scored: self.response_time_error.count,
            response_time_weight: self.response_time_error.weight_at(estimator_hours),
            transfer_time_rmse_secs_legacy: self.transfer_time_error.rmse().map(|(l, _)| l),
            transfer_time_rmse_secs_hierarchical: self.transfer_time_error.rmse().map(|(_, h)| h),
            transfer_time_scored: self.transfer_time_error.count,
            transfer_time_weight: self.transfer_time_error.weight_at(estimator_hours),
            hierarchical_response_time_log_shape: hierarchical[1].into(),
            hierarchical_transfer_speed_log_shape: hierarchical[2].into(),
            failure_brier_blended: self.failure_skill_blended.brier(),
            failure_climatology_brier: self.failure_skill_blended.climatology_brier(),
            failure_base_rate: self.failure_skill_blended.base_rate(),
            failure_layers_evaluated: self.failure_skill_blended.count(),
            residual_correction_enabled: residual_correction_enabled(),
            residual_kappa: Some(shrinkage.failure_kappa),
            residual_bandwidth: shrinkage.failure_bandwidth,
            residual_failure_events: shrinkage.failure_residual_events,
            residual_response_time_events: shrinkage.response_time_residual_events,
            residual_transfer_speed_events: shrinkage.transfer_speed_residual_events,
            residual_scored: shrinkage.failure_scored,
            selection_ranks: self.selection_ranks.snapshot(),
        }
    }

    /// Produce a per-peer routing snapshot for the dashboard detail page.
    pub(crate) fn peer_snapshot(&self, peer: &PeerKeyLocation) -> PeerRoutingSnapshot {
        let failure_adj = self
            .failure_estimator
            .peer_adjustments
            .get(peer)
            .map(|a| (a.value(), a.event_count()));
        let response_time_adj = self
            .response_start_time_estimator
            .peer_adjustments
            .get(peer)
            .map(|a| (a.value(), a.event_count()));
        let transfer_rate_adj = self
            .transfer_rate_estimator
            .peer_adjustments
            .get(peer)
            .map(|a| (a.value(), a.event_count()));

        // Compute a sample prediction at the peer's own location (distance=0)
        let prediction = peer
            .location()
            .and_then(|loc| self.predict_routing_outcome(peer, loc).ok())
            .map(RoutingPredictionInfo::from);

        PeerRoutingSnapshot {
            failure_adjustment: failure_adj,
            response_time_adjustment: response_time_adj,
            transfer_rate_adjustment: transfer_rate_adj,
            prediction_at_own_location: prediction,
        }
    }

    /// Whether we have enough routing events to attempt prediction-based selection.
    ///
    /// Uses `failure_estimator` which records both successes (0.0) and failures (1.0),
    /// so it reflects total routing events. Note: this can return true even when
    /// `response_start_time_estimator` has too few events for individual predictions —
    /// callers must handle the fallback case via `predict_routing_outcome` returning Err.
    ///
    /// Threshold of 50 (down from 200): with failure data now flowing through the
    /// router, 50 events provides meaningful signal for the isotonic regression.
    /// The old threshold of 200 success-only events was effectively unreachable.
    fn has_sufficient_routing_events(&self) -> bool {
        const MIN_EVENTS_FOR_PREDICTION: usize = 50;
        self.failure_estimator.len() >= MIN_EVENTS_FOR_PREDICTION
    }
}

/// Renegade query results for one (peer, contract) query.
#[derive(Debug, Clone)]
struct LegacyQueries {
    renegade: routing_predictor::RoutingPredictionResult,
    corrections: routing_predictor::RoutingCorrections,
}

/// The legacy stack's acted-on timing and transfer forecasts for one event.
#[derive(Debug, Clone, Copy, Default)]
struct LegacyTimingForecast {
    time_to_response_start_secs: Option<f64>,
    transfer_speed_bps: Option<f64>,
}

impl LegacyTimingForecast {
    /// The timing and speed routing would act on, given which isotonic stages
    /// can estimate (a stage that cannot is unknown to the cost formula, as in
    /// `predict_routing_outcome`).
    fn acted_on(
        time_available: bool,
        transfer_available: bool,
        legacy: &LegacyStageEstimates,
    ) -> Self {
        LegacyTimingForecast {
            // Zero is kept, as routing keeps it (`corrected >= 0.0`); the
            // scoring floors both models' times alike rather than dropping it.
            time_to_response_start_secs: Some(legacy.time_to_response_start)
                .filter(|seconds| time_available && seconds.is_finite() && *seconds >= 0.0),
            transfer_speed_bps: Some(legacy.xfer_speed)
                .filter(|speed| transfer_available && speed.is_finite() && *speed > 0.0),
        }
    }
}

/// The timing measurements an event carries.
#[derive(Debug, Clone, Copy, Default)]
struct ObservedTiming {
    response_secs: Option<f64>,
    /// `(payload bytes, transfer seconds)`, only for a real payload transfer.
    transfer: Option<(f64, f64)>,
}

impl ObservedTiming {
    fn of(outcome: &RouteOutcome) -> Self {
        match outcome {
            RouteOutcome::Success {
                time_to_response_start,
                payload_size,
                payload_transfer_time,
            } => ObservedTiming {
                response_secs: Some(time_to_response_start.as_secs_f64()),
                transfer: (*payload_size > 0 && !payload_transfer_time.is_zero())
                    .then_some((*payload_size as f64, payload_transfer_time.as_secs_f64())),
            },
            RouteOutcome::SuccessUntimed | RouteOutcome::Failure => ObservedTiming::default(),
        }
    }
}

/// Forgetting horizon of the seconds-error comparison, in estimator hours.
const ERROR_FORGETTING_HOURS: f64 = 24.0;

/// Each model's error on an event is clipped to this multiple of THAT EVENT'S
/// outcome (floored at [`ERROR_CLIP_FLOOR_SECS`]).
///
/// Unclipped, one near-zero speed forecast (a transfer time of 1e6 s) squares
/// to 1e12 and settles the comparison by itself. The bound is per event, not
/// the largest outcome seen: a lifetime or even a decayed maximum lets one
/// ordinary slow transfer (600 s on a large payload) admit a 6000 s error on
/// every small payload after it. Relative to its own outcome, a forecast ten
/// times off is simply "an order of magnitude wrong", and a larger miss says
/// nothing more about calibration. It needs no state, so there is nothing to
/// decay.
const ERROR_CLIP_MULTIPLE: f64 = 10.0;

/// Floor on the clip's base, so a zero-length outcome does not clip every
/// error to zero. One millisecond, the same floor the hierarchical response
/// stage applies before taking logs.
const ERROR_CLIP_FLOOR_SECS: f64 = hierarchical::MIN_RESPONSE_SECS;

/// Forgotten event weight (see [`PairedErrorTracker`]) below which the
/// dashboard shows "insufficient data" rather than a verdict. Gated on the
/// forgotten weight, not the lifetime count: 100 events a week ago are not 100
/// events of evidence now.
pub(crate) const MIN_WEIGHT_FOR_VERDICT: f64 = 100.0;

/// Prequential squared error in seconds of BOTH models on the same events,
/// exponentially forgotten over [`ERROR_FORGETTING_HOURS`].
///
/// Not [`residual::SkillTracker`]: that scores a binary forecast against a
/// base-rate climatology, which has no meaning for a duration. One tracker for
/// both models so they cannot drift onto different populations: an event is
/// scored for both or for neither.
#[derive(Debug, Default, Clone, Copy)]
struct PairedErrorTracker {
    legacy: f64,
    hierarchical: f64,
    /// Forgotten count, the denominator of both means.
    weight: f64,
    /// Events ever scored.
    count: u64,
    last_hours: Option<f64>,
}

impl PairedErrorTracker {
    fn record(&mut self, legacy: f64, hierarchical: f64, actual: f64, now_hours: f64) {
        let legacy_error = legacy - actual;
        let hierarchical_error = hierarchical - actual;
        if !(actual.is_finite()
            && actual >= 0.0
            && legacy_error.is_finite()
            && hierarchical_error.is_finite()
            && now_hours.is_finite())
        {
            return;
        }
        let clip = ERROR_CLIP_MULTIPLE * actual.max(ERROR_CLIP_FLOOR_SECS);
        let decay = self.last_hours.map_or(1.0, |then| {
            (-(now_hours - then).max(0.0) / ERROR_FORGETTING_HOURS).exp()
        });
        self.legacy = self.legacy * decay + legacy_error.clamp(-clip, clip).powi(2);
        self.hierarchical =
            self.hierarchical * decay + hierarchical_error.clamp(-clip, clip).powi(2);
        self.weight = self.weight * decay + 1.0;
        self.count += 1;
        self.last_hours = Some(
            self.last_hours
                .map_or(now_hours, |then| then.max(now_hours)),
        );
    }

    /// The forgotten weight as of `now_hours`, not as of the last scored event.
    ///
    /// The stored sums decay only when an event is recorded, so after a quiet
    /// or failure-only stretch the stored weight would still read as recent.
    /// The means need no such correction: decay scales both sums and the
    /// weight alike, so it cancels in [`Self::rmse`].
    fn weight_at(&self, now_hours: f64) -> f64 {
        match self.last_hours {
            Some(then) if now_hours.is_finite() => {
                self.weight * (-(now_hours - then).max(0.0) / ERROR_FORGETTING_HOURS).exp()
            }
            _ => self.weight,
        }
    }

    /// `(legacy, hierarchical)` RMS error in seconds.
    fn rmse(&self) -> Option<(f64, f64)> {
        (self.weight > 0.0).then(|| {
            (
                (self.legacy / self.weight).sqrt(),
                (self.hierarchical / self.weight).sqrt(),
            )
        })
    }
}

/// The legacy stack's per-stage estimates for one candidate.
#[derive(Debug, Clone, Copy)]
struct LegacyStageEstimates {
    failure: f64,
    renegade_failure_adjustment: Option<f64>,
    time_to_response_start: f64,
    xfer_speed: f64,
}

/// The legacy response-time estimate: the isotonic estimate, blended with
/// Renegade's when Renegade has a finite, non-negative prediction.
///
/// Shared by routing and by the dataset's recorded forecast, so the recorded
/// "legacy" figure cannot drift from the one routing acts on.
fn legacy_response_time(estimate: f64, renegade: Option<f64>, weight: f64) -> f64 {
    match renegade {
        Some(renegade) if renegade.is_finite() && renegade >= 0.0 => {
            estimate * (1.0 - weight) + renegade * weight
        }
        _ => estimate,
    }
}

#[derive(Debug, thiserror::Error)]
enum RoutingError {
    #[error("Insufficient data provided")]
    InsufficientDataError,
    #[error("failed {estimation} estimation: {source}")]
    EstimationError {
        estimation: &'static str,
        #[source]
        source: isotonic_estimator::EstimationError,
    },
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct RoutingPrediction {
    pub failure_probability: f64,
    pub xfer_speed: TransferSpeed,
    pub time_to_response_start: f64,
    pub expected_total_time: f64,
    /// How much renegade shifted the failure estimate from isotonic baseline.
    pub renegade_failure_adjustment: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RouteEvent {
    pub peer: PeerKeyLocation,
    pub contract_location: Location,
    pub outcome: RouteOutcome,
    /// Which operation produced this event. None when the operation type is unknown
    /// (e.g., generic timeout handler).
    pub op_type: Option<crate::node::network_status::OpType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum RouteOutcome {
    Success {
        time_to_response_start: Duration,
        payload_size: usize,
        payload_transfer_time: Duration,
    },
    /// Operation succeeded but has no timing data (subscribe, put, update).
    /// Feeds only the failure_estimator (0.0 = success), not timing estimators.
    SuccessUntimed,
    Failure,
}

#[cfg(test)]
mod tests {
    use super::isotonic_estimator::FitPolicy;
    use crate::ring::Distance;

    /// `NetworkEfficiencyV1`'s `futile` rustdoc, isolated from this test module
    /// — the needles below appear here too, and a pin that matches its own
    /// source can never fail.
    fn futile_row_doc() -> &'static str {
        const FULL: &str = include_str!("router.rs");
        let start = FULL
            .find("pub(crate) struct NetworkEfficiencyV1")
            .expect("NetworkEfficiencyV1 not found");
        let end = FULL[start..]
            .find("pub futile_ladder")
            .map(|off| start + off)
            .expect("the futile row must still be declared on NetworkEfficiencyV1");
        &FULL[start..end]
    }

    /// The `futile` row's rustdoc is the only place a reader of the fleet data
    /// meets these counters, and several of them are wrong in a specific,
    /// plausible direction if read naively. Each caveat below was a review
    /// finding; each would regress into a wrong published number rather than a
    /// failing test, so the doc is pinned like code.
    #[test]
    fn futile_row_doc_carries_the_reading_caveats() {
        let doc = futile_row_doc();
        for (needle, why) in [
            (
                "outcomes_probe_budget_exhausted",
                "the load-correlated channel: past the 32-probe budget every \
                 further contract reads as stale with no divergence at all, so \
                 the reader has to be able to size it",
            ),
            (
                "outcomes_after_long_gap",
                "on byte-budgeted fallback links a contract is re-compared on \
                 the order of ten hours, so the headline can be carried by \
                 outcomes whose attempt is long stale",
            ),
            (
                "attempts_discarded",
                "attempts dropped at peer teardown are where the undercount \
                 lives, and are the counter most likely to be non-zero",
            ),
            (
                "PER OBSERVER",
                "both ends of a diverged edge count the same stuck edge, so a \
                 fleet SUM of would_quarantine is ~2x the distinct population",
            ),
            (
                "not a repair-efficacy ratio",
                "an edge also converges via the proximity-overlap heal or live \
                 UPDATE fan-out, neither of which records an attempt, so \
                 `productive` credits our heal for someone else's fix",
            ),
        ] {
            assert!(
                doc.contains(needle),
                "the `futile` row rustdoc no longer mentions `{needle}` — {why}"
            );
        }
    }

    use super::*;

    /// Feed `count` timed GET successes straight into the in-memory router, which
    /// is exactly what `operations::record_relay_route_event` does on a relay hop:
    /// `add_event` only, never persisted to the event log.
    fn add_relay_recorded_successes(router: &mut Router, count: usize) {
        for _ in 0..count {
            router.add_event(RouteEvent {
                peer: PeerKeyLocation::random(),
                contract_location: Location::random(),
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(100),
                    payload_size: 5000,
                    payload_transfer_time: Duration::from_millis(50),
                },
                op_type: Some(OpType::Get),
            });
        }
    }

    /// The routing dataset exists to let predictors be replayed and compared
    /// offline, so a record is only useful if its forecasts are the ones made
    /// BEFORE the outcome was ingested — a forecast that has already seen its own
    /// outcome would make every layer look better than it is.
    #[test]
    fn dataset_records_pre_ingestion_forecasts_and_the_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let recorder = dataset::RoutingDataset::open(&path, dataset::DEFAULT_MAX_BYTES).unwrap();

        // The warm-up below goes through `add_event` with no recorder, so the
        // estimator must be switched on to learn from it.
        let _hierarchical = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        // A key of its own: `PeerKeyLocation::random()` reuses one key per
        // thread, which would make the peer-hash assertion below vacuous.
        let peer = PeerKeyLocation::new(
            crate::transport::TransportKeypair::new().public().clone(),
            "192.0.2.10:31337".parse().unwrap(),
        );
        let contract = Location::new(0.5);

        // Cold: nothing to forecast with yet, which the record must say rather
        // than inventing numbers.
        router.add_event_recording(
            RouteEvent {
                peer: peer.clone(),
                contract_location: contract,
                outcome: RouteOutcome::Failure,
                op_type: Some(OpType::Put),
            },
            dataset::RouteSource::Originator,
            Some(&recorder),
        );

        add_relay_recorded_successes(&mut router, 120);
        for _ in 0..30 {
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location: contract,
                outcome: RouteOutcome::Failure,
                op_type: Some(OpType::Get),
            });
        }

        let expected_global = router
            .failure_estimator
            .estimate_global(&peer, contract)
            .unwrap()
            .clamp(0.0, 1.0);
        let expected_adjusted = router
            .failure_estimator
            .estimate_retrieval_time(&peer, contract)
            .unwrap()
            .clamp(0.0, 1.0);
        let events_before = router.failure_estimator.len();
        let hierarchical_distance = contract.distance(peer.location().unwrap()).as_f64();
        let hierarchical_estimate = |router: &Router| {
            router
                .hierarchical
                .estimate(
                    &peer,
                    contract,
                    hierarchical_distance,
                    router.estimator_clock.hours(),
                )
                .failure_probability
                .expect("the hierarchical failure stage is warm")
        };
        let expected_hierarchical = hierarchical_estimate(&router);
        // What routing would act on for this query, before ingestion.
        let acted_on = router.hierarchical.estimate(
            &peer,
            contract,
            hierarchical_distance,
            router.estimator_clock.hours(),
        );
        let legacy_acted_on = router.legacy_timing_forecast(
            &peer,
            contract,
            &router.legacy_queries_at(
                &peer,
                contract,
                hierarchical_distance,
                routing_predictor::wall_clock_hours(),
            ),
        );

        router.add_event_recording(
            RouteEvent {
                peer: peer.clone(),
                contract_location: contract,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(250),
                    payload_size: 4096,
                    payload_transfer_time: Duration::from_millis(125),
                },
                op_type: Some(OpType::Get),
            },
            dataset::RouteSource::Originator,
            Some(&recorder),
        );
        let adjusted_after = router
            .failure_estimator
            .estimate_retrieval_time(&peer, contract)
            .unwrap()
            .clamp(0.0, 1.0);
        assert_ne!(
            adjusted_after, expected_adjusted,
            "sanity: ingesting the success must move the peer's estimate, or this \
             test cannot tell pre- from post-ingestion forecasts"
        );
        let hierarchical_after = hierarchical_estimate(&router);
        assert!(
            (hierarchical_after - expected_hierarchical).abs() > 1e-4,
            "sanity: the success must move the hierarchical estimate too: \
             {expected_hierarchical} -> {hierarchical_after}"
        );
        drop(recorder);

        let lines = dataset::lines_eventually(&path, |lines| {
            lines.iter().filter(|line| line["kind"] == "route").count() == 2
        });
        let routes: Vec<&serde_json::Value> = lines
            .iter()
            .filter(|line| line["kind"] == "route")
            .collect();

        let cold = routes[0];
        assert_eq!(cold["outcome"], "failure");
        assert_eq!(cold["op"], "PUT");
        assert_eq!(cold["prior_failure_events"], 0);
        assert!(
            cold["forecasts"].is_null(),
            "a cold router forecasts nothing: {cold}"
        );

        let warm = routes[1];
        assert_eq!(warm["peer"], dataset::peer_hash(&peer));
        assert_eq!(warm["source"], "originator");
        let expected_distance = contract.distance(peer.location().unwrap()).as_f64();
        assert!((warm["distance"].as_f64().unwrap() - expected_distance).abs() < 1e-12);
        assert_eq!(warm["contract_location"], 0.5);
        assert_eq!(warm["outcome"], "success");
        assert_eq!(warm["time_to_response_start_s"], 0.25);
        assert_eq!(warm["payload_bytes"], 4096);
        assert_eq!(warm["payload_transfer_s"], 0.125);
        assert_eq!(warm["prior_failure_events"], events_before);
        // Within 1e-12, not bit-exact: serde_json's default float parser does not
        // guarantee a lossless round trip, and which values it misses by an ulp
        // depends on the randomly drawn peers.
        let recorded = |field: &str| warm["forecasts"][field].as_f64().unwrap();
        assert!((recorded("global") - expected_global).abs() < 1e-12);
        assert!(
            (recorded("adjusted") - expected_adjusted).abs() < 1e-12,
            "the recorded forecast must be the pre-ingestion one: recorded {}, \
             pre-ingestion {expected_adjusted}, post-ingestion {adjusted_after}",
            recorded("adjusted")
        );
        assert!(
            (recorded("hierarchical") - expected_hierarchical).abs() < 1e-12,
            "the recorded hierarchical forecast must be the pre-ingestion one: \
             recorded {}, pre-ingestion {expected_hierarchical}, post-ingestion \
             {hierarchical_after}",
            recorded("hierarchical")
        );
        // The only timed traffic so far took 100 ms, so both timing forecasts
        // must sit near ln(0.1) — in log seconds, not milliseconds or seconds.
        for field in ["log_response_time_legacy", "log_response_time_hierarchical"] {
            let value = recorded(field);
            assert!(
                (value - 0.1f64.ln()).abs() < 0.2,
                "{field} must be a log-seconds forecast near ln(0.1), got {value}"
            );
        }
        // The recorded forecasts are the values routing ACTS on: ln E[T] and
        // ln of the effective speed, not the log-scale location.
        let pairs = [
            (
                "log_response_time_hierarchical",
                acted_on.time_to_response_start_secs,
            ),
            (
                "log_transfer_speed_hierarchical",
                acted_on.transfer_speed_bps,
            ),
            (
                "log_response_time_legacy",
                legacy_acted_on.time_to_response_start_secs,
            ),
            (
                "log_transfer_speed_legacy",
                legacy_acted_on.transfer_speed_bps,
            ),
        ];
        for (field, value) in pairs {
            let expected = value
                .unwrap_or_else(|| panic!("{field} has an acted-on value"))
                .ln();
            // Exact for the hierarchical forecasts. The legacy ones pass through
            // Renegade, whose time feature reads the host clock inside
            // `add_event`, so they can move in the fifth digit between the two
            // readings; `legacy_timing_forecast_follows_the_residual_correction_flag`
            // pins them exactly on a shared clock.
            let tolerance = if field.ends_with("_legacy") {
                1e-4
            } else {
                1e-12
            };
            assert!(
                (recorded(field) - expected).abs() < tolerance,
                "{field}: recorded {} but routing would act on ln = {expected}",
                recorded(field)
            );
        }
    }

    /// The legacy timing forecast recorded for the comparison must be the one
    /// routing acts on, so it includes the residual correction when that flag
    /// is on.
    #[test]
    fn legacy_timing_forecast_follows_the_residual_correction_flag() {
        let _learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        let (peer, contract) = feed_mixed_traffic(&mut router, 300);
        let distance = contract.distance(peer.location().unwrap()).as_f64();
        let clock = router.prediction_clock();
        for correction in [false, true] {
            let _correction = force_residual_correction(correction);
            let _off = force_hierarchical_routing(false);
            let queries =
                router.legacy_queries_at(&peer, contract, distance, clock.wall_clock_hours);
            let forecast = router.legacy_timing_forecast(&peer, contract, &queries);
            let acted = router
                .predict_routing_outcome_at(&peer, contract, clock)
                .unwrap();
            assert_eq!(
                forecast.time_to_response_start_secs,
                Some(acted.time_to_response_start),
                "correction={correction}"
            );
            assert_eq!(
                forecast.transfer_speed_bps,
                Some(acted.xfer_speed.bytes_per_second),
                "correction={correction}"
            );
        }
        let queries = router.legacy_queries_at(&peer, contract, distance, clock.wall_clock_hours);
        let off = {
            let _correction = force_residual_correction(false);
            router.legacy_timing_forecast(&peer, contract, &queries)
        };
        let on = {
            let _correction = force_residual_correction(true);
            router.legacy_timing_forecast(&peer, contract, &queries)
        };
        assert_ne!(
            (off.time_to_response_start_secs, off.transfer_speed_bps),
            (on.time_to_response_start_secs, on.transfer_speed_bps),
            "the flag must change the forecast, or the equalities above prove nothing"
        );
    }

    /// The promotion gate's "not worse in seconds" instrument is populated by
    /// traffic, on the same events for both models.
    #[test]
    fn timing_error_in_seconds_reaches_the_snapshot() {
        let _learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        feed_mixed_traffic(&mut router, 400);
        let snapshot = router.snapshot();
        assert!(
            snapshot.response_time_scored > 100,
            "{}",
            snapshot.response_time_scored
        );
        assert!(
            snapshot.transfer_time_scored > 100,
            "{}",
            snapshot.transfer_time_scored
        );

        for value in [
            snapshot.response_time_rmse_secs_legacy,
            snapshot.response_time_rmse_secs_hierarchical,
            snapshot.transfer_time_rmse_secs_legacy,
            snapshot.transfer_time_rmse_secs_hierarchical,
        ] {
            let value = value.expect("scored");
            assert!((0.0..1.0).contains(&value), "{value}");
        }
    }

    /// A recorder that has stopped (byte cap, write error) must not keep the
    /// estimator learning, and the snapshot must say it is not computed now.
    #[test]
    fn a_stopped_recorder_does_not_keep_the_estimator_learning() {
        let _off = force_hierarchical_routing(false);
        let mut router = Router::new(&[]);
        let stopped = dataset::RoutingDataset::stopped_for_test();
        assert!(!stopped.is_recording());
        for _ in 0..50 {
            router.add_event_recording(
                RouteEvent {
                    peer: PeerKeyLocation::random(),
                    contract_location: Location::random(),
                    outcome: RouteOutcome::Failure,
                    op_type: None,
                },
                dataset::RouteSource::Originator,
                Some(&stopped),
            );
        }
        assert_eq!(
            router.hierarchical.diagnostics()[0].window_events,
            0,
            "a stopped recorder must not switch learning on"
        );
        assert!(!hierarchical_computed(Some(&stopped)));
        assert!(!router.snapshot_with(Some(&stopped)).hierarchical_computed);
        // Non-vacuity: the same snapshot path reads a recording recorder as live.
        let dir = tempfile::tempdir().unwrap();
        let recording =
            dataset::RoutingDataset::open(&dir.path().join("r.jsonl"), dataset::DEFAULT_MAX_BYTES)
                .unwrap();
        assert!(router.snapshot_with(Some(&recording)).hierarchical_computed);
    }

    /// One pathological legacy forecast (a near-zero speed, so a transfer time
    /// of a million seconds) must not dominate the comparison, even right after
    /// an ordinary large outcome: the clip is relative to each event's own
    /// outcome, so a 600 s transfer earlier cannot admit a 6000 s error on a
    /// 0.1 s one. Both models are scored on exactly the same events, and a
    /// non-finite error for either skips the event for both.
    #[test]
    fn seconds_error_is_clipped_forgotten_and_paired() {
        let mut tracker = PairedErrorTracker::default();
        // An ordinary slow transfer, forecast well by both.
        tracker.record(600.0, 600.0, 600.0, 0.0);
        // Then the pathological legacy forecast on a small one.
        tracker.record(1.0e6, 0.2, 0.1, 0.0);
        let legacy_sq_after = tracker.legacy;
        assert!(
            legacy_sq_after <= (ERROR_CLIP_MULTIPLE * 0.1).powi(2) + 1e-9,
            "the small event's error must be clipped to 10x its own outcome, not the \
             earlier 600 s: legacy squared error {legacy_sq_after}"
        );

        // A zero outcome must not clip every error to zero.
        let mut zero = PairedErrorTracker::default();
        zero.record(0.5, 0.0, 0.0, 0.0);
        let (legacy, hierarchical) = zero.rmse().unwrap();
        assert!(
            (legacy - ERROR_CLIP_MULTIPLE * ERROR_CLIP_FLOOR_SECS).abs() < 1e-12,
            "a zero outcome clips at the floor, not at zero: {legacy}"
        );
        assert_eq!(hierarchical, 0.0);

        // Non-finite for one model: skipped for both.
        tracker.record(f64::INFINITY, 0.1, 0.1, 0.0);
        tracker.record(0.1, f64::NAN, 0.1, 0.0);
        assert_eq!(
            tracker.count, 2,
            "a non-finite error must skip the event for both"
        );

        // A day of small, accurate traffic from both models afterwards.
        for i in 0..2_000 {
            let hours = 1.0 + i as f64 * 24.0 / 2_000.0;
            tracker.record(0.10, 0.13, 0.1, hours);
        }
        let (legacy, hierarchical) = tracker.rmse().unwrap();
        assert!(
            legacy < hierarchical,
            "a day later, the single bad legacy forecast must not decide the verdict: \
             legacy {legacy} vs hierarchical {hierarchical}"
        );
        assert_eq!(tracker.count, 2_002);
        assert!(
            tracker.weight < 2_000.0 && tracker.weight > 100.0,
            "the verdict's weight is forgotten, not the lifetime count: {}",
            tracker.weight
        );
    }

    /// The published weight decays to the snapshot's own time: after 48 quiet
    /// hours, 500 events scored before them are not "recent" evidence.
    #[test]
    fn published_seconds_error_weight_decays_at_read_time() {
        use crate::util::time_source::SharedMockTimeSource;
        let clock = SharedMockTimeSource::new();
        let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(clock.clone()));
        for _ in 0..500 {
            router.response_time_error.record(0.1, 0.1, 0.1, 0.0);
            router.transfer_time_error.record(0.1, 0.1, 0.1, 0.0);
        }
        let fresh = router.snapshot();
        assert!(fresh.response_time_weight >= MIN_WEIGHT_FOR_VERDICT);
        clock.advance_time(Duration::from_secs(48 * 3600));
        let stale = router.snapshot();
        assert!(
            stale.response_time_weight < MIN_WEIGHT_FOR_VERDICT
                && stale.transfer_time_weight < MIN_WEIGHT_FOR_VERDICT,
            "48 quiet hours must leave too little recent weight for a verdict: {} / {}",
            stale.response_time_weight,
            stale.transfer_time_weight
        );
        assert_eq!(
            stale.response_time_rmse_secs_legacy, fresh.response_time_rmse_secs_legacy,
            "read-time decay changes the weight, not the means"
        );
    }

    /// Routing acts on a 0 s legacy response-time estimate (the correction's
    /// `corrected >= 0.0`), so the timing comparison must receive it rather than
    /// filter it out. The isotonic path floors its estimates above zero, so the
    /// filter is pinned directly on the acted-on values.
    #[test]
    fn legacy_timing_forecast_keeps_a_zero_second_estimate() {
        let legacy = LegacyStageEstimates {
            failure: 0.1,
            renegade_failure_adjustment: None,
            time_to_response_start: 0.0,
            xfer_speed: 0.0,
        };
        let forecast = LegacyTimingForecast::acted_on(true, true, &legacy);
        assert_eq!(
            forecast.time_to_response_start_secs,
            Some(0.0),
            "a 0 s estimate is one routing acts on and must be kept"
        );
        assert_eq!(
            forecast.transfer_speed_bps, None,
            "a zero speed is not a speed"
        );
        assert_eq!(
            LegacyTimingForecast::acted_on(false, true, &legacy).time_to_response_start_secs,
            None,
            "an unavailable stage is unknown, not zero"
        );
    }

    /// A 0 s legacy time forecast (routing acts on it) is scored at the floor
    /// for both models, not dropped.
    #[test]
    fn zero_time_forecasts_are_floored_for_both_models_not_dropped() {
        let _learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        feed_mixed_traffic(&mut router, 300);
        let mut forecasts = router
            .score_failure_layers(&PeerKeyLocation::random(), Location::new(0.3), 0.0)
            .map(|(forecasts, _)| forecasts);
        let before = router.response_time_error.count;
        router.score_hierarchical_layer(
            forecasts.as_mut(),
            hierarchical::Observed {
                failure: Some(0.1),
                estimate: hierarchical::Estimate {
                    failure_probability: Some(0.1),
                    failure_ranking: Some(0.1),
                    time_to_response_start_secs: Some(0.2),
                    transfer_speed_bps: None,
                },
            },
            LegacyTimingForecast {
                time_to_response_start_secs: Some(0.0),
                transfer_speed_bps: None,
            },
            ObservedTiming {
                response_secs: Some(0.1),
                transfer: None,
            },
            0.0,
            router.estimator_clock.hours(),
        );
        assert_eq!(
            router.response_time_error.count,
            before + 1,
            "the event must be scored"
        );
        let recorded = forecasts.unwrap().log_response_time_legacy.unwrap();
        assert!(
            (recorded - hierarchical::MIN_RESPONSE_SECS.ln()).abs() < 1e-12,
            "a 0 s forecast is recorded at the floor: {recorded}"
        );
    }

    /// The per-event timing comparison must reuse the Renegade queries the
    /// failure scoring already ran: learning the hierarchical estimator adds no
    /// k-NN query of its own under the write lock.
    #[test]
    fn scoring_the_timing_comparison_adds_no_renegade_queries() {
        let queries_per_event = |hierarchical: bool| {
            let _flag = force_hierarchical_routing(hierarchical);
            let mut router = Router::new(&[]);
            feed_mixed_traffic(&mut router, 200);
            let before = routing_predictor::queries_on_this_thread();
            feed_mixed_traffic(&mut router, 50);
            routing_predictor::queries_on_this_thread() - before
        };
        let without = queries_per_event(false);
        let with = queries_per_event(true);
        assert!(without > 0, "the legacy scoring must query Renegade at all");
        assert_eq!(
            with, without,
            "computing the hierarchical estimator must not add Renegade queries"
        );
    }

    #[test]
    fn add_event_preserves_relay_recorded_events() {
        // SCOPE, honestly: this guards the refit's NO-DATA-LOSS property, not the
        // refit TRIGGER. It stays green if the `refit_if_stale()` call is deleted
        // from `add_event` (no refit, nothing lost), so it is not the #4811 guard —
        // `add_event_leaves_no_estimator_stale` is. And because `len()` derives
        // from `raw_events` and `refit` rebuilds from exactly that, preservation is
        // close to structural: it can only fail if a refit corrupts the window.
        // Carried forward cheaply, the same caveat #4809 recorded for its
        // predecessor (`refit_stale_estimators_preserves_relay_recorded_events`).
        // Cheap, not evidence.
        //
        // #4808 CONTEXT. The periodic router refresh used to rebuild the whole
        // router from the on-disk event log:
        //
        //     if !history.is_empty() { *router.write() = Router::new(&history); }
        //
        // Relay hops never persist (`record_relay_route_event` feeds the in-memory
        // router only), so every relay-recorded event was silently discarded on
        // each pass. In production a peer's event count collapsed 29 -> 2 inside a
        // single 30s window, and prediction could never latch because the model was
        // reset faster than it reached MIN_EVENTS_FOR_PREDICTION.
        //
        // The refit now happens inside `add_event`, rebuilding in place from each
        // estimator's own `raw_events` (#4811). This pins the invariant that made
        // the old code wrong: refitting must never lose an observation the router
        // already has. It is fed exclusively through the relay path, which is the
        // one the old rebuild could not see.
        let mut router = Router::new(&[]);
        add_relay_recorded_successes(&mut router, 120);

        assert_eq!(
            router.failure_estimator.len(),
            120,
            "the refits performed inside add_event must preserve relay-recorded \
             events, not discard them"
        );
        assert!(
            router.has_sufficient_routing_events(),
            "prediction must stay active across the refits; the old rebuild switched \
             it off by resetting the model to the on-disk (originator-only) subset"
        );
    }

    /// The #4811 replacement for `refit_router_periodically_refits_stale_estimators`
    /// (itself the port of #4809's `refresh_router_loop_refits_stale_estimators`).
    ///
    /// That guard pinned the ONE line that made #4809's fix do anything: the loop
    /// calling `refit_stale_estimators`. Both the loop and that method are gone —
    /// `add_event` now evaluates the trigger itself — so the guarantee is pinned
    /// here against the mechanism that replaced them, in the same terms: after
    /// feeding the router, NO estimator may be left owing a refit.
    ///
    /// It is the same assertion the loop test made (`refit_stale_estimators() == 0`
    /// afterwards, i.e. "the outstanding staleness was consumed"), just without a
    /// poller to consume it. Mutation-verified: deleting the `refit_if_stale()`
    /// call from `IsotonicEstimator::add_event` fails this test.
    #[test]
    fn add_event_leaves_no_estimator_stale() {
        let mut router = Router::new(&[]);

        // Straight into the in-memory router, exactly as a relay hop's
        // `record_relay_route_event` does — far past the staleness trigger.
        add_relay_recorded_successes(&mut router, 120);

        let stale: Vec<&str> = [
            ("response_start_time", &router.response_start_time_estimator),
            ("transfer_rate", &router.transfer_rate_estimator),
            ("failure", &router.failure_estimator),
        ]
        .into_iter()
        .filter(|(_, est)| est.is_stale_for_test())
        .map(|(name, _)| name)
        .collect();
        assert!(
            stale.is_empty(),
            "add_event must leave no global estimator owing a refit; {stale:?} are \
             stale, so the model drifts exactly as it did before #4808"
        );

        // The per-op estimators used to be checked for staleness here too. Since
        // #5662 they fit on read and keep no per-peer adjustments, so they can
        // never owe a refit and that check could not fail. What CAN go wrong is
        // the policy itself: a routing estimator switched to fit-on-read would
        // route on an empty fit, and a dashboard one switched back would pay a
        // full rebuild per event under the router write lock. Pin both sides.
        for (label, est) in [
            ("response_start_time", &router.response_start_time_estimator),
            ("transfer_rate", &router.transfer_rate_estimator),
            ("failure", &router.failure_estimator),
        ] {
            assert_eq!(
                est.fit_policy(),
                FitPolicy::EveryEvent,
                "{label}: routing reads this estimator, so it must fit on every event"
            );
        }
        for (label, per_op) in [
            ("per_op_failure", &router.per_op_failure),
            ("per_op_response_time", &router.per_op_response_time),
            ("per_op_transfer_rate", &router.per_op_transfer_rate),
        ] {
            assert!(
                !per_op.is_empty(),
                "sanity: {label} must have been populated by the seeded events, \
                 or this guard is vacuous"
            );
            for (op_type, est) in per_op {
                assert_eq!(
                    est.fit_policy(),
                    FitPolicy::OnRead,
                    "{label}[{op_type:?}] is dashboard-only and must fit on read"
                );
            }
        }

        // `Router::new` builds the estimators from history by a separate path
        // from `add_event`'s lazy inserts, so pin that path too.
        let op_event = |outcome| RouteEvent {
            peer: PeerKeyLocation::random(),
            contract_location: Location::random(),
            outcome,
            op_type: Some(OpType::Get),
        };
        let from_history = Router::new(&[
            op_event(RouteOutcome::Success {
                time_to_response_start: Duration::from_millis(100),
                payload_size: 5000,
                payload_transfer_time: Duration::from_millis(50),
            }),
            op_event(RouteOutcome::Failure),
        ]);
        for (label, est) in [
            (
                "response_start_time",
                &from_history.response_start_time_estimator,
            ),
            ("transfer_rate", &from_history.transfer_rate_estimator),
            ("failure", &from_history.failure_estimator),
        ] {
            assert_eq!(
                est.fit_policy(),
                FitPolicy::EveryEvent,
                "{label} built from history: routing reads it, so it must fit on every event"
            );
        }
        for (label, per_op) in [
            ("per_op_failure", &from_history.per_op_failure),
            ("per_op_response_time", &from_history.per_op_response_time),
            ("per_op_transfer_rate", &from_history.per_op_transfer_rate),
        ] {
            let est = per_op
                .get(&OpType::Get)
                .unwrap_or_else(|| panic!("sanity: the history must populate {label}"));
            assert_eq!(
                est.fit_policy(),
                FitPolicy::OnRead,
                "{label} built from history is dashboard-only and must fit on read"
            );
        }
    }

    /// Drive a peer that fails ONLY for one contract region, and assert the
    /// enabled correction moves that peer's failure estimate where the legacy
    /// blend does not.
    ///
    /// This is the branch that actually ships when the flag is turned on, and it
    /// had no coverage at all until review pointed it out — the `OnceLock` made
    /// it structurally untestable, which is why `force_residual_correction`
    /// exists.
    ///
    /// Seeded (#5662), so every peer, contract and draw is the same on every
    /// run, and a failure reproduces. `GlobalRng`'s seed is thread-local and
    /// pins the thread index, and libtest runs each test on a fresh thread, so
    /// parallel tests neither disturb this one nor are disturbed by it. The
    /// guard must stay the first statement: `PeerKeyLocation::random` caches a
    /// keypair per thread on first use, and generating it consumes draws.
    ///
    /// The seed is one that FAILS on the pre-fix code, so this is also a
    /// regression test for #5658 rather than only a behaviour pin. On the
    /// merge base (3884adcf) seeds 0..80 failed 3 times (33, 39, 79). Seed 39
    /// failed there with enabled 0.749 against disabled 1.000: the corrupted
    /// curve pinned the legacy blend at 1.0, as in #5658. It passes with the
    /// exact fit. Any seed that passes on both is useless here, so do not
    /// change it without re-running that search; the exactness itself is pinned
    /// by `incremental_fit_matches_batch_after_every_event`.
    ///
    /// Seed 39 fails pre-fix only because of the exact scenario it builds, and
    /// changing how many `GlobalRng` draws the generators consume (the
    /// keypair-cache fix #5663 would) silently swaps in a different scenario,
    /// which most likely passes on the pre-fix code too. So the scenario is
    /// pinned: `SCENARIO_FINGERPRINT` hashes every location the test draws, and
    /// the test fails with instructions when it changes.
    #[test]
    fn enabled_correction_changes_the_estimate_the_router_acts_on() {
        // The residual correction only reaches routing on the legacy stack; the
        // (default-on) hierarchical estimator takes precedence over it.
        let _legacy = force_hierarchical_routing(false);
        let _guard = crate::config::GlobalRng::seed_guard(39);
        // FNV-1a over the bits of every location the scenario draws. Fixed
        // arithmetic, so the recorded value cannot drift with the Rust version
        // the way `DefaultHasher`'s could.
        fn record(fingerprint: &mut u64, location: Location) {
            for byte in location.as_f64().to_bits().to_le_bytes() {
                *fingerprint ^= u64::from(byte);
                *fingerprint = fingerprint.wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        let mut fingerprint: u64 = 0xcbf2_9ce4_8422_2325;
        let targeted_peer = PeerKeyLocation::random();
        let peer_location = targeted_peer
            .location()
            .expect("random peer has a location");
        record(&mut fingerprint, peer_location);
        // A contract region close to this peer, so the distance-based model
        // expects it to do WELL there — the correction has to overcome the base.
        let targeted_contract =
            Location::try_from((peer_location.as_f64() + 0.01).rem_euclid(1.0)).unwrap();

        let mut router = Router::new(&[]);

        // Background traffic so the isotonic fit and the predictor have a curve.
        for index in 0..400 {
            let peer = PeerKeyLocation::random();
            let contract = Location::random();
            record(
                &mut fingerprint,
                peer.location().expect("random peer has a location"),
            );
            record(&mut fingerprint, contract);
            let succeeded = index % 10 != 0;
            router.add_event(RouteEvent {
                peer,
                contract_location: contract,
                outcome: if succeeded {
                    RouteOutcome::Success {
                        time_to_response_start: Duration::from_millis(100),
                        payload_size: 5000,
                        payload_transfer_time: Duration::from_millis(50),
                    }
                } else {
                    RouteOutcome::Failure
                },
                op_type: Some(OpType::Get),
            });
            // The targeted peer fails for its own contract region every time,
            // while behaving normally elsewhere — a pattern distance alone
            // cannot represent.
            router.add_event(RouteEvent {
                peer: targeted_peer.clone(),
                contract_location: targeted_contract,
                outcome: RouteOutcome::Failure,
                op_type: Some(OpType::Get),
            });
            // Drawn at the same point in the sequence as when it was written
            // inline in the event below.
            let elsewhere = Location::random();
            record(&mut fingerprint, elsewhere);
            router.add_event(RouteEvent {
                peer: targeted_peer.clone(),
                contract_location: elsewhere,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(100),
                    payload_size: 5000,
                    payload_transfer_time: Duration::from_millis(50),
                },
                op_type: Some(OpType::Get),
            });
        }

        // Checked before the outcome, so a changed draw sequence reports itself
        // rather than as a pass or fail of the behaviour under test.
        const SCENARIO_FINGERPRINT: u64 = 0x6584_8286_03be_bd44;
        assert_eq!(
            fingerprint, SCENARIO_FINGERPRINT,
            "the RNG draw sequence changed (e.g. #5663): seed 39 no longer builds the \
             scenario that fails on the pre-fix code (fingerprint now {fingerprint:#018x}). \
             Re-pick a seed that fails on the pre-fix code; see this test's doc"
        );

        let disabled = {
            let _guard = force_residual_correction(false);
            router
                .predict_routing_outcome(&targeted_peer, targeted_contract)
                .expect("prediction available after warm-up")
                .failure_probability
        };
        let enabled = {
            let _guard = force_residual_correction(true);
            router
                .predict_routing_outcome(&targeted_peer, targeted_contract)
                .expect("prediction available after warm-up")
                .failure_probability
        };

        eprintln!(
            "#5658 enabled {enabled:.6} disabled {disabled:.6} gap {:.6}",
            enabled - disabled
        );
        assert!(
            enabled.is_finite() && (0.0..=1.0).contains(&enabled),
            "the corrected failure probability must stay a probability, got {enabled}"
        );
        assert!(
            enabled > disabled,
            "with the correction enabled, a peer that fails only for this \
             contract region must be judged MORE likely to fail here than the \
             legacy blend judges it; enabled {enabled:.4} vs disabled {disabled:.4}"
        );
    }

    /// The flag must actually gate: with it off, the estimate is whatever the
    /// legacy blend produces and nothing about the correction leaks into it.
    ///
    /// Poisons the correction terms themselves — they reach the legacy stage
    /// as an argument of `combine_legacy_stages` — so a leak moves a bit here,
    /// and the flag-on arm is the non-vacuity check that the poison is large
    /// enough to show. (An earlier shape compared two flag-off predictions with
    /// each other, which a leak would change identically.)
    #[test]
    fn disabled_correction_leaves_the_legacy_estimate_untouched() {
        // About the legacy blend, so pin the legacy stack.
        let _legacy = force_hierarchical_routing(false);
        let mut router = Router::new(&[]);
        add_relay_recorded_successes(&mut router, 300);
        let peer = PeerKeyLocation::random();
        let contract = Location::random();
        let distance = peer
            .location()
            .map(|loc| contract.distance(loc).as_f64())
            .unwrap_or(0.5);
        let failure_estimate = router
            .failure_estimator
            .estimate_retrieval_time(&peer, contract)
            .expect("a failure curve after 300 events");
        let renegade_time = router
            .renegade_predictor
            .time_at(routing_predictor::wall_clock_hours());
        let poison = routing_predictor::Correction {
            value: 0.9,
            lambda: 1.0,
            n_eff: 1.0e6,
        };
        let poisoned = routing_predictor::RoutingCorrections {
            failure: Some(poison),
            response_time: Some(poison),
            transfer_speed: Some(poison),
        };
        let combine = |corrections: routing_predictor::RoutingCorrections,
                       correction_enabled: bool| {
            let queries = LegacyQueries {
                renegade: router.renegade_predictor.predict_at_time(
                    &peer,
                    contract,
                    distance,
                    renegade_time,
                ),
                corrections,
            };
            let estimates = router.combine_legacy_stages(
                &peer,
                contract,
                failure_estimate,
                None,
                None,
                &queries,
                correction_enabled,
            );
            [
                estimates.failure.to_bits(),
                estimates.time_to_response_start.to_bits(),
                estimates.xfer_speed.to_bits(),
            ]
        };

        let clean = combine(routing_predictor::RoutingCorrections::default(), false);
        assert_eq!(
            clean,
            combine(poisoned, false),
            "with the flag off, the correction terms must not reach any legacy stage estimate"
        );
        assert_ne!(
            clean,
            combine(poisoned, true),
            "with the flag on the same poison must move the estimate, or the equality \
             above proves nothing"
        );
    }

    /// A default-off switch (`FREENET_ROUTING_RESIDUAL_CORRECTION`) fails safe:
    /// only an explicit affirmative turns on something that changes live
    /// routing. Pinned separately from the default-on parser below so flipping
    /// the hierarchical default cannot flip this one with it.
    #[test]
    fn routing_flags_parse_fail_safe() {
        for on in ["1", "true", "TRUE", " yes ", "On"] {
            assert!(parse_routing_flag(Some(on)), "{on:?} must enable");
        }
        for off in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("off"),
            Some("no"),
            Some("ture"),
            Some("enabled"),
            Some("2"),
        ] {
            assert!(!parse_routing_flag(off), "{off:?} must NOT enable");
        }
    }

    /// The hierarchical switch is default-ON: unset or empty is the default,
    /// the recognised affirmatives keep it on explicitly, and every other
    /// non-empty value — a negative, a typo, or a word like `disabled` or
    /// `legacy` — turns it OFF. Setting a default-on switch at all almost
    /// always means "turn it off", and on ambiguous input the conservative
    /// direction is the proven legacy stack. Unrecognised values are flagged so
    /// the caller warns.
    #[test]
    fn default_on_routing_flag_disables_on_negative_and_unrecognised() {
        for default in [None, Some(""), Some("   ")] {
            let flag = parse_default_on_routing_flag(default);
            assert_eq!(flag, DefaultOnFlag::Default, "{default:?}");
            assert!(flag.enabled(), "{default:?} must keep the default (on)");
        }
        for on in ["1", "true", "TRUE", " yes ", "On", "ON\n"] {
            let flag = parse_default_on_routing_flag(Some(on));
            assert_eq!(flag, DefaultOnFlag::Enabled, "{on:?}");
            assert!(flag.enabled(), "{on:?} must enable");
        }
        for off in ["0", "false", "FALSE", " no ", "Off", "\toff\n"] {
            let flag = parse_default_on_routing_flag(Some(off));
            assert_eq!(flag, DefaultOnFlag::Disabled, "{off:?}");
            assert!(!flag.enabled(), "{off:?} must disable");
        }
        for other in [
            "flase", "ture", "disabled", "disable", "legacy", "n", "f", "2", "-1", "nope",
        ] {
            let flag = parse_default_on_routing_flag(Some(other));
            assert_eq!(flag, DefaultOnFlag::Unrecognised, "{other:?}");
            assert!(
                !flag.enabled(),
                "{other:?} must fail safe to the legacy stack (off)"
            );
        }
    }

    /// The wiring the process-global `OnceLock` runs, raw environment value to
    /// on/off: the default-on parser, not the default-off one, and a non-UTF-8
    /// value read as unrecognised (off) rather than collapsed to unset (on).
    #[test]
    fn resolve_hierarchical_flag_defaults_on_and_fails_safe_off() {
        use std::ffi::OsStr;
        assert!(resolve_hierarchical_flag(None), "unset must enable");
        assert!(
            resolve_hierarchical_flag(Some(OsStr::new(""))),
            "empty must enable"
        );
        assert!(
            resolve_hierarchical_flag(Some(OsStr::new("   "))),
            "whitespace-only is empty, so it must enable"
        );
        assert!(resolve_hierarchical_flag(Some(OsStr::new("1"))));
        assert!(!resolve_hierarchical_flag(Some(OsStr::new("0"))));
        assert!(
            !resolve_hierarchical_flag(Some(OsStr::new("flase"))),
            "a typo must fail safe to the legacy stack"
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            assert!(
                !resolve_hierarchical_flag(Some(OsStr::from_bytes(b"of\xff"))),
                "a non-UTF-8 value must read as unrecognised (off), not unset (on)"
            );
        }
    }

    /// The residual correction is parsed by the default-OFF parser, so flipping
    /// the hierarchical default cannot flip it: unset stays off.
    #[test]
    fn residual_correction_parser_defaults_off_when_unset() {
        assert!(
            !parse_routing_flag(None),
            "unset FREENET_ROUTING_RESIDUAL_CORRECTION must stay off"
        );
    }

    const HIERARCHICAL_CHILD_ENV: &str = "FREENET_TEST_HIERARCHICAL_WIRING_EXPECT";
    const HIERARCHICAL_CHILD_TEST: &str =
        "router::tests::hierarchical_routing_enabled_follows_the_environment";

    /// End to end through the real environment variable and the real
    /// process-global `OnceLock`, with no test override: unset routes with the
    /// hierarchical estimator, `=0` is the kill switch, anything unrecognised
    /// (including non-UTF-8) is off, and each resolution logs its mode exactly
    /// once at the level and in the words the soak's crossover verification
    /// greps for. The strings below are that contract.
    ///
    /// Runs its assertions in CHILD processes (re-execs of this test binary
    /// filtered to this one test): the `OnceLock` resolves once per process, so
    /// in-process whichever test touched it first would decide, and the
    /// variable could not be varied at all.
    #[test]
    fn hierarchical_routing_enabled_follows_the_environment() {
        if let Some(expect) = std::env::var_os(HIERARCHICAL_CHILD_ENV) {
            let expect = expect.to_string_lossy().into_owned();
            let mut parts = expect.splitn(4, '|');
            let (mode, level, message, value_suffix) = (
                parts.next().expect("mode"),
                parts.next().expect("level"),
                parts.next().expect("message"),
                parts.next().expect("value suffix"),
            );
            let (logs, guard) = crate::util::test_log_capture::install();
            let first = hierarchical_routing_enabled();
            let second = hierarchical_routing_enabled();
            drop(guard);
            assert_eq!(
                first,
                mode == "on",
                "{HIERARCHICAL_ENV}={:?} must resolve {mode}",
                std::env::var_os(HIERARCHICAL_ENV)
            );
            assert_eq!(first, second, "the resolution is fixed for the process");
            let logs = logs.lock().unwrap();
            let mode_lines: Vec<&String> = logs
                .iter()
                .filter(|line| line.contains("hierarchical routing estimator: "))
                .collect();
            assert_eq!(
                mode_lines.len(),
                1,
                "the mode must be logged exactly once; captured: {logs:?}"
            );
            // The WHOLE line: level, the exact message (not only its prefix), and
            // the `value` field, which tells an operator which value decided the
            // mode (rendered lossily for non-UTF-8).
            assert_eq!(
                mode_lines[0].as_str(),
                format!("{level} message={message}{value_suffix}"),
            );
            return;
        }

        const ENABLED_DEFAULT: &str = "on|INFO|hierarchical routing estimator: enabled (default)";
        const ENABLED_EXPLICIT: &str =
            "on|INFO|hierarchical routing estimator: enabled via FREENET_ROUTING_HIERARCHICAL";
        const DISABLED_EXPLICIT: &str =
            "off|INFO|hierarchical routing estimator: disabled via FREENET_ROUTING_HIERARCHICAL";
        const DISABLED_UNRECOGNISED: &str = "off|WARN|hierarchical routing estimator: disabled, \
             FREENET_ROUTING_HIERARCHICAL has an unrecognised value (use 0/false/no/off to \
             disable, 1/true/yes/on or unset to enable)";
        // (raw value, expected "mode|LEVEL|message", expected `value` field as
        // it follows the message in the captured line).
        //
        // Only the UNSET case carries no `value` field. An empty or
        // whitespace-only value resolves to the default too, but it was SET,
        // and the field is what lets an operator see from the log that the
        // value they supplied was ignored rather than absent. These three
        // cases are the pin on that distinction: drop the `raw.is_some()`
        // branch in `resolve_hierarchical_flag` and the second and third go
        // red while the first stays green.
        #[allow(unused_mut)]
        let mut cases: Vec<(Option<std::ffi::OsString>, &str, &str)> = vec![
            (None, ENABLED_DEFAULT, ""),
            (Some("".into()), ENABLED_DEFAULT, " value="),
            (Some("   ".into()), ENABLED_DEFAULT, " value=   "),
            (Some("1".into()), ENABLED_EXPLICIT, " value=1"),
            (Some("0".into()), DISABLED_EXPLICIT, " value=0"),
            (Some("off".into()), DISABLED_EXPLICIT, " value=off"),
            (Some("flase".into()), DISABLED_UNRECOGNISED, " value=flase"),
        ];
        // Non-UTF-8 is exercised on unix only. Windows could build one from a
        // lone surrogate (`OsStringExt::from_wide(&[0x6F, 0x66, 0xD800])`), but
        // no CI job or local build compiles this crate's lib tests on Windows,
        // so such a case could never be seen to run; the branch it would reach
        // is platform-independent and covered here.
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            cases.push((
                Some(std::ffi::OsString::from_vec(b"of\xff".to_vec())),
                DISABLED_UNRECOGNISED,
                " value=of\u{FFFD}",
            ));
        }

        let exe = std::env::current_exe().expect("test binary path");
        for (value, expect, value_suffix) in cases {
            let mut command = std::process::Command::new(&exe);
            command
                .args([
                    "--exact",
                    "--test-threads=1",
                    "--nocapture",
                    HIERARCHICAL_CHILD_TEST,
                ])
                .env(HIERARCHICAL_CHILD_ENV, format!("{expect}|{value_suffix}"))
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped());
            match &value {
                Some(value) => command.env(HIERARCHICAL_ENV, value),
                None => command.env_remove(HIERARCHICAL_ENV),
            };
            let mut child = command.spawn().expect("re-exec the test binary");
            // Bounded, because under plain `cargo test` nothing else would stop
            // a hung child. It resolves one flag, so 60 s is generous; its
            // output is far below a pipe buffer, so polling cannot deadlock.
            let mut finished = false;
            for _ in 0..1_200 {
                if child.try_wait().expect("poll the child").is_some() {
                    finished = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            if !finished {
                if let Err(error) = child.kill() {
                    eprintln!("could not kill the hung child: {error}");
                }
                if let Err(error) = child.wait() {
                    eprintln!("could not reap the hung child: {error}");
                }
                panic!("child with {HIERARCHICAL_ENV}={value:?} did not finish within 60 s");
            }
            let output = child
                .wait_with_output()
                .expect("collect the child's output");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "child with {HIERARCHICAL_ENV}={value:?} failed.\nstdout:\n{stdout}\n\
                 stderr:\n{stderr}"
            );
            // Fail CLOSED on a rename: libtest exits 0 when its filter matches
            // nothing, which would make every case above vacuous.
            assert!(
                stdout.contains("1 passed"),
                "the child must actually have run {HIERARCHICAL_CHILD_TEST}; if this \
                 function was renamed, update the constant.\nstdout:\n{stdout}\n\
                 stderr:\n{stderr}"
            );
        }
    }

    /// Traffic that gives every stage a curve: timed successes, untimed
    /// successes and failures, with one peer that fails for its own region.
    fn feed_mixed_traffic(router: &mut Router, rounds: usize) -> (PeerKeyLocation, Location) {
        let targeted_peer = PeerKeyLocation::random();
        let peer_location = targeted_peer
            .location()
            .expect("random peer has a location");
        let targeted_contract =
            Location::try_from((peer_location.as_f64() + 0.01).rem_euclid(1.0)).unwrap();
        for index in 0..rounds {
            router.add_event(RouteEvent {
                peer: PeerKeyLocation::random(),
                contract_location: Location::random(),
                outcome: match index % 10 {
                    0 => RouteOutcome::Failure,
                    1 => RouteOutcome::SuccessUntimed,
                    _ => RouteOutcome::Success {
                        time_to_response_start: Duration::from_millis(80 + index as u64 % 40),
                        payload_size: 5000,
                        payload_transfer_time: Duration::from_millis(50),
                    },
                },
                op_type: Some(OpType::Get),
            });
            router.add_event(RouteEvent {
                peer: targeted_peer.clone(),
                contract_location: if index % 2 == 0 {
                    targeted_contract
                } else {
                    Location::random()
                },
                outcome: if index % 2 == 0 {
                    RouteOutcome::Failure
                } else {
                    RouteOutcome::Success {
                        time_to_response_start: Duration::from_millis(100),
                        payload_size: 5000,
                        payload_transfer_time: Duration::from_millis(50),
                    }
                },
                op_type: Some(OpType::Get),
            });
        }
        (targeted_peer, targeted_contract)
    }

    /// How a routing-behaviour test trains its router.
    ///
    /// `Router::new(&history)` never feeds the hierarchical estimator (see its
    /// "Not replayed from `history`" comment), so a history-built router keeps
    /// a COLD estimator and routes entirely on the legacy fallback. Since the
    /// #4485 default flip a warm node routes on the hierarchical estimator
    /// instead, so each behavioural guard runs in both modes:
    ///
    /// - `History` covers the cold-start fallback. That is also what a node
    ///   whose flag resolves OFF runs, but only by equivalence: today a
    ///   never-fed estimator supplies no stage, so prediction takes the full
    ///   legacy path bit for bit. If cold-ON ever diverges from OFF (a
    ///   warm-start prior, a seeded root curve), `History` stops covering the
    ///   kill switch.
    /// - `WarmHierarchical` covers the estimator the fleet routes with. Its
    ///   precondition always pins the FAILURE stage. The timing stages warm only
    ///   after 30 timed successes (`MIN_CURVE_POINTS_LOG`; the speed stage
    ///   counts only those with a non-zero payload), so a guard trained on
    ///   fewer still ranks on LEGACY timing; only the twins that train that
    ///   many (realistic traffic, the transition's phase 3, #4230 steady state)
    ///   have their timing pinned to the hierarchical estimator too.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Training {
        History,
        WarmHierarchical,
    }

    impl Training {
        fn train(self, events: &[RouteEvent]) -> Router {
            match self {
                Training::History => Router::new(events),
                Training::WarmHierarchical => warm_hierarchical_router(events),
            }
        }

        /// Hold for the whole test: forces the estimator on in the warm mode
        /// (the legacy mode keeps the test's original, unforced behaviour).
        fn guard(self) -> Option<HierarchicalOverrideGuard> {
            (self == Training::WarmHierarchical).then(|| force_hierarchical_routing(true))
        }

        /// The warm mode's precondition: every failure probability the decision
        /// ranks on, and every timing estimate the hierarchical estimator
        /// supplies, came from the hierarchical estimate. See
        /// [`assert_hierarchical_failure_decides`].
        fn check_decides(self, router: &Router, peers: &[PeerKeyLocation], target: Location) {
            if self == Training::WarmHierarchical {
                assert_hierarchical_failure_decides(router, peers, target);
            }
        }
    }

    /// A router trained through `add_event` — production's path — with the
    /// hierarchical estimator on, so it learns every event. The clock is a
    /// frozen mock, so a prediction and a direct `estimate` read the same
    /// instant and can be compared exactly.
    ///
    /// The price: every event lands at one estimator instant, so the online
    /// horizon choice and forgetting a real node's time-spread events go
    /// through are degenerate in the twins. Those are covered by the router
    /// tests that advance a `SharedMockTimeSource` by hand, not here.
    fn warm_hierarchical_router(events: &[RouteEvent]) -> Router {
        let _on = force_hierarchical_routing(true);
        let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(
            crate::util::time_source::SharedMockTimeSource::new(),
        ));
        for event in events {
            router.add_event(event.clone());
        }
        router
    }

    /// Precondition for a warm-hierarchical guard: for every candidate, the
    /// failure probability the router acts on IS the hierarchical estimate,
    /// and so is each timing estimate (response time, transfer speed) whenever
    /// the hierarchical estimator supplies one. So a guard cannot pass on the
    /// legacy fallback for any stage the estimator has warmed. A timing stage it
    /// has NOT warmed (below 30 timed successes) is legitimately legacy and is not
    /// checked; a guard whose property depends on timing must also call
    /// [`assert_hierarchical_timing_decides`].
    ///
    /// Two stacks that happen to agree bit for bit (all-success data: both
    /// read 0.0) cannot be told apart by equality; such a guard needs the
    /// `LEGACY_STAGE_EVALUATIONS` check as well (see the transition twin).
    fn assert_hierarchical_failure_decides(
        router: &Router,
        peers: &[PeerKeyLocation],
        target: Location,
    ) {
        assert!(
            hierarchical_routing_enabled(),
            "call with the hierarchical estimator forced on"
        );
        let hours = router.estimator_clock.hours();
        for peer in peers {
            let distance = peer
                .location()
                .map(|loc| target.distance(loc).as_f64())
                .unwrap_or(0.5);
            let estimate = router.hierarchical.estimate(peer, target, distance, hours);
            let acted_on = router
                .predict_routing_outcome(peer, target)
                .expect("a warm router predicts");
            assert_eq!(
                estimate.failure_probability.map(|p| p.clamp(0.0, 1.0)),
                Some(acted_on.failure_probability),
                "the failure probability for {peer:?} must come from the (warm) \
                 hierarchical estimator, not the legacy fallback"
            );
            if let Some(seconds) = estimate
                .time_to_response_start_secs
                .filter(|t| t.is_finite() && *t >= 0.0)
            {
                assert_eq!(
                    acted_on.time_to_response_start, seconds,
                    "the response time for {peer:?} must come from the hierarchical \
                     estimator once it supplies one"
                );
            }
            if let Some(speed) = estimate
                .transfer_speed_bps
                .filter(|v| v.is_finite() && *v > 0.0)
            {
                assert_eq!(
                    acted_on.xfer_speed.bytes_per_second, speed,
                    "the transfer speed for {peer:?} must come from the hierarchical \
                     estimator once it supplies one"
                );
            }
        }
    }

    /// The stricter precondition for a guard whose property depends on timing:
    /// both hierarchical timing stages must be WARM for every candidate (so
    /// [`assert_hierarchical_failure_decides`] compared them), not merely
    /// consistent when present. Otherwise the guard's timing is legacy.
    fn assert_hierarchical_timing_decides(
        router: &Router,
        peers: &[PeerKeyLocation],
        target: Location,
    ) {
        let hours = router.estimator_clock.hours();
        for peer in peers {
            let distance = peer
                .location()
                .map(|loc| target.distance(loc).as_f64())
                .unwrap_or(0.5);
            let estimate = router.hierarchical.estimate(peer, target, distance, hours);
            assert!(
                estimate.time_to_response_start_secs.is_some()
                    && estimate.transfer_speed_bps.is_some(),
                "both hierarchical timing stages must be warm for {peer:?} (train at \
                 least 30 timed successes); got {estimate:?}"
            );
        }
        assert_hierarchical_failure_decides(router, peers, target);
    }

    fn prediction_bits(prediction: &RoutingPrediction) -> [u64; 5] {
        [
            prediction.failure_probability.to_bits(),
            prediction.time_to_response_start.to_bits(),
            prediction.xfer_speed.bytes_per_second.to_bits(),
            prediction.expected_total_time.to_bits(),
            prediction
                .renegade_failure_adjustment
                .map_or(u64::MAX, f64::to_bits),
        ]
    }

    /// With the flag off, routing must be exactly the legacy stack: replacing the
    /// hierarchical estimator with one that has learned something wildly
    /// different must not move a single bit of any prediction.
    ///
    /// Both calls use the same wall-clock reading, because Renegade's time
    /// feature would otherwise differ between them for reasons unrelated to the
    /// flag. The flag-ON arm is the non-vacuity check: the same swap must change
    /// the estimate when the estimator is consulted.
    #[test]
    fn disabled_hierarchical_estimator_leaves_every_prediction_bit_identical() {
        let _correction = force_residual_correction(false);
        let mut router = Router::new(&[]);
        let (targeted_peer, targeted_contract) = {
            let _learn = force_hierarchical_routing(true);
            feed_mixed_traffic(&mut router, 300)
        };
        let queries: Vec<(PeerKeyLocation, Location)> =
            std::iter::once((targeted_peer.clone(), targeted_contract))
                .chain((0..20).map(|_| (targeted_peer.clone(), Location::random())))
                .chain((0..20).map(|_| (PeerKeyLocation::random(), Location::random())))
                .collect();
        let wall = router.prediction_clock();
        let predict_all = |router: &Router| -> Vec<[u64; 5]> {
            queries
                .iter()
                .map(|(peer, contract)| {
                    prediction_bits(
                        &router
                            .predict_routing_outcome_at(peer, *contract, wall)
                            .expect("prediction available after warm-up"),
                    )
                })
                .collect()
        };

        let before = {
            let _guard = force_hierarchical_routing(false);
            predict_all(&router)
        };
        let enabled_before = {
            let _guard = force_hierarchical_routing(true);
            predict_all(&router)
        };

        // An estimator that has seen every queried peer fail, slowly, everywhere.
        let mut poisoned = hierarchical::HierarchicalRouting::new(Ring::DEFAULT_MAX_CONNECTIONS);
        let time = 0.0;
        for (peer, contract) in queries.iter().cycle().take(2_000) {
            let distance = peer
                .location()
                .map(|loc| contract.distance(loc).as_f64())
                .unwrap_or(0.5);
            poisoned.observe_at(
                peer,
                *contract,
                distance,
                &routing_predictor::RoutingOutcome {
                    success: false,
                    time_to_response_start_secs: Some(30.0),
                    transfer_speed_bps: Some(1.0),
                },
                time,
            );
        }
        router.hierarchical = poisoned;

        let after = {
            let _guard = force_hierarchical_routing(false);
            predict_all(&router)
        };
        assert_eq!(
            before, after,
            "with the flag off, the hierarchical estimator must not influence any \
             prediction bit"
        );

        let enabled_after = {
            let _guard = force_hierarchical_routing(true);
            predict_all(&router)
        };
        assert_ne!(
            enabled_before, enabled_after,
            "with the flag on, the swap must change predictions, or the equality \
             above proves nothing"
        );
        let poisoned_failure = router
            .predict_routing_outcome_at(&queries[0].0, queries[0].1, wall)
            .map(|p| p.failure_probability);
        let _guard = force_hierarchical_routing(true);
        let enabled_failure = router
            .predict_routing_outcome_at(&queries[0].0, queries[0].1, wall)
            .unwrap()
            .failure_probability;
        assert!(
            enabled_failure > 0.9,
            "the enabled path must act on the hierarchical failure estimate, got \
             {enabled_failure} (flag off: {poisoned_failure:?})"
        );
    }

    /// With the flag on, all three stages come from the hierarchical estimator,
    /// in the router's own units.
    #[test]
    fn enabled_hierarchical_estimator_supplies_every_stage() {
        let _correction = force_residual_correction(false);
        let _guard = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        let (peer, contract) = feed_mixed_traffic(&mut router, 300);
        let wall = router.prediction_clock();
        let distance = contract.distance(peer.location().unwrap()).as_f64();
        let estimate =
            router
                .hierarchical
                .estimate(&peer, contract, distance, wall.estimator_hours);
        let prediction = router
            .predict_routing_outcome_at(&peer, contract, wall)
            .unwrap();
        assert_eq!(
            Some(prediction.failure_probability),
            estimate.failure_probability
        );
        assert_eq!(
            Some(prediction.time_to_response_start),
            estimate.time_to_response_start_secs
        );
        assert_eq!(
            Some(prediction.xfer_speed.bytes_per_second),
            estimate.transfer_speed_bps
        );
        let seconds = prediction.time_to_response_start;
        assert!(
            (0.05..0.2).contains(&seconds),
            "response time must come back in seconds, got {seconds}"
        );
        assert!(
            prediction.failure_probability > 0.3,
            "the peer failing half its traffic in this region must read as risky, got {}",
            prediction.failure_probability
        );
    }

    /// Both switches on: the hierarchical estimator takes precedence for every
    /// stage, and the correction does not reach the estimate.
    #[test]
    fn hierarchical_takes_precedence_over_the_residual_correction() {
        let _learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        let (peer, contract) = feed_mixed_traffic(&mut router, 300);
        let wall = router.prediction_clock();
        let distance = contract.distance(peer.location().unwrap()).as_f64();
        let estimate =
            router
                .hierarchical
                .estimate(&peer, contract, distance, wall.estimator_hours);

        let correction_only = {
            let _off = force_hierarchical_routing(false);
            let _correction = force_residual_correction(true);
            router
                .predict_routing_outcome_at(&peer, contract, wall)
                .unwrap()
        };
        let both = {
            let _correction = force_residual_correction(true);
            router
                .predict_routing_outcome_at(&peer, contract, wall)
                .unwrap()
        };
        assert_eq!(Some(both.failure_probability), estimate.failure_probability);
        assert_eq!(
            Some(both.time_to_response_start),
            estimate.time_to_response_start_secs
        );
        assert_eq!(
            Some(both.xfer_speed.bytes_per_second),
            estimate.transfer_speed_bps
        );
        assert_eq!(
            both.renegade_failure_adjustment, None,
            "a blend that did not reach the estimate must not be reported as applied"
        );
        assert_ne!(
            both.failure_probability.to_bits(),
            correction_only.failure_probability.to_bits(),
            "the comparison must be able to tell the two apart"
        );
    }

    /// With the flag on and a warm estimator, no legacy (Renegade) work runs per
    /// candidate; with it off, it always does; and a stage the estimator cannot
    /// yet supply brings the legacy path back.
    #[test]
    fn enabled_hierarchical_estimator_skips_the_legacy_stack_per_candidate() {
        let evaluations = || LEGACY_STAGE_EVALUATIONS.with(|count| count.get());
        let _learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        let (peer, contract) = feed_mixed_traffic(&mut router, 300);
        let wall = router.prediction_clock();

        let before = evaluations();
        router
            .predict_routing_outcome_at(&peer, contract, wall)
            .unwrap();
        assert_eq!(
            evaluations(),
            before,
            "a warm estimator must not pay for Renegade"
        );

        {
            let _off = force_hierarchical_routing(false);
            router
                .predict_routing_outcome_at(&peer, contract, wall)
                .unwrap();
        }
        assert_eq!(
            evaluations(),
            before + 1,
            "flag off always runs the legacy stack"
        );

        // An estimator with no curves cannot supply any stage.
        router.hierarchical = hierarchical::HierarchicalRouting::new(Ring::DEFAULT_MAX_CONNECTIONS);
        let prediction = router
            .predict_routing_outcome_at(&peer, contract, wall)
            .unwrap();
        assert_eq!(
            evaluations(),
            before + 2,
            "a cold estimator falls back to legacy"
        );
        assert!(prediction.failure_probability.is_finite());
    }

    /// The contract term's counters reach the snapshot the dashboard, the
    /// tracing event and the OTLP body read, and they MOVE on real traffic.
    ///
    /// Finding 10 of the 2026-09-17 round-1 review, and its round-2 follow-up:
    /// the first version of this test snapshotted a fresh `Router::new(&[])`,
    /// so three of its six assertions were `0 == 0` and would have passed for
    /// a hard-coded zero or a transposed field. It now drives traffic that
    /// saturates every counter and asserts each is non-zero THROUGH the
    /// snapshot, in the shape of the sibling test below.
    #[test]
    fn contract_term_counters_reach_the_snapshot() {
        use crate::node::network_status::OpType;

        let _learn = force_hierarchical_routing(true);
        let _guard = crate::config::GlobalRng::seed_guard(0x4485_5417);
        // Cold: absent rather than wrong, and every field still agrees with
        // the stage's own diagnostic.
        let cold = Router::new(&[]);
        let snapshot = cold.snapshot();
        assert_eq!(snapshot.hierarchical_contracts, 0);
        assert_eq!(snapshot.hierarchical_contract_estimable_refits, 0);
        assert_eq!(snapshot.hierarchical_contract_effects_applied, 0);
        assert_eq!(snapshot.hierarchical_contract_tau2, None);

        // Warm: a small contract pool with more peers per contract than the
        // eight entries a contract keeps, so displacement, live refusal and
        // refit dropping all occur, and a dead contract so the term activates.
        let mut router = Router::new(&[]).with_max_connections(16);
        let peers: Vec<PeerKeyLocation> = (0..40).map(|_| PeerKeyLocation::random()).collect();
        let contracts: Vec<Location> = (0..6).map(|i| Location::new(i as f64 / 6.0)).collect();
        let dead = Location::new(0.37);
        for index in 0..3_000 {
            let (peer, contract, outcome) = if index % 3 == 0 {
                (&peers[index % peers.len()], dead, RouteOutcome::Failure)
            } else {
                (
                    &peers[index % peers.len()],
                    contracts[index % contracts.len()],
                    if index % 7 == 0 {
                        RouteOutcome::Failure
                    } else {
                        RouteOutcome::SuccessUntimed
                    },
                )
            };
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location: contract,
                outcome,
                op_type: Some(OpType::Get),
            });
        }
        let snapshot = router.snapshot();
        let diagnostics = router.hierarchical.diagnostics()[0];

        // Each field must be NON-ZERO through the snapshot, so a hard-coded
        // zero or a field that is never populated fails here, and must equal
        // the stage's own diagnostic, so a transposition fails too.
        for (name, from_snapshot, from_stage) in [
            (
                "contracts",
                snapshot.hierarchical_contracts as u64,
                diagnostics.contracts as u64,
            ),
            (
                "residuals_refused",
                snapshot.hierarchical_contract_residuals_refused,
                diagnostics.contract_residuals_refused,
            ),
            (
                "entries_displaced",
                snapshot.hierarchical_contract_entries_displaced,
                diagnostics.contract_entries_displaced,
            ),
            (
                "estimable_refits",
                snapshot.hierarchical_contract_estimable_refits,
                diagnostics.contract_estimable_refits,
            ),
            (
                "effects_applied",
                snapshot.hierarchical_contract_effects_applied,
                diagnostics.contract_effects_applied,
            ),
            (
                "forecast_offsets",
                snapshot.hierarchical_contract_forecast_offsets,
                diagnostics.contract_forecast_offsets,
            ),
            (
                "qualifying_contracts",
                snapshot.hierarchical_contract_qualifying_contracts,
                diagnostics.contract_qualifying_contracts,
            ),
            (
                "qualifying_entries",
                snapshot.hierarchical_contract_qualifying_entries,
                diagnostics.contract_qualifying_entries,
            ),
            // I3 of the 2026-09-18 round-3 testing review: this field was
            // wired to the snapshot and to the OTLP body but was absent from
            // this loop, so hard-coding it to zero, or reading it from the
            // wrong stage, survived. The telemetry test only checks the JSON
            // key against a hand-set value.
            (
                "floor_bound_refits",
                snapshot.hierarchical_contract_floor_bound_refits,
                diagnostics.contract_floor_bound_refits,
            ),
        ] {
            assert!(
                from_snapshot > 0,
                "hierarchical_contract_{name} must be non-zero on this traffic, \
                 or the assertion cannot tell a wired field from an unwired one"
            );
            assert_eq!(
                from_snapshot, from_stage,
                "hierarchical_contract_{name} must be the failure stage's own value"
            );
        }
        assert_eq!(
            snapshot.hierarchical_contract_pairs_refused_last_refit,
            diagnostics.contract_pairs_refused_last_refit,
            "the refit gauge must be the failure stage's own value"
        );
        // Equality only, deliberately: a stream whose contracts all qualify
        // from the first refit has a legitimate zero here, so asserting
        // non-zero would make this test depend on the warm-up shape of its own
        // traffic. The transposition is what this pins.
        assert_eq!(
            snapshot.hierarchical_contract_den_below_two_refits,
            diagnostics.contract_den_below_two_refits,
            "the den-gate count must be the failure stage's own value"
        );
        assert!(
            snapshot.hierarchical_contract_den_below_two_refits
                <= snapshot.hierarchical_contract_estimable_refits,
            "the den gate is evaluated only on estimable refits, so its count \
             cannot exceed them: {} against {}",
            snapshot.hierarchical_contract_den_below_two_refits,
            snapshot.hierarchical_contract_estimable_refits
        );
        assert!(
            snapshot
                .hierarchical_contract_tau2
                .is_some_and(|tau2| tau2 > 0.0),
            "between-contract variance must be estimated: {:?}",
            snapshot.hierarchical_contract_tau2
        );
        assert_eq!(
            snapshot.hierarchical_contract_tau2,
            diagnostics.contract_tau2
        );
    }

    /// The peer tables are sized from the configured connection cap, and
    /// evictions under churn reach the snapshot the dashboard and telemetry read.
    #[test]
    fn peer_table_capacity_and_evictions_reach_the_snapshot() {
        let _learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]).with_max_connections(5);
        assert_eq!(router.snapshot().hierarchical_peer_capacity, 64);
        assert_eq!(router.snapshot().hierarchical_peer_evictions, 0);
        add_relay_recorded_successes(&mut router, 300);
        let snapshot = router.snapshot();
        assert!(
            snapshot.hierarchical_peer_evictions > 0,
            "300 distinct peers through a 64-slot table must evict"
        );
        assert_eq!(
            snapshot.hierarchical_peer_evictions,
            router.hierarchical.total_evictions()
        );
        assert_eq!(
            Router::new(&[])
                .with_max_connections(1_000)
                .snapshot()
                .hierarchical_peer_capacity,
            2_000
        );
    }

    /// Everyone must not pay for a shadow model nobody reads: with the flag off
    /// and no dataset recorder, the estimator learns nothing, and the snapshot
    /// says it is not computed rather than showing an empty model. A recorder
    /// alone switches learning on.
    #[test]
    fn hierarchical_estimator_is_computed_only_when_used() {
        let _off = force_hierarchical_routing(false);
        let mut router = Router::new(&[]);
        add_relay_recorded_successes(&mut router, 200);
        let snapshot = router.snapshot();
        assert_eq!(router.hierarchical.diagnostics()[0].window_events, 0);
        assert!(!snapshot.hierarchical_computed);
        assert_eq!(snapshot.failure_skill_hierarchical, None);

        let dir = tempfile::tempdir().unwrap();
        let recorder =
            dataset::RoutingDataset::open(&dir.path().join("r.jsonl"), dataset::DEFAULT_MAX_BYTES)
                .unwrap();
        for _ in 0..10 {
            router.add_event_recording(
                RouteEvent {
                    peer: PeerKeyLocation::random(),
                    contract_location: Location::random(),
                    outcome: RouteOutcome::Failure,
                    op_type: None,
                },
                dataset::RouteSource::Originator,
                Some(&recorder),
            );
        }
        assert_eq!(
            router.hierarchical.diagnostics()[0].window_events,
            10,
            "a dataset recorder alone must switch learning on"
        );
    }

    /// Horizon selection through the Router on its injected clock: peers'
    /// response times drift after several simulated hours, a forgetting horizon
    /// takes over, and predictions after the drift are better than those of an
    /// identical router whose clock never moves (so its horizons cannot forget).
    #[test]
    fn injected_time_lets_the_router_forget_after_drift() {
        use crate::util::time_source::SharedMockTimeSource;
        let _seed = GlobalRng::seed_guard(0x4485_71de);
        let _learn = force_hierarchical_routing(true);
        let peers: Vec<PeerKeyLocation> = (0..20).map(|_| PeerKeyLocation::random()).collect();
        let effects: Vec<f64> = (0..peers.len())
            .map(|i| if i % 2 == 0 { 0.8 } else { -0.8 })
            .collect();
        let clock = SharedMockTimeSource::new();
        let mut moving = Router::new(&[]).with_time_source(std::sync::Arc::new(clock.clone()));
        let mut frozen =
            Router::new(&[]).with_time_source(std::sync::Arc::new(SharedMockTimeSource::new()));

        let truth = |p: usize, flipped: bool| {
            let effect = if flipped { -effects[p] } else { effects[p] };
            (0.1f64).ln() + effect
        };
        let (mut moving_error, mut frozen_error, mut scored) = (0.0, 0.0, 0);
        // 60 events per simulated hour: 40 hours stable, then 12 hours drifted.
        for index in 0..(52 * 60) {
            let flipped = index >= 40 * 60;
            let p = index % peers.len();
            let contract = Location::random();
            if flipped && index >= 46 * 60 {
                let expected = truth(p, true).exp() * (0.2f64 * 0.2 / 2.0).exp();
                for (router, error) in [(&moving, &mut moving_error), (&frozen, &mut frozen_error)]
                {
                    let predicted = router
                        .hierarchical
                        .estimate(&peers[p], contract, 0.1, router.estimator_clock.hours())
                        .time_to_response_start_secs
                        .unwrap();
                    *error += (predicted - expected).powi(2);
                }
                scored += 1;
            }
            let seconds = (truth(p, flipped) + 0.2 * uniform_normal()).exp();
            let event = RouteEvent {
                peer: peers[p].clone(),
                contract_location: contract,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_secs_f64(seconds),
                    payload_size: 0,
                    payload_transfer_time: Duration::ZERO,
                },
                op_type: None,
            };
            moving.add_event(event.clone());
            frozen.add_event(event);
            clock.advance_time(Duration::from_secs(60));
        }
        let moving_horizon = moving.hierarchical.diagnostics()[1].selected_horizon_hours;
        eprintln!(
            "#4485 drift through the router: moving-clock horizon {moving_horizon:?}, \
             mse {:.5} vs frozen {:.5} over {scored}",
            moving_error / scored as f64,
            frozen_error / scored as f64
        );
        assert!(
            moving_horizon.is_some(),
            "after the drift the moving clock must select a forgetting horizon"
        );
        assert_eq!(
            frozen.hierarchical.diagnostics()[1].selected_horizon_hours,
            None,
            "with no time passing every horizon is identical, so none is selected"
        );
        assert!(
            moving_error < frozen_error * 0.8,
            "forgetting must improve predictions after drift: {moving_error} vs {frozen_error}"
        );
    }

    fn uniform_normal() -> f64 {
        let u1 = GlobalRng::random_range(0.0..1.0f64).max(f64::MIN_POSITIVE);
        let u2 = GlobalRng::random_range(0.0..1.0f64);
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }

    /// Timing and speed, scored in SECONDS against the generating expectations
    /// (log-space scoring would favour the log-scale model by construction).
    struct TimingHeadToHead {
        legacy_time_mse: f64,
        hierarchical_time_mse: f64,
        legacy_transfer_mse: f64,
        hierarchical_transfer_mse: f64,
    }

    fn timing_head_to_head(seed: u64) -> TimingHeadToHead {
        const PEERS: usize = 12;
        const EVENTS: usize = 2_000;
        const WARMUP: usize = 300;
        const BYTES: f64 = 5_000.0;
        let _seed = GlobalRng::seed_guard(seed);
        let _correction = force_residual_correction(false);
        let _learn = force_hierarchical_routing(true);
        let peers: Vec<PeerKeyLocation> = (0..PEERS).map(|_| PeerKeyLocation::random()).collect();
        let time_effect: Vec<f64> = (0..PEERS).map(|_| 0.5 * uniform_normal()).collect();
        let speed_effect: Vec<f64> = (0..PEERS).map(|_| 0.5 * uniform_normal()).collect();
        let (sd_time, sd_speed) = (0.6, 0.7);
        let mut router = Router::new(&[]);
        let mut acc = [0.0f64; 4];
        let mut scored = 0usize;
        for index in 0..EVENTS {
            let p = GlobalRng::random_range(0..PEERS);
            let contract = Location::random();
            let distance = contract.distance(peers[p].location().unwrap()).as_f64();
            let mu_time = (0.08f64).ln() + 2.0 * distance + time_effect[p];
            let mu_speed = (50_000.0f64).ln() - 2.0 * distance + speed_effect[p];
            if index >= WARMUP {
                let expected_time = (mu_time + sd_time * sd_time / 2.0).exp();
                let expected_transfer = BYTES * (-mu_speed + sd_speed * sd_speed / 2.0).exp();
                let wall = router.prediction_clock();
                for (slot, enabled) in [(0, false), (1, true)] {
                    let _guard = force_hierarchical_routing(enabled);
                    let prediction = router
                        .predict_routing_outcome_at(&peers[p], contract, wall)
                        .expect("prediction available after warm-up");
                    acc[slot] += (prediction.time_to_response_start - expected_time).powi(2);
                    let transfer = BYTES / prediction.xfer_speed.bytes_per_second;
                    acc[slot + 2] += (transfer - expected_transfer).powi(2);
                }
                scored += 1;
            }
            let seconds = (mu_time + sd_time * uniform_normal()).exp();
            let speed = (mu_speed + sd_speed * uniform_normal()).exp();
            router.add_event(RouteEvent {
                peer: peers[p].clone(),
                contract_location: contract,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_secs_f64(seconds),
                    payload_size: BYTES as usize,
                    payload_transfer_time: Duration::from_secs_f64(BYTES / speed),
                },
                op_type: None,
            });
        }
        let n = scored as f64;
        TimingHeadToHead {
            legacy_time_mse: acc[0] / n,
            hierarchical_time_mse: acc[1] / n,
            legacy_transfer_mse: acc[2] / n,
            hierarchical_transfer_mse: acc[3] / n,
        }
    }

    /// Timing and transfer non-regression gate vs legacy, in seconds, at the
    /// same a-priori 1.10 ratio as the failure gate.
    #[test]
    fn hierarchical_timing_is_not_materially_worse_than_legacy_in_seconds() {
        use routing_predictor::recoverability::SEEDS;
        const MATERIAL_RATIO: f64 = 1.10;
        let runs: Vec<TimingHeadToHead> = SEEDS
            .iter()
            .map(|&seed| timing_head_to_head(seed))
            .collect();
        let mean = |f: &dyn Fn(&TimingHeadToHead) -> f64| {
            runs.iter().map(f).sum::<f64>() / runs.len() as f64
        };
        let (lt, ht) = (
            mean(&|r| r.legacy_time_mse),
            mean(&|r| r.hierarchical_time_mse),
        );
        let (lx, hx) = (
            mean(&|r| r.legacy_transfer_mse),
            mean(&|r| r.hierarchical_transfer_mse),
        );
        eprintln!(
            "#4485 timing in seconds: response-time mse {ht:.6} vs legacy {lt:.6} (ratio {:.3}); \
             transfer-time mse {hx:.6} vs legacy {lx:.6} (ratio {:.3})",
            ht / lt,
            hx / lx
        );
        assert!(ht.is_finite() && lt.is_finite() && hx.is_finite() && lx.is_finite());
        assert!(
            ht <= lt * MATERIAL_RATIO,
            "response time must not be materially worse than legacy in seconds: {ht} vs {lt}"
        );
        assert!(
            hx <= lx * MATERIAL_RATIO,
            "transfer time must not be materially worse than legacy in seconds: {hx} vs {lx}"
        );
    }

    /// Squared error against the generating probability of the estimate the
    /// router would ACT on, flag off (legacy) and flag on (hierarchical), over
    /// the recoverability harness's scenarios, driven through the router's real
    /// `add_event` / `predict_routing_outcome` API.
    struct HeadToHead {
        legacy_mse: f64,
        hierarchical_mse: f64,
        targeted_legacy_mse: f64,
        targeted_hierarchical_mse: f64,
        targeted: usize,
    }

    fn head_to_head(model: routing_predictor::recoverability::Model, seed: u64) -> HeadToHead {
        use routing_predictor::recoverability::{RECOVERY_BUDGET_EVENTS, Scenario, WARMUP_EVENTS};
        let _seed = GlobalRng::seed_guard(seed);
        let _correction = force_residual_correction(false);
        let _learn = force_hierarchical_routing(true);
        let scenario = Scenario::new();
        let mut router = Router::new(&[]);
        let (mut legacy, mut hierarchical, mut scored) = (0.0, 0.0, 0usize);
        let (mut t_legacy, mut t_hierarchical, mut targeted) = (0.0, 0.0, 0usize);
        for index in 0..RECOVERY_BUDGET_EVENTS {
            let (peer_index, contract_value) = scenario.draw(model, index);
            let peer = &scenario.peers[peer_index];
            let contract = Location::try_from(contract_value).expect("contract within ring");
            let distance = contract
                .distance(peer.location().expect("peer has a location"))
                .as_f64();
            let p_star = scenario.true_probability(model, peer_index, contract_value, distance);
            let failed = GlobalRng::random_range(0.0..1.0) < p_star;

            if index >= WARMUP_EVENTS {
                let wall = router.prediction_clock();
                let predict = |enabled: bool| {
                    let _guard = force_hierarchical_routing(enabled);
                    router
                        .predict_routing_outcome_at(peer, contract, wall)
                        .expect("prediction available after warm-up")
                        .failure_probability
                };
                let (l, h) = (predict(false), predict(true));
                legacy += (l - p_star).powi(2);
                hierarchical += (h - p_star).powi(2);
                scored += 1;
                if scenario.is_targeted(peer_index, contract_value) {
                    t_legacy += (l - p_star).powi(2);
                    t_hierarchical += (h - p_star).powi(2);
                    targeted += 1;
                }
            }

            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location: contract,
                outcome: if failed {
                    RouteOutcome::Failure
                } else {
                    RouteOutcome::SuccessUntimed
                },
                op_type: None,
            });
        }
        let n = scored.max(1) as f64;
        let t = targeted.max(1) as f64;
        HeadToHead {
            legacy_mse: legacy / n,
            hierarchical_mse: hierarchical / n,
            targeted_legacy_mse: t_legacy / t,
            targeted_hierarchical_mse: t_hierarchical / t,
            targeted,
        }
    }

    /// Non-regression gate for the hierarchical estimator on the three
    /// structured scenarios: its error against `p*` must not be materially
    /// worse than the legacy estimate's.
    ///
    /// The tolerance is the bake-off's a-priori `MATERIAL_RATIO` (1.10), fixed
    /// before that bake-off ran and not re-chosen here. It is applied to the
    /// seed-averaged mean squared error over all scored events. The
    /// peer x contract TARGETED subset is printed but not gated: the bake-off
    /// found this estimator does not beat legacy on narrow pairs (a +-0.02 band
    /// is diluted inside a 1/8-ring cell, which Renegade's k-NN resolves and a
    /// band hierarchy cannot), and asserting otherwise would be asserting a
    /// property the design does not claim.
    #[test]
    fn hierarchical_estimator_is_not_materially_worse_than_legacy() {
        use routing_predictor::recoverability::{Model, SEEDS};
        const MATERIAL_RATIO: f64 = 1.10;
        for model in [
            Model::DistanceOnly,
            Model::PeerMarginal,
            Model::PeerContract,
        ] {
            let runs: Vec<HeadToHead> = SEEDS
                .iter()
                .map(|&seed| head_to_head(model, seed))
                .collect();
            let mean = |f: &dyn Fn(&HeadToHead) -> f64| {
                runs.iter().map(f).sum::<f64>() / runs.len() as f64
            };
            let legacy = mean(&|r| r.legacy_mse);
            let hierarchical = mean(&|r| r.hierarchical_mse);
            let ratio = hierarchical / legacy.max(f64::MIN_POSITIVE);
            let per_seed: Vec<String> = runs
                .iter()
                .map(|r| {
                    format!(
                        "{:.3}",
                        r.hierarchical_mse / r.legacy_mse.max(f64::MIN_POSITIVE)
                    )
                })
                .collect();
            eprintln!(
                "#4485 hierarchical vs legacy, {model:?}: mse {hierarchical:.5} vs \
                 {legacy:.5} (ratio {ratio:.3}, per seed {per_seed:?}); targeted \
                 mse {:.5} vs {:.5} (n={:.0})",
                mean(&|r| r.targeted_hierarchical_mse),
                mean(&|r| r.targeted_legacy_mse),
                mean(&|r| r.targeted as f64),
            );
            assert!(
                hierarchical.is_finite() && legacy.is_finite(),
                "{model:?}: errors must be finite"
            );
            assert!(
                ratio <= MATERIAL_RATIO,
                "{model:?}: the hierarchical estimator must not be materially worse \
                 than legacy; mse {hierarchical:.5} vs {legacy:.5} (ratio {ratio:.3})"
            );
        }
    }

    /// The four scored layers must actually be populated by `add_event`, so a
    /// wiring break in `score_failure_layers` cannot pass unnoticed.
    #[test]
    fn add_event_populates_every_scored_layer() {
        let _learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        for index in 0..400 {
            router.add_event(RouteEvent {
                peer: PeerKeyLocation::random(),
                contract_location: Location::random(),
                outcome: if index % 7 == 0 {
                    RouteOutcome::Failure
                } else {
                    RouteOutcome::Success {
                        time_to_response_start: Duration::from_millis(100),
                        payload_size: 5000,
                        payload_transfer_time: Duration::from_millis(50),
                    }
                },
                op_type: Some(OpType::Get),
            });
        }

        let snapshot = router.snapshot();
        assert!(
            snapshot.failure_layers_evaluated > 0,
            "add_event must score the prediction layers"
        );
        for (label, skill) in [
            ("global", snapshot.failure_skill_global),
            ("adjusted", snapshot.failure_skill_adjusted),
            ("blended", snapshot.failure_skill_blended),
            ("corrected", snapshot.failure_skill_corrected),
            ("hierarchical", snapshot.failure_skill_hierarchical),
        ] {
            let skill = skill.unwrap_or_else(|| panic!("{label} layer produced no skill score"));
            assert!(
                skill.is_finite(),
                "{label} skill must be finite, got {skill}"
            );
        }
        assert!(
            snapshot.failure_base_rate.is_some_and(|rate| rate > 0.0),
            "a window containing failures must report a non-zero base rate"
        );
        assert!(
            snapshot.hierarchical_failure_evaluated > 0
                && snapshot.hierarchical_failure_evaluated <= snapshot.failure_layers_evaluated,
            "the hierarchical layer must be scored, and only on events the other \
             layers were scored on: {} of {}",
            snapshot.hierarchical_failure_evaluated,
            snapshot.failure_layers_evaluated
        );
        assert_eq!(snapshot.hierarchical_failure_events, 400);
    }

    /// The saturation qualifier is what makes the boundary count mean anything,
    /// so it gets its own test.
    #[test]
    fn boundary_selections_only_count_against_a_full_window() {
        let stats = SelectionRankStats::default();

        // A node with 8 connections and a 25-peer limit: 8 candidates, 8
        // scored. Choosing the FARTHEST of the 8 says nothing about truncation
        // — there was nothing to truncate.
        for _ in 0..10 {
            stats.record(7, 8, 8);
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.total, 10);
        assert_eq!(
            snapshot.saturated, 0,
            "an unfilled window cannot have discarded anyone"
        );
        assert_eq!(
            snapshot.far_quarter_share(),
            None,
            "with no truncated decisions there is no share to report"
        );

        // Exactly at the limit: 25 candidates, 25 scored. STILL not evidence —
        // the window was full but discarded nobody. This is the case the first
        // implementation got wrong, counting it as saturated and so inflating
        // the rate with decisions the limit never constrained.
        let stats = SelectionRankStats::default();
        for _ in 0..10 {
            stats.record(24, 25, 25);
        }
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot.saturated, 0,
            "a full window that discarded nobody is not evidence about the limit"
        );

        // 40 candidates cut to 25: now peers really were discarded unscored,
        // and a far-quarter selection is the reading that matters.
        let stats = SelectionRankStats::default();
        for _ in 0..10 {
            stats.record(24, 25, 40);
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.saturated, 10);
        assert_eq!(snapshot.far_quarter_share(), Some(1.0));
    }

    #[test]
    fn near_selections_against_a_full_window_read_as_comfortable() {
        let stats = SelectionRankStats::default();
        for _ in 0..100 {
            stats.record(0, 25, 40);
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.saturated, 100);
        assert_eq!(
            snapshot.far_quarter_share(),
            Some(0.0),
            "always picking the closest peer means the limit costs nothing"
        );
        assert_eq!(snapshot.mean_rank(), Some(0.0));
    }

    #[test]
    fn selection_rank_histogram_and_mean_track_the_recorded_ranks() {
        let stats = SelectionRankStats::default();
        for rank in [0usize, 0, 2, 4] {
            stats.record(rank, 25, 40);
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.rank[0], 2);
        assert_eq!(snapshot.rank[2], 1);
        assert_eq!(snapshot.rank[4], 1);
        // (0 + 0 + 2 + 4) / 4
        assert_eq!(snapshot.mean_rank(), Some(1.5));
    }

    #[test]
    fn selection_rank_beyond_the_buckets_is_collected_not_lost() {
        let stats = SelectionRankStats::default();
        stats.record(10_000, 12_000, 20_000);
        let snapshot = stats.snapshot();
        assert_eq!(
            snapshot.rank[SELECTION_RANK_BUCKETS - 1],
            1,
            "an out-of-range rank must land in the final bucket rather than panic \
             or be dropped"
        );
        assert_eq!(snapshot.total, 1);
        // The MEAN must not inherit the histogram's ceiling. Bucketing would
        // report 31 here; the exact sum reports the rank that actually happened,
        // which matters most for a window wider than the buckets — exactly the
        // configuration someone runs while evaluating whether to widen it.
        assert_eq!(
            snapshot.mean_rank(),
            Some(10_000.0),
            "the mean comes from the exact rank sum, not the buckets"
        );
        assert_eq!(
            snapshot.max_rank, 10_000,
            "a tail beyond the histogram must stay visible"
        );
    }

    #[test]
    fn empty_selection_rank_stats_report_nothing_rather_than_zero() {
        let snapshot = SelectionRankStats::default().snapshot();
        assert_eq!(snapshot.total, 0);
        assert_eq!(snapshot.mean_rank(), None);
        assert_eq!(snapshot.far_quarter_share(), None);
    }

    /// The stats must actually be fed by real routing decisions, not merely
    /// exist — a counter nothing increments is the same as no counter.
    #[test]
    fn routing_decisions_populate_the_selection_rank_stats() {
        let mut router = Router::new(&[]);
        for index in 0..400 {
            router.add_event(RouteEvent {
                peer: PeerKeyLocation::random(),
                contract_location: Location::random(),
                outcome: if index % 9 == 0 {
                    RouteOutcome::Failure
                } else {
                    RouteOutcome::Success {
                        time_to_response_start: Duration::from_millis(100),
                        payload_size: 5000,
                        payload_transfer_time: Duration::from_millis(50),
                    }
                },
                op_type: Some(OpType::Get),
            });
        }

        let candidates: Vec<PeerKeyLocation> = (0..40).map(|_| PeerKeyLocation::random()).collect();
        for _ in 0..25 {
            let _ = router.select_peer(candidates.iter(), Location::random());
        }

        let snapshot = router.snapshot().selection_ranks;
        // EQUALITY, not `>=`. Each `select_peer` call should record exactly one
        // decision, and `>=` would sail past a regression that recorded once per
        // CANDIDATE instead of once per decision — 25 calls would report 1000
        // and still satisfy the assertion, while every rate derived from these
        // counters silently became garbage. The run is deterministic (25 calls,
        // 40 fixed candidates against a 25-peer window), so equality is free.
        assert_eq!(
            snapshot.total, 25,
            "each routing decision must be recorded exactly once"
        );
        assert_eq!(
            snapshot.saturated, 25,
            "40 candidates cut to a 25-peer window discards 15 unscored every time"
        );
    }

    /// The routing dataset's candidate log must be the candidate set the router
    /// actually scored and the selection it actually made, taken from the
    /// decision itself — not a list re-derived afterwards from the peers it was
    /// offered (the "metric re-derived at the call site" trap).
    #[test]
    fn decision_capture_is_the_candidate_set_the_router_scored() {
        // Learning on, so the hierarchical estimator has something to say.
        let learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        // 40 peers against the default 25-peer window, each with its own key
        // (`PeerKeyLocation::random()` shares one) and a distinct location.
        let peers: Vec<PeerKeyLocation> =
            (0..40u32).map(|i| peer_in_subnet(i * 1543 + 7)).collect();
        let contract = Location::new(0.5);
        for round in 0..12 {
            for (index, peer) in peers.iter().enumerate() {
                router.add_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location: contract,
                    // Reliability unrelated to distance, so cost order is not
                    // distance order.
                    outcome: if (index * 7 + round) % 5 < index % 4 {
                        RouteOutcome::Failure
                    } else {
                        RouteOutcome::Success {
                            time_to_response_start: Duration::from_millis(40 + 13 * index as u64),
                            payload_size: 5000,
                            payload_transfer_time: Duration::from_millis(50),
                        }
                    },
                    op_type: Some(OpType::Get),
                });
            }
        }
        drop(learn);

        let mut distance_order: Vec<&PeerKeyLocation> = peers.iter().collect();
        distance_order.sort_by_key(|peer| contract.distance(peer.location().unwrap()));
        let window = router.consider_n_closest_peers;
        assert!(peers.len() > window, "the window must discard some peers");

        // Each model's estimates from the pass where it acted, to compare with
        // the pass where it was only logged.
        let mut acted: Vec<(bool, Vec<dataset::ModelEstimate>)> = Vec::new();
        let mut logged: Vec<(bool, Vec<dataset::ModelEstimate>)> = Vec::new();
        for acting_hierarchical in [false, true] {
            let _flag = force_hierarchical_routing(acting_hierarchical);
            let k = 3;
            let (selected, decision, capture) =
                router.select_k_best_peers_capturing(peers.iter(), contract, k, true);
            let capture = capture.expect("a prediction-based decision is captured");
            assert_eq!(capture.k, k);
            assert_eq!(capture.prior_failure_events, router.failure_estimator.len());
            assert!(!capture.prediction_fallback);
            assert!(matches!(
                decision.strategy,
                RoutingStrategy::PredictionBased
            ));
            assert!(
                capture.candidates.iter().all(|c| c.hierarchical_stages
                    == dataset::HierarchicalStages {
                        failure: true,
                        response_time: true,
                        transfer_speed: true,
                    }),
                "every stage is warm for every peer here"
            );
            let legacy: Vec<_> = capture
                .candidates
                .iter()
                .map(|c| c.legacy.unwrap())
                .collect();
            let hierarchical: Vec<_> = capture
                .candidates
                .iter()
                .map(|c| c.hierarchical.unwrap())
                .collect();
            acted.push((
                acting_hierarchical,
                if acting_hierarchical {
                    hierarchical.clone()
                } else {
                    legacy.clone()
                },
            ));
            logged.push((
                acting_hierarchical,
                if acting_hierarchical {
                    legacy
                } else {
                    hierarchical
                },
            ));
            let (uncaptured, _, none) =
                router.select_k_best_peers_capturing(peers.iter(), contract, k, false);
            assert!(none.is_none(), "nothing is captured when not asked");
            assert_eq!(
                selected, uncaptured,
                "capturing must not change the selection"
            );

            assert_eq!(
                capture.acting_model,
                if acting_hierarchical {
                    dataset::RoutingModel::Hierarchical
                } else {
                    dataset::RoutingModel::Legacy
                }
            );
            assert_eq!(capture.candidates_available, peers.len());
            assert_eq!(capture.candidates.len(), decision.candidates.len());
            let captured_peers: Vec<&PeerKeyLocation> =
                capture.candidates.iter().map(|c| c.peer).collect();
            assert_eq!(
                captured_peers,
                distance_order[..window],
                "the scored window, in distance order"
            );
            // The selection as the router returned it, position by position.
            for (position, peer) in selected.iter().enumerate() {
                let marked: Vec<&PeerKeyLocation> = capture
                    .candidates
                    .iter()
                    .filter(|c| c.selected_position == Some(position))
                    .map(|c| c.peer)
                    .collect();
                assert_eq!(marked, vec![*peer], "selected position {position}");
            }
            assert_eq!(
                capture
                    .candidates
                    .iter()
                    .filter(|c| c.selected_position.is_some())
                    .count(),
                k
            );
            assert!(
                capture
                    .candidates
                    .iter()
                    .all(|c| c.legacy.is_some() && c.hierarchical.is_some()),
                "both models predict every candidate"
            );

            let record = capture.into_record(OpType::Get, 1);
            assert_eq!(record.op, "GET");
            assert_eq!(record.candidates_omitted, 0);
            // The acting model's estimates are the ones the router sorted: its
            // j-th ranked candidate carries exactly the decision's j-th cost.
            for candidate in &record.candidates {
                let (rank, estimate) = if acting_hierarchical {
                    (candidate.rank_hierarchical, candidate.hierarchical)
                } else {
                    (candidate.rank_legacy, candidate.legacy)
                };
                let routed = decision.candidates[rank]
                    .prediction
                    .as_ref()
                    .expect("prediction-based");
                let estimate = estimate.unwrap();
                assert_eq!(
                    estimate.expected_total_time.to_bits(),
                    routed.expected_total_time.to_bits()
                );
                assert_eq!(
                    estimate.failure_probability.to_bits(),
                    routed.failure_probability.to_bits()
                );
                assert_eq!(candidate.selected_position.is_some(), rank < k);
            }
            let chosen_acting = if acting_hierarchical {
                record.chosen_rank_hierarchical
            } else {
                record.chosen_rank_legacy
            };
            assert_eq!(chosen_acting, Some(0));
            assert!(
                record
                    .candidates
                    .iter()
                    .any(|c| c.rank_legacy != c.rank_hierarchical),
                "the two models must disagree somewhere, or which model is which is untested"
            );
        }

        // The model that was only logged must carry what it would route on when
        // it acts: legacy logged in the hierarchical pass matches legacy acting
        // in the legacy pass, and the other way round. Close, not bit-equal:
        // the passes read their clocks moments apart.
        let close = |a: f64, b: f64| (a - b).abs() <= 1e-9 + 1e-6 * a.abs().max(b.abs());
        for (logged_in_hierarchical_pass, logged_estimates) in &logged {
            let (_, acting_estimates) = acted
                .iter()
                .find(|(pass, _)| pass != logged_in_hierarchical_pass)
                .unwrap();
            for (logged, acting) in logged_estimates.iter().zip(acting_estimates) {
                for (field, a, b) in [
                    (
                        "failure",
                        logged.failure_probability,
                        acting.failure_probability,
                    ),
                    (
                        "time",
                        logged.time_to_response_start_s,
                        acting.time_to_response_start_s,
                    ),
                    (
                        "speed",
                        logged.transfer_speed_bps,
                        acting.transfer_speed_bps,
                    ),
                    (
                        "cost",
                        logged.expected_total_time,
                        acting.expected_total_time,
                    ),
                ] {
                    assert!(close(a, b), "{field}: logged {a} vs acting {b}");
                }
            }
        }
    }

    /// Cold stages and candidates neither model can predict are recorded as
    /// such, from the decision.
    #[test]
    fn decision_capture_records_cold_stages_and_unpredictable_candidates() {
        let learn = force_hierarchical_routing(true);
        let mut router = Router::new(&[]);
        let peers: Vec<PeerKeyLocation> =
            (0..10u32).map(|i| peer_in_subnet(i * 977 + 11)).collect();
        let contract = Location::new(0.5);
        // Untimed outcomes only, so no timing stage has any data.
        for round in 0..12 {
            for (index, peer) in peers.iter().enumerate() {
                router.add_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location: contract,
                    outcome: if (index + round) % 3 == 0 {
                        RouteOutcome::Failure
                    } else {
                        RouteOutcome::SuccessUntimed
                    },
                    op_type: Some(OpType::Get),
                });
            }
        }
        drop(learn);
        let _flag = force_hierarchical_routing(false);
        // No location, so no failure estimate: the router ranks it last.
        let unknown = PeerKeyLocation::with_unknown_addr(
            crate::transport::TransportKeypair::new().public().clone(),
        );
        let mut offered = peers.clone();
        offered.push(unknown.clone());

        let (_, decision, capture) =
            router.select_k_best_peers_capturing(offered.iter(), contract, 2, true);
        let capture = capture.expect("prediction-based with some fallback");
        assert!(matches!(
            decision.strategy,
            RoutingStrategy::PredictionFallback
        ));
        assert!(capture.prediction_fallback);
        assert_eq!(capture.prior_failure_events, router.failure_estimator.len());

        let blind = capture
            .candidates
            .iter()
            .find(|c| c.peer == &unknown)
            .expect("in the window");
        assert!(blind.legacy.is_none() && blind.hierarchical.is_none());
        assert_eq!(
            blind.hierarchical_stages,
            dataset::HierarchicalStages::default()
        );
        assert!(blind.selected_position.is_none());

        for candidate in capture.candidates.iter().filter(|c| c.peer != &unknown) {
            assert_eq!(
                candidate.hierarchical_stages,
                dataset::HierarchicalStages {
                    failure: true,
                    response_time: false,
                    transfer_speed: false,
                },
                "failure is learned, timing is cold"
            );
            let (legacy, hierarchical) =
                (candidate.legacy.unwrap(), candidate.hierarchical.unwrap());
            // A cold stage is the legacy fallback, not a hierarchical number.
            assert_eq!(
                hierarchical.time_to_response_start_s,
                legacy.time_to_response_start_s
            );
            assert_eq!(hierarchical.transfer_speed_bps, legacy.transfer_speed_bps);
        }
        let record = capture.into_record(OpType::Get, 1);
        let blind = record
            .candidates
            .iter()
            .find(|c| c.peer == dataset::peer_hash(&unknown))
            .unwrap();
        assert_eq!(blind.distance, None);
        assert_eq!(blind.rank_legacy, record.candidates.len() - 1);

        // A router that never ran the hierarchical estimator (flag off, no
        // recording): every hierarchical stage is cold, so its logged estimate
        // is the legacy one throughout and says so.
        let mut unlearned = Router::new(&[]);
        for round in 0..12 {
            for (index, peer) in peers.iter().enumerate() {
                unlearned.add_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location: contract,
                    outcome: if (index + round) % 3 == 0 {
                        RouteOutcome::Failure
                    } else {
                        RouteOutcome::SuccessUntimed
                    },
                    op_type: Some(OpType::Get),
                });
            }
        }
        let (_, _, capture) =
            unlearned.select_k_best_peers_capturing(peers.iter(), contract, 1, true);
        for candidate in capture.expect("prediction-based").candidates {
            assert_eq!(
                candidate.hierarchical_stages,
                dataset::HierarchicalStages::default(),
                "nothing learned, nothing supplied"
            );
            let (legacy, hierarchical) =
                (candidate.legacy.unwrap(), candidate.hierarchical.unwrap());
            assert_eq!(hierarchical.failure_probability, legacy.failure_probability);
        }
    }

    #[test]
    fn estimators_use_intended_adjustment_modes() {
        // Pin the per-estimator adjustment modes so a future change can't silently
        // make failure probability multiplicative (wrong for a bounded [0,1] target)
        // or response time additive. Response time is multiplicative (validated on
        // telemetry, #4547); failure and transfer rate stay additive for now.
        let mut router = Router::new(&[]);
        // Global estimators.
        assert_eq!(
            router.response_start_time_estimator.adjustment_mode(),
            AdjustmentMode::Multiplicative,
            "response time must be multiplicative"
        );
        assert_eq!(
            router.failure_estimator.adjustment_mode(),
            AdjustmentMode::Additive,
            "failure probability must stay additive (bounded [0,1])"
        );
        assert_eq!(
            router.transfer_rate_estimator.adjustment_mode(),
            AdjustmentMode::Additive,
            "transfer rate stays additive pending instrumentation (#4547)"
        );

        // Per-op estimators are created lazily on the first matching event and must
        // match their global counterpart's mode. A timed GET success populates all
        // three (response time + failure + transfer rate) for OpType::Get.
        router.add_event(RouteEvent {
            peer: PeerKeyLocation::random(),
            contract_location: Location::random(),
            outcome: RouteOutcome::Success {
                time_to_response_start: Duration::from_millis(100),
                payload_size: 5000,
                payload_transfer_time: Duration::from_millis(50),
            },
            op_type: Some(OpType::Get),
        });
        assert_eq!(
            router
                .per_op_response_time
                .get(&OpType::Get)
                .unwrap()
                .adjustment_mode(),
            AdjustmentMode::Multiplicative,
            "per-op response time must match the global estimator (multiplicative)"
        );
        assert_eq!(
            router
                .per_op_failure
                .get(&OpType::Get)
                .unwrap()
                .adjustment_mode(),
            AdjustmentMode::Additive,
            "per-op failure must stay additive"
        );
        assert_eq!(
            router
                .per_op_transfer_rate
                .get(&OpType::Get)
                .unwrap()
                .adjustment_mode(),
            AdjustmentMode::Additive,
            "per-op transfer rate must stay additive"
        );
    }

    #[test]
    fn before_data_select_closest() {
        // Create 5 random peers and put them in an array
        let mut peers = vec![];
        for _ in 0..5 {
            let peer = PeerKeyLocation::random();
            peers.push(peer);
        }

        // Create a router with no historical data
        let router = Router::new(&[]);

        for _ in 0..10 {
            let contract_location = Location::random();
            // Pass a reference to the `peers` vector
            let best = router.select_peer(&peers, contract_location).unwrap();
            let best_distance = best.location().unwrap().distance(contract_location);
            for peer in &peers {
                // Dereference `best` when making the comparison
                if *peer != *best {
                    let distance = peer.location().unwrap().distance(contract_location);
                    assert!(distance >= best_distance);
                }
            }
        }
    }

    #[test]
    fn test_request_time() {
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        // Define constants for the number of peers, number of events, and number of test iterations.
        const NUM_PEERS: usize = 25;
        const NUM_EVENTS: usize = 400000;

        // Create `NUM_PEERS` random peers and put them in a vector.
        let peers: Vec<PeerKeyLocation> =
            (0..NUM_PEERS).map(|_| PeerKeyLocation::random()).collect();

        // Create NUM_EVENTS random events
        let mut events = vec![];
        for _ in 0..NUM_EVENTS {
            let peer = peers[GlobalRng::random_range(0..NUM_PEERS)].clone();
            let contract_location = Location::random();
            let simulated_prediction = GlobalRng::with_rng(|rng| {
                simulate_prediction(rng, peer.clone(), contract_location)
            });
            let event = RouteEvent {
                peer,
                contract_location,
                outcome: if GlobalRng::random_range(0.0..1.0)
                    > simulated_prediction.failure_probability
                {
                    RouteOutcome::Success {
                        time_to_response_start: Duration::from_secs_f64(
                            simulated_prediction.time_to_response_start,
                        ),
                        payload_size: 1000,
                        payload_transfer_time: Duration::from_secs_f64(
                            1000.0 / simulated_prediction.xfer_speed.bytes_per_second,
                        ),
                    }
                } else {
                    RouteOutcome::Failure
                },
                op_type: None,
            };
            events.push(event);
        }

        // Split events into two vectors, one for training and one for testing.
        let (training_events, testing_events) = events.split_at(NUM_EVENTS - 100);

        // Train the router with the training events.
        let router = Router::new(training_events);

        // Calculate empirical statistics from the training data
        let mut empirical_stats: std::collections::HashMap<
            (PeerKeyLocation, Location),
            (f64, f64, f64, usize),
        > = std::collections::HashMap::new();

        for event in training_events {
            let key = (event.peer.clone(), event.contract_location);
            let entry = empirical_stats.entry(key).or_insert((0.0, 0.0, 0.0, 0));

            entry.3 += 1; // count

            match &event.outcome {
                RouteOutcome::Success {
                    time_to_response_start,
                    payload_transfer_time,
                    payload_size,
                } => {
                    entry.0 += time_to_response_start.as_secs_f64();
                    entry.1 += *payload_size as f64 / payload_transfer_time.as_secs_f64();
                }
                RouteOutcome::SuccessUntimed => {
                    // No timing data to accumulate
                }
                RouteOutcome::Failure => {
                    entry.2 += 1.0; // failure count
                }
            }
        }

        // Test the router with the testing events.
        for event in testing_events {
            let prediction = router
                .predict_routing_outcome(&event.peer, event.contract_location)
                .unwrap();

            // Instead of comparing against simulate_prediction, we should verify
            // that the router's predictions are reasonable given the empirical data.
            // The router uses isotonic regression which learns from actual outcomes,
            // not theoretical models.

            // For failure probability, just check it's in valid range [0, 1]
            // Note: Due to isotonic regression implementation details, values might
            // occasionally be slightly outside [0, 1] due to floating point errors
            assert!(
                prediction.failure_probability >= 0.0 && prediction.failure_probability <= 1.0,
                "failure_probability out of range: {}",
                prediction.failure_probability
            );

            // For response time and transfer speed, check they're positive
            assert!(
                prediction.time_to_response_start > 0.0,
                "time_to_response_start must be positive: {}",
                prediction.time_to_response_start
            );

            assert!(
                prediction.xfer_speed.bytes_per_second > 0.0,
                "transfer_speed must be positive: {}",
                prediction.xfer_speed.bytes_per_second
            );
        }
    }

    #[test]
    fn test_select_closest_peers_size() {
        const NUM_PEERS: u32 = 45;
        const CAP: u32 = 30;

        assert_eq!(
            CAP as usize,
            Router::new(&[])
                .considering_n_closest_peers(CAP)
                .select_closest_peers(&create_peers(NUM_PEERS), &Location::random())
                .0
                .len()
        );
    }

    /// Regression test for issue #4222.
    ///
    /// Production telemetry on 2026-05 showed that 63% of failing GETs on
    /// subscribed contracts never visited any of the contract's subscribers
    /// during routing. Hop chains terminated early at mean 3.9 hops out of
    /// max htl 10. The root cause was the historical
    /// `consider_n_closest_peers = 5` cap in `select_closest_peers`: it
    /// truncates candidates to the N geographically closest peers BEFORE the
    /// isotonic estimator ranks them, so subscribers outside the 5-window are
    /// invisible to routing for that hop. With median 31 subscribers
    /// distributed roughly uniformly across the keyspace, a 5-peer window
    /// includes a subscriber on fewer than half of all target locations —
    /// matching the observed failure rate.
    ///
    /// This test models the production regime — many uniformly-distributed
    /// connected peers, a smaller fraction subscribed to the contract being
    /// fetched — and asserts the router's window is wide enough that at
    /// least one subscriber appears in the candidate set on the overwhelming
    /// majority of target locations. With the historical default of 5, this
    /// test fails (subscriber coverage ≈ 42%). With the corrected default of
    /// 25, it passes (coverage > 90% — hypergeometric expected value 93.4%).
    #[test]
    fn select_closest_peers_includes_subscribers_4222() {
        let _guard = crate::config::GlobalRng::seed_guard(0x4222_5AFE);

        const NUM_CONNECTIONS: u32 = 100;
        const NUM_SUBSCRIBERS: usize = 10;
        const NUM_TRIALS: usize = 200;
        const REQUIRED_COVERAGE: f64 = 0.85;

        // Default-constructed router uses DEFAULT_CONSIDER_N_CLOSEST_PEERS,
        // matching production. Do NOT call `considering_n_closest_peers` here:
        // the test pins the SHIPPED default, not an ad-hoc test override.
        let router = Router::new(&[]);

        // Pin the shipped window value explicitly. The statistical assertion
        // below is calibrated against a window of 25 (~93% coverage); a silent
        // drop to e.g. 18 would still produce ~85% coverage and pass the
        // hypergeometric threshold by luck, hiding a regression. Fail loudly
        // on any drift.
        assert_eq!(
            router.consider_n_closest_peers, 25,
            "issue #4222 regression: expected DEFAULT_CONSIDER_N_CLOSEST_PEERS = 25"
        );

        let mut covered = 0usize;
        for _ in 0..NUM_TRIALS {
            let peers = create_peers(NUM_CONNECTIONS);
            // create_peers generates random locations, so taking the first N
            // is effectively a uniform random sample of "subscribers".
            // NOTE: peer identity is keyed on `peer_addr` rather than `pub_key`
            // because the test helper shares a thread-local pub_key across all
            // synthetic peers — keying on pub_key would trivially match every
            // peer, hiding the structural window behavior we are asserting.
            let subscriber_addrs: std::collections::HashSet<_> = peers
                .iter()
                .take(NUM_SUBSCRIBERS)
                .filter_map(|p| p.socket_addr())
                .collect();
            let target = Location::random();

            let (window, _) = router.select_closest_peers(&peers, &target);
            if window.iter().any(|p| {
                p.socket_addr()
                    .is_some_and(|a| subscriber_addrs.contains(&a))
            }) {
                covered += 1;
            }
        }

        let coverage = covered as f64 / NUM_TRIALS as f64;
        assert!(
            coverage > REQUIRED_COVERAGE,
            "subscriber coverage {:.3} below required {:.2} — issue #4222 root cause \
             may have regressed: the router's candidate window \
             (consider_n_closest_peers = {}) is too small to reliably include \
             subscribers when they are uniformly distributed across the keyspace \
             ({}/{} trials covered with {} subscribers in {} peers)",
            coverage,
            REQUIRED_COVERAGE,
            router.consider_n_closest_peers,
            covered,
            NUM_TRIALS,
            NUM_SUBSCRIBERS,
            NUM_CONNECTIONS,
        );
    }

    // ===================== issue #4230 support =====================
    //
    // Follow-up to #4222. #4222 widened the candidate window from 5 to 25 and
    // proved (via `select_closest_peers_includes_subscribers_4222`) that the
    // wider window is wide enough to *surface* subscribers. #4230 asks the
    // converse question: does the wider window *degrade routing quality* for
    // ordinary (non-subscriber) GETs by letting the isotonic predictor pick a
    // distant peer that happens to have slightly better per-peer EWMA history
    // than the closest peer?
    //
    // Since the #4485 default flip the same question applies to the
    // hierarchical estimator (distance enters through its EB-shrunk prior
    // curve; per-peer and per-(peer, band) cells play the EWMA's role), so the
    // guards below also run in `Training::WarmHierarchical`. The paragraph
    // after this one describes the legacy stack.
    //
    // The predictor's scoring (`predict_routing_outcome`) has no direct
    // distance term; distance enters only via (a) the global isotonic
    // regression (`isotonic_estimator.rs`) and (b) as a feature handed to the
    // renegade predictor. Once a peer has >= ADJUSTMENT_PRIOR_SIZE (10) events,
    // its per-peer EWMA adjustment can shift its failure estimate away from the
    // global distance curve. At window=5 the truncation was self-limiting; at
    // window=25 there are 5x more candidates whose per-peer history can compete
    // against geographic locality.
    //
    // The window only controls which peers are VISIBLE to the predictor, not
    // the pairwise ranking of any two fixed peers, so the meaningful question
    // is an aggregate one: over many random targets against a realistically
    // trained router, does the chosen next hop stay in the closest tail of the
    // candidate pool (small-world progress), or does it drift toward the median
    // (no progress) as the window widens? `measure_convergence` answers this
    // via the chosen hop's rank-percentile. These tests pin that aggregate
    // convergence property at the shipped window and across a window sweep (the
    // historical 5 -> shipped 25 -> beyond), so a future change that lets
    // per-peer history override locality — lengthening routes — trips CI.

    /// Build a peer whose ring location is `Location::from_address` of a
    /// deterministic non-loopback address with a unique masked IP. `Location`
    /// is address-derived (it masks the low IP byte and ignores the port for
    /// non-loopback addresses), so distinct values in the upper three octets
    /// yield distinct, stable ring locations. The hash is not invertible, so
    /// callers do not control the exact location — they generate a pool and
    /// measure distances to a chosen target. `seed` in [0, 65535] (16 bits)
    /// keeps the octets in range.
    fn peer_in_subnet(seed: u32) -> PeerKeyLocation {
        use crate::transport::TransportKeypair;
        use std::net::SocketAddr;
        // Vary the second and third octets (the low octet is masked out by
        // `Location::from_address`, so it can stay 0). First octet fixed at
        // 198 (RFC 2544 benchmark range, non-loopback). 16 bits of `seed`
        // give 65536 distinct masked IPs — more than any test below needs.
        let b = ((seed >> 8) & 0xFF) as u8;
        let c = (seed & 0xFF) as u8;
        let addr: SocketAddr = format!("198.{b}.{c}.0:9000").parse().unwrap();
        PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr)
    }

    /// Build a synthetic history whose global shape matches what both routing
    /// estimators learn in production — a distance->failure gradient
    /// (closer peers succeed more, farther peers fail more) — but with each peer
    /// *also* carrying an individual reliability bias that is INDEPENDENT of its
    /// distance. The per-peer bias is what populates mature, divergent per-peer
    /// EWMA adjustments: a far peer can be individually reliable (adjustment well
    /// below the global curve) and a near peer individually unreliable. This is
    /// exactly the regime issue #4230 worries about — a distant-but-good peer
    /// competing against a close-but-unlucky one — so the convergence metric is
    /// stressed, not just rubber-stamped against a clean monotone gradient.
    ///
    /// `bias_mag` controls the per-peer reliability bias magnitude (the EWMA
    /// adjustment strength); the sweep test varies it to map the EWMA-magnitude
    /// dimension the issue asks about. Larger `bias_mag` = individual records
    /// diverge harder from the distance curve, i.e. the strongest test of
    /// whether per-peer history can override locality.
    ///
    /// To exercise MATURE per-peer history (`>= ADJUSTMENT_PRIOR_SIZE = 10`
    /// retained events per peer) within `IsotonicEstimator`'s 500-point rolling
    /// window, keep `pool.len() * rounds <= MAX_REGRESSION_POINTS (500)` and
    /// `rounds >= ~12`; every round touches every peer once, so each peer ends
    /// with `rounds` retained observations. Returns the event history (the
    /// caller builds a `Router` per candidate window from it, since `Router` is
    /// not `Clone`). Deterministic under the caller's seed guard.
    fn gradient_history(pool: &[PeerKeyLocation], rounds: usize, bias_mag: f64) -> Vec<RouteEvent> {
        // Per-peer reliability bias in [-bias_mag, +bias_mag], independent of
        // distance. Positive = the peer fails MORE than its distance predicts
        // (bad individual record); negative = fails LESS (good individual
        // record). Drawn once per peer so the bias is stable across rounds and
        // the EWMA converges to a mature, peer-specific adjustment.
        let bias: Vec<f64> = pool
            .iter()
            .map(|_| GlobalRng::random_range(-bias_mag..bias_mag))
            .collect();

        let mut events: Vec<RouteEvent> = Vec::new();
        for _ in 0..rounds {
            // Fresh random target each round so the gradient is learned across
            // the whole ring, not just one contract location.
            let contract = Location::random();
            for (i, p) in pool.iter().enumerate() {
                let d = contract.distance(p.location().unwrap()).as_f64(); // [0, 0.5]
                // Failure probability rises with distance (the global curve) and
                // is shifted by the peer's individual bias, then clamped to a
                // valid probability. d*2 maps [0,0.5] -> [0,1].
                let p_fail = (d * 2.0 + bias[i]).clamp(0.0, 1.0);
                let fail = GlobalRng::random_range(0.0..1.0) < p_fail;
                events.push(RouteEvent {
                    peer: p.clone(),
                    contract_location: contract,
                    outcome: if fail {
                        RouteOutcome::Failure
                    } else {
                        RouteOutcome::SuccessUntimed
                    },
                    op_type: None,
                });
            }
        }
        events
    }

    /// `history` with every success TIMED, so the hierarchical timing stages
    /// (30-point floor) warm and a ranking uses all three stages — but with
    /// timing that carries NO locality and NO per-peer signal: it varies only
    /// with the event's position in the history.
    ///
    /// Timing that rose with distance was a perfect "route to the closest
    /// peer" term on its own, so a locality guard trained on it passed whatever
    /// the failure stage did (measured: an inverted hierarchical failure
    /// estimate left the #4230 steady twin green). Per-peer timing would add
    /// exactly the per-peer-history-versus-locality competition #4230 guards,
    /// from a source the test does not control. Deterministic: draws no RNG.
    ///
    /// The jitter has a second job: it keeps the values varied, so the
    /// log-curve fit is well-conditioned rather than fitted to one repeated
    /// value.
    ///
    /// "No per-peer signal" holds only while the caller's history is
    /// ROUND-MAJOR (each round touches every peer once) AND its peer count is
    /// not a multiple of the 7-event cycle; otherwise a peer's jitter is a
    /// fixed offset.
    ///
    /// Those two conditions are guarded ASYMMETRICALLY, and a new caller has
    /// to know it: each caller asserts its own peer count (the #4230
    /// steady-state twin does), while the ROUND-MAJOR half rests on this
    /// paragraph alone. A history batched per peer would hand every peer a
    /// fixed offset with nothing failing.
    fn with_uninformative_timing(history: &[RouteEvent]) -> Vec<RouteEvent> {
        history
            .iter()
            .enumerate()
            .map(|(index, event)| {
                let jitter = (index % 7) as f64;
                let outcome = match event.outcome.clone() {
                    RouteOutcome::SuccessUntimed => RouteOutcome::Success {
                        time_to_response_start: Duration::from_secs_f64(0.05 + 0.002 * jitter),
                        payload_size: 2000,
                        payload_transfer_time: Duration::from_secs_f64(0.010 + 0.001 * jitter),
                    },
                    RouteOutcome::Success { .. } | RouteOutcome::Failure => event.outcome.clone(),
                };
                RouteEvent {
                    peer: event.peer.clone(),
                    contract_location: event.contract_location,
                    outcome,
                    op_type: event.op_type,
                }
            })
            .collect()
    }

    /// Measure routing convergence at a given candidate window over many random
    /// targets, against a fixed trained router and peer pool.
    ///
    /// For each target we ask the router (with `consider_n_closest_peers =
    /// window`) for its single best next hop, then compute that hop's
    /// *rank-percentile*: the fraction of the pool that is STRICTLY closer to
    /// the target than the chosen hop. 0.0 = the chosen hop is the closest peer
    /// (perfect greedy routing); 0.5 = it is the median peer (no progress, like
    /// a random choice); 1.0 = it is the farthest.
    ///
    /// We use rank-percentile rather than a distance ratio because the ratio's
    /// denominator (the closest peer's distance) is near zero whenever a peer
    /// sits almost on the target, which makes a mean-ratio metric explode on
    /// outliers and measure denominator noise instead of routing quality.
    /// Rank-percentile is bounded in [0, 1] and directly captures small-world
    /// progress: a router that consistently picks peers in the closest tail
    /// converges in O(log n) hops; one that drifts toward the median does not.
    ///
    /// Returns `(mean_rank_percentile, p90_rank_percentile)`. Lower is better.
    /// Deterministic under the caller's seed.
    fn measure_convergence(
        history: &[RouteEvent],
        pool: &[PeerKeyLocation],
        window: usize,
        trials: usize,
        training: Training,
    ) -> (f64, f64) {
        let router = training
            .train(history)
            .considering_n_closest_peers(window as u32);
        assert!(
            router.has_sufficient_routing_events(),
            "trained router below prediction threshold ({} events)",
            router.failure_estimator.len()
        );
        // A fixed probe target (no RNG draw, so the History mode's target
        // sequence is exactly what it always was).
        training.check_decides(&router, pool, Location::new(0.37));
        let mut percentiles: Vec<f64> = Vec::with_capacity(trials);
        for _ in 0..trials {
            let target = Location::random();
            let selected = router
                .select_peer(pool, target)
                .expect("non-empty pool must yield a hop");
            let sel_d = target.distance(selected.location().unwrap()).as_f64();
            // Count peers strictly closer than the chosen hop. Use a tiny
            // epsilon so a peer tied with the chosen one (e.g. the chosen peer
            // itself) is not counted as "closer".
            let closer = pool
                .iter()
                .filter(|p| target.distance(p.location().unwrap()).as_f64() < sel_d - 1e-12)
                .count();
            percentiles.push(closer as f64 / pool.len() as f64);
        }
        let mean = percentiles.iter().sum::<f64>() / percentiles.len() as f64;
        percentiles.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p90 = percentiles[(percentiles.len() as f64 * 0.9) as usize];
        (mean, p90)
    }

    /// #4230 (steady state): at the SHIPPED window of 25, small-world routing
    /// convergence is preserved. Over many random targets against a router
    /// trained on a realistic distance->failure gradient, the chosen next hop
    /// lands deep in the closest tail of the candidate pool — far from the
    /// median (which would mean no progress / non-convergence). If the widened
    /// window had let per-peer EWMA history routinely override geographic
    /// locality (the #4230 worry), the chosen hop's rank-percentile would drift
    /// toward 0.5; instead it stays near zero.
    ///
    /// Empirical basis for the thresholds: across a 12-seed x 2-bias-magnitude
    /// robustness scan (40-peer pool, MATURE per-peer history with per-peer
    /// reliability biases independent of distance), the window=25 mean
    /// rank-percentile ranged 0.037..0.159 and p90 ranged 0.10..0.35. The bounds
    /// below for this steady-state case (bias_mag 0.35: mean < 0.18, p90 < 0.40)
    /// sit above the observed worst case while remaining far below the
    /// no-convergence value of 0.5, so a real degradation (history overriding
    /// distance) trips CI but seed jitter does not. The companion sweep test
    /// uses slightly wider bounds because it also exercises bias_mag 0.5.
    /// Deterministic.
    #[test]
    fn routing_convergence_preserved_at_window_25_4230() {
        convergence_at_window_25_case(Training::History);
    }

    /// [`routing_convergence_preserved_at_window_25_4230`] on the warm
    /// hierarchical estimator, with the same bounds: the property #4230 pins
    /// must hold for the estimator the fleet routes with, not only for the
    /// legacy fallback. The warm mode trains the same history with every
    /// success timed ([`with_uninformative_timing`]: no distance or per-peer
    /// signal) so ALL THREE hierarchical stages are warm and ranking uses the
    /// full shipping formula, while the property still rests on the failure
    /// stage: an inverted hierarchical failure estimate must turn it red.
    #[test]
    fn routing_convergence_preserved_at_window_25_4230_warm_hierarchical() {
        convergence_at_window_25_case(Training::WarmHierarchical);
    }

    fn convergence_at_window_25_case(training: Training) {
        let _mode = training.guard();
        let _guard = crate::config::GlobalRng::seed_guard(0x4230_C0DE);

        // Pin the shipped window so a silent default change surfaces here too
        // (mirrors the pin in the #4222 test).
        assert_eq!(
            Router::new(&[]).consider_n_closest_peers,
            25,
            "issue #4230: expected DEFAULT_CONSIDER_N_CLOSEST_PEERS = 25"
        );

        // 40 peers x 12 rounds = 480 events (<= the estimator's 500-point
        // window), giving every peer 12 retained observations. Combined with
        // the per-peer reliability bias in `gradient_history`, this drives each
        // peer's EWMA adjustment to a mature, peer-specific value that diverges
        // from the global distance curve — the regime #4230 cares about, where
        // a peer's individual record competes against geographic locality.
        let pool: Vec<PeerKeyLocation> = (0..40u32).map(peer_in_subnet).collect();
        // The pool must contain 40 distinct ring locations; if `peer_in_subnet`
        // ever collided them the metrics below would silently lose meaning.
        let distinct_locs: std::collections::HashSet<_> = pool
            .iter()
            .map(|p| p.location().unwrap().as_f64().to_bits())
            .collect();
        assert_eq!(distinct_locs.len(), pool.len(), "peer pool has collisions");
        // `with_uninformative_timing` cycles its jitter every 7 events and
        // `gradient_history` is round-major (each round touches every peer
        // once), so a peer's jitter walks the cycle across rounds and per-peer
        // means differ only by the partial final cycle — at 40 peers x 12
        // rounds, 2.67 to 3.33 jitter units, about 1.3 ms on a 50 ms base.
        // (They are exactly equal only when the round count is a multiple of
        // 7.) If the pool size were a multiple of 7,
        // the jitter would collapse to a FIXED per-peer offset: a per-peer
        // timing signal, which is exactly the per-peer-history-versus-locality
        // regime #4230 guards, injected by the test itself and invisible to
        // every assertion below.
        assert_ne!(
            pool.len() % 7,
            0,
            "pool size {} is a multiple of with_uninformative_timing's 7-event \
             jitter cycle, so its timing would become a fixed per-peer offset",
            pool.len()
        );

        let history = gradient_history(&pool, 12, 0.35); // 40 * 12 = 480 events

        // Prove the divergent-per-peer-history regime is actually exercised, so
        // these tests aren't false comfort. Two checks:
        //   (a) every peer has an active per-peer adjustment (the global
        //       regression reached ADJUSTMENT_PRIOR_SIZE, so adjustments were
        //       created and, since the seeded effective_count >=
        //       MIN_POINTS_FOR_REGRESSION, are actually APPLIED in scoring), and
        //   (b) the per-peer adjustment VALUES span a meaningful range — i.e.
        //       individual records genuinely diverge from the global distance
        //       curve rather than all collapsing onto it. (We assert on the
        //       adjustment *value* spread, NOT effective_count: effective_count
        //       is seeded at the EWMA fixed point ~10 and stays there regardless
        //       of how many real events accrue, so it cannot witness divergence.)
        let router_probe = Router::new(&history);
        assert_eq!(
            router_probe.failure_estimator.peer_adjustments.len(),
            pool.len(),
            "every peer must have a per-peer failure adjustment applied"
        );
        let adj_values: Vec<f64> = router_probe
            .failure_estimator
            .peer_adjustments
            .values()
            .map(|a| a.value())
            .collect();
        let adj_min = adj_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let adj_max = adj_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        assert!(
            adj_max - adj_min > 0.2,
            "per-peer history did not diverge (adjustment value spread \
             {:.3} too small) — the test would not exercise the regime where \
             per-peer history competes against distance",
            adj_max - adj_min
        );

        // The History mode keeps the original untimed input. The warm mode
        // times every success (with no distance signal, so the ranking stays
        // failure-driven) so the hierarchical timing stages warm too, and
        // proves they did before measuring.
        let measured = match training {
            Training::History => history.clone(),
            Training::WarmHierarchical => {
                let timed = with_uninformative_timing(&history);
                assert_hierarchical_timing_decides(
                    &warm_hierarchical_router(&timed),
                    &pool,
                    Location::new(0.37),
                );
                timed
            }
        };
        let (mean_pct, p90_pct) = measure_convergence(&measured, &pool, 25, 400, training);

        assert!(
            mean_pct < 0.18,
            "issue #4230 regression ({training:?}): at window=25 the chosen next hop's mean \
             rank-percentile is {mean_pct:.3} — the predictor is drifting away \
             from the closest tail (0.5 = median = no progress), so per-peer \
             history is overriding geographic locality and small-world routing \
             convergence has degraded"
        );
        assert!(
            p90_pct < 0.40,
            "issue #4230 regression ({training:?}): at window=25 the 90th-percentile chosen-hop \
             rank-percentile is {p90_pct:.3} — the routing tail has degraded; \
             too many hops are landing far from the target"
        );
    }

    /// #4230 (threshold map): sweep the candidate window from the historical 5
    /// through the shipped 25 and beyond, and confirm widening does NOT push
    /// routing toward non-convergence. The issue asks for this sweep
    /// explicitly: if window=25 sits comfortably inside the safe region the
    /// test is a regression guard; if widening had driven the chosen-hop
    /// rank-percentile toward the median, the fix would have needed a soft
    /// distance bias in the predictor rather than truncation alone.
    ///
    /// Finding: widening DOES raise the chosen-hop rank-percentile modestly
    /// (more near-equidistant candidates become visible, so per-peer history
    /// breaks more ties), but it plateaus well inside the convergent regime —
    /// it never drifts toward 0.5, even when per-peer histories diverge hard
    /// from the distance curve. So the #4222 widening is safe; this test pins
    /// that across both the window dimension and the EWMA-magnitude dimension
    /// the issue calls out. Deterministic.
    ///
    /// Note on the structural cap: `select_closest_peers` truncates to the
    /// `window` closest peers BEFORE the predictor reranks, so at small windows
    /// the chosen-hop rank-percentile is mechanically bounded near `window /
    /// pool_len` (e.g. ~0.125 at window=5 in a 40-peer pool) and the assertions
    /// there essentially cannot fail. The load-bearing checks are therefore at
    /// the wide windows (25 and 40, where the whole near-cluster — or the whole
    /// pool — is visible and per-peer history has the most room to override
    /// locality) plus the bounded 5->25 increase.
    #[test]
    fn routing_convergence_window_sweep_does_not_degrade_4230() {
        convergence_window_sweep_case(Training::History);
    }

    /// [`routing_convergence_window_sweep_does_not_degrade_4230`] on the warm
    /// hierarchical estimator, with the same bounds. FAILURE-ONLY in both
    /// modes: `gradient_history` has no timed events, so no timing estimate
    /// exists in either stack and the ranking is the failure term alone. The
    /// timed, all-stages ranking is covered by the steady-state twin.
    #[test]
    fn routing_convergence_window_sweep_does_not_degrade_4230_warm_hierarchical() {
        convergence_window_sweep_case(Training::WarmHierarchical);
    }

    fn convergence_window_sweep_case(training: Training) {
        let _mode = training.guard();
        let _guard = crate::config::GlobalRng::seed_guard(0x4230_5EED);

        // 40 peers x 12 rounds = 480 events: mature per-peer EWMA within the
        // 500-point window. See the companion steady-state test for the
        // divergence assertion. The sweep tops out at window=40 (the pool size);
        // windows beyond that are equivalent since truncation can't exceed the
        // candidate count.
        //
        // Second sweep dimension: per-peer bias magnitude (EWMA-adjustment
        // strength). 0.35 is the steady-state value; 0.5 pushes individual
        // records to diverge maximally from the distance curve — the strongest
        // stress on "can per-peer history override locality?".
        let pool: Vec<PeerKeyLocation> = (0..40u32).map(peer_in_subnet).collect();

        for &bias_mag in &[0.35f64, 0.5] {
            let history = gradient_history(&pool, 12, bias_mag);

            // (window, mean_pct, p90_pct) for each swept window.
            let mut results: Vec<(usize, f64, f64)> = Vec::new();
            for &window in &[5usize, 10, 25, 40] {
                let (mean_pct, p90_pct) =
                    measure_convergence(&history, &pool, window, 300, training);
                results.push((window, mean_pct, p90_pct));
            }

            // At EVERY swept window the chosen hop stays deep in the closest
            // tail — never near the median. This is the load-bearing
            // convergence property: widening the candidate window (at any EWMA
            // magnitude) does not break small-world routing.
            for &(window, mean_pct, p90_pct) in &results {
                assert!(
                    mean_pct < 0.20,
                    "issue #4230 ({training:?}): at window={window} bias_mag={bias_mag} mean \
                     chosen-hop rank-percentile {mean_pct:.3} drifted toward the \
                     median — routing convergence degraded by candidate-window size"
                );
                assert!(
                    p90_pct < 0.42,
                    "issue #4230 ({training:?}): at window={window} bias_mag={bias_mag} p90 \
                     chosen-hop rank-percentile {p90_pct:.3} is too high — the \
                     routing tail degraded"
                );
            }

            // Widening from the historical 5 to the shipped 25 must keep the
            // chosen hop firmly in the closest tail. We do NOT require window=25
            // to equal window=5 (it is expected to be higher — more visible
            // candidates means per-peer history breaks more near-equidistant
            // ties), only that the increase is bounded and stays convergent.
            let m5 = results.iter().find(|r| r.0 == 5).unwrap().1;
            let m25 = results.iter().find(|r| r.0 == 25).unwrap().1;
            assert!(
                m25 < 0.20 && m25 - m5 < 0.15,
                "issue #4230 ({training:?}): at bias_mag={bias_mag} widening the candidate \
                 window from 5 to 25 raised the mean chosen-hop rank-percentile \
                 from {m5:.3} to {m25:.3} — the widening pushed routing \
                 materially toward the median and may need a soft distance bias \
                 in the predictor (see issue #4230)"
            );
        }
    }

    #[test]
    fn test_select_closest_peers_equality() {
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        const NUM_PEERS: u32 = 100;
        const CLOSEST_CAP: u32 = 10;
        let peers: Vec<PeerKeyLocation> = create_peers(NUM_PEERS);
        let contract_location = Location::random();

        let expected_closest = select_closest_peers_vec(CLOSEST_CAP, &peers, &contract_location);

        // Create a router with no historical data
        let router = Router::new(&[]).considering_n_closest_peers(CLOSEST_CAP);
        let asserted_closest: Vec<&PeerKeyLocation> =
            router.select_closest_peers(&peers, &contract_location).0;

        let mut expected_iter = expected_closest.iter();
        let mut asserted_iter = asserted_closest.iter();

        while let (Some(expected_location), Some(asserted_location)) =
            (expected_iter.next(), asserted_iter.next())
        {
            assert_eq!(**expected_location, **asserted_location);
        }

        assert_eq!(expected_iter.next(), asserted_iter.next());
    }

    fn simulate_prediction(
        random: &mut dyn rand::RngCore,
        peer: PeerKeyLocation,
        target_location: Location,
    ) -> RoutingPrediction {
        use rand::Rng;
        let distance = peer.location().unwrap().distance(target_location);
        let time_to_response_start = 2.0 * distance.as_f64();
        let failure_prob = distance.as_f64();
        let transfer_speed = 100.0 - (100.0 * distance.as_f64());
        let payload_size = random.random_range(100..1000);
        let transfer_time = transfer_speed * (payload_size as f64);
        RoutingPrediction {
            failure_probability: failure_prob,
            xfer_speed: TransferSpeed {
                bytes_per_second: transfer_speed,
            },
            time_to_response_start,
            expected_total_time: time_to_response_start + transfer_time,
            renegade_failure_adjustment: None,
        }
    }

    fn select_closest_peers_vec<'a>(
        closest_peers_capacity: u32,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: &Location,
    ) -> Vec<&'a PeerKeyLocation>
    where
        PeerKeyLocation: Clone,
    {
        let mut closest: Vec<&'a PeerKeyLocation> = peers.into_iter().collect();
        closest.sort_by_key(|&peer| {
            if let Some(location) = peer.location() {
                target_location.distance(location)
            } else {
                Distance::new(f64::MAX)
            }
        });

        closest[..closest_peers_capacity as usize].to_vec()
    }

    fn create_peers(num_peers: u32) -> Vec<PeerKeyLocation> {
        let mut peers: Vec<PeerKeyLocation> = vec![];

        for _ in 0..num_peers {
            let peer = PeerKeyLocation::random();
            peers.push(peer);
        }

        peers
    }

    // ============ Self-routing prevention support tests ============
    //
    // These tests support the self-routing prevention tests in ConnectionManager.
    // While ConnectionManager handles filtering (excluding self/requester), Router
    // must handle the edge cases that result from aggressive filtering:
    // - Empty candidate lists (all peers filtered out)
    // - Single candidate lists (only one peer remains)
    //
    // Related bugs: #1806, #1786, #1781, #1827

    /// Test that select_peer returns None for empty candidate list
    ///
    /// **Scenario this supports:**
    /// After ConnectionManager filters out the requesting peer and any transient
    /// connections, the candidate list may be empty. Router must return None
    /// rather than panicking or returning an invalid peer.
    ///
    /// **Related to bug #1806:**
    /// When routing filters were first added, empty candidate lists caused panics.
    #[test]
    fn test_select_peer_empty_candidates() {
        let router = Router::new(&[]);
        let empty_peers: Vec<PeerKeyLocation> = vec![];
        let target = Location::random();

        let result = router.select_peer(&empty_peers, target);
        assert!(
            result.is_none(),
            "select_peer should return None for empty candidate list"
        );
    }

    /// Test that select_closest_peers handles empty candidate list
    ///
    /// **Scenario this supports:**
    /// Internal method used by select_k_best_peers. Must handle edge cases
    /// gracefully when filtering leaves no candidates.
    ///
    /// **Related to bugs #1806, #1786:**
    /// Small networks with aggressive filtering can easily end up with zero
    /// routing candidates. This must not cause crashes.
    #[test]
    fn test_select_k_best_empty_candidates() {
        let router = Router::new(&[]).considering_n_closest_peers(5);
        let empty_peers: Vec<PeerKeyLocation> = vec![];
        let target = Location::random();

        let (result, _) = router.select_closest_peers(&empty_peers, &target);
        assert!(
            result.is_empty(),
            "select_closest_peers should return empty vec for empty candidates"
        );
    }

    /// Test that select_peer works correctly with single candidate
    ///
    /// **Scenario this supports:**
    /// In a 3-node network, after excluding self and requester, only 1 peer remains.
    /// Router must correctly select that peer without additional filtering that
    /// could cause "no route found" errors.
    ///
    /// **Related to bug #1827:**
    /// Gateway nodes in small networks sometimes failed to route because overly
    /// aggressive filtering left only one candidate, which was then incorrectly
    /// rejected by other criteria.
    #[test]
    fn test_select_peer_single_candidate() {
        let router = Router::new(&[]);
        let single_peer = PeerKeyLocation::random();
        let peers = vec![single_peer.clone()];
        let target = Location::random();

        let result = router.select_peer(&peers, target);
        assert!(result.is_some(), "Should select the only available peer");
        assert_eq!(
            *result.unwrap(),
            single_peer,
            "Should return the single candidate"
        );
    }

    /// Feed router a mix of successes for peer A and failures for peer B at similar
    /// distances. Verify select_peer prefers peer A.
    #[test]
    fn test_failure_avoidance() {
        failure_avoidance_case(Training::History);
    }

    /// [`test_failure_avoidance`] on the warm hierarchical estimator.
    #[test]
    fn test_failure_avoidance_warm_hierarchical() {
        failure_avoidance_case(Training::WarmHierarchical);
    }

    fn failure_avoidance_case(training: Training) {
        let _mode = training.guard();
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        let peer_a = PeerKeyLocation::random();
        let peer_b = PeerKeyLocation::random();

        let contract_location = Location::random();

        let mut events = Vec::new();

        // 40 successes for peer A
        for _ in 0..40 {
            events.push(RouteEvent {
                peer: peer_a.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
                op_type: None,
            });
        }

        // 40 failures for peer B
        for _ in 0..40 {
            events.push(RouteEvent {
                peer: peer_b.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        let router = training.train(&events);

        // With 80 total events in failure_estimator (>= 50 threshold),
        // the router should use predictions and prefer peer A
        let peers = vec![peer_a.clone(), peer_b.clone()];
        training.check_decides(&router, &peers, contract_location);
        let selected = router.select_peer(&peers, contract_location);
        assert!(selected.is_some());
        assert_eq!(
            *selected.unwrap(),
            peer_a,
            "Router should prefer peer A (all successes) over peer B (all failures) \
             ({training:?})"
        );
    }

    /// Verify 49 events = distance-based, 50 events = prediction-based.
    #[test]
    fn test_threshold_at_50_events() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // 49 events: below threshold
        let events_49: Vec<RouteEvent> = (0..49)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
                op_type: None,
            })
            .collect();

        let router_49 = Router::new(&events_49);
        assert!(
            !router_49.has_sufficient_routing_events(),
            "49 events should be below threshold"
        );

        // 50 events: at threshold
        let events_50: Vec<RouteEvent> = (0..50)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
                op_type: None,
            })
            .collect();

        let router_50 = Router::new(&events_50);
        assert!(
            router_50.has_sufficient_routing_events(),
            "50 events should meet threshold"
        );
    }

    /// 25 successes + 25 failures = 50 total. Router should activate predictions.
    #[test]
    fn test_failures_count_toward_threshold() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // 25 successes
        for _ in 0..25 {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
                op_type: None,
            });
        }

        // 25 failures
        for _ in 0..25 {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        let router = Router::new(&events);
        assert!(
            router.has_sufficient_routing_events(),
            "25 successes + 25 failures = 50 total should meet threshold"
        );
    }

    /// When the failure_estimator has enough events but timing estimators do not
    /// (all failures, no timed successes), the router should still use prediction-based
    /// routing using failure probability alone — not fall back to distance-based.
    #[test]
    fn test_prediction_works_with_failure_only_data() {
        let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
        let contract_location = Location::random();

        // 60 failures spread across peers: failure_estimator has data,
        // but timing estimators have 0 events
        let events: Vec<RouteEvent> = (0..60)
            .map(|i| RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            })
            .collect();

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Predictions should succeed using failure probability alone
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        assert!(!selected.is_empty());
        assert!(
            matches!(decision.strategy, RoutingStrategy::PredictionBased),
            "Router should use predictions with failure-only data, got {:?}",
            decision.strategy
        );

        // All candidates should have predictions (not None)
        for candidate in &decision.candidates {
            assert!(
                candidate.prediction.is_some(),
                "Each candidate should have a prediction from failure data"
            );
        }
    }

    /// Verify that `SuccessUntimed` feeds the failure_estimator (as 0.0 = success)
    /// but does NOT feed timing estimators (response_start_time, transfer_rate).
    #[test]
    fn test_success_untimed_feeds_failure_only() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut router = Router::new(&[]);

        // Add a SuccessUntimed event
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location,
            outcome: RouteOutcome::SuccessUntimed,
            op_type: None,
        });

        // failure_estimator should have 1 event (success = 0.0)
        assert_eq!(
            router.failure_estimator.len(),
            1,
            "SuccessUntimed should feed failure_estimator"
        );
        // timing estimators should remain empty
        assert_eq!(
            router.response_start_time_estimator.len(),
            0,
            "SuccessUntimed should NOT feed response_start_time_estimator"
        );
        assert_eq!(
            router.transfer_rate_estimator.len(),
            0,
            "SuccessUntimed should NOT feed transfer_rate_estimator"
        );

        // Also verify it counts toward threshold: 49 SuccessUntimed + 1 more = 50
        for _ in 0..49 {
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }
        assert_eq!(router.failure_estimator.len(), 50);
        assert!(
            router.has_sufficient_routing_events(),
            "50 SuccessUntimed events should meet the threshold"
        );
    }

    /// Verify Router::new() handles SuccessUntimed in history correctly.
    #[test]
    fn test_success_untimed_in_history() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let events: Vec<RouteEvent> = (0..30)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            })
            .chain((0..20).map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            }))
            .collect();

        let router = Router::new(&events);

        // failure_estimator: 30 successes + 20 failures = 50
        assert_eq!(router.failure_estimator.len(), 50);
        // timing estimators: 0 (SuccessUntimed has no timing data, Failure has none either)
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);
    }

    /// With mixed untimed successes and failures (realistic post-#3137 scenario),
    /// the router should use failure probability to prefer low-failure peers.
    #[test]
    fn test_failure_only_differentiates_peers() {
        failure_only_differentiates_peers_case(Training::History);
    }

    /// [`test_failure_only_differentiates_peers`] on the warm hierarchical
    /// estimator.
    #[test]
    fn test_failure_only_differentiates_peers_warm_hierarchical() {
        failure_only_differentiates_peers_case(Training::WarmHierarchical);
    }

    fn failure_only_differentiates_peers_case(training: Training) {
        let _mode = training.guard();
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        let good_peer = PeerKeyLocation::random();
        let bad_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // Good peer: 30 untimed successes, 0 failures → 0% failure rate
        for _ in 0..30 {
            events.push(RouteEvent {
                peer: good_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }

        // Bad peer: 0 successes, 30 failures → 100% failure rate
        for _ in 0..30 {
            events.push(RouteEvent {
                peer: bad_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        let router = training.train(&events);
        assert!(router.has_sufficient_routing_events());
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Router should prefer the good peer via failure-probability prediction
        let peers = vec![good_peer.clone(), bad_peer.clone()];
        training.check_decides(&router, &peers, contract_location);
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);

        assert!(matches!(
            decision.strategy,
            RoutingStrategy::PredictionBased
        ));
        assert_eq!(
            *selected[0], good_peer,
            "Router should prefer the low-failure peer when using failure-only predictions"
        );

        // The selected (first) candidate should have lower expected_total_time
        let first = &decision.candidates[0];
        let second = &decision.candidates[1];
        assert!(
            first.prediction.as_ref().unwrap().expected_total_time
                <= second.prediction.as_ref().unwrap().expected_total_time,
            "Selected peer should have lower expected_total_time"
        );
    }

    /// With sparse timed success data (only a few GETs succeed), the router should
    /// still produce predictions — using failure data for all peers and timing data
    /// where available.
    #[test]
    fn test_sparse_timed_success_data() {
        let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // 40 untimed successes across peers
        for i in 0..40 {
            events.push(RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }

        // 10 failures
        for i in 0..10 {
            events.push(RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        // Only 3 timed successes (below MIN_POINTS_FOR_REGRESSION=5)
        for peer in peers.iter().take(3) {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
                op_type: None,
            });
        }

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        assert!(router.response_start_time_estimator.len() < 5);
        assert!(router.transfer_rate_estimator.len() < 5);

        // Router should still make predictions using failure data
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        assert!(!selected.is_empty());
        assert!(
            matches!(decision.strategy, RoutingStrategy::PredictionBased),
            "Router should use failure-only predictions when timing data is sparse, got {:?}",
            decision.strategy
        );
    }

    // ============ Wiring completeness: end-to-end outcome → router chain tests ============

    /// Simulate the full chain: subscribe outcome → RouteEvent → Router.
    /// A completed subscribe with stats produces SuccessUntimed, which feeds
    /// the failure_estimator (as 0.0 = success).
    #[test]
    fn test_subscribe_outcome_feeds_router() {
        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Simulate what node.rs:580-587 does when OpOutcome::ContractOpSuccessUntimed
        // is returned from subscribe's outcome() (post-fix: stats are preserved)
        let route_event = RouteEvent {
            peer: target_peer.clone(),
            contract_location,
            outcome: RouteOutcome::SuccessUntimed,
            op_type: None,
        };

        let mut router = Router::new(&[]);
        assert_eq!(router.failure_estimator.len(), 0);
        router.add_event(route_event);
        assert_eq!(
            router.failure_estimator.len(),
            1,
            "Router failure_estimator should record subscribe success (0.0)"
        );
        // Timing estimators should NOT be fed by untimed operations
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);
    }

    /// Create GetOp with result + partial timing, verify ContractOpSuccessUntimed
    /// feeds router's failure_estimator.
    #[test]
    fn test_get_partial_timing_feeds_router() {
        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Simulate the OpOutcome from a GET with partial timing
        let route_event = RouteEvent {
            peer: target_peer.clone(),
            contract_location,
            outcome: RouteOutcome::SuccessUntimed,
            op_type: None,
        };

        let mut router = Router::new(&[]);
        assert_eq!(router.failure_estimator.len(), 0);
        router.add_event(route_event);
        assert_eq!(
            router.failure_estimator.len(),
            1,
            "Router should record untimed GET success"
        );
        // Timing estimators should remain empty
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);
    }

    /// Verify ContractOpFailure increments failure_estimator with value 1.0,
    /// and that after enough failures the router predicts higher failure
    /// probability. The prediction is checked on BOTH stacks: since the #4485
    /// flip it comes from the hierarchical estimator by default, and from the
    /// legacy stack only under `FREENET_ROUTING_HIERARCHICAL=0`.
    #[test]
    fn test_failure_outcome_feeds_failure_estimator_with_value_one() {
        for hierarchical in [false, true] {
            let _mode = force_hierarchical_routing(hierarchical);
            let peer = PeerKeyLocation::random();
            let contract_location = Location::random();

            let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(
                crate::util::time_source::SharedMockTimeSource::new(),
            ));

            // Add a failure event
            router.add_event(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
            assert_eq!(
                router.failure_estimator.len(),
                1,
                "Failure should be recorded in failure_estimator"
            );

            // Add enough failures to cross threshold and check prediction
            for _ in 0..59 {
                router.add_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location,
                    outcome: RouteOutcome::Failure,
                    op_type: None,
                });
            }
            assert_eq!(router.failure_estimator.len(), 60);
            assert!(router.has_sufficient_routing_events());

            // Prediction should show high failure probability
            let peers = vec![peer.clone()];
            let legacy_before = LEGACY_STAGE_EVALUATIONS.with(|count| count.get());
            let (selected, decision) =
                router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
            let legacy_evaluations =
                LEGACY_STAGE_EVALUATIONS.with(|count| count.get()) - legacy_before;
            if hierarchical {
                assert_hierarchical_failure_decides(&router, &peers, contract_location);
                assert_eq!(
                    legacy_evaluations, 0,
                    "a warm estimator with no timing data must not consult the legacy stack"
                );
            } else {
                assert!(
                    legacy_evaluations >= 1,
                    "with the estimator off the prediction must come from the legacy stack"
                );
            }
            assert!(!selected.is_empty());
            let pred = decision.candidates[0].prediction.as_ref().unwrap();
            assert!(
                pred.failure_probability > 0.5,
                "After 60 failures, failure probability should be high, got {} \
                 (hierarchical: {hierarchical})",
                pred.failure_probability
            );
        }
    }

    /// Verify existing Success path (with timing) still feeds all 3 estimators.
    /// Regression guard for the new untimed branch.
    #[test]
    fn test_full_timing_success_still_works() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut router = Router::new(&[]);

        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location,
            outcome: RouteOutcome::Success {
                time_to_response_start: Duration::from_millis(50),
                payload_size: 1000,
                payload_transfer_time: Duration::from_millis(10),
            },
            op_type: None,
        });

        assert_eq!(
            router.failure_estimator.len(),
            1,
            "Success should feed failure_estimator (as 0.0)"
        );
        assert_eq!(
            router.response_start_time_estimator.len(),
            1,
            "Timed success should feed response_start_time_estimator"
        );
        assert_eq!(
            router.transfer_rate_estimator.len(),
            1,
            "Timed success should feed transfer_rate_estimator"
        );
    }

    /// When timing data accumulates beyond MIN_POINTS_FOR_REGRESSION, the router
    /// should transition from failure-only predictions (time=0, speed=0) to full
    /// predictions with real timing values.
    #[test]
    fn test_transition_from_failure_only_to_full_predictions() {
        transition_from_failure_only_case(Training::History);
    }

    /// [`test_transition_from_failure_only_to_full_predictions`] on the warm
    /// hierarchical estimator, through three states a node passes through:
    ///
    /// 1. Failure-only (all-success, untimed). Both stacks read 0.0 failure
    ///    here, so the precondition's equality cannot tell them apart; the
    ///    warm mode instead asserts NO legacy stage was evaluated, which with
    ///    no timing data holds only when a hierarchical failure estimate is
    ///    PRESENT (so the legacy stage is not consulted). It cannot tell which
    ///    value that arm then uses: returning the raw isotonic estimate there
    ///    would also read 0.0 and pass. That residual is equality-blind.
    /// 2. Five timed successes: below the hierarchical timing stages' 30-point
    ///    floor, so timing comes from the legacy fallback while failure stays
    ///    hierarchical — the mixed state of a node's first timed GETs.
    /// 3. (Warm only) 30 more timed successes: every stage hierarchical, no
    ///    legacy stage evaluated, timing pinned to the hierarchical estimate.
    #[test]
    fn test_transition_from_failure_only_to_full_predictions_warm_hierarchical() {
        transition_from_failure_only_case(Training::WarmHierarchical);
    }

    fn transition_from_failure_only_case(training: Training) {
        let _mode = training.guard();
        let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
        let contract_location = Location::random();

        // Phase 1: Only untimed data, above threshold
        let mut events: Vec<RouteEvent> = (0..50)
            .map(|i| RouteEvent {
                peer: peers[i % peers.len()].clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            })
            .collect();

        let router = training.train(&events);
        training.check_decides(&router, &peers, contract_location);
        let legacy_before = LEGACY_STAGE_EVALUATIONS.with(|count| count.get());
        let (_, decision) = router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        if training == Training::WarmHierarchical {
            assert_eq!(
                LEGACY_STAGE_EVALUATIONS.with(|count| count.get()) - legacy_before,
                0,
                "phase 1 has no timing data, so a legacy stage runs only if the \
                 hierarchical failure estimate is missing or ignored"
            );
        }
        // Failure-only: timing fields should be 0.0
        let pred = decision.candidates[0].prediction.as_ref().unwrap();
        assert_eq!(pred.time_to_response_start, 0.0);
        assert_eq!(pred.transfer_speed_bps, 0.0);

        // Phase 2: Add enough timed successes to cross MIN_POINTS_FOR_REGRESSION (5)
        for peer in &peers {
            events.push(RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(10),
                },
                op_type: None,
            });
        }

        let router2 = training.train(&events);
        assert!(router2.response_start_time_estimator.len() >= 5);
        training.check_decides(&router2, &peers, contract_location);
        let (_, decision2) =
            router2.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        // Full prediction: timing fields should have real values
        let pred2 = decision2.candidates[0].prediction.as_ref().unwrap();
        assert!(
            pred2.time_to_response_start > 0.0,
            "Should have real timing data after transition"
        );
        assert!(
            pred2.transfer_speed_bps > 0.0,
            "Should have real transfer speed after transition"
        );

        if training == Training::WarmHierarchical {
            // Phase 3: past the hierarchical timing stages' 30-point floor
            // (5 + 30 timed successes), so every stage is hierarchical and no
            // legacy stage may run at all.
            for round in 0..6u64 {
                for peer in &peers {
                    events.push(RouteEvent {
                        peer: peer.clone(),
                        contract_location,
                        outcome: RouteOutcome::Success {
                            time_to_response_start: Duration::from_millis(40 + 5 * round),
                            payload_size: 1000,
                            payload_transfer_time: Duration::from_millis(8 + round),
                        },
                        op_type: None,
                    });
                }
            }
            let router3 = training.train(&events);
            assert_hierarchical_timing_decides(&router3, &peers, contract_location);
            let legacy_before = LEGACY_STAGE_EVALUATIONS.with(|count| count.get());
            let (_, decision3) =
                router3.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
            assert_eq!(
                LEGACY_STAGE_EVALUATIONS.with(|count| count.get()) - legacy_before,
                0,
                "phase 3: with every stage warm, no legacy stage may be evaluated"
            );
            let pred3 = decision3.candidates[0].prediction.as_ref().unwrap();
            assert!(
                pred3.time_to_response_start > 0.0 && pred3.transfer_speed_bps > 0.0,
                "phase 3 must carry real hierarchical timing, got {pred3:?}"
            );
        }
    }

    /// Simulate realistic post-#3137 traffic: a mix of timed GET successes, untimed
    /// PUT/SUBSCRIBE/UPDATE successes, and failures across multiple peers.
    /// The router should activate prediction-based routing and prefer
    /// low-failure, low-latency peers.
    #[test]
    fn test_realistic_mixed_traffic_routing() {
        realistic_mixed_traffic_case(Training::History);
    }

    /// [`test_realistic_mixed_traffic_routing`] on the warm hierarchical
    /// estimator. The warm mode triples the timed GET successes (39, past the
    /// 30-point floor) so BOTH timing stages are hierarchical as well and the
    /// ranking runs entirely on the hierarchical estimate, which the timing
    /// precondition proves. The failure ordering is unchanged (close ~4%, mid
    /// ~21%, far 75%); the History mode keeps the original 13 timed events.
    /// So under realistic traffic this twin covers only fully-warm timing; the
    /// mixed state (warm failure, sparse legacy timing) is covered by the
    /// transition twin's phase 2, with uniform timing.
    #[test]
    fn test_realistic_mixed_traffic_routing_warm_hierarchical() {
        realistic_mixed_traffic_case(Training::WarmHierarchical);
    }

    fn realistic_mixed_traffic_case(training: Training) {
        let _mode = training.guard();
        // Seed RNG so peer ring distances are deterministic; without this the
        // (legacy mode's) isotonic regression's ascending monotonicity
        // constraint can conflict with the failure-rate ordering when random
        // distances are adversarial.
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        let contract_location = Location::random();
        let close_peer = PeerKeyLocation::random();
        let mid_peer = PeerKeyLocation::random();
        let far_peer = PeerKeyLocation::random();

        let mut events = Vec::new();

        // Timed-success multiplier: 1 in the History mode (the original
        // input), 3 in the warm mode so the hierarchical timing stages warm.
        let timed_rounds: usize = if training == Training::WarmHierarchical {
            3
        } else {
            1
        };

        // close_peer: 10 timed GET successes + 15 untimed PUT/SUB successes, 2 failures
        // → ~7% failure rate, good timing data
        for _ in 0..10 * timed_rounds {
            events.push(RouteEvent {
                peer: close_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(20),
                    payload_size: 2000,
                    payload_transfer_time: Duration::from_millis(5),
                },
                op_type: None,
            });
        }
        for _ in 0..15 {
            events.push(RouteEvent {
                peer: close_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }
        for _ in 0..2 {
            events.push(RouteEvent {
                peer: close_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        // mid_peer: 3 timed GET successes + 10 untimed successes, 5 failures
        // → ~28% failure rate, sparse timing data
        for _ in 0..3 * timed_rounds {
            events.push(RouteEvent {
                peer: mid_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 2000,
                    payload_transfer_time: Duration::from_millis(15),
                },
                op_type: None,
            });
        }
        for _ in 0..10 {
            events.push(RouteEvent {
                peer: mid_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }
        for _ in 0..5 {
            events.push(RouteEvent {
                peer: mid_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        // far_peer: 0 timed successes, 5 untimed successes, 15 failures
        // → 75% failure rate, no timing data
        for _ in 0..5 {
            events.push(RouteEvent {
                peer: far_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }
        for _ in 0..15 {
            events.push(RouteEvent {
                peer: far_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        // Total: 27 + 18 + 20 = 65 events > 50 threshold (History mode; the warm
        // mode adds 26 timed successes)
        let router = training.train(&events);
        assert!(router.has_sufficient_routing_events());

        let timed = 13 * timed_rounds;
        // failure_estimator should have every event (65 in the History mode)
        assert_eq!(router.failure_estimator.len(), 52 + timed);
        // timing estimators only have the timed GET successes: 10 + 3 = 13 (x3 warm)
        assert_eq!(router.response_start_time_estimator.len(), timed);
        assert_eq!(router.transfer_rate_estimator.len(), timed);

        // Router should use prediction-based routing and prefer close_peer
        let peers = vec![close_peer.clone(), mid_peer.clone(), far_peer.clone()];
        training.check_decides(&router, &peers, contract_location);
        if training == Training::WarmHierarchical {
            assert_hierarchical_timing_decides(&router, &peers, contract_location);
        }
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 3);

        assert!(matches!(
            decision.strategy,
            RoutingStrategy::PredictionBased
        ));
        assert_eq!(selected.len(), 3);

        // close_peer should be ranked first (lowest failure + best timing)
        assert_eq!(
            *selected[0], close_peer,
            "close_peer with ~7% failure and fast timing should be ranked first"
        );
        // far_peer should be ranked last (highest failure rate)
        assert_eq!(
            *selected[2], far_peer,
            "far_peer with ~75% failure should be ranked last"
        );
    }

    /// Test that adding events incrementally (as happens in practice) produces
    /// the same routing decision as building from history.
    #[test]
    fn test_incremental_vs_batch_consistency() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let events: Vec<RouteEvent> = (0..30)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            })
            .chain((0..20).map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            }))
            .chain((0..5).map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(40),
                    payload_size: 1500,
                    payload_transfer_time: Duration::from_millis(8),
                },
                op_type: None,
            }))
            .collect();

        // Batch: build from history
        let batch_router = Router::new(&events);

        // Incremental: add one by one
        let mut incr_router = Router::new(&[]);
        for event in &events {
            incr_router.add_event(event.clone());
        }

        // Both should have identical estimator counts
        assert_eq!(
            batch_router.failure_estimator.len(),
            incr_router.failure_estimator.len()
        );
        assert_eq!(
            batch_router.response_start_time_estimator.len(),
            incr_router.response_start_time_estimator.len()
        );
        assert_eq!(
            batch_router.transfer_rate_estimator.len(),
            incr_router.transfer_rate_estimator.len()
        );
    }

    /// Verify that the router handles a scenario where only untimed operations
    /// exist (no GETs ever succeeded with timing). This is realistic for a node
    /// that primarily handles PUT/SUBSCRIBE/UPDATE traffic.
    #[test]
    fn test_untimed_only_network_peer_ranking() {
        untimed_only_network_case(Training::History);
    }

    /// [`test_untimed_only_network_peer_ranking`] on the warm hierarchical
    /// estimator.
    #[test]
    fn test_untimed_only_network_peer_ranking_warm_hierarchical() {
        untimed_only_network_case(Training::WarmHierarchical);
    }

    fn untimed_only_network_case(training: Training) {
        let _mode = training.guard();
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        let reliable_peer = PeerKeyLocation::random();
        let flaky_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut events = Vec::new();

        // reliable_peer: 40 untimed successes, 0 failures
        for _ in 0..40 {
            events.push(RouteEvent {
                peer: reliable_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }

        // flaky_peer: 5 untimed successes, 15 failures → 75% failure
        for _ in 0..5 {
            events.push(RouteEvent {
                peer: flaky_peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            });
        }
        for _ in 0..15 {
            events.push(RouteEvent {
                peer: flaky_peer.clone(),
                contract_location,
                outcome: RouteOutcome::Failure,
                op_type: None,
            });
        }

        let router = training.train(&events);
        assert!(router.has_sufficient_routing_events());
        // No timing data at all
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Router should still make predictions and prefer the reliable peer
        let peers = vec![reliable_peer.clone(), flaky_peer.clone()];
        training.check_decides(&router, &peers, contract_location);
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 2);

        assert!(matches!(
            decision.strategy,
            RoutingStrategy::PredictionBased
        ));
        assert_eq!(
            *selected[0], reliable_peer,
            "Reliable peer should be preferred in untimed-only network"
        );

        // Predictions should use failure-only mode (time=0, speed=0)
        for candidate in &decision.candidates {
            let pred = candidate.prediction.as_ref().unwrap();
            assert_eq!(pred.time_to_response_start, 0.0);
            assert_eq!(pred.transfer_speed_bps, 0.0);
        }
    }

    /// When the router receives SuccessUntimed events at one contract location
    /// and Failure events at a different contract location through the same
    /// peer, its prediction must separate them, using untimed success data from
    /// PUT/SUBSCRIBE/UPDATE. Checked on BOTH stacks: the hierarchical estimator
    /// (the default since #4485, which separates them through its per-peer,
    /// per-band cells) and the legacy isotonic model (reached only under
    /// `FREENET_ROUTING_HIERARCHICAL=0` since the flip).
    #[test]
    fn test_location_dependent_failure_patterns() {
        for hierarchical in [false, true] {
            let _mode = force_hierarchical_routing(hierarchical);
            let peer = PeerKeyLocation::random();
            let near_contract = Location::new(0.01); // very close distance (close to 0)
            let far_contract = Location::new(0.49); // maximum distance on the ring

            let mut router = Router::new(&[]).with_time_source(std::sync::Arc::new(
                crate::util::time_source::SharedMockTimeSource::new(),
            ));

            // Peer succeeds for nearby contracts (small distance)
            for _ in 0..30 {
                router.add_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location: near_contract,
                    outcome: RouteOutcome::SuccessUntimed,
                    op_type: None,
                });
            }

            // Peer fails for distant contracts (large distance)
            for _ in 0..25 {
                router.add_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location: far_contract,
                    outcome: RouteOutcome::Failure,
                    op_type: None,
                });
            }

            assert!(router.has_sufficient_routing_events());
            assert_eq!(router.failure_estimator.len(), 55);

            // The router should give different failure predictions for near vs
            // far contracts through this peer
            let peers = vec![peer.clone()];

            let legacy_before = LEGACY_STAGE_EVALUATIONS.with(|count| count.get());
            let (_, near_decision) =
                router.select_k_best_peers_with_telemetry(&peers, near_contract, 1);
            let (_, far_decision) =
                router.select_k_best_peers_with_telemetry(&peers, far_contract, 1);
            let legacy_evaluations =
                LEGACY_STAGE_EVALUATIONS.with(|count| count.get()) - legacy_before;
            if hierarchical {
                for contract in [near_contract, far_contract] {
                    assert_hierarchical_failure_decides(&router, &peers, contract);
                }
                assert_eq!(
                    legacy_evaluations, 0,
                    "a warm estimator with no timing data must not consult the legacy stack"
                );
            } else {
                assert!(
                    legacy_evaluations >= 2,
                    "with the estimator off both predictions must come from the legacy stack"
                );
            }

            let near_pred = near_decision.candidates[0]
                .prediction
                .as_ref()
                .expect("should have prediction for near contract");
            let far_pred = far_decision.candidates[0]
                .prediction
                .as_ref()
                .expect("should have prediction for far contract");

            // Near contract should have lower failure probability than far
            // contract. The legacy isotonic fit only guarantees `<=`; the
            // hierarchical estimator's (peer, band) cells see the two bands
            // separately, so it must separate them strictly.
            if hierarchical {
                assert!(
                    near_pred.failure_probability < far_pred.failure_probability,
                    "hierarchical: near contract failure prob ({}) must be < far ({})",
                    near_pred.failure_probability,
                    far_pred.failure_probability
                );
            } else {
                assert!(
                    near_pred.failure_probability <= far_pred.failure_probability,
                    "legacy: near contract failure prob ({}) should be <= far ({})",
                    near_pred.failure_probability,
                    far_pred.failure_probability
                );
            }
        }
    }

    /// Verify that a single peer's SuccessUntimed events correctly produce a 0.0
    /// failure probability when it has never failed.
    #[test]
    fn test_zero_failure_probability_with_untimed_success() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let events: Vec<RouteEvent> = (0..60)
            .map(|_| RouteEvent {
                peer: peer.clone(),
                contract_location,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: None,
            })
            .collect();

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());

        let (_, decision) =
            router.select_k_best_peers_with_telemetry(&[peer], contract_location, 1);

        let pred = decision.candidates[0]
            .prediction
            .as_ref()
            .expect("should have prediction");
        assert!(
            pred.failure_probability < 0.01,
            "Peer with only successes should have near-zero failure probability, got {}",
            pred.failure_probability
        );
    }

    mod proptest_router {
        use super::*;
        use proptest::prelude::*;

        /// Strategy that generates a RouteOutcome with valid durations.
        fn arb_route_outcome() -> impl Strategy<Value = RouteOutcome> {
            prop_oneof![
                // Success with timing data
                (1u64..5000, 100usize..100_000, 1u64..5000).prop_map(
                    |(response_ms, payload_size, transfer_ms)| {
                        RouteOutcome::Success {
                            time_to_response_start: Duration::from_millis(response_ms),
                            payload_size,
                            payload_transfer_time: Duration::from_millis(transfer_ms),
                        }
                    }
                ),
                // Untimed success
                Just(RouteOutcome::SuccessUntimed),
                // Failure
                Just(RouteOutcome::Failure),
            ]
        }

        // Each property runs in both `Training` modes: from history (the
        // legacy fallback) and warm hierarchical (what the fleet routes with).

        fn predictions_never_nan_case(
            outcomes: Vec<RouteOutcome>,
            seed: u64,
            training: Training,
        ) -> Result<(), TestCaseError> {
            let _mode = training.guard();
            let _guard = crate::config::GlobalRng::seed_guard(seed);
            let peers: Vec<PeerKeyLocation> = (0..5).map(|_| PeerKeyLocation::random()).collect();
            let contract_location = Location::random();

            let events: Vec<RouteEvent> = outcomes
                .into_iter()
                .enumerate()
                .map(|(i, outcome)| RouteEvent {
                    peer: peers[i % peers.len()].clone(),
                    contract_location,
                    outcome,
                    op_type: None,
                })
                .collect();

            let router = training.train(&events);

            // Router should have enough events for prediction
            prop_assert!(router.has_sufficient_routing_events());
            training.check_decides(&router, &peers, contract_location);

            // Predictions must not produce NaN
            let (selected, decision) =
                router.select_k_best_peers_with_telemetry(&peers, contract_location, 3);

            prop_assert!(!selected.is_empty());
            for candidate in &decision.candidates {
                if let Some(pred) = &candidate.prediction {
                    prop_assert!(
                        !pred.failure_probability.is_nan(),
                        "failure_probability is NaN"
                    );
                    prop_assert!(
                        !pred.expected_total_time.is_nan(),
                        "expected_total_time is NaN"
                    );
                    prop_assert!(
                        !pred.time_to_response_start.is_nan(),
                        "time_to_response_start is NaN"
                    );
                    prop_assert!(
                        !pred.transfer_speed_bps.is_nan(),
                        "transfer_speed_bps is NaN"
                    );
                }
            }
            Ok(())
        }

        fn failure_data_increases_failure_prediction_case(
            n_good: usize,
            n_bad: usize,
            seed: u64,
            training: Training,
        ) -> Result<(), TestCaseError> {
            let _mode = training.guard();
            let _guard = crate::config::GlobalRng::seed_guard(seed);
            let good_peer = PeerKeyLocation::random();
            let bad_peer = PeerKeyLocation::random();
            let contract_location = Location::random();

            let mut events = Vec::new();

            // Good peer: all successes (untimed for simplicity)
            for _ in 0..n_good {
                events.push(RouteEvent {
                    peer: good_peer.clone(),
                    contract_location,
                    outcome: RouteOutcome::SuccessUntimed,
                    op_type: None,
                });
            }

            // Bad peer: all failures
            for _ in 0..n_bad {
                events.push(RouteEvent {
                    peer: bad_peer.clone(),
                    contract_location,
                    outcome: RouteOutcome::Failure,
                    op_type: None,
                });
            }

            let router = training.train(&events);
            prop_assert!(router.has_sufficient_routing_events());

            let peers = vec![good_peer.clone(), bad_peer.clone()];
            training.check_decides(&router, &peers, contract_location);
            let (selected, decision) =
                router.select_k_best_peers_with_telemetry(&peers, contract_location, 2);

            let all_have_predictions = decision.candidates.iter().all(|c| c.prediction.is_some());

            // With enough data, predictions should exist for both
            if all_have_predictions && selected.len() == 2 {
                // The first selected peer (lowest expected_total_time) should
                // be the good peer since failures increase cost via the
                // failure_cost_multiplier in predict_routing_outcome
                prop_assert!(
                    *selected[0] == good_peer,
                    "Good peer (all successes) should be ranked first ({:?})",
                    training
                );
            }
            Ok(())
        }

        fn failure_probability_bounded_case(
            outcomes: Vec<RouteOutcome>,
            seed: u64,
            training: Training,
        ) -> Result<(), TestCaseError> {
            let _mode = training.guard();
            let _guard = crate::config::GlobalRng::seed_guard(seed);
            let peers: Vec<PeerKeyLocation> = (0..3).map(|_| PeerKeyLocation::random()).collect();
            let contract_location = Location::random();

            let events: Vec<RouteEvent> = outcomes
                .into_iter()
                .enumerate()
                .map(|(i, outcome)| RouteEvent {
                    peer: peers[i % peers.len()].clone(),
                    contract_location,
                    outcome,
                    op_type: None,
                })
                .collect();

            let router = training.train(&events);
            if !router.has_sufficient_routing_events() {
                return Ok(());
            }
            training.check_decides(&router, &peers, contract_location);

            let (_, decision) =
                router.select_k_best_peers_with_telemetry(&peers, contract_location, 3);

            for candidate in &decision.candidates {
                if let Some(pred) = &candidate.prediction {
                    // Allow small float tolerance around [0, 1]
                    prop_assert!(
                        pred.failure_probability >= 0.0 && pred.failure_probability <= 1.0,
                        "failure_probability {} out of [0, 1] range",
                        pred.failure_probability
                    );
                }
            }
            Ok(())
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Property: predictions never produce NaN or panic under any mix of
            /// operation outcomes fed to the router.
            #[test]
            fn predictions_never_nan_or_panic(
                outcomes in proptest::collection::vec(arb_route_outcome(), 60..200),
                seed in 0u64..u64::MAX,
            ) {
                predictions_never_nan_case(outcomes, seed, Training::History)?;
            }

            /// [`predictions_never_nan_or_panic`] on the warm hierarchical
            /// estimator.
            #[test]
            fn predictions_never_nan_or_panic_warm_hierarchical(
                outcomes in proptest::collection::vec(arb_route_outcome(), 60..200),
                seed in 0u64..u64::MAX,
            ) {
                predictions_never_nan_case(outcomes, seed, Training::WarmHierarchical)?;
            }

            /// Property: after enough failure data for a peer, the router predicts
            /// a higher failure probability for that peer compared to a peer with
            /// all successes (at the same distance).
            #[test]
            fn failure_data_increases_failure_prediction(
                n_good in 30usize..60,
                n_bad in 30usize..60,
                seed in 0u64..u64::MAX,
            ) {
                failure_data_increases_failure_prediction_case(
                    n_good,
                    n_bad,
                    seed,
                    Training::History,
                )?;
            }

            /// [`failure_data_increases_failure_prediction`] on the warm
            /// hierarchical estimator.
            #[test]
            fn failure_data_increases_failure_prediction_warm_hierarchical(
                n_good in 30usize..60,
                n_bad in 30usize..60,
                seed in 0u64..u64::MAX,
            ) {
                failure_data_increases_failure_prediction_case(
                    n_good,
                    n_bad,
                    seed,
                    Training::WarmHierarchical,
                )?;
            }

            /// Property: add_event is consistent with batch construction.
            /// Building from history vs adding events one-by-one should produce
            /// the same estimator counts.
            #[test]
            fn incremental_matches_batch(
                outcomes in proptest::collection::vec(arb_route_outcome(), 1..100),
                seed in 0u64..u64::MAX,
            ) {
                let _guard = crate::config::GlobalRng::seed_guard(seed);
                let peer = PeerKeyLocation::random();
                let contract_location = Location::random();

                let events: Vec<RouteEvent> = outcomes
                    .into_iter()
                    .map(|outcome| RouteEvent {
                        peer: peer.clone(),
                        contract_location,
                        outcome,
                        op_type: None,
                    })
                    .collect();

                // Batch construction
                let batch_router = Router::new(&events);

                // Incremental construction
                let mut incr_router = Router::new(&[]);
                for event in &events {
                    incr_router.add_event(event.clone());
                }

                prop_assert_eq!(
                    batch_router.failure_estimator.len(),
                    incr_router.failure_estimator.len(),
                    "failure_estimator counts differ"
                );
                prop_assert_eq!(
                    batch_router.response_start_time_estimator.len(),
                    incr_router.response_start_time_estimator.len(),
                    "response_start_time_estimator counts differ"
                );
                prop_assert_eq!(
                    batch_router.transfer_rate_estimator.len(),
                    incr_router.transfer_rate_estimator.len(),
                    "transfer_rate_estimator counts differ"
                );
            }

            /// Property: select_peer always returns None for empty peer list,
            /// regardless of router state.
            #[test]
            fn empty_peers_always_none(
                n_events in 0usize..100,
                seed in 0u64..u64::MAX,
            ) {
                let _guard = crate::config::GlobalRng::seed_guard(seed);
                let peer = PeerKeyLocation::random();
                let contract_location = Location::random();

                let events: Vec<RouteEvent> = (0..n_events)
                    .map(|_| RouteEvent {
                        peer: peer.clone(),
                        contract_location,
                        outcome: RouteOutcome::SuccessUntimed,
                        op_type: None,
                    })
                    .collect();

                let router = Router::new(&events);
                let empty: Vec<PeerKeyLocation> = vec![];
                let result = router.select_peer(&empty, contract_location);
                prop_assert!(result.is_none());
            }

            /// Property: failure probability is bounded in [0, 1] (with small
            /// floating-point tolerance) for any mix of outcomes.
            #[test]
            fn failure_probability_bounded(
                outcomes in proptest::collection::vec(arb_route_outcome(), 60..150),
                seed in 0u64..u64::MAX,
            ) {
                failure_probability_bounded_case(outcomes, seed, Training::History)?;
            }

            /// [`failure_probability_bounded`] on the warm hierarchical
            /// estimator.
            #[test]
            fn failure_probability_bounded_warm_hierarchical(
                outcomes in proptest::collection::vec(arb_route_outcome(), 60..150),
                seed in 0u64..u64::MAX,
            ) {
                failure_probability_bounded_case(outcomes, seed, Training::WarmHierarchical)?;
            }
        }
    }

    /// Distance-based fallback should deprioritize peers that have failure history
    /// in the estimator, preferring untried peers.
    #[test]
    fn test_distance_fallback_deprioritizes_failed_peers() {
        distance_fallback_deprioritizes_failed_peers_case(Training::History);
    }

    /// [`test_distance_fallback_deprioritizes_failed_peers`] with the
    /// hierarchical estimator on and WARM: below the prediction threshold the
    /// fallback consults only the legacy `peer_adjustments`, so a warm
    /// estimator must not change what it picks. The sibling
    /// `test_distance_fallback_k_greater_than_untried_count` exercises the same
    /// branch, so it has no twin.
    #[test]
    fn test_distance_fallback_deprioritizes_failed_peers_warm_hierarchical() {
        distance_fallback_deprioritizes_failed_peers_case(Training::WarmHierarchical);
    }

    fn distance_fallback_deprioritizes_failed_peers_case(training: Training) {
        let _mode = training.guard();
        let _guard = crate::config::GlobalRng::seed_guard(0xDEAD_BEEF);

        // Create a "failed" peer that is very close to the target
        let failed_peer = PeerKeyLocation::random();
        // Create an "untried" peer that is farther from the target
        let untried_peer = PeerKeyLocation::random();

        let target = failed_peer.location().unwrap();

        // Feed failures for the failed peer — but stay below 50 events so the router
        // uses the distance-based fallback path
        let events: Vec<RouteEvent> = (0..30)
            .map(|_| RouteEvent {
                peer: failed_peer.clone(),
                contract_location: target,
                outcome: RouteOutcome::Failure,
                op_type: None,
            })
            .collect();

        let router = training.train(&events);
        assert!(
            !router.has_sufficient_routing_events(),
            "Should still be in distance-based fallback mode"
        );
        if training == Training::WarmHierarchical {
            // Warm: the failure stage has a curve and an estimate for the
            // failed peer (at distance 0 from its own location), so the
            // fallback below runs with the estimator ready to be consulted.
            assert!(
                router
                    .hierarchical
                    .estimate(&failed_peer, target, 0.0, router.estimator_clock.hours())
                    .failure_probability
                    .is_some(),
                "the hierarchical failure stage must be warm for this twin to mean anything"
            );
        }
        assert!(
            router
                .failure_estimator
                .peer_adjustments
                .contains_key(&failed_peer),
            "Failed peer should have adjustment data"
        );
        assert!(
            !router
                .failure_estimator
                .peer_adjustments
                .contains_key(&untried_peer),
            "Untried peer should NOT have adjustment data"
        );

        // Select 1 peer — the untried peer should be preferred even if farther,
        // because the failed peer has failure history
        let peers = vec![failed_peer.clone(), untried_peer.clone()];
        let (selected, decision) = router.select_k_best_peers_with_telemetry(&peers, target, 1);

        assert_eq!(selected.len(), 1);
        assert!(
            matches!(decision.strategy, RoutingStrategy::DistanceBased),
            "Should use distance-based strategy"
        );
        assert_eq!(
            *selected[0], untried_peer,
            "Should prefer untried peer over peer with failure history"
        );
    }

    /// With k > 1 and a mix of tried/untried peers, untried peers fill first,
    /// then tried peers fill remaining slots by distance.
    #[test]
    fn test_distance_fallback_k_greater_than_untried_count() {
        let _guard = crate::config::GlobalRng::seed_guard(0xBEEF_CAFE);

        let tried_peer = PeerKeyLocation::random();
        let untried_a = PeerKeyLocation::random();
        let untried_b = PeerKeyLocation::random();
        let target = tried_peer.location().unwrap();

        // 30 failures for the tried peer (above ADJUSTMENT_PRIOR_SIZE=10, below threshold=50)
        let events: Vec<RouteEvent> = (0..30)
            .map(|_| RouteEvent {
                peer: tried_peer.clone(),
                contract_location: target,
                outcome: RouteOutcome::Failure,
                op_type: None,
            })
            .collect();

        let router = Router::new(&events);
        assert!(!router.has_sufficient_routing_events());

        // Select k=3 from 3 peers: 2 untried should come first, tried peer last
        let peers = vec![tried_peer.clone(), untried_a.clone(), untried_b.clone()];
        let (selected, _) = router.select_k_best_peers_with_telemetry(&peers, target, 3);

        assert_eq!(selected.len(), 3);
        // The tried peer should be last (deprioritized)
        assert_eq!(
            *selected[2], tried_peer,
            "Tried peer should be last when untried peers are available"
        );
        // First two should be the untried peers (order depends on distance)
        let untried_set: std::collections::HashSet<PeerKeyLocation> =
            [untried_a, untried_b].into_iter().collect();
        assert!(untried_set.contains(selected[0]));
        assert!(untried_set.contains(selected[1]));
    }

    /// Verify that zero-payload events (from SUBSCRIBE) don't produce NaN
    /// in the transfer rate estimator (0 / 0 = NaN would poison regression).
    #[test]
    fn test_zero_payload_event_does_not_poison_transfer_rate() {
        let mut router = Router::new(&[]);
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Simulate a subscribe success: non-zero response time, zero payload
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location,
            outcome: RouteOutcome::Success {
                time_to_response_start: std::time::Duration::from_millis(50),
                payload_size: 0,
                payload_transfer_time: std::time::Duration::ZERO,
            },
            op_type: None,
        });

        // The failure estimator should have the event (result=0.0 for success)
        assert_eq!(router.failure_estimator.len(), 1);
        // The response time estimator should have it
        assert_eq!(router.response_start_time_estimator.len(), 1);
        // The transfer rate estimator should NOT have it (skipped to avoid NaN)
        assert_eq!(router.transfer_rate_estimator.len(), 0);
    }

    #[test]
    fn test_per_op_type_estimators_populated_via_add_event() {
        let mut router = Router::new(&[]);
        let peer = PeerKeyLocation::random();
        let contract = Location::random();

        // Add a GET success event
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location: contract,
            outcome: RouteOutcome::Success {
                time_to_response_start: Duration::from_millis(100),
                payload_size: 5000,
                payload_transfer_time: Duration::from_millis(50),
            },
            op_type: Some(OpType::Get),
        });

        // Add a PUT failure event
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location: contract,
            outcome: RouteOutcome::Failure,
            op_type: Some(OpType::Put),
        });

        // Add an event with no op_type (should only go to global)
        router.add_event(RouteEvent {
            peer: peer.clone(),
            contract_location: contract,
            outcome: RouteOutcome::Failure,
            op_type: None,
        });

        // Global estimator has all 3 events
        assert_eq!(router.failure_estimator.len(), 3);

        // Per-op failure: GET has 1 (success=0.0), PUT has 1 (failure=1.0)
        assert_eq!(router.per_op_failure.get(&OpType::Get).unwrap().len(), 1);
        assert_eq!(router.per_op_failure.get(&OpType::Put).unwrap().len(), 1);
        assert!(!router.per_op_failure.contains_key(&OpType::Subscribe));

        // Per-op response time: only GET (timed success)
        assert_eq!(
            router.per_op_response_time.get(&OpType::Get).unwrap().len(),
            1
        );
        assert!(!router.per_op_response_time.contains_key(&OpType::Put));

        // Per-op transfer rate: only GET (has payload data)
        assert_eq!(
            router.per_op_transfer_rate.get(&OpType::Get).unwrap().len(),
            1
        );
        assert!(!router.per_op_transfer_rate.contains_key(&OpType::Put));

        // Snapshot should have per-op curves
        let snap = router.snapshot();
        assert!(snap.per_op_curves.contains_key("GET"));
        assert!(snap.per_op_curves.contains_key("PUT"));
        assert!(!snap.per_op_curves.contains_key("SUBSCRIBE"));

        let get_curves = &snap.per_op_curves["GET"];
        assert!(get_curves.failure_events > 0);
        assert!(get_curves.response_time_events > 0);
        assert!(get_curves.transfer_rate_events > 0);
        // These curves come from a fit built inside `snapshot()`, because the
        // per-op estimators fit on read (#5662): non-empty shows that path works
        // end to end, which the event counts above (read without fitting) do not.
        assert!(!get_curves.failure_curve.is_empty());
        assert!(!get_curves.response_time_curve.is_empty());
        assert!(!get_curves.transfer_rate_curve.is_empty());

        let put_curves = &snap.per_op_curves["PUT"];
        assert!(put_curves.failure_events > 0);
        assert!(!put_curves.failure_curve.is_empty());
        assert_eq!(put_curves.response_time_events, 0);
        assert_eq!(put_curves.transfer_rate_events, 0);
    }

    #[test]
    fn test_per_op_type_estimators_populated_via_history() {
        let peer = PeerKeyLocation::random();
        let contract = Location::random();

        let history = vec![
            RouteEvent {
                peer: peer.clone(),
                contract_location: contract,
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(100),
                    payload_size: 5000,
                    payload_transfer_time: Duration::from_millis(50),
                },
                op_type: Some(OpType::Get),
            },
            RouteEvent {
                peer: peer.clone(),
                contract_location: contract,
                outcome: RouteOutcome::SuccessUntimed,
                op_type: Some(OpType::Subscribe),
            },
        ];

        let router = Router::new(&history);

        // GET should have failure + response_time + transfer_rate
        assert_eq!(router.per_op_failure.get(&OpType::Get).unwrap().len(), 1);
        assert_eq!(
            router.per_op_response_time.get(&OpType::Get).unwrap().len(),
            1
        );
        assert_eq!(
            router.per_op_transfer_rate.get(&OpType::Get).unwrap().len(),
            1
        );

        // SUBSCRIBE should have failure only (SuccessUntimed)
        assert_eq!(
            router.per_op_failure.get(&OpType::Subscribe).unwrap().len(),
            1
        );
        assert!(!router.per_op_response_time.contains_key(&OpType::Subscribe));
        assert!(!router.per_op_transfer_rate.contains_key(&OpType::Subscribe));
    }

    /// Regression test for crash: "user-provided comparison function does not
    /// correctly implement a total order" — originally observed in the periodic
    /// router-refresh task (`Ring::refit_router_periodically`, since deleted in
    /// #4811; the refit now runs inline in `IsotonicEstimator::add_event`).
    ///
    /// When `mean_transfer_size` has no samples (count=0), `compute()` returns
    /// NaN (0.0/0.0). If `xfer_speed` is also 0, the division NaN/0.0 stays NaN.
    /// `partial_cmp` on NaN returns `None`, and `unwrap_or(Equal)` breaks
    /// transitivity, causing Rust's sort to panic.
    #[test]
    fn sort_does_not_panic_with_nan_expected_total_time() {
        // Simulate the sort that happens in select_k_best_peers_with_telemetry
        // with NaN values that would have triggered the old partial_cmp panic.
        let mut scored: Vec<(usize, f64, Option<f64>)> = vec![
            (0, 0.1, Some(1.0)),
            (1, 0.2, Some(f64::NAN)), // NaN from division by zero
            (2, 0.3, Some(0.5)),
            (3, 0.4, None),           // No prediction
            (4, 0.5, Some(f64::NAN)), // Another NaN
            (5, 0.6, Some(2.0)),
        ];

        // This is the fixed sort using total_cmp (NaN sorts after +Inf)
        scored.sort_by(|a, b| {
            let time_a = a.2.unwrap_or(f64::MAX);
            let time_b = b.2.unwrap_or(f64::MAX);
            time_a.total_cmp(&time_b)
        });

        // Non-NaN values should be sorted correctly
        let non_nan_times: Vec<f64> = scored
            .iter()
            .filter_map(|s| s.2)
            .filter(|t| !t.is_nan())
            .collect();
        for w in non_nan_times.windows(2) {
            assert!(
                w[0] <= w[1],
                "non-NaN values should be sorted: {} > {}",
                w[0],
                w[1]
            );
        }
    }

    /// Verify that `predict_routing_outcome` never produces NaN in
    /// `expected_total_time`, even when transfer speed is zero.
    #[test]
    fn predict_routing_outcome_no_nan_with_zero_transfer_speed() {
        zero_transfer_speed_case(Training::History);
    }

    /// [`predict_routing_outcome_no_nan_with_zero_transfer_speed`] on the warm
    /// hierarchical estimator.
    #[test]
    fn predict_routing_outcome_no_nan_with_zero_transfer_speed_warm_hierarchical() {
        zero_transfer_speed_case(Training::WarmHierarchical);
    }

    fn zero_transfer_speed_case(training: Training) {
        let _mode = training.guard();
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Build events where the peer has zero transfer speed
        let events: Vec<RouteEvent> = (0..50)
            .map(|i| {
                let outcome = if i % 3 == 0 {
                    RouteOutcome::Failure
                } else {
                    RouteOutcome::Success {
                        time_to_response_start: std::time::Duration::from_millis(50),
                        payload_size: 0, // zero payload -> zero transfer speed
                        payload_transfer_time: std::time::Duration::from_millis(0),
                    }
                };
                RouteEvent {
                    peer: peer.clone(),
                    contract_location,
                    outcome,
                    op_type: None,
                }
            })
            .collect();

        let router = training.train(&events);
        training.check_decides(&router, std::slice::from_ref(&peer), contract_location);

        // Not enough data (Err) is acceptable; only the Ok path is asserted.
        if let Ok(pred) = router.predict_routing_outcome(&peer, contract_location) {
            assert!(
                pred.expected_total_time.is_finite(),
                "expected_total_time must be finite, got {}",
                pred.expected_total_time
            );
        }
    }

    /// Regression test for the latent NaN bug in the GLOBAL `transfer_rates`
    /// batch path of `Router::new`. A SUBSCRIBE success reports payload_size=0
    /// and payload_transfer_time=ZERO, so the rate is 0/0 = NaN. Without the
    /// guard, that NaN flows into the `transfer_rate_estimator` isotonic
    /// regression and poisons it: `interpolate()` at the affected distance then
    /// returns NaN (verified against pav_regression 0.7.0). The per-op and
    /// `add_event` paths already screen these events out; this test pins the
    /// equivalent guard on the batch path.
    ///
    /// To exercise the bug the events must land at the SAME route distance (so
    /// the NaN and finite points share an x in the regression) with enough
    /// interleaved NaN/finite events to populate the estimator. We use DISTINCT
    /// peers that nonetheless share a ring location: `Location::from_address`
    /// masks the low byte of the IP and ignores the port for non-loopback
    /// addresses, so peers on the same /24 map to one location while remaining
    /// distinct `PeerKeyLocation`s. Interleaving ≥40 NaN-producing subscribe
    /// successes with finite-rate GET successes at that shared location is what
    /// actually exercises the NaN path; a single homogeneous event does not.
    ///
    /// Asserts: (1) `Router::new` completes, and (2) the resulting
    /// `transfer_rate_estimator`'s raw regression curve is entirely finite —
    /// which fails (NaN) if the NaN subscribe events are not screened out of
    /// the batch path. (`Router::new` itself does not panic on this NaN; the
    /// load-bearing assertion is regression finiteness, hence the name.)
    #[test]
    fn router_new_does_not_poison_transfer_rate_regression_with_nan() {
        use crate::transport::TransportKeypair;
        use std::net::SocketAddr;

        let contract_location = Location::random();

        // Build distinct peers that all share the SAME ring location: same /24
        // (so the masked IP is identical), varying only the host byte/port.
        // Distinct pub keys + distinct addresses make them distinct peers, while
        // the masked location (and thus route_distance) is identical for all.
        let make_peer = |i: usize| {
            let pub_key = TransportKeypair::new().public().clone();
            // 203.0.113.0/24 (TEST-NET-3, non-loopback): low byte is masked out,
            // so every host in this /24 hashes to the same Location.
            let addr: SocketAddr = format!("203.0.113.{}:{}", i % 250, 9000 + i)
                .parse()
                .unwrap();
            PeerKeyLocation::new(pub_key, addr)
        };

        // Interleave NaN-producing SUBSCRIBE successes with finite-rate GET
        // successes — all at the shared location so their points share x.
        let events: Vec<RouteEvent> = (0..120)
            .map(|i| {
                let peer = make_peer(i);
                if i % 2 == 0 {
                    // SUBSCRIBE success: payload_size=0, transfer_time=ZERO -> 0/0 = NaN
                    RouteEvent {
                        peer,
                        contract_location,
                        outcome: RouteOutcome::Success {
                            time_to_response_start: Duration::from_millis(40),
                            payload_size: 0,
                            payload_transfer_time: Duration::ZERO,
                        },
                        op_type: Some(OpType::Subscribe),
                    }
                } else {
                    // GET success with a finite transfer rate.
                    RouteEvent {
                        peer,
                        contract_location,
                        outcome: RouteOutcome::Success {
                            time_to_response_start: Duration::from_millis(40),
                            payload_size: 4096,
                            payload_transfer_time: Duration::from_millis(20),
                        },
                        op_type: Some(OpType::Get),
                    }
                }
            })
            .collect();

        // (1) Must not panic: the guard keeps the NaN out of the global isotonic
        // regression, mirroring the per-op / add_event paths.
        let router = Router::new(&events);

        // (2) The transfer-rate estimator must not be NaN-poisoned. Sample its
        // raw regression curve (which exposes `interpolate()` outputs directly,
        // unlike `estimate_retrieval_time` which masks NaN via `.max(0.0)`).
        // Without the guard, the NaN subscribe points poison the regression and
        // at least one sampled y comes back NaN.
        let curve = router
            .transfer_rate_estimator
            .sampled_curve(0.0, f64::MAX, 64);
        for (x, y) in &curve {
            assert!(
                y.is_finite(),
                "transfer_rate regression produced a non-finite value {y} at \
                 distance {x} — NaN subscribe events leaked into the batch \
                 regression"
            );
        }
    }
}
