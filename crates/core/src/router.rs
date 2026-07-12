mod isotonic_estimator;
mod routing_predictor;
mod util;

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::GlobalRng;
use crate::node::network_status::OpType;
use crate::ring::{Distance, Location, PeerKeyLocation};
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

/// Periodic snapshot of the router model state for telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct RouterSnapshotInfo {
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
    /// Background-task health gauges (#4440), populated by `Ring` from the
    /// process-global `BACKGROUND_TASK_HEALTH` the monitored tasks publish into.
    /// `refresh_router`'s transient `get_router_events` errors were made
    /// non-fatal by the v0.2.74 #4438 hotfix, so a persistently-failing refresh
    /// is now silent; these make it observable (partially addresses #4440 (a)).
    /// `last_success_age_secs` is seconds since the last successful run (`None`
    /// until the first success); `consecutive_failures` is the current run of
    /// failures since the last success.
    pub refresh_router_last_success_age_secs: Option<u64>,
    pub refresh_router_consecutive_failures: Option<u64>,
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
}

impl Clone for Router {
    fn clone(&self) -> Self {
        // RoutingPredictor is not cloneable. When Router is cloned (e.g., for
        // batch reconstruction from history), the predictor starts empty and
        // gets rebuilt as events are added.
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
        }
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
            renegade_predictor.record_at_time(
                &event.peer,
                event.contract_location,
                distance,
                outcome,
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
            per_op_failure: per_op_failure
                .into_iter()
                .map(|(k, v)| (k, IsotonicEstimator::new(v, EstimatorType::Positive)))
                .collect(),
            per_op_response_time: per_op_response_time
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        IsotonicEstimator::new_with_mode(
                            v,
                            EstimatorType::Positive,
                            AdjustmentMode::Multiplicative,
                        ),
                    )
                })
                .collect(),
            per_op_transfer_rate: per_op_transfer_rate
                .into_iter()
                .map(|(k, v)| (k, IsotonicEstimator::new(v, EstimatorType::Negative)))
                .collect(),
            renegade_predictor,
        }
    }

    #[allow(dead_code)]
    pub fn considering_n_closest_peers(mut self, n: u32) -> Self {
        self.consider_n_closest_peers = n as usize;
        self
    }

    pub fn add_event(&mut self, event: RouteEvent) {
        let was_below_threshold = !self.has_sufficient_routing_events();
        let op_type = event.op_type;

        // Feed renegade predictor (before isotonic, which moves event.peer)
        let distance = event
            .peer
            .location()
            .map(|loc| event.contract_location.distance(loc).as_f64())
            .unwrap_or(0.5);

        let (renegade_outcome, _) =
            routing_predictor::RoutingOutcome::from_route_outcome(&event.outcome);
        self.renegade_predictor.record(
            &event.peer,
            event.contract_location,
            distance,
            renegade_outcome,
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
                    self.per_op_response_time
                        .entry(ot)
                        .or_insert_with(|| {
                            // Multiplicative to match the global response-time estimator.
                            IsotonicEstimator::new_with_mode(
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
                            IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive)
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
                                IsotonicEstimator::new(std::iter::empty(), EstimatorType::Negative)
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
                            IsotonicEstimator::new(std::iter::empty(), EstimatorType::Positive)
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

    fn select_closest_peers<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: &Location,
    ) -> Vec<&'a PeerKeyLocation> {
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
        peer_distances.truncate(k);
        peer_distances.sort_by_key(|&(_, distance)| distance);
        peer_distances.into_iter().map(|(peer, _)| peer).collect()
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

    fn predict_routing_outcome(
        &self,
        peer: &PeerKeyLocation,
        target_location: Location,
    ) -> Result<RoutingPrediction, RoutingError> {
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

        // Clamp before using in cost formulas — per-peer EWMA adjustments can
        // push the raw estimate slightly outside [0, 1].
        let mut failure_estimate = failure_estimate.clamp(0.0, 1.0);
        let isotonic_failure = failure_estimate;
        let failure_cost_multiplier = 3.0;

        // Blend renegade prediction if available. The renegade predictor captures
        // peer × contract interactions that the global isotonic model cannot see
        // (e.g., a peer selectively dropping requests for specific contracts).
        let distance = peer
            .location()
            .map(|loc| target_location.distance(loc).as_f64())
            .unwrap_or(0.5);
        let renegade = self
            .renegade_predictor
            .predict(peer, target_location, distance);

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

        let mut time_to_response_start = time_estimate.unwrap_or(0.0);
        let mut xfer_speed = transfer_estimate.unwrap_or(0.0);

        // Blend renegade timing predictions if available (only when finite and positive)
        if let Some(renegade_time) = renegade.time_to_response_start {
            if time_estimate.is_some() && renegade_time.is_finite() && renegade_time >= 0.0 {
                let w = self.renegade_predictor.response_time_weight();
                time_to_response_start = time_to_response_start * (1.0 - w) + renegade_time * w;
            }
        }
        if let Some(renegade_speed) = renegade.transfer_speed {
            if transfer_estimate.is_some() && renegade_speed.is_finite() && renegade_speed > 0.0 {
                let w = self.renegade_predictor.transfer_speed_weight();
                xfer_speed = xfer_speed * (1.0 - w) + renegade_speed * w;
            }
        }

        let expected_total_time = if time_estimate.is_some() && transfer_estimate.is_some() {
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
                + (time_to_response_start * failure_estimate * failure_cost_multiplier)
        } else {
            failure_estimate * failure_cost_multiplier
        };

        Ok(RoutingPrediction {
            failure_probability: failure_estimate,
            xfer_speed: TransferSpeed {
                bytes_per_second: xfer_speed,
            },
            time_to_response_start,
            expected_total_time,
            renegade_failure_adjustment,
        })
    }

    /// Like `select_k_best_peers` but also returns a `RoutingDecisionInfo` for telemetry.
    pub fn select_k_best_peers_with_telemetry<'a>(
        &self,
        peers: impl IntoIterator<Item = &'a PeerKeyLocation>,
        target_location: Location,
        k: usize,
    ) -> (Vec<&'a PeerKeyLocation>, RoutingDecisionInfo) {
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
            (selected, decision)
        } else {
            let closest = self.select_closest_peers(peers, &target_location);
            let mut fallback_count = 0;

            let mut scored: Vec<(&'a PeerKeyLocation, f64, Option<RoutingPrediction>)> = closest
                .iter()
                .map(|peer| {
                    let distance = peer
                        .location()
                        .map(|loc| target_location.distance(loc).as_f64())
                        .unwrap_or(0.5);
                    match self.predict_routing_outcome(peer, target_location) {
                        Ok(pred) => (*peer, distance, Some(pred)),
                        Err(_) => {
                            fallback_count += 1;
                            (*peer, distance, None)
                        }
                    }
                })
                .collect();

            // Sort: peers with predictions by expected_total_time, others at the end
            scored.sort_by(|a, b| {
                let time_a = a.2.map(|p| p.expected_total_time).unwrap_or(f64::MAX);
                let time_b = b.2.map(|p| p.expected_total_time).unwrap_or(f64::MAX);
                time_a.total_cmp(&time_b)
            });

            let strategy = if fallback_count == 0 {
                RoutingStrategy::PredictionBased
            } else {
                // Some or all predictions failed; using distance as tiebreaker
                RoutingStrategy::PredictionFallback
            };

            let candidates: Vec<RoutingCandidate> = scored
                .iter()
                .enumerate()
                .map(|(i, (_, dist, pred))| RoutingCandidate {
                    distance: *dist,
                    prediction: pred.map(RoutingPredictionInfo::from),
                    selected: i < k,
                })
                .collect();

            scored.truncate(k);
            let selected: Vec<&'a PeerKeyLocation> =
                scored.into_iter().map(|(peer, _, _)| peer).collect();

            let decision = RoutingDecisionInfo {
                target_location: target_location.as_f64(),
                strategy,
                candidates,
                total_routing_events,
            };
            (selected, decision)
        }
    }

    /// Produce a snapshot of the router model state for telemetry.
    pub fn snapshot(&self) -> RouterSnapshotInfo {
        RouterSnapshotInfo {
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
                        let p = est.sampled_curve(0.0, 1.0, 50);
                        c.failure_curve = p;
                        c.failure_data_range = est.data_x_range();
                        c.failure_events = est.len();
                        c.failure_points = est.sampled_raw_points(100);
                    }
                    if let Some(est) = self.per_op_response_time.get(&ot) {
                        let p = est.sampled_curve(0.0, f64::INFINITY, 50);
                        c.response_time_curve = p;
                        c.response_time_data_range = est.data_x_range();
                        c.response_time_events = est.len();
                        c.response_time_points = est.sampled_raw_points(100);
                    }
                    if let Some(est) = self.per_op_transfer_rate.get(&ot) {
                        let p = est.sampled_curve(0.0, f64::INFINITY, 50);
                        c.transfer_rate_curve = p;
                        c.transfer_rate_data_range = est.data_x_range();
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
            // Terminal advertisement-consult counters (piece C, #4646),
            // populated by Ring from the network_status singleton on the
            // snapshot cadence (#4658).
            terminal_consult_attempts: None,
            terminal_consult_hits: None,
            terminal_consult_resolved_found: None,
            terminal_consult_still_not_found: None,
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
            broadcast_stream_failures_last_snapshot: None,
            refresh_router_last_success_age_secs: None,
            refresh_router_consecutive_failures: None,
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
    use crate::ring::Distance;

    use super::*;

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

            let window = router.select_closest_peers(&peers, &target);
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

    /// Train a `Router` on a synthetic history whose global shape matches what
    /// the production isotonic regression learns — a distance->failure gradient
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
    ) -> (f64, f64) {
        let router = Router::new(history).considering_n_closest_peers(window as u32);
        assert!(
            router.has_sufficient_routing_events(),
            "trained router below prediction threshold ({} events)",
            router.failure_estimator.len()
        );
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

        let (mean_pct, p90_pct) = measure_convergence(&history, &pool, 25, 400);

        assert!(
            mean_pct < 0.18,
            "issue #4230 regression: at window=25 the chosen next hop's mean \
             rank-percentile is {mean_pct:.3} — the predictor is drifting away \
             from the closest tail (0.5 = median = no progress), so per-peer \
             history is overriding geographic locality and small-world routing \
             convergence has degraded"
        );
        assert!(
            p90_pct < 0.40,
            "issue #4230 regression: at window=25 the 90th-percentile chosen-hop \
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
                let (mean_pct, p90_pct) = measure_convergence(&history, &pool, window, 300);
                results.push((window, mean_pct, p90_pct));
            }

            // At EVERY swept window the chosen hop stays deep in the closest
            // tail — never near the median. This is the load-bearing
            // convergence property: widening the candidate window (at any EWMA
            // magnitude) does not break small-world routing.
            for &(window, mean_pct, p90_pct) in &results {
                assert!(
                    mean_pct < 0.20,
                    "issue #4230: at window={window} bias_mag={bias_mag} mean \
                     chosen-hop rank-percentile {mean_pct:.3} drifted toward the \
                     median — routing convergence degraded by candidate-window size"
                );
                assert!(
                    p90_pct < 0.42,
                    "issue #4230: at window={window} bias_mag={bias_mag} p90 \
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
                "issue #4230: at bias_mag={bias_mag} widening the candidate \
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
            router.select_closest_peers(&peers, &contract_location);

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

        let result = router.select_closest_peers(&empty_peers, &target);
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

        let router = Router::new(&events);

        // With 80 total events in failure_estimator (>= 50 threshold),
        // the router should use predictions and prefer peer A
        let peers = vec![peer_a.clone(), peer_b.clone()];
        let selected = router.select_peer(&peers, contract_location);
        assert!(selected.is_some());
        assert_eq!(
            *selected.unwrap(),
            peer_a,
            "Router should prefer peer A (all successes) over peer B (all failures)"
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

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Router should prefer the good peer via failure-probability prediction
        let peers = vec![good_peer.clone(), bad_peer.clone()];
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
    /// and that after enough failures the router predicts higher failure probability.
    #[test]
    fn test_failure_outcome_feeds_failure_estimator_with_value_one() {
        let peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let mut router = Router::new(&[]);

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
        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
        assert!(!selected.is_empty());
        let pred = decision.candidates[0].prediction.as_ref().unwrap();
        assert!(
            pred.failure_probability > 0.5,
            "After 60 failures, failure probability should be high, got {}",
            pred.failure_probability
        );
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

        let router = Router::new(&events);
        let (_, decision) = router.select_k_best_peers_with_telemetry(&peers, contract_location, 1);
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

        let router2 = Router::new(&events);
        assert!(router2.response_start_time_estimator.len() >= 5);
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
    }

    /// Simulate realistic post-#3137 traffic: a mix of timed GET successes, untimed
    /// PUT/SUBSCRIBE/UPDATE successes, and failures across multiple peers.
    /// The router should activate prediction-based routing and prefer
    /// low-failure, low-latency peers.
    #[test]
    fn test_realistic_mixed_traffic_routing() {
        // Seed RNG so peer ring distances are deterministic; without this the
        // isotonic regression's ascending monotonicity constraint can conflict
        // with the failure-rate ordering when random distances are adversarial.
        let _guard = crate::config::GlobalRng::seed_guard(0xCAFE_BABE);
        let contract_location = Location::random();
        let close_peer = PeerKeyLocation::random();
        let mid_peer = PeerKeyLocation::random();
        let far_peer = PeerKeyLocation::random();

        let mut events = Vec::new();

        // close_peer: 10 timed GET successes + 15 untimed PUT/SUB successes, 2 failures
        // → ~7% failure rate, good timing data
        for _ in 0..10 {
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
        for _ in 0..3 {
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

        // Total: 27 + 18 + 20 = 65 events > 50 threshold
        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());

        // failure_estimator should have all 65 events
        assert_eq!(router.failure_estimator.len(), 65);
        // timing estimators only have the timed GET successes: 10 + 3 = 13
        assert_eq!(router.response_start_time_estimator.len(), 13);
        assert_eq!(router.transfer_rate_estimator.len(), 13);

        // Router should use prediction-based routing and prefer close_peer
        let peers = vec![close_peer.clone(), mid_peer.clone(), far_peer.clone()];
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

        let router = Router::new(&events);
        assert!(router.has_sufficient_routing_events());
        // No timing data at all
        assert_eq!(router.response_start_time_estimator.len(), 0);
        assert_eq!(router.transfer_rate_estimator.len(), 0);

        // Router should still make predictions and prefer the reliable peer
        let peers = vec![reliable_peer.clone(), flaky_peer.clone()];
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
    /// and Failure events at a different contract location through the same peer,
    /// the failure estimator should track these as distinct (distance, result)
    /// data points. This validates the isotonic model learns location-dependent
    /// failure patterns using untimed success data from PUT/SUBSCRIBE/UPDATE.
    #[test]
    fn test_location_dependent_failure_patterns() {
        let peer = PeerKeyLocation::random();
        let near_contract = Location::new(0.01); // very close distance (close to 0)
        let far_contract = Location::new(0.49); // maximum distance on the ring

        let mut router = Router::new(&[]);

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

        // The isotonic model should give different failure predictions
        // for near vs far contracts through this peer
        let peers = vec![peer.clone()];

        let (_, near_decision) =
            router.select_k_best_peers_with_telemetry(&peers, near_contract, 1);
        let (_, far_decision) = router.select_k_best_peers_with_telemetry(&peers, far_contract, 1);

        let near_pred = near_decision.candidates[0]
            .prediction
            .as_ref()
            .expect("should have prediction for near contract");
        let far_pred = far_decision.candidates[0]
            .prediction
            .as_ref()
            .expect("should have prediction for far contract");

        // Near contract should have lower failure probability than far contract
        assert!(
            near_pred.failure_probability <= far_pred.failure_probability,
            "Near contract failure prob ({}) should be <= far contract failure prob ({})",
            near_pred.failure_probability,
            far_pred.failure_probability
        );
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

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Property: predictions never produce NaN or panic under any mix of
            /// operation outcomes fed to the router.
            #[test]
            fn predictions_never_nan_or_panic(
                outcomes in proptest::collection::vec(arb_route_outcome(), 60..200),
                seed in 0u64..u64::MAX,
            ) {
                let _guard = crate::config::GlobalRng::seed_guard(seed);
                let peers: Vec<PeerKeyLocation> =
                    (0..5).map(|_| PeerKeyLocation::random()).collect();
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

                let router = Router::new(&events);

                // Router should have enough events for prediction
                prop_assert!(router.has_sufficient_routing_events());

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

                let router = Router::new(&events);
                prop_assert!(router.has_sufficient_routing_events());

                let peers = vec![good_peer.clone(), bad_peer.clone()];
                let (selected, decision) =
                    router.select_k_best_peers_with_telemetry(&peers, contract_location, 2);

                let all_have_predictions = decision.candidates.iter()
                    .all(|c| c.prediction.is_some());

                // With enough data, predictions should exist for both
                if all_have_predictions && selected.len() == 2 {
                    // The first selected peer (lowest expected_total_time) should
                    // be the good peer since failures increase cost via the
                    // failure_cost_multiplier in predict_routing_outcome
                    prop_assert!(
                        *selected[0] == good_peer,
                        "Good peer (all successes) should be ranked first"
                    );
                }
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
                let _guard = crate::config::GlobalRng::seed_guard(seed);
                let peers: Vec<PeerKeyLocation> =
                    (0..3).map(|_| PeerKeyLocation::random()).collect();
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

                let router = Router::new(&events);
                if !router.has_sufficient_routing_events() {
                    return Ok(());
                }

                let (_, decision) =
                    router.select_k_best_peers_with_telemetry(&peers, contract_location, 3);

                for candidate in &decision.candidates {
                    if let Some(pred) = &candidate.prediction {
                        // Allow small float tolerance around [0, 1]
                        prop_assert!(
                            pred.failure_probability >= 0.0
                                && pred.failure_probability <= 1.0,
                            "failure_probability {} out of [0, 1] range",
                            pred.failure_probability
                        );
                    }
                }
            }
        }
    }

    /// Distance-based fallback should deprioritize peers that have failure history
    /// in the estimator, preferring untried peers.
    #[test]
    fn test_distance_fallback_deprioritizes_failed_peers() {
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

        let router = Router::new(&events);
        assert!(
            !router.has_sufficient_routing_events(),
            "Should still be in distance-based fallback mode"
        );
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

        let put_curves = &snap.per_op_curves["PUT"];
        assert!(put_curves.failure_events > 0);
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
    /// correctly implement a total order" in refresh_router.
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

        let router = Router::new(&events);

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
