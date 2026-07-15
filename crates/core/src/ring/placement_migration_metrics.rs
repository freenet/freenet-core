//! Placement-migration observability (#4404 follow-up).
//!
//! The SubscribeHint placement migration nudges a closer-to-the-key neighbor to
//! subscribe-and-host a contract, so hosting drifts toward each contract's ideal
//! ring location over time. That effect was previously invisible to telemetry
//! (SubscribeHint is `EventKind::Ignored`), so this module adds two cheap,
//! low-overhead observability primitives that piggyback on the existing 5-minute
//! `router_snapshot` gauge cadence — no new per-message events:
//!
//! 1. [`PlacementMigrationMetrics`] — cumulative `AtomicU64` counters
//!    (`sent` / `received` / `acted` / `renewal_terminus_satisfied`, plus the
//!    #4534 diagnostic breakdowns: per-gate refusal reasons and directed-
//!    subscribe success/failure) incremented at the migration sites, the renewal
//!    driver, and the SubscribeHint admission gates, read once per snapshot. Lets
//!    us trend whether the migration is firing and at what rate, why inbound
//!    hints are refused, whether acted migrations succeed, and how often a
//!    subscription-root renewal short-circuits (#4440 proposal 1).
//! 2. [`placement_quality`] — a pure function over this node's ring location and
//!    the locations of the contracts it hosts. Produces the host-to-hosted-key
//!    ring-distance distribution (median / p90 / fraction within 0.1 / min /
//!    mean). If the migration works, that distribution tightens over time.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative lifetime counters tracking placement-migration activity.
///
/// Constructed once per node in `Ring::new` and shared via `Arc`: the migration
/// SEND site (`p2p_protoc::migration::consider_contract_migration`), the
/// RECEIVE/admission-gate sites (`node::process_message`), and the directed-
/// subscribe driver (`operations::subscribe::op_ctx_task`) reach it through
/// `op_manager.ring`, while `emit_router_snapshot_telemetry` reads it. Counters
/// are monotonic lifetime totals — the collector differences them across the
/// snapshot cadence to derive a rate. Cheap `Relaxed` atomics; never blocks.
#[derive(Debug, Default)]
pub(crate) struct PlacementMigrationMetrics {
    /// Incremented each time this node dispatches a `SubscribeHint` nudge to a
    /// closer-to-the-key neighbor.
    sent: AtomicU64,
    /// Incremented for every inbound `SubscribeHint` this node receives, counted
    /// before any admission gate (version / already-hosted / holder / cache).
    received: AtomicU64,
    /// Incremented only when an inbound `SubscribeHint` actually triggers a
    /// directed subscribe (the migration is acted upon, not dropped by a gate).
    acted: AtomicU64,
    /// Incremented each time a subscription-renewal cycle short-circuits because
    /// this node is the body-holding subscription root for the contract (#4440
    /// proposal 1). A body-holding root has no peer closer than itself to
    /// subscribe to, so renewing an upstream subscription would route greedily
    /// toward the contract, dead-end, and retry — the renewal storm. The root
    /// instead satisfies its renewal locally and sends no wire request; this
    /// counter trends how much renewal traffic that removes.
    renewal_terminus_satisfied: AtomicU64,
    /// Per-gate refusal counters (#4534 diagnostics): which admission gate dropped
    /// an inbound `SubscribeHint`. Each is incremented at the matching
    /// `return Ok(())` in the SubscribeHint receive arm, so together they
    /// partition `received - acted` by reason and let us see, post-release, why
    /// hints are being refused (e.g. cache-admission vs already-hosting). Surfaced
    /// only in the existing 5-min `router_snapshot` — no per-event volume.
    received_refused_version_floor: AtomicU64,
    received_refused_already_hosting: AtomicU64,
    received_refused_holder_mismatch: AtomicU64,
    received_refused_cache_admission: AtomicU64,
    /// Directed-subscribe outcome counters (#4534 diagnostics): of the hints this
    /// node ACTED on, how many directed subscribes completed vs failed
    /// (timeout / infrastructure error folded into failed). Incremented in the
    /// outcome match of `start_directed_subscribe`; lets us tell whether acted
    /// migrations actually succeed in hosting the contract.
    acted_succeeded: AtomicU64,
    acted_failed: AtomicU64,
}

/// A point-in-time read of [`PlacementMigrationMetrics`] for telemetry emission.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PlacementMigrationSnapshot {
    pub sent: u64,
    pub received: u64,
    pub acted: u64,
    pub renewal_terminus_satisfied: u64,
    pub received_refused_version_floor: u64,
    pub received_refused_already_hosting: u64,
    pub received_refused_holder_mismatch: u64,
    pub received_refused_cache_admission: u64,
    pub acted_succeeded: u64,
    pub acted_failed: u64,
}

impl PlacementMigrationMetrics {
    /// Record that this node dispatched a `SubscribeHint` nudge.
    #[inline]
    pub(crate) fn record_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that this node received an inbound `SubscribeHint` (any inbound
    /// hint, counted before the admission gates).
    #[inline]
    pub(crate) fn record_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an inbound `SubscribeHint` was acted on (a directed subscribe
    /// was started).
    #[inline]
    pub(crate) fn record_acted(&self) {
        self.acted.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a renewal cycle short-circuited because this node is the
    /// body-holding subscription root for the contract (#4440 proposal 1).
    #[inline]
    pub(crate) fn record_renewal_terminus_satisfied(&self) {
        self.renewal_terminus_satisfied
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an inbound `SubscribeHint` was refused because this node's
    /// version is below the SubscribeHint re-enable floor (#4534 diagnostics).
    #[inline]
    pub(crate) fn record_refused_version_floor(&self) {
        self.received_refused_version_floor
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an inbound `SubscribeHint` was refused because this node
    /// already hosts the contract (#4534 diagnostics).
    #[inline]
    pub(crate) fn record_refused_already_hosting(&self) {
        self.received_refused_already_hosting
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an inbound `SubscribeHint` was refused because the hint's
    /// holder is not the sender (#4534 diagnostics).
    #[inline]
    pub(crate) fn record_refused_holder_mismatch(&self) {
        self.received_refused_holder_mismatch
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an inbound `SubscribeHint` was refused by the module-cache
    /// admission gate (occupancy at/above the ceiling) (#4534 diagnostics).
    #[inline]
    pub(crate) fn record_refused_cache_admission(&self) {
        self.received_refused_cache_admission
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an acted-on directed subscribe completed (now hosting) (#4534
    /// diagnostics).
    #[inline]
    pub(crate) fn record_acted_succeeded(&self) {
        self.acted_succeeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an acted-on directed subscribe failed (error / infra / timeout)
    /// (#4534 diagnostics).
    #[inline]
    pub(crate) fn record_acted_failed(&self) {
        self.acted_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Read all counters for telemetry.
    pub(crate) fn snapshot(&self) -> PlacementMigrationSnapshot {
        PlacementMigrationSnapshot {
            sent: self.sent.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            acted: self.acted.load(Ordering::Relaxed),
            renewal_terminus_satisfied: self.renewal_terminus_satisfied.load(Ordering::Relaxed),
            received_refused_version_floor: self
                .received_refused_version_floor
                .load(Ordering::Relaxed),
            received_refused_already_hosting: self
                .received_refused_already_hosting
                .load(Ordering::Relaxed),
            received_refused_holder_mismatch: self
                .received_refused_holder_mismatch
                .load(Ordering::Relaxed),
            received_refused_cache_admission: self
                .received_refused_cache_admission
                .load(Ordering::Relaxed),
            acted_succeeded: self.acted_succeeded.load(Ordering::Relaxed),
            acted_failed: self.acted_failed.load(Ordering::Relaxed),
        }
    }
}

/// Ring-distance threshold below which a hosted contract is considered "close"
/// to this node, for the `frac_within_0_1` summary statistic. Chosen as the
/// clearest single "are hosted contracts near their host" number: 0.1 is 20% of
/// the maximum possible ring distance (0.5), so a tightening fraction-within-0.1
/// over time is direct evidence the placement migration is working.
pub(crate) const CLOSE_RING_DISTANCE: f64 = 0.1;

/// Summary of the distribution of ring distances between this node's location
/// and the ring locations of the contracts it currently hosts.
///
/// All distances are ring distances in `[0.0, 0.5]` (the maximum distance on the
/// unit ring). Emitted as gauge fields on the 5-minute `router_snapshot`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct HostedKeyDistanceStats {
    /// Number of hosted contracts the distribution was computed over.
    pub count: u64,
    /// Median host-to-hosted-key ring distance.
    pub median: f64,
    /// 90th-percentile host-to-hosted-key ring distance.
    pub p90: f64,
    /// Smallest host-to-hosted-key ring distance.
    pub min: f64,
    /// Mean host-to-hosted-key ring distance.
    pub mean: f64,
    /// Fraction of hosted contracts within [`CLOSE_RING_DISTANCE`] of this node.
    pub frac_within_0_1: f64,
}

/// Compute the host-to-hosted-key ring-distance distribution.
///
/// `node_location` is this node's own ring location in `[0.0, 1.0]`.
/// `contract_locations` are the ring locations (also in `[0.0, 1.0]`) of the
/// contracts this node currently hosts. Returns `None` when the node hosts
/// nothing (no distribution to summarize) — the caller emits `count = 0` and
/// leaves the distance gauges absent.
///
/// Distances use the same wrap-around ring metric as the rest of the crate
/// (`Location::distance`), so the result is in `[0.0, 0.5]`.
///
/// # Example
///
/// ```ignore
/// let stats = placement_quality(0.5, &[0.5, 0.55, 0.9]).unwrap();
/// assert_eq!(stats.count, 3);
/// ```
pub(crate) fn placement_quality(
    node_location: f64,
    contract_locations: &[f64],
) -> Option<HostedKeyDistanceStats> {
    if contract_locations.is_empty() {
        return None;
    }

    let node = crate::ring::Location::new_rounded(node_location);
    let mut distances: Vec<f64> = contract_locations
        .iter()
        .map(|&loc| {
            node.distance(crate::ring::Location::new_rounded(loc))
                .as_f64()
        })
        .collect();

    // Sort for percentile extraction. NaN is impossible here: ring distances
    // are finite values in [0.0, 0.5], so `total_cmp` gives a total order.
    distances.sort_by(|a, b| a.total_cmp(b));

    let count = distances.len();
    let sum: f64 = distances.iter().sum();
    let mean = sum / count as f64;
    let min = distances[0];
    let within = distances
        .iter()
        .filter(|&&d| d <= CLOSE_RING_DISTANCE)
        .count();
    let frac_within_0_1 = within as f64 / count as f64;

    Some(HostedKeyDistanceStats {
        count: count as u64,
        median: percentile(&distances, 0.5),
        p90: percentile(&distances, 0.9),
        min,
        mean,
        frac_within_0_1,
    })
}

/// Nearest-rank percentile of an already-sorted, non-empty slice.
///
/// `q` is in `[0.0, 1.0]`. Uses the nearest-rank method (rank = ceil(q * n),
/// clamped to `[1, n]`), which is exact for the small distributions this is used
/// on and avoids interpolation ambiguity. The slice MUST be sorted ascending and
/// non-empty (callers guarantee both).
fn percentile(sorted: &[f64], q: f64) -> f64 {
    debug_assert!(!sorted.is_empty(), "percentile of empty slice");
    debug_assert!((0.0..=1.0).contains(&q), "percentile q out of range");
    let n = sorted.len();
    // Nearest-rank: smallest index whose cumulative share is >= q.
    let rank = (q * n as f64).ceil() as usize;
    let idx = rank.clamp(1, n) - 1;
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_quality_empty_is_none() {
        assert!(placement_quality(0.5, &[]).is_none());
    }

    #[test]
    fn placement_quality_single_contract_at_node() {
        // A single contract exactly at the node's location: distance 0.
        let stats = placement_quality(0.5, &[0.5]).expect("non-empty");
        assert_eq!(stats.count, 1);
        assert!((stats.median - 0.0).abs() < 1e-12);
        assert!((stats.p90 - 0.0).abs() < 1e-12);
        assert!((stats.min - 0.0).abs() < 1e-12);
        assert!((stats.mean - 0.0).abs() < 1e-12);
        assert!((stats.frac_within_0_1 - 1.0).abs() < 1e-12);
    }

    #[test]
    fn placement_quality_known_distribution() {
        // Node at 0.5. Contracts at distances 0.0, 0.05, 0.1, 0.2, 0.4 from it.
        // Place them on the +side so the wrap-around picks the direct arc.
        let node = 0.5;
        let contracts = [0.5, 0.55, 0.6, 0.7, 0.9];
        let stats = placement_quality(node, &contracts).expect("non-empty");

        assert_eq!(stats.count, 5);
        // Sorted distances: [0.0, 0.05, 0.1, 0.2, 0.4]
        // Nearest-rank median (q=0.5): rank=ceil(2.5)=3 → idx 2 → 0.1
        assert!((stats.median - 0.1).abs() < 1e-9, "median={}", stats.median);
        // p90 (q=0.9): rank=ceil(4.5)=5 → idx 4 → 0.4
        assert!((stats.p90 - 0.4).abs() < 1e-9, "p90={}", stats.p90);
        assert!((stats.min - 0.0).abs() < 1e-12, "min={}", stats.min);
        // mean = (0.0+0.05+0.1+0.2+0.4)/5 = 0.15
        assert!((stats.mean - 0.15).abs() < 1e-9, "mean={}", stats.mean);
        // within 0.1 (inclusive): 0.0, 0.05, 0.1 → 3 of 5 = 0.6
        assert!(
            (stats.frac_within_0_1 - 0.6).abs() < 1e-9,
            "frac_within_0_1={}",
            stats.frac_within_0_1
        );
    }

    #[test]
    fn placement_quality_uses_wrap_around_distance() {
        // Node near the top of the ring; a contract just past 1.0 wrap.
        // Node 0.95, contract 0.02 → direct arc would be 0.93, wrap arc 0.07.
        let stats = placement_quality(0.95, &[0.02]).expect("non-empty");
        assert!(
            (stats.median - 0.07).abs() < 1e-9,
            "wrap distance median={}",
            stats.median
        );
        assert!(stats.median <= 0.5, "ring distance must be <= 0.5");
    }

    #[test]
    fn placement_quality_far_contracts_low_close_fraction() {
        // All contracts at the antipode region: none within 0.1.
        let stats = placement_quality(0.0, &[0.4, 0.45, 0.5]).expect("non-empty");
        assert_eq!(stats.count, 3);
        assert!(
            (stats.frac_within_0_1 - 0.0).abs() < 1e-12,
            "no contract should be within 0.1, frac={}",
            stats.frac_within_0_1
        );
        assert!(stats.min >= CLOSE_RING_DISTANCE);
    }

    #[test]
    fn percentile_nearest_rank_endpoints() {
        let sorted = [0.0, 0.1, 0.2, 0.3, 0.4];
        // q=0.0 → rank=ceil(0)=0 → clamp to 1 → idx 0
        assert_eq!(percentile(&sorted, 0.0), 0.0);
        // q=1.0 → rank=5 → idx 4
        assert_eq!(percentile(&sorted, 1.0), 0.4);
    }

    #[test]
    fn metrics_counters_increment_independently() {
        let m = PlacementMigrationMetrics::default();
        let s0 = m.snapshot();
        assert_eq!(
            (
                s0.sent,
                s0.received,
                s0.acted,
                s0.renewal_terminus_satisfied
            ),
            (0, 0, 0, 0)
        );

        m.record_sent();
        m.record_sent();
        m.record_received();
        m.record_acted();
        m.record_received();
        m.record_received();
        m.record_renewal_terminus_satisfied();
        m.record_renewal_terminus_satisfied();
        m.record_renewal_terminus_satisfied();
        m.record_renewal_terminus_satisfied();

        let s1 = m.snapshot();
        assert_eq!(s1.sent, 2, "sent");
        assert_eq!(s1.received, 3, "received");
        assert_eq!(s1.acted, 1, "acted");
        assert_eq!(
            s1.renewal_terminus_satisfied, 4,
            "renewal_terminus_satisfied"
        );
    }

    /// The #4534 diagnostic counters (per-gate refusals + directed-subscribe
    /// outcomes) each accumulate independently and surface in the snapshot.
    #[test]
    fn diagnostic_counters_increment_independently() {
        let m = PlacementMigrationMetrics::default();
        m.record_refused_version_floor();
        m.record_refused_already_hosting();
        m.record_refused_already_hosting();
        m.record_refused_holder_mismatch();
        m.record_refused_holder_mismatch();
        m.record_refused_holder_mismatch();
        m.record_refused_cache_admission();
        m.record_acted_succeeded();
        m.record_acted_failed();
        m.record_acted_failed();

        let s = m.snapshot();
        assert_eq!(s.received_refused_version_floor, 1, "refused_version_floor");
        assert_eq!(
            s.received_refused_already_hosting, 2,
            "refused_already_hosting"
        );
        assert_eq!(
            s.received_refused_holder_mismatch, 3,
            "refused_holder_mismatch"
        );
        assert_eq!(
            s.received_refused_cache_admission, 1,
            "refused_cache_admission"
        );
        assert_eq!(s.acted_succeeded, 1, "acted_succeeded");
        assert_eq!(s.acted_failed, 2, "acted_failed");
    }

    /// Source-pin the migration counter sites so a refactor that drops a
    /// `record_*` call (or moves an `acted` increment onto a gated branch) fails
    /// the build. The sites live in different files and on different control-flow
    /// branches, so an integration test would need a full multi-node migration to
    /// exercise them; pinning the call sites in source is the cheap, deterministic
    /// guard (same approach as
    /// `broadcast_to_single_peer_records_attempt_on_every_streaming_exit_pin` in
    /// `node/network_bridge/broadcast_queue.rs`).
    #[test]
    fn migration_counter_sites_present() {
        // SEND: exactly one `record_sent()` in the migration helper module.
        let migration_src = include_str!("../node/network_bridge/p2p_protoc/migration.rs");
        assert_eq!(
            migration_src
                .matches(".placement_migration_metrics()")
                .count(),
            1,
            "the SubscribeHint SEND site must reach the placement-migration metrics exactly once"
        );
        assert_eq!(
            migration_src.matches(".record_sent()").count(),
            1,
            "the SubscribeHint SEND site must increment `sent` exactly once"
        );

        // RECEIVE + ACTED: both live in the `SubscribeHint` arm of
        // `node::process_message`. Scope to that arm so unrelated mentions don't
        // count. `received` is at the top (before the gates); `acted` is on the
        // branch that starts the directed subscribe.
        let node_src = include_str!("../node.rs");
        let arm_start = node_src
            .find("NetMessageV1::SubscribeHint(hint) =>")
            .expect("SubscribeHint arm present in node.rs");
        let after = &node_src[arm_start..];
        // The arm ends at the next top-level match arm (`Aborted`).
        let arm_end = after
            .find("NetMessageV1::Aborted(tx) =>")
            .expect("Aborted arm follows SubscribeHint arm");
        let arm = &after[..arm_end];
        assert_eq!(
            arm.matches(".record_received()").count(),
            1,
            "the SubscribeHint RECEIVE site must increment `received` exactly once \
             (counted before the admission gates)"
        );
        assert_eq!(
            arm.matches(".record_acted()").count(),
            1,
            "the SubscribeHint ACTED site must increment `acted` exactly once \
             (only on the branch that starts the directed subscribe)"
        );
        // The `acted` increment must come AFTER the directed-subscribe log line,
        // i.e. on the act branch past all gates — not at the top with `received`.
        let acted_at = arm.find(".record_acted()").expect("acted present");
        let act_log_at = arm
            .find("Received SubscribeHint — starting directed subscribe to holder")
            .expect("act-branch log present");
        assert!(
            acted_at > act_log_at,
            "`record_acted()` must sit on the act branch (after the directed-subscribe \
             log), so gated/dropped hints are not counted as acted"
        );

        // REFUSAL breakdown (#4534 diagnostics): each per-gate refusal counter is
        // incremented exactly once, at its matching `return Ok(())` in the arm.
        for method in [
            ".record_refused_version_floor()",
            ".record_refused_already_hosting()",
            ".record_refused_holder_mismatch()",
            ".record_refused_cache_admission()",
        ] {
            assert_eq!(
                arm.matches(method).count(),
                1,
                "the SubscribeHint arm must call `{method}` exactly once (at its refusal gate)"
            );
        }

        // DIRECTED-SUBSCRIBE OUTCOME breakdown (#4534 diagnostics): the outcome
        // match in `start_directed_subscribe` records success once and failure on
        // BOTH the Publish(Err) and InfrastructureError arms.
        let directed_src = include_str!("../operations/subscribe/op_ctx_task.rs");
        assert_eq!(
            directed_src.matches(".record_acted_succeeded()").count(),
            1,
            "start_directed_subscribe must record `acted_succeeded` exactly once"
        );
        assert_eq!(
            directed_src.matches(".record_acted_failed()").count(),
            2,
            "start_directed_subscribe must record `acted_failed` on BOTH the error \
             and infrastructure-error outcome arms"
        );
    }
}
