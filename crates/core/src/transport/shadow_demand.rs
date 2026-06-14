//! Phase 1.6 outbound-demand / queue-depth / must-flow-vs-bulk shadow
//! telemetry for the outer-loop rate controller (issue #4074).
//!
//! Phases 1 and 1.5 (`rolling_rtt_stats.rs`, `reference_ping.rs`) measure
//! RTT-based contention signals. Phase 1.6 adds the *demand* side of the
//! floor analysis: how much the node is trying to push, how that compares
//! to the rate cap `R`, how deep the broadcast backlog is, and how the
//! outbound bytes split between traffic the controller can slow (bulk
//! contract transfers) and traffic it cannot (keepalives / control).
//!
//! This module owns three process-wide observation hooks, all written
//! from the hot send path with a single relaxed atomic add and read only
//! by the 1 Hz aggregator tasks below:
//!
//! - **Outbound class counters** (`record_outbound`): cumulative bytes
//!   split into `MustFlow` / `Short` / `Bulk`. Fed from `packet_sending`,
//!   `send_pong`, and the keep-alive `Ping` site.
//! - **Broadcast-queue depth gauge** (`record_broadcast_queue_depth`):
//!   updated by `broadcast_queue.rs` under its own lock so the shadow
//!   reader never contends on the queue mutex.
//! - **Global-bandwidth handle** (`register_global_bandwidth`): a `Weak`
//!   to the node's `GlobalBandwidthManager` so the demand aggregator can
//!   read the effective aggregate rate `R` when the node runs in
//!   global-pool mode.
//!
//! Two 1 Hz tasks emit the telemetry:
//!
//! - `shadow_rate_demand` — achieved outbound throughput vs `R`, plus
//!   active-connection count and broadcast-queue depth.
//! - `shadow_outbound_class` — the must-flow / short / bulk byte split.
//!
//! The OS-interface-tx signal (`shadow_iface_tx`) lives in the sibling
//! `shadow_iface_tx.rs` module because it is Linux-specific, gated, and
//! does file I/O.
//!
//! **Observation only.** Exactly like the Phase 1/1.5 shadow registry,
//! nothing in the production data path reads these counters or the
//! registered bandwidth handle back — the rule in
//! `.claude/rules/transport.md` ("NEVER read … from the production data
//! path") applies here too. The hot path only *writes* the cheap
//! counters; all reads happen in the aggregator tasks.
//!
//! ## What the class split covers — and what it does not
//!
//! The class counters are tagged at the data-path send choke point
//! (`packet_sending`, which carries `ShortMessage` / `NoOp` /
//! `StreamFragment`) plus the two bypass sites `send_pong` and the
//! keep-alive `Ping`. They deliberately do **not** cover: retransmits
//! (resent below `packet_sending`), the handshake / intro `AckConnection`
//! sends, or standalone connection ACKs. Those bytes are still counted in
//! the node total (`shadow_rate_demand.sent_bytes_per_sec`, derived from
//! `TRANSPORT_METRICS.cumulative_bytes_sent` at the socket layer), so the
//! split is a classified *subset* of the total, not the whole of it. The
//! excluded paths are connection-establishment or recovery traffic, not
//! steady-state data flow, so the #4074 floor lower bound (must-flow rate
//! *when not bulk-transferring*) is essentially unaffected.
//!
//! Note for the analysis consumer: `must_flow` here is strictly
//! transport-level liveness (`Ping` / `Pong` / `NoOp` / `AckConnection`).
//! The issue's conceptual "must-flow" also includes subscription
//! heartbeats and relay obligations, but those ride inside `ShortMessage`
//! / `StreamFragment` at the transport layer and so land in the `short` /
//! `bulk` buckets — the transport cannot see them. The `short` bucket
//! therefore likely needs partial apportionment to must-flow during the
//! offline analysis.
//!
//! ## Telemetry budget
//!
//! These two always-on emitters plus the Phase 1 RTT aggregator put three
//! 1 Hz shadow events (and, on a gateway with reference-ping and iface-tx
//! enabled, up to five) into an OTLP pipe rate-limited at 10 events/sec
//! with no priority ordering (`tracing/telemetry.rs`). On a busy gateway
//! some events — shadow or operational — may be dropped within a 1 s
//! window. Acceptable as a statistical tradeoff for shadow telemetry; a
//! shadow-event priority / sub-budget should land before Phase 2 adds
//! more always-on streams (tracked separately).
//!
//! ## Process-wide, single-node-per-process
//!
//! The class counters and the broadcast gauge are process-global statics,
//! matching the existing `TRANSPORT_METRICS.cumulative_bytes_sent` and
//! `TELEMETRY_SENDER` patterns. A real deployment is one node per process,
//! so the globals reflect that node. In multi-node in-process simulation
//! the globals are shared across nodes, but simulation builds initialise
//! no telemetry sender, so the events are dropped and nothing depends on
//! the values being per-node.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use super::global_bandwidth::GlobalBandwidthManager;
use super::symmetric_message::SymmetricMessagePayload;
use crate::node::background_task_monitor::BackgroundTaskMonitor;
use crate::transport::TRANSPORT_METRICS;

/// Classification of an outbound transport payload for the floor
/// lower/upper-bound analysis in #4074.
///
/// The split is by transport variant because that is all the transport
/// layer can see without parsing the encrypted application payload. It is
/// also the operationally meaningful axis: `Bulk` (stream fragments) is
/// the *only* class the token bucket meters today, i.e. the only outbound
/// traffic a future rate controller could actually slow. Everything else
/// flows unthrottled and therefore behaves as "must-flow" under the
/// current lever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutboundClass {
    /// Keepalive / liveness / handshake control that flows regardless of
    /// bulk transfer: `Ping`, `Pong`, `NoOp` (ACK carrier), and
    /// `AckConnection`. Not rate-limited today.
    MustFlow,
    /// `ShortMessage` — a serialized `NetMessage`. Opaque at the transport
    /// layer: it may be a control op (Connect / Subscribe / routing) or a
    /// small contract op (a sub-streaming-threshold GET / PUT / UPDATE).
    /// Not token-bucket metered today. Kept as its own bucket because the
    /// transport cannot split it further without decrypting and parsing
    /// the application payload — the analysis can fold it into must-flow
    /// or treat it separately.
    Short,
    /// `StreamFragment` — large contract / state payload, the only
    /// outbound class the token bucket meters, i.e. the slowable bulk.
    Bulk,
}

/// Classify an outbound payload by its transport variant.
///
/// Cheap: a single exhaustive match, no allocation, no lock. Safe to call
/// on the hot send path. Intentionally exhaustive (no wildcard) so a new
/// `SymmetricMessagePayload` variant forces a compile error here rather
/// than being silently miscounted.
pub(crate) fn classify(payload: &SymmetricMessagePayload) -> OutboundClass {
    match payload {
        SymmetricMessagePayload::StreamFragment { .. } => OutboundClass::Bulk,
        SymmetricMessagePayload::ShortMessage { .. } => OutboundClass::Short,
        SymmetricMessagePayload::NoOp
        | SymmetricMessagePayload::AckConnection { .. }
        | SymmetricMessagePayload::Ping { .. }
        | SymmetricMessagePayload::Pong { .. } => OutboundClass::MustFlow,
    }
}

/// Cumulative outbound byte counters, split by class. Monotonic; the
/// aggregator reports per-interval deltas.
struct OutboundCounters {
    must_flow_bytes: AtomicU64,
    short_bytes: AtomicU64,
    bulk_bytes: AtomicU64,
}

static OUTBOUND: OutboundCounters = OutboundCounters {
    must_flow_bytes: AtomicU64::new(0),
    short_bytes: AtomicU64::new(0),
    bulk_bytes: AtomicU64::new(0),
};

/// Record `bytes` of outbound traffic of the given class. One relaxed
/// atomic add — no allocation, no lock, no blocking. Called from the hot
/// send path after the bytes are actually put on the wire.
pub(crate) fn record_outbound(class: OutboundClass, bytes: usize) {
    let counter = match class {
        OutboundClass::MustFlow => &OUTBOUND.must_flow_bytes,
        OutboundClass::Short => &OUTBOUND.short_bytes,
        OutboundClass::Bulk => &OUTBOUND.bulk_bytes,
    };
    counter.fetch_add(bytes as u64, Ordering::Relaxed);
}

/// Snapshot of the cumulative class counters at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OutboundSnapshot {
    must_flow_bytes: u64,
    short_bytes: u64,
    bulk_bytes: u64,
}

fn snapshot_outbound() -> OutboundSnapshot {
    OutboundSnapshot {
        must_flow_bytes: OUTBOUND.must_flow_bytes.load(Ordering::Relaxed),
        short_bytes: OUTBOUND.short_bytes.load(Ordering::Relaxed),
        bulk_bytes: OUTBOUND.bulk_bytes.load(Ordering::Relaxed),
    }
}

/// Test-only accessor for the process-global class counters, returned as
/// `(must_flow, short, bulk)` cumulative byte totals. Used by the
/// send-path integration tests in `peer_connection.rs` to assert the
/// hot-path hooks feed the right counter. Callers use delta assertions to
/// stay robust against other tests touching these monotonic statics.
#[cfg(test)]
pub(crate) fn outbound_counters_snapshot() -> (u64, u64, u64) {
    let s = snapshot_outbound();
    (s.must_flow_bytes, s.short_bytes, s.bulk_bytes)
}

/// Bytes accumulated in each class between two snapshots.
///
/// `saturating_sub` guards the (practically impossible for u64, but free
/// to defend) wrap / reset case so a counter reset can never produce a
/// nonsensically huge delta.
fn outbound_delta(prev: OutboundSnapshot, now: OutboundSnapshot) -> (u64, u64, u64) {
    (
        now.must_flow_bytes.saturating_sub(prev.must_flow_bytes),
        now.short_bytes.saturating_sub(prev.short_bytes),
        now.bulk_bytes.saturating_sub(prev.bulk_bytes),
    )
}

/// Current broadcast-queue depth (entries pending fan-out). Updated by
/// `broadcast_queue.rs` under its queue lock; the default per-connection
/// build has no global queue so this stays at 0.
static BROADCAST_QUEUE_DEPTH: AtomicUsize = AtomicUsize::new(0);

/// Record the current broadcast-queue depth. Called from
/// `broadcast_queue.rs` *under the queue lock* after each enqueue / drain
/// mutation, so the gauge tracks the real depth while the shadow reader
/// (the demand aggregator) reads it lock-free and never contends on the
/// queue's async mutex.
///
/// The only production caller (`broadcast_queue.rs`) is gated
/// `#[cfg(not(feature = "simulation_tests"))]`, so under `simulation_tests`
/// the fn has no production caller and clippy's `-D warnings` would promote
/// the resulting `dead_code` warning to a hard error. The `cfg(test)` gauge
/// round-trip test below still exercises it; the `allow` only suppresses the
/// non-test simulation_tests build, leaving default-feature builds unchanged.
#[cfg_attr(feature = "simulation_tests", allow(dead_code))]
pub(crate) fn record_broadcast_queue_depth(depth: usize) {
    BROADCAST_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
}

/// Weak handle to the node's global bandwidth manager. `OnceLock` because
/// it is registered exactly once at construction; `Weak` so the shadow
/// side never extends the manager's lifetime. Unset under the default
/// per-connection FixedRate mode (no `total_bandwidth_limit` configured),
/// in which case the demand event's `R` fields are null.
static GLOBAL_BANDWIDTH: OnceLock<Weak<GlobalBandwidthManager>> = OnceLock::new();

/// Register the node's `GlobalBandwidthManager` so the demand aggregator
/// can read the effective aggregate rate `R`. Called once at GBM
/// construction. A no-op if a handle was already registered (only one GBM
/// exists per node).
pub(crate) fn register_global_bandwidth(manager: &Arc<GlobalBandwidthManager>) {
    // `set` returns `Err` if already initialised; that only happens if
    // construction ran twice (it doesn't — one GBM per node), so ignoring
    // the result is correct rather than merely convenient.
    if GLOBAL_BANDWIDTH.set(Arc::downgrade(manager)).is_err() {
        tracing::debug!(
            target: "freenet::transport::shadow_demand",
            "global bandwidth handle already registered; keeping the first"
        );
    }
}

fn global_bandwidth() -> Option<Arc<GlobalBandwidthManager>> {
    GLOBAL_BANDWIDTH.get().and_then(Weak::upgrade)
}

/// Number of live peer connections, read from the Phase 1 shadow RTT
/// registry (one entry per `RemoteConnection`). Always available,
/// independent of bandwidth mode, so the demand event can express
/// per-connection demand even when the global-pool `R` fields are null.
fn active_connections() -> usize {
    super::rolling_rtt_stats::SHADOW_RTT_REGISTRY.len()
}

/// 1 Hz cadence, matching the Phase 1/1.5 aggregators so all shadow
/// streams align at the collector.
const AGGREGATOR_INTERVAL: Duration = Duration::from_secs(1);

/// Spawn the `shadow_rate_demand` aggregator and register it with the
/// `BackgroundTaskMonitor`. Always-on (cheap: atomic loads + one OTLP
/// event per second), like the RTT aggregator. Call once at node startup.
///
/// Emits achieved outbound throughput (a faithful proxy for offered
/// demand, since the transport never drops outbound — it sleeps on the
/// token bucket / cwnd instead, which surfaces as broadcast-queue depth),
/// the active-connection count, the broadcast-queue depth, and the
/// global-pool rate `R` when available.
pub(crate) fn spawn_demand_aggregator(local_peer_id: String, monitor: &BackgroundTaskMonitor) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(AGGREGATOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick; take the throughput baseline once
        // the loop is aligned to the cadence.
        ticker.tick().await;
        let mut prev_sent = TRANSPORT_METRICS.cumulative_bytes_sent();
        loop {
            ticker.tick().await;
            let now_sent = TRANSPORT_METRICS.cumulative_bytes_sent();
            let sent_delta = now_sent.saturating_sub(prev_sent);
            prev_sent = now_sent;
            emit_demand_snapshot(&local_peer_id, sent_delta);
        }
    });
    monitor.register("shadow_demand_aggregator", handle);
}

/// Emit one `shadow_rate_demand` event. See the module-level rustdoc for
/// why the OTLP send is independent of the `tracing::debug!` level: the
/// local mirror is at DEBUG (compiled out of release builds via
/// `release_max_level_info`), while `send_standalone_event_with_peer_id`
/// reaches the collector regardless of log level.
fn emit_demand_snapshot(local_peer_id: &str, sent_bytes_last_interval: u64) {
    let active_connections = active_connections();
    let broadcast_queue_depth = BROADCAST_QUEUE_DEPTH.load(Ordering::Relaxed);
    let gbm = global_bandwidth();
    let global_total_limit_bytes = gbm.as_ref().map(|m| m.total_limit() as u64);
    let global_per_connection_rate_bytes =
        gbm.as_ref().map(|m| m.current_per_connection_rate() as u64);
    let global_active_connections = gbm.as_ref().map(|m| m.connection_count() as u64);

    tracing::debug!(
        target: "freenet::transport::shadow_demand",
        sent_bytes_last_interval,
        active_connections,
        broadcast_queue_depth,
        global_total_limit_bytes,
        global_per_connection_rate_bytes,
        "shadow_rate_demand"
    );
    crate::tracing::telemetry::send_standalone_event_with_peer_id(
        "shadow_rate_demand",
        local_peer_id,
        serde_json::json!({
            // Achieved wire throughput over the last interval (~1 s). A
            // proxy for offered demand; true offered>granted backlog shows
            // up as broadcast_queue_depth and (Phase 2) token-bucket debt.
            "sent_bytes_per_sec": sent_bytes_last_interval,
            "active_connections": active_connections,
            "broadcast_queue_depth": broadcast_queue_depth,
            // Effective aggregate rate R — present only in global-pool
            // mode (total_bandwidth_limit configured). Null under the
            // default per-connection FixedRate mode, where the effective
            // per-connection R is the FixedRate default constant.
            "global_total_limit_bytes": global_total_limit_bytes,
            "global_per_connection_rate_bytes": global_per_connection_rate_bytes,
            "global_active_connections": global_active_connections,
        }),
    );
}

/// Spawn the `shadow_outbound_class` aggregator and register it with the
/// `BackgroundTaskMonitor`. Always-on, like the demand aggregator. Reports
/// the per-interval must-flow / short / bulk byte split that feeds both
/// floor bounds (lower bound = must-flow rate when not bulk-transferring;
/// upper bound combines with the OS counter).
pub(crate) fn spawn_outbound_class_aggregator(
    local_peer_id: String,
    monitor: &BackgroundTaskMonitor,
) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(AGGREGATOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        let mut prev = snapshot_outbound();
        loop {
            ticker.tick().await;
            let now = snapshot_outbound();
            let (must_flow, short, bulk) = outbound_delta(prev, now);
            prev = now;
            emit_class_snapshot(&local_peer_id, must_flow, short, bulk);
        }
    });
    monitor.register("shadow_outbound_class_aggregator", handle);
}

fn emit_class_snapshot(
    local_peer_id: &str,
    must_flow_bytes: u64,
    short_bytes: u64,
    bulk_bytes: u64,
) {
    tracing::debug!(
        target: "freenet::transport::shadow_demand",
        must_flow_bytes,
        short_bytes,
        bulk_bytes,
        "shadow_outbound_class"
    );
    crate::tracing::telemetry::send_standalone_event_with_peer_id(
        "shadow_outbound_class",
        local_peer_id,
        serde_json::json!({
            "must_flow_bytes_per_sec": must_flow_bytes,
            "short_message_bytes_per_sec": short_bytes,
            "bulk_bytes_per_sec": bulk_bytes,
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn short_message() -> SymmetricMessagePayload {
        SymmetricMessagePayload::ShortMessage {
            payload: bytes::Bytes::new(),
        }
    }

    fn stream_fragment() -> SymmetricMessagePayload {
        SymmetricMessagePayload::StreamFragment {
            stream_id: crate::transport::peer_connection::StreamId::next(),
            total_length_bytes: 1000,
            fragment_number: 0,
            payload: bytes::Bytes::new(),
            metadata_bytes: None,
        }
    }

    #[test]
    fn classify_maps_every_variant_to_its_bucket() {
        assert_eq!(classify(&stream_fragment()), OutboundClass::Bulk);
        assert_eq!(classify(&short_message()), OutboundClass::Short);
        assert_eq!(
            classify(&SymmetricMessagePayload::NoOp),
            OutboundClass::MustFlow
        );
        assert_eq!(
            classify(&SymmetricMessagePayload::Ping { sequence: 1 }),
            OutboundClass::MustFlow
        );
        assert_eq!(
            classify(&SymmetricMessagePayload::Pong { sequence: 1 }),
            OutboundClass::MustFlow
        );
        assert_eq!(
            classify(&SymmetricMessagePayload::AckConnection {
                result: Err(std::borrow::Cow::Borrowed("rejected")),
            }),
            OutboundClass::MustFlow
        );
    }

    #[test]
    fn outbound_delta_is_per_interval_difference() {
        let prev = OutboundSnapshot {
            must_flow_bytes: 100,
            short_bytes: 50,
            bulk_bytes: 1000,
        };
        let now = OutboundSnapshot {
            must_flow_bytes: 175,
            short_bytes: 50,
            bulk_bytes: 4000,
        };
        assert_eq!(outbound_delta(prev, now), (75, 0, 3000));
    }

    #[test]
    fn outbound_delta_saturates_on_reset() {
        // A counter that appears to go backwards (reset / wrap) must yield
        // 0, never a giant spurious delta.
        let prev = OutboundSnapshot {
            must_flow_bytes: 5000,
            short_bytes: 5000,
            bulk_bytes: 5000,
        };
        let now = OutboundSnapshot {
            must_flow_bytes: 10,
            short_bytes: 5000,
            bulk_bytes: 6000,
        };
        assert_eq!(outbound_delta(prev, now), (0, 0, 1000));
    }

    #[test]
    fn record_outbound_accumulates_into_the_right_counter() {
        // Delta-based so this is robust to other tests touching the same
        // process-global counters (cf. the rolling_rtt_stats registry
        // tests). Record a unique amount and assert it shows up.
        let before = snapshot_outbound();
        record_outbound(OutboundClass::MustFlow, 11);
        record_outbound(OutboundClass::Short, 22);
        record_outbound(OutboundClass::Bulk, 33);
        let after = snapshot_outbound();
        assert!(after.must_flow_bytes - before.must_flow_bytes >= 11);
        assert!(after.short_bytes - before.short_bytes >= 22);
        assert!(after.bulk_bytes - before.bulk_bytes >= 33);
    }

    #[test]
    fn outbound_delta_is_zero_for_a_quiet_interval() {
        // Equal prev/now snapshots (no traffic in the interval) must yield
        // all-zero deltas, not a spurious value.
        let snap = OutboundSnapshot {
            must_flow_bytes: 12_345,
            short_bytes: 67_890,
            bulk_bytes: 1_000_000,
        };
        assert_eq!(outbound_delta(snap, snap), (0, 0, 0));
    }

    #[test]
    fn broadcast_queue_depth_gauge_round_trips() {
        // Store-then-load is atomic and safe here: the only production
        // writer (`broadcast_queue.rs`) is `#[cfg(not(feature =
        // "simulation_tests"))]` and never instantiated in `--lib` unit
        // tests, so no concurrent writer races this assertion.
        record_broadcast_queue_depth(42);
        assert_eq!(BROADCAST_QUEUE_DEPTH.load(Ordering::Relaxed), 42);
        record_broadcast_queue_depth(0);
        assert_eq!(BROADCAST_QUEUE_DEPTH.load(Ordering::Relaxed), 0);
    }

    /// `spawn_demand_aggregator` must produce a task that survives across
    /// several ticks without the JoinHandle exiting. Mirror of
    /// `rolling_rtt_stats::aggregator_emits_periodically`.
    #[tokio::test(start_paused = true)]
    async fn demand_aggregator_survives_multiple_ticks() {
        let monitor = BackgroundTaskMonitor::new();
        spawn_demand_aggregator("test-peer".to_string(), &monitor);

        tokio::time::advance(AGGREGATOR_INTERVAL * 3 + Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        let exit = monitor.wait_for_any_exit();
        tokio::pin!(exit);
        let still_running = tokio::time::timeout(Duration::from_millis(50), &mut exit)
            .await
            .is_err();
        assert!(
            still_running,
            "demand aggregator task should still be alive after a few ticks"
        );
    }

    /// Same survival pin for the class aggregator.
    #[tokio::test(start_paused = true)]
    async fn class_aggregator_survives_multiple_ticks() {
        let monitor = BackgroundTaskMonitor::new();
        spawn_outbound_class_aggregator("test-peer".to_string(), &monitor);

        tokio::time::advance(AGGREGATOR_INTERVAL * 3 + Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        let exit = monitor.wait_for_any_exit();
        tokio::pin!(exit);
        let still_running = tokio::time::timeout(Duration::from_millis(50), &mut exit)
            .await
            .is_err();
        assert!(
            still_running,
            "class aggregator task should still be alive after a few ticks"
        );
    }
}
