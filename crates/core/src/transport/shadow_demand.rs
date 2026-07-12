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
//! These two always-on emitters plus the Phase 1 RTT aggregator sample at
//! 1 Hz but emit only one rolled-up event each per
//! [`super::shadow_stats::SHADOW_ROLLUP_WINDOW_SECS`] (see `shadow_stats.rs`);
//! the per-window `min`/`max`/`mean` preserve the offline floor-analysis
//! distribution while cutting the central-collector record count by that
//! factor. Every production peer reports to the central collector by default,
//! so the raw 1 Hz stream was ~56% of all central telemetry volume — the
//! rollup is what reclaims it. Shadow events are also admitted under the
//! low-priority sub-budget (#4380) so they yield to operational telemetry
//! under load (`tracing/telemetry.rs`).
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
use super::shadow_stats::{SHADOW_ROLLUP_WINDOW_SECS, WindowedStat};
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

/// 1 Hz sampling cadence, matching the Phase 1/1.5 aggregators so all shadow
/// streams align at the collector. The aggregator samples the cheap
/// process-global signals at this cadence but only *emits* one OTLP rollup per
/// [`SHADOW_ROLLUP_WINDOW_SECS`] samples (see `shadow_stats.rs`).
const AGGREGATOR_INTERVAL: Duration = Duration::from_secs(1);

/// One 1 Hz sample of the demand signals, taken at tick time.
struct DemandSample {
    sent_bytes: u64,
    active_connections: u64,
    broadcast_queue_depth: u64,
    global_total_limit_bytes: Option<u64>,
    global_per_connection_rate_bytes: Option<u64>,
    global_active_connections: Option<u64>,
}

/// Take one demand sample: the interval throughput delta plus the current
/// gauges. Reads atomics + the (weak) bandwidth handle only — safe on any task.
fn demand_sample(sent_bytes: u64) -> DemandSample {
    let gbm = global_bandwidth();
    DemandSample {
        sent_bytes,
        active_connections: active_connections() as u64,
        broadcast_queue_depth: BROADCAST_QUEUE_DEPTH.load(Ordering::Relaxed) as u64,
        global_total_limit_bytes: gbm.as_ref().map(|m| m.total_limit() as u64),
        global_per_connection_rate_bytes: gbm
            .as_ref()
            .map(|m| m.current_per_connection_rate() as u64),
        global_active_connections: gbm.as_ref().map(|m| m.connection_count() as u64),
    }
}

/// Windowed rollup accumulator for the `shadow_rate_demand` stream. Folds up to
/// [`SHADOW_ROLLUP_WINDOW_SECS`] one-second samples, then emits one summary
/// event and resets.
#[derive(Default)]
struct DemandWindow {
    samples: u32,
    sent_bytes: WindowedStat,
    active_connections: WindowedStat,
    broadcast_queue_depth: WindowedStat,
    global_total_limit_bytes: WindowedStat,
    global_per_connection_rate_bytes: WindowedStat,
    global_active_connections: WindowedStat,
}

impl DemandWindow {
    fn record(&mut self, sample: DemandSample) {
        self.samples += 1;
        self.sent_bytes.record(sample.sent_bytes);
        self.active_connections.record(sample.active_connections);
        self.broadcast_queue_depth
            .record(sample.broadcast_queue_depth);
        self.global_total_limit_bytes
            .record_opt(sample.global_total_limit_bytes);
        self.global_per_connection_rate_bytes
            .record_opt(sample.global_per_connection_rate_bytes);
        self.global_active_connections
            .record_opt(sample.global_active_connections);
    }

    fn is_full(&self) -> bool {
        self.samples >= SHADOW_ROLLUP_WINDOW_SECS
    }
}

/// Spawn the `shadow_rate_demand` aggregator and register it with the
/// `BackgroundTaskMonitor`. Always-on (cheap: atomic loads + one OTLP
/// rollup per [`SHADOW_ROLLUP_WINDOW_SECS`]), like the RTT aggregator. Call
/// once at node startup.
///
/// Emits achieved outbound throughput (a faithful proxy for offered
/// demand, since the transport never drops outbound — it sleeps on the
/// token bucket / cwnd instead, which surfaces as broadcast-queue depth),
/// the active-connection count, the broadcast-queue depth, and the
/// global-pool rate `R` when available — each as a per-window mean plus the
/// distribution fields the #4074 floor analysis consumes.
pub(crate) fn spawn_demand_aggregator(local_peer_id: String, monitor: &BackgroundTaskMonitor) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(AGGREGATOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick; take the throughput baseline once
        // the loop is aligned to the cadence.
        ticker.tick().await;
        let mut prev_sent = TRANSPORT_METRICS.cumulative_bytes_sent();
        let mut window = DemandWindow::default();
        loop {
            ticker.tick().await;
            let now_sent = TRANSPORT_METRICS.cumulative_bytes_sent();
            let sent_delta = now_sent.saturating_sub(prev_sent);
            prev_sent = now_sent;
            window.record(demand_sample(sent_delta));
            if window.is_full() {
                emit_demand_rollup(&local_peer_id, &window);
                window = DemandWindow::default();
            }
        }
    });
    monitor.register("shadow_demand_aggregator", handle);
}

/// Emit one `shadow_rate_demand` rollup. See the module-level rustdoc for
/// why the OTLP send is independent of the `tracing::debug!` level: the
/// local mirror is at DEBUG (compiled out of release builds via
/// `release_max_level_info`), while `send_standalone_shadow_event_with_peer_id`
/// reaches the collector regardless of log level (tagged low-priority so it
/// yields to operational telemetry under the rate-limiter sub-budget, #4380).
///
/// Each original field name carries the window **mean** in its original unit
/// (a per-second rate stays a per-second rate — the mean of the one-second
/// deltas is the average bytes/sec over the window), so existing consumers of
/// the flat field keep working. `*_min` / `*_p50` / `*_max` (the byte-rate
/// distribution — `p50` is a more robust central-tendency signal than `mean`
/// for a bursty rate) and `window_secs` / `samples` are additive.
fn emit_demand_rollup(local_peer_id: &str, window: &DemandWindow) {
    let payload = demand_rollup_json(window);
    // Record the Option<u64> means directly (NOT via `?`): tracing renders a
    // `Some(n)` as the bare number and omits the field for `None`, matching the
    // OTLP number-or-null. `?` would emit the literal `Some(n)` and break
    // structured local-log parsers.
    tracing::debug!(
        target: "freenet::transport::shadow_demand",
        sent_bytes_per_sec = window.sent_bytes.mean(),
        active_connections = window.active_connections.mean(),
        broadcast_queue_depth = window.broadcast_queue_depth.mean(),
        window_secs = SHADOW_ROLLUP_WINDOW_SECS,
        "shadow_rate_demand"
    );
    crate::tracing::telemetry::send_standalone_shadow_event_with_peer_id(
        "shadow_rate_demand",
        local_peer_id,
        payload,
    );
}

/// Build the `shadow_rate_demand` rollup JSON. Pure so the schema (compat
/// field = window mean, additive distribution fields) is unit-testable without
/// the telemetry sender.
fn demand_rollup_json(window: &DemandWindow) -> serde_json::Value {
    serde_json::json!({
        // Achieved wire throughput, averaged over the rollup window. A
        // proxy for offered demand; true offered>granted backlog shows
        // up as broadcast_queue_depth and (Phase 2) token-bucket debt.
        "sent_bytes_per_sec": window.sent_bytes.mean(),
        "sent_bytes_per_sec_min": window.sent_bytes.min(),
        "sent_bytes_per_sec_p50": window.sent_bytes.p50(),
        "sent_bytes_per_sec_max": window.sent_bytes.max(),
        "active_connections": window.active_connections.mean(),
        "broadcast_queue_depth": window.broadcast_queue_depth.mean(),
        // Peak backlog in the window — the value the floor analysis cares
        // about, since a single congested second is where demand exceeds R.
        "broadcast_queue_depth_max": window.broadcast_queue_depth.max(),
        // Effective aggregate rate R — present only in global-pool
        // mode (total_bandwidth_limit configured). Null under the
        // default per-connection FixedRate mode, where the effective
        // per-connection R is the FixedRate default constant.
        "global_total_limit_bytes": window.global_total_limit_bytes.mean(),
        "global_per_connection_rate_bytes": window.global_per_connection_rate_bytes.mean(),
        "global_active_connections": window.global_active_connections.mean(),
        "window_secs": SHADOW_ROLLUP_WINDOW_SECS,
        "samples": window.samples,
    })
}

/// Windowed rollup accumulator for the `shadow_outbound_class` stream.
#[derive(Default)]
struct ClassWindow {
    samples: u32,
    must_flow_bytes: WindowedStat,
    short_bytes: WindowedStat,
    bulk_bytes: WindowedStat,
}

impl ClassWindow {
    fn record(&mut self, must_flow: u64, short: u64, bulk: u64) {
        self.samples += 1;
        self.must_flow_bytes.record(must_flow);
        self.short_bytes.record(short);
        self.bulk_bytes.record(bulk);
    }

    fn is_full(&self) -> bool {
        self.samples >= SHADOW_ROLLUP_WINDOW_SECS
    }
}

/// Spawn the `shadow_outbound_class` aggregator and register it with the
/// `BackgroundTaskMonitor`. Always-on, like the demand aggregator. Reports
/// the must-flow / short / bulk byte split that feeds both floor bounds
/// (lower bound = must-flow rate when not bulk-transferring; upper bound
/// combines with the OS counter), sampled at 1 Hz and emitted as one rollup
/// per [`SHADOW_ROLLUP_WINDOW_SECS`].
pub(crate) fn spawn_outbound_class_aggregator(
    local_peer_id: String,
    monitor: &BackgroundTaskMonitor,
) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(AGGREGATOR_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        let mut prev = snapshot_outbound();
        let mut window = ClassWindow::default();
        loop {
            ticker.tick().await;
            let now = snapshot_outbound();
            let (must_flow, short, bulk) = outbound_delta(prev, now);
            prev = now;
            window.record(must_flow, short, bulk);
            if window.is_full() {
                emit_class_rollup(&local_peer_id, &window);
                window = ClassWindow::default();
            }
        }
    });
    monitor.register("shadow_outbound_class_aggregator", handle);
}

/// Emit one `shadow_outbound_class` rollup. Each original field carries the
/// window mean per-second rate; `*_min` (the must-flow floor, i.e. the least
/// the class ever pushed in a second), `*_p50` (robust central tendency for a
/// bursty rate) and `*_max` (the burst peak) are the additive distribution
/// fields the #4074 floor bounds consume directly.
fn emit_class_rollup(local_peer_id: &str, window: &ClassWindow) {
    let payload = class_rollup_json(window);
    // Record the Option<u64> means directly (NOT via `?`): see the
    // `emit_demand_rollup` note — bare Option renders as the number-or-null
    // form the OTLP path uses; `?` would emit `Some(n)` and break parsers.
    tracing::debug!(
        target: "freenet::transport::shadow_demand",
        must_flow_bytes = window.must_flow_bytes.mean(),
        short_bytes = window.short_bytes.mean(),
        bulk_bytes = window.bulk_bytes.mean(),
        window_secs = SHADOW_ROLLUP_WINDOW_SECS,
        "shadow_outbound_class"
    );
    crate::tracing::telemetry::send_standalone_shadow_event_with_peer_id(
        "shadow_outbound_class",
        local_peer_id,
        payload,
    );
}

/// Build the `shadow_outbound_class` rollup JSON. Pure so the schema is
/// unit-testable without the telemetry sender.
fn class_rollup_json(window: &ClassWindow) -> serde_json::Value {
    serde_json::json!({
        "must_flow_bytes_per_sec": window.must_flow_bytes.mean(),
        "must_flow_bytes_per_sec_min": window.must_flow_bytes.min(),
        "must_flow_bytes_per_sec_p50": window.must_flow_bytes.p50(),
        "must_flow_bytes_per_sec_max": window.must_flow_bytes.max(),
        "short_message_bytes_per_sec": window.short_bytes.mean(),
        "short_message_bytes_per_sec_min": window.short_bytes.min(),
        "short_message_bytes_per_sec_p50": window.short_bytes.p50(),
        "short_message_bytes_per_sec_max": window.short_bytes.max(),
        "bulk_bytes_per_sec": window.bulk_bytes.mean(),
        "bulk_bytes_per_sec_min": window.bulk_bytes.min(),
        "bulk_bytes_per_sec_p50": window.bulk_bytes.p50(),
        "bulk_bytes_per_sec_max": window.bulk_bytes.max(),
        "window_secs": SHADOW_ROLLUP_WINDOW_SECS,
        "samples": window.samples,
    })
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

    #[test]
    fn demand_rollup_json_keeps_mean_under_original_names_and_adds_distribution() {
        let mut window = DemandWindow::default();
        window.record(DemandSample {
            sent_bytes: 100,
            active_connections: 4,
            broadcast_queue_depth: 0,
            global_total_limit_bytes: None,
            global_per_connection_rate_bytes: None,
            global_active_connections: None,
        });
        window.record(DemandSample {
            sent_bytes: 300,
            active_connections: 6,
            broadcast_queue_depth: 10,
            global_total_limit_bytes: None,
            global_per_connection_rate_bytes: None,
            global_active_connections: None,
        });
        let json = demand_rollup_json(&window);
        // Compat: the original flat field carries the window MEAN rate.
        assert_eq!(json["sent_bytes_per_sec"], 200);
        // Additive distribution fields.
        assert_eq!(json["sent_bytes_per_sec_min"], 100);
        // Upper-middle median of [100,300] = sorted[len/2] = sorted[1] = 300.
        assert_eq!(json["sent_bytes_per_sec_p50"], 300);
        assert_eq!(json["sent_bytes_per_sec_max"], 300);
        assert_eq!(json["active_connections"], 5);
        assert_eq!(json["broadcast_queue_depth"], 5);
        assert_eq!(json["broadcast_queue_depth_max"], 10);
        // FixedRate mode: no global-pool R, so these stay null (unchanged).
        assert!(json["global_total_limit_bytes"].is_null());
        assert!(json["global_per_connection_rate_bytes"].is_null());
        assert_eq!(json["window_secs"], SHADOW_ROLLUP_WINDOW_SECS);
        assert_eq!(json["samples"], 2);
    }

    #[test]
    fn class_rollup_json_keeps_mean_under_original_names_and_adds_distribution() {
        let mut window = ClassWindow::default();
        window.record(10, 20, 1000);
        window.record(30, 20, 3000);
        let json = class_rollup_json(&window);
        assert_eq!(json["must_flow_bytes_per_sec"], 20);
        assert_eq!(json["must_flow_bytes_per_sec_min"], 10);
        // Upper-middle median of [10,30] = sorted[1] = 30.
        assert_eq!(json["must_flow_bytes_per_sec_p50"], 30);
        assert_eq!(json["must_flow_bytes_per_sec_max"], 30);
        assert_eq!(json["short_message_bytes_per_sec"], 20);
        // Both samples are 20, so every summary stat is 20.
        assert_eq!(json["short_message_bytes_per_sec_p50"], 20);
        assert_eq!(json["bulk_bytes_per_sec"], 2000);
        assert_eq!(json["bulk_bytes_per_sec_min"], 1000);
        // Upper-middle median of [1000,3000] = sorted[1] = 3000.
        assert_eq!(json["bulk_bytes_per_sec_p50"], 3000);
        assert_eq!(json["bulk_bytes_per_sec_max"], 3000);
        assert_eq!(json["window_secs"], SHADOW_ROLLUP_WINDOW_SECS);
        assert_eq!(json["samples"], 2);
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
