//! Telemetry reporter that sends events to a central OpenTelemetry collector.
//!
//! This module provides remote telemetry reporting to help debug network issues.
//! It sends operation events (connect, put, get, subscribe, update) to a central
//! collector via OTLP/HTTP protocol.
//!
//! Features:
//! - Exponential backoff on connection failures (1s → 2s → 4s → ... → 5min max)
//! - Event batching (send every 10 seconds or when buffer reaches 100 events)
//! - Rate limiting (max 10 operational events/second aggregate). Fixed-cadence
//!   periodic node streams bypass that cap, because their timers already bound
//!   them and a busy node otherwise drops most of them (#6001). The fixed
//!   30-minute #5090 diagnostic has its own bounded reserved path.
//! - Priority-based dropping when buffer is full
//!
//! ## Design Notes
//!
//! ### Why custom backoff instead of `crate::util::backoff::ExponentialBackoff`?
//! The shared `ExponentialBackoff` utility is stateless and designed for finite retry scenarios.
//! Telemetry needs infinite retries with reset-on-success, which requires different state management.
//!
//! ### Why HTTP instead of HTTPS?
//! TODO(#2456): Enable HTTPS once TLS is configured on the collector server.
//! Currently the server only accepts HTTP connections.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::time::Instant;

use either::Either;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::config::GlobalRng;

use crate::config::{GlobalExecutor, TelemetryConfig};
use crate::message::Transaction;
use crate::transport::TRANSPORT_METRICS;

use super::{EventKind, NetEventLog, NetEventRegister, NetLogMessage};

/// Global telemetry sender for standalone event emission from contexts
/// that don't have access to a `NetEventRegister` (e.g., background tasks
/// like subscription renewal and interest sweep).
///
/// Initialized when `TelemetryReporter::new()` creates the reporter.
/// If telemetry is disabled, this remains unset and `send_standalone_event`
/// silently drops events.
static TELEMETRY_SENDER: OnceLock<TelemetryChannel> = OnceLock::new();

/// The fixed set of background "shadow" rollups with per-stream delivery
/// counters. All of them are also [`PERIODIC_EVENT_TYPES`].
///
/// This is deliberately an enum rather than an event-name map: #5090 needs to
/// identify which aligned rollup was lost without creating attacker-controlled
/// telemetry cardinality. Keep [`Self::ALL`] exhaustive when adding a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KnownShadowRollup {
    ShadowRttAggregate,
    ShadowRateDemand,
    ShadowOutboundClass,
    ShadowReferencePing,
    ShadowIfaceTx,
    OutboundMessageMix,
    BroadcastPayloadMix,
}

impl KnownShadowRollup {
    pub(crate) const ALL: [Self; 7] = [
        Self::ShadowRttAggregate,
        Self::ShadowRateDemand,
        Self::ShadowOutboundClass,
        Self::ShadowReferencePing,
        Self::ShadowIfaceTx,
        Self::OutboundMessageMix,
        Self::BroadcastPayloadMix,
    ];

    const fn index(self) -> usize {
        match self {
            Self::ShadowRttAggregate => 0,
            Self::ShadowRateDemand => 1,
            Self::ShadowOutboundClass => 2,
            Self::ShadowReferencePing => 3,
            Self::ShadowIfaceTx => 4,
            Self::OutboundMessageMix => 5,
            Self::BroadcastPayloadMix => 6,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ShadowRttAggregate => "shadow_rtt_aggregate",
            Self::ShadowRateDemand => "shadow_rate_demand",
            Self::ShadowOutboundClass => "shadow_outbound_class",
            Self::ShadowReferencePing => "shadow_reference_ping",
            Self::ShadowIfaceTx => "shadow_iface_tx",
            Self::OutboundMessageMix => "outbound_message_mix",
            Self::BroadcastPayloadMix => "broadcast_payload_mix",
        }
    }

    fn from_event_type(event_type: &str) -> Option<Self> {
        match event_type {
            "shadow_rtt_aggregate" => Some(Self::ShadowRttAggregate),
            "shadow_rate_demand" => Some(Self::ShadowRateDemand),
            "shadow_outbound_class" => Some(Self::ShadowOutboundClass),
            "shadow_reference_ping" => Some(Self::ShadowReferencePing),
            "shadow_iface_tx" => Some(Self::ShadowIfaceTx),
            "outbound_message_mix" => Some(Self::OutboundMessageMix),
            "broadcast_payload_mix" => Some(Self::BroadcastPayloadMix),
            _ => None,
        }
    }
}

/// Loss counters for one fixed shadow-rollup stream.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub(crate) struct KnownShadowRollupMetricsSnapshot {
    pub event_type: &'static str,
    pub enqueue_attempts_total: u64,
    pub enqueue_dropped_full_total: u64,
    pub enqueue_dropped_closed_total: u64,
    pub rate_limit_admitted_total: u64,
    pub dropped_backoff_buffer_full_total: u64,
    pub retry_truncated_total: u64,
    pub sent_total: u64,
}

/// Process-local telemetry delivery counters for the five-minute node snapshot.
///
/// Every field is a monotonic lifetime total. A later successful snapshot can
/// therefore report a snapshot that was itself dropped; that loss appears one
/// snapshot late rather than disappearing with the failed event.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct TelemetryLocalMetricsSnapshot {
    pub enqueue_attempts_total: u64,
    pub enqueue_succeeded_total: u64,
    pub enqueue_dropped_full_total: u64,
    pub enqueue_dropped_closed_total: u64,
    pub rate_limit_admitted_operational_total: u64,
    /// [`EventPriority::Periodic`] events, which bypass the per-second cap.
    pub rate_limit_admitted_periodic_total: u64,
    pub dropped_aggregate_limit_operational_total: u64,
    pub dropped_backoff_buffer_full_total: u64,
    pub batch_send_attempts_total: u64,
    pub batch_send_successes_total: u64,
    pub batch_send_failures_total: u64,
    pub batch_events_sent_total: u64,
    pub batch_retry_truncated_events_total: u64,
    pub known_shadow_rollups: [KnownShadowRollupMetricsSnapshot; KnownShadowRollup::ALL.len()],
    /// Delivery path for router snapshots carrying `network_efficiency_v1`:
    /// attempts, enqueue full/closed, admitted, rate-limit drop, coalesced
    /// pending sample, retry truncation, final sent.
    pub network_efficiency_delivery: [u64; 8],
    /// Per event type, in [`KNOWN_EVENT_TYPES`] order: `[admitted, dropped]`
    /// at the per-second rate limiter. Always exactly
    /// `KNOWN_EVENT_TYPES.len()` rows, so the exported cardinality is fixed.
    /// Counts only events that reached the worker; channel-full drops before
    /// it are in `enqueue_dropped_full_total`, not per type.
    pub event_type_admissions: Vec<[u64; 2]>,
}

/// The telemetry rows of `network_efficiency_v1` (`tel`, `shadow`, `eff`,
/// `tel_t`), built in one place so the positions are unit-testable.
pub(crate) struct NetworkEfficiencyTelemetryRows {
    pub tel: [u64; 16],
    pub shadow: [[u64; 9]; KnownShadowRollup::ALL.len()],
    pub eff: [u64; 8],
    pub tel_t: Vec<[u64; 2]>,
}

impl TelemetryLocalMetricsSnapshot {
    /// Lay these totals out as the `network_efficiency_v1` rows.
    ///
    /// `tel` positions 5, 7 and 8 were the shadow sub-budget's admitted,
    /// aggregate-drop and sub-budget-drop counts, and `shadow` columns 4 and 5
    /// its aggregate-limit and sub-budget drops. #6001 replaced that priority
    /// with the periodic bypass, so they are always 0, kept so every later
    /// position stays where readers expect it. The periodic admitted count is
    /// appended as `tel` position 15.
    ///
    /// `tel` position 4 (operational admitted) also changed meaning at #6001:
    /// before it, it included `transport_snapshot`, `resource_utilization`,
    /// `router_snapshot` (without the diagnostic block),
    /// `interest_heartbeat_cycle` and `subscription_health_snapshot`, which now
    /// count in position 15. Read pre- and post-#6001 series separately. The
    /// schema stays `v: 1`: no consumer reading these positions was found when
    /// this changed.
    pub(crate) fn network_efficiency_rows(self) -> NetworkEfficiencyTelemetryRows {
        NetworkEfficiencyTelemetryRows {
            tel: [
                self.enqueue_attempts_total,
                self.enqueue_succeeded_total,
                self.enqueue_dropped_full_total,
                self.enqueue_dropped_closed_total,
                self.rate_limit_admitted_operational_total,
                0,
                self.dropped_aggregate_limit_operational_total,
                0,
                0,
                self.dropped_backoff_buffer_full_total,
                self.batch_send_attempts_total,
                self.batch_send_successes_total,
                self.batch_send_failures_total,
                self.batch_events_sent_total,
                self.batch_retry_truncated_events_total,
                self.rate_limit_admitted_periodic_total,
            ],
            shadow: self.known_shadow_rollups.map(|stream| {
                [
                    stream.enqueue_attempts_total,
                    stream.enqueue_dropped_full_total,
                    stream.enqueue_dropped_closed_total,
                    stream.rate_limit_admitted_total,
                    0,
                    0,
                    stream.dropped_backoff_buffer_full_total,
                    stream.retry_truncated_total,
                    stream.sent_total,
                ]
            }),
            eff: self.network_efficiency_delivery,
            tel_t: self.event_type_admissions,
        }
    }
}

#[derive(Default)]
struct NetworkEfficiencyDeliveryMetrics {
    attempts: AtomicU64,
    enqueue_full: AtomicU64,
    enqueue_closed: AtomicU64,
    admitted: AtomicU64,
    rate_limit_drop: AtomicU64,
    coalesced: AtomicU64,
    retry_truncated: AtomicU64,
    sent: AtomicU64,
}

impl NetworkEfficiencyDeliveryMetrics {
    fn snapshot(&self) -> [u64; 8] {
        [
            self.attempts.load(Ordering::Relaxed),
            self.enqueue_full.load(Ordering::Relaxed),
            self.enqueue_closed.load(Ordering::Relaxed),
            self.admitted.load(Ordering::Relaxed),
            self.rate_limit_drop.load(Ordering::Relaxed),
            self.coalesced.load(Ordering::Relaxed),
            self.retry_truncated.load(Ordering::Relaxed),
            self.sent.load(Ordering::Relaxed),
        ]
    }
}

#[derive(Default)]
struct KnownShadowRollupMetrics {
    enqueue_attempts_total: AtomicU64,
    enqueue_dropped_full_total: AtomicU64,
    enqueue_dropped_closed_total: AtomicU64,
    rate_limit_admitted_total: AtomicU64,
    dropped_backoff_buffer_full_total: AtomicU64,
    retry_truncated_total: AtomicU64,
    sent_total: AtomicU64,
}

impl KnownShadowRollupMetrics {
    fn snapshot(&self, stream: KnownShadowRollup) -> KnownShadowRollupMetricsSnapshot {
        KnownShadowRollupMetricsSnapshot {
            event_type: stream.as_str(),
            enqueue_attempts_total: self.enqueue_attempts_total.load(Ordering::Relaxed),
            enqueue_dropped_full_total: self.enqueue_dropped_full_total.load(Ordering::Relaxed),
            enqueue_dropped_closed_total: self.enqueue_dropped_closed_total.load(Ordering::Relaxed),
            rate_limit_admitted_total: self.rate_limit_admitted_total.load(Ordering::Relaxed),
            dropped_backoff_buffer_full_total: self
                .dropped_backoff_buffer_full_total
                .load(Ordering::Relaxed),
            retry_truncated_total: self.retry_truncated_total.load(Ordering::Relaxed),
            sent_total: self.sent_total.load(Ordering::Relaxed),
        }
    }
}

/// Admitted and dropped counts at the per-second rate limiter, one row per
/// [`KNOWN_EVENT_TYPES`] entry. Before #6001 only the shadow rollups had
/// per-stream counters, so the mix of event types a node offered (and which
/// types the cap was dropping) could not be measured.
struct EventTypeAdmissionMetrics {
    rows: [[AtomicU64; 2]; KNOWN_EVENT_TYPES.len()],
}

impl Default for EventTypeAdmissionMetrics {
    fn default() -> Self {
        // `Default` for arrays stops at 32 elements, so build it explicitly.
        Self {
            rows: std::array::from_fn(|_| [AtomicU64::new(0), AtomicU64::new(0)]),
        }
    }
}

impl EventTypeAdmissionMetrics {
    fn record(&self, event_type: &str, admitted: bool) {
        self.rows[event_type_index(event_type)][usize::from(!admitted)]
            .fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> Vec<[u64; 2]> {
        self.rows
            .iter()
            .map(|[admitted, dropped]| {
                [
                    admitted.load(Ordering::Relaxed),
                    dropped.load(Ordering::Relaxed),
                ]
            })
            .collect()
    }
}

#[derive(Default)]
struct TelemetryLocalMetrics {
    enqueue_attempts_total: AtomicU64,
    enqueue_succeeded_total: AtomicU64,
    enqueue_dropped_full_total: AtomicU64,
    enqueue_dropped_closed_total: AtomicU64,
    rate_limit_admitted_operational_total: AtomicU64,
    rate_limit_admitted_periodic_total: AtomicU64,
    dropped_aggregate_limit_operational_total: AtomicU64,
    dropped_backoff_buffer_full_total: AtomicU64,
    batch_send_attempts_total: AtomicU64,
    batch_send_successes_total: AtomicU64,
    batch_send_failures_total: AtomicU64,
    batch_events_sent_total: AtomicU64,
    batch_retry_truncated_events_total: AtomicU64,
    known_shadow_rollups: [KnownShadowRollupMetrics; KnownShadowRollup::ALL.len()],
    network_efficiency_delivery: NetworkEfficiencyDeliveryMetrics,
    event_types: EventTypeAdmissionMetrics,
}

impl TelemetryLocalMetrics {
    fn shadow(&self, event_type: &str) -> Option<&KnownShadowRollupMetrics> {
        KnownShadowRollup::from_event_type(event_type)
            .map(|stream| &self.known_shadow_rollups[stream.index()])
    }

    fn snapshot(&self) -> TelemetryLocalMetricsSnapshot {
        TelemetryLocalMetricsSnapshot {
            enqueue_attempts_total: self.enqueue_attempts_total.load(Ordering::Relaxed),
            enqueue_succeeded_total: self.enqueue_succeeded_total.load(Ordering::Relaxed),
            enqueue_dropped_full_total: self.enqueue_dropped_full_total.load(Ordering::Relaxed),
            enqueue_dropped_closed_total: self.enqueue_dropped_closed_total.load(Ordering::Relaxed),
            rate_limit_admitted_operational_total: self
                .rate_limit_admitted_operational_total
                .load(Ordering::Relaxed),
            rate_limit_admitted_periodic_total: self
                .rate_limit_admitted_periodic_total
                .load(Ordering::Relaxed),
            dropped_aggregate_limit_operational_total: self
                .dropped_aggregate_limit_operational_total
                .load(Ordering::Relaxed),
            dropped_backoff_buffer_full_total: self
                .dropped_backoff_buffer_full_total
                .load(Ordering::Relaxed),
            batch_send_attempts_total: self.batch_send_attempts_total.load(Ordering::Relaxed),
            batch_send_successes_total: self.batch_send_successes_total.load(Ordering::Relaxed),
            batch_send_failures_total: self.batch_send_failures_total.load(Ordering::Relaxed),
            batch_events_sent_total: self.batch_events_sent_total.load(Ordering::Relaxed),
            batch_retry_truncated_events_total: self
                .batch_retry_truncated_events_total
                .load(Ordering::Relaxed),
            known_shadow_rollups: std::array::from_fn(|index| {
                self.known_shadow_rollups[index].snapshot(KnownShadowRollup::ALL[index])
            }),
            network_efficiency_delivery: self.network_efficiency_delivery.snapshot(),
            event_type_admissions: self.event_types.snapshot(),
        }
    }
}

struct TelemetryChannel {
    sender: mpsc::Sender<TelemetryCommand>,
    metrics: Arc<TelemetryLocalMetrics>,
}

/// Read this process's telemetry admission/drop totals for periodic export.
#[allow(dead_code)] // Read by the router-snapshot integration added alongside #5090.
pub(crate) fn telemetry_local_metrics_snapshot() -> Option<TelemetryLocalMetricsSnapshot> {
    TELEMETRY_SENDER
        .get()
        .map(|channel| channel.metrics.snapshot())
}

/// Admission priority for a telemetry event.
///
/// The per-second rate limiter ([`TelemetryWorker::admit_event`]) enforces a
/// flat aggregate cap ([`MAX_EVENTS_PER_SECOND`]) on operational telemetry.
///
/// `Periodic` events bypass that cap. Each is a node-level stream emitted on a
/// fixed timer (see [`PERIODIC_EVENT_TYPES`]), so its timer already bounds its
/// rate to a few events per minute per node. Before #6001 they shared the cap
/// with per-operation events, and on the production gateways, which offer
/// about 60 events/s against a cap of 10, only 22-30% of them were uploaded.
/// They also do not consume the cap's slots, so they cannot crowd out
/// operational telemetry. That was the purpose of the #4380 shadow
/// sub-budget, which this priority replaces.
///
/// `NetworkEfficiency` is the one fixed, 30-minute diagnostic used to decide
/// #5090. It has a dedicated one-slot producer channel and one reserved buffer
/// slot, and is exempt from the per-second cap. Those structural bounds make
/// eventual observation independent of ordinary telemetry saturation without
/// creating an unbounded or attacker-controlled priority path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventPriority {
    /// Operational net-events and node telemetry. Admitted up to the
    /// aggregate cap. This is the default for everything that is not a
    /// fixed-cadence periodic stream.
    #[default]
    Operational,
    /// A fixed-cadence node stream listed in [`PERIODIC_EVENT_TYPES`].
    /// Assigned only by [`priority_for_event_type`], and honoured by the
    /// limiter only for a listed type.
    Periodic,
    /// The fixed `router_snapshot.network_efficiency_v1` diagnostic. Routed
    /// only by [`TelemetryReporter::send_event`], never exposed to standalone
    /// event producers.
    NetworkEfficiency,
    /// Replaced by [`Self::Periodic`] in #6001, which removed the shadow
    /// sub-budget. Kept so code naming it still compiles. The limiter treats
    /// it exactly like `Periodic`: it bypasses the cap only for a type listed
    /// in [`PERIODIC_EVENT_TYPES`], and is operational otherwise.
    #[deprecated(note = "the shadow sub-budget was removed in #6001; use `Periodic`")]
    Shadow,
}

/// Event types emitted once per tick of a fixed node-level timer, which
/// therefore bypass the per-second rate limit (#6001). Each entry names the
/// timer that bounds it:
///
/// - `transport_snapshot`: the worker's transport-snapshot interval
///   (configured, clamped to at least [`MIN_TRANSPORT_SNAPSHOT_INTERVAL_SECS`]).
/// - `resource_utilization`: every [`RESOURCE_TELEMETRY_BATCH_INTERVAL`]
///   batch ticks (about 60 s).
/// - `router_snapshot`: `Ring::emit_router_snapshot_telemetry`, every 5 min.
/// - the five `shadow_*` rollups: one per
///   `transport::shadow_stats::SHADOW_ROLLUP_WINDOW_SECS` (30 s) from one
///   aggregator task each (`shadow_reference_ping` probes a single target).
/// - `broadcast_payload_mix`, `outbound_message_mix`: one per 60 s
///   `ROLLUP_WINDOW` from one aggregator task each.
/// - `interest_heartbeat_cycle`: at most one per half
///   `INTEREST_HEARTBEAT_INTERVAL` (2.5 min), from `Ring::interest_heartbeat`.
/// - `subscription_health_snapshot`: at most one per half
///   `INTEREST_SWEEP_INTERVAL` (30 s), from the interest sweep.
///
/// The router-snapshot interval and the worker's two intervals use
/// `MissedTickBehavior::Delay`, so their timer is the bound even after a
/// stall. The last two loops keep tokio's default `Burst`, which the interest
/// sweep's reconciliation is written around (#5782), so a stall can run
/// several passes back to back. For those two the bound is a
/// [`PeriodicEmissionGuard`] at the producer, which skips the event when less
/// than half its period has passed since the last one. The shadow and
/// message-mix aggregators use `Delay` as well.
///
/// Together that is about 0.27 events/s per node at the default 30 s
/// transport-snapshot interval with both opt-in shadow streams on, and about
/// 0.34 at the 10 s minimum. Do NOT add an event emitted per operation, per
/// peer, per contract or per subscription: only a fixed timer makes bypassing
/// the cap safe. `interest_expired`, for example, is emitted from the 1-minute
/// interest sweep but once per expired interest, so it stays operational.
pub(crate) const PERIODIC_EVENT_TYPES: [&str; 12] = [
    "transport_snapshot",
    "resource_utilization",
    "router_snapshot",
    "shadow_rtt_aggregate",
    "shadow_rate_demand",
    "shadow_outbound_class",
    "shadow_reference_ping",
    "shadow_iface_tx",
    "broadcast_payload_mix",
    "outbound_message_mix",
    "interest_heartbeat_cycle",
    "subscription_health_snapshot",
];

/// Rate bound for a periodic event whose producer loop runs on a burst-mode
/// timer (see [`PERIODIC_EVENT_TYPES`]). The loop itself keeps every tick; only
/// the telemetry event is skipped when it would follow the previous one by
/// less than `min_gap`.
#[derive(Debug)]
pub(crate) struct PeriodicEmissionGuard {
    min_gap: Duration,
    last_emitted: Option<Instant>,
}

impl PeriodicEmissionGuard {
    /// `period` is the producer's timer period; events are at least half of
    /// it apart.
    pub(crate) fn half_of(period: Duration) -> Self {
        Self {
            min_gap: period / 2,
            last_emitted: None,
        }
    }

    /// Whether to emit at `now`. Records the emission when it returns `true`.
    pub(crate) fn should_emit(&mut self, now: Instant) -> bool {
        if self
            .last_emitted
            .is_some_and(|last| now.saturating_duration_since(last) < self.min_gap)
        {
            return false;
        }
        self.last_emitted = Some(now);
        true
    }
}

fn is_periodic_event_type(event_type: &str) -> bool {
    PERIODIC_EVENT_TYPES.contains(&event_type)
}

/// The admission priority every ordinary producer uses. `NetworkEfficiency`
/// is assigned separately, by [`TelemetryReporter::send_event`].
fn priority_for_event_type(event_type: &str) -> EventPriority {
    if is_periodic_event_type(event_type) {
        EventPriority::Periodic
    } else {
        EventPriority::Operational
    }
}

/// Every event type this crate sends to the collector, in the fixed order of
/// the per-type admission counters exported as `network_efficiency_v1.tel_t`.
/// Append only: a reader indexes rows by position, and treats a shorter list
/// as coming from an older node. Anything not listed is counted under the
/// final `other` row, so the exported cardinality never depends on what a
/// producer names its event. A non-zero `other` means an event type is
/// missing from this list.
pub(crate) const KNOWN_EVENT_TYPES: [&str; 74] = [
    // `event_kind_to_string`, in its order.
    "connect_start",
    "connect_connected",
    "connect_finished",
    "connect_request_sent",
    "connect_request_received",
    "connect_response_sent",
    "connect_response_received",
    "connect_rejected",
    "disconnect",
    "put_request",
    "put_success",
    "put_failure",
    "put_response_sent",
    "put_broadcast_emitted",
    "put_broadcast_received",
    "get_request",
    "get_success",
    "get_not_found",
    "get_failure",
    "get_response_sent",
    "get_forwarding_ack_sent",
    "get_forwarding_ack_received",
    "get_terminal",
    "subscribe_request",
    "subscribe_success",
    "subscribe_not_found",
    "subscribe_timeout",
    "subscribe_response_sent",
    "hosting_started",
    "hosting_stopped",
    "subscribe_reserved",
    "unsubscribe_sent",
    "unsubscribe_received",
    "update_request",
    "update_success",
    "update_broadcast_emitted",
    "update_broadcast_complete",
    "update_broadcast_received",
    "update_broadcast_applied",
    "update_failure",
    "update_broadcast_delivery_summary",
    "transfer_started",
    "transfer_completed",
    "transfer_failed",
    "route_success",
    "route_success_untimed",
    "route_failure",
    "ignored",
    "timeout",
    "peer_startup",
    "peer_shutdown",
    "transport_snapshot",
    "interest_resync_request_received",
    "interest_resync_response_sent",
    "interest_state_confirmed",
    "routing_decision",
    "router_snapshot",
    // Emitted by the worker and by standalone producers.
    "resource_utilization",
    "shadow_rtt_aggregate",
    "shadow_rate_demand",
    "shadow_outbound_class",
    "shadow_reference_ping",
    "shadow_iface_tx",
    "broadcast_payload_mix",
    "outbound_message_mix",
    "interest_heartbeat_cycle",
    "subscription_health_snapshot",
    "interest_expired",
    "subscription_root_detected",
    "contract_directed_connect",
    "subscription_renewal_outcome",
    "stranded_recovery",
    crate::ring::FLAG_TELEMETRY_EVENT,
    // Must stay last.
    "other",
];

const OTHER_EVENT_TYPE_INDEX: usize = KNOWN_EVENT_TYPES.len() - 1;

/// Row of `event_type` in [`KNOWN_EVENT_TYPES`], or the `other` row. A linear
/// scan: about 70 short comparisons per admitted-or-dropped event, at most a
/// few thousand per second on the busiest gateway.
fn event_type_index(event_type: &str) -> usize {
    KNOWN_EVENT_TYPES[..OTHER_EVENT_TYPE_INDEX]
        .iter()
        .position(|known| *known == event_type)
        .unwrap_or(OTHER_EVENT_TYPE_INDEX)
}

/// Send a standalone telemetry event from any context.
///
/// This is a non-blocking, best-effort send. Events are dropped silently if:
/// - Telemetry is not enabled (TELEMETRY_SENDER not initialized)
/// - The telemetry channel is full
///
/// The `peer_id` OTLP attribute is left empty. For events that originate
/// from a specific node and should be filterable by that node in the
/// collector, use [`send_standalone_event_with_peer_id`] instead.
///
/// The admission priority follows from the event type
/// ([`priority_for_event_type`]).
pub fn send_standalone_event(event_type: &str, event_data: serde_json::Value) {
    send_standalone_event_inner(event_type, "", event_data);
}

/// Send a standalone telemetry event tagged with the local node's peer id.
///
/// Used by node-level periodic emitters whose output is only meaningful when
/// the collector can disaggregate samples by source node. The `peer_id` is
/// set as the OTLP record attribute so the collector can group/filter without
/// parsing the event body.
///
/// The admission priority follows from the event type
/// ([`priority_for_event_type`]).
pub fn send_standalone_event_with_peer_id(
    event_type: &str,
    peer_id: &str,
    event_data: serde_json::Value,
) {
    send_standalone_event_inner(event_type, peer_id, event_data);
}

/// Send a standalone telemetry event tagged with the local node's peer id.
///
/// Before #6001 this admitted the event under the shadow sub-budget. That
/// budget is gone: the priority now follows from the event type, exactly as
/// in [`send_standalone_event_with_peer_id`], which this forwards to.
#[deprecated(note = "use `send_standalone_event_with_peer_id`; the priority \
                     now follows from the event type (#6001)")]
pub fn send_standalone_shadow_event_with_peer_id(
    event_type: &str,
    peer_id: &str,
    event_data: serde_json::Value,
) {
    send_standalone_event_with_peer_id(event_type, peer_id, event_data);
}

fn send_standalone_event_inner(event_type: &str, peer_id: &str, event_data: serde_json::Value) {
    if let Some(channel) = TELEMETRY_SENDER.get() {
        let event = standalone_event(event_type, peer_id, event_data);
        try_enqueue_event(&channel.sender, &channel.metrics, event);
    }
}

/// Build a standalone event. Separate from the global-sender lookup so the
/// priority it assigns is testable without initializing process-global state.
fn standalone_event(
    event_type: &str,
    peer_id: &str,
    event_data: serde_json::Value,
) -> TelemetryEvent {
    TelemetryEvent {
        timestamp: current_timestamp_ms(),
        peer_id: peer_id.to_string(),
        transaction_id: String::new(),
        event_type: event_type.to_string(),
        event_data,
        priority: priority_for_event_type(event_type),
    }
}

/// Maximum number of events to buffer before sending
const MAX_BUFFER_SIZE: usize = 100;

/// One structurally reserved buffer slot for the 30-minute #5090 diagnostic.
/// Ordinary telemetry remains capped at [`MAX_BUFFER_SIZE`].
const NETWORK_EFFICIENCY_BUFFER_RESERVE: usize = 1;

/// How often to send batched events (in seconds)
const BATCH_INTERVAL_SECS: u64 = 10;

/// Emit `resource_utilization` telemetry once every this many batch ticks.
///
/// The batch tick fires every [`BATCH_INTERVAL_SECS`] (10s), but resource
/// utilization (memory RSS/ceiling, CPU time, cumulative bandwidth) is a
/// slowly-changing, node-wide signal, so sampling it on every 10s tick is
/// wasteful. Crucially, EVERY production peer reports telemetry to the central
/// collector (nova) by default, so a 10s cadence multiplies collector load
/// across the whole fleet for a signal that barely moves in 10s. Emitting once
/// per 6 batch ticks (~60s) cuts that ~6x with no real signal loss.
///
/// Only `resource_utilization`'s cadence is throttled this way; every other
/// telemetry event still batches and flushes on the normal [`BATCH_INTERVAL_SECS`]
/// tick.
const RESOURCE_TELEMETRY_BATCH_INTERVAL: u64 = 6;

/// Maximum operational events per second. [`EventPriority::Periodic`] and
/// [`EventPriority::NetworkEfficiency`] events bypass it.
const MAX_EVENTS_PER_SECOND: usize = 10;

/// Initial backoff duration on failure
const INITIAL_BACKOFF_MS: u64 = 1000;

/// Minimum transport snapshot interval (to prevent spamming telemetry)
const MIN_TRANSPORT_SNAPSHOT_INTERVAL_SECS: u64 = 10;

/// Maximum backoff duration
const MAX_BACKOFF_MS: u64 = 300_000; // 5 minutes

/// Get current timestamp in milliseconds since Unix epoch.
/// Logs a warning if system time is unavailable (e.g., clock went backwards).
pub fn current_timestamp_ms() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "System time error while generating telemetry timestamp; using 0 as fallback"
            );
            0
        }
    }
}

/// Telemetry reporter that sends events to a central OTLP collector.
#[derive(Clone)]
pub struct TelemetryReporter {
    sender: mpsc::Sender<TelemetryCommand>,
    /// Dedicated one-slot path for the fixed 30-minute diagnostic. Ordinary
    /// event bursts cannot consume this channel's capacity.
    network_efficiency_sender: mpsc::Sender<TelemetryCommand>,
    metrics: Arc<TelemetryLocalMetrics>,
    /// This node's own peer id, stamped on events that are emitted
    /// without a `NetLogMessage` carrying one (timeouts, transfer
    /// events, transport snapshots). Without it those events carry an
    /// empty `peer_id` and cannot be attributed to a sender in the
    /// collector (#4345 observability gap). The address portion is
    /// best-effort for non-gateway nodes (listener fallback until
    /// external-address discovery — same caveat as the shadow-RTT
    /// events, see `p2p_impl.rs` and #4294).
    local_peer_id: String,
}

#[allow(dead_code)]
enum TelemetryCommand {
    Event(TelemetryEvent),
    Shutdown, // TODO(#2456): implement graceful shutdown - send this when node shuts down
}

#[derive(Debug, Clone, Serialize)]
struct TelemetryEvent {
    timestamp: u64,
    peer_id: String,
    transaction_id: String,
    event_type: String,
    event_data: serde_json::Value,
    /// Admission priority. Not serialized to OTLP — it only steers the
    /// in-process rate limiter (#4380). `#[serde(skip)]` keeps the wire
    /// payload byte-identical to pre-#4380 so the collector schema is
    /// unchanged.
    #[serde(skip)]
    priority: EventPriority,
}

fn is_network_efficiency_event(event: &TelemetryEvent) -> bool {
    event.event_type == "router_snapshot"
        && event
            .event_data
            .get("network_efficiency_v1")
            .is_some_and(|value| !value.is_null())
}

/// Enqueue without blocking, preserving the reporter's original best-effort
/// behavior while making both local loss outcomes observable.
fn try_enqueue_event(
    sender: &mpsc::Sender<TelemetryCommand>,
    metrics: &TelemetryLocalMetrics,
    event: TelemetryEvent,
) {
    metrics
        .enqueue_attempts_total
        .fetch_add(1, Ordering::Relaxed);
    let is_efficiency = is_network_efficiency_event(&event);
    if is_efficiency {
        metrics
            .network_efficiency_delivery
            .attempts
            .fetch_add(1, Ordering::Relaxed);
    }
    let event_type = event.event_type.clone();
    if let Some(shadow) = metrics.shadow(&event_type) {
        shadow
            .enqueue_attempts_total
            .fetch_add(1, Ordering::Relaxed);
    }
    match sender.try_send(TelemetryCommand::Event(event)) {
        Ok(()) => {
            metrics
                .enqueue_succeeded_total
                .fetch_add(1, Ordering::Relaxed);
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics
                .enqueue_dropped_full_total
                .fetch_add(1, Ordering::Relaxed);
            if let Some(shadow) = metrics.shadow(&event_type) {
                shadow
                    .enqueue_dropped_full_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            if is_efficiency {
                metrics
                    .network_efficiency_delivery
                    .enqueue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            metrics
                .enqueue_dropped_closed_total
                .fetch_add(1, Ordering::Relaxed);
            if let Some(shadow) = metrics.shadow(&event_type) {
                shadow
                    .enqueue_dropped_closed_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            if is_efficiency {
                metrics
                    .network_efficiency_delivery
                    .enqueue_closed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Best-effort runtime detection of a cargo test/bench harness.
///
/// Cargo builds every unit-test, integration-test and bench binary into a
/// `deps/` directory (e.g. `target/debug/deps/operations_before_join-<hash>`)
/// and runs it from there. The real node binary is run as `…/freenet` (from a
/// `bin/` install dir, or `target/<profile>/freenet` under `cargo run`), whose
/// parent directory is never `deps`. So "the current executable's parent dir is
/// named `deps`" cleanly distinguishes a cargo test harness from a production
/// node, regardless of which Cargo features were enabled.
///
/// This is the only signal that catches the documented local dev command
/// `cargo test -p freenet` run WITHOUT `--features testing`: there the library
/// is a plain dependency (so `cfg!(test)` is false) and the integration tests
/// build `ConfigArgs::default()` (so `is_test_environment` is false). See
/// issue #4366.
///
/// Best-effort: if `current_exe()` fails we return `false` and fall back to the
/// compile-time guards, which already cover unit tests and CI.
pub(crate) fn running_under_cargo_test() -> bool {
    std::env::current_exe()
        .ok()
        .and_then(|exe| {
            exe.parent()
                .and_then(|dir| dir.file_name())
                .map(|name| name == "deps")
        })
        .unwrap_or(false)
}

/// Why a [`TelemetryReporter`] was not initialized.
///
/// Extracted from [`TelemetryReporter::new`] so the prod-vs-test decision is a
/// pure function that can be unit-tested for BOTH directions without being
/// defeated by the ambient `cfg(test)` / cargo-`deps/` environment of the test
/// process itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TelemetrySuppression {
    /// Operator turned telemetry off (`telemetry-enabled = false`).
    Disabled,
    /// `--id` test environment (integration/CLI harness sets `is_test_environment`).
    TestEnvironmentFlag,
    /// A `cfg(test)` build or a binary running under a cargo test/bench harness.
    TestHarness,
}

/// Decide whether telemetry should be suppressed, given the config plus the two
/// test signals that a genuine production release binary never trips.
///
/// Pure and side-effect free: callers pass `cfg!(test)` and the result of
/// [`running_under_cargo_test`] so this can be unit-tested for a prod release
/// binary (`is_test_build = false`, `running_under_cargo_test = false`, no
/// `--id` → MUST return `None`) AND for every test/sim path (MUST return
/// `Some`).
///
/// CRITICAL (#4366 intent + the 0.2.81 telemetry-blackout regression):
/// suppression is keyed ONLY on signals a real release binary never matches —
/// it is deliberately NOT keyed on `cfg!(feature = "testing")`. That compile
/// flag leaked onto the shipped `freenet` binary via Cargo resolver-v2 feature
/// unification with `fdev` (which depends on `freenet` with
/// `features = ["testing"]`), so every auto-updating release node since 0.2.81
/// silently self-disabled telemetry. The real test/sim runs that #4366 must
/// keep suppressed all run either under `cfg(test)` (unit tests) or from a
/// cargo `deps/` harness (integration tests + nightly nextest sims), so the two
/// remaining signals still catch them. The DST simulation framework
/// (`node/testing_impl.rs`, used by `fdev` sims) never constructs a
/// `TelemetryReporter` at all, so it cannot emit regardless.
fn telemetry_suppression_reason(
    config: &TelemetryConfig,
    is_test_build: bool,
    running_under_cargo_test: bool,
) -> Option<TelemetrySuppression> {
    if !config.enabled {
        return Some(TelemetrySuppression::Disabled);
    }
    if config.is_test_environment {
        return Some(TelemetrySuppression::TestEnvironmentFlag);
    }
    if is_test_build || running_under_cargo_test {
        return Some(TelemetrySuppression::TestHarness);
    }
    None
}

impl TelemetryReporter {
    /// Create a new telemetry reporter.
    ///
    /// `local_peer_id` is this node's own peer id (public key +
    /// best-effort address), used to attribute transport-level events
    /// that have no `NetLogMessage` context.
    ///
    /// Returns None if telemetry is disabled, in a `--id` test environment,
    /// or in a test build/harness (`cfg(test)` / running under a cargo
    /// `deps/` harness — see `running_under_cargo_test` and
    /// `telemetry_suppression_reason`).
    pub fn new(config: &TelemetryConfig, local_peer_id: String) -> Option<Self> {
        // The suppression decision lives in the pure `telemetry_suppression_reason`
        // so it can be unit-tested for both a production release binary (must NOT
        // suppress) and every test/sim path (must suppress). See that function for
        // why suppression is deliberately NOT keyed on `cfg!(feature = "testing")`
        // — doing so silently disabled telemetry on every 0.2.81+ release node when
        // Cargo feature-unification with `fdev` leaked `testing` onto the shipped
        // `freenet` binary.
        //
        // #4366 intent preserved: the IN-PROCESS integration tests under
        // `crates/core/tests/*.rs` (operations_before_join / hosted_mode_* /
        // in_process_restart / …) build `NodeConfig` directly without `--id`, so
        // `is_test_environment` stays false; they still trip the
        // `running_under_cargo_test()` signal (every cargo test binary runs from a
        // `deps/` dir), so they continue to be suppressed and do not re-pollute the
        // production OTLP collector.
        //
        // CAVEAT: that `deps/` signal only covers nodes running INSIDE the test
        // binary. A test that spawns the real `freenet` binary as a SUBPROCESS
        // (e.g. `crates/core/tests/persistence_roundtrip.rs`) runs it from
        // `target/<profile>/freenet`, whose parent is not `deps/`, and without
        // `--id` — so neither test signal fires and the subprocess would default
        // telemetry ON. Such tests MUST pin it off explicitly with
        // `.env("FREENET_TELEMETRY_ENABLED", "false")` (the same env this flag
        // reads). Do not assume subprocess-spawned nodes are auto-suppressed.
        match telemetry_suppression_reason(config, cfg!(test), running_under_cargo_test()) {
            Some(TelemetrySuppression::Disabled) => {
                tracing::info!("Telemetry reporting is disabled");
                return None;
            }
            Some(TelemetrySuppression::TestEnvironmentFlag) => {
                tracing::info!("Telemetry disabled in test environment (--id flag detected)");
                return None;
            }
            Some(TelemetrySuppression::TestHarness) => {
                tracing::info!("Telemetry disabled in test build/harness");
                return None;
            }
            None => {}
        }

        let endpoint = config.endpoint.clone();
        let transport_snapshot_interval_secs = config.transport_snapshot_interval_secs;

        tracing::info!(
            endpoint = %endpoint,
            transport_snapshot_interval_secs,
            "Telemetry reporting enabled"
        );

        // Channel capacity: 1000 events provides ~100 seconds of buffer at max rate (10 events/sec)
        // plus headroom for bursts. Events are dropped via try_send if channel is full.
        let (sender, receiver) = mpsc::channel(1000);
        let (network_efficiency_sender, network_efficiency_receiver) = mpsc::channel(1);

        // Standalone emitters, the reporter, and the worker must update the same
        // counters. Repeated initialization keeps the first global sender (the
        // pre-existing OnceLock behavior) and also reuses its metrics so the
        // crate-visible snapshot never silently switches accumulator instances.
        let proposed_metrics = Arc::new(TelemetryLocalMetrics::default());
        let global = TELEMETRY_SENDER.get_or_init(|| TelemetryChannel {
            sender: sender.clone(),
            metrics: proposed_metrics.clone(),
        });
        let metrics = global.metrics.clone();

        // Initialize the transfer event channel for per-transfer telemetry
        let transfer_event_receiver = crate::transport::metrics::init_transfer_event_channel();

        // Spawn the background worker
        let worker = TelemetryWorker::new_with_network_efficiency_receiver(
            endpoint,
            receiver,
            network_efficiency_receiver,
            transport_snapshot_interval_secs,
            transfer_event_receiver,
            local_peer_id.clone(),
            metrics.clone(),
        );
        GlobalExecutor::spawn(worker.run());

        Some(Self {
            sender,
            network_efficiency_sender,
            metrics,
            local_peer_id,
        })
    }

    async fn send_event(&self, mut event: TelemetryEvent) {
        let sender = if is_network_efficiency_event(&event) {
            event.priority = EventPriority::NetworkEfficiency;
            &self.network_efficiency_sender
        } else {
            &self.sender
        };
        try_enqueue_event(sender, &self.metrics, event);
    }
}

impl NetEventRegister for TelemetryReporter {
    fn register_events<'a>(
        &'a self,
        logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async move {
            for log_msg in NetLogMessage::to_log_message(logs) {
                let event_type = event_kind_to_string(&log_msg.kind);
                // Aggregate connect-emission counts onto the periodic
                // RouterSnapshot (`connect_accepts_emitted`/`_rejects_emitted`).
                // This is the exact point the per-event `connect_connected` /
                // `connect_rejected` firehose (~92k events / 18 min) is emitted
                // to the collector.
                //
                // TODO(follow-up): retire per-event connect_rejected/
                // connect_connected once the dashboard reads the snapshot
                // counters — deferred because an external dashboard likely still
                // consumes the per-event stream (topology / rejection panels).
                // Retiring here (skip the two variants) would make net telemetry
                // volume NEGATIVE.
                //
                // BEFORE retiring connect_rejected, read #5335. The terminus-
                // rejection log lines in operations/connect.rs are `debug!`,
                // which `release_max_level_info` compiles out of release
                // builds, so this per-event stream's `reason` field is now the
                // ONLY thing that distinguishes those causes in production.
                // The snapshot counters have no reason dimension, so retiring
                // the per-event stream without adding one would silently
                // delete that signal entirely rather than relocate it.
                match event_type.as_str() {
                    "connect_connected" => {
                        crate::node::network_status::record_connect_accept_emitted()
                    }
                    "connect_rejected" => {
                        crate::node::network_status::record_connect_reject_emitted()
                    }
                    _ => {}
                }
                let priority = priority_for_event_type(&event_type);
                let event = TelemetryEvent {
                    timestamp: current_timestamp_ms(),
                    peer_id: log_msg.peer_id.to_string(),
                    transaction_id: log_msg.tx.to_string(),
                    event_type,
                    event_data: event_kind_to_json(&log_msg.kind),
                    priority,
                };
                self.send_event(event).await;
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(
        &mut self,
        tx: Transaction,
        op_type: &str,
        target_peer: Option<String>,
    ) -> BoxFuture<'_, ()> {
        let sender = self.sender.clone();
        let metrics = self.metrics.clone();
        let op_type = op_type.to_string();
        let local_peer_id = self.local_peer_id.clone();
        async move {
            let event = TelemetryEvent {
                timestamp: current_timestamp_ms(),
                peer_id: local_peer_id,
                transaction_id: tx.to_string(),
                event_type: "timeout".to_string(),
                event_data: serde_json::json!({
                    "transaction": tx.to_string(),
                    "op_type": op_type,
                    "target_peer": target_peer,
                }),
                priority: priority_for_event_type("timeout"),
            };
            try_enqueue_event(&sender, &metrics, event);
        }
        .boxed()
    }
}

/// Background worker that batches and sends telemetry events.
struct TelemetryWorker {
    endpoint: String,
    receiver: mpsc::Receiver<TelemetryCommand>,
    network_efficiency_receiver: mpsc::Receiver<TelemetryCommand>,
    network_efficiency_channel_open: bool,
    buffer: Vec<TelemetryEvent>,
    http_client: reqwest::Client,
    backoff_ms: u64,
    last_send: Instant,
    events_this_second: usize,
    rate_limit_window_start: Instant,
    /// Interval for transport snapshots (0 = disabled)
    transport_snapshot_interval_secs: u64,
    /// Receiver for per-transfer telemetry events from transport layer
    transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
    /// This node's own peer id — see `TelemetryReporter::local_peer_id`.
    local_peer_id: String,
    /// Batch ticks counted since startup, used to throttle
    /// `resource_utilization` emits to one per
    /// [`RESOURCE_TELEMETRY_BATCH_INTERVAL`] ticks (~60s). Advanced by
    /// [`Self::should_emit_resource_utilization`] on every batch tick.
    resource_emit_batch_counter: u64,
    /// Shared with every producer feeding this worker so enqueue and worker-side
    /// losses reconcile into one process-local snapshot.
    metrics: Arc<TelemetryLocalMetrics>,
}

impl TelemetryWorker {
    #[cfg(test)]
    fn new(
        endpoint: String,
        receiver: mpsc::Receiver<TelemetryCommand>,
        transport_snapshot_interval_secs: u64,
        transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
        local_peer_id: String,
        metrics: Arc<TelemetryLocalMetrics>,
    ) -> Self {
        let (_network_efficiency_sender, network_efficiency_receiver) = mpsc::channel(1);
        Self::new_inner(
            endpoint,
            receiver,
            network_efficiency_receiver,
            false,
            transport_snapshot_interval_secs,
            transfer_event_receiver,
            local_peer_id,
            metrics,
        )
    }

    fn new_with_network_efficiency_receiver(
        endpoint: String,
        receiver: mpsc::Receiver<TelemetryCommand>,
        network_efficiency_receiver: mpsc::Receiver<TelemetryCommand>,
        transport_snapshot_interval_secs: u64,
        transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
        local_peer_id: String,
        metrics: Arc<TelemetryLocalMetrics>,
    ) -> Self {
        Self::new_inner(
            endpoint,
            receiver,
            network_efficiency_receiver,
            true,
            transport_snapshot_interval_secs,
            transfer_event_receiver,
            local_peer_id,
            metrics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        endpoint: String,
        receiver: mpsc::Receiver<TelemetryCommand>,
        network_efficiency_receiver: mpsc::Receiver<TelemetryCommand>,
        network_efficiency_channel_open: bool,
        transport_snapshot_interval_secs: u64,
        transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
        local_peer_id: String,
        metrics: Arc<TelemetryLocalMetrics>,
    ) -> Self {
        Self {
            endpoint,
            receiver,
            network_efficiency_receiver,
            network_efficiency_channel_open,
            buffer: Vec::with_capacity(MAX_BUFFER_SIZE + NETWORK_EFFICIENCY_BUFFER_RESERVE),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build telemetry HTTP client; check TLS/proxy configuration"),
            backoff_ms: 0,
            last_send: Instant::now(),
            events_this_second: 0,
            rate_limit_window_start: Instant::now(),
            transport_snapshot_interval_secs,
            transfer_event_receiver,
            local_peer_id,
            resource_emit_batch_counter: 0,
            metrics,
        }
    }

    /// Wrap a transport metrics snapshot into a `TelemetryEvent`.
    /// Node-wide metrics, attributed to this node's own peer id —
    /// same #4345 rationale as `transfer_event_to_telemetry`.
    fn snapshot_to_telemetry(
        &self,
        snapshot: &crate::transport::metrics::TransportSnapshot,
    ) -> TelemetryEvent {
        TelemetryEvent {
            timestamp: current_timestamp_ms(),
            peer_id: self.local_peer_id.clone(),
            transaction_id: String::new(), // Not tied to a transaction
            event_type: "transport_snapshot".to_string(),
            event_data: snapshot.upload_json().unwrap_or_else(|error| {
                tracing::warn!(%error, "Failed to serialize transport snapshot");
                serde_json::Value::Null
            }),
            priority: priority_for_event_type("transport_snapshot"),
        }
    }

    /// Sample this node's OWN resource utilization (memory RSS + ceiling, CPU
    /// time, cumulative bandwidth) into a `resource_utilization` telemetry
    /// event, attributed to this node's peer id.
    ///
    /// Foundational piece (A1) of the demand-driven hosting redesign
    /// (freenet/freenet-core#4642): the capability-relative hosting budget and
    /// eviction policy (pieces A2/A3) must be validated against a node's real
    /// scarcest-resource headroom, which was previously not observable at all.
    /// Sampled from cheap process-local sources — see
    /// [`crate::node::resource_metrics`]; nothing crosses the peer wire.
    fn resource_utilization_telemetry(&self) -> TelemetryEvent {
        TelemetryEvent {
            timestamp: current_timestamp_ms(),
            peer_id: self.local_peer_id.clone(),
            transaction_id: String::new(), // Node-wide, not tied to a transaction.
            event_type: "resource_utilization".to_string(),
            event_data: crate::node::resource_metrics::sample().to_telemetry_value(),
            priority: priority_for_event_type("resource_utilization"),
        }
    }

    /// Decide whether this batch tick should emit a `resource_utilization`
    /// sample, advancing the internal batch-tick counter as a side effect.
    ///
    /// Returns `true` on the first tick (counter starts at 0, for prompt
    /// startup visibility) and once every [`RESOURCE_TELEMETRY_BATCH_INTERVAL`]
    /// ticks thereafter, so the sample lands on a ~60s cadence rather than on
    /// every 10s batch tick — see [`RESOURCE_TELEMETRY_BATCH_INTERVAL`] for
    /// why. Pulled out of [`Self::run`] so the reduced cadence is unit-testable
    /// without driving the async interval/flush machinery (same approach as
    /// [`Self::admit_event`]).
    fn should_emit_resource_utilization(&mut self) -> bool {
        let emit = self.resource_emit_batch_counter % RESOURCE_TELEMETRY_BATCH_INTERVAL == 0;
        // wrapping_add so the counter can never panic on overflow; the modulo
        // phase is irrelevant on the ~5.8-trillion-year horizon that would take.
        self.resource_emit_batch_counter = self.resource_emit_batch_counter.wrapping_add(1);
        emit
    }

    /// Wrap a transport-layer transfer event into a `TelemetryEvent`,
    /// stamped with this node's own peer id. The transport layer has
    /// no peer-identity context of its own, so before #4345 these
    /// events carried an empty `peer_id` and sender attribution in the
    /// collector was impossible.
    fn transfer_event_to_telemetry(&self, transfer_event: super::TransferEvent) -> TelemetryEvent {
        let event_type = match &transfer_event {
            super::TransferEvent::Started { .. } => "transfer_started",
            super::TransferEvent::Completed { .. } => "transfer_completed",
            super::TransferEvent::Failed { .. } => "transfer_failed",
        };
        TelemetryEvent {
            timestamp: current_timestamp_ms(),
            peer_id: self.local_peer_id.clone(),
            transaction_id: String::new(), // Not available at transport layer
            event_type: event_type.to_string(),
            event_data: event_kind_to_json(&super::EventKind::Transfer(transfer_event)),
            priority: priority_for_event_type(event_type),
        }
    }

    async fn run(mut self) {
        let mut batch_interval = tokio::time::interval(Duration::from_secs(BATCH_INTERVAL_SECS));
        // `Delay`, not tokio's default `Burst`: after a stall, Burst fires
        // every missed tick at once, and the ticks emit periodic events,
        // which bypass the rate limit (#6001).
        batch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Transport snapshots are optional (disabled if interval is 0)
        // Clamp to minimum to prevent spamming telemetry server
        let snapshot_interval_secs = if self.transport_snapshot_interval_secs == 0 {
            // Use a long interval but we'll skip the actual snapshot logic
            u64::MAX / 2 // Effectively disabled
        } else if self.transport_snapshot_interval_secs < MIN_TRANSPORT_SNAPSHOT_INTERVAL_SECS {
            tracing::warn!(
                "Transport snapshot interval {}s is below minimum {}s, using minimum",
                self.transport_snapshot_interval_secs,
                MIN_TRANSPORT_SNAPSHOT_INTERVAL_SECS
            );
            MIN_TRANSPORT_SNAPSHOT_INTERVAL_SECS
        } else {
            self.transport_snapshot_interval_secs
        };
        let mut transport_snapshot_interval =
            tokio::time::interval(Duration::from_secs(snapshot_interval_secs));
        // Same reason as `batch_interval`.
        transport_snapshot_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            crate::deterministic_select! {
                cmd = async {
                    if self.network_efficiency_channel_open {
                        self.network_efficiency_receiver.recv().await
                    } else {
                        std::future::pending::<Option<TelemetryCommand>>().await
                    }
                } => {
                    match cmd {
                        Some(TelemetryCommand::Event(event)) => {
                            self.handle_event(event).await;
                        }
                        Some(TelemetryCommand::Shutdown) => {
                            self.flush().await;
                            break;
                        }
                        None => {
                            // Reporter clones own both senders, but the global
                            // standalone channel can keep the ordinary sender
                            // alive after reporter shutdown. Disable only this
                            // branch so a closed reserved channel cannot spin.
                            self.network_efficiency_channel_open = false;
                        }
                    }
                },
                cmd = self.receiver.recv() => {
                    match cmd {
                        Some(TelemetryCommand::Event(event)) => {
                            self.handle_event(event).await;
                        }
                        Some(TelemetryCommand::Shutdown) | None => {
                            // Flush remaining events before shutdown
                            self.flush().await;
                            break;
                        }
                    }
                },
                transfer_event = self.transfer_event_receiver.recv() => {
                    // Handle per-transfer telemetry events from transport layer
                    if let Some(transfer_event) = transfer_event {
                        let event = self.transfer_event_to_telemetry(transfer_event);
                        self.handle_event(event).await;
                    }
                },
                _ = batch_interval.tick() => {
                    // Emit this node's own resource-utilization sample (#4642
                    // A1) so a peer's memory/CPU/bandwidth headroom is
                    // observable on the telemetry stream the dashboard consumes.
                    // Throttled to a ~60s cadence (every
                    // RESOURCE_TELEMETRY_BATCH_INTERVAL batch ticks), NOT every
                    // 10s tick: it's a slowly-changing, node-wide signal and
                    // every production peer reports to the central collector by
                    // default, so a 10s cadence needlessly multiplies collector
                    // load across the fleet. Cheap process-local sources;
                    // buffered like any other event. The flush below still runs
                    // on every batch tick — only the resource sample is
                    // throttled, not the batch/flush cadence.
                    if self.should_emit_resource_utilization() {
                        let resource_event = self.resource_utilization_telemetry();
                        self.handle_event(resource_event).await;
                    }
                    self.flush().await;
                },
                _ = transport_snapshot_interval.tick() => {
                    // Emit transport layer metrics snapshot (only if enabled)
                    if self.transport_snapshot_interval_secs > 0 {
                        if let Some(snapshot) = TRANSPORT_METRICS.take_snapshot() {
                            let event = self.snapshot_to_telemetry(&snapshot);
                            self.handle_event(event).await;
                        }
                    }
                },
            }
        }
    }

    /// Decide whether an event of the given priority is admitted under the
    /// per-second rate limit, updating the window counters as a side effect.
    ///
    /// Pulled out of [`Self::handle_event`] so the admission policy is
    /// unit-testable without driving the async buffer/backoff/HTTP machinery.
    /// Returns `true` if the event is admitted.
    ///
    /// Policy:
    /// - The window rolls over once `now` is >= 1s past its start, resetting
    ///   the count.
    /// - `Operational` events are admitted while the count is below
    ///   [`MAX_EVENTS_PER_SECOND`].
    /// - `Periodic` events of a type listed in [`PERIODIC_EVENT_TYPES`]
    ///   bypass the count and do not consume it, so they neither wait for nor
    ///   displace operational events. Their fixed producer timers provide the
    ///   bound (#6001). A `Periodic` event of any other type is treated as
    ///   `Operational`, so the bypass cannot grow beyond that fixed list.
    /// - `NetworkEfficiency` bypasses the count too. Its dedicated one-slot
    ///   channel, reserved buffer slot, and fixed 30-minute producer cadence
    ///   provide the bound.
    ///
    /// Every outcome is also counted per event type
    /// ([`TelemetryLocalMetricsSnapshot::event_type_admissions`]).
    fn admit_event(&mut self, priority: EventPriority, event_type: &str, now: Instant) -> bool {
        if now.duration_since(self.rate_limit_window_start) >= Duration::from_secs(1) {
            self.rate_limit_window_start = now;
            self.events_this_second = 0;
        }

        #[allow(deprecated)] // `Shadow` is handled as `Periodic`.
        let (bypasses_cap, is_periodic) = match priority {
            EventPriority::NetworkEfficiency => (true, false),
            EventPriority::Periodic | EventPriority::Shadow => {
                let listed = is_periodic_event_type(event_type);
                (listed, listed)
            }
            EventPriority::Operational => (false, false),
        };

        if bypasses_cap {
            if is_periodic {
                self.metrics
                    .rate_limit_admitted_periodic_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        } else if self.events_this_second >= MAX_EVENTS_PER_SECOND {
            self.metrics
                .dropped_aggregate_limit_operational_total
                .fetch_add(1, Ordering::Relaxed);
            self.metrics.event_types.record(event_type, false);
            return false;
        } else {
            self.events_this_second += 1;
            self.metrics
                .rate_limit_admitted_operational_total
                .fetch_add(1, Ordering::Relaxed);
        }

        self.metrics.event_types.record(event_type, true);
        if let Some(shadow) = self.metrics.shadow(event_type) {
            shadow
                .rate_limit_admitted_total
                .fetch_add(1, Ordering::Relaxed);
        }
        true
    }

    async fn handle_event(&mut self, event: TelemetryEvent) {
        let is_efficiency = is_network_efficiency_event(&event);
        // Rate limiting (periodic streams bypass the cap, #6001)
        if !self.admit_event(event.priority, &event.event_type, Instant::now()) {
            if is_efficiency {
                self.metrics
                    .network_efficiency_delivery
                    .rate_limit_drop
                    .fetch_add(1, Ordering::Relaxed);
            }
            // Drop event due to rate limiting
            return;
        }
        if is_efficiency {
            self.metrics
                .network_efficiency_delivery
                .admitted
                .fetch_add(1, Ordering::Relaxed);

            // A collector outage can span multiple 30-minute ticks. Keep the
            // newest diagnostic rather than consuming more reserved memory.
            // Its lifetime counters subsume the older counters and its gauges
            // are the freshest point sample, but this intentionally discards
            // one historical gauge sample, so record that terminal outcome.
            if let Some(existing) = self.buffer.iter_mut().find(|queued| {
                queued.priority == EventPriority::NetworkEfficiency
                    || is_network_efficiency_event(queued)
            }) {
                self.metrics
                    .network_efficiency_delivery
                    .coalesced
                    .fetch_add(1, Ordering::Relaxed);
                *existing = event;
                return;
            }
        }

        // Drop events if buffer is full and we're in backoff (prevents unbounded memory growth)
        let buffer_capacity = MAX_BUFFER_SIZE
            + usize::from(self.buffer.iter().any(is_network_efficiency_event))
                * NETWORK_EFFICIENCY_BUFFER_RESERVE;
        if !is_efficiency && self.buffer.len() >= buffer_capacity && self.backoff_ms > 0 {
            let elapsed = self.last_send.elapsed();
            if elapsed < Duration::from_millis(self.backoff_ms) {
                // Still in backoff and buffer full - drop event
                self.metrics
                    .dropped_backoff_buffer_full_total
                    .fetch_add(1, Ordering::Relaxed);
                if let Some(shadow) = self.metrics.shadow(&event.event_type) {
                    shadow
                        .dropped_backoff_buffer_full_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                return;
            }
        }

        self.buffer.push(event);
        debug_assert!(
            self.buffer.len() <= MAX_BUFFER_SIZE + NETWORK_EFFICIENCY_BUFFER_RESERVE,
            "only the fixed network-efficiency event may occupy the reserved slot"
        );

        // Send if buffer is full
        if self.buffer.len() >= MAX_BUFFER_SIZE {
            self.flush().await;
        }
    }

    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        // Check backoff
        if self.backoff_ms > 0 {
            let elapsed = self.last_send.elapsed();
            if elapsed < Duration::from_millis(self.backoff_ms) {
                // Still in backoff period - keep events in buffer
                return;
            }
        }

        let events = std::mem::take(&mut self.buffer);
        self.buffer = Vec::with_capacity(MAX_BUFFER_SIZE + NETWORK_EFFICIENCY_BUFFER_RESERVE);

        self.metrics
            .batch_send_attempts_total
            .fetch_add(1, Ordering::Relaxed);

        match self.send_batch(&events).await {
            Ok(()) => {
                self.metrics
                    .batch_send_successes_total
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .batch_events_sent_total
                    .fetch_add(events.len() as u64, Ordering::Relaxed);
                for event in &events {
                    if let Some(shadow) = self.metrics.shadow(&event.event_type) {
                        shadow.sent_total.fetch_add(1, Ordering::Relaxed);
                    }
                    if is_network_efficiency_event(event) {
                        self.metrics
                            .network_efficiency_delivery
                            .sent
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                // Reset backoff on success
                self.backoff_ms = 0;
                self.last_send = Instant::now();
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to send telemetry batch, will retry with backoff");

                // Exponential backoff with jitter (0-25% of base backoff) to prevent thundering herd
                let base_backoff = if self.backoff_ms == 0 {
                    INITIAL_BACKOFF_MS
                } else {
                    (self.backoff_ms * 2).min(MAX_BACKOFF_MS)
                };
                let jitter = GlobalRng::random_range(0..=(base_backoff / 4));
                self.backoff_ms = base_backoff + jitter;
                self.last_send = Instant::now();

                self.requeue_failed_batch(events);
            }
        }
    }

    /// Restore a failed HTTP batch without changing the existing retry policy,
    /// accounting for the otherwise-silent tail if capacity ever becomes
    /// smaller than the failed batch. Under today's serial worker this tail is
    /// normally zero; keeping the counter at the actual truncation point makes
    /// that invariant observable and regression-testable.
    fn requeue_failed_batch(&mut self, events: Vec<TelemetryEvent>) {
        self.metrics
            .batch_send_failures_total
            .fetch_add(1, Ordering::Relaxed);
        let has_network_efficiency = self.buffer.iter().any(is_network_efficiency_event)
            || events.iter().any(is_network_efficiency_event);
        let capacity = MAX_BUFFER_SIZE
            + usize::from(has_network_efficiency) * NETWORK_EFFICIENCY_BUFFER_RESERVE;
        let remaining_capacity = capacity.saturating_sub(self.buffer.len());
        let retained = events.len().min(remaining_capacity);
        let truncated = events.len().saturating_sub(retained);
        if truncated > 0 {
            self.metrics
                .batch_retry_truncated_events_total
                .fetch_add(truncated as u64, Ordering::Relaxed);
            for event in &events[retained..] {
                if let Some(shadow) = self.metrics.shadow(&event.event_type) {
                    shadow.retry_truncated_total.fetch_add(1, Ordering::Relaxed);
                }
                if is_network_efficiency_event(event) {
                    self.metrics
                        .network_efficiency_delivery
                        .retry_truncated
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.buffer.extend(events.into_iter().take(retained));
    }

    async fn send_batch(&self, events: &[TelemetryEvent]) -> Result<(), reqwest::Error> {
        // Convert to OTLP JSON format
        let otlp_payload = to_otlp_logs(events);

        let url = format!("{}/v1/logs", self.endpoint);

        self.http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&otlp_payload)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}

fn to_otlp_logs(events: &[TelemetryEvent]) -> serde_json::Value {
    let log_records: Vec<serde_json::Value> = events
        .iter()
        .map(|e| {
            let body_string = serde_json::to_string(&e.event_data).unwrap_or_else(|err| {
                tracing::warn!(
                    error = %err,
                    event_type = %e.event_type,
                    "Failed to serialize telemetry event_data"
                );
                String::new()
            });
            serde_json::json!({
                "timeUnixNano": e.timestamp * 1_000_000, // Convert ms to ns (OTLP expects numeric)
                "severityNumber": 9, // INFO
                "severityText": "INFO",
                "body": {
                    "stringValue": body_string
                },
                "attributes": [
                    {
                        "key": "peer_id",
                        "value": {"stringValue": &e.peer_id}
                    },
                    {
                        "key": "transaction_id",
                        "value": {"stringValue": &e.transaction_id}
                    },
                    {
                        "key": "event_type",
                        "value": {"stringValue": &e.event_type}
                    }
                ]
            })
        })
        .collect();

    serde_json::json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    {
                        "key": "service.name",
                        "value": {"stringValue": "freenet-peer"}
                    },
                    {
                        // Standard OTLP placement for the sender's
                        // crate version — lets collector queries
                        // attribute every event type to a release
                        // without joining against peer_startup events
                        // (#4345 observability gap).
                        "key": "service.version",
                        "value": {"stringValue": env!("CARGO_PKG_VERSION")}
                    }
                ]
            },
            "scopeLogs": [{
                "scope": {
                    "name": "freenet.telemetry"
                },
                "logRecords": log_records
            }]
        }]
    })
}

fn event_kind_to_string(kind: &EventKind) -> String {
    match kind {
        EventKind::Connect(connect_event) => {
            use super::ConnectEvent;
            match connect_event {
                ConnectEvent::StartConnection { .. } => "connect_start".to_string(),
                ConnectEvent::Connected { .. } => "connect_connected".to_string(),
                ConnectEvent::Finished { .. } => "connect_finished".to_string(),
                ConnectEvent::RequestSent { .. } => "connect_request_sent".to_string(),
                ConnectEvent::RequestReceived { .. } => "connect_request_received".to_string(),
                ConnectEvent::ResponseSent { .. } => "connect_response_sent".to_string(),
                ConnectEvent::ResponseReceived { .. } => "connect_response_received".to_string(),
                ConnectEvent::Rejected { .. } => "connect_rejected".to_string(),
            }
        }
        EventKind::Disconnected { .. } => "disconnect".to_string(),
        EventKind::Put(put_event) => {
            use super::PutEvent;
            match put_event {
                PutEvent::Request { .. } => "put_request".to_string(),
                PutEvent::PutSuccess { .. } => "put_success".to_string(),
                PutEvent::PutFailure { .. } => "put_failure".to_string(),
                PutEvent::ResponseSent { .. } => "put_response_sent".to_string(),
                PutEvent::BroadcastEmitted { .. } => "put_broadcast_emitted".to_string(),
                PutEvent::BroadcastReceived { .. } => "put_broadcast_received".to_string(),
            }
        }
        EventKind::Get(get_event) => {
            use super::GetEvent;
            match get_event {
                GetEvent::Request { .. } => "get_request".to_string(),
                GetEvent::GetSuccess { .. } => "get_success".to_string(),
                GetEvent::GetNotFound { .. } => "get_not_found".to_string(),
                GetEvent::GetFailure { .. } => "get_failure".to_string(),
                GetEvent::ResponseSent { .. } => "get_response_sent".to_string(),
                GetEvent::ForwardingAckSent { .. } => "get_forwarding_ack_sent".to_string(),
                GetEvent::ForwardingAckReceived { .. } => "get_forwarding_ack_received".to_string(),
                GetEvent::ClientTerminal { .. } => "get_terminal".to_string(),
            }
        }
        EventKind::Subscribe(subscribe_event) => {
            use super::SubscribeEvent;
            match subscribe_event {
                SubscribeEvent::Request { .. } => "subscribe_request".to_string(),
                SubscribeEvent::SubscribeSuccess { .. } => "subscribe_success".to_string(),
                SubscribeEvent::SubscribeNotFound { .. } => "subscribe_not_found".to_string(),
                SubscribeEvent::SubscribeTimeout { .. } => "subscribe_timeout".to_string(),
                SubscribeEvent::ResponseSent { .. } => "subscribe_response_sent".to_string(),
                SubscribeEvent::HostingStarted { .. } => "hosting_started".to_string(),
                SubscribeEvent::HostingStopped { .. } => "hosting_stopped".to_string(),
                // Reserved discriminants for removed variants
                SubscribeEvent::_Reserved6
                | SubscribeEvent::_Reserved7
                | SubscribeEvent::_Reserved8
                | SubscribeEvent::_Reserved9
                | SubscribeEvent::_Reserved10 => "subscribe_reserved".to_string(),
                SubscribeEvent::UnsubscribeSent { .. } => "unsubscribe_sent".to_string(),
                SubscribeEvent::UnsubscribeReceived { .. } => "unsubscribe_received".to_string(),
            }
        }
        EventKind::Update(update_event) => {
            use super::UpdateEvent;
            match update_event {
                UpdateEvent::Request { .. } => "update_request".to_string(),
                UpdateEvent::UpdateSuccess { .. } => "update_success".to_string(),
                UpdateEvent::BroadcastEmitted { .. } => "update_broadcast_emitted".to_string(),
                UpdateEvent::BroadcastComplete { .. } => "update_broadcast_complete".to_string(),
                UpdateEvent::BroadcastReceived { .. } => "update_broadcast_received".to_string(),
                UpdateEvent::BroadcastApplied { .. } => "update_broadcast_applied".to_string(),
                UpdateEvent::UpdateFailure { .. } => "update_failure".to_string(),
                UpdateEvent::BroadcastDeliverySummary { .. } => {
                    "update_broadcast_delivery_summary".to_string()
                }
            }
        }
        EventKind::Transfer(transfer_event) => {
            use super::TransferEvent;
            match transfer_event {
                TransferEvent::Started { .. } => "transfer_started".to_string(),
                TransferEvent::Completed { .. } => "transfer_completed".to_string(),
                TransferEvent::Failed { .. } => "transfer_failed".to_string(),
            }
        }
        EventKind::Route(route_event) => {
            use crate::router::RouteOutcome;
            match &route_event.outcome {
                RouteOutcome::Success { .. } => "route_success".to_string(),
                RouteOutcome::SuccessUntimed => "route_success_untimed".to_string(),
                RouteOutcome::Failure => "route_failure".to_string(),
            }
        }
        EventKind::Ignored => "ignored".to_string(),
        EventKind::Timeout { .. } => "timeout".to_string(),
        EventKind::Lifecycle(lifecycle_event) => {
            use super::PeerLifecycleEvent;
            match lifecycle_event {
                PeerLifecycleEvent::Startup { .. } => "peer_startup".to_string(),
                PeerLifecycleEvent::Shutdown { .. } => "peer_shutdown".to_string(),
            }
        }
        EventKind::TransportSnapshot(_) => "transport_snapshot".to_string(),
        EventKind::InterestSync(interest_sync_event) => {
            use crate::tracing::InterestSyncEvent;
            match interest_sync_event {
                InterestSyncEvent::ResyncRequestReceived { .. } => {
                    "interest_resync_request_received".to_string()
                }
                InterestSyncEvent::ResyncResponseSent { .. } => {
                    "interest_resync_response_sent".to_string()
                }
                InterestSyncEvent::StateConfirmed { .. } => "interest_state_confirmed".to_string(),
            }
        }
        EventKind::RoutingDecision(_) => "routing_decision".to_string(),
        EventKind::RouterSnapshot(_) => "router_snapshot".to_string(),
    }
}

fn event_kind_to_json(kind: &EventKind) -> serde_json::Value {
    match kind {
        EventKind::Connect(connect_event) => {
            use super::ConnectEvent;
            match connect_event {
                ConnectEvent::StartConnection { from, is_gateway } => {
                    serde_json::json!({
                        "type": "start_connection",
                        "from": from.to_string(),
                        "is_gateway": is_gateway,
                    })
                }
                ConnectEvent::Connected {
                    this,
                    connected,
                    elapsed_ms,
                    connection_type,
                    latency_ms,
                    this_peer_connection_count,
                    initiated_by,
                } => {
                    serde_json::json!({
                        "type": "connected",
                        "this_peer": this.to_string(),
                        "this_peer_id": this.pub_key().to_string(),
                        "this_peer_addr": this.peer_addr.to_string(),
                        "connected_peer": connected.to_string(),
                        "connected_peer_id": connected.pub_key().to_string(),
                        "connected_peer_addr": connected.peer_addr.to_string(),
                        "elapsed_ms": elapsed_ms,
                        "connection_type": connection_type.to_string(),
                        "latency_ms": latency_ms,
                        "connection_count": this_peer_connection_count,
                        "initiated_by": initiated_by.as_ref().map(|p| p.to_string()),
                    })
                }
                ConnectEvent::Finished {
                    initiator,
                    location,
                    elapsed_ms,
                } => {
                    serde_json::json!({
                        "type": "finished",
                        "initiator": initiator.to_string(),
                        "location": location.as_f64(),
                        "elapsed_ms": elapsed_ms,
                    })
                }
                ConnectEvent::RequestSent {
                    desired_location,
                    joiner,
                    target,
                    ttl,
                    is_initial,
                } => {
                    serde_json::json!({
                        "type": "request_sent",
                        "desired_location": desired_location.as_f64(),
                        "joiner": joiner.to_string(),
                        "target": target.to_string(),
                        "ttl": ttl,
                        "is_initial": is_initial,
                    })
                }
                ConnectEvent::RequestReceived {
                    desired_location,
                    joiner,
                    from_addr,
                    from_peer,
                    forwarded_to,
                    accepted,
                    ttl,
                } => {
                    serde_json::json!({
                        "type": "request_received",
                        "desired_location": desired_location.as_f64(),
                        "joiner": joiner.to_string(),
                        "from_addr": from_addr.to_string(),
                        "from_peer": from_peer.as_ref().map(|p| p.to_string()),
                        "forwarded_to": forwarded_to.as_ref().map(|p| p.to_string()),
                        "accepted": accepted,
                        "ttl": ttl,
                    })
                }
                ConnectEvent::ResponseSent { acceptor, joiner } => {
                    serde_json::json!({
                        "type": "response_sent",
                        "acceptor": acceptor.to_string(),
                        "joiner": joiner.to_string(),
                    })
                }
                ConnectEvent::ResponseReceived {
                    acceptor,
                    elapsed_ms,
                } => {
                    serde_json::json!({
                        "type": "response_received",
                        "acceptor": acceptor.to_string(),
                        "elapsed_ms": elapsed_ms,
                    })
                }
                ConnectEvent::Rejected {
                    desired_location,
                    reason,
                } => {
                    serde_json::json!({
                        "type": "rejected",
                        "desired_location": desired_location.as_f64(),
                        "reason": reason,
                    })
                }
            }
        }
        EventKind::Disconnected {
            from,
            reason,
            connection_duration_ms,
            bytes_sent,
            bytes_received,
        } => {
            serde_json::json!({
                "type": "disconnected",
                "from": from.to_string(),
                "from_peer_id": from.pub_key().to_string(),
                "from_peer_addr": from.socket_addr().to_string(),
                "reason": reason.to_string(),
                "connection_duration_ms": connection_duration_ms,
                "bytes_sent": bytes_sent,
                "bytes_received": bytes_received,
            })
        }
        EventKind::Put(put_event) => {
            use super::PutEvent;
            match put_event {
                PutEvent::Request {
                    requester,
                    target,
                    key,
                    id,
                    htl,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "request",
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "id": id.to_string(),
                        "htl": htl,
                        "timestamp": timestamp,
                    })
                }
                PutEvent::PutSuccess {
                    requester,
                    target,
                    key,
                    id,
                    hop_count,
                    elapsed_ms,
                    timestamp,
                    state_hash,
                    state_size,
                } => {
                    let mut json = serde_json::json!({
                        "type": "success",
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "id": id.to_string(),
                        "hop_count": hop_count,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    });
                    if let Some(hash) = state_hash {
                        json["state_hash"] = serde_json::Value::String(hash.clone());
                    }
                    if let Some(size) = state_size {
                        json["state_size"] = serde_json::json!(size);
                    }
                    json
                }
                PutEvent::BroadcastEmitted {
                    upstream,
                    broadcast_to,
                    broadcasted_to,
                    key,
                    sender,
                    id,
                    timestamp,
                    ..
                } => {
                    serde_json::json!({
                        "type": "broadcast_emitted",
                        "upstream": upstream.to_string(),
                        "broadcast_to": broadcast_to.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                        "broadcasted_to": broadcasted_to,
                        "key": key.to_string(),
                        "sender": sender.to_string(),
                        "id": id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                PutEvent::BroadcastReceived {
                    requester,
                    target,
                    key,
                    id,
                    timestamp,
                    ..
                } => {
                    serde_json::json!({
                        "type": "broadcast_received",
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "id": id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                PutEvent::PutFailure {
                    id,
                    requester,
                    target,
                    key,
                    hop_count,
                    reason,
                    elapsed_ms,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "failure",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "hop_count": hop_count,
                        "reason": reason.to_string(),
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    })
                }
                PutEvent::ResponseSent {
                    id,
                    from,
                    to,
                    key,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "response_sent",
                        "id": id.to_string(),
                        "from": from.to_string(),
                        "to": to.to_string(),
                        "key": key.to_string(),
                        "timestamp": timestamp,
                    })
                }
            }
        }
        EventKind::Get(get_event) => {
            use super::GetEvent;
            match get_event {
                GetEvent::Request {
                    id,
                    requester,
                    instance_id,
                    target,
                    htl,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "get_request",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "instance_id": instance_id.to_string(),
                        "target": target.to_string(),
                        "htl": htl,
                        "timestamp": timestamp,
                    })
                }
                GetEvent::GetSuccess {
                    id,
                    requester,
                    target,
                    key,
                    hop_count,
                    elapsed_ms,
                    timestamp,
                    state_hash,
                } => {
                    let mut json = serde_json::json!({
                        "type": "get_success",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "hop_count": hop_count,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    });
                    if let Some(hash) = state_hash {
                        json["state_hash"] = serde_json::Value::String(hash.clone());
                    }
                    json
                }
                GetEvent::GetNotFound {
                    id,
                    requester,
                    instance_id,
                    target,
                    hop_count,
                    elapsed_ms,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "get_not_found",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "instance_id": instance_id.to_string(),
                        "target": target.to_string(),
                        "hop_count": hop_count,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    })
                }
                GetEvent::GetFailure {
                    id,
                    requester,
                    instance_id,
                    target,
                    hop_count,
                    reason,
                    elapsed_ms,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "get_failure",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "instance_id": instance_id.to_string(),
                        "target": target.to_string(),
                        "hop_count": hop_count,
                        "reason": reason.to_string(),
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    })
                }
                GetEvent::ResponseSent {
                    id,
                    from,
                    to,
                    key,
                    timestamp,
                } => {
                    let mut json = serde_json::json!({
                        "type": "get_response_sent",
                        "id": id.to_string(),
                        "from": from.to_string(),
                        "to": to.to_string(),
                        "timestamp": timestamp,
                    });
                    if let Some(k) = key {
                        json["key"] = serde_json::Value::String(k.to_string());
                    }
                    json
                }
                GetEvent::ForwardingAckSent {
                    id,
                    from,
                    to,
                    instance_id,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "get_forwarding_ack_sent",
                        "id": id.to_string(),
                        "from": from.to_string(),
                        "to": to.to_string(),
                        "instance_id": instance_id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                GetEvent::ForwardingAckReceived {
                    id,
                    receiver,
                    instance_id,
                    elapsed_ms,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "get_forwarding_ack_received",
                        "id": id.to_string(),
                        "receiver": receiver.to_string(),
                        "instance_id": instance_id.to_string(),
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    })
                }
                GetEvent::ClientTerminal {
                    id,
                    requester,
                    instance_id,
                    key,
                    outcome,
                    streamed,
                    is_sub_op,
                    attempts,
                    hop_count,
                    fragments_received,
                    total_fragments,
                    stream_abort_cause,
                    exhaustion_reason,
                    elapsed_ms,
                    timestamp,
                } => {
                    let mut json = serde_json::json!({
                        "type": "get_terminal",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "instance_id": instance_id.to_string(),
                        "outcome": outcome.as_str(),
                        "streamed": streamed,
                        "is_sub_op": is_sub_op,
                        "attempts": attempts,
                        "hop_count": hop_count,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    });
                    if let Some(k) = key {
                        json["key"] = serde_json::Value::String(k.to_string());
                    }
                    // Streamed-transfer failure-isolation fields (Group B): only
                    // present on the streaming path, so emit them only when set
                    // to keep the common non-streaming terminal lean.
                    if let Some(fr) = fragments_received {
                        json["fragments_received"] = serde_json::json!(fr);
                    }
                    if let Some(tf) = total_fragments {
                        json["total_fragments"] = serde_json::json!(tf);
                    }
                    if let Some(cause) = stream_abort_cause {
                        json["stream_abort_cause"] =
                            serde_json::Value::String(cause.as_str().to_string());
                    }
                    // Candidate-exhaustion discriminator (#5252): only present
                    // on the RetryLoopOutcome::Exhausted path.
                    if let Some(reason) = exhaustion_reason {
                        json["exhaustion_reason"] =
                            serde_json::Value::String(reason.as_str().to_string());
                    }
                    json
                }
            }
        }
        EventKind::Subscribe(subscribe_event) => {
            use super::SubscribeEvent;
            match subscribe_event {
                SubscribeEvent::Request {
                    id,
                    requester,
                    instance_id,
                    target,
                    htl,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "subscribe_request",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "instance_id": instance_id.to_string(),
                        "target": target.to_string(),
                        "htl": htl,
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::SubscribeSuccess {
                    id,
                    key,
                    at,
                    hop_count,
                    elapsed_ms,
                    timestamp,
                    requester,
                } => {
                    serde_json::json!({
                        "type": "subscribe_success",
                        "id": id.to_string(),
                        "key": key.to_string(),
                        "at": at.to_string(),
                        "hop_count": hop_count,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                        "requester": requester.to_string(),
                    })
                }
                SubscribeEvent::SubscribeNotFound {
                    id,
                    requester,
                    instance_id,
                    target,
                    hop_count,
                    elapsed_ms,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "subscribe_not_found",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "instance_id": instance_id.to_string(),
                        "target": target.to_string(),
                        "hop_count": hop_count,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::SubscribeTimeout {
                    id,
                    requester,
                    instance_id,
                    retries,
                    elapsed_ms,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "subscribe_timeout",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "instance_id": instance_id.to_string(),
                        "retries": retries,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::ResponseSent {
                    id,
                    from,
                    to,
                    key,
                    timestamp,
                } => {
                    let mut json = serde_json::json!({
                        "type": "subscribe_response_sent",
                        "id": id.to_string(),
                        "from": from.to_string(),
                        "to": to.to_string(),
                        "timestamp": timestamp,
                    });
                    if let Some(k) = key {
                        json["key"] = serde_json::Value::String(k.to_string());
                    }
                    json
                }
                SubscribeEvent::HostingStarted {
                    instance_id,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "hosting_started",
                        "instance_id": instance_id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::HostingStopped {
                    instance_id,
                    reason,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "hosting_stopped",
                        "instance_id": instance_id.to_string(),
                        "reason": format!("{:?}", reason),
                        "timestamp": timestamp,
                    })
                }
                // Reserved discriminants for removed variants
                SubscribeEvent::_Reserved6
                | SubscribeEvent::_Reserved7
                | SubscribeEvent::_Reserved8
                | SubscribeEvent::_Reserved9
                | SubscribeEvent::_Reserved10 => serde_json::json!({"type": "reserved"}),
                SubscribeEvent::UnsubscribeSent {
                    id,
                    instance_id,
                    from,
                    to,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "unsubscribe_sent",
                        "id": id.to_string(),
                        "instance_id": instance_id.to_string(),
                        "from": from.to_string(),
                        "to": to.to_string(),
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::UnsubscribeReceived {
                    id,
                    instance_id,
                    from,
                    at,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "unsubscribe_received",
                        "id": id.to_string(),
                        "instance_id": instance_id.to_string(),
                        "from": from.to_string(),
                        "at": at.to_string(),
                        "timestamp": timestamp,
                    })
                }
            }
        }
        EventKind::Update(update_event) => {
            use super::UpdateEvent;
            match update_event {
                UpdateEvent::Request {
                    requester,
                    target,
                    key,
                    id,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "request",
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "id": id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                UpdateEvent::UpdateSuccess {
                    requester,
                    target,
                    key,
                    id,
                    timestamp,
                    state_hash_before,
                    state_hash_after,
                    state_size,
                } => {
                    let mut json = serde_json::json!({
                        "type": "success",
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "id": id.to_string(),
                        "timestamp": timestamp,
                    });
                    if let Some(hash) = state_hash_before {
                        json["state_hash_before"] = serde_json::Value::String(hash.clone());
                    }
                    if let Some(hash) = state_hash_after {
                        json["state_hash_after"] = serde_json::Value::String(hash.clone());
                    }
                    if let Some(size) = state_size {
                        json["state_size"] = serde_json::json!(size);
                    }
                    json
                }
                UpdateEvent::BroadcastEmitted {
                    upstream,
                    broadcast_to,
                    broadcasted_to,
                    key,
                    sender,
                    id,
                    timestamp,
                    ..
                } => {
                    serde_json::json!({
                        "type": "broadcast_emitted",
                        "upstream": upstream.to_string(),
                        "broadcast_to": broadcast_to.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                        "broadcasted_to": broadcasted_to,
                        "key": key.to_string(),
                        "sender": sender.to_string(),
                        "id": id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                UpdateEvent::BroadcastComplete {
                    id,
                    key,
                    delta_sends,
                    full_state_sends,
                    bytes_saved,
                    state_size,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "broadcast_complete",
                        "id": id.to_string(),
                        "key": key.to_string(),
                        "delta_sends": delta_sends,
                        "full_state_sends": full_state_sends,
                        "bytes_saved": bytes_saved,
                        "state_size": state_size,
                        "delta_ratio": if *delta_sends + *full_state_sends > 0 {
                            *delta_sends as f64 / (*delta_sends + *full_state_sends) as f64
                        } else {
                            0.0
                        },
                        "timestamp": timestamp,
                    })
                }
                UpdateEvent::BroadcastReceived {
                    requester,
                    target,
                    key,
                    id,
                    timestamp,
                    ..
                } => {
                    serde_json::json!({
                        "type": "broadcast_received",
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "id": id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                UpdateEvent::BroadcastApplied {
                    id,
                    key,
                    target,
                    timestamp,
                    state_hash_before,
                    state_hash_after,
                    changed,
                    state_size,
                } => {
                    let mut json = serde_json::json!({
                        "type": "broadcast_applied",
                        "id": id.to_string(),
                        "key": key.to_string(),
                        "target": target.to_string(),
                        "timestamp": timestamp,
                        "changed": changed,
                        "state_size": state_size,
                    });
                    if let Some(hash) = state_hash_before {
                        json["state_hash_before"] = serde_json::Value::String(hash.clone());
                    }
                    if let Some(hash) = state_hash_after {
                        json["state_hash_after"] = serde_json::Value::String(hash.clone());
                    }
                    json
                }
                UpdateEvent::BroadcastDeliverySummary {
                    key,
                    proximity_found,
                    proximity_resolve_failed,
                    interest_found,
                    interest_resolve_failed,
                    skipped_self,
                    skipped_sender,
                    skipped_summary_match,
                    targets_sent,
                    send_failed,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "broadcast_delivery_summary",
                        "key": key.to_string(),
                        "proximity_found": proximity_found,
                        "proximity_resolve_failed": proximity_resolve_failed,
                        "interest_found": interest_found,
                        "interest_resolve_failed": interest_resolve_failed,
                        "skipped_self": skipped_self,
                        "skipped_sender": skipped_sender,
                        "skipped_summary_match": skipped_summary_match,
                        "targets_sent": targets_sent,
                        "send_failed": send_failed,
                        "timestamp": timestamp,
                    })
                }
                UpdateEvent::UpdateFailure {
                    id,
                    requester,
                    target,
                    key,
                    reason,
                    elapsed_ms,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "update_failure",
                        "id": id.to_string(),
                        "requester": requester.to_string(),
                        "target": target.to_string(),
                        "key": key.to_string(),
                        "reason": reason.to_string(),
                        "elapsed_ms": elapsed_ms,
                        "timestamp": timestamp,
                    })
                }
            }
        }
        EventKind::Transfer(transfer_event) => {
            use super::TransferEvent;
            match transfer_event {
                TransferEvent::Started {
                    stream_id,
                    peer_addr,
                    expected_bytes,
                    direction,
                    tx_id,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "started",
                        "stream_id": stream_id,
                        "peer_addr": peer_addr.to_string(),
                        "expected_bytes": expected_bytes,
                        "direction": format!("{:?}", direction),
                        "tx_id": tx_id.as_ref().map(|t| t.to_string()),
                        "timestamp": timestamp,
                    })
                }
                TransferEvent::Completed {
                    stream_id,
                    peer_addr,
                    bytes_transferred,
                    elapsed_ms,
                    avg_throughput_bps,
                    peak_cwnd_bytes,
                    final_cwnd_bytes,
                    slowdowns_triggered,
                    final_srtt_ms,
                    final_ssthresh_bytes,
                    min_ssthresh_floor_bytes,
                    total_timeouts,
                    min_rtt_ms,
                    peak_flightsize_bytes,
                    direction,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "completed",
                        "stream_id": stream_id,
                        "peer_addr": peer_addr.to_string(),
                        "bytes_transferred": bytes_transferred,
                        "elapsed_ms": elapsed_ms,
                        "avg_throughput_bps": avg_throughput_bps,
                        "peak_cwnd_bytes": peak_cwnd_bytes,
                        "final_cwnd_bytes": final_cwnd_bytes,
                        "slowdowns_triggered": slowdowns_triggered,
                        "final_srtt_ms": final_srtt_ms,
                        "final_ssthresh_bytes": final_ssthresh_bytes,
                        "min_ssthresh_floor_bytes": min_ssthresh_floor_bytes,
                        "total_timeouts": total_timeouts,
                        "min_rtt_ms": min_rtt_ms,
                        "peak_flightsize_bytes": peak_flightsize_bytes,
                        "direction": format!("{:?}", direction),
                        "timestamp": timestamp,
                    })
                }
                TransferEvent::Failed {
                    stream_id,
                    peer_addr,
                    bytes_transferred,
                    reason,
                    elapsed_ms,
                    direction,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "failed",
                        "stream_id": stream_id,
                        "peer_addr": peer_addr.to_string(),
                        "bytes_transferred": bytes_transferred,
                        "reason": reason,
                        "elapsed_ms": elapsed_ms,
                        "direction": format!("{:?}", direction),
                        "timestamp": timestamp,
                    })
                }
            }
        }
        EventKind::Route(route_event) => {
            use crate::router::RouteOutcome;
            let distance = route_event
                .peer
                .location()
                .map(|l| route_event.contract_location.distance(l).as_f64());
            match &route_event.outcome {
                RouteOutcome::Success {
                    time_to_response_start,
                    payload_size,
                    payload_transfer_time,
                } => {
                    serde_json::json!({
                        "type": "route_success",
                        "peer_location": route_event.peer.location().map(|l| l.as_f64()),
                        "contract_location": route_event.contract_location.as_f64(),
                        "distance": distance,
                        "time_to_response_start_ms": time_to_response_start.as_millis() as u64,
                        "payload_size": payload_size,
                        "payload_transfer_time_ms": payload_transfer_time.as_millis() as u64,
                    })
                }
                RouteOutcome::SuccessUntimed | RouteOutcome::Failure => {
                    let type_str = match &route_event.outcome {
                        RouteOutcome::SuccessUntimed => "route_success_untimed",
                        RouteOutcome::Success { .. } | RouteOutcome::Failure => "route_failure",
                    };
                    serde_json::json!({
                        "type": type_str,
                        "peer_location": route_event.peer.location().map(|l| l.as_f64()),
                        "contract_location": route_event.contract_location.as_f64(),
                        "distance": distance,
                    })
                }
            }
        }
        EventKind::Ignored => {
            serde_json::json!({"type": "ignored"})
        }
        EventKind::Timeout {
            id,
            timestamp,
            op_type,
            target_peer,
        } => {
            serde_json::json!({
                "type": "timeout",
                "id": id.to_string(),
                "timestamp": timestamp,
                "op_type": op_type,
                "target_peer": target_peer,
            })
        }
        EventKind::Lifecycle(lifecycle_event) => {
            use super::PeerLifecycleEvent;
            match lifecycle_event {
                PeerLifecycleEvent::Startup {
                    version,
                    git_commit,
                    git_dirty,
                    arch,
                    os,
                    os_version,
                    is_gateway,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "startup",
                        "version": version,
                        "git_commit": git_commit,
                        "git_dirty": git_dirty,
                        "arch": arch,
                        "os": os,
                        "os_version": os_version,
                        "is_gateway": is_gateway,
                        "timestamp": timestamp,
                    })
                }
                PeerLifecycleEvent::Shutdown {
                    graceful,
                    reason,
                    uptime_secs,
                    total_connections,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "shutdown",
                        "graceful": graceful,
                        "reason": reason,
                        "uptime_secs": uptime_secs,
                        "total_connections": total_connections,
                        "timestamp": timestamp,
                    })
                }
            }
        }
        EventKind::TransportSnapshot(snapshot) => {
            // Explicit field list, so a new snapshot field is exported here
            // only by a deliberate edit. `transport_snapshot_json_lists_every_field`
            // fails if this list and the struct drift apart. (Built as a map:
            // `json!` exceeds the macro recursion limit at this many keys.)
            let mut m = serde_json::Map::new();
            m.insert("type".into(), serde_json::json!("transport_snapshot"));
            m.insert(
                "transfers_completed".into(),
                serde_json::json!(snapshot.transfers_completed),
            );
            m.insert(
                "transfers_failed".into(),
                serde_json::json!(snapshot.transfers_failed),
            );
            m.insert("bytes_sent".into(), serde_json::json!(snapshot.bytes_sent));
            m.insert(
                "bytes_received".into(),
                serde_json::json!(snapshot.bytes_received),
            );
            m.insert(
                "avg_transfer_time_ms".into(),
                serde_json::json!(snapshot.avg_transfer_time_ms),
            );
            m.insert(
                "peak_throughput_bps".into(),
                serde_json::json!(snapshot.peak_throughput_bps),
            );
            m.insert(
                "avg_cwnd_bytes".into(),
                serde_json::json!(snapshot.avg_cwnd_bytes),
            );
            m.insert(
                "peak_cwnd_bytes".into(),
                serde_json::json!(snapshot.peak_cwnd_bytes),
            );
            m.insert(
                "min_cwnd_bytes".into(),
                serde_json::json!(snapshot.min_cwnd_bytes),
            );
            m.insert(
                "slowdowns_triggered".into(),
                serde_json::json!(snapshot.slowdowns_triggered),
            );
            m.insert(
                "nat_traversal_attempts".into(),
                serde_json::json!(snapshot.nat_traversal_attempts),
            );
            m.insert(
                "nat_traversal_established".into(),
                serde_json::json!(snapshot.nat_traversal_established),
            );
            m.insert(
                "nat_traversal_failed_error".into(),
                serde_json::json!(snapshot.nat_traversal_failed_error),
            );
            m.insert(
                "nat_traversal_failed_version".into(),
                serde_json::json!(snapshot.nat_traversal_failed_version),
            );
            m.insert("avg_rtt_us".into(), serde_json::json!(snapshot.avg_rtt_us));
            m.insert("min_rtt_us".into(), serde_json::json!(snapshot.min_rtt_us));
            m.insert("max_rtt_us".into(), serde_json::json!(snapshot.max_rtt_us));
            m.insert(
                "conn_rtt_connections".into(),
                serde_json::json!(snapshot.conn_rtt_connections),
            );
            m.insert(
                "conn_srtt_p50_us".into(),
                serde_json::json!(snapshot.conn_srtt_p50_us),
            );
            m.insert(
                "conn_srtt_p90_us".into(),
                serde_json::json!(snapshot.conn_srtt_p90_us),
            );
            m.insert(
                "conn_srtt_max_us".into(),
                serde_json::json!(snapshot.conn_srtt_max_us),
            );
            m.insert(
                "conn_min_rtt_p50_us".into(),
                serde_json::json!(snapshot.conn_min_rtt_p50_us),
            );
            m.insert(
                "conn_min_rtt_p90_us".into(),
                serde_json::json!(snapshot.conn_min_rtt_p90_us),
            );
            m.insert(
                "conn_min_rtt_max_us".into(),
                serde_json::json!(snapshot.conn_min_rtt_max_us),
            );
            m.insert(
                "packets_sent".into(),
                serde_json::json!(snapshot.packets_sent),
            );
            m.insert(
                "packets_retransmitted".into(),
                serde_json::json!(snapshot.packets_retransmitted),
            );
            m.insert(
                "packets_abandoned".into(),
                serde_json::json!(snapshot.packets_abandoned),
            );
            m.insert(
                "rto_expiries".into(),
                serde_json::json!(snapshot.rto_expiries),
            );
            m.insert("tlp_probes".into(), serde_json::json!(snapshot.tlp_probes));
            m.insert(
                "standalone_ack_packets".into(),
                serde_json::json!(snapshot.standalone_ack_packets),
            );
            m.insert(
                "standalone_ack_bytes".into(),
                serde_json::json!(snapshot.standalone_ack_bytes),
            );
            m.insert(
                "nat_fail_no_response".into(),
                serde_json::json!(snapshot.nat_fail_no_response),
            );
            m.insert(
                "nat_fail_undecryptable".into(),
                serde_json::json!(snapshot.nat_fail_undecryptable),
            );
            m.insert(
                "nat_fail_version_incompatible".into(),
                serde_json::json!(snapshot.nat_fail_version_incompatible),
            );
            m.insert(
                "nat_fail_incomplete".into(),
                serde_json::json!(snapshot.nat_fail_incomplete),
            );
            m.insert(
                "nat_fail_send_error".into(),
                serde_json::json!(snapshot.nat_fail_send_error),
            );
            m.insert(
                "nat_fail_other".into(),
                serde_json::json!(snapshot.nat_fail_other),
            );
            m.insert(
                "observed_addr_reports".into(),
                serde_json::json!(snapshot.observed_addr_reports),
            );
            m.insert(
                "observed_addr_port_changes".into(),
                serde_json::json!(snapshot.observed_addr_port_changes),
            );
            m.insert(
                "observed_addr_ip_changes".into(),
                serde_json::json!(snapshot.observed_addr_ip_changes),
            );
            m.insert(
                "transfers_acked".into(),
                serde_json::json!(snapshot.transfers_acked),
            );
            m.insert(
                "avg_transfer_ack_time_ms".into(),
                serde_json::json!(snapshot.avg_transfer_ack_time_ms),
            );
            m.insert(
                "max_transfer_ack_time_ms".into(),
                serde_json::json!(snapshot.max_transfer_ack_time_ms),
            );
            m.insert(
                "inbound_queue_depth_hwm".into(),
                serde_json::json!(snapshot.inbound_queue_depth_hwm),
            );
            m.insert(
                "inbound_queue_drops".into(),
                serde_json::json!(snapshot.inbound_queue_drops),
            );
            m.insert(
                "upgrade_control_received".into(),
                serde_json::json!(snapshot.upgrade_control_received),
            );
            m.insert(
                "handshake_unauthenticated_dropped".into(),
                serde_json::json!(snapshot.handshake_unauthenticated_dropped),
            );
            m.insert(
                "quic_endpoint_down".into(),
                serde_json::json!(snapshot.quic_endpoint_down),
            );
            m.insert(
                "quic_adapter_panicked".into(),
                serde_json::json!(snapshot.quic_adapter_panicked),
            );
            m.insert(
                "quic_breaker_trips".into(),
                serde_json::json!(snapshot.quic_breaker_trips),
            );
            m.insert(
                "quic_breaker_suppressed".into(),
                serde_json::json!(snapshot.quic_breaker_suppressed),
            );
            m.insert(
                "quic_close_sent_node_dropped_peer".into(),
                serde_json::json!(snapshot.quic_close_sent_node_dropped_peer),
            );
            m.insert(
                "quic_close_sent_downgrade".into(),
                serde_json::json!(snapshot.quic_close_sent_downgrade),
            );
            m.insert(
                "quic_close_sent_identity_mismatch".into(),
                serde_json::json!(snapshot.quic_close_sent_identity_mismatch),
            );
            m.insert(
                "quic_close_sent_send_abort_limit".into(),
                serde_json::json!(snapshot.quic_close_sent_send_abort_limit),
            );
            m.insert(
                "quic_close_sent_peer_not_acknowledging".into(),
                serde_json::json!(snapshot.quic_close_sent_peer_not_acknowledging),
            );
            m.insert(
                "nat_fail_samples".into(),
                serde_json::json!(snapshot.nat_fail_samples),
            );
            m.insert(
                "nat_fail_samples_dropped".into(),
                serde_json::json!(snapshot.nat_fail_samples_dropped),
            );
            m.insert(
                "inbound_stream_refused_invalid_length".into(),
                serde_json::json!(snapshot.inbound_stream_refused_invalid_length),
            );
            m.insert(
                "inbound_stream_refused_connection_bytes".into(),
                serde_json::json!(snapshot.inbound_stream_refused_connection_bytes),
            );
            m.insert(
                "inbound_stream_refused_connection_streams".into(),
                serde_json::json!(snapshot.inbound_stream_refused_connection_streams),
            );
            m.insert(
                "inbound_stream_closed_too_many_refusals".into(),
                serde_json::json!(snapshot.inbound_stream_closed_too_many_refusals),
            );
            m.insert(
                "inbound_stream_evicted_idle".into(),
                serde_json::json!(snapshot.inbound_stream_evicted_idle),
            );
            m.insert(
                "inbound_stream_evicted_age".into(),
                serde_json::json!(snapshot.inbound_stream_evicted_age),
            );
            m.insert(
                "nat_attempts_connect_joiner".into(),
                serde_json::json!(snapshot.nat_attempts_connect_joiner),
            );
            m.insert(
                "nat_attempts_connect_acceptor".into(),
                serde_json::json!(snapshot.nat_attempts_connect_acceptor),
            );
            m.insert(
                "nat_attempts_gateway".into(),
                serde_json::json!(snapshot.nat_attempts_gateway),
            );
            m.insert(
                "nat_attempts_ring_peer_no_transport".into(),
                serde_json::json!(snapshot.nat_attempts_ring_peer_no_transport),
            );
            m.insert(
                "nat_established_connect_joiner".into(),
                serde_json::json!(snapshot.nat_established_connect_joiner),
            );
            m.insert(
                "nat_established_connect_acceptor".into(),
                serde_json::json!(snapshot.nat_established_connect_acceptor),
            );
            m.insert(
                "nat_established_gateway".into(),
                serde_json::json!(snapshot.nat_established_gateway),
            );
            m.insert(
                "nat_established_ring_peer_no_transport".into(),
                serde_json::json!(snapshot.nat_established_ring_peer_no_transport),
            );
            m.insert(
                "stray_intro_after_deadline".into(),
                serde_json::json!(snapshot.stray_intro_after_deadline),
            );
            m.insert(
                "stray_intro_same_ip_other_port".into(),
                serde_json::json!(snapshot.stray_intro_same_ip_other_port),
            );
            m.insert(
                "stray_intro_other".into(),
                serde_json::json!(snapshot.stray_intro_other),
            );
            m.insert(
                "stray_intro_failures_evicted".into(),
                serde_json::json!(snapshot.stray_intro_failures_evicted),
            );
            m.insert(
                "dial_queue_wait_lt_100ms".into(),
                serde_json::json!(snapshot.dial_queue_wait_lt_100ms),
            );
            m.insert(
                "dial_queue_wait_lt_1s".into(),
                serde_json::json!(snapshot.dial_queue_wait_lt_1s),
            );
            m.insert(
                "dial_queue_wait_lt_3s".into(),
                serde_json::json!(snapshot.dial_queue_wait_lt_3s),
            );
            m.insert(
                "dial_queue_wait_ge_3s".into(),
                serde_json::json!(snapshot.dial_queue_wait_ge_3s),
            );
            m.insert(
                "demux_legacy".into(),
                serde_json::json!(snapshot.demux_legacy),
            );
            m.insert("demux_quic".into(), serde_json::json!(snapshot.demux_quic));
            m.insert(
                "demux_other".into(),
                serde_json::json!(snapshot.demux_other),
            );
            m.insert(
                "quic_ingress_forwarded".into(),
                serde_json::json!(snapshot.quic_ingress_forwarded),
            );
            m.insert(
                "quic_ingress_not_allowed".into(),
                serde_json::json!(snapshot.quic_ingress_not_allowed),
            );
            m.insert(
                "quic_ingress_queue_full".into(),
                serde_json::json!(snapshot.quic_ingress_queue_full),
            );
            m.insert(
                "quic_ingress_source_full".into(),
                serde_json::json!(snapshot.quic_ingress_source_full),
            );
            m.insert(
                "quic_ingress_no_ingress".into(),
                serde_json::json!(snapshot.quic_ingress_no_ingress),
            );
            m.insert(
                "egress_immediate".into(),
                serde_json::json!(snapshot.egress_immediate),
            );
            m.insert(
                "egress_waited".into(),
                serde_json::json!(snapshot.egress_waited),
            );
            m.insert(
                "egress_unpaced".into(),
                serde_json::json!(snapshot.egress_unpaced),
            );
            m.insert(
                "egress_bytes".into(),
                serde_json::json!(snapshot.egress_bytes),
            );
            m.insert(
                "egress_bytes_waited".into(),
                serde_json::json!(snapshot.egress_bytes_waited),
            );
            m.insert(
                "egress_wait_us_sum".into(),
                serde_json::json!(snapshot.egress_wait_us_sum),
            );
            m.insert(
                "egress_wait_us_max".into(),
                serde_json::json!(snapshot.egress_wait_us_max),
            );
            m.insert(
                "egress_long_waits_cap".into(),
                serde_json::json!(snapshot.egress_long_waits_cap),
            );
            m.insert(
                "egress_long_waits_connection".into(),
                serde_json::json!(snapshot.egress_long_waits_connection),
            );
            m.insert(
                "egress_long_waits_other".into(),
                serde_json::json!(snapshot.egress_long_waits_other),
            );
            m.insert(
                "egress_streams_abandoned_cap".into(),
                serde_json::json!(snapshot.egress_streams_abandoned_cap),
            );
            m.insert(
                "egress_streams_abandoned_connection".into(),
                serde_json::json!(snapshot.egress_streams_abandoned_connection),
            );
            m.insert(
                "egress_streams_abandoned_other".into(),
                serde_json::json!(snapshot.egress_streams_abandoned_other),
            );
            m.insert(
                "egress_streams_refused_cap".into(),
                serde_json::json!(snapshot.egress_streams_refused_cap),
            );
            m.insert(
                "egress_streams_refused_connection".into(),
                serde_json::json!(snapshot.egress_streams_refused_connection),
            );
            m.insert(
                "egress_streams_refused_other".into(),
                serde_json::json!(snapshot.egress_streams_refused_other),
            );
            m.insert(
                "egress_debt_hwm_bytes".into(),
                serde_json::json!(snapshot.egress_debt_hwm_bytes),
            );
            m.insert(
                "egress_debt_forgiven_bytes".into(),
                serde_json::json!(snapshot.egress_debt_forgiven_bytes),
            );
            for (key, count) in snapshot.nonzero_conn_identity() {
                m.insert(key, serde_json::json!(count));
            }
            for (key, value) in snapshot.nonzero_inbound_concurrency() {
                m.insert(key.into(), serde_json::json!(value));
            }
            serde_json::Value::Object(m)
        }
        EventKind::InterestSync(interest_sync_event) => {
            use crate::tracing::InterestSyncEvent;
            match interest_sync_event {
                InterestSyncEvent::ResyncRequestReceived {
                    key,
                    from_peer,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "resync_request_received",
                        "contract_key": key.to_string(),
                        "contract_id": key.id().to_string(),
                        "from_peer": from_peer.to_string(),
                        "from_peer_addr": from_peer.peer_addr.to_string(),
                        "timestamp": timestamp,
                    })
                }
                InterestSyncEvent::ResyncResponseSent {
                    key,
                    to_peer,
                    state_size,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "resync_response_sent",
                        "contract_key": key.to_string(),
                        "contract_id": key.id().to_string(),
                        "to_peer": to_peer.to_string(),
                        "to_peer_addr": to_peer.peer_addr.to_string(),
                        "state_size": state_size,
                        "timestamp": timestamp,
                    })
                }
                InterestSyncEvent::StateConfirmed { key, state_hash } => {
                    serde_json::json!({
                        "type": "state_confirmed",
                        "contract_key": key.to_string(),
                        "contract_id": key.id().to_string(),
                        "state_hash": state_hash,
                    })
                }
            }
        }
        EventKind::RoutingDecision(decision) => {
            serde_json::json!({
                "type": "routing_decision",
                "target_location": decision.target_location,
                "strategy": format!("{:?}", decision.strategy),
                "num_candidates": decision.candidates.len(),
                "total_routing_events": decision.total_routing_events,
                "candidates": decision.candidates.iter().map(|c| {
                    serde_json::json!({
                        "distance": c.distance,
                        "selected": c.selected,
                        "failure_probability": c.prediction.as_ref().map(|p| p.failure_probability),
                        "time_to_response_start": c.prediction.as_ref().map(|p| p.time_to_response_start),
                        "expected_total_time": c.prediction.as_ref().map(|p| p.expected_total_time),
                        "transfer_speed_bps": c.prediction.as_ref().map(|p| p.transfer_speed_bps),
                    })
                }).collect::<Vec<_>>(),
            })
        }
        EventKind::RouterSnapshot(snapshot) => {
            let mut body = serde_json::json!({
                "type": "router_snapshot",
                "failure_events": snapshot.failure_events,
                "success_events": snapshot.success_events,
                "transfer_rate_events": snapshot.transfer_rate_events,
                "prediction_active": snapshot.prediction_active,
                "mean_transfer_size_bytes": snapshot.mean_transfer_size_bytes,
                "consider_n_closest_peers": snapshot.consider_n_closest_peers,
                "peers_with_failure_adjustments": snapshot.peers_with_failure_adjustments,
                "peers_with_response_adjustments": snapshot.peers_with_response_adjustments,
                "failure_curve": snapshot.failure_curve,
                "failure_data_range": snapshot.failure_data_range,
                "response_time_curve": snapshot.response_time_curve,
                "response_time_data_range": snapshot.response_time_data_range,
                "transfer_rate_curve": snapshot.transfer_rate_curve,
                "transfer_rate_data_range": snapshot.transfer_rate_data_range,
                "connect_forward_curve": snapshot.connect_forward_curve,
                "connect_forward_data_range": snapshot.connect_forward_data_range,
                "connect_forward_events": snapshot.connect_forward_events,
                "connect_forward_peer_adjustments": snapshot.connect_forward_peer_adjustments,
                // Node-health gauges (#4440). This body is a hand-mirrored
                // `json!`, so new RouterSnapshotInfo fields must be added here or
                // they never reach the collector — pinned by
                // `router_snapshot_json_includes_fd_gauges`.
                "open_fds": snapshot.open_fds,
                // Chain-blame soak histogram (#5657); pinned by
                // `router_snapshot_json_includes_timeout_label_histogram`.
                "timeout_label_peers_1": snapshot.timeout_label_peers_1,
                "timeout_label_peers_2_3": snapshot.timeout_label_peers_2_3,
                "timeout_label_peers_4_7": snapshot.timeout_label_peers_4_7,
                "timeout_label_peers_8_plus": snapshot.timeout_label_peers_8_plus,
                "timeout_label_max_per_peer": snapshot.timeout_label_max_per_peer,
                "timeout_labels_untracked": snapshot.timeout_labels_untracked,
                "fd_soft_limit": snapshot.fd_soft_limit,
                "contract_module_cache_entries": snapshot.contract_module_cache_entries,
                "contract_module_cache_total_bytes": snapshot.contract_module_cache_total_bytes,
                "contract_module_cache_budget_bytes": snapshot.contract_module_cache_budget_bytes,
                "contract_module_cache_evictions_total": snapshot.contract_module_cache_evictions_total,
                "delegate_module_cache_entries": snapshot.delegate_module_cache_entries,
                "delegate_module_cache_total_bytes": snapshot.delegate_module_cache_total_bytes,
                "delegate_module_cache_budget_bytes": snapshot.delegate_module_cache_budget_bytes,
                "delegate_module_cache_evictions_total": snapshot.delegate_module_cache_evictions_total,
                "broadcast_stream_attempts_total": snapshot.broadcast_stream_attempts_total,
                "broadcast_stream_failures_total": snapshot.broadcast_stream_failures_total,
                "broadcast_stream_failures_last_snapshot": snapshot.broadcast_stream_failures_last_snapshot,
                "per_op_curves": snapshot.per_op_curves,
            });
            // Placement-quality + placement-migration gauges (#4404 follow-up).
            // Inserted AFTER the `json!` (not as inline keys) only to keep the
            // macro under its expansion recursion limit — this body is still
            // hand-mirrored, so a new `RouterSnapshotInfo` field is invisible to
            // the collector unless added here. Pinned by
            // `router_snapshot_json_includes_placement_gauges`.
            if let Some(obj) = body.as_object_mut() {
                // #5090: one compact, versioned, fixed-cardinality diagnostic
                // object. Kept nested so its marginal serialized size and
                // schema can be pinned independently of the legacy snapshot.
                obj.insert(
                    "network_efficiency_v1".to_string(),
                    serde_json::json!(snapshot.network_efficiency_v1),
                );
                // Contract-term activation counters (#4485, #5700). PLAN-v2's
                // live safety watch has to tell "the term is working" from
                // "the term never activated" on a deployed gateway, and the
                // estimable-refit count alone cannot: the effect is also
                // refused per query below the present-peer bar. So the two
                // APPLIED counts are mirrored beside it, with the
                // between-contract variance and the number of contracts it
                // rests on (it has no minimum group count). Same hand-mirroring
                // footgun as everything else in this block: a new
                // `RouterSnapshotInfo` field is invisible to the collector
                // unless added here. Pinned by
                // `router_snapshot_json_includes_contract_term_activation`.
                for (name, value) in [
                    (
                        "hierarchical_contract_effects_applied",
                        snapshot.hierarchical_contract_effects_applied,
                    ),
                    (
                        "hierarchical_contract_forecast_offsets",
                        snapshot.hierarchical_contract_forecast_offsets,
                    ),
                    (
                        "hierarchical_contract_estimable_refits",
                        snapshot.hierarchical_contract_estimable_refits,
                    ),
                    (
                        "hierarchical_contract_qualifying_contracts",
                        snapshot.hierarchical_contract_qualifying_contracts,
                    ),
                    (
                        "hierarchical_contract_floor_bound_refits",
                        snapshot.hierarchical_contract_floor_bound_refits,
                    ),
                    (
                        "hierarchical_contracts",
                        snapshot.hierarchical_contracts as u64,
                    ),
                ] {
                    obj.insert(name.to_string(), serde_json::json!(value));
                }
                obj.insert(
                    "hierarchical_contract_tau2".to_string(),
                    serde_json::json!(snapshot.hierarchical_contract_tau2),
                );
                // Contract-exec WASM counters: the cache-hit / WASM-miss split
                // that makes a summarize or delta rate interpretable at all.
                // Same hand-mirroring footgun as everything else in this block —
                // a new `RouterSnapshotInfo` field is invisible to the collector
                // unless added here. Pinned by
                // `router_snapshot_json_includes_contract_exec_counters`, which
                // asserts the FULL set so a partially-mirrored addition fails.
                for (name, value) in [
                    (
                        "contract_exec_summarize_fast_hits_total",
                        snapshot.contract_exec_summarize_fast_hits_total,
                    ),
                    (
                        "contract_exec_summarize_reload_hits_total",
                        snapshot.contract_exec_summarize_reload_hits_total,
                    ),
                    (
                        "contract_exec_summarize_wasm_calls_total",
                        snapshot.contract_exec_summarize_wasm_calls_total,
                    ),
                    (
                        "contract_exec_summarize_wasm_uncached_total",
                        snapshot.contract_exec_summarize_wasm_uncached_total,
                    ),
                    (
                        "contract_exec_delta_fast_hits_total",
                        snapshot.contract_exec_delta_fast_hits_total,
                    ),
                    (
                        "contract_exec_delta_reload_hits_total",
                        snapshot.contract_exec_delta_reload_hits_total,
                    ),
                    (
                        "contract_exec_delta_wasm_calls_total",
                        snapshot.contract_exec_delta_wasm_calls_total,
                    ),
                    (
                        "contract_exec_delta_wasm_uncached_total",
                        snapshot.contract_exec_delta_wasm_uncached_total,
                    ),
                    (
                        "contract_exec_summarize_fast_hits_last_snapshot",
                        snapshot.contract_exec_summarize_fast_hits_last_snapshot,
                    ),
                    (
                        "contract_exec_summarize_reload_hits_last_snapshot",
                        snapshot.contract_exec_summarize_reload_hits_last_snapshot,
                    ),
                    (
                        "contract_exec_summarize_wasm_calls_last_snapshot",
                        snapshot.contract_exec_summarize_wasm_calls_last_snapshot,
                    ),
                    (
                        "contract_exec_summarize_wasm_uncached_last_snapshot",
                        snapshot.contract_exec_summarize_wasm_uncached_last_snapshot,
                    ),
                    (
                        "contract_exec_delta_fast_hits_last_snapshot",
                        snapshot.contract_exec_delta_fast_hits_last_snapshot,
                    ),
                    (
                        "contract_exec_delta_reload_hits_last_snapshot",
                        snapshot.contract_exec_delta_reload_hits_last_snapshot,
                    ),
                    (
                        "contract_exec_delta_wasm_calls_last_snapshot",
                        snapshot.contract_exec_delta_wasm_calls_last_snapshot,
                    ),
                    (
                        "contract_exec_delta_wasm_uncached_last_snapshot",
                        snapshot.contract_exec_delta_wasm_uncached_last_snapshot,
                    ),
                    (
                        "contract_summary_cache_entries",
                        snapshot.contract_summary_cache_entries,
                    ),
                    (
                        "contract_summary_cache_bytes",
                        snapshot.contract_summary_cache_bytes,
                    ),
                    (
                        "contract_summary_cache_budget_bytes",
                        snapshot.contract_summary_cache_budget_bytes,
                    ),
                    (
                        "contract_summary_cache_count_cap",
                        snapshot.contract_summary_cache_count_cap,
                    ),
                    (
                        "contract_summary_cache_count_cap_evictions_total",
                        snapshot.contract_summary_cache_count_cap_evictions_total,
                    ),
                    (
                        "contract_summary_cache_byte_budget_evictions_total",
                        snapshot.contract_summary_cache_byte_budget_evictions_total,
                    ),
                    (
                        "contract_delta_cache_entries",
                        snapshot.contract_delta_cache_entries,
                    ),
                    (
                        "contract_delta_cache_bytes",
                        snapshot.contract_delta_cache_bytes,
                    ),
                    (
                        "contract_delta_cache_budget_bytes",
                        snapshot.contract_delta_cache_budget_bytes,
                    ),
                    (
                        "contract_delta_cache_count_cap",
                        snapshot.contract_delta_cache_count_cap,
                    ),
                    (
                        "contract_delta_cache_count_cap_evictions_total",
                        snapshot.contract_delta_cache_count_cap_evictions_total,
                    ),
                    (
                        "contract_delta_cache_byte_budget_evictions_total",
                        snapshot.contract_delta_cache_byte_budget_evictions_total,
                    ),
                ] {
                    obj.insert(name.to_string(), serde_json::json!(value));
                }
                obj.insert(
                    "hosted_contracts_count".to_string(),
                    serde_json::json!(snapshot.hosted_contracts_count),
                );
                obj.insert(
                    "hosted_key_distance_median".to_string(),
                    serde_json::json!(snapshot.hosted_key_distance_median),
                );
                obj.insert(
                    "hosted_key_distance_p90".to_string(),
                    serde_json::json!(snapshot.hosted_key_distance_p90),
                );
                obj.insert(
                    "hosted_key_distance_min".to_string(),
                    serde_json::json!(snapshot.hosted_key_distance_min),
                );
                obj.insert(
                    "hosted_key_distance_mean".to_string(),
                    serde_json::json!(snapshot.hosted_key_distance_mean),
                );
                obj.insert(
                    "hosted_key_distance_frac_within_0_1".to_string(),
                    serde_json::json!(snapshot.hosted_key_distance_frac_within_0_1),
                );
                obj.insert(
                    "subscribe_hint_sent".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_sent),
                );
                obj.insert(
                    "subscribe_hint_received".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_received),
                );
                obj.insert(
                    "subscribe_hint_acted".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_acted),
                );
                // Per-gate refusal + directed-subscribe outcome breakdown (#4534
                // diagnostics).
                obj.insert(
                    "subscribe_hint_refused_version".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_refused_version),
                );
                obj.insert(
                    "subscribe_hint_refused_already_hosting".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_refused_already_hosting),
                );
                obj.insert(
                    "subscribe_hint_refused_holder".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_refused_holder),
                );
                obj.insert(
                    "subscribe_hint_refused_cache".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_refused_cache),
                );
                obj.insert(
                    "subscribe_hint_acted_succeeded".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_acted_succeeded),
                );
                obj.insert(
                    "subscribe_hint_acted_failed".to_string(),
                    serde_json::json!(snapshot.subscribe_hint_acted_failed),
                );
                // Nearest-neighbor ring-lattice completeness + probe health
                // (#4760 / #4642). Same hand-mirroring footgun as the placement
                // gauges above — a new `RouterSnapshotInfo` field is invisible to
                // the collector unless added here. Pinned by
                // `router_snapshot_json_includes_placement_gauges`. `Option`
                // fields emit `null` when unset, matching hosted_key_distance_*.
                obj.insert(
                    "lattice_has_successor".to_string(),
                    serde_json::json!(snapshot.lattice_has_successor),
                );
                obj.insert(
                    "lattice_has_predecessor".to_string(),
                    serde_json::json!(snapshot.lattice_has_predecessor),
                );
                obj.insert(
                    "lattice_successor_distance".to_string(),
                    serde_json::json!(snapshot.lattice_successor_distance),
                );
                obj.insert(
                    "lattice_predecessor_distance".to_string(),
                    serde_json::json!(snapshot.lattice_predecessor_distance),
                );
                obj.insert(
                    "lattice_probes_issued".to_string(),
                    serde_json::json!(snapshot.lattice_probes_issued),
                );
                obj.insert(
                    "lattice_probe_improvements".to_string(),
                    serde_json::json!(snapshot.lattice_probe_improvements),
                );
                obj.insert(
                    "lattice_probe_misses".to_string(),
                    serde_json::json!(snapshot.lattice_probe_misses),
                );
                // Version-gate refusal counters (#5156): same hand-mirror
                // footgun as the gauges above — a new `RouterSnapshotInfo`
                // field is invisible to the collector unless added here.
                // These split the hash-first-summaries / summary-first-PUT
                // fail-closed fallback into its two causes (unknown remote
                // version vs known-but-pre-floor), which have opposite
                // self-healing implications. Pinned by
                // `router_snapshot_json_includes_version_gate_refusal_counters`.
                obj.insert(
                    "hash_first_summaries_declined_unknown_version".to_string(),
                    serde_json::json!(snapshot.hash_first_summaries_declined_unknown_version),
                );
                obj.insert(
                    "hash_first_summaries_declined_pre_floor".to_string(),
                    serde_json::json!(snapshot.hash_first_summaries_declined_pre_floor),
                );
                obj.insert(
                    "summary_first_put_declined_unknown_version".to_string(),
                    serde_json::json!(snapshot.summary_first_put_declined_unknown_version),
                );
                obj.insert(
                    "summary_first_put_declined_pre_floor".to_string(),
                    serde_json::json!(snapshot.summary_first_put_declined_pre_floor),
                );
                // Interest-weighted (two-tier) module-cache SHADOW gauges
                // (#4441/#4534). Inserted here (not as inline `json!` keys) to
                // keep the macro under its recursion limit, same as the
                // placement gauges above — but this body is still hand-mirrored,
                // so a new `RouterSnapshotInfo` field is invisible to the
                // collector unless added here. Pinned by
                // `router_snapshot_json_includes_interest_tier_shadow_gauges`.
                obj.insert(
                    "contract_module_cache_cold_evictable_bytes".to_string(),
                    serde_json::json!(snapshot.contract_module_cache_cold_evictable_bytes),
                );
                obj.insert(
                    "contract_module_cache_interested_bytes".to_string(),
                    serde_json::json!(snapshot.contract_module_cache_interested_bytes),
                );
                obj.insert(
                    "contract_module_cache_evictions_would_reclassify_total".to_string(),
                    serde_json::json!(
                        snapshot.contract_module_cache_evictions_would_reclassify_total
                    ),
                );
                obj.insert(
                    "migration_admission_recovered_total".to_string(),
                    serde_json::json!(snapshot.migration_admission_recovered_total),
                );
                // Renewal-terminus short-circuit counter (#4440 proposal 1).
                obj.insert(
                    "renewal_terminus_satisfied".to_string(),
                    serde_json::json!(snapshot.renewal_terminus_satisfied),
                );
                // Capability-relative hosting-budget gauges (#4642 A2). Inserted
                // here (not as inline `json!` keys) to keep the macro under its
                // recursion limit, same as the gauges above — but this body is
                // still hand-mirrored, so a new `RouterSnapshotInfo` field is
                // invisible to the collector unless added here. Pinned by
                // `router_snapshot_json_includes_hosting_budget_gauges`.
                obj.insert(
                    "hosting_budget_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_budget_bytes),
                );
                obj.insert(
                    "hosting_current_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_current_bytes),
                );
                obj.insert(
                    "hosting_contract_count".to_string(),
                    serde_json::json!(snapshot.hosting_contract_count),
                );
                obj.insert(
                    "hosting_budget_evictions_total".to_string(),
                    serde_json::json!(snapshot.hosting_budget_evictions_total),
                );
                // Resident-overhead pressure axis (#5325, #5647) — the SECOND,
                // independent eviction pressure alongside the state-byte budget
                // above: the memory hosted contracts hold in RAM, counted. Same
                // hand-mirrored footgun; pinned by
                // `router_snapshot_json_includes_resident_overhead_gauges`.
                obj.insert(
                    "hosting_resident_overhead_budget_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_resident_overhead_budget_bytes),
                );
                obj.insert(
                    "hosting_resident_overhead_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_resident_overhead_bytes),
                );
                obj.insert(
                    "hosting_resident_overhead_evictions_total".to_string(),
                    serde_json::json!(snapshot.hosting_resident_overhead_evictions_total),
                );
                obj.insert(
                    "hosting_resident_overhead_evicted_charged_bytes_total".to_string(),
                    serde_json::json!(
                        snapshot.hosting_resident_overhead_evicted_charged_bytes_total
                    ),
                );
                obj.insert(
                    "interest_resident_bytes_total".to_string(),
                    serde_json::json!(snapshot.interest_resident_bytes_total),
                );
                obj.insert(
                    "interest_neighbour_summary_bytes".to_string(),
                    serde_json::json!(snapshot.interest_neighbour_summary_bytes),
                );
                obj.insert(
                    "interest_summary_bound_trims_total".to_string(),
                    serde_json::json!(snapshot.interest_summary_bound_trims_total),
                );
                obj.insert(
                    "interest_summary_bound_trimmed_bytes_total".to_string(),
                    serde_json::json!(snapshot.interest_summary_bound_trimmed_bytes_total),
                );
                // Demand-ordered eviction gauges (#4642 A3). Same
                // hand-mirrored footgun as the A2 gauges above: a new
                // `RouterSnapshotInfo` field is invisible to the collector unless
                // added here. Pinned by
                // `router_snapshot_json_includes_hosting_fuel_gauge_gauges`.
                obj.insert(
                    "hosting_evictions_of_recently_read_total".to_string(),
                    serde_json::json!(snapshot.hosting_evictions_of_recently_read_total),
                );
                obj.insert(
                    "hosting_local_hits_total".to_string(),
                    serde_json::json!(snapshot.hosting_local_hits_total),
                );
                obj.insert(
                    "hosting_local_misses_total".to_string(),
                    serde_json::json!(snapshot.hosting_local_misses_total),
                );
                // PUT-durability falsifier gauges (#4642). Same hand-mirrored
                // footgun as the gauges above: a new `RouterSnapshotInfo` field is
                // invisible to the collector unless added here. These answer
                // whether freshly-seeded contracts are evicted before their first
                // reader — the field check for the "PUT is not read-demand"
                // decision. Pinned by
                // `router_snapshot_json_includes_put_durability_gauges`.
                obj.insert(
                    "hosting_evicted_unread_total".to_string(),
                    serde_json::json!(snapshot.hosting_evicted_unread_total),
                );
                obj.insert(
                    "hosting_evicted_unread_age_secs_sum".to_string(),
                    serde_json::json!(snapshot.hosting_evicted_unread_age_secs_sum),
                );
                // Aggregate on-disk usage gauges (#4683). Same hand-mirrored
                // footgun as the gauges above: a new `RouterSnapshotInfo` field
                // is invisible to the collector unless added here. Pinned by
                // `router_snapshot_json_includes_hosting_disk_gauges`.
                obj.insert(
                    "hosting_disk_state_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_disk_state_bytes),
                );
                obj.insert(
                    "hosting_disk_db_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_disk_db_bytes),
                );
                obj.insert(
                    "hosting_disk_wasm_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_disk_wasm_bytes),
                );
                obj.insert(
                    "hosting_disk_compile_cache_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_disk_compile_cache_bytes),
                );
                obj.insert(
                    "hosting_disk_webapp_cache_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_disk_webapp_cache_bytes),
                );
                obj.insert(
                    "hosting_disk_total_bytes".to_string(),
                    serde_json::json!(snapshot.hosting_disk_total_bytes),
                );
                // OOM-valve falsifier (#4642 subscriber-primary rework): pinned by
                // `router_snapshot_json_includes_oom_valve_gauge`. Same
                // hand-mirrored footgun — invisible to the collector unless added
                // here. Stays 0 until the Overflow trigger is wired.
                obj.insert(
                    "hosting_oom_valve_evictions_total".to_string(),
                    serde_json::json!(snapshot.hosting_oom_valve_evictions_total),
                );
                // Subscribed-eviction falsifier (#4642 subscriber-primary rework):
                // count of evictions that shed a SUBSCRIBED contract. Same
                // hand-mirrored footgun — invisible to the collector unless added
                // here. Unlike the OOM-valve gauge this can go nonzero as soon as
                // the rework ships (AtCapacity now sheds subscribed as a last
                // resort). Pinned by
                // `router_snapshot_json_includes_subscribed_evictions_gauge`.
                obj.insert(
                    "hosting_subscribed_evictions_total".to_string(),
                    serde_json::json!(snapshot.hosting_subscribed_evictions_total),
                );
                // Cost-pressure eviction falsifier (cost-aware eviction,
                // #4861): zero-demand contracts shed for dominating the
                // node's attributed update-work. Same hand-mirrored footgun —
                // invisible to the collector unless added here. Pinned by
                // `router_snapshot_json_includes_cost_evictions_gauge`.
                obj.insert(
                    "hosting_cost_evictions_total".to_string(),
                    serde_json::json!(snapshot.hosting_cost_evictions_total),
                );
                // Local notification-delivery outcomes (#4681). These replace a
                // diagnostic that #5040 had to remove: the no-local-subscriber
                // case cannot be logged per occurrence (22k lines/day) and its
                // `debug!` is compiled out of release entirely, so a counter is
                // the only thing an operator can actually see in production.
                // Same hand-mirrored footgun — invisible to the collector
                // unless added here. Pinned by
                // `router_snapshot_json_includes_notification_delivery_gauges`.
                obj.insert(
                    "notifications_dropped_channel_full".to_string(),
                    serde_json::json!(snapshot.notifications_dropped_channel_full),
                );
                obj.insert(
                    "notifications_dropped_channel_closed".to_string(),
                    serde_json::json!(snapshot.notifications_dropped_channel_closed),
                );
                obj.insert(
                    "notifications_no_local_subscriber".to_string(),
                    serde_json::json!(snapshot.notifications_no_local_subscriber),
                );
                // Phantom-hosting falsifier (SUBSCRIBE-retirement step 10 §1d):
                // current count of contracts in-use via a downstream subscriber
                // with NO state on disk. Should read 0 after register-after-state.
                // Same hand-mirrored footgun — invisible to the collector unless
                // added here. Pinned by
                // `router_snapshot_json_includes_phantom_in_use_gauge`.
                obj.insert(
                    "phantom_in_use_contracts".to_string(),
                    serde_json::json!(snapshot.phantom_in_use_contracts),
                );
                // Terminal advertisement-consult counters (hosting redesign
                // piece C, #4646; exported per #4658). Same hand-mirrored
                // footgun as the gauges above: a new `RouterSnapshotInfo` field
                // is invisible to the collector unless added here. These are the
                // findability-baseline signal that decomposes production
                // dead-ends (attempts → hits → resolved_found vs still_not_found)
                // ahead of the piece-E / 0.2.92 decision. Pinned by
                // `router_snapshot_json_includes_terminal_consult_counters`.
                obj.insert(
                    "terminal_consult_attempts".to_string(),
                    serde_json::json!(snapshot.terminal_consult_attempts),
                );
                obj.insert(
                    "terminal_consult_hits".to_string(),
                    serde_json::json!(snapshot.terminal_consult_hits),
                );
                obj.insert(
                    "terminal_consult_resolved_found".to_string(),
                    serde_json::json!(snapshot.terminal_consult_resolved_found),
                );
                obj.insert(
                    "terminal_consult_still_not_found".to_string(),
                    serde_json::json!(snapshot.terminal_consult_still_not_found),
                );
                // Eviction-retraction emission counters (#5059): whether the
                // retraction that stops co-host fan-out at an evicted contract
                // actually reached the wire. Pinned by
                // `router_snapshot_json_includes_hosting_retraction_counters`.
                obj.insert(
                    "hosting_retractions_emitted".to_string(),
                    serde_json::json!(snapshot.hosting_retractions_emitted),
                );
                obj.insert(
                    "hosting_retractions_dropped".to_string(),
                    serde_json::json!(snapshot.hosting_retractions_dropped),
                );
                // Streamed-transfer abort counters (Group B), routing/hosting
                // attribution (Group C), and connect-emit counters — same
                // hand-mirror footgun: a new `RouterSnapshotInfo` field is
                // invisible to the collector unless added here. All `Option<u64>`
                // so one loop suffices. Pinned by
                // `router_snapshot_json_includes_stream_abort_counters` /
                // `_routing_attribution_counters` / `_connect_emit_counters`.
                for (key, value) in [
                    (
                        "stream_recv_aborts_inactivity_total",
                        snapshot.stream_recv_aborts_inactivity_total,
                    ),
                    (
                        "stream_recv_aborts_cancelled_total",
                        snapshot.stream_recv_aborts_cancelled_total,
                    ),
                    (
                        "stream_recv_aborts_claim_timeout_total",
                        snapshot.stream_recv_aborts_claim_timeout_total,
                    ),
                    (
                        "stream_recv_aborts_deserialize_total",
                        snapshot.stream_recv_aborts_deserialize_total,
                    ),
                    (
                        "stream_send_aborts_cwnd_total",
                        snapshot.stream_send_aborts_cwnd_total,
                    ),
                    (
                        "stream_recv_abort_frac_0",
                        snapshot.stream_recv_abort_frac_0,
                    ),
                    (
                        "stream_recv_abort_frac_1",
                        snapshot.stream_recv_abort_frac_1,
                    ),
                    (
                        "stream_recv_abort_frac_lt50",
                        snapshot.stream_recv_abort_frac_lt50,
                    ),
                    (
                        "stream_recv_abort_frac_50_90",
                        snapshot.stream_recv_abort_frac_50_90,
                    ),
                    (
                        "stream_recv_abort_frac_ge90",
                        snapshot.stream_recv_abort_frac_ge90,
                    ),
                    ("ring_connections", snapshot.ring_connections),
                    ("transient_connections", snapshot.transient_connections),
                    ("connections_to_gateways", snapshot.connections_to_gateways),
                    ("relayed_gets_total", snapshot.relayed_gets_total),
                    ("relayed_puts_total", snapshot.relayed_puts_total),
                    (
                        "relayed_subscribes_total",
                        snapshot.relayed_subscribes_total,
                    ),
                    ("relayed_updates_total", snapshot.relayed_updates_total),
                    ("connect_accepts_emitted", snapshot.connect_accepts_emitted),
                    ("connect_rejects_emitted", snapshot.connect_rejects_emitted),
                    (
                        "bootstrap_transient_registered",
                        snapshot.bootstrap_transient_registered,
                    ),
                    (
                        "bootstrap_time_to_first_ring_connection_ms",
                        snapshot.bootstrap_time_to_first_ring_connection_ms,
                    ),
                    (
                        "bootstrap_connect_attempts_before_first_connection",
                        snapshot.bootstrap_connect_attempts_before_first_connection,
                    ),
                    (
                        "bootstrap_transient_expired",
                        snapshot.bootstrap_transient_expired,
                    ),
                    (
                        "bootstrap_promoted_to_ring",
                        snapshot.bootstrap_promoted_to_ring,
                    ),
                    (
                        "bootstrap_startup_rounds_connect_issued_gateway",
                        snapshot.bootstrap_startup_rounds_connect_issued_gateway,
                    ),
                    (
                        "bootstrap_startup_rounds_connect_issued_routed",
                        snapshot.bootstrap_startup_rounds_connect_issued_routed,
                    ),
                    (
                        "bootstrap_startup_rounds_backoff_blocked",
                        snapshot.bootstrap_startup_rounds_backoff_blocked,
                    ),
                    (
                        "bootstrap_startup_rounds_no_target",
                        snapshot.bootstrap_startup_rounds_no_target,
                    ),
                ] {
                    obj.insert(key.to_string(), serde_json::json!(value));
                }
                // Bootstrap-acceptance-churn time-to-bootstrap gauge (#4787):
                // an `Option<f64>`, so it can't join the `Option<u64>` loop
                // above. Pinned by
                // `router_snapshot_json_includes_bootstrap_churn_counters`.
                obj.insert(
                    "bootstrap_time_to_min_connections_secs".to_string(),
                    serde_json::json!(snapshot.bootstrap_time_to_min_connections_secs),
                );
                // Disambiguates the `null` above: `false` = this node has never
                // reached min_connections, absent = the field wasn't reported.
                obj.insert(
                    "bootstrap_completed".to_string(),
                    serde_json::json!(snapshot.bootstrap_completed),
                );
                // Computed-upstream vs. stored-flag divergence counters (piece D,
                // #4642 / #4671) — same hand-mirror footgun: a new
                // `RouterSnapshotInfo` field is invisible to the collector unless
                // added here. These give production field evidence for the stored
                // `is_upstream` flag's drift ahead of the reconcile-core keystone
                // deleting it. Pinned by
                // `router_snapshot_json_includes_upstream_divergence_counters`.
                obj.insert(
                    "upstream_computed_vs_stored_comparisons".to_string(),
                    serde_json::json!(snapshot.upstream_computed_vs_stored_comparisons),
                );
                obj.insert(
                    "upstream_computed_vs_stored_divergences".to_string(),
                    serde_json::json!(snapshot.upstream_computed_vs_stored_divergences),
                );
                // Reconcile-controller SHADOW comparison counters (keystone
                // step-2, #4642) — same hand-mirror footgun: a new
                // `RouterSnapshotInfo` field is invisible to the collector unless
                // added here. These give production field evidence for how far
                // the pure `reconcile` controller diverges from today's scattered
                // decisions ahead of the flip. Pinned by
                // `router_snapshot_json_includes_reconcile_shadow_counters`.
                for (key, value) in [
                    (
                        "reconcile_shadow_collapse_comparisons",
                        snapshot.reconcile_shadow_collapse_comparisons,
                    ),
                    (
                        "reconcile_shadow_collapse_divergences",
                        snapshot.reconcile_shadow_collapse_divergences,
                    ),
                    (
                        "reconcile_shadow_collapse_subscribe_diffs",
                        snapshot.reconcile_shadow_collapse_subscribe_diffs,
                    ),
                    (
                        "reconcile_shadow_collapse_renew_diffs",
                        snapshot.reconcile_shadow_collapse_renew_diffs,
                    ),
                    (
                        "reconcile_shadow_collapse_unsubscribe_diffs",
                        snapshot.reconcile_shadow_collapse_unsubscribe_diffs,
                    ),
                    (
                        "reconcile_shadow_collapse_collapse_diffs",
                        snapshot.reconcile_shadow_collapse_collapse_diffs,
                    ),
                    (
                        "reconcile_shadow_collapse_announce_diffs",
                        snapshot.reconcile_shadow_collapse_announce_diffs,
                    ),
                    (
                        "reconcile_shadow_collapse_retract_diffs",
                        snapshot.reconcile_shadow_collapse_retract_diffs,
                    ),
                    (
                        "reconcile_shadow_collapse_reroot_search_diffs",
                        snapshot.reconcile_shadow_collapse_reroot_search_diffs,
                    ),
                    (
                        "reconcile_shadow_renewal_comparisons",
                        snapshot.reconcile_shadow_renewal_comparisons,
                    ),
                    (
                        "reconcile_shadow_renewal_divergences",
                        snapshot.reconcile_shadow_renewal_divergences,
                    ),
                    (
                        "reconcile_shadow_renewal_subscribe_diffs",
                        snapshot.reconcile_shadow_renewal_subscribe_diffs,
                    ),
                    (
                        "reconcile_shadow_renewal_renew_diffs",
                        snapshot.reconcile_shadow_renewal_renew_diffs,
                    ),
                    (
                        "reconcile_shadow_renewal_unsubscribe_diffs",
                        snapshot.reconcile_shadow_renewal_unsubscribe_diffs,
                    ),
                    (
                        "reconcile_shadow_renewal_collapse_diffs",
                        snapshot.reconcile_shadow_renewal_collapse_diffs,
                    ),
                    (
                        "reconcile_shadow_renewal_announce_diffs",
                        snapshot.reconcile_shadow_renewal_announce_diffs,
                    ),
                    (
                        "reconcile_shadow_renewal_retract_diffs",
                        snapshot.reconcile_shadow_renewal_retract_diffs,
                    ),
                    (
                        "reconcile_shadow_renewal_reroot_search_diffs",
                        snapshot.reconcile_shadow_renewal_reroot_search_diffs,
                    ),
                    // Single-aspect edge sites (keystone step-2 completion,
                    // #4642): comparisons + divergences only (focused on one
                    // class each).
                    (
                        "reconcile_shadow_inbound_unsubscribe_comparisons",
                        snapshot.reconcile_shadow_inbound_unsubscribe_comparisons,
                    ),
                    (
                        "reconcile_shadow_inbound_unsubscribe_divergences",
                        snapshot.reconcile_shadow_inbound_unsubscribe_divergences,
                    ),
                    (
                        "reconcile_shadow_connection_drop_comparisons",
                        snapshot.reconcile_shadow_connection_drop_comparisons,
                    ),
                    (
                        "reconcile_shadow_connection_drop_divergences",
                        snapshot.reconcile_shadow_connection_drop_divergences,
                    ),
                    (
                        "reconcile_shadow_host_formation_comparisons",
                        snapshot.reconcile_shadow_host_formation_comparisons,
                    ),
                    (
                        "reconcile_shadow_host_formation_divergences",
                        snapshot.reconcile_shadow_host_formation_divergences,
                    ),
                ] {
                    obj.insert(key.to_string(), serde_json::json!(value));
                }
            }
            body
        }
    }
}

#[cfg(test)]
mod tests {
    // `TelemetryConfig` and `TelemetryReporter` come in via `super::*`.
    use super::*;

    /// A config that looks exactly like a real production node: telemetry on,
    /// the production `DEFAULT_TELEMETRY_ENDPOINT`, and `is_test_environment:
    /// false` (no `--id`). The only thing separating a prod node from a test
    /// run is the two ambient signals passed to `telemetry_suppression_reason`.
    fn prod_like_config() -> TelemetryConfig {
        TelemetryConfig {
            enabled: true,
            endpoint: crate::config::DEFAULT_TELEMETRY_ENDPOINT.to_string(),
            transport_snapshot_interval_secs: 30,
            is_test_environment: false,
            reference_ping_enabled: false,
            iface_tx_enabled: false,
        }
    }

    /// Regression for #4366: a test build must NOT initialize a
    /// `TelemetryReporter` (and thus must not ship events to the production
    /// OTLP collector) even when the config looks like a real production
    /// node. That is exactly the config the integration tests under
    /// `crates/core/tests/*.rs` build (via `ConfigArgs { ..Default::default() }`,
    /// which leaves `--id` unset). Before the fix they leaked synthetic peers /
    /// simulation-loopback `get_not_found` events to `nova.locut.us:4318`.
    ///
    /// This test runs under `cfg(test)` AND from the cargo `deps/` harness, so
    /// it pins the real entry point's early-return via the two surviving
    /// signals — `cfg!(test)` and `running_under_cargo_test()`. The production
    /// binary trips neither and is unaffected.
    #[test]
    fn telemetry_reporter_suppressed_in_test_build() {
        assert!(
            TelemetryReporter::new(&prod_like_config(), "peer-under-test".to_string()).is_none(),
            "test builds must not ship telemetry to the production collector (#4366)"
        );
    }

    /// The fix for the 0.2.81 telemetry blackout: a genuine production release
    /// binary — `cfg(test)` false, NOT run from a cargo `deps/` harness, no
    /// `--id` — must NOT self-suppress telemetry. Regressing this is exactly
    /// what happened when Cargo feature-unification with `fdev` leaked
    /// `feature = "testing"` onto the shipped `freenet` binary and the guard
    /// keyed suppression on it: telemetry went silently OFF on every
    /// auto-updating release node from 0.2.81 onward.
    #[test]
    fn prod_release_binary_emits_telemetry() {
        assert_eq!(
            telemetry_suppression_reason(
                &prod_like_config(),
                /* is_test_build */ false,
                /* running_under_cargo_test */ false,
            ),
            None,
            "a production release binary must NOT self-suppress telemetry"
        );
    }

    /// The #4366 intent preserved: every real test/sim path that reaches
    /// `TelemetryReporter::new` must still be suppressed so it can't re-pollute
    /// the production collector. The DST simulation framework never builds a
    /// `TelemetryReporter` at all; the paths that DO are unit tests (`cfg(test)`)
    /// and integration / nightly-nextest sims (run from a cargo `deps/` harness),
    /// both covered here, plus the `--id` CLI harness.
    #[test]
    fn test_and_sim_runs_still_suppress_telemetry() {
        // Unit-test build: `cfg!(test)` is true.
        assert_eq!(
            telemetry_suppression_reason(&prod_like_config(), true, false),
            Some(TelemetrySuppression::TestHarness),
            "cfg(test) unit-test builds must suppress (#4366)"
        );
        // Integration test / nightly nextest sim: not a cfg(test) build, but the
        // binary runs from a cargo `deps/` dir (no `--id`).
        assert_eq!(
            telemetry_suppression_reason(&prod_like_config(), false, true),
            Some(TelemetrySuppression::TestHarness),
            "cargo deps/ test harness runs must suppress (#4366)"
        );
        // `--id` test environment (integration/CLI harness).
        let mut id_env = prod_like_config();
        id_env.is_test_environment = true;
        assert_eq!(
            telemetry_suppression_reason(&id_env, false, false),
            Some(TelemetrySuppression::TestEnvironmentFlag),
            "--id test environments must suppress"
        );
        // Operator turned telemetry off.
        let mut disabled = prod_like_config();
        disabled.enabled = false;
        assert_eq!(
            telemetry_suppression_reason(&disabled, false, false),
            Some(TelemetrySuppression::Disabled),
            "telemetry-enabled = false must suppress"
        );
    }

    /// The runtime harness detector must fire for this test binary: cargo
    /// always runs test binaries from a `deps/` directory. This is the signal
    /// that covers the documented `cargo test -p freenet` path run without
    /// `--features testing`, where the compile-time guards don't apply.
    #[test]
    fn detects_running_under_cargo_test() {
        assert!(
            running_under_cargo_test(),
            "test binaries run from a cargo deps/ dir and must be detected (#4366)"
        );
    }

    /// Pin: the chain-blame timeout-label histogram (#5657) must reach the
    /// hand-mirrored `router_snapshot` OTLP body, like every other
    /// `RouterSnapshotInfo` field (the #4009/#4010 manually-mirrored-telemetry
    /// footgun).
    #[test]
    fn router_snapshot_json_includes_timeout_label_histogram() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.timeout_label_peers_1 = Some(11);
        info.timeout_label_peers_2_3 = Some(12);
        info.timeout_label_peers_4_7 = Some(13);
        info.timeout_label_peers_8_plus = Some(14);
        info.timeout_label_max_per_peer = Some(15);
        info.timeout_labels_untracked = Some(16);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (field, value) in [
            ("timeout_label_peers_1", 11),
            ("timeout_label_peers_2_3", 12),
            ("timeout_label_peers_4_7", 13),
            ("timeout_label_peers_8_plus", 14),
            ("timeout_label_max_per_peer", 15),
            ("timeout_labels_untracked", 16),
        ] {
            assert_eq!(json[field], value, "{field} must reach the OTLP body");
        }
    }

    /// Pin: the manually-mirrored `router_snapshot` OTLP body
    /// (`event_kind_to_json`) must forward the node-health gauges. The body is a
    /// hand-written `json!` block, so a new `RouterSnapshotInfo` field is
    /// silently dropped from central telemetry unless it is added there too
    /// (the #4009/#4010 manually-mirrored-telemetry footgun). See #4440.
    #[test]
    fn router_snapshot_json_includes_fd_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.open_fds = Some(123);
        info.fd_soft_limit = Some(4096);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(json["open_fds"], 123, "open_fds must reach the OTLP body");
        assert_eq!(
            json["fd_soft_limit"], 4096,
            "fd_soft_limit must reach the OTLP body"
        );
    }

    #[test]
    fn router_snapshot_network_efficiency_v1_has_explicit_realistic_and_worst_case_budgets() {
        use arbitrary::Arbitrary;

        const BUSY_FLEET_COUNTER: u64 = 999_999;
        // Raised from 2_048 by #6001 for the per-event-type admission rows
        // (`tel_t`, 74 rows of two counters) and the appended `tel` entry: an
        // all-zero row is `[0,0],`, 6 bytes. Measured 2441.
        const MAX_EMPTY_JSON_BYTES: usize = 2_560;
        // Raised from 5_120 to admit TWO independently-developed blocks that
        // landed together on this soak branch:
        //
        //  * the hosting-observability counters (#4642): `host_begin`
        //    (7 causes) + `host_reads` (6 buckets) + `host_recency`
        //    (5 buckets) = 18 counters, +173 JSON bytes at busy-fleet values
        //    (5095 -> 5268, measured on that branch alone);
        //  * the shadow-mode futile-repair block
        //    (`crate::ring::futile_repair`): `futile` (14 scalars) +
        //    `futile_ladder` (an 8-rung survival ladder) = 22 counters,
        //    +183 JSON bytes at busy-fleet values (5095 -> 5278, measured on
        //    that branch alone).
        //
        // The two costs are ADDITIVE and each branch had independently raised
        // this budget to 5_376, which merges without a conflict marker because
        // both sides are the same text. The merged busy-fleet block is
        // MEASURED at 5451 bytes (5095 base + 173 + 183), which OVERFLOWS
        // 5_376 by 75, so the budget is raised one further 256-byte step.
        //
        //   40 new counters
        //   +356 JSON bytes at busy-fleet values
        //   emitted once per ~30 min per peer, ~2000 reporting peers
        //   => 48 * 2000 * ~356 B ~= 34 MB/day
        //
        // against a collector ingesting ~88.8 GB/day: about 0.04%. Both blocks
        // are already the minimum that answers their question — the hosting
        // rows are the ONLY record of why a peer hosts anything plus the only
        // fleet-wide view of `read_count` / `last_genuine_access` (the demand
        // signals the eviction ranking is built on, previously reaching the
        // node's own HTML dashboard and nothing else), and the futile ladder is
        // 8 rungs rather than a per-value histogram. Neither carries a
        // per-contract or per-peer label: contract keys are attacker-chosen, so
        // labelling would hand an attacker control of collector cardinality.
        // Do NOT raise this again without redoing the arithmetic.
        //
        // Raised again from 5_632 by #6001, which added `tel_t`: lifetime
        // admitted and dropped counts per telemetry event type at the node's
        // rate limiter (74 types x 2 = 148 counters), plus one `tel` entry.
        // Measured 6701 with EVERY counter at the busy-fleet value, about
        // +1070 bytes:
        //
        //   149 new counters
        //   +1070 JSON bytes at busy-fleet values, an upper bound: a node
        //   emits perhaps 25 of the 74 types, and a zero row is 6 bytes
        //   emitted once per ~30 min per peer, ~2000 reporting peers
        //   => 48 * 2000 * ~1070 B ~= 103 MB/day
        //
        // against a collector ingesting ~88.8 GB/day: about 0.12% at that
        // bound. Per-type is the minimum that answers #6001's question (which
        // event types a busy node offers, and which the cap drops); the label
        // set is the fixed `KNOWN_EVENT_TYPES` list with an `other` row, never
        // a producer-chosen or per-transaction name.
        const MAX_BUSY_JSON_BYTES: usize = 6_912;
        // Raised from 14_336 alongside the busy budget, same 40 counters. This
        // is the MATHEMATICAL ceiling (every counter at u64::MAX, 20 digits),
        // measured 15195; no fleet value approaches it, so it constrains
        // schema shape rather than real bytes. #5647 added 7 counters (the
        // `rejected_oversized` population outcome across the 6 sources, and
        // the `over_size_bound` entry in `current`), 7 x 21 bytes at u64::MAX
        // = 147: measured 15342, still within this limit. Raised by #6001 for
        // the 149 counters of `tel_t` and `tel`: measured 18629.
        const MAX_WORST_CASE_JSON_BYTES: usize = 18_688;
        // Raised from 2_048 by #5647 for the same 7 new counters, about 2 bytes
        // each in an all-zero block (a digit and a comma): measured 2057.
        // Raised again by #6001 for `tel_t` and `tel`: measured 2515.
        const MAX_EMPTY_OTLP_MARGINAL_BYTES: usize = 2_560;
        // Raised from 5_120 (2026-08-07) to admit `ms_size` + `ms_unt_age`,
        // the two counters added for #5153. The budget exists to force this
        // arithmetic, not to forbid growth, so here it is:
        //
        //   30 new counters (4 size rows x 6 buckets, + 6 age buckets)
        //   ~210 JSON bytes at busy-fleet values, 5157 OTLP marginal measured
        //   emitted once per ~30 min per peer, ~2000 reporting peers
        //   => 48 * 2000 * ~210 B ~= 20 MB/day
        //
        // against a collector ingesting ~88.8 GB/day: about 0.02%. The block
        // is also already trimmed twice to get here — the histogram covers 4
        // of 10 classes (the other 6 are a measured zero) and 6 size buckets
        // rather than 8. Do NOT raise this again without redoing the
        // arithmetic; the JSON-bytes budgets above are deliberately unchanged.
        // Raised again from 5_376 for the 40 counters of the two blocks merged
        // onto this soak branch (hosting observability + futile repair); see
        // the arithmetic on MAX_BUSY_JSON_BYTES above. Measured 5523. Note
        // the OTLP marginal is NOT the JSON figure plus a constant — it is a
        // different axis (the JSON block re-encoded as an escaped OTLP string
        // body), so it is measured separately rather than inferred. Raised
        // again by #6001 for `tel_t` and `tel`, same arithmetic as
        // MAX_BUSY_JSON_BYTES: measured 6775.
        const MAX_BUSY_OTLP_MARGINAL_BYTES: usize = 6_912;
        // Raised from 14_336 alongside the busy budget above, same 30 new
        // counters, same #5153 rationale, then again for the 40 counters of the
        // two blocks merged onto this soak branch — measured 15267. This
        // bound is the MATHEMATICAL ceiling (every counter at u64::MAX, 20
        // digits); no fleet value approaches it, so it constrains schema shape
        // rather than real bytes. Raised by #5647 for the same 7 counters as
        // MAX_WORST_CASE_JSON_BYTES (147 bytes at u64::MAX): measured 15414.
        // Raised by #6001 for `tel_t` and `tel`: measured 18703.
        const MAX_WORST_OTLP_MARGINAL_BYTES: usize = 18_816;
        const MAX_NULL_OTLP_MARGINAL_BYTES: usize = 64;

        let diagnostic = |value| crate::router::NetworkEfficiencyV1 {
            v: 1,
            ms_s: [value; 10],
            ms_b: [value; 10],
            ms_age: [value; 5],
            ms_size: [[value; 6]; 4],
            ms_unt_age: [value; 6],
            reg_ow_k: [value; 7],
            reg_ow_m: [value; 7],
            reg_new_k: [value; 7],
            reg_new_m: [value; 7],
            reg_cap: [value; 7],
            removed: [value; crate::ring::interest::InterestRemovalCause::COUNT],
            current: [value; crate::ring::interest::SummaryMissingReason::COUNT + 1],
            recreated: [value; crate::ring::interest::InterestRemovalCause::COUNT],
            populated: [[value; crate::ring::interest::SummaryPopulationOutcome::COUNT]; 6],
            corr_ovf: [value; 2],
            queue: [value; 15],
            recv_n: [[value; 9]; 4],
            recv_tn: [[value; 9]; 10],
            recv_tb: [[value; 9]; 10],
            state_n: [value; 9],
            state_b: [value; 9],
            state: [value; 9],
            evict_n: [[value; 9]; 3],
            evict_b: [[value; 9]; 3],
            vict_n: [[value; 9]; 3],
            vict_b: [[value; 9]; 3],
            cost: [[value; 5]; 3],
            cost_ba: [[value; 9]; 3],
            cost_be: [[value; 9]; 3],
            tel: [value; 16],
            shadow: [[value; 9]; 7],
            eff: [value; 8],
            tel_t: vec![[value; 2]; KNOWN_EVENT_TYPES.len()],
            host_begin: [value; 7],
            host_reads: [value; 6],
            host_recency: [value; 5],
            futile: [value; crate::ring::futile_repair::SNAPSHOT_SCALARS],
            futile_ladder: [value; crate::ring::futile_repair::LADDER_LEN],
        };

        let mut u = arbitrary::Unstructured::new(&[0_u8; 32_768]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        let base_info = info.clone();
        info.network_efficiency_v1 = Some(diagnostic(BUSY_FLEET_COUNTER));

        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info.clone())));
        let block = &json["network_efficiency_v1"];
        let object = block
            .as_object()
            .expect("network_efficiency_v1 must remain a JSON object");
        assert_eq!(object.len(), 39, "schema must remain fixed-cardinality");
        let encoded = serde_json::to_vec(block).expect("serialize diagnostic block");
        assert!(
            encoded.len() <= MAX_BUSY_JSON_BYTES,
            "busy-fleet diagnostic grew to {} bytes (limit {MAX_BUSY_JSON_BYTES})",
            encoded.len()
        );

        let empty = serde_json::to_vec(&diagnostic(0)).expect("serialize empty diagnostic block");
        assert!(
            empty.len() <= MAX_EMPTY_JSON_BYTES,
            "empty diagnostic grew to {} bytes (limit {MAX_EMPTY_JSON_BYTES})",
            empty.len()
        );

        let worst_case = serde_json::to_vec(&diagnostic(u64::MAX))
            .expect("serialize worst-case diagnostic block");
        assert!(
            worst_case.len() <= MAX_WORST_CASE_JSON_BYTES,
            "mathematical worst-case diagnostic grew to {} bytes (limit \
             {MAX_WORST_CASE_JSON_BYTES})",
            worst_case.len()
        );

        let otlp_data_len = |event_data| {
            let event = TelemetryEvent {
                timestamp: 1,
                peer_id: "peer".to_string(),
                transaction_id: String::new(),
                event_type: "router_snapshot".to_string(),
                event_data,
                priority: EventPriority::Operational,
            };
            serde_json::to_vec(&to_otlp_logs(&[event])).unwrap().len()
        };
        let base_data = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(base_info.clone())));
        let base_otlp = otlp_data_len(base_data.clone());
        let mut without_field = base_data;
        without_field
            .as_object_mut()
            .unwrap()
            .remove("network_efficiency_v1");
        let null_otlp = base_otlp.saturating_sub(otlp_data_len(without_field));
        let busy_otlp = otlp_data_len(event_kind_to_json(&EventKind::RouterSnapshot(Box::new(
            info,
        ))))
        .saturating_sub(base_otlp);
        let mut empty_info = base_info.clone();
        empty_info.network_efficiency_v1 = Some(diagnostic(0));
        let empty_otlp = otlp_data_len(event_kind_to_json(&EventKind::RouterSnapshot(Box::new(
            empty_info,
        ))))
        .saturating_sub(base_otlp);
        let mut worst_info = base_info;
        worst_info.network_efficiency_v1 = Some(diagnostic(u64::MAX));
        let worst_otlp = otlp_data_len(event_kind_to_json(&EventKind::RouterSnapshot(Box::new(
            worst_info,
        ))))
        .saturating_sub(base_otlp);
        assert_eq!(null_otlp, 31, "null snapshots are part of the fleet budget");
        // Report the measured value, not just the verdict: when this trips, the
        // useful question is "by how much", and a bare assert makes every
        // adjustment a guess-and-recompile loop.
        assert!(
            null_otlp <= MAX_NULL_OTLP_MARGINAL_BYTES,
            "null OTLP marginal {null_otlp} > {MAX_NULL_OTLP_MARGINAL_BYTES}"
        );
        assert!(
            empty_otlp <= MAX_EMPTY_OTLP_MARGINAL_BYTES,
            "empty OTLP marginal {empty_otlp} > {MAX_EMPTY_OTLP_MARGINAL_BYTES}"
        );
        assert!(
            busy_otlp <= MAX_BUSY_OTLP_MARGINAL_BYTES,
            "busy OTLP marginal {busy_otlp} > {MAX_BUSY_OTLP_MARGINAL_BYTES}"
        );
        assert!(
            worst_otlp <= MAX_WORST_OTLP_MARGINAL_BYTES,
            "worst-case OTLP marginal {worst_otlp} > {MAX_WORST_OTLP_MARGINAL_BYTES}"
        );
    }

    /// The per-event-type admission rows (#6001) and the extended `tel` row
    /// must reach the OTLP body with distinct values in their own positions,
    /// so a transposed or truncated row fails here rather than in the fleet
    /// data.
    #[test]
    fn router_snapshot_json_carries_event_type_admission_rows() {
        use arbitrary::Arbitrary;

        let mut u = arbitrary::Unstructured::new(&[0_u8; 32_768]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        let mut block_bytes = arbitrary::Unstructured::new(&[0_u8; 32_768]);
        let mut block = crate::router::NetworkEfficiencyV1::arbitrary(&mut block_bytes)
            .expect("construct NetworkEfficiencyV1 for test");
        block.tel_t = (0..KNOWN_EVENT_TYPES.len() as u64)
            .map(|row| [1_000 + row, 2_000 + row])
            .collect();
        block.tel = std::array::from_fn(|position| 100 + position as u64);
        info.network_efficiency_v1 = Some(block);

        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        let efficiency = &json["network_efficiency_v1"];
        let rows = efficiency["tel_t"]
            .as_array()
            .expect("tel_t must be a JSON array");
        assert_eq!(rows.len(), KNOWN_EVENT_TYPES.len());
        let transport = event_type_index("transport_snapshot") as u64;
        assert_eq!(
            efficiency["tel_t"][transport as usize],
            serde_json::json!([1_000 + transport, 2_000 + transport])
        );
        assert_eq!(
            efficiency["tel_t"][OTHER_EVENT_TYPE_INDEX],
            serde_json::json!([
                1_000 + OTHER_EVENT_TYPE_INDEX as u64,
                2_000 + OTHER_EVENT_TYPE_INDEX as u64
            ])
        );
        assert_eq!(efficiency["tel"].as_array().map(Vec::len), Some(16));
        assert_eq!(
            efficiency["tel"][15], 115,
            "periodic admitted is appended last"
        );
    }

    /// The hosting-observability rows must survive SERIALIZATION into the
    /// `router_snapshot` body, not merely exist as struct fields.
    ///
    /// `network_efficiency_v1` is the only telemetry family with a reserved
    /// channel and buffer slot that also bypasses the node-side rate limiter
    /// and the collector's 5% sampler, so a field that
    /// silently fails to serialize is not "degraded" — it is absent, with no
    /// second path to notice by. Each row is given a DISTINCT value so a
    /// transposed assignment (`host_reads` fed from `host_recency`, say) fails
    /// here instead of quietly publishing the wrong series.
    #[test]
    fn router_snapshot_json_carries_hosting_observability_rows() {
        use arbitrary::Arbitrary;

        let mut u = arbitrary::Unstructured::new(&[0_u8; 32_768]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        let mut block_bytes = arbitrary::Unstructured::new(&[0_u8; 32_768]);
        let mut block = crate::router::NetworkEfficiencyV1::arbitrary(&mut block_bytes)
            .expect("construct NetworkEfficiencyV1 for test");
        block.host_begin = [11, 12, 13, 14, 15, 16, 17];
        block.host_reads = [21, 22, 23, 24, 25, 26];
        block.host_recency = [31, 32, 33, 34, 35];
        info.network_efficiency_v1 = Some(block);

        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        let efficiency = &json["network_efficiency_v1"];
        assert_eq!(
            efficiency["host_begin"],
            serde_json::json!([11, 12, 13, 14, 15, 16, 17]),
            "hosting-begin causes must reach the OTLP body"
        );
        assert_eq!(
            efficiency["host_reads"],
            serde_json::json!([21, 22, 23, 24, 25, 26]),
            "the read_count gauge must reach the OTLP body"
        );
        assert_eq!(
            efficiency["host_recency"],
            serde_json::json!([31, 32, 33, 34, 35]),
            "the genuine-access recency gauge must reach the OTLP body"
        );
    }

    /// Pin: the module-cache gauges (#4440) must also reach the hand-mirrored
    /// OTLP body — same footgun as the fd gauges above.
    #[test]
    fn router_snapshot_json_includes_module_cache_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.contract_module_cache_entries = Some(7);
        info.contract_module_cache_total_bytes = Some(11);
        info.contract_module_cache_budget_bytes = Some(13);
        info.contract_module_cache_evictions_total = Some(17);
        info.delegate_module_cache_entries = Some(19);
        info.delegate_module_cache_total_bytes = Some(23);
        info.delegate_module_cache_budget_bytes = Some(29);
        info.delegate_module_cache_evictions_total = Some(31);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("contract_module_cache_entries", 7),
            ("contract_module_cache_total_bytes", 11),
            ("contract_module_cache_budget_bytes", 13),
            ("contract_module_cache_evictions_total", 17),
            ("delegate_module_cache_entries", 19),
            ("delegate_module_cache_total_bytes", 23),
            ("delegate_module_cache_budget_bytes", 29),
            ("delegate_module_cache_evictions_total", 31),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the capability-relative hosting-budget gauges (#4642 A2) must also
    /// reach the hand-mirrored OTLP body — same footgun as the gauges above. A
    /// silent drop would re-blind us to whether the RAM-scaled budget keeps a
    /// small box off the #4565 OOM path (the whole point of instrumenting A2).
    #[test]
    fn router_snapshot_json_includes_hosting_budget_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_budget_bytes = Some(257);
        info.hosting_current_bytes = Some(263);
        info.hosting_contract_count = Some(269);
        info.hosting_budget_evictions_total = Some(271);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hosting_budget_bytes", 257),
            ("hosting_current_bytes", 263),
            ("hosting_contract_count", 269),
            ("hosting_budget_evictions_total", 271),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the resident-overhead pressure gauges (#5325) must reach the
    /// hand-mirrored OTLP body. This is the second hop of a two-hop hand-mirror
    /// (`HostingCacheStats` → `RouterSnapshotInfo` → here); the first hop is
    /// pinned by `ring::hosting_stats_mirror_source_tests`. The axis shipped
    /// computed-and-rendered-but-unexported, so a fleet audit could see a node's
    /// state-byte occupancy sitting at 13% with no way to tell it was
    /// nevertheless evicting under resident-overhead pressure.
    #[test]
    fn router_snapshot_json_includes_resident_overhead_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_resident_overhead_budget_bytes = Some(277);
        info.hosting_resident_overhead_bytes = Some(281);
        info.hosting_resident_overhead_evictions_total = Some(293);
        info.hosting_resident_overhead_evicted_charged_bytes_total = Some(307);
        info.interest_resident_bytes_total = Some(311);
        info.interest_summary_bound_trims_total = Some(313);
        info.interest_neighbour_summary_bytes = Some(331);
        info.interest_summary_bound_trimmed_bytes_total = Some(317);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hosting_resident_overhead_budget_bytes", 277),
            ("hosting_resident_overhead_bytes", 281),
            ("hosting_resident_overhead_evictions_total", 293),
            ("hosting_resident_overhead_evicted_charged_bytes_total", 307),
            ("interest_resident_bytes_total", 311),
            ("interest_summary_bound_trims_total", 313),
            ("interest_neighbour_summary_bytes", 331),
            ("interest_summary_bound_trimmed_bytes_total", 317),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the demand-ordered eviction gauges (#4642 A3) must
    /// also reach the hand-mirrored OTLP body — same footgun as the A2 gauges. A
    /// silent drop would re-blind us to the #4338 miscalibration signal
    /// (evicting repeatedly-read contracts) and the local hit-rate, the whole
    /// point of instrumenting A3.
    #[test]
    fn router_snapshot_json_includes_hosting_fuel_gauge_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_evictions_of_recently_read_total = Some(277);
        info.hosting_local_hits_total = Some(281);
        info.hosting_local_misses_total = Some(283);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hosting_evictions_of_recently_read_total", 277),
            ("hosting_local_hits_total", 281),
            ("hosting_local_misses_total", 283),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the PUT-durability falsifier gauges (#4642) must also reach the
    /// hand-mirrored OTLP body — same footgun as the A2/A3 gauges above. These are
    /// the field check for the "a PUT seeds a contract but does NOT count as
    /// read-demand" decision (are seeds evicted before their first reader?); a
    /// silent drop would re-blind central telemetry to that question — the exact
    /// gap this change closes.
    #[test]
    fn router_snapshot_json_includes_put_durability_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_evicted_unread_total = Some(349);
        info.hosting_evicted_unread_age_secs_sum = Some(353);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hosting_evicted_unread_total", 349),
            ("hosting_evicted_unread_age_secs_sum", 353),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the aggregate on-disk usage gauges (#4683) must also reach the
    /// hand-mirrored OTLP body — same footgun as the gauges above. A silent drop
    /// would blind central telemetry to the disk-budget accounting this PR
    /// introduces (the measurement the eviction floor + admission gate build on).
    #[test]
    fn router_snapshot_json_includes_hosting_disk_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_disk_state_bytes = Some(401);
        info.hosting_disk_db_bytes = Some(431);
        info.hosting_disk_wasm_bytes = Some(409);
        info.hosting_disk_compile_cache_bytes = Some(419);
        info.hosting_disk_webapp_cache_bytes = Some(433);
        info.hosting_disk_total_bytes = Some(1229);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hosting_disk_state_bytes", 401),
            // #5007: the database measurement and the webapp cache are the two
            // gauges that were entirely absent. Without them in the OTLP body
            // there is no way to see the dead-space gap from central telemetry.
            ("hosting_disk_db_bytes", 431),
            ("hosting_disk_wasm_bytes", 409),
            ("hosting_disk_compile_cache_bytes", 419),
            ("hosting_disk_webapp_cache_bytes", 433),
            ("hosting_disk_total_bytes", 1229),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the OOM-valve falsifier gauge (#4642 subscriber-primary rework) must
    /// reach the hand-mirrored OTLP body — same footgun as the falsifier gauges
    /// above. This counts evictions that shed a SUBSCRIBED contract under genuine
    /// RAM overflow (the valve piercing the in-use pin). It stays 0 until the
    /// Overflow trigger is wired, but the telemetry must be live now so a nonzero
    /// rate is visible the moment the trigger lands; a silent drop would blind
    /// central telemetry to the single riskiest new hosting behavior.
    #[test]
    fn router_snapshot_json_includes_oom_valve_gauge() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_oom_valve_evictions_total = Some(359);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(
            json["hosting_oom_valve_evictions_total"], 359,
            "hosting_oom_valve_evictions_total must reach the OTLP body"
        );
    }

    /// Pin: the subscribed-eviction falsifier gauge (#4642 subscriber-primary
    /// rework) must reach the hand-mirrored OTLP body. This counts evictions that
    /// shed a SUBSCRIBED contract as a last resort — unlike the OOM-valve gauge it
    /// can go nonzero the moment the rework ships (AtCapacity now sheds subscribed
    /// contracts), so a silent drop would blind central telemetry to the single
    /// riskiest new hosting behavior exactly when it first fires.
    #[test]
    fn router_snapshot_json_includes_subscribed_evictions_gauge() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_subscribed_evictions_total = Some(287);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(
            json["hosting_subscribed_evictions_total"], 287,
            "hosting_subscribed_evictions_total must reach the OTLP body"
        );
    }

    /// Pin: the cost-pressure eviction falsifier gauge (cost-aware eviction,
    /// #4861) must reach the hand-mirrored OTLP body. It counts zero-demand
    /// contracts shed for dominating the node's attributed update-work (CPU /
    /// broadcast fan-out); a silent drop would blind central telemetry to
    /// whether the cost trigger fires in the field — the validation signal the
    /// whole cost-aware-eviction change rests on.
    #[test]
    fn router_snapshot_json_includes_cost_evictions_gauge() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_cost_evictions_total = Some(58);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(
            json["hosting_cost_evictions_total"], 58,
            "hosting_cost_evictions_total must reach the OTLP body"
        );
    }

    /// Pin: the notification-delivery gauges (#4681) must reach the
    /// hand-mirrored OTLP body.
    ///
    /// These carry more weight than a usual gauge. #5040 had to stop logging
    /// the no-local-subscriber case per occurrence (22,413 lines in one day on
    /// a single node), and its replacement `debug!` is compiled out of release
    /// builds by `release_max_level_info`. So in production these counters are
    /// the ONLY visibility into local notification delivery — a silent drop
    /// here restores exactly the blind spot #4773 was merged to close.
    #[test]
    fn router_snapshot_json_includes_notification_delivery_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.notifications_dropped_channel_full = Some(7);
        info.notifications_dropped_channel_closed = Some(11);
        info.notifications_no_local_subscriber = Some(22413);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(
            json["notifications_dropped_channel_full"], 7,
            "notifications_dropped_channel_full must reach the OTLP body"
        );
        assert_eq!(
            json["notifications_dropped_channel_closed"], 11,
            "notifications_dropped_channel_closed must reach the OTLP body"
        );
        assert_eq!(
            json["notifications_no_local_subscriber"], 22413,
            "notifications_no_local_subscriber must reach the OTLP body"
        );
    }

    /// Pin: the phantom-hosting falsifier gauge (SUBSCRIBE-retirement step 10
    /// §1d) must reach the hand-mirrored OTLP body. It counts contracts in-use
    /// via a downstream subscriber with NO state on disk
    /// (`contract_in_use && !contract_state_present`) and should read 0 after the
    /// register-after-state fix; a silent drop would blind central telemetry to a
    /// regression that re-introduces the phantom (#4404/#4612).
    #[test]
    fn router_snapshot_json_includes_phantom_in_use_gauge() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.phantom_in_use_contracts = Some(7);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(
            json["phantom_in_use_contracts"], 7,
            "phantom_in_use_contracts must reach the OTLP body"
        );
    }

    /// Pin: the version-gate refusal counters (#5156) must reach the
    /// hand-mirrored OTLP body — same footgun as the gauges above. These
    /// split why `supports_hash_first_summaries` / `supports_summary_first_put`
    /// fell back to the full-bytes path into its two causes (unknown remote
    /// version, which never self-heals, vs known-but-pre-floor, which
    /// self-heals as the fleet upgrades). A silent drop would re-blind
    /// central telemetry to which cause actually dominates the fallback.
    #[test]
    fn router_snapshot_json_includes_version_gate_refusal_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hash_first_summaries_declined_unknown_version = Some(41);
        info.hash_first_summaries_declined_pre_floor = Some(43);
        info.summary_first_put_declined_unknown_version = Some(47);
        info.summary_first_put_declined_pre_floor = Some(53);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hash_first_summaries_declined_unknown_version", 41),
            ("hash_first_summaries_declined_pre_floor", 43),
            ("summary_first_put_declined_unknown_version", 47),
            ("summary_first_put_declined_pre_floor", 53),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the terminal advertisement-consult counters (hosting redesign piece
    /// C, #4646) must also reach the hand-mirrored OTLP body — same footgun as
    /// the gauges above. These four counters are the findability baseline #4658
    /// exports so the piece-E / 0.2.92 decision rests on the production dead-end
    /// decomposition (off-path-advertised-host dead-ends C closes vs the
    /// no-host-near-key residual needing piece D), not simulation. A silent drop
    /// would re-blind central telemetry to C's effectiveness — the exact gap
    /// this change closes.
    #[test]
    fn router_snapshot_json_includes_terminal_consult_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.terminal_consult_attempts = Some(311);
        info.terminal_consult_hits = Some(313);
        info.terminal_consult_resolved_found = Some(317);
        info.terminal_consult_still_not_found = Some(331);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("terminal_consult_attempts", 311),
            ("terminal_consult_hits", 313),
            ("terminal_consult_resolved_found", 317),
            ("terminal_consult_still_not_found", 331),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the eviction-retraction counters (#5059) must reach the
    /// hand-mirrored OTLP body — same footgun as the counters above. The
    /// retraction is emitted best-effort on the cap-2048 node-event channel and
    /// its drop is logged at `debug`, which compiles out in release, so these
    /// counters are the ONLY production signal distinguishing "retraction
    /// dropped, healing on the ~5-min heartbeat" from "the fix is not working".
    #[test]
    fn router_snapshot_json_includes_hosting_retraction_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosting_retractions_emitted = Some(509);
        info.hosting_retractions_dropped = Some(521);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hosting_retractions_emitted", 509),
            ("hosting_retractions_dropped", 521),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the streamed-transfer abort counters (Group B) must reach the
    /// hand-mirrored OTLP body — same footgun as the counters above. These are
    /// the large-contract failure-isolation signal; a silent drop re-blinds
    /// central telemetry to WHERE large fetches fail.
    #[test]
    fn router_snapshot_json_includes_stream_abort_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.stream_recv_aborts_inactivity_total = Some(11);
        info.stream_recv_aborts_cancelled_total = Some(12);
        info.stream_recv_aborts_claim_timeout_total = Some(13);
        info.stream_recv_aborts_deserialize_total = Some(14);
        info.stream_send_aborts_cwnd_total = Some(15);
        info.stream_recv_abort_frac_0 = Some(1);
        info.stream_recv_abort_frac_1 = Some(2);
        info.stream_recv_abort_frac_lt50 = Some(3);
        info.stream_recv_abort_frac_50_90 = Some(4);
        info.stream_recv_abort_frac_ge90 = Some(5);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("stream_recv_aborts_inactivity_total", 11),
            ("stream_recv_aborts_cancelled_total", 12),
            ("stream_recv_aborts_claim_timeout_total", 13),
            ("stream_recv_aborts_deserialize_total", 14),
            ("stream_send_aborts_cwnd_total", 15),
            ("stream_recv_abort_frac_0", 1),
            ("stream_recv_abort_frac_1", 2),
            ("stream_recv_abort_frac_lt50", 3),
            ("stream_recv_abort_frac_50_90", 4),
            ("stream_recv_abort_frac_ge90", 5),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the routing/hosting attribution gauges + counters (Group C) must
    /// reach the hand-mirrored OTLP body.
    #[test]
    fn router_snapshot_json_includes_routing_attribution_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.ring_connections = Some(21);
        info.transient_connections = Some(22);
        info.connections_to_gateways = Some(23);
        info.relayed_gets_total = Some(24);
        info.relayed_puts_total = Some(25);
        info.relayed_subscribes_total = Some(26);
        info.relayed_updates_total = Some(27);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("ring_connections", 21),
            ("transient_connections", 22),
            ("connections_to_gateways", 23),
            ("relayed_gets_total", 24),
            ("relayed_puts_total", 25),
            ("relayed_subscribes_total", 26),
            ("relayed_updates_total", 27),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the connect-emission counters (firehose-retirement precursor) must
    /// reach the hand-mirrored OTLP body.
    #[test]
    fn router_snapshot_json_includes_connect_emit_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.connect_accepts_emitted = Some(41);
        info.connect_rejects_emitted = Some(42);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(json["connect_accepts_emitted"], 41);
        assert_eq!(json["connect_rejects_emitted"], 42);
    }

    /// Pin: the bootstrap-acceptance-churn counters (#4787 instrumentation)
    /// must reach the hand-mirrored OTLP body — same hand-mirror footgun as
    /// the connect-emit counters above.
    #[test]
    fn router_snapshot_json_includes_bootstrap_churn_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.bootstrap_transient_registered = Some(10);
        info.bootstrap_transient_expired = Some(9);
        info.bootstrap_promoted_to_ring = Some(1);
        info.bootstrap_time_to_min_connections_secs = Some(12.5);
        info.bootstrap_completed = Some(true);
        info.bootstrap_time_to_first_ring_connection_ms = Some(1500);
        info.bootstrap_connect_attempts_before_first_connection = Some(7);
        info.bootstrap_startup_rounds_connect_issued_gateway = Some(4);
        info.bootstrap_startup_rounds_connect_issued_routed = Some(5);
        info.bootstrap_startup_rounds_backoff_blocked = Some(3);
        info.bootstrap_startup_rounds_no_target = Some(2);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(json["bootstrap_transient_registered"], 10);
        assert_eq!(json["bootstrap_transient_expired"], 9);
        assert_eq!(json["bootstrap_promoted_to_ring"], 1);
        assert_eq!(json["bootstrap_time_to_min_connections_secs"], 12.5);
        assert_eq!(json["bootstrap_completed"], true);
        assert_eq!(json["bootstrap_time_to_first_ring_connection_ms"], 1500);
        assert_eq!(
            json["bootstrap_connect_attempts_before_first_connection"],
            7
        );
        assert_eq!(json["bootstrap_startup_rounds_connect_issued_gateway"], 4);
        assert_eq!(json["bootstrap_startup_rounds_connect_issued_routed"], 5);
        assert_eq!(json["bootstrap_startup_rounds_backoff_blocked"], 3);
        assert_eq!(json["bootstrap_startup_rounds_no_target"], 2);
    }

    /// Pin the "never bootstrapped" vs "no data" distinction (#4787 finding 3):
    /// a node that has not reached `min_connections` must emit
    /// `bootstrap_completed: false` alongside the null latency, so a
    /// permanently-stuck joiner is visible instead of indistinguishable from a
    /// build that doesn't report the field.
    #[test]
    fn router_snapshot_json_distinguishes_never_bootstrapped_from_no_data() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.bootstrap_time_to_min_connections_secs = None;
        info.bootstrap_completed = Some(false);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        assert!(json["bootstrap_time_to_min_connections_secs"].is_null());
        assert_eq!(
            json["bootstrap_completed"], false,
            "a stuck joiner must report bootstrap_completed=false, not merely a \
             null latency that also means 'field not reported'"
        );
    }

    /// Pin: the computed-upstream vs. stored-flag divergence counters (piece D,
    /// #4642 / #4671) must also reach the hand-mirrored OTLP body — same footgun
    /// as the terminal-consult counters above. These give production field
    /// evidence for the stored `is_upstream` flag's drift ahead of the
    /// reconcile-core keystone deleting it; a silent drop would re-blind central
    /// telemetry to that drift — the exact gap this change closes.
    #[test]
    fn router_snapshot_json_includes_upstream_divergence_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.upstream_computed_vs_stored_comparisons = Some(337);
        info.upstream_computed_vs_stored_divergences = Some(347);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("upstream_computed_vs_stored_comparisons", 337),
            ("upstream_computed_vs_stored_divergences", 347),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the reconcile-controller SHADOW comparison counters (keystone
    /// step-2, #4642) must also reach the hand-mirrored OTLP body — same footgun
    /// as the divergence counters above. These are the production field evidence
    /// for how far the pure `reconcile` controller diverges from today's
    /// scattered decisions before the flip; a silent drop would re-blind central
    /// telemetry to the exact signal this change adds.
    #[test]
    fn router_snapshot_json_includes_reconcile_shadow_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.reconcile_shadow_collapse_comparisons = Some(401);
        info.reconcile_shadow_collapse_divergences = Some(409);
        info.reconcile_shadow_collapse_subscribe_diffs = Some(419);
        info.reconcile_shadow_collapse_renew_diffs = Some(421);
        info.reconcile_shadow_collapse_unsubscribe_diffs = Some(431);
        info.reconcile_shadow_collapse_collapse_diffs = Some(433);
        info.reconcile_shadow_collapse_announce_diffs = Some(439);
        info.reconcile_shadow_collapse_retract_diffs = Some(443);
        info.reconcile_shadow_collapse_reroot_search_diffs = Some(449);
        info.reconcile_shadow_renewal_comparisons = Some(457);
        info.reconcile_shadow_renewal_divergences = Some(461);
        info.reconcile_shadow_renewal_subscribe_diffs = Some(463);
        info.reconcile_shadow_renewal_renew_diffs = Some(467);
        info.reconcile_shadow_renewal_unsubscribe_diffs = Some(479);
        info.reconcile_shadow_renewal_collapse_diffs = Some(487);
        info.reconcile_shadow_renewal_announce_diffs = Some(491);
        info.reconcile_shadow_renewal_retract_diffs = Some(499);
        info.reconcile_shadow_renewal_reroot_search_diffs = Some(503);
        info.reconcile_shadow_inbound_unsubscribe_comparisons = Some(509);
        info.reconcile_shadow_inbound_unsubscribe_divergences = Some(521);
        info.reconcile_shadow_connection_drop_comparisons = Some(523);
        info.reconcile_shadow_connection_drop_divergences = Some(541);
        info.reconcile_shadow_host_formation_comparisons = Some(547);
        info.reconcile_shadow_host_formation_divergences = Some(557);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("reconcile_shadow_collapse_comparisons", 401),
            ("reconcile_shadow_collapse_divergences", 409),
            ("reconcile_shadow_collapse_subscribe_diffs", 419),
            ("reconcile_shadow_collapse_renew_diffs", 421),
            ("reconcile_shadow_collapse_unsubscribe_diffs", 431),
            ("reconcile_shadow_collapse_collapse_diffs", 433),
            ("reconcile_shadow_collapse_announce_diffs", 439),
            ("reconcile_shadow_collapse_retract_diffs", 443),
            ("reconcile_shadow_collapse_reroot_search_diffs", 449),
            ("reconcile_shadow_renewal_comparisons", 457),
            ("reconcile_shadow_renewal_divergences", 461),
            ("reconcile_shadow_renewal_subscribe_diffs", 463),
            ("reconcile_shadow_renewal_renew_diffs", 467),
            ("reconcile_shadow_renewal_unsubscribe_diffs", 479),
            ("reconcile_shadow_renewal_collapse_diffs", 487),
            ("reconcile_shadow_renewal_announce_diffs", 491),
            ("reconcile_shadow_renewal_retract_diffs", 499),
            ("reconcile_shadow_renewal_reroot_search_diffs", 503),
            ("reconcile_shadow_inbound_unsubscribe_comparisons", 509),
            ("reconcile_shadow_inbound_unsubscribe_divergences", 521),
            ("reconcile_shadow_connection_drop_comparisons", 523),
            ("reconcile_shadow_connection_drop_divergences", 541),
            ("reconcile_shadow_host_formation_comparisons", 547),
            ("reconcile_shadow_host_formation_divergences", 557),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the interest-weighted (two-tier) module-cache SHADOW gauges
    /// (#4441/#4534) must also reach the hand-mirrored OTLP body — same footgun
    /// as the gauges above. These are always-on (flag-independent) and are the
    /// production signal for whether flipping the
    /// `FREENET_MODULE_CACHE_INTEREST_TIERED` flag (and the later #4534
    /// admission-gate change) would help, so a silent drop here would defeat the
    /// whole shadow-metrics purpose of this change.
    #[test]
    fn router_snapshot_json_includes_interest_tier_shadow_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.contract_module_cache_cold_evictable_bytes = Some(101);
        info.contract_module_cache_interested_bytes = Some(103);
        info.contract_module_cache_evictions_would_reclassify_total = Some(107);
        info.migration_admission_recovered_total = Some(109);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("contract_module_cache_cold_evictable_bytes", 101),
            ("contract_module_cache_interested_bytes", 103),
            (
                "contract_module_cache_evictions_would_reclassify_total",
                107,
            ),
            ("migration_admission_recovered_total", 109),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// Pin: the UPDATE-broadcast stream-assembly gauges must also reach the
    /// hand-mirrored OTLP body — same footgun as the fd / module-cache gauges
    /// above. The broadcast stream-assembly failure rate is the signal that
    /// flagged the v0.2.73 incident, so a silent drop here would re-blind us to a
    /// re-enable going wrong.
    ///
    /// This used to cover the #4440 background-task health gauges too. Both are
    /// now gone, each with the thing it measured:
    ///
    /// - `refresh_router_consecutive_failures` went in the #4808 follow-up with
    ///   the startup routing-history restore — the task's last fallible call, so
    ///   the counter had no failure source left and was pinned at 0 forever.
    /// - `refresh_router_last_success_age_secs` went in #4811 with
    ///   `Ring::refit_router_periodically` itself. It was a liveness heartbeat for
    ///   that loop, and only for it: it existed because `BackgroundTaskMonitor`
    ///   watches for task *death* only, leaving "alive but starved" invisible.
    ///   With the refit moved onto `IsotonicEstimator::add_event` there is no loop
    ///   to starve — a stalled refit now means a stalled relay path, which is
    ///   loudly visible in the operation telemetry rather than needing a
    ///   dedicated gauge.
    #[test]
    fn router_snapshot_json_includes_broadcast_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.broadcast_stream_attempts_total = Some(37);
        info.broadcast_stream_failures_total = Some(41);
        info.broadcast_stream_failures_last_snapshot = Some(43);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("broadcast_stream_attempts_total", 37),
            ("broadcast_stream_failures_total", 41),
            ("broadcast_stream_failures_last_snapshot", 43),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// The contract term's activation counters must reach the OTLP body, or
    /// the live safety watch cannot tell a term that worked from one that
    /// never activated. Distinct values per field, so mirroring one field's
    /// value under another field's key fails too.
    ///
    /// This covers the counters the COLLECTOR needs, which is NOT the whole
    /// set the snapshot carries. `_residuals_refused`,
    /// `_pairs_refused_last_refit` and `_entries_displaced` describe table
    /// SATURATION on one node; they are not exported, and no dashboard page
    /// shows them since the peer page stopped carrying model diagnostics
    /// (#5794), so they are read only from the snapshot itself.
    /// `_den_below_two_refits` is shown on `/routing` and is derivable from
    /// the two exported here (`_estimable_refits` minus the refits that could
    /// act). If a fleet-wide question ever needs one of them, add it to the
    /// mirrored block above and to this list together.
    /// `route_events_discarded_unlocated` (route events the router discarded
    /// for a peer with no location) is likewise not exported: it is a
    /// should-be-zero invariant counter read on one node's `/routing` page.
    #[test]
    fn router_snapshot_json_includes_contract_term_activation() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hierarchical_contract_effects_applied = 201;
        info.hierarchical_contract_forecast_offsets = 202;
        info.hierarchical_contract_estimable_refits = 203;
        info.hierarchical_contract_qualifying_contracts = 204;
        info.hierarchical_contract_floor_bound_refits = 206;
        info.hierarchical_contracts = 205;
        info.hierarchical_contract_tau2 = Some(0.25);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hierarchical_contract_effects_applied", 201),
            ("hierarchical_contract_forecast_offsets", 202),
            ("hierarchical_contract_estimable_refits", 203),
            ("hierarchical_contract_qualifying_contracts", 204),
            ("hierarchical_contract_floor_bound_refits", 206),
            ("hierarchical_contracts", 205),
        ] {
            assert_eq!(
                json.get(key).and_then(|v| v.as_u64()),
                Some(want),
                "{key} must reach the OTLP body"
            );
        }
        assert_eq!(
            json.get("hierarchical_contract_tau2")
                .and_then(|v| v.as_f64()),
            Some(0.25),
            "hierarchical_contract_tau2 must reach the OTLP body"
        );
    }

    /// The contract-exec WASM counters must reach the hand-mirrored OTLP body.
    ///
    /// These are the fields that make a summarize/delta rate interpretable —
    /// without them, the only production signal is a handler-entry span that
    /// counts cache hits and WASM invocations identically, which is how five
    /// consecutive storm fixes were sized against an undifferentiated number.
    /// Losing one to the hand-mirroring footgun would re-blind us in exactly the
    /// way this change exists to fix, so the assertion covers the FULL set: a
    /// partially-mirrored addition fails here rather than shipping half-visible.
    ///
    /// Every value below is DISTINCT, so a copy-paste slip that mirrors one
    /// field's value under another field's key fails too — an all-`Some(1)`
    /// fixture would pass under that mutation.
    #[test]
    fn router_snapshot_json_includes_contract_exec_counters() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.contract_exec_summarize_fast_hits_total = Some(101);
        info.contract_exec_summarize_reload_hits_total = Some(102);
        info.contract_exec_summarize_wasm_calls_total = Some(103);
        info.contract_exec_summarize_wasm_uncached_total = Some(104);
        info.contract_exec_delta_fast_hits_total = Some(105);
        info.contract_exec_delta_reload_hits_total = Some(106);
        info.contract_exec_delta_wasm_calls_total = Some(107);
        info.contract_exec_delta_wasm_uncached_total = Some(108);
        info.contract_exec_summarize_fast_hits_last_snapshot = Some(109);
        info.contract_exec_summarize_reload_hits_last_snapshot = Some(110);
        info.contract_exec_summarize_wasm_calls_last_snapshot = Some(111);
        info.contract_exec_summarize_wasm_uncached_last_snapshot = Some(112);
        info.contract_exec_delta_fast_hits_last_snapshot = Some(113);
        info.contract_exec_delta_reload_hits_last_snapshot = Some(114);
        info.contract_exec_delta_wasm_calls_last_snapshot = Some(115);
        info.contract_exec_delta_wasm_uncached_last_snapshot = Some(116);
        info.contract_summary_cache_entries = Some(117);
        info.contract_summary_cache_bytes = Some(118);
        info.contract_summary_cache_budget_bytes = Some(119);
        info.contract_summary_cache_count_cap = Some(120);
        info.contract_summary_cache_count_cap_evictions_total = Some(121);
        info.contract_summary_cache_byte_budget_evictions_total = Some(122);
        info.contract_delta_cache_entries = Some(123);
        info.contract_delta_cache_bytes = Some(124);
        info.contract_delta_cache_budget_bytes = Some(125);
        info.contract_delta_cache_count_cap = Some(126);
        info.contract_delta_cache_count_cap_evictions_total = Some(127);
        info.contract_delta_cache_byte_budget_evictions_total = Some(128);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("contract_exec_summarize_fast_hits_total", 101),
            ("contract_exec_summarize_reload_hits_total", 102),
            ("contract_exec_summarize_wasm_calls_total", 103),
            ("contract_exec_summarize_wasm_uncached_total", 104),
            ("contract_exec_delta_fast_hits_total", 105),
            ("contract_exec_delta_reload_hits_total", 106),
            ("contract_exec_delta_wasm_calls_total", 107),
            ("contract_exec_delta_wasm_uncached_total", 108),
            ("contract_exec_summarize_fast_hits_last_snapshot", 109),
            ("contract_exec_summarize_reload_hits_last_snapshot", 110),
            ("contract_exec_summarize_wasm_calls_last_snapshot", 111),
            ("contract_exec_summarize_wasm_uncached_last_snapshot", 112),
            ("contract_exec_delta_fast_hits_last_snapshot", 113),
            ("contract_exec_delta_reload_hits_last_snapshot", 114),
            ("contract_exec_delta_wasm_calls_last_snapshot", 115),
            ("contract_exec_delta_wasm_uncached_last_snapshot", 116),
            ("contract_summary_cache_entries", 117),
            ("contract_summary_cache_bytes", 118),
            ("contract_summary_cache_budget_bytes", 119),
            ("contract_summary_cache_count_cap", 120),
            ("contract_summary_cache_count_cap_evictions_total", 121),
            ("contract_summary_cache_byte_budget_evictions_total", 122),
            ("contract_delta_cache_entries", 123),
            ("contract_delta_cache_bytes", 124),
            ("contract_delta_cache_budget_bytes", 125),
            ("contract_delta_cache_count_cap", 126),
            ("contract_delta_cache_count_cap_evictions_total", 127),
            ("contract_delta_cache_byte_budget_evictions_total", 128),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
    }

    /// The placement-quality + placement-migration gauges (#4404 follow-up) must
    /// reach the hand-mirrored OTLP `json!` body, same footgun as the gauges
    /// above: a new `RouterSnapshotInfo` field is invisible to the collector
    /// unless it is added to that block. These fields are exactly how we observe
    /// whether the placement migration pulls hosting closer to each contract's
    /// key, so a silent drop would re-blind us to it.
    #[test]
    fn router_snapshot_json_includes_placement_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.hosted_contracts_count = Some(5);
        info.hosted_key_distance_median = Some(0.1);
        info.hosted_key_distance_p90 = Some(0.4);
        info.hosted_key_distance_min = Some(0.0);
        info.hosted_key_distance_mean = Some(0.15);
        info.hosted_key_distance_frac_within_0_1 = Some(0.6);
        info.subscribe_hint_sent = Some(11);
        info.subscribe_hint_received = Some(13);
        info.subscribe_hint_acted = Some(7);
        info.renewal_terminus_satisfied = Some(9);
        info.subscribe_hint_refused_version = Some(21);
        info.subscribe_hint_refused_already_hosting = Some(22);
        info.subscribe_hint_refused_holder = Some(23);
        info.subscribe_hint_refused_cache = Some(24);
        info.subscribe_hint_acted_succeeded = Some(25);
        info.subscribe_hint_acted_failed = Some(26);
        // Nearest-neighbor ring-lattice gauges (#4760 / #4642): successor held,
        // predecessor unheld, so the two boolean states are both exercised.
        info.lattice_has_successor = Some(true);
        info.lattice_has_predecessor = Some(false);
        info.lattice_successor_distance = Some(0.05);
        info.lattice_predecessor_distance = Some(0.07);
        info.lattice_probes_issued = Some(31);
        info.lattice_probe_improvements = Some(17);
        info.lattice_probe_misses = Some(19);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("hosted_contracts_count", 5u64),
            ("subscribe_hint_sent", 11),
            ("subscribe_hint_received", 13),
            ("subscribe_hint_acted", 7),
            ("renewal_terminus_satisfied", 9),
            ("subscribe_hint_refused_version", 21),
            ("subscribe_hint_refused_already_hosting", 22),
            ("subscribe_hint_refused_holder", 23),
            ("subscribe_hint_refused_cache", 24),
            ("subscribe_hint_acted_succeeded", 25),
            ("subscribe_hint_acted_failed", 26),
            ("lattice_probes_issued", 31),
            ("lattice_probe_improvements", 17),
            ("lattice_probe_misses", 19),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
        for (key, want) in [
            ("hosted_key_distance_median", 0.1),
            ("hosted_key_distance_p90", 0.4),
            ("hosted_key_distance_min", 0.0),
            ("hosted_key_distance_mean", 0.15),
            ("hosted_key_distance_frac_within_0_1", 0.6),
            ("lattice_successor_distance", 0.05),
            ("lattice_predecessor_distance", 0.07),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
        // Boolean lattice completeness flags reach the body with both states
        // distinguishable from `null` (unpopulated).
        assert_eq!(
            json["lattice_has_successor"], true,
            "lattice_has_successor must reach the OTLP body"
        );
        assert_eq!(
            json["lattice_has_predecessor"], false,
            "lattice_has_predecessor must reach the OTLP body"
        );
    }

    #[test]
    fn test_backoff_calculation() {
        let mut backoff_ms: u64 = 0;

        // Initial backoff
        backoff_ms = if backoff_ms == 0 {
            INITIAL_BACKOFF_MS
        } else {
            (backoff_ms * 2).min(MAX_BACKOFF_MS)
        };
        assert_eq!(backoff_ms, 1000);

        // Exponential growth
        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
        assert_eq!(backoff_ms, 2000);

        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
        assert_eq!(backoff_ms, 4000);

        // Test max cap
        backoff_ms = MAX_BACKOFF_MS + 1000;
        backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
        assert_eq!(backoff_ms, MAX_BACKOFF_MS);
    }

    #[test]
    fn test_event_kind_to_string() {
        // Test ignored event (simplest case that doesn't require PeerId construction)
        let ignored = EventKind::Ignored;
        assert_eq!(event_kind_to_string(&ignored), "ignored");
    }

    #[test]
    fn test_event_kind_to_string_interest_sync() {
        use crate::ring::PeerKeyLocation;
        use crate::tracing::InterestSyncEvent;
        use freenet_stdlib::prelude::{ContractCode, ContractKey, Parameters};

        // Create test fixtures
        let code = ContractCode::from(vec![1, 2, 3, 4]);
        let params = Parameters::from(vec![5, 6, 7, 8]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let peer = PeerKeyLocation::random();

        // Test ResyncRequestReceived
        let event = EventKind::InterestSync(InterestSyncEvent::ResyncRequestReceived {
            key,
            from_peer: peer.clone(),
            timestamp: 12345,
        });
        assert_eq!(
            event_kind_to_string(&event),
            "interest_resync_request_received"
        );

        // Test ResyncResponseSent
        let event = EventKind::InterestSync(InterestSyncEvent::ResyncResponseSent {
            key,
            to_peer: peer,
            state_size: 1024,
            timestamp: 12345,
        });
        assert_eq!(
            event_kind_to_string(&event),
            "interest_resync_response_sent"
        );
    }

    #[test]
    fn test_event_kind_to_json_interest_sync() {
        use crate::ring::PeerKeyLocation;
        use crate::tracing::InterestSyncEvent;
        use freenet_stdlib::prelude::{ContractCode, ContractKey, Parameters};

        // Create test fixtures
        let code = ContractCode::from(vec![1, 2, 3, 4]);
        let params = Parameters::from(vec![5, 6, 7, 8]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let peer = PeerKeyLocation::random();

        // Test ResyncRequestReceived JSON structure
        let event = EventKind::InterestSync(InterestSyncEvent::ResyncRequestReceived {
            key,
            from_peer: peer.clone(),
            timestamp: 12345,
        });
        let json = event_kind_to_json(&event);
        assert_eq!(json["type"], "resync_request_received");
        assert!(json["contract_key"].is_string());
        assert!(json["contract_id"].is_string());
        assert!(json["from_peer"].is_string());
        assert_eq!(json["timestamp"], 12345);

        // Test ResyncResponseSent JSON structure
        let event = EventKind::InterestSync(InterestSyncEvent::ResyncResponseSent {
            key,
            to_peer: peer,
            state_size: 1024,
            timestamp: 67890,
        });
        let json = event_kind_to_json(&event);
        assert_eq!(json["type"], "resync_response_sent");
        assert!(json["contract_key"].is_string());
        assert!(json["contract_id"].is_string());
        assert!(json["to_peer"].is_string());
        assert_eq!(json["state_size"], 1024);
        assert_eq!(json["timestamp"], 67890);
    }

    #[test]
    fn test_event_kind_to_json_timeout_enriched() {
        use crate::message::Transaction;

        let tx = Transaction::new::<crate::operations::connect::ConnectMsg>();
        let event = EventKind::Timeout {
            id: tx,
            timestamp: 99999,
            op_type: "get".to_string(),
            target_peer: Some("peer-abc-123".to_string()),
        };

        let json = event_kind_to_json(&event);
        assert_eq!(json["type"], "timeout");
        assert_eq!(json["timestamp"], 99999);
        assert_eq!(json["op_type"], "get");
        assert_eq!(json["target_peer"], "peer-abc-123");

        // Test with no target peer
        let event_no_peer = EventKind::Timeout {
            id: tx,
            timestamp: 88888,
            op_type: "put".to_string(),
            target_peer: None,
        };
        let json2 = event_kind_to_json(&event_no_peer);
        assert_eq!(json2["op_type"], "put");
        assert!(json2["target_peer"].is_null());
    }

    #[test]
    fn test_send_standalone_event_no_panic_without_init() {
        // When telemetry is not initialized, send_standalone_event should silently drop
        send_standalone_event(
            "subscription_renewal_outcome",
            serde_json::json!({
                "contract": "test-contract",
                "outcome": "success",
                "error": null,
            }),
        );
        // No panic = success
    }

    #[test]
    fn test_send_standalone_event_with_peer_id_no_panic_without_init() {
        // Mirror of the above for the peer-id-tagged variant: must
        // silently drop when telemetry is not initialized.
        send_standalone_event_with_peer_id(
            "shadow_rtt_aggregate",
            "test-peer-id-abc",
            serde_json::json!({"active_peers": 0}),
        );
        // No panic = success
    }

    #[test]
    fn test_otlp_serialization_carries_peer_id_attribute() {
        // Direct unit test on the serialization path: a TelemetryEvent
        // constructed with a non-empty peer_id must surface that id in
        // the OTLP `peer_id` attribute. Without this pin, a future
        // refactor that drops the attribute mapping would silently
        // anonymize every event reaching the collector.
        let event = TelemetryEvent {
            timestamp: 1_700_000_000_000,
            peer_id: "test-peer-id-xyz".to_string(),
            transaction_id: String::new(),
            event_type: "shadow_rtt_aggregate".to_string(),
            event_data: serde_json::json!({"active_peers": 3}),
            priority: EventPriority::Periodic,
        };
        let otlp = to_otlp_logs(std::slice::from_ref(&event));
        let attrs = &otlp["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"];
        let peer_attr = attrs
            .as_array()
            .expect("attributes must be an array")
            .iter()
            .find(|a| a["key"] == "peer_id")
            .expect("peer_id attribute must be present");
        assert_eq!(peer_attr["value"]["stringValue"], "test-peer-id-xyz");
    }

    #[test]
    fn test_transport_snapshot_carries_local_peer_id() {
        // #4345 observability gap, same class as transfer_failed:
        // transport_snapshot events carried an empty peer_id. The
        // worker must stamp its own peer id on snapshot events.
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let (_transfer_tx, transfer_rx) = mpsc::channel(1);
        let worker = TelemetryWorker::new(
            "http://localhost:4318".to_string(),
            cmd_rx,
            0,
            transfer_rx,
            "snapshot-peer-id".to_string(),
            Arc::new(TelemetryLocalMetrics::default()),
        );
        let snapshot = crate::transport::metrics::TransportSnapshot::default();
        let event = worker.snapshot_to_telemetry(&snapshot);
        assert_eq!(event.event_type, "transport_snapshot");
        let sent_identity_keys: Vec<&String> = event
            .event_data
            .as_object()
            .expect("snapshot JSON object")
            .keys()
            .filter(|key| key.starts_with("conn_identity_"))
            .collect();
        assert!(
            sent_identity_keys.is_empty(),
            "zero connection identity counts are not uploaded: {sent_identity_keys:?}"
        );
        let event = worker.snapshot_to_telemetry(&crate::transport::metrics::TransportSnapshot {
            conn_identity_duplicate_key: 3,
            ..Default::default()
        });
        let sent_identity_keys: Vec<&String> = event
            .event_data
            .as_object()
            .expect("snapshot JSON object")
            .keys()
            .filter(|key| key.starts_with("conn_identity_"))
            .collect();
        assert_eq!(sent_identity_keys, vec!["conn_identity_duplicate_key"]);
        assert_eq!(event.event_data["conn_identity_duplicate_key"], 3);
        let concurrency_keys = |event: &TelemetryEvent| -> Vec<String> {
            event
                .event_data
                .as_object()
                .expect("snapshot JSON object")
                .keys()
                .filter(|key| key.starts_with("inbound_concurrency_"))
                .cloned()
                .collect()
        };
        assert!(
            concurrency_keys(&event).is_empty(),
            "zero inbound concurrency values are not uploaded"
        );
        let event = worker.snapshot_to_telemetry(&crate::transport::metrics::TransportSnapshot {
            inbound_concurrency_conns_sampled: 4,
            inbound_concurrency_max: 5,
            ..Default::default()
        });
        assert_eq!(
            concurrency_keys(&event),
            vec![
                "inbound_concurrency_conns_sampled",
                "inbound_concurrency_max"
            ]
        );
        assert_eq!(event.event_data["inbound_concurrency_conns_sampled"], 4);
        assert_eq!(event.event_data["inbound_concurrency_max"], 5);
        assert_eq!(
            event.peer_id, "snapshot-peer-id",
            "transport snapshots must be attributed to the emitting \
             node's own peer id (#4345)"
        );
    }

    #[test]
    fn test_resource_utilization_carries_local_peer_id() {
        // #4642 A1: node self-resource-utilization events must be attributed
        // to the emitting node's own peer id, tagged with the stable
        // `resource_utilization` event_type at Operational priority, and carry
        // the sampled axes as a per-node aggregate-scalar object (never a
        // per-contract / per-request map). Same attribution contract as the
        // transport_snapshot / transfer siblings above.
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let (_transfer_tx, transfer_rx) = mpsc::channel(1);
        let worker = TelemetryWorker::new(
            "http://localhost:4318".to_string(),
            cmd_rx,
            0,
            transfer_rx,
            "resource-peer-id".to_string(),
            Arc::new(TelemetryLocalMetrics::default()),
        );
        let event = worker.resource_utilization_telemetry();
        assert_eq!(event.event_type, "resource_utilization");
        assert_eq!(
            event.peer_id, "resource-peer-id",
            "resource-utilization events must be attributed to the emitting \
             node's own peer id"
        );
        assert_eq!(
            event.priority,
            EventPriority::Periodic,
            "resource-utilization is a fixed-cadence periodic stream, so it \
             bypasses the operational cap (#6001)"
        );
        let obj = event
            .event_data
            .as_object()
            .expect("resource_utilization event_data must be a JSON object");
        for key in [
            "memory_rss_bytes",
            "memory_limit_bytes",
            "cpu_time_seconds",
            "cumulative_bytes_sent",
            "cumulative_bytes_received",
        ] {
            assert!(obj.contains_key(key), "event_data missing key `{key}`");
        }
    }

    #[test]
    fn test_resource_utilization_emits_once_per_six_batch_ticks() {
        // #4642 A1 cadence: resource_utilization is a slowly-changing,
        // node-wide signal that every peer reports to the central collector,
        // so it emits on a reduced ~60s cadence (once every
        // RESOURCE_TELEMETRY_BATCH_INTERVAL = 6 batch ticks of
        // BATCH_INTERVAL_SECS = 10s each) rather than on every 10s batch tick.
        // Guards against a regression that would restore the wasteful 10s
        // cadence.
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let (_transfer_tx, transfer_rx) = mpsc::channel(1);
        let mut worker = TelemetryWorker::new(
            "http://localhost:4318".to_string(),
            cmd_rx,
            0,
            transfer_rx,
            "cadence-peer-id".to_string(),
            Arc::new(TelemetryLocalMetrics::default()),
        );

        // The constant yields the intended ~60s cadence.
        assert_eq!(
            RESOURCE_TELEMETRY_BATCH_INTERVAL * BATCH_INTERVAL_SECS,
            60,
            "resource telemetry should emit ~once per 60s"
        );

        // Over two full cycles, only the ticks at multiples of the interval
        // (0, 6, 12, ...) emit: exactly one emit per six batch ticks. The
        // first tick emits for prompt startup visibility.
        let total_ticks = RESOURCE_TELEMETRY_BATCH_INTERVAL * 2;
        let mut emits = 0usize;
        for tick in 0..total_ticks {
            let emitted = worker.should_emit_resource_utilization();
            let expected = tick % RESOURCE_TELEMETRY_BATCH_INTERVAL == 0;
            assert_eq!(
                emitted, expected,
                "batch tick {tick}: resource_utilization emit should be {expected}"
            );
            if emitted {
                emits += 1;
            }
        }
        assert_eq!(
            emits, 2,
            "expected exactly one resource_utilization emit per \
             {RESOURCE_TELEMETRY_BATCH_INTERVAL} batch ticks over two cycles"
        );
    }

    #[tokio::test]
    async fn test_timeout_events_carry_local_peer_id() {
        // #4345 observability gap, same class as transfer_failed:
        // timeout events carried an empty peer_id.
        use crate::message::Transaction;
        use crate::operations::get::GetMsg;

        let (sender, mut receiver) = mpsc::channel(1);
        let (network_efficiency_sender, _network_efficiency_receiver) = mpsc::channel(1);
        let metrics = Arc::new(TelemetryLocalMetrics::default());
        let mut reporter = TelemetryReporter {
            sender,
            network_efficiency_sender,
            metrics,
            local_peer_id: "timeout-peer-id".to_string(),
        };
        let tx = Transaction::new::<GetMsg>();
        reporter
            .notify_of_time_out(tx, "get", Some("target-peer".to_string()))
            .await;
        let TelemetryCommand::Event(event) =
            receiver.try_recv().expect("timeout event must be sent")
        else {
            panic!("expected an Event command");
        };
        assert_eq!(event.event_type, "timeout");
        assert_eq!(
            event.peer_id, "timeout-peer-id",
            "timeout events must be attributed to the emitting node's \
             own peer id (#4345)"
        );
    }

    #[test]
    fn test_otlp_resource_includes_service_version() {
        // #4345 observability gap: every OTLP export must carry the
        // sender's crate version as a standard resource attribute, so
        // collector queries can attribute events (transfer_failed in
        // particular) to a release without joining against
        // peer_startup events.
        let event = TelemetryEvent {
            timestamp: 1_700_000_000_000,
            peer_id: "some-peer".to_string(),
            transaction_id: String::new(),
            event_type: "transfer_failed".to_string(),
            event_data: serde_json::json!({}),
            priority: EventPriority::Operational,
        };
        let otlp = to_otlp_logs(std::slice::from_ref(&event));
        let resource_attrs = &otlp["resourceLogs"][0]["resource"]["attributes"];
        let version_attr = resource_attrs
            .as_array()
            .expect("resource attributes must be an array")
            .iter()
            .find(|a| a["key"] == "service.version")
            .expect("service.version resource attribute must be present");
        assert_eq!(
            version_attr["value"]["stringValue"],
            env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    fn test_transfer_events_carry_local_peer_id() {
        // #4345 observability gap: transfer_failed events carried an
        // empty peer_id, making sender attribution in the collector
        // impossible. The worker must stamp its own peer id on every
        // transport-level transfer event.
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let (_transfer_tx, transfer_rx) = mpsc::channel(1);
        let worker = TelemetryWorker::new(
            "http://localhost:4318".to_string(),
            cmd_rx,
            0,
            transfer_rx,
            "test-local-peer-id".to_string(),
            Arc::new(TelemetryLocalMetrics::default()),
        );
        let event = worker.transfer_event_to_telemetry(crate::tracing::TransferEvent::Failed {
            stream_id: 42,
            peer_addr: "127.0.0.1:31337".parse().unwrap(),
            bytes_transferred: 1024,
            reason: "cwnd wait timeout".to_string(),
            elapsed_ms: 3000,
            direction: crate::tracing::TransferDirection::Send,
            timestamp: 1_700_000_000_000,
        });
        assert_eq!(event.event_type, "transfer_failed");
        assert_eq!(
            event.peer_id, "test-local-peer-id",
            "transfer events must be attributed to the emitting node's \
             own peer id (#4345)"
        );
    }

    #[test]
    fn test_otlp_serialization_empty_peer_id_when_unset() {
        // Pin the back-compat path: events sent via the original
        // `send_standalone_event` have an empty peer_id. The OTLP
        // attribute is still present (so the field exists in the
        // collector schema) but contains an empty string.
        let event = TelemetryEvent {
            timestamp: 1_700_000_000_000,
            peer_id: String::new(),
            transaction_id: String::new(),
            event_type: "other_event".to_string(),
            event_data: serde_json::json!({}),
            priority: EventPriority::Operational,
        };
        let otlp = to_otlp_logs(std::slice::from_ref(&event));
        let attrs = &otlp["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"];
        let peer_attr = attrs
            .as_array()
            .expect("attributes must be an array")
            .iter()
            .find(|a| a["key"] == "peer_id")
            .expect("peer_id attribute must be present even when unset");
        assert_eq!(peer_attr["value"]["stringValue"], "");
    }

    #[test]
    fn test_event_kind_to_json_connect_rejected() {
        use crate::ring::Location;
        use crate::tracing::ConnectEvent;

        let loc = Location::new(0.42);
        let event = EventKind::Connect(ConnectEvent::Rejected {
            desired_location: loc,
            reason: "ring full".to_string(),
        });

        // Verify event_kind_to_string
        assert_eq!(event_kind_to_string(&event), "connect_rejected");

        // Verify JSON structure
        let json = event_kind_to_json(&event);
        assert_eq!(json["type"], "rejected");
        assert!((json["desired_location"].as_f64().unwrap() - 0.42).abs() < 1e-6);
        assert_eq!(json["reason"], "ring full");
    }

    #[test]
    fn test_event_kind_to_json_update_failure() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{OperationFailure, UpdateEvent};
        use freenet_stdlib::prelude::{ContractCode, ContractKey, Parameters};

        let tx = Transaction::new::<crate::operations::update::UpdateMsg>();
        let requester = PeerKeyLocation::random();
        let target = PeerKeyLocation::random();
        let code = ContractCode::from(vec![10, 20, 30]);
        let params = Parameters::from(vec![40, 50, 60]);
        let key = ContractKey::from_params_and_code(&params, &code);

        let event = EventKind::Update(UpdateEvent::UpdateFailure {
            id: tx,
            requester: requester.clone(),
            target: target.clone(),
            key,
            reason: OperationFailure::HtlExhausted,
            elapsed_ms: 1234,
            timestamp: 99999,
        });

        // Verify event_kind_to_string
        assert_eq!(event_kind_to_string(&event), "update_failure");

        // Verify JSON structure
        let json = event_kind_to_json(&event);
        assert_eq!(json["type"], "update_failure");
        assert_eq!(json["id"], tx.to_string());
        assert_eq!(json["requester"], requester.to_string());
        assert_eq!(json["target"], target.to_string());
        assert!(json["key"].is_string());
        assert_eq!(json["reason"], "htl_exhausted");
        assert_eq!(json["elapsed_ms"], 1234);
        assert_eq!(json["timestamp"], 99999);
    }

    /// Core-bug regression guard: a streaming (> 64 KB) GET success must
    /// reach the OTLP body as `get_terminal` with `outcome=success` and
    /// `streamed=true`. This is the exact event the inline `get_success`
    /// path drops (streaming replies arrive as `ResponseStreaming`, which
    /// `register.rs` maps to `Ignored`), so without this the findability of
    /// River rooms / UI-container contracts (all > 64 KB) is unmeasurable.
    #[test]
    fn test_event_kind_to_json_get_terminal_streaming_success() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{GetEvent, GetTerminalOutcome};
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

        let tx = Transaction::new::<crate::operations::get::GetMsg>();
        let requester = PeerKeyLocation::random();
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        );
        let instance_id = ContractInstanceId::new([9u8; 32]);

        let event = EventKind::Get(GetEvent::ClientTerminal {
            id: tx,
            requester: requester.clone(),
            instance_id,
            key: Some(key),
            outcome: GetTerminalOutcome::Success,
            streamed: true,
            is_sub_op: false,
            attempts: 2,
            hop_count: Some(3),
            fragments_received: Some(8),
            total_fragments: Some(8),
            stream_abort_cause: None,
            exhaustion_reason: None,
            elapsed_ms: 1234,
            timestamp: 99999,
        });

        // Metric name the central collector segments on.
        assert_eq!(event_kind_to_string(&event), "get_terminal");

        let json = event_kind_to_json(&event);
        assert_eq!(json["type"], "get_terminal");
        assert_eq!(json["id"], tx.to_string());
        assert_eq!(json["requester"], requester.to_string());
        assert_eq!(json["instance_id"], instance_id.to_string());
        assert_eq!(json["outcome"], "success");
        assert_eq!(json["streamed"], true);
        assert_eq!(json["is_sub_op"], false);
        assert_eq!(json["attempts"], 2);
        assert_eq!(json["hop_count"], 3);
        assert_eq!(json["elapsed_ms"], 1234);
        assert_eq!(json["timestamp"], 99999);
        assert!(json["key"].is_string());
        // Streaming success carries fragment counts and NO abort cause.
        assert_eq!(json["fragments_received"], 8);
        assert_eq!(json["total_fragments"], 8);
        assert!(json.get("stream_abort_cause").is_none());
    }

    /// A streamed GET FAILURE carries the classified abort cause and the
    /// fragment progress of the last assembly attempt — the WHERE signal that
    /// isolates the large-contract failure class (Group B). Streaming outcomes
    /// flow exclusively through `ClientTerminal`, so this is the only terminal
    /// event that can carry them.
    #[test]
    fn test_event_kind_to_json_get_terminal_streaming_failure_abort_cause() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{GetEvent, GetTerminalOutcome, StreamAbortCause};
        use freenet_stdlib::prelude::ContractInstanceId;

        let tx = Transaction::new::<crate::operations::get::GetMsg>();
        let instance_id = ContractInstanceId::new([4u8; 32]);

        let event = EventKind::Get(GetEvent::ClientTerminal {
            id: tx,
            requester: PeerKeyLocation::random(),
            instance_id,
            key: None,
            outcome: GetTerminalOutcome::TimeoutExhausted,
            streamed: true,
            is_sub_op: false,
            attempts: 3,
            hop_count: None,
            fragments_received: Some(3),
            total_fragments: Some(10),
            stream_abort_cause: Some(StreamAbortCause::InactivityTimeout),
            exhaustion_reason: None,
            elapsed_ms: 4321,
            timestamp: 7,
        });

        assert_eq!(event_kind_to_string(&event), "get_terminal");
        let json = event_kind_to_json(&event);
        assert_eq!(json["outcome"], "timeout_exhausted");
        assert_eq!(json["streamed"], true);
        assert_eq!(json["fragments_received"], 3);
        assert_eq!(json["total_fragments"], 10);
        assert_eq!(json["stream_abort_cause"], "inactivity_timeout");
    }

    /// A timeout-exhausted client GET now emits a `get_terminal` event with
    /// `outcome=timeout_exhausted` (previously this path returned NotFound
    /// to the client with NO telemetry event at all). No key on this path.
    #[test]
    fn test_event_kind_to_json_get_terminal_timeout_exhausted() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{GetEvent, GetTerminalOutcome};
        use freenet_stdlib::prelude::ContractInstanceId;

        let tx = Transaction::new::<crate::operations::get::GetMsg>();
        let requester = PeerKeyLocation::random();
        let instance_id = ContractInstanceId::new([7u8; 32]);

        let event = EventKind::Get(GetEvent::ClientTerminal {
            id: tx,
            requester,
            instance_id,
            key: None,
            outcome: GetTerminalOutcome::TimeoutExhausted,
            streamed: false,
            is_sub_op: false,
            attempts: 3,
            hop_count: None,
            fragments_received: None,
            total_fragments: None,
            stream_abort_cause: None,
            exhaustion_reason: None,
            elapsed_ms: 60000,
            timestamp: 1,
        });

        assert_eq!(event_kind_to_string(&event), "get_terminal");
        let json = event_kind_to_json(&event);
        assert_eq!(json["outcome"], "timeout_exhausted");
        assert_eq!(json["streamed"], false);
        // No key and no hop_count on the timeout path — both serialize null.
        assert!(json["key"].is_null());
        assert!(json["hop_count"].is_null());
        // Non-streaming terminal omits the streaming-only fields entirely.
        assert!(json.get("fragments_received").is_none());
        assert!(json.get("total_fragments").is_none());
        assert!(json.get("stream_abort_cause").is_none());
    }

    /// #5252: a retry-loop exhaustion carries WHY it gave up — out of
    /// distinct routing candidates, vs. having hit the retry budget with
    /// candidates still available. Before this, that distinction existed
    /// only in a `debug!` log line compiled out of release builds
    /// (`release_max_level_info`) and never wired to an `EventKind` at all.
    #[test]
    fn test_event_kind_to_json_get_terminal_exhaustion_reason() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{GetEvent, GetExhaustionReason, GetTerminalOutcome};
        use freenet_stdlib::prelude::ContractInstanceId;

        let tx = Transaction::new::<crate::operations::get::GetMsg>();
        let instance_id = ContractInstanceId::new([8u8; 32]);

        let event = EventKind::Get(GetEvent::ClientTerminal {
            id: tx,
            requester: PeerKeyLocation::random(),
            instance_id,
            key: None,
            outcome: GetTerminalOutcome::TimeoutExhausted,
            streamed: false,
            is_sub_op: false,
            attempts: 2,
            hop_count: None,
            fragments_received: None,
            total_fragments: None,
            stream_abort_cause: None,
            exhaustion_reason: Some(GetExhaustionReason::NoRoutingCandidates),
            elapsed_ms: 500,
            timestamp: 1,
        });

        assert_eq!(event_kind_to_string(&event), "get_terminal");
        let json = event_kind_to_json(&event);
        assert_eq!(json["outcome"], "timeout_exhausted");
        assert_eq!(json["exhaustion_reason"], "no_routing_candidates");
        // `attempts` is REQUESTS SENT (driver.requests_sent), NOT a peer
        // count — the infra-retry arm re-sends to the same peer without an
        // advance, so it over-reports peers. See the field's doc comment on
        // GetEvent::ClientTerminal; a retries-derived `peer_advancements`
        // field was dropped in review for the mirror-image bias.
        assert_eq!(json["attempts"], 2);
    }

    /// The addressless-candidate exhaustion is a DIFFERENT cause from an
    /// empty candidate set — the ring returned a peer, it just had no wire
    /// address (a local ring defect, `warn!`-logged) — so it must export its
    /// own string rather than collapsing into `no_routing_candidates`.
    /// Without the split, a spike of ring defects is indistinguishable from
    /// a genuine topology dead-end in the collector.
    #[test]
    fn test_event_kind_to_json_get_terminal_addressless_candidate() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{GetEvent, GetExhaustionReason, GetTerminalOutcome};
        use freenet_stdlib::prelude::ContractInstanceId;

        let tx = Transaction::new::<crate::operations::get::GetMsg>();
        let event = EventKind::Get(GetEvent::ClientTerminal {
            id: tx,
            requester: PeerKeyLocation::random(),
            instance_id: ContractInstanceId::new([9u8; 32]),
            key: None,
            outcome: GetTerminalOutcome::TimeoutExhausted,
            streamed: false,
            is_sub_op: false,
            attempts: 1,
            hop_count: None,
            fragments_received: None,
            total_fragments: None,
            stream_abort_cause: None,
            exhaustion_reason: Some(GetExhaustionReason::AddresslessCandidate),
            elapsed_ms: 500,
            timestamp: 1,
        });

        let json = event_kind_to_json(&event);
        assert_eq!(json["exhaustion_reason"], "addressless_candidate");
    }

    /// A terminal that reached a real reply (success) never carries an
    /// exhaustion reason — that field is specific to the
    /// `RetryLoopOutcome::Exhausted` path and must not leak into other JSON
    /// bodies.
    #[test]
    fn test_event_kind_to_json_get_terminal_omits_exhaustion_fields_on_success() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{GetEvent, GetTerminalOutcome};
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

        let tx = Transaction::new::<crate::operations::get::GetMsg>();
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        );

        let event = EventKind::Get(GetEvent::ClientTerminal {
            id: tx,
            requester: PeerKeyLocation::random(),
            instance_id: ContractInstanceId::new([1u8; 32]),
            key: Some(key),
            outcome: GetTerminalOutcome::Success,
            streamed: false,
            is_sub_op: false,
            attempts: 1,
            hop_count: Some(1),
            fragments_received: None,
            total_fragments: None,
            stream_abort_cause: None,
            exhaustion_reason: None,
            elapsed_ms: 5,
            timestamp: 1,
        });

        let json = event_kind_to_json(&event);
        assert!(json.get("exhaustion_reason").is_none());
    }

    /// A local-cache-hit client GET emits a `ClientTerminal` with
    /// `attempts=0` (the local-hit convention) so the findability metric
    /// covers ALL client GET successes — gateways serve many local hits,
    /// which would otherwise bias the number toward network misses.
    #[test]
    fn test_event_kind_to_json_get_terminal_local_hit() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::{GetEvent, GetTerminalOutcome};
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};

        let tx = Transaction::new::<crate::operations::get::GetMsg>();
        let key = ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        );

        let event = EventKind::Get(GetEvent::ClientTerminal {
            id: tx,
            requester: PeerKeyLocation::random(),
            instance_id: ContractInstanceId::new([1u8; 32]),
            key: Some(key),
            outcome: GetTerminalOutcome::Success,
            streamed: false,
            is_sub_op: false,
            attempts: 0,
            hop_count: None,
            fragments_received: None,
            total_fragments: None,
            stream_abort_cause: None,
            exhaustion_reason: None,
            elapsed_ms: 1,
            timestamp: 1,
        });

        assert_eq!(event_kind_to_string(&event), "get_terminal");
        let json = event_kind_to_json(&event);
        assert_eq!(json["outcome"], "success");
        assert_eq!(json["attempts"], 0);
        assert_eq!(json["streamed"], false);
        assert_eq!(json["is_sub_op"], false);
        assert!(json["key"].is_string());
    }

    // ----- rate limiting: operational cap, periodic bypass (#4380, #6001) -----

    /// Build a worker purely to exercise the admission policy. The endpoint
    /// and channels are inert; only the rate-limit counters are touched.
    fn rate_limit_test_worker() -> TelemetryWorker {
        let (_cmd_tx, cmd_rx) = mpsc::channel(1);
        let (_transfer_tx, transfer_rx) = mpsc::channel(1);
        TelemetryWorker::new(
            "http://localhost:4318".to_string(),
            cmd_rx,
            0,
            transfer_rx,
            "rate-limit-test-peer".to_string(),
            Arc::new(TelemetryLocalMetrics::default()),
        )
    }

    fn test_telemetry_event(event_type: &str, priority: EventPriority) -> TelemetryEvent {
        TelemetryEvent {
            timestamp: 1,
            peer_id: "test-peer".to_string(),
            transaction_id: String::new(),
            event_type: event_type.to_string(),
            event_data: serde_json::json!({}),
            priority,
        }
    }

    fn test_network_efficiency_event(sequence: u64) -> TelemetryEvent {
        TelemetryEvent {
            timestamp: sequence,
            peer_id: "test-peer".to_string(),
            transaction_id: String::new(),
            event_type: "router_snapshot".to_string(),
            event_data: serde_json::json!({
                "network_efficiency_v1": {"v": 1, "sequence": sequence}
            }),
            priority: EventPriority::NetworkEfficiency,
        }
    }

    /// Admit `count` events of `priority` at instant `now`, returning how many
    /// were admitted.
    fn admit_n(
        worker: &mut TelemetryWorker,
        priority: EventPriority,
        event_type: &str,
        count: usize,
        now: Instant,
    ) -> usize {
        (0..count)
            .filter(|_| worker.admit_event(priority, event_type, now))
            .count()
    }

    /// The periodic streams #6001 found thinned to 22-30% on the production
    /// gateways, plus `subscription_health_snapshot`, which has the same
    /// fixed-timer shape. Written out here rather than read from
    /// [`PERIODIC_EVENT_TYPES`], so that dropping a stream from that list fails
    /// the saturation test below instead of shrinking what it checks.
    const EXPECTED_PERIODIC_EVENT_TYPES: [&str; 12] = [
        "transport_snapshot",
        "resource_utilization",
        "router_snapshot",
        "shadow_rtt_aggregate",
        "shadow_rate_demand",
        "shadow_outbound_class",
        "shadow_reference_ping",
        "shadow_iface_tx",
        "broadcast_payload_mix",
        "outbound_message_mix",
        "interest_heartbeat_cycle",
        "subscription_health_snapshot",
    ];

    /// Saturate the per-second cap with operational events, as a busy gateway
    /// does (#6001: about 60 offered per second against a cap of 10).
    fn saturate_with_operational(worker: &mut TelemetryWorker, now: Instant) {
        let admitted = admit_n(
            worker,
            EventPriority::Operational,
            "get_request",
            MAX_EVENTS_PER_SECOND * 6,
            now,
        );
        assert_eq!(admitted, MAX_EVENTS_PER_SECOND, "the cap must be saturated");
    }

    #[test]
    fn test_every_periodic_event_is_admitted_when_operational_events_saturate_the_cap() {
        // Regression for #6001: periodic node streams shared the aggregate cap
        // with per-operation events, so a gateway uploaded only 22-30% of its
        // transport snapshots, resource samples and router snapshots.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();
        saturate_with_operational(&mut worker, now);

        // Every stream's timer can fire in the same saturated second, and more
        // than once if ticks were delayed.
        for _ in 0..3 {
            for event_type in EXPECTED_PERIODIC_EVENT_TYPES {
                assert!(
                    worker.admit_event(priority_for_event_type(event_type), event_type, now),
                    "periodic `{event_type}` was dropped by an operational flood"
                );
            }
        }
        assert_eq!(
            worker.events_this_second, MAX_EVENTS_PER_SECOND,
            "periodic events must not consume the operational budget"
        );
        assert!(
            !worker.admit_event(EventPriority::Operational, "get_request", now),
            "the operational cap must still hold after periodic admissions"
        );

        let snapshot = worker.metrics.snapshot();
        assert_eq!(
            snapshot.rate_limit_admitted_periodic_total,
            3 * EXPECTED_PERIODIC_EVENT_TYPES.len() as u64
        );
        assert_eq!(
            snapshot.rate_limit_admitted_operational_total,
            MAX_EVENTS_PER_SECOND as u64
        );
        for event_type in EXPECTED_PERIODIC_EVENT_TYPES {
            assert_eq!(
                snapshot.event_type_admissions[event_type_index(event_type)],
                [3, 0],
                "per-type counters for `{event_type}`"
            );
        }
    }

    #[test]
    fn test_periodic_event_types_are_exactly_the_expected_streams() {
        // Adding a stream to the bypass is a decision about its cadence, so it
        // must be made here as well as in the list.
        let mut actual = PERIODIC_EVENT_TYPES.to_vec();
        let mut expected = EXPECTED_PERIODIC_EVENT_TYPES.to_vec();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        for event_type in PERIODIC_EVENT_TYPES {
            assert_ne!(
                event_type_index(event_type),
                OTHER_EVENT_TYPE_INDEX,
                "periodic `{event_type}` must have its own per-type counter row"
            );
        }
    }

    #[tokio::test]
    async fn test_worker_built_periodic_events_survive_saturation() {
        // The worker builds `transport_snapshot` and `resource_utilization`
        // itself, so check the events it actually builds, through the real
        // `handle_event` path.
        let mut worker = rate_limit_test_worker();
        saturate_with_operational(&mut worker, Instant::now());

        let snapshot =
            worker.snapshot_to_telemetry(&crate::transport::metrics::TransportSnapshot::default());
        worker.handle_event(snapshot).await;
        let resource = worker.resource_utilization_telemetry();
        worker.handle_event(resource).await;

        let buffered: Vec<_> = worker
            .buffer
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(buffered, ["transport_snapshot", "resource_utilization"]);
    }

    #[test]
    fn test_net_event_router_snapshot_is_periodic() {
        // `router_snapshot` reaches the worker through `register_events`, which
        // derives the priority from the event-type name.
        use arbitrary::Arbitrary;
        let mut u = arbitrary::Unstructured::new(&[0_u8; 32_768]);
        let info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        let name = event_kind_to_string(&EventKind::RouterSnapshot(Box::new(info)));
        assert_eq!(priority_for_event_type(&name), EventPriority::Periodic);
        assert_eq!(
            priority_for_event_type("get_request"),
            EventPriority::Operational
        );
    }

    /// Drive the real `register_events` -> `send_event` path and return the
    /// event it queued on the ordinary channel.
    async fn queued_by_register_events(kind: EventKind) -> TelemetryEvent {
        let (sender, mut receiver) = mpsc::channel(4);
        let (network_efficiency_sender, _network_efficiency_receiver) = mpsc::channel(1);
        let reporter = TelemetryReporter {
            sender,
            network_efficiency_sender,
            metrics: Arc::new(TelemetryLocalMetrics::default()),
            local_peer_id: "test-peer".to_string(),
        };
        reporter
            .register_events(Either::Left(NetEventLog {
                tx: Transaction::NULL,
                peer_id: crate::node::PeerId::random(),
                kind,
            }))
            .await;
        let Ok(TelemetryCommand::Event(event)) = receiver.try_recv() else {
            panic!("register_events must queue the event on the ordinary channel");
        };
        event
    }

    #[tokio::test]
    async fn test_register_events_queues_router_snapshot_as_periodic() {
        // #6001: `register_events` is how `router_snapshot` reaches the
        // worker. Building it as Operational is the regression this PR fixes.
        use arbitrary::Arbitrary;
        let mut u = arbitrary::Unstructured::new(&[0_u8; 32_768]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.network_efficiency_v1 = None;
        let event = queued_by_register_events(EventKind::RouterSnapshot(Box::new(info))).await;
        assert_eq!(event.event_type, "router_snapshot");
        assert_eq!(event.priority, EventPriority::Periodic);

        // And an operation event from the same path stays under the cap.
        let event = queued_by_register_events(EventKind::Ignored).await;
        assert_eq!(event.event_type, "ignored");
        assert_eq!(event.priority, EventPriority::Operational);
    }

    #[test]
    fn test_standalone_events_get_their_priority_from_the_type() {
        // `send_standalone_event*` is how the shadow rollups, the message
        // mixes, `interest_heartbeat_cycle` and `subscription_health_snapshot`
        // reach the worker.
        for event_type in EXPECTED_PERIODIC_EVENT_TYPES {
            let event = standalone_event(event_type, "peer", serde_json::json!({}));
            assert_eq!(
                event.priority,
                EventPriority::Periodic,
                "standalone `{event_type}`"
            );
        }
        for event_type in ["interest_expired", "stranded_recovery"] {
            let event = standalone_event(event_type, "peer", serde_json::json!({}));
            assert_eq!(
                event.priority,
                EventPriority::Operational,
                "standalone `{event_type}`"
            );
        }
    }

    #[test]
    #[allow(deprecated)]
    fn test_deprecated_shadow_priority_behaves_like_periodic() {
        // Kept for API compatibility (#6001): listed types bypass the cap,
        // anything else is operational.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();
        saturate_with_operational(&mut worker, now);
        assert!(worker.admit_event(EventPriority::Shadow, "shadow_rtt_aggregate", now));
        assert!(!worker.admit_event(EventPriority::Shadow, "get_request", now));
        assert_eq!(
            worker.metrics.snapshot().rate_limit_admitted_periodic_total,
            1
        );
    }

    #[test]
    fn test_network_efficiency_rows_place_every_counter_and_zero_the_retired_slots() {
        // Distinct non-zero values everywhere, so a counter in the wrong
        // position, a dropped one, or a retired slot that stops being 0 fails.
        let metrics = TelemetryLocalMetricsSnapshot {
            enqueue_attempts_total: 1,
            enqueue_succeeded_total: 2,
            enqueue_dropped_full_total: 3,
            enqueue_dropped_closed_total: 4,
            rate_limit_admitted_operational_total: 5,
            rate_limit_admitted_periodic_total: 6,
            dropped_aggregate_limit_operational_total: 7,
            dropped_backoff_buffer_full_total: 8,
            batch_send_attempts_total: 9,
            batch_send_successes_total: 10,
            batch_send_failures_total: 11,
            batch_events_sent_total: 12,
            batch_retry_truncated_events_total: 13,
            known_shadow_rollups: std::array::from_fn(|stream| {
                let base = 100 * (stream as u64 + 1);
                KnownShadowRollupMetricsSnapshot {
                    event_type: KnownShadowRollup::ALL[stream].as_str(),
                    enqueue_attempts_total: base + 1,
                    enqueue_dropped_full_total: base + 2,
                    enqueue_dropped_closed_total: base + 3,
                    rate_limit_admitted_total: base + 4,
                    dropped_backoff_buffer_full_total: base + 5,
                    retry_truncated_total: base + 6,
                    sent_total: base + 7,
                }
            }),
            network_efficiency_delivery: [21, 22, 23, 24, 25, 26, 27, 28],
            event_type_admissions: (0..KNOWN_EVENT_TYPES.len() as u64)
                .map(|row| [1_000 + row, 2_000 + row])
                .collect(),
        };

        let rows = metrics.network_efficiency_rows();
        assert_eq!(
            rows.tel,
            [1, 2, 3, 4, 5, 0, 7, 0, 0, 8, 9, 10, 11, 12, 13, 6],
            "tel positions; 5, 7 and 8 are retired and must stay 0, the \
             periodic admitted count is position 15"
        );
        for (stream, row) in rows.shadow.iter().enumerate() {
            let base = 100 * (stream as u64 + 1);
            assert_eq!(
                *row,
                [
                    base + 1,
                    base + 2,
                    base + 3,
                    base + 4,
                    0,
                    0,
                    base + 5,
                    base + 6,
                    base + 7
                ],
                "shadow row {stream}; columns 4 and 5 are retired and must stay 0"
            );
        }
        assert_eq!(rows.eff, [21, 22, 23, 24, 25, 26, 27, 28]);
        assert_eq!(rows.tel_t.len(), KNOWN_EVENT_TYPES.len());
        assert_eq!(rows.tel_t[0], [1_000, 2_000]);
        assert_eq!(
            rows.tel_t[OTHER_EVENT_TYPE_INDEX],
            [
                1_000 + OTHER_EVENT_TYPE_INDEX as u64,
                2_000 + OTHER_EVENT_TYPE_INDEX as u64
            ]
        );
    }

    #[test]
    fn test_periodic_emission_guard_emits_once_for_back_to_back_passes() {
        // A burst-mode timer that fell behind runs its passes milliseconds
        // apart. Only the first may emit; the next emission needs half the
        // period to have passed (#6001 review).
        let period = Duration::from_secs(60);
        let mut guard = PeriodicEmissionGuard::half_of(period);
        let t0 = Instant::now();

        let emitted = [0, 1, 2, 5]
            .into_iter()
            .filter(|ms| guard.should_emit(t0 + Duration::from_millis(*ms)))
            .count();
        assert_eq!(emitted, 1, "back-to-back passes must emit one event");

        assert!(!guard.should_emit(t0 + period / 2 - Duration::from_millis(1)));
        assert!(
            guard.should_emit(t0 + period / 2),
            "half a period later emits"
        );
        assert!(
            guard.should_emit(t0 + period / 2 + period),
            "a normal tick emits"
        );
    }

    /// Body of the `fn` whose signature starts with `signature`, from an
    /// indented method declaration to its closing brace at the same indent.
    fn method_body<'a>(src: &'a str, signature: &str) -> &'a str {
        let start = src
            .find(&format!("\n    {signature}"))
            .unwrap_or_else(|| panic!("`{signature}` not found at method indent"));
        let len = src[start..]
            .find("\n    }\n")
            .expect("method end at the same indent");
        &src[start..start + len]
    }

    #[test]
    fn test_burst_timer_periodic_producers_use_the_emission_guard() {
        // The two periodic producers whose loops keep `MissedTickBehavior::Burst`
        // must check their guard before sending, or a stall releases an
        // uncapped burst.
        const RING: &str = include_str!("../ring.rs");
        const INTEREST: &str = include_str!("../ring/interest.rs");
        // Comments are stripped and each guard must be the condition of an
        // `if` at statement position, so a commented-out or moved guard fails.
        // `interest_heartbeat` skips the rest of the loop when the guard says
        // no; `sweep_task` sends inside the guarded branch.
        for (src, signature, guard_if, event, guarded_send) in [
            (
                RING,
                "async fn interest_heartbeat(",
                "if !cycle_event_guard.should_emit(",
                "\"interest_heartbeat_cycle\"",
                false,
            ),
            (
                INTEREST,
                "async fn sweep_task(",
                "if health_event_guard.should_emit(",
                "\"subscription_health_snapshot\"",
                true,
            ),
        ] {
            let body =
                crate::contract::source_pin_util::strip_comments(method_body(src, signature));
            let checked = body
                .lines()
                .position(|line| line.trim_start().starts_with(guard_if))
                .unwrap_or_else(|| panic!("`{signature}` no longer opens with `{guard_if}`"));
            let lines: Vec<&str> = body.lines().collect();
            let emitted = lines
                .iter()
                .position(|line| line.contains(event))
                .unwrap_or_else(|| panic!("`{signature}` no longer emits {event}"));
            assert!(
                checked < emitted,
                "`{signature}` must check `{guard_if}` before emitting {event}"
            );
            let guard_indent = lines[checked].len() - lines[checked].trim_start().len();
            let block_end = lines[checked + 1..]
                .iter()
                .position(|line| {
                    line.trim_start().starts_with('}')
                        && line.len() - line.trim_start().len() == guard_indent
                })
                .map(|offset| checked + 1 + offset)
                .unwrap_or_else(|| panic!("`{signature}`: end of the guard's block"));
            if guarded_send {
                assert!(
                    emitted < block_end,
                    "`{signature}` must send {event} inside the `{guard_if}` branch"
                );
            } else {
                assert!(
                    lines[checked + 1..block_end]
                        .iter()
                        .any(|line| line.trim() == "continue;"),
                    "`{signature}` must skip the event when `{guard_if}` says no"
                );
            }
            assert!(
                body.contains("PeriodicEmissionGuard::half_of("),
                "`{signature}` must build its guard from its period"
            );
        }
    }

    #[test]
    fn test_periodic_priority_on_an_unlisted_type_is_capped() {
        // The bypass is bounded by the fixed list, not by whoever sets the
        // priority: anything else marked Periodic is treated as operational.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();
        saturate_with_operational(&mut worker, now);

        assert!(!worker.admit_event(EventPriority::Periodic, "get_request", now));
        let snapshot = worker.metrics.snapshot();
        assert_eq!(snapshot.rate_limit_admitted_periodic_total, 0);
        assert_eq!(
            snapshot.event_type_admissions[event_type_index("get_request")],
            [
                MAX_EVENTS_PER_SECOND as u64,
                (MAX_EVENTS_PER_SECOND * 5 + 1) as u64
            ]
        );
    }

    #[test]
    fn test_periodic_flood_cannot_starve_operational() {
        // #4380's concern, now met by construction: periodic events arriving
        // first in a window take no operational slot.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();

        for _ in 0..5 {
            for event_type in EXPECTED_PERIODIC_EVENT_TYPES {
                assert!(worker.admit_event(EventPriority::Periodic, event_type, now));
            }
        }
        let op_admitted = admit_n(
            &mut worker,
            EventPriority::Operational,
            "test_operational",
            50,
            now,
        );
        assert_eq!(
            op_admitted, MAX_EVENTS_PER_SECOND,
            "operational events must keep the whole cap however many periodic \
             events arrived first"
        );
    }

    #[test]
    fn test_event_type_counters_count_admitted_and_dropped_per_type() {
        let mut worker = rate_limit_test_worker();
        let t0 = Instant::now();

        assert_eq!(
            admit_n(
                &mut worker,
                EventPriority::Operational,
                "get_request",
                7,
                t0
            ),
            7
        );
        assert_eq!(
            admit_n(
                &mut worker,
                EventPriority::Operational,
                "connect_rejected",
                5,
                t0
            ),
            3
        );
        // A name nobody listed lands in the `other` row, so the exported
        // cardinality cannot grow.
        assert!(!worker.admit_event(EventPriority::Operational, "not_a_known_event", t0));
        // A new window refills the cap.
        let t1 = t0 + Duration::from_secs(1);
        assert!(worker.admit_event(EventPriority::Operational, "not_a_known_event", t1));
        assert!(worker.admit_event(EventPriority::Periodic, "transport_snapshot", t1));
        assert!(worker.admit_event(EventPriority::NetworkEfficiency, "router_snapshot", t1));

        let rows = worker.metrics.snapshot().event_type_admissions;
        assert_eq!(rows.len(), KNOWN_EVENT_TYPES.len());
        assert_eq!(rows[event_type_index("get_request")], [7, 0]);
        assert_eq!(rows[event_type_index("connect_rejected")], [3, 2]);
        assert_eq!(rows[OTHER_EVENT_TYPE_INDEX], [1, 1]);
        assert_eq!(rows[event_type_index("transport_snapshot")], [1, 0]);
        assert_eq!(rows[event_type_index("router_snapshot")], [1, 0]);
        let total: u64 = rows.iter().flatten().sum();
        assert_eq!(
            total,
            7 + 5 + 2 + 2,
            "every outcome is counted exactly once"
        );
    }

    #[test]
    fn test_known_event_types_are_unique_and_other_is_last() {
        let mut seen = std::collections::HashSet::new();
        for (index, event_type) in KNOWN_EVENT_TYPES.iter().enumerate() {
            assert!(seen.insert(*event_type), "`{event_type}` listed twice");
            assert_eq!(event_type_index(event_type), index);
        }
        assert_eq!(KNOWN_EVENT_TYPES[OTHER_EVENT_TYPE_INDEX], "other");
    }

    /// The body of `fn event_kind_to_string` in this file, bounded to that
    /// function and taken from outside the test module.
    fn event_kind_to_string_source() -> &'static str {
        const SRC: &str = include_str!("telemetry.rs");
        let test_module = SRC
            .find("\n#[cfg(test)]\nmod tests")
            .expect("test module marker");
        let start = SRC
            .find("\nfn event_kind_to_string(kind: &EventKind) -> String {")
            .expect("event_kind_to_string signature must stay at column 0");
        assert!(start < test_module, "matched inside the test module");
        let len = SRC[start..].find("\n}\n").expect("function end");
        &SRC[start..start + len]
    }

    #[test]
    fn test_known_event_types_cover_every_net_event_name() {
        let body = event_kind_to_string_source();
        let names: Vec<&str> = body
            .split(".to_string()")
            .filter_map(|chunk| {
                let chunk = chunk.trim_end();
                let chunk = chunk.strip_suffix('"')?;
                Some(&chunk[chunk.rfind('"')? + 1..])
            })
            .collect();
        // Exact, so a scrape that silently stops matching some arms fails
        // here. Adding or removing an `event_kind_to_string` arm changes this
        // count: bump it deliberately, together with `KNOWN_EVENT_TYPES`.
        const EXPECTED_NET_EVENT_NAMES: usize = 57;
        assert_eq!(
            names.len(),
            EXPECTED_NET_EVENT_NAMES,
            "event_kind_to_string scrape found {} names; if you added or removed \
             an arm, bump EXPECTED_NET_EVENT_NAMES and KNOWN_EVENT_TYPES",
            names.len()
        );
        for name in names {
            assert_ne!(
                event_type_index(name),
                OTHER_EVENT_TYPE_INDEX,
                "net event `{name}` is missing from KNOWN_EVENT_TYPES"
            );
        }
    }

    #[test]
    fn test_known_event_types_cover_every_standalone_producer() {
        // Discover every `send_standalone_event*` call in the crate, so a new
        // producer cannot silently land in the `other` row.
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("read src dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    out.push(path);
                }
            }
        }
        let mut files = Vec::new();
        walk(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut files,
        );

        let mut literal_names = Vec::new();
        for file in files {
            let src = std::fs::read_to_string(&file).expect("read source");
            for needle in [
                "send_standalone_event(",
                "send_standalone_event_with_peer_id(",
                "send_standalone_shadow_event_with_peer_id(",
            ] {
                for (at, _) in src.match_indices(needle) {
                    let before = &src[..at];
                    // Skip the definitions and occurrences inside string
                    // literals (pin tests quote the call).
                    if before.ends_with("fn ") || before.ends_with('"') {
                        continue;
                    }
                    let arg = src[at + needle.len()..].trim_start();
                    if let Some(rest) = arg.strip_prefix('"') {
                        let end = rest.find('"').expect("closing quote");
                        literal_names.push((file.clone(), rest[..end].to_string()));
                    } else {
                        let ident: String = arg
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect();
                        // This file's own forwarding wrappers pass the caller's
                        // `event_type` through.
                        if file.ends_with("tracing/telemetry.rs") && ident == "event_type" {
                            continue;
                        }
                        assert_eq!(
                            ident,
                            "FLAG_TELEMETRY_EVENT",
                            "{}: non-literal event type; add it to KNOWN_EVENT_TYPES \
                             and to this test",
                            file.display()
                        );
                    }
                }
            }
        }
        // Exact count of call sites with a literal event type (production and
        // test). A new producer changes it: bump it deliberately after giving
        // the event its own `KNOWN_EVENT_TYPES` row.
        const EXPECTED_LITERAL_STANDALONE_CALLS: usize = 16;
        assert_eq!(
            literal_names.len(),
            EXPECTED_LITERAL_STANDALONE_CALLS,
            "found {} literal send_standalone_event* calls; if you added or \
             removed one, bump EXPECTED_LITERAL_STANDALONE_CALLS",
            literal_names.len()
        );
        for (file, name) in literal_names {
            assert_ne!(
                event_type_index(&name),
                OTHER_EVENT_TYPE_INDEX,
                "{}: standalone event `{name}` is missing from KNOWN_EVENT_TYPES",
                file.display()
            );
        }
        assert_ne!(
            event_type_index(crate::ring::FLAG_TELEMETRY_EVENT),
            OTHER_EVENT_TYPE_INDEX
        );
    }

    #[test]
    fn test_operational_can_use_full_aggregate_cap() {
        // Operational telemetry is unchanged from pre-#4380: it may use the
        // entire aggregate cap when there are no shadow events.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();

        let admitted = admit_n(
            &mut worker,
            EventPriority::Operational,
            "test_operational",
            100,
            now,
        );
        assert_eq!(
            admitted, MAX_EVENTS_PER_SECOND,
            "operational events alone must be able to fill the aggregate cap"
        );
    }

    #[test]
    fn test_network_efficiency_has_one_structurally_bounded_reserved_rate_slot() {
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();

        assert_eq!(
            admit_n(
                &mut worker,
                EventPriority::Operational,
                "test_operational",
                MAX_EVENTS_PER_SECOND,
                now,
            ),
            MAX_EVENTS_PER_SECOND
        );
        assert!(
            worker.admit_event(EventPriority::NetworkEfficiency, "router_snapshot", now,),
            "ordinary saturation must not starve the fixed 30-minute diagnostic"
        );
        assert_eq!(
            worker.events_this_second, MAX_EVENTS_PER_SECOND,
            "the reserved diagnostic must not expand the ordinary event budget"
        );
    }

    #[tokio::test]
    async fn test_network_efficiency_uses_dedicated_channel_when_ordinary_channel_is_full() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .try_send(TelemetryCommand::Event(test_telemetry_event(
                "ordinary-filler",
                EventPriority::Operational,
            )))
            .expect("fill ordinary channel");
        let (network_efficiency_sender, mut network_efficiency_receiver) = mpsc::channel(1);
        let metrics = Arc::new(TelemetryLocalMetrics::default());
        let reporter = TelemetryReporter {
            sender,
            network_efficiency_sender,
            metrics: metrics.clone(),
            local_peer_id: "test-peer".to_string(),
        };

        let mut event = test_network_efficiency_event(1);
        // Production callers initially create every NetEvent as Operational;
        // send_event owns both classification and reserved routing.
        event.priority = EventPriority::Operational;
        reporter.send_event(event).await;

        assert!(
            receiver.try_recv().is_ok(),
            "ordinary filler remains queued"
        );
        let TelemetryCommand::Event(reserved) = network_efficiency_receiver
            .try_recv()
            .expect("diagnostic must enter its dedicated channel")
        else {
            panic!("expected reserved Event command");
        };
        assert_eq!(reserved.priority, EventPriority::NetworkEfficiency);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.network_efficiency_delivery[0], 1);
        assert_eq!(snapshot.network_efficiency_delivery[1], 0);
        assert_eq!(snapshot.enqueue_succeeded_total, 1);
    }

    #[tokio::test]
    async fn test_network_efficiency_keeps_reserved_buffer_slot_during_backoff() {
        let mut worker = rate_limit_test_worker();
        worker.buffer = (0..MAX_BUFFER_SIZE)
            .map(|_| test_telemetry_event("filler", EventPriority::Operational))
            .collect();
        worker.backoff_ms = MAX_BACKOFF_MS;
        worker.last_send = Instant::now();

        worker.handle_event(test_network_efficiency_event(1)).await;
        assert_eq!(
            worker.buffer.len(),
            MAX_BUFFER_SIZE + NETWORK_EFFICIENCY_BUFFER_RESERVE
        );
        assert!(worker.buffer.iter().any(is_network_efficiency_event));
        let snapshot = worker.metrics.snapshot();
        assert_eq!(snapshot.network_efficiency_delivery[3], 1);
        assert_eq!(snapshot.network_efficiency_delivery[4], 0);
        assert_eq!(snapshot.network_efficiency_delivery[5], 0);

        // A later cumulative sample supersedes the old one rather than using
        // a second reserved slot during a prolonged collector outage.
        worker.handle_event(test_network_efficiency_event(2)).await;
        assert_eq!(
            worker.buffer.len(),
            MAX_BUFFER_SIZE + NETWORK_EFFICIENCY_BUFFER_RESERVE
        );
        let retained: Vec<_> = worker
            .buffer
            .iter()
            .filter(|event| is_network_efficiency_event(event))
            .collect();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].timestamp, 2);
        assert_eq!(
            worker.metrics.snapshot().network_efficiency_delivery[5],
            1,
            "the replaced historical point sample must be a visible terminal outcome"
        );
    }

    #[tokio::test]
    async fn test_reserved_slot_does_not_reduce_ordinary_buffer_capacity() {
        let mut worker = rate_limit_test_worker();
        worker.buffer = (0..MAX_BUFFER_SIZE - 1)
            .map(|_| test_telemetry_event("filler", EventPriority::Operational))
            .collect();
        worker.buffer.push(test_network_efficiency_event(1));
        worker.backoff_ms = MAX_BACKOFF_MS;
        worker.last_send = Instant::now();

        worker
            .handle_event(test_telemetry_event(
                "last-ordinary-slot",
                EventPriority::Operational,
            ))
            .await;

        assert_eq!(
            worker.buffer.len(),
            MAX_BUFFER_SIZE + NETWORK_EFFICIENCY_BUFFER_RESERVE,
            "the diagnostic reserve must not take one of the 100 ordinary slots"
        );
    }

    #[test]
    fn test_failed_full_batch_retains_network_efficiency_reserved_slot() {
        let mut worker = rate_limit_test_worker();
        let mut failed = (0..MAX_BUFFER_SIZE)
            .map(|_| test_telemetry_event("ordinary", EventPriority::Operational))
            .collect::<Vec<_>>();
        failed.push(test_network_efficiency_event(1));

        worker.requeue_failed_batch(failed);

        assert_eq!(
            worker.buffer.len(),
            MAX_BUFFER_SIZE + NETWORK_EFFICIENCY_BUFFER_RESERVE
        );
        assert!(worker.buffer.iter().any(is_network_efficiency_event));
        let snapshot = worker.metrics.snapshot();
        assert_eq!(snapshot.batch_retry_truncated_events_total, 0);
        assert_eq!(snapshot.network_efficiency_delivery[6], 0);
    }

    #[test]
    fn test_shadow_rollup_bypasses_a_saturated_cap_with_per_stream_accounting() {
        // Before #6001 a saturated aggregate cap dropped every shadow rollup.
        // They are periodic now, and the per-stream counters still see them.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();
        saturate_with_operational(&mut worker, now);

        let admitted = admit_n(
            &mut worker,
            priority_for_event_type("outbound_message_mix"),
            "outbound_message_mix",
            10,
            now,
        );
        assert_eq!(admitted, 10);
        let snapshot = worker.metrics.snapshot();
        assert_eq!(
            snapshot.known_shadow_rollups[KnownShadowRollup::OutboundMessageMix.index()]
                .rate_limit_admitted_total,
            10
        );
    }

    #[test]
    fn test_enqueue_full_and_closed_are_accounted_by_fixed_stream() {
        let metrics = TelemetryLocalMetrics::default();

        let (full_sender, _full_receiver) = mpsc::channel(1);
        full_sender
            .try_send(TelemetryCommand::Event(test_telemetry_event(
                "filler",
                EventPriority::Operational,
            )))
            .expect("fill the one-slot channel");
        try_enqueue_event(
            &full_sender,
            &metrics,
            test_telemetry_event("broadcast_payload_mix", EventPriority::Periodic),
        );

        let (closed_sender, closed_receiver) = mpsc::channel(1);
        drop(closed_receiver);
        try_enqueue_event(
            &closed_sender,
            &metrics,
            test_telemetry_event("outbound_message_mix", EventPriority::Periodic),
        );

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.enqueue_attempts_total, 2);
        assert_eq!(snapshot.enqueue_succeeded_total, 0);
        assert_eq!(snapshot.enqueue_dropped_full_total, 1);
        assert_eq!(snapshot.enqueue_dropped_closed_total, 1);
        assert_eq!(
            snapshot.known_shadow_rollups[KnownShadowRollup::BroadcastPayloadMix.index()]
                .enqueue_dropped_full_total,
            1
        );
        assert_eq!(
            snapshot.known_shadow_rollups[KnownShadowRollup::OutboundMessageMix.index()]
                .enqueue_dropped_closed_total,
            1
        );
    }

    #[tokio::test]
    async fn test_backoff_full_buffer_drop_is_accounted_by_fixed_stream() {
        let mut worker = rate_limit_test_worker();
        worker.buffer = (0..MAX_BUFFER_SIZE)
            .map(|_| test_telemetry_event("filler", EventPriority::Operational))
            .collect();
        worker.backoff_ms = INITIAL_BACKOFF_MS;
        worker.last_send = Instant::now();

        worker
            .handle_event(test_telemetry_event(
                "broadcast_payload_mix",
                EventPriority::Periodic,
            ))
            .await;

        assert_eq!(worker.buffer.len(), MAX_BUFFER_SIZE);
        let snapshot = worker.metrics.snapshot();
        assert_eq!(snapshot.rate_limit_admitted_periodic_total, 1);
        assert_eq!(snapshot.dropped_backoff_buffer_full_total, 1);
        assert_eq!(
            snapshot.known_shadow_rollups[KnownShadowRollup::BroadcastPayloadMix.index()]
                .dropped_backoff_buffer_full_total,
            1
        );
    }

    #[test]
    fn test_failed_batch_retry_truncation_is_accounted_by_fixed_stream() {
        let mut worker = rate_limit_test_worker();
        worker.buffer = (0..MAX_BUFFER_SIZE - 1)
            .map(|_| test_telemetry_event("buffered", EventPriority::Operational))
            .collect();

        worker.requeue_failed_batch(vec![
            test_telemetry_event("retained", EventPriority::Operational),
            test_telemetry_event("broadcast_payload_mix", EventPriority::Periodic),
            test_telemetry_event("outbound_message_mix", EventPriority::Periodic),
        ]);

        assert_eq!(worker.buffer.len(), MAX_BUFFER_SIZE);
        let snapshot = worker.metrics.snapshot();
        assert_eq!(snapshot.batch_send_failures_total, 1);
        assert_eq!(snapshot.batch_retry_truncated_events_total, 2);
        assert_eq!(
            snapshot.known_shadow_rollups[KnownShadowRollup::BroadcastPayloadMix.index()]
                .retry_truncated_total,
            1
        );
        assert_eq!(
            snapshot.known_shadow_rollups[KnownShadowRollup::OutboundMessageMix.index()]
                .retry_truncated_total,
            1
        );
    }

    #[test]
    fn test_rate_limit_window_resets_the_operational_count() {
        // After the 1s window rolls over the count resets, so a new window
        // admits a fresh operational budget.
        let mut worker = rate_limit_test_worker();
        let t0 = Instant::now();

        assert_eq!(
            admit_n(
                &mut worker,
                EventPriority::Operational,
                "test_operational",
                20,
                t0,
            ),
            MAX_EVENTS_PER_SECOND
        );
        assert!(!worker.admit_event(EventPriority::Operational, "test_operational", t0));

        // Roll the window over (exactly 1s is the boundary: >= 1s resets).
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(
            admit_n(
                &mut worker,
                EventPriority::Operational,
                "test_operational",
                20,
                t1,
            ),
            MAX_EVENTS_PER_SECOND,
            "aggregate cap must refresh on window rollover"
        );
    }

    #[test]
    fn test_window_does_not_reset_before_one_second() {
        // Just under 1s must NOT roll the window over: counters persist.
        let mut worker = rate_limit_test_worker();
        let t0 = Instant::now();

        assert_eq!(
            admit_n(
                &mut worker,
                EventPriority::Operational,
                "test_operational",
                MAX_EVENTS_PER_SECOND,
                t0
            ),
            MAX_EVENTS_PER_SECOND
        );

        let almost = t0 + Duration::from_millis(999);
        assert!(
            !worker.admit_event(EventPriority::Operational, "test_operational", almost),
            "window must not reset before a full second elapses"
        );
    }

    #[test]
    fn test_priority_default_is_operational() {
        // The default priority must be Operational, so an event constructed
        // without a priority is subject to the cap.
        assert_eq!(EventPriority::default(), EventPriority::Operational);
    }

    #[test]
    fn test_priority_not_serialized_to_otlp() {
        // The priority field steers the in-process rate limiter only; it must
        // not change the OTLP wire payload (the collector schema is unchanged
        // from pre-#4380). Verify it is absent from the serialized event.
        let event = TelemetryEvent {
            timestamp: 1,
            peer_id: "p".to_string(),
            transaction_id: String::new(),
            event_type: "shadow_rtt_aggregate".to_string(),
            event_data: serde_json::json!({"k": "v"}),
            priority: EventPriority::Periodic,
        };
        let serialized = serde_json::to_value(&event).expect("serialize");
        assert!(
            serialized.get("priority").is_none(),
            "priority must be #[serde(skip)] so the OTLP payload is unchanged"
        );
    }

    /// #3445: a client subscribe that exhausts every candidate without a
    /// terminal reply must emit a `subscribe_timeout` outcome event so the
    /// dashboard pairs the original `subscribe_request` with an outcome
    /// instead of leaving it dangling. Without the new `SubscribeTimeout`
    /// variant + its name-mapping and JSON arms this test does not compile,
    /// and before they were wired in the timeout was completely invisible.
    #[test]
    fn test_event_kind_to_json_subscribe_timeout() {
        use crate::message::Transaction;
        use crate::ring::PeerKeyLocation;
        use crate::tracing::SubscribeEvent;
        use freenet_stdlib::prelude::ContractInstanceId;

        let tx = Transaction::new::<crate::operations::subscribe::SubscribeMsg>();
        let requester = PeerKeyLocation::random();
        let instance_id = ContractInstanceId::new([7u8; 32]);

        let event = EventKind::Subscribe(SubscribeEvent::SubscribeTimeout {
            id: tx,
            requester: requester.clone(),
            instance_id,
            retries: 4,
            elapsed_ms: 60000,
            timestamp: 12345,
        });

        // Name-mapping (OTLP event_type): must be a dedicated, queryable name.
        assert_eq!(event_kind_to_string(&event), "subscribe_timeout");

        // JSON serialization (dashboard payload): every field round-trips.
        let json = event_kind_to_json(&event);
        assert_eq!(json["type"], "subscribe_timeout");
        assert_eq!(json["id"], tx.to_string());
        assert_eq!(json["requester"], requester.to_string());
        assert_eq!(json["instance_id"], instance_id.to_string());
        assert_eq!(json["retries"], 4);
        assert_eq!(json["elapsed_ms"], 60000);
        assert_eq!(json["timestamp"], 12345);

        // It is classified as a (failure) subscribe outcome so dashboards
        // counting paired outcomes recognize it, unlike non-terminal events.
        assert_eq!(event.subscribe_outcome(), Some(false));
    }

    /// The event-log JSON for `EventKind::TransportSnapshot` lists fields
    /// explicitly; it must carry exactly the struct's serde keys (plus
    /// `type`), so neither side can drift silently.
    #[test]
    fn transport_snapshot_json_lists_every_field() {
        let snapshot = crate::transport::metrics::TransportSnapshot::default();
        // The connection identity and inbound concurrency keys are sent only
        // when non-zero, so a default snapshot carries none of them (see the
        // tests below).
        let mut expected: std::collections::BTreeSet<String> = serde_json::to_value(&snapshot)
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .filter(|key| {
                !key.starts_with("conn_identity_") && !key.starts_with("inbound_concurrency_")
            })
            .cloned()
            .collect();
        expected.insert("type".to_string());
        let actual: std::collections::BTreeSet<String> = event_kind_to_json(
            &super::super::EventKind::TransportSnapshot(Box::new(snapshot)),
        )
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
        assert_eq!(actual, expected);
    }

    /// As below, for the NAT-origin, stray-intro and dial-queue counts:
    /// each is distinct and non-zero, so a swapped or mis-keyed one is caught.
    #[test]
    fn transport_snapshot_json_carries_nat_origin_values() {
        let snapshot = crate::transport::metrics::TransportSnapshot {
            nat_attempts_connect_joiner: 1,
            nat_attempts_connect_acceptor: 2,
            nat_attempts_gateway: 3,
            nat_attempts_ring_peer_no_transport: 5,
            nat_established_connect_joiner: 6,
            nat_established_connect_acceptor: 7,
            nat_established_gateway: 8,
            nat_established_ring_peer_no_transport: 10,
            stray_intro_after_deadline: 11,
            stray_intro_same_ip_other_port: 12,
            stray_intro_other: 13,
            stray_intro_failures_evicted: 18,
            dial_queue_wait_lt_100ms: 14,
            dial_queue_wait_lt_1s: 15,
            dial_queue_wait_lt_3s: 16,
            dial_queue_wait_ge_3s: 17,
            demux_legacy: 19,
            demux_quic: 20,
            demux_other: 21,
            quic_ingress_forwarded: 46,
            quic_ingress_not_allowed: 47,
            quic_ingress_queue_full: 48,
            quic_ingress_source_full: 50,
            quic_ingress_no_ingress: 49,
            egress_immediate: 22,
            egress_waited: 23,
            egress_unpaced: 25,
            egress_bytes: 26,
            egress_bytes_waited: 27,
            egress_wait_us_sum: 28,
            egress_wait_us_max: 29,
            egress_long_waits_cap: 40,
            egress_long_waits_connection: 41,
            egress_long_waits_other: 42,
            egress_streams_abandoned_cap: 43,
            egress_streams_abandoned_connection: 44,
            egress_streams_abandoned_other: 45,
            egress_streams_refused_cap: 117,
            egress_streams_refused_connection: 118,
            egress_streams_refused_other: 119,
            egress_debt_hwm_bytes: 30,
            egress_debt_forgiven_bytes: 31,
            ..Default::default()
        };
        let reference = serde_json::to_value(&snapshot).unwrap();
        let json = event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
            snapshot,
        )));
        for (key, expected) in [
            ("nat_attempts_connect_joiner", 1),
            ("nat_attempts_connect_acceptor", 2),
            ("nat_attempts_gateway", 3),
            ("nat_attempts_ring_peer_no_transport", 5),
            ("nat_established_connect_joiner", 6),
            ("nat_established_connect_acceptor", 7),
            ("nat_established_gateway", 8),
            ("nat_established_ring_peer_no_transport", 10),
            ("stray_intro_after_deadline", 11),
            ("stray_intro_same_ip_other_port", 12),
            ("stray_intro_other", 13),
            ("stray_intro_failures_evicted", 18),
            ("dial_queue_wait_lt_100ms", 14),
            ("dial_queue_wait_lt_1s", 15),
            ("dial_queue_wait_lt_3s", 16),
            ("dial_queue_wait_ge_3s", 17),
            ("demux_legacy", 19),
            ("demux_quic", 20),
            ("demux_other", 21),
            ("quic_ingress_forwarded", 46),
            ("quic_ingress_not_allowed", 47),
            ("quic_ingress_queue_full", 48),
            ("quic_ingress_source_full", 50),
            ("quic_ingress_no_ingress", 49),
            ("egress_immediate", 22),
            ("egress_waited", 23),
            ("egress_unpaced", 25),
            ("egress_bytes", 26),
            ("egress_bytes_waited", 27),
            ("egress_wait_us_sum", 28),
            ("egress_wait_us_max", 29),
            ("egress_long_waits_cap", 40),
            ("egress_long_waits_connection", 41),
            ("egress_long_waits_other", 42),
            ("egress_streams_abandoned_cap", 43),
            ("egress_streams_abandoned_connection", 44),
            ("egress_streams_abandoned_other", 45),
            ("egress_streams_refused_cap", 117),
            ("egress_streams_refused_connection", 118),
            ("egress_streams_refused_other", 119),
            ("egress_debt_hwm_bytes", 30),
            ("egress_debt_forgiven_bytes", 31),
        ] {
            assert_eq!(json[key], expected, "event-log value for {key}");
            assert_eq!(json[key], reference[key], "serde value for {key}");
        }
    }

    /// The key set is not enough: the event-log JSON must also carry each
    /// field's VALUE under its own key. The six inbound-stream counts are
    /// distinct and non-zero so a swapped or mis-keyed one is caught.
    #[test]
    fn transport_snapshot_json_carries_inbound_stream_values() {
        let snapshot = crate::transport::metrics::TransportSnapshot {
            inbound_stream_refused_invalid_length: 1,
            inbound_stream_refused_connection_bytes: 2,
            inbound_stream_refused_connection_streams: 3,
            inbound_stream_closed_too_many_refusals: 4,
            inbound_stream_evicted_idle: 5,
            inbound_stream_evicted_age: 6,
            ..Default::default()
        };
        let reference = serde_json::to_value(&snapshot).unwrap();
        let json = event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
            snapshot,
        )));
        for (key, expected) in [
            ("inbound_stream_refused_invalid_length", 1),
            ("inbound_stream_refused_connection_bytes", 2),
            ("inbound_stream_refused_connection_streams", 3),
            ("inbound_stream_closed_too_many_refusals", 4),
            ("inbound_stream_evicted_idle", 5),
            ("inbound_stream_evicted_age", 6),
        ] {
            assert_eq!(json[key], expected, "event-log value for {key}");
            assert_eq!(json[key], reference[key], "serde value for {key}");
        }
    }

    /// The event-log JSON carries the dropped-handshake-packet count.
    #[test]
    fn transport_snapshot_json_carries_handshake_unauthenticated_dropped() {
        let snapshot = crate::transport::metrics::TransportSnapshot {
            handshake_unauthenticated_dropped: 7,
            ..Default::default()
        };
        let json = event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
            snapshot,
        )));
        assert_eq!(json["handshake_unauthenticated_dropped"], 7);
    }

    /// The event-log JSON carries each connection identity count under its
    /// own key (distinct non-zero values, so a swapped key is caught).
    #[test]
    fn transport_snapshot_json_carries_conn_identity_values() {
        let snapshot = crate::transport::metrics::TransportSnapshot {
            conn_identity_set_dialed: 61,
            conn_identity_expected_inbound_bound: 62,
            conn_identity_expected_inbound_unbound: 80,
            conn_identity_set_connect_request: 63,
            conn_identity_same_connect_request: 64,
            conn_identity_changed_connect_request: 65,
            conn_identity_connect_request_known_addr: 66,
            conn_identity_set_connect_peer: 67,
            conn_identity_same_connect_peer: 68,
            conn_identity_changed_connect_peer: 69,
            conn_identity_changed_ring_promotion: 196,
            conn_identity_claim_held: 70,
            conn_identity_pending_claim_bound: 199,
            conn_identity_quiet_holder_replaced: 198,
            conn_identity_claim_held_long: 197,
            conn_identity_own_key: 71,
            conn_identity_duplicate_key: 72,
            conn_identity_teardown_key_elsewhere: 73,
            conn_identity_teardown_unidentified: 74,
            conn_identity_teardown_key_unmapped: 79,
            ..Default::default()
        };
        let reference = serde_json::to_value(&snapshot).unwrap();
        let json = event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
            snapshot,
        )));
        for (key, expected) in [
            ("conn_identity_set_dialed", 61),
            ("conn_identity_expected_inbound_bound", 62),
            ("conn_identity_expected_inbound_unbound", 80),
            ("conn_identity_set_connect_request", 63),
            ("conn_identity_same_connect_request", 64),
            ("conn_identity_changed_connect_request", 65),
            ("conn_identity_connect_request_known_addr", 66),
            ("conn_identity_set_connect_peer", 67),
            ("conn_identity_same_connect_peer", 68),
            ("conn_identity_changed_connect_peer", 69),
            ("conn_identity_changed_ring_promotion", 196),
            ("conn_identity_claim_held", 70),
            ("conn_identity_pending_claim_bound", 199),
            ("conn_identity_quiet_holder_replaced", 198),
            ("conn_identity_claim_held_long", 197),
            ("conn_identity_own_key", 71),
            ("conn_identity_duplicate_key", 72),
            ("conn_identity_teardown_key_elsewhere", 73),
            ("conn_identity_teardown_unidentified", 74),
            ("conn_identity_teardown_key_unmapped", 79),
        ] {
            assert_eq!(json[key], expected, "event-log value for {key}");
            assert_eq!(json[key], reference[key], "serde value for {key}");
        }
    }

    /// The event-log JSON carries the QUIC endpoint-down, adapter-panic and
    /// close-code counts, with the same values serde gives.
    #[test]
    fn transport_snapshot_json_carries_quic_endpoint_and_close_counts() {
        let snapshot = crate::transport::metrics::TransportSnapshot {
            quic_endpoint_down: 1,
            quic_adapter_panicked: 5,
            quic_close_sent_node_dropped_peer: 2,
            quic_close_sent_downgrade: 3,
            quic_close_sent_identity_mismatch: 4,
            quic_close_sent_send_abort_limit: 5,
            quic_close_sent_peer_not_acknowledging: 6,
            quic_breaker_trips: 7,
            quic_breaker_suppressed: true,
            ..Default::default()
        };
        let reference = serde_json::to_value(&snapshot).unwrap();
        let json = event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
            snapshot,
        )));
        for (key, expected) in [
            ("quic_endpoint_down", 1),
            ("quic_adapter_panicked", 5),
            ("quic_close_sent_node_dropped_peer", 2),
            ("quic_close_sent_downgrade", 3),
            ("quic_close_sent_identity_mismatch", 4),
            ("quic_close_sent_send_abort_limit", 5),
            ("quic_close_sent_peer_not_acknowledging", 6),
            ("quic_breaker_trips", 7),
        ] {
            assert_eq!(json[key], expected, "event-log value for {key}");
            assert_eq!(json[key], reference[key], "serde value for {key}");
        }
        assert_eq!(json["quic_breaker_suppressed"], true);
        assert_eq!(
            json["quic_breaker_suppressed"],
            reference["quic_breaker_suppressed"]
        );
    }

    /// Each connection identity key is in the event-log JSON exactly when its
    /// count is non-zero, and every serde `conn_identity_*` field is covered.
    #[test]
    fn transport_snapshot_json_sends_conn_identity_keys_only_when_nonzero() {
        let to_json = |snapshot: crate::transport::metrics::TransportSnapshot| {
            event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
                snapshot,
            )))
        };
        let identity_keys = |json: &serde_json::Value| -> Vec<String> {
            json.as_object()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with("conn_identity_"))
                .cloned()
                .collect()
        };
        let zero = crate::transport::metrics::TransportSnapshot::default();
        assert!(identity_keys(&to_json(zero.clone())).is_empty());

        let serde_keys = identity_keys(&serde_json::to_value(&zero).unwrap());
        assert_eq!(
            serde_keys.len(),
            crate::transport::ConnIdentity::COUNT,
            "one serde field per kind"
        );
        for kind in crate::transport::ConnIdentity::ALL {
            let key = format!("conn_identity_{}", kind.label());
            assert!(serde_keys.contains(&key), "serde field {key}");
            let mut snapshot = zero.clone();
            let mut serde_value = serde_json::to_value(&snapshot).unwrap();
            serde_value[key.as_str()] = serde_json::json!(5);
            snapshot = serde_json::from_value(serde_value).unwrap();
            let json = to_json(snapshot);
            assert_eq!(
                identity_keys(&json),
                vec![key.clone()],
                "only {key} is sent"
            );
            assert_eq!(json[key.as_str()], 5, "value for {key}");
        }
    }

    /// The event-log JSON carries each inbound concurrency value under its
    /// own key (distinct non-zero values, so a swapped key is caught), with
    /// the value serde gives.
    #[test]
    fn transport_snapshot_json_carries_inbound_concurrency_values() {
        let snapshot = crate::transport::metrics::TransportSnapshot {
            inbound_concurrency_conns_sampled: 81,
            inbound_concurrency_conns_ge_8: 82,
            inbound_concurrency_conns_ge_16: 83,
            inbound_concurrency_conns_ge_32: 84,
            inbound_concurrency_conns_ge_64: 85,
            inbound_concurrency_conns_ge_128: 86,
            inbound_concurrency_conns_ge_256: 87,
            inbound_concurrency_max: 88,
            ..Default::default()
        };
        let reference = serde_json::to_value(&snapshot).unwrap();
        let json = event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
            snapshot,
        )));
        for (key, expected) in [
            ("inbound_concurrency_conns_sampled", 81),
            ("inbound_concurrency_conns_ge_8", 82),
            ("inbound_concurrency_conns_ge_16", 83),
            ("inbound_concurrency_conns_ge_32", 84),
            ("inbound_concurrency_conns_ge_64", 85),
            ("inbound_concurrency_conns_ge_128", 86),
            ("inbound_concurrency_conns_ge_256", 87),
            ("inbound_concurrency_max", 88),
        ] {
            assert_eq!(json[key], expected, "event-log value for {key}");
            assert_eq!(json[key], reference[key], "serde value for {key}");
        }
    }

    /// Each inbound concurrency key is in the event-log JSON exactly when
    /// its value is non-zero, and every serde `inbound_concurrency_*` field
    /// is covered.
    #[test]
    fn transport_snapshot_json_sends_inbound_concurrency_keys_only_when_nonzero() {
        let to_json = |snapshot: crate::transport::metrics::TransportSnapshot| {
            event_kind_to_json(&super::super::EventKind::TransportSnapshot(Box::new(
                snapshot,
            )))
        };
        let concurrency_keys = |json: &serde_json::Value| -> Vec<String> {
            json.as_object()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with("inbound_concurrency_"))
                .cloned()
                .collect()
        };
        let zero = crate::transport::metrics::TransportSnapshot::default();
        assert!(concurrency_keys(&to_json(zero.clone())).is_empty());

        let serde_keys = concurrency_keys(&serde_json::to_value(&zero).unwrap());
        assert_eq!(serde_keys.len(), 8, "one serde field per value");
        for key in serde_keys {
            let mut serde_value = serde_json::to_value(&zero).unwrap();
            serde_value[key.as_str()] = serde_json::json!(5);
            let snapshot = serde_json::from_value(serde_value).unwrap();
            let json = to_json(snapshot);
            assert_eq!(
                concurrency_keys(&json),
                vec![key.clone()],
                "only {key} is sent"
            );
            assert_eq!(json[key.as_str()], 5, "value for {key}");
        }
    }
}
