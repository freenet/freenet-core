//! Telemetry reporter that sends events to a central OpenTelemetry collector.
//!
//! This module provides remote telemetry reporting to help debug network issues.
//! It sends operation events (connect, put, get, subscribe, update) to a central
//! collector via OTLP/HTTP protocol.
//!
//! Features:
//! - Exponential backoff on connection failures (1s → 2s → 4s → ... → 5min max)
//! - Event batching (send every 10 seconds or when buffer reaches 100 events)
//! - Rate limiting (max 10 events/second aggregate), with a separate shadow
//!   sub-budget (max 4/second) so always-on low-priority shadow telemetry
//!   cannot starve operational telemetry (#4380)
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

use std::sync::OnceLock;
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
use crate::router::RouteEvent;
use crate::transport::TRANSPORT_METRICS;

use super::{EventKind, NetEventLog, NetEventRegister, NetLogMessage};

/// Global telemetry sender for standalone event emission from contexts
/// that don't have access to a `NetEventRegister` (e.g., background tasks
/// like subscription renewal and interest sweep).
///
/// Initialized when `TelemetryReporter::new()` creates the reporter.
/// If telemetry is disabled, this remains unset and `send_standalone_event`
/// silently drops events.
static TELEMETRY_SENDER: OnceLock<mpsc::Sender<TelemetryCommand>> = OnceLock::new();

/// Admission priority for a telemetry event.
///
/// The per-second rate limiter ([`TelemetryWorker::handle_event`]) enforces a
/// flat aggregate cap shared by every event. Without a priority distinction,
/// always-on background "shadow" telemetry (the #4074 floor-analysis emitters)
/// competes for the same slots as operational net-events (GET/PUT/SUBSCRIBE
/// results, routing decisions, connection state). On a busy node the shadow
/// stream (3–5 events/sec) deterministically consumed 30–50% of the budget,
/// crowding out exactly the operational telemetry an operator needs when the
/// network is busiest (#4380).
///
/// `Shadow` events are admitted under a *separate, smaller* sub-budget
/// ([`MAX_SHADOW_EVENTS_PER_SECOND`]) carved out of the aggregate cap, so they
/// can never starve `Operational` telemetry. The sub-budget is best-effort:
/// excess shadow events are dropped silently, same as any other rate-limited
/// event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventPriority {
    /// Operational net-events and node telemetry. Admitted up to the full
    /// aggregate cap. This is the default for everything except the
    /// always-on shadow emitters.
    #[default]
    Operational,
    /// Always-on background "shadow" telemetry (#4074 Phase 1.x floor
    /// analysis). Admitted only under the shadow sub-budget so it yields to
    /// `Operational` events under load.
    Shadow,
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
/// Emitted at [`EventPriority::Operational`]. For always-on background
/// "shadow" telemetry, use [`send_standalone_shadow_event_with_peer_id`].
pub fn send_standalone_event(event_type: &str, event_data: serde_json::Value) {
    send_standalone_event_inner(event_type, "", event_data, EventPriority::Operational);
}

/// Send a standalone telemetry event tagged with the local node's peer id.
///
/// Used by node-level periodic emitters whose output is only meaningful when
/// the collector can disaggregate samples by source node. The `peer_id` is
/// set as the OTLP record attribute so the collector can group/filter without
/// parsing the event body.
///
/// Emitted at [`EventPriority::Operational`]. For always-on background
/// "shadow" telemetry, use [`send_standalone_shadow_event_with_peer_id`].
pub fn send_standalone_event_with_peer_id(
    event_type: &str,
    peer_id: &str,
    event_data: serde_json::Value,
) {
    send_standalone_event_inner(event_type, peer_id, event_data, EventPriority::Operational);
}

/// Send a low-priority "shadow" telemetry event tagged with the local node's
/// peer id.
///
/// Used by the always-on #4074 floor-analysis emitters (`shadow_rtt_aggregate`,
/// `shadow_reference_ping`, `shadow_rate_demand`, `shadow_outbound_class`,
/// `shadow_iface_tx`). Emitted at [`EventPriority::Shadow`] so the rate limiter
/// admits them only under the shadow sub-budget and they cannot starve
/// operational telemetry (#4380).
pub fn send_standalone_shadow_event_with_peer_id(
    event_type: &str,
    peer_id: &str,
    event_data: serde_json::Value,
) {
    send_standalone_event_inner(event_type, peer_id, event_data, EventPriority::Shadow);
}

fn send_standalone_event_inner(
    event_type: &str,
    peer_id: &str,
    event_data: serde_json::Value,
    priority: EventPriority,
) {
    if let Some(sender) = TELEMETRY_SENDER.get() {
        let event = TelemetryEvent {
            timestamp: current_timestamp_ms(),
            peer_id: peer_id.to_string(),
            transaction_id: String::new(),
            event_type: event_type.to_string(),
            event_data,
            priority,
        };
        // Fire-and-forget: channel full means telemetry event is dropped
        #[allow(clippy::let_underscore_must_use)]
        let _ = sender.try_send(TelemetryCommand::Event(event));
    }
}

/// Maximum number of events to buffer before sending
const MAX_BUFFER_SIZE: usize = 100;

/// How often to send batched events (in seconds)
const BATCH_INTERVAL_SECS: u64 = 10;

/// Maximum events per second (aggregate rate limiting across all priorities)
const MAX_EVENTS_PER_SECOND: usize = 10;

/// Maximum *shadow* (low-priority) events per second.
///
/// Shadow telemetry is admitted under this sub-budget, carved out of the
/// aggregate [`MAX_EVENTS_PER_SECOND`] cap, so that always-on background
/// emitters cannot starve operational telemetry (#4380). With the current
/// always-on baseline of 3 shadow events/sec (`shadow_rtt_aggregate` +
/// `shadow_rate_demand` + `shadow_outbound_class`), rising to 5/sec on a
/// gateway that also enables reference-ping and iface-tx, a cap of 4 admits
/// the always-on set while still reserving at least
/// `MAX_EVENTS_PER_SECOND - MAX_SHADOW_EVENTS_PER_SECOND` (= 6) slots/sec for
/// operational events even when shadow demand spikes (e.g. Phase 2 adds the
/// shadow controller decision log). Shadow events still also count against the
/// aggregate cap.
const MAX_SHADOW_EVENTS_PER_SECOND: usize = 4;

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
fn running_under_cargo_test() -> bool {
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

        // Store a clone in the global sender for standalone event emission
        // OnceLock::set returns Err if already initialized; expected on repeated calls
        #[allow(clippy::let_underscore_must_use)]
        let _ = TELEMETRY_SENDER.set(sender.clone());

        // Initialize the transfer event channel for per-transfer telemetry
        let transfer_event_receiver = crate::transport::metrics::init_transfer_event_channel();

        // Spawn the background worker
        let worker = TelemetryWorker::new(
            endpoint,
            receiver,
            transport_snapshot_interval_secs,
            transfer_event_receiver,
            local_peer_id.clone(),
        );
        GlobalExecutor::spawn(worker.run());

        Some(Self {
            sender,
            local_peer_id,
        })
    }

    async fn send_event(&self, event: TelemetryEvent) {
        // Fire-and-forget: non-blocking send, drop if channel is full
        #[allow(clippy::let_underscore_must_use)]
        let _ = self.sender.try_send(TelemetryCommand::Event(event));
    }
}

impl NetEventRegister for TelemetryReporter {
    fn register_events<'a>(
        &'a self,
        logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
    ) -> BoxFuture<'a, ()> {
        async move {
            for log_msg in NetLogMessage::to_log_message(logs) {
                let event = TelemetryEvent {
                    timestamp: current_timestamp_ms(),
                    peer_id: log_msg.peer_id.to_string(),
                    transaction_id: log_msg.tx.to_string(),
                    event_type: event_kind_to_string(&log_msg.kind),
                    event_data: event_kind_to_json(&log_msg.kind),
                    priority: EventPriority::Operational,
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
                priority: EventPriority::Operational,
            };
            // Fire-and-forget: channel full means telemetry event is dropped
            #[allow(clippy::let_underscore_must_use)]
            let _ = sender.try_send(TelemetryCommand::Event(event));
        }
        .boxed()
    }

    fn get_router_events(&self, _number: usize) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
        // Telemetry reporter doesn't store events locally
        async { Ok(vec![]) }.boxed()
    }
}

/// Background worker that batches and sends telemetry events.
struct TelemetryWorker {
    endpoint: String,
    receiver: mpsc::Receiver<TelemetryCommand>,
    buffer: Vec<TelemetryEvent>,
    http_client: reqwest::Client,
    backoff_ms: u64,
    last_send: Instant,
    events_this_second: usize,
    /// Shadow (low-priority) events admitted in the current 1s window.
    /// Bounded by [`MAX_SHADOW_EVENTS_PER_SECOND`] so shadow telemetry cannot
    /// starve operational telemetry (#4380). Reset alongside
    /// `events_this_second` when the window rolls over.
    shadow_events_this_second: usize,
    rate_limit_window_start: Instant,
    /// Interval for transport snapshots (0 = disabled)
    transport_snapshot_interval_secs: u64,
    /// Receiver for per-transfer telemetry events from transport layer
    transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
    /// This node's own peer id — see `TelemetryReporter::local_peer_id`.
    local_peer_id: String,
}

impl TelemetryWorker {
    fn new(
        endpoint: String,
        receiver: mpsc::Receiver<TelemetryCommand>,
        transport_snapshot_interval_secs: u64,
        transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
        local_peer_id: String,
    ) -> Self {
        Self {
            endpoint,
            receiver,
            buffer: Vec::with_capacity(MAX_BUFFER_SIZE),
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("failed to build telemetry HTTP client; check TLS/proxy configuration"),
            backoff_ms: 0,
            last_send: Instant::now(),
            events_this_second: 0,
            shadow_events_this_second: 0,
            rate_limit_window_start: Instant::now(),
            transport_snapshot_interval_secs,
            transfer_event_receiver,
            local_peer_id,
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
            event_data: serde_json::to_value(snapshot).unwrap_or_default(),
            priority: EventPriority::Operational,
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
            priority: EventPriority::Operational,
        }
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
            priority: EventPriority::Operational,
        }
    }

    async fn run(mut self) {
        let mut batch_interval = tokio::time::interval(Duration::from_secs(BATCH_INTERVAL_SECS));

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

        loop {
            crate::deterministic_select! {
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
                    // A1) on the batch cadence, so a peer's memory/CPU/bandwidth
                    // headroom is observable on the telemetry stream the
                    // dashboard consumes. Cheap process-local sources; buffered
                    // like any other event, then flushed below.
                    let resource_event = self.resource_utilization_telemetry();
                    self.handle_event(resource_event).await;
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
    /// Pulled out of [`Self::handle_event`] so the admission policy (the
    /// #4380 shadow sub-budget) is unit-testable without driving the async
    /// buffer/backoff/HTTP machinery. Returns `true` if the event is admitted.
    ///
    /// Policy:
    /// - The window rolls over once `now` is >= 1s past its start, resetting
    ///   both counters.
    /// - `Operational` events are admitted while the aggregate count is below
    ///   [`MAX_EVENTS_PER_SECOND`] — unchanged from the pre-#4380 behavior.
    /// - `Shadow` events are admitted only while BOTH the shadow sub-budget
    ///   ([`MAX_SHADOW_EVENTS_PER_SECOND`]) AND the aggregate cap have room.
    ///   Because the sub-budget (4) is strictly below the aggregate cap (10),
    ///   shadow events can occupy at most `MAX_SHADOW_EVENTS_PER_SECOND` of
    ///   the slots, always leaving
    ///   `MAX_EVENTS_PER_SECOND - MAX_SHADOW_EVENTS_PER_SECOND` for
    ///   operational telemetry.
    fn admit_event(&mut self, priority: EventPriority, now: Instant) -> bool {
        if now.duration_since(self.rate_limit_window_start) >= Duration::from_secs(1) {
            self.rate_limit_window_start = now;
            self.events_this_second = 0;
            self.shadow_events_this_second = 0;
        }

        // Aggregate cap applies to every event regardless of priority.
        if self.events_this_second >= MAX_EVENTS_PER_SECOND {
            return false;
        }

        // Shadow events additionally yield to the sub-budget so they cannot
        // crowd out operational telemetry (#4380).
        if priority == EventPriority::Shadow {
            if self.shadow_events_this_second >= MAX_SHADOW_EVENTS_PER_SECOND {
                return false;
            }
            self.shadow_events_this_second += 1;
        }

        self.events_this_second += 1;
        true
    }

    async fn handle_event(&mut self, event: TelemetryEvent) {
        // Rate limiting (#4380: shadow events admitted under a sub-budget)
        if !self.admit_event(event.priority, Instant::now()) {
            // Drop event due to rate limiting
            return;
        }

        // Drop events if buffer is full and we're in backoff (prevents unbounded memory growth)
        if self.buffer.len() >= MAX_BUFFER_SIZE && self.backoff_ms > 0 {
            let elapsed = self.last_send.elapsed();
            if elapsed < Duration::from_millis(self.backoff_ms) {
                // Still in backoff and buffer full - drop event
                return;
            }
        }

        self.buffer.push(event);

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
        self.buffer = Vec::with_capacity(MAX_BUFFER_SIZE);

        match self.send_batch(&events).await {
            Ok(()) => {
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

                // Put events back in buffer (up to limit)
                let remaining_capacity = MAX_BUFFER_SIZE.saturating_sub(self.buffer.len());
                self.buffer
                    .extend(events.into_iter().take(remaining_capacity));
            }
        }
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
            serde_json::json!({
                "type": "transport_snapshot",
                "transfers_completed": snapshot.transfers_completed,
                "transfers_failed": snapshot.transfers_failed,
                "bytes_sent": snapshot.bytes_sent,
                "bytes_received": snapshot.bytes_received,
                "avg_transfer_time_ms": snapshot.avg_transfer_time_ms,
                "peak_throughput_bps": snapshot.peak_throughput_bps,
                "avg_cwnd_bytes": snapshot.avg_cwnd_bytes,
                "peak_cwnd_bytes": snapshot.peak_cwnd_bytes,
                "min_cwnd_bytes": snapshot.min_cwnd_bytes,
                "slowdowns_triggered": snapshot.slowdowns_triggered,
                "avg_rtt_us": snapshot.avg_rtt_us,
                "min_rtt_us": snapshot.min_rtt_us,
                "max_rtt_us": snapshot.max_rtt_us,
            })
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
                "refresh_router_last_success_age_secs": snapshot.refresh_router_last_success_age_secs,
                "refresh_router_consecutive_failures": snapshot.refresh_router_consecutive_failures,
                "per_op_curves": snapshot.per_op_curves,
            });
            // Placement-quality + placement-migration gauges (#4404 follow-up).
            // Inserted AFTER the `json!` (not as inline keys) only to keep the
            // macro under its expansion recursion limit — this body is still
            // hand-mirrored, so a new `RouterSnapshotInfo` field is invisible to
            // the collector unless added here. Pinned by
            // `router_snapshot_json_includes_placement_gauges`.
            if let Some(obj) = body.as_object_mut() {
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

    /// Pin: the UPDATE-broadcast stream-assembly gauges and the background-task
    /// health gauges (#4440) must also reach the hand-mirrored OTLP body — same
    /// footgun as the fd / module-cache gauges above. The broadcast
    /// stream-assembly failure rate is the signal that flagged the v0.2.73
    /// incident, so a silent drop here would re-blind us to a re-enable going
    /// wrong.
    #[test]
    fn router_snapshot_json_includes_broadcast_and_bgtask_gauges() {
        use arbitrary::{Arbitrary, Unstructured};
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut info = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        info.broadcast_stream_attempts_total = Some(37);
        info.broadcast_stream_failures_total = Some(41);
        info.broadcast_stream_failures_last_snapshot = Some(43);
        info.refresh_router_last_success_age_secs = Some(47);
        info.refresh_router_consecutive_failures = Some(53);
        let json = event_kind_to_json(&EventKind::RouterSnapshot(Box::new(info)));
        for (key, want) in [
            ("broadcast_stream_attempts_total", 37),
            ("broadcast_stream_failures_total", 41),
            ("broadcast_stream_failures_last_snapshot", 43),
            ("refresh_router_last_success_age_secs", 47),
            ("refresh_router_consecutive_failures", 53),
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
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
        for (key, want) in [
            ("hosted_key_distance_median", 0.1),
            ("hosted_key_distance_p90", 0.4),
            ("hosted_key_distance_min", 0.0),
            ("hosted_key_distance_mean", 0.15),
            ("hosted_key_distance_frac_within_0_1", 0.6),
        ] {
            assert_eq!(json[key], want, "{key} must reach the OTLP body");
        }
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
            priority: EventPriority::Shadow,
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
        );
        let snapshot = crate::transport::metrics::TransportSnapshot::default();
        let event = worker.snapshot_to_telemetry(&snapshot);
        assert_eq!(event.event_type, "transport_snapshot");
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
            EventPriority::Operational,
            "resource-utilization is operational telemetry, not shadow"
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

    #[tokio::test]
    async fn test_timeout_events_carry_local_peer_id() {
        // #4345 observability gap, same class as transfer_failed:
        // timeout events carried an empty peer_id.
        use crate::message::Transaction;
        use crate::operations::get::GetMsg;

        let (sender, mut receiver) = mpsc::channel(1);
        let mut reporter = TelemetryReporter {
            sender,
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

    // ----- #4380: shadow sub-budget rate limiting -----

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
        )
    }

    /// Admit `count` events of `priority` at instant `now`, returning how many
    /// were admitted.
    fn admit_n(
        worker: &mut TelemetryWorker,
        priority: EventPriority,
        count: usize,
        now: Instant,
    ) -> usize {
        (0..count)
            .filter(|_| worker.admit_event(priority, now))
            .count()
    }

    #[test]
    fn test_shadow_subbudget_caps_shadow_events() {
        // Sanity-check the constants the policy depends on: the sub-budget
        // must be strictly below the aggregate cap, otherwise shadow events
        // could consume the whole budget and the carve-out is meaningless.
        const {
            assert!(
                MAX_SHADOW_EVENTS_PER_SECOND < MAX_EVENTS_PER_SECOND,
                "shadow sub-budget must leave room for operational events"
            );
        }

        let mut worker = rate_limit_test_worker();
        let now = Instant::now();

        // A flood of shadow-only events is capped at the sub-budget, never the
        // full aggregate cap.
        let admitted = admit_n(&mut worker, EventPriority::Shadow, 100, now);
        assert_eq!(
            admitted, MAX_SHADOW_EVENTS_PER_SECOND,
            "shadow events must be capped at the sub-budget within a window"
        );
    }

    #[test]
    fn test_shadow_flood_cannot_starve_operational() {
        // Regression for #4380: before the sub-budget, a burst of shadow
        // events arriving first in a window could consume the entire flat cap,
        // dropping operational telemetry. Now shadow is capped at the
        // sub-budget, leaving (cap - sub-budget) slots for operational events.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();

        // Shadow events arrive first and try to grab everything.
        let shadow_admitted = admit_n(&mut worker, EventPriority::Shadow, 50, now);
        assert_eq!(shadow_admitted, MAX_SHADOW_EVENTS_PER_SECOND);

        // Operational events arriving afterwards still get the slots the
        // shadow sub-budget reserved for them.
        let expected_operational = MAX_EVENTS_PER_SECOND - MAX_SHADOW_EVENTS_PER_SECOND;
        let op_admitted = admit_n(&mut worker, EventPriority::Operational, 50, now);
        assert_eq!(
            op_admitted, expected_operational,
            "operational events must keep (aggregate cap - shadow sub-budget) \
             slots even when shadow floods first (#4380)"
        );
    }

    #[test]
    fn test_operational_can_use_full_aggregate_cap() {
        // Operational telemetry is unchanged from pre-#4380: it may use the
        // entire aggregate cap when there are no shadow events.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();

        let admitted = admit_n(&mut worker, EventPriority::Operational, 100, now);
        assert_eq!(
            admitted, MAX_EVENTS_PER_SECOND,
            "operational events alone must be able to fill the aggregate cap"
        );
    }

    #[test]
    fn test_shadow_still_bounded_by_aggregate_cap() {
        // The aggregate cap is the hard ceiling: if operational events have
        // already filled it, no shadow event is admitted even though the
        // shadow sub-budget still nominally has room.
        let mut worker = rate_limit_test_worker();
        let now = Instant::now();

        let op_admitted = admit_n(
            &mut worker,
            EventPriority::Operational,
            MAX_EVENTS_PER_SECOND,
            now,
        );
        assert_eq!(op_admitted, MAX_EVENTS_PER_SECOND);

        // Sub-budget untouched, but the aggregate cap is exhausted.
        let shadow_admitted = admit_n(&mut worker, EventPriority::Shadow, 10, now);
        assert_eq!(
            shadow_admitted, 0,
            "shadow events must still respect the aggregate cap, not just the \
             sub-budget"
        );
    }

    #[test]
    fn test_rate_limit_window_resets_both_counters() {
        // After the 1s window rolls over, both the aggregate and shadow
        // counters reset, so a new window admits a fresh budget of each.
        let mut worker = rate_limit_test_worker();
        let t0 = Instant::now();

        // Fill the aggregate cap (some shadow, some operational) in window 0.
        assert_eq!(
            admit_n(&mut worker, EventPriority::Shadow, 10, t0),
            MAX_SHADOW_EVENTS_PER_SECOND
        );
        assert_eq!(
            admit_n(&mut worker, EventPriority::Operational, 10, t0),
            MAX_EVENTS_PER_SECOND - MAX_SHADOW_EVENTS_PER_SECOND
        );
        // Window 0 is now full.
        assert!(!worker.admit_event(EventPriority::Operational, t0));

        // Roll the window over (exactly 1s is the boundary: >= 1s resets).
        let t1 = t0 + Duration::from_secs(1);
        assert_eq!(
            admit_n(&mut worker, EventPriority::Shadow, 10, t1),
            MAX_SHADOW_EVENTS_PER_SECOND,
            "shadow sub-budget must refresh on window rollover"
        );
        assert_eq!(
            admit_n(&mut worker, EventPriority::Operational, 10, t1),
            MAX_EVENTS_PER_SECOND - MAX_SHADOW_EVENTS_PER_SECOND,
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
                MAX_EVENTS_PER_SECOND,
                t0
            ),
            MAX_EVENTS_PER_SECOND
        );

        let almost = t0 + Duration::from_millis(999);
        assert!(
            !worker.admit_event(EventPriority::Operational, almost),
            "window must not reset before a full second elapses"
        );
    }

    #[test]
    fn test_priority_default_is_operational() {
        // The default priority must be Operational so any event constructed
        // without explicitly opting into Shadow keeps the full budget.
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
            priority: EventPriority::Shadow,
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
}
