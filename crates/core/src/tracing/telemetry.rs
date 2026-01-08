//! Telemetry reporter that sends events to a central OpenTelemetry collector.
//!
//! This module provides remote telemetry reporting to help debug network issues.
//! It sends operation events (connect, put, get, subscribe, update) to a central
//! collector via OTLP/HTTP protocol.
//!
//! Features:
//! - Exponential backoff on connection failures (1s → 2s → 4s → ... → 5min max)
//! - Event batching (send every 10 seconds or when buffer reaches 100 events)
//! - Rate limiting (max 10 events/second aggregate)
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

use std::time::Duration;
use tokio::time::Instant;

use either::Either;
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::config::GlobalRng;

use crate::config::{GlobalExecutor, TelemetryConfig};
use crate::message::Transaction;
use crate::router::RouteEvent;
use crate::transport::TRANSPORT_METRICS;

use super::{EventKind, NetEventLog, NetEventRegister, NetLogMessage};

/// Maximum number of events to buffer before sending
const MAX_BUFFER_SIZE: usize = 100;

/// How often to send batched events (in seconds)
const BATCH_INTERVAL_SECS: u64 = 10;

/// Maximum events per second (rate limiting)
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
}

impl TelemetryReporter {
    /// Create a new telemetry reporter.
    ///
    /// Returns None if telemetry is disabled or in a test environment.
    pub fn new(config: &TelemetryConfig) -> Option<Self> {
        if !config.enabled {
            tracing::info!("Telemetry reporting is disabled");
            return None;
        }

        // Disable telemetry in test environments (detected via --id flag) to avoid
        // flooding the collector with CI/integration test data.
        if config.is_test_environment {
            tracing::info!("Telemetry disabled in test environment (--id flag detected)");
            return None;
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

        // Initialize the transfer event channel for per-transfer telemetry
        let transfer_event_receiver = crate::transport::metrics::init_transfer_event_channel();

        // Spawn the background worker
        let worker = TelemetryWorker::new(
            endpoint,
            receiver,
            transport_snapshot_interval_secs,
            transfer_event_receiver,
        );
        GlobalExecutor::spawn(worker.run());

        Some(Self { sender })
    }

    async fn send_event(&self, event: TelemetryEvent) {
        // Non-blocking send - drop if channel is full
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
                };
                self.send_event(event).await;
            }
        }
        .boxed()
    }

    fn trait_clone(&self) -> Box<dyn NetEventRegister> {
        Box::new(self.clone())
    }

    fn notify_of_time_out(&mut self, tx: Transaction) -> BoxFuture<'_, ()> {
        let sender = self.sender.clone();
        async move {
            let event = TelemetryEvent {
                timestamp: current_timestamp_ms(),
                peer_id: String::new(),
                transaction_id: tx.to_string(),
                event_type: "timeout".to_string(),
                event_data: serde_json::json!({"transaction": tx.to_string()}),
            };
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
    rate_limit_window_start: Instant,
    /// Interval for transport snapshots (0 = disabled)
    transport_snapshot_interval_secs: u64,
    /// Receiver for per-transfer telemetry events from transport layer
    transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
}

impl TelemetryWorker {
    fn new(
        endpoint: String,
        receiver: mpsc::Receiver<TelemetryCommand>,
        transport_snapshot_interval_secs: u64,
        transfer_event_receiver: mpsc::Receiver<super::TransferEvent>,
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
            rate_limit_window_start: Instant::now(),
            transport_snapshot_interval_secs,
            transfer_event_receiver,
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
                        let event_type = match &transfer_event {
                            super::TransferEvent::Started { .. } => "transfer_started",
                            super::TransferEvent::Completed { .. } => "transfer_completed",
                            super::TransferEvent::Failed { .. } => "transfer_failed",
                        };
                        let event = TelemetryEvent {
                            timestamp: current_timestamp_ms(),
                            peer_id: String::new(), // Transport layer doesn't have peer identity
                            transaction_id: String::new(), // Not available at transport layer
                            event_type: event_type.to_string(),
                            event_data: event_kind_to_json(&super::EventKind::Transfer(transfer_event)),
                        };
                        self.handle_event(event).await;
                    }
                },
                _ = batch_interval.tick() => {
                    self.flush().await;
                },
                _ = transport_snapshot_interval.tick() => {
                    // Emit transport layer metrics snapshot (only if enabled)
                    if self.transport_snapshot_interval_secs > 0 {
                        if let Some(snapshot) = TRANSPORT_METRICS.take_snapshot() {
                            let event = TelemetryEvent {
                                timestamp: current_timestamp_ms(),
                                peer_id: String::new(), // Transport metrics are node-wide, not peer-specific
                                transaction_id: String::new(), // Not tied to a transaction
                                event_type: "transport_snapshot".to_string(),
                                event_data: serde_json::to_value(&snapshot).unwrap_or_default(),
                            };
                            self.handle_event(event).await;
                        }
                    }
                },
            }
        }
    }

    async fn handle_event(&mut self, event: TelemetryEvent) {
        // Rate limiting
        let now = Instant::now();
        if now.duration_since(self.rate_limit_window_start) >= Duration::from_secs(1) {
            self.rate_limit_window_start = now;
            self.events_this_second = 0;
        }

        if self.events_this_second >= MAX_EVENTS_PER_SECOND {
            // Drop event due to rate limiting
            return;
        }
        self.events_this_second += 1;

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
        let otlp_payload = self.to_otlp_logs(events);

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

    fn to_otlp_logs(&self, events: &[TelemetryEvent]) -> serde_json::Value {
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
                    "attributes": [{
                        "key": "service.name",
                        "value": {"stringValue": "freenet-peer"}
                    }]
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
            }
        }
        EventKind::Subscribe(subscribe_event) => {
            use super::SubscribeEvent;
            match subscribe_event {
                SubscribeEvent::Request { .. } => "subscribe_request".to_string(),
                SubscribeEvent::SubscribeSuccess { .. } => "subscribe_success".to_string(),
                SubscribeEvent::SubscribeNotFound { .. } => "subscribe_not_found".to_string(),
                SubscribeEvent::ResponseSent { .. } => "subscribe_response_sent".to_string(),
                SubscribeEvent::SeedingStarted { .. } => "seeding_started".to_string(),
                SubscribeEvent::SeedingStopped { .. } => "seeding_stopped".to_string(),
                SubscribeEvent::DownstreamAdded { .. } => "downstream_added".to_string(),
                SubscribeEvent::DownstreamRemoved { .. } => "downstream_removed".to_string(),
                SubscribeEvent::UpstreamSet { .. } => "upstream_set".to_string(),
                SubscribeEvent::Unsubscribed { .. } => "unsubscribed".to_string(),
                SubscribeEvent::SubscriptionState { .. } => "subscription_state".to_string(),
            }
        }
        EventKind::Update(update_event) => {
            use super::UpdateEvent;
            match update_event {
                UpdateEvent::Request { .. } => "update_request".to_string(),
                UpdateEvent::UpdateSuccess { .. } => "update_success".to_string(),
                UpdateEvent::BroadcastEmitted { .. } => "update_broadcast_emitted".to_string(),
                UpdateEvent::BroadcastReceived { .. } => "update_broadcast_received".to_string(),
                UpdateEvent::BroadcastApplied { .. } => "update_broadcast_applied".to_string(),
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
        EventKind::Route(_) => "route".to_string(),
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
                "from_peer_id": from.pub_key.to_string(),
                "from_peer_addr": from.addr.to_string(),
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
                SubscribeEvent::SeedingStarted {
                    instance_id,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "seeding_started",
                        "instance_id": instance_id.to_string(),
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::SeedingStopped {
                    instance_id,
                    reason,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "seeding_stopped",
                        "instance_id": instance_id.to_string(),
                        "reason": format!("{:?}", reason),
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::DownstreamAdded {
                    key,
                    subscriber,
                    downstream_count,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "downstream_added",
                        "key": key.to_string(),
                        "subscriber": subscriber.to_string(),
                        "downstream_count": downstream_count,
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::DownstreamRemoved {
                    key,
                    subscriber,
                    reason,
                    downstream_count,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "downstream_removed",
                        "key": key.to_string(),
                        "subscriber": subscriber.as_ref().map(|s| s.to_string()),
                        "reason": format!("{:?}", reason),
                        "downstream_count": downstream_count,
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::UpstreamSet {
                    key,
                    upstream,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "upstream_set",
                        "key": key.to_string(),
                        "upstream": upstream.to_string(),
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::Unsubscribed {
                    key,
                    reason,
                    upstream,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "unsubscribed",
                        "key": key.to_string(),
                        "reason": format!("{:?}", reason),
                        "upstream": upstream.as_ref().map(|u| u.to_string()),
                        "timestamp": timestamp,
                    })
                }
                SubscribeEvent::SubscriptionState {
                    key,
                    is_seeding,
                    upstream,
                    downstream_count,
                    downstream,
                    timestamp,
                } => {
                    serde_json::json!({
                        "type": "subscription_state",
                        "key": key.to_string(),
                        "is_seeding": is_seeding,
                        "upstream": upstream.as_ref().map(|u| u.to_string()),
                        "downstream_count": downstream_count,
                        "downstream": downstream.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
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
                } => {
                    let mut json = serde_json::json!({
                        "type": "broadcast_applied",
                        "id": id.to_string(),
                        "key": key.to_string(),
                        "target": target.to_string(),
                        "timestamp": timestamp,
                        "changed": changed,
                    });
                    if let Some(hash) = state_hash_before {
                        json["state_hash_before"] = serde_json::Value::String(hash.clone());
                    }
                    if let Some(hash) = state_hash_after {
                        json["state_hash_after"] = serde_json::Value::String(hash.clone());
                    }
                    json
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
            serde_json::json!({
                "type": "route",
                "event": format!("{:?}", route_event),
            })
        }
        EventKind::Ignored => {
            serde_json::json!({"type": "ignored"})
        }
        EventKind::Timeout { id, timestamp } => {
            serde_json::json!({
                "type": "timeout",
                "id": id.to_string(),
                "timestamp": timestamp,
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
