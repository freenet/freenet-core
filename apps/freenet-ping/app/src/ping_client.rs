use std::collections::HashMap;
use std::time::{Duration, Instant};

use freenet_ping_types::chrono::{DateTime, Utc};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::client_api::{
    ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi,
};
use freenet_stdlib::prelude::*;
use tokio::time::timeout;

/// Statistics collected during a ping client session
#[derive(Debug, Default)]
pub struct PingStats {
    /// Number of pings sent
    pub sent_count: usize,
    /// Count of pings received from each peer
    pub received_counts: HashMap<String, usize>,
    /// Last update time for each peer
    pub last_updates: HashMap<String, DateTime<Utc>>,
}

impl PingStats {
    pub fn new() -> Self {
        Self {
            sent_count: 0,
            received_counts: HashMap::new(),
            last_updates: HashMap::new(),
        }
    }

    pub fn record_sent(&mut self) {
        self.sent_count += 1;
    }

    pub fn record_received(&mut self, peer: String, time: Vec<DateTime<Utc>>) {
        *self.received_counts.entry(peer.clone()).or_insert(0) += 1;
        if let Some(latest) = time.first() {
            self.last_updates.insert(peer, *latest);
        }
    }
}

// Wait for a PUT response with the expected key
pub async fn wait_for_put_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<ContractKey, Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(60), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                if &key == expected_key {
                    return Ok(key);
                } else {
                    return Err("unexpected key".into());
                }
            }
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                key,
                summary,
            }))) => {
                if &key == expected_key {
                    tracing::info!(
                        "Received update response for key: {}, summary: {:?}",
                        key,
                        summary
                    );
                    return Ok(key);
                } else {
                    return Err("unexpected key".into());
                }
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for put: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                return Err("timeout waiting for put response".into());
            }
        }
    }
}

// Wait for a GET response with the expected key and return the deserialized Ping state
// Has an overall timeout of 60 seconds to prevent getting stuck on UpdateNotification floods
pub async fn wait_for_get_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<Ping, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut skipped_notifications = 0;

    loop {
        // Check overall timeout
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "timeout waiting for get response (skipped {} other messages)",
                skipped_notifications
            )
            .into());
        }

        // Use the smaller of remaining time or 5 seconds for per-recv timeout
        let recv_timeout = remaining.min(Duration::from_secs(5));
        let resp = timeout(recv_timeout, client.recv()).await;

        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: _,
                state,
            }))) => {
                if &key != expected_key {
                    return Err("unexpected key".into());
                }

                if skipped_notifications > 0 {
                    tracing::debug!(
                        "Received GetResponse after skipping {} other messages",
                        skipped_notifications
                    );
                }

                match serde_json::from_slice::<Ping>(&state) {
                    Ok(ping) => {
                        tracing::info!(num_entries = %ping.len(), "old state fetched successfully!");
                        return Ok(ping);
                    }
                    Err(e) => {
                        tracing::error!("Failed to deserialize Ping: {}", e);
                        tracing::error!("Raw state data: {:?}", String::from_utf8_lossy(&state));
                        return Err(Box::new(e));
                    }
                };
            }
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                ..
            }))) => {
                // Silently skip update notifications (expected when subscribed)
                skipped_notifications += 1;
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for get: {}", other);
                skipped_notifications += 1;
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                // Per-recv timeout, continue checking overall deadline
                continue;
            }
        }
    }
}

// Wait for a SUBSCRIBE response with the expected key
pub async fn wait_for_subscribe_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(60), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::SubscribeResponse {
                key,
                subscribed,
                ..
            }))) => {
                if &key != expected_key {
                    return Err("unexpected key".into());
                }

                if subscribed {
                    return Ok(());
                } else {
                    return Err("failed to subscribe".into());
                }
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for subscribe: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                return Err("timeout waiting for subscribe response".into());
            }
        }
    }
}

// Wait for an UPDATE response with the expected key
pub async fn wait_for_update_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<ContractKey, Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(60), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                key,
                summary,
            }))) => {
                if &key == expected_key {
                    tracing::debug!(
                        "Received update response for key: {}, summary: {:?}",
                        key,
                        summary
                    );
                    return Ok(key);
                } else {
                    return Err(
                        format!("unexpected key: expected {}, got {}", expected_key, key).into(),
                    );
                }
            }
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::UpdateNotification {
                key,
                ..
            }))) => {
                // Update notifications are expected when subscribed - just skip them
                tracing::trace!("Received update notification for key: {}", key);
                continue;
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for update: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                return Err("timeout waiting for update response".into());
            }
        }
    }
}

/// WebSocket configuration with increased message size limit to match server (100MB)
fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(100 * 1024 * 1024)) // 100MB to match server
        .max_frame_size(Some(16 * 1024 * 1024)) // 16MB frames
}

// Create a new ping client by connecting to the given host
pub async fn connect_to_host(
    host: &str,
) -> Result<WebApi, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let uri = format!("ws://{host}/v1/contract/command?encodingProtocol=native");
    let (stream, _resp) =
        tokio_tungstenite::connect_async_with_config(&uri, Some(ws_config()), false)
            .await
            .map_err(|e| {
                tracing::error!(err=%e);
                e
            })?;
    Ok(WebApi::start(stream))
}

/// Run the ping client's main loop until shutdown
pub async fn run_ping_client(
    client: &mut WebApi,
    contract_key: ContractKey,
    parameters: PingContractOptions,
    node_id: String,
    local_state: &mut Ping,
    // For testing, allow external control of when to shut down
    mut shutdown_signal: Option<tokio::sync::oneshot::Receiver<()>>,
    // Max run duration for tests
    max_duration: Option<Duration>,
) -> Result<PingStats, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut stats = PingStats::new();
    let mut send_tick = tokio::time::interval(parameters.frequency);

    let start_time = Instant::now();
    let mut errors = 0;

    loop {
        // Check max duration if specified
        if let Some(max_dur) = max_duration {
            if start_time.elapsed() >= max_dur {
                tracing::info!("reached maximum test duration, shutting down...");
                break;
            }
        }

        if errors > 100 {
            tracing::error!("too many errors, shutting down...");
            return Err("too many errors".into());
        }

        tokio::select! {
            _ = send_tick.tick() => {
                let mut ping = Ping::default();
                ping.insert(node_id.clone());
                stats.record_sent();
                if let Err(e) = client.send(ClientRequest::ContractOp(ContractRequest::Update {
                    key: contract_key,
                    data: UpdateData::Delta(StateDelta::from(serde_json::to_vec(&ping).unwrap())),
                })).await {
                    tracing::error!(err=%e, "failed to send update request");
                }
            },
            res = client.recv() => {
                match res {
                    Ok(resp) => match resp {
                        HostResponse::ContractResponse(resp) => {
                            match resp {
                                ContractResponse::UpdateNotification { key, update } => {
                                    if key != contract_key {
                                        return Err("unexpected key".into());
                                    }

                                    let mut handle_update = |state: &[u8], stats: &mut PingStats| {
                                        let new_ping = if state.is_empty() {
                                            Ping::default()
                                        } else {
                                            match serde_json::from_slice::<Ping>(state) {
                                                Ok(p) => p,
                                                Err(e) => return Err(e),
                                            }
                                        };

                                        let updates = local_state.merge(new_ping, parameters.ttl);

                                        for (name, timestamps) in updates.into_iter() {
                                            if !timestamps.is_empty() {
                                                if let Some(last) = timestamps.first() {
                                                    tracing::info!("{} last updated at {}", name, last);
                                                }
                                                stats.record_received(name, timestamps);
                                            }
                                        }
                                        Ok(())
                                    };

                                    match update {
                                        UpdateData::State(state) =>  {
                                            if let Err(e) = handle_update(&state, &mut stats) {
                                                tracing::error!(err=%e);
                                            }
                                        },
                                        UpdateData::Delta(delta) => {
                                            if let Err(e) = handle_update(&delta, &mut stats) {
                                                tracing::error!(err=%e);
                                            }
                                        },
                                        UpdateData::StateAndDelta { state, delta } => {
                                            if let Err(e) = handle_update(&state, &mut stats) {
                                                tracing::error!(err=%e);
                                            }

                                            if let Err(e) = handle_update(&delta, &mut stats) {
                                                tracing::error!(err=%e);
                                            }
                                        },
                                        _ => unreachable!("unknown state"),
                                    }
                                },
                                _ => {
                                    tracing::debug!("Received other contract response: {:?}", resp);
                                },
                            }
                        },
                        HostResponse::DelegateResponse { .. } => {},
                        HostResponse::Ok => {},
                        _ => unreachable!(),
                    },
                    Err(e) => {
                        tracing::error!(err=%e);
                        errors += 1;
                    },
                }
            }
            _ = async { if let Some(ref mut rx) = shutdown_signal {
                let _ = rx.await;
                true
            } else {
                std::future::pending().await
            }} => {
                tracing::info!("received shutdown signal...");
                break;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down on ctrl-c...");
                break;
            }
        }
    }

    Ok(stats)
}
