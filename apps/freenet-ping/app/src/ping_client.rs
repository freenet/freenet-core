use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
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

    pub fn record_received(&mut self, peer: String, time: DateTime<Utc>) {
        *self.received_counts.entry(peer.clone()).or_insert(0) += 1;
        self.last_updates.insert(peer, time);
    }
}

// Wait for a PUT response with the expected key
pub async fn wait_for_put_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<ContractKey, Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::PutResponse { key }))) => {
                if &key == expected_key {
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
pub async fn wait_for_get_response(
    client: &mut WebApi,
    expected_key: &ContractKey,
) -> Result<Ping, Box<dyn std::error::Error + Send + Sync + 'static>> {
    loop {
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
        match resp {
            Ok(Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key,
                contract: _,
                state,
            }))) => {
                if &key != expected_key {
                    return Err("unexpected key".into());
                }

                let old_ping = serde_json::from_slice::<Ping>(&state)?;
                tracing::info!(num_entries = %old_ping.len(), "old state fetched successfully!");
                return Ok(old_ping);
            }
            Ok(Ok(other)) => {
                tracing::warn!("Unexpected response while waiting for get: {}", other);
            }
            Ok(Err(err)) => {
                tracing::error!(err=%err);
                return Err(err.into());
            }
            Err(_) => {
                return Err("timeout waiting for get response".into());
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
        let resp = timeout(Duration::from_secs(30), client.recv()).await;
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

// Create a new ping client by connecting to the given host
pub async fn connect_to_host(
    host: &str,
) -> Result<WebApi, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let uri = format!("ws://{}/v1/contract/command?encodingProtocol=native", host);
    let (stream, _resp) = tokio_tungstenite::connect_async(&uri).await.map_err(|e| {
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

                                        for (name, update_time) in updates.into_iter() {
                                            tracing::info!("{} last updated at {}", name, update_time);
                                            stats.record_received(name, update_time);
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
