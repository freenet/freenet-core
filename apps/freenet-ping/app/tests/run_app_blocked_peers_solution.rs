use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::future::BoxFuture;
use rand::{Rng, SeedableRng};
use testresult::TestResult;
use tokio::{
    select,
    sync::{mpsc, Mutex},
    time::sleep,
};
use tracing::{info, span, warn, Instrument, Level};
use tracing_subscriber::EnvFilter;

use freenet_ping_app::{
    ping_client::{wait_for_put_response, wait_for_subscribe_response},
    ping_contract::Ping,
};

const MAX_UPDATE_RETRIES: usize = 5;

const BASE_DELAY_MS: u64 = 500;

const MAX_TEST_DURATION_SECS: u64 = 180;

#[derive(Debug, Clone)]
struct LogEntry {
    timestamp: std::time::SystemTime,
    source: String,
    message: String,
}

#[tokio::test]
async fn test_ping_blocked_peers_solution() -> TestResult {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("debug,freenet::operations::subscribe=trace,freenet::contract=trace,freenet::operations::update=trace")
    });
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();

    let (log_tx, mut log_rx) = mpsc::channel::<LogEntry>(1000);

    let log_task = tokio::spawn(async move {
        let mut logs = Vec::new();
        while let Some(log) = log_rx.recv().await {
            logs.push(log);
            logs.sort_by_key(|log| log.timestamp);

            if logs.len() > 10 {
                println!("--- CHRONOLOGICAL LOGS (LAST 10) ---");
                for log in logs.iter().rev().take(10).rev() {
                    println!(
                        "[{}] {}: {}",
                        humantime::format_rfc3339(log.timestamp),
                        log.source,
                        log.message
                    );
                }
                println!("----------------------------------");
            }
        }
    });

    let create_logger = |source: String, log_tx: mpsc::Sender<LogEntry>| {
        move |message: String| {
            let log_entry = LogEntry {
                timestamp: std::time::SystemTime::now(),
                source: source.clone(),
                message,
            };
            let _ = log_tx.try_send(log_entry);
        }
    };

    let test = async {
        let log_tx_clone = log_tx.clone();
        let log_gateway = create_logger("Gateway".to_string(), log_tx_clone.clone());
        let log_node1 = create_logger("Node1".to_string(), log_tx_clone.clone());
        let log_node2 = create_logger("Node2".to_string(), log_tx_clone);

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let ws_port_gw = rng.gen_range(40000..50000);
        let ws_port_node1 = rng.gen_range(50001..60000);
        let ws_port_node2 = rng.gen_range(60001..70000);
        let network_port_gw = rng.gen_range(30000..40000);
        let network_port_node1 = rng.gen_range(20000..30000);
        let network_port_node2 = rng.gen_range(10000..20000);

        let temp_dir = tempfile::tempdir()?;
        let gw_dir = tempfile::tempdir_in(temp_dir.path())?;
        let node1_dir = tempfile::tempdir_in(temp_dir.path())?;
        let node2_dir = tempfile::tempdir_in(temp_dir.path())?;

        log_gateway("Starting gateway node".to_string());
        let gateway_node = freenet::start_with_config(
            freenet::Config::default()
                .with_ws_api_port(ws_port_gw)
                .with_network_port(network_port_gw)
                .with_data_dir(gw_dir.path())
                .with_bootstrap_peers(vec![])
                .with_gateway_peers(vec![]),
        )
        .await?;

        let gateway_peer_id = gateway_node.peer_id().await?;
        log_gateway(format!(
            "Gateway node started with peer ID: {}",
            gateway_peer_id
        ));

        log_node1("Starting node1".to_string());
        let node1 = freenet::start_with_config(
            freenet::Config::default()
                .with_ws_api_port(ws_port_node1)
                .with_network_port(network_port_node1)
                .with_data_dir(node1_dir.path())
                .with_bootstrap_peers(vec![])
                .with_gateway_peers(vec![format!(
                    "127.0.0.1:{}:{}",
                    network_port_gw, gateway_peer_id
                )])
                .with_blocked_peers(vec![]),
        )
        .await?;

        let node1_peer_id = node1.peer_id().await?;
        log_node1(format!("Node1 started with peer ID: {}", node1_peer_id));

        log_node2("Starting node2".to_string());
        let node2 = freenet::start_with_config(
            freenet::Config::default()
                .with_ws_api_port(ws_port_node2)
                .with_network_port(network_port_node2)
                .with_data_dir(node2_dir.path())
                .with_bootstrap_peers(vec![])
                .with_gateway_peers(vec![format!(
                    "127.0.0.1:{}:{}",
                    network_port_gw, gateway_peer_id
                )])
                .with_blocked_peers(vec![node1_peer_id.clone()]),
        )
        .await?;

        let node2_peer_id = node2.peer_id().await?;
        log_node2(format!("Node2 started with peer ID: {}", node2_peer_id));

        log_node1(format!("Updating node1 to block node2: {}", node2_peer_id));
        node1
            .update_blocked_peers(vec![node2_peer_id.clone()])
            .await?;

        log_gateway("Waiting for nodes to connect to the gateway".to_string());
        sleep(Duration::from_secs(5)).await;

        let client_gw = WebApi::connect(format!("ws://127.0.0.1:{}", ws_port_gw)).await?;
        let client_node1 = WebApi::connect(format!("ws://127.0.0.1:{}", ws_port_node1)).await?;
        let client_node2 = WebApi::connect(format!("ws://127.0.0.1:{}", ws_port_node2)).await?;

        let client_gw = Arc::new(Mutex::new(client_gw));
        let client_node1 = Arc::new(Mutex::new(client_node1));
        let client_node2 = Arc::new(Mutex::new(client_node2));

        log_gateway("Deploying ping contract on gateway node".to_string());
        let mut client_gw_lock = client_gw.lock().await;

        let ping = Ping::default();
        let state = serde_json::to_vec(&ping)?;

        client_gw_lock
            .send(ClientRequest::ContractOp {
                key: None,
                request: ContractRequest {
                    return_contract_code: false,
                    subscribe: true,
                    op: Some(state),
                },
            })
            .await?;

        let response = client_gw_lock.recv().await?;
        let key = if let HostResponse::ContractResponse(ContractResponse::PutResponse { key }) =
            response
        {
            key
        } else {
            return Err("Failed to deploy ping contract".into());
        };

        log_gateway(format!("Ping contract deployed with key: {}", key));
        drop(client_gw_lock);

        log_node1(format!("Subscribing node1 to the contract: {}", key));
        let mut client_node1_lock = client_node1.lock().await;
        client_node1_lock
            .send(ClientRequest::ContractOp {
                key: Some(key),
                request: ContractRequest {
                    return_contract_code: false,
                    subscribe: true,
                    op: None,
                },
            })
            .await?;
        let response = wait_for_subscribe_response(&mut client_node1_lock, &key).await?;
        log_node1(format!("Node1 subscription response: {:?}", response));
        drop(client_node1_lock);

        log_node2(format!("Subscribing node2 to the contract: {}", key));
        let mut client_node2_lock = client_node2.lock().await;
        client_node2_lock
            .send(ClientRequest::ContractOp {
                key: Some(key),
                request: ContractRequest {
                    return_contract_code: false,
                    subscribe: true,
                    op: None,
                },
            })
            .await?;
        let response = wait_for_subscribe_response(&mut client_node2_lock, &key).await?;
        log_node2(format!("Node2 subscription response: {:?}", response));
        drop(client_node2_lock);

        log_gateway("Waiting for subscriptions to propagate".to_string());
        sleep(Duration::from_secs(5)).await;

        let get_all_states = |client_gw: &Arc<Mutex<WebApi>>,
                              client_node1: &Arc<Mutex<WebApi>>,
                              client_node2: &Arc<Mutex<WebApi>>,
                              key: ContractKey,
                              log_gateway: &dyn Fn(String),
                              log_node1: &dyn Fn(String),
                              log_node2: &dyn Fn(String)|
         -> BoxFuture<'_, anyhow::Result<(Ping, Ping, Ping)>> {
            Box::pin(async move {
                log_gateway("Querying all nodes for current state...".to_string());

                let client_gw_clone = client_gw.clone();
                let client_node1_clone = client_node1.clone();
                let client_node2_clone = client_node2.clone();

                let gw_state_future = tokio::spawn(async move {
                    let mut client = client_gw_clone.lock().await;
                    let response = client
                        .send(ClientRequest::ContractOp {
                            key: Some(key),
                            request: ContractRequest {
                                return_contract_code: false,
                                subscribe: false,
                                op: None,
                            },
                        })
                        .await?;

                    if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                        value: Some(value),
                        ..
                    }) = response
                    {
                        let state = serde_json::from_slice::<Ping>(&value)?;
                        Ok(state)
                    } else {
                        Err(anyhow::anyhow!("Failed to get state from gateway"))
                    }
                });

                let node1_state_future = tokio::spawn(async move {
                    let mut client = client_node1_clone.lock().await;
                    let response = client
                        .send(ClientRequest::ContractOp {
                            key: Some(key),
                            request: ContractRequest {
                                return_contract_code: false,
                                subscribe: false,
                                op: None,
                            },
                        })
                        .await?;

                    if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                        value: Some(value),
                        ..
                    }) = response
                    {
                        let state = serde_json::from_slice::<Ping>(&value)?;
                        Ok(state)
                    } else {
                        Err(anyhow::anyhow!("Failed to get state from node1"))
                    }
                });

                let node2_state_future = tokio::spawn(async move {
                    let mut client = client_node2_clone.lock().await;
                    let response = client
                        .send(ClientRequest::ContractOp {
                            key: Some(key),
                            request: ContractRequest {
                                return_contract_code: false,
                                subscribe: false,
                                op: None,
                            },
                        })
                        .await?;

                    if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                        value: Some(value),
                        ..
                    }) = response
                    {
                        let state = serde_json::from_slice::<Ping>(&value)?;
                        Ok(state)
                    } else {
                        Err(anyhow::anyhow!("Failed to get state from node2"))
                    }
                });

                let gw_result = gw_state_future.await??;
                let node1_result = node1_state_future.await??;
                let node2_result = node2_state_future.await??;

                log_gateway(format!("Gateway state: {:?}", gw_result));
                log_node1(format!("Node1 state: {:?}", node1_result));
                log_node2(format!("Node2 state: {:?}", node2_result));

                Ok((gw_result, node1_result, node2_result))
            })
        };

        let update_with_retry = |client: &Arc<Mutex<WebApi>>,
                                 node_name: &str,
                                 key: ContractKey,
                                 name: String,
                                 timestamp: u64,
                                 log_gateway: &dyn Fn(String),
                                 log_node1: &dyn Fn(String),
                                 log_node2: &dyn Fn(String)|
         -> BoxFuture<'_, anyhow::Result<()>> {
            Box::pin(async move {
                let logger = match node_name {
                    "Gateway" => log_gateway,
                    "Node1" => log_node1,
                    "Node2" => log_node2,
                    _ => log_gateway,
                };

                for retry in 0..MAX_UPDATE_RETRIES {
                    logger(format!(
                        "Updating contract on {} (attempt {})",
                        node_name,
                        retry + 1
                    ));

                    let delay = BASE_DELAY_MS * (2_u64.pow(retry as u32));

                    let mut client_lock = client.lock().await;

                    let mut ping = Ping::default();
                    ping.insert(name.clone(), timestamp);
                    let state = serde_json::to_vec(&ping)?;

                    client_lock
                        .send(ClientRequest::ContractOp {
                            key: Some(key),
                            request: ContractRequest {
                                return_contract_code: false,
                                subscribe: false,
                                op: Some(state),
                            },
                        })
                        .await?;

                    let response = wait_for_put_response(&mut client_lock, &key).await?;
                    logger(format!("{} update response: {:?}", node_name, response));
                    drop(client_lock);

                    sleep(Duration::from_millis(delay)).await;

                    let (gw_state, node1_state, node2_state) = get_all_states(
                        &client_gw,
                        &client_node1,
                        &client_node2,
                        key,
                        log_gateway,
                        log_node1,
                        log_node2,
                    )
                    .await?;

                    let update_propagated = match node_name {
                        "Gateway" => {
                            gw_state.contains_key(&name)
                                && node1_state.contains_key(&name)
                                && node2_state.contains_key(&name)
                        }
                        "Node1" => {
                            gw_state.contains_key(&name)
                                && node1_state.contains_key(&name)
                                && node2_state.contains_key(&name)
                        }
                        "Node2" => {
                            gw_state.contains_key(&name)
                                && node1_state.contains_key(&name)
                                && node2_state.contains_key(&name)
                        }
                        _ => false,
                    };

                    if update_propagated {
                        logger(format!(
                            "Update from {} successfully propagated to all nodes",
                            node_name
                        ));
                        return Ok(());
                    }

                    logger(format!(
                        "Update from {} not fully propagated, retrying...",
                        node_name
                    ));
                }

                Err(anyhow::anyhow!(
                    "Failed to propagate update from {} after {} retries",
                    node_name,
                    MAX_UPDATE_RETRIES
                ))
            })
        };

        log_gateway("Testing update propagation from Gateway to Node1 and Node2".to_string());
        let gateway_update_result = update_with_retry(
            &client_gw,
            "Gateway",
            key,
            "Gateway Update".to_string(),
            42,
            &log_gateway,
            &log_node1,
            &log_node2,
        )
        .await;

        log_node1("Testing update propagation from Node1 to Gateway and Node2".to_string());
        let node1_update_result = update_with_retry(
            &client_node1,
            "Node1",
            key,
            "Node1 Update".to_string(),
            43,
            &log_gateway,
            &log_node1,
            &log_node2,
        )
        .await;

        log_node2("Testing update propagation from Node2 to Gateway and Node1".to_string());
        let node2_update_result = update_with_retry(
            &client_node2,
            "Node2",
            key,
            "Node2 Update".to_string(),
            44,
            &log_gateway,
            &log_node1,
            &log_node2,
        )
        .await;

        let all_updates_successful = gateway_update_result.is_ok()
            && node1_update_result.is_ok()
            && node2_update_result.is_ok();

        if all_updates_successful {
            log_gateway("All updates successfully propagated!".to_string());
        } else {
            if let Err(e) = &gateway_update_result {
                log_gateway(format!("Gateway update error: {}", e));
            }
            if let Err(e) = &node1_update_result {
                log_node1(format!("Node1 update error: {}", e));
            }
            if let Err(e) = &node2_update_result {
                log_node2(format!("Node2 update error: {}", e));
            }
        }

        assert!(
            gateway_update_result.is_ok(),
            "Gateway update failed to propagate"
        );
        assert!(
            node1_update_result.is_ok(),
            "Node1 update failed to propagate"
        );
        assert!(
            node2_update_result.is_ok(),
            "Node2 update failed to propagate"
        );

        drop(log_tx);

        Ok::<_, anyhow::Error>(())
    };

    let instrumented_test = test.instrument(span!(Level::INFO, "test_ping_blocked_peers_solution"));

    let result = select! {
        result = instrumented_test => result.map_err(|e| e.into()),
        _ = sleep(Duration::from_secs(MAX_TEST_DURATION_SECS)) => {
            Err("Test timed out".into())
        }
    };

    let _ = log_task.await;

    result
}
