
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::{future::BoxFuture, FutureExt};
use rand::{random, Rng, SeedableRng};
use testresult::TestResult;
use tokio::{select, time::sleep};
use tracing::{info, span, warn, Level};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use freenet_ping_app::{
    ping_client::{PingClient, PingClientConfig},
    ping_server::{PingServer, PingServerConfig},
};
use freenet_ping_types::Ping;

const MAX_UPDATE_RETRIES: usize = 8;
const INITIAL_DELAY_MS: u64 = 500;
const MAX_DELAY_MS: u64 = 15000;
const PROPAGATION_CHECK_INTERVAL_MS: u64 = 1000;
const MAX_PROPAGATION_CHECKS: usize = 10;

#[derive(Debug, Clone)]
struct LogEntry {
    timestamp: SystemTime,
    node_name: String,
    message: String,
}

struct ChronologicalLogger {
    entries: Arc<Mutex<Vec<LogEntry>>>,
}

impl ChronologicalLogger {
    fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn log(&self, node_name: &str, message: &str) {
        let entry = LogEntry {
            timestamp: SystemTime::now(),
            node_name: node_name.to_string(),
            message: message.to_string(),
        };
        
        if let Ok(mut entries) = self.entries.lock() {
            entries.push(entry);
        }
    }

    fn print_logs(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
            
            println!("\n=== CHRONOLOGICAL LOGS FROM ALL NODES ===");
            for entry in entries.iter() {
                let timestamp = entry.timestamp.duration_since(UNIX_EPOCH).unwrap_or_default();
                println!("[{}.{:03}] [{}] {}", 
                    timestamp.as_secs(),
                    timestamp.subsec_millis(),
                    entry.node_name,
                    entry.message
                );
            }
            println!("=== END OF LOGS ===\n");
        }
    }
}

#[tokio::test]
#[tracing::instrument]
async fn test_ping_broadcast_mechanism() -> TestResult {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "debug,freenet::operations::update=trace,freenet::operations::subscribe=trace,freenet::contract=trace",
        )
    });

    let logger = Arc::new(ChronologicalLogger::new());

    let gateway_logger = logger.clone();
    let node1_logger = logger.clone();
    let node2_logger = logger.clone();

    let gateway_config = PingServerConfig {
        port: 0,
        ws_api_port: 0,
        network_port: 0,
        data_dir: None,
        config_dir: None,
        bootstrap_nodes: vec![],
        log_level: "debug".to_string(),
    };

    let gateway_server = PingServer::new(gateway_config).await?;
    let gateway_port = gateway_server.ws_api_port();
    let gateway_network_port = gateway_server.network_port();
    let gateway_peer_id = gateway_server.peer_id();

    gateway_logger.log("Gateway", &format!("Started gateway node with peer_id: {}", gateway_peer_id));

    let node1_config = PingServerConfig {
        port: 0,
        ws_api_port: 0,
        network_port: 0,
        data_dir: None,
        config_dir: None,
        bootstrap_nodes: vec![format!("127.0.0.1:{}", gateway_network_port)],
        log_level: "debug".to_string(),
    };

    let node1_server = PingServer::new(node1_config).await?;
    let node1_port = node1_server.ws_api_port();
    let node1_peer_id = node1_server.peer_id();

    node1_logger.log("Node1", &format!("Started Node1 with peer_id: {}", node1_peer_id));

    let node2_config = PingServerConfig {
        port: 0,
        ws_api_port: 0,
        network_port: 0,
        data_dir: None,
        config_dir: None,
        bootstrap_nodes: vec![format!("127.0.0.1:{}", gateway_network_port)],
        log_level: "debug".to_string(),
    };

    let node2_server = PingServer::new(node2_config).await?;
    let node2_port = node2_server.ws_api_port();
    let node2_peer_id = node2_server.peer_id();

    node2_logger.log("Node2", &format!("Started Node2 with peer_id: {}", node2_peer_id));

    let mut client_gw = WebApi::new(&format!("ws://127.0.0.1:{}", gateway_port)).await?;
    let mut client_node1 = WebApi::new(&format!("ws://127.0.0.1:{}", node1_port)).await?;
    let mut client_node2 = WebApi::new(&format!("ws://127.0.0.1:{}", node2_port)).await?;

    client_node1
        .send(ClientRequest::BlockPeer {
            peer_id: node2_peer_id.clone(),
        })
        .await?;
    node1_logger.log("Node1", &format!("Blocked peer: {}", node2_peer_id));

    client_node2
        .send(ClientRequest::BlockPeer {
            peer_id: node1_peer_id.clone(),
        })
        .await?;
    node2_logger.log("Node2", &format!("Blocked peer: {}", node1_peer_id));

    sleep(Duration::from_secs(2)).await;

    let ping_client_gw = PingClient::new(PingClientConfig {
        ws_api_url: format!("ws://127.0.0.1:{}", gateway_port),
    })
    .await?;

    let ping_client_node1 = PingClient::new(PingClientConfig {
        ws_api_url: format!("ws://127.0.0.1:{}", node1_port),
    })
    .await?;

    let ping_client_node2 = PingClient::new(PingClientConfig {
        ws_api_url: format!("ws://127.0.0.1:{}", node2_port),
    })
    .await?;

    let key = ping_client_gw.deploy().await?;
    gateway_logger.log("Gateway", &format!("Deployed ping contract with key: {}", key));

    ping_client_gw.subscribe(&key).await?;
    gateway_logger.log("Gateway", &format!("Subscribed to contract: {}", key));

    ping_client_node1.subscribe(&key).await?;
    node1_logger.log("Node1", &format!("Subscribed to contract: {}", key));

    ping_client_node2.subscribe(&key).await?;
    node2_logger.log("Node2", &format!("Subscribed to contract: {}", key));

    sleep(Duration::from_secs(5)).await;

    let get_all_states = |client_gw: &mut WebApi,
                          client_node1: &mut WebApi,
                          client_node2: &mut WebApi,
                          key: ContractKey| -> BoxFuture<'_, anyhow::Result<(Ping, Ping, Ping)>> {
        Box::pin(async move {
            info!("Querying all nodes for current state...");

            let gw_state = client_gw
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key,
                    return_contract_code: false,
                    subscribe: false,
                }))
                .await?;

            let node1_state = client_node1
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key,
                    return_contract_code: false,
                    subscribe: false,
                }))
                .await?;

            let node2_state = client_node2
                .send(ClientRequest::ContractOp(ContractRequest::Get {
                    key,
                    return_contract_code: false,
                    subscribe: false,
                }))
                .await?;

            let gw_ping = if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                state,
                ..
            }) = gw_state
            {
                serde_json::from_slice::<Ping>(&state)?
            } else {
                return Err(anyhow::anyhow!("Unexpected response from gateway"));
            };

            let node1_ping = if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                state,
                ..
            }) = node1_state
            {
                serde_json::from_slice::<Ping>(&state)?
            } else {
                return Err(anyhow::anyhow!("Unexpected response from node1"));
            };

            let node2_ping = if let HostResponse::ContractResponse(ContractResponse::GetResponse {
                state,
                ..
            }) = node2_state
            {
                serde_json::from_slice::<Ping>(&state)?
            } else {
                return Err(anyhow::anyhow!("Unexpected response from node2"));
            };

            Ok((gw_ping, node1_ping, node2_ping))
        })
    };

    let send_update_and_check_propagation = |source_node: &str,
                                             source_client: &PingClient,
                                             key: &ContractKey,
                                             logger: Arc<ChronologicalLogger>| -> BoxFuture<'_, anyhow::Result<bool>> {
        let key = *key;
        let source_node = source_node.to_string();
        let logger = logger.clone();
        
        Box::pin(async move {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            
            let update_id = format!("update-{}-{}", source_node, timestamp);
            
            logger.log(&source_node, &format!("Sending update with ID: {}", update_id));
            
            let mut delay_ms = INITIAL_DELAY_MS;
            let mut success = false;
            
            for attempt in 1..=MAX_UPDATE_RETRIES {
                match source_client.ping(&key, &update_id).await {
                    Ok(_) => {
                        logger.log(&source_node, &format!("Update sent successfully on attempt {}", attempt));
                        success = true;
                        break;
                    }
                    Err(e) => {
                        logger.log(&source_node, &format!("Update attempt {} failed: {}", attempt, e));
                        
                        delay_ms = (delay_ms * 2).min(MAX_DELAY_MS);
                        sleep(Duration::from_millis(delay_ms)).await;
                    }
                }
            }
            
            if !success {
                logger.log(&source_node, "Failed to send update after all retry attempts");
                return Ok(false);
            }
            
            let mut client_gw = WebApi::new(&format!("ws://127.0.0.1:{}", gateway_port)).await?;
            let mut client_node1 = WebApi::new(&format!("ws://127.0.0.1:{}", node1_port)).await?;
            let mut client_node2 = WebApi::new(&format!("ws://127.0.0.1:{}", node2_port)).await?;
            
            for check in 1..=MAX_PROPAGATION_CHECKS {
                logger.log("Test", &format!("Propagation check {} for update {}", check, update_id));
                
                sleep(Duration::from_millis(PROPAGATION_CHECK_INTERVAL_MS)).await;
                
                let states_result = get_all_states(&mut client_gw, &mut client_node1, &mut client_node2, key).await;
                
                match states_result {
                    Ok((gw_ping, node1_ping, node2_ping)) => {
                        let gw_has_update = gw_ping.pings.contains_key(&update_id);
                        let node1_has_update = node1_ping.pings.contains_key(&update_id);
                        let node2_has_update = node2_ping.pings.contains_key(&update_id);
                        
                        logger.log("Test", &format!(
                            "Update {} propagation status - Gateway: {}, Node1: {}, Node2: {}",
                            update_id, gw_has_update, node1_has_update, node2_has_update
                        ));
                        
                        if gw_has_update && node1_has_update && node2_has_update {
                            logger.log("Test", &format!("Update {} successfully propagated to all nodes", update_id));
                            return Ok(true);
                        }
                    }
                    Err(e) => {
                        logger.log("Test", &format!("Error checking propagation: {}", e));
                    }
                }
            }
            
            logger.log("Test", &format!("Update {} failed to propagate to all nodes after {} checks", 
                update_id, MAX_PROPAGATION_CHECKS));
            
            Ok(false)
        })
    };

    let node1_to_node2_result = send_update_and_check_propagation(
        "Node1", 
        &ping_client_node1, 
        &key, 
        logger.clone()
    ).await?;

    let node2_to_node1_result = send_update_and_check_propagation(
        "Node2", 
        &ping_client_node2, 
        &key, 
        logger.clone()
    ).await?;

    let gateway_to_nodes_result = send_update_and_check_propagation(
        "Gateway", 
        &ping_client_gw, 
        &key, 
        logger.clone()
    ).await?;

    logger.print_logs();

    assert!(
        !node1_to_node2_result || !node2_to_node1_result || !gateway_to_nodes_result,
        "Expected at least one update propagation test to fail, but all succeeded"
    );

    gateway_server.shutdown().await;
    node1_server.shutdown().await;
    node2_server.shutdown().await;

    Ok(())
}
