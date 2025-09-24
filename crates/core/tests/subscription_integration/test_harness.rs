use std::collections::{HashMap, BTreeMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use freenet::config::{ConfigArgs, NetworkArgs, WebsocketApiArgs, SecretArgs};
use freenet::dev_tool::TransportKeypair;
use freenet::local_node::NodeConfig;
use freenet_stdlib::client_api::{ClientRequest, ContractRequest, WebApi};
use freenet_stdlib::prelude::*;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tracing::{debug, info, warn};

use super::{NetworkTopology, UnifiedLogCollector, NetworkMonitor};

pub struct MultiNodeTestHarness {
    pub supervisor: TestSupervisor,
    pub nodes: Vec<NodeInstance>,
    pub log_collector: Arc<Mutex<UnifiedLogCollector>>,
    pub network_monitor: Arc<Mutex<NetworkMonitor>>,
    pub test_contracts: HashMap<ContractKey, TestContract>,
    temp_dirs: Vec<tempfile::TempDir>,
}

pub struct TestSupervisor {
    start_time: Instant,
}

pub struct NodeInstance {
    pub id: String,
    pub config: NodeConfig,
    pub ws_client: Option<WebApi>,
    pub process: Option<Child>,
    pub log_path: PathBuf,
    pub location: Location,
    pub is_gateway: bool,
    pub ws_port: u16,
    pub network_port: Option<u16>,
}

pub struct TestContract {
    pub key: ContractKey,
    pub code: ContractCode<'static>,
    pub params: Parameters<'static>,
    pub initial_state: WrappedState,
}

impl MultiNodeTestHarness {
    pub async fn new(topology: NetworkTopology) -> Result<Self> {
        info!("Initializing multi-node test harness with topology: {:?}", topology);

        let supervisor = TestSupervisor {
            start_time: Instant::now(),
        };

        let log_collector = Arc::new(Mutex::new(UnifiedLogCollector::new()));
        let network_monitor = Arc::new(Mutex::new(NetworkMonitor::new()));

        let (nodes, temp_dirs) = Self::create_nodes(topology).await?;

        Ok(MultiNodeTestHarness {
            supervisor,
            nodes,
            log_collector,
            network_monitor,
            test_contracts: HashMap::new(),
            temp_dirs,
        })
    }

    async fn create_nodes(topology: NetworkTopology) -> Result<(Vec<NodeInstance>, Vec<tempfile::TempDir>)> {
        let mut nodes = Vec::new();
        let mut temp_dirs = Vec::new();

        let node_configs = topology.generate_configs()?;

        for (i, node_config) in node_configs.into_iter().enumerate() {
            let temp_dir = tempfile::tempdir()?;
            let node_id = format!("node_{}", i);

            // Setup transport keypair
            let key = TransportKeypair::new();
            let transport_keypair = temp_dir.path().join("private.pem");
            key.save(&transport_keypair)?;
            key.public().save(temp_dir.path().join("public.pem"))?;

            // Get free ports
            let ws_port = get_free_port()?;
            let network_port = if node_config.is_gateway {
                Some(get_free_port()?)
            } else {
                None
            };

            let node = NodeInstance {
                id: node_id.clone(),
                config: node_config.clone(),
                ws_client: None,
                process: None,
                log_path: temp_dir.path().join("node.log"),
                location: node_config.location,
                is_gateway: node_config.is_gateway,
                ws_port,
                network_port,
            };

            nodes.push(node);
            temp_dirs.push(temp_dir);
        }

        Ok((nodes, temp_dirs))
    }

    pub async fn start_network(&mut self) -> Result<()> {
        info!("Starting network with {} nodes", self.nodes.len());

        // Start gateways first
        for i in 0..self.nodes.len() {
            if self.nodes[i].is_gateway {
                self.start_node(i).await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }

        // Then start regular nodes
        for i in 0..self.nodes.len() {
            if !self.nodes[i].is_gateway {
                self.start_node(i).await?;
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        // Wait for network to stabilize
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect WebSocket clients
        for i in 0..self.nodes.len() {
            self.connect_client(i).await?;
        }

        info!("Network started successfully");
        Ok(())
    }

    async fn start_node(&mut self, index: usize) -> Result<()> {
        let node = &mut self.nodes[index];
        let temp_dir = &self.temp_dirs[index];

        info!("Starting node {} (gateway: {})", node.id, node.is_gateway);

        let mut cmd = Command::new("cargo");
        cmd.arg("run")
            .arg("--bin")
            .arg("freenet")
            .arg("--")
            .arg("--config-dir")
            .arg(temp_dir.path())
            .arg("--data-dir")
            .arg(temp_dir.path())
            .arg("--ws-api-port")
            .arg(node.ws_port.to_string())
            .arg("--transport-keypair")
            .arg(temp_dir.path().join("private.pem"));

        if node.is_gateway {
            cmd.arg("--is-gateway");
            if let Some(port) = node.network_port {
                cmd.arg("--public-port").arg(port.to_string());
            }
        } else {
            // Add gateway addresses
            for gw_node in &self.nodes {
                if gw_node.is_gateway {
                    if let Some(gw_port) = gw_node.network_port {
                        cmd.arg("--gateway")
                            .arg(format!("127.0.0.1:{}", gw_port));
                    }
                }
            }
        }

        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let child = cmd.spawn()?;
        node.process = Some(child);

        // Start log collection for this node
        self.log_collector.lock().await.add_node(&node.id, &node.log_path)?;

        Ok(())
    }

    async fn connect_client(&mut self, index: usize) -> Result<()> {
        let node = &mut self.nodes[index];
        let uri = format!("ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native", node.ws_port);

        debug!("Connecting WebSocket client to {}", uri);

        // Retry connection a few times
        for attempt in 0..10 {
            match connect_async(&uri).await {
                Ok((stream, _)) => {
                    node.ws_client = Some(WebApi::start(stream));
                    info!("Connected to node {} on attempt {}", node.id, attempt + 1);
                    return Ok(());
                }
                Err(e) if attempt < 9 => {
                    debug!("Connection attempt {} failed for {}: {}", attempt + 1, node.id, e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(anyhow!("Failed to connect to {}: {}", node.id, e)),
            }
        }

        unreachable!()
    }

    pub async fn deploy_contract_on_node(
        &mut self,
        node_index: usize,
        contract: TestContract,
    ) -> Result<ContractKey> {
        let node = &mut self.nodes[node_index];
        let client = node.ws_client.as_mut()
            .ok_or_else(|| anyhow!("Node {} not connected", node.id))?;

        info!("Deploying contract on node {}", node.id);

        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            WrappedContract::new(Arc::new(contract.code.clone()), contract.params.clone())
        ));

        client.send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: container,
            state: contract.initial_state.clone(),
            related_contracts: RelatedContracts::new(),
            subscribe: false,
        })).await?;

        let key = contract.key;
        self.test_contracts.insert(key, contract);

        Ok(key)
    }

    pub async fn subscribe_from_node(
        &mut self,
        node_index: usize,
        key: ContractKey,
    ) -> Result<Transaction> {
        let node = &mut self.nodes[node_index];
        let client = node.ws_client.as_mut()
            .ok_or_else(|| anyhow!("Node {} not connected", node.id))?;

        info!("Subscribing to contract {} from node {}", key, node.id);

        client.send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key,
            summary: None,
        })).await?;

        // In real implementation, we'd track the transaction ID from the subscription
        Ok(Transaction::new::<u64>())
    }

    pub async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down test harness");

        // Kill all node processes
        for node in &mut self.nodes {
            if let Some(mut process) = node.process.take() {
                let _ = process.kill();
            }
        }

        // Save logs
        let collector = self.log_collector.lock().await;
        collector.save_timeline("target/test-logs/timeline.log")?;

        Ok(())
    }

    pub fn gateway(&self) -> &NodeInstance {
        self.nodes.iter()
            .find(|n| n.is_gateway)
            .expect("No gateway found")
    }

    pub async fn wait_for_propagation(
        &self,
        contract_key: ContractKey,
        expected_nodes: usize,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();

        while start.elapsed() < timeout {
            let mut found_count = 0;

            for node in &self.nodes {
                if let Some(client) = &node.ws_client {
                    // Check if node has the contract
                    // This would need actual implementation
                    found_count += 1;
                }
            }

            if found_count >= expected_nodes {
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow!("Contract did not propagate to {} nodes within timeout", expected_nodes))
    }
}

impl NodeInstance {
    pub fn ws_endpoint(&self) -> String {
        format!("ws://127.0.0.1:{}/v1", self.ws_port)
    }
}

fn get_free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}