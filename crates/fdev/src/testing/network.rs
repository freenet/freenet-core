use super::{Error, TestConfig};
use anyhow::anyhow;
use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use freenet::dev_tool::{
    EventChain, MemoryEventsGen, NetworkEventGenerator, NetworkPeer, NodeConfig, NodeLabel, PeerId,
    PeerMessage, PeerStatus, SimNetwork,
};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use http::{Response, StatusCode};
use libp2p_identity::Keypair;
use reqwest;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    net::SocketAddr,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use thiserror::Error;
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::{
    process::Command,
    sync::{oneshot, Mutex},
};

#[derive(Debug, Error)]
pub enum NetworkSimulationError {
    #[error("Server start failed: {0}")]
    ServerStartError(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Subprocess start failed: {0}")]
    SubProcessStartError(String),
}

#[derive(Default, Clone, clap::ValueEnum)]
pub enum Process {
    #[default]
    Supervisor,
    Peer,
}

impl Display for Process {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Supervisor => write!(f, "supervisor"),
            Self::Peer => write!(f, "peer"),
        }
    }
}

#[derive(clap::Parser, Clone)]
pub struct NetworkProcessConfig {
    #[clap(long, default_value_t = Process::Supervisor)]
    pub mode: Process,
    #[clap(long)]
    pub id: Option<String>,
}

struct SubProcess {
    child: tokio::process::Child,
    id: PeerId,
}

impl SubProcess {
    fn build_command(config: &TestConfig, seed: u64) -> Vec<String> {
        let mut args = Vec::new();

        args.push("test".to_owned());

        if let Some(name) = &config.name {
            args.push("--name".to_owned());
            args.push(name.to_string());
        }

        args.push("--seed".to_owned());
        args.push(seed.to_string());

        args.push("--gateways".to_owned());
        args.push(config.gateways.to_string());
        args.push("--nodes".to_owned());
        args.push(config.nodes.to_string());
        args.push("--ring-max-htl".to_owned());
        args.push(config.ring_max_htl.to_string());
        args.push("--rnd-if-htl-above".to_owned());
        args.push(config.rnd_if_htl_above.to_string());
        args.push("--max-connections".to_owned());
        args.push(config.max_connections.to_string());
        args.push("--min-connections".to_owned());
        args.push(config.min_connections.to_string());

        if let Some(start_backoff) = config.peer_start_backoff_ms {
            args.push("--peer-start-backoff-ms".to_owned());
            args.push(start_backoff.to_string());
        }

        if let Some(max_contract_number) = config.max_contract_number {
            args.push("--max-contract-number".to_owned());
            args.push(max_contract_number.to_string());
        }

        args.push("network".to_owned());
        args.push("--mode".to_owned());
        args.push("peer".to_owned());

        args
    }

    async fn start(
        cmd_args: &[String],
        label: &NodeLabel,
        id: PeerId,
    ) -> anyhow::Result<Self, Error> {
        let child = Command::new("fdev")
            .kill_on_drop(true)
            .args(cmd_args)
            .arg("--id")
            .arg(label.to_string())
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| {
                NetworkSimulationError::SubProcessStartError(format!(
                    "Failed to start subprocess: {}",
                    e
                ))
            })?;

        Ok(Self { child, id })
    }

    async fn close(mut self) {
        let _ = self.child.kill().await;
    }
}

async fn start_supervisor(config: &TestConfig) -> anyhow::Result<(), Error> {
    let mut network = super::config_sim_network(config).await.map_err(|e| {
        NetworkSimulationError::NetworkError(format!(
            "Failed to configure simulation network: {}",
            e
        ))
    })?;

    let supervisor = Arc::new(Supervisor::new(&mut network).await);
    start_server(supervisor.clone()).await?;
    run_network(supervisor, config, network).await?;
    Ok(())
}

async fn start_peer(config: &TestConfig, cmd_config: &NetworkProcessConfig) -> Result<(), Error> {
    std::env::set_var("FREENET_PEER_ID", cmd_config.clone().id.unwrap());
    freenet::config::set_logger();
    if let Some(peer_id) = &cmd_config.id {
        let mut peer = NetworkPeer::new(peer_id.clone()).await?;
        peer.run(config, peer_id.clone()).await?;
    }
    Ok(())
}

pub(super) async fn run(
    config: &TestConfig,
    cmd_config: &NetworkProcessConfig,
) -> Result<(), Error> {
    match &cmd_config.mode {
        Process::Supervisor => start_supervisor(config).await,
        Process::Peer => start_peer(config, cmd_config).await,
    }
}

pub async fn start_server(supervisor: Arc<Supervisor>) -> Result<(), NetworkSimulationError> {
    let (startup_sender, startup_receiver) = oneshot::channel();
    let peers_config = supervisor.peers_config.clone();

    let cloned_supervisor = supervisor.clone();

    let router = Router::new()
        .route("/ws", get(|ws| ws_handler(ws, cloned_supervisor)))
        .route(
            "/config/:peer_id",
            get(|path: Path<String>| config_handler(peers_config, path)),
        );

    let socket = SocketAddr::from(([0, 0, 0, 0], 3000));

    tokio::spawn(async move {
        tracing::info!("Supervisor running on {}", socket);
        let listener = tokio::net::TcpListener::bind(socket).await.map_err(|_| {
            NetworkSimulationError::ServerStartError("Failed to bind TCP listener".into())
        })?;

        if startup_sender.send(()).is_err() {
            tracing::error!("Failed to send startup signal");
            return Err(NetworkSimulationError::ServerStartError(
                "Failed to send startup signal".into(),
            ));
        }

        axum::serve(listener, router)
            .await
            .map_err(|e| NetworkSimulationError::ServerStartError(format!("Server error: {}", e)))
    });

    startup_receiver
        .await
        .map_err(|_| NetworkSimulationError::ServerStartError("Server startup failed".into()))?;

    tracing::info!("Server started successfully");
    Ok(())
}

pub async fn run_network(
    supervisor: Arc<Supervisor>,
    test_config: &TestConfig,
    network: SimNetwork,
) -> Result<(), Error> {
    tracing::info!("Starting network");

    let cmd_args = SubProcess::build_command(&test_config, test_config.seed());
    supervisor.start_peer_gateways(&cmd_args).await?;
    supervisor.start_peer_nodes(&cmd_args).await?;

    let peers: Vec<(NodeLabel, PeerId)> = supervisor
        .get_all_peers()
        .await
        .into_iter()
        .map(|(label, config)| (label.clone(), config.peer_id))
        .collect();

    let events_sender = supervisor.user_ev_controller.lock().await.clone();

    let mut events = EventChain::new(peers, events_sender, test_config.events, true);
    let next_event_wait_time = test_config
        .event_wait_ms
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(200));
    let (connectivity_timeout, network_connection_percent) =
        test_config.get_connection_check_params();
    let events_generated = tokio::task::spawn(async move {
        tracing::info!(
            "Waiting for network to be sufficiently connected ({}ms timeout, {}%)",
            connectivity_timeout.as_millis(),
            network_connection_percent * 100.0
        );
        network.check_partial_connectivity(connectivity_timeout, network_connection_percent)?;
        tracing::info!("Network is sufficiently connected, start sending events");
        while events.next().await.is_some() {
            tokio::time::sleep(next_event_wait_time).await;
        }
        Ok::<_, super::Error>(())
    });

    let ctrl_c = tokio::signal::ctrl_c();

    tokio::pin!(events_generated);
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c  /* SIGINT handling */ => {
                break;
            }
            res = &mut events_generated => {
                match res? {
                    Ok(()) => {
                        tracing::info!("Test events generated successfully");
                        *events_generated = tokio::task::spawn(futures::future::pending::<anyhow::Result<()>>());
                        continue;
                    }
                    Err(e) => {
                        tracing::error!("Test finalized with error: {}", e);
                        return Err(e);
                    }
                }
            }
        }
    }

    for (_, subprocess) in supervisor.processes.lock().await.drain() {
        subprocess.close().await;
    }
    tracing::info!("Simulation finished");

    Ok(())
}

async fn config_handler(
    peers_config: Arc<Mutex<HashMap<NodeLabel, NodeConfig>>>,
    Path(peer_id): Path<String>,
) -> axum::response::Response {
    tracing::info!("Received config request for peer_id: {}", peer_id);
    let config = peers_config.lock().await;
    let id = NodeLabel::from(peer_id.as_str());
    match config.get(&id) {
        Some(node_config) => {
            tracing::info!("Found config for peer_id: {}", peer_id);
            axum::response::Json(node_config.clone()).into_response()
        }
        None => {
            let body = format!("No config found for peer_id: {}", peer_id);
            let response = Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(body))
                .unwrap()
                .into_response();
            response.into_response()
        }
    }
}

async fn ws_handler(ws: WebSocketUpgrade, supervisor: Arc<Supervisor>) -> axum::response::Response {
    let on_upgrade = move |ws: WebSocket| async move {
        let cloned_supervisor = supervisor.clone();
        if let Err(error) = handle_socket(ws, cloned_supervisor).await {
            tracing::error!("{error}");
        }
    };
    ws.on_upgrade(on_upgrade)
}

async fn handle_socket(mut socket: WebSocket, supervisor: Arc<Supervisor>) -> anyhow::Result<()> {
    // Clone supervisor to allow safe concurrent access in async tasks.
    let cloned_supervisor = supervisor.clone();
    let (mut sender, mut receiver): (SplitSink<WebSocket, Message>, SplitStream<WebSocket>) =
        socket.split();

    // Spawn a task for handling outgoing messages.
    let mut sender_task: JoinHandle<Result<(), Error>> =
        tokio::spawn(
            async move { handle_outgoing_messages(&cloned_supervisor, &mut sender).await },
        );

    // Spawn a task for handling incoming messages.
    let mut receiver_task: JoinHandle<Result<(), Error>> =
        tokio::spawn(async move { handle_incoming_messages(&supervisor, &mut receiver).await });

    // Wait for either the sender or receiver task to complete and then clean up.
    tokio::select! {
        event_s = &mut sender_task => {
            match event_s {
                Ok(_) => {
                    tracing::info!("Sender task finished");
                    receiver_task.abort();
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Sender task failed: {}", e);
                    receiver_task.abort();
                    Err(e.into())
                }
            }
        }
        peer_r = &mut receiver_task => {
            match peer_r {
                Ok(_) => {
                    tracing::info!("Receiver task finished");
                    sender_task.abort();
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Receiver task failed: {}", e);
                    sender_task.abort();
                    Err(e.into())
                }
            }
        }
    }
}

async fn handle_outgoing_messages(
    supervisor: &Arc<Supervisor>,
    sender: &mut SplitSink<WebSocket, Message>,
) -> Result<(), anyhow::Error> {
    let mut event_rx = supervisor.event_rx.lock().await;
    while let Some((event, peer_id)) = event_rx.recv().await {
        tracing::info!("Received event {} for peer {}", event, peer_id);
        let serialized_msg: Vec<u8> = bincode::serialize(&(event, peer_id))
            .map_err(|e| anyhow!("Failed to serialize message: {}", e))?;

        if let Err(e) = sender.send(Message::Binary(serialized_msg)).await {
            tracing::error!("Failed to send event {} for peer {}: {}", event, peer_id, e);
        }
    }
    Ok(())
}

async fn handle_incoming_messages(
    supervisor: &Arc<Supervisor>,
    receiver: &mut SplitStream<WebSocket>,
) -> Result<(), anyhow::Error> {
    while let Some(result) = receiver.next().await {
        // Handle the received message or log the error.
        match result {
            Ok(message) => process_message(message, supervisor).await?,
            Err(e) => eprintln!("Error in WebSocket communication: {}", e),
        }
    }
    Ok(())
}

async fn process_message(
    message: Message,
    supervisor: &Arc<Supervisor>,
) -> Result<(), anyhow::Error> {
    match message {
        Message::Binary(bytes) => {
            let peer_msg: PeerMessage = bincode::deserialize(&bytes)
                .map_err(|e| anyhow!("Failed to deserialize message: {}", e))?;
            handle_peer_message(peer_msg, supervisor).await
        }
        Message::Text(error_msg) => {
            tracing::error!("Received error message: {:?}", error_msg);
            Ok(())
        }
        _ => {
            tracing::error!("Received unexpected message: {:?}", message);
            Ok(())
        }
    }
}

async fn handle_peer_message(
    peer_msg: PeerMessage,
    supervisor: &Arc<Supervisor>,
) -> Result<(), anyhow::Error> {
    match peer_msg {
        PeerMessage::Event(event) => {
            // TODO: Implement actual event handling logic here.
            tracing::info!("Received event: {:?}", event);
            Ok(())
        }
        // Handle Status messages.
        PeerMessage::Status(status) => {
            tracing::info!("Received status: {:?}", status);
            match status {
                PeerStatus::PeerStarted(id) => {
                    tracing::info!("Received peer started message for id {}", id);
                    supervisor.dequeue_peer(id).await;
                    Ok(())
                }
                PeerStatus::GatewayStarted(id) => {
                    tracing::info!("Received gateway started message for id {}", id);
                    supervisor.dequeue_gateway(id).await;
                    Ok(())
                }
                PeerStatus::Error(error_msg) => {
                    tracing::error!("{}", error_msg);
                    Ok(())
                }
            }
        }
        PeerMessage::Info(info_msg) => {
            tracing::info!("{}", info_msg);
            Ok(())
        }
    }
}

pub struct Supervisor {
    peers_config: Arc<Mutex<HashMap<NodeLabel, NodeConfig>>>,
    processes: Mutex<HashMap<PeerId, SubProcess>>,
    waiting_peers: Arc<Mutex<VecDeque<usize>>>,
    waiting_gateways: Arc<Mutex<VecDeque<usize>>>,
    user_ev_controller: Arc<Mutex<tokio::sync::mpsc::Sender<(u32, PeerId)>>>,
    event_rx: Arc<Mutex<tokio::sync::mpsc::Receiver<(u32, PeerId)>>>,
}

impl Supervisor {
    pub async fn new(network: &mut SimNetwork) -> Self {
        let peers = network.build_peers();
        let peers_config = Arc::new(Mutex::new(peers.into_iter().collect::<HashMap<_, _>>()));
        let (user_ev_controller, event_rx) = tokio::sync::mpsc::channel(1);

        Supervisor {
            peers_config,
            processes: Mutex::new(HashMap::new()),
            waiting_peers: Arc::new(Mutex::new(VecDeque::new())),
            waiting_gateways: Arc::new(Mutex::new(VecDeque::new())),
            user_ev_controller: Arc::new(Mutex::new(user_ev_controller)),
            event_rx: Arc::new(Mutex::new(event_rx)),
        }
    }
    async fn start_process(
        &self,
        cmd_args: &[String],
        label: &NodeLabel,
        config: &NodeConfig,
    ) -> Result<(), Error> {
        let process = SubProcess::start(cmd_args, label, config.peer_id).await?;
        self.processes.lock().await.insert(config.peer_id, process);
        Ok(())
    }

    pub async fn get_all_peers(&self) -> Vec<(NodeLabel, NodeConfig)> {
        let mut peers: Vec<(NodeLabel, NodeConfig)> = self.get_peer_gateways().await;
        peers.extend(self.get_peer_nodes().await);

        peers.sort_by(|a, b| a.0.cmp(&b.0));

        peers
    }
    pub async fn get_peer_nodes(&self) -> Vec<(NodeLabel, NodeConfig)> {
        self.peers_config
            .lock()
            .await
            .iter()
            .filter(|(_, config)| !config.is_gateway())
            .map(|(label, config)| (label.clone(), config.clone()))
            .collect()
    }

    pub async fn get_peer_gateways(&self) -> Vec<(NodeLabel, NodeConfig)> {
        self.peers_config
            .lock()
            .await
            .iter()
            .filter(|(_, config)| config.is_gateway())
            .map(|(label, config)| (label.clone(), config.clone()))
            .collect()
    }

    pub async fn start_peer_nodes(&self, cmd_args: &[String]) -> Result<(), Error> {
        let nodes: Vec<(NodeLabel, NodeConfig)> = self.get_peer_nodes().await;
        for (label, config) in nodes {
            self.enqueue_node(label.number()).await;
            self.start_process(cmd_args, &label, &config).await?;
        }
        tracing::info!("Waiting for all gateways to start");
        while !self.waiting_gateways.lock().await.is_empty() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::info!("All gateways started");
        Ok(())
    }

    async fn wait_while_gateway_start(&self, id: &usize) {
        tracing::info!("Waiting for gateway {} to start", id);
        while !self.waiting_gateways.lock().await.contains(id) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::info!("Gateway {} started", id);
    }

    pub async fn start_peer_gateways(&self, cmd_args: &[String]) -> Result<(), Error> {
        let nodes: Vec<(NodeLabel, NodeConfig)> = self.get_peer_gateways().await;
        for (label, config) in nodes {
            self.enqueue_gateway(label.number()).await;
            self.start_process(cmd_args, &label, &config).await?;
            self.wait_while_gateway_start(&label.number()).await;
        }
        tracing::info!("All gateways started");
        Ok(())
    }

    pub async fn enqueue_node(&self, id: usize) {
        tracing::info!("Enqueueing node {}", id);
        let mut queue = self.waiting_peers.lock().await;
        queue.push_back(id);
    }

    pub async fn dequeue_peer(&self, id: usize) {
        tracing::info!("Dequeueing node {}", id);
        let mut queue = self.waiting_peers.lock().await;
        if let Some(position) = queue.iter().position(|x| x == &id) {
            queue.remove(position);
        }
    }

    pub async fn enqueue_gateway(&self, id: usize) {
        tracing::info!("Enqueueing gateway {}", id);
        let mut queue = self.waiting_gateways.lock().await;
        queue.push_back(id);
    }

    pub async fn dequeue_gateway(&self, id: usize) {
        tracing::info!("Dequeueing gateway {}", id);
        let mut queue = self.waiting_gateways.lock().await;
        if let Some(position) = queue.iter().position(|x| x == &id) {
            queue.remove(position);
        }
    }
}

pub trait Runnable {
    async fn run(&self, config: &TestConfig, peer_id: String) -> anyhow::Result<()>;
}

impl Runnable for NetworkPeer {
    async fn run(&self, config: &TestConfig, peer_id: String) -> anyhow::Result<()> {
        tracing::info!("Starting node {}", peer_id);
        let mut receiver_ch = self.receiver_ch.deref().clone();
        receiver_ch.borrow_and_update();

        let mut memory_event_generator = MemoryEventsGen::<fastrand::Rng>::new_with_seed(
            receiver_ch,
            self.config.peer_id,
            config.seed.expect("seed should be set for child process"),
        );
        let peer_id_num = NodeLabel::from(peer_id.as_str()).number();

        memory_event_generator.rng_params(
            peer_id_num,
            config.gateways + config.nodes,
            config.max_contract_number.unwrap_or(config.nodes * 10),
            config.events as usize,
        );

        let ws_client = match self.ws_client.clone() {
            Some(ws_client) => ws_client,
            None => {
                return Err(anyhow!("Websocket client not initialized"));
            }
        };

        let event_generator = NetworkEventGenerator::new(
            self.config.peer_id,
            memory_event_generator,
            ws_client,
            config.seed(),
        );

        // Obtain an identity::Keypair instance for the private_key
        let private_key = Keypair::generate_ed25519();

        match self
            .build(peer_id.clone(), [Box::new(event_generator)], private_key)
            .await
        {
            Ok(node) => match node.run().await {
                Ok(_) => {
                    tracing::info!("Node {} finished", peer_id);
                    let msg = match self.config.is_gateway() {
                        true => PeerMessage::Status(PeerStatus::GatewayStarted(peer_id_num)),
                        false => PeerMessage::Status(PeerStatus::PeerStarted(peer_id_num)),
                    };
                    self.send_peer_msg(msg).await;
                }
                Err(e) => {
                    tracing::error!("Node {} failed: {}", peer_id, e);
                }
            },
            Err(e) => {
                tracing::error!("Failed to build node: {}", e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use tokio;

    fn build_supervisor() -> Supervisor {
        Supervisor {
            peers_config: Arc::new(Mutex::new(HashMap::new())),
            processes: Mutex::new(HashMap::new()),
            waiting_peers: Arc::new(Mutex::new(VecDeque::new())),
            waiting_gateways: Arc::new(Mutex::new(VecDeque::new())),
            user_ev_controller: Arc::new(Mutex::new(tokio::sync::mpsc::channel(1).0)),
            event_rx: Arc::new(Mutex::new(tokio::sync::mpsc::channel(1).1)),
        }
    }

    #[tokio::test]
    async fn test_network() {
        // Generate keys for the nodes
        let peer1_key = Keypair::generate_ed25519();
        let peer2_key = Keypair::generate_ed25519();

        // Create configurations for the peers
        let mut peer_config1 = NodeConfig::new();
        peer_config1
            .with_ip(IpAddr::V4(Ipv4Addr::LOCALHOST))
            .with_port(3001);
        let mut peer_config2 = NodeConfig::new();
        peer_config2
            .with_ip(IpAddr::V4(Ipv4Addr::LOCALHOST))
            .with_port(3002);

        // Create event generators for the peer 1
        let (user_ev_controller1, receiver_ch1): (Sender<(u32, PeerId)>, Receiver<(u32, PeerId)>) =
            tokio::sync::watch::channel((0, peer_config1.peer_id));
        let mut event_generator1 = MemoryEventsGen::<fastrand::Rng>::new_with_seed(
            receiver_ch1.clone(),
            peer_config1.peer_id,
            1994,
        );

        // Create event generators for the peer 2
        let (user_ev_controller2, receiver_ch2): (Sender<(u32, PeerId)>, Receiver<(u32, PeerId)>) =
            tokio::sync::watch::channel((0, peer_config2.peer_id));
        let mut event_generator2 = MemoryEventsGen::<fastrand::Rng>::new_with_seed(
            receiver_ch2.clone(),
            peer_config2.peer_id,
            1994,
        );

        let peer1 = NetworkPeer {
            id: "peer1".to_string(),
            config: peer_config1.clone(),
            ws_client: None,
            user_ev_controller: Arc::new(user_ev_controller1),
            receiver_ch: Arc::new(receiver_ch1),
        }
        .build("peer1".to_string(), [Box::new(event_generator1)], peer1_key)
        .await
        .unwrap();

        let peer2 = NetworkPeer {
            id: "peer2".to_string(),
            config: peer_config2.clone(),
            ws_client: None,
            user_ev_controller: Arc::new(user_ev_controller2),
            receiver_ch: Arc::new(receiver_ch2),
        }
        .build("peer2".to_string(), [Box::new(event_generator2)], peer2_key)
        .await
        .unwrap();

        // Run the nodes
        todo!();
    }
}
