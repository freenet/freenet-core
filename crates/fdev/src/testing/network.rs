use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    io::Write,
    net::SocketAddr,
    ops::Deref,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, bail};
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
    EventChain, MemoryEventsGen, NetworkEventGenerator, NetworkPeer, NodeConfig, NodeLabel,
    PeerMessage, PeerStatus, SimNetwork, TransportPublicKey,
};
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use http::{Response, StatusCode};
use thiserror::Error;
use tokio::{
    process::Command,
    sync::{oneshot, Mutex},
    task::JoinHandle,
};

use super::{Error, TestConfig};

mod v1;

#[derive(Debug, Error)]
pub enum NetworkSimulationError {
    #[error("Server start failed: {0}")]
    ServerStartFailure(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Subprocess start failed: {0}")]
    SubProcessStartFailure(String),
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

    async fn start(cmd_args: &[String], label: &NodeLabel) -> anyhow::Result<Self, Error> {
        let mut command = if cfg!(debug_assertions) {
            Command::new("cargo")
        } else {
            Command::new("fdev")
        };
        #[cfg(debug_assertions)]
        {
            let args = ["run", "--"]
                .into_iter()
                .chain(cmd_args.iter().map(Deref::deref));
            command.args(args);
        }
        #[cfg(not(debug_assertions))]
        {
            let args = cmd_args;
            command.args(args);
        }
        let child = command
            .kill_on_drop(true)
            .arg("--id")
            .arg(label.to_string())
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| {
                NetworkSimulationError::SubProcessStartFailure(format!(
                    "Failed to start subprocess: {}",
                    e
                ))
            })?;

        Ok(Self { child })
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

async fn start_child(config: &TestConfig, cmd_config: &NetworkProcessConfig) -> Result<(), Error> {
    let Some(peer_id) = &cmd_config.id else {
        bail!("Peer id not set");
    };
    std::env::set_var("FREENET_PEER_ID", peer_id);
    freenet::config::set_logger(None, None);
    let peer = NetworkPeer::new(peer_id.clone()).await?;
    peer.run(config, peer_id.clone()).await?;
    Ok(())
}

pub(super) async fn run(
    config: &TestConfig,
    cmd_config: &NetworkProcessConfig,
) -> Result<(), Error> {
    match &cmd_config.mode {
        Process::Supervisor => start_supervisor(config).await,
        Process::Peer => {
            if let Err(err) = start_child(config, cmd_config).await {
                let _ = std::io::stderr()
                    .write_all(format!("exit cause (node: {:?}): {err}", cmd_config.id).as_bytes());
                std::process::exit(1)
            }
            Ok(())
        }
    }
}

pub async fn start_server(supervisor: Arc<Supervisor>) -> Result<(), NetworkSimulationError> {
    v1::start_server_v1(supervisor).await
}

pub async fn run_network(
    supervisor: Arc<Supervisor>,
    test_config: &TestConfig,
    network: SimNetwork,
) -> Result<(), Error> {
    tracing::info!("Starting network");

    let cmd_args = SubProcess::build_command(test_config, test_config.seed());
    supervisor.start_peer_gateways(&cmd_args).await?;
    supervisor.start_peer_nodes(&cmd_args).await?;

    let peers: Vec<_> = supervisor
        .get_all_peers()
        .await
        .into_iter()
        .map(|(label, config)| (label.clone(), config.key_pair.public().clone()))
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
        // FIXME: we are getting connectivity check that is not real since peers are not reporting if they
        // are connected or not to other peers
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
    tracing::debug!("Received config request for peer_id: {}", peer_id);
    let config = peers_config.lock().await;
    let id = NodeLabel::from(peer_id.as_str());
    match config.get(&id) {
        Some(node_config) => axum::response::Json(node_config.clone()).into_response(),
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

async fn handle_socket(socket: WebSocket, supervisor: Arc<Supervisor>) -> anyhow::Result<()> {
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
) -> anyhow::Result<()> {
    let mut event_rx = supervisor.event_rx.lock().await;
    while let Some((event, peer_id)) = event_rx.recv().await {
        tracing::info!("Sending event {} to peer {}", event, peer_id);
        let serialized_msg: Vec<u8> = bincode::serialize(&(event, peer_id.clone()))
            .map_err(|e| anyhow!("Failed to serialize message: {}", e))?;

        if let Err(e) = sender.send(Message::Binary(serialized_msg)).await {
            tracing::error!(
                "Failed to send event {} for peer {}: {}",
                event,
                peer_id.clone(),
                e
            );
        }
    }
    Ok(())
}

async fn handle_incoming_messages(
    supervisor: &Arc<Supervisor>,
    receiver: &mut SplitStream<WebSocket>,
) -> anyhow::Result<()> {
    while let Some(result) = receiver.next().await {
        // Handle the received message or log the error.
        match result {
            Ok(message) => process_message(message, supervisor).await?,
            Err(e) => eprintln!("Error in WebSocket communication: {}", e),
        }
    }
    Ok(())
}

async fn process_message(message: Message, supervisor: &Arc<Supervisor>) -> anyhow::Result<()> {
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
) -> anyhow::Result<()> {
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
    processes: Mutex<HashMap<TransportPublicKey, SubProcess>>,
    waiting_peers: Arc<Mutex<VecDeque<usize>>>,
    waiting_gateways: Arc<Mutex<VecDeque<usize>>>,
    user_ev_controller: Arc<Mutex<tokio::sync::mpsc::Sender<(u32, TransportPublicKey)>>>,
    event_rx: Arc<Mutex<tokio::sync::mpsc::Receiver<(u32, TransportPublicKey)>>>,
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
        let process = SubProcess::start(cmd_args, label).await?;
        self.processes
            .lock()
            .await
            .insert(config.key_pair.public().clone(), process);
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
            .filter(|(_, config)| !config.is_gateway)
            .map(|(label, config)| (label.clone(), config.clone()))
            .collect()
    }

    pub async fn get_peer_gateways(&self) -> Vec<(NodeLabel, NodeConfig)> {
        self.peers_config
            .lock()
            .await
            .iter()
            .filter(|(_, config)| config.is_gateway)
            .map(|(label, config)| (label.clone(), config.clone()))
            .collect()
    }

    async fn wait_while_node_start(&self, id: &usize) {
        tracing::info!("Waiting for node {} to start", id);
        while !self.waiting_peers.lock().await.contains(id) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::info!("Node {} started", id);
    }

    pub async fn start_peer_nodes(&self, cmd_args: &[String]) -> Result<(), Error> {
        let nodes: Vec<(NodeLabel, NodeConfig)> = self.get_peer_nodes().await;
        for (label, config) in nodes {
            self.enqueue_node(label.number()).await;
            self.start_process(cmd_args, &label, &config).await?;
            self.wait_while_node_start(&label.number()).await;
        }
        tracing::info!("All nodes started");
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
        let peer = self.config.key_pair.public().clone();
        if self.config.is_gateway {
            tracing::info!(%peer, "Starting gateway {}", peer_id);
        } else {
            tracing::info!(%peer, "Starting node {}", peer_id);
        }
        let mut receiver_ch = self.receiver_ch.deref().clone();
        receiver_ch.borrow_and_update();

        let mut memory_event_generator: MemoryEventsGen = MemoryEventsGen::new_with_seed(
            receiver_ch,
            peer.clone(),
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

        tracing::info!("Starting event generator for peer {}", peer.clone());

        let event_generator =
            NetworkEventGenerator::new(peer.clone(), memory_event_generator, ws_client);

        match self
            .build(peer_id.clone(), [Box::new(event_generator)])
            .await
        {
            Ok(node) => match node.run().await {
                Ok(_) => {
                    if self.config.is_gateway {
                        tracing::info!("Gateway {} finished", peer_id);
                    } else {
                        tracing::info!("Node {} finished", peer_id);
                    }
                    let msg = match self.config.is_gateway {
                        true => PeerMessage::Status(PeerStatus::GatewayStarted(peer_id_num)),
                        false => PeerMessage::Status(PeerStatus::PeerStarted(peer_id_num)),
                    };
                    self.send_peer_msg(msg).await;
                }
                Err(e) => {
                    tracing::error!("Node {} failed: {}", peer_id, e);
                    bail!("Node {} failed: {}", peer_id, e);
                }
            },
            Err(e) => {
                tracing::error!("Failed to build node: {}", e);
            }
        }

        Ok(())
    }
}
