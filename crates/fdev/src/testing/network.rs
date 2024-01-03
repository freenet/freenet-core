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
    EventChain, Executor, MemoryEventsGen, NetworkEventGenerator, NodeConfig, NodeLabel,
    OperationMode, PeerCliConfig, PeerId, Runtime, SimNetwork,
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
    net::TcpStream,
    process::Command,
    sync::{oneshot, Mutex},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
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

pub(super) async fn run(
    config: &TestConfig,
    cmd_config: &NetworkProcessConfig,
) -> Result<(), Error> {
    match &cmd_config.mode {
        Process::Supervisor => {
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
        Process::Peer => {
            if let Some(peer_id) = &cmd_config.id {
                let mut peer = Peer::new(peer_id.clone()).await?;
                peer.run(config, peer_id.clone()).await?;
            }
            Ok(())
        }
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
    start_processes(supervisor.clone(), &cmd_args, test_config).await?;

    tracing::info!("Waiting for all peers to start");
    wait_for_peers(supervisor.clone()).await;
    tracing::info!("All peers started");

    let peers: Vec<(NodeLabel, PeerId)> = supervisor
        .peers_config
        .lock()
        .await
        .iter()
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

async fn start_processes(
    supervisor: Arc<Supervisor>,
    cmd_args: &[String],
    test_config: &TestConfig,
) -> Result<(), Error> {
    for (label, config) in supervisor.peers_config.lock().await.iter() {
        let process = SubProcess::start(&cmd_args, &label, config.peer_id)
            .await
            .unwrap();
        supervisor
            .processes
            .lock()
            .await
            .insert(config.peer_id, process);
        supervisor.enqueue_peer(label.number()).await;
    }
    Ok(())
}

async fn wait_for_peers(supervisor: Arc<Supervisor>) {
    while !supervisor.waiting_peers.lock().await.is_empty() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn config_handler(
    peers_config: Arc<Mutex<HashMap<NodeLabel, NodeConfig>>>,
    Path(peer_id): Path<String>,
) -> axum::response::Response {
    let config = peers_config.lock().await;
    let id = NodeLabel::from(peer_id.as_str());
    tracing::info!("Received config request for peer_id: {}", peer_id);
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
                PeerStatus::Finished(id) => {
                    supervisor.dequeue_peer(id).await;
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
            user_ev_controller: Arc::new(Mutex::new(user_ev_controller)),
            event_rx: Arc::new(Mutex::new(event_rx)),
        }
    }

    pub async fn enqueue_peer(&self, id: usize) {
        tracing::info!("Enqueueing peer {}", id);
        let mut queue = self.waiting_peers.lock().await;
        queue.push_back(id);
    }

    pub async fn dequeue_peer(&self, id: usize) {
        tracing::info!("Dequeueing peer {}", id);
        let mut queue = self.waiting_peers.lock().await;
        if let Some(position) = queue.iter().position(|x| x == &id) {
            queue.remove(position);
        }
    }
}

pub trait Runnable {
    async fn run(&self, config: &TestConfig, peer_id: String) -> anyhow::Result<()>;
}

#[derive(Debug, Serialize, Deserialize)]
enum PeerStatus {
    Finished(usize),
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
enum PeerMessage {
    Event(Vec<u8>),
    Status(PeerStatus),
    Info(String),
}

struct Peer {
    id: String,
    config: NodeConfig,
    ws_client: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    user_ev_controller: Arc<Sender<(u32, PeerId)>>,
    receiver_ch: Arc<Receiver<(u32, PeerId)>>,
}

impl Peer {
    async fn new(peer_id: String) -> Result<Self, Error> {
        let (ws_stream, _) = tokio_tungstenite::connect_async("ws://localhost:3000/ws")
            .await
            .expect("Failed to connect to supervisor");

        let config_url = format!("http://localhost:3000/config/{}", peer_id);
        let response = reqwest::get(&config_url).await?;
        tracing::info!("Response config from server: {:?}", response);
        let peer_config = response.json::<NodeConfig>().await?;

        let (user_ev_controller, receiver_ch): (Sender<(u32, PeerId)>, Receiver<(u32, PeerId)>) =
            tokio::sync::watch::channel((0, peer_config.peer_id));

        Ok(Peer {
            id: peer_id,
            config: peer_config,
            ws_client: Arc::new(Mutex::new(ws_stream)),
            user_ev_controller: Arc::new(user_ev_controller),
            receiver_ch: Arc::new(receiver_ch),
        })
    }

    async fn event_loop(
        ws_client: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
        user_ev_controller: Arc<Sender<(u32, PeerId)>>,
    ) -> anyhow::Result<()> {
        let ws_client = ws_client.clone();
        loop {
            let msg = ws_client.lock().await.next().await;
            match msg {
                Some(Ok(msg)) => match msg {
                    tokio_tungstenite::tungstenite::protocol::Message::Binary(bytes) => {
                        let (event, peer_id) = bincode::deserialize(&bytes).unwrap();
                        user_ev_controller
                            .send((event, peer_id))
                            .map_err(|e| anyhow!("Failed to send event: {}", e))?;
                    }
                    tokio_tungstenite::tungstenite::protocol::Message::Text(error_msg) => {
                        tracing::error!("Received error message: {:?}", error_msg);
                        break Err(anyhow!("Received error message: {:?}", error_msg));
                    }
                    _ => {
                        tracing::error!("Received unexpected message: {:?}", msg);
                        break Err(anyhow!("Received unexpected message: {:?}", msg));
                    }
                },
                Some(Err(e)) => {
                    tracing::error!("Failed to receive message: {}", e);
                    Err(anyhow!("Failed to receive message: {}", e))?;
                }
                None => {
                    tracing::error!("Connection closed");
                    break Err(anyhow!("Connection closed"));
                }
            }
        }
    }

    async fn send_peer_msg(&self, msg: PeerMessage) {
        let serialized_msg: Vec<u8> = bincode::serialize(&msg).unwrap();
        self.ws_client
            .lock()
            .await
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                serialized_msg,
            ))
            .await
            .unwrap();
    }

    async fn send_error_msg(&self, msg: String) {
        self.ws_client
            .lock()
            .await
            .send(tokio_tungstenite::tungstenite::protocol::Message::Text(msg))
            .await
            .unwrap();
    }
}

impl Runnable for Peer {
    async fn run(&self, config: &TestConfig, peer_id: String) -> anyhow::Result<()> {
        let mut receiver_ch = self.receiver_ch.deref().clone();
        receiver_ch.borrow_and_update();

        let conf_str = serde_json::to_string(&self.config).unwrap();
        Self::send_peer_msg(
            self,
            PeerMessage::Info(format!("Node {} with config: {}", peer_id, conf_str)),
        )
        .await;
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
        let event_generator = NetworkEventGenerator::new(
            self.config.peer_id,
            memory_event_generator,
            self.ws_client.clone(),
            config.seed(),
        );

        // Obtain an identity::Keypair instance for the private_key
        let private_key = Keypair::generate_ed25519();

        let data_dir = Executor::<Runtime>::test_data_dir(peer_id.as_str());

        let cli_config = PeerCliConfig {
            mode: OperationMode::Network,
            node_data_dir: Some(data_dir),
            address: self.config.local_ip.unwrap(),
            port: self.config.local_port.unwrap(),
        };

        let event_loop_task =
            Self::event_loop(self.ws_client.clone(), self.user_ev_controller.clone());
        tokio::task::spawn(event_loop_task);

        match self
            .config
            .clone()
            .build::<1>(cli_config, [Box::new(event_generator)], private_key)
            .await
        {
            Ok(node) => match node.run().await {
                Ok(_) => {
                    tracing::info!("Node {} finished", peer_id);
                    self.send_peer_msg(PeerMessage::Status(PeerStatus::Finished(peer_id_num)))
                        .await;
                }
                Err(e) => {
                    tracing::error!("Node {} failed: {}", peer_id, e);
                    Self::send_error_msg(self, format!("Node {} failed: {}", peer_id, e)).await;
                }
            },
            Err(e) => {
                Self::send_error_msg(self, format!("Failed to build node: {}", e)).await;
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
            user_ev_controller: Arc::new(Mutex::new(tokio::sync::mpsc::channel(1).0)),
            event_rx: Arc::new(Mutex::new(tokio::sync::mpsc::channel(1).1)),
        }
    }
    #[tokio::test]
    async fn test_peer() {
        let mut supervisor = build_supervisor();

        let network_config = NetworkProcessConfig {
            mode: Process::Peer,
            id: Some("node-1".to_string()),
        };

        let config = super::super::TestConfig {
            name: Some("TestName".to_string()),
            seed: Some(12345),
            gateways: 0,
            nodes: 1,
            ring_max_htl: 20,
            rnd_if_htl_above: 10,
            max_connections: 20,
            min_connections: 0,
            max_contract_number: Some(100),
            events: 2,
            event_wait_ms: Some(1000),
            connection_wait_ms: Some(2000),
            peer_start_backoff_ms: Some(2000),
            execution_data: None,
            disable_metrics: false,
            command: super::super::TestMode::Network(NetworkProcessConfig {
                mode: Process::Peer,
                id: Some("node-1".to_string()),
            }),
        };

        let server_task = start_server(Arc::new(supervisor));
        tokio::task::spawn(server_task);

        let (ws_client, _) = tokio_tungstenite::connect_async("ws://localhost:3000/ws")
            .await
            .expect("Failed to connect to supervisor");

        let (user_ev_controller, mut receiver_ch) =
            tokio::sync::watch::channel((0, PeerId::random()));
        receiver_ch.borrow_and_update();

        let mut node_config = NodeConfig::new();
        node_config
            .with_ip(IpAddr::V4(Ipv4Addr::LOCALHOST))
            .with_port(3001);

        let mut memory_event_generator = MemoryEventsGen::<fastrand::Rng>::new_with_seed(
            receiver_ch,
            node_config.peer_id,
            config.seed.expect("seed should be set for child process"),
        );
        let peer_id_num = NodeLabel::from("node-1").number();

        memory_event_generator.rng_params(
            peer_id_num,
            config.gateways + config.nodes,
            config.max_contract_number.unwrap_or(config.nodes * 10),
            config.events as usize,
        );
        let event_generator = NetworkEventGenerator::new(
            node_config.peer_id,
            memory_event_generator,
            Arc::new(Mutex::new(ws_client)),
            config.seed(),
        );

        // Obtain an identity::Keypair instance for the private_key
        let private_key = Keypair::generate_ed25519();

        let data_dir = Executor::<Runtime>::test_data_dir("node-1");

        let cli_config = PeerCliConfig {
            mode: OperationMode::Network,
            node_data_dir: Some(data_dir),
            address: node_config.local_ip.unwrap(),
            port: node_config.local_port.unwrap(),
        };

        let node = node_config
            .clone()
            .build::<1>(cli_config, [Box::new(event_generator)], private_key)
            .await
            .unwrap();

        match node.run().await {
            Ok(_) => {
                tracing::info!("Node {} finished", "1");
            }
            Err(e) => {
                tracing::error!("Node {} failed: {}", "1", e);
            }
        };
    }
}
