use super::{Error, TestConfig};
use axum::body::Body;
use axum::extract::ws::Message;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        Path,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use freenet::dev_tool::{
    EventChain, MemoryEventsGen, NetworkEventGenerator, NodeConfig, NodeLabel, OperationMode,
    PeerCliConfig, PeerId, SimNetwork,
};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt};
use http::{Response, StatusCode};
use libp2p_identity::Keypair;
use reqwest;
use std::fmt::Display;
use std::process::Stdio;
use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::net::TcpStream;
use tokio::process::{ChildStdout, Command};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

#[derive(clap::Parser, Default, Clone)]
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
    #[clap(subcommand)]
    pub mode: Process,
    #[arg(long)]
    id: Option<usize>,
}

pub(super) async fn run(
    config: &TestConfig,
    cmd_config: &NetworkProcessConfig,
) -> anyhow::Result<(), Error> {
    match &cmd_config.mode {
        Process::Supervisor => {
            let mut network = super::config_sim_network(config).await.unwrap();
            network.debug(); // set to avoid deleting temp dirs created

            let mut supervisor = Supervisor::new(config, &mut network).await;
            supervisor.run_network(config, network).await;
            Ok(())
        }
        Process::Peer => {
            if let Some(peer_id) = cmd_config.id {
                let peer = Peer::new(peer_id).await;
                peer.run(config, peer_id).await;
            }
            Ok(())
        }
    }
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
        args.push("child".to_owned());

        args
    }

    fn start(cmd_args: &[String], label: &NodeLabel, id: PeerId) -> anyhow::Result<Self, Error> {
        let child = Command::new("fdev")
            .kill_on_drop(true)
            .args(cmd_args)
            .arg("--id")
            .arg(label.number().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(Self { child, id })
    }
}

type PeerResponses = Vec<(PeerId, Vec<u8>)>;

pub struct Supervisor {
    peers_config: Arc<Mutex<HashMap<NodeLabel, NodeConfig>>>,
    processes: HashMap<PeerId, SubProcess>,
    responses: FuturesUnordered<BoxFuture<'static, PeerResponses>>,
}

impl Supervisor {
    pub async fn new(config: &super::TestConfig, network: &mut SimNetwork) -> Self {
        let peers = network.build_peers();
        let peers_config = Arc::new(Mutex::new(peers.into_iter().collect::<HashMap<_, _>>()));

        Supervisor {
            peers_config,
            processes: HashMap::new(),
            responses: FuturesUnordered::new(),
        }
    }

    async fn server_task(
        peers_config: Arc<Mutex<HashMap<NodeLabel, NodeConfig>>>,
        startup_sender: oneshot::Sender<()>,
    ) -> JoinHandle<Result<(), ()>> {
        let router = Router::new().route("/ws", get(ws_handler)).route(
            "/config/:peer_id",
            get(|path: Path<String>| config_handler(peers_config, path)),
        );
        let socket = SocketAddr::from(([0, 0, 0, 0], 3000));

        tokio::spawn(async move {
            tracing::info!("Supervisor running on {}", socket);
            let listener = tokio::net::TcpListener::bind(socket).await.unwrap();
            if let Err(_) = startup_sender.send(()) {
                tracing::error!("Failed to send startup signal");
                return Err(());
            }
            axum::serve(listener, router).await.map_err(|e| {
                tracing::error!("Error while running HTTP supervisor server: {e}");
                ()
            })
        })
    }

    pub async fn run_network(&mut self, test_config: &TestConfig, network: SimNetwork) {
        let (startup_sender, startup_receiver) = oneshot::channel();
        let server_task = Self::server_task(self.peers_config.clone(), startup_sender);
        let (user_ev_controller, event_rx) = tokio::sync::mpsc::channel(1);

        // Wait for server to start
        tokio::pin!(server_task);
        if startup_receiver.await.is_err() {
            tracing::error!("Server failed to start");
            return;
        }

        let cmd_args = SubProcess::build_command(&test_config, test_config.seed());
        for (label, config) in self.peers_config.lock().await.iter() {
            let process = SubProcess::start(&cmd_args, &label, config.peer_id).unwrap();
            self.processes.insert(config.peer_id, process);
        }

        let peers: Vec<(NodeLabel, PeerId)> = self
            .peers_config
            .lock()
            .await
            .iter()
            .map(|(label, config)| (label.clone(), config.peer_id))
            .collect();

        let mut events = EventChain::new(peers, user_ev_controller, test_config.events, true);
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
    }
}

async fn config_handler(
    peers_config: Arc<Mutex<HashMap<NodeLabel, NodeConfig>>>,
    Path(peer_id): Path<String>,
) -> axum::response::Response {
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

async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(handle_socket)
}

async fn handle_socket(mut socket: WebSocket) {
    while let Some(result) = socket.recv().await {
        match result {
            Ok(message) => {
                match message {
                    Message::Binary(bytes) => {
                        // todo: define a type to deserialize the message
                        let msg: String = bincode::deserialize(&bytes).unwrap();
                        println!("Received message: {:?}", msg);
                        todo!("handle message")
                    }
                    Message::Text(_) => {}
                    Message::Ping(_) => {}
                    Message::Pong(_) => {}
                    Message::Close(_) => {}
                }
            }
            Err(e) => eprintln!("Error in WebSocket communication: {}", e),
        }
    }
}

struct Peer {
    id: usize,
    config: NodeConfig,
    ws_client: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
}

impl Peer {
    async fn new(peer_id: usize) -> Self {
        let config_url = format!("http://localhost:3000/config/{}", peer_id);
        let peer_config: NodeConfig = reqwest::get(&config_url)
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let (ws_stream, _) = tokio_tungstenite::connect_async("ws://localhost:3000/ws")
            .await
            .expect("Failed to connect to supervisor");

        Peer {
            id: peer_id,
            config: peer_config,
            ws_client: Arc::new(Mutex::new(ws_stream)),
        }
    }

    async fn run(&self, test_config: &super::TestConfig, peer_id: usize) -> anyhow::Result<()> {
        let (user_ev_controller, mut receiver_ch) =
            tokio::sync::watch::channel((0, self.config.peer_id));
        receiver_ch.borrow_and_update();
        let mut memory_event_generator = MemoryEventsGen::<fastrand::Rng>::new_with_seed(
            receiver_ch.clone(),
            self.config.peer_id,
            test_config
                .seed
                .expect("seed should be set for child process"),
        );
        memory_event_generator.rng_params(
            self.id,
            test_config.gateways + test_config.nodes,
            test_config
                .max_contract_number
                .unwrap_or(test_config.nodes * 10),
            test_config.events as usize,
        );
        let event_generator =
            NetworkEventGenerator::new(memory_event_generator, self.ws_client.clone());

        // Obtain an identity::Keypair instance for the private_key
        let private_key = Keypair::generate_ed25519();

        let cli_config = PeerCliConfig {
            mode: OperationMode::Network,
            node_data_dir: None,
            address: self.config.local_ip.unwrap(),
            port: self.config.local_port.unwrap(),
        };

        match self
            .config
            .clone()
            .build::<1>(cli_config, [Box::new(event_generator)], private_key)
            .await
        {
            Ok(node) => match node.run().await {
                Ok(_) => {
                    tracing::info!("Node {} finished", peer_id);
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
    use tokio;

    #[tokio::test]
    async fn test_supervisor() {
        let network_config = NetworkProcessConfig {
            mode: Process::Supervisor,
            id: Some(0),
        };
        let config = super::super::TestConfig {
            name: Some("TestName".to_string()),
            seed: Some(12345),
            gateways: 1,
            nodes: 2,
            ring_max_htl: 20,
            rnd_if_htl_above: 10,
            max_connections: 20,
            min_connections: 10,
            max_contract_number: Some(100),
            events: 5,
            event_wait_ms: Some(1000),
            connection_wait_ms: Some(2000),
            peer_start_backoff_ms: Some(2000),
            execution_data: None,
            disable_metrics: false,
            command: super::super::TestMode::Network(NetworkProcessConfig {
                mode: Process::Supervisor,
                id: None,
            }),
        };
        let res = run(&config, &network_config).await.unwrap();
        let a = 2;
    }
}
