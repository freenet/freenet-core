use crate::client_events::BoxedClient;
use crate::contract::MemoryContractHandler;
use crate::node::p2p_impl::NodeP2P;
use crate::node::Node;
use crate::tracing::EventRegister;
use anyhow::Error;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub struct NetworkPeer {
    pub id: String,
    pub config: crate::node::NodeConfig,
    pub ws_client: Option<Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    pub user_ev_controller: Arc<Sender<(u32, crate::node::PeerId)>>,
    pub receiver_ch: Arc<Receiver<(u32, crate::node::PeerId)>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerStatus {
    PeerStarted(usize),
    GatewayStarted(usize),
    Error(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Event(Vec<u8>),
    Status(PeerStatus),
    Info(String),
}

type PeerEventSender = Sender<(u32, crate::node::PeerId)>;
type PeerEventReceiver = Receiver<(u32, crate::node::PeerId)>;

impl NetworkPeer {
    pub async fn new(peer_id: String) -> Result<Self, Error> {
        let (ws_stream, _) = tokio_tungstenite::connect_async("ws://localhost:3000/ws")
            .await
            .expect("Failed to connect to supervisor");

        let config_url = format!("http://localhost:3000/config/{}", peer_id);
        let response = reqwest::get(&config_url).await?;
        let peer_config = response.json::<crate::node::NodeConfig>().await?;

        tracing::debug!("Received peer config");

        let (user_ev_controller, receiver_ch): (PeerEventSender, PeerEventReceiver) =
            tokio::sync::watch::channel((0, peer_config.get_peer_id().unwrap()));

        Ok(NetworkPeer {
            id: peer_id,
            config: peer_config,
            ws_client: Some(Arc::new(Mutex::new(ws_stream))),
            user_ev_controller: Arc::new(user_ev_controller),
            receiver_ch: Arc::new(receiver_ch),
        })
    }

    /// Builds a node using the default backend connection manager.
    pub async fn build<const CLIENTS: usize>(
        &self,
        identifier: String,
        clients: [BoxedClient; CLIENTS],
    ) -> Result<Node, anyhow::Error> {
        let event_register = {
            #[cfg(feature = "trace-ot")]
            {
                use crate::tracing::OTEventRegister;
                crate::tracing::CombinedRegister::new([
                    Box::new(EventRegister::new(self.config.config.event_log())),
                    Box::new(OTEventRegister::new()),
                ])
            }
            #[cfg(not(feature = "trace-ot"))]
            {
                EventRegister::new(self.config.config.event_log())
            }
        };
        let node = NodeP2P::build::<MemoryContractHandler, CLIENTS, _>(
            self.config.clone(),
            clients,
            event_register,
            identifier,
        )
        .await?;
        Ok(Node(node))
    }

    pub async fn send_peer_msg(&self, msg: PeerMessage) {
        let serialized_msg: Vec<u8> = bincode::serialize(&msg).unwrap();
        if let Some(ws_client) = self.ws_client.as_deref() {
            ws_client
                .lock()
                .await
                .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                    serialized_msg,
                ))
                .await
                .unwrap();
        }
    }
}
