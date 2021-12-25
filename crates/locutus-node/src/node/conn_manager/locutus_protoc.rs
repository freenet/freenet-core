use std::{collections::VecDeque, net::IpAddr, sync::Arc};

use dashmap::{DashMap, DashSet};
use either::{Left, Right};
use futures::{future, FutureExt, StreamExt};
use libp2p::{
    core::{muxing, transport, ConnectedPoint, UpgradeInfo},
    identify,
    identity::Keypair,
    multiaddr::Protocol,
    ping,
    swarm::{
        protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend},
        AddressScore, KeepAlive, NegotiatedSubstream, NetworkBehaviour, ProtocolsHandler,
        ProtocolsHandlerEvent, ProtocolsHandlerUpgrErr, SubstreamProtocol, SwarmBuilder,
        SwarmEvent,
    },
    InboundUpgrade, Multiaddr, OutboundUpgrade, PeerId, Swarm,
};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    config::GlobalExecutor,
    message::Message,
    node::{process_message, OpManager, PeerKey},
    NodeConfig,
};

use super::{ConnectionBridge, ConnectionError};

const CURRENT_AGENT_VER: &str = "locutus/agent/0.1.0";
const CURRENT_PROTOC_VER: &[u8] = b"locutus/agent/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "id/1.0.0";

fn config_behaviour(local_key: &Keypair) -> NetBehaviour {
    let ident_config =
        identify::IdentifyConfig::new(CURRENT_IDENTIFY_PROTOC_VER.to_string(), local_key.public())
            .with_agent_version(CURRENT_AGENT_VER.to_string());

    let ping = if cfg!(debug_assertions) {
        ping::Ping::new(ping::PingConfig::new().with_keep_alive(true))
    } else {
        ping::Ping::default()
    };
    NetBehaviour {
        identify: identify::Identify::new(ident_config),
        ping,
        locutus: LocutusBehaviour {
            queue: VecDeque::new(),
        },
    }
}

/// Small helper function to convert a tuple composed of an IP address and a port
/// to a libp2p Multiaddr type.
fn multiaddr_from_connection(conn: (IpAddr, u16)) -> Multiaddr {
    let mut addr = Multiaddr::with_capacity(2);
    addr.push(Protocol::from(conn.0));
    addr.push(Protocol::Tcp(conn.1));
    addr
}

#[derive(Debug, Clone)]
struct P2pConnBridge {
    active_net_connections: Arc<DashMap<PeerKey, Multiaddr>>,
    accepted_peers: Arc<DashSet<PeerKey>>,
    sender: Sender<(PeerKey, Box<Message>)>,
}

impl P2pConnBridge {
    fn new(sender: Sender<(PeerKey, Box<Message>)>) -> Self {
        Self {
            active_net_connections: Arc::new(DashMap::new()),
            accepted_peers: Arc::new(DashSet::new()),
            sender,
        }
    }
}

#[async_trait::async_trait]
impl ConnectionBridge for P2pConnBridge {
    fn add_connection(&mut self, peer: PeerKey) -> super::ConnResult<()> {
        if self.active_net_connections.contains_key(&peer) {
            self.accepted_peers.insert(peer);
        }
        Ok(())
    }

    fn drop_connection(&mut self, peer: &PeerKey) {
        self.accepted_peers.remove(peer);
    }

    async fn send(&self, target: &PeerKey, msg: Message) -> super::ConnResult<()> {
        self.sender
            .send((*target, Box::new(msg)))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        Ok(())
    }
}

pub(in crate::node) struct LocutusConnManager {
    pub swarm: Swarm<NetBehaviour>,
    listen_on: Option<(IpAddr, u16)>,
}

impl LocutusConnManager {
    pub fn build(
        transport: transport::Boxed<(PeerId, muxing::StreamMuxerBox)>,
        config: &NodeConfig,
    ) -> Self {
        // We set a global executor which is virtually the Tokio multi-threaded executor
        // to reuse it's thread pool and scheduler in order to drive futures.
        let global_executor = Box::new(GlobalExecutor);
        let builder = SwarmBuilder::new(
            transport,
            config_behaviour(&config.local_key),
            PeerId::from(config.local_key.public()),
        )
        .executor(global_executor);

        let mut swarm = builder.build();
        for remote_addr in config.remote_nodes.iter().filter_map(|r| r.addr.clone()) {
            swarm.add_external_address(remote_addr, AddressScore::Infinite);
        }

        LocutusConnManager {
            swarm,
            listen_on: config.local_ip.zip(config.local_port),
        }
    }

    pub fn listen_on(&mut self) -> Result<(), anyhow::Error> {
        if let Some(conn) = self.listen_on {
            let listening_addr = multiaddr_from_connection(conn);
            self.swarm.listen_on(listening_addr)?;
        }
        Ok(())
    }

    pub async fn run_event_listener<CErr>(
        mut self,
        op_manager: Arc<OpManager<CErr>>,
        mut notification_channel: Receiver<Message>,
    ) where
        CErr: std::error::Error + Send + Sync + 'static,
    {
        use ConnMngrActions::*;

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let conn_manager = P2pConnBridge::new(tx);

        loop {
            let net_msg = self.swarm.select_next_some().map(|event| match event {
                SwarmEvent::Behaviour(NetEvent::Locutus(msg)) => Ok(Left(msg)),
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint: ConnectedPoint::Listener { send_back_addr, .. },
                    ..
                } => Ok(Right(ConnMngrActions::ConnectionEstablished {
                    peer_id: PeerKey(peer_id),
                    addr: send_back_addr,
                })),
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint: ConnectedPoint::Dialer { address },
                    ..
                } => Ok(Right(ConnMngrActions::ConnectionEstablished {
                    peer_id: PeerKey(peer_id),
                    addr: address,
                })),
                other_event => {
                    log::debug!("{:?}", other_event);
                    Ok(Right(ConnMngrActions::NoAction))
                }
            });

            let notification_msg = notification_channel.recv().map(|m| match m {
                Some(m) => Ok(Left(Box::new(m))),
                None => Ok(Right(ClosedChannel)),
            });

            let msg: Result<_, ConnectionError> = tokio::select! {
                msg = notification_msg => { msg }
                msg = net_msg => { msg }
                msg = rx.recv() => {
                    if let Some((peer,msg)) = msg {
                        Ok(Right(SendMessage { peer, msg }))
                    } else {
                        Ok(Right(ClosedChannel))
                    }
                }
            };

            match msg {
                Ok(Left(msg)) => {
                    let cb = conn_manager.clone();
                    GlobalExecutor::spawn(process_message(Ok(*msg), op_manager.clone(), cb, None));
                }
                Ok(Right(ConnectionEstablished { addr, peer_id })) => {
                    conn_manager.active_net_connections.insert(peer_id, addr);
                }
                Ok(Right(SendMessage { peer, msg })) => {
                    self.swarm
                        .behaviour_mut()
                        .locutus
                        .queue
                        .push_back((peer.0, *msg));
                    todo!()
                }
                Ok(Right(ClosedChannel)) => {
                    log::info!("notification channel closed");
                    break;
                }
                Err(err) => {
                    let cb = conn_manager.clone();
                    GlobalExecutor::spawn(process_message(Err(err), op_manager.clone(), cb, None));
                    break;
                }
                Ok(Right(NoAction)) => {}
            }
        }
    }

    async fn join(&self) {
        todo!()
    }
}

enum ConnMngrActions {
    /// Received a new connection
    ConnectionEstablished {
        peer_id: PeerKey,
        addr: Multiaddr,
    },
    ClosedChannel,
    SendMessage {
        peer: PeerKey,
        msg: Box<Message>,
    },
    NoAction,
}

pub(crate) struct LocutusBehaviour {
    queue: VecDeque<(PeerId, Message)>,
}

impl NetworkBehaviour for LocutusBehaviour {
    type ProtocolsHandler = Handler;

    type OutEvent = Message;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        Handler {}
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: <<Self::ProtocolsHandler as libp2p::swarm::IntoProtocolsHandler>::Handler as libp2p::swarm::ProtocolsHandler>::OutEvent,
    ) {
        todo!()
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<
        libp2p::swarm::NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>,
    > {
        todo!()
    }
}

pub(crate) struct Handler {}

impl ProtocolsHandler for Handler {
    type InEvent = Message;

    type OutEvent = Message;

    type Error = ConnectionError;

    type InboundProtocol = LocutusProtocol;

    type OutboundProtocol = LocutusProtocol;

    type InboundOpenInfo = ();

    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(LocutusProtocol {}, ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as InboundUpgradeSend>::Output,
        info: Self::InboundOpenInfo,
    ) {
        todo!()
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output,
        info: Self::OutboundOpenInfo,
    ) {
        todo!()
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        todo!()
    }

    fn inject_dial_upgrade_error(
        &mut self,
        info: Self::OutboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
    ) {
        todo!()
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        todo!()
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        todo!()
    }
}

pub(crate) struct LocutusProtocol {}

impl UpgradeInfo for LocutusProtocol {
    type Info = &'static [u8];
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(CURRENT_PROTOC_VER)
    }
}

impl InboundUpgrade<NegotiatedSubstream> for LocutusProtocol {
    type Output = NegotiatedSubstream;
    type Error = ConnectionError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, stream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        // TODO: check protocol ver
        future::ok(stream)
    }
}

impl OutboundUpgrade<NegotiatedSubstream> for LocutusProtocol {
    type Output = NegotiatedSubstream;
    type Error = ConnectionError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        // TODO: check protocol ver
        future::ok(stream)
    }
}

/// The network behaviour implements the following capabilities:
///
/// - [Identify](https://github.com/libp2p/specs/tree/master/identify) libp2p protocol.
/// - Pinging between peers.
#[derive(libp2p::NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "NetEvent")]
pub(crate) struct NetBehaviour {
    identify: identify::Identify,
    ping: ping::Ping,
    locutus: LocutusBehaviour,
}

#[derive(Debug)]
pub(crate) enum NetEvent {
    Locutus(Box<Message>),
    Identify(Box<identify::IdentifyEvent>),
    Ping(ping::PingEvent),
}

impl From<identify::IdentifyEvent> for NetEvent {
    fn from(event: identify::IdentifyEvent) -> NetEvent {
        Self::Identify(Box::new(event))
    }
}

impl From<ping::PingEvent> for NetEvent {
    fn from(event: ping::PingEvent) -> NetEvent {
        Self::Ping(event)
    }
}

impl From<Message> for NetEvent {
    fn from(event: Message) -> NetEvent {
        Self::Locutus(Box::new(event))
    }
}
