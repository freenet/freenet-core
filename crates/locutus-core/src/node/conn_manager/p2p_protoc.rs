use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Instant,
};

use asynchronous_codec::{BytesMut, Framed};
use dashmap::{DashMap, DashSet};
use either::{Either, Left, Right};
use futures::{
    future::{self},
    sink, stream, AsyncRead, AsyncWrite, FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt,
};
use libp2p::{
    autonat,
    core::{muxing, transport, UpgradeInfo},
    identify,
    identity::Keypair,
    multiaddr::Protocol,
    ping,
    swarm::{
        self,
        dial_opts::DialOpts,
        handler::{DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionHandlerEvent, ConnectionId, FromSwarm, KeepAlive,
        NetworkBehaviour, NotifyHandler, Stream as NegotiatedSubstream, SubstreamProtocol,
        SwarmBuilder, SwarmEvent, ToSwarm,
    },
    InboundUpgrade, Multiaddr, OutboundUpgrade, PeerId, Swarm,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use unsigned_varint::codec::UviBytes;

use super::{ConnectionBridge, ConnectionError};
use crate::{
    config::{self, GlobalExecutor},
    message::{Message, NodeEvent, TransactionType},
    node::{handle_cancelled_op, join_ring_request, process_message, OpManager, PeerKey},
    operations::OpError,
    ring::PeerKeyLocation,
    util::IterExt,
    InitPeerNode, NodeConfig,
};

/// The default maximum size for a varint length-delimited packet.
pub const DEFAULT_MAX_PACKET_SIZE: usize = 16 * 1024;

const CURRENT_AGENT_VER: &str = "/locutus/agent/0.1.0";
const CURRENT_PROTOC_VER: &str = "/locutus/0.1.0";
const CURRENT_PROTOC_VER_STR: &str = "/locutus/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "/id/1.0.0";

fn config_behaviour(
    local_key: &Keypair,
    gateways: &[InitPeerNode],
    _public_addr: &Option<Multiaddr>,
) -> NetBehaviour {
    let routing_table: HashMap<_, _> = gateways
        .iter()
        .filter_map(|p| {
            p.addr
                .as_ref()
                .map(|addr| (p.identifier, HashSet::from_iter([addr.clone()])))
        })
        .collect();

    let ident_config =
        identify::Config::new(CURRENT_IDENTIFY_PROTOC_VER.to_string(), local_key.public())
            .with_agent_version(CURRENT_AGENT_VER.to_string());

    let ping = ping::Behaviour::default();

    let peer_id = local_key.public().to_peer_id();
    let auto_nat = {
        let config = autonat::Config {
            ..Default::default()
        };
        let mut behaviour = autonat::Behaviour::new(peer_id, config);

        for (peer, addr) in gateways.iter().map(|p| (&p.identifier, &p.addr)) {
            behaviour.add_server(*peer, addr.clone());
        }
        behaviour
    };

    NetBehaviour {
        ping,
        identify: identify::Behaviour::new(ident_config),
        auto_nat,
        locutus: LocutusBehaviour {
            outbound: VecDeque::new(),
            routing_table,
            connected: HashMap::new(),
            openning_connection: HashSet::new(),
            inbound: VecDeque::new(),
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

type P2pBridgeEvent = Either<(PeerKey, Box<Message>), NodeEvent>;

#[derive(Clone)]
pub(in crate::node) struct P2pBridge {
    active_net_connections: Arc<DashMap<PeerKey, Multiaddr>>,
    accepted_peers: Arc<DashSet<PeerKey>>,
    ev_listener_tx: Sender<P2pBridgeEvent>,
}

impl P2pBridge {
    fn new(sender: Sender<P2pBridgeEvent>) -> Self {
        Self {
            active_net_connections: Arc::new(DashMap::new()),
            accepted_peers: Arc::new(DashSet::new()),
            ev_listener_tx: sender,
        }
    }
}

#[async_trait::async_trait]
impl ConnectionBridge for P2pBridge {
    async fn add_connection(&mut self, peer: PeerKey) -> super::ConnResult<()> {
        if self.active_net_connections.contains_key(&peer) {
            self.accepted_peers.insert(peer);
        }
        self.ev_listener_tx
            .send(Right(NodeEvent::AcceptConnection(peer)))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        Ok(())
    }

    async fn drop_connection(&mut self, peer: &PeerKey) -> super::ConnResult<()> {
        self.accepted_peers.remove(peer);
        self.ev_listener_tx
            .send(Right(NodeEvent::DropConnection(*peer)))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        Ok(())
    }

    async fn send(&self, target: &PeerKey, msg: Message) -> super::ConnResult<()> {
        self.ev_listener_tx
            .send(Left((*target, Box::new(msg))))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        Ok(())
    }
}

pub(in crate::node) struct P2pConnManager {
    pub(in crate::node) swarm: Swarm<NetBehaviour>,
    pub(in crate::node) gateways: Vec<PeerKeyLocation>,
    pub(in crate::node) bridge: P2pBridge,
    conn_bridge_rx: Receiver<P2pBridgeEvent>,
    /// last valid observed public address
    public_addr: Option<Multiaddr>,
}

impl P2pConnManager {
    pub fn build<const CLIENTS: usize>(
        transport: transport::Boxed<(PeerId, muxing::StreamMuxerBox)>,
        config: &NodeConfig<CLIENTS>,
    ) -> Result<Self, anyhow::Error> {
        // We set a global executor which is virtually the Tokio multi-threaded executor
        // to reuse it's thread pool and scheduler in order to drive futures.
        let global_executor = GlobalExecutor;

        let public_addr = if let Some(conn) = config.local_ip.zip(config.local_port) {
            let public_addr = multiaddr_from_connection(conn);
            Some(public_addr)
        } else {
            None
        };

        let builder = SwarmBuilder::with_executor(
            transport,
            config_behaviour(&config.local_key, &config.remote_nodes, &public_addr),
            PeerId::from(config.local_key.public()),
            global_executor,
        );

        let mut swarm = builder.build();
        for remote_addr in config.remote_nodes.iter().filter_map(|r| r.addr.clone()) {
            swarm.add_external_address(remote_addr);
        }

        let (tx_bridge_cmd, rx_bridge_cmd) = channel(100);
        let bridge = P2pBridge::new(tx_bridge_cmd);

        let gateways = config.get_gateways()?;
        Ok(P2pConnManager {
            swarm,
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            public_addr,
        })
    }

    pub fn listen_on(&mut self) -> Result<(), anyhow::Error> {
        if let Some(listening_addr) = &self.public_addr {
            self.swarm.listen_on(listening_addr.clone())?;
        }
        Ok(())
    }

    pub async fn run_event_listener<CErr>(
        mut self,
        op_manager: Arc<OpManager<CErr>>,
        mut notification_channel: Receiver<Either<Message, NodeEvent>>,
    ) -> Result<(), anyhow::Error>
    where
        CErr: std::error::Error + Send + Sync + 'static,
    {
        use ConnMngrActions::*;

        loop {
            let net_msg = self.swarm.select_next_some().map(|event| match event {
                SwarmEvent::Behaviour(NetEvent::Locutus(msg)) => {
                    tracing::debug!("Message inbound: {:?}", msg);
                    Ok(Left(*msg))
                }
                SwarmEvent::ConnectionClosed { peer_id, .. } => {
                    Ok(Right(ConnMngrActions::ConnectionClosed {
                        peer: PeerKey::from(peer_id),
                    }))
                }
                SwarmEvent::Dialing { peer_id, .. } => {
                    if let Some(peer_id) = peer_id {
                        tracing::debug!("Attempting connection to {}", peer_id);
                    }
                    Ok(Right(ConnMngrActions::NoAction))
                }
                SwarmEvent::Behaviour(NetEvent::Identify(id)) => {
                    if let identify::Event::Received { peer_id, info } = *id {
                        if Self::is_compatible_peer(&info) {
                            Ok(Right(ConnMngrActions::ConnectionEstablished {
                                peer: PeerKey(peer_id),
                                address: info.observed_addr,
                            }))
                        } else {
                            tracing::warn!("Incompatible peer: {}, disconnecting", peer_id);
                            Ok(Right(ConnMngrActions::ConnectionClosed {
                                peer: PeerKey::from(peer_id),
                            }))
                        }
                    } else {
                        Ok(Right(ConnMngrActions::NoAction))
                    }
                }
                SwarmEvent::Behaviour(NetEvent::Autonat(event)) => match event {
                    autonat::Event::InboundProbe(autonat::InboundProbeEvent::Response {
                        address,
                        peer,
                        ..
                    }) => {
                        tracing::debug!(
                            "Successful autonat probe, established conn with {peer} @ {address}"
                        );
                        Ok(Right(ConnMngrActions::ConnectionEstablished {
                            peer: PeerKey(peer),
                            address,
                        }))
                    }
                    autonat::Event::InboundProbe(autonat::InboundProbeEvent::Error {
                        peer,
                        error: autonat::InboundProbeError::Response(err),
                        ..
                    }) => match err {
                        autonat::ResponseError::DialError | autonat::ResponseError::DialRefused => {
                            Ok(Right(ConnMngrActions::IsPrivatePeer(peer)))
                        }
                        _ => Ok(Right(ConnMngrActions::NoAction)),
                    },
                    autonat::Event::StatusChanged {
                        new: autonat::NatStatus::Public(address),
                        ..
                    } => {
                        tracing::debug!("NAT status: public @ {address}");
                        Ok(Right(ConnMngrActions::UpdatePublicAddr(address)))
                    }
                    _ => Ok(Right(ConnMngrActions::NoAction)),
                },
                other_event => {
                    tracing::debug!("Received other swarm event: {:?}", other_event);
                    Ok(Right(ConnMngrActions::NoAction))
                }
            });

            let notification_msg = notification_channel.recv().map(|m| match m {
                None => Ok(Right(ClosedChannel)),
                Some(Left(msg)) => Ok(Left(msg)),
                Some(Right(action)) => Ok(Right(NodeAction(action))),
            });

            let bridge_msg = self.conn_bridge_rx.recv().map(|msg| match msg {
                Some(Left((peer, msg))) => {
                    tracing::debug!("Message outbound: {:?}", msg);
                    Ok(Right(SendMessage { peer, msg }))
                }
                Some(Right(action)) => Ok(Right(NodeAction(action))),
                None => Ok(Right(ClosedChannel)),
            });

            let msg: Result<_, ConnectionError> = tokio::select! {
                msg = net_msg => { msg }
                msg = notification_msg => { msg }
                msg = bridge_msg => { msg }
            };

            match msg {
                Ok(Left(msg)) => {
                    let cb = self.bridge.clone();
                    match msg {
                        Message::Canceled(tx) => {
                            let tx_type = tx.tx_type();
                            let res = handle_cancelled_op(
                                tx,
                                op_manager.ring.peer_key,
                                self.gateways.iter(),
                                &op_manager,
                                &mut self.bridge,
                            )
                            .await;
                            match res {
                                Err(OpError::MaxRetriesExceeded(_, _))
                                    if tx_type == TransactionType::JoinRing
                                        && self.public_addr.is_none() /* FIXME: this should be not a gateway instead */ =>
                                {
                                    tracing::warn!("Retrying joining the ring with an other peer");
                                    let gateway = self.gateways.iter().shuffle().next().unwrap();
                                    join_ring_request(
                                        None,
                                        op_manager.ring.peer_key,
                                        gateway,
                                        &op_manager,
                                        &mut self.bridge,
                                    )
                                    .await?
                                }
                                Err(err) => return Err(anyhow::anyhow!(err)),
                                Ok(_) => {}
                            }
                            continue;
                        }
                        msg => {
                            GlobalExecutor::spawn(process_message(
                                Ok(msg),
                                op_manager.clone(),
                                cb,
                                None,
                            ));
                        }
                    }
                }
                Ok(Right(SendMessage { peer, msg })) => {
                    tracing::debug!(
                        "Sending swarm message from {} to {}",
                        op_manager.ring.peer_key,
                        peer
                    );
                    self.swarm
                        .behaviour_mut()
                        .locutus
                        .outbound
                        .push_front((peer.0, Left(*msg)));
                }
                Ok(Right(NodeAction(NodeEvent::ShutdownNode))) => {
                    tracing::info!("Shutting down message loop gracefully");
                    break;
                }
                Ok(Right(NodeAction(NodeEvent::Error(err)))) => {
                    tracing::error!("Bridge conn error: {err}");
                }
                Ok(Right(NodeAction(NodeEvent::AcceptConnection(_key)))) => {
                    // todo: if we prefilter connections, should only accept ones informed this way
                    //       (except 'join ring' requests)
                }
                Ok(Right(ConnectionEstablished {
                    address: addr,
                    peer,
                })) => {
                    tracing::debug!("Established connection with peer {} @ {}", peer, addr);
                    self.bridge.active_net_connections.insert(peer, addr);
                }
                Ok(Right(ConnectionClosed { peer: peer_id }))
                | Ok(Right(NodeAction(NodeEvent::DropConnection(peer_id)))) => {
                    self.bridge.active_net_connections.remove(&peer_id);
                    op_manager.prune_connection(peer_id);
                    // todo: notify the handler, read `disconnect_peer_id` doc
                    let _ = self.swarm.disconnect_peer_id(peer_id.0);
                    tracing::debug!("Dropped connection with peer {}", peer_id);
                }
                Ok(Right(UpdatePublicAddr(address))) => {
                    self.public_addr = Some(address);
                }
                Ok(Right(IsPrivatePeer(_peer))) => {
                    todo!("attempt hole punching")
                }
                Ok(Right(ClosedChannel)) => {
                    tracing::info!("Notification channel closed");
                    break;
                }
                Err(err) => {
                    let cb = self.bridge.clone();
                    GlobalExecutor::spawn(process_message(Err(err), op_manager.clone(), cb, None));
                }
                Ok(Right(NoAction)) | Ok(Right(NodeAction(NodeEvent::ConfirmedInbound))) => {}
            }
        }
        Ok(())
    }

    fn is_compatible_peer(info: &identify::Info) -> bool {
        let compatible_agent = info.agent_version == CURRENT_AGENT_VER;
        let compatible_protoc = info
            .protocols
            .iter()
            .any(|s| s.as_ref() == CURRENT_PROTOC_VER_STR);
        compatible_agent && compatible_protoc
    }
}

enum ConnMngrActions {
    /// Received a new connection
    ConnectionEstablished {
        peer: PeerKey,
        address: Multiaddr,
    },
    /// Closed a connection with the peer
    ConnectionClosed {
        peer: PeerKey,
    },
    /// Outbound message
    SendMessage {
        peer: PeerKey,
        msg: Box<Message>,
    },
    /// Update self own public address, useful when communicating for first time
    UpdatePublicAddr(Multiaddr),
    /// A peer which we attempted connection to is private, attempt hole-punching
    IsPrivatePeer(PeerId),
    NodeAction(NodeEvent),
    ClosedChannel,
    NoAction,
}

/// Manages network connections with different peers and event routing within the swarm.
pub(in crate::node) struct LocutusBehaviour {
    // FIFO queue for outbound messages
    outbound: VecDeque<(PeerId, Either<Message, NodeEvent>)>,
    // FIFO queue for inbound messages
    inbound: VecDeque<Either<Message, NodeEvent>>,
    routing_table: HashMap<PeerId, HashSet<Multiaddr>>,
    connected: HashMap<PeerId, ConnectionId>,
    openning_connection: HashSet<PeerId>,
}

impl NetworkBehaviour for LocutusBehaviour {
    type ConnectionHandler = Handler;

    type ToSwarm = Message;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        _local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        self.openning_connection.remove(&peer_id);
        self.openning_connection.shrink_to_fit();
        self.connected.insert(peer_id, connection_id);
        self.routing_table
            .entry(peer_id)
            .or_default()
            .insert(remote_addr.clone());
        Ok(Handler::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer_id: PeerId,
        addr: &Multiaddr,
        _role_override: libp2p::core::Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        self.openning_connection.remove(&peer_id);
        self.openning_connection.shrink_to_fit();
        self.connected.insert(peer_id, connection_id);
        self.routing_table
            .entry(peer_id)
            .or_default()
            .insert(addr.clone());
        Ok(Handler::new())
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        _connection: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::ToBehaviour,
    ) {
        match event {
            HandlerEvent::Outbound(msg) => {
                self.outbound.push_front((peer_id, msg));
            }
            HandlerEvent::Inbound(msg) => {
                self.inbound.push_front(msg);
            }
        }
    }

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        if let FromSwarm::ConnectionClosed(swarm::ConnectionClosed { peer_id, .. }) = event {
            self.connected.remove(&peer_id);
        }
    }

    fn poll(
        &mut self,
        _: &mut std::task::Context<'_>,
        _: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<
        ToSwarm<Self::ToSwarm, <Self::ConnectionHandler as ConnectionHandler>::FromBehaviour>,
    > {
        if let Some(Left(msg)) = self.inbound.pop_back() {
            let send_to_ev_listener = ToSwarm::GenerateEvent(msg);
            return Poll::Ready(send_to_ev_listener);
        }

        if let Some((peer_id, msg)) = self.outbound.pop_back() {
            if let Right(NodeEvent::Error(err)) = msg {
                tracing::warn!("Connection error: {}", err);
                return Poll::Pending;
            }

            if let Some(id) = self.connected.get(&peer_id) {
                let send_to_handler = ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::One(*id),
                    event: HandlerEvent::Outbound(msg),
                };
                Poll::Ready(send_to_handler)
            } else if self.openning_connection.contains(&peer_id) {
                // waiting to have an open connection
                self.outbound.push_front((peer_id, msg));
                Poll::Pending
            } else if let Some(conn) = self.routing_table.get(&peer_id) {
                // initiate a connection if one does not exist
                // FIXME: we dial as listener to perform NAT hole-punching though the `override_role` method,
                //        if this is required because the other peer
                let peer_opts = DialOpts::peer_id(peer_id)
                    .addresses(conn.iter().cloned().collect())
                    .extend_addresses_through_behaviour();
                let initiate_conn = ToSwarm::Dial {
                    opts: peer_opts.build(),
                };
                self.outbound.push_front((peer_id, msg));
                self.openning_connection.insert(peer_id);
                Poll::Ready(initiate_conn)
            } else {
                Poll::Pending
            }
        } else {
            Poll::Pending
        }
    }
}

type UniqConnId = usize;

#[derive(Debug)]
pub(in crate::node) enum HandlerEvent {
    Inbound(Either<Message, NodeEvent>),
    Outbound(Either<Message, NodeEvent>),
}

/// Handles the connection with a given peer.
pub(in crate::node) struct Handler {
    substreams: Vec<SubstreamState>,
    keep_alive: KeepAlive,
    uniq_conn_id: UniqConnId,
    protocol_status: ProtocolStatus,
    pending: Vec<Message>,
}

#[allow(dead_code)]
enum ProtocolStatus {
    Unconfirmed,
    Confirmed,
    Reported,
    Failed,
}

#[allow(dead_code)]
enum SubstreamState {
    /// We haven't started opening the outgoing substream yet.
    /// Contains the initial request we want to send.
    OutPendingOpen {
        msg: Box<Message>,
        conn_id: UniqConnId,
    },
    /// Waiting for the first message after requesting an outbound open connection.
    AwaitingFirst { conn_id: UniqConnId },
    FreeStream {
        conn_id: UniqConnId,
        substream: LocutusStream<NegotiatedSubstream>,
    },
    /// Waiting to send a message to the remote.
    PendingSend {
        conn_id: UniqConnId,
        substream: LocutusStream<NegotiatedSubstream>,
        msg: Box<Either<Message, NodeEvent>>,
    },
    /// Waiting to flush the substream so that the data arrives to the remote.
    PendingFlush {
        conn_id: UniqConnId,
        substream: LocutusStream<NegotiatedSubstream>,
    },
    /// Waiting for an answer back from the remote.
    WaitingMsg {
        conn_id: UniqConnId,
        substream: LocutusStream<NegotiatedSubstream>,
    },
    /// An error happened on the substream and we should report the error to the user.
    ReportError { error: ConnectionError },
}

impl SubstreamState {
    fn is_free(&self) -> bool {
        matches!(self, SubstreamState::FreeStream { .. })
    }
}

impl Handler {
    fn new() -> Self {
        Self {
            substreams: vec![],
            keep_alive: KeepAlive::Until(Instant::now() + config::PEER_TIMEOUT),
            uniq_conn_id: 0,
            protocol_status: ProtocolStatus::Unconfirmed,
            pending: Vec::new(),
        }
    }

    #[inline]
    fn send_to_free_substream(&mut self, msg: Message) -> Option<Message> {
        let pos = self
            .substreams
            .iter()
            .position(|state| matches!(state, SubstreamState::FreeStream { .. }));

        if let Some(pos) = pos {
            let (conn_id, substream) = match self.substreams.swap_remove(pos) {
                SubstreamState::FreeStream {
                    substream: stream,
                    conn_id,
                } => (conn_id, stream),
                _ => unreachable!(),
            };

            self.substreams.push(SubstreamState::PendingSend {
                msg: Box::new(Left(msg)),
                conn_id,
                substream,
            });
            None
        } else {
            Some(msg)
        }
    }
}

type HandlePollingEv = ConnectionHandlerEvent<LocutusProtocol, (), HandlerEvent, ConnectionError>;

impl ConnectionHandler for Handler {
    /// Event received from the network by the handler
    type FromBehaviour = HandlerEvent;

    /// Event producer by the handler and processed by the swarm
    type ToBehaviour = HandlerEvent;

    type Error = ConnectionError;

    type InboundProtocol = LocutusProtocol;

    type OutboundProtocol = LocutusProtocol;

    type InboundOpenInfo = ();

    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(LocutusProtocol, ())
    }

    fn on_behaviour_event(&mut self, msg: Self::FromBehaviour) {
        match msg {
            HandlerEvent::Outbound(Left(msg)) => {
                if let Some(msg) = self.send_to_free_substream(msg) {
                    let conn_id = self.uniq_conn_id;
                    self.uniq_conn_id += 1;
                    // is the first request initiated and/or there are no free substreams, open a new one
                    self.substreams.push(SubstreamState::OutPendingOpen {
                        msg: Box::new(msg),
                        conn_id,
                    });
                }
            }
            HandlerEvent::Outbound(Right(node_ev)) => {
                tracing::debug!("Received node event at connection handler: {node_ev}");
            }
            HandlerEvent::Inbound(_) => unreachable!(),
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<HandlePollingEv> {
        if self.substreams.is_empty() {
            return Poll::Pending;
        }

        if let ProtocolStatus::Confirmed = self.protocol_status {
            self.protocol_status = ProtocolStatus::Reported;
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::Outbound(Right(NodeEvent::ConfirmedInbound)),
            ));
        }

        for n in (0..self.substreams.len()).rev() {
            let mut stream = self.substreams.swap_remove(n);
            loop {
                match stream {
                    SubstreamState::OutPendingOpen { msg, conn_id } => {
                        let event = ConnectionHandlerEvent::OutboundSubstreamRequest {
                            protocol: SubstreamProtocol::new(LocutusProtocol, ()),
                        };
                        self.substreams
                            .push(SubstreamState::AwaitingFirst { conn_id });
                        self.pending.push(*msg);
                        if self.substreams.is_empty() {
                            self.keep_alive =
                                KeepAlive::Until(Instant::now() + config::PEER_TIMEOUT);
                        }
                        return Poll::Ready(event);
                    }
                    SubstreamState::AwaitingFirst { conn_id } => {
                        self.substreams
                            .push(SubstreamState::AwaitingFirst { conn_id });
                        break;
                    }
                    SubstreamState::FreeStream { substream, conn_id } => {
                        if let Some(msg) = self.pending.pop() {
                            stream = SubstreamState::PendingSend {
                                substream,
                                conn_id,
                                msg: Box::new(Left(msg)),
                            };
                            continue;
                        } else {
                            self.substreams
                                .push(SubstreamState::WaitingMsg { substream, conn_id });
                            break;
                        }
                    }
                    SubstreamState::PendingSend {
                        mut substream,
                        msg,
                        conn_id,
                    } => match Sink::poll_ready(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => match *msg {
                            Right(action) => match action {
                                NodeEvent::ConfirmedInbound => {
                                    stream = SubstreamState::FreeStream { substream, conn_id };
                                    continue;
                                }
                                _ => break,
                            },
                            Left(msg) => match Sink::start_send(Pin::new(&mut substream), msg) {
                                Ok(()) => {
                                    stream = SubstreamState::PendingFlush { substream, conn_id };
                                }
                                Err(err) => {
                                    let event = ConnectionHandlerEvent::NotifyBehaviour(
                                        HandlerEvent::Inbound(Right(NodeEvent::Error(err))),
                                    );
                                    return Poll::Ready(event);
                                }
                            },
                        },
                        Poll::Pending => {
                            stream = SubstreamState::PendingSend {
                                substream,
                                msg,
                                conn_id,
                            };
                            continue;
                        }
                        Poll::Ready(Err(err)) => {
                            let event = ConnectionHandlerEvent::NotifyBehaviour(
                                HandlerEvent::Inbound(Right(NodeEvent::Error(err))),
                            );
                            return Poll::Ready(event);
                        }
                    },
                    SubstreamState::PendingFlush {
                        mut substream,
                        conn_id,
                    } => match Sink::poll_flush(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            stream = SubstreamState::WaitingMsg { substream, conn_id };
                            continue;
                        }
                        Poll::Pending => {
                            self.substreams
                                .push(SubstreamState::PendingFlush { substream, conn_id });
                            break;
                        }
                        Poll::Ready(Err(err)) => {
                            let event = ConnectionHandlerEvent::NotifyBehaviour(
                                HandlerEvent::Inbound(Right(NodeEvent::Error(err))),
                            );
                            return Poll::Ready(event);
                        }
                    },
                    SubstreamState::WaitingMsg {
                        mut substream,
                        conn_id,
                    } => match Stream::poll_next(Pin::new(&mut substream), cx) {
                        Poll::Ready(Some(Ok(msg))) => {
                            if !msg.terminal() {
                                // received a message, the other peer is waiting for an answer
                                self.substreams
                                    .push(SubstreamState::FreeStream { substream, conn_id });
                            }
                            let event = ConnectionHandlerEvent::NotifyBehaviour(
                                HandlerEvent::Inbound(Left(msg)),
                            );
                            return Poll::Ready(event);
                        }
                        Poll::Pending => {
                            self.substreams
                                .push(SubstreamState::WaitingMsg { substream, conn_id });
                            break;
                        }
                        Poll::Ready(Some(Err(err))) => {
                            let event = ConnectionHandlerEvent::NotifyBehaviour(
                                HandlerEvent::Inbound(Right(NodeEvent::Error(err))),
                            );
                            return Poll::Ready(event);
                        }
                        Poll::Ready(None) => {
                            let event = ConnectionHandlerEvent::NotifyBehaviour(
                                HandlerEvent::Inbound(Right(NodeEvent::Error(
                                    std::io::Error::from(io::ErrorKind::UnexpectedEof).into(),
                                ))),
                            );
                            return Poll::Ready(event);
                        }
                    },
                    SubstreamState::ReportError { error, .. } => {
                        let event = ConnectionHandlerEvent::NotifyBehaviour(HandlerEvent::Inbound(
                            Right(NodeEvent::Error(error)),
                        ));
                        return Poll::Ready(event);
                    }
                }
            }
        }

        if self.substreams.is_empty() || self.substreams.iter().all(|s| s.is_free()) {
            // We destroyed all substreams in this iteration or all substreams are free
            self.keep_alive = KeepAlive::Until(Instant::now() + config::PEER_TIMEOUT);
        } else {
            self.keep_alive = KeepAlive::Yes;
        }

        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: libp2p::swarm::handler::ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            swarm::handler::ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: stream,
                ..
            }) => {
                if let Some(prev_stream) = self
                    .substreams
                    .iter()
                    .position(|state| matches!(state, SubstreamState::AwaitingFirst { .. }))
                {
                    match self.substreams.swap_remove(prev_stream) {
                        SubstreamState::AwaitingFirst { conn_id } => {
                            self.substreams.push(SubstreamState::FreeStream {
                                conn_id,
                                substream: stream,
                            });
                        }
                        _ => unreachable!(),
                    }
                } else {
                    self.substreams.push(SubstreamState::WaitingMsg {
                        conn_id: self.uniq_conn_id,
                        substream: stream,
                    });
                    self.uniq_conn_id += 1;
                }

                if let ProtocolStatus::Unconfirmed = self.protocol_status {
                    self.protocol_status = ProtocolStatus::Confirmed;
                }
            }
            swarm::handler::ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                ..
            }) => {
                if let Some(pos) = self
                    .substreams
                    .iter()
                    .position(|state| matches!(state, SubstreamState::AwaitingFirst { .. }))
                {
                    let conn_id = match self.substreams.swap_remove(pos) {
                        SubstreamState::AwaitingFirst { conn_id } => conn_id,
                        _ => unreachable!(),
                    };
                    self.substreams.push(SubstreamState::FreeStream {
                        conn_id,
                        substream: stream,
                    });
                } else {
                    unreachable!();
                }
            }
            swarm::handler::ConnectionEvent::AddressChange(_) => {}
            swarm::handler::ConnectionEvent::DialUpgradeError(DialUpgradeError {
                error, ..
            }) => {
                self.protocol_status = ProtocolStatus::Failed;
                self.substreams.push(SubstreamState::ReportError {
                    error: error.into(),
                });
                self.uniq_conn_id += 1;
            }
            swarm::handler::ConnectionEvent::ListenUpgradeError(
                swarm::handler::ListenUpgradeError { error, .. },
            ) => {
                self.protocol_status = ProtocolStatus::Failed;
                self.substreams.push(SubstreamState::ReportError { error });
                self.uniq_conn_id += 1;
            }
            swarm::handler::ConnectionEvent::LocalProtocolsChange(_) => {}
            swarm::handler::ConnectionEvent::RemoteProtocolsChange(_) => {}
        }
    }
}

pub(crate) struct LocutusProtocol;

impl UpgradeInfo for LocutusProtocol {
    type Info = &'static str;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(CURRENT_PROTOC_VER)
    }
}

pub(crate) type LocutusStream<S> = stream::AndThen<
    sink::With<
        stream::ErrInto<Framed<S, UviBytes<io::Cursor<Vec<u8>>>>, ConnectionError>,
        io::Cursor<Vec<u8>>,
        Message,
        future::Ready<Result<io::Cursor<Vec<u8>>, ConnectionError>>,
        fn(Message) -> future::Ready<Result<io::Cursor<Vec<u8>>, ConnectionError>>,
    >,
    future::Ready<Result<Message, ConnectionError>>,
    fn(BytesMut) -> future::Ready<Result<Message, ConnectionError>>,
>;

impl<S> InboundUpgrade<S> for LocutusProtocol
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Output = LocutusStream<S>;
    type Error = ConnectionError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, incoming: S, _: Self::Info) -> Self::Future {
        frame_stream(incoming)
    }
}

impl<S> OutboundUpgrade<S> for LocutusProtocol
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Output = LocutusStream<S>;
    type Error = ConnectionError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, incoming: S, _: Self::Info) -> Self::Future {
        frame_stream(incoming)
    }
}

fn frame_stream<S>(incoming: S) -> future::Ready<Result<LocutusStream<S>, ConnectionError>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut codec = UviBytes::default();
    codec.set_max_len(DEFAULT_MAX_PACKET_SIZE);
    let framed = Framed::new(incoming, codec)
        .err_into()
        .with::<_, _, fn(_) -> _, _>(|response| match encode_msg(response) {
            Ok(msg) => future::ready(Ok(io::Cursor::new(msg))),
            Err(err) => future::ready(Err(err)),
        })
        .and_then::<_, fn(_) -> _>(|bytes| future::ready(decode_msg(bytes)));
    future::ok(framed)
}

#[inline(always)]
fn encode_msg(msg: Message) -> Result<Vec<u8>, ConnectionError> {
    bincode::serialize(&msg).map_err(|err| ConnectionError::Serialization(Some(err)))
}

#[inline(always)]
fn decode_msg(buf: BytesMut) -> Result<Message, ConnectionError> {
    let cursor = std::io::Cursor::new(buf);
    bincode::deserialize_from(cursor).map_err(|err| ConnectionError::Serialization(Some(err)))
}

/// The network behaviour implements the following capabilities:
///
/// - [Identify](https://github.com/libp2p/specs/tree/master/identify) libp2p protocol.
/// - [Ping](https://docs.rs/libp2p/latest/libp2p/ping/index.html) `/ipfs/ping/1.0.0` protocol.
/// - Locutus ring protocol, which handles the messages.
/// - [AutoNAT](https://github.com/libp2p/specs/tree/master/autonat) libp2p protocol.
#[derive(libp2p::swarm::NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(to_swarm = "NetEvent")]
pub(in crate::node) struct NetBehaviour {
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    locutus: LocutusBehaviour,
    auto_nat: autonat::Behaviour,
}

#[derive(Debug)]
pub(in crate::node) enum NetEvent {
    Locutus(Box<Message>),
    Identify(Box<identify::Event>),
    Ping(ping::Event),
    Autonat(autonat::Event),
}

impl From<autonat::Event> for NetEvent {
    fn from(event: autonat::Event) -> NetEvent {
        Self::Autonat(event)
    }
}

impl From<identify::Event> for NetEvent {
    fn from(event: identify::Event) -> NetEvent {
        Self::Identify(Box::new(event))
    }
}

impl From<ping::Event> for NetEvent {
    fn from(event: ping::Event) -> NetEvent {
        Self::Ping(event)
    }
}

impl From<Message> for NetEvent {
    fn from(event: Message) -> NetEvent {
        Self::Locutus(Box::new(event))
    }
}
