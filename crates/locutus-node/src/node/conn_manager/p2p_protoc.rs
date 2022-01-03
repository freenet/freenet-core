use std::{
    collections::VecDeque,
    io,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use asynchronous_codec::{BytesMut, Framed};
use dashmap::{DashMap, DashSet};
use either::{Left, Right};
use futures::{
    future::{self},
    sink, stream, AsyncRead, AsyncWrite, FutureExt, Sink, SinkExt, Stream, StreamExt, TryStreamExt,
};
use libp2p::{
    core::{muxing, transport, UpgradeInfo},
    identify,
    identity::Keypair,
    multiaddr::Protocol,
    ping,
    swarm::{
        protocols_handler::OutboundUpgradeSend, AddressScore, KeepAlive, NegotiatedSubstream,
        NetworkBehaviour, NetworkBehaviourAction, ProtocolsHandler, ProtocolsHandlerEvent,
        ProtocolsHandlerUpgrErr, SubstreamProtocol, SwarmBuilder, SwarmEvent,
    },
    InboundUpgrade, Multiaddr, OutboundUpgrade, PeerId, Swarm,
};
use tokio::sync::mpsc::{Receiver, Sender};
use unsigned_varint::codec::UviBytes;

use crate::{
    config::{self, GlobalExecutor},
    message::{Message, NodeActions, Transaction},
    node::{process_message, OpManager, PeerKey},
    NodeConfig,
};

use super::{ConnectionBridge, ConnectionError};

/// The default maximum size for a varint length-delimited packet.
pub const DEFAULT_MAX_PACKET_SIZE: usize = 16 * 1024;

const CURRENT_AGENT_VER: &str = "/locutus/agent/0.1.0";
const CURRENT_PROTOC_VER: &[u8] = b"/locutus/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "/id/1.0.0";

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

pub(in crate::node) struct P2pConnManager {
    pub(in crate::node) swarm: Swarm<NetBehaviour>,
    listen_on: Option<(IpAddr, u16)>,
}

impl P2pConnManager {
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

        P2pConnManager {
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
                    peer_id, endpoint, ..
                } => Ok(Right(ConnMngrActions::ConnectionEstablished {
                    peer_id: PeerKey(peer_id),
                    addr: endpoint.get_remote_address().clone(),
                })),
                SwarmEvent::ConnectionClosed { .. } => {
                    todo!("remove from the ring");
                }
                other_event => {
                    log::debug!("{:?}", other_event);
                    Ok(Right(ConnMngrActions::NoAction))
                }
            });

            let notification_msg = notification_channel.recv().map(|m| match m {
                Some(m) => Ok(Left(Box::new(m))),
                None => Ok(Right(ClosedChannel)),
            });

            let bridge_msg = rx.recv().map(|msg| {
                if let Some((peer, msg)) = msg {
                    Ok(Right(SendMessage { peer, msg }))
                } else {
                    Ok(Right(ClosedChannel))
                }
            });

            let msg: Result<_, ConnectionError> = tokio::select! {
                msg = net_msg => { msg }
                msg = notification_msg => { msg }
                msg = bridge_msg => { msg }
            };

            match msg {
                Ok(Left(msg)) => {
                    let cb = conn_manager.clone();
                    GlobalExecutor::spawn(process_message(Ok(*msg), op_manager.clone(), cb, None));
                }
                Ok(Right(ConnectionEstablished { addr, peer_id })) => {
                    log::debug!("established connection with peer {} @ {}", peer_id, addr);
                    conn_manager.active_net_connections.insert(peer_id, addr);
                }
                Ok(Right(ConnectionClosed { peer_id })) => {
                    conn_manager.active_net_connections.remove(&peer_id);
                    log::debug!("dropped connection with peer {}", peer_id);
                }
                Ok(Right(SendMessage { peer, msg })) => {
                    self.swarm
                        .behaviour_mut()
                        .locutus
                        .queue
                        .push_front((peer.0, *msg));
                    todo!()
                }
                Ok(Right(ClosedChannel)) => {
                    log::info!("notification channel closed");
                    break;
                }
                Err(err) => {
                    let cb = conn_manager.clone();
                    GlobalExecutor::spawn(process_message(Err(err), op_manager.clone(), cb, None));
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
    /// Closed a connection with the peer
    ConnectionClosed {
        peer_id: PeerKey,
    },
    SendMessage {
        peer: PeerKey,
        msg: Box<Message>,
    },
    ClosedChannel,
    NoAction,
}

/// Manages network connections with different peers and event routing within the swarm.
pub(in crate::node) struct LocutusBehaviour {
    queue: VecDeque<(PeerId, Message)>,
}

impl NetworkBehaviour for LocutusBehaviour {
    type ProtocolsHandler = Handler;

    type OutEvent = Message;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        Handler::new()
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        _connection: libp2p::core::connection::ConnectionId,
        msg: Message,
    ) {
        self.queue.push_front((peer_id, msg));
    }

    fn poll(
        &mut self,
        _: &mut std::task::Context<'_>,
        _: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>> {
        if let Some((_peer, msg)) = self.queue.pop_back() {
            Poll::Ready(NetworkBehaviourAction::GenerateEvent(msg))
        } else {
            Poll::Pending
        }
    }
}

type UniqConnId = usize;

/// Handles the connection with a given peer.
pub(in crate::node) struct Handler {
    substreams: Vec<SubstreamState>,
    keep_alive: KeepAlive,
    uniq_connection_id: UniqConnId,
    protocol_status: ProtocolStatus,
}

enum ProtocolStatus {
    Unconfirmed,
    Confirmed,
    Reported,
    FailedUpgrade,
}

enum SubstreamState {
    /// We haven't started opening the outgoing substream yet.
    /// Contains the request we want to send.
    OutPendingOpen(Message),
    /// Waiting to send a message to the remote.
    PendingSend {
        substream: LocutusStream<NegotiatedSubstream>,
        init_msg: Box<Message>,
    },
    /// Waiting to flush the substream so that the data arrives to the remote.
    PendingFlush {
        substream: LocutusStream<NegotiatedSubstream>,
        id: Transaction,
    },
    /// Waiting for an answer back from the remote.
    WaitingMsg {
        substream: LocutusStream<NegotiatedSubstream>,
        id: Transaction,
    },
    /// An error happened on the substream and we should report the error to the user.
    ReportError {
        error: ConnectionError,
        msg: Box<Message>,
    },
    /// Waiting for a request from the remote
    InWaitingMsg {
        conn_id: UniqConnId,
        substream: LocutusStream<NegotiatedSubstream>,
    },
    /// Waiting to send an answer back to the remote.
    InPendingSend {
        conn_id: UniqConnId,
        substream: LocutusStream<NegotiatedSubstream>,
        msg: Box<Message>,
    },
    /// Waiting to send an answer back to the remote.
    InPendingFlush {
        conn_id: UniqConnId,
        substream: LocutusStream<NegotiatedSubstream>,
    },
}

impl Handler {
    const KEEP_ALIVE: Duration = Duration::from_secs(30);

    fn new() -> Self {
        Self {
            substreams: vec![],
            keep_alive: KeepAlive::Until(Instant::now() + config::PEER_TIMEOUT),
            uniq_connection_id: 0,
            protocol_status: ProtocolStatus::Unconfirmed,
        }
    }
}

#[derive(Debug)]
pub(in crate::node) struct HandlerInputEvent {
    msg: Message,
    conn_id: Option<UniqConnId>,
    is_response: bool,
}

type HandlePollingEv = ProtocolsHandlerEvent<LocutusProtocol, Message, Message, ConnectionError>;

impl ProtocolsHandler for Handler {
    /// Event received from the network by the handler
    type InEvent = HandlerInputEvent;

    /// Event producer by the handler and processed by the swarm
    type OutEvent = Message;

    type Error = ConnectionError;

    type InboundProtocol = LocutusProtocol;

    type OutboundProtocol = LocutusProtocol;

    type InboundOpenInfo = ();

    /// Initial message due to be sent to the remote.
    type OutboundOpenInfo = Message;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(LocutusProtocol {}, ())
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        stream: <Self::OutboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        init_msg: Self::OutboundOpenInfo,
    ) {
        self.substreams.push(SubstreamState::PendingSend {
            substream: stream,
            init_msg: Box::new(init_msg),
        });
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        stream: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        _info: Self::InboundOpenInfo,
    ) {
        self.substreams.push(SubstreamState::InWaitingMsg {
            substream: stream,
            conn_id: self.uniq_connection_id,
        });
        self.uniq_connection_id += 1;
        if let ProtocolStatus::Unconfirmed = self.protocol_status {
            self.protocol_status = ProtocolStatus::Confirmed;
        }
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        if event.is_response {
            debug_assert!(event.conn_id.is_some());
            // is a reponse to a previous msg and a substream should exist,
            // the msg is available and the chain can be now identified by id
            let pos = self.substreams.iter().position(|state| match state {
                SubstreamState::InWaitingMsg { conn_id, .. } => {
                    Some(conn_id) == event.conn_id.as_ref()
                }
                _ => false,
            });

            if let Some(pos) = pos {
                let (conn_id, substream) = match self.substreams.remove(pos) {
                    SubstreamState::InWaitingMsg {
                        substream: stream,
                        conn_id,
                    } => (conn_id, stream),
                    _ => unreachable!(),
                };

                self.substreams.push(SubstreamState::InPendingSend {
                    msg: Box::new(event.msg),
                    conn_id,
                    substream,
                });
            }
        } else {
            // is a request, initiate a chain of messages, identificable by the tx id
            self.substreams
                .push(SubstreamState::OutPendingOpen(event.msg));
        }
    }

    fn inject_dial_upgrade_error(
        &mut self,
        msg: Self::OutboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
    ) {
        self.protocol_status = ProtocolStatus::FailedUpgrade;
        self.substreams.push(SubstreamState::ReportError {
            error: (Box::new(error)).into(),
            msg: Box::new(msg),
        });
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
            return Poll::Ready(ProtocolsHandlerEvent::Custom(Message::Internal(
                NodeActions::ConfirmedInbound,
            )));
        }

        for n in (0..self.substreams.len()).rev() {
            let mut stream = self.substreams.swap_remove(n);
            loop {
                match stream {
                    SubstreamState::OutPendingOpen(msg) => {
                        let event = ProtocolsHandlerEvent::OutboundSubstreamRequest {
                            protocol: SubstreamProtocol::new(LocutusProtocol {}, msg),
                        };
                        if self.substreams.is_empty() {
                            self.keep_alive =
                                KeepAlive::Until(Instant::now() + config::PEER_TIMEOUT);
                        }
                        return Poll::Ready(event);
                    }
                    SubstreamState::PendingSend {
                        mut substream,
                        init_msg,
                    } => match Sink::poll_ready(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(())) => {
                            let id = *init_msg.id();
                            match Sink::start_send(Pin::new(&mut substream), *init_msg) {
                                Ok(()) => {
                                    stream = SubstreamState::PendingFlush { substream, id };
                                }
                                Err(err) => {
                                    let event = ProtocolsHandlerEvent::Custom(Message::Internal(
                                        NodeActions::Error(err, id),
                                    ));
                                    return Poll::Ready(event);
                                }
                            }
                        }
                        Poll::Pending => {
                            stream = SubstreamState::PendingSend {
                                substream,
                                init_msg,
                            };
                            continue;
                        }
                        Poll::Ready(Err(err)) => {
                            let event = ProtocolsHandlerEvent::Custom(Message::Internal(
                                NodeActions::Error(err, *init_msg.id()),
                            ));
                            return Poll::Ready(event);
                        }
                    },
                    SubstreamState::PendingFlush { mut substream, id } => {
                        match Sink::poll_flush(Pin::new(&mut substream), cx) {
                            Poll::Ready(Ok(())) => {
                                stream = SubstreamState::WaitingMsg { substream, id };
                                continue;
                            }
                            Poll::Pending => {
                                self.substreams
                                    .push(SubstreamState::PendingFlush { substream, id });
                                break;
                            }
                            Poll::Ready(Err(err)) => {
                                let event = ProtocolsHandlerEvent::Custom(Message::Internal(
                                    NodeActions::Error(err, id),
                                ));
                                return Poll::Ready(event);
                            }
                        }
                    }
                    SubstreamState::WaitingMsg { mut substream, id } => {
                        match Stream::poll_next(Pin::new(&mut substream), cx) {
                            Poll::Ready(Some(Ok(msg))) => {
                                // FIXME: probably check this is not a final message and shut down the stream in case it is
                                self.substreams
                                    .push(SubstreamState::WaitingMsg { substream, id });
                                let event = ProtocolsHandlerEvent::Custom(msg);
                                return Poll::Ready(event);
                            }
                            Poll::Pending => {
                                self.substreams
                                    .push(SubstreamState::WaitingMsg { substream, id });
                                break;
                            }
                            Poll::Ready(Some(Err(err))) => {
                                let event = ProtocolsHandlerEvent::Custom(Message::Internal(
                                    NodeActions::Error(err, id),
                                ));
                                return Poll::Ready(event);
                            }
                            Poll::Ready(None) => {
                                let event = ProtocolsHandlerEvent::Custom(Message::Internal(
                                    NodeActions::Error(
                                        ConnectionError::IOError(Some(
                                            io::ErrorKind::UnexpectedEof.into(),
                                        )),
                                        id,
                                    ),
                                ));
                                return Poll::Ready(event);
                            }
                        }
                    }
                    SubstreamState::ReportError { error, msg } => {
                        let event = ProtocolsHandlerEvent::Custom(Message::Internal(
                            NodeActions::Error(error, *msg.id()),
                        ));
                        return Poll::Ready(event);
                    }
                    SubstreamState::InWaitingMsg {
                        conn_id,
                        mut substream,
                    } => match Stream::poll_next(Pin::new(&mut substream), cx) {
                        Poll::Ready(Some(Ok(msg))) => {
                            self.substreams.push(SubstreamState::WaitingMsg {
                                id: *msg.id(),
                                substream,
                            });
                            let event = ProtocolsHandlerEvent::Custom(msg);
                            return Poll::Ready(event);
                        }
                        Poll::Pending => {
                            self.substreams
                                .push(SubstreamState::InWaitingMsg { conn_id, substream });
                            break;
                        }
                        Poll::Ready(None) => {
                            log::trace!("Inbound substream: EOF");
                            break;
                        }
                        Poll::Ready(Some(Err(e))) => {
                            log::debug!("Inbound substream error: {}", e);
                            break;
                        }
                    },
                    SubstreamState::InPendingSend {
                        conn_id,
                        mut substream,
                        msg,
                    } => match Sink::poll_ready(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(_)) => {
                            match Sink::start_send(Pin::new(&mut substream), *msg) {
                                Ok(_) => {
                                    stream = SubstreamState::InPendingFlush { conn_id, substream };
                                    continue;
                                }
                                Err(err) => {
                                    log::debug!("{}", err);
                                    break;
                                }
                            }
                        }
                        Poll::Pending => {
                            self.substreams.push(SubstreamState::InPendingSend {
                                conn_id,
                                substream,
                                msg,
                            });
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(err)) => {
                            log::debug!("{}", err);
                            break;
                        }
                    },
                    SubstreamState::InPendingFlush {
                        conn_id,
                        mut substream,
                    } => match Sink::poll_flush(Pin::new(&mut substream), cx) {
                        Poll::Ready(Ok(_)) => {
                            stream = SubstreamState::InWaitingMsg { conn_id, substream };
                            continue;
                        }
                        Poll::Pending => {
                            self.substreams
                                .push(SubstreamState::InPendingFlush { conn_id, substream });
                            break;
                        }
                        Poll::Ready(Err(err)) => {
                            log::debug!("{}", err);
                            break;
                        }
                    },
                }
            }
        }

        if self.substreams.is_empty() {
            // We destroyed all substreams in this function.
            self.keep_alive = KeepAlive::Until(Instant::now() + config::PEER_TIMEOUT);
        } else {
            self.keep_alive = KeepAlive::Yes;
        }

        Poll::Pending
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
/// - Pinging between peers.
#[derive(libp2p::NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "NetEvent")]
pub(in crate::node) struct NetBehaviour {
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
