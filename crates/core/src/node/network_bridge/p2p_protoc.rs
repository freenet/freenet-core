use std::net::SocketAddr;
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
};

use asynchronous_codec::BytesMut;
use dashmap::{DashMap, DashSet};
use either::{Either, Left, Right};
use futures::{FutureExt, Sink, SinkExt, StreamExt, TryStreamExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::node::PeerId;
use crate::transport::connection_handler::ConnectionHandler;
use crate::transport::{crypto::TransportKeypair, peer_connection::PeerConnection, BytesPerSecond};
use crate::{
    client_events::ClientId,
    config::GlobalExecutor,
    contract::{
        ClientResponsesSender, ContractHandlerChannel, ExecutorToEventLoopChannel,
        NetworkEventListenerHalve, WaitingResolution,
    },
    message::{NetMessage, NodeEvent, Transaction},
    node::{
        handle_aborted_op, process_message, InitPeerNode, NetEventRegister, NodeConfig, OpManager,
        PeerId as FreenetPeerId,
    },
    ring::PeerKeyLocation,
    tracing::NetEventLog,
};

/// The default maximum size for a varint length-delimited packet.
pub const DEFAULT_MAX_PACKET_SIZE: usize = 16 * 1024;

const CURRENT_AGENT_VER: &str = "/freenet/agent/0.1.0";
const CURRENT_PROTOC_VER: &str = "/freenet/0.1.0";
const CURRENT_PROTOC_VER_STR: &str = "/freenet/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "/id/1.0.0";

type P2pBridgeEvent = Either<(FreenetPeerId, Box<NetMessage>), NodeEvent>;

#[derive(Clone)]
pub(crate) struct P2pBridge {
    active_net_connections: Arc<DashMap<FreenetPeerId, SocketAddr>>,
    accepted_peers: Arc<DashSet<FreenetPeerId>>,
    ev_listener_tx: Sender<P2pBridgeEvent>,
    op_manager: Arc<OpManager>,
    log_register: Arc<dyn NetEventRegister>,
}

impl P2pBridge {
    fn new<EL>(
        sender: Sender<P2pBridgeEvent>,
        op_manager: Arc<OpManager>,
        event_register: EL,
    ) -> Self
    where
        EL: NetEventRegister,
    {
        Self {
            active_net_connections: Arc::new(DashMap::new()),
            accepted_peers: Arc::new(DashSet::new()),
            ev_listener_tx: sender,
            op_manager,
            log_register: Arc::new(event_register),
        }
    }
}

#[async_trait::async_trait]
impl NetworkBridge for P2pBridge {
    async fn add_connection(&mut self, peer: FreenetPeerId) -> super::ConnResult<()> {
        if self.active_net_connections.contains_key(&peer) {
            self.accepted_peers.insert(peer);
        }
        self.ev_listener_tx
            .send(Right(NodeEvent::AcceptConnection(peer)))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        Ok(())
    }

    async fn drop_connection(&mut self, peer: &FreenetPeerId) -> super::ConnResult<()> {
        self.accepted_peers.remove(peer);
        self.ev_listener_tx
            .send(Right(NodeEvent::DropConnection(*peer)))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        self.log_register
            .register_events(Either::Left(NetEventLog::disconnected(
                &self.op_manager.ring,
                peer,
            )))
            .await;
        Ok(())
    }

    async fn send(&self, target: &FreenetPeerId, msg: NetMessage) -> super::ConnResult<()> {
        self.log_register
            .register_events(NetEventLog::from_outbound_msg(&msg, &self.op_manager.ring))
            .await;
        self.op_manager.sending_transaction(target, &msg);
        self.ev_listener_tx
            .send(Left((*target, Box::new(msg))))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted)?;
        Ok(())
    }
}

pub(in crate::node) struct P2pConnManager {
    conn_handler: ConnectionHandler,
    pub(in crate::node) gateways: Vec<PeerKeyLocation>,
    pub(in crate::node) bridge: P2pBridge,
    conn_bridge_rx: Receiver<P2pBridgeEvent>,
    /// last valid observed public address
    public_addr: Option<SocketAddr>,
    listening_addr: Option<SocketAddr>,
    event_listener: Box<dyn NetEventRegister>,
    connection: HashMap<PeerId, PeerConnection>,
}

impl P2pConnManager {
    pub fn build(
        config: &NodeConfig,
        op_manager: Arc<OpManager>,
        event_listener: impl NetEventRegister + Clone,
        private_key: TransportKeypair,
    ) -> Result<Self, anyhow::Error> {
        // We set a global executor which is virtually the Tokio multi-threaded executor
        // to reuse it's thread pool and scheduler in order to drive futures.
        let global_executor = GlobalExecutor;

        let private_addr = if let Some(conn) = config.local_ip.zip(config.local_port) {
            let public_addr = SocketAddr::from(conn);
            Some(public_addr)
        } else {
            None
        };

        let public_addr = if let Some(conn) = config.public_ip.zip(config.public_port) {
            let public_addr = SocketAddr::from(conn);
            Some(public_addr)
        } else {
            None
        };

        let listen_port = public_addr
            .ok_or_else(|| anyhow::anyhow!("private_addr does not contain a port"))?
            .port();

        let conn_handler = ConnectionHandler::new(
            private_key,
            listen_port,
            config.is_gateway(),
            BytesPerSecond::new(0.1),
        );

        let (tx_bridge_cmd, rx_bridge_cmd) = mpsc::channel(100);
        let bridge = P2pBridge::new(tx_bridge_cmd, op_manager, event_listener.clone());

        let gateways = config.get_gateways()?;
        Ok(P2pConnManager {
            conn_handler,
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            public_addr,
            listening_addr: private_addr,
            event_listener: Box::new(event_listener),
            connection: HashMap::new(),
        })
    }

    pub fn listen_on(&mut self) -> Result<(), anyhow::Error> {
        // if let Some(listening_addr) = &self.listening_addr {
        //     self.swarm.listen_on(listening_addr.clone())?;
        // }
        Ok(())
    }

    #[tracing::instrument(name = "network_event_listener", fields(peer = %self.bridge.op_manager.ring.peer_key), skip_all)]
    pub async fn run_event_listener(
        mut self,
        op_manager: Arc<OpManager>,
        mut client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
        mut notification_channel: EventLoopNotificationsReceiver,
        mut executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
        cli_response_sender: ClientResponsesSender,
        mut node_controller: Receiver<NodeEvent>,
    ) -> Result<(), anyhow::Error> {
        use ConnMngrActions::*;

        // FIXME: this two containers need to be clean up on transaction time-out
        let mut pending_from_executor = HashSet::new();
        let mut tx_to_client: HashMap<Transaction, ClientId> = HashMap::new();

        let this_peer = FreenetPeerId::from(
            crate::config::Config::conf()
                .local_peer_keypair
                .public()
                .to_peer_id(),
        );

        loop {
            // let network_msg = self.swarm.select_next_some().map(|event| match event {
            //     SwarmEvent::Behaviour(NetEvent::Freenet(msg)) => {
            //         tracing::debug!("Message inbound: {:?}", msg);
            //         Ok(Left(*msg))
            //     }
            //     SwarmEvent::ConnectionClosed { peer_id, .. } => {
            //         Ok(Right(ConnMngrActions::ConnectionClosed {
            //             peer: FreenetPeerId::from(peer_id),
            //         }))
            //     }
            //     SwarmEvent::Dialing { peer_id, .. } => {
            //         if let Some(peer_id) = peer_id {
            //             tracing::debug!("Attempting connection to {}", peer_id);
            //         }
            //         Ok(Right(ConnMngrActions::NoAction))
            //     }
            //     SwarmEvent::Behaviour(NetEvent::Identify(id)) => {
            //         if let identify::Event::Received { peer_id, info } = *id {
            //             if Self::is_compatible_peer(&info) {
            //                 Ok(Right(ConnMngrActions::ConnectionEstablished {
            //                     peer: FreenetPeerId::from(peer_id),
            //                     address: info.observed_addr,
            //                 }))
            //             } else {
            //                 tracing::warn!("Incompatible peer: {}, disconnecting", peer_id);
            //                 Ok(Right(ConnMngrActions::ConnectionClosed {
            //                     peer: FreenetPeerId::from(peer_id),
            //                 }))
            //             }
            //         } else {
            //             Ok(Right(ConnMngrActions::NoAction))
            //         }
            //     }
            //     SwarmEvent::Behaviour(NetEvent::Autonat(event)) => match event {
            //         autonat::Event::InboundProbe(autonat::InboundProbeEvent::Response {
            //             address,
            //             peer,
            //             ..
            //         }) => {
            //             tracing::debug!(
            //                 "Successful autonat probe, established conn with {peer} @ {address}"
            //             );
            //             Ok(Right(ConnMngrActions::ConnectionEstablished {
            //                 peer: FreenetPeerId::from(peer),
            //                 address,
            //             }))
            //         }
            //         autonat::Event::InboundProbe(autonat::InboundProbeEvent::Error {
            //             peer,
            //             error: autonat::InboundProbeError::Response(err),
            //             ..
            //         }) => match err {
            //             autonat::ResponseError::DialError | autonat::ResponseError::DialRefused => {
            //                 Ok(Right(ConnMngrActions::IsPrivatePeer(peer)))
            //             }
            //             _ => Ok(Right(ConnMngrActions::NoAction)),
            //         },
            //         autonat::Event::StatusChanged {
            //             new: autonat::NatStatus::Public(address),
            //             ..
            //         } => {
            //             tracing::debug!("NAT status: public @ {address}");
            //             Ok(Right(ConnMngrActions::UpdatePublicAddr(address)))
            //         }
            //         _ => Ok(Right(ConnMngrActions::NoAction)),
            //     },
            //     other_event => {
            //         tracing::debug!("Received other swarm event: {:?}", other_event);
            //         Ok(Right(ConnMngrActions::NoAction))
            //     }
            // });

            let notification_msg = notification_channel.0.recv().map(|m| match m {
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
                //msg = network_msg => { msg }
                msg = notification_msg => { msg }
                msg = bridge_msg => { msg }
                msg = node_controller.recv() => {
                    if let Some(msg) = msg {
                        Ok(Right(NodeAction(msg)))
                    } else {
                        Ok(Right(ClosedChannel))
                    }
                }
                event_id = client_wait_for_transaction.relay_transaction_result_to_client() => {
                    let (client_id, transaction) = event_id.map_err(|err| anyhow::anyhow!(err))?;
                    tx_to_client.insert(transaction, client_id);
                    continue;
                }
                id = executor_listener.transaction_from_executor() => {
                    let id = id.map_err(|err| anyhow::anyhow!(err))?;
                    pending_from_executor.insert(id);
                    continue;
                }
            };

            match msg {
                Ok(Left(msg)) => {
                    let cb = self.bridge.clone();
                    match msg {
                        NetMessage::Aborted(tx) => {
                            handle_aborted_op(
                                tx,
                                op_manager.ring.peer_key,
                                &op_manager,
                                &mut self.bridge,
                                &self.gateways,
                            )
                            .await?;
                            continue;
                        }
                        msg => {
                            let executor_callback = pending_from_executor
                                .remove(msg.id())
                                .then(|| executor_listener.callback());
                            let pending_client_req = tx_to_client.get(msg.id()).copied();
                            let client_req_handler_callback = if pending_client_req.is_some() {
                                Some(cli_response_sender.clone())
                            } else {
                                None
                            };
                            let parent_span = tracing::Span::current();
                            let span = tracing::info_span!(
                                parent: parent_span,
                                "process_network_message",
                                peer = %this_peer, transaction = %msg.id(),
                                tx_type = %msg.id().transaction_type()
                            );
                            GlobalExecutor::spawn(
                                process_message(
                                    msg,
                                    op_manager.clone(),
                                    cb,
                                    self.event_listener.trait_clone(),
                                    executor_callback,
                                    client_req_handler_callback,
                                    pending_client_req,
                                )
                                .instrument(span),
                            );
                        }
                    }
                }
                Ok(Right(SendMessage { peer, msg })) => {
                    tracing::debug!(
                        "Sending swarm message from {} to {}",
                        op_manager.ring.peer_key,
                        peer
                    );
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
                Ok(Right(NodeAction(NodeEvent::Disconnect { cause }))) => {
                    match cause {
                        Some(cause) => tracing::warn!("Shutting down node: {cause}"),
                        None => tracing::warn!("Shutting down node"),
                    }
                    return Ok(());
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
                    op_manager.ring.prune_connection(peer_id).await;
                    // todo: notify the handler, read `disconnect_peer_id` doc
                    tracing::info!("Dropped connection with peer {}", peer_id);
                }
                Ok(Right(UpdatePublicAddr(address))) => {
                    self.public_addr = Some(address);
                }
                Ok(Right(IsPrivatePeer(_peer))) => {
                    todo!("this peer is private, attempt hole punching")
                }
                Ok(Right(ClosedChannel)) => {
                    tracing::info!("Notification channel closed");
                    break;
                }
                Err(err) => {
                    super::super::report_result(
                        None,
                        Err(err.into()),
                        &op_manager,
                        None,
                        None,
                        &mut *self.event_listener as &mut _,
                    )
                    .await;
                }
                Ok(Right(NoAction)) | Ok(Right(NodeAction(NodeEvent::ConfirmedInbound))) => {}
            }
        }
        Ok(())
    }
}

enum ConnMngrActions {
    /// Received a new connection
    ConnectionEstablished {
        peer: FreenetPeerId,
        address: SocketAddr,
    },
    /// Closed a connection with the peer
    ConnectionClosed {
        peer: FreenetPeerId,
    },
    /// Outbound message
    SendMessage {
        peer: FreenetPeerId,
        msg: Box<NetMessage>,
    },
    /// Update self own public address, useful when communicating for first time
    UpdatePublicAddr(SocketAddr),
    /// This is private, so when establishing connections hole-punching should be performed
    IsPrivatePeer(SocketAddr),
    NodeAction(NodeEvent),
    ClosedChannel,
    NoAction,
}

type UniqConnId = usize;

#[derive(Debug)]
pub(in crate::node) enum HandlerEvent {
    Inbound(Either<NetMessage, NodeEvent>),
    Outbound(Either<NetMessage, NodeEvent>),
}

#[allow(dead_code)]
enum ProtocolStatus {
    Unconfirmed,
    Confirmed,
    Reported,
    Failed,
}

#[inline(always)]
fn encode_msg(msg: NetMessage) -> Result<Vec<u8>, ConnectionError> {
    bincode::serialize(&msg).map_err(|err| ConnectionError::Serialization(Some(err)))
}

#[inline(always)]
fn decode_msg(buf: BytesMut) -> Result<NetMessage, ConnectionError> {
    let cursor = std::io::Cursor::new(buf);
    bincode::deserialize_from(cursor).map_err(|err| ConnectionError::Serialization(Some(err)))
}
