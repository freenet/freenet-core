//! Operation which seeks new connections in the ring.
use std::borrow::Borrow;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use freenet_stdlib::client_api::HostResponse;
use futures::Future;

pub(crate) use self::messages::{ConnectMsg, ConnectRequest, ConnectResponse};
use super::{connect, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::client_events::HostResult;
use crate::dev_tool::Location;
use crate::message::{NetMessageV1, NodeEvent};
use crate::node::IsOperationCompleted;
use crate::ring::ConnectionManager;
use crate::router::Router;
use crate::transport::TransportPublicKey;
use crate::{
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager, PeerId},
    operations::OpEnum,
    ring::PeerKeyLocation,
    util::Backoff,
};

#[derive(Debug)]
pub(crate) struct ConnectOp {
    id: Transaction,
    pub(crate) state: Option<ConnectState>,
    pub gateway: Option<Box<PeerKeyLocation>>,
    /// keeps track of the number of retries and applies an exponential backoff cooldown period
    pub backoff: Option<Backoff>,
}

impl ConnectOp {
    pub fn new(
        id: Transaction,
        state: Option<ConnectState>,
        gateway: Option<Box<PeerKeyLocation>>,
        backoff: Option<Backoff>,
    ) -> Self {
        Self {
            id,
            state,
            gateway,
            backoff,
        }
    }

    #[allow(dead_code)]
    pub fn has_backoff(&self) -> bool {
        self.backoff.is_some()
    }

    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, Some(ConnectState::Connected))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        // this shouldn't ever be called since clients can't request explicit connects
        Ok(HostResponse::Ok)
    }
}

impl IsOperationCompleted for ConnectOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(connect::ConnectState::Connected))
    }
}

/// Not really used since client requests will never interact with this directly.
pub(crate) struct ConnectResult {}

impl TryFrom<ConnectOp> for ConnectResult {
    type Error = OpError;

    fn try_from(_value: ConnectOp) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}

impl Operation for ConnectOp {
    type Message = ConnectMsg;
    type Result = ConnectResult;

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
        let sender;
        let tx = *msg.id();
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Connect(connect_op))) => {
                sender = msg.sender().cloned();
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization {
                    op: *connect_op,
                    sender,
                })
            }
            Ok(Some(op)) => {
                let _ = op_manager.push(tx, op).await;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                let gateway = if !matches!(
                    msg,
                    ConnectMsg::Request {
                        msg: ConnectRequest::FindOptimalPeer { .. },
                        ..
                    }
                ) {
                    Some(Box::new(op_manager.ring.connection_manager.own_location()))
                } else {
                    None
                };
                // new request to join this node, initialize the state
                Ok(OpInitialization {
                    op: Self {
                        id: tx,
                        state: Some(ConnectState::Initializing),
                        backoff: None,
                        gateway,
                    },
                    sender: None,
                })
            }
            Err(err) => {
                #[cfg(debug_assertions)]
                if matches!(err, crate::node::OpNotAvailable::Completed) {
                    let target = msg.target();
                    let target = target.as_ref().map(|b| b.borrow());
                    tracing::warn!(%tx, peer = ?target, "filtered");
                }
                Err(err.into())
            }
        }
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a, NB: NetworkBridge>(
        mut self,
        network_bridge: &'a mut NB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let new_state;

            match input {
                ConnectMsg::Request {
                    msg:
                        ConnectRequest::FindOptimalPeer {
                            query_target,
                            ideal_location,
                            joiner,
                            max_hops_to_live,
                            skip_connections,
                            skip_forwards,
                        },
                    id,
                    ..
                } => {
                    let ring_max_htl = op_manager.ring.max_hops_to_live.max(1);
                    let mut max_hops = (*max_hops_to_live).min(ring_max_htl);
                    if max_hops == 0 {
                        max_hops = 1;
                    }
                    let own_loc = op_manager.ring.connection_manager.own_location();
                    let PeerKeyLocation {
                        peer: this_peer,
                        location: Some(_),
                    } = &own_loc
                    else {
                        return Err(OpError::RingError(crate::ring::RingError::NoLocation));
                    };
                    let mut skip_connections = skip_connections.clone();
                    let mut skip_forwards = skip_forwards.clone();
                    skip_connections.extend([
                        this_peer.clone(),
                        query_target.peer.clone(),
                        joiner.peer.clone(),
                    ]);
                    skip_forwards.extend([this_peer.clone(), query_target.peer.clone()]);
                    if this_peer == &query_target.peer {
                        // this peer should be the original target queries
                        tracing::info!(
                            tx = %id,
                            query_target = %query_target.peer,
                            joiner = %joiner.peer,
                            skip_connections_count = skip_connections.len(),
                            "Gateway received FindOptimalPeer request from joiner",
                        );
                        // Use the full skip_connections set to avoid recommending peers
                        // that the joiner is already connected to (including the gateway itself)
                        if let Some(desirable_peer) = op_manager.ring.closest_to_location(
                            *ideal_location,
                            skip_connections.iter().cloned().collect(),
                        ) {
                            tracing::info!(
                                tx = %id,
                                query_target = %query_target.peer,
                                joiner = %joiner.peer,
                                desirable_peer = %desirable_peer.peer,
                                "Gateway found desirable peer, forwarding to joiner",
                            );
                            let msg = create_forward_message(
                                *id,
                                &own_loc,
                                joiner,
                                &desirable_peer,
                                max_hops,
                                max_hops,
                                skip_connections,
                                skip_forwards,
                            );
                            network_bridge.send(&desirable_peer.peer, msg).await?;
                            return_msg = None;
                            new_state = Some(ConnectState::AwaitingConnectionAcquisition {});
                        } else {
                            tracing::warn!(
                                tx = %id,
                                query_target = %query_target.peer,
                                joiner = %joiner.peer,
                                "Gateway found no suitable peers to forward CheckConnectivity request",
                            );
                            // Send a negative response back to the joiner to inform them
                            // that no suitable peers are currently available
                            let response = ConnectResponse::AcceptedBy {
                                accepted: false,
                                acceptor: own_loc.clone(),
                                joiner: joiner.peer.clone(),
                            };
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                sender: own_loc.clone(),
                                target: joiner.clone(),
                                msg: response,
                            });
                            new_state = None;
                        }
                    } else {
                        // this peer is the one establishing connections
                        tracing::debug!(
                            tx = %id,
                            query_target = %query_target.peer,
                            this_peer = %joiner.peer,
                            "Querying the query target for new connections",
                        );
                        debug_assert_eq!(this_peer, &joiner.peer);
                        new_state = Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                            remaining_connections: max_hops,
                        }));
                        let msg = ConnectMsg::Request {
                            id: *id,
                            target: query_target.clone(),
                            msg: ConnectRequest::FindOptimalPeer {
                                query_target: query_target.clone(),
                                ideal_location: *ideal_location,
                                joiner: joiner.clone(),
                                max_hops_to_live: max_hops,
                                skip_connections,
                                skip_forwards,
                            },
                        };
                        network_bridge.send(&query_target.peer, msg.into()).await?;
                        return_msg = None;
                    }
                }
                ConnectMsg::Request {
                    id,
                    msg:
                        ConnectRequest::CheckConnectivity {
                            sender,
                            joiner,
                            hops_to_live,
                            max_hops_to_live,
                            skip_connections,
                            skip_forwards,
                            ..
                        },
                    ..
                } => {
                    let this_peer = op_manager.ring.connection_manager.own_location();
                    let ring_max_htl = op_manager.ring.max_hops_to_live.max(1);
                    let mut max_htl = (*max_hops_to_live).min(ring_max_htl);
                    if max_htl == 0 {
                        max_htl = 1;
                    }
                    let mut hops_left = (*hops_to_live).min(max_htl);
                    if hops_left == 0 {
                        tracing::warn!(
                            tx = %id,
                            sender = %sender.peer,
                            joiner = %joiner.peer,
                            "Received CheckConnectivity with zero hops to live; clamping to 1"
                        );
                        hops_left = 1;
                    }
                    if sender.peer == joiner.peer {
                        tracing::error!(
                            tx = %id,
                            sender = %sender.peer,
                            joiner = %joiner.peer,
                            at = %this_peer.peer,
                            "Connectivity check from self (sender == joiner), rejecting operation"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }
                    if this_peer.peer == joiner.peer {
                        tracing::error!(
                            tx = %id,
                            this_peer = %this_peer.peer,
                            joiner = %joiner.peer,
                            sender = %sender.peer,
                            "Received CheckConnectivity where this peer is the joiner (self-connection attempt), rejecting operation"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }
                    let joiner_loc = joiner
                        .location
                        .expect("should be already set at the p2p bridge level");

                    tracing::debug!(
                        tx = %id,
                        at = %this_peer.peer,
                        hops_to_live = %hops_left,
                        joiner = %joiner,
                        "Checking connectivity request received"
                    );

                    let requested_accept = op_manager
                        .ring
                        .connection_manager
                        .should_accept(joiner_loc, &joiner.peer);
                    let acceptance_status = if requested_accept {
                        tracing::info!(tx = %id, %joiner, "CheckConnectivity: Accepting connection from, will trigger ConnectPeer");
                        // Ensure the transport layer is ready for the incoming handshake before we notify upstream.
                        op_manager
                            .notify_node_event(NodeEvent::ExpectPeerConnection {
                                peer: joiner.peer.clone(),
                            })
                            .await?;
                        if sender.peer != this_peer.peer {
                            let accept_msg = ConnectMsg::Response {
                                id: *id,
                                sender: this_peer.clone(),
                                target: sender.clone(),
                                msg: ConnectResponse::AcceptedBy {
                                    accepted: true,
                                    acceptor: this_peer.clone(),
                                    joiner: joiner.peer.clone(),
                                },
                            };
                            op_manager
                                .notify_node_event(NodeEvent::SendMessage {
                                    target: sender.peer.clone(),
                                    msg: Box::new(NetMessage::from(accept_msg)),
                                })
                                .await?;
                        }
                        let (callback, mut result) = tokio::sync::mpsc::channel(10);
                        // Attempt to connect to the joiner
                        op_manager
                            .notify_node_event(NodeEvent::ConnectPeer {
                                peer: joiner.peer.clone(),
                                tx: *id,
                                callback,
                                is_gw: false,
                            })
                            .await?;
                        let mut status = true;
                        match result.recv().await.ok_or(OpError::NotificationError)? {
                            Ok((peer_id, remaining_checks)) => {
                                tracing::info!(
                                    tx = %id,
                                    joiner = %joiner.peer,
                                    connected_peer = %peer_id,
                                    remaining_checks,
                                    "ConnectPeer completed successfully"
                                );
                                let was_reserved = true; // reserved just above in call to should_accept
                                op_manager
                                    .ring
                                    .add_connection(joiner_loc, joiner.peer.clone(), was_reserved)
                                    .await;
                            }
                            Err(()) => {
                                tracing::info!(
                                    tx = %id,
                                    joiner = %joiner.peer,
                                    "ConnectPeer failed to establish connection"
                                );
                                op_manager
                                    .ring
                                    .connection_manager
                                    .prune_in_transit_connection(&joiner.peer);
                                status = false;
                                if sender.peer != this_peer.peer {
                                    let decline_msg = ConnectMsg::Response {
                                        id: *id,
                                        sender: this_peer.clone(),
                                        target: sender.clone(),
                                        msg: ConnectResponse::AcceptedBy {
                                            accepted: false,
                                            acceptor: this_peer.clone(),
                                            joiner: joiner.peer.clone(),
                                        },
                                    };
                                    op_manager
                                        .notify_node_event(NodeEvent::SendMessage {
                                            target: sender.peer.clone(),
                                            msg: Box::new(NetMessage::from(decline_msg)),
                                        })
                                        .await?;
                                }
                            }
                        }
                        status
                    } else {
                        tracing::debug!(tx = %id, at = %this_peer.peer, from = %joiner, "Rejecting connection");
                        if sender.peer != this_peer.peer {
                            let decline_msg = ConnectMsg::Response {
                                id: *id,
                                sender: this_peer.clone(),
                                target: sender.clone(),
                                msg: ConnectResponse::AcceptedBy {
                                    accepted: false,
                                    acceptor: this_peer.clone(),
                                    joiner: joiner.peer.clone(),
                                },
                            };
                            op_manager
                                .notify_node_event(NodeEvent::SendMessage {
                                    target: sender.peer.clone(),
                                    msg: Box::new(NetMessage::from(decline_msg)),
                                })
                                .await?;
                        }
                        false
                    };

                    {
                        let mut new_skip_list = skip_connections.clone();
                        new_skip_list.insert(this_peer.peer.clone());
                        if let Some(updated_state) = forward_conn(
                            *id,
                            &op_manager.ring.connection_manager,
                            op_manager.ring.router.clone(),
                            network_bridge,
                            ForwardParams {
                                left_htl: hops_left,
                                max_htl,
                                accepted: requested_accept,
                                skip_connections: skip_connections.clone(),
                                skip_forwards: skip_forwards.clone(),
                                req_peer: sender.clone(),
                                joiner: joiner.clone(),
                                is_gateway: op_manager.ring.is_gateway,
                            },
                        )
                        .await?
                        {
                            new_state = Some(updated_state);
                        } else {
                            new_state = None
                        }
                    }

                    let response_msg = ConnectMsg::Response {
                        id: *id,
                        sender: this_peer.clone(),
                        target: sender.clone(),
                        msg: ConnectResponse::AcceptedBy {
                            accepted: acceptance_status,
                            acceptor: this_peer.clone(),
                            joiner: joiner.peer.clone(),
                        },
                    };
                    return_msg = Some(response_msg);
                }
                ConnectMsg::Response {
                    id,
                    sender,
                    target,
                    msg:
                        ConnectResponse::AcceptedBy {
                            accepted,
                            acceptor,
                            joiner,
                        },
                } => {
                    tracing::debug!(
                        tx = %id,
                        at = %target.peer,
                        from = %sender.peer,
                        "Connect response received",
                    );

                    let this_peer_id = op_manager
                        .ring
                        .connection_manager
                        .get_peer_key()
                        .expect("peer id not found");

                    match self.state.as_mut() {
                        Some(ConnectState::ConnectingToNode(info)) => {
                            assert!(info.remaining_connections > 0);
                            let remaining_connections =
                                info.remaining_connections.saturating_sub(1);

                            if *accepted {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    connected_to = %acceptor.peer,
                                    "Open connection acknowledged at requesting joiner peer",
                                );
                                if acceptor.peer != this_peer_id {
                                    // Ensure inbound handshake packets from the acceptor aren't dropped.
                                    op_manager
                                        .notify_node_event(NodeEvent::ExpectPeerConnection {
                                            peer: acceptor.peer.clone(),
                                        })
                                        .await?;
                                }
                                tracing::info!(
                                    tx = %id,
                                    joiner = %this_peer_id,
                                    acceptor = %acceptor.peer,
                                    location = ?acceptor.location,
                                    "Connect response accepted; registering connection"
                                );
                                info.accepted_by.insert(acceptor.clone());
                                op_manager
                                    .ring
                                    .add_connection(
                                        acceptor.location.expect("location not found"),
                                        acceptor.peer.clone(),
                                        true,
                                    )
                                    .await;
                            } else {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    rejected_peer = %acceptor.peer,
                                    "Connection rejected",
                                );
                                tracing::info!(
                                    tx = %id,
                                    joiner = %this_peer_id,
                                    rejector = %acceptor.peer,
                                    "Connect response rejected by peer"
                                );
                            }

                            let your_location: Location =
                                target.location.expect("location not found");
                            tracing::debug!(
                                tx = %id,
                                at = %this_peer_id,
                                location = %your_location,
                                "Updating assigned location"
                            );
                            op_manager
                                .ring
                                .connection_manager
                                .update_location(target.location);
                            tracing::info!(
                                tx = %id,
                                at = %this_peer_id,
                                new_location = ?target.location,
                                "Updated joiner location from connect response"
                            );

                            if remaining_connections == 0 {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    "All available connections established",
                                );

                                try_clean_gw_connection(*id, network_bridge, info, target.clone())
                                    .await?;

                                new_state = Some(ConnectState::Connected);
                            } else {
                                new_state = Some(ConnectState::ConnectingToNode(info.clone()));
                            }
                            return_msg = None;
                        }
                        Some(ConnectState::AwaitingConnectivity(ConnectivityInfo {
                            remaining_checks,
                            requester,
                            ..
                        })) => {
                            assert!(*remaining_checks > 0);
                            let remaining_checks = remaining_checks.saturating_sub(1);

                            tracing::debug!(
                                tx = %id,
                                at = %this_peer_id,
                                from = %sender.peer,
                                acceptor = %acceptor.peer,
                                accepted = %accepted,
                                "Connectivity check",
                            );

                            if remaining_checks == 0 {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    "All connectivity checks done",
                                );
                                new_state = None;
                            } else {
                                new_state = Some(ConnectState::AwaitingConnectivity(
                                    ConnectivityInfo::new(requester.clone(), remaining_checks),
                                ));
                            }
                            let response = ConnectResponse::AcceptedBy {
                                accepted: *accepted,
                                acceptor: acceptor.clone(),
                                joiner: joiner.clone(),
                            };
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                sender: target.clone(),
                                msg: response,
                                target: requester.clone(),
                            });
                        }
                        Some(ConnectState::AwaitingNewConnection(info)) => {
                            tracing::debug!(
                                tx = %id,
                                at = %this_peer_id,
                                from = %sender.peer,
                                "Connection request forwarded",
                            );
                            assert!(info.remaining_connections > 0);
                            let remaining_connections =
                                info.remaining_connections.saturating_sub(1);

                            if *accepted && *joiner == this_peer_id && acceptor.peer != this_peer_id
                            {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    acceptor = %acceptor.peer,
                                    "Forward path accepted connection; registering inbound expectation"
                                );
                                op_manager
                                    .notify_node_event(NodeEvent::ExpectPeerConnection {
                                        peer: acceptor.peer.clone(),
                                    })
                                    .await?;
                            }

                            if remaining_connections == 0 {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    "All available connections established",
                                );
                                op_manager
                                    .ring
                                    .live_tx_tracker
                                    .missing_candidate_peers(sender.peer.clone())
                                    .await;
                                new_state = None;
                            } else {
                                new_state =
                                    Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                                        remaining_connections,
                                    }));
                            }

                            return_msg = None;
                        }
                        _ => {
                            tracing::debug!(
                                tx = %id,
                                peer = %this_peer_id,
                                "Failed to establish any connections, aborting"
                            );
                            let op = ConnectOp {
                                id: *id,
                                state: None,
                                gateway: self.gateway,
                                backoff: self.backoff,
                            };
                            op_manager
                                .notify_op_change(
                                    NetMessage::V1(NetMessageV1::Aborted(*id)),
                                    OpEnum::Connect(op.into()),
                                )
                                .await?;
                            return Err(OpError::StatePushed);
                        }
                    }
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, self.gateway, self.backoff)
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<ConnectState>,
    msg: Option<ConnectMsg>,
    gateway: Option<Box<PeerKeyLocation>>,
    backoff: Option<Backoff>,
) -> Result<OperationResult, OpError> {
    tracing::debug!(tx = %id, ?msg, "Connect operation result");
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        state: state.map(|state| {
            OpEnum::Connect(Box::new(ConnectOp {
                id,
                state: Some(state),
                gateway,
                backoff,
            }))
        }),
    })
}

async fn try_clean_gw_connection<NB>(
    id: Transaction,
    conn_bridge: &mut NB,
    state: &mut ConnectionInfo,
    joiner: PeerKeyLocation,
) -> Result<(), OpError>
where
    NB: NetworkBridge,
{
    let need_to_clean_gw_conn = state
        .accepted_by
        .iter()
        .all(|pkloc| pkloc.peer != state.gateway.peer);

    if need_to_clean_gw_conn {
        let msg = ConnectMsg::Request {
            id,
            target: state.gateway.clone(),
            msg: ConnectRequest::CleanConnection { joiner },
        };
        conn_bridge.send(&state.gateway.peer, msg.into()).await?;
    }
    Ok(())
}

type Requester = PeerKeyLocation;

#[derive(Debug)]
pub enum ConnectState {
    Initializing,
    #[allow(dead_code)]
    ConnectingToNode(ConnectionInfo),
    AwaitingConnectivity(ConnectivityInfo),
    AwaitingConnectionAcquisition,
    AwaitingNewConnection(NewConnectionInfo),
    Connected,
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectivityInfo {
    remaining_checks: usize,
    requester: Requester,
    /// Indicates this is a gateway bootstrap acceptance that should be registered immediately.
    /// See forward_conn() bootstrap logic and handshake handler for details.
    pub(crate) is_bootstrap_acceptance: bool,
}

impl ConnectivityInfo {
    pub fn new(requester: Requester, remaining_checks: usize) -> Self {
        Self {
            requester,
            remaining_checks,
            is_bootstrap_acceptance: false,
        }
    }

    pub fn new_bootstrap(requester: Requester, remaining_checks: usize) -> Self {
        Self {
            requester,
            remaining_checks,
            is_bootstrap_acceptance: true,
        }
    }

    /// Decrements the remaining checks and returns whether the checks are complete.
    pub fn decrement_check(&mut self) -> bool {
        self.remaining_checks = self.remaining_checks.saturating_sub(1);
        self.remaining_checks == 0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionInfo {
    gateway: PeerKeyLocation,
    accepted_by: HashSet<PeerKeyLocation>,
    remaining_connections: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct NewConnectionInfo {
    remaining_connections: usize,
}

impl ConnectState {
    #[allow(dead_code)]
    fn try_unwrap_connecting(self) -> Result<ConnectionInfo, OpError> {
        if let Self::ConnectingToNode(conn_info) = self {
            Ok(conn_info)
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// # Arguments
///
/// - gateways: Inmutable list of known gateways. Passed when starting up the node.
///   After the initial connections through the gateways are established all other connections
///   (to gateways or regular peers) will be treated as regular connections.
pub(crate) struct ForwardParams {
    pub left_htl: usize,
    pub max_htl: usize,
    pub accepted: bool,
    /// Avoid connecting to these peers.
    pub skip_connections: HashSet<PeerId>,
    /// Avoid forwarding to these peers.
    pub skip_forwards: HashSet<PeerId>,
    pub req_peer: PeerKeyLocation,
    pub joiner: PeerKeyLocation,
    /// Whether this node is a gateway
    pub is_gateway: bool,
}

pub(crate) async fn forward_conn<NB>(
    id: Transaction,
    connection_manager: &ConnectionManager,
    router: Arc<parking_lot::RwLock<Router>>,
    network_bridge: &mut NB,
    params: ForwardParams,
) -> Result<Option<ConnectState>, OpError>
where
    NB: NetworkBridge,
{
    let ForwardParams {
        left_htl,
        max_htl,
        accepted,
        mut skip_connections,
        mut skip_forwards,
        req_peer,
        joiner,
        is_gateway,
    } = params;
    if left_htl == 0 {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Couldn't forward connect petition, no hops left",
        );
        return Ok(None);
    }

    let num_connections = connection_manager.num_connections();
    let num_reserved = connection_manager.get_reserved_connections();
    tracing::info!(
        tx = %id,
        joiner = %joiner.peer,
        num_connections = %num_connections,
        num_reserved = %num_reserved,
        is_gateway = %is_gateway,
        accepted = %accepted,
        skip_connections_count = %skip_connections.len(),
        skip_forwards_count = %skip_forwards.len(),
        "forward_conn: checking connection forwarding",
    );

    // Bootstrap: gateway has no neighbours yet, so we keep the courtesy link and stop here.
    if is_gateway && accepted && num_connections == 0 {
        if num_reserved != 1 {
            tracing::debug!(
                tx = %id,
                joiner = %joiner.peer,
                num_reserved,
                "Gateway bootstrap registration proceeding despite reserved count"
            );
        }
        tracing::info!(
            tx = %id,
            joiner = %joiner.peer,
            "Gateway bootstrap: accepting first neighbour directly"
        );
        let connectivity_info = ConnectivityInfo::new_bootstrap(joiner.clone(), 1);
        return Ok(Some(ConnectState::AwaitingConnectivity(connectivity_info)));
    }

    if num_connections == 0 {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            is_gateway = %is_gateway,
            num_reserved = %num_reserved,
            "Cannot forward or accept: no existing connections, or reserved connections pending",
        );
        return Ok(None);
    }

    // Try to forward the connection request to an existing peer
    if num_connections > 0 {
        let target_peer = {
            let router = router.read();
            select_forward_target(
                id,
                connection_manager,
                &router,
                &req_peer,
                &joiner,
                left_htl,
                &skip_forwards,
            )
        };

        skip_connections.insert(req_peer.peer.clone());
        skip_forwards.insert(req_peer.peer.clone());

        match target_peer {
            Some(target_peer) => {
                tracing::info!(
                    tx = %id,
                    joiner = %joiner.peer,
                    next_hop = %target_peer.peer,
                    htl = left_htl,
                    "forward_conn: forwarding connection request to peer candidate"
                );
                // Successfully found a peer to forward to
                let forward_msg = create_forward_message(
                    id,
                    &req_peer,
                    &joiner,
                    &target_peer,
                    left_htl,
                    max_htl,
                    skip_connections,
                    skip_forwards,
                );
                tracing::debug!(
                    target: "network",
                    tx = %id,
                    "Forwarding connection request to {:?}",
                    target_peer
                );
                network_bridge.send(&target_peer.peer, forward_msg).await?;
                let forwarded_state = update_state_with_forward_info(&req_peer, left_htl)?;
                return Ok(forwarded_state);
            }
            None => {
                // Couldn't find suitable peer to forward to
                tracing::info!(
                    tx = %id,
                    joiner = %joiner.peer,
                    skip_count = skip_forwards.len(),
                    connections = num_connections,
                    accepted_flag = %accepted,
                    "forward_conn: no suitable peer found for forwarding despite available connections"
                );
                return Ok(None);
            }
        }
    }

    // Should be unreachable - we either forwarded or returned None
    unreachable!("forward_conn should have returned by now")
}

fn select_forward_target(
    id: Transaction,
    connection_manager: &ConnectionManager,
    router: &Router,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    left_htl: usize,
    skip_forwards: &HashSet<PeerId>,
) -> Option<PeerKeyLocation> {
    // Create an extended skip list that includes the joiner to prevent forwarding to the joiner
    let mut extended_skip = skip_forwards.clone();
    extended_skip.insert(joiner.peer.clone());
    if let Some(self_peer) = connection_manager.get_peer_key() {
        extended_skip.insert(self_peer);
    }

    if left_htl >= connection_manager.rnd_if_htl_above {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Randomly selecting peer to forward connect request",
        );
        let candidate = connection_manager.random_peer(|p| !extended_skip.contains(p));
        if candidate.is_none() {
            tracing::info!(
                tx = %id,
                joiner = %joiner.peer,
                skip = ?extended_skip,
                "select_forward_target: random selection found no candidate"
            );
        } else if let Some(ref c) = candidate {
            tracing::info!(
                tx = %id,
                joiner = %joiner.peer,
                next_hop = %c.peer,
                "select_forward_target: random candidate selected"
            );
        }
        candidate
    } else {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Selecting close peer to forward request",
        );
        let candidate = connection_manager
            .routing(
                joiner.location.unwrap(),
                Some(&request_peer.peer),
                &extended_skip,
                router,
            )
            .and_then(|pkl| (pkl.peer != joiner.peer).then_some(pkl));
        if candidate.is_none() {
            tracing::info!(
                tx = %id,
                joiner = %joiner.peer,
                skip = ?extended_skip,
                "select_forward_target: router returned no candidate"
            );
        } else if let Some(ref c) = candidate {
            tracing::info!(
                tx = %id,
                joiner = %joiner.peer,
                next_hop = %c.peer,
                "select_forward_target: routing candidate selected"
            );
        }
        candidate
    }
}

#[allow(clippy::too_many_arguments)]
fn create_forward_message(
    id: Transaction,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    target: &PeerKeyLocation,
    hops_to_live: usize,
    max_hops_to_live: usize,
    skip_connections: HashSet<PeerId>,
    skip_forwards: HashSet<PeerId>,
) -> NetMessage {
    NetMessage::from(ConnectMsg::Request {
        id,
        target: target.clone(),
        msg: ConnectRequest::CheckConnectivity {
            sender: request_peer.clone(),
            joiner: joiner.clone(),
            hops_to_live: hops_to_live.saturating_sub(1), // decrement the hops to live for the next hop
            max_hops_to_live,
            skip_connections,
            skip_forwards,
        },
    })
}

fn update_state_with_forward_info(
    requester: &PeerKeyLocation,
    left_htl: usize,
) -> Result<Option<ConnectState>, OpError> {
    let connecivity_info = ConnectivityInfo::new(requester.clone(), left_htl);
    let new_state = ConnectState::AwaitingConnectivity(connecivity_info);
    Ok(Some(new_state))
}

mod messages {
    use std::fmt::Display;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum ConnectMsg {
        Request {
            id: Transaction,
            target: PeerKeyLocation,
            msg: ConnectRequest,
        },
        Response {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            msg: ConnectResponse,
        },
        Connected {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
        },
    }

    impl InnerMessage for ConnectMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. } => id,
                Self::Response { id, .. } => id,
                Self::Connected { id, .. } => id,
            }
        }

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            use ConnectMsg::*;
            match self {
                Request { target, .. } => Some(target),
                Response { target, .. } => Some(target),
                Connected { target, .. } => Some(target),
            }
        }

        fn requested_location(&self) -> Option<Location> {
            self.target().and_then(|pkloc| pkloc.borrow().location)
        }
    }

    impl ConnectMsg {
        pub fn sender(&self) -> Option<&PeerId> {
            use ConnectMsg::*;
            match self {
                Response { sender, .. } => Some(&sender.peer),
                Connected { sender, .. } => Some(&sender.peer),
                Request { .. } => None,
            }
        }
    }

    impl Display for ConnectMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    target,
                    msg: ConnectRequest::StartJoinReq { .. },
                    ..
                } => write!(f, "StartRequest(id: {id}, target: {target})"),
                Self::Request {
                    target,
                    msg: ConnectRequest::CheckConnectivity {
                        sender,
                        joiner,
                        ..
                    },
                    ..
                } => write!(
                    f,
                    "CheckConnectivity(id: {id}, target: {target}, sender: {sender}, joiner: {joiner})"
                ),
                Self::Response {
                    target,
                    msg:
                        ConnectResponse::AcceptedBy {
                            accepted, acceptor, ..
                        },
                    ..
                } => write!(
                    f,
                    "AcceptedBy(id: {id}, target: {target}, accepted: {accepted}, acceptor: {acceptor})"
                ),
                Self::Connected { .. } => write!(f, "Connected(id: {id})"),
                ConnectMsg::Request { id, target, .. } => write!(f, "Request(id: {id}, target: {target})"),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectRequest {
        /// A request to join a gateway.
        StartJoinReq {
            // The peer who is trying to join, should be set when PeerConnection is established
            joiner: Option<PeerId>,
            joiner_key: TransportPublicKey,
            /// Used for deterministic testing purposes. In production, this should be none and will be ignored
            /// by the gateway.
            joiner_location: Option<Location>,
            hops_to_live: usize,
            max_hops_to_live: usize,
            // Peers we don't want to connect to directly
            skip_connections: HashSet<PeerId>,
            // Peers we don't want to forward connectivity messages to (to avoid loops)
            skip_forwards: HashSet<PeerId>,
        },
        /// Query target should find a good candidate for joiner to join.
        FindOptimalPeer {
            /// Peer whom you are querying new connection about.
            query_target: PeerKeyLocation,
            /// The ideal location of the peer to which you would connect.
            ideal_location: Location,
            joiner: PeerKeyLocation,
            max_hops_to_live: usize,
            skip_connections: HashSet<PeerId>,
            skip_forwards: HashSet<PeerId>,
        },
        CheckConnectivity {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
            max_hops_to_live: usize,
            skip_connections: HashSet<PeerId>,
            skip_forwards: HashSet<PeerId>,
        },
        CleanConnection {
            joiner: PeerKeyLocation,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectResponse {
        AcceptedBy {
            accepted: bool,
            acceptor: PeerKeyLocation,
            joiner: PeerId,
        },
    }
}
