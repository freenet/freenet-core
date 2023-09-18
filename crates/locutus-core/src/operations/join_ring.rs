use futures::Future;
use std::pin::Pin;
use std::{collections::HashSet, time::Duration};

use super::{OpError, OperationResult};
use crate::operations::op_trait::Operation;
use crate::operations::OpInitialization;
use crate::{
    config::PEER_TIMEOUT,
    message::{InnerMessage, Message, Transaction},
    node::{ConnectionBridge, ConnectionError, OpManager, PeerKey},
    operations::OpEnum,
    ring::{Location, PeerKeyLocation, Ring},
    util::ExponentialBackoff,
};

pub(crate) use self::messages::{JoinRequest, JoinResponse, JoinRingMsg};

const MAX_JOIN_RETRIES: usize = 3;

pub(crate) struct JoinRingOp {
    id: Transaction,
    state: Option<JRState>,
    pub gateway: Box<PeerKeyLocation>,
    /// keeps track of the number of retries and applies an exponential backoff cooldown period
    pub backoff: Option<ExponentialBackoff>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl JoinRingOp {
    pub fn has_backoff(&self) -> bool {
        self.backoff.is_some()
    }
}

pub(crate) struct JoinRingResult {}

impl TryFrom<JoinRingOp> for JoinRingResult {
    type Error = OpError;

    fn try_from(_value: JoinRingOp) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl<CB: ConnectionBridge> Operation<CB> for JoinRingOp {
    type Message = JoinRingMsg;
    type Result = JoinRingResult;

    fn load_or_init(
        op_storage: &OpManager,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
        let sender;
        let tx = *msg.id();
        match op_storage.pop(msg.id()) {
            Some(OpEnum::JoinRing(join_op)) => {
                sender = msg.sender().cloned();
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization {
                    op: *join_op,
                    sender,
                })
            }
            Some(_) => Err(OpError::OpNotPresent(tx)),
            None => {
                // new request to join this node, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        id: tx,
                        state: Some(JRState::Initializing),
                        backoff: None,
                        gateway: Box::new(op_storage.ring.own_location()),
                        _ttl: PEER_TIMEOUT,
                    },
                    sender: None,
                })
            }
        }
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a>(
        self,
        conn_manager: &'a mut CB,
        op_storage: &'a OpManager,
        input: Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let mut return_msg = None;
            let mut new_state = None;

            match input {
                JoinRingMsg::Request {
                    id,
                    msg:
                        JoinRequest::StartReq {
                            target: this_node_loc,
                            req_peer,
                            hops_to_live,
                            ..
                        },
                } => {
                    // likely a gateway which accepts connections
                    tracing::debug!(
                        "Initial join request received from {} with HTL {} @ {}",
                        req_peer,
                        hops_to_live,
                        this_node_loc.peer
                    );

                    let new_location = Location::random();
                    let accepted_by = if op_storage.ring.should_accept(&new_location) {
                        tracing::debug!("Accepting connection from {}", req_peer,);
                        HashSet::from_iter([this_node_loc])
                    } else {
                        tracing::debug!("Rejecting connection from peer {}", req_peer);
                        HashSet::new()
                    };

                    let new_peer_loc = PeerKeyLocation {
                        location: Some(new_location),
                        peer: req_peer,
                    };
                    if let Some(mut updated_state) = forward_conn(
                        id,
                        &op_storage.ring,
                        conn_manager,
                        new_peer_loc,
                        new_peer_loc,
                        hops_to_live,
                        accepted_by.len(),
                    )
                    .await?
                    {
                        tracing::debug!(
                            "Awaiting proxy response from @ {} (tx: {})",
                            this_node_loc.peer,
                            id
                        );
                        updated_state.add_new_proxy(accepted_by)?;
                        // awaiting responses from proxies
                        new_state = Some(updated_state);
                        return_msg = None;
                    } else {
                        if !accepted_by.is_empty() {
                            tracing::debug!(
                                "OC received at gateway {} from requesting peer {}",
                                this_node_loc.peer,
                                req_peer
                            );
                            new_state = Some(JRState::OCReceived);
                        } else {
                            new_state = None
                        }
                        return_msg = Some(JoinRingMsg::Response {
                            id,
                            sender: this_node_loc,
                            msg: JoinResponse::AcceptedBy {
                                peers: accepted_by,
                                your_location: new_location,
                                your_peer_id: req_peer,
                            },
                            target: PeerKeyLocation {
                                peer: req_peer,
                                location: Some(new_location),
                            },
                        });
                    }
                }
                JoinRingMsg::Request {
                    id,
                    msg:
                        JoinRequest::Proxy {
                            sender,
                            joiner,
                            hops_to_live,
                        },
                } => {
                    let own_loc = op_storage.ring.own_location();
                    tracing::debug!(
                        "Proxy join request received from {} to join new peer {} with HTL {} @ {}",
                        sender.peer,
                        joiner.peer,
                        hops_to_live,
                        own_loc.peer
                    );
                    let mut accepted_by = if op_storage
                        .ring
                        .should_accept(&joiner.location.ok_or(ConnectionError::LocationUnknown)?)
                    {
                        tracing::debug!("Accepting proxy connection from {}", joiner.peer);
                        HashSet::from_iter([own_loc])
                    } else {
                        tracing::debug!(
                            "Not accepting new proxy connection for sender {}",
                            joiner.peer
                        );
                        HashSet::new()
                    };

                    if let Some(mut updated_state) = forward_conn(
                        id,
                        &op_storage.ring,
                        conn_manager,
                        sender,
                        joiner,
                        hops_to_live,
                        accepted_by.len(),
                    )
                    .await?
                    {
                        updated_state.add_new_proxy(accepted_by)?;
                        // awaiting responses from proxies
                        new_state = Some(updated_state);
                        return_msg = None;
                    } else {
                        match self.state {
                            Some(JRState::Initializing) => {
                                let (state, msg) = try_proxy_connection(
                                    &id,
                                    &sender,
                                    &own_loc,
                                    accepted_by.clone(),
                                );
                                new_state = state;
                                return_msg = msg;
                            }
                            Some(JRState::AwaitingProxyResponse {
                                accepted_by: mut previously_accepted,
                                new_peer_id,
                                target,
                                new_location,
                            }) => {
                                // Check if the request reached the target node and if the request
                                // has been accepted by any node
                                let match_target = new_peer_id == target.peer;
                                let is_accepted = !accepted_by.is_empty();

                                if is_accepted {
                                    previously_accepted.extend(accepted_by.drain());
                                }

                                if match_target {
                                    new_state = Some(JRState::OCReceived);
                                    tracing::debug!(
                                        "Sending response to join request with all the peers that accepted \
                                        connection from gateway {} to peer {}",
                                        sender.peer,
                                        target.peer
                                    );
                                    return_msg = Some(JoinRingMsg::Response {
                                        id,
                                        target,
                                        sender,
                                        msg: JoinResponse::AcceptedBy {
                                            peers: accepted_by,
                                            your_location: new_location,
                                            your_peer_id: new_peer_id,
                                        },
                                    });
                                } else {
                                    // for proxies just consider the connection open directly
                                    // what would happen in case that the connection is not confirmed end-to-end
                                    // is that we would end up with a dead connection;
                                    // this then must be dealed with by the normal mechanisms that keep
                                    // connections alive and prune any dead connections
                                    new_state = Some(JRState::Connected);
                                    tracing::debug!(
                                        "Sending response to join request with all the peers that accepted \
                                        connection from proxy peer {} to proxy peer {}",
                                        sender.peer,
                                        own_loc.peer
                                    );
                                    return_msg = Some(JoinRingMsg::Response {
                                        id,
                                        target,
                                        sender,
                                        msg: JoinResponse::Proxy { accepted_by },
                                    });
                                }
                            }
                            _ => return Err(OpError::InvalidStateTransition(self.id)),
                        };
                        if let Some(state) = new_state.clone() {
                            if state.is_connected() {
                                new_state = None;
                            }
                        };
                    }
                }
                JoinRingMsg::Response {
                    id,
                    sender,
                    msg:
                        JoinResponse::AcceptedBy {
                            peers: accepted_by,
                            your_location,
                            your_peer_id,
                        },
                    ..
                } => {
                    tracing::debug!("Join response received from {}", sender.peer);

                    // Set the given location
                    let pk_loc = PeerKeyLocation {
                        location: Some(your_location),
                        peer: your_peer_id,
                    };

                    match self.state {
                        Some(JRState::Connecting(ConnectionInfo { gateway, .. })) => {
                            if !accepted_by.clone().is_empty() {
                                tracing::debug!(
                                    "OC received and acknowledged at requesting peer {} from gateway {}",
                                    your_peer_id,
                                    gateway.peer
                                );
                                new_state = Some(JRState::OCReceived);
                                return_msg = Some(JoinRingMsg::Response {
                                    id,
                                    msg: JoinResponse::ReceivedOC { by_peer: pk_loc },
                                    sender: pk_loc,
                                    target: sender,
                                });
                            }
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };

                    op_storage.ring.update_location(Some(your_location));

                    for other_peer in accepted_by {
                        let _ = propagate_oc_to_accepted_peers(
                            conn_manager,
                            op_storage,
                            sender,
                            &other_peer,
                            JoinRingMsg::Response {
                                id,
                                target: other_peer,
                                sender: pk_loc,
                                msg: JoinResponse::ReceivedOC { by_peer: pk_loc },
                            },
                        )
                        .await;
                    }
                    op_storage.ring.update_location(Some(your_location));
                }
                JoinRingMsg::Response {
                    id,
                    sender,
                    target,
                    msg: JoinResponse::Proxy { mut accepted_by },
                } => {
                    tracing::debug!("Received proxy join at @ {}", target.peer);
                    match self.state {
                        Some(JRState::Initializing) => {
                            // the sender of the response is the target of the request and
                            // is only a completed tx if it accepted the connection
                            if accepted_by.contains(&sender) {
                                tracing::debug!(
                                    "Return to {}, connected at proxy {} (tx: {})",
                                    target.peer,
                                    sender.peer,
                                    id
                                );
                                new_state = Some(JRState::Connected);
                            } else {
                                tracing::debug!("Failed to connect at proxy {}", sender.peer);
                                new_state = None;
                            }
                            return_msg = Some(JoinRingMsg::Response {
                                msg: JoinResponse::Proxy { accepted_by },
                                sender,
                                id,
                                target,
                            });
                        }
                        Some(JRState::AwaitingProxyResponse {
                            accepted_by: mut previously_accepted,
                            new_peer_id,
                            target: state_target,
                            new_location,
                        }) => {
                            // Check if the response reached the target node and if the request
                            // has been accepted by any node
                            let is_accepted = !accepted_by.is_empty();
                            let is_target_peer = new_peer_id == state_target.peer;

                            if is_accepted {
                                previously_accepted.extend(accepted_by.drain());
                                if is_target_peer {
                                    new_state = Some(JRState::OCReceived);
                                } else {
                                    // for proxies just consider the connection open directly
                                    // what would happen in case that the connection is not confirmed end-to-end
                                    // is that we would end up with a dead connection;
                                    // this then must be dealed with by the normal mechanisms that keep
                                    // connections alive and prune any dead connections
                                    new_state = Some(JRState::Connected);
                                }
                            }

                            if is_target_peer {
                                tracing::debug!(
                                    "Sending response to join request with all the peers that accepted \
                                    connection from gateway {} to peer {}",
                                    target.peer,
                                    state_target.peer
                                );
                                return_msg = Some(JoinRingMsg::Response {
                                    id,
                                    target: state_target,
                                    sender: target,
                                    msg: JoinResponse::AcceptedBy {
                                        peers: accepted_by,
                                        your_location: new_location,
                                        your_peer_id: new_peer_id,
                                    },
                                });
                            } else {
                                tracing::debug!(
                                    "Sending response to join request with all the peers that accepted \
                                    connection from proxy peer {} to proxy peer {}",
                                    target.peer,
                                    state_target.peer
                                );

                                return_msg = Some(JoinRingMsg::Response {
                                    id,
                                    target: state_target,
                                    sender: target,
                                    msg: JoinResponse::Proxy { accepted_by },
                                });
                            }
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }
                    if let Some(state) = new_state.clone() {
                        if state.is_connected() {
                            new_state = None;
                        }
                    };
                }
                JoinRingMsg::Response {
                    id,
                    sender,
                    msg: JoinResponse::ReceivedOC { by_peer },
                    target,
                } => {
                    match self.state {
                        Some(JRState::OCReceived) => {
                            tracing::debug!("Acknowledge connected at gateway");
                            new_state = Some(JRState::Connected);
                            return_msg = Some(JoinRingMsg::Connected {
                                id,
                                sender: target,
                                target: sender,
                            });
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }
                    if let Some(state) = new_state.clone() {
                        if !state.is_connected() {
                            return Err(OpError::InvalidStateTransition(id));
                        } else {
                            conn_manager.add_connection(sender.peer).await?;
                            op_storage.ring.add_connection(
                                sender.location.ok_or(ConnectionError::LocationUnknown)?,
                                sender.peer,
                            );
                            tracing::debug!("Opened connection with peer {}", by_peer.peer);
                            new_state = None;
                        }
                    };
                }
                JoinRingMsg::Connected { target, sender, id } => {
                    match self.state {
                        Some(JRState::OCReceived) => {
                            tracing::debug!("Acknowledge connected at peer");
                            new_state = Some(JRState::Connected);
                            return_msg = None;
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };
                    if let Some(state) = new_state.clone() {
                        if !state.is_connected() {
                            return Err(OpError::InvalidStateTransition(id));
                        } else {
                            tracing::info!(
                                "Successfully completed connection @ {}, new location = {:?}",
                                target.peer,
                                op_storage.ring.own_location().location
                            );
                            conn_manager.add_connection(sender.peer).await?;
                            op_storage.ring.add_connection(
                                sender.location.ok_or(ConnectionError::LocationUnknown)?,
                                sender.peer,
                            );
                            new_state = None;
                        }
                    };
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(
                self.id,
                new_state,
                return_msg,
                self.gateway,
                self.backoff,
                self._ttl,
            )
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<JRState>,
    msg: Option<JoinRingMsg>,
    gateway: Box<PeerKeyLocation>,
    backoff: Option<ExponentialBackoff>,
    ttl: Duration,
) -> Result<OperationResult, OpError> {
    let output_op = Some(JoinRingOp {
        id,
        state,
        gateway,
        backoff,
        _ttl: ttl,
    });
    Ok(OperationResult {
        return_msg: msg.map(Message::from),
        state: output_op.map(|op| OpEnum::JoinRing(Box::new(op))),
    })
}

fn try_proxy_connection(
    id: &Transaction,
    sender: &PeerKeyLocation,
    own_loc: &PeerKeyLocation,
    accepted_by: HashSet<PeerKeyLocation>,
) -> (Option<JRState>, Option<JoinRingMsg>) {
    let new_state = if accepted_by.contains(own_loc) {
        tracing::debug!(
            "Return to {}, connected at proxy {} (tx: {})",
            sender.peer,
            own_loc.peer,
            id
        );
        Some(JRState::Connected)
    } else {
        tracing::debug!("Failed to connect at proxy {}", sender.peer);
        None
    };
    let return_msg = Some(JoinRingMsg::Response {
        msg: JoinResponse::Proxy { accepted_by },
        sender: *own_loc,
        id: *id,
        target: *sender,
    });
    (new_state, return_msg)
}

async fn propagate_oc_to_accepted_peers<CB: ConnectionBridge>(
    conn_manager: &mut CB,
    op_storage: &OpManager,
    sender: PeerKeyLocation,
    other_peer: &PeerKeyLocation,
    msg: JoinRingMsg,
) -> Result<(), OpError> {
    if op_storage.ring.should_accept(
        &other_peer
            .location
            .ok_or(ConnectionError::LocationUnknown)?,
    ) {
        tracing::info!("Established connection to {}", other_peer.peer);
        conn_manager.add_connection(other_peer.peer).await?;
        op_storage.ring.add_connection(
            other_peer
                .location
                .ok_or(ConnectionError::LocationUnknown)?,
            other_peer.peer,
        );
        if other_peer.peer != sender.peer {
            // notify all the additional peers which accepted a request;
            // the gateway will be notified in the last message
            let _ = conn_manager.send(&other_peer.peer, msg.into()).await;
        }
    } else {
        tracing::debug!("Not accepting connection to {}", other_peer.peer);
    }

    Ok(())
}

mod states {
    use super::*;
    use std::fmt::Display;

    impl Display for JRState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Initializing => write!(f, "Initializing"),
                Self::Connecting(connection_info) => {
                    write!(f, "Connecting(info: {connection_info:?})")
                }
                Self::AwaitingProxyResponse { .. } => write!(f, "AwaitingProxyResponse"),
                Self::OCReceived => write!(f, "OCReceived"),
                Self::Connected => write!(f, "Connected"),
            }
        }
    }
}

#[derive(Debug, Clone)]
enum JRState {
    Initializing,
    Connecting(ConnectionInfo),
    AwaitingProxyResponse {
        /// Could be either the requester or nodes which have been previously forwarded to
        target: PeerKeyLocation,
        accepted_by: HashSet<PeerKeyLocation>,
        new_location: Location,
        new_peer_id: PeerKey,
    },
    OCReceived,
    Connected,
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    gateway: PeerKeyLocation,
    this_peer: PeerKey,
    max_hops_to_live: usize,
}

impl JRState {
    fn try_unwrap_connecting(self) -> Result<ConnectionInfo, OpError> {
        if let Self::Connecting(conn_info) = self {
            Ok(conn_info)
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }

    fn is_connected(&self) -> bool {
        matches!(self, JRState::Connected { .. })
    }

    fn add_new_proxy(
        &mut self,
        proxies: impl IntoIterator<Item = PeerKeyLocation>,
    ) -> Result<(), OpError> {
        if let Self::AwaitingProxyResponse { accepted_by, .. } = self {
            accepted_by.extend(proxies);
            Ok(())
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

pub(crate) fn initial_request(
    this_peer: PeerKey,
    gateway: PeerKeyLocation,
    max_hops_to_live: usize,
    id: Transaction,
) -> JoinRingOp {
    tracing::debug!("Connecting to gw {} from {}", gateway.peer, this_peer);
    let state = JRState::Connecting(ConnectionInfo {
        gateway,
        this_peer,
        max_hops_to_live,
    });
    JoinRingOp {
        id,
        state: Some(state),
        gateway: Box::new(gateway),
        backoff: Some(ExponentialBackoff::new(
            Duration::from_secs(1),
            Duration::from_secs(120),
            MAX_JOIN_RETRIES,
        )),
        _ttl: PEER_TIMEOUT,
    }
}

/// Join ring routine, called upon performing a join operation for this node.
pub(crate) async fn join_ring_request<CB>(
    tx: Transaction,
    op_storage: &OpManager,
    conn_manager: &mut CB,
    mut join_op: JoinRingOp,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    let ConnectionInfo {
        gateway,
        this_peer,
        max_hops_to_live,
    } = join_op
        .state
        .as_mut()
        .expect("Infallible")
        .clone()
        .try_unwrap_connecting()?;

    tracing::info!(
        "Joining ring via {} (at {}) (tx: {})",
        gateway.peer,
        gateway.location.ok_or(ConnectionError::LocationUnknown)?,
        tx
    );

    conn_manager.add_connection(gateway.peer).await?;
    let join_req = Message::from(messages::JoinRingMsg::Request {
        id: tx,
        msg: messages::JoinRequest::StartReq {
            target: gateway,
            req_peer: this_peer,
            hops_to_live: max_hops_to_live,
            max_hops_to_live,
        },
    });
    conn_manager.send(&gateway.peer, join_req).await?;
    op_storage.push(tx, OpEnum::JoinRing(Box::new(join_op)))?;
    Ok(())
}

// FIXME: don't forward to previously rejecting nodes (keep track of skip list)
async fn forward_conn<CM>(
    id: Transaction,
    ring: &Ring,
    conn_manager: &mut CM,
    req_peer: PeerKeyLocation,
    new_peer_loc: PeerKeyLocation,
    left_htl: usize,
    num_accepted: usize,
) -> Result<Option<JRState>, OpError>
where
    CM: ConnectionBridge,
{
    if left_htl == 0 || (ring.num_connections() == 0 && num_accepted == 0) {
        return Ok(None);
    }

    let forward_to = if left_htl >= ring.rnd_if_htl_above {
        tracing::debug!(
            "Randomly selecting peer to forward JoinRequest (requester: {})",
            req_peer.peer
        );
        ring.random_peer(|p| p.peer != req_peer.peer)
    } else {
        tracing::debug!(
            "Selecting close peer to forward request (requester: {})",
            req_peer.peer
        );
        ring.routing(
            &new_peer_loc.location.unwrap(),
            Some(&req_peer.peer),
            1,
            &[],
        )
        .pop()
        .filter(|&pkl| pkl.peer != new_peer_loc.peer)
    };

    if let Some(forward_to) = forward_to {
        let forwarded = Message::from(JoinRingMsg::Request {
            id,
            msg: JoinRequest::Proxy {
                joiner: new_peer_loc,
                hops_to_live: left_htl.min(ring.max_hops_to_live) - 1,
                sender: ring.own_location(),
            },
        });
        tracing::debug!(
            "Forwarding JoinRequest from sender {} to {}",
            req_peer.peer,
            forward_to.peer
        );
        conn_manager.send(&forward_to.peer, forwarded).await?;
        // awaiting for responses from forward nodes
        let new_state = JRState::AwaitingProxyResponse {
            target: req_peer,
            accepted_by: HashSet::new(),
            new_location: new_peer_loc.location.unwrap(),
            new_peer_id: new_peer_loc.peer,
        };
        Ok(Some(new_state))
    } else {
        if num_accepted != 0 {
            tracing::warn!(
                "Unable to forward, will only be connected to one peer (tx: {})",
                id
            );
        } else {
            tracing::warn!("Unable to forward or accept any connections (tx: {})", id);
        }
        Ok(None)
    }
}

mod messages {
    use std::fmt::Display;

    use super::*;
    use crate::ring::{Location, PeerKeyLocation};

    use crate::message::InnerMessage;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum JoinRingMsg {
        Request {
            id: Transaction,
            msg: JoinRequest,
        },
        Response {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            msg: JoinResponse,
        },
        Connected {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
        },
    }

    impl InnerMessage for JoinRingMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. } => id,
                Self::Response { id, .. } => id,
                Self::Connected { id, .. } => id,
            }
        }
    }

    impl JoinRingMsg {
        pub fn sender(&self) -> Option<&PeerKey> {
            use JoinRingMsg::*;
            match self {
                Response { sender, .. } => Some(&sender.peer),
                Connected { sender, .. } => Some(&sender.peer),
                Request {
                    msg: JoinRequest::StartReq { req_peer, .. },
                    ..
                } => Some(req_peer),
                _ => None,
            }
        }

        pub fn target(&self) -> Option<&PeerKeyLocation> {
            use JoinRingMsg::*;
            match self {
                Response { target, .. } => Some(target),
                Request {
                    msg: JoinRequest::StartReq { target, .. },
                    ..
                } => Some(target),
                Connected { target, .. } => Some(target),
                _ => None,
            }
        }

        pub fn terminal(&self) -> bool {
            use JoinRingMsg::*;
            matches!(
                self,
                Response {
                    msg: JoinResponse::Proxy { .. },
                    ..
                } | Connected { .. }
            )
        }
    }

    impl Display for JoinRingMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    msg: JoinRequest::StartReq { .. },
                    ..
                } => write!(f, "StartRequest(id: {id})"),
                Self::Request {
                    msg: JoinRequest::Accepted { .. },
                    ..
                } => write!(f, "RequestAccepted(id: {id})"),
                Self::Request {
                    msg: JoinRequest::Proxy { .. },
                    ..
                } => write!(f, "ProxyRequest(id: {id})"),
                Self::Response {
                    msg: JoinResponse::AcceptedBy { .. },
                    ..
                } => write!(f, "RouteValue(id: {id})"),
                Self::Response {
                    msg: JoinResponse::ReceivedOC { .. },
                    ..
                } => write!(f, "RouteValue(id: {id})"),
                Self::Response {
                    msg: JoinResponse::Proxy { .. },
                    ..
                } => write!(f, "RouteValue(id: {id})"),
                Self::Connected { .. } => write!(f, "Connected(id: {id})"),
                _ => todo!(),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum JoinRequest {
        StartReq {
            target: PeerKeyLocation,
            req_peer: PeerKey,
            hops_to_live: usize,
            max_hops_to_live: usize,
        },
        Accepted {
            gateway: PeerKeyLocation,
            accepted_by: HashSet<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerKey,
        },
        Proxy {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
        },
        ReceivedOC,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum JoinResponse {
        AcceptedBy {
            peers: HashSet<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerKey,
        },
        ReceivedOC {
            by_peer: PeerKeyLocation,
        },
        Proxy {
            accepted_by: HashSet<PeerKeyLocation>,
        },
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::node::tests::{check_connectivity, SimNetwork};

    /// Given a network of one node and one gateway test that both are connected.
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_node_connects_to_gw() {
        let mut sim_nodes = SimNetwork::new(1, 1, 1, 1, 2, 2).await;
        sim_nodes.build().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(sim_nodes.connected("node-0"));
    }

    /// Once a gateway is left without remaining open slots, ensure forwarding connects
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_connection_to_node() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 10usize;
        const NUM_GW: usize = 1usize;
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 2).await;
        sim_nodes.build().await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await
    }

    /// Given a network of N peers all nodes should have connections.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore]
    async fn all_nodes_should_connect() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 10usize;
        const NUM_GW: usize = 1usize;
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 1000, 2).await;
        sim_nodes.build().await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(10)).await
    }
}
