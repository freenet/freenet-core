//! Operation which seeks new connections in the ring.
use futures::Future;
use std::pin::Pin;
use std::{collections::HashSet, time::Duration};

use super::{OpError, OpOutcome, OperationResult};
use crate::operations::op_trait::Operation;
use crate::operations::OpInitialization;
use crate::{
    client_events::ClientId,
    config::PEER_TIMEOUT,
    message::{InnerMessage, Message, Transaction},
    node::{ConnectionBridge, ConnectionError, OpManager, PeerKey},
    operations::OpEnum,
    ring::{Location, PeerKeyLocation, Ring},
    util::ExponentialBackoff,
};

pub(crate) use self::messages::{ConnectMsg, ConnectRequest, ConnectResponse};

pub(crate) struct ConnectOp {
    id: Transaction,
    state: Option<ConnectState>,
    pub gateway: Box<PeerKeyLocation>,
    /// keeps track of the number of retries and applies an exponential backoff cooldown period
    pub backoff: Option<ExponentialBackoff>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl ConnectOp {
    pub fn has_backoff(&self) -> bool {
        self.backoff.is_some()
    }

    pub(super) fn outcome(&self) -> OpOutcome {
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, Some(ConnectState::Connected))
    }

    pub(super) fn record_transfer(&mut self) {}
}

/// Not really used since client requests will never interact with this directly.
pub(crate) struct ConnectResult {}

impl TryFrom<ConnectOp> for ConnectResult {
    type Error = OpError;

    fn try_from(_value: ConnectOp) -> Result<Self, Self::Error> {
        Ok(Self {})
    }
}

/*
Will need to do some changes for when we perform parallel joins.

t0: (#1 join attempt)
joiner:
    1. dont knows location
    3. knows location
    n. connected to the ring
gateway:
    2. assigned_location: None; assigned location to `joiner` based on IP and communicate to joiner
    4. forward to N peers
    ...

(2 join subsequently to acquire more/better connections)
join:
    1. knows location
gateway:
    2. assigned_location: Some(loc)
*/

impl Operation for ConnectOp {
    type Message = ConnectMsg;
    type Result = ConnectResult;

    fn load_or_init(
        op_storage: &OpManager,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
        let sender;
        let tx = *msg.id();
        match op_storage.pop(msg.id()) {
            Some(OpEnum::Connect(connect_op)) => {
                sender = msg.sender().cloned();
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization {
                    op: *connect_op,
                    sender,
                })
            }
            Some(_) => Err(OpError::OpNotPresent(tx)),
            None => {
                // new request to join this node, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        id: tx,
                        state: Some(ConnectState::Initializing),
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

    fn process_message<'a, CB: ConnectionBridge>(
        self,
        conn_manager: &'a mut CB,
        op_storage: &'a OpManager,
        input: Self::Message,
        _client_id: Option<ClientId>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let mut new_state = None;

            match input {
                ConnectMsg::Request {
                    id,
                    msg:
                        ConnectRequest::StartReq {
                            target: this_node_loc,
                            joiner,
                            hops_to_live,
                            assigned_location,
                            ..
                        },
                } => {
                    // likely a gateway which accepts connections
                    tracing::debug!(
                        tx = %id,
                        "Connection request received from {} with HTL {} @ {}",
                        joiner,
                        hops_to_live,
                        this_node_loc.peer
                    );

                    // todo: location should be based on your public IP
                    let new_location = assigned_location.unwrap_or_else(Location::random);
                    // FIXME: don't try to forward to peers which have already been tried (add a rejected_by list)
                    let accepted_by = if op_storage.ring.should_accept(&new_location) {
                        tracing::debug!(tx = %id, "Accepting connection from {}", joiner,);
                        HashSet::from_iter([this_node_loc])
                    } else {
                        tracing::debug!(tx = %id, at_peer = %this_node_loc.peer, "Rejecting connection from peer {}", joiner);
                        HashSet::new()
                    };

                    let new_peer_loc = PeerKeyLocation {
                        location: Some(new_location),
                        peer: joiner,
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
                            tx = %id,
                            "Awaiting proxy response from @ {}",
                            this_node_loc.peer,
                        );
                        updated_state.add_new_proxy(accepted_by)?;
                        // awaiting responses from proxies
                        new_state = Some(updated_state);
                        return_msg = None;
                    } else {
                        if !accepted_by.is_empty() {
                            tracing::debug!(
                                tx = %id,
                                "OC received at gateway {} from requesting peer {}",
                                this_node_loc.peer,
                                joiner
                            );
                            new_state = Some(ConnectState::OCReceived);
                        } else {
                            new_state = None
                        }
                        return_msg = Some(ConnectMsg::Response {
                            id,
                            sender: this_node_loc,
                            msg: ConnectResponse::AcceptedBy {
                                peers: accepted_by,
                                your_location: new_location,
                                your_peer_id: joiner,
                            },
                            target: PeerKeyLocation {
                                peer: joiner,
                                location: Some(new_location),
                            },
                        });
                    }
                }
                ConnectMsg::Request {
                    id,
                    msg:
                        ConnectRequest::Proxy {
                            sender,
                            joiner,
                            hops_to_live,
                        },
                } => {
                    let own_loc = op_storage.ring.own_location();
                    tracing::debug!(
                        tx = %id,
                        "Proxy connect request received from {} to ing new peer {} with HTL {} @ {}",
                        sender.peer,
                        joiner.peer,
                        hops_to_live,
                        own_loc.peer
                    );
                    let mut accepted_by = if op_storage
                        .ring
                        .should_accept(&joiner.location.ok_or(ConnectionError::LocationUnknown)?)
                    {
                        tracing::debug!(tx = %id, "Accepting proxy connection from {}", joiner.peer);
                        HashSet::from_iter([own_loc])
                    } else {
                        tracing::debug!(
                            tx = %id,
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
                            Some(ConnectState::Initializing) => {
                                let (state, msg) = try_proxy_connection(
                                    &id,
                                    &sender,
                                    &own_loc,
                                    accepted_by.clone(),
                                );
                                new_state = state;
                                return_msg = msg;
                            }
                            Some(ConnectState::AwaitingProxyResponse {
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
                                    new_state = Some(ConnectState::OCReceived);
                                    tracing::debug!(
                                        tx = %id,
                                        "Sending response to join request with all the peers that accepted \
                                        connection from gateway {} to peer {}",
                                        sender.peer,
                                        target.peer
                                    );
                                    return_msg = Some(ConnectMsg::Response {
                                        id,
                                        target,
                                        sender,
                                        msg: ConnectResponse::AcceptedBy {
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
                                    new_state = Some(ConnectState::Connected);
                                    tracing::debug!(
                                        tx = %id,
                                        "Sending response to connect request with all the peers that accepted \
                                        connection from proxy peer {} to proxy peer {}",
                                        sender.peer,
                                        own_loc.peer
                                    );
                                    return_msg = Some(ConnectMsg::Response {
                                        id,
                                        target,
                                        sender,
                                        msg: ConnectResponse::Proxy { accepted_by },
                                    });
                                }
                            }
                            _ => return Err(OpError::InvalidStateTransition(self.id)),
                        };
                        if let Some(state) = new_state {
                            if state.is_connected() {
                                new_state = None;
                            } else {
                                new_state = Some(state);
                            }
                        };
                    }
                }
                ConnectMsg::Response {
                    id,
                    sender,
                    msg:
                        ConnectResponse::AcceptedBy {
                            peers: accepted_by,
                            your_location,
                            your_peer_id,
                        },
                    ..
                } => {
                    tracing::debug!(tx = %id, "Connect response received from {}", sender.peer);

                    // Set the given location
                    let pk_loc = PeerKeyLocation {
                        location: Some(your_location),
                        peer: your_peer_id,
                    };

                    // fixme: remove
                    tracing::debug!("accepted by state: {:?} ", self.state,);
                    let Some(ConnectState::Connecting(ConnectionInfo { gateway, .. })) = self.state
                    else {
                        return Err(OpError::InvalidStateTransition(self.id));
                    };
                    if !accepted_by.is_empty() {
                        tracing::debug!("accepted by list: {:?} ", accepted_by);
                        tracing::debug!(
                            tx = %id,
                            "OC received and acknowledged at requesting peer {} from gateway {}",
                            your_peer_id,
                            gateway.peer
                        );
                        new_state = Some(ConnectState::OCReceived);
                        return_msg = Some(ConnectMsg::Response {
                            id,
                            msg: ConnectResponse::ReceivedOC { by_peer: pk_loc },
                            sender: pk_loc,
                            target: sender,
                        });
                        tracing::debug!(
                            tx = %id,
                            this_peer = %your_peer_id,
                            location = %your_location,
                            "Updating assigned location"
                        );
                        op_storage.ring.update_location(Some(your_location));

                        for other_peer in accepted_by {
                            let _ = propagate_oc_to_accepted_peers(
                                conn_manager,
                                op_storage,
                                sender,
                                &other_peer,
                                ConnectMsg::Response {
                                    id,
                                    target: other_peer,
                                    sender: pk_loc,
                                    msg: ConnectResponse::ReceivedOC { by_peer: pk_loc },
                                },
                            )
                            .await;
                        }
                    } else {
                        // no connections accepted, failed
                        tracing::debug!(
                            tx = %id,
                            peer = %your_peer_id,
                            "Failed to establish any connections, aborting"
                        );
                        let op = ConnectOp {
                            id,
                            state: None,
                            gateway: self.gateway,
                            backoff: self.backoff,
                            _ttl: self._ttl,
                        };
                        op_storage
                            .notify_op_change(
                                Message::Aborted(id),
                                OpEnum::Connect(op.into()),
                                None,
                            )
                            .await?;
                        return Err(OpError::StatePushed);
                    }
                }
                ConnectMsg::Response {
                    id,
                    sender,
                    target,
                    msg: ConnectResponse::Proxy { mut accepted_by },
                } => {
                    tracing::debug!(tx = %id, "Received proxy connect at @ {}", target.peer);
                    match self.state {
                        Some(ConnectState::Initializing) => {
                            // the sender of the response is the target of the request and
                            // is only a completed tx if it accepted the connection
                            if accepted_by.contains(&sender) {
                                tracing::debug!(
                                    tx = %id,
                                    "Return to {}, connected at proxy {}",
                                    target.peer,
                                    sender.peer,
                                );
                                new_state = Some(ConnectState::Connected);
                            } else {
                                tracing::debug!("Failed to connect at proxy {}", sender.peer);
                                new_state = None;
                            }
                            return_msg = Some(ConnectMsg::Response {
                                msg: ConnectResponse::Proxy { accepted_by },
                                sender,
                                id,
                                target,
                            });
                        }
                        Some(ConnectState::AwaitingProxyResponse {
                            accepted_by: mut previously_accepted,
                            new_peer_id,
                            target: original_target,
                            new_location,
                        }) => {
                            // Check if the response reached the target node and if the request
                            // has been accepted by any node
                            let is_accepted = !accepted_by.is_empty();
                            let is_target_peer = new_peer_id == original_target.peer;

                            if is_accepted {
                                previously_accepted.extend(accepted_by.drain());
                                if is_target_peer {
                                    new_state = Some(ConnectState::OCReceived);
                                } else {
                                    // for proxies just consider the connection open directly
                                    // what would happen in case that the connection is not confirmed end-to-end
                                    // is that we would end up with a dead connection;
                                    // this then must be dealed with by the normal mechanisms that keep
                                    // connections alive and prune any dead connections
                                    new_state = Some(ConnectState::Connected);
                                }
                            }

                            if is_target_peer {
                                tracing::debug!(
                                    tx = %id,
                                    "Sending response to connect request with all the peers that accepted \
                                    connection from gateway {} to peer {}",
                                    target.peer,
                                    original_target.peer
                                );
                                return_msg = Some(ConnectMsg::Response {
                                    id,
                                    target: original_target,
                                    sender: target,
                                    msg: ConnectResponse::AcceptedBy {
                                        peers: previously_accepted,
                                        your_location: new_location,
                                        your_peer_id: new_peer_id,
                                    },
                                });
                            } else {
                                tracing::debug!(
                                    tx = %id,
                                    "Sending response to connect request with all the peers that accepted \
                                    connection from proxy peer {} to proxy peer {}",
                                    target.peer,
                                    original_target.peer
                                );

                                return_msg = Some(ConnectMsg::Response {
                                    id,
                                    target: original_target,
                                    sender: target,
                                    msg: ConnectResponse::Proxy {
                                        accepted_by: previously_accepted,
                                    },
                                });
                            }
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }
                    if let Some(state) = new_state {
                        if state.is_connected() {
                            new_state = None;
                        } else {
                            new_state = Some(state)
                        }
                    };
                }
                ConnectMsg::Response {
                    id,
                    sender,
                    msg: ConnectResponse::ReceivedOC { by_peer },
                    target,
                } => {
                    match self.state {
                        Some(ConnectState::OCReceived) => {
                            tracing::debug!(tx = %id, "Acknowledge connected at gateway");
                            new_state = Some(ConnectState::Connected);
                            return_msg = Some(ConnectMsg::Connected {
                                id,
                                sender: target,
                                target: sender,
                            });
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }
                    if let Some(state) = new_state {
                        if !state.is_connected() {
                            return Err(OpError::InvalidStateTransition(id));
                        } else {
                            conn_manager.add_connection(sender.peer).await?;
                            op_storage.ring.add_connection(
                                sender.location.ok_or(ConnectionError::LocationUnknown)?,
                                sender.peer,
                            );
                            tracing::debug!(tx = %id, "Opened connection with peer {}", by_peer.peer);
                            new_state = None;
                        }
                    };
                }
                ConnectMsg::Connected { target, sender, id } => {
                    match self.state {
                        Some(ConnectState::OCReceived) => {
                            tracing::debug!(tx = %id, "Acknowledge connected at peer {}", target.peer);
                            new_state = Some(ConnectState::Connected);
                            return_msg = None;
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };
                    if let Some(state) = new_state {
                        if !state.is_connected() {
                            return Err(OpError::InvalidStateTransition(id));
                        } else {
                            tracing::info!(
                                tx = %id,
                                assigned_location = ?op_storage.ring.own_location().location,
                                "Successfully completed connection @ {}",
                                target.peer,
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
    state: Option<ConnectState>,
    msg: Option<ConnectMsg>,
    gateway: Box<PeerKeyLocation>,
    backoff: Option<ExponentialBackoff>,
    ttl: Duration,
) -> Result<OperationResult, OpError> {
    let output_op = Some(ConnectOp {
        id,
        state,
        gateway,
        backoff,
        _ttl: ttl,
    });
    Ok(OperationResult {
        return_msg: msg.map(Message::from),
        state: output_op.map(|op| OpEnum::Connect(Box::new(op))),
    })
}

fn try_proxy_connection(
    id: &Transaction,
    sender: &PeerKeyLocation,
    own_loc: &PeerKeyLocation,
    accepted_by: HashSet<PeerKeyLocation>,
) -> (Option<ConnectState>, Option<ConnectMsg>) {
    let new_state = if accepted_by.contains(own_loc) {
        tracing::debug!(
            tx = %id,
            "Return to {}, connected at proxy {}",
            sender.peer,
            own_loc.peer,
        );
        Some(ConnectState::Connected)
    } else {
        tracing::debug!(tx = %id, "Failed to connect at proxy {}", sender.peer);
        None
    };
    let return_msg = Some(ConnectMsg::Response {
        msg: ConnectResponse::Proxy { accepted_by },
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
    msg: ConnectMsg,
) -> Result<(), OpError> {
    let id = msg.id();
    if op_storage.ring.should_accept(
        &other_peer
            .location
            .ok_or(ConnectionError::LocationUnknown)?,
    ) {
        tracing::info!(tx = %id, "Establishing connection to {}", other_peer.peer);
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
        tracing::debug!(tx = %id, "Not accepting connection to {}", other_peer.peer);
    }

    Ok(())
}

mod states {
    use super::*;
    use std::fmt::Display;

    impl Display for ConnectState {
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

#[derive(Debug)]
enum ConnectState {
    Initializing,
    Connecting(ConnectionInfo),
    AwaitingProxyResponse {
        /// Could be either the joiner or nodes which have been previously forwarded to
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

impl ConnectState {
    fn try_unwrap_connecting(self) -> Result<ConnectionInfo, OpError> {
        if let Self::Connecting(conn_info) = self {
            Ok(conn_info)
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }

    fn is_connected(&self) -> bool {
        matches!(self, ConnectState::Connected { .. })
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
) -> ConnectOp {
    const MAX_JOIN_RETRIES: usize = 3;
    tracing::debug!(tx = %id, "Connecting to gateway {} from {}", gateway.peer, this_peer);
    let state = ConnectState::Connecting(ConnectionInfo {
        gateway,
        this_peer,
        max_hops_to_live,
    });
    let ceiling = if cfg!(test) {
        Duration::from_secs(1)
    } else {
        Duration::from_secs(120)
    };
    ConnectOp {
        id,
        state: Some(state),
        gateway: Box::new(gateway),
        backoff: Some(ExponentialBackoff::new(
            Duration::from_secs(1),
            ceiling,
            MAX_JOIN_RETRIES,
        )),
        _ttl: PEER_TIMEOUT,
    }
}

/// Join ring routine, called upon performing a join operation for this node.
pub(crate) async fn connect_request<CB>(
    tx: Transaction,
    op_storage: &OpManager,
    conn_manager: &mut CB,
    join_op: ConnectOp,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    let ConnectOp {
        id,
        state,
        backoff,
        _ttl,
        ..
    } = join_op;
    let ConnectionInfo {
        gateway,
        this_peer,
        max_hops_to_live,
    } = state.expect("infallible").try_unwrap_connecting()?;

    tracing::info!(
        tx = %id,
        "Connecting to peer {} (at {})",
        gateway.peer,
        gateway.location.ok_or(ConnectionError::LocationUnknown)?,
    );

    conn_manager.add_connection(gateway.peer).await?;
    let assigned_location = op_storage.ring.own_location().location;
    let join_req = Message::from(messages::ConnectMsg::Request {
        id: tx,
        msg: messages::ConnectRequest::StartReq {
            target: gateway,
            joiner: this_peer,
            assigned_location,
            hops_to_live: max_hops_to_live,
            max_hops_to_live,
        },
    });
    conn_manager.send(&gateway.peer, join_req).await?;
    op_storage.push(
        tx,
        OpEnum::Connect(Box::new(ConnectOp {
            id,
            state: Some(ConnectState::Connecting(ConnectionInfo {
                gateway,
                this_peer,
                max_hops_to_live,
            })),
            gateway: Box::new(gateway),
            backoff,
            _ttl,
        })),
    )?;
    Ok(())
}

// FIXME: don't forward to previously rejecting nodes (keep track of skip list)
async fn forward_conn<CM>(
    id: Transaction,
    ring: &Ring,
    conn_manager: &mut CM,
    req_peer: PeerKeyLocation,
    joiner: PeerKeyLocation,
    left_htl: usize,
    num_accepted: usize,
) -> Result<Option<ConnectState>, OpError>
where
    CM: ConnectionBridge,
{
    if left_htl == 0 {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Couldn't forward connect petition, no hops left or enough connections",
        );
        return Ok(None);
    }

    if ring.num_connections() == 0 {
        tracing::warn!(
            tx = %id,
            joiner = %joiner.peer,
            "Couldn't forward connect petition, not enough connections",
        );
        return Ok(None);
    }

    let forward_to = if left_htl >= ring.rnd_if_htl_above {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Randomly selecting peer to forward connect request",
        );
        ring.random_peer(|p| p != &req_peer.peer)
    } else {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Selecting close peer to forward request",
        );
        // FIXME: target the `desired_location`
        ring.routing(&joiner.location.unwrap(), Some(&req_peer.peer), &[])
            .and_then(|pkl| (pkl.peer != joiner.peer).then_some(pkl))
    };

    if let Some(forward_to) = forward_to {
        let forwarded = Message::from(ConnectMsg::Request {
            id,
            msg: ConnectRequest::Proxy {
                joiner,
                hops_to_live: left_htl.min(ring.max_hops_to_live) - 1,
                sender: ring.own_location(),
            },
        });
        tracing::debug!(
            tx = %id,
            "Forwarding connect request from sender {} to {}",
            req_peer.peer,
            forward_to.peer
        );
        conn_manager.send(&forward_to.peer, forwarded).await?;
        // awaiting for responses from forward nodes
        let new_state = ConnectState::AwaitingProxyResponse {
            target: req_peer,
            accepted_by: HashSet::new(),
            new_location: joiner.location.unwrap(),
            new_peer_id: joiner.peer,
        };
        Ok(Some(new_state))
    } else {
        if num_accepted != 0 {
            tracing::warn!(
                tx = %id,
                "Unable to forward, will only be connecting to one peer",
            );
        } else {
            tracing::warn!(tx = %id, "Unable to forward or accept any connections");
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
    pub(crate) enum ConnectMsg {
        Request {
            id: Transaction,
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

        fn target(&self) -> Option<&PeerKeyLocation> {
            use ConnectMsg::*;
            match self {
                Response { target, .. } => Some(target),
                Request {
                    msg: ConnectRequest::StartReq { target, .. },
                    ..
                } => Some(target),
                Connected { target, .. } => Some(target),
                _ => None,
            }
        }

        fn terminal(&self) -> bool {
            use ConnectMsg::*;
            matches!(
                self,
                Response {
                    msg: ConnectResponse::Proxy { .. },
                    ..
                } | Connected { .. }
            )
        }
    }

    impl ConnectMsg {
        pub fn sender(&self) -> Option<&PeerKey> {
            use ConnectMsg::*;
            match self {
                Response { sender, .. } => Some(&sender.peer),
                Connected { sender, .. } => Some(&sender.peer),
                Request {
                    msg:
                        ConnectRequest::StartReq {
                            joiner: req_peer, ..
                        },
                    ..
                } => Some(req_peer),
                _ => None,
            }
        }
    }

    impl Display for ConnectMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    msg: ConnectRequest::StartReq { .. },
                    ..
                } => write!(f, "StartRequest(id: {id})"),
                Self::Request {
                    msg: ConnectRequest::Accepted { .. },
                    ..
                } => write!(f, "RequestAccepted(id: {id})"),
                Self::Request {
                    msg: ConnectRequest::Proxy { .. },
                    ..
                } => write!(f, "ProxyRequest(id: {id})"),
                Self::Response {
                    msg: ConnectResponse::AcceptedBy { .. },
                    ..
                } => write!(f, "RouteValue(id: {id})"),
                Self::Response {
                    msg: ConnectResponse::ReceivedOC { .. },
                    ..
                } => write!(f, "RouteValue(id: {id})"),
                Self::Response {
                    msg: ConnectResponse::Proxy { .. },
                    ..
                } => write!(f, "RouteValue(id: {id})"),
                Self::Connected { .. } => write!(f, "Connected(id: {id})"),
                _ => unimplemented!(),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectRequest {
        StartReq {
            target: PeerKeyLocation,
            joiner: PeerKey,
            assigned_location: Option<Location>,
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
    pub(crate) enum ConnectResponse {
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

    use crate::node::tests::SimNetwork;

    /// Given a network of one node and one gateway test that both are connected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_node_connects_to_gw() {
        let mut sim_nodes = SimNetwork::new("join_one_node_connects_to_gw", 1, 1, 1, 1, 2, 2).await;
        sim_nodes.start().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(sim_nodes.connected(&"node-0".into()));
    }

    /// Once a gateway is left without remaining open slots, ensure forwarding connects
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_connection_to_node() -> Result<(), anyhow::Error> {
        // crate::config::set_logger();
        const NUM_NODES: usize = 3usize;
        const NUM_GW: usize = 1usize;
        let mut sim_nw = SimNetwork::new(
            "join_forward_connection_to_node",
            NUM_GW,
            NUM_NODES,
            2,
            1,
            2,
            1,
        )
        .await;
        // sim_nw.with_start_backoff(Duration::from_millis(100));
        sim_nw.start().await;
        sim_nw.check_connectivity(Duration::from_secs(3)).await?;
        let some_forwarded = sim_nw
            .node_connectivity()
            .into_iter()
            .flat_map(|(_this, (_, conns))| conns.into_keys())
            .any(|c| c.is_node());
        assert!(
            some_forwarded,
            "didn't find any connection succesfully forwarded"
        );
        Ok(())
    }

    /// Given a network of N peers all good connectivity
    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn network_should_achieve_good_connectivity() -> Result<(), anyhow::Error> {
        crate::config::set_logger();
        const NUM_NODES: usize = 10usize;
        const NUM_GW: usize = 2usize;
        let mut sim_nw = SimNetwork::new(
            "join_all_nodes_should_connect",
            NUM_GW,
            NUM_NODES,
            5,
            3,
            6,
            2,
        )
        .await;
        sim_nw.with_start_backoff(Duration::from_millis(200));
        sim_nw.start().await;
        sim_nw.check_connectivity(Duration::from_secs(10)).await?;
        sim_nw.network_connectivity_quality()
    }
}
