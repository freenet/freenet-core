//! Operation which seeks new connections in the ring.
use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use std::pin::Pin;
use std::{collections::HashSet, time::Duration};

use super::{OpError, OpOutcome, OperationResult};
use crate::operations::op_trait::Operation;
use crate::operations::OpInitialization;
use crate::{
    client_events::ClientId,
    config::PEER_TIMEOUT,
    message::{InnerMessage, Message, Transaction},
    node::{ConnectionError, NetworkBridge, OpManager, PeerKey},
    operations::OpEnum,
    ring::{Location, PeerKeyLocation, Ring},
    util::ExponentialBackoff,
};

pub(crate) use self::messages::{ConnectMsg, ConnectRequest, ConnectResponse};

pub(crate) struct ConnectOp {
    id: Transaction,
    state: Option<ConnectState>,
    pub gateway: Option<Box<PeerKeyLocation>>,
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

    fn load_or_init<'a>(
        op_storage: &'a OpManager,
        msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<OpInitialization<Self>, OpError>> {
        async move {
            let sender;
            let tx = *msg.id();
            match op_storage.pop(msg.id()) {
                Ok(Some(OpEnum::Connect(connect_op))) => {
                    sender = msg.sender().cloned();
                    // was an existing operation, the other peer messaged back
                    Ok(OpInitialization {
                        op: *connect_op,
                        sender,
                    })
                }
                Ok(Some(op)) => {
                    let _ = op_storage.push(tx, op).await;
                    Err(OpError::OpNotPresent(tx))
                }
                Ok(None) => {
                    let gateway = if !matches!(
                        msg,
                        ConnectMsg::Request {
                            msg: ConnectRequest::FindPeer { .. },
                            ..
                        }
                    ) {
                        Some(Box::new(op_storage.ring.own_location()))
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
                            _ttl: PEER_TIMEOUT,
                        },
                        sender: None,
                    })
                }
                Err(err) => {
                    #[cfg(debug_assertions)]
                    if matches!(err, crate::node::OpNotAvailable::Completed) {
                        let target = msg.target();
                        tracing::warn!(%tx, peer = ?target, "filtered");
                    }
                    Err(err.into())
                }
            }
        }
        .boxed()
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a, NB: NetworkBridge>(
        self,
        network_bridge: &'a mut NB,
        op_storage: &'a OpManager,
        input: &'a Self::Message,
        _client_id: Option<ClientId>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let mut new_state;

            match input {
                ConnectMsg::Request {
                    msg:
                        ConnectRequest::FindPeer {
                            query_target,
                            ideal_location,
                            joiner,
                        },
                    id,
                } => {
                    let own_loc = op_storage.ring.own_location();
                    let PeerKeyLocation {
                        peer: this_peer,
                        location: Some(_),
                    } = &own_loc
                    else {
                        return Err(OpError::RingError(crate::ring::RingError::NoLocation));
                    };
                    if this_peer == &query_target.peer {
                        // this peer should be the original target queries
                        if let Some(peer) = op_storage.ring.closest_to_location(*ideal_location) {
                            let msg = ConnectMsg::Request {
                                id: *id,
                                msg: ConnectRequest::Proxy {
                                    sender: own_loc,
                                    joiner: *joiner,
                                    hops_to_live: 0,
                                    skip_list: vec![*this_peer],
                                    accepted_by: HashSet::new(),
                                },
                            };
                            network_bridge.send(&peer.peer, msg.into()).await?;
                            return_msg = None;
                            new_state = Some(ConnectState::AwaitingConnectionAcquisition {
                                joiner: *joiner,
                            });
                        } else {
                            todo!("reply back that we didn't found any");
                        }
                    } else {
                        // this peer is the one establishing connections
                        debug_assert_eq!(this_peer, &joiner.peer);
                        new_state = Some(ConnectState::AwaitingNewConnection);
                        let msg = ConnectMsg::Request {
                            id: *id,
                            msg: ConnectRequest::FindPeer {
                                query_target: *query_target,
                                ideal_location: *ideal_location,
                                joiner: *joiner,
                            },
                        };
                        network_bridge.send(&query_target.peer, msg.into()).await?;
                        return_msg = None;
                    }
                }
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
                    let accepted_by = if op_storage.ring.should_accept(&new_location) {
                        tracing::debug!(tx = %id, "Accepting connection from {}", joiner,);
                        HashSet::from_iter([*this_node_loc])
                    } else {
                        tracing::debug!(tx = %id, at_peer = %this_node_loc.peer, "Rejecting connection from peer {}", joiner);
                        HashSet::new()
                    };

                    let new_peer_loc = PeerKeyLocation {
                        location: Some(new_location),
                        peer: *joiner,
                    };
                    if let Some(mut updated_state) = forward_conn(
                        *id,
                        &op_storage.ring,
                        network_bridge,
                        (new_peer_loc, new_peer_loc),
                        *hops_to_live,
                        accepted_by.clone(),
                        vec![this_node_loc.peer],
                    )
                    .await?
                    {
                        tracing::debug!(
                            tx = %id,
                            "Awaiting proxy response from @ {}",
                            this_node_loc.peer,
                        );
                        updated_state.add_new_proxy(accepted_by.iter().copied())?;
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
                            op_storage.completed(*id);
                            new_state = None
                        }
                        return_msg = Some(ConnectMsg::Response {
                            id: *id,
                            sender: *this_node_loc,
                            msg: ConnectResponse::AcceptedBy {
                                peers: accepted_by,
                                your_location: new_location,
                                your_peer_id: *joiner,
                            },
                            target: PeerKeyLocation {
                                peer: *joiner,
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
                            skip_list,
                            accepted_by,
                        },
                } => {
                    let own_loc = op_storage.ring.own_location();
                    let mut accepted_by = accepted_by.clone();
                    tracing::debug!(
                        tx = %id,
                        "Proxy connect request received from {} to connect with peer {} (HTL {hops_to_live} @ {})",
                        sender.peer,
                        joiner.peer,
                        own_loc.peer
                    );
                    if op_storage
                        .ring
                        .should_accept(&joiner.location.ok_or(ConnectionError::LocationUnknown)?)
                    {
                        tracing::debug!(tx = %id, "Accepting proxy connection from {}", joiner.peer);
                        accepted_by.insert(own_loc);
                    } else {
                        tracing::debug!(
                            tx = %id,
                            "Not accepting new proxy connection for sender {}",
                            joiner.peer
                        );
                    }

                    let mut skip_list = skip_list.clone();
                    skip_list.push(own_loc.peer);
                    if let Some(mut updated_state) = forward_conn(
                        *id,
                        &op_storage.ring,
                        network_bridge,
                        (*sender, *joiner),
                        *hops_to_live,
                        accepted_by.clone(),
                        skip_list,
                    )
                    .await?
                    {
                        updated_state.add_new_proxy(accepted_by.iter().copied())?;
                        // awaiting responses from proxies
                        new_state = Some(updated_state);
                        return_msg = None;
                    } else {
                        match self.state {
                            Some(ConnectState::Initializing) => {
                                let (state, msg) = try_returning_proxy_connection(
                                    id,
                                    sender,
                                    &own_loc,
                                    accepted_by,
                                );
                                new_state = state;
                                return_msg = msg;
                            }
                            _ => return Err(OpError::InvalidStateTransition(self.id)),
                        };
                    }
                }
                ConnectMsg::Response {
                    id,
                    sender,
                    target,
                    msg:
                        ConnectResponse::AcceptedBy {
                            peers: accepted_by,
                            your_location,
                            your_peer_id,
                        },
                } => {
                    tracing::debug!(tx = %id, "Connect response received from {}", sender.peer);

                    // Set the given location
                    let pk_loc = PeerKeyLocation {
                        location: Some(*your_location),
                        peer: *your_peer_id,
                    };

                    let Some(ConnectState::Connecting(ConnectionInfo { gateway, .. })) = self.state
                    else {
                        return Err(OpError::InvalidStateTransition(self.id));
                    };
                    if !accepted_by.is_empty() {
                        tracing::debug!(
                            tx = %id,
                            "OC received and acknowledged at requesting peer {} from gateway {}",
                            your_peer_id,
                            gateway.peer
                        );
                        new_state = Some(ConnectState::OCReceived);
                        return_msg = Some(ConnectMsg::Response {
                            id: *id,
                            msg: ConnectResponse::ReceivedOC {
                                by_peer: pk_loc,
                                gateway,
                            },
                            sender: pk_loc,
                            target: *sender,
                        });
                        tracing::debug!(
                            tx = %id,
                            this_peer = %your_peer_id,
                            location = %your_location,
                            "Updating assigned location"
                        );
                        op_storage.ring.update_location(Some(*your_location));

                        for other_peer in accepted_by.iter().filter(|pl| pl.peer != target.peer) {
                            let _ = propagate_oc_to_accepted_peers(
                                network_bridge,
                                op_storage,
                                *sender,
                                other_peer,
                                ConnectMsg::Response {
                                    id: *id,
                                    target: *other_peer,
                                    sender: pk_loc,
                                    msg: ConnectResponse::ReceivedOC {
                                        by_peer: pk_loc,
                                        gateway,
                                    },
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
                            id: *id,
                            state: None,
                            gateway: self.gateway,
                            backoff: self.backoff,
                            _ttl: self._ttl,
                        };
                        op_storage
                            .notify_op_change(
                                Message::Aborted(*id),
                                OpEnum::Connect(op.into()),
                                None,
                            )
                            .await?;
                        return Err(OpError::StatePushed);
                    }
                }
                ConnectMsg::Response {
                    id,
                    target,
                    sender,
                    msg: ConnectResponse::Proxy { accepted_by },
                } => {
                    tracing::debug!(tx = %id, "Received proxy connect response at @ {}", target.peer);
                    match self.state {
                        Some(ConnectState::AwaitingProxyResponse {
                            accepted_by: mut previously_accepted,
                            new_peer_id,
                            target: original_target,
                            new_location,
                        }) => {
                            let own_loc = op_storage.ring.own_location();
                            let is_accepted: bool = accepted_by.contains(&own_loc);
                            let target_is_joiner = new_peer_id == original_target.peer;

                            previously_accepted.extend(accepted_by.iter().copied());
                            if is_accepted {
                                new_state = Some(ConnectState::OCReceived);
                            } else {
                                new_state = None;
                                op_storage.completed(*id);
                            }

                            if target_is_joiner {
                                tracing::debug!(
                                    tx = %id,
                                    "Sending response to connect request with all the peers that accepted \
                                    connection from gateway {} to connecting peer {}",
                                    target.peer,
                                    original_target.peer
                                );
                                return_msg = Some(ConnectMsg::Response {
                                    id: *id,
                                    target: original_target,
                                    sender: *target,
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
                                    id: *id,
                                    target: original_target,
                                    sender: *target,
                                    msg: ConnectResponse::Proxy {
                                        accepted_by: previously_accepted,
                                    },
                                });
                            }
                        }
                        Some(ConnectState::AwaitingConnectionAcquisition { joiner }) => {
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                target: joiner,
                                sender: *target,
                                msg: ConnectResponse::Proxy {
                                    accepted_by: HashSet::from([*sender]),
                                },
                            });
                            new_state = None;
                        }
                        Some(ConnectState::AwaitingNewConnection) => {
                            let joiner = *target;
                            for peer in accepted_by {
                                propagate_oc_to_accepted_peers(
                                    network_bridge,
                                    op_storage,
                                    *sender,
                                    peer,
                                    ConnectMsg::Response {
                                        id: *id,
                                        sender: joiner,
                                        target: *peer,
                                        msg: ConnectResponse::ReceivedOC {
                                            by_peer: joiner,
                                            gateway: *sender, // irrelevant in this case
                                        },
                                    },
                                )
                                .await?;
                            }
                            return_msg = None;
                            new_state = None;
                        }
                        _other_state => {
                            return Err(OpError::InvalidStateTransition(self.id));
                        }
                    }
                }
                ConnectMsg::Response {
                    id,
                    sender,
                    target,
                    msg: ConnectResponse::ReceivedOC { by_peer, gateway },
                } => {
                    match self.state {
                        Some(ConnectState::OCReceived) => {
                            if target == gateway {
                                tracing::debug!(tx = %id, by_peer = %by_peer.peer, at = %target.peer, "Acknowledge connected at gateway");
                                return_msg = Some(ConnectMsg::Connected {
                                    id: *id,
                                    sender: *target,
                                    target: *sender,
                                });
                                new_state = Some(ConnectState::Connected);
                            } else {
                                tracing::debug!(tx = %id, by_peer = %by_peer.peer, at = %target.peer, "Acknowledge connected at peer");
                                return_msg = None;
                                new_state = None;
                            }
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }

                    network_bridge.add_connection(sender.peer).await?;
                    op_storage.ring.add_connection(
                        sender.location.ok_or(ConnectionError::LocationUnknown)?,
                        sender.peer,
                    );
                    tracing::debug!(tx = %id, "Opened connection with peer {}", by_peer.peer);
                    if target != gateway {
                        new_state = None;
                    }
                    op_storage.completed(*id);
                }
                ConnectMsg::Connected { target, sender, id } => {
                    match self.state {
                        Some(ConnectState::OCReceived) => {
                            tracing::debug!(tx = %id, "Acknowledge connected at peer {}", target.peer);
                            return_msg = None;
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };
                    tracing::info!(
                        tx = %id,
                        assigned_location = ?op_storage.ring.own_location().location,
                        "Successfully completed connection @ {}",
                        target.peer,
                    );
                    network_bridge.add_connection(sender.peer).await?;
                    op_storage.ring.add_connection(
                        sender.location.ok_or(ConnectionError::LocationUnknown)?,
                        sender.peer,
                    );
                    new_state = None;
                    op_storage.completed(*id);
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
    gateway: Option<Box<PeerKeyLocation>>,
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

fn try_returning_proxy_connection(
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
        Some(ConnectState::OCReceived)
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

async fn propagate_oc_to_accepted_peers<NB: NetworkBridge>(
    network_bridge: &mut NB,
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
        tracing::info!(tx = %id, from = %sender.peer, "Establishing connection to {}", other_peer.peer);
        network_bridge.add_connection(other_peer.peer).await?;
        op_storage.ring.add_connection(
            other_peer
                .location
                .ok_or(ConnectionError::LocationUnknown)?,
            other_peer.peer,
        );
        if other_peer.peer != sender.peer {
            // notify all the additional peers which accepted a request;
            // the gateway will be notified in the last message
            let _ = network_bridge.send(&other_peer.peer, msg.into()).await;
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
                ConnectState::Initializing => write!(f, "Initializing"),
                ConnectState::Connecting(connection_info) => {
                    write!(f, "Connecting(info: {connection_info:?})")
                }
                ConnectState::AwaitingProxyResponse { .. } => write!(f, "AwaitingProxyResponse"),
                ConnectState::OCReceived => write!(f, "OCReceived"),
                ConnectState::Connected => write!(f, "Connected"),
                ConnectState::AwaitingNewConnection => write!(f, "AwaitingNewConnection"),
                ConnectState::AwaitingConnectionAcquisition { .. } => {
                    write!(f, "AwaitingConnectionAcquisition")
                }
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
    AwaitingConnectionAcquisition {
        joiner: PeerKeyLocation,
    },
    AwaitingNewConnection,
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
        gateway: Some(Box::new(gateway)),
        backoff: Some(ExponentialBackoff::new(
            Duration::from_secs(1),
            ceiling,
            MAX_JOIN_RETRIES,
        )),
        _ttl: PEER_TIMEOUT,
    }
}

/// Join ring routine, called upon performing a join operation for this node.
pub(crate) async fn connect_request<NB>(
    tx: Transaction,
    op_storage: &OpManager,
    conn_bridge: &mut NB,
    join_op: ConnectOp,
) -> Result<(), OpError>
where
    NB: NetworkBridge,
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

    conn_bridge.add_connection(gateway.peer).await?;
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
    conn_bridge.send(&gateway.peer, join_req).await?;
    op_storage
        .push(
            tx,
            OpEnum::Connect(Box::new(ConnectOp {
                id,
                state: Some(ConnectState::Connecting(ConnectionInfo {
                    gateway,
                    this_peer,
                    max_hops_to_live,
                })),
                gateway: Some(Box::new(gateway)),
                backoff,
                _ttl,
            })),
        )
        .await?;
    Ok(())
}

async fn forward_conn<NB>(
    id: Transaction,
    ring: &Ring,
    network_bridge: &mut NB,
    (req_peer, joiner): (PeerKeyLocation, PeerKeyLocation),
    left_htl: usize,
    accepted_by: HashSet<PeerKeyLocation>,
    skip_list: Vec<PeerKey>,
) -> Result<Option<ConnectState>, OpError>
where
    NB: NetworkBridge,
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
        ring.routing(&joiner.location.unwrap(), Some(&req_peer.peer), &skip_list)
            .and_then(|pkl| (pkl.peer != joiner.peer).then_some(pkl))
    };

    if let Some(forward_to) = forward_to {
        let forwarded = Message::from(ConnectMsg::Request {
            id,
            msg: ConnectRequest::Proxy {
                joiner,
                hops_to_live: left_htl.saturating_sub(1),
                sender: ring.own_location(),
                skip_list,
                accepted_by: accepted_by.clone(),
            },
        });
        tracing::debug!(
            tx = %id,
            "Forwarding connect request from sender {} to {}",
            req_peer.peer,
            forward_to.peer
        );
        network_bridge.send(&forward_to.peer, forwarded).await?;
        // awaiting for responses from forward nodes
        let new_state = ConnectState::AwaitingProxyResponse {
            target: req_peer,
            accepted_by,
            new_location: joiner.location.unwrap(),
            new_peer_id: joiner.peer,
        };
        Ok(Some(new_state))
    } else {
        if !accepted_by.is_empty() {
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

    #[derive(Debug, Serialize, Deserialize)]
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
        FindPeer {
            /// Peer to which you are querying new connection about.
            query_target: PeerKeyLocation,
            /// The ideal location of the peer to which you would connect.
            ideal_location: Location,
            joiner: PeerKeyLocation,
        },
        Proxy {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
            skip_list: Vec<PeerKey>,
            accepted_by: HashSet<PeerKeyLocation>,
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
            gateway: PeerKeyLocation,
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
    // #[ignore]
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
