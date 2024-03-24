//! Operation which seeks new connections in the ring.
use freenet_stdlib::client_api::HostResponse;
use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use std::pin::Pin;
use std::{collections::HashSet, time::Duration};

use super::{OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::client_events::HostResult;
use crate::{
    message::{InnerMessage, NetMessage, Transaction},
    node::{ConnectionError, NetworkBridge, OpManager, PeerId},
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

    pub(super) fn to_host_result(&self) -> HostResult {
        // this should't ever be called since clients can't request explicit connects
        Ok(HostResponse::Ok)
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

    fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<OpInitialization<Self>, OpError>> {
        async move {
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
                        Some(Box::new(op_manager.ring.own_location()))
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
        op_manager: &'a OpManager,
        input: &'a Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let mut new_state;

            match input {
                ConnectMsg::Request {
                    msg:
                        ConnectRequest::FindOptimalPeer {
                            query_target,
                            ideal_location,
                            joiner,
                            max_hops_to_live,
                        },
                    id,
                } => {
                    let own_loc = op_manager.ring.own_location();
                    let PeerKeyLocation {
                        peer: this_peer,
                        location: Some(_),
                    } = &own_loc
                    else {
                        return Err(OpError::RingError(crate::ring::RingError::NoLocation));
                    };
                    if this_peer == &query_target.peer {
                        // this peer should be the original target queries
                        tracing::debug!(
                            tx = %id,
                            query_target = %query_target.peer,
                            joiner = %joiner.peer,
                            "Got queried for new connections from joiner",
                        );
                        if let Some(desirable_peer) = op_manager
                            .ring
                            .closest_to_location(*ideal_location, &[joiner.peer])
                        {
                            let msg = ConnectMsg::Request {
                                id: *id,
                                msg: ConnectRequest::Proxy {
                                    sender: own_loc,
                                    joiner: *joiner,
                                    hops_to_live: *max_hops_to_live,
                                    skip_list: vec![*this_peer, joiner.peer],
                                    accepted_by: HashSet::new(),
                                },
                            };
                            network_bridge
                                .send(&desirable_peer.peer, msg.into())
                                .await?;
                            return_msg = None;
                            new_state = Some(ConnectState::AwaitingConnectionAcquisition {
                                joiner: *joiner,
                            });
                        } else {
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                sender: *query_target,
                                target: *joiner,
                                msg: ConnectResponse::Proxy {
                                    accepted_by: HashSet::new(),
                                    joiner: joiner.peer,
                                },
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
                        new_state = Some(ConnectState::AwaitingNewConnection {
                            query_target: query_target.peer,
                        });
                        let msg = ConnectMsg::Request {
                            id: *id,
                            msg: ConnectRequest::FindOptimalPeer {
                                query_target: *query_target,
                                ideal_location: *ideal_location,
                                joiner: *joiner,
                                max_hops_to_live: *max_hops_to_live,
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
                            target: this_peer,
                            joiner,
                            hops_to_live,
                            assigned_location,
                            ..
                        },
                } => {
                    // likely a gateway which accepts connections
                    tracing::debug!(
                        tx = %id,
                        at = %this_peer.peer,
                        from = %joiner,
                        %hops_to_live,
                        "Connection request received",
                    );

                    // todo: location should be based on your public IP
                    let new_location = assigned_location.unwrap_or_else(Location::random);
                    let accepted_by = if op_manager.ring.should_accept(new_location, joiner) {
                        tracing::debug!(tx = %id, %joiner, "Accepting connection from");
                        HashSet::from_iter([*this_peer])
                    } else {
                        tracing::debug!(tx = %id, at = %this_peer.peer, from = %joiner, "Rejecting connection");
                        HashSet::new()
                    };

                    let new_peer_loc = PeerKeyLocation {
                        location: Some(new_location),
                        peer: *joiner,
                    };
                    if let Some(mut updated_state) = forward_conn(
                        *id,
                        &op_manager.ring,
                        network_bridge,
                        (new_peer_loc, new_peer_loc),
                        *hops_to_live,
                        accepted_by.clone(),
                        vec![this_peer.peer, *joiner],
                    )
                    .await?
                    {
                        tracing::debug!(
                            tx = %id,
                            at = %this_peer.peer,
                            "Awaiting proxy response",
                        );
                        updated_state.add_new_proxy(accepted_by.iter().copied())?;
                        // awaiting responses from proxies
                        new_state = Some(updated_state);
                        return_msg = None;
                    } else {
                        if !accepted_by.is_empty() {
                            tracing::debug!(
                                tx = %id,
                                at = %this_peer.peer,
                                %joiner,
                                "Open connection received at gateway",
                            );
                            new_state = Some(ConnectState::OCReceived);
                        } else {
                            new_state = None
                        }
                        return_msg = Some(ConnectMsg::Response {
                            id: *id,
                            sender: *this_peer,
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
                    let own_loc = op_manager.ring.own_location();
                    let mut accepted_by = accepted_by.clone();
                    tracing::debug!(
                        tx = %id,
                        from = %sender.peer,
                        joiner = %joiner.peer,
                        at = %own_loc.peer,
                        %hops_to_live,
                        "Proxy connect request received to connect with peer",
                    );
                    if op_manager.ring.should_accept(
                        joiner.location.ok_or(ConnectionError::LocationUnknown)?,
                        &joiner.peer,
                    ) {
                        tracing::debug!(tx = %id, "Accepting proxy connection from {}", joiner.peer);
                        accepted_by.insert(own_loc);
                    } else {
                        tracing::debug!(
                            tx = %id,
                            joiner = %joiner.peer,
                            at = %own_loc.peer,
                            "Not accepting new proxy connection",
                        );
                    }

                    let mut skip_list = skip_list.clone();
                    skip_list.push(own_loc.peer);
                    if let Some(mut updated_state) = forward_conn(
                        *id,
                        &op_manager.ring,
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
                                    joiner.peer,
                                );
                                new_state = state;
                                return_msg = msg;
                            }
                            Some(other_state) => {
                                return Err(OpError::invalid_transition_with_state(
                                    self.id,
                                    Box::new(other_state),
                                ))
                            }
                            None => return Err(OpError::invalid_transition(self.id)),
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
                    tracing::debug!(
                        tx = %id,
                        at = %target.peer,
                        from = %sender.peer,
                        "Connect response received",
                    );

                    // Set the given location
                    let pk_loc = PeerKeyLocation {
                        location: Some(*your_location),
                        peer: *your_peer_id,
                    };

                    let Some(ConnectState::Connecting(ConnectionInfo { gateway, .. })) = self.state
                    else {
                        return Err(OpError::invalid_transition(self.id));
                    };
                    if !accepted_by.is_empty() {
                        tracing::debug!(
                            tx = %id,
                            at = %your_peer_id,
                            from = %sender.peer,
                            "Open connections acknowledged at requesting peer",
                        );
                        new_state = Some(ConnectState::OCReceived);
                        if accepted_by.contains(&gateway) {
                            // just send back a message in case the gw accepteds the connection
                            // otherwise skip sending anything back
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                msg: ConnectResponse::ReceivedOC {
                                    by_peer: pk_loc,
                                    gateway,
                                },
                                sender: pk_loc,
                                target: gateway,
                            });
                        } else {
                            return_msg = None;
                        }
                        tracing::debug!(
                            tx = %id,
                            at = %your_peer_id,
                            location = %your_location,
                            "Updating assigned location"
                        );
                        op_manager.ring.update_location(Some(*your_location));

                        for other_peer in accepted_by.iter().filter(|pl| pl.peer != target.peer) {
                            let _ = propagate_oc_to_responding_peers(
                                network_bridge,
                                op_manager,
                                gateway,
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
                        };
                        op_manager
                            .notify_op_change(NetMessage::Aborted(*id), OpEnum::Connect(op.into()))
                            .await?;
                        return Err(OpError::StatePushed);
                    }
                }
                ConnectMsg::Response {
                    id,
                    target,
                    sender,
                    msg:
                        ConnectResponse::Proxy {
                            accepted_by,
                            joiner,
                        },
                } => {
                    tracing::debug!(tx = %id, at = %target.peer, "Received proxy connect response");
                    match self.state {
                        Some(ConnectState::AwaitingProxyResponse {
                            accepted_by: mut previously_accepted,
                            new_peer_id,
                            target: original_target,
                            new_location,
                        }) => {
                            let own_loc = op_manager.ring.own_location();
                            let target_is_joiner = new_peer_id == original_target.peer;

                            previously_accepted.extend(accepted_by.iter().copied());
                            let is_accepted: bool = previously_accepted.contains(&own_loc);
                            if is_accepted {
                                new_state = Some(ConnectState::OCReceived);
                            } else {
                                new_state = None;
                            }

                            if target_is_joiner {
                                tracing::debug!(
                                    tx = %id,
                                    original_receiver = %target.peer,
                                    original_target = %original_target.peer,
                                    "Sending response to connect request with all the peers that accepted \
                                    connection from original target",
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
                                    at = %target.peer,
                                    to = %original_target.peer,
                                    "Sending response to connect request with all the peers that accepted \
                                    connection from proxy peer",
                                );

                                return_msg = Some(ConnectMsg::Response {
                                    id: *id,
                                    target: original_target,
                                    sender: *target,
                                    msg: ConnectResponse::Proxy {
                                        accepted_by: previously_accepted,
                                        joiner: *joiner,
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
                                    accepted_by: accepted_by.clone(),
                                    joiner: joiner.peer,
                                },
                            });
                            new_state = None;
                        }
                        Some(ConnectState::AwaitingNewConnection { query_target }) => {
                            let joiner = *target;
                            if accepted_by.is_empty() {
                                op_manager
                                    .ring
                                    .live_tx_tracker
                                    .missing_candidate_peers(query_target)
                                    .await;
                                return_msg = None;
                                new_state = None;
                            } else {
                                for peer in accepted_by {
                                    propagate_oc_to_responding_peers(
                                        network_bridge,
                                        op_manager,
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
                        }
                        Some(other_state) => {
                            return Err(OpError::invalid_transition_with_state(
                                self.id,
                                Box::new(other_state),
                            ))
                        }
                        None => return Err(OpError::invalid_transition(self.id)),
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
                                tracing::debug!(tx = %id, from = %by_peer.peer, at = %target.peer, "Acknowledge connected at gateway");
                                return_msg = Some(ConnectMsg::Connected {
                                    id: *id,
                                    sender: *target,
                                    target: *sender,
                                });
                                new_state = Some(ConnectState::Connected);
                            } else {
                                tracing::debug!(tx = %id, from = %by_peer.peer, at = %target.peer, "Acknowledge connected at peer");
                                return_msg = None;
                                new_state = None;
                            }
                        }
                        Some(other_state) => {
                            return Err(OpError::invalid_transition_with_state(
                                self.id,
                                Box::new(other_state),
                            ))
                        }
                        None => {
                            tracing::error!(tx = %self.id, at =  %target.peer, "completed");
                            return Err(OpError::invalid_transition(self.id));
                        }
                    }

                    network_bridge.add_connection(sender.peer).await?;
                    op_manager
                        .ring
                        .add_connection(
                            sender.location.ok_or(ConnectionError::LocationUnknown)?,
                            sender.peer,
                        )
                        .await;
                    tracing::debug!(tx = %id, from = %by_peer.peer, "Opened connection with peer");
                    if target != gateway {
                        new_state = None;
                    }
                }
                ConnectMsg::Connected { target, sender, id } => {
                    match self.state {
                        Some(ConnectState::OCReceived) => {
                            tracing::debug!(tx = %id, at = %target.peer, "Acknowledge connected at peer");
                            return_msg = None;
                        }
                        Some(other_state) => {
                            return Err(OpError::invalid_transition_with_state(
                                self.id,
                                Box::new(other_state),
                            ))
                        }
                        None => return Err(OpError::invalid_transition(self.id)),
                    };
                    tracing::info!(
                        tx = %id,
                        at = %target.peer,
                        assigned_location = ?op_manager.ring.own_location().location,
                        "Successfully completed connection",
                    );
                    network_bridge.add_connection(sender.peer).await?;
                    op_manager
                        .ring
                        .add_connection(
                            sender.location.ok_or(ConnectionError::LocationUnknown)?,
                            sender.peer,
                        )
                        .await;
                    new_state = None;
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
    backoff: Option<ExponentialBackoff>,
) -> Result<OperationResult, OpError> {
    let output_op = Some(ConnectOp {
        id,
        state,
        gateway,
        backoff,
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        state: output_op.map(|op| OpEnum::Connect(Box::new(op))),
    })
}

fn try_returning_proxy_connection(
    id: &Transaction,
    sender: &PeerKeyLocation,
    own_loc: &PeerKeyLocation,
    accepted_by: HashSet<PeerKeyLocation>,
    joiner: PeerId,
) -> (Option<ConnectState>, Option<ConnectMsg>) {
    let new_state = if accepted_by.contains(own_loc) {
        tracing::debug!(
            tx = %id,
            to = % sender.peer,
            proxy = %own_loc.peer,
            "Return message, connected at proxy",
        );
        Some(ConnectState::OCReceived)
    } else {
        tracing::debug!(tx = %id, proxy = % sender.peer, "Failed to connect at proxy");
        None
    };
    let return_msg = Some(ConnectMsg::Response {
        msg: ConnectResponse::Proxy {
            accepted_by,
            joiner,
        },
        sender: *own_loc,
        id: *id,
        target: *sender,
    });
    (new_state, return_msg)
}

async fn propagate_oc_to_responding_peers<NB: NetworkBridge>(
    network_bridge: &mut NB,
    op_manager: &OpManager,
    sender: PeerKeyLocation,
    other_peer: &PeerKeyLocation,
    msg: ConnectMsg,
) -> Result<(), OpError> {
    let id = msg.id();
    if op_manager.ring.should_accept(
        other_peer
            .location
            .ok_or(ConnectionError::LocationUnknown)?,
        &other_peer.peer,
    ) {
        tracing::info!(tx = %id, from = %sender.peer, to = %other_peer.peer, "Established connection");
        network_bridge.add_connection(other_peer.peer).await?;
        op_manager
            .ring
            .add_connection(
                other_peer
                    .location
                    .ok_or(ConnectionError::LocationUnknown)?,
                other_peer.peer,
            )
            .await;
        if other_peer.peer != sender.peer {
            // notify all the additional peers which accepted a request;
            // the gateway will be notified in the last message
            let _ = network_bridge.send(&other_peer.peer, msg.into()).await;
        }
    } else {
        tracing::debug!(tx = %id, from = %sender.peer, to = %other_peer.peer, "Not accepting connection to");
    }

    Ok(())
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
        new_peer_id: PeerId,
    },
    AwaitingConnectionAcquisition {
        joiner: PeerKeyLocation,
    },
    AwaitingNewConnection {
        query_target: PeerId,
    },
    OCReceived,
    Connected,
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    gateway: PeerKeyLocation,
    this_peer: PeerId,
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

/// # Arguments
///
/// - gateways: Inmutable list of known gateways. Passed when starting up the node.
/// After the initial connections through the gateways are established all other connections
/// (to gateways or regular peers) will be treated as regular connections.
///
/// - is_gateway: Whether this peer is a gateway or not.
pub(crate) async fn initial_join_procedure<CM>(
    op_manager: &OpManager,
    conn_manager: &mut CM,
    this_peer: PeerId,
    gateways: &[PeerKeyLocation],
) -> Result<(), OpError>
where
    CM: NetworkBridge + Send,
{
    use crate::util::IterExt;
    let number_of_parallel_connections = {
        let max_potential_conns_per_gw = op_manager.ring.max_hops_to_live;
        // e.g. 10 gateways and htl 5 -> only need 2 connections in parallel
        let needed_to_cover_max = gateways
            .iter()
            .filter(|conn| conn.peer != this_peer)
            .count()
            / max_potential_conns_per_gw;
        needed_to_cover_max.max(1)
    };
    tracing::info!(
        "Attempting to connect to {} gateways in parallel",
        number_of_parallel_connections
    );
    for gateway in gateways
        .iter()
        .shuffle()
        .filter(|conn| conn.peer != this_peer)
        .take(number_of_parallel_connections)
    {
        join_ring_request(None, this_peer, gateway, op_manager, conn_manager).await?;
    }
    Ok(())
}

pub(crate) async fn join_ring_request<CM>(
    backoff: Option<ExponentialBackoff>,
    peer_key: PeerId,
    gateway: &PeerKeyLocation,
    op_manager: &OpManager,
    conn_manager: &mut CM,
) -> Result<(), OpError>
where
    CM: NetworkBridge + Send,
{
    let tx_id = Transaction::new::<ConnectMsg>();
    let mut op = initial_request(peer_key, *gateway, op_manager.ring.max_hops_to_live, tx_id);
    if let Some(mut backoff) = backoff {
        // backoff to retry later in case it failed
        tracing::warn!("Performing a new join, attempt {}", backoff.retries() + 1);
        if backoff.sleep().await.is_none() {
            tracing::error!("Max number of retries reached");
            if op_manager.ring.open_connections() == 0 {
                // only consider this a complete failure if no connections were established at all
                // if connections where established the peer should incrementally acquire more over time
                return Err(OpError::MaxRetriesExceeded(tx_id, tx_id.transaction_type()));
            } else {
                return Ok(());
            }
        }
        // on first run the backoff will be initialized at the `initial_request` function
        // if the op was to fail and retried this function will be called with the previous backoff
        // passed as an argument and advanced
        op.backoff = Some(backoff);
    }
    connect_request(tx_id, op_manager, conn_manager, op).await?;
    Ok(())
}

fn initial_request(
    this_peer: PeerId,
    gateway: PeerKeyLocation,
    max_hops_to_live: usize,
    id: Transaction,
) -> ConnectOp {
    const MAX_JOIN_RETRIES: usize = usize::MAX;
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
    }
}

/// Join ring routine, called upon performing a join operation for this node.
async fn connect_request<NB>(
    tx: Transaction,
    op_manager: &OpManager,
    conn_bridge: &mut NB,
    join_op: ConnectOp,
) -> Result<(), OpError>
where
    NB: NetworkBridge,
{
    let ConnectOp {
        id, state, backoff, ..
    } = join_op;
    let ConnectionInfo {
        gateway,
        this_peer,
        max_hops_to_live,
    } = state.expect("infallible").try_unwrap_connecting()?;

    tracing::info!(
        tx = %id,
        %this_peer,
        gateway = %gateway,
        "Connecting to gateway",
    );

    conn_bridge.add_connection(gateway.peer).await?;
    let assigned_location = op_manager.ring.own_location().location;
    let join_req = NetMessage::from(messages::ConnectMsg::Request {
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
    op_manager
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
    skip_list: Vec<PeerId>,
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
        ring.random_peer(|p| !skip_list.contains(p))
    } else {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Selecting close peer to forward request",
        );
        // FIXME: target the `desired_location`
        let desired_location = joiner.location.unwrap();
        ring.routing(desired_location, Some(&req_peer.peer), skip_list.as_slice())
            .and_then(|pkl| (pkl.peer != joiner.peer).then_some(pkl))
    };

    if let Some(forward_to) = forward_to {
        let forwarded = NetMessage::from(ConnectMsg::Request {
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
            sender = %req_peer.peer,
            forward_target = %forward_to.peer,
            "Forwarding connect request from sender to other peer",
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

        fn requested_location(&self) -> Option<Location> {
            self.target().and_then(|pkloc| pkloc.location)
        }
    }

    impl ConnectMsg {
        pub fn sender(&self) -> Option<&PeerId> {
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
            joiner: PeerId,
            assigned_location: Option<Location>,
            hops_to_live: usize,
            max_hops_to_live: usize,
        },
        /// Query target should find a good candidate for joiner to join.
        FindOptimalPeer {
            /// Peer whom you are querying new connection about.
            query_target: PeerKeyLocation,
            /// The ideal location of the peer to which you would connect.
            ideal_location: Location,
            joiner: PeerKeyLocation,
            max_hops_to_live: usize,
        },
        Proxy {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
            skip_list: Vec<PeerId>,
            accepted_by: HashSet<PeerKeyLocation>,
        },
        ReceivedOC,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectResponse {
        AcceptedBy {
            peers: HashSet<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerId,
        },
        ReceivedOC {
            by_peer: PeerKeyLocation,
            gateway: PeerKeyLocation,
        },
        Proxy {
            accepted_by: HashSet<PeerKeyLocation>,
            joiner: PeerId,
        },
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::node::testing_impl::SimNetwork;

    /// Given a network of one node and one gateway test that both are connected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_node_connects_to_gw() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 1usize;
        const NUM_GW: usize = 1usize;
        const MAX_HTL: usize = 1usize;
        const RAND_IF_HTL_ABOVE: usize = 1usize;
        const MAX_CONNS: usize = 1usize;
        const MIN_CONNS: usize = 1usize;
        let mut sim_nw = SimNetwork::new(
            "join_one_node_connects_to_gw",
            NUM_NODES,
            NUM_GW,
            MAX_HTL,
            RAND_IF_HTL_ABOVE,
            MAX_CONNS,
            MIN_CONNS,
        )
        .await;
        sim_nw.start().await;
        sim_nw.check_connectivity(Duration::from_secs(1))?;
        assert!(sim_nw.connected(&"node-1".into()));
        Ok(())
    }

    /// Once a gateway is left without remaining open slots, ensure forwarding connects
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_connection_to_node() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 3usize;
        const NUM_GW: usize = 1usize;
        const MAX_HTL: usize = 2usize;
        const RAND_IF_HTL_ABOVE: usize = 1usize;
        const MAX_CONNS: usize = 2usize;
        const MIN_CONNS: usize = 1usize;
        let mut sim_nw = SimNetwork::new(
            "join_forward_connection_to_node",
            NUM_GW,
            NUM_NODES,
            MAX_HTL,
            RAND_IF_HTL_ABOVE,
            MAX_CONNS,
            MIN_CONNS,
        )
        .await;
        // sim_nw.with_start_backoff(Duration::from_millis(100));
        sim_nw.start().await;
        sim_nw.check_connectivity(Duration::from_secs(3))?;
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
    #[tokio::test(flavor = "multi_thread")]
    async fn network_should_achieve_good_connectivity() -> Result<(), anyhow::Error> {
        // crate::config::set_logger();
        const NUM_NODES: usize = 10usize;
        const NUM_GW: usize = 2usize;
        const MAX_HTL: usize = 5usize;
        const RAND_IF_HTL_ABOVE: usize = 3usize;
        const MAX_CONNS: usize = 4usize;
        const MIN_CONNS: usize = 2usize;
        let mut sim_nw = SimNetwork::new(
            "join_all_nodes_should_connect",
            NUM_GW,
            NUM_NODES,
            MAX_HTL,
            RAND_IF_HTL_ABOVE,
            MAX_CONNS,
            MIN_CONNS,
        )
        .await;
        sim_nw.start().await;
        sim_nw.check_connectivity(Duration::from_secs(10))?;
        // wait for a bit so peers can acquire more connections
        tokio::time::sleep(Duration::from_secs(3)).await;
        sim_nw.network_connectivity_quality()?;
        sim_nw.print_network_connections();
        sim_nw.print_ring_distribution();
        Ok(())
    }
}
