use std::{collections::HashSet, time::Duration};

use super::{handle_op_result, state_machine::StateMachineImpl, OpError, OperationResult};
use crate::{
    config::PEER_TIMEOUT,
    conn_manager::{self, ConnectionBridge, PeerKey},
    message::{Message, Transaction},
    node::OpManager,
    operations::{state_machine::StateMachine, Operation},
    ring::{Location, PeerKeyLocation, Ring},
    utils::ExponentialBackoff,
};

pub(crate) use self::messages::{JoinRequest, JoinResponse, JoinRingMsg};

const MAX_JOIN_RETRIES: usize = 10;

pub(crate) struct JoinRingOp {
    sm: StateMachine<JROpSm>,
    /// keeps track of the number of retries and applies an exponential backoff cooldown period
    pub backoff: Option<ExponentialBackoff>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl JoinRingOp {
    pub fn initial_request(
        this_peer: PeerKey,
        gateway: PeerKeyLocation,
        max_hops_to_live: usize,
        id: Transaction,
    ) -> Self {
        log::debug!("Connecting to gw {} from {}", gateway.peer, this_peer);
        let sm = StateMachine::from_state(
            JRState::Connecting(ConnectionInfo {
                gateway,
                this_peer,
                max_hops_to_live,
            }),
            id,
        );
        JoinRingOp {
            sm,
            backoff: Some(ExponentialBackoff::new(
                Duration::from_secs(1),
                Duration::from_secs(120),
                MAX_JOIN_RETRIES,
            )),
            _ttl: PEER_TIMEOUT,
        }
    }
}

#[derive(Debug)]
struct JROpSm;

impl StateMachineImpl for JROpSm {
    type Input = JoinRingMsg;

    type State = JRState;

    type Output = JoinRingMsg;

    fn state_transition(state: &mut Self::State, input: &mut Self::Input) -> Option<Self::State> {
        match (state, input) {
            // initial gateway states
            (
                JRState::Initializing,
                JoinRingMsg::Request {
                    msg:
                        JoinRequest::Accepted {
                            gateway,
                            your_peer_id,
                            accepted_by,
                            ..
                        },
                    ..
                },
            ) if !accepted_by.is_empty() => {
                log::debug!(
                    "OC received at gateway {} from requesting peer {}",
                    gateway.peer,
                    your_peer_id
                );
                Some(JRState::OCReceived)
            }
            (
                JRState::OCReceived,
                JoinRingMsg::Response {
                    msg: JoinResponse::ReceivedOC { .. },
                    ..
                },
            ) => {
                log::debug!("Acknowledge connected at gateway");
                Some(JRState::Connected)
            }
            // requester states
            (
                JRState::Connecting(ConnectionInfo { gateway, .. }),
                JoinRingMsg::Response {
                    msg:
                        JoinResponse::AcceptedBy {
                            your_peer_id,
                            peers,
                            ..
                        },
                    ..
                },
            ) if !peers.is_empty() => {
                log::debug!(
                    "OC received at requesting peer {} from gateway {}",
                    your_peer_id,
                    gateway.peer
                );
                Some(JRState::OCReceived)
            }
            (JRState::OCReceived, JoinRingMsg::Connected { .. }) => {
                log::debug!("Acknowledge connected at peer");
                Some(JRState::Connected)
            }
            // proxies state
            (
                JRState::Initializing,
                JoinRingMsg::Response {
                    msg: JoinResponse::Proxy { accepted_by },
                    sender,
                    ..
                },
            ) => {
                if accepted_by.contains(sender) {
                    Some(JRState::Connected)
                } else {
                    None
                }
            }
            // interism status (proxy + gw)
            (
                JRState::AwaitingProxyResponse {
                    accepted_by: previously_accepted,
                    ..
                },
                JoinRingMsg::Response {
                    msg: JoinResponse::Proxy { accepted_by },
                    ..
                },
            ) if !accepted_by.is_empty() => {
                previously_accepted.extend(accepted_by.drain());
                Some(JRState::Connected)
            }
            _ => None,
        }
    }

    fn output_from_input(state: Self::State, input: Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (
                JRState::Initializing,
                JoinRingMsg::Request {
                    id,
                    msg:
                        JoinRequest::Accepted {
                            gateway,
                            accepted_by,
                            your_location,
                            your_peer_id,
                        },
                },
            ) => Some(JoinRingMsg::Response {
                id,
                sender: gateway,
                msg: JoinResponse::AcceptedBy {
                    peers: accepted_by,
                    your_location,
                    your_peer_id,
                },
                target: PeerKeyLocation {
                    peer: your_peer_id,
                    location: Some(your_location),
                },
            }),
            (
                JRState::Connecting(ConnectionInfo { .. }),
                JoinRingMsg::Response {
                    id,
                    msg:
                        JoinResponse::AcceptedBy {
                            your_location,
                            your_peer_id,
                            peers,
                        },
                    sender: prev_sender,
                    ..
                },
            ) if !peers.is_empty() => {
                log::debug!(
                    "Acknowledge OC at init peer {} from gw {}",
                    your_peer_id,
                    prev_sender.peer
                );
                let sender = PeerKeyLocation {
                    peer: your_peer_id,
                    location: Some(your_location),
                };
                Some(JoinRingMsg::Response {
                    id,
                    msg: JoinResponse::ReceivedOC { by_peer: sender },
                    sender,
                    target: prev_sender,
                })
            }
            (
                JRState::AwaitingProxyResponse {
                    target,
                    accepted_by,
                    new_location,
                    identifier,
                },
                JoinRingMsg::Response {
                    id,
                    msg: JoinResponse::Proxy { .. },
                    target: this_peer,
                    ..
                },
            ) => {
                // returning message from a proxy action to the previous requester
                let resp = if identifier == target.peer {
                    log::debug!(
                        "Sending response to join request with all the peers that accepted \
                    connection from gateway {} to peer {}",
                        this_peer.peer,
                        target.peer
                    );
                    JoinRingMsg::Response {
                        id,
                        target,
                        sender: this_peer,
                        msg: JoinResponse::AcceptedBy {
                            peers: accepted_by,
                            your_location: new_location,
                            your_peer_id: identifier,
                        },
                    }
                } else {
                    log::debug!(
                        "Sending response to join request with all the peers that accepted \
                        connection from proxy peer {} to proxy peer {}",
                        this_peer.peer,
                        target.peer
                    );
                    JoinRingMsg::Response {
                        id,
                        target,
                        sender: this_peer,
                        msg: JoinResponse::Proxy { accepted_by },
                    }
                };
                Some(resp)
            }
            // proxies state
            (
                JRState::Initializing,
                JoinRingMsg::Response {
                    msg: JoinResponse::Proxy { accepted_by },
                    sender,
                    id,
                    target,
                },
            ) => {
                log::debug!(
                    "Returning from a proxy to the previous requester. Sender: {}; target: {}",
                    sender.peer,
                    target.peer
                );
                Some(JoinRingMsg::Response {
                    msg: JoinResponse::Proxy { accepted_by },
                    sender,
                    id,
                    target,
                })
            }
            (
                JRState::OCReceived,
                JoinRingMsg::Response {
                    msg: JoinResponse::ReceivedOC { .. },
                    id,
                    target,
                    sender,
                },
            ) => Some(JoinRingMsg::Connected {
                id,
                sender: target,
                target: sender,
            }),
            _ => None,
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
        identifier: PeerKey,
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
    fn try_unwrap_connecting<CErr>(self) -> Result<ConnectionInfo, OpError<CErr>>
    where
        CErr: std::error::Error,
    {
        if let Self::Connecting(conn_info) = self {
            Ok(conn_info)
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }

    fn is_connected(&self) -> bool {
        matches!(self, JRState::Connected { .. })
    }

    fn add_new_proxy<CErr>(
        &mut self,
        proxies: impl IntoIterator<Item = PeerKeyLocation>,
    ) -> Result<(), OpError<CErr>>
    where
        CErr: std::error::Error,
    {
        if let Self::AwaitingProxyResponse { accepted_by, .. } = self {
            accepted_by.extend(proxies.into_iter());
            Ok(())
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// Join ring routine, called upon performing a join operation for this node.
pub(crate) async fn join_ring_request<CB, CErr>(
    tx: Transaction,
    op_storage: &OpManager<CErr>,
    conn_manager: &mut CB,
    mut join_op: JoinRingOp,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    CErr: std::error::Error,
{
    let ConnectionInfo {
        gateway,
        this_peer,
        max_hops_to_live,
    } = join_op.sm.state().clone().try_unwrap_connecting()?;

    log::info!(
        "Joining ring via {} (at {})",
        gateway.peer,
        gateway
            .location
            .ok_or(conn_manager::ConnError::LocationUnknown)?
    );

    conn_manager.add_connection(gateway, true);
    let join_req = Message::from(messages::JoinRingMsg::Request {
        id: tx,
        msg: messages::JoinRequest::StartReq {
            target_loc: gateway,
            req_peer: this_peer,
            hops_to_live: max_hops_to_live,
            max_hops_to_live,
        },
    });
    conn_manager.send(gateway.peer, join_req).await?;
    op_storage.push(tx, Operation::JoinRing(join_op))?;
    Ok(())
}

/// Join ring routine, called upon processing a request to join or while performing
/// a join operation for this node after initial request (see [`join_ring_request`]).
///
/// # Cancellation Safety
/// This future is not cancellation safe.
pub(crate) async fn handle_join_ring<CB, CErr>(
    op_storage: &OpManager<CErr>,
    conn_manager: &mut CB,
    join_op: JoinRingMsg,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    CErr: std::error::Error,
{
    let sender;
    let tx = *join_op.id();
    let result: Result<_, OpError<CErr>> = match op_storage.pop(join_op.id()) {
        Some(Operation::JoinRing(state)) => {
            sender = join_op.sender().cloned();
            // was an existing operation, the other peer messaged back
            update_state(conn_manager, state, join_op, &op_storage.ring).await
        }
        Some(_) => return Err(OpError::TxUpdateFailure(tx)),
        None => {
            sender = join_op.sender().cloned();
            // new request to join this node, initialize the machine
            let machine = JoinRingOp {
                sm: StateMachine::from_state(JRState::Initializing, tx),
                backoff: None,
                _ttl: PEER_TIMEOUT,
            };
            update_state(conn_manager, machine, join_op, &op_storage.ring).await
        }
    };

    handle_op_result(
        op_storage,
        conn_manager,
        result.map_err(|err| (err, tx)),
        sender,
    )
    .await
}

async fn update_state<CB, CErr>(
    conn_manager: &mut CB,
    mut state: JoinRingOp,
    other_host_msg: JoinRingMsg,
    ring: &Ring,
) -> Result<OperationResult, OpError<CErr>>
where
    CB: ConnectionBridge,
    CErr: std::error::Error,
{
    let return_msg;
    let new_state;
    match other_host_msg {
        JoinRingMsg::Request {
            id,
            msg:
                JoinRequest::StartReq {
                    target_loc: this_node_loc,
                    req_peer,
                    hops_to_live,
                    ..
                },
        } => {
            // likely a gateway which accepts connections
            log::debug!(
                "Initial join request received from {} with HTL {} @ {}",
                req_peer,
                hops_to_live,
                this_node_loc.peer
            );
            let new_location = Location::random();
            let accepted_by = if ring.should_accept(&new_location) {
                log::debug!("Accepting connections from {}", req_peer,);
                HashSet::from_iter([this_node_loc])
            } else {
                log::debug!("Rejecting connection from peer {}", req_peer);
                HashSet::new()
            };

            let new_peer_loc = PeerKeyLocation {
                location: Some(new_location),
                peer: req_peer,
            };
            if let Some(mut updated_state) = forward_conn(
                id,
                ring,
                conn_manager,
                new_peer_loc,
                new_peer_loc,
                hops_to_live,
                accepted_by.len(),
            )
            .await?
            {
                updated_state.add_new_proxy(accepted_by)?;
                // awaiting responses from proxies
                *state.sm.state() = updated_state;
                new_state = Some(state);
                return_msg = None;
            } else {
                return_msg = state
                    .sm
                    .consume_to_output(JoinRingMsg::Request {
                        id,
                        msg: JoinRequest::Accepted {
                            gateway: this_node_loc,
                            accepted_by,
                            your_location: new_location,
                            your_peer_id: req_peer,
                        },
                    })
                    .map_err(|_: OpError<CErr>| OpError::TxUpdateFailure(id))?
                    .map(Message::from);
                new_state = Some(state);
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
            let own_loc = ring.own_location();
            let accepted_by =
                if ring.should_accept(&joiner.location.ok_or(OpError::TxUpdateFailure(id))?) {
                    log::debug!("Accepting proxy connections from {}", joiner.peer);
                    HashSet::from_iter([own_loc])
                } else {
                    log::debug!(
                        "Not accepting new proxy connection for sender {}",
                        joiner.peer
                    );
                    HashSet::new()
                };

            if let Some(mut updated_state) = forward_conn(
                id,
                ring,
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
                *state.sm.state() = updated_state;
                new_state = Some(state);
                return_msg = None;
            } else {
                return_msg = state
                    .sm
                    .consume_to_output(JoinRingMsg::Response {
                        id,
                        sender: own_loc,
                        target: sender,
                        msg: JoinResponse::Proxy { accepted_by },
                    })
                    .map_err(|_: OpError<CErr>| OpError::TxUpdateFailure(id))?
                    .map(Message::from);
                if state.sm.state().is_connected() {
                    new_state = None;
                } else {
                    new_state = Some(state);
                }
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
            target,
        } => {
            log::debug!("Join response received from {}", sender.peer,);
            return_msg = state
                .sm
                .consume_to_output(JoinRingMsg::Response {
                    id,
                    sender,
                    msg: JoinResponse::AcceptedBy {
                        peers: accepted_by.clone(),
                        your_location,
                        your_peer_id,
                    },
                    target,
                })?
                .map(Message::from);
            let pkloc = PeerKeyLocation {
                location: Some(your_location),
                peer: your_peer_id,
            };
            ring.update_location(Some(your_location));
            for other_peer in accepted_by {
                if ring.should_accept(
                    &other_peer
                        .location
                        .ok_or(conn_manager::ConnError::LocationUnknown)?,
                ) {
                    log::info!("Established connection to {}", other_peer.peer);
                    conn_manager.add_connection(other_peer, false);
                    ring.add_connection(
                        other_peer
                            .location
                            .ok_or(conn_manager::ConnError::LocationUnknown)?,
                        other_peer.peer,
                    );
                    if other_peer.peer != sender.peer {
                        // notify all the additional peers which accepted a request;
                        // the gateway will be notified in the last message
                        let _ = conn_manager
                            .send(
                                other_peer.peer,
                                JoinRingMsg::Response {
                                    id,
                                    target: other_peer,
                                    sender: pkloc,
                                    msg: JoinResponse::ReceivedOC { by_peer: pkloc },
                                }
                                .into(),
                            )
                            .await;
                    }
                } else {
                    log::warn!("Not accepting connection to {}", other_peer.peer);
                }
            }
            ring.update_location(Some(your_location));
            new_state = Some(state);
        }
        JoinRingMsg::Response {
            id,
            sender,
            target,
            msg: JoinResponse::Proxy { accepted_by },
        } => {
            return_msg = state
                .sm
                .consume_to_output(JoinRingMsg::Response {
                    id,
                    sender,
                    target,
                    msg: JoinResponse::Proxy { accepted_by },
                })?
                .map(Message::from);
            if state.sm.state().is_connected() {
                new_state = None;
            } else {
                // keep waiting for more responses
                // right now should't happen since is only forwarded to one peer at each hop
                new_state = Some(state);
            }
        }
        JoinRingMsg::Response {
            id,
            sender,
            msg: JoinResponse::ReceivedOC { by_peer },
            target,
        } => {
            return_msg = state
                .sm
                .consume_to_output(JoinRingMsg::Response {
                    id,
                    sender,
                    msg: JoinResponse::ReceivedOC { by_peer },
                    target,
                })?
                .map(Message::from);
            if !state.sm.state().is_connected() {
                return Err(OpError::InvalidStateTransition(id));
            } else {
                conn_manager.add_connection(sender, false);
                ring.add_connection(
                    sender
                        .location
                        .ok_or(conn_manager::ConnError::LocationUnknown)?,
                    sender.peer,
                );
                log::debug!("Openned connection with peer {}", by_peer.peer);
                new_state = None;
            }
        }
        JoinRingMsg::Connected { target, sender, id } => {
            return_msg = state
                .sm
                .consume_to_output(JoinRingMsg::Connected { target, sender, id })?
                .map(Message::from);
            if !state.sm.state().is_connected() {
                return Err(OpError::InvalidStateTransition(id));
            } else {
                log::info!(
                    "Successfully completed connection @ {}, new location = {:?}",
                    target.peer,
                    ring.own_location().location
                );
                conn_manager.add_connection(sender, false);
                ring.add_connection(
                    sender
                        .location
                        .ok_or(conn_manager::ConnError::LocationUnknown)?,
                    sender.peer,
                );
                new_state = None;
            }
        }
        _ => return Err(OpError::UnexpectedOpState),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::JoinRing),
    })
}

#[allow(clippy::too_many_arguments)]
async fn forward_conn<CM, Err>(
    id: Transaction,
    ring: &Ring,
    conn_manager: &mut CM,
    req_peer: PeerKeyLocation,
    new_peer_loc: PeerKeyLocation,
    left_htl: usize,
    num_accepted: usize,
) -> Result<Option<JRState>, OpError<Err>>
where
    CM: ConnectionBridge,
    Err: std::error::Error,
{
    if left_htl == 0 || (ring.num_connections() == 0 && num_accepted == 0) {
        return Ok(None);
    }

    let forward_to = if left_htl >= ring.rnd_if_htl_above {
        log::debug!(
            "Randomly selecting peer to forward JoinRequest, sender: {}",
            req_peer.peer
        );
        ring.random_peer(|p| p.peer != req_peer.peer)
    } else {
        log::debug!(
            "Selecting close peer to forward request, sender: {}",
            req_peer.peer
        );
        ring.routing(&new_peer_loc.location.unwrap(), 1, &[]).pop()
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
        log::debug!(
            "Forwarding JoinRequest from sender {} to {}",
            req_peer.peer,
            forward_to.peer
        );
        conn_manager.send(forward_to.peer, forwarded).await?;
        // awaiting for responses from forward nodes
        let new_state = JRState::AwaitingProxyResponse {
            target: req_peer,
            accepted_by: HashSet::new(),
            new_location: new_peer_loc.location.unwrap(),
            identifier: new_peer_loc.peer,
        };
        Ok(Some(new_state))
    } else {
        if num_accepted != 0 {
            log::warn!(
                "Tx {}: unable to forward, will only be connected to one peer",
                id
            );
        } else {
            log::warn!("Tx {}: unable to forward or accept any connections", id);
        }
        Ok(None)
    }
}

mod messages {
    use std::fmt::Display;

    use super::*;
    use crate::ring::{Location, PeerKeyLocation};

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

    impl JoinRingMsg {
        pub fn id(&self) -> &Transaction {
            use JoinRingMsg::*;
            match self {
                Request { id, .. } => id,
                Response { id, .. } => id,
                Connected { id, .. } => id,
            }
        }

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
                Connected { target, .. } => Some(target),
                _ => None,
            }
        }
    }

    impl Display for JoinRingMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    msg: JoinRequest::StartReq { .. },
                    ..
                } => write!(f, "StartRequest(id: {})", id),
                Self::Request {
                    msg: JoinRequest::Accepted { .. },
                    ..
                } => write!(f, "RequestAccepted(id: {})", id),
                Self::Request {
                    msg: JoinRequest::Proxy { .. },
                    ..
                } => write!(f, "ProxyRequest(id: {})", id),
                Self::Response {
                    msg: JoinResponse::AcceptedBy { .. },
                    ..
                } => write!(f, "RouteValue(id: {})", id),
                Self::Response {
                    msg: JoinResponse::ReceivedOC { .. },
                    ..
                } => write!(f, "RouteValue(id: {})", id),
                Self::Response {
                    msg: JoinResponse::Proxy { .. },
                    ..
                } => write!(f, "RouteValue(id: {})", id),
                Self::Connected { .. } => write!(f, "Connected(id: {})", id),
                _ => todo!(),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum JoinRequest {
        StartReq {
            target_loc: PeerKeyLocation,
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
    use std::time::{Duration, Instant};

    use super::*;
    use crate::{
        message::TxType,
        node::{test_utils::SimNetwork, SimStorageError},
    };

    #[test]
    fn succesful_join_ring_seq() {
        let peer = PeerKey::random();
        let id = Transaction::new(<JoinRingMsg as TxType>::tx_type_id(), &peer);
        let new_loc = Location::random();
        let mut new_peer = PeerKeyLocation {
            peer,
            location: Some(new_loc),
        };
        let gateway = PeerKeyLocation {
            peer: PeerKey::random(),
            location: Some(Location::random()),
        };

        let mut join_gw_1 = JoinRingOp::initial_request(new_peer.peer, gateway, 0, id).sm;
        let mut join_new_peer_2 = StateMachine::<JROpSm>::from_state(JRState::Initializing, id);

        let req = JoinRingMsg::Request {
            id,
            msg: JoinRequest::Accepted {
                gateway,
                accepted_by: HashSet::from_iter([gateway]),
                your_location: new_loc,
                your_peer_id: new_peer.peer,
            },
        };
        let res = join_new_peer_2
            .consume_to_output::<SimStorageError>(req)
            .unwrap()
            .unwrap();
        let expected = JoinRingMsg::Response {
            id,
            msg: JoinResponse::AcceptedBy {
                peers: HashSet::from_iter([gateway]),
                your_location: new_loc,
                your_peer_id: new_peer.peer,
            },
            target: new_peer,
            sender: gateway,
        };
        assert_eq!(res, expected);
        assert!(matches!(join_new_peer_2.state(), JRState::OCReceived));

        let res = join_gw_1
            .consume_to_output::<SimStorageError>(res)
            .unwrap()
            .unwrap();
        new_peer.location = Some(new_loc);
        let expected = JoinRingMsg::Response {
            msg: JoinResponse::ReceivedOC { by_peer: new_peer },
            id,
            sender: new_peer,
            target: gateway,
        };
        assert_eq!(res, expected);
        assert!(matches!(join_gw_1.state(), JRState::OCReceived));

        let res = join_new_peer_2
            .consume_to_output::<SimStorageError>(res)
            .unwrap()
            .unwrap();
        let expected = JoinRingMsg::Connected {
            id,
            target: new_peer,
            sender: gateway,
        };
        assert_eq!(res, expected);
        assert!(matches!(join_new_peer_2.state(), JRState::Connected { .. }));

        assert!(join_gw_1
            .consume_to_output::<SimStorageError>(res.clone())
            .unwrap()
            .is_none());
        assert!(matches!(join_gw_1.state(), JRState::Connected { .. }));

        // transaction finished, should not return anymore
        assert!(join_new_peer_2
            .consume_to_output::<SimStorageError>(res.clone())
            .is_err());
        assert!(join_gw_1.consume_to_output::<SimStorageError>(res).is_err());
    }

    /// Given a network of one node and one gateway test that both are connected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_node_connects_to_gw() {
        let mut sim_net = SimNetwork::new(1, 1, 1, 1, 2, 2);
        sim_net.build().await;
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert!(sim_net.connected("node-0"));
    }

    /// Given a network of 100 peers all nodes should have connections.
    #[tokio::test(flavor = "multi_thread")]
    async fn all_nodes_should_connect() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 998usize;
        const NUM_GW: usize = 2usize;
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 5, 4, 10, 2);
        sim_nodes.build().await;

        let mut connected = HashSet::new();
        let elapsed = Instant::now();
        while elapsed.elapsed() < Duration::from_secs(20) && connected.len() < NUM_NODES {
            for node in 0..NUM_NODES {
                if !connected.contains(&node) && sim_nodes.connected(&format!("node-{}", node)) {
                    connected.insert(node);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        let expected = HashSet::from_iter(0..NUM_NODES);
        let diff: Vec<_> = expected.difference(&connected).collect();
        if !diff.is_empty() {
            log::error!("Nodes without connection: {:?}", diff);
        }
        assert!(diff.is_empty());
        log::info!(
            "Required time for connecting all peers: {} secs",
            elapsed.elapsed().as_secs()
        );

        let hist: Vec<_> = sim_nodes.ring_distribution(1).collect();
        log::info!("Ring distribution: {:?}", hist);

        let node_connectivity = sim_nodes.node_connectivity();
        let mut connections_per_peer: Vec<_> = node_connectivity
            .iter()
            .map(|(k, v)| (k, v.len()))
            .filter_map(|(k, v)| {
                if !k.starts_with("gateway") {
                    Some(v)
                } else {
                    None
                }
            })
            .collect();

        // ensure at least some normal nodes have more than one connection
        connections_per_peer.sort_unstable_by_key(|num_conn| *num_conn);
        assert!(connections_per_peer.iter().last().unwrap() > &1);

        // ensure the average number of connections per peer is above N
        let avg_connections: usize = connections_per_peer.iter().sum::<usize>() / NUM_NODES;
        log::info!("Average connections: {}", avg_connections);
        assert!(avg_connections > 1);

        Ok(())
    }
}
