use std::{collections::HashSet, time::Duration};

use super::{handle_op_result, state_machine::StateMachineImpl, OpError, OperationResult};
use crate::{
    config::PEER_TIMEOUT,
    conn_manager::{self, ConnectionBridge, PeerKey},
    message::{Message, Transaction},
    node::OpManager,
    operations::{state_machine::StateMachine, Operation},
    ring::{Location, PeerKeyLocation, Ring},
};

pub(crate) use self::messages::{JoinRequest, JoinResponse, JoinRingMsg};

pub(crate) struct JoinRingOp {
    sm: StateMachine<JROpSm>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl JoinRingOp {
    pub fn initial_request(
        this_peer: PeerKey,
        gateway: PeerKeyLocation,
        max_hops_to_live: usize,
    ) -> Self {
        log::debug!("Connecting to gw {} from {}", gateway.peer, this_peer);
        let sm = StateMachine::from_state(JRState::Connecting(ConnectionInfo {
            gateway,
            this_peer,
            max_hops_to_live,
        }));
        JoinRingOp {
            sm,
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
            (
                JRState::Initializing,
                JoinRingMsg::Request {
                    msg:
                        JoinRequest::Accepted {
                            gateway,
                            your_peer_id,
                            ..
                        },
                    ..
                },
            ) => {
                log::debug!(
                    "OC received at gw {} from peer {}",
                    gateway.peer,
                    your_peer_id
                );
                Some(JRState::OCReceived)
            }
            (
                JRState::Connecting(ConnectionInfo { gateway, .. }),
                JoinRingMsg::Response {
                    msg: JoinResponse::AcceptedBy { your_peer_id, .. },
                    ..
                },
            ) => {
                log::debug!(
                    "OC received at init peer {} from gw {}",
                    your_peer_id,
                    gateway.peer
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
                log::debug!("Ack connected at gateway");
                Some(JRState::Connected)
            }
            (JRState::OCReceived, JoinRingMsg::Connected { .. }) => {
                log::debug!("Ack connected at peer");
                Some(JRState::Connected)
            }
            (
                JRState::AwaitingProxyResponse {
                    accepted_by: previously_accepted,
                    ..
                },
                JoinRingMsg::Response {
                    msg: JoinResponse::Proxy { accepted_by },
                    ..
                },
            ) => {
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
                            ..
                        },
                    sender: prev_sender,
                    ..
                },
            ) => {
                log::debug!(
                    "Ack OC at init peer {} from gw {}",
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
                        connection so far from proxy {} to proxy {}",
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
    fn try_unwrap_connecting<CErr: std::error::Error>(
        self,
    ) -> Result<ConnectionInfo, OpError<CErr>> {
        if let Self::Connecting(conn_info) = self {
            Ok(conn_info)
        } else {
            Err(OpError::InvalidStateTransition)
        }
    }

    fn is_connected(&self) -> bool {
        matches!(self, JRState::Connected { .. })
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
    conn_manager.send(gateway, join_req).await?;
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
            // new request to join from this node, initialize the machine
            let machine = JoinRingOp {
                sm: StateMachine::from_state(JRState::Initializing),
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
                log::debug!("Not accepting new connection for sender {}", req_peer);
                HashSet::new()
            };

            log::debug!(
                "Sending join response to {} accepting {} connections",
                req_peer,
                accepted_by.len()
            );

            let new_peer_loc = PeerKeyLocation {
                location: Some(new_location),
                peer: req_peer,
            };

            let accepted_by_this = || -> JoinRingMsg {
                JoinRingMsg::Request {
                    id,
                    msg: JoinRequest::Accepted {
                        gateway: this_node_loc,
                        accepted_by: accepted_by.clone(),
                        your_location: new_location,
                        your_peer_id: req_peer,
                    },
                }
            };

            let (state, msg) = forward_conn(
                id,
                state,
                ring,
                conn_manager,
                new_peer_loc,
                new_peer_loc,
                hops_to_live,
                accepted_by_this,
            )
            .await?;
            new_state = Some(state);
            return_msg = msg;
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
                    log::debug!("Accepting connections from {}", joiner.peer);
                    HashSet::from_iter([own_loc])
                } else {
                    log::debug!("Not accepting new connection for sender {}", joiner.peer);
                    HashSet::new()
                };

            let accepted_by_this = || -> JoinRingMsg {
                JoinRingMsg::Response {
                    id,
                    sender,
                    target: sender,
                    msg: JoinResponse::Proxy { accepted_by },
                }
            };

            let (state, msg) = forward_conn(
                id,
                state,
                ring,
                conn_manager,
                sender,
                joiner,
                hops_to_live,
                accepted_by_this,
            )
            .await?;
            new_state = Some(state);
            return_msg = msg;
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
                                other_peer,
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
        // JoinRing
        JoinRingMsg::Response {
            id,
            sender,
            target,
            msg: JoinResponse::Proxy { accepted_by },
        } => {
            return_msg = state
                .sm
                .consume_to_state(JoinRingMsg::Response {
                    id,
                    sender,
                    target,
                    msg: JoinResponse::Proxy { accepted_by },
                })?
                .map(Message::from);
            let completed_conn = state.sm.state().is_connected();
            if completed_conn {
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
                return Err(OpError::InvalidStateTransition);
            } else {
                log::debug!("Successfully completed connection to peer {}", by_peer.peer);
                new_state = None;
            }
        }
        JoinRingMsg::Connected { target, sender, id } => {
            return_msg = state
                .sm
                .consume_to_output(JoinRingMsg::Connected { target, sender, id })?
                .map(Message::from);
            if !state.sm.state().is_connected() {
                return Err(OpError::InvalidStateTransition);
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
        _ => return Err(OpError::InvalidStateTransition),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::JoinRing),
    })
}

#[allow(clippy::too_many_arguments)]
async fn forward_conn<F, CM, Err>(
    id: Transaction,
    mut state: JoinRingOp,
    ring: &Ring,
    conn_manager: &mut CM,
    req_peer: PeerKeyLocation,
    new_peer_loc: PeerKeyLocation,
    left_htl: usize,
    accepted_once: F,
) -> Result<(JoinRingOp, Option<Message>), OpError<Err>>
where
    CM: ConnectionBridge,
    Err: std::error::Error,
    F: FnOnce() -> JoinRingMsg,
{
    if left_htl > 0 && ring.num_connections() > 0 {
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
        let forwarded = if let Some(forward_to) = forward_to {
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
            conn_manager.send(forward_to, forwarded).await?;
            true
        } else {
            false
        };

        if forwarded {
            // awaiting for responses from forward nodes
            let accepted_by = HashSet::from_iter([ring.own_location()]);
            let curr_state = state.sm.state();
            *curr_state = JRState::AwaitingProxyResponse {
                target: req_peer,
                accepted_by,
                new_location: new_peer_loc.location.unwrap(),
                identifier: new_peer_loc.peer,
            };
            Ok((state, None))
        } else {
            log::warn!("Unable to forward, will only be connected to one peer");
            let return_msg = state
                .sm
                .consume_to_output(accepted_once())?
                .map(Message::from);
            Ok((state, return_msg))
        }
    } else {
        let return_msg = state
            .sm
            .consume_to_output(accepted_once())?
            .map(Message::from);
        Ok((state, return_msg))
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

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            use JoinRingMsg::*;
            match self {
                Response { sender, .. } => Some(sender),
                Connected { sender, .. } => Some(sender),
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

        let mut join_gw_1 = JoinRingOp::initial_request(new_peer.peer, gateway, 0).sm;
        let mut join_new_peer_2 = StateMachine::<JROpSm>::from_state(JRState::Initializing);

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
    async fn node0_to_gateway_conn() {
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

        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 4, 4, 4, 2);
        sim_nodes.build().await;

        let mut connected = HashSet::new();
        let elapsed = Instant::now();
        while elapsed.elapsed() < Duration::from_secs(120) && connected.len() < NUM_NODES {
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
