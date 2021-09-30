use std::collections::HashSet;

use rust_fsm::*;

use super::{OpError, OperationResult};
use crate::{
    conn_manager::{self, ConnectionBridge, PeerKey, PeerKeyLocation},
    message::{Message, Transaction},
    node::{OpExecutionError, OpStateStorage},
    operations::Operation,
    ring::{Location, Ring},
};

pub(crate) use self::messages::{JoinRequest, JoinResponse, JoinRingMsg};

pub(crate) struct JoinRingOp(StateMachine<JROpSM>);

impl JoinRingOp {
    pub fn initial_request(
        this_peer: PeerKey,
        gateway: PeerKeyLocation,
        max_hops_to_live: usize,
    ) -> Self {
        log::info!("Connecting to gw {} from {}", gateway.peer, this_peer);
        let sm = StateMachine::from_state(JRState::Connecting(ConnectionInfo {
            gateway,
            this_peer,
            max_hops_to_live,
        }));
        JoinRingOp(sm)
    }
}

#[derive(Debug)]
struct JROpSM;

impl StateMachineImpl for JROpSM {
    type Input = JoinRingMsg;

    type State = JRState;

    type Output = JoinRingMsg;

    const INITIAL_STATE: Self::State = JRState::Initializing;

    fn transition(state: &Self::State, input: &Self::Input) -> Option<Self::State> {
        match (state, input) {
            (
                JRState::Initializing,
                JoinRingMsg::Req {
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
                JoinRingMsg::Resp {
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
                JoinRingMsg::Resp {
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
            _ => None,
        }
    }

    fn output(state: &Self::State, input: &Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (
                JRState::Initializing,
                JoinRingMsg::Req {
                    id,
                    msg:
                        JoinRequest::Accepted {
                            gateway,
                            accepted_by,
                            your_location,
                            your_peer_id,
                        },
                },
            ) => Some(JoinRingMsg::Resp {
                id: *id,
                sender: *gateway,
                msg: JoinResponse::AcceptedBy {
                    peers: accepted_by.clone(),
                    your_location: *your_location,
                    your_peer_id: *your_peer_id,
                },
                target: PeerKeyLocation {
                    peer: *your_peer_id,
                    location: Some(*your_location),
                },
            }),
            (
                JRState::Connecting(ConnectionInfo { .. }),
                JoinRingMsg::Resp {
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
                    peer: *your_peer_id,
                    location: Some(*your_location),
                };
                Some(JoinRingMsg::Resp {
                    id: *id,
                    msg: JoinResponse::ReceivedOC { by_peer: sender },
                    sender,
                    target: *prev_sender,
                })
            }
            (
                JRState::OCReceived,
                JoinRingMsg::Resp {
                    msg: JoinResponse::ReceivedOC { .. },
                    id,
                    target,
                    sender,
                },
            ) => Some(JoinRingMsg::Connected {
                id: *id,
                sender: *target,
                target: *sender,
            }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
enum JRState {
    Initializing,
    Connecting(ConnectionInfo),
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
            Err(OpError::IllegalStateTransition)
        }
    }

    fn is_connected(&self) -> bool {
        matches!(self, JRState::Connected)
    }
}

/// Join ring routine, called upon processing a request to join or while performing
/// a join operation for this node after initial request (see [`join_ring_request`]).
///
/// # Cancellation Safety
/// This future is not cancellation safe.
pub(crate) async fn handle_join_ring<CB>(
    op_storage: &mut OpStateStorage,
    conn_manager: &mut CB,
    join_op: JoinRingMsg,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    let sender;
    let tx = *join_op.id();
    let result = match op_storage.pop(join_op.id()) {
        Some(Operation::JoinRing(state)) => {
            sender = join_op.sender().cloned();
            // was an existing operation, the other peer messaged back
            update_state(conn_manager, state, join_op, &mut op_storage.ring).await
        }
        Some(_) => return Err(OpExecutionError::TxUpdateFailure(tx).into()),
        None => {
            sender = join_op.sender().cloned();
            // new request to join from this node, initialize the machine
            let machine = JoinRingOp(StateMachine::new());
            update_state(conn_manager, machine, join_op, &mut op_storage.ring).await
        }
    };

    match result {
        Err(err) => {
            log::error!("error while processing join request: {}", err);
            if let Some(sender) = sender {
                conn_manager.send(&sender, Message::Canceled(tx)).await?;
            }
            return Err(err);
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: Some(updated_state),
        }) => {
            // updated op
            let id = *msg.id();
            if let Some(target) = msg.target() {
                conn_manager.send(&target, msg).await?;
            }
            op_storage.push(id, Operation::JoinRing(updated_state))?;
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: None,
        }) => {
            // finished the operation at this node, informing back
            if let Some(target) = msg.target() {
                conn_manager.send(&target, msg).await?;
            }
        }
        Ok(OperationResult {
            return_msg: None,
            state: None,
        }) => {
            // operation finished_completely
        }
        _ => unreachable!(),
    }
    Ok(())
}

#[inline(always)]
async fn update_state<CB>(
    conn_manager: &mut CB,
    mut state: JoinRingOp,
    other_host_msg: JoinRingMsg,
    ring: &mut Ring,
) -> Result<OperationResult<JoinRingOp>, OpError>
where
    CB: ConnectionBridge,
{
    let return_msg;
    let new_state;
    match other_host_msg {
        JoinRingMsg::Req {
            id,
            msg:
                JoinRequest::Initial {
                    target_loc: gw_location,
                    req_peer,
                    hops_to_live,
                    ..
                },
        } => {
            log::debug!(
                "Initial join request received from {} with HTL {} @ {}",
                req_peer,
                hops_to_live,
                gw_location.peer
            );
            let new_location = Location::random();
            let accepted_by = if ring.should_accept(
                &gw_location
                    .location
                    .ok_or(OpExecutionError::TxUpdateFailure(id))?,
                &new_location,
            ) {
                log::debug!("Accepting connections from {}", req_peer,);
                conn_manager.add_connection(gw_location, false);
                vec![gw_location]
            } else {
                log::debug!("Not accepting new connection for sender {}", req_peer);
                Vec::new()
            };

            log::debug!(
                "Sending join response to {} accepting {} connections",
                req_peer,
                accepted_by.len()
            );
            return_msg = state
                .0
                .consume(&JoinRingMsg::Req {
                    id,
                    msg: JoinRequest::Accepted {
                        gateway: gw_location,
                        accepted_by: accepted_by.clone(),
                        your_location: new_location,
                        your_peer_id: req_peer,
                    },
                })?
                .map(Message::from);
            new_state = Some(state);

            let new_peer_loc = PeerKeyLocation {
                location: Some(new_location),
                peer: req_peer,
            };

            if hops_to_live > 0 && !ring.connections_by_location.read().is_empty() {
                let forward_to = if hops_to_live >= ring.rnd_if_htl_above {
                    log::debug!(
                        "Randomly selecting peer to forward JoinRequest, sender: {}",
                        req_peer
                    );
                    ring.random_peer(|p| p.peer != req_peer)
                } else {
                    log::debug!(
                        "Selecting close peer to forward request, sender: {}",
                        req_peer
                    );
                    ring.connections_by_location
                        .read()
                        .get(&new_location)
                        .filter(|it| it.peer != req_peer)
                        .copied()
                };

                if let Some(forward_to) = forward_to {
                    let forwarded = Message::from(JoinRingMsg::Req {
                        id,
                        msg: JoinRequest::Proxy {
                            joiner: new_peer_loc,
                            hops_to_live: hops_to_live.min(ring.max_hops_to_live) - 1,
                        },
                    });
                    log::debug!(
                        "Forwarding JoinRequest from sender {} to {}",
                        req_peer,
                        forward_to.peer
                    );
                    conn_manager.send(&forward_to, forwarded).await?;
                    let _forwarded_acceptors = accepted_by.into_iter().collect::<HashSet<_>>();
                    // this will would jump to JoinRingMsg::Resp::JoinResponse::Proxy after peer return
                    // TODO: add a new state that transits from Connecting -> WaitingProxyResponse
                    todo!()
                }
            }
        }
        JoinRingMsg::Req {
            id,
            msg:
                JoinRequest::Proxy {
                    joiner,
                    hops_to_live,
                },
        } => {
            todo!()
        }
        JoinRingMsg::Resp {
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
                .0
                .consume(&JoinRingMsg::Resp {
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
            for new_peer in accepted_by {
                if ring.should_accept(
                    &your_location,
                    &new_peer
                        .location
                        .ok_or(conn_manager::ConnError::LocationUnknown)?,
                ) {
                    log::info!("Establishing connection to {}", new_peer.peer);
                    conn_manager.add_connection(new_peer, false);
                } else {
                    log::info!("Not accepting connection to {}", new_peer.peer);
                }
            }
            ring.own_location = Some(your_location);
            new_state = Some(state);
        }
        JoinRingMsg::Resp {
            id,
            sender,
            msg: JoinResponse::Proxy { accepted_by },
            target,
        } => {
            //         let register_acceptors =
            //             move |jr_sender: PeerKeyLocation, join_resp| -> conn_manager::Result<()> {
            //                 if let Message::JoinResponse(tx, resp) = join_resp {
            //                     let new_acceptors = match resp {
            //                         JoinResponse::Initial { accepted_by, .. } => accepted_by,
            //                         JoinResponse::Proxy { accepted_by, .. } => accepted_by,
            //                     };
            //                     let fa = &mut *forwarded_acceptors.lock();
            //                     new_acceptors.iter().for_each(|p| {
            //                         if !fa.contains(p) {
            //                             fa.insert(*p);
            //                         }
            //                     });
            //                     let msg = Message::from((
            //                         tx,
            //                         JoinResponse::Proxy {
            //                             accepted_by: new_acceptors,
            //                         },
            //                     ));
            //                     self_cp2.conn_manager.send(jr_sender, tx, msg)?;
            //                 };
            //                 Ok(())
            //             };
            todo!()
        }
        JoinRingMsg::Resp {
            id,
            sender,
            msg: JoinResponse::ReceivedOC { by_peer },
            target,
        } => {
            return_msg = state
                .0
                .consume(&JoinRingMsg::Resp {
                    id,
                    sender,
                    msg: JoinResponse::ReceivedOC { by_peer },
                    target,
                })?
                .map(Message::from);
            if !state.0.state().is_connected() {
                return Err(OpError::IllegalStateTransition);
            } else {
                log::debug!("Successfully completed connection to peer {}", by_peer.peer);
                new_state = None;
            }
        }
        JoinRingMsg::Connected { target, sender, id } => {
            return_msg = state
                .0
                .consume(&JoinRingMsg::Connected { target, sender, id })?
                .map(Message::from);
            if !state.0.state().is_connected() {
                return Err(OpError::IllegalStateTransition);
            } else {
                log::info!(
                    "Successfully completed connection @ {}, new location = {:?}",
                    target.peer,
                    ring.own_location
                );
                new_state = None;
            }
        }
        _ => unimplemented!(),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state,
    })
}

/// Join ring routine, called upon performing a join operation for this node.
pub(crate) async fn join_ring_request<CB>(
    tx: Transaction,
    op_storage: &mut OpStateStorage,
    conn_manager: &mut CB,
    join_op: JoinRingOp,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    let ConnectionInfo {
        gateway,
        this_peer,
        max_hops_to_live,
    } = (&join_op.0).state().clone().try_unwrap_connecting()?;

    log::info!(
        "Joining ring via {} (at {})",
        gateway.peer,
        gateway
            .location
            .ok_or(conn_manager::ConnError::LocationUnknown)?
    );

    conn_manager.add_connection(gateway, true);
    let join_req = Message::from(messages::JoinRingMsg::Req {
        id: tx,
        msg: messages::JoinRequest::Initial {
            target_loc: gateway,
            req_peer: this_peer,
            hops_to_live: max_hops_to_live,
            max_hops_to_live,
        },
    });
    conn_manager.send(&gateway, join_req).await?;
    op_storage.push(tx, Operation::JoinRing(join_op))?;
    Ok(())
}

mod messages {
    use super::*;
    use crate::{conn_manager::PeerKeyLocation, ring::Location};

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum JoinRingMsg {
        Req {
            id: Transaction,
            msg: JoinRequest,
        },
        Resp {
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
                Req { id, .. } => id,
                Resp { id, .. } => id,
                Connected { id, .. } => id,
            }
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            use JoinRingMsg::*;
            match self {
                Resp { sender, .. } => Some(sender),
                Connected { sender, .. } => Some(sender),
                _ => None,
            }
        }

        pub fn target(&self) -> Option<PeerKeyLocation> {
            use JoinRingMsg::*;
            match self {
                Resp { target, .. } => Some(*target),
                Connected { target, .. } => Some(*target),
                _ => None,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum JoinRequest {
        Initial {
            target_loc: PeerKeyLocation,
            req_peer: PeerKey,
            hops_to_live: usize,
            max_hops_to_live: usize,
        },
        Accepted {
            gateway: PeerKeyLocation,
            accepted_by: Vec<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerKey,
        },
        Proxy {
            joiner: PeerKeyLocation,
            hops_to_live: usize,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum JoinResponse {
        AcceptedBy {
            peers: Vec<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerKey,
        },
        ReceivedOC {
            by_peer: PeerKeyLocation,
        },
        Proxy {
            accepted_by: Vec<PeerKeyLocation>,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use libp2p::identity::Keypair;

    use super::*;
    use crate::{
        config::tracing::Logger,
        message::TransactionTypeId,
        node::test_utils::{EventType, SimNetwork},
    };

    #[test]
    fn succesful_join_ring_seq() {
        let id = Transaction::new(TransactionTypeId::JoinRing);
        let new_loc = Location::random();
        let mut new_peer = PeerKeyLocation {
            peer: PeerKey::from(Keypair::generate_ed25519().public()),
            location: Some(new_loc),
        };
        let gateway = PeerKeyLocation {
            peer: PeerKey::from(Keypair::generate_ed25519().public()),
            location: Some(Location::random()),
        };

        let mut join_gw_1 =
            StateMachine::<JROpSM>::from_state(JRState::Connecting(ConnectionInfo {
                gateway,
                this_peer: new_peer.peer,
                max_hops_to_live: 0,
            }));
        let mut join_new_peer_2 = StateMachine::<JROpSM>::new();

        let req = JoinRingMsg::Req {
            id,
            msg: JoinRequest::Accepted {
                gateway,
                accepted_by: vec![gateway],
                your_location: new_loc,
                your_peer_id: new_peer.peer,
            },
        };
        let res = join_new_peer_2.consume(&req).unwrap().unwrap();
        let expected = JoinRingMsg::Resp {
            id,
            msg: JoinResponse::AcceptedBy {
                peers: vec![gateway],
                your_location: new_loc,
                your_peer_id: new_peer.peer,
            },
            target: new_peer,
            sender: gateway,
        };
        assert_eq!(res, expected);
        assert!(matches!(join_new_peer_2.state(), JRState::OCReceived));

        let res = join_gw_1.consume(&res).unwrap().unwrap();
        new_peer.location = Some(new_loc);
        let expected = JoinRingMsg::Resp {
            msg: JoinResponse::ReceivedOC { by_peer: new_peer },
            id,
            sender: new_peer,
            target: gateway,
        };
        assert_eq!(res, expected);
        assert!(matches!(join_gw_1.state(), JRState::OCReceived));

        let res = join_new_peer_2.consume(&res).unwrap().unwrap();
        let expected = JoinRingMsg::Connected {
            id,
            target: new_peer,
            sender: gateway,
        };
        assert_eq!(res, expected);
        assert!(matches!(join_new_peer_2.state(), JRState::Connected));

        assert!(join_gw_1.consume(&res).unwrap().is_none());
        assert!(matches!(join_gw_1.state(), JRState::Connected));

        // transaction finished, should not return anymore
        assert!(join_new_peer_2.consume(&res).is_err());
        assert!(join_gw_1.consume(&res).is_err());
        // and the final state should be connected
        assert!(matches!(join_gw_1.state(), JRState::Connected));
        assert!(matches!(join_new_peer_2.state(), JRState::Connected));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node0_to_gateway_conn() -> Result<(), Box<dyn std::error::Error>> {
        //! Given a network of one node and one gateway test that both are connected.
        Logger::init_logger();
        let mut sim_net = SimNetwork::build(1, 1, 0);
        match tokio::time::timeout(Duration::from_secs(10), sim_net.recv_net_events()).await {
            Ok(Some(Ok(event))) => match event.event {
                EventType::JoinSuccess { peer } => {
                    log::info!("Successful join op for peer {}", peer);
                    Ok(())
                }
            },
            _ => Err("no event received".into()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_nodes_should_connect() -> Result<(), Box<dyn std::error::Error>> {
        //! Given a network of 1000 peers all nodes should have connections.
        Logger::init_logger();

        let _sim_nodes = SimNetwork::build(10, 10, 7);
        // tokio::time::sleep(Duration::from_secs(300)).await;
        // let _hist: Vec<_> = _ring_distribution(sim_nodes.values()).collect();

        // FIXME: enable probing
        // const NUM_PROBES: usize = 10;
        // let mut probe_responses = Vec::with_capacity(NUM_PROBES);
        // for probe_idx in 0..NUM_PROBES {
        //     let target = Location::random();
        //     let idx: usize = rand::thread_rng().gen_range(0..sim_nodes.len());
        //     let rnd_node = sim_nodes
        //         .get_mut(&format!("node-{}", idx))
        //         .ok_or("node not found")?;
        //     let probe_response = ProbeProtocol::probe(
        //         rnd_node.ring_protocol.clone(),
        //         Transaction::new(<ProbeRequest as TransactionType>::msg_type_id()),
        //         ProbeRequest {
        //             hops_to_live: 7,
        //             target,
        //         },
        //     )
        //     .await
        //     .expect("failed to get probe response");
        //     probe_responses.push(probe_response);
        // }
        // probe_proto::utils::plot_probe_responses(probe_responses);

        // let any_empties = sim_nodes
        //     .peers
        //     .values()
        //     .map(|node| {
        //         node.op_storage
        //             .ring
        //             .connections_by_location
        //             .read()
        //             .is_empty()
        //     })
        //     .any(|is_empty| is_empty);
        // assert!(!any_empties);

        Ok(())
    }
}
