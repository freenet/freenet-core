use rust_fsm::*;

use super::{OpError, OperationResult};
use crate::{
    conn_manager::{self, ConnectionBridge, PeerKeyLocation},
    message::{Message, Transaction, TransactionType},
    node::{OpExecutionError, OpStateStorage},
};

pub(crate) use self::messages::JoinRingMsg;

pub(crate) struct JoinRingOp(StateMachine<InternalJROp>);

impl JoinRingOp {
    pub fn new(this_peer: PeerKeyLocation, gateway: PeerKeyLocation) -> Self {
        let mut machine = StateMachine::new();
        machine
            .consume(&JRInput::Connecting { gateway, this_peer })
            .expect("");
        JoinRingOp(machine)
    }
}

#[derive(Debug)]
struct InternalJROp;

impl StateMachineImpl for InternalJROp {
    type Input = JRInput;

    type State = JRState;

    type Output = JROutput;

    const INITIAL_STATE: Self::State = JRState::Initializing;

    fn transition(state: &Self::State, input: &Self::Input) -> Option<Self::State> {
        match (state, input) {
            (
                JRState::Initializing,
                JRInput::Connecting {
                    gateway, this_peer, ..
                },
            ) => Some(JRState::Connecting(ConnectionInfo {
                gateway: *gateway,
                this_peer: *this_peer,
                max_hops_to_live: todo!(),
            })),
            (JRState::Connecting { .. }, JRInput::Connecting { .. }) => Some(JRState::OCReceived),
            (JRState::Connecting { .. }, JRInput::OCReceived | JRInput::Connected) => {
                Some(JRState::Connected)
            }
            (JRState::OCReceived { .. }, JRInput::Connected) => Some(JRState::Connected),
            (JRState::Connected, _) => None,
            _ => None,
        }
    }

    fn output(state: &Self::State, input: &Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (JRState::Initializing, JRInput::Connecting { this_peer, .. }) => {
                Some(JROutput::OCReceived {
                    by_peer: *this_peer,
                })
            }
            (JRState::Connecting { .. }, JRInput::Connecting { this_peer, .. }) => {
                Some(JROutput::OCReceived {
                    by_peer: *this_peer,
                })
            }
            (JRState::Connecting { .. }, JRInput::OCReceived | JRInput::Connected) => {
                Some(JROutput::Connected)
            }
            (JRState::OCReceived, JRInput::Connected) => Some(JROutput::Connected),
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
    this_peer: PeerKeyLocation,
    max_hops_to_live: usize,
}

impl JRState {
    fn try_unwrap_connecting(self) -> Result<ConnectionInfo, OpError> {
        if let Self::Connecting(conn_info) = self {
            Ok(conn_info)
        } else {
            Err(OpExecutionError::UpdateFailure.into())
        }
    }
}

enum JRInput {
    Connecting {
        gateway: PeerKeyLocation,
        this_peer: PeerKeyLocation,
    },
    OCReceived,
    Connected,
}

enum JROutput {
    OCReceived { by_peer: PeerKeyLocation },
    Connected,
}

/// Join ring routine, called upon processing a request to join or performing
/// a join operation for this node.
///
/// # Arguments
/// - join_op: no nodes
pub(crate) async fn join_ring<CB>(
    op_storage: &mut OpStateStorage,
    conn_manager: &mut CB,
    join_op: JoinRingMsg,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    let result = if let Some(state) = op_storage.pop_join_ring_op(join_op.id()) {
        // was an existing operation, the other peer messaged back
        update_state(state, join_op)
    } else {
        // new request to join from this node, initialize the machine
        let machine = JoinRingOp(StateMachine::new());
        update_state(machine, join_op)
    };

    match result.map_err(|_| OpExecutionError::UpdateFailure) {
        Err(tx) => {
            log::error!("error while processing join request: {}", tx);
            let tx = todo!();
            conn_manager.send(Message::Canceled(tx)).await?;
            return Err(OpExecutionError::UpdateFailure.into());
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: Some(updated_state),
        }) => {
            // updated op
            let id = *msg.id();
            conn_manager.send(msg).await?;
            op_storage.push_join_ring_op(id, updated_state)?;
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: None,
        }) => {
            // finished the operation at this node, informing back
            conn_manager.send(msg).await?;
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
fn update_state(
    current_state: JoinRingOp,
    other_host_msg: JoinRingMsg,
) -> Result<OperationResult<JoinRingOp>, ()> {
    //         let join_response_cb =
    //             move |sender: PeerKeyLocation, join_res: Message| -> conn_manager::Result<()> {
    //                 let (accepted_by, tx) = if let Message::JoinResponse(
    //                     incoming_tx,
    //                     messages::JoinResponse::Initial {
    //                         accepted_by,
    //                         your_location,
    //                         ..
    //                     },
    //                 ) = join_res
    //                 {
    //                     log::debug!("JoinResponse received from {}", sender.peer,);
    //                     if incoming_tx != tx {
    //                         return Err(conn_manager::ConnError::UnexpectedTx(tx, incoming_tx));
    //                     }
    //                     let loc = &mut *ring_proto.location.write();
    //                     *loc = Some(your_location);
    //                     (accepted_by, incoming_tx)
    //                 } else {
    //                     return Err(conn_manager::ConnError::UnexpectedResponseMessage(join_res));
    //                 };
    //                 let self_location = &*ring_proto.location.read();
    //                 let self_location =
    //                     &self_location.ok_or(conn_manager::ConnError::LocationUnknown)?;
    //                 for new_peer_key in accepted_by {
    //                     if ring_proto.ring.should_accept(
    //                         self_location,
    //                         &new_peer_key
    //                             .location
    //                             .ok_or(conn_manager::ConnError::LocationUnknown)?,
    //                     ) {
    //                         log::info!("Establishing connection to {}", new_peer_key.peer);
    //                         ring_proto.establish_conn(new_peer_key, tx);
    //                     } else {
    //                         log::debug!("Not accepting connection to {}", new_peer_key.peer);
    //                     }
    //                 }
    //                 Ok(())
    //             };
    todo!()
}

pub(crate) async fn initial_join_request<CB>(
    op_storage: &mut OpStateStorage,
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
    } = (&join_op.0).state().clone().try_unwrap_connecting()?;

    log::info!(
        "Joining ring via {} at {}",
        gateway.peer,
        gateway
            .location
            .ok_or(conn_manager::ConnError::LocationUnknown)?
    );

    conn_manager.add_connection(gateway, true);
    let tx = Transaction::new(<JoinRingMsg as TransactionType>::msg_type_id());
    let join_req = Message::from(messages::JoinRingMsg::Req {
        id: tx,
        msg: messages::JoinRequest::Initial {
            key: this_peer.peer,
            hops_to_live: max_hops_to_live,
        },
    });
    log::debug!(
        "Sending initial join tx: {:?} to {}",
        join_req,
        gateway.peer
    );
    conn_manager.send(join_req).await?;
    join_op
        .0
        .consume(&JRInput::Connecting { gateway, this_peer })
        .map_err(|_| OpError::IllegalStateTransition)?;
    op_storage.push_join_ring_op(tx, join_op)?;
    Ok(())
}

// fn establish_conn(self: &Arc<Self>, new_peer: PeerKeyLocation, tx: Transaction) {
//     self.conn_manager.add_connection(new_peer, false);
//     let self_cp = self.clone();
//     let state = Arc::new(RwLock::new(messages::OpenConnection::Connecting));

//     let state_cp = state.clone();
//     let ack_peer = move |peer: PeerKeyLocation, msg: Message| -> conn_manager::Result<()> {
//         let (tx, oc) = match msg {
//             Message::OpenConnection(tx, oc) => (tx, oc),
//             msg => return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg)),
//         };
//         let mut current_state = state_cp.write();
//         current_state.transition(oc);
//         if !current_state.is_connected() {
//             let open_conn: Message = (tx, *current_state).into();
//             log::debug!("Acknowledging OC");
//             self_cp
//                 .conn_manager
//                 .send(peer, *open_conn.id(), open_conn)?;
//         } else {
//             log::info!(
//                 "{} connected to {}, adding to ring",
//                 self_cp.peer_key,
//                 new_peer.peer
//             );
//             self_cp.conn_manager.send(
//                 peer,
//                 tx,
//                 Message::from((tx, messages::OpenConnection::Connected)),
//             )?;
//             self_cp.ring.connections_by_location.write().insert(
//                 new_peer
//                     .location
//                     .ok_or(conn_manager::ConnError::LocationUnknown)?,
//                 new_peer,
//             );
//         }
//         Ok(())
//     };
//     self.conn_manager.listen_to_replies(tx, ack_peer);
//     let conn_manager = self.conn_manager.clone();
//     tokio::spawn(async move {
//         let curr_time = Instant::now();
//         let mut attempts = 0;
//         while !state.read().is_connected() && curr_time.elapsed() <= Duration::from_secs(30) {
//             log::debug!(
//                 "Sending {} to {}, number of messages sent: {}",
//                 *state.read(),
//                 new_peer.peer,
//                 attempts
//             );
//             conn_manager.send(new_peer, tx, Message::OpenConnection(tx, *state.read()))?;
//             attempts += 1;
//             tokio::time::sleep(Duration::from_millis(200)).await
//         }
//         if curr_time.elapsed() > Duration::from_secs(30) {
//             log::error!("Timed out trying to connect to {}", new_peer.peer);
//             Err(conn_manager::ConnError::NegotationFailed)
//         } else {
//             conn_manager.remove_listener(tx);
//             log::info!("Success negotiating connection to {}", new_peer.peer);
//             Ok(())
//         }
//     });
// }

// fn listen_for_join_req(self: &Arc<Self>) -> ListenerHandle {
//     let self_cp = self.clone();
//     let process_join_req = move |sender: PeerKeyLocation,
//                                  msg: Message|
//           -> conn_manager::Result<()> {
//         let (tx, join_req) = if let Message::JoinRequest(id, join_req) = msg {
//             (id, join_req)
//         } else {
//             return Err(conn_manager::ConnError::UnexpectedResponseMessage(msg));
//         };
//         enum ReqType {
//             Initial,
//             Proxy,
//         }
//         let peer_key_loc;
//         let req_type;
//         let jr_hpt = match join_req {
//             messages::JoinRequest::Initial { key, hops_to_live } => {
//                 peer_key_loc = PeerKeyLocation {
//                     peer: key,
//                     location: Some(Location::random()),
//                 };
//                 req_type = ReqType::Initial;
//                 hops_to_live
//             }
//             messages::JoinRequest::Proxy {
//                 joiner,
//                 hops_to_live,
//             } => {
//                 peer_key_loc = joiner;
//                 req_type = ReqType::Proxy;
//                 hops_to_live
//             }
//         };
//         log::debug!(
//             "JoinRequest received by {} with HTL {}",
//             sender
//                 .location
//                 .ok_or(conn_manager::ConnError::LocationUnknown)?,
//             jr_hpt
//         );
//         let your_location = self_cp
//             .location
//             .read()
//             .ok_or(conn_manager::ConnError::LocationUnknown)?;
//         let accepted_by = if self_cp.ring.should_accept(
//             &your_location,
//             &peer_key_loc
//                 .location
//                 .ok_or(conn_manager::ConnError::LocationUnknown)?,
//         ) {
//             log::debug!(
//                 "Accepting connections to {:?}, establising connection @ {}",
//                 peer_key_loc,
//                 self_cp.peer_key
//             );
//             self_cp.establish_conn(peer_key_loc, tx);
//             vec![PeerKeyLocation {
//                 peer: self_cp.peer_key,
//                 location: Some(your_location),
//             }]
//         } else {
//             log::debug!("Not accepting new connection sender {:?}", peer_key_loc);
//             Vec::new()
//         };
//         log::debug!(
//             "Sending JoinResponse to {} accepting {} connections",
//             sender.peer,
//             accepted_by.len()
//         );
//         let join_response = match req_type {
//             ReqType::Initial => Message::from((
//                 tx,
//                 JoinResponse::Initial {
//                     accepted_by: accepted_by.clone(),
//                     your_location: peer_key_loc
//                         .location
//                         .ok_or(conn_manager::ConnError::LocationUnknown)?,
//                     your_peer_id: peer_key_loc.peer,
//                 },
//             )),
//             ReqType::Proxy => Message::from((
//                 tx,
//                 JoinResponse::Proxy {
//                     accepted_by: accepted_by.clone(),
//                 },
//             )),
//         };
//         self_cp.conn_manager.send(peer_key_loc, tx, join_response)?;
//         // NOTE: this is in practica a jump to: join_ring.join_response_cb
//         if jr_hpt > 0 && !self_cp.ring.connections_by_location.read().is_empty() {
//             let forward_to = if jr_hpt >= self_cp.rnd_if_htl_above {
//                 log::debug!(
//                     "Randomly selecting peer to forward JoinRequest sender {}",
//                     sender.peer
//                 );
//                 self_cp.ring.random_peer(|p| p.peer != sender.peer)
//             } else {
//                 log::debug!(
//                     "Selecting close peer to forward request sender {}",
//                     sender.peer
//                 );
//                 self_cp
//                     .ring
//                     .connections_by_location
//                     .read()
//                     .get(
//                         &peer_key_loc
//                             .location
//                             .ok_or(conn_manager::ConnError::LocationUnknown)?,
//                     )
//                     .filter(|it| it.peer != sender.peer)
//                     .copied()
//             };
//             if let Some(forward_to) = forward_to {
//                 let forwarded = Message::from((
//                     tx,
//                     JoinRequest::Proxy {
//                         joiner: peer_key_loc,
//                         hops_to_live: jr_hpt.min(self_cp.max_hops_to_live) - 1,
//                     },
//                 ));
//                 let forwarded_acceptors =
//                     Arc::new(Mutex::new(accepted_by.into_iter().collect::<HashSet<_>>()));
//                 log::debug!(
//                     "Forwarding JoinRequest sender {} to {}",
//                     sender.peer,
//                     forward_to.peer
//                 );
//                 let self_cp2 = self_cp.clone();
//                 let register_acceptors =
//                     move |jr_sender: PeerKeyLocation, join_resp| -> conn_manager::Result<()> {
//                         if let Message::JoinResponse(tx, resp) = join_resp {
//                             let new_acceptors = match resp {
//                                 JoinResponse::Initial { accepted_by, .. } => accepted_by,
//                                 JoinResponse::Proxy { accepted_by, .. } => accepted_by,
//                             };
//                             let fa = &mut *forwarded_acceptors.lock();
//                             new_acceptors.iter().for_each(|p| {
//                                 if !fa.contains(p) {
//                                     fa.insert(*p);
//                                 }
//                             });
//                             let msg = Message::from((
//                                 tx,
//                                 JoinResponse::Proxy {
//                                     accepted_by: new_acceptors,
//                                 },
//                             ));
//                             self_cp2.conn_manager.send(jr_sender, tx, msg)?;
//                         };
//                         Ok(())
//                     };
//                 self_cp.conn_manager.send_with_callback(
//                     forward_to,
//                     tx,
//                     forwarded,
//                     register_acceptors,
//                 )?;
//             }
//         }
//         Ok(())
//     };
//     self.conn_manager
//         .listen(<JoinRequest as TransactionType>::msg_type_id(), process_join_req)
// }

mod messages {
    use super::*;
    use crate::{conn_manager::PeerKeyLocation, ring_proto::Location, PeerKey};

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum JoinRingMsg {
        Req { id: Transaction, msg: JoinRequest },
        OC { id: Transaction, msg: JoinResponse },
        Resp { id: Transaction, msg: JoinResponse },
    }

    impl JoinRingMsg {
        pub fn id(&self) -> &Transaction {
            use JoinRingMsg::*;
            match self {
                Req { id, .. } => id,
                OC { id, .. } => id,
                Resp { id, .. } => id,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum JoinRequest {
        Initial {
            key: PeerKey,
            hops_to_live: usize,
        },
        Proxy {
            joiner: PeerKeyLocation,
            hops_to_live: usize,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum JoinResponse {
        Initial {
            accepted_by: Vec<PeerKeyLocation>,
            your_location: Location,
            your_peer_id: PeerKey,
        },
        Proxy {
            accepted_by: Vec<PeerKeyLocation>,
        },
    }
}

#[cfg(test)]
mod tests {
    use libp2p::identity::Keypair;

    use super::*;
    use crate::PeerKey;

    #[test]
    fn join_ring_transitions() {
        let h1 = PeerKeyLocation {
            peer: PeerKey::from(Keypair::generate_ed25519().public()),
            location: None,
        };
        let h2 = PeerKeyLocation {
            peer: PeerKey::from(Keypair::generate_ed25519().public()),
            location: None,
        };

        let mut join_op_host_1 = StateMachine::<InternalJROp>::new();
        let res = join_op_host_1
            .consume(&JRInput::Connecting {
                gateway: h2,
                this_peer: h1,
            })
            .unwrap()
            .unwrap();
        assert!(matches!(res, JROutput::OCReceived { by_peer: h1 }));

        let mut join_op_host_2 = StateMachine::<InternalJROp>::new();
        let res = join_op_host_2
            .consume(&JRInput::OCReceived)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JROutput::Connected));

        let res = join_op_host_1
            .consume(&JRInput::Connected)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JROutput::Connected));
    }
}
