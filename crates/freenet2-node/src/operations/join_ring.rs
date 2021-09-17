use rust_fsm::*;

use super::{OpError, OperationResult};
use crate::{
    conn_manager::ConnectionBridge,
    message::{Message, Transaction},
    node::OpStateStorage,
};

pub(crate) use self::messages::JoinRingMsg;

#[derive(Debug)]
pub(crate) struct JoinRingOpState(InternalJoinRingOpState);

impl From<InternalJoinRingOpState> for JoinRingOpState {
    fn from(state: InternalJoinRingOpState) -> Self {
        JoinRingOpState(state)
    }
}

state_machine! {
    derive(Debug)
    InternalJoinRingOp(Connecting)

    Connecting =>  {
        Connecting => OCReceived [OCReceived],
        OCReceived => Connected [Connected],
        Connected => Connected [Connected],
    },
    OCReceived(Connected) => Connected [Connected],
}

pub(crate) async fn join_ring<CB>(
    op_storage: &mut OpStateStorage,
    conn_manager: &mut CB,
    join_op: JoinRingMsg,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    let id = *join_op.id();
    if let Some(state) = op_storage.pop_join_ring_op(&id) {
        // was an existing operation
        match update_state(state.0, join_op) {
            Err(tx) => {
                log::error!("error while processing {}", tx);
                conn_manager.send(Message::Canceled(tx)).await?;
            }
            Ok(OperationResult {
                return_msg: Some(msg),
                state: Some(updated_state),
            }) => {
                // updated op
                conn_manager.send(msg).await?;
                op_storage.push_join_ring_op(id, updated_state.into())?;
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
    } else {
    }
    Ok(())
}

fn update_state(
    current_state: InternalJoinRingOpState,
    other_host_msg: JoinRingMsg,
) -> Result<OperationResult<InternalJoinRingOpState>, Transaction> {
    todo!()
}

// fn join_ring(self: &Arc<Self>) -> Result<()> {
//     if self.conn_manager.transport().is_open() && self.gateways.read().is_empty() {
//         match *self.location.read() {
//             Some(loc) => {
//                 log::info!(
//                     "No gateways to join through, listening for connections at loc: {}",
//                     loc
//                 );
//                 return Ok(());
//             }
//             None => return Err(RingProtoError::Join),
//         }
//     }

//     // FIXME: this iteration should be shuffled, must write an extension iterator shuffle items "in place"
//     // the idea here is to limit the amount of gateways being contacted that's why shuffling is required
//     for gateway in self.gateways.read().iter() {
//         log::info!(
//             "Joining ring via {} at {}",
//             gateway.peer,
//             gateway
//                 .location
//                 .ok_or(conn_manager::ConnError::LocationUnknown)?
//         );
//         self.conn_manager.add_connection(*gateway, true);
//         let tx = Transaction::new(<JoinRequest as TransactionType>::msg_type_id());
//         let join_req = messages::JoinRequest::Initial {
//             key: self.peer_key,
//             hops_to_live: self.max_hops_to_live,
//         };
//         log::debug!("Sending {:?} to {}", join_req, gateway.peer);

//         let ring_proto = self.clone();
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
//         log::debug!("Initiating JoinRequest transaction: {}", tx);
//         let msg: Message = (tx, join_req).into();
//         self.conn_manager
//             .send_with_callback(*gateway, tx, msg, join_response_cb)?;
//     }

//     Ok(())
// }

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
    use super::*;

    #[test]
    fn join_ring_transitions() {
        let mut join_op_host_1 = StateMachine::<InternalJoinRingOp>::new();
        let res = join_op_host_1
            .consume(&InternalJoinRingOpInput::Connecting)
            .unwrap()
            .unwrap();
        assert!(matches!(res, InternalJoinRingOpOutput::OCReceived));

        let mut join_op_host_2 = StateMachine::<InternalJoinRingOp>::new();
        let res = join_op_host_2
            .consume(&InternalJoinRingOpInput::OCReceived)
            .unwrap()
            .unwrap();
        assert!(matches!(res, InternalJoinRingOpOutput::Connected));

        let res = join_op_host_1
            .consume(&InternalJoinRingOpInput::Connected)
            .unwrap()
            .unwrap();
        assert!(matches!(res, InternalJoinRingOpOutput::Connected));
    }
}
