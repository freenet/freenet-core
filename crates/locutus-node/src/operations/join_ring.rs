use futures::Future;
use std::pin::Pin;
use std::{collections::HashSet, time::Duration};

use super::{handle_op_result, state_machine::StateMachineImpl, OpError, OperationResult};
use crate::operations::op_trait::Operation;
use crate::operations::OpInitialization;
use crate::{
    config::PEER_TIMEOUT,
    message::{Message, Transaction, InnerMessage},
    node::{ConnectionBridge, ConnectionError, OpManager, PeerKey},
    operations::{state_machine::StateMachine, OpEnum},
    ring::{Location, PeerKeyLocation, Ring},
    util::ExponentialBackoff,
};
use crate::message::TransactionType::JoinRing;

pub(crate) use self::messages::{JoinRequest, JoinResponse, JoinRingMsg};

const MAX_JOIN_RETRIES: usize = 3;

pub(crate) struct JoinRingOp {
    // TODO dejar de usar el struct y definir aquí los tipos necesarios para manejar el estado
    state: Option<JRState>,
    id: Transaction,
    pub gateway: Box<PeerKeyLocation>,
    /// keeps track of the number of retries and applies an exponential backoff cooldown period
    pub backoff: Option<ExponentialBackoff>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl<CErr: std::error::Error, CB: ConnectionBridge> Operation<CErr, CB> for JoinRingOp {
    type Message = JoinRingMsg;

    type Error = OpError<CErr>;

    fn load_or_init(
        op_storage: &OpManager<CErr>,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError<CErr>> {
        let sender;
        let tx = *msg.id();
        match op_storage.pop(msg.id()) {
            Some(OpEnum::JoinRing(join_op)) => {
                sender = msg.sender().cloned();
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization {
                    op: join_op,
                    sender,
                })
            }
            Some(_) => return Err(OpError::OpNotPresent(tx)),
            None => {
                sender = msg.sender().cloned();
                // new request to join this node, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(JRState::Initializing),
                        id: tx,
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

    fn process_message(
        self,
        conn_manager: &mut CB,
        op_storage: &OpManager<CErr>,
        input: Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, Self::Error>> + Send + 'static>> {
        let fut = async move {

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
                    log::debug!(
                        "Initial join request received from {} with HTL {} @ {}",
                        req_peer,
                        hops_to_live,
                        this_node_loc.peer
                    );

                    let new_location = Location::random();
                    let accepted_by = if op_storage.ring.should_accept(&new_location) {
                        log::debug!("Accepting connection from {}", req_peer,);
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
                        &op_storage.ring,
                        conn_manager,
                        new_peer_loc,
                        new_peer_loc,
                        hops_to_live,
                        accepted_by.len(),
                    )
                        .await?
                    {
                        log::debug!("Awaiting @ {} (tx: {})", this_node_loc.peer, id);
                        updated_state.add_new_proxy(accepted_by)?;
                        // awaiting responses from proxies
                        new_state = Some(updated_state);
                        return_msg = None;
                    } else {
                        if accepted_by.is_empty() {
                            log::debug!(
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
                    log::debug!(
                        "Proxy join request received from {} to join new peer {} with HTL {} @ {}",
                        sender.peer,
                        joiner.peer,
                        hops_to_live,
                        own_loc.peer
                    );
                    let mut accepted_by = if op_storage.ring.should_accept(
                        &joiner
                            .location
                            .ok_or_else(|| OpError::from(ConnectionError::LocationUnknown))?,
                    ) {
                        log::debug!("Accepting proxy connection from {}", joiner.peer);
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
                                if accepted_by.contains(&own_loc) {
                                    log::debug!(
                                        "Return to {}, connected at proxy {} (tx: {})",
                                        sender.peer,
                                        own_loc.peer,
                                        id
                                    );
                                    new_state = Some(JRState::Connected);
                                } else {
                                    log::debug!("Failed to connect at proxy {}", sender.peer);
                                    new_state = None;
                                }
                                return_msg = Some(JoinRingMsg::Response {
                                    msg: JoinResponse::Proxy { accepted_by },
                                    sender: own_loc,
                                    id,
                                    target: sender,
                                });
                            }
                            Some(JRState::AwaitingProxyResponse {
                                     accepted_by: mut previously_accepted,
                                     new_peer_id,
                                     target,
                                     ..
                            }) => {
                                if !accepted_by.is_empty() {
                                    previously_accepted.extend(accepted_by.drain());
                                    if new_peer_id == target.peer {
                                        new_state = Some(JRState::OCReceived)
                                    } else {
                                        // for proxies just consider the connection open directly
                                        // what would happen in case that the connection is not confirmed end-to-end
                                        // is that we would end up with a dead connection;
                                        // this then must be dealed with by the normal mechanisms that keep
                                        // connections alive and prune any dead connections
                                        new_state = Some(JRState::Connected)
                                    }
                                }
                                log::debug!(
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
                            _ => return Err(OpError::InvalidStateTransition(self.id))
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
                    target,
                } => {
                    log::debug!("Join response received from {}", sender.peer);
                    match self.state {
                        Some(JRState::OCReceived) => {
                            log::debug!("Acknowledge connected at gateway");
                            new_state = Some(JRState::Connected);
                            return_msg = None;
                        }
                        Some(JRState::Connecting(ConnectionInfo { gateway, .. })) => {
                            if !accepted_by.clone().is_empty() {
                                log::debug!(
                                    "OC received at requesting peer {} from gateway {}",
                                    your_peer_id,
                                    gateway.peer
                                );
                                log::debug!(
                                    "Acknowledge OC at init peer {} from gw {}",
                                    your_peer_id,
                                    sender.peer
                                );
                                new_state = Some(JRState::OCReceived);
                                return_msg = Some(JoinRingMsg::Response {
                                    id,
                                    msg: JoinResponse::ReceivedOC { by_peer: sender },
                                    sender,
                                    target,
                                });
                            } else {
                                new_state = None;
                                return_msg = None;
                            }
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id))
                    };

                    let pkloc = PeerKeyLocation {
                        location: Some(your_location),
                        peer: your_peer_id,
                    };
                    op_storage.ring.update_location(Some(your_location));
                    for other_peer in accepted_by {
                        if op_storage.ring.should_accept(
                            &other_peer
                                .location
                                .ok_or(ConnectionError::LocationUnknown)?,
                        ) {
                            log::info!("Established connection to {}", other_peer.peer);
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
                                let _ = conn_manager
                                    .send(
                                        &other_peer.peer,
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
                            log::debug!("Not accepting connection to {}", other_peer.peer);
                        }
                    }
                    op_storage.ring.update_location(Some(your_location));
                }
                JoinRingMsg::Response {
                    id,
                    sender,
                    target,
                    msg: JoinResponse::Proxy { mut accepted_by },
                } => {
                    log::debug!("Received proxy join @ {}", target.peer);
                    match self.state {
                        Some(JRState::Initializing) => {
                            // the sender of the response is the target of the request and
                            // is only a completed tx if it accepted the connection
                            if accepted_by.contains(&sender) {
                                log::debug!(
                                    "Return to {}, connected at proxy {} (tx: {})",
                                    target.peer,
                                    sender.peer,
                                    id
                                );
                                new_state = Some(JRState::Connected);
                            } else {
                                log::debug!("Failed to connect at proxy {}", sender.peer);
                                new_state = None;
                            }
                            return_msg = Some(JoinRingMsg::Response {
                                msg: JoinResponse::Proxy { accepted_by },
                                sender,
                                id,
                                target,
                            });
                        }
                        Some( JRState::AwaitingProxyResponse {
                            accepted_by: mut previously_accepted,
                            new_peer_id,
                            target,
                            ..
                        }) => {
                            if !accepted_by.is_empty() {
                                previously_accepted.extend(accepted_by.drain());
                                if new_peer_id == target.peer {
                                    log::debug!(
                                        "Sending response to join request with all the peers that accepted \
                                        connection from gateway {} to peer {}",
                                        target.peer,
                                        sender.peer
                                    );
                                    new_state = Some(JRState::OCReceived);
                                    return_msg = Some(JoinRingMsg::Response {
                                        id,
                                        target,
                                        sender: target,
                                        msg: JoinResponse::AcceptedBy {
                                            peers: accepted_by,
                                            your_location: target.location.unwrap(),
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
                                    return_msg = Some(JoinRingMsg::Response {
                                        id,
                                        target,
                                        sender: target,
                                        msg: JoinResponse::Proxy { accepted_by }
                                    });
                                }
                            }
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id))
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
                            log::debug!("Acknowledge connected at gateway");
                            new_state = Some(JRState::Connected);
                            return_msg = Some(JoinRingMsg::Connected {
                                id,
                                sender: target,
                                target: sender,
                            });
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id))
                    }
                    if let Some(state) = new_state.clone() {
                        if state.is_connected() {
                            return Err(OpError::InvalidStateTransition(id));
                        } else {
                            conn_manager.add_connection(sender.peer).await?;
                            op_storage.ring.add_connection(
                                sender.location.ok_or(ConnectionError::LocationUnknown)?,
                                sender.peer,
                            );
                            log::debug!("Openned connection with peer {}", by_peer.peer);
                            new_state = None;
                        }
                    };
                }
                JoinRingMsg::Connected { target, sender, id } => {
                    match self.state {
                        Some(JRState::OCReceived) => {
                            log::debug!("Acknowledge connected at peer");
                            new_state = Some(JRState::Connected);
                            return_msg = None;
                        },
                        _ => return Err(OpError::InvalidStateTransition(self.id))
                    };
                    if let Some(state) = new_state.clone() {
                        if state.is_connected() {
                            return Err(OpError::InvalidStateTransition(id));
                        } else {
                            log::info!(
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
            };

            let new_op = Some(Self {
                id: self.id,
                state: new_state,
                gateway: self.gateway,
                backoff: self.backoff,
                _ttl: self._ttl
            });

            Ok(OperationResult {
                return_msg: return_msg.map(Message::from),
                state: new_op.map(OpEnum::JoinRing)})
        };
        Box::pin(fut)
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
                    target,
                    id,
                    ..
                },
            ) => {
                // the sender of the response is the target of the request and
                // is only a completed tx if it accepted the connection
                if accepted_by.contains(sender) {
                    log::debug!(
                        "Return to {}, connected at proxy {} (tx: {})",
                        target.peer,
                        sender.peer,
                        id
                    );
                    Some(JRState::Connected)
                } else {
                    log::debug!("Failed to connect at proxy {}", sender.peer);
                    None
                }
            }
            // interism status (proxy + gw)
            (
                JRState::AwaitingProxyResponse {
                    accepted_by: previously_accepted,
                    new_peer_id,
                    target,
                    ..
                },
                JoinRingMsg::Response {
                    msg: JoinResponse::Proxy { accepted_by },
                    ..
                },
            ) if !accepted_by.is_empty() => {
                previously_accepted.extend(accepted_by.drain());
                if new_peer_id == &target.peer {
                    Some(JRState::OCReceived)
                } else {
                    // for proxies just consider the connection open directly
                    // what would happen in case that the connection is not confirmed end-to-end
                    // is that we would end up with a dead connection;
                    // this then must be dealed with by the normal mechanisms that keep
                    // connections alive and prune any dead connections
                    Some(JRState::Connected)
                }
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
                    new_peer_id,
                },
                JoinRingMsg::Response {
                    id,
                    msg: JoinResponse::Proxy { .. },
                    target: this_peer,
                    ..
                },
            ) => {
                // returning message from a proxy action to the previous requester
                let resp = if new_peer_id == target.peer {
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
                            your_peer_id: new_peer_id,
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
            ) => Some(JoinRingMsg::Response {
                msg: JoinResponse::Proxy { accepted_by },
                sender,
                id,
                target,
            }),
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

mod states {
    use std::fmt::Display;
    use super::*;

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

    impl Display for JRState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Initializing => write!(f, "Initializing"),
                Self::Connecting {
                    0: ConnectionInfo
                } => write!(f, "Connecting(info: {})", 0),
                Self::AwaitingProxyResponse { .. } => write!("AwaitingProxyResponse"),
                Self::OCReceived => write!("OCReceived"),
                Self::Connected => write!("Connected")
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum JoinOpError {
    #[error("no capacity left")]
    NoCapacityLeft,
}

pub(crate) fn initial_request(
    this_peer: PeerKey,
    gateway: PeerKeyLocation,
    max_hops_to_live: usize,
    id: Transaction,
) -> JoinRingOp {
    log::debug!("Connecting to gw {} from {}", gateway.peer, this_peer);
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
    } = join_op.state.as_mut().expect("Infallible").clone().try_unwrap_connecting()?;

    log::info!(
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
    op_storage.push(tx, OpEnum::JoinRing(join_op))?;
    Ok(())
}

// FIXME: don't forward to previously rejecting nodes (keep track of skip list)
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
            "Randomly selecting peer to forward JoinRequest (requester: {})",
            req_peer.peer
        );
        ring.random_peer(|p| p.peer != req_peer.peer)
    } else {
        log::debug!(
            "Selecting close peer to forward request (requester: {})",
            req_peer.peer
        );
        match ring
            .routing(
                &new_peer_loc.location.unwrap(),
                Some(&req_peer.peer),
                1,
                &[],
            )
            .pop()
        {
            Some(pkl) => {
                if pkl.peer == new_peer_loc.peer {
                    // concurrently this peer was connected already
                    None
                } else {
                    Some(pkl)
                }
            }
            None => None,
        }
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
            log::warn!(
                "Unable to forward, will only be connected to one peer (tx: {})",
                id
            );
        } else {
            log::warn!("Unable to forward or accept any connections (tx: {})", id);
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

    use super::*;
    use crate::{
        contract::SimStoreError,
        message::TxType,
        node::test::{check_connectivity, SimNetwork},
    };

    #[test]
    fn successful_join_ring_seq() {
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

        let mut join_gw_1 = initial_request(new_peer.peer, gateway, 0, id).sm;
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
            .consume_to_output::<SimStoreError>(req)
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
            .consume_to_output::<SimStoreError>(res)
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
            .consume_to_output::<SimStoreError>(res)
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
            .consume_to_output::<SimStoreError>(res.clone())
            .unwrap()
            .is_none());
        assert!(matches!(join_gw_1.state(), JRState::Connected { .. }));

        // transaction finished, should not return anymore
        assert!(join_new_peer_2
            .consume_to_output::<SimStoreError>(res.clone())
            .is_err());
        assert!(join_gw_1.consume_to_output::<SimStoreError>(res).is_err());
    }

    /// Given a network of one node and one gateway test that both are connected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_node_connects_to_gw() {
        let mut sim_nodes = SimNetwork::new(1, 1, 1, 1, 2, 2);
        sim_nodes.build().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(sim_nodes.connected("node-0"));
    }

    /// Once a gateway is left without remaining open slots, ensure forwarding connects
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forward_connection_to_node() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 10usize;
        const NUM_GW: usize = 1usize;
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 2);
        sim_nodes.build().await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await
    }

    /// Given a network of N peers all nodes should have connections.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_nodes_should_connect() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 10usize;
        const NUM_GW: usize = 1usize;
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 1000, 2);
        sim_nodes.build().await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(10)).await
    }
}
