//! Operation which seeks new connections in the ring.
use freenet_stdlib::client_api::HostResponse;
use futures::Future;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::time::Duration;

use super::{OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::client_events::HostResult;
use crate::dev_tool::Location;
use crate::ring::Ring;
use crate::transport::TransportPublicKey;
use crate::{
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager, PeerId},
    operations::OpEnum,
    ring::PeerKeyLocation,
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

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
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
                            .closest_to_location(*ideal_location, &[joiner.peer.clone()])
                        {
                            let msg = ConnectMsg::Request {
                                id: *id,
                                msg: ConnectRequest::CheckConnectivity {
                                    sender: own_loc.clone(),
                                    joiner: joiner.clone(),
                                    hops_to_live: *max_hops_to_live,
                                    max_hops_to_live: *max_hops_to_live,
                                    skip_list: vec![],
                                },
                            };
                            network_bridge
                                .send(&desirable_peer.peer, msg.into())
                                .await?;
                            return_msg = None;
                            new_state = Some(ConnectState::AwaitingConnectionAcquisition {
                                joiner: joiner.clone(),
                            });
                        } else {
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                sender: query_target.clone(),
                                target: joiner.clone(),
                                msg: ConnectResponse::AcceptedBy {
                                    accepted: false,
                                    joiner: joiner.clone(),
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
                            query_target: query_target.peer.clone(),
                        });
                        let msg = ConnectMsg::Request {
                            id: *id,
                            msg: ConnectRequest::FindOptimalPeer {
                                query_target: query_target.clone(),
                                ideal_location: *ideal_location,
                                joiner: joiner.clone(),
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
                        ConnectRequest::StartJoinReq {
                            joiner,
                            joiner_key,
                            assigned_location,
                            hops_to_live,
                            skip_list, //
                            ..
                        },
                } => {
                    let joiner = joiner
                        .clone()
                        .expect("should be already set at the p2p bridge level");
                    let this_peer = op_manager.ring.own_location();
                    let assigned_location = match assigned_location {
                        Some(location) => location,
                        None => &Location::random(),
                    };

                    let mut should_accept = if op_manager
                        .ring
                        .should_accept(*assigned_location, Some(&joiner))
                    {
                        tracing::debug!(
                            tx = %id,
                            at = %this_peer.peer,
                            hops_to_live = %hops_to_live,
                            joiner = %joiner,
                            accepted = %true,
                            "Connection request received"
                        );
                        true
                    } else {
                        tracing::debug!(tx = %id, at = %this_peer.peer, from = %joiner, "Rejecting connection");
                        false
                    };

                    let new_peer_loc = PeerKeyLocation {
                        location: Some(*assigned_location),
                        peer: joiner.clone(),
                    };

                    let actual_state = self.state;

                    if let Some(mut updated_state) = forward_conn(
                        *id,
                        &op_manager.ring,
                        actual_state,
                        network_bridge,
                        (new_peer_loc.clone(), new_peer_loc.clone()),
                        *hops_to_live,
                        should_accept,
                        skip_list.clone(),
                    )
                    .await?
                    {
                        new_state = Some(updated_state);
                    } else {
                        tracing::debug!(tx = %id, at = %this_peer.peer, "Rejecting connection from {:?}", joiner);
                        new_state = None;
                        should_accept = false;
                    }

                    let response = ConnectResponse::AcceptedBy {
                        accepted: should_accept,
                        joiner: new_peer_loc.clone(),
                    };

                    return_msg = Some(ConnectMsg::Response {
                        id: *id,
                        sender: this_peer.clone(),
                        msg: response,
                        target: new_peer_loc,
                    });
                }
                ConnectMsg::Request {
                    id,
                    msg:
                        ConnectRequest::CheckConnectivity {
                            sender,
                            joiner,
                            hops_to_live,
                            max_hops_to_live,
                            skip_list,
                        },
                } => {
                    let this_peer = op_manager.ring.own_location();
                    let joiner_loc = joiner
                        .location
                        .expect("should be already set at the p2p bridge level");

                    tracing::debug!(
                        tx = %id,
                        at = %this_peer.peer,
                        hops_to_live = %hops_to_live,
                        joiner = %joiner,
                        "Checking connectivity request received"
                    );

                    let should_accept = if op_manager
                        .ring
                        .should_accept(joiner_loc, Some(&joiner.peer))
                    {
                        tracing::debug!(tx = %id, %joiner, "Accepting connection from");
                        true
                    } else {
                        tracing::debug!(tx = %id, at = %this_peer.peer, from = %joiner, "Rejecting connection");
                        false
                    };

                    let should_accept = op_manager
                        .ring
                        .should_accept(joiner_loc, Some(&joiner.peer));

                    let actual_state = self.state;

                    if let Some(mut updated_state) = forward_conn(
                        *id,
                        &op_manager.ring,
                        actual_state,
                        network_bridge,
                        (sender.clone(), joiner.clone()),
                        *hops_to_live,
                        should_accept,
                        skip_list.clone(),
                    )
                    .await?
                    {
                        new_state = Some(updated_state);
                    } else {
                        tracing::debug!(tx = %id, at = %this_peer.peer, "Rejecting connection from {:?}", joiner);
                        new_state = None
                    }

                    let response = ConnectResponse::AcceptedBy {
                        accepted: should_accept,
                        joiner: joiner.clone(),
                    };

                    return_msg = Some(ConnectMsg::Response {
                        id: *id,
                        sender: this_peer.clone(),
                        msg: response,
                        target: sender.clone(),
                    });
                }
                ConnectMsg::Response {
                    id,
                    sender,
                    target,
                    msg: ConnectResponse::AcceptedBy { accepted, joiner },
                } => {
                    tracing::debug!(
                        tx = %id,
                        at = %target.peer,
                        from = %sender.peer,
                        "Connect response received",
                    );

                    let this_peer_id = op_manager.ring.get_peer_key().expect("peer id not found");
                    let your_location = joiner.location.expect("location not found");

                    if this_peer_id == joiner.peer && *accepted {
                        tracing::debug!(
                            tx = %id,
                            at = %this_peer_id,
                            from = %sender.peer,
                            "Open connections acknowledged at requesting gateway peer",
                        );
                        return_msg = None;
                        new_state = Some(ConnectState::Connected);
                        tracing::debug!(
                            tx = %id,
                            at = %this_peer_id,
                            location = %your_location,
                            "Updating assigned location"
                        );
                        op_manager.ring.update_location(joiner.location);
                    } else if let Some(ConnectState::AwaitingConnectivity(ConnectivityInfo {
                        mut waiting_for,
                    })) = self.state
                    {
                        if waiting_for.contains_key(&joiner) {
                            let next_peer =
                                waiting_for.remove(&joiner).expect("No next peer found");
                            new_state =
                                Some(ConnectState::AwaitingConnectivity(ConnectivityInfo {
                                    waiting_for,
                                }));
                            let response = ConnectResponse::AcceptedBy {
                                accepted: *accepted,
                                joiner: joiner.clone(),
                            };
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                sender: target.clone(),
                                msg: response,
                                target: next_peer.clone(),
                            });
                        }
                    } else {
                        tracing::debug!(
                            tx = %id,
                            peer = %this_peer_id,
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

type Joiner = PeerKeyLocation;
type Target = PeerKeyLocation;

#[derive(Debug)]
enum ConnectState {
    Initializing,
    ConnectingToNode(ConnectionInfo),
    AwaitingConnectivity(ConnectivityInfo),
    AwaitingConnectionAcquisition { joiner: PeerKeyLocation },
    AwaitingNewConnection { query_target: PeerId },
    Connected,
}

#[derive(Debug, Clone)]
struct ConnectivityInfo {
    waiting_for: HashMap<Joiner, Target>,
}

#[derive(Debug, Clone)]
struct ConnectionInfo {
    gateway: PeerKeyLocation,
    this_peer: Option<PeerId>,
    peer_pub_key: TransportPublicKey,
    max_hops_to_live: usize,
}

impl ConnectState {
    fn try_unwrap_connecting(self) -> Result<ConnectionInfo, OpError> {
        if let Self::ConnectingToNode(conn_info) = self {
            Ok(conn_info)
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
    peer_pub_key: TransportPublicKey,
    gateways: &[PeerKeyLocation],
) -> Result<(), OpError>
where
    CM: NetworkBridge + Send,
{
    use crate::util::IterExt;
    let number_of_parallel_connections = {
        let max_potential_conns_per_gw = op_manager.ring.max_hops_to_live;
        // e.g. 10 gateways and htl 5 -> only need 2 connections in parallel
        let needed_to_cover_max = gateways.iter().count() / max_potential_conns_per_gw;
        needed_to_cover_max.max(1)
    };
    tracing::info!(
        "Attempting to connect to {} gateways in parallel",
        number_of_parallel_connections
    );
    for gateway in gateways
        .iter()
        .shuffle()
        .take(number_of_parallel_connections)
    {
        join_ring_request(
            None,
            peer_pub_key.clone(),
            gateway,
            op_manager,
            conn_manager,
        )
        .await?;
    }
    Ok(())
}

pub(crate) async fn join_ring_request<CM>(
    backoff: Option<ExponentialBackoff>,
    peer_pub_key: TransportPublicKey,
    gateway: &PeerKeyLocation,
    op_manager: &OpManager,
    conn_manager: &mut CM,
) -> Result<(), OpError>
where
    CM: NetworkBridge + Send,
{
    let tx_id = Transaction::new::<ConnectMsg>();
    let mut op = initial_request(
        peer_pub_key,
        gateway.clone(),
        op_manager.ring.max_hops_to_live,
        tx_id,
    );
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
    peer_pub_key: TransportPublicKey,
    gateway: PeerKeyLocation,
    max_hops_to_live: usize,
    id: Transaction,
) -> ConnectOp {
    const MAX_JOIN_RETRIES: usize = usize::MAX;
    let state = ConnectState::ConnectingToNode(ConnectionInfo {
        gateway: gateway.clone(),
        this_peer: None,
        peer_pub_key: peer_pub_key.clone(),
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
        peer_pub_key,
        max_hops_to_live,
    } = state.expect("infallible").try_unwrap_connecting()?;

    tracing::info!(
        tx = %id,
        this_peer = %this_peer.unwrap(),
        gateway = %gateway,
        "Connecting to gateway",
    );

    conn_bridge.try_add_connection(gateway.peer.clone()).await?;
    let join_req = NetMessage::from(messages::ConnectMsg::Request {
        id: tx,
        msg: ConnectRequest::StartJoinReq {
            joiner: None,
            joiner_key: peer_pub_key.clone(),
            hops_to_live: max_hops_to_live,
            assigned_location: None,
            max_hops_to_live,
            skip_list: vec![],
        },
    });
    conn_bridge.send(&gateway.peer, join_req).await?;
    op_manager
        .push(
            tx,
            OpEnum::Connect(Box::new(ConnectOp {
                id,
                state: Some(ConnectState::ConnectingToNode(ConnectionInfo {
                    gateway: gateway.clone(),
                    this_peer: None,
                    peer_pub_key,
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
    state: Option<ConnectState>,
    network_bridge: &mut NB,
    (req_peer, joiner): (PeerKeyLocation, PeerKeyLocation),
    left_htl: usize,
    accepted: bool,
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

    let target_peer = select_forward_target(id, ring, &req_peer, &joiner, left_htl, &skip_list);
    match target_peer {
        Some(target_peer) => {
            let forward_msg = create_forward_message(
                id,
                &req_peer,
                &joiner,
                left_htl - 1,
                &[req_peer.peer.clone()],
            );
            tracing::debug!(target: "network", "Forwarding connection request to {:?}", target_peer);
            network_bridge.send(&target_peer.peer, forward_msg).await?;
            update_state_with_forward_info(state, &joiner, target_peer)
        }
        None => handle_unforwardable_connection(id, accepted),
    }
}

fn select_forward_target(
    id: Transaction,
    ring: &Ring,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    left_htl: usize,
    skip_list: &[PeerId],
) -> Option<PeerKeyLocation> {
    if left_htl >= ring.rnd_if_htl_above {
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
        ring.routing(
            joiner.location.unwrap(),
            Some(&request_peer.peer),
            skip_list,
        )
        .and_then(|pkl| (pkl.peer != joiner.peer).then_some(pkl))
    }
}

fn create_forward_message(
    id: Transaction,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    hops_to_live: usize,
    skip_list: &[PeerId],
) -> NetMessage {
    NetMessage::from(ConnectMsg::Request {
        id,
        msg: ConnectRequest::CheckConnectivity {
            sender: request_peer.clone(),
            joiner: joiner.clone(),
            hops_to_live,
            max_hops_to_live: hops_to_live,
            skip_list: skip_list.to_vec(),
        },
    })
}

fn update_state_with_forward_info(
    state: Option<ConnectState>,
    joiner: &PeerKeyLocation,
    forward_to: PeerKeyLocation,
) -> Result<Option<ConnectState>, OpError> {
    let new_state = match state {
        Some(ConnectState::AwaitingConnectivity(conn_info)) => {
            update_connectivity_info(conn_info, joiner, forward_to)
        }
        _ => ConnectState::AwaitingConnectivity(ConnectivityInfo {
            waiting_for: HashMap::from_iter([(joiner.clone(), forward_to.clone())]),
        }),
    };
    Ok(Some(new_state))
}

fn handle_unforwardable_connection(
    id: Transaction,
    accepted: bool,
) -> Result<Option<ConnectState>, OpError> {
    if accepted {
        tracing::warn!(
            tx = %id,
            "Unable to forward, will only be connecting to one peer",
        );
    } else {
        tracing::warn!(tx = %id, "Unable to forward or accept any connections");
    }
    Ok(None)
}

fn update_connectivity_info(
    mut conn_info: ConnectivityInfo,
    joiner: &PeerKeyLocation,
    forward_to: PeerKeyLocation,
) -> ConnectState {
    let mut waiting_peers = conn_info.waiting_for.clone();
    waiting_peers
        .entry(joiner.clone())
        .or_insert_with(|| forward_to.clone());
    ConnectState::AwaitingConnectivity(ConnectivityInfo {
        waiting_for: waiting_peers,
    })
}

mod messages {
    use std::fmt::Display;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
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
                    msg:
                        ConnectRequest::StartJoinReq {
                            joiner,
                            assigned_location,
                            ..
                        },
                    ..
                } => {
                    if let Some(peer) = joiner {
                        Some(&PeerKeyLocation {
                            peer: peer.clone(),
                            location: assigned_location.clone(),
                        })
                    } else {
                        None
                    }
                }
                Connected { target, .. } => Some(target),
                _ => None,
            }
        }

        fn terminal(&self) -> bool {
            use ConnectMsg::*;
            matches!(
                self,
                Response {
                    msg: ConnectResponse::AcceptedBy { .. },
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
                Request { .. } => None,
                _ => None,
            }
        }
    }

    impl Display for ConnectMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    msg: ConnectRequest::StartJoinReq { .. },
                    ..
                } => write!(f, "StartRequest(id: {id})"),
                Self::Response {
                    msg: ConnectResponse::AcceptedBy { .. },
                    ..
                } => write!(f, "RouteValue(id: {id})"),
                Self::Connected { .. } => write!(f, "Connected(id: {id})"),
                _ => unimplemented!(),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectRequest {
        StartJoinReq {
            // The peer who is trying to join, should be set when PeerConnection is established
            joiner: Option<PeerId>,
            joiner_key: TransportPublicKey,
            /// The location assigned to the joiner, initially at gateway connection this is None
            assigned_location: Option<Location>,
            hops_to_live: usize,
            max_hops_to_live: usize,
            // The list of peers to skip when forwarding the connection request, avoiding loops
            skip_list: Vec<PeerId>,
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
        CheckConnectivity {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
            max_hops_to_live: usize,
            // The list of peers to skip when forwarding the connection request, avoiding loops
            skip_list: Vec<PeerId>,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectResponse {
        AcceptedBy {
            accepted: bool,
            joiner: PeerKeyLocation,
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
