//! Operation which seeks new connections in the ring.
use std::borrow::Borrow;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::client_api::HostResponse;
use futures::Future;

use super::{OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::client_events::HostResult;
use crate::dev_tool::Location;
use crate::message::{NetMessageV1, NodeEvent};
use crate::ring::ConnectionManager;
use crate::router::Router;
use crate::transport::TransportPublicKey;
use crate::{
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager, PeerId},
    operations::OpEnum,
    ring::PeerKeyLocation,
    util::ExponentialBackoff,
};

pub(crate) use self::messages::{ConnectMsg, ConnectRequest, ConnectResponse};

#[derive(Debug)]
pub(crate) struct ConnectOp {
    id: Transaction,
    state: Option<ConnectState>,
    pub gateway: Option<Box<PeerKeyLocation>>,
    /// keeps track of the number of retries and applies an exponential backoff cooldown period
    pub backoff: Option<ExponentialBackoff>,
}

impl ConnectOp {
    pub fn new(
        id: Transaction,
        state: Option<ConnectState>,
        gateway: Option<Box<PeerKeyLocation>>,
        backoff: Option<ExponentialBackoff>,
    ) -> Self {
        Self {
            id,
            state,
            gateway,
            backoff,
        }
    }

    pub fn has_backoff(&self) -> bool {
        self.backoff.is_some()
    }

    pub(super) fn outcome(&self) -> OpOutcome {
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, Some(ConnectState::Connected))
    }

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
                    Some(Box::new(op_manager.ring.connection_manager.own_location()))
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
                    let target = target.as_ref().map(|b| b.borrow());
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
        mut self,
        network_bridge: &'a mut NB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let new_state;

            match input {
                ConnectMsg::Request {
                    msg:
                        ConnectRequest::FindOptimalPeer {
                            query_target,
                            ideal_location,
                            joiner,
                            max_hops_to_live,
                            skip_list,
                        },
                    id,
                    ..
                } => {
                    let own_loc = op_manager.ring.connection_manager.own_location();
                    let PeerKeyLocation {
                        peer: this_peer,
                        location: Some(_),
                    } = &own_loc
                    else {
                        return Err(OpError::RingError(crate::ring::RingError::NoLocation));
                    };
                    let mut skip_list = skip_list.clone();
                    skip_list.extend([this_peer.clone(), query_target.peer.clone()]);
                    if this_peer == &query_target.peer {
                        // this peer should be the original target queries
                        tracing::debug!(
                            tx = %id,
                            query_target = %query_target.peer,
                            joiner = %joiner.peer,
                            skip_list = ?skip_list,
                            "Got queried for new connections from joiner",
                        );
                        if let Some(desirable_peer) = op_manager
                            .ring
                            .closest_to_location(*ideal_location, &[joiner.peer.clone()])
                        {
                            tracing::debug!(
                                tx = %id,
                                query_target = %query_target.peer,
                                joiner = %joiner.peer,
                                desirable_peer = %desirable_peer.peer,
                                "Found a desirable peer to connect to",
                            );
                            let msg = create_forward_message(
                                *id,
                                &own_loc,
                                joiner,
                                &desirable_peer,
                                *max_hops_to_live,
                                *max_hops_to_live,
                                skip_list,
                            );
                            network_bridge.send(&desirable_peer.peer, msg).await?;
                            return_msg = None;
                            new_state = Some(ConnectState::AwaitingConnectionAcquisition {});
                        } else {
                            tracing::debug!(
                                tx = %id,
                                query_target = %query_target.peer,
                                joiner = %joiner.peer,
                                "No desirable peer found to connect to",
                            );
                            return_msg = None;
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
                        new_state = Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                            remaining_connetions: *max_hops_to_live,
                        }));
                        let msg = ConnectMsg::Request {
                            id: *id,
                            target: query_target.clone(),
                            msg: ConnectRequest::FindOptimalPeer {
                                query_target: query_target.clone(),
                                ideal_location: *ideal_location,
                                joiner: joiner.clone(),
                                max_hops_to_live: *max_hops_to_live,
                                skip_list,
                            },
                        };
                        network_bridge.send(&query_target.peer, msg.into()).await?;
                        return_msg = None;
                    }
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
                            ..
                        },
                    ..
                } => {
                    if sender.peer == joiner.peer {
                        tracing::warn!(
                            tx = %id,
                            sender = %sender.peer,
                            joiner = %joiner.peer,
                            at = %op_manager.ring.connection_manager.own_location().peer,
                            "Connectivity check from self, aborting"
                        );
                        std::process::exit(1);
                    }
                    let this_peer = op_manager.ring.connection_manager.own_location();
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
                        .connection_manager
                        .should_accept(joiner_loc, &joiner.peer)
                    {
                        tracing::debug!(tx = %id, %joiner, "Accepting connection from");
                        let (callback, mut result) = tokio::sync::mpsc::channel(1);
                        // Attempt to connect to the joiner
                        op_manager
                            .notify_node_event(NodeEvent::ConnectPeer {
                                peer: joiner.peer.clone(),
                                tx: *id,
                                callback,
                                is_gw: false,
                            })
                            .await?;
                        if result
                            .recv()
                            .await
                            .ok_or(OpError::NotificationError)?
                            .is_ok()
                        {
                            let was_reserved = {
                                // reserved just above in call to should_accept
                                true
                            };
                            // Add the connection to the ring
                            op_manager
                                .ring
                                .add_connection(joiner_loc, joiner.peer.clone(), was_reserved)
                                .await;
                            true
                        } else {
                            // If the connection was not completed, prune the reserved connection
                            op_manager
                                .ring
                                .connection_manager
                                .prune_in_transit_connection(&joiner.peer);
                            false
                        }
                    } else {
                        tracing::debug!(tx = %id, at = %this_peer.peer, from = %joiner, "Rejecting connection");
                        false
                    };

                    {
                        if let Some(updated_state) = forward_conn(
                            *id,
                            &op_manager.ring.connection_manager,
                            op_manager.ring.router.clone(),
                            network_bridge,
                            ForwardParams {
                                left_htl: *hops_to_live,
                                max_htl: *max_hops_to_live,
                                accepted: should_accept,
                                skip_list: skip_list.clone(),
                                req_peer: sender.clone(),
                                joiner: joiner.clone(),
                            },
                        )
                        .await?
                        {
                            new_state = Some(updated_state);
                        } else {
                            new_state = None
                        }
                    }

                    let response = ConnectResponse::AcceptedBy {
                        accepted: should_accept,
                        acceptor: this_peer.clone(),
                        joiner: joiner.peer.clone(),
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
                    msg:
                        ConnectResponse::AcceptedBy {
                            accepted,
                            acceptor,
                            joiner,
                        },
                } => {
                    tracing::debug!(
                        tx = %id,
                        at = %target.peer,
                        from = %sender.peer,
                        "Connect response received",
                    );

                    let this_peer_id = op_manager
                        .ring
                        .connection_manager
                        .get_peer_key()
                        .expect("peer id not found");

                    match self.state.as_mut() {
                        Some(ConnectState::ConnectingToNode(info)) => {
                            assert!(info.remaining_connections > 0);
                            let remaining_connetions = info.remaining_connections.saturating_sub(1);

                            if *accepted {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    connectect_to = %acceptor.peer,
                                    "Open connection acknowledged at requesting joiner peer",
                                );
                                info.accepted_by.insert(acceptor.clone());
                                op_manager
                                    .ring
                                    .add_connection(
                                        acceptor.location.expect("location not found"),
                                        acceptor.peer.clone(),
                                        true, // we reserved the connection to this peer before asking to join
                                    )
                                    .await;
                            } else {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    rejected_peer = %acceptor.peer,
                                    "Connection rejected",
                                );
                            }

                            let your_location: Location =
                                target.location.expect("location not found");
                            tracing::debug!(
                                tx = %id,
                                at = %this_peer_id,
                                location = %your_location,
                                "Updating assigned location"
                            );
                            op_manager
                                .ring
                                .connection_manager
                                .update_location(target.location);

                            if remaining_connetions == 0 {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    "All available connections established",
                                );

                                try_clean_gw_connection(*id, network_bridge, info, target.clone())
                                    .await?;

                                new_state = Some(ConnectState::Connected);
                            } else {
                                new_state = Some(ConnectState::ConnectingToNode(info.clone()));
                            }
                            return_msg = None;
                        }
                        Some(ConnectState::AwaitingConnectivity(ConnectivityInfo {
                            remaining_checks,
                            requester,
                        })) => {
                            assert!(*remaining_checks > 0);
                            let remaining_checks = remaining_checks.saturating_sub(1);

                            tracing::debug!(
                                tx = %id,
                                at = %this_peer_id,
                                from = %sender.peer,
                                acceptor = %acceptor.peer,
                                accepted = %accepted,
                                "Connectivity check",
                            );

                            if remaining_checks == 0 {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    "All connectivity checks done",
                                );
                                new_state = None;
                            } else {
                                new_state =
                                    Some(ConnectState::AwaitingConnectivity(ConnectivityInfo {
                                        remaining_checks,
                                        requester: requester.clone(),
                                    }));
                            }
                            let response = ConnectResponse::AcceptedBy {
                                accepted: *accepted,
                                acceptor: acceptor.clone(),
                                joiner: joiner.clone(),
                            };
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                sender: target.clone(),
                                msg: response,
                                target: requester.clone(),
                            });
                        }
                        Some(ConnectState::AwaitingNewConnection(info)) => {
                            tracing::debug!(
                                tx = %id,
                                at = %this_peer_id,
                                from = %sender.peer,
                                "Connection request forwarded",
                            );
                            assert!(info.remaining_connetions > 0);
                            let remaining_connetions = info.remaining_connetions.saturating_sub(1);

                            if remaining_connetions == 0 {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    "All available connections established",
                                );
                                op_manager
                                    .ring
                                    .live_tx_tracker
                                    .missing_candidate_peers(this_peer_id)
                                    .await;
                                new_state = None;
                            } else {
                                new_state =
                                    Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                                        remaining_connetions,
                                    }));
                            }

                            return_msg = None;
                        }
                        _ => {
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
                                .notify_op_change(
                                    NetMessage::V1(NetMessageV1::Aborted(*id)),
                                    OpEnum::Connect(op.into()),
                                )
                                .await?;
                            return Err(OpError::StatePushed);
                        }
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
    tracing::debug!(tx = %id, ?msg, "Connect operation result");
    let output_op = Some(OpEnum::Connect(Box::new(ConnectOp {
        id,
        state,
        gateway,
        backoff,
    })));
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        state: output_op,
    })
}

async fn try_clean_gw_connection<NB>(
    id: Transaction,
    conn_bridge: &mut NB,
    state: &mut ConnectionInfo,
    joiner: PeerKeyLocation,
) -> Result<(), OpError>
where
    NB: NetworkBridge,
{
    let need_to_clean_gw_conn = state
        .accepted_by
        .iter()
        .all(|pkloc| pkloc.peer != state.gateway.peer);

    if need_to_clean_gw_conn {
        let msg = ConnectMsg::Request {
            id,
            target: state.gateway.clone(),
            msg: ConnectRequest::CleanConnection { joiner },
        };
        conn_bridge.send(&state.gateway.peer, msg.into()).await?;
    }
    Ok(())
}

type Requester = PeerKeyLocation;

#[derive(Debug)]
pub enum ConnectState {
    Initializing,
    ConnectingToNode(ConnectionInfo),
    AwaitingConnectivity(ConnectivityInfo),
    AwaitingConnectionAcquisition,
    AwaitingNewConnection(NewConnectionInfo),
    Connected,
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectivityInfo {
    remaining_checks: usize,
    requester: Requester,
}

impl ConnectivityInfo {
    pub fn new(requester: Requester, remaining_checks: usize) -> Self {
        Self {
            requester,
            remaining_checks,
        }
    }

    /// Decrements the remaining checks and returns whether the checks are complete.
    pub fn decrement_check(&mut self) -> bool {
        self.remaining_checks = self.remaining_checks.saturating_sub(1);
        self.remaining_checks == 0
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectionInfo {
    gateway: PeerKeyLocation,
    accepted_by: HashSet<PeerKeyLocation>,
    remaining_connections: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct NewConnectionInfo {
    remaining_connetions: usize,
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
///   After the initial connections through the gateways are established all other connections
///   (to gateways or regular peers) will be treated as regular connections.
///
/// - is_gateway: Whether this peer is a gateway or not.
pub(crate) async fn initial_join_procedure(
    op_manager: Arc<OpManager>,
    gateways: &[PeerKeyLocation],
) -> Result<(), OpError> {
    use crate::util::IterExt;
    let number_of_parallel_connections = {
        let max_potential_conns_per_gw = op_manager.ring.max_hops_to_live;
        // e.g. 10 gateways and htl 5 -> only need 2 connections in parallel
        let needed_to_cover_max =
            op_manager.ring.connection_manager.max_connections / max_potential_conns_per_gw;
        gateways.iter().take(needed_to_cover_max).count().max(1)
    };
    let gateways = gateways.to_vec();
    tokio::task::spawn(async move {
        if gateways.is_empty() {
            tracing::warn!("No gateways available, aborting join procedure");
            return;
        }
        loop {
            if op_manager.ring.open_connections() == 0 {
                tracing::info!(
                    "Attempting to connect to {} gateways in parallel",
                    number_of_parallel_connections
                );
                for gateway in op_manager
                    .ring
                    .is_not_connected(gateways.iter())
                    .shuffle()
                    .take(number_of_parallel_connections)
                {
                    tracing::info!(%gateway, "Attempting connection to gateway");
                    if let Err(error) = join_ring_request(None, gateway, &op_manager).await {
                        if !matches!(
                            error,
                            OpError::ConnError(crate::node::ConnectionError::UnwantedConnection)
                        ) {
                            tracing::error!(%error, "Failed while attempting connection to gateway");
                        }
                    }
                }
            }
            #[cfg(debug_assertions)]
            const WAIT_TIME: u64 = 15;
            #[cfg(not(debug_assertions))]
            const WAIT_TIME: u64 = 3;
            tokio::time::sleep(Duration::from_secs(WAIT_TIME)).await;
        }
    });
    Ok(())
}

#[tracing::instrument(fields(peer = %op_manager.ring.connection_manager.pub_key), skip_all)]
pub(crate) async fn join_ring_request(
    backoff: Option<ExponentialBackoff>,
    gateway: &PeerKeyLocation,
    op_manager: &OpManager,
) -> Result<(), OpError> {
    use crate::node::ConnectionError;
    if !op_manager.ring.connection_manager.should_accept(
        gateway.location.ok_or_else(|| {
            tracing::error!(
                "Gateway location not found, this should not be possible, report an error"
            );
            OpError::ConnError(ConnectionError::LocationUnknown)
        })?,
        &gateway.peer,
    ) {
        // ensure that we still want to connect AND reserve an spot implicitly
        return Err(OpError::ConnError(ConnectionError::UnwantedConnection));
    }

    let tx_id = Transaction::new::<ConnectMsg>();
    tracing::info!(%gateway.peer, "Attempting network join");
    let mut op = initial_request(gateway.clone(), op_manager.ring.max_hops_to_live, tx_id);
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
    connect_request(tx_id, op_manager, op).await?;
    Ok(())
}

fn initial_request(
    gateway: PeerKeyLocation,
    max_hops_to_live: usize,
    id: Transaction,
) -> ConnectOp {
    const MAX_JOIN_RETRIES: usize = usize::MAX;
    let state = ConnectState::ConnectingToNode(ConnectionInfo {
        gateway: gateway.clone(),
        accepted_by: HashSet::new(),
        remaining_connections: max_hops_to_live,
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
async fn connect_request(
    tx: Transaction,
    op_manager: &OpManager,
    join_op: ConnectOp,
) -> Result<(), OpError> {
    let ConnectOp {
        id, state, backoff, ..
    } = join_op;
    let ConnectionInfo { gateway, .. } = state.expect("infallible").try_unwrap_connecting()?;

    tracing::info!(
        tx = %id,
        gateway = %gateway,
        "Connecting to gateway",
    );

    let (callback, mut result) = tokio::sync::mpsc::channel(1);
    op_manager
        .notify_node_event(NodeEvent::ConnectPeer {
            peer: gateway.peer.clone(),
            tx,
            callback,
            is_gw: true,
        })
        .await?;
    match result.recv().await.ok_or(OpError::NotificationError)? {
        Ok((joiner, remaining_checks)) => {
            op_manager
                .ring
                .add_connection(
                    gateway.location.expect("location not found"),
                    gateway.peer.clone(),
                    true,
                )
                .await;
            let Some(remaining_connetions) = remaining_checks else {
                tracing::error!(tx = %id, "Failed to connect to gateway, missing remaining checks");
                return Err(OpError::ConnError(
                    crate::node::ConnectionError::FailedConnectOp,
                ));
            };
            tracing::debug!(
                tx = %id,
                gateway = %gateway,
                joiner = %joiner,
                "Sending connection request to gateway",
            );
            // at this point the gateway has accepted the connection so already
            // sent StartJoinReq to the gateway and are waiting for new upstream connections
            op_manager
                .push(
                    tx,
                    OpEnum::Connect(Box::new(ConnectOp {
                        id,
                        state: Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                            remaining_connetions,
                        })),
                        gateway: Some(Box::new(gateway)),
                        backoff,
                    })),
                )
                .await?;
            Ok(())
        }
        Err(_) => Err(OpError::ConnError(
            crate::node::ConnectionError::FailedConnectOp,
        )),
    }
}

pub(crate) struct ForwardParams {
    pub left_htl: usize,
    pub max_htl: usize,
    pub accepted: bool,
    pub skip_list: Vec<PeerId>,
    pub req_peer: PeerKeyLocation,
    pub joiner: PeerKeyLocation,
}

pub(crate) async fn forward_conn<NB>(
    id: Transaction,
    connection_manager: &ConnectionManager,
    router: Arc<parking_lot::RwLock<Router>>,
    network_bridge: &mut NB,
    params: ForwardParams,
) -> Result<Option<ConnectState>, OpError>
where
    NB: NetworkBridge,
{
    let ForwardParams {
        left_htl,
        max_htl,
        accepted,
        mut skip_list,
        req_peer,
        joiner,
    } = params;
    if left_htl == 0 {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Couldn't forward connect petition, no hops left or enough connections",
        );
        return Ok(None);
    }

    if connection_manager.num_connections() == 0 {
        tracing::warn!(
            tx = %id,
            joiner = %joiner.peer,
            "Couldn't forward connect petition, not enough connections",
        );
        return Ok(None);
    }

    let target_peer = {
        let router = router.read();
        select_forward_target(
            id,
            connection_manager,
            &router,
            &req_peer,
            &joiner,
            left_htl,
            &skip_list,
        )
    };
    skip_list.push(req_peer.peer.clone());
    match target_peer {
        Some(target_peer) => {
            let forward_msg = create_forward_message(
                id,
                &req_peer,
                &joiner,
                &target_peer,
                left_htl,
                max_htl,
                skip_list,
            );
            tracing::debug!(target: "network", "Forwarding connection request to {:?}", target_peer);
            network_bridge.send(&target_peer.peer, forward_msg).await?;
            update_state_with_forward_info(&req_peer, left_htl)
        }
        None => handle_unforwardable_connection(id, accepted),
    }
}

fn select_forward_target(
    id: Transaction,
    connection_manager: &ConnectionManager,
    router: &Router,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    left_htl: usize,
    skip_list: &[PeerId],
) -> Option<PeerKeyLocation> {
    if left_htl >= connection_manager.rnd_if_htl_above {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Randomly selecting peer to forward connect request",
        );
        connection_manager.random_peer(|p| !skip_list.contains(p))
    } else {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Selecting close peer to forward request",
        );
        connection_manager
            .routing(
                joiner.location.unwrap(),
                Some(&request_peer.peer),
                skip_list,
                router,
            )
            .and_then(|pkl| (pkl.peer != joiner.peer).then_some(pkl))
    }
}

fn create_forward_message(
    id: Transaction,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    target: &PeerKeyLocation,
    hops_to_live: usize,
    max_hops_to_live: usize,
    skip_list: Vec<PeerId>,
) -> NetMessage {
    NetMessage::from(ConnectMsg::Request {
        id,
        target: target.clone(),
        msg: ConnectRequest::CheckConnectivity {
            sender: request_peer.clone(),
            joiner: joiner.clone(),
            hops_to_live: hops_to_live.saturating_sub(1), // decrement the hops to live for the next hop
            max_hops_to_live,
            skip_list,
        },
    })
}

fn update_state_with_forward_info(
    requester: &PeerKeyLocation,
    left_htl: usize,
) -> Result<Option<ConnectState>, OpError> {
    let connecivity_info = ConnectivityInfo::new(requester.clone(), left_htl);
    let new_state = ConnectState::AwaitingConnectivity(connecivity_info);
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

mod messages {
    use std::fmt::Display;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum ConnectMsg {
        Request {
            id: Transaction,
            target: PeerKeyLocation,
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

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            use ConnectMsg::*;
            match self {
                Request { target, .. } => Some(target),
                Response { target, .. } => Some(target),
                Connected { target, .. } => Some(target),
            }
        }

        fn requested_location(&self) -> Option<Location> {
            self.target().and_then(|pkloc| pkloc.borrow().location)
        }
    }

    impl ConnectMsg {
        pub fn sender(&self) -> Option<&PeerId> {
            use ConnectMsg::*;
            match self {
                Response { sender, .. } => Some(&sender.peer),
                Connected { sender, .. } => Some(&sender.peer),
                Request { .. } => None,
            }
        }
    }

    impl Display for ConnectMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    target,
                    msg: ConnectRequest::StartJoinReq { .. },
                    ..
                } => write!(f, "StartRequest(id: {id}, target: {target})"),
                Self::Request {
                    target,
                    msg: ConnectRequest::CheckConnectivity {
                        sender,
                        joiner,
                        ..
                    },
                    ..
                } => write!(
                    f,
                    "CheckConnectivity(id: {id}, target: {target}, sender: {sender}, joiner: {joiner})"
                ),
                Self::Response {
                    target,
                    msg:
                        ConnectResponse::AcceptedBy {
                            accepted, acceptor, ..
                        },
                    ..
                } => write!(
                    f,
                    "AcceptedBy(id: {id}, target: {target}, accepted: {accepted}, acceptor: {acceptor})"
                ),
                Self::Connected { .. } => write!(f, "Connected(id: {id})"),
                ConnectMsg::Request { id, target, .. } => write!(f, "Request(id: {id}, target: {target})"),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectRequest {
        /// A request to join a gateway.
        StartJoinReq {
            // The peer who is trying to join, should be set when PeerConnection is established
            joiner: Option<PeerId>,
            joiner_key: TransportPublicKey,
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
            skip_list: Vec<PeerId>,
        },
        CheckConnectivity {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
            max_hops_to_live: usize,
            // The list of peers to skip when forwarding the connection request, avoiding loops
            skip_list: Vec<PeerId>,
        },
        CleanConnection {
            joiner: PeerKeyLocation,
        },
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum ConnectResponse {
        AcceptedBy {
            accepted: bool,
            acceptor: PeerKeyLocation,
            joiner: PeerId,
        },
    }
}
