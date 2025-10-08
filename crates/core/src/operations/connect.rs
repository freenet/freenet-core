//! Operation which seeks new connections in the ring.
use std::borrow::Borrow;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::client_api::HostResponse;
use futures::{Future, StreamExt};

pub(crate) use self::messages::{ConnectMsg, ConnectRequest, ConnectResponse};
use super::{connect, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::client_events::HostResult;
use crate::dev_tool::Location;
use crate::message::{NetMessageV1, NodeEvent};
use crate::node::IsOperationCompleted;
use crate::ring::ConnectionManager;
use crate::router::Router;
use crate::transport::TransportPublicKey;
use crate::{
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager, PeerId},
    operations::OpEnum,
    ring::PeerKeyLocation,
    util::Backoff,
};

#[derive(Debug)]
pub(crate) struct ConnectOp {
    id: Transaction,
    pub(crate) state: Option<ConnectState>,
    pub gateway: Option<Box<PeerKeyLocation>>,
    /// keeps track of the number of retries and applies an exponential backoff cooldown period
    pub backoff: Option<Backoff>,
}

impl ConnectOp {
    pub fn new(
        id: Transaction,
        state: Option<ConnectState>,
        gateway: Option<Box<PeerKeyLocation>>,
        backoff: Option<Backoff>,
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

    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, Some(ConnectState::Connected))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        // this shouldn't ever be called since clients can't request explicit connects
        Ok(HostResponse::Ok)
    }
}

impl IsOperationCompleted for ConnectOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(connect::ConnectState::Connected))
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
                            skip_connections,
                            skip_forwards,
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
                    let mut skip_connections = skip_connections.clone();
                    let mut skip_forwards = skip_forwards.clone();
                    skip_connections.extend([this_peer.clone(), query_target.peer.clone()]);
                    skip_forwards.extend([this_peer.clone(), query_target.peer.clone()]);
                    if this_peer == &query_target.peer {
                        // this peer should be the original target queries
                        tracing::info!(
                            tx = %id,
                            query_target = %query_target.peer,
                            joiner = %joiner.peer,
                            skip_connections_count = skip_connections.len(),
                            "Gateway received FindOptimalPeer request from joiner",
                        );
                        // Use the full skip_connections set to avoid recommending peers
                        // that the joiner is already connected to (including the gateway itself)
                        if let Some(desirable_peer) = op_manager.ring.closest_to_location(
                            *ideal_location,
                            skip_connections.iter().cloned().collect(),
                        ) {
                            tracing::info!(
                                tx = %id,
                                query_target = %query_target.peer,
                                joiner = %joiner.peer,
                                desirable_peer = %desirable_peer.peer,
                                "Gateway found desirable peer, forwarding to joiner",
                            );
                            let msg = create_forward_message(
                                *id,
                                &own_loc,
                                joiner,
                                &desirable_peer,
                                *max_hops_to_live,
                                *max_hops_to_live,
                                skip_connections,
                                skip_forwards,
                            );
                            network_bridge.send(&desirable_peer.peer, msg).await?;
                            return_msg = None;
                            new_state = Some(ConnectState::AwaitingConnectionAcquisition {});
                        } else {
                            tracing::warn!(
                                tx = %id,
                                query_target = %query_target.peer,
                                joiner = %joiner.peer,
                                "Gateway found no suitable peers to forward CheckConnectivity request",
                            );
                            // Send a negative response back to the joiner to inform them
                            // that no suitable peers are currently available
                            let response = ConnectResponse::AcceptedBy {
                                accepted: false,
                                acceptor: own_loc.clone(),
                                joiner: joiner.peer.clone(),
                            };
                            return_msg = Some(ConnectMsg::Response {
                                id: *id,
                                sender: own_loc.clone(),
                                target: joiner.clone(),
                                msg: response,
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
                        new_state = Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                            remaining_connections: *max_hops_to_live,
                        }));
                        let msg = ConnectMsg::Request {
                            id: *id,
                            target: query_target.clone(),
                            msg: ConnectRequest::FindOptimalPeer {
                                query_target: query_target.clone(),
                                ideal_location: *ideal_location,
                                joiner: joiner.clone(),
                                max_hops_to_live: *max_hops_to_live,
                                skip_connections,
                                skip_forwards,
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
                            skip_connections,
                            skip_forwards,
                            ..
                        },
                    ..
                } => {
                    if sender.peer == joiner.peer {
                        tracing::error!(
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
                        tracing::info!(tx = %id, %joiner, "CheckConnectivity: Accepting connection from, will trigger ConnectPeer");
                        let (callback, mut result) = tokio::sync::mpsc::channel(10);
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
                        let mut new_skip_list = skip_connections.clone();
                        new_skip_list.insert(this_peer.peer.clone());
                        if let Some(updated_state) = forward_conn(
                            *id,
                            &op_manager.ring.connection_manager,
                            op_manager.ring.router.clone(),
                            network_bridge,
                            ForwardParams {
                                left_htl: *hops_to_live,
                                max_htl: *max_hops_to_live,
                                accepted: should_accept,
                                skip_connections: skip_connections.clone(),
                                skip_forwards: skip_forwards.clone(),
                                req_peer: sender.clone(),
                                joiner: joiner.clone(),
                                is_gateway: op_manager.ring.is_gateway,
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
                            let remaining_connections =
                                info.remaining_connections.saturating_sub(1);

                            if *accepted {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    connected_to = %acceptor.peer,
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

                            if remaining_connections == 0 {
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
                            ..
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
                                new_state = Some(ConnectState::AwaitingConnectivity(
                                    ConnectivityInfo::new(requester.clone(), remaining_checks),
                                ));
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
                            assert!(info.remaining_connections > 0);
                            let remaining_connections =
                                info.remaining_connections.saturating_sub(1);

                            if remaining_connections == 0 {
                                tracing::debug!(
                                    tx = %id,
                                    at = %this_peer_id,
                                    from = %sender.peer,
                                    "All available connections established",
                                );
                                op_manager
                                    .ring
                                    .live_tx_tracker
                                    .missing_candidate_peers(sender.peer.clone())
                                    .await;
                                new_state = None;
                            } else {
                                new_state =
                                    Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                                        remaining_connections,
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
    backoff: Option<Backoff>,
) -> Result<OperationResult, OpError> {
    tracing::debug!(tx = %id, ?msg, "Connect operation result");
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        state: state.map(|state| {
            OpEnum::Connect(Box::new(ConnectOp {
                id,
                state: Some(state),
                gateway,
                backoff,
            }))
        }),
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
    /// Indicates this is a gateway bootstrap acceptance that should be registered immediately.
    /// See forward_conn() bootstrap logic and handshake handler for details.
    pub(crate) is_bootstrap_acceptance: bool,
}

impl ConnectivityInfo {
    pub fn new(requester: Requester, remaining_checks: usize) -> Self {
        Self {
            requester,
            remaining_checks,
            is_bootstrap_acceptance: false,
        }
    }

    pub fn new_bootstrap(requester: Requester, remaining_checks: usize) -> Self {
        Self {
            requester,
            remaining_checks,
            is_bootstrap_acceptance: true,
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
    remaining_connections: usize,
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
        // if we have 2 gws, we will at least attempt 2 parallel connections
        gateways.iter().take(needed_to_cover_max).count().max(2)
    };
    let gateways = gateways.to_vec();
    tokio::task::spawn(async move {
        if gateways.is_empty() {
            tracing::warn!("No gateways available, aborting join procedure");
            return;
        }

        const WAIT_TIME: u64 = 1;
        const LONG_WAIT_TIME: u64 = 30;
        const BOOTSTRAP_THRESHOLD: usize = 4;

        tracing::info!(
            "Starting initial join procedure with {} gateways",
            gateways.len()
        );

        loop {
            let open_conns = op_manager.ring.open_connections();
            let unconnected_gateways: Vec<_> =
                op_manager.ring.is_not_connected(gateways.iter()).collect();

            tracing::debug!(
                "Connection status: open_connections = {}, unconnected_gateways = {}",
                open_conns,
                unconnected_gateways.len()
            );

            // Only try to connect to gateways if we have fewer than BOOTSTRAP_THRESHOLD connections
            // This prevents overloading gateways once peers have basic connectivity
            let unconnected_count = unconnected_gateways.len();

            if open_conns < BOOTSTRAP_THRESHOLD && unconnected_count > 0 {
                tracing::info!(
                    "Below bootstrap threshold ({} < {}), attempting to connect to {} gateways",
                    open_conns,
                    BOOTSTRAP_THRESHOLD,
                    number_of_parallel_connections.min(unconnected_count)
                );
                let select_all = futures::stream::FuturesUnordered::new();
                for gateway in unconnected_gateways
                    .into_iter()
                    .shuffle()
                    .take(number_of_parallel_connections)
                {
                    tracing::info!(%gateway, "Attempting connection to gateway");
                    let op_manager = op_manager.clone();
                    select_all.push(async move {
                        (join_ring_request(None, gateway, &op_manager).await, gateway)
                    });
                }
                select_all.for_each(|(res, gateway)| async move {
                    if let Err(error) = res {
                        if !matches!(
                            error,
                            OpError::ConnError(crate::node::ConnectionError::UnwantedConnection)
                        ) {
                            tracing::error!(%gateway, %error, "Failed while attempting connection to gateway");
                        }
                    }
                }).await;
            } else if open_conns >= BOOTSTRAP_THRESHOLD {
                tracing::trace!(
                    "Have {} connections (>= threshold of {}), not attempting gateway connections",
                    open_conns,
                    BOOTSTRAP_THRESHOLD
                );
            }

            // Determine wait time based on connection state
            let wait_time = if open_conns == 0 {
                // No connections at all - retry quickly
                tracing::debug!("No connections yet, waiting {}s before retry", WAIT_TIME);
                WAIT_TIME
            } else if open_conns < BOOTSTRAP_THRESHOLD {
                // Some connections but below threshold - moderate wait
                tracing::debug!(
                    "Have {} connections (below threshold of {}), waiting {}s",
                    open_conns,
                    BOOTSTRAP_THRESHOLD,
                    WAIT_TIME * 3
                );
                WAIT_TIME * 3
            } else {
                // Healthy connection pool - long wait
                tracing::trace!(
                    "Connection pool healthy ({} connections), waiting {}s",
                    open_conns,
                    LONG_WAIT_TIME
                );
                LONG_WAIT_TIME
            };

            tokio::time::sleep(Duration::from_secs(wait_time)).await;
        }
    });
    Ok(())
}

#[tracing::instrument(fields(peer = %op_manager.ring.connection_manager.pub_key), skip_all)]
pub(crate) async fn join_ring_request(
    backoff: Option<Backoff>,
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
        backoff: Some(Backoff::new(
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

    let (callback, mut result) = tokio::sync::mpsc::channel(10);
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
            let Some(remaining_connections) = remaining_checks else {
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

            // Update state to indicate we're waiting for new connections
            op_manager
                .push(
                    tx,
                    OpEnum::Connect(Box::new(ConnectOp {
                        id,
                        state: Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                            remaining_connections,
                        })),
                        gateway: Some(Box::new(gateway.clone())),
                        backoff,
                    })),
                )
                .await?;

            // After connecting to gateway, immediately request to find more peers
            // We'll create a new transaction for this follow-up request
            let new_tx_id = Transaction::new::<ConnectMsg>();
            let ideal_location = Location::random();
            let joiner_location = op_manager.ring.connection_manager.own_location();

            // Track this transaction so connection maintenance knows about it
            op_manager
                .ring
                .live_tx_tracker
                .add_transaction(gateway.peer.clone(), new_tx_id);

            let msg = ConnectMsg::Request {
                id: new_tx_id,
                target: gateway.clone(),
                msg: ConnectRequest::FindOptimalPeer {
                    query_target: gateway.clone(),
                    ideal_location,
                    joiner: joiner_location,
                    max_hops_to_live: op_manager.ring.max_hops_to_live,
                    skip_connections: HashSet::from([joiner.clone()]),
                    skip_forwards: HashSet::new(),
                },
            };

            tracing::info!(
                tx = %new_tx_id,
                gateway = %gateway.peer,
                ideal_location = %ideal_location,
                "Immediately requesting more peer connections from gateway"
            );

            // Send the message through the op_manager's notification system
            // We need to create a new ConnectOp for this new transaction
            let new_op = ConnectOp::new(
                new_tx_id,
                Some(ConnectState::AwaitingNewConnection(NewConnectionInfo {
                    remaining_connections: op_manager.ring.max_hops_to_live,
                })),
                Some(Box::new(gateway.clone())),
                None,
            );

            // Push the new operation
            op_manager
                .push(new_tx_id, OpEnum::Connect(Box::new(new_op)))
                .await?;

            // Send the FindOptimalPeer message to the gateway over the network
            // We use notify_node_event with a SendMessage event to ensure it goes through
            // the proper network channel, not just local processing
            op_manager
                .notify_node_event(NodeEvent::SendMessage {
                    target: gateway.peer.clone(),
                    msg: Box::new(NetMessage::from(msg)),
                })
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
    /// Avoid connecting to these peers.
    pub skip_connections: HashSet<PeerId>,
    /// Avoid forwarding to these peers.
    pub skip_forwards: HashSet<PeerId>,
    pub req_peer: PeerKeyLocation,
    pub joiner: PeerKeyLocation,
    /// Whether this node is a gateway
    pub is_gateway: bool,
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
        mut skip_connections,
        mut skip_forwards,
        req_peer,
        joiner,
        is_gateway,
    } = params;
    if left_htl == 0 {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Couldn't forward connect petition, no hops left",
        );
        return Ok(None);
    }

    let num_connections = connection_manager.num_connections();
    let num_reserved = connection_manager.get_reserved_connections();
    let max_connections = connection_manager.max_connections;

    tracing::debug!(
        tx = %id,
        joiner = %joiner.peer,
        num_connections = %num_connections,
        num_reserved = %num_reserved,
        is_gateway = %is_gateway,
        accepted = %accepted,
        "forward_conn: checking connection forwarding",
    );

    // Special case: Gateway bootstrap when starting with zero connections AND only one reserved
    // Note: num_reserved will be 1 (not 0) because should_accept() already reserved a slot
    // for this connection. This ensures only the very first connection is accepted directly,
    // avoiding race conditions where multiple concurrent join attempts would all be accepted directly.
    //
    // IMPORTANT: Bootstrap acceptances are marked with is_bootstrap_acceptance=true so that
    // the handshake handler (see handshake.rs forward_or_accept_join) can immediately register
    // the connection in the ring. This bypasses the normal CheckConnectivity flow which doesn't
    // apply to bootstrap since:
    // 1. There are no other peers to forward to
    // 2. The "already connected" bug doesn't apply (this is the first connection)
    // 3. We need the connection registered so the gateway can respond to FindOptimalPeer requests
    //
    // See PR #1871 discussion with @iduartgomez for context.
    //
    // IMPORTANT (issue #1908): Extended to cover early network formation (first few peers)
    // During early network formation, the gateway should accept connections directly to ensure
    // bidirectional connections are established. Without this, peers 2+ only get unidirectional
    // connections (peer → gateway) but not the reverse (gateway → peer).
    //
    // However, we still respect max_connections - this only applies when there's capacity.
    const EARLY_NETWORK_THRESHOLD: usize = 4;
    let has_capacity = num_connections + num_reserved < max_connections;
    let is_early_network = is_gateway && accepted && num_connections < EARLY_NETWORK_THRESHOLD;

    if num_connections == 0 || (is_early_network && has_capacity) {
        if num_reserved == 1 && is_gateway && accepted {
            tracing::info!(
                tx = %id,
                joiner = %joiner.peer,
                connections = num_connections,
                has_capacity = %has_capacity,
                "Gateway early network: accepting connection directly (will register immediately)",
            );
            let connectivity_info = ConnectivityInfo::new_bootstrap(
                joiner.clone(),
                1, // Single check for direct connection
            );
            return Ok(Some(ConnectState::AwaitingConnectivity(connectivity_info)));
        } else if num_connections == 0 {
            tracing::debug!(
                tx = %id,
                joiner = %joiner.peer,
                is_gateway = %is_gateway,
                num_reserved = %num_reserved,
                "Cannot forward or accept: no existing connections, or reserved connections pending",
            );
            return Ok(None);
        }
    }

    // Try to forward the connection request to an existing peer
    if num_connections > 0 {
        let target_peer = {
            let router = router.read();
            select_forward_target(
                id,
                connection_manager,
                &router,
                &req_peer,
                &joiner,
                left_htl,
                &skip_forwards,
            )
        };

        skip_connections.insert(req_peer.peer.clone());
        skip_forwards.insert(req_peer.peer.clone());

        match target_peer {
            Some(target_peer) => {
                // Successfully found a peer to forward to
                let forward_msg = create_forward_message(
                    id,
                    &req_peer,
                    &joiner,
                    &target_peer,
                    left_htl,
                    max_htl,
                    skip_connections,
                    skip_forwards,
                );
                tracing::debug!(
                    target: "network",
                    tx = %id,
                    "Forwarding connection request to {:?}",
                    target_peer
                );
                network_bridge.send(&target_peer.peer, forward_msg).await?;
                return update_state_with_forward_info(&req_peer, left_htl);
            }
            None => {
                // Couldn't find suitable peer to forward to
                tracing::debug!(
                    tx = %id,
                    joiner = %joiner.peer,
                    "No suitable peer found for forwarding despite having {} connections",
                    num_connections
                );
                return Ok(None);
            }
        }
    }

    // Should be unreachable - we either forwarded or returned None
    unreachable!("forward_conn should have returned by now")
}

fn select_forward_target(
    id: Transaction,
    connection_manager: &ConnectionManager,
    router: &Router,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    left_htl: usize,
    skip_forwards: &HashSet<PeerId>,
) -> Option<PeerKeyLocation> {
    if left_htl >= connection_manager.rnd_if_htl_above {
        tracing::debug!(
            tx = %id,
            joiner = %joiner.peer,
            "Randomly selecting peer to forward connect request",
        );
        connection_manager.random_peer(|p| !skip_forwards.contains(p))
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
                skip_forwards,
                router,
            )
            .and_then(|pkl| (pkl.peer != joiner.peer).then_some(pkl))
    }
}

#[allow(clippy::too_many_arguments)]
fn create_forward_message(
    id: Transaction,
    request_peer: &PeerKeyLocation,
    joiner: &PeerKeyLocation,
    target: &PeerKeyLocation,
    hops_to_live: usize,
    max_hops_to_live: usize,
    skip_connections: HashSet<PeerId>,
    skip_forwards: HashSet<PeerId>,
) -> NetMessage {
    NetMessage::from(ConnectMsg::Request {
        id,
        target: target.clone(),
        msg: ConnectRequest::CheckConnectivity {
            sender: request_peer.clone(),
            joiner: joiner.clone(),
            hops_to_live: hops_to_live.saturating_sub(1), // decrement the hops to live for the next hop
            max_hops_to_live,
            skip_connections,
            skip_forwards,
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
            /// Used for deterministic testing purposes. In production, this should be none and will be ignored
            /// by the gateway.
            joiner_location: Option<Location>,
            hops_to_live: usize,
            max_hops_to_live: usize,
            // Peers we don't want to connect to directly
            skip_connections: HashSet<PeerId>,
            // Peers we don't want to forward connectivity messages to (to avoid loops)
            skip_forwards: HashSet<PeerId>,
        },
        /// Query target should find a good candidate for joiner to join.
        FindOptimalPeer {
            /// Peer whom you are querying new connection about.
            query_target: PeerKeyLocation,
            /// The ideal location of the peer to which you would connect.
            ideal_location: Location,
            joiner: PeerKeyLocation,
            max_hops_to_live: usize,
            skip_connections: HashSet<PeerId>,
            skip_forwards: HashSet<PeerId>,
        },
        CheckConnectivity {
            sender: PeerKeyLocation,
            joiner: PeerKeyLocation,
            hops_to_live: usize,
            max_hops_to_live: usize,
            skip_connections: HashSet<PeerId>,
            skip_forwards: HashSet<PeerId>,
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
