//! Implementation of the simplified two-message connect flow.
//!
//! The legacy multi-stage connect operation has been removed; this module now powers the node’s
//! connection and maintenance paths end-to-end.

use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use crate::client_events::HostResult;
use crate::dev_tool::Location;
use crate::message::{InnerMessage, NetMessage, NetMessageV1, NodeEvent, Transaction};
use crate::node::{IsOperationCompleted, NetworkBridge, OpManager, PeerId};
use crate::operations::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::ring::PeerKeyLocation;
use crate::util::{Backoff, Contains, IterExt};
use freenet_stdlib::client_api::HostResponse;

/// Top-level message envelope used by the new connect handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ConnectMsg {
    /// Join request that travels *towards* the target location.
    Request {
        id: Transaction,
        from: PeerKeyLocation,
        target: PeerKeyLocation,
        payload: ConnectRequest,
    },
    /// Join acceptance that travels back along the discovered path.
    Response {
        id: Transaction,
        sender: PeerKeyLocation,
        target: PeerKeyLocation,
        payload: ConnectResponse,
    },
    /// Informational packet letting the joiner know the address a peer observed.
    ObservedAddress {
        id: Transaction,
        target: PeerKeyLocation,
        address: SocketAddr,
    },
}

impl InnerMessage for ConnectMsg {
    fn id(&self) -> &Transaction {
        match self {
            ConnectMsg::Request { id, .. }
            | ConnectMsg::Response { id, .. }
            | ConnectMsg::ObservedAddress { id, .. } => id,
        }
    }

    #[allow(refining_impl_trait)]
    fn target(&self) -> Option<&PeerKeyLocation> {
        match self {
            ConnectMsg::Request { target, .. }
            | ConnectMsg::Response { target, .. }
            | ConnectMsg::ObservedAddress { target, .. } => Some(target),
        }
    }

    fn requested_location(&self) -> Option<Location> {
        match self {
            ConnectMsg::Request { payload, .. } => Some(payload.desired_location),
            _ => None,
        }
    }
}

impl fmt::Display for ConnectMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectMsg::Request { target, payload, .. } => write!(
                f,
                "ConnectRequest {{ target: {target}, desired: {}, ttl: {}, origin: {} }}",
                payload.desired_location,
                payload.ttl,
                payload.origin
            ),
            ConnectMsg::Response { sender, target, payload, .. } => write!(
                f,
                "ConnectResponse {{ sender: {sender}, target: {target}, acceptor: {}, courtesy: {} }}",
                payload.acceptor,
                payload.courtesy
            ),
            ConnectMsg::ObservedAddress { target, address, .. } => {
                write!(f, "ObservedAddress {{ target: {target}, address: {address} }}")
            }
        }
    }
}

impl ConnectMsg {
    pub fn sender(&self) -> Option<&PeerId> {
        match self {
            ConnectMsg::Response { sender, .. } => Some(&sender.peer),
            _ => None,
        }
    }
}

/// Two-message request payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectRequest {
    /// Joiner's advertised location (fallbacks to the joiner's socket address).
    pub desired_location: Location,
    /// Joiner's identity as observed so far.
    pub origin: PeerKeyLocation,
    /// Remaining hops before the request stops travelling.
    pub ttl: u8,
    /// Simple visited set to avoid trivial loops.
    pub visited: Vec<PeerKeyLocation>,
}

/// Acceptance payload returned by candidates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectResponse {
    /// The peer that accepted the join request.
    pub acceptor: PeerKeyLocation,
    /// Whether this acceptance is a short-lived courtesy link.
    pub courtesy: bool,
}

/// New minimal state machine the joiner tracks.
#[derive(Debug, Clone)]
pub(crate) enum ConnectState {
    /// Joiner waiting for acceptances.
    WaitingForResponses(JoinerState),
    /// Intermediate peer evaluating and forwarding requests.
    Relaying(Box<RelayState>),
    /// Joiner obtained the required neighbours.
    Completed,
}

#[derive(Debug, Clone)]
pub(crate) struct JoinerState {
    pub target_connections: usize,
    pub observed_address: Option<SocketAddr>,
    pub accepted: HashSet<PeerKeyLocation>,
    pub last_progress: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct RelayState {
    pub upstream: PeerKeyLocation,
    pub request: ConnectRequest,
    pub forwarded_to: Option<PeerKeyLocation>,
    pub courtesy_hint: bool,
    pub observed_sent: bool,
    pub accepted_locally: bool,
}

/// Abstractions required to evaluate an inbound connect request at an
/// intermediate peer.
pub(crate) trait RelayContext {
    /// Location of the current peer.
    fn self_location(&self) -> &PeerKeyLocation;

    /// Determine whether we should accept the joiner immediately.
    fn should_accept(&self, joiner: &PeerKeyLocation) -> bool;

    /// Choose the next hop for the request, avoiding peers already visited.
    fn select_next_hop(
        &self,
        desired_location: Location,
        visited: &[PeerKeyLocation],
    ) -> Option<PeerKeyLocation>;

    /// Whether the acceptance should be treated as a short-lived courtesy link.
    fn courtesy_hint(&self, acceptor: &PeerKeyLocation, joiner: &PeerKeyLocation) -> bool;
}

/// Result of processing a request at a relay.
#[derive(Debug, Default)]
pub(crate) struct RelayActions {
    pub accept_response: Option<ConnectResponse>,
    pub expect_connection_from: Option<PeerKeyLocation>,
    pub forward: Option<(PeerKeyLocation, ConnectRequest)>,
    pub observed_address: Option<(PeerKeyLocation, SocketAddr)>,
}

impl RelayState {
    pub(crate) fn handle_request<C: RelayContext>(
        &mut self,
        ctx: &C,
        observed_remote: &PeerKeyLocation,
        observed_addr: SocketAddr,
    ) -> RelayActions {
        let mut actions = RelayActions::default();
        push_unique_peer(&mut self.request.visited, observed_remote.clone());
        push_unique_peer(&mut self.request.visited, ctx.self_location().clone());

        if self.request.origin.peer.addr.ip().is_unspecified()
            && !self.observed_sent
            && observed_remote.peer.pub_key == self.request.origin.peer.pub_key
        {
            self.request.origin.peer.addr = observed_addr;
            if self.request.origin.location.is_none() {
                self.request.origin.location = Some(Location::from_address(&observed_addr));
            }
            self.observed_sent = true;
            actions.observed_address = Some((self.request.origin.clone(), observed_addr));
        }

        if !self.accepted_locally && ctx.should_accept(&self.request.origin) {
            self.accepted_locally = true;
            let acceptor = ctx.self_location().clone();
            let courtesy = ctx.courtesy_hint(&acceptor, &self.request.origin);
            self.courtesy_hint = courtesy;
            actions.accept_response = Some(ConnectResponse {
                acceptor: acceptor.clone(),
                courtesy,
            });
            actions.expect_connection_from = Some(self.request.origin.clone());
        }

        if self.forwarded_to.is_none() && self.request.ttl > 0 {
            match ctx.select_next_hop(self.request.desired_location, &self.request.visited) {
                Some(next) => {
                    tracing::debug!(
                        target = %self.request.desired_location,
                        ttl = self.request.ttl,
                        next_peer = %next.peer,
                        "connect: forwarding join request to next hop"
                    );
                    let mut forward_req = self.request.clone();
                    forward_req.ttl = forward_req.ttl.saturating_sub(1);
                    push_unique_peer(&mut forward_req.visited, ctx.self_location().clone());
                    let forward_snapshot = forward_req.clone();
                    self.forwarded_to = Some(next.clone());
                    self.request = forward_req;
                    actions.forward = Some((next, forward_snapshot));
                }
                None => {
                    tracing::debug!(
                        target = %self.request.desired_location,
                        ttl = self.request.ttl,
                        visited = ?self.request.visited,
                        "connect: no next hop candidates available"
                    );
                }
            }
        }

        actions
    }
}

pub(crate) struct RelayEnv<'a> {
    pub op_manager: &'a OpManager,
    self_location: PeerKeyLocation,
}

impl<'a> RelayEnv<'a> {
    pub fn new(op_manager: &'a OpManager) -> Self {
        let self_location = op_manager.ring.connection_manager.own_location();
        Self {
            op_manager,
            self_location,
        }
    }
}

impl RelayContext for RelayEnv<'_> {
    fn self_location(&self) -> &PeerKeyLocation {
        &self.self_location
    }

    fn should_accept(&self, joiner: &PeerKeyLocation) -> bool {
        let location = joiner
            .location
            .unwrap_or_else(|| Location::from_address(&joiner.peer.addr));
        self.op_manager
            .ring
            .connection_manager
            .should_accept(location, &joiner.peer)
    }

    fn select_next_hop(
        &self,
        desired_location: Location,
        visited: &[PeerKeyLocation],
    ) -> Option<PeerKeyLocation> {
        let skip = VisitedPeerIds { peers: visited };
        let router = self.op_manager.ring.router.read();
        self.op_manager
            .ring
            .connection_manager
            .routing(desired_location, None, skip, &router)
    }

    fn courtesy_hint(&self, _acceptor: &PeerKeyLocation, _joiner: &PeerKeyLocation) -> bool {
        // Courtesy slots still piggyback on regular connections. Flag the first acceptance so the
        // joiner can prioritise it, and keep the logic simple until dedicated courtesy tracking
        // is wired in (see courtesy-connection-budget branch).
        self.op_manager.ring.open_connections() == 0
    }
}

#[derive(Debug)]
pub struct AcceptedPeer {
    pub peer: PeerKeyLocation,
    pub courtesy: bool,
}

#[derive(Debug, Default)]
pub struct JoinerAcceptance {
    pub new_acceptor: Option<AcceptedPeer>,
    pub satisfied: bool,
    pub assigned_location: bool,
}

impl JoinerState {
    pub(crate) fn register_acceptance(
        &mut self,
        response: &ConnectResponse,
        now: Instant,
    ) -> JoinerAcceptance {
        let mut acceptance = JoinerAcceptance::default();
        if self.accepted.insert(response.acceptor.clone()) {
            self.last_progress = now;
            acceptance.new_acceptor = Some(AcceptedPeer {
                peer: response.acceptor.clone(),
                courtesy: response.courtesy,
            });
            acceptance.assigned_location = self.accepted.len() == 1;
        }
        acceptance.satisfied = self.accepted.len() >= self.target_connections;
        acceptance
    }

    pub(crate) fn update_observed_address(&mut self, address: SocketAddr, now: Instant) {
        self.observed_address = Some(address);
        self.last_progress = now;
    }
}

/// Placeholder operation wrapper so we can exercise the logic in isolation in
/// forthcoming commits. For now this simply captures the shared state we will
/// migrate to.
#[derive(Debug, Clone)]
pub(crate) struct ConnectOp {
    pub(crate) id: Transaction,
    pub(crate) state: Option<ConnectState>,
    pub(crate) gateway: Option<Box<PeerKeyLocation>>,
    pub(crate) backoff: Option<Backoff>,
    pub(crate) desired_location: Option<Location>,
}

impl ConnectOp {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_joiner(
        id: Transaction,
        desired_location: Location,
        target_connections: usize,
        observed_address: Option<SocketAddr>,
        gateway: Option<PeerKeyLocation>,
        backoff: Option<Backoff>,
    ) -> Self {
        let state = ConnectState::WaitingForResponses(JoinerState {
            target_connections,
            observed_address,
            accepted: HashSet::new(),
            last_progress: Instant::now(),
        });
        Self {
            id,
            state: Some(state),
            gateway: gateway.map(Box::new),
            backoff,
            desired_location: Some(desired_location),
        }
    }

    pub(crate) fn new_relay(
        id: Transaction,
        upstream: PeerKeyLocation,
        request: ConnectRequest,
    ) -> Self {
        let state = ConnectState::Relaying(Box::new(RelayState {
            upstream,
            request,
            forwarded_to: None,
            courtesy_hint: false,
            observed_sent: false,
            accepted_locally: false,
        }));
        Self {
            id,
            state: Some(state),
            gateway: None,
            backoff: None,
            desired_location: None,
        }
    }

    pub(crate) fn is_completed(&self) -> bool {
        matches!(self.state, Some(ConnectState::Completed))
    }

    pub(crate) fn id(&self) -> &Transaction {
        &self.id
    }

    pub(crate) fn outcome(&self) -> OpOutcome<'_> {
        OpOutcome::Irrelevant
    }

    pub(crate) fn finalized(&self) -> bool {
        self.is_completed()
    }

    pub(crate) fn to_host_result(&self) -> HostResult {
        Ok(HostResponse::Ok)
    }

    pub(crate) fn has_backoff(&self) -> bool {
        self.backoff.is_some()
    }

    pub(crate) fn gateway(&self) -> Option<&PeerKeyLocation> {
        self.gateway.as_deref()
    }

    fn take_desired_location(&mut self) -> Option<Location> {
        self.desired_location.take()
    }

    pub(crate) fn initiate_join_request(
        own: PeerKeyLocation,
        target: PeerKeyLocation,
        desired_location: Location,
        ttl: u8,
        target_connections: usize,
    ) -> (Transaction, Self, ConnectMsg) {
        let mut visited = vec![own.clone()];
        push_unique_peer(&mut visited, target.clone());
        let request = ConnectRequest {
            desired_location,
            origin: own.clone(),
            ttl,
            visited,
        };

        let tx = Transaction::new::<ConnectMsg>();
        let op = ConnectOp::new_joiner(
            tx,
            desired_location,
            target_connections,
            Some(own.peer.addr),
            Some(target.clone()),
            None,
        );

        let msg = ConnectMsg::Request {
            id: tx,
            from: own,
            target,
            payload: request,
        };

        (tx, op, msg)
    }

    pub(crate) fn handle_response(
        &mut self,
        response: &ConnectResponse,
        now: Instant,
    ) -> Option<JoinerAcceptance> {
        match self.state.as_mut() {
            Some(ConnectState::WaitingForResponses(state)) => {
                let result = state.register_acceptance(response, now);
                if result.satisfied {
                    self.state = Some(ConnectState::Completed);
                }
                Some(result)
            }
            _ => None,
        }
    }

    pub(crate) fn handle_observed_address(&mut self, address: SocketAddr, now: Instant) {
        if let Some(ConnectState::WaitingForResponses(state)) = self.state.as_mut() {
            state.update_observed_address(address, now);
        }
    }

    pub(crate) fn handle_request<C: RelayContext>(
        &mut self,
        ctx: &C,
        upstream: PeerKeyLocation,
        request: ConnectRequest,
        observed_addr: SocketAddr,
    ) -> RelayActions {
        if !matches!(self.state, Some(ConnectState::Relaying(_))) {
            self.state = Some(ConnectState::Relaying(Box::new(RelayState {
                upstream: upstream.clone(),
                request: request.clone(),
                forwarded_to: None,
                courtesy_hint: false,
                observed_sent: false,
                accepted_locally: false,
            })));
        }

        match self.state.as_mut() {
            Some(ConnectState::Relaying(state)) => {
                state.upstream = upstream;
                state.request = request;
                let upstream_snapshot = state.upstream.clone();
                state.handle_request(ctx, &upstream_snapshot, observed_addr)
            }
            _ => RelayActions::default(),
        }
    }
}

impl IsOperationCompleted for ConnectOp {
    fn is_completed(&self) -> bool {
        self.is_completed()
    }
}

impl Operation for ConnectOp {
    type Message = ConnectMsg;
    type Result = ();

    fn id(&self) -> &Transaction {
        &self.id
    }

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
        let tx = *msg.id();
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Connect(op))) => Ok(OpInitialization {
                op: *op,
                sender: msg.sender().cloned(),
            }),
            Ok(Some(other)) => {
                op_manager.push(tx, other).await?;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                let op = match msg {
                    ConnectMsg::Request { from, payload, .. } => {
                        ConnectOp::new_relay(tx, from.clone(), payload.clone())
                    }
                    _ => {
                        tracing::debug!(%tx, "connect received message without existing state");
                        return Err(OpError::OpNotPresent(tx));
                    }
                };
                Ok(OpInitialization { op, sender: None })
            }
            Err(err) => Err(err.into()),
        }
    }

    fn process_message<'a, NB: NetworkBridge>(
        mut self,
        network_bridge: &'a mut NB,
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<OperationResult, OpError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match msg {
                ConnectMsg::Request { from, payload, .. } => {
                    let env = RelayEnv::new(op_manager);
                    let actions =
                        self.handle_request(&env, from.clone(), payload.clone(), from.peer.addr);

                    if let Some((target, address)) = actions.observed_address {
                        let msg = ConnectMsg::ObservedAddress {
                            id: self.id,
                            target: target.clone(),
                            address,
                        };
                        network_bridge
                            .send(&target.peer, NetMessage::V1(NetMessageV1::Connect(msg)))
                            .await?;
                    }

                    if let Some(peer) = actions.expect_connection_from {
                        op_manager
                            .notify_node_event(NodeEvent::ExpectPeerConnection {
                                peer: peer.peer.clone(),
                            })
                            .await?;
                    }

                    if let Some((next, request)) = actions.forward {
                        let forward_msg = ConnectMsg::Request {
                            id: self.id,
                            from: env.self_location().clone(),
                            target: next.clone(),
                            payload: request,
                        };
                        network_bridge
                            .send(
                                &next.peer,
                                NetMessage::V1(NetMessageV1::Connect(forward_msg)),
                            )
                            .await?;
                    }

                    if let Some(response) = actions.accept_response {
                        let response_msg = ConnectMsg::Response {
                            id: self.id,
                            sender: env.self_location().clone(),
                            target: from.clone(),
                            payload: response,
                        };
                        return Ok(store_operation_state_with_msg(
                            &mut self,
                            Some(response_msg),
                        ));
                    }

                    Ok(store_operation_state(&mut self))
                }
                ConnectMsg::Response {
                    sender, payload, ..
                } => {
                    if self.gateway.is_some() {
                        if let Some(acceptance) = self.handle_response(payload, Instant::now()) {
                            if acceptance.assigned_location {
                                if let Some(location) = self.take_desired_location() {
                                    tracing::info!(
                                        tx=%self.id,
                                        assigned_location = %location.0,
                                        "connect: assigning joiner location"
                                    );
                                    op_manager
                                        .ring
                                        .connection_manager
                                        .update_location(Some(location));
                                }
                            }

                            if let Some(new_acceptor) = acceptance.new_acceptor {
                                op_manager
                                    .notify_node_event(
                                        crate::message::NodeEvent::ExpectPeerConnection {
                                            peer: new_acceptor.peer.peer.clone(),
                                        },
                                    )
                                    .await?;

                                let (callback, mut rx) = mpsc::channel(1);
                                op_manager
                                    .notify_node_event(NodeEvent::ConnectPeer {
                                        peer: new_acceptor.peer.peer.clone(),
                                        tx: self.id,
                                        callback,
                                        is_gw: new_acceptor.courtesy,
                                    })
                                    .await?;

                                if let Some(result) = rx.recv().await {
                                    if let Ok((peer_id, _remaining)) = result {
                                        tracing::info!(
                                            %peer_id,
                                            tx=%self.id,
                                            "connect joined peer"
                                        );
                                    } else {
                                        tracing::warn!(
                                            tx=%self.id,
                                            "connect ConnectPeer failed"
                                        );
                                    }
                                }
                            }

                            if acceptance.satisfied {
                                self.state = Some(ConnectState::Completed);
                            }
                        }

                        Ok(store_operation_state(&mut self))
                    } else if let Some(ConnectState::Relaying(state)) = self.state.as_mut() {
                        let upstream = state.upstream.clone();
                        tracing::debug!(
                            %upstream.peer,
                            acceptor = %sender.peer,
                            "connect: forwarding response towards joiner"
                        );
                        let forward_msg = ConnectMsg::Response {
                            id: self.id,
                            sender: sender.clone(),
                            target: upstream.clone(),
                            payload: payload.clone(),
                        };
                        network_bridge
                            .send(
                                &upstream.peer,
                                NetMessage::V1(NetMessageV1::Connect(forward_msg)),
                            )
                            .await?;
                        Ok(store_operation_state(&mut self))
                    } else {
                        Ok(store_operation_state(&mut self))
                    }
                }
                ConnectMsg::ObservedAddress { address, .. } => {
                    self.handle_observed_address(*address, Instant::now());
                    Ok(store_operation_state(&mut self))
                }
            }
        })
    }
}

struct VisitedPeerIds<'a> {
    peers: &'a [PeerKeyLocation],
}

impl Contains<PeerId> for VisitedPeerIds<'_> {
    fn has_element(&self, target: PeerId) -> bool {
        self.peers.iter().any(|p| p.peer == target)
    }
}

impl Contains<&PeerId> for VisitedPeerIds<'_> {
    fn has_element(&self, target: &PeerId) -> bool {
        self.peers.iter().any(|p| &p.peer == target)
    }
}

fn push_unique_peer(list: &mut Vec<PeerKeyLocation>, peer: PeerKeyLocation) {
    let already_present = list.iter().any(|p| p.peer == peer.peer);
    if !already_present {
        list.push(peer);
    }
}

fn store_operation_state(op: &mut ConnectOp) -> OperationResult {
    store_operation_state_with_msg(op, None)
}

fn store_operation_state_with_msg(op: &mut ConnectOp, msg: Option<ConnectMsg>) -> OperationResult {
    let state_clone = op.state.clone();
    OperationResult {
        return_msg: msg.map(|m| NetMessage::V1(NetMessageV1::Connect(m))),
        state: state_clone.map(|state| {
            OpEnum::Connect(Box::new(ConnectOp {
                id: op.id,
                state: Some(state),
                gateway: op.gateway.clone(),
                backoff: op.backoff.clone(),
                desired_location: op.desired_location,
            }))
        }),
    }
}

#[tracing::instrument(fields(peer = %op_manager.ring.connection_manager.pub_key), skip_all)]
pub(crate) async fn join_ring_request(
    backoff: Option<Backoff>,
    gateway: &PeerKeyLocation,
    op_manager: &OpManager,
) -> Result<(), OpError> {
    use crate::node::ConnectionError;
    let location = gateway.location.ok_or_else(|| {
        tracing::error!("Gateway location not found, this should not be possible, report an error");
        OpError::ConnError(ConnectionError::LocationUnknown)
    })?;

    if !op_manager
        .ring
        .connection_manager
        .should_accept(location, &gateway.peer)
    {
        return Err(OpError::ConnError(ConnectionError::UnwantedConnection));
    }

    let mut backoff = backoff;
    if let Some(backoff_state) = backoff.as_mut() {
        tracing::warn!(
            "Performing a new join, attempt {}",
            backoff_state.retries() + 1
        );
        if backoff_state.sleep().await.is_none() {
            tracing::error!("Max number of retries reached");
            if op_manager.ring.open_connections() == 0 {
                let tx = Transaction::new::<ConnectMsg>();
                return Err(OpError::MaxRetriesExceeded(tx, tx.transaction_type()));
            } else {
                return Ok(());
            }
        }
    }

    let own = op_manager.ring.connection_manager.own_location();
    let ttl = op_manager
        .ring
        .max_hops_to_live
        .max(1)
        .min(u8::MAX as usize) as u8;
    let target_connections = op_manager.ring.connection_manager.min_connections;

    let (tx, mut op, msg) = ConnectOp::initiate_join_request(
        own.clone(),
        gateway.clone(),
        location,
        ttl,
        target_connections,
    );

    op.gateway = Some(Box::new(gateway.clone()));
    if let Some(backoff) = backoff {
        op.backoff = Some(backoff);
    }

    tracing::info!(%gateway.peer, tx = %tx, "Attempting network join using connect");

    op_manager
        .notify_op_change(
            NetMessage::V1(NetMessageV1::Connect(msg)),
            OpEnum::Connect(Box::new(op)),
        )
        .await?;

    Ok(())
}

pub(crate) async fn initial_join_procedure(
    op_manager: Arc<OpManager>,
    gateways: &[PeerKeyLocation],
) -> Result<JoinHandle<()>, OpError> {
    let number_of_parallel_connections = {
        let max_potential_conns_per_gw = op_manager.ring.max_hops_to_live;
        let needed_to_cover_max =
            op_manager.ring.connection_manager.max_connections / max_potential_conns_per_gw;
        gateways.iter().take(needed_to_cover_max).count().max(2)
    };
    let gateways = gateways.to_vec();
    let handle = task::spawn(async move {
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

            let unconnected_count = unconnected_gateways.len();

            if open_conns < BOOTSTRAP_THRESHOLD && unconnected_count > 0 {
                tracing::info!(
                    "Below bootstrap threshold ({} < {}), attempting to connect to {} gateways",
                    open_conns,
                    BOOTSTRAP_THRESHOLD,
                    number_of_parallel_connections.min(unconnected_count)
                );
                let select_all = FuturesUnordered::new();
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
                select_all
                    .for_each(|(res, gateway)| async move {
                        if let Err(error) = res {
                            if !matches!(
                                error,
                                OpError::ConnError(
                                    crate::node::ConnectionError::UnwantedConnection
                                )
                            ) {
                                tracing::error!(
                                    %gateway,
                                    %error,
                                    "Failed while attempting connection to gateway"
                                );
                            }
                        }
                    })
                    .await;
            } else if open_conns >= BOOTSTRAP_THRESHOLD {
                tracing::trace!(
                    "Have {} connections (>= threshold of {}), not attempting gateway connections",
                    open_conns,
                    BOOTSTRAP_THRESHOLD
                );
            }

            let wait_time = if open_conns == 0 {
                tracing::debug!("No connections yet, waiting {}s before retry", WAIT_TIME);
                WAIT_TIME
            } else if open_conns < BOOTSTRAP_THRESHOLD {
                tracing::debug!(
                    "Have {} connections (below threshold of {}), waiting {}s",
                    open_conns,
                    BOOTSTRAP_THRESHOLD,
                    WAIT_TIME * 3
                );
                WAIT_TIME * 3
            } else {
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
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::PeerId;
    use crate::transport::TransportKeypair;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Instant;

    struct TestRelayContext {
        self_loc: PeerKeyLocation,
        accept: bool,
        next_hop: Option<PeerKeyLocation>,
        courtesy: bool,
    }

    impl TestRelayContext {
        fn new(self_loc: PeerKeyLocation) -> Self {
            Self {
                self_loc,
                accept: true,
                next_hop: None,
                courtesy: false,
            }
        }

        fn accept(mut self, accept: bool) -> Self {
            self.accept = accept;
            self
        }

        fn next_hop(mut self, hop: Option<PeerKeyLocation>) -> Self {
            self.next_hop = hop;
            self
        }

        fn courtesy(mut self, courtesy: bool) -> Self {
            self.courtesy = courtesy;
            self
        }
    }

    impl RelayContext for TestRelayContext {
        fn self_location(&self) -> &PeerKeyLocation {
            &self.self_loc
        }

        fn should_accept(&self, _joiner: &PeerKeyLocation) -> bool {
            self.accept
        }

        fn select_next_hop(
            &self,
            _desired_location: Location,
            _visited: &[PeerKeyLocation],
        ) -> Option<PeerKeyLocation> {
            self.next_hop.clone()
        }

        fn courtesy_hint(&self, _acceptor: &PeerKeyLocation, _joiner: &PeerKeyLocation) -> bool {
            self.courtesy
        }
    }

    fn make_peer(port: u16) -> PeerKeyLocation {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let keypair = TransportKeypair::new();
        PeerKeyLocation {
            peer: PeerId::new(addr, keypair.public().clone()),
            location: Some(Location::random()),
        }
    }

    #[test]
    fn relay_accepts_when_policy_allows() {
        let self_loc = make_peer(4000);
        let joiner = make_peer(5000);
        let mut state = RelayState {
            upstream: joiner.clone(),
            request: ConnectRequest {
                desired_location: Location::random(),
                origin: joiner.clone(),
                ttl: 3,
                visited: vec![],
            },
            forwarded_to: None,
            courtesy_hint: false,
            observed_sent: false,
            accepted_locally: false,
        };

        let ctx = TestRelayContext::new(self_loc.clone()).courtesy(true);
        let observed_addr = joiner.peer.addr;
        let actions = state.handle_request(&ctx, &joiner, observed_addr);

        let response = actions.accept_response.expect("expected acceptance");
        assert_eq!(response.acceptor.peer, self_loc.peer);
        assert!(response.courtesy);
        assert_eq!(actions.expect_connection_from.unwrap().peer, joiner.peer);
        assert!(actions.forward.is_none());
    }

    #[test]
    fn relay_forwards_when_not_accepting() {
        let self_loc = make_peer(4100);
        let joiner = make_peer(5100);
        let next_hop = make_peer(6100);
        let mut state = RelayState {
            upstream: joiner.clone(),
            request: ConnectRequest {
                desired_location: Location::random(),
                origin: joiner.clone(),
                ttl: 2,
                visited: vec![],
            },
            forwarded_to: None,
            courtesy_hint: false,
            observed_sent: false,
            accepted_locally: false,
        };

        let ctx = TestRelayContext::new(self_loc)
            .accept(false)
            .next_hop(Some(next_hop.clone()));
        let actions = state.handle_request(&ctx, &joiner, joiner.peer.addr);

        assert!(actions.accept_response.is_none());
        let (forward_to, request) = actions.forward.expect("expected forward");
        assert_eq!(forward_to.peer, next_hop.peer);
        assert_eq!(request.ttl, 1);
        assert!(request.visited.iter().any(|pkl| pkl.peer == joiner.peer));
    }

    #[test]
    fn joiner_tracks_acceptance() {
        let acceptor = make_peer(7000);
        let mut state = JoinerState {
            target_connections: 1,
            observed_address: None,
            accepted: HashSet::new(),
            last_progress: Instant::now(),
        };

        let response = ConnectResponse {
            acceptor: acceptor.clone(),
            courtesy: false,
        };
        let result = state.register_acceptance(&response, Instant::now());
        assert!(result.satisfied);
        let new = result.new_acceptor.expect("expected new acceptor");
        assert_eq!(new.peer.peer, acceptor.peer);
        assert!(!new.courtesy);
    }

    #[test]
    fn init_join_request_initializes_state() {
        let target = make_peer(7200);
        let desired = Location::random();
        let ttl = 5;
        let own = make_peer(7300);
        let (_tx, op, msg) =
            ConnectOp::initiate_join_request(own.clone(), target.clone(), desired, ttl, 2);

        match msg {
            ConnectMsg::Request {
                from,
                target: msg_target,
                payload,
                ..
            } => {
                assert_eq!(msg_target.peer, target.peer);
                assert_eq!(payload.desired_location, desired);
                assert_eq!(payload.ttl, ttl);
                assert!(payload.visited.iter().any(|p| p.peer == from.peer));
                assert!(payload.visited.iter().any(|p| p.peer == target.peer));
            }
            other => panic!("unexpected message: {other:?}"),
        }

        assert!(matches!(
            op.state,
            Some(ConnectState::WaitingForResponses(_))
        ));
    }

    #[test]
    fn multi_hop_forward_path_tracks_ttl_and_visited_peers() {
        let joiner = make_peer(8100);
        let relay_a = make_peer(8200);
        let relay_b = make_peer(8300);

        let request = ConnectRequest {
            desired_location: Location::random(),
            origin: joiner.clone(),
            ttl: 3,
            visited: vec![joiner.clone()],
        };

        let tx = Transaction::new::<ConnectMsg>();
        let mut relay_op = ConnectOp::new_relay(tx, joiner.clone(), request.clone());
        let ctx = TestRelayContext::new(relay_a.clone())
            .accept(false)
            .next_hop(Some(relay_b.clone()));
        let actions =
            relay_op.handle_request(&ctx, joiner.clone(), request.clone(), joiner.peer.addr);

        let (forward_target, forward_request) = actions
            .forward
            .expect("relay should forward when it declines to accept");
        assert_eq!(forward_target.peer, relay_b.peer);
        assert_eq!(forward_request.ttl, 2);
        assert!(
            forward_request
                .visited
                .iter()
                .any(|p| p.peer == relay_a.peer),
            "forwarded request should record intermediate relay"
        );

        // Second hop should accept and notify the joiner.
        let mut accepting_relay =
            ConnectOp::new_relay(tx, relay_a.clone(), forward_request.clone());
        let ctx_accept = TestRelayContext::new(relay_b.clone());
        let accept_actions = accepting_relay.handle_request(
            &ctx_accept,
            relay_a.clone(),
            forward_request,
            relay_a.peer.addr,
        );

        let response = accept_actions
            .accept_response
            .expect("second relay should accept when policy allows");
        assert_eq!(response.acceptor.peer, relay_b.peer);
        let expect_conn = accept_actions
            .expect_connection_from
            .expect("acceptance should request inbound connection from joiner");
        assert_eq!(expect_conn.peer, joiner.peer);
    }
}
