//! Implementation of the simplified two-message connect flow.
//!
//! The legacy multi-stage connect operation has been removed; this module now powers the node’s
//! connection and maintenance paths end-to-end.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use crate::client_events::HostResult;
use crate::dev_tool::Location;
use crate::message::{InnerMessage, NetMessage, NetMessageV1, NodeEvent, Transaction};
use crate::node::{ConnectionError, IsOperationCompleted, NetworkBridge, OpManager, PeerId};
use crate::operations::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::ring::{PeerAddr, PeerKeyLocation};
use crate::router::{EstimatorType, IsotonicEstimator, IsotonicEvent};
use crate::transport::TransportKeypair;
use crate::util::{Backoff, Contains, IterExt};
use freenet_stdlib::client_api::HostResponse;

const FORWARD_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20);
const RECENCY_COOLDOWN: Duration = Duration::from_secs(30);

/// Top-level message envelope used by the new connect handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ConnectMsg {
    /// Join request that travels *towards* the target location.
    /// The sender is determined from the transport layer's source address.
    Request {
        id: Transaction,
        target: PeerKeyLocation,
        payload: ConnectRequest,
    },
    /// Join acceptance that travels back along the discovered path.
    /// The sender is determined from the transport layer's source address.
    Response {
        id: Transaction,
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
            ConnectMsg::Request {
                target, payload, ..
            } => write!(
                f,
                "ConnectRequest {{ target: {target}, desired: {}, ttl: {}, joiner: {} }}",
                payload.desired_location, payload.ttl, payload.joiner
            ),
            ConnectMsg::Response {
                target, payload, ..
            } => write!(
                f,
                "ConnectResponse {{ target: {target}, acceptor: {} }}",
                payload.acceptor,
            ),
            ConnectMsg::ObservedAddress {
                target, address, ..
            } => {
                write!(
                    f,
                    "ObservedAddress {{ target: {target}, address: {address} }}"
                )
            }
        }
    }
}

impl ConnectMsg {
    /// Returns the socket address of the target peer for routing.
    /// Used by OperationResult to determine where to send the message.
    pub fn target_addr(&self) -> Option<SocketAddr> {
        match self {
            ConnectMsg::Request { target, .. }
            | ConnectMsg::Response { target, .. }
            | ConnectMsg::ObservedAddress { target, .. } => target.socket_addr(),
        }
    }
}

/// Two-message request payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectRequest {
    /// Joiner's advertised location (fallbacks to the joiner's socket address).
    pub desired_location: Location,
    /// Joiner's identity and address. When the joiner creates this request,
    /// `joiner.peer_addr` is set to `PeerAddr::Unknown` because the joiner
    /// doesn't know its own external address (especially behind NAT).
    /// The first recipient (gateway) fills this in from the packet source address.
    pub joiner: PeerKeyLocation,
    /// Remaining hops before the request stops travelling.
    pub ttl: u8,
    /// Simple visited set to avoid trivial loops (addresses of peers that have seen this request).
    pub visited: Vec<SocketAddr>,
}

/// Acceptance payload returned by candidates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectResponse {
    /// The peer that accepted the join request.
    pub acceptor: PeerKeyLocation,
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
    /// Address of the peer that sent us this request (for response routing).
    /// This is determined from the transport layer's source address.
    pub upstream_addr: SocketAddr,
    pub request: ConnectRequest,
    pub forwarded_to: Option<PeerKeyLocation>,
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
        visited: &[SocketAddr],
        recency: &HashMap<PeerId, Instant>,
        estimator: &ConnectForwardEstimator,
    ) -> Option<PeerKeyLocation>;
}

/// Result of processing a request at a relay.
#[derive(Debug, Default)]
pub(crate) struct RelayActions {
    pub accept_response: Option<ConnectResponse>,
    pub expect_connection_from: Option<PeerKeyLocation>,
    pub forward: Option<(PeerKeyLocation, ConnectRequest)>,
    pub observed_address: Option<(PeerKeyLocation, SocketAddr)>,
    /// The target to send the ConnectResponse to (with observed external address).
    pub response_target: Option<PeerKeyLocation>,
}

#[derive(Debug, Clone)]
pub(crate) struct ForwardAttempt {
    peer: PeerKeyLocation,
    desired: Location,
    sent_at: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct ConnectForwardEstimator {
    estimator: IsotonicEstimator,
}

impl ConnectForwardEstimator {
    pub(crate) fn new() -> Self {
        // Seed with neutral points to allow construction without history. This estimator
        // learns, per-node, how often downstream peers accept/complete forwarded Connect
        // requests so we can bias forwarding toward peers likely to have capacity.
        let key = TransportKeypair::new();
        let dummy_peer = PeerKeyLocation::with_location(
            key.public().clone(),
            "127.0.0.1:0".parse().unwrap(),
            Location::new(0.0),
        );
        let seed_events = [
            IsotonicEvent {
                peer: dummy_peer.clone(),
                contract_location: Location::new(0.0),
                result: 0.5,
            },
            IsotonicEvent {
                peer: dummy_peer,
                contract_location: Location::new(0.5),
                result: 0.5,
            },
        ];

        Self {
            estimator: IsotonicEstimator::new(seed_events, EstimatorType::Negative),
        }
    }

    fn record(&mut self, peer: &PeerKeyLocation, desired: Location, success: bool) {
        if peer.location.is_none() {
            return;
        }
        // Treat each downstream forward attempt as a Bernoulli outcome for this peer and
        // desired ring location; these accumulate to bias future routing choices.
        let event = IsotonicEvent {
            peer: peer.clone(),
            contract_location: desired,
            result: if success { 1.0 } else { 0.0 },
        };
        self.estimator.add_event(event);
    }

    fn estimate(&self, peer: &PeerKeyLocation, desired: Location) -> Option<f64> {
        peer.location?;
        self.estimator
            .estimate_retrieval_time(peer, desired)
            .ok()
            .map(|p| p.clamp(0.0, 1.0))
    }
}
impl RelayState {
    pub(crate) fn handle_request<C: RelayContext>(
        &mut self,
        ctx: &C,
        recency: &HashMap<PeerId, Instant>,
        forward_attempts: &mut HashMap<PeerId, ForwardAttempt>,
        estimator: &ConnectForwardEstimator,
    ) -> RelayActions {
        let mut actions = RelayActions::default();
        // Add upstream's address (determined from transport layer) to visited list
        push_unique_addr(&mut self.request.visited, self.upstream_addr);
        // Add our own address to visited list
        push_unique_addr(&mut self.request.visited, ctx.self_location().addr());

        // Fill in joiner's external address from transport layer if unknown.
        // This is the key step where the first recipient (gateway) determines the joiner's
        // external address from the actual packet source address.
        if self.request.joiner.peer_addr.is_unknown() {
            self.request.joiner.set_addr(self.upstream_addr);
        }

        // If joiner's address is now known (was filled in above or by network bridge from packet source)
        // and we haven't yet sent the ObservedAddress notification, do so now.
        // This tells the joiner their external address for future connections.
        if let PeerAddr::Known(joiner_addr) = &self.request.joiner.peer_addr {
            if !self.observed_sent {
                if self.request.joiner.location.is_none() {
                    self.request.joiner.location = Some(Location::from_address(joiner_addr));
                }
                self.observed_sent = true;
                actions.observed_address = Some((self.request.joiner.clone(), *joiner_addr));
            }
        }

        if !self.accepted_locally && ctx.should_accept(&self.request.joiner) {
            self.accepted_locally = true;
            let self_loc = ctx.self_location();
            // Use PeerAddr::Unknown for acceptor - the acceptor doesn't know their own
            // external address (especially behind NAT). The first recipient of the response
            // will fill this in from the packet source address.
            let acceptor = PeerKeyLocation {
                pub_key: self_loc.pub_key().clone(),
                peer_addr: PeerAddr::Unknown,
                location: self_loc.location,
            };
            let dist = ring_distance(acceptor.location, self.request.joiner.location);
            actions.accept_response = Some(ConnectResponse {
                acceptor: acceptor.clone(),
            });
            actions.expect_connection_from = Some(self.request.joiner.clone());
            // Use the joiner with updated observed address for response routing
            actions.response_target = Some(self.request.joiner.clone());
            tracing::info!(
                acceptor_pub_key = %acceptor.pub_key(),
                joiner_pub_key = %self.request.joiner.pub_key(),
                acceptor_loc = ?acceptor.location,
                joiner_loc = ?self.request.joiner.location,
                ring_distance = ?dist,
                "connect: acceptance issued"
            );
        }

        if self.forwarded_to.is_none() && self.request.ttl > 0 {
            match ctx.select_next_hop(
                self.request.desired_location,
                &self.request.visited,
                recency,
                estimator,
            ) {
                Some(next) => {
                    let dist = ring_distance(next.location, Some(self.request.desired_location));
                    tracing::info!(
                        target = %self.request.desired_location,
                        ttl = self.request.ttl,
                        next_peer = %next.peer(),
                        next_loc = ?next.location,
                        ring_distance_to_target = ?dist,
                        "connect: forwarding join request to next hop"
                    );
                    let mut forward_req = self.request.clone();
                    forward_req.ttl = forward_req.ttl.saturating_sub(1);
                    push_unique_addr(&mut forward_req.visited, ctx.self_location().addr());
                    let forward_snapshot = forward_req.clone();
                    self.forwarded_to = Some(next.clone());
                    self.request = forward_req;
                    forward_attempts.insert(
                        next.peer().clone(),
                        ForwardAttempt {
                            peer: next.clone(),
                            desired: self.request.desired_location,
                            sent_at: Instant::now(),
                        },
                    );
                    actions.forward = Some((next, forward_snapshot));
                }
                None => {
                    tracing::info!(
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
            .unwrap_or_else(|| Location::from_address(&joiner.addr()));
        self.op_manager
            .ring
            .connection_manager
            .should_accept(location, &joiner.peer())
    }

    fn select_next_hop(
        &self,
        desired_location: Location,
        visited: &[SocketAddr],
        recency: &HashMap<PeerId, Instant>,
        estimator: &ConnectForwardEstimator,
    ) -> Option<PeerKeyLocation> {
        // Use SkipListWithSelf to explicitly exclude ourselves from candidates,
        // ensuring we never select ourselves as a forwarding target even if
        // self wasn't added to visited by upstream callers.
        let skip = SkipListWithSelf {
            visited,
            self_peer: &self.self_location.peer(),
            conn_manager: &self.op_manager.ring.connection_manager,
        };
        let router = self.op_manager.ring.router.read();
        let candidates = self.op_manager.ring.connection_manager.routing_candidates(
            desired_location,
            None,
            skip,
        );

        let now = Instant::now();
        let mut scored: Vec<(f64, PeerKeyLocation)> = Vec::new();
        let mut eligible: Vec<PeerKeyLocation> = Vec::new();

        for cand in candidates {
            if let Some(ts) = recency.get(&cand.peer()) {
                if now.duration_since(*ts) < RECENCY_COOLDOWN {
                    continue;
                }
            }

            if cand.location.is_some() {
                if let Some(score) = estimator.estimate(&cand, desired_location) {
                    scored.push((score, cand.clone()));
                    continue;
                }
                // Keep candidates without estimates for fallback.
                eligible.push(cand.clone());
                continue;
            }
            eligible.push(cand.clone());
        }

        if !scored.is_empty() {
            let best_score = scored
                .iter()
                .map(|(s, _)| *s)
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(0.0);
            let best: Vec<_> = scored
                .into_iter()
                .filter(|(s, _)| (*s - best_score).abs() < f64::EPSILON)
                .map(|(_, c)| c)
                .collect();
            if !best.is_empty() {
                return router.select_peer(best.iter(), desired_location).cloned();
            }
        }

        if eligible.is_empty() {
            None
        } else {
            router
                .select_peer(eligible.iter(), desired_location)
                .cloned()
        }
    }
}

#[derive(Debug)]
pub struct AcceptedPeer {
    pub peer: PeerKeyLocation,
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
    /// Tracks when we last forwarded this connect to a peer, to avoid hammering the same
    /// neighbors when no acceptors are available. Peers without an entry are treated as
    /// immediately eligible.
    recency: HashMap<PeerId, Instant>,
    forward_attempts: HashMap<PeerId, ForwardAttempt>,
    connect_forward_estimator: Arc<RwLock<ConnectForwardEstimator>>,
}

impl ConnectOp {
    fn record_forward_outcome(&mut self, peer: &PeerKeyLocation, desired: Location, success: bool) {
        self.forward_attempts.remove(&peer.peer());
        self.connect_forward_estimator
            .write()
            .record(peer, desired, success);
    }

    fn expire_forward_attempts(&mut self, now: Instant) {
        let mut expired = Vec::new();
        for (peer, attempt) in self.forward_attempts.iter() {
            if now.duration_since(attempt.sent_at) >= FORWARD_ATTEMPT_TIMEOUT {
                expired.push((peer.clone(), attempt.desired));
            }
        }
        for (peer, desired) in expired {
            if let Some(attempt) = self.forward_attempts.remove(&peer) {
                self.record_forward_outcome(&attempt.peer, desired, false);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_joiner(
        id: Transaction,
        desired_location: Location,
        target_connections: usize,
        observed_address: Option<SocketAddr>,
        gateway: Option<PeerKeyLocation>,
        backoff: Option<Backoff>,
        connect_forward_estimator: Arc<RwLock<ConnectForwardEstimator>>,
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
            recency: HashMap::new(),
            forward_attempts: HashMap::new(),
            connect_forward_estimator,
        }
    }

    pub(crate) fn new_relay(
        id: Transaction,
        upstream_addr: SocketAddr,
        request: ConnectRequest,
        connect_forward_estimator: Arc<RwLock<ConnectForwardEstimator>>,
    ) -> Self {
        let state = ConnectState::Relaying(Box::new(RelayState {
            upstream_addr,
            request,
            forwarded_to: None,
            observed_sent: false,
            accepted_locally: false,
        }));
        Self {
            id,
            state: Some(state),
            gateway: None,
            backoff: None,
            desired_location: None,
            recency: HashMap::new(),
            forward_attempts: HashMap::new(),
            connect_forward_estimator,
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
        connect_forward_estimator: Arc<RwLock<ConnectForwardEstimator>>,
    ) -> (Transaction, Self, ConnectMsg) {
        // Initialize visited list with addresses of ourself and the target gateway
        let mut visited = vec![own.addr()];
        push_unique_addr(&mut visited, target.addr());

        // Create joiner with PeerAddr::Unknown - the joiner doesn't know their own
        // external address (especially behind NAT). The first recipient (gateway)
        // will fill this in from the packet source address.
        let joiner = PeerKeyLocation::with_unknown_addr(own.pub_key.clone());
        let request = ConnectRequest {
            desired_location,
            joiner,
            ttl,
            visited,
        };

        let tx = Transaction::new::<ConnectMsg>();
        let op = ConnectOp::new_joiner(
            tx,
            desired_location,
            target_connections,
            Some(own.addr()),
            Some(target.clone()),
            None,
            connect_forward_estimator,
        );

        let msg = ConnectMsg::Request {
            id: tx,
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
                tracing::info!(
                    acceptor_pub_key = %response.acceptor.pub_key(),
                    acceptor_loc = ?response.acceptor.location,
                    "connect: joiner received ConnectResponse"
                );
                let result = state.register_acceptance(response, now);
                if let Some(new_acceptor) = &result.new_acceptor {
                    self.recency.remove(&new_acceptor.peer.peer());
                }
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
        upstream_addr: SocketAddr,
        request: ConnectRequest,
        estimator: &ConnectForwardEstimator,
    ) -> RelayActions {
        self.expire_forward_attempts(Instant::now());
        if !matches!(self.state, Some(ConnectState::Relaying(_))) {
            self.state = Some(ConnectState::Relaying(Box::new(RelayState {
                upstream_addr,
                request: request.clone(),
                forwarded_to: None,
                observed_sent: false,
                accepted_locally: false,
            })));
        }

        match self.state.as_mut() {
            Some(ConnectState::Relaying(state)) => {
                state.upstream_addr = upstream_addr;
                state.request = request;
                state.handle_request(ctx, &self.recency, &mut self.forward_attempts, estimator)
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
        source_addr: Option<std::net::SocketAddr>,
    ) -> Result<OpInitialization<Self>, OpError> {
        let tx = *msg.id();
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Connect(op))) => Ok(OpInitialization {
                op: *op,
                source_addr,
            }),
            Ok(Some(other)) => {
                op_manager.push(tx, other).await?;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                let op = match (msg, source_addr) {
                    (ConnectMsg::Request { payload, .. }, Some(upstream_addr)) => {
                        ConnectOp::new_relay(
                            tx,
                            upstream_addr,
                            payload.clone(),
                            op_manager.connect_forward_estimator.clone(),
                        )
                    }
                    (ConnectMsg::Request { .. }, None) => {
                        tracing::warn!(%tx, "connect request received without source address");
                        return Err(OpError::OpNotPresent(tx));
                    }
                    _ => {
                        tracing::debug!(%tx, "connect received message without existing state");
                        return Err(OpError::OpNotPresent(tx));
                    }
                };
                Ok(OpInitialization { op, source_addr })
            }
            Err(err) => Err(err.into()),
        }
    }

    fn process_message<'a, NB: NetworkBridge>(
        mut self,
        network_bridge: &'a mut NB,
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<OperationResult, OpError>> + Send + 'a>,
    > {
        Box::pin(async move {
            match msg {
                ConnectMsg::Request { payload, .. } => {
                    let env = RelayEnv::new(op_manager);
                    let estimator = {
                        let estimator_guard = self.connect_forward_estimator.read();
                        estimator_guard.clone()
                    };
                    // Use source_addr from transport layer as upstream address
                    let upstream_addr = source_addr.ok_or_else(|| {
                        OpError::from(ConnectionError::TransportError(
                            "ConnectMsg::Request received without source_addr".into(),
                        ))
                    })?;
                    let actions =
                        self.handle_request(&env, upstream_addr, payload.clone(), &estimator);

                    if let Some((target, address)) = actions.observed_address {
                        let msg = ConnectMsg::ObservedAddress {
                            id: self.id,
                            target: target.clone(),
                            address,
                        };
                        // Route through upstream (where the request came from) since we may
                        // not have a direct connection to the target.
                        // Note: upstream_addr is already validated from source_addr at the start of this match arm.
                        network_bridge
                            .send(upstream_addr, NetMessage::V1(NetMessageV1::Connect(msg)))
                            .await?;
                    }

                    if let Some(peer) = actions.expect_connection_from {
                        op_manager
                            .notify_node_event(NodeEvent::ExpectPeerConnection {
                                peer: peer.peer().clone(),
                            })
                            .await?;
                    }

                    if let Some((next, request)) = actions.forward {
                        // Record recency for this forward to avoid hammering the same neighbor.
                        self.recency.insert(next.peer().clone(), Instant::now());
                        let forward_msg = ConnectMsg::Request {
                            id: self.id,
                            target: next.clone(),
                            payload: request,
                        };
                        network_bridge
                            .send(
                                next.addr(),
                                NetMessage::V1(NetMessageV1::Connect(forward_msg)),
                            )
                            .await?;
                    }

                    if let Some(response) = actions.accept_response {
                        // response_target has the joiner's address (filled in from packet source)
                        let response_target = actions.response_target.ok_or_else(|| {
                            OpError::from(ConnectionError::TransportError(
                                "ConnectMsg::Request: accept_response but no response_target"
                                    .into(),
                            ))
                        })?;
                        let response_msg = ConnectMsg::Response {
                            id: self.id,
                            target: response_target,
                            payload: response,
                        };
                        // Route the response through upstream (where the request came from)
                        // since we may not have a direct connection to the joiner.
                        // Note: upstream_addr is already validated from source_addr at the start of this match arm.
                        network_bridge
                            .send(
                                upstream_addr,
                                NetMessage::V1(NetMessageV1::Connect(response_msg)),
                            )
                            .await?;
                        return Ok(store_operation_state(&mut self));
                    }

                    Ok(store_operation_state(&mut self))
                }
                ConnectMsg::Response { payload, .. } => {
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
                                            peer: new_acceptor.peer.peer().clone(),
                                        },
                                    )
                                    .await?;

                                let (callback, mut rx) = mpsc::channel(1);
                                op_manager
                                    .notify_node_event(NodeEvent::ConnectPeer {
                                        peer: new_acceptor.peer.peer().clone(),
                                        tx: self.id,
                                        callback,
                                        is_gw: false,
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
                        let (forwarded, desired, upstream_addr, joiner) = {
                            let st = state;
                            (
                                st.forwarded_to.clone(),
                                st.request.desired_location,
                                st.upstream_addr,
                                st.request.joiner.clone(),
                            )
                        };
                        if let Some(fwd) = forwarded {
                            self.record_forward_outcome(&fwd, desired, true);
                        }

                        // Fill in acceptor's external address from source_addr if unknown.
                        // The acceptor doesn't know their own external address (especially behind NAT),
                        // so the first relay peer that receives the response fills it in from the
                        // transport layer's source address.
                        let forward_payload = if payload.acceptor.peer_addr.is_unknown() {
                            if let Some(acceptor_addr) = source_addr {
                                let mut updated_payload = payload.clone();
                                updated_payload.acceptor.peer_addr = PeerAddr::Known(acceptor_addr);
                                tracing::debug!(
                                    acceptor_pub_key = %updated_payload.acceptor.pub_key(),
                                    acceptor_addr = %acceptor_addr,
                                    "connect: filled acceptor address from source_addr"
                                );
                                updated_payload
                            } else {
                                tracing::warn!(
                                    acceptor_pub_key = %payload.acceptor.pub_key(),
                                    "connect: response received without source_addr, cannot fill acceptor address"
                                );
                                payload.clone()
                            }
                        } else {
                            payload.clone()
                        };

                        tracing::debug!(
                            upstream_addr = %upstream_addr,
                            acceptor_pub_key = %forward_payload.acceptor.pub_key(),
                            "connect: forwarding response towards joiner"
                        );
                        // Forward response toward the joiner via upstream
                        let forward_msg = ConnectMsg::Response {
                            id: self.id,
                            target: joiner,
                            payload: forward_payload,
                        };
                        network_bridge
                            .send(
                                upstream_addr,
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

/// Skip list that combines visited peers with the current node's own peer ID.
/// This ensures we never select ourselves as a forwarding target, even if
/// self wasn't properly added to the visited list by upstream callers.
struct SkipListWithSelf<'a> {
    visited: &'a [SocketAddr],
    self_peer: &'a PeerId,
    conn_manager: &'a crate::ring::ConnectionManager,
}

impl Contains<PeerId> for SkipListWithSelf<'_> {
    fn has_element(&self, target: PeerId) -> bool {
        if &target == self.self_peer {
            return true;
        }
        // Check if any visited address belongs to this peer
        for addr in self.visited {
            if let Some(peer_id) = self.conn_manager.get_peer_by_addr(*addr) {
                if peer_id == target {
                    return true;
                }
            }
        }
        false
    }
}

impl Contains<&PeerId> for SkipListWithSelf<'_> {
    fn has_element(&self, target: &PeerId) -> bool {
        self.has_element(target.clone())
    }
}

fn push_unique_addr(list: &mut Vec<SocketAddr>, addr: SocketAddr) {
    if !list.contains(&addr) {
        list.push(addr);
    }
}

fn store_operation_state(op: &mut ConnectOp) -> OperationResult {
    store_operation_state_with_msg(op, None)
}

fn store_operation_state_with_msg(op: &mut ConnectOp, msg: Option<ConnectMsg>) -> OperationResult {
    let state_clone = op.state.clone();
    // Extract target address from the message for routing
    let target_addr = msg.as_ref().and_then(|m| m.target_addr());
    OperationResult {
        return_msg: msg.map(|m| NetMessage::V1(NetMessageV1::Connect(m))),
        target_addr,
        state: state_clone.map(|state| {
            OpEnum::Connect(Box::new(ConnectOp {
                id: op.id,
                state: Some(state),
                gateway: op.gateway.clone(),
                backoff: op.backoff.clone(),
                desired_location: op.desired_location,
                recency: op.recency.clone(),
                forward_attempts: op.forward_attempts.clone(),
                connect_forward_estimator: op.connect_forward_estimator.clone(),
            }))
        }),
    }
}

fn ring_distance(a: Option<Location>, b: Option<Location>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.distance(b).as_f64()),
        _ => None,
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
        .should_accept(location, &gateway.peer())
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
        op_manager.connect_forward_estimator.clone(),
    );

    op.gateway = Some(Box::new(gateway.clone()));
    if let Some(backoff) = backoff {
        op.backoff = Some(backoff);
    }

    tracing::info!(gateway = %gateway.peer(), tx = %tx, "Attempting network join using connect");

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
    }

    impl TestRelayContext {
        fn new(self_loc: PeerKeyLocation) -> Self {
            Self {
                self_loc,
                accept: true,
                next_hop: None,
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
            _visited: &[SocketAddr],
            _recency: &HashMap<PeerId, Instant>,
            _estimator: &ConnectForwardEstimator,
        ) -> Option<PeerKeyLocation> {
            self.next_hop.clone()
        }
    }

    fn make_peer(port: u16) -> PeerKeyLocation {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let keypair = TransportKeypair::new();
        PeerKeyLocation::with_location(keypair.public().clone(), addr, Location::random())
    }

    #[test]
    fn forward_estimator_handles_missing_location() {
        let mut estimator = ConnectForwardEstimator::new();
        let key = TransportKeypair::new();
        let peer = PeerKeyLocation::new(key.public().clone(), "127.0.0.1:1111".parse().unwrap());
        estimator.record(&peer, Location::new(0.25), true);
    }

    #[test]
    fn expired_forward_attempts_are_cleared() {
        let mut op = ConnectOp::new_joiner(
            Transaction::new::<ConnectMsg>(),
            Location::new(0.1),
            1,
            None,
            None,
            None,
            Arc::new(RwLock::new(ConnectForwardEstimator::new())),
        );
        let peer = make_peer(2000);
        op.forward_attempts.insert(
            peer.peer().clone(),
            ForwardAttempt {
                peer: peer.clone(),
                desired: Location::new(0.2),
                sent_at: Instant::now() - FORWARD_ATTEMPT_TIMEOUT - Duration::from_secs(1),
            },
        );
        op.expire_forward_attempts(Instant::now());
        assert!(op.forward_attempts.is_empty());
    }

    #[test]
    fn relay_accepts_when_policy_allows() {
        let self_loc = make_peer(4000);
        let joiner = make_peer(5000);
        let mut state = RelayState {
            upstream_addr: joiner.addr(), // Now uses SocketAddr
            request: ConnectRequest {
                desired_location: Location::random(),
                joiner: joiner.clone(),
                ttl: 3,
                visited: vec![],
            },
            forwarded_to: None,
            observed_sent: false,
            accepted_locally: false,
        };

        let ctx = TestRelayContext::new(self_loc.clone());
        let recency = HashMap::new();
        let mut forward_attempts = HashMap::new();
        let estimator = ConnectForwardEstimator::new();
        let actions = state.handle_request(&ctx, &recency, &mut forward_attempts, &estimator);

        let response = actions.accept_response.expect("expected acceptance");
        // Compare pub_key since acceptor's address is intentionally Unknown (NAT scenario)
        assert_eq!(response.acceptor.pub_key(), self_loc.pub_key());
        assert_eq!(
            actions.expect_connection_from.unwrap().pub_key(),
            joiner.pub_key()
        );
        assert!(actions.forward.is_none());
    }

    #[test]
    fn relay_forwards_when_not_accepting() {
        let self_loc = make_peer(4100);
        let joiner = make_peer(5100);
        let next_hop = make_peer(6100);
        let mut state = RelayState {
            upstream_addr: joiner.addr(), // Now uses SocketAddr
            request: ConnectRequest {
                desired_location: Location::random(),
                joiner: joiner.clone(),
                ttl: 2,
                visited: vec![],
            },
            forwarded_to: None,
            observed_sent: false,
            accepted_locally: false,
        };

        let ctx = TestRelayContext::new(self_loc)
            .accept(false)
            .next_hop(Some(next_hop.clone()));
        let recency = HashMap::new();
        let mut forward_attempts = HashMap::new();
        let estimator = ConnectForwardEstimator::new();
        let actions = state.handle_request(&ctx, &recency, &mut forward_attempts, &estimator);

        assert!(actions.accept_response.is_none());
        let (forward_to, request) = actions.forward.expect("expected forward");
        assert_eq!(forward_to.peer(), next_hop.peer());
        assert_eq!(request.ttl, 1);
        // visited now contains SocketAddr
        assert!(request.visited.contains(&joiner.addr()));
    }

    #[test]
    fn relay_emits_observed_address_for_private_joiner() {
        let self_loc = make_peer(4050);
        let joiner_base = make_peer(5050);
        let observed_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            joiner_base.addr().port(),
        );
        // Create a joiner with the observed address (simulating what the network
        // bridge does when it fills in the address from the packet source)
        let joiner_with_observed_addr = PeerKeyLocation::with_location(
            joiner_base.pub_key().clone(),
            observed_addr,
            joiner_base.location.unwrap(),
        );
        let mut state = RelayState {
            upstream_addr: joiner_base.addr(), // Now uses SocketAddr
            request: ConnectRequest {
                desired_location: Location::random(),
                joiner: joiner_with_observed_addr.clone(),
                ttl: 3,
                visited: vec![],
            },
            forwarded_to: None,
            observed_sent: false,
            accepted_locally: false,
        };

        let ctx = TestRelayContext::new(self_loc);
        let recency = HashMap::new();
        let mut forward_attempts = HashMap::new();
        let estimator = ConnectForwardEstimator::new();
        let actions = state.handle_request(&ctx, &recency, &mut forward_attempts, &estimator);

        let (target, addr) = actions
            .observed_address
            .expect("expected observed address update");
        assert_eq!(addr, observed_addr);
        assert_eq!(target.addr(), observed_addr);
        assert_eq!(state.request.joiner.addr(), observed_addr);
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
        };
        let result = state.register_acceptance(&response, Instant::now());
        assert!(result.satisfied);
        let new = result.new_acceptor.expect("expected new acceptor");
        assert_eq!(new.peer.peer(), acceptor.peer());
    }

    #[test]
    fn init_join_request_initializes_state() {
        let target = make_peer(7200);
        let desired = Location::random();
        let ttl = 5;
        let own = make_peer(7300);
        let (_tx, op, msg) = ConnectOp::initiate_join_request(
            own.clone(),
            target.clone(),
            desired,
            ttl,
            2,
            Arc::new(RwLock::new(ConnectForwardEstimator::new())),
        );

        match msg {
            ConnectMsg::Request {
                target: msg_target,
                payload,
                ..
            } => {
                assert_eq!(msg_target.peer(), target.peer());
                assert_eq!(payload.desired_location, desired);
                assert_eq!(payload.ttl, ttl);
                // visited now contains SocketAddr, not PeerKeyLocation
                assert!(payload.visited.contains(&own.addr()));
                assert!(payload.visited.contains(&target.addr()));
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
            joiner: joiner.clone(),
            ttl: 3,
            visited: vec![joiner.addr()], // Now uses SocketAddr
        };

        let tx = Transaction::new::<ConnectMsg>();
        let mut relay_op = ConnectOp::new_relay(
            tx,
            joiner.addr(), // Now uses SocketAddr
            request.clone(),
            Arc::new(RwLock::new(ConnectForwardEstimator::new())),
        );
        let ctx = TestRelayContext::new(relay_a.clone())
            .accept(false)
            .next_hop(Some(relay_b.clone()));
        let estimator = ConnectForwardEstimator::new();
        let actions = relay_op.handle_request(&ctx, joiner.addr(), request.clone(), &estimator);

        let (forward_target, forward_request) = actions
            .forward
            .expect("relay should forward when it declines to accept");
        assert_eq!(forward_target.peer(), relay_b.peer());
        assert_eq!(forward_request.ttl, 2);
        assert!(
            forward_request.visited.contains(&relay_a.addr()),
            "forwarded request should record intermediate relay's address"
        );

        // Second hop should accept and notify the joiner.
        let mut accepting_relay = ConnectOp::new_relay(
            tx,
            relay_a.addr(), // Now uses SocketAddr
            forward_request.clone(),
            Arc::new(RwLock::new(ConnectForwardEstimator::new())),
        );
        let ctx_accept = TestRelayContext::new(relay_b.clone());
        let estimator = ConnectForwardEstimator::new();
        let accept_actions = accepting_relay.handle_request(
            &ctx_accept,
            relay_a.addr(), // Now uses SocketAddr
            forward_request,
            &estimator,
        );

        let response = accept_actions
            .accept_response
            .expect("second relay should accept when policy allows");
        // Compare pub_key since acceptor's address is intentionally Unknown (NAT scenario)
        assert_eq!(response.acceptor.pub_key(), relay_b.pub_key());
        let expect_conn = accept_actions
            .expect_connection_from
            .expect("acceptance should request inbound connection from joiner");
        assert_eq!(expect_conn.pub_key(), joiner.pub_key());
    }

    /// Regression test for issue #2141: ConnectResponse must be sent to the joiner's
    /// observed external address, not the original private/NAT address.
    #[test]
    fn connect_response_uses_observed_address_not_private() {
        // Joiner behind NAT: original creation used private address, but the network bridge
        // fills in the observed public address from the packet source.
        let private_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 9000);
        let keypair = TransportKeypair::new();
        let joiner_original = PeerKeyLocation::with_location(
            keypair.public().clone(),
            private_addr,
            Location::random(),
        );

        // Gateway observes joiner's public/external address and fills it into joiner.peer_addr
        let observed_public_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 50)), 9000);
        let joiner_with_observed_addr = PeerKeyLocation::with_location(
            keypair.public().clone(),
            observed_public_addr,
            joiner_original.location.unwrap(),
        );

        let relay = make_peer(5000);

        let mut state = RelayState {
            upstream_addr: private_addr, // The address we received the request from
            request: ConnectRequest {
                desired_location: Location::random(),
                joiner: joiner_with_observed_addr.clone(),
                ttl: 3,
                visited: vec![],
            },
            forwarded_to: None,
            observed_sent: false,
            accepted_locally: false,
        };

        let ctx = TestRelayContext::new(relay.clone());
        let recency = HashMap::new();
        let mut forward_attempts = HashMap::new();
        let estimator = ConnectForwardEstimator::new();
        let actions = state.handle_request(&ctx, &recency, &mut forward_attempts, &estimator);

        // Verify acceptance was issued
        assert!(
            actions.accept_response.is_some(),
            "relay should accept joiner"
        );

        // Critical: response_target must have the observed public address, not private
        let response_target = actions
            .response_target
            .expect("response_target should be set when accepting");
        assert_eq!(
            response_target.addr(),
            observed_public_addr,
            "response_target must use observed external address ({}) not private address ({})",
            observed_public_addr,
            private_addr
        );

        // Double-check: the original joiner had the private address
        assert_eq!(
            joiner_original.addr(),
            private_addr,
            "original joiner should have private address"
        );
    }

    // Note: The SkipListWithSelf test has been removed as it now requires a ConnectionManager
    // to look up peers by address. The skip list behavior is tested via integration tests
    // and the self-exclusion logic is straightforward.
}
