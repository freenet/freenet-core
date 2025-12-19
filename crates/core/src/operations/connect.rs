//! # Connect Protocol
//!
//! This module implements the connect protocol for establishing peer connections in the Freenet
//! network. Understanding this protocol is essential for working on topology and routing.
//!
//! ## Key Concepts
//!
//! - **Joiner**: The peer initiating the connection request
//! - **Relay**: Any peer that receives and processes the request (first hop is often a gateway,
//!   but can be any known peer)
//! - **Acceptor**: A peer that agrees to connect with the joiner
//!
//! ## Critical Behavior: Accept Only at Terminus
//!
//! **Relays only ACCEPT when they can't forward to a closer peer** (terminus). This means:
//!
//! ```text
//! Joiner sends ConnectRequest targeting location 0.3
//!     |
//!     v
//! Gateway (loc=0.7) ──> FORWARDS toward 0.3 (not at terminus, doesn't accept)
//!     |
//!     v
//! Relay A (loc=0.5) ──> FORWARDS toward 0.3 (not at terminus, doesn't accept)
//!     |
//!     v
//! Relay B (loc=0.35) ──> FORWARDS toward 0.3 (not at terminus, doesn't accept)
//!     |
//!     v
//! Relay C (loc=0.31) ──> ACCEPTS (at terminus - no closer peer to forward to)
//! ```
//!
//! The joiner typically receives ONE ConnectResponse from the terminus peer. To get multiple
//! connections, the joiner sends multiple ConnectRequests (potentially targeting different
//! locations). This ensures connections are naturally local (short ring distance).
//!
//! ## Message Flow
//!
//! 1. **ConnectRequest**: Joiner → Relay₁ → Relay₂ → ... (routed toward `desired_location`)
//! 2. **ConnectResponse**: Each acceptor → back through relay chain → Joiner
//! 3. **ObservedAddress**: First relay → Joiner (tells joiner their external IP for NAT traversal)
//!
//! ## Address Discovery (NAT Traversal)
//!
//! The joiner typically doesn't know its external address (behind NAT). The protocol handles this:
//!
//! 1. Joiner creates ConnectRequest with `joiner.peer_addr = Unknown`
//! 2. First relay observes joiner's address from UDP packet source
//! 3. First relay fills in `joiner.peer_addr` and sends `ObservedAddress` back to joiner
//! 4. Subsequent relays see the already-filled address
//!
//! ## Acceptance Criteria: Accept Only at Terminus
//!
//! Relays only accept connect requests when they're at the routing terminus - meaning they
//! can't forward to a peer closer to the target location. This naturally creates local
//! connections without arbitrary distance thresholds.
//!
//! **Algorithm:**
//! 1. First, check if we can forward to a closer peer via `select_next_hop()`
//! 2. If we CAN forward: forward only (don't accept)
//! 3. If we CAN'T forward (terminus): accept if `should_accept()` allows
//!
//! `should_accept()` still uses capacity-based evaluation:
//! - Below min_connections → **accept**
//! - At max_connections → **reject**
//! - Between min and max → use density-based evaluation
//!
//! **Why this works for small-world topology:**
//! - Requests route toward the target location via greedy routing
//! - Only peers that can't forward further (near the target) accept
//! - Gateway and early relays forward without accepting
//! - Result: connections are naturally local (short ring distance)
//!
//! ## Routing
//!
//! The request is routed toward `desired_location` using greedy routing:
//! - Each relay forwards to its neighbor closest to `desired_location`
//! - The `visited` list prevents routing loops
//! - TTL decrements at each hop
//!
//! ## Why Target Location Matters
//!
//! If the joiner targets their own location, the request routes toward peers near them on the
//! ring. With accept-only-at-terminus, only the peer closest to the target (who can't forward
//! further) accepts. This naturally creates local connections.
//!
//! To build multiple connections, the joiner sends multiple ConnectRequests. Early in bootstrap
//! (0-4 connections), peers target their own location to build local neighborhoods. Later,
//! density-based targeting is used to optimize for request patterns.

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
use crate::node::{ConnectionError, IsOperationCompleted, NetworkBridge, OpManager};
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
    /// Routing is determined by `payload.desired_location`, not an embedded target.
    /// The sender is determined from the transport layer's source address.
    Request {
        id: Transaction,
        payload: ConnectRequest,
    },
    /// Join acceptance that travels back along the discovered path.
    /// Routing uses hop-by-hop via `upstream_addr` stored in operation state.
    /// The sender is determined from the transport layer's source address.
    Response {
        id: Transaction,
        payload: ConnectResponse,
    },
    /// Informational packet letting the joiner know the address a peer observed.
    /// Routing uses hop-by-hop via `upstream_addr` stored in operation state.
    ObservedAddress {
        id: Transaction,
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
            ConnectMsg::Request { payload, .. } => write!(
                f,
                "ConnectRequest {{ desired: {}, ttl: {}, joiner: {} }}",
                payload.desired_location, payload.ttl, payload.joiner
            ),
            ConnectMsg::Response { payload, .. } => {
                write!(f, "ConnectResponse {{ acceptor: {} }}", payload.acceptor,)
            }
            ConnectMsg::ObservedAddress { address, .. } => {
                write!(f, "ObservedAddress {{ address: {address} }}")
            }
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
    /// True if the joiner started without knowing their external address.
    /// Used for invariant checking: if true, we must receive ObservedAddress.
    pub started_without_address: bool,
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
        recency: &HashMap<PeerKeyLocation, Instant>,
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
        let dummy_peer = PeerKeyLocation::new(key.public().clone(), "127.0.0.1:0".parse().unwrap());
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
        if peer.location().is_none() {
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
        peer.location()?;
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
        recency: &HashMap<PeerKeyLocation, Instant>,
        forward_attempts: &mut HashMap<PeerKeyLocation, ForwardAttempt>,
        estimator: &ConnectForwardEstimator,
    ) -> RelayActions {
        let mut actions = RelayActions::default();
        tracing::debug!(
            joiner_addr = ?self.request.joiner.peer_addr,
            upstream_addr = %self.upstream_addr,
            observed_sent = self.observed_sent,
            "RelayState::step() start"
        );
        // Add upstream's address (determined from transport layer) to visited list
        push_unique_addr(&mut self.request.visited, self.upstream_addr);
        // Add our own address to visited list
        if let Some(self_addr) = ctx.self_location().socket_addr() {
            push_unique_addr(&mut self.request.visited, self_addr);
        }

        // Fill in joiner's external address from transport layer if unknown.
        // This is the key step where the first recipient (gateway) determines the joiner's
        // external address from the actual packet source address.
        let discovered_joiner_addr = if self.request.joiner.peer_addr.is_unknown() {
            self.request.joiner.set_addr(self.upstream_addr);
            true
        } else {
            false
        };

        // Only send ObservedAddress if WE discovered the joiner's address (it was unknown
        // and we just filled it in from the packet source). If the address was already known
        // in the incoming request, the upstream relay already sent ObservedAddress.
        // NOTE: Always emit ObservedAddress from the first hop (gateway) since it observes
        // the joiner's external address from the packet source. The `discovered_joiner_addr`
        // flag indicates we just filled in the address from the transport layer.
        if discovered_joiner_addr {
            if let PeerAddr::Known(joiner_addr) = &self.request.joiner.peer_addr {
                if !self.observed_sent {
                    self.observed_sent = true;
                    let expected_location = crate::ring::Location::from_address(joiner_addr);
                    tracing::debug!(
                        joiner_addr = %joiner_addr,
                        expected_location = %expected_location,
                        upstream_addr = %self.upstream_addr,
                        "discovered joiner addr, emitting ObservedAddress"
                    );
                    actions.observed_address = Some((self.request.joiner.clone(), *joiner_addr));
                }
            }
        }

        // ACCEPT ONLY AT TERMINUS: Relays only accept when they can't forward to a closer peer.
        // This naturally creates local connections because only peers near the target accept.
        //
        // Algorithm:
        // 1. First, check if we can forward to a closer peer
        // 2. If we can forward: forward only (don't accept)
        // 3. If we can't forward (terminus): accept if should_accept() allows
        //
        // This prevents early relays (gateway, first hops) from accepting connections that
        // would be non-local on the ring. Only peers that are actually close to the target
        // (and thus can't forward further) will accept.

        let can_forward = self.forwarded_to.is_none() && self.request.ttl > 0;
        let next_hop = if can_forward {
            ctx.select_next_hop(
                self.request.desired_location,
                &self.request.visited,
                recency,
                estimator,
            )
        } else {
            None
        };

        let is_terminus = next_hop.is_none();

        // Forward if we have a next hop
        if let Some(next) = next_hop {
            let dist = ring_distance(next.location(), Some(self.request.desired_location));
            tracing::debug!(
                target = %self.request.desired_location,
                ttl = self.request.ttl,
                next_peer = %next.pub_key(),
                next_loc = ?next.location(),
                ring_distance_to_target = ?dist,
                "connect: forwarding join request to next hop (not accepting - not at terminus)"
            );
            let mut forward_req = self.request.clone();
            forward_req.ttl = forward_req.ttl.saturating_sub(1);
            if let Some(self_addr) = ctx.self_location().socket_addr() {
                push_unique_addr(&mut forward_req.visited, self_addr);
            }
            let forward_snapshot = forward_req.clone();
            self.forwarded_to = Some(next.clone());
            self.request = forward_req;
            forward_attempts.insert(
                next.clone(),
                ForwardAttempt {
                    peer: next.clone(),
                    desired: self.request.desired_location,
                    sent_at: Instant::now(),
                },
            );
            actions.forward = Some((next, forward_snapshot));
        }

        // Only accept at terminus (can't forward to a closer peer)
        if is_terminus && !self.accepted_locally && ctx.should_accept(&self.request.joiner) {
            self.accepted_locally = true;
            // Use unknown address for the acceptor - the acceptor doesn't know their own
            // external address (especially behind NAT). The first relay that receives this
            // response will fill in the address from the packet source, similar to how
            // joiner addresses are filled in for ConnectRequest.
            let self_loc = ctx.self_location();
            let acceptor = PeerKeyLocation::with_unknown_addr(self_loc.pub_key().clone());
            let dist = ring_distance(self_loc.location(), self.request.joiner.location());
            actions.accept_response = Some(ConnectResponse {
                acceptor: acceptor.clone(),
            });
            actions.expect_connection_from = Some(self.request.joiner.clone());
            // Response is routed hop-by-hop via upstream_addr, no target embedded in message
            tracing::info!(
                acceptor_pub_key = %self_loc.pub_key(),
                joiner_pub_key = %self.request.joiner.pub_key(),
                acceptor_loc = ?self_loc.location(),
                joiner_loc = ?self.request.joiner.location(),
                ring_distance = ?dist,
                "connect: acceptance issued at terminus (acceptor addr will be filled by relay)"
            );
        } else if is_terminus && !self.accepted_locally {
            tracing::info!(
                target = %self.request.desired_location,
                ttl = self.request.ttl,
                visited = ?self.request.visited,
                "connect: at terminus but should_accept() returned false"
            );
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
        let addr = joiner
            .socket_addr()
            .expect("joiner must have address to determine should_accept");
        let location = joiner
            .location()
            .unwrap_or_else(|| Location::from_address(&addr));
        self.op_manager
            .ring
            .connection_manager
            .should_accept(location, addr)
    }

    fn select_next_hop(
        &self,
        desired_location: Location,
        visited: &[SocketAddr],
        recency: &HashMap<PeerKeyLocation, Instant>,
        estimator: &ConnectForwardEstimator,
    ) -> Option<PeerKeyLocation> {
        // Use SkipListWithSelf to explicitly exclude ourselves from candidates,
        // ensuring we never select ourselves as a forwarding target even if
        // self wasn't added to visited by upstream callers.
        let skip = SkipListWithSelf {
            visited,
            self_addr: self.self_location.socket_addr(),
        };
        let router = self.op_manager.ring.router.read();

        let candidates = self.op_manager.ring.connection_manager.routing_candidates(
            desired_location,
            None,
            skip,
        );

        // Calculate our own distance to the target for greedy routing check
        let my_distance = self
            .self_location
            .location()
            .map(|loc| loc.distance(desired_location));

        let now = Instant::now();
        let mut scored: Vec<(f64, PeerKeyLocation)> = Vec::new();
        let mut eligible: Vec<PeerKeyLocation> = Vec::new();

        for cand in candidates {
            if let Some(ts) = recency.get(&cand) {
                if now.duration_since(*ts) < RECENCY_COOLDOWN {
                    continue;
                }
            }

            // GREEDY ROUTING: Only consider neighbors that are CLOSER to the target than we are.
            // This is essential for proper terminus detection - we're at terminus when no
            // neighbor is closer to the target than we are.
            //
            // Note: We use `>` (not `>=`) to allow forwarding to equally-distant peers.
            // This prevents routing deadlocks when multiple peers are equidistant to the target
            // (common near ring boundary 0.0/1.0).
            if let (Some(cand_loc), Some(my_dist)) = (cand.location(), my_distance) {
                let cand_distance = cand_loc.distance(desired_location);
                if cand_distance > my_dist {
                    // Neighbor is farther from target than us - skip
                    continue;
                }
            }

            if cand.location().is_some() {
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
    /// The peer we sent/forwarded the connect request to (first hop from joiner's perspective).
    pub(crate) first_hop: Option<Box<PeerKeyLocation>>,
    pub(crate) backoff: Option<Backoff>,
    pub(crate) desired_location: Option<Location>,
    /// Tracks when we last forwarded this connect to a peer, to avoid hammering the same
    /// neighbors when no acceptors are available. Peers without an entry are treated as
    /// immediately eligible.
    recency: HashMap<PeerKeyLocation, Instant>,
    forward_attempts: HashMap<PeerKeyLocation, ForwardAttempt>,
    connect_forward_estimator: Arc<RwLock<ConnectForwardEstimator>>,
}

impl ConnectOp {
    fn record_forward_outcome(&mut self, peer: &PeerKeyLocation, desired: Location, success: bool) {
        self.forward_attempts.remove(peer);
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
        let started_without_address = observed_address.is_none();
        let state = ConnectState::WaitingForResponses(JoinerState {
            target_connections,
            observed_address,
            accepted: HashSet::new(),
            last_progress: Instant::now(),
            started_without_address,
        });
        Self {
            id,
            state: Some(state),
            first_hop: gateway.map(Box::new),
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
            first_hop: None,
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
        self.first_hop.as_deref()
    }

    /// Get the next hop address if this operation is in a state that needs to send
    /// an outbound message. For Connect, this is the first hop peer we're connecting through.
    pub(crate) fn get_next_hop_addr(&self) -> Option<std::net::SocketAddr> {
        self.first_hop.as_deref().and_then(|g| g.socket_addr())
    }

    /// Get the full target peer (including public key) for connection establishment.
    /// For Connect operations, this returns the gateway peer.
    pub(crate) fn get_target_peer(&self) -> Option<crate::ring::PeerKeyLocation> {
        self.first_hop.as_deref().cloned()
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
        let mut visited = Vec::new();
        if let Some(own_addr) = own.socket_addr() {
            visited.push(own_addr);
        }
        if let Some(target_addr) = target.socket_addr() {
            push_unique_addr(&mut visited, target_addr);
        }

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
            own.socket_addr(),
            Some(target.clone()),
            None,
            connect_forward_estimator,
        );

        let msg = ConnectMsg::Request {
            id: tx,
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
                    acceptor_loc = ?response.acceptor.location(),
                    target_connections = state.target_connections,
                    accepted_count = state.accepted.len(),
                    "connect: joiner received ConnectResponse"
                );
                let result = state.register_acceptance(response, now);
                if let Some(new_acceptor) = &result.new_acceptor {
                    self.recency.remove(&new_acceptor.peer);
                }
                tracing::info!(
                    tx = %self.id,
                    satisfied = result.satisfied,
                    accepted_count = state.accepted.len(),
                    target_connections = state.target_connections,
                    "connect: register_acceptance result"
                );
                if result.satisfied {
                    // INVARIANT: If the joiner started without knowing their external address,
                    // they must have received ObservedAddress by the time the connect completes.
                    // This catches bugs where ObservedAddress is not emitted (e.g., if the
                    // transport layer prematurely fills in the address).
                    debug_assert!(
                        !state.started_without_address || state.observed_address.is_some(),
                        "BUG: Connect completed but joiner never received ObservedAddress. \
                         This indicates the transport layer may have prematurely filled in \
                         the joiner's address, preventing ObservedAddress emission."
                    );
                    tracing::info!(
                        tx = %self.id,
                        elapsed_ms = self.id.elapsed().as_millis(),
                        "Connect operation completed"
                    );
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
                        tracing::warn!(tx = %tx, phase = "error", "connect request received without source address");
                        return Err(OpError::OpNotPresent(tx));
                    }
                    (ConnectMsg::ObservedAddress { address, .. }, _) => {
                        // ObservedAddress arrived but no operation exists - this can happen if:
                        // 1. We're not the joiner (wrong recipient)
                        // 2. The operation was never created (shouldn't happen)
                        // Still try to update our address since this might be meant for us
                        let location = Location::from_address(address);
                        op_manager.ring.connection_manager.set_own_addr(*address);
                        op_manager
                            .ring
                            .connection_manager
                            .update_location(Some(location));
                        tracing::info!(
                            tx = %tx,
                            observed_address = %address,
                            location = %location,
                            "connect: updated own_addr from ObservedAddress (no op state found)"
                        );
                        return Err(OpError::OpNotPresent(tx));
                    }
                    _ => {
                        tracing::debug!(%tx, "connect received message without existing state");
                        return Err(OpError::OpNotPresent(tx));
                    }
                };
                Ok(OpInitialization { op, source_addr })
            }
            Err(err) => {
                // Special case: ObservedAddress can arrive when the operation is in various states:
                // - Completed: The joiner received Response before ObservedAddress
                // - Running: The operation is currently being processed elsewhere
                // We still need to update our external address in both cases.
                if let ConnectMsg::ObservedAddress { address, .. } = msg {
                    let location = Location::from_address(address);
                    op_manager.ring.connection_manager.set_own_addr(*address);
                    op_manager
                        .ring
                        .connection_manager
                        .update_location(Some(location));
                    tracing::info!(
                        tx = %tx,
                        observed_address = %address,
                        location = %location,
                        error = ?err,
                        "connect: updated own_addr from ObservedAddress (op state: {:?})",
                        err
                    );
                }
                Err(err.into())
            }
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

                    if let Some((_target, address)) = actions.observed_address {
                        let msg = ConnectMsg::ObservedAddress {
                            id: self.id,
                            address,
                        };
                        // Route through upstream (where the request came from) using hop-by-hop routing.
                        // Note: upstream_addr is already validated from source_addr at the start of this match arm.
                        tracing::debug!(
                            tx = %self.id,
                            observed_address = %address,
                            sending_to = %upstream_addr,
                            "Sending ObservedAddress to joiner"
                        );
                        network_bridge
                            .send(upstream_addr, NetMessage::V1(NetMessageV1::Connect(msg)))
                            .await?;
                    }

                    if let Some(peer) = actions.expect_connection_from {
                        if let Some(addr) = peer.socket_addr() {
                            // Log to confirm this code path is executed
                            tracing::info!(
                                joiner_addr = %addr,
                                tx = %self.id,
                                "connect: acceptor accepted joiner, initiating hole punch"
                            );

                            // Register expectation for incoming connection from joiner
                            op_manager
                                .notify_node_event(NodeEvent::ExpectPeerConnection { addr })
                                .await?;

                            // UDP hole punching: The acceptor must ALSO initiate an outbound
                            // connection to the joiner. This creates a NAT binding on the
                            // acceptor's side, allowing the joiner's packets to pass through.
                            // Without this, NAT peers cannot connect to each other because
                            // only the joiner would be sending packets.
                            let (callback, mut rx) = mpsc::channel(1);
                            op_manager
                                .notify_node_event(NodeEvent::ConnectPeer {
                                    peer: peer.clone(),
                                    tx: self.id,
                                    callback,
                                    is_gw: false,
                                })
                                .await?;

                            // Don't block waiting for connection result - the joiner will
                            // also be connecting to us simultaneously. Just spawn a task
                            // to log the outcome.
                            let tx_id = self.id;
                            let peer_clone = peer.clone();
                            tokio::spawn(async move {
                                if let Some(result) = rx.recv().await {
                                    match result {
                                        Ok((connected_peer, _)) => {
                                            tracing::info!(
                                                %connected_peer,
                                                tx=%tx_id,
                                                "connect: acceptor hole-punch connection succeeded"
                                            );
                                        }
                                        Err(_) => {
                                            tracing::debug!(
                                                %peer_clone,
                                                tx=%tx_id,
                                                "connect: acceptor hole-punch connection failed (joiner may connect to us instead)"
                                            );
                                        }
                                    }
                                }
                            });
                        }
                    }

                    if let Some((next, request)) = actions.forward {
                        // Record recency for this forward to avoid hammering the same neighbor.
                        self.recency.insert(next.clone(), Instant::now());
                        let forward_msg = ConnectMsg::Request {
                            id: self.id,
                            payload: request,
                        };
                        if let Some(next_addr) = next.socket_addr() {
                            network_bridge
                                .send(
                                    next_addr,
                                    NetMessage::V1(NetMessageV1::Connect(forward_msg)),
                                )
                                .await?;
                        }
                    }

                    if let Some(response) = actions.accept_response {
                        let response_msg = ConnectMsg::Response {
                            id: self.id,
                            payload: response,
                        };
                        // Route the response through upstream (where the request came from)
                        // using hop-by-hop routing. We don't embed target in the message.
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
                    // Fill in acceptor's external address from source_addr if unknown.
                    // The acceptor doesn't know their own external address (especially behind NAT),
                    // so the first peer that receives the response fills it in from the
                    // transport layer's source address.
                    let payload = if payload.acceptor.peer_addr.is_unknown() {
                        if let Some(acceptor_addr) = source_addr {
                            let mut updated = payload.clone();
                            updated.acceptor.peer_addr = PeerAddr::Known(acceptor_addr);
                            tracing::debug!(
                                acceptor_pub_key = %updated.acceptor.pub_key(),
                                acceptor_addr = %acceptor_addr,
                                "connect: filled acceptor address from source_addr"
                            );
                            updated
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

                    if let Some(ConnectState::WaitingForResponses(_)) = &self.state {
                        // Joiner: process the response and connect to acceptor
                        if let Some(acceptance) = self.handle_response(&payload, Instant::now()) {
                            // Note: Location assignment happens in ObservedAddress handler,
                            // not here. The joiner's ring location is derived from their
                            // external IP address (observed by the gateway), not from
                            // the routing target (desired_location).

                            if let Some(new_acceptor) = acceptance.new_acceptor {
                                if let Some(addr) = new_acceptor.peer.socket_addr() {
                                    op_manager
                                        .notify_node_event(
                                            crate::message::NodeEvent::ExpectPeerConnection {
                                                addr,
                                            },
                                        )
                                        .await?;
                                }

                                let (callback, mut rx) = mpsc::channel(1);
                                op_manager
                                    .notify_node_event(NodeEvent::ConnectPeer {
                                        peer: new_acceptor.peer.clone(),
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
                                            elapsed_ms = self.id.elapsed().as_millis(),
                                            "connect joined peer"
                                        );
                                    } else {
                                        tracing::warn!(
                                            tx=%self.id,
                                            elapsed_ms = self.id.elapsed().as_millis(),
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
                        // Relay: forward response toward joiner via hop-by-hop routing
                        let (forwarded, desired, upstream_addr) = {
                            let st = state;
                            (
                                st.forwarded_to.clone(),
                                st.request.desired_location,
                                st.upstream_addr,
                            )
                        };
                        if let Some(fwd) = forwarded {
                            self.record_forward_outcome(&fwd, desired, true);
                        }

                        tracing::debug!(
                            upstream_addr = %upstream_addr,
                            acceptor_pub_key = %payload.acceptor.pub_key(),
                            "connect: forwarding response towards joiner"
                        );
                        // Forward response toward the joiner via upstream using hop-by-hop routing
                        let forward_msg = ConnectMsg::Response {
                            id: self.id,
                            payload,
                        };
                        network_bridge
                            .send(
                                upstream_addr,
                                NetMessage::V1(NetMessageV1::Connect(forward_msg)),
                            )
                            .await?;
                        Ok(store_operation_state(&mut self))
                    } else {
                        tracing::warn!(
                            tx = %self.id,
                            state = ?self.state,
                            "connect: received Response but not in WaitingForResponses or Relaying state"
                        );
                        Ok(store_operation_state(&mut self))
                    }
                }
                ConnectMsg::ObservedAddress { address, .. } => {
                    // DEBUG: Log what state we're in when receiving ObservedAddress
                    let state_desc = match &self.state {
                        Some(ConnectState::WaitingForResponses(_)) => {
                            "WaitingForResponses (joiner)"
                        }
                        Some(ConnectState::Relaying(_)) => "Relaying (relay/gateway)",
                        Some(ConnectState::Completed) => "Completed",
                        None => "None",
                    };
                    let current_addr = op_manager.ring.connection_manager.get_own_addr();
                    tracing::debug!(
                        tx = %self.id,
                        state = state_desc,
                        current_own_addr = ?current_addr,
                        received_observed_address = %address,
                        "ObservedAddress received"
                    );

                    self.handle_observed_address(*address, Instant::now());
                    // Update this node's external address and ring location based on the observed
                    // address. The joiner doesn't know their external IP (behind NAT), so the
                    // gateway observes it from the UDP packet source and sends it back.
                    // We must update both:
                    // 1. own_addr - so diagnostics reports the correct address
                    // 2. own_location - so routing uses the correct ring location
                    let location = Location::from_address(address);
                    op_manager.ring.connection_manager.set_own_addr(*address);
                    op_manager
                        .ring
                        .connection_manager
                        .update_location(Some(location));
                    tracing::info!(
                        tx = %self.id,
                        observed_address = %address,
                        location = %location,
                        "connect: updated own_addr and location from observed address"
                    );
                    Ok(store_operation_state(&mut self))
                }
            }
        })
    }
}

/// Skip list that combines visited peers with the current node's own address.
/// This ensures we never select ourselves as a forwarding target, even if
/// self wasn't properly added to the visited list by upstream callers.
struct SkipListWithSelf<'a> {
    visited: &'a [SocketAddr],
    self_addr: Option<SocketAddr>,
}

impl Contains<SocketAddr> for SkipListWithSelf<'_> {
    fn has_element(&self, target: SocketAddr) -> bool {
        // Check if target matches our own address
        if let Some(self_addr) = self.self_addr {
            if target == self_addr {
                return true;
            }
        }
        // Check if target is in the visited list
        self.visited.contains(&target)
    }
}

impl Contains<&SocketAddr> for SkipListWithSelf<'_> {
    fn has_element(&self, target: &SocketAddr) -> bool {
        self.has_element(*target)
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
    // Hop-by-hop routing: messages are sent directly via network_bridge.send() with
    // explicit target addresses. No next_hop is embedded in the result.
    OperationResult {
        return_msg: msg.map(|m| NetMessage::V1(NetMessageV1::Connect(m))),
        next_hop: None,
        state: state_clone.map(|state| {
            OpEnum::Connect(Box::new(ConnectOp {
                id: op.id,
                state: Some(state),
                first_hop: op.first_hop.clone(),
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
    let location = gateway.location().ok_or_else(|| {
        tracing::error!(
            phase = "error",
            "Gateway location not found, this should not be possible, report an error"
        );
        OpError::ConnError(ConnectionError::LocationUnknown)
    })?;

    let gateway_addr = gateway.socket_addr().ok_or_else(|| {
        tracing::error!(phase = "error", "Gateway address not found");
        OpError::ConnError(ConnectionError::LocationUnknown)
    })?;

    if !op_manager
        .ring
        .connection_manager
        .should_accept(location, gateway_addr)
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
            tracing::error!(phase = "error", "Max number of retries reached");
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

    op.first_hop = Some(Box::new(gateway.clone()));
    if let Some(backoff) = backoff {
        op.backoff = Some(backoff);
    }

    tracing::info!(
        gateway = %gateway.pub_key(),
        tx = %tx,
        target_connections,
        ttl,
        "Attempting network join using connect"
    );

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
            tracing::warn!(
                phase = "error",
                "No gateways available, aborting join procedure"
            );
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
            _recency: &HashMap<PeerKeyLocation, Instant>,
            _estimator: &ConnectForwardEstimator,
        ) -> Option<PeerKeyLocation> {
            self.next_hop.clone()
        }
    }

    fn make_peer(port: u16) -> PeerKeyLocation {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let keypair = TransportKeypair::new();
        PeerKeyLocation::new(keypair.public().clone(), addr)
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
            peer.clone(),
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
            upstream_addr: joiner.socket_addr().expect("test peer must have address"),
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
        // Verify acceptor has correct identity
        assert_eq!(response.acceptor.pub_key(), self_loc.pub_key());
        // Acceptor address should be Unknown - relay fills it in from packet source
        // This is critical for NAT traversal: acceptor doesn't know its external address
        assert!(
            response.acceptor.peer_addr.is_unknown(),
            "ConnectResponse acceptor should have Unknown address for NAT traversal"
        );
        assert_eq!(
            actions.expect_connection_from.unwrap().pub_key(),
            joiner.pub_key()
        );
        assert!(actions.forward.is_none());
    }

    #[test]
    fn connect_response_acceptor_starts_with_unknown_address() {
        // Updated from #2207: ConnectResponse.acceptor now starts with Unknown address.
        // The first relay that receives the response fills in the address from the
        // packet source. This is critical for NAT traversal - the acceptor (behind NAT)
        // doesn't know its own external address.
        let self_loc = make_peer(4001);
        let joiner = make_peer(5001);
        let mut state = RelayState {
            upstream_addr: joiner.socket_addr().expect("test peer must have address"),
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

        // Critical invariant: acceptor address must be Unknown initially
        // Relay will fill it in from the packet source address
        assert!(
            response.acceptor.peer_addr.is_unknown(),
            "ConnectResponse.acceptor must have Unknown address for NAT traversal"
        );
        // pub_key should still match
        assert_eq!(
            response.acceptor.pub_key(),
            self_loc.pub_key(),
            "acceptor pub_key should come from self_location"
        );
    }

    #[test]
    fn relay_forwards_when_not_accepting() {
        let self_loc = make_peer(4100);
        let joiner = make_peer(5100);
        let next_hop = make_peer(6100);
        let mut state = RelayState {
            upstream_addr: joiner.socket_addr().expect("test peer must have address"),
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
        assert_eq!(forward_to.pub_key(), next_hop.pub_key());
        assert_eq!(request.ttl, 1);
        // visited now contains SocketAddr
        assert!(request
            .visited
            .contains(&joiner.socket_addr().expect("test peer must have address")));
    }

    /// Test the terminus-only acceptance behavior: relays should NOT accept when they can
    /// forward to a closer peer, even if should_accept() returns true.
    #[test]
    fn relay_does_not_accept_when_not_at_terminus() {
        let self_loc = make_peer(4150);
        let joiner = make_peer(5150);
        let next_hop = make_peer(6150);
        let mut state = RelayState {
            upstream_addr: joiner.socket_addr().expect("test peer must have address"),
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

        // should_accept() returns true, but we have a next_hop - so we're NOT at terminus
        let ctx = TestRelayContext::new(self_loc)
            .accept(true) // Relay WANTS to accept
            .next_hop(Some(next_hop.clone())); // But there's a closer peer
        let recency = HashMap::new();
        let mut forward_attempts = HashMap::new();
        let estimator = ConnectForwardEstimator::new();
        let actions = state.handle_request(&ctx, &recency, &mut forward_attempts, &estimator);

        // Should NOT accept (not at terminus)
        assert!(
            actions.accept_response.is_none(),
            "relay should NOT accept when not at terminus, even if should_accept() is true"
        );

        // Should forward to next hop
        let (forward_to, _) = actions
            .forward
            .expect("relay should forward when not at terminus");
        assert_eq!(forward_to.pub_key(), next_hop.pub_key());
    }

    #[test]
    fn relay_emits_observed_address_when_discovering_joiner_addr() {
        // Test the gateway/relay discovering the joiner's external address.
        // The joiner sends ConnectRequest with Unknown address (doesn't know their NAT address).
        // The gateway/relay observes the actual packet source address and sends ObservedAddress back.
        let self_loc = make_peer(4050);
        let joiner_base = make_peer(5050);

        // The joiner's EXTERNAL address as seen by the gateway (from the UDP packet source)
        // This is what the gateway will discover and send back via ObservedAddress
        let external_nat_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            joiner_base
                .socket_addr()
                .expect("test peer must have address")
                .port(),
        );

        // Create joiner with UNKNOWN address - the joiner doesn't know their external NAT address
        let joiner_with_unknown_addr =
            PeerKeyLocation::with_unknown_addr(joiner_base.pub_key().clone());

        let mut state = RelayState {
            // upstream_addr is the ACTUAL source address from the transport layer
            // This is the joiner's external NAT address as seen by the gateway
            upstream_addr: external_nat_addr,
            request: ConnectRequest {
                desired_location: Location::random(),
                // Joiner has Unknown address in the request
                joiner: joiner_with_unknown_addr.clone(),
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

        // Gateway should emit ObservedAddress since it discovered the joiner's external address
        let (target, addr) = actions.observed_address.expect(
            "gateway should emit ObservedAddress when discovering joiner's external address",
        );

        // The address in ObservedAddress should be the external NAT address
        assert_eq!(addr, external_nat_addr);

        // The target should have the discovered address
        assert_eq!(
            target.socket_addr().expect("target must have address"),
            external_nat_addr
        );

        // The request's joiner should be updated with the discovered address
        assert_eq!(
            state
                .request
                .joiner
                .socket_addr()
                .expect("joiner must have address after discovery"),
            external_nat_addr
        );
    }

    #[test]
    fn relay_does_not_emit_observed_address_when_joiner_addr_already_known() {
        // Test that ObservedAddress is NOT emitted when the joiner already has a known address.
        // This happens for non-first hops in the routing path.
        let self_loc = make_peer(4051);
        let joiner_base = make_peer(5051);
        let known_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 11)),
            joiner_base
                .socket_addr()
                .expect("test peer must have address")
                .port(),
        );

        // Joiner already has a Known address (filled in by the gateway/first hop)
        let joiner_with_known_addr =
            PeerKeyLocation::new(joiner_base.pub_key().clone(), known_addr);

        let mut state = RelayState {
            upstream_addr: known_addr,
            request: ConnectRequest {
                desired_location: Location::random(),
                joiner: joiner_with_known_addr.clone(),
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

        // Should NOT emit ObservedAddress since the address was already known
        assert!(
            actions.observed_address.is_none(),
            "non-first hop should NOT emit ObservedAddress when joiner addr already known"
        );
    }

    #[test]
    fn joiner_tracks_acceptance() {
        let acceptor = make_peer(7000);
        let mut state = JoinerState {
            target_connections: 1,
            observed_address: None,
            accepted: HashSet::new(),
            last_progress: Instant::now(),
            started_without_address: true,
        };

        let response = ConnectResponse {
            acceptor: acceptor.clone(),
        };
        let result = state.register_acceptance(&response, Instant::now());
        assert!(result.satisfied);
        let new = result.new_acceptor.expect("expected new acceptor");
        assert_eq!(new.peer.pub_key(), acceptor.pub_key());
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
            ConnectMsg::Request { payload, .. } => {
                assert_eq!(payload.desired_location, desired);
                assert_eq!(payload.ttl, ttl);
                // visited now contains SocketAddr, not PeerKeyLocation
                if let Some(own_addr) = own.socket_addr() {
                    assert!(payload.visited.contains(&own_addr));
                }
                if let Some(target_addr) = target.socket_addr() {
                    assert!(payload.visited.contains(&target_addr));
                }
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

        let joiner_addr = joiner.socket_addr().expect("test peer must have address");
        let request = ConnectRequest {
            desired_location: Location::random(),
            joiner: joiner.clone(),
            ttl: 3,
            visited: vec![joiner_addr],
        };

        let tx = Transaction::new::<ConnectMsg>();
        let mut relay_op = ConnectOp::new_relay(
            tx,
            joiner_addr,
            request.clone(),
            Arc::new(RwLock::new(ConnectForwardEstimator::new())),
        );
        let ctx = TestRelayContext::new(relay_a.clone())
            .accept(false)
            .next_hop(Some(relay_b.clone()));
        let estimator = ConnectForwardEstimator::new();
        let actions = relay_op.handle_request(&ctx, joiner_addr, request.clone(), &estimator);

        let (forward_target, forward_request) = actions
            .forward
            .expect("relay should forward when it declines to accept");
        assert_eq!(forward_target.pub_key(), relay_b.pub_key());
        assert_eq!(forward_request.ttl, 2);
        let relay_a_addr = relay_a.socket_addr().expect("test peer must have address");
        assert!(
            forward_request.visited.contains(&relay_a_addr),
            "forwarded request should record intermediate relay's address"
        );

        // Second hop should accept and notify the joiner.
        let mut accepting_relay = ConnectOp::new_relay(
            tx,
            relay_a_addr,
            forward_request.clone(),
            Arc::new(RwLock::new(ConnectForwardEstimator::new())),
        );
        let ctx_accept = TestRelayContext::new(relay_b.clone());
        let estimator = ConnectForwardEstimator::new();
        let accept_actions =
            accepting_relay.handle_request(&ctx_accept, relay_a_addr, forward_request, &estimator);

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

    /// Regression test for issue #2141: expect_connection_from must have the joiner's
    /// observed external address for NAT hole-punching to work.
    #[test]
    fn connect_expect_connection_uses_observed_address() {
        // Joiner behind NAT: original creation used private address, but the network bridge
        // fills in the observed public address from the packet source.
        let private_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 9000);
        let keypair = TransportKeypair::new();
        let joiner_original = PeerKeyLocation::new(keypair.public().clone(), private_addr);

        // Gateway observes joiner's public/external address and fills it into joiner.peer_addr
        let observed_public_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 50)), 9000);
        let joiner_with_observed_addr =
            PeerKeyLocation::new(keypair.public().clone(), observed_public_addr);

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

        // Critical: expect_connection_from must have the observed public address
        // so the acceptor can initiate hole-punching to the correct address
        let expect_conn = actions
            .expect_connection_from
            .expect("expect_connection_from should be set when accepting");
        assert_eq!(
            expect_conn
                .socket_addr()
                .expect("expect_connection_from must have address"),
            observed_public_addr,
            "expect_connection_from must use observed external address ({}) not private address ({})",
            observed_public_addr,
            private_addr
        );

        // Double-check: the original joiner had the private address
        assert_eq!(
            joiner_original
                .socket_addr()
                .expect("original joiner must have address"),
            private_addr,
            "original joiner should have private address"
        );
    }

    // Note: The SkipListWithSelf test has been removed as it now requires a ConnectionManager
    // to look up peers by address. The skip list behavior is tested via integration tests
    // and the self-exclusion logic is straightforward.

    /// Regression test: ConnectResponse acceptor must have Unknown address initially.
    /// The relay that first receives the response fills in the acceptor's address from
    /// the packet source. This is critical for NAT traversal - the acceptor (behind NAT)
    /// doesn't know its own external address.
    ///
    /// Bug scenario this prevents:
    /// 1. Peer1 (behind NAT at 192.168.1.x) accepts joiner and creates ConnectResponse
    /// 2. If acceptor uses self_location(), it has 127.0.0.1 or private IP
    /// 3. Joiner receives response with wrong address, can't connect back
    /// 4. NAT hole-punching fails - joiner sends to wrong address
    ///
    /// Correct behavior:
    /// 1. Peer1 creates ConnectResponse with acceptor.peer_addr = Unknown
    /// 2. First relay receives response, fills in acceptor address from UDP source
    /// 3. Joiner receives response with correct external address
    /// 4. NAT hole-punching works - both peers have each other's external addresses
    #[test]
    fn connect_response_acceptor_has_unknown_address() {
        let joiner = make_peer(9100);
        let acceptor_peer = make_peer(9200);

        let joiner_addr = joiner.socket_addr().expect("test peer must have address");
        let request = ConnectRequest {
            desired_location: Location::random(),
            joiner: joiner.clone(),
            ttl: 3,
            visited: vec![joiner_addr],
        };

        let tx = Transaction::new::<ConnectMsg>();
        let mut relay_op = ConnectOp::new_relay(
            tx,
            joiner_addr,
            request.clone(),
            Arc::new(RwLock::new(ConnectForwardEstimator::new())),
        );

        // Acceptor context - this peer will accept the joiner
        let ctx = TestRelayContext::new(acceptor_peer.clone());
        let estimator = ConnectForwardEstimator::new();
        let actions = relay_op.handle_request(&ctx, joiner_addr, request.clone(), &estimator);

        // Verify acceptance was issued
        let response = actions
            .accept_response
            .expect("acceptor should issue ConnectResponse");

        // CRITICAL: acceptor's address must be Unknown, not the acceptor's local address
        assert!(
            response.acceptor.peer_addr.is_unknown(),
            "acceptor address should be Unknown for NAT traversal. Got: {:?}",
            response.acceptor.peer_addr
        );

        // The pub_key should be set correctly
        assert_eq!(
            response.acceptor.pub_key(),
            acceptor_peer.pub_key(),
            "acceptor pub_key should match"
        );
    }
}
