//! Prototype implementation of the simplified two-message connect flow.
//!
//! This module is *not yet wired in*. It exists so we can develop the new
//! handshake logic incrementally while keeping the current connect operation
//! intact. Once fully implemented we will switch the node to use this module
//! and delete the legacy code.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::dev_tool::Location;
use crate::message::Transaction;
use crate::node::{OpManager, PeerId};
use crate::ring::PeerKeyLocation;
use crate::util::{Backoff, Contains};

/// Top-level message envelope used by the new connect handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ConnectMsgV2 {
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
#[derive(Debug)]
pub(crate) enum ConnectState {
    /// Joiner waiting for acceptances.
    WaitingForResponses(JoinerState),
    /// Intermediate peer evaluating and forwarding requests.
    Relaying(Box<RelayState>),
    /// Joiner obtained the required neighbours.
    Completed,
}

#[derive(Debug)]
pub(crate) struct JoinerState {
    pub desired_location: Location,
    pub target_connections: usize,
    pub observed_address: Option<SocketAddr>,
    pub accepted: HashSet<PeerKeyLocation>,
    pub last_progress: Instant,
}

#[derive(Debug)]
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
            if let Some(next) =
                ctx.select_next_hop(self.request.desired_location, &self.request.visited)
            {
                let mut forward_req = self.request.clone();
                forward_req.ttl = forward_req.ttl.saturating_sub(1);
                push_unique_peer(&mut forward_req.visited, ctx.self_location().clone());
                let forward_snapshot = forward_req.clone();
                self.forwarded_to = Some(next.clone());
                self.request = forward_req;
                actions.forward = Some((next, forward_snapshot));
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
#[derive(Debug)]
pub(crate) struct ConnectOpV2 {
    pub(crate) id: Transaction,
    pub(crate) state: Option<ConnectState>,
    pub(crate) gateway: Option<Box<PeerKeyLocation>>,
    pub(crate) backoff: Option<Backoff>,
}

impl ConnectOpV2 {
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
            desired_location,
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
        }
    }

    pub(crate) fn is_completed(&self) -> bool {
        matches!(self.state, Some(ConnectState::Completed))
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
