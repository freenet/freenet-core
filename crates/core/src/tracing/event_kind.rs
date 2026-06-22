use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    message::Transaction,
    node::PeerId,
    ring::{Location, PeerKeyLocation},
    router::RouteEvent,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[non_exhaustive]
#[allow(private_interfaces)]
// todo: make this take by ref instead, probably will need an owned version
pub enum EventKind {
    Connect(ConnectEvent),
    Put(PutEvent),
    Get(GetEvent),
    Subscribe(SubscribeEvent),
    Route(RouteEvent),
    Update(UpdateEvent),
    /// Data transfer events for stream-level transfers.
    Transfer(TransferEvent),
    /// Peer lifecycle events (startup, shutdown).
    Lifecycle(PeerLifecycleEvent),
    Ignored,
    Disconnected {
        from: PeerId,
        /// Structured reason for disconnection.
        reason: DisconnectReason,
        /// How long the connection was open before disconnecting (milliseconds).
        /// None if duration tracking is unavailable.
        connection_duration_ms: Option<u64>,
        /// Bytes sent to this peer during the connection.
        /// None if byte tracking is unavailable.
        bytes_sent: Option<u64>,
        /// Bytes received from this peer during the connection.
        /// None if byte tracking is unavailable.
        bytes_received: Option<u64>,
    },
    /// A transaction timed out without completing.
    Timeout {
        /// The transaction that timed out.
        id: Transaction,
        timestamp: u64,
        /// The operation type (e.g. "connect", "put", "get", "subscribe", "update").
        op_type: String,
        /// The target peer for the operation, if known from routing info.
        target_peer: Option<String>,
    },
    /// Periodic transport layer metrics snapshot.
    ///
    /// Emitted every N seconds (default 30s) with aggregate transport statistics.
    /// This is more efficient than per-transfer events and provides trend data.
    TransportSnapshot(crate::transport::TransportSnapshot),
    /// Interest sync events for delta-based state synchronization.
    ///
    /// Tracks ResyncRequests and ResyncResponses which indicate delta application
    /// failures. Useful for monitoring the health of the delta sync protocol.
    InterestSync(InterestSyncEvent),
    /// A routing decision snapshot: which peers were considered and why one was selected.
    ///
    /// Currently emitted via `tracing::debug!` at call sites (sync context prevents
    /// async event registration). This variant is available for future async callers
    /// that want to emit routing decisions through OTLP with sampling.
    RoutingDecision(crate::router::RoutingDecisionInfo),
    /// Periodic snapshot of the router model (isotonic regression curves, event counts).
    ///
    /// Boxed because `RouterSnapshotInfo` is by far the largest `EventKind`
    /// payload (regression curves + per-op maps + the #4440 node-health gauges);
    /// inlining it would bloat every other variant (`clippy::large_enum_variant`).
    /// `Box` serializes transparently, so the AOF/OTLP wire format is unchanged.
    RouterSnapshot(Box<crate::router::RouterSnapshotInfo>),
}

impl EventKind {
    pub(crate) const CONNECT: u8 = 0;
    pub(crate) const PUT: u8 = 1;
    pub(crate) const GET: u8 = 2;
    pub(crate) const ROUTE: u8 = 3;
    pub(crate) const SUBSCRIBE: u8 = 4;
    pub(crate) const IGNORED: u8 = 5;
    pub(crate) const DISCONNECTED: u8 = 6;
    pub(crate) const UPDATE: u8 = 7;
    pub(crate) const TIMEOUT: u8 = 8;
    pub(crate) const TRANSFER: u8 = 9;
    pub(crate) const LIFECYCLE: u8 = 10;
    pub(crate) const TRANSPORT_SNAPSHOT: u8 = 11;
    pub(crate) const INTEREST_SYNC: u8 = 12;
    pub(crate) const ROUTING_DECISION: u8 = 13;
    pub(crate) const ROUTER_SNAPSHOT: u8 = 14;

    pub(crate) const fn varint_id(&self) -> u8 {
        match self {
            EventKind::Connect(_) => Self::CONNECT,
            EventKind::Put(_) => Self::PUT,
            EventKind::Get(_) => Self::GET,
            EventKind::Route(_) => Self::ROUTE,
            EventKind::Subscribe(_) => Self::SUBSCRIBE,
            EventKind::Ignored => Self::IGNORED,
            EventKind::Disconnected { .. } => Self::DISCONNECTED,
            EventKind::Update(_) => Self::UPDATE,
            EventKind::Timeout { .. } => Self::TIMEOUT,
            EventKind::Transfer(_) => Self::TRANSFER,
            EventKind::Lifecycle(_) => Self::LIFECYCLE,
            EventKind::TransportSnapshot(_) => Self::TRANSPORT_SNAPSHOT,
            EventKind::InterestSync(_) => Self::INTEREST_SYNC,
            EventKind::RoutingDecision(_) => Self::ROUTING_DECISION,
            EventKind::RouterSnapshot(_) => Self::ROUTER_SNAPSHOT,
        }
    }

    /// Extracts the contract key from this event, if applicable.
    ///
    /// Returns the key for Put, Get, Subscribe, and Update events.
    pub fn contract_key(&self) -> Option<freenet_stdlib::prelude::ContractKey> {
        match self {
            EventKind::Put(put) => Some(put.contract_key()),
            EventKind::Get(get) => get.contract_key(),
            EventKind::Subscribe(sub) => sub.contract_key(),
            EventKind::Update(upd) => Some(upd.contract_key()),
            EventKind::Connect(_)
            | EventKind::Route(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
            EventKind::InterestSync(InterestSyncEvent::StateConfirmed { key, .. }) => Some(*key),
            EventKind::InterestSync(_) => None,
        }
    }

    /// Extracts the state hash from this event, if applicable.
    ///
    /// Returns the hash for Put and Update success/broadcast events.
    pub fn state_hash(&self) -> Option<&str> {
        match self {
            EventKind::Put(put) => put.state_hash(),
            EventKind::Update(upd) => upd.state_hash(),
            EventKind::Connect(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns the state hash only for events representing stored state changes.
    ///
    /// This is critical for convergence checking - it only returns hashes for events
    /// where state was actually stored locally:
    /// - PutSuccess: State was stored after PUT operation
    /// - UpdateSuccess: State was updated locally
    /// - BroadcastApplied: State was stored after applying received broadcast
    ///
    /// Does NOT return hashes for:
    /// - BroadcastReceived: Incoming state that may not have been applied yet
    /// - BroadcastEmitted: Outgoing state being sent to others
    ///
    /// Using `state_hash()` for convergence checking would include BroadcastReceived
    /// events, which record the incoming state hash before application, not the
    /// actual stored state after merge/application.
    pub fn stored_state_hash(&self) -> Option<&str> {
        match self {
            EventKind::Put(put) => put.stored_state_hash(),
            EventKind::Update(upd) => upd.stored_state_hash(),
            EventKind::Connect(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
            EventKind::InterestSync(InterestSyncEvent::StateConfirmed { state_hash, .. }) => {
                Some(state_hash.as_str())
            }
            EventKind::InterestSync(_) => None,
        }
    }

    /// Returns true if this is an Update BroadcastEmitted event.
    pub fn is_update_broadcast_emitted(&self) -> bool {
        matches!(
            self,
            EventKind::Update(UpdateEvent::BroadcastEmitted { .. })
        )
    }

    /// Returns router snapshot summary data for `RouterSnapshot` events.
    ///
    /// Returns `(failure_events, success_events, prediction_active)`.
    /// `failure_events` counts all events in the failure estimator (successes scored 0.0,
    /// failures scored 1.0). `success_events` counts only timed GET successes.
    /// `prediction_active` is true when `failure_events >= 50`.
    pub fn router_snapshot_summary(&self) -> Option<(usize, usize, bool)> {
        match self {
            EventKind::RouterSnapshot(info) => Some((
                info.failure_events,
                info.success_events,
                info.prediction_active,
            )),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_) => None,
        }
    }

    /// Returns whether this is a route outcome event, and if so, whether it succeeded.
    ///
    /// Returns `Some(true)` for `RouteOutcome::Success` or `SuccessUntimed`,
    /// `Some(false)` for `RouteOutcome::Failure`, `None` for non-Route events.
    pub fn route_outcome_is_success(&self) -> Option<bool> {
        match self {
            EventKind::Route(re) => Some(match re.outcome {
                crate::router::RouteOutcome::Success { .. }
                | crate::router::RouteOutcome::SuccessUntimed => true,
                crate::router::RouteOutcome::Failure => false,
            }),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns the outcome of a GET operation event.
    ///
    /// Returns `Some(true)` for `GetSuccess`, `Some(false)` for `GetNotFound` or `GetFailure`,
    /// `None` for all other events (including GET requests and responses).
    pub fn get_outcome(&self) -> Option<bool> {
        match self {
            EventKind::Get(GetEvent::GetSuccess { .. }) => Some(true),
            EventKind::Get(GetEvent::GetNotFound { .. } | GetEvent::GetFailure { .. }) => {
                Some(false)
            }
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns the elapsed time in milliseconds for a completed GET operation.
    ///
    /// Returns `Some(ms)` for `GetSuccess`, `GetNotFound`, or `GetFailure`,
    /// `None` for all other events.
    pub fn get_elapsed_ms(&self) -> Option<u64> {
        match self {
            EventKind::Get(GetEvent::GetSuccess { elapsed_ms, .. })
            | EventKind::Get(GetEvent::GetNotFound { elapsed_ms, .. })
            | EventKind::Get(GetEvent::GetFailure { elapsed_ms, .. }) => Some(*elapsed_ms),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns `true` if this is a ForwardingAck received event.
    pub fn is_forwarding_ack_received(&self) -> bool {
        matches!(self, EventKind::Get(GetEvent::ForwardingAckReceived { .. }))
    }

    /// Returns `true` if this is a GET response sent event (contract found and response dispatched).
    pub fn is_get_response_sent(&self) -> bool {
        matches!(self, EventKind::Get(GetEvent::ResponseSent { .. }))
    }

    /// Returns `true` if this is a GET request event.
    pub fn is_get_request(&self) -> bool {
        matches!(self, EventKind::Get(GetEvent::Request { .. }))
    }

    /// Returns the HTL carried by a GET request event, `None` for all
    /// other events.
    ///
    /// Useful for distinguishing originator-side dispatch from relay
    /// hops in test analysis: the client driver's loopback Request is
    /// registered at the originating node with `htl == max_hops_to_live`,
    /// while every relay-received Request has already been decremented
    /// (#4361 — dispatched-vs-scheduled accounting).
    // Wildcard is deliberate, mirroring `hop_count`: this accessor cares
    // about exactly one variant; new variants should not require updates.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn get_request_htl(&self) -> Option<usize> {
        match self {
            EventKind::Get(GetEvent::Request { htl, .. }) => Some(*htl),
            _ => None,
        }
    }

    /// Returns whether this is a subscribe outcome event (success or failure).
    ///
    /// Returns `Some(true)` for `SubscribeSuccess`, `Some(false)` for the
    /// failure outcomes `SubscribeNotFound` and `SubscribeTimeout` (#3445),
    /// `None` for all other events (including subscribe requests/responses).
    pub fn subscribe_outcome(&self) -> Option<bool> {
        match self {
            EventKind::Subscribe(SubscribeEvent::SubscribeSuccess { .. }) => Some(true),
            EventKind::Subscribe(SubscribeEvent::SubscribeNotFound { .. })
            | EventKind::Subscribe(SubscribeEvent::SubscribeTimeout { .. }) => Some(false),
            EventKind::Connect(_)
            | EventKind::Put(_)
            | EventKind::Get(_)
            | EventKind::Subscribe(_)
            | EventKind::Route(_)
            | EventKind::Update(_)
            | EventKind::Transfer(_)
            | EventKind::Lifecycle(_)
            | EventKind::Ignored
            | EventKind::Disconnected { .. }
            | EventKind::Timeout { .. }
            | EventKind::TransportSnapshot(_)
            | EventKind::InterestSync(_)
            | EventKind::RoutingDecision(_)
            | EventKind::RouterSnapshot(_) => None,
        }
    }

    /// Returns `true` if this event is an update broadcast received by a peer.
    ///
    /// Matches `UpdateEvent::BroadcastReceived` — the moment a peer receives
    /// an update broadcast (before application). Used to verify that subscription
    /// interest is maintained across lease cycles.
    pub fn is_update_broadcast_received(&self) -> bool {
        matches!(
            self,
            EventKind::Update(UpdateEvent::BroadcastReceived { .. })
        )
    }

    /// Returns true if this is a Connect event.
    pub fn is_connect(&self) -> bool {
        matches!(self, EventKind::Connect(_))
    }

    /// Returns true if this is a Disconnected event.
    pub fn is_disconnected(&self) -> bool {
        matches!(self, EventKind::Disconnected { .. })
    }

    /// Returns true if this is a ConnectEvent::RequestReceived where accepted=true.
    pub fn is_connect_accepted(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived { accepted: true, .. })
        )
    }

    /// Returns true if this is a ConnectEvent::RequestReceived where accepted=false.
    pub fn is_connect_not_accepted(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                accepted: false,
                ..
            })
        )
    }

    /// Returns true if this is a ConnectEvent::Connected.
    pub fn is_connect_connected(&self) -> bool {
        matches!(self, EventKind::Connect(ConnectEvent::Connected { .. }))
    }

    /// Returns true if this is a ConnectEvent::RequestSent with is_initial=true.
    pub fn is_connect_initiated(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestSent {
                is_initial: true,
                ..
            })
        )
    }

    /// Returns true if this is a terminus acceptance (accepted=true, forwarded_to=None).
    pub fn is_connect_terminus_accepted(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                accepted: true,
                forwarded_to: None,
                ..
            })
        )
    }

    /// Returns true if this is a terminus rejection (accepted=false, forwarded_to=None).
    pub fn is_connect_terminus_rejected(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                accepted: false,
                forwarded_to: None,
                ..
            })
        )
    }

    /// Returns true if this is a forwarded request (forwarded_to=Some).
    pub fn is_connect_forwarded(&self) -> bool {
        matches!(
            self,
            EventKind::Connect(ConnectEvent::RequestReceived {
                forwarded_to: Some(_),
                ..
            })
        )
    }

    /// Returns the connection count from a ConnectEvent::Connected event.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn connect_peer_connection_count(&self) -> Option<usize> {
        match self {
            EventKind::Connect(ConnectEvent::Connected {
                this_peer_connection_count,
                ..
            }) => Some(*this_peer_connection_count),
            _ => None,
        }
    }

    /// Returns the contract instance id if this is an UnsubscribeReceived event.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn unsubscribe_received_instance_id(&self) -> Option<&ContractInstanceId> {
        match self {
            EventKind::Subscribe(SubscribeEvent::UnsubscribeReceived { instance_id, .. }) => {
                Some(instance_id)
            }
            _ => None,
        }
    }

    /// Returns the contract instance id if this is an UnsubscribeSent event.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn unsubscribe_sent_instance_id(&self) -> Option<&ContractInstanceId> {
        match self {
            EventKind::Subscribe(SubscribeEvent::UnsubscribeSent { instance_id, .. }) => {
                Some(instance_id)
            }
            _ => None,
        }
    }

    /// Returns the `hop_count` recorded in this event, if any.
    ///
    /// Populated for terminal GET events (`GetSuccess`, `GetNotFound`,
    /// `GetFailure`) since PR #4245, and for terminal PUT/SUBSCRIBE
    /// events (`PutSuccess`, `SubscribeSuccess`, `SubscribeNotFound`)
    /// since PR #4248.  The value is the number of forward hops the
    /// originating request traversed before reaching its terminal state.
    /// Returns `None` for non-terminal events (e.g. `Request`) and for
    /// events that don't carry a hop_count field.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub fn hop_count(&self) -> Option<usize> {
        match self {
            EventKind::Get(GetEvent::GetSuccess { hop_count, .. })
            | EventKind::Get(GetEvent::GetNotFound { hop_count, .. })
            | EventKind::Get(GetEvent::GetFailure { hop_count, .. })
            | EventKind::Put(PutEvent::PutSuccess { hop_count, .. })
            | EventKind::Subscribe(SubscribeEvent::SubscribeSuccess { hop_count, .. })
            | EventKind::Subscribe(SubscribeEvent::SubscribeNotFound { hop_count, .. }) => {
                *hop_count
            }
            _ => None,
        }
    }

    /// Returns the variant name of this event kind.
    pub fn variant_name(&self) -> &'static str {
        match self {
            EventKind::Connect(_) => "Connect",
            EventKind::Put(_) => "Put",
            EventKind::Get(_) => "Get",
            EventKind::Subscribe(_) => "Subscribe",
            EventKind::Route(_) => "Route",
            EventKind::Update(_) => "Update",
            EventKind::Transfer(_) => "Transfer",
            EventKind::Lifecycle(_) => "Lifecycle",
            EventKind::Ignored => "Ignored",
            EventKind::Disconnected { .. } => "Disconnected",
            EventKind::Timeout { .. } => "Timeout",
            EventKind::TransportSnapshot(_) => "TransportSnapshot",
            EventKind::InterestSync(_) => "InterestSync",
            EventKind::RoutingDecision(_) => "RoutingDecision",
            EventKind::RouterSnapshot(_) => "RouterSnapshot",
        }
    }
}

/// The type of connection between peers.
///
/// This helps understand network topology and debug connectivity issues.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ConnectionType {
    /// Direct UDP connection between peers.
    Direct,
    /// Connection relayed through a gateway or other peer.
    Relayed,
    /// Connection to/from a gateway node.
    Gateway,
    /// Unknown connection type (for backwards compatibility or when detection fails).
    #[default]
    Unknown,
}

/// Reason for a peer disconnection.
///
/// Understanding disconnect reasons is critical for debugging network stability.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum DisconnectReason {
    /// Connection was explicitly closed by local node.
    Explicit(String),
    /// Connection pruned due to topology optimization.
    Pruned,
    /// Connection timed out (no response within timeout period).
    Timeout,
    /// Network error (transport layer failure).
    NetworkError(String),
    /// Peer was unresponsive to keep-alive pings.
    Unresponsive,
    /// Connection dropped by the remote peer.
    RemoteDropped,
    /// Maximum connection limit reached.
    ConnectionLimitReached,
    /// Unknown reason (for backwards compatibility).
    #[default]
    Unknown,
}

/// **Deprecated**: This conversion uses fragile substring matching which can break
/// if error message formats change. Use DisconnectReason enum variants directly instead.
///
/// This impl exists only for backwards compatibility with the `disconnected()` helper.
/// New code should construct DisconnectReason variants directly.
impl From<Option<String>> for DisconnectReason {
    fn from(reason: Option<String>) -> Self {
        match reason {
            Some(s) if s.contains("pruned") => DisconnectReason::Pruned,
            Some(s) if s.contains("timeout") => DisconnectReason::Timeout,
            Some(s) if s.contains("unresponsive") => DisconnectReason::Unresponsive,
            Some(s) if s.contains("limit") => DisconnectReason::ConnectionLimitReached,
            Some(s) => DisconnectReason::Explicit(s),
            None => DisconnectReason::Unknown,
        }
    }
}

/// Connection lifecycle events.
///
/// Note on event format versioning: New variants are added at the end of enums
/// to maintain backwards compatibility with persisted AOF logs within a session.
/// AOF logs are session-scoped and cleared on restart, so cross-version
/// compatibility is not required.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[allow(clippy::large_enum_variant)] // Connected variant needs PeerKeyLocation for observability
pub(crate) enum ConnectEvent {
    /// Initial connection start (legacy event - see RequestSent/RequestReceived for detailed tracking).
    StartConnection {
        from: PeerId,
        /// Whether this is a connection to a gateway node.
        is_gateway: bool,
    },
    Connected {
        this: PeerKeyLocation,
        connected: PeerKeyLocation,
        /// Time elapsed since connection started (milliseconds).
        /// None when timing information is unavailable (e.g., connection events
        /// without transaction context).
        elapsed_ms: Option<u64>,
        /// Type of connection (direct, relayed, gateway).
        connection_type: ConnectionType,
        /// Smoothed RTT to the peer in milliseconds, if available.
        /// This is measured via the transport layer's RTT estimation.
        latency_ms: Option<u64>,
        /// Number of open connections this peer has after this connection.
        /// Helps identify if a peer is having widespread connectivity issues.
        this_peer_connection_count: usize,
        /// The peer that initiated the connection (joiner).
        /// Helps trace connection establishment patterns.
        initiated_by: Option<PeerId>,
    },
    /// Connection process finished (legacy event - see RequestSent/RequestReceived/ResponseReceived for detailed tracking).
    Finished {
        initiator: PeerId,
        location: Location,
        /// Time elapsed since connection started (milliseconds).
        elapsed_ms: Option<u64>,
    },

    // === Protocol Message Events ===
    // These track the actual ConnectRequest/Response messages flowing through the network,
    // enabling visualization of message routing paths in the dashboard.
    /// A ConnectRequest message was sent (by joiner or forwarded by relay).
    RequestSent {
        /// The target ring location this request is routing toward.
        desired_location: Location,
        /// The joiner's identity and location.
        joiner: PeerKeyLocation,
        /// The peer we're sending this request to.
        target: PeerKeyLocation,
        /// Remaining hops-to-live.
        ttl: u8,
        /// True if this is the initial send from the joiner (not a forward).
        is_initial: bool,
    },
    /// A ConnectRequest message was received.
    RequestReceived {
        /// The target ring location this request is routing toward.
        desired_location: Location,
        /// The joiner's identity and location.
        joiner: PeerKeyLocation,
        /// The socket address of the peer that sent us this request.
        from_addr: std::net::SocketAddr,
        /// The peer that sent us this request (if we have them in our connection table).
        /// None when receiving from a new joiner who isn't connected yet.
        from_peer: Option<PeerKeyLocation>,
        /// Where we're forwarding to (None if we're at terminus).
        forwarded_to: Option<PeerKeyLocation>,
        /// Whether we accepted this connection request.
        accepted: bool,
        /// Remaining hops-to-live when received.
        ttl: u8,
    },
    /// A ConnectResponse message was sent (acceptor sending back to joiner).
    ResponseSent {
        /// The acceptor (us) who is accepting the connection.
        acceptor: PeerKeyLocation,
        /// The joiner we're accepting.
        joiner: PeerKeyLocation,
    },
    /// A ConnectResponse message was received (joiner receiving from acceptor).
    ResponseReceived {
        /// The acceptor who accepted our connection request.
        acceptor: PeerKeyLocation,
        /// Time elapsed since we sent the original request (milliseconds).
        elapsed_ms: u64,
    },
    /// A ConnectRequest was rejected (sent back to upstream).
    Rejected {
        /// The target ring location that was being requested.
        desired_location: Location,
        /// Reason for rejection.
        reason: String,
    },
}

/// Reason for operation failure.
///
/// Understanding failure reasons helps debug network and contract issues.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum OperationFailure {
    /// Connection to peer dropped during operation.
    ConnectionDropped,
    /// Operation exceeded maximum retries.
    MaxRetriesExceeded { retries: usize },
    /// HTL (hops to live) exhausted before finding result.
    HtlExhausted,
    /// No peers available in the ring to route to.
    NoPeersAvailable,
    /// Contract handler returned an error.
    ContractError(String),
    /// Operation timed out.
    Timeout,
    /// Other failure with description.
    Other(String),
}

impl std::fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Direct => write!(f, "direct"),
            ConnectionType::Relayed => write!(f, "relayed"),
            ConnectionType::Gateway => write!(f, "gateway"),
            ConnectionType::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisconnectReason::Explicit(reason) => write!(f, "explicit: {}", reason),
            DisconnectReason::Pruned => write!(f, "pruned"),
            DisconnectReason::Timeout => write!(f, "timeout"),
            DisconnectReason::NetworkError(err) => write!(f, "network_error: {}", err),
            DisconnectReason::Unresponsive => write!(f, "unresponsive"),
            DisconnectReason::RemoteDropped => write!(f, "remote_dropped"),
            DisconnectReason::ConnectionLimitReached => write!(f, "connection_limit_reached"),
            DisconnectReason::Unknown => write!(f, "unknown"),
        }
    }
}

impl std::fmt::Display for OperationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationFailure::ConnectionDropped => write!(f, "connection_dropped"),
            OperationFailure::MaxRetriesExceeded { retries } => {
                write!(f, "max_retries_exceeded: {}", retries)
            }
            OperationFailure::HtlExhausted => write!(f, "htl_exhausted"),
            OperationFailure::NoPeersAvailable => write!(f, "no_peers_available"),
            OperationFailure::ContractError(err) => write!(f, "contract_error: {}", err),
            OperationFailure::Timeout => write!(f, "timeout"),
            OperationFailure::Other(reason) => write!(f, "other: {}", reason),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum PutEvent {
    Request {
        id: Transaction,
        requester: PeerKeyLocation,
        key: ContractKey,
        target: PeerKeyLocation,
        /// Hops to live - remaining hops before request fails.
        htl: usize,
        timestamp: u64,
    },
    PutSuccess {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        /// Number of hops the request traversed (initial HTL - remaining HTL).
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
        /// Short hash of the stored state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
        /// Size of the stored state in bytes.
        state_size: Option<usize>,
    },
    /// Put operation failed.
    PutFailure {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        /// Number of hops the request traversed before failure.
        hop_count: Option<usize>,
        /// Reason for the failure.
        reason: OperationFailure,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Put response being sent back to the requester.
    ///
    /// Tracks when this peer sends a successful put response to an upstream peer.
    /// This provides sender-side visibility for debugging message routing.
    ResponseSent {
        id: Transaction,
        /// The peer sending the response.
        from: PeerKeyLocation,
        /// The peer receiving the response.
        to: PeerKeyLocation,
        key: ContractKey,
        timestamp: u64,
    },
    BroadcastEmitted {
        id: Transaction,
        upstream: PeerKeyLocation,
        /// subscribed peers
        broadcast_to: Vec<PeerKeyLocation>,
        broadcasted_to: usize,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: WrappedState,
        sender: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the broadcast state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    BroadcastReceived {
        id: Transaction,
        /// peer who started the broadcast op
        requester: PeerKeyLocation,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: WrappedState,
        /// target peer
        target: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the received state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
}

impl PutEvent {
    /// Returns the contract key for this event.
    fn contract_key(&self) -> ContractKey {
        match self {
            PutEvent::Request { key, .. }
            | PutEvent::PutSuccess { key, .. }
            | PutEvent::PutFailure { key, .. }
            | PutEvent::ResponseSent { key, .. }
            | PutEvent::BroadcastEmitted { key, .. }
            | PutEvent::BroadcastReceived { key, .. } => *key,
        }
    }

    /// Returns the state hash if available.
    fn state_hash(&self) -> Option<&str> {
        match self {
            PutEvent::PutSuccess { state_hash, .. }
            | PutEvent::BroadcastEmitted { state_hash, .. }
            | PutEvent::BroadcastReceived { state_hash, .. } => state_hash.as_deref(),
            PutEvent::Request { .. }
            | PutEvent::PutFailure { .. }
            | PutEvent::ResponseSent { .. } => None,
        }
    }

    /// Returns the state hash only for events representing stored state.
    ///
    /// Only returns hash for PutSuccess (state actually stored locally).
    /// Does NOT return hash for BroadcastEmitted/BroadcastReceived (state in transit).
    fn stored_state_hash(&self) -> Option<&str> {
        match self {
            PutEvent::PutSuccess { state_hash, .. } => state_hash.as_deref(),
            PutEvent::Request { .. }
            | PutEvent::PutFailure { .. }
            | PutEvent::ResponseSent { .. }
            | PutEvent::BroadcastEmitted { .. }
            | PutEvent::BroadcastReceived { .. } => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum UpdateEvent {
    Request {
        id: Transaction,
        requester: PeerKeyLocation,
        key: ContractKey,
        target: PeerKeyLocation,
        timestamp: u64,
    },
    UpdateSuccess {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        timestamp: u64,
        /// Short hash of state before update (first 4 bytes of Blake3, 8 hex chars).
        state_hash_before: Option<String>,
        /// Short hash of state after update (first 4 bytes of Blake3, 8 hex chars).
        state_hash_after: Option<String>,
        /// Size of the state after update in bytes.
        state_size: Option<usize>,
    },
    BroadcastEmitted {
        id: Transaction,
        upstream: PeerKeyLocation,
        /// subscribed peers
        broadcast_to: Vec<PeerKeyLocation>,
        broadcasted_to: usize,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was updated
        value: WrappedState,
        sender: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the broadcast state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    /// Update operation failed.
    UpdateFailure {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        key: ContractKey,
        /// Reason for the failure.
        reason: OperationFailure,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Emitted after broadcasting completes with delta sync statistics.
    /// This provides telemetry for monitoring delta sync effectiveness.
    BroadcastComplete {
        id: Transaction,
        key: ContractKey,
        /// Number of peers that received a delta update.
        delta_sends: usize,
        /// Number of peers that received full state (no cached summary or delta failed).
        full_state_sends: usize,
        /// Total bytes saved by sending deltas instead of full state.
        /// Calculated as sum of (state_size - delta_size) for each delta send.
        bytes_saved: u64,
        /// Size of the full state in bytes.
        state_size: usize,
        timestamp: u64,
    },
    BroadcastReceived {
        id: Transaction,
        /// peer who started the broadcast op
        requester: PeerKeyLocation,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was updated
        value: WrappedState,
        /// target peer
        target: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the received state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    /// Emitted after a broadcast update has been applied locally.
    /// This captures the resulting state after the delta/merge operation,
    /// enabling state convergence monitoring across the network.
    BroadcastApplied {
        id: Transaction,
        /// key of the contract which value was updated
        key: ContractKey,
        /// this peer (where the update was applied)
        target: PeerKeyLocation,
        timestamp: u64,
        /// Short hash of the incoming broadcast state (before merge).
        state_hash_before: Option<String>,
        /// Short hash of the resulting state (after merge).
        state_hash_after: Option<String>,
        /// Whether the local state actually changed after applying the update.
        changed: bool,
        /// Size of the state after applying the update in bytes.
        state_size: usize,
    },
    /// Emitted after handle_broadcast_state_change() completes with a full
    /// breakdown of why each potential peer was or was not sent the broadcast.
    /// This enables diagnosing missed broadcast deliveries (issue #3046).
    BroadcastDeliverySummary {
        key: ContractKey,
        proximity_found: usize,
        proximity_resolve_failed: usize,
        interest_found: usize,
        interest_resolve_failed: usize,
        skipped_self: usize,
        skipped_sender: usize,
        skipped_summary_match: usize,
        targets_sent: usize,
        send_failed: usize,
        timestamp: u64,
    },
}

impl UpdateEvent {
    /// Returns the contract key for this event.
    fn contract_key(&self) -> ContractKey {
        match self {
            UpdateEvent::Request { key, .. }
            | UpdateEvent::UpdateSuccess { key, .. }
            | UpdateEvent::BroadcastEmitted { key, .. }
            | UpdateEvent::BroadcastComplete { key, .. }
            | UpdateEvent::BroadcastReceived { key, .. }
            | UpdateEvent::BroadcastApplied { key, .. }
            | UpdateEvent::UpdateFailure { key, .. }
            | UpdateEvent::BroadcastDeliverySummary { key, .. } => *key,
        }
    }

    /// Returns the state hash if available (uses state_hash_after for success/applied events).
    fn state_hash(&self) -> Option<&str> {
        match self {
            UpdateEvent::UpdateSuccess {
                state_hash_after, ..
            }
            | UpdateEvent::BroadcastApplied {
                state_hash_after, ..
            } => state_hash_after.as_deref(),
            UpdateEvent::BroadcastEmitted { state_hash, .. }
            | UpdateEvent::BroadcastReceived { state_hash, .. } => state_hash.as_deref(),
            UpdateEvent::Request { .. }
            | UpdateEvent::UpdateFailure { .. }
            | UpdateEvent::BroadcastComplete { .. }
            | UpdateEvent::BroadcastDeliverySummary { .. } => None,
        }
    }

    /// Returns the state hash only for events representing stored state.
    ///
    /// Only returns hash for:
    /// - UpdateSuccess: State was updated locally
    /// - BroadcastApplied: State was stored after applying received broadcast
    ///
    /// Does NOT return hash for:
    /// - BroadcastReceived: Incoming state not yet applied
    /// - BroadcastEmitted: Outgoing state being sent
    fn stored_state_hash(&self) -> Option<&str> {
        match self {
            UpdateEvent::UpdateSuccess {
                state_hash_after, ..
            }
            | UpdateEvent::BroadcastApplied {
                state_hash_after, ..
            } => state_hash_after.as_deref(),
            UpdateEvent::Request { .. }
            | UpdateEvent::UpdateFailure { .. }
            | UpdateEvent::BroadcastEmitted { .. }
            | UpdateEvent::BroadcastComplete { .. }
            | UpdateEvent::BroadcastReceived { .. }
            | UpdateEvent::BroadcastDeliverySummary { .. } => None,
        }
    }
}

/// GET operation events for tracking the lifecycle of contract retrieval.
///
/// Similar to `PutEvent`, this enum captures the full sequence of a Get operation:
/// - Request initiation
/// - Success when contract is found
/// - NotFound when contract doesn't exist after search
/// - Failure when operation fails due to network/system errors
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum GetEvent {
    /// A Get request was initiated or received.
    Request {
        id: Transaction,
        /// The peer initiating or receiving the request.
        requester: PeerKeyLocation,
        /// Contract instance being requested (full key may not be known yet).
        instance_id: ContractInstanceId,
        /// Target peer (with hop-by-hop routing, this is the current node).
        target: PeerKeyLocation,
        /// Hops remaining before giving up.
        htl: usize,
        timestamp: u64,
    },
    /// Contract was successfully retrieved.
    GetSuccess {
        id: Transaction,
        requester: PeerKeyLocation,
        target: PeerKeyLocation,
        /// Full contract key (only available after successful retrieval).
        key: ContractKey,
        /// Number of hops the request traversed.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
        /// Short hash of the retrieved state (first 4 bytes of Blake3, 8 hex chars).
        state_hash: Option<String>,
    },
    /// Contract was not found after exhaustive search.
    GetNotFound {
        id: Transaction,
        requester: PeerKeyLocation,
        /// Contract instance that was searched for.
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        /// Number of hops the request traversed before exhaustion.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Get operation failed due to network or system error.
    GetFailure {
        id: Transaction,
        requester: PeerKeyLocation,
        /// Contract instance that was searched for.
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        /// Number of hops the request traversed before failure.
        hop_count: Option<usize>,
        /// Reason for the failure.
        reason: OperationFailure,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Get response being sent back to the requester.
    ///
    /// Tracks when this peer sends a get response (success or not found) to an upstream peer.
    /// This provides sender-side visibility for debugging message routing and understanding
    /// how search results propagate back through the network.
    ResponseSent {
        id: Transaction,
        /// The peer sending the response.
        from: PeerKeyLocation,
        /// The peer receiving the response.
        to: PeerKeyLocation,
        key: Option<ContractKey>,
        timestamp: u64,
    },
    /// A relay peer sent a ForwardingAck to its upstream peer.
    ///
    /// Emitted when a relay peer forwards a GET request deeper and ACKs the upstream
    /// to signal "I'm working on it". This prevents the upstream's GC task from treating
    /// the relay as dead — but also disables speculative retry (#3570).
    ForwardingAckSent {
        id: Transaction,
        /// The relay peer sending the ACK.
        from: PeerKeyLocation,
        /// The upstream peer receiving the ACK.
        to: PeerKeyLocation,
        instance_id: ContractInstanceId,
        timestamp: u64,
    },
    /// An upstream peer received a ForwardingAck from a downstream relay.
    ///
    /// When received, `ack_received` is set to `true`, which prevents the GC task
    /// from launching speculative retries. If the downstream chain then stalls
    /// (downstream peer never formally disconnects), the originator waits the full
    /// OPERATION_TTL with no recovery (#3570). Formal disconnect of the immediate
    /// downstream peer now wakes the parked driver sub-ms via
    /// `handle_orphaned_transactions` (#4154); the no-recovery window remains for
    /// silent stalls / slow-loris where no disconnect signal arrives.
    ForwardingAckReceived {
        id: Transaction,
        /// The peer that received the ACK (originator or intermediate relay).
        receiver: PeerKeyLocation,
        instance_id: ContractInstanceId,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
}

impl GetEvent {
    /// Returns the contract key for this event if available.
    /// Only GetSuccess and ResponseSent may have the full key; other variants have instance_id.
    fn contract_key(&self) -> Option<ContractKey> {
        match self {
            GetEvent::GetSuccess { key, .. } => Some(*key),
            GetEvent::ResponseSent { key, .. } => *key,
            GetEvent::Request { .. }
            | GetEvent::GetNotFound { .. }
            | GetEvent::GetFailure { .. }
            | GetEvent::ForwardingAckSent { .. }
            | GetEvent::ForwardingAckReceived { .. } => None,
        }
    }
}

/// SUBSCRIBE operation events for tracking the lifecycle of contract subscriptions.
///
/// Similar to `GetEvent` and `PutEvent`, this enum captures the full sequence of a Subscribe operation:
/// - Request initiation
/// - Success when subscription is established
/// - NotFound when contract doesn't exist after search
/// - Hosting state changes (local client subscriptions)
///
/// # Serialization Compatibility
///
/// Discriminants 6-10 are reserved for removed variants (2026-01 lease-based refactor).
/// New variants should be added after `_Reserved10` (discriminant 11+).
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) enum SubscribeEvent {
    /// A Subscribe request was initiated or received.
    Request {
        id: Transaction,
        /// The peer initiating or receiving the request.
        requester: PeerKeyLocation,
        /// Contract instance being subscribed to (full key may not be known yet).
        instance_id: ContractInstanceId,
        /// Target peer (with hop-by-hop routing, this is the current node).
        target: PeerKeyLocation,
        /// Hops remaining before giving up.
        htl: usize,
        timestamp: u64,
    },
    /// Subscription was successfully established.
    SubscribeSuccess {
        id: Transaction,
        /// Full contract key (only available after successful subscription).
        key: ContractKey,
        /// Location where subscription was established.
        at: PeerKeyLocation,
        /// Number of hops the request traversed.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
        requester: PeerKeyLocation,
    },
    /// Contract was not found after exhaustive search.
    SubscribeNotFound {
        id: Transaction,
        requester: PeerKeyLocation,
        /// Contract instance that was searched for.
        instance_id: ContractInstanceId,
        target: PeerKeyLocation,
        /// Number of hops the request traversed before exhaustion.
        hop_count: Option<usize>,
        /// Time elapsed since operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
    /// Subscribe response being sent back to the requester.
    ///
    /// Tracks when this peer sends a subscribe response (success or not found) to an upstream peer.
    /// This provides sender-side visibility for debugging subscription propagation.
    ResponseSent {
        id: Transaction,
        /// The peer sending the response.
        from: PeerKeyLocation,
        /// The peer receiving the response.
        to: PeerKeyLocation,
        key: Option<ContractKey>,
        timestamp: u64,
    },
    /// A local client started hosting a contract (via WebSocket subscription).
    ///
    /// This event fires when a local application subscribes to a contract,
    /// indicating this peer is now interested in receiving updates for the contract.
    HostingStarted {
        /// Contract instance being hosted.
        instance_id: ContractInstanceId,
        timestamp: u64,
    },
    /// A local client stopped hosting a contract (last WebSocket client unsubscribed).
    ///
    /// This event fires when the last local client unsubscribes from a contract,
    /// indicating this peer no longer has local interest in the contract.
    HostingStopped {
        /// Contract instance that is no longer being hosted locally.
        instance_id: ContractInstanceId,
        /// Reason for stopping hosting.
        reason: HostingStoppedReason,
        timestamp: u64,
    },
    // Reserved discriminants 6-10 for removed variants (2026-01 lease-based refactor).
    // These placeholders ensure old stored events with these discriminants fail
    // deserialization cleanly rather than being misinterpreted as new variants.
    // New variants should be added after _Reserved10.
    #[doc(hidden)]
    _Reserved6,
    #[doc(hidden)]
    _Reserved7,
    #[doc(hidden)]
    _Reserved8,
    #[doc(hidden)]
    _Reserved9,
    #[doc(hidden)]
    _Reserved10,
    /// An explicit Unsubscribe message was sent upstream for fast cleanup.
    UnsubscribeSent {
        id: Transaction,
        /// Contract instance being unsubscribed from.
        instance_id: ContractInstanceId,
        /// The peer sending the unsubscribe.
        from: PeerKeyLocation,
        /// The upstream peer receiving the unsubscribe.
        to: PeerKeyLocation,
        timestamp: u64,
    },
    /// An explicit Unsubscribe message was received from a downstream peer.
    UnsubscribeReceived {
        id: Transaction,
        /// Contract instance being unsubscribed from.
        instance_id: ContractInstanceId,
        /// The downstream peer that sent the unsubscribe.
        from: PeerKeyLocation,
        /// This peer (the upstream that received it).
        at: PeerKeyLocation,
        timestamp: u64,
    },
    /// A client-initiated Subscribe operation gave up without a terminal
    /// reply (every candidate peer timed out / errored before any of them
    /// returned Subscribed or NotFound). This is a terminal outcome, like
    /// `SubscribeSuccess`/`SubscribeNotFound`, but distinct because the
    /// originator never heard back from the network at all.
    ///
    /// Issue #3445: without this event a timed-out subscribe left a
    /// `subscribe_request` on the dashboard with no paired outcome, making
    /// the failure invisible (the River container contract showed 196
    /// requests and 0 outcomes — all silent timeouts).
    SubscribeTimeout {
        id: Transaction,
        /// The peer that initiated the subscribe (this node).
        requester: PeerKeyLocation,
        /// Contract instance that was being subscribed to (the full key is
        /// not known on the originator until a successful Subscribed reply).
        instance_id: ContractInstanceId,
        /// Number of routing rounds attempted before giving up.
        retries: usize,
        /// Time elapsed since the operation started (milliseconds).
        elapsed_ms: u64,
        timestamp: u64,
    },
}

impl SubscribeEvent {
    /// Returns the contract key for this event if available.
    /// Only some variants have the full key; Request and some others have instance_id.
    fn contract_key(&self) -> Option<ContractKey> {
        match self {
            SubscribeEvent::SubscribeSuccess { key, .. } => Some(*key),
            SubscribeEvent::ResponseSent { key, .. } => *key,
            SubscribeEvent::Request { .. }
            | SubscribeEvent::SubscribeNotFound { .. }
            | SubscribeEvent::HostingStarted { .. }
            | SubscribeEvent::HostingStopped { .. }
            | SubscribeEvent::_Reserved6
            | SubscribeEvent::_Reserved7
            | SubscribeEvent::_Reserved8
            | SubscribeEvent::_Reserved9
            | SubscribeEvent::_Reserved10
            | SubscribeEvent::UnsubscribeSent { .. }
            | SubscribeEvent::UnsubscribeReceived { .. }
            | SubscribeEvent::SubscribeTimeout { .. } => None,
        }
    }
}

/// Reason why local hosting stopped for a contract.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum HostingStoppedReason {
    /// Last local client unsubscribed.
    LastClientUnsubscribed,
    /// Client disconnected.
    ClientDisconnected,
}

/// Direction of data transfer.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum TransferDirection {
    /// Sending data to a peer.
    Send,
    /// Receiving data from a peer.
    Receive,
}

/// Data transfer events for tracking stream-level transfers.
///
/// These events track the start and completion of data transfers between peers,
/// including LEDBAT congestion control metrics. This enables:
/// - Monitoring transfer throughput and latency
/// - Debugging LEDBAT behavior (slowdowns, cwnd evolution)
/// - Identifying bottlenecks in data propagation
///
/// Note: Individual packets are NOT reported to avoid flooding the telemetry system.
/// Only transfer-level events (start/complete/failed) are emitted.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum TransferEvent {
    /// A data stream transfer has started.
    Started {
        /// Unique identifier for this stream.
        stream_id: u64,
        /// The remote peer's socket address.
        /// Note: Uses SocketAddr rather than PeerKeyLocation because the transport
        /// layer doesn't always have access to the peer's public key (especially
        /// for inbound connections at gateways before identity is established).
        peer_addr: std::net::SocketAddr,
        /// Total expected bytes to transfer.
        expected_bytes: u64,
        /// Whether we're sending or receiving.
        direction: TransferDirection,
        /// Transaction ID associated with this transfer (if available).
        /// May be None when the transport layer doesn't have transaction context.
        tx_id: Option<Transaction>,
        timestamp: u64,
    },
    /// A data stream transfer completed successfully.
    Completed {
        /// Unique identifier for this stream.
        stream_id: u64,
        /// The remote peer's socket address.
        peer_addr: std::net::SocketAddr,
        /// Actual bytes transferred.
        bytes_transferred: u64,
        /// Time elapsed from start to completion (milliseconds).
        elapsed_ms: u64,
        /// Average throughput (bytes per second).
        avg_throughput_bps: u64,
        /// Peak congestion window during transfer (bytes).
        peak_cwnd_bytes: Option<u32>,
        /// Final congestion window at completion (bytes).
        final_cwnd_bytes: Option<u32>,
        /// Number of LEDBAT slowdowns triggered during transfer.
        /// A high count indicates congestion or competing flows.
        slowdowns_triggered: Option<u32>,
        /// Final smoothed RTT at completion (milliseconds).
        final_srtt_ms: Option<u32>,
        /// Final slow start threshold at completion (bytes).
        /// Key diagnostic for death spiral: if ssthresh collapses to min_cwnd,
        /// slow start can't recover useful throughput.
        final_ssthresh_bytes: Option<u32>,
        /// Effective minimum ssthresh floor (bytes).
        /// This floor prevents ssthresh from collapsing too low during timeouts.
        min_ssthresh_floor_bytes: Option<u32>,
        /// Total retransmission timeouts (RTO events) during transfer.
        /// High values indicate severe congestion or path issues.
        total_timeouts: Option<u32>,
        /// Whether we were sending or receiving.
        direction: TransferDirection,
        timestamp: u64,
    },
    /// A data stream transfer failed.
    Failed {
        /// Unique identifier for this stream.
        stream_id: u64,
        /// The remote peer's socket address.
        peer_addr: std::net::SocketAddr,
        /// Bytes transferred before failure.
        bytes_transferred: u64,
        /// Reason for failure.
        reason: String,
        /// Time elapsed before failure (milliseconds).
        elapsed_ms: u64,
        /// Whether we were sending or receiving.
        direction: TransferDirection,
        timestamp: u64,
    },
}

/// Peer lifecycle events for tracking node startup and shutdown.
///
/// These events help with:
/// - Monitoring fleet health (which peers are online/offline)
/// - Understanding version distribution across the network
/// - Debugging issues specific to certain OS/architecture combinations
/// - Tracking graceful vs ungraceful shutdowns
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum PeerLifecycleEvent {
    /// Peer has started and is ready to participate in the network.
    Startup {
        /// Freenet core version (from Cargo.toml).
        version: String,
        /// Git commit hash (if available).
        git_commit: Option<String>,
        /// Whether the build has uncommitted changes.
        git_dirty: Option<bool>,
        /// Target architecture (e.g., "x86_64", "aarch64").
        arch: String,
        /// Operating system (e.g., "linux", "macos", "windows").
        os: String,
        /// OS version/release (e.g., "Ubuntu 22.04", "macOS 14.0").
        os_version: Option<String>,
        /// Whether this peer is configured as a gateway.
        is_gateway: bool,
        /// Timestamp when the peer started.
        timestamp: u64,
    },
    /// Peer is shutting down.
    Shutdown {
        /// Whether this is a graceful shutdown (true) or unexpected (false).
        graceful: bool,
        /// Reason for shutdown if available.
        reason: Option<String>,
        /// Total uptime in seconds.
        uptime_secs: u64,
        /// Total connections made during uptime.
        total_connections: u64,
        /// Timestamp when shutdown was initiated.
        timestamp: u64,
    },
}

/// Interest sync events for delta-based state synchronization.
///
/// These events track the interest sync protocol operations, particularly
/// ResyncRequests which indicate delta application failures. High ResyncRequest
/// counts may indicate incorrect summary caching (see PR #2763).
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum InterestSyncEvent {
    /// A ResyncRequest was received from a peer.
    ///
    /// This indicates the peer failed to apply a delta we sent them,
    /// likely due to version mismatch. We respond with full state.
    ResyncRequestReceived {
        /// Contract for which resync was requested.
        key: ContractKey,
        /// The peer that sent the ResyncRequest.
        from_peer: PeerKeyLocation,
        /// Timestamp of the event.
        timestamp: u64,
    },
    /// A ResyncResponse (full state) was sent to a peer.
    ///
    /// This is the response to a ResyncRequest, providing the peer
    /// with our full state so they can recover from the delta failure.
    ResyncResponseSent {
        /// Contract for which resync response was sent.
        key: ContractKey,
        /// The peer we sent the response to.
        to_peer: PeerKeyLocation,
        /// Size of the state sent (bytes).
        state_size: usize,
        /// Timestamp of the event.
        timestamp: u64,
    },
    /// Periodic confirmation of a contract's current state hash.
    ///
    /// Emitted by the Summaries handler during interest-sync heartbeat
    /// processing. This ensures the convergence checker has up-to-date
    /// state hashes even when state changes occur through code paths
    /// that don't emit PutSuccess/UpdateSuccess/BroadcastApplied events
    /// (e.g., CRDT merge with version-based comparison).
    StateConfirmed {
        /// Contract whose state was confirmed.
        key: ContractKey,
        /// Blake3 hash of the current state (hex string).
        state_hash: String,
    },
}
