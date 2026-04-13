use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use either::Either;

pub(crate) use self::messages::{SubscribeMsg, SubscribeMsgResult};
use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult, get};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::node::IsOperationCompleted;
use crate::ring::PeerKeyLocation;
use crate::tracing::NetEventLog;
use crate::{
    client_events::HostResult,
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager},
    ring::{KnownPeerKeyLocation, Location, RingError},
};
use freenet_stdlib::{
    client_api::{ContractResponse, ErrorKind, HostResponse},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, sleep};

/// Maximum peers to try per hop (breadth search).
/// Matches GET operation's DEFAULT_MAX_BREADTH; change both together.
const MAX_BREADTH: usize = 3;

/// Maximum retry rounds (each round queries k_closest for new candidates).
/// Matches GET operation's MAX_RETRIES; change both together.
const MAX_RETRIES: usize = 10;

/// Minimum HTL for speculative retries.
///
/// Retries use a reduced HTL (capped at current_hop) to avoid full-depth
/// traversal storms. This floor ensures retries still reach peers 2-3 hops
/// away, which is the minimum useful search depth in any topology.
/// Matches GET operation's MIN_RETRY_HTL; change both together.
const MIN_RETRY_HTL: usize = 3;

/// Timeout for waiting on contract storage notification.
/// Used when a subscription arrives before the contract has been propagated via PUT.
const CONTRACT_WAIT_TIMEOUT_MS: u64 = 2_000;

/// Wait for a contract to become available, using channel-based notification.
///
/// This handles the race condition where a subscription arrives before the contract
/// has been propagated via PUT. The flow is:
/// 1. Fast path: check if contract already exists
/// 2. Register notification waiter
/// 3. Check again (handles race between step 1 and 2)
/// 4. Wait for notification or timeout
/// 5. Final verification of actual state
async fn wait_for_contract_with_timeout(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
    timeout_ms: u64,
) -> Result<Option<ContractKey>, OpError> {
    // Fast path - contract already exists
    if let Some(key) = super::has_contract(op_manager, instance_id).await? {
        return Ok(Some(key));
    }

    // Register waiter BEFORE second check to avoid race condition
    let notifier = op_manager.wait_for_contract(instance_id);

    // Check again - contract may have arrived between first check and registration
    if let Some(key) = super::has_contract(op_manager, instance_id).await? {
        return Ok(Some(key));
    }

    // Wait for notification or timeout (we don't care which triggers first)
    crate::deterministic_select! {
        _ = notifier => {},
        _ = sleep(Duration::from_millis(timeout_ms)) => {},
    }

    // Always verify actual state - don't trust notification alone
    super::has_contract(op_manager, instance_id).await
}

async fn fetch_contract_if_missing(
    op_manager: &OpManager,
    instance_id: ContractInstanceId,
) -> Result<Option<ContractKey>, OpError> {
    if let Some(key) = super::has_contract(op_manager, instance_id).await? {
        return Ok(Some(key));
    }

    // Start a GET operation to fetch the contract
    let get_op = get::start_op(instance_id, true, false, false);
    let visited = super::VisitedPeers::new(&get_op.id);
    get::request_get(op_manager, get_op, visited).await?;

    // Wait for contract to arrive
    wait_for_contract_with_timeout(op_manager, instance_id, CONTRACT_WAIT_TIMEOUT_MS).await
}

// ── Type-state data structs ──────────────────────────────────────────────
//
// Each state in the subscribe state machine is a named struct with typed
// transition methods.  This gives compile-time guarantees:
//
//   PrepareRequest ──┬── into_awaiting_response() ──► AwaitingResponse
//                    └── into_completed()          ──► Completed
//   AwaitingResponse ── into_completed()           ──► Completed
//
// Invalid transitions (e.g. Completed → PrepareRequest) are unrepresentable
// because the target type simply has no such method.

/// Data for the PrepareRequest state: operation is being prepared for network dispatch.
#[derive(Debug)]
struct PrepareRequestData {
    id: Transaction,
    instance_id: ContractInstanceId,
    is_renewal: bool,
}

impl PrepareRequestData {
    /// Transition to AwaitingResponse after sending the subscribe request.
    ///
    /// Encodes valid transition: PrepareRequest → AwaitingResponse.
    /// The instance_id is carried forward automatically.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern in AGENTS.md
    fn into_awaiting_response(
        self,
        next_hop: Option<std::net::SocketAddr>,
    ) -> AwaitingResponseData {
        AwaitingResponseData {
            next_hop,
            instance_id: self.instance_id,
            retries: 0,
            current_hop: 0,
            tried_peers: HashSet::new(),
            alternatives: Vec::new(),
            attempts_at_hop: 0,
            visited: super::VisitedPeers::new(&self.id),
        }
    }

    /// Transition directly to Completed when the contract is available locally
    /// and no network forwarding is needed.
    ///
    /// Encodes valid transition: PrepareRequest → Completed (local-only path).
    #[allow(dead_code)] // Documents valid transition; see type-state pattern in AGENTS.md
    fn into_completed(self, key: ContractKey) -> CompletedData {
        CompletedData { key }
    }
}

/// Data for the AwaitingResponse state: request dispatched, waiting for peer response.
#[derive(Debug)]
struct AwaitingResponseData {
    /// The target we're sending to (for hop-by-hop routing)
    next_hop: Option<std::net::SocketAddr>,
    /// The contract being subscribed to (needed for error notification on abort)
    instance_id: ContractInstanceId,
    /// Retry round counter (each round queries k_closest for new candidates)
    retries: usize,
    /// Current HTL value for the request
    current_hop: usize,
    /// Peers already tried at this hop level
    tried_peers: HashSet<std::net::SocketAddr>,
    /// Alternative peers from the same k_closest call, not yet tried
    alternatives: Vec<PeerKeyLocation>,
    /// How many of the breadth candidates have been tried at this hop
    attempts_at_hop: usize,
    /// Bloom filter tracking visited peers across all hops
    visited: super::VisitedPeers,
}

impl AwaitingResponseData {
    /// Transition to Completed on successful subscription response.
    ///
    /// Encodes valid transition: AwaitingResponse → Completed.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern in AGENTS.md
    fn into_completed(self, key: ContractKey) -> CompletedData {
        CompletedData { key }
    }
}

/// Data for the Completed state: subscription was successfully established.
#[derive(Debug, Clone, Copy)]
struct CompletedData {
    key: ContractKey,
}

// ── State enum (wraps the typed structs) ─────────────────────────────────

#[derive(Debug)]
enum SubscribeState {
    /// Prepare the request to subscribe.
    PrepareRequest(PrepareRequestData),
    /// Awaiting response from downstream peer.
    AwaitingResponse(AwaitingResponseData),
    /// Subscription completed successfully.
    Completed(CompletedData),
    /// The operation failed — contract not found and not available locally.
    Failed,
}

pub(crate) struct SubscribeResult {}

impl TryFrom<SubscribeOp> for SubscribeResult {
    type Error = OpError;

    fn try_from(value: SubscribeOp) -> Result<Self, Self::Error> {
        if let SubscribeState::Completed(_) = value.state {
            Ok(SubscribeResult {})
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// Start a new subscription operation.
///
/// `is_renewal` indicates whether this is a renewal (requester already has the contract).
/// If true, the responder will skip sending state to save bandwidth.
pub(crate) fn start_op(instance_id: ContractInstanceId, is_renewal: bool) -> SubscribeOp {
    let id = Transaction::new::<SubscribeMsg>();
    SubscribeOp {
        id,
        state: SubscribeState::PrepareRequest(PrepareRequestData {
            id,
            instance_id,
            is_renewal,
        }),
        requester_addr: None, // Local operation, we are the originator
        requester_pub_key: None,
        is_renewal,
        stats: None,
        ack_received: false,
        speculative_paths: 0,
    }
}

/// Create a Subscribe operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(
    instance_id: ContractInstanceId,
    id: Transaction,
    is_renewal: bool,
) -> SubscribeOp {
    SubscribeOp {
        id,
        state: SubscribeState::PrepareRequest(PrepareRequestData {
            id,
            instance_id,
            is_renewal,
        }),
        requester_addr: None, // Local operation, we are the originator
        requester_pub_key: None,
        is_renewal,
        stats: None,
        ack_received: false,
        speculative_paths: 0,
    }
}

/// Create a SubscribeOp for routing an Unsubscribe message to a target peer.
///
/// The operation is created in `AwaitingResponse` state so `peek_next_hop_addr`
/// resolves the target address for the event loop's message router.
/// The caller should mark it completed immediately after sending.
pub(crate) fn create_unsubscribe_op(
    instance_id: ContractInstanceId,
    tx: Transaction,
    target_addr: std::net::SocketAddr,
) -> SubscribeOp {
    SubscribeOp {
        id: tx,
        state: SubscribeState::AwaitingResponse(AwaitingResponseData {
            next_hop: Some(target_addr),
            instance_id,
            retries: 0,
            current_hop: 0,
            tried_peers: HashSet::new(),
            alternatives: Vec::new(),
            attempts_at_hop: 0,
            visited: super::VisitedPeers::new(&tx),
        }),
        requester_addr: None,
        requester_pub_key: None,
        is_renewal: false,
        stats: None,
        ack_received: false,
        speculative_paths: 0,
    }
}

/// Request to subscribe to value changes from a contract.
///
/// # Errors
///
/// Returns `RingError::PeerNotJoined` if the peer hasn't established its ring location
/// (i.e., hasn't completed joining the network) AND the contract is not available locally.
/// This allows callers to retry after the peer has completed joining.
///
/// If the contract exists locally and no network is needed, the subscription completes
/// locally even without a ring location (standalone node case).
pub(crate) async fn request_subscribe(
    op_manager: &OpManager,
    sub_op: SubscribeOp,
) -> Result<(), OpError> {
    let SubscribeState::PrepareRequest(ref data) = sub_op.state else {
        return Err(OpError::UnexpectedOpState);
    };
    let id = data.id;
    let instance_id = data.instance_id;
    let is_renewal = data.is_renewal;

    tracing::debug!(tx = %id, contract = %instance_id, is_renewal, "subscribe: request_subscribe invoked");

    match prepare_initial_request(op_manager, id, instance_id, is_renewal).await? {
        InitialRequest::LocallyComplete { key } => {
            complete_local_subscription(op_manager, id, key, is_renewal).await
        }
        InitialRequest::NoHostingPeers => Err(RingError::NoHostingPeers(instance_id).into()),
        InitialRequest::PeerNotJoined => Err(RingError::PeerNotJoined.into()),
        InitialRequest::NetworkRequest {
            target,
            target_addr,
            visited,
            alternatives,
            htl,
        } => {
            let msg = SubscribeMsg::Request {
                id,
                instance_id,
                htl,
                visited: visited.clone(),
                is_renewal,
            };

            let mut tried_peers = HashSet::new();
            tried_peers.insert(target_addr);

            let op = SubscribeOp {
                id,
                state: SubscribeState::AwaitingResponse(AwaitingResponseData {
                    next_hop: Some(target_addr),
                    instance_id,
                    retries: 0,
                    current_hop: htl,
                    tried_peers,
                    alternatives, // remaining candidates after remove(0)
                    attempts_at_hop: 1,
                    visited,
                }),
                requester_addr: None, // We're the originator
                requester_pub_key: None,
                is_renewal,
                stats: Some(SubscribeStats {
                    target_peer: target,
                    contract_location: Location::from(&instance_id),
                    request_sent_at: Instant::now(),
                }),
                ack_received: false,
                speculative_paths: 0,
            };

            // Renewals use non-blocking send to fail fast under congestion rather
            // than blocking 30 s and compounding it. Client subscribes use the
            // blocking path for a definitive success/failure response.
            if is_renewal {
                op_manager
                    .notify_op_change_nonblocking(NetMessage::from(msg), OpEnum::Subscribe(op))
                    .await?;
            } else {
                op_manager
                    .notify_op_change(NetMessage::from(msg), OpEnum::Subscribe(op))
                    .await?;
            }

            Ok(())
        }
    }
}

/// Outcome of [`prepare_initial_request`]: the decision about how to originate
/// a subscribe request based on the node's current ring state and contract
/// availability.
///
/// This type exists so both the legacy state-machine path (`request_subscribe`)
/// and the task-per-tx path (`op_ctx_task::run_client_subscribe`, #1454 Phase
/// 2b) can share the "which peer, or local-complete, or give up?" decision
/// logic without duplicating `k_closest_potentially_hosting` + fallback +
/// local-completion handling.
///
/// The returned values describe what the caller should do; the helper does
/// NOT mutate `op_manager` or push state. Any side-effects (emitting
/// telemetry via `NetEventLog::subscribe_request`, calling
/// `complete_local_subscription`, pushing state, sending the wire message)
/// are the caller's responsibility.
#[derive(Debug)]
pub(super) enum InitialRequest {
    /// Contract is available locally and no network round-trip is required.
    /// Caller should call [`complete_local_subscription`] with the original
    /// transaction id and propagate its result.
    LocallyComplete { key: ContractKey },
    /// No remote peers could be found and the contract is not available
    /// locally. Caller should return `RingError::NoHostingPeers`.
    NoHostingPeers,
    /// Peer has not joined the ring yet (no own location) and the contract is
    /// not available locally. Caller should return `RingError::PeerNotJoined`.
    PeerNotJoined,
    /// A target peer was selected and the caller should send a
    /// `SubscribeMsg::Request` to it. `visited` and `alternatives` carry the
    /// routing state the caller must thread into its retry logic.
    NetworkRequest {
        /// The chosen target peer (already removed from `alternatives`).
        target: PeerKeyLocation,
        /// Socket address of `target`; pre-resolved for convenience.
        target_addr: std::net::SocketAddr,
        /// Bloom filter of visited peers, already including `own_addr` and
        /// `target_addr`. Callers should clone this into the outgoing
        /// `SubscribeMsg::Request.visited`.
        visited: super::VisitedPeers,
        /// Remaining k_closest candidates not yet tried at this hop.
        alternatives: Vec<PeerKeyLocation>,
        /// Initial HTL for the request (= `op_manager.ring.max_hops_to_live`).
        htl: usize,
    },
}

/// Compute the initial "where do we send this subscribe, or do we complete
/// locally?" decision for a subscribe request.
///
/// Factored out of [`request_subscribe`] so the task-per-tx client-initiated
/// path (`op_ctx_task::run_client_subscribe`, #1454 Phase 2b) can reuse the
/// exact same ring lookup / fallback / local-completion logic without
/// duplicating it. The helper is pure modulo telemetry emission: it calls
/// `NetEventLog::subscribe_request` on the `NetworkRequest` branch so both
/// callers get identical event logs, but it does not mutate `op_manager`
/// state and does not push any `SubscribeOp` into the per-op DashMap.
///
/// The decision branches exactly mirror the pre-extraction body of
/// `request_subscribe`:
///
/// 1. If the peer has no ring location (hasn't joined), check local contract
///    availability and either complete locally or return `PeerNotJoined`.
/// 2. Query `k_closest_potentially_hosting` with `own_addr` already visited.
///    If it returns candidates, take the first as the target.
/// 3. If `k_closest` returned empty, fall back to any connected peer that
///    hasn't been visited (sorted by location for determinism).
/// 4. If no fallback is available either, check local contract availability
///    one more time (standalone node / sole holder case) and either complete
///    locally or return `NoHostingPeers`.
pub(super) async fn prepare_initial_request(
    op_manager: &OpManager,
    id: Transaction,
    instance_id: ContractInstanceId,
    is_renewal: bool,
) -> Result<InitialRequest, OpError> {
    let own_addr = match op_manager.ring.connection_manager.peer_addr() {
        Ok(addr) => addr,
        Err(_) => {
            // Peer hasn't joined the network yet - check if contract is available locally
            if let Some(key) = super::has_contract(op_manager, instance_id).await? {
                tracing::info!(
                    tx = %id,
                    contract = %key,
                    phase = "local_complete",
                    "Peer not joined, but contract available locally - completing subscription locally"
                );
                return Ok(InitialRequest::LocallyComplete { key });
            }
            tracing::warn!(
                tx = %id,
                contract = %instance_id,
                phase = "peer_not_joined",
                "Cannot subscribe: peer has not joined network yet and contract not available locally"
            );
            return Ok(InitialRequest::PeerNotJoined);
        }
    };

    // IMPORTANT: Even if we have the contract locally (e.g., from PUT forwarding),
    // we MUST send a Subscribe::Request to the network to register ourselves as a
    // downstream subscriber in the subscription tree. Otherwise, when the contract
    // is updated at the source, we won't receive the update because we're not
    // registered in the upstream peer's subscriber list.
    //
    // The local subscription completion happens when we receive the Response back.
    // This ensures proper subscription tree management for update propagation.

    // Find a peer to forward the request to (needed even if we have contract locally)
    let mut visited = super::VisitedPeers::new(&id);
    visited.mark_visited(own_addr);

    let mut candidates =
        op_manager
            .ring
            .k_closest_potentially_hosting(&instance_id, &visited, MAX_BREADTH);

    // First try the best candidates from k_closest_potentially_hosting.
    // If that returns empty, fall back to any available connection.
    // This ensures we join the subscription tree even when the routing algorithm
    // can't find ideal candidates (e.g., due to timing or location filtering).
    let target = if !candidates.is_empty() {
        candidates.remove(0)
    } else {
        // k_closest_potentially_hosting returned empty - try any connected peer as fallback.
        // The subscription will be forwarded toward the contract location.
        let connections = op_manager
            .ring
            .connection_manager
            .get_connections_by_location();
        // Sort keys for deterministic iteration order (HashMap iteration is non-deterministic)
        let mut sorted_keys: Vec<_> = connections.keys().collect();
        sorted_keys.sort();
        let fallback_target = sorted_keys
            .into_iter()
            .filter_map(|loc| connections.get(loc))
            .flatten()
            .find(|conn| {
                conn.location
                    .socket_addr()
                    .map(|addr| !visited.probably_visited(addr))
                    .unwrap_or(false)
            })
            .map(|conn| conn.location.clone());

        match fallback_target {
            Some(target) => {
                tracing::debug!(
                    tx = %id,
                    contract = %instance_id,
                    target = ?target.socket_addr(),
                    phase = "fallback_routing",
                    "Using fallback connection for subscription (k_closest returned empty)"
                );
                target
            }
            None => {
                // Truly no connections available - fall back to local completion only if isolated.
                // This handles the case of a standalone node or when we're the only node with the contract.
                if let Some(key) = super::has_contract(op_manager, instance_id).await? {
                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        phase = "local_complete",
                        "Contract available locally and no network connections, completing subscription locally"
                    );
                    return Ok(InitialRequest::LocallyComplete { key });
                }
                tracing::warn!(tx = %id, contract = %instance_id, phase = "error", "No remote peers available for subscription");
                return Ok(InitialRequest::NoHostingPeers);
            }
        }
    };

    let target_addr = target
        .socket_addr()
        .expect("target must have socket address");
    visited.mark_visited(target_addr);

    tracing::debug!(
        tx = %id,
        contract = %instance_id,
        is_renewal,
        target_peer = %target_addr,
        "subscribe: forwarding Request to target peer"
    );

    // Emit telemetry for subscribe request initiation. Placed inside the
    // helper so both the legacy and task-per-tx paths produce identical
    // `NetEventLog::subscribe_request` events.
    if let Some(event) = NetEventLog::subscribe_request(
        &id,
        &op_manager.ring,
        instance_id,
        target.clone(),
        op_manager.ring.max_hops_to_live,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    Ok(InitialRequest::NetworkRequest {
        target,
        target_addr,
        visited,
        alternatives: candidates,
        htl: op_manager.ring.max_hops_to_live,
    })
}

/// Complete a **standalone** local subscription by notifying the client layer.
///
/// **IMPORTANT:** This function is ONLY used when no remote peers are available (standalone node).
/// For normal network subscriptions, the operation returns a `Completed` state and goes through
/// `handle_op_result`, which sends results via `result_router_tx` directly.
///
/// **Architecture Note (Issue #2075):**
/// Local client subscriptions are deliberately kept separate from network subscriptions:
/// - **Network subscriptions** are stored in `ring.hosting_manager.subscribers` and are used
///   for peer-to-peer UPDATE propagation between nodes
/// - **Local subscriptions** are managed by the contract executor via `update_notifications`
///   channels, which deliver `UpdateNotification` directly to WebSocket clients
async fn complete_local_subscription(
    op_manager: &OpManager,
    id: Transaction,
    key: ContractKey,
    is_renewal: bool,
) -> Result<(), OpError> {
    tracing::debug!(
        %key,
        tx = %id,
        is_renewal,
        "Local subscription completed - client will receive updates via executor notification channel"
    );

    // Register local interest so that ChangeInterests from peers get properly
    // processed. This enables bidirectional interest discovery: when peers
    // announce they seed this contract via ChangeInterests, our has_local_interest()
    // check will pass, and we'll register their peer interest, enabling direct
    // update broadcasts from them to us.
    if !is_renewal {
        let became_interested = op_manager.interest_manager.add_local_client(&key);
        if became_interested {
            super::broadcast_change_interests(op_manager, vec![key], vec![]).await;
        }
    }

    // Notify client layer that subscription is complete.
    // The actual update delivery happens through the executor's update_notifications
    // when contract state changes, not through network broadcast targets.
    op_manager
        .notify_node_event(crate::message::NodeEvent::LocalSubscribeComplete {
            tx: id,
            key,
            subscribed: true,
            is_renewal,
        })
        .await?;

    op_manager.completed(id);
    Ok(())
}

/// Routing stats for subscribe operations, used to report failures to the router.
struct SubscribeStats {
    target_peer: crate::ring::PeerKeyLocation,
    contract_location: Location,
    /// When the subscribe request was sent; used to compute response time.
    request_sent_at: Instant,
}

pub(crate) struct SubscribeOp {
    pub id: Transaction,
    state: SubscribeState,
    /// The address of the peer that requested this subscription.
    /// Used for routing responses back and registering them as downstream subscribers.
    requester_addr: Option<std::net::SocketAddr>,
    /// The public key of the requesting peer, resolved at op init time.
    /// Used for interest registration instead of addr-based lookup, which can fail
    /// during NAT traversal timing windows when the ring layer hasn't mapped the
    /// transport address yet. See #2886.
    requester_pub_key: Option<crate::transport::TransportPublicKey>,
    /// Whether this is a renewal (requester already has the contract).
    /// Preserved across request/response to avoid sending state to renewals.
    is_renewal: bool,
    /// Routing stats for failure reporting.
    stats: Option<SubscribeStats>,
    /// True when a downstream relay has acknowledged forwarding this request.
    /// Used by the GC task to distinguish "peer is dead" from "peer is working on it".
    pub(crate) ack_received: bool,
    /// Number of speculative parallel paths launched by the originator's GC task.
    /// Capped at MAX_SPECULATIVE_PATHS to bound network overhead.
    pub(crate) speculative_paths: u8,
}

impl SubscribeOp {
    /// Extract the contract instance_id from the current state, if available.
    pub(crate) fn instance_id(&self) -> Option<ContractInstanceId> {
        match &self.state {
            SubscribeState::PrepareRequest(data) => Some(data.instance_id),
            SubscribeState::AwaitingResponse(data) => Some(data.instance_id),
            SubscribeState::Completed(_) | SubscribeState::Failed => None,
        }
    }

    /// Extract the contract key from a completed subscribe operation.
    pub(crate) fn completed_key(&self) -> Option<ContractKey> {
        match &self.state {
            SubscribeState::Completed(data) => Some(data.key),
            SubscribeState::PrepareRequest(_)
            | SubscribeState::AwaitingResponse(_)
            | SubscribeState::Failed => None,
        }
    }

    /// Whether this is a subscription renewal (node-internal, no client waiting).
    pub(crate) fn is_renewal(&self) -> bool {
        self.is_renewal
    }

    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        if self.finalized() {
            if let Some(ref stats) = self.stats {
                let response_time = stats.request_sent_at.elapsed();
                return OpOutcome::ContractOpSuccess {
                    target_peer: &stats.target_peer,
                    contract_location: stats.contract_location,
                    first_response_time: response_time,
                    payload_size: 0,
                    payload_transfer_time: std::time::Duration::ZERO,
                };
            }
            return OpOutcome::Irrelevant;
        }
        // Not completed — if we have stats, report as failure
        if let Some(ref stats) = self.stats {
            OpOutcome::ContractOpFailure {
                target_peer: &stats.target_peer,
                contract_location: stats.contract_location,
            }
        } else {
            OpOutcome::Incomplete
        }
    }

    /// Extract routing failure info for timeout reporting.
    pub(crate) fn failure_routing_info(&self) -> Option<(crate::ring::PeerKeyLocation, Location)> {
        self.stats
            .as_ref()
            .map(|s| (s.target_peer.clone(), s.contract_location))
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, SubscribeState::Completed(_))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let SubscribeState::Completed(data) = &self.state {
            Ok(HostResponse::ContractResponse(
                ContractResponse::SubscribeResponse {
                    key: data.key,
                    subscribed: true,
                },
            ))
        } else {
            Err(ErrorKind::OperationError {
                cause: "subscribe didn't finish successfully".into(),
            }
            .into())
        }
    }

    /// Get the next hop address if this operation is in a state that needs to send
    /// an outbound message. Used for hop-by-hop routing.
    pub(crate) fn get_next_hop_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.state {
            SubscribeState::AwaitingResponse(data) => data.next_hop,
            SubscribeState::PrepareRequest(_)
            | SubscribeState::Completed(_)
            | SubscribeState::Failed => None,
        }
    }

    /// Whether this node is the originator of this subscribe operation.
    /// The originator has no `requester_addr` (no upstream peer to respond to).
    pub(crate) fn is_originator(&self) -> bool {
        self.requester_addr.is_none()
    }

    /// Retry the subscribe operation with the next alternative peer.
    ///
    /// Similar to GET's `retry_with_next_alternative`: picks the next untried peer
    /// from local alternatives, or injects fallback peers if alternatives are exhausted.
    /// Returns `Ok((op, msg))` with the updated op and new Request message,
    /// or `Err(op)` if no alternatives remain.
    pub(crate) fn retry_with_next_alternative(
        mut self,
        max_hops_to_live: usize,
        fallback_peers: &[PeerKeyLocation],
    ) -> Result<(SubscribeOp, SubscribeMsg), Box<SubscribeOp>> {
        match self.state {
            SubscribeState::AwaitingResponse(ref mut data) => {
                // If local alternatives exhausted, inject fallback peers we haven't tried.
                // Filter through both tried_peers and visited bloom filter (#3570).
                if data.alternatives.is_empty() && !fallback_peers.is_empty() {
                    for peer in fallback_peers {
                        if let Some(addr) = peer.socket_addr() {
                            if !data.tried_peers.contains(&addr)
                                && !data.visited.probably_visited(addr)
                            {
                                data.alternatives.push(peer.clone());
                            }
                        }
                    }
                    if !data.alternatives.is_empty() {
                        tracing::info!(
                            tx = %self.id,
                            contract = %data.instance_id,
                            new_candidates = data.alternatives.len(),
                            "Subscribe broadening search to all connected peers (fallback)"
                        );
                    }
                }

                if data.alternatives.is_empty() {
                    return Err(Box::new(self));
                }

                // Take ownership of state to modify it
                let SubscribeState::AwaitingResponse(mut data) =
                    std::mem::replace(&mut self.state, SubscribeState::Failed)
                else {
                    unreachable!();
                };

                let instance_id = data.instance_id;
                let is_renewal = self.is_renewal;

                // Find next alternative with a known socket address.
                // Skip peers without addresses — they can't be contacted.
                let (next_target, addr) = loop {
                    if data.alternatives.is_empty() {
                        // All remaining alternatives lack addresses
                        self.state = SubscribeState::AwaitingResponse(data);
                        return Err(Box::new(self));
                    }
                    let candidate = data.alternatives.remove(0);
                    if let Some(addr) = candidate.socket_addr() {
                        break (candidate, addr);
                    }
                };
                data.tried_peers.insert(addr);
                // Mark in bloom filter so future retries skip this peer (#3570).
                data.visited.mark_visited(addr);
                data.next_hop = Some(addr);
                data.attempts_at_hop += 1;
                let visited = data.visited.clone();

                tracing::info!(
                    tx = %self.id,
                    contract = %instance_id,
                    target = ?next_target.socket_addr(),
                    remaining_alternatives = data.alternatives.len(),
                    "Subscribe retrying with alternative peer after timeout"
                );

                // Update stats for the new target (reset timing for new attempt)
                self.stats = Some(SubscribeStats {
                    target_peer: next_target,
                    contract_location: Location::from(&instance_id),
                    request_sent_at: Instant::now(),
                });

                // Reduce HTL on each retry, floored at MIN_RETRY_HTL (#3570).
                let retry_htl = (max_hops_to_live / (data.attempts_at_hop.max(1)))
                    .max(MIN_RETRY_HTL)
                    .min(max_hops_to_live);

                self.state = SubscribeState::AwaitingResponse(data);

                let msg = SubscribeMsg::Request {
                    id: self.id,
                    instance_id,
                    htl: retry_htl,
                    visited,
                    is_renewal,
                };

                Ok((self, msg))
            }
            SubscribeState::PrepareRequest(_)
            | SubscribeState::Completed(_)
            | SubscribeState::Failed => Err(Box::new(self)),
        }
    }

    /// Build a NotFound result to send back to the requester, or fail locally if we're the originator.
    ///
    /// # Information disclosure note
    /// Before this fix, subscribe failures caused a 60s timeout which provided implicit
    /// rate limiting on network probing. Instant NotFound responses allow faster contract
    /// hosting enumeration. The risk is low (contract locations are already somewhat
    /// predictable from the DHT topology), but future hardening could add jitter or
    /// rate-limit NotFound responses if probing becomes a practical concern.
    fn not_found_result(
        id: Transaction,
        instance_id: ContractInstanceId,
        requester_addr: Option<std::net::SocketAddr>,
        reason: &str,
    ) -> Result<OperationResult, OpError> {
        if let Some(requester_addr) = requester_addr {
            // We're an intermediate node — send NotFound back to the upstream requester
            tracing::debug!(
                tx = %id,
                %instance_id,
                requester = %requester_addr,
                %reason,
                "Sending NotFound response to requester"
            );
            Ok(OperationResult::SendAndComplete {
                msg: NetMessage::from(SubscribeMsg::Response {
                    id,
                    instance_id,
                    result: SubscribeMsgResult::NotFound,
                }),
                next_hop: Some(requester_addr),
                stream_data: None,
            })
        } else {
            // We're the originator — fail the operation directly
            tracing::warn!(
                tx = %id,
                %instance_id,
                %reason,
                phase = "not_found",
                "Subscribe failed at originator"
            );
            Err(RingError::NoHostingPeers(instance_id).into())
        }
    }

    /// Handle aborted connections by retrying with alternative peers before failing.
    ///
    /// Follows the same breadth/retry pattern as GET: try alternatives at the same
    /// hop level first, then seek new candidates via k_closest.
    pub(crate) async fn handle_abort(self, op_manager: &OpManager) -> Result<(), OpError> {
        // Extract fields from self BEFORE destructuring self.state (which moves it).
        let tx_id = self.id;
        let requester_addr = self.requester_addr;
        let requester_pub_key = self.requester_pub_key;
        let is_renewal = self.is_renewal;
        let stats = self.stats;
        let is_sub_op = op_manager.is_sub_operation(tx_id);

        tracing::debug!(
            tx = %tx_id,
            ?requester_addr,
            "Subscribe operation aborted due to connection failure"
        );

        if let SubscribeState::AwaitingResponse(AwaitingResponseData {
            next_hop: failed_hop,
            instance_id,
            retries,
            current_hop,
            mut tried_peers,
            mut alternatives,
            attempts_at_hop,
            mut visited,
        }) = self.state
        {
            // Mark the failed peer as tried
            if let Some(addr) = failed_hop {
                tried_peers.insert(addr);
                visited.mark_visited(addr);
            }

            // Phase 1: Try the next alternative at same hop
            if !alternatives.is_empty() && attempts_at_hop < MAX_BREADTH {
                let next_target = alternatives.remove(0);
                if let Some(next_addr) = next_target.socket_addr() {
                    tried_peers.insert(next_addr);
                    visited.mark_visited(next_addr);

                    tracing::debug!(
                        tx = %tx_id,
                        %instance_id,
                        next_target = %next_addr,
                        "Subscribe: connection aborted, trying alternative peer"
                    );

                    let msg = SubscribeMsg::Request {
                        id: tx_id,
                        instance_id,
                        htl: current_hop,
                        visited: visited.clone(),
                        is_renewal,
                    };

                    let op = SubscribeOp {
                        id: tx_id,
                        state: SubscribeState::AwaitingResponse(AwaitingResponseData {
                            next_hop: Some(next_addr),
                            instance_id,
                            retries,
                            current_hop,
                            tried_peers,
                            alternatives,
                            attempts_at_hop: attempts_at_hop + 1,
                            visited,
                        }),
                        requester_addr,
                        requester_pub_key,
                        is_renewal,
                        stats: stats.map(|mut s| {
                            s.target_peer = next_target.clone();
                            s
                        }),
                        ack_received: false,
                        speculative_paths: 0,
                    };

                    op_manager
                        .notify_op_change(NetMessage::from(msg), OpEnum::Subscribe(op))
                        .await?;
                    return Err(OpError::StatePushed);
                }
            }

            // Phase 2: Seek new candidates via k_closest
            if retries < MAX_RETRIES {
                for addr in &tried_peers {
                    visited.mark_visited(*addr);
                }

                let mut new_candidates = op_manager.ring.k_closest_potentially_hosting(
                    &instance_id,
                    &visited,
                    MAX_BREADTH,
                );

                if !new_candidates.is_empty() {
                    let next_target = new_candidates.remove(0);
                    if let Some(next_addr) = next_target.socket_addr() {
                        let mut new_tried_peers = HashSet::new();
                        new_tried_peers.insert(next_addr);
                        visited.mark_visited(next_addr);

                        tracing::debug!(
                            tx = %tx_id,
                            %instance_id,
                            next_target = %next_addr,
                            retries = retries + 1,
                            "Subscribe: connection aborted, found new candidate"
                        );

                        let msg = SubscribeMsg::Request {
                            id: tx_id,
                            instance_id,
                            htl: current_hop,
                            visited: visited.clone(),
                            is_renewal,
                        };

                        let op = SubscribeOp {
                            id: tx_id,
                            state: SubscribeState::AwaitingResponse(AwaitingResponseData {
                                next_hop: Some(next_addr),
                                instance_id,
                                retries: retries + 1,
                                current_hop,
                                tried_peers: new_tried_peers,
                                alternatives: new_candidates,
                                attempts_at_hop: 1,
                                visited,
                            }),
                            requester_addr,
                            requester_pub_key,
                            is_renewal,
                            stats: stats.map(|mut s| {
                                s.target_peer = next_target.clone();
                                s
                            }),
                            ack_received: false,
                            speculative_paths: 0,
                        };

                        op_manager
                            .notify_op_change(NetMessage::from(msg), OpEnum::Subscribe(op))
                            .await?;
                        return Err(OpError::StatePushed);
                    }
                }
            }

            // Phase 3: All retries exhausted

            // Forward NotFound upstream if we're an intermediate node
            if let Some(req_addr) = requester_addr {
                tracing::warn!(
                    tx = %tx_id,
                    %instance_id,
                    requester = %req_addr,
                    phase = "not_found",
                    "Subscribe aborted (retries exhausted) - sending NotFound upstream"
                );
                // Emit telemetry: relay returning NotFound after abort
                if let Some(event) =
                    NetEventLog::subscribe_not_found(&tx_id, &op_manager.ring, instance_id, None)
                {
                    op_manager.ring.register_events(Either::Left(event)).await;
                }

                let response_op = SubscribeOp {
                    id: tx_id,
                    state: SubscribeState::Failed,
                    requester_addr,
                    requester_pub_key,
                    is_renewal,
                    stats,
                    ack_received: false,
                    speculative_paths: 0,
                };

                op_manager
                    .notify_op_change(
                        NetMessage::from(SubscribeMsg::Response {
                            id: tx_id,
                            instance_id,
                            result: SubscribeMsgResult::NotFound,
                        }),
                        OpEnum::Subscribe(response_op),
                    )
                    .await?;
                return Err(OpError::StatePushed);
            }

            // We're the originator — notify client of failure
            notify_abort_failure(op_manager, tx_id, is_sub_op, is_renewal, &instance_id).await;
            op_manager.completed(tx_id);
            return Ok(());
        }

        // Not in AwaitingResponse state — just complete
        op_manager.completed(tx_id);
        Ok(())
    }
}

/// Notify the appropriate listener about an abort failure at the originator.
async fn notify_abort_failure(
    op_manager: &OpManager,
    tx_id: Transaction,
    is_sub_op: bool,
    is_renewal: bool,
    instance_id: &ContractInstanceId,
) {
    if is_sub_op {
        let reason = format!(
            "Subscription failed for contract {}: peer connection dropped",
            instance_id
        );
        if let Err(e) = op_manager
            .notify_contract_handler(
                crate::contract::ContractHandlerEvent::NotifySubscriptionError {
                    key: *instance_id,
                    reason,
                },
            )
            .await
        {
            tracing::debug!(
                tx = %tx_id,
                contract = %instance_id,
                error = %e,
                "Failed to send subscription abort error to notification channels"
            );
        }
    } else if is_renewal {
        tracing::debug!(
            tx = %tx_id,
            "Subscription renewal aborted, no client to notify"
        );
    } else {
        let error_result: crate::client_events::HostResult =
            Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "Subscribe operation failed: peer connection dropped".into(),
            }
            .into());
        // Use try_send to avoid blocking the event loop (see channel-safety.md).
        if let Err(err) = op_manager.result_router_tx.try_send((tx_id, error_result)) {
            tracing::error!(
                tx = %tx_id,
                error = %err,
                "Failed to send abort notification to client \
                 (result router channel full or closed)"
            );
        }
    }
}

/// Register a downstream subscriber for a contract.
///
/// Resolves the requester's `PeerKey` from the pre-resolved public key (preferred,
/// avoids NAT timing window failures) or falls back to an address lookup. If a key
/// is found, records the peer in both the downstream subscriber list and the interest
/// manager so UPDATE broadcasts reach them immediately.
pub(crate) async fn register_downstream_subscriber(
    op_manager: &OpManager,
    key: &ContractKey,
    requester_addr: std::net::SocketAddr,
    requester_pub_key: Option<&crate::transport::TransportPublicKey>,
    source_addr: Option<std::net::SocketAddr>,
    tx: &Transaction,
    warn_suffix: &str,
) {
    let peer_key = requester_pub_key
        .map(|pk| crate::ring::interest::PeerKey::from(pk.clone()))
        .or_else(|| {
            op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(requester_addr)
                .or_else(|| {
                    source_addr
                        .and_then(|sa| op_manager.ring.connection_manager.get_peer_by_addr(sa))
                })
                .map(|pkl| crate::ring::interest::PeerKey::from(pkl.pub_key.clone()))
        });

    if let Some(peer_key) = peer_key {
        if op_manager
            .ring
            .add_downstream_subscriber(key, peer_key.clone())
        {
            let is_new_peer = op_manager
                .interest_manager
                .register_peer_interest(key, peer_key, None, false);
            // Only increment downstream count for genuinely new peers, not
            // renewals. add_downstream_subscriber (hosting) returns true for
            // both new and renewed peers, so use register_peer_interest's
            // is_new return to avoid over-counting on renewal cycles.
            if is_new_peer {
                let became_interested = op_manager.interest_manager.add_downstream_subscriber(key);
                if became_interested {
                    super::broadcast_change_interests(op_manager, vec![*key], vec![]).await;
                }
            }
        } else {
            tracing::warn!(
                tx = %tx,
                contract = %key,
                "Downstream subscriber limit reached — skipping peer interest registration"
            );
        }
    } else {
        tracing::warn!(
            tx = %tx,
            contract = %key,
            requester_addr = %requester_addr,
            source_addr = ?source_addr,
            "Subscribe: could not find peer to register interest{}",
            warn_suffix
        );
    }
}

impl Operation for SubscribeOp {
    type Message = SubscribeMsg;
    type Result = SubscribeResult;

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Result<OpInitialization<Self>, OpError> {
        let id = *msg.id();
        let msg_type = match msg {
            SubscribeMsg::Request { .. } => "Request",
            SubscribeMsg::Response { .. } => "Response",
            SubscribeMsg::Unsubscribe { .. } => "Unsubscribe",
            SubscribeMsg::ForwardingAck { .. } => "ForwardingAck",
        };
        tracing::debug!(
            tx = %id,
            %msg_type,
            source_addr = ?source_addr,
            "LOAD_OR_INIT_ENTRY: entering load_or_init for Subscribe"
        );

        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Subscribe(subscribe_op))) => {
                // Existing operation - response from downstream peer
                tracing::debug!(
                    tx = %id,
                    %msg_type,
                    "LOAD_OR_INIT_POPPED: found existing Subscribe operation"
                );
                Ok(OpInitialization {
                    op: subscribe_op,
                    source_addr,
                })
            }
            Ok(Some(op)) => {
                if let Err(e) = op_manager.push(id, op).await {
                    tracing::warn!(tx = %id, error = %e, "failed to push mismatched op back");
                }
                Err(OpError::OpNotPresent(id))
            }
            Ok(None) => {
                // Check if this is a response message - if so, the operation was likely
                // cleaned up due to timeout and we should not create a new operation
                if matches!(
                    msg,
                    SubscribeMsg::Response { .. } | SubscribeMsg::ForwardingAck { .. }
                ) {
                    tracing::debug!(
                        tx = %id,
                        %msg_type,
                        phase = "load_or_init",
                        "SUBSCRIBE_OP_MISSING: response/ack arrived for non-existent operation"
                    );
                    return Err(OpError::OpNotPresent(id));
                }

                // New Request or Unsubscribe from another peer
                let (is_renewal, msg_instance_id) = match msg {
                    SubscribeMsg::Request {
                        is_renewal,
                        instance_id,
                        ..
                    } => (*is_renewal, *instance_id),
                    SubscribeMsg::Unsubscribe { instance_id, .. } => (false, *instance_id),
                    SubscribeMsg::Response { .. } | SubscribeMsg::ForwardingAck { .. } => {
                        unreachable!("Response/ForwardingAck handled above")
                    }
                };
                // Resolve requester's public key at init time, when the connection
                // is freshest. This avoids addr->pubkey lookup failures during NAT
                // traversal timing windows later at interest registration. (#2886)
                let requester_pub_key = source_addr.and_then(|addr| {
                    op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(addr)
                        .map(|pkl| pkl.pub_key().clone())
                });
                Ok(OpInitialization {
                    op: Self {
                        id,
                        state: SubscribeState::AwaitingResponse(AwaitingResponseData {
                            next_hop: None, // Will be determined during processing
                            instance_id: msg_instance_id,
                            retries: 0,
                            current_hop: 0,
                            tried_peers: HashSet::new(),
                            alternatives: Vec::new(),
                            attempts_at_hop: 0,
                            visited: super::VisitedPeers::new(&id),
                        }),
                        requester_addr: source_addr, // Store who sent us this message
                        requester_pub_key,
                        is_renewal,
                        stats: None,
                        ack_received: false,
                        speculative_paths: 0,
                    },
                    source_addr,
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a, NB: NetworkBridge>(
        self,
        conn_manager: &'a mut NB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let id = self.id;

            match input {
                SubscribeMsg::Request {
                    id,
                    instance_id,
                    htl,
                    visited,
                    is_renewal,
                } => {
                    tracing::debug!(
                        tx = %id,
                        %instance_id,
                        htl,
                        is_renewal,
                        requester_addr = ?self.requester_addr,
                        "subscribe: processing Request"
                    );

                    // Check if we have the contract
                    if let Some(key) = super::has_contract(op_manager, *instance_id).await? {
                        // We have the contract - respond to confirm subscription
                        // State is NOT sent here - requester gets state via GET, not SUBSCRIBE
                        // In the lease-based model (2026-01), we just confirm we have the contract.
                        // Updates propagate via proximity cache, not explicit tree.
                        if let Some(requester_addr) = self.requester_addr {
                            // Register the subscribing peer as a downstream subscriber.
                            // Uses requester_pub_key (resolved at init time) to avoid
                            // addr-based lookup failures during NAT timing windows. (#2886)
                            register_downstream_subscriber(
                                op_manager,
                                &key,
                                requester_addr,
                                self.requester_pub_key.as_ref(),
                                source_addr,
                                id,
                                "",
                            )
                            .await;
                            tracing::info!(tx = %id, contract = %key, is_renewal, phase = "response", "Subscription fulfilled, sending Response");
                            return Ok(OperationResult::SendAndComplete {
                                msg: NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    instance_id: *instance_id,
                                    result: SubscribeMsgResult::Subscribed { key },
                                }),
                                next_hop: Some(requester_addr),
                                stream_data: None,
                            });
                        } else {
                            // We're the originator and have the contract locally
                            tracing::info!(tx = %id, contract = %key, phase = "complete", "Subscribe completed (originator has contract locally)");
                            return Ok(OperationResult::ContinueOp(OpEnum::Subscribe(
                                SubscribeOp {
                                    id: *id,
                                    state: SubscribeState::Completed(CompletedData { key }),
                                    requester_addr: None,
                                    requester_pub_key: None,
                                    is_renewal: self.is_renewal,
                                    stats: self.stats,
                                    ack_received: false,
                                    speculative_paths: 0,
                                },
                            )));
                        }
                    }

                    // Contract not found - wait briefly for in-flight PUT
                    if let Some(key) = wait_for_contract_with_timeout(
                        op_manager,
                        *instance_id,
                        CONTRACT_WAIT_TIMEOUT_MS,
                    )
                    .await?
                    {
                        // Contract arrived - respond to confirm subscription
                        // State is NOT sent here - requester gets state via GET, not SUBSCRIBE
                        if let Some(requester_addr) = self.requester_addr {
                            // Register the subscribing peer as a downstream subscriber.
                            // Uses requester_pub_key (resolved at init time) to avoid
                            // addr-based lookup failures during NAT timing windows. (#2886)
                            register_downstream_subscriber(
                                op_manager,
                                &key,
                                requester_addr,
                                self.requester_pub_key.as_ref(),
                                source_addr,
                                id,
                                " (after contract wait)",
                            )
                            .await;
                            return Ok(OperationResult::SendAndComplete {
                                msg: NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    instance_id: *instance_id,
                                    result: SubscribeMsgResult::Subscribed { key },
                                }),
                                next_hop: Some(requester_addr),
                                stream_data: None,
                            });
                        } else {
                            tracing::info!(tx = %id, contract = %key, phase = "complete", "Subscribe completed (originator, contract arrived after wait)");
                            return Ok(OperationResult::ContinueOp(OpEnum::Subscribe(
                                SubscribeOp {
                                    id: *id,
                                    state: SubscribeState::Completed(CompletedData { key }),
                                    requester_addr: None,
                                    requester_pub_key: None,
                                    is_renewal: self.is_renewal,
                                    stats: self.stats,
                                    ack_received: false,
                                    speculative_paths: 0,
                                },
                            )));
                        }
                    }

                    // Contract still not found - try to forward
                    if *htl == 0 {
                        tracing::warn!(tx = %id, contract = %instance_id, htl = 0, phase = "not_found", "Subscribe request exhausted HTL");
                        // Emit telemetry: relay returning NotFound due to HTL exhaustion
                        if self.requester_addr.is_some() {
                            if let Some(event) = NetEventLog::subscribe_not_found(
                                id,
                                &op_manager.ring,
                                *instance_id,
                                Some(op_manager.ring.max_hops_to_live),
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }
                        }
                        return Self::not_found_result(
                            *id,
                            *instance_id,
                            self.requester_addr,
                            "HTL exhausted",
                        );
                    }

                    // Find next hop
                    let own_addr = op_manager.ring.connection_manager.peer_addr()?;
                    // Restore hash keys after deserialization (they're derived from tx)
                    let mut new_visited = visited.clone().with_transaction(id);
                    new_visited.mark_visited(own_addr);
                    if let Some(requester) = self.requester_addr {
                        new_visited.mark_visited(requester);
                    }

                    let mut candidates = op_manager.ring.k_closest_potentially_hosting(
                        instance_id,
                        &new_visited,
                        MAX_BREADTH,
                    );

                    if candidates.is_empty() {
                        tracing::warn!(tx = %id, contract = %instance_id, phase = "not_found", "No closer peers to forward subscribe request");
                        // Emit telemetry: relay returning NotFound (no forwarding targets)
                        if self.requester_addr.is_some() {
                            if let Some(event) = NetEventLog::subscribe_not_found(
                                id,
                                &op_manager.ring,
                                *instance_id,
                                None,
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }
                        }
                        return Self::not_found_result(
                            *id,
                            *instance_id,
                            self.requester_addr,
                            "no closer peers available",
                        );
                    }

                    let next_hop = candidates.remove(0);

                    // Convert to KnownPeerKeyLocation for compile-time address guarantee
                    let next_hop_known = match KnownPeerKeyLocation::try_from(&next_hop) {
                        Ok(known) => known,
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                pub_key = %e.pub_key,
                                "INTERNAL ERROR: next_hop has unknown address - routing selected peer without address"
                            );
                            return Self::not_found_result(
                                *id,
                                *instance_id,
                                self.requester_addr,
                                "next hop has unknown address",
                            );
                        }
                    };
                    let next_addr = next_hop_known.socket_addr();
                    new_visited.mark_visited(next_addr);

                    let new_htl = htl.saturating_sub(1);
                    let mut tried_peers = HashSet::new();
                    tried_peers.insert(next_addr);

                    tracing::debug!(tx = %id, %instance_id, next = %next_addr, alternatives = candidates.len(), is_renewal, "Forwarding subscribe request");

                    // Emit telemetry: relay forwarding subscribe request
                    if let Some(event) = NetEventLog::subscribe_request(
                        id,
                        &op_manager.ring,
                        *instance_id,
                        next_hop.clone(),
                        new_htl,
                    ) {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }

                    // Send ForwardingAck to requester peer before forwarding.
                    // This tells the requester "I'm working on it" so the GC task
                    // can distinguish dead peers from slow multi-hop chains.
                    if let Some(requester) = self.requester_addr {
                        let ack = NetMessage::from(SubscribeMsg::ForwardingAck {
                            id: *id,
                            instance_id: *instance_id,
                        });
                        drop(conn_manager.send(requester, ack).await);
                    }

                    Ok(OperationResult::SendAndContinue {
                        msg: NetMessage::from(SubscribeMsg::Request {
                            id: *id,
                            instance_id: *instance_id,
                            htl: new_htl,
                            visited: new_visited.clone(),
                            is_renewal: *is_renewal,
                        }),
                        next_hop: Some(next_addr),
                        state: OpEnum::Subscribe(SubscribeOp {
                            id: *id,
                            state: SubscribeState::AwaitingResponse(AwaitingResponseData {
                                next_hop: None,
                                instance_id: *instance_id,
                                retries: 0,
                                current_hop: new_htl,
                                tried_peers,
                                alternatives: candidates,
                                attempts_at_hop: 1,
                                visited: new_visited,
                            }),
                            requester_addr: self.requester_addr,
                            requester_pub_key: self.requester_pub_key,
                            is_renewal: self.is_renewal,
                            // Track the forward target so timeouts report to
                            // PeerHealthTracker and the failure estimator.
                            stats: Some(SubscribeStats {
                                target_peer: next_hop.clone(),
                                contract_location: Location::from(instance_id),
                                request_sent_at: Instant::now(),
                            }),
                            ack_received: false,
                            speculative_paths: 0,
                        }),
                        stream_data: None,
                    })
                }

                SubscribeMsg::Response {
                    id: msg_id,
                    instance_id,
                    result,
                } => {
                    tracing::debug!(
                        tx = %msg_id,
                        %instance_id,
                        requester_addr = ?self.requester_addr,
                        source_addr = ?source_addr,
                        "SUBSCRIBE_RESPONSE_ENTRY: entered Response handler"
                    );
                    match result {
                        SubscribeMsgResult::Subscribed { key } => {
                            tracing::debug!(
                                tx = %msg_id,
                                %key,
                                requester_addr = ?self.requester_addr,
                                source_addr = ?source_addr,
                                "subscribe: processing Subscribed response"
                            );

                            if let Some(requester_addr) = self.requester_addr {
                                // Relay path. We are forwarding a Subscribed response from
                                // an upstream fulfilling peer back to a downstream requester.
                                //
                                // Critically, we do NOT call `ring.subscribe()`,
                                // `record_subscription()`, `announce_contract_hosted()`, or
                                // register ourselves as having an upstream. Relays are not
                                // subscribers in their own right; they only mediate updates
                                // between the upstream fulfilling node and the downstream
                                // requester. Doing any of the above on a relay would:
                                //   - Install a lease in `active_subscriptions`, which the
                                //     renewal cycle (ring.rs::contracts_needing_renewal path 1)
                                //     would then refresh every ~2 minutes indefinitely. Each
                                //     renewal routes through fresh relays, which themselves
                                //     then subscribe — a positive feedback loop that inflates
                                //     subscription trees and UPDATE fan-out with peers that
                                //     have no genuine interest in the contract. This mirrors
                                //     the #3763 subscription-storm incident.
                                //   - Broadcast a "I host this" announcement populating
                                //     neighbors' proximity caches with the relay, further
                                //     amplifying UPDATE broadcast targets.
                                //   - Corrupt `subscription_backoff` by recording a bogus
                                //     "successful subscription" for a request the relay
                                //     never actually made.
                                //
                                // The GET-piggyback relay path already follows this model;
                                // see `operations.rs::setup_subscription_forwarding_at_relay`
                                // which documents the same constraint. The explicit
                                // SUBSCRIBE path was asymmetric historically and is brought
                                // into line here.
                                //
                                // UPDATE propagation back through the relay is driven by
                                // `register_downstream_subscriber` below (which registers
                                // the requester in both the hosting manager's downstream
                                // subscriber list and the interest manager). The upstream
                                // fulfilling node already registered *this relay* as its
                                // downstream subscriber when it processed our forwarded
                                // Request, so UPDATEs will reach us and we'll fan them out
                                // to the registered downstream.
                                register_downstream_subscriber(
                                    op_manager,
                                    key,
                                    requester_addr,
                                    self.requester_pub_key.as_ref(),
                                    None,
                                    msg_id,
                                    " (relay registration on Response)",
                                )
                                .await;

                                tracing::debug!(tx = %msg_id, %key, requester = %requester_addr, "Forwarding Subscribed response to requester");
                                // Note: ResponseSent telemetry is emitted by from_outbound_msg()
                                return Ok(OperationResult::SendAndComplete {
                                    msg: NetMessage::from(SubscribeMsg::Response {
                                        id: *msg_id,
                                        instance_id: *instance_id,
                                        result: SubscribeMsgResult::Subscribed { key: *key },
                                    }),
                                    next_hop: Some(requester_addr),
                                    stream_data: None,
                                });
                            }

                            // Originator path. We initiated this subscribe (client or
                            // renewal cycle) and the upstream peer confirmed. Install the
                            // local lease, fetch the contract if needed, and record the
                            // upstream as our interest target for future Unsubscribes.

                            // Register our subscription locally.
                            op_manager.ring.subscribe(*key);
                            op_manager.ring.complete_subscription_request(key, true);

                            // Note: we intentionally do NOT call touch_hosting() here.
                            // Subscription renewal is internal maintenance, not user activity.
                            // Only genuine user access (GET) should refresh hosting TTL,
                            // otherwise renewal creates an immortal-entry feedback loop.

                            tracing::info!(
                                tx = %msg_id,
                                contract = %format!("{:.8}", key),
                                "SUBSCRIPTION_ACCEPTED: registered lease-based subscription"
                            );
                            crate::node::network_status::record_subscription(format!("{key}"));

                            // Fetch contract if we don't have it.
                            // This is non-fatal - if it fails, we still complete the
                            // subscription. The contract will eventually arrive via
                            // UPDATE broadcasts.
                            if let Err(e) = fetch_contract_if_missing(op_manager, *key.id()).await {
                                tracing::debug!(
                                    tx = %msg_id,
                                    contract = %format!("{:.8}", key),
                                    error = ?e,
                                    "fetch_contract_if_missing failed, will receive state via UPDATE"
                                );
                            }

                            // CRITICAL: Announce to neighbors that we cache this contract.
                            // This ensures UPDATE broadcasts will reach us. Without this,
                            // if the contract was already cached (fetch_contract_if_missing returned early),
                            // neighbors wouldn't know we have the contract and wouldn't broadcast updates to us.
                            super::announce_contract_hosted(op_manager, key).await;

                            // Register the responding peer as our upstream in the interest manager.
                            // This peer fulfilled our subscription, so it's the target for
                            // Unsubscribe messages when we no longer need updates.
                            if let Some(resp_addr) = source_addr {
                                if let Some(pkl) = op_manager
                                    .ring
                                    .connection_manager
                                    .get_peer_by_addr(resp_addr)
                                {
                                    let peer_key =
                                        crate::ring::interest::PeerKey::from(pkl.pub_key.clone());
                                    op_manager
                                        .interest_manager
                                        .register_peer_interest(key, peer_key, None, true);
                                }
                            }

                            tracing::info!(tx = %msg_id, contract = %key, phase = "complete", "Subscribe completed (originator)");

                            // Register local interest so that ChangeInterests from peers
                            // get properly processed. Without this, when other nodes broadcast
                            // ChangeInterests for contracts they seed, the has_local_interest()
                            // check in the ChangeInterests handler fails, preventing peer
                            // interest registration and breaking update propagation.
                            if !self.is_renewal {
                                let became_interested =
                                    op_manager.interest_manager.add_local_client(key);
                                if became_interested {
                                    super::broadcast_change_interests(
                                        op_manager,
                                        vec![*key],
                                        vec![],
                                    )
                                    .await;
                                }
                            }

                            // Emit telemetry for successful subscription
                            let own_loc = op_manager.ring.connection_manager.own_location();
                            if let Some(event) = NetEventLog::subscribe_success(
                                msg_id,
                                &op_manager.ring,
                                *key,
                                own_loc,
                                None, // hop_count not tracked in subscribe
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }

                            Ok(OperationResult::ContinueOp(OpEnum::Subscribe(
                                SubscribeOp {
                                    id,
                                    state: SubscribeState::Completed(CompletedData { key: *key }),
                                    requester_addr: None,
                                    requester_pub_key: None,
                                    is_renewal: self.is_renewal,
                                    stats: self.stats,
                                    ack_received: false,
                                    speculative_paths: 0,
                                },
                            )))
                        }
                        SubscribeMsgResult::NotFound => {
                            tracing::debug!(
                                tx = %msg_id,
                                %instance_id,
                                requester_addr = ?self.requester_addr,
                                source_addr = ?source_addr,
                                "subscribe: processing NotFound response"
                            );

                            // Extract retry state from current AwaitingResponse
                            let (
                                retries,
                                current_hop,
                                mut tried_peers,
                                mut alternatives,
                                attempts_at_hop,
                                mut visited,
                            ) = if let SubscribeState::AwaitingResponse(ref data) = self.state {
                                (
                                    data.retries,
                                    data.current_hop,
                                    data.tried_peers.clone(),
                                    data.alternatives.clone(),
                                    data.attempts_at_hop,
                                    data.visited.clone(),
                                )
                            } else {
                                (
                                    0,
                                    0,
                                    HashSet::new(),
                                    Vec::new(),
                                    0,
                                    super::VisitedPeers::new(msg_id),
                                )
                            };

                            // Mark the source that returned NotFound as tried
                            if let Some(addr) = source_addr {
                                tried_peers.insert(addr);
                                visited.mark_visited(addr);
                            }

                            // --- Breadth retry: try alternative peers at same hop ---
                            if !alternatives.is_empty() && attempts_at_hop < MAX_BREADTH {
                                let next_target = alternatives.remove(0);
                                if let Some(next_addr) = next_target.socket_addr() {
                                    tried_peers.insert(next_addr);
                                    visited.mark_visited(next_addr);

                                    tracing::info!(
                                        tx = %msg_id,
                                        %instance_id,
                                        peer_addr = %next_addr,
                                        attempts_at_hop = attempts_at_hop + 1,
                                        max_attempts = MAX_BREADTH,
                                        phase = "retry",
                                        "Subscribe: trying alternative peer at same hop"
                                    );

                                    return Ok(OperationResult::SendAndContinue {
                                        msg: NetMessage::from(SubscribeMsg::Request {
                                            id: *msg_id,
                                            instance_id: *instance_id,
                                            htl: current_hop,
                                            visited: visited.clone(),
                                            is_renewal: self.is_renewal,
                                        }),
                                        next_hop: Some(next_addr),
                                        state: OpEnum::Subscribe(SubscribeOp {
                                            id,
                                            state: SubscribeState::AwaitingResponse(
                                                AwaitingResponseData {
                                                    next_hop: Some(next_addr),
                                                    instance_id: *instance_id,
                                                    retries,
                                                    current_hop,
                                                    tried_peers,
                                                    alternatives,
                                                    attempts_at_hop: attempts_at_hop + 1,
                                                    visited,
                                                },
                                            ),
                                            requester_addr: self.requester_addr,
                                            requester_pub_key: self.requester_pub_key,
                                            is_renewal: self.is_renewal,
                                            stats: self.stats.map(|mut s| {
                                                s.target_peer = next_target.clone();
                                                s
                                            }),
                                            ack_received: false,
                                            speculative_paths: 0,
                                        }),
                                        stream_data: None,
                                    });
                                }
                            }

                            // --- Retry round: seek new candidates via k_closest ---
                            if retries < MAX_RETRIES {
                                for addr in &tried_peers {
                                    visited.mark_visited(*addr);
                                }

                                let mut new_candidates =
                                    op_manager.ring.k_closest_potentially_hosting(
                                        instance_id,
                                        &visited,
                                        MAX_BREADTH,
                                    );

                                if !new_candidates.is_empty() {
                                    let next_target = new_candidates.remove(0);
                                    if let Some(next_addr) = next_target.socket_addr() {
                                        let mut new_tried_peers = HashSet::new();
                                        new_tried_peers.insert(next_addr);
                                        visited.mark_visited(next_addr);

                                        tracing::info!(
                                            tx = %msg_id,
                                            %instance_id,
                                            peer_addr = %next_addr,
                                            retries = retries + 1,
                                            max_retries = MAX_RETRIES,
                                            new_candidates = new_candidates.len(),
                                            phase = "retry",
                                            "Subscribe: seeking new candidates after exhausted alternatives"
                                        );

                                        return Ok(OperationResult::SendAndContinue {
                                            msg: NetMessage::from(SubscribeMsg::Request {
                                                id: *msg_id,
                                                instance_id: *instance_id,
                                                htl: current_hop,
                                                visited: visited.clone(),
                                                is_renewal: self.is_renewal,
                                            }),
                                            next_hop: Some(next_addr),
                                            state: OpEnum::Subscribe(SubscribeOp {
                                                id,
                                                state: SubscribeState::AwaitingResponse(
                                                    AwaitingResponseData {
                                                        next_hop: Some(next_addr),
                                                        instance_id: *instance_id,
                                                        retries: retries + 1,
                                                        current_hop,
                                                        tried_peers: new_tried_peers,
                                                        alternatives: new_candidates,
                                                        attempts_at_hop: 1,
                                                        visited,
                                                    },
                                                ),
                                                requester_addr: self.requester_addr,
                                                requester_pub_key: self.requester_pub_key,
                                                is_renewal: self.is_renewal,
                                                stats: self.stats.map(|mut s| {
                                                    s.target_peer = next_target.clone();
                                                    s
                                                }),
                                                ack_received: false,
                                                speculative_paths: 0,
                                            }),
                                            stream_data: None,
                                        });
                                    }
                                }
                            }

                            // --- All retries exhausted ---
                            if let Some(requester_addr) = self.requester_addr {
                                // We're an intermediate node - forward NotFound to requester
                                tracing::debug!(tx = %msg_id, %instance_id, requester = %requester_addr, "Forwarding NotFound response to requester (retries exhausted)");
                                Ok(OperationResult::SendAndComplete {
                                    msg: NetMessage::from(SubscribeMsg::Response {
                                        id: *msg_id,
                                        instance_id: *instance_id,
                                        result: SubscribeMsgResult::NotFound,
                                    }),
                                    next_hop: Some(requester_addr),
                                    stream_data: None,
                                })
                            } else {
                                // We're the originator - check if we have the contract locally
                                // before failing. If we do, re-seed the network and complete.
                                let local_contract = match op_manager
                                    .notify_contract_handler(ContractHandlerEvent::GetQuery {
                                        instance_id: *instance_id,
                                        return_contract_code: true,
                                    })
                                    .await
                                {
                                    Ok(ContractHandlerEvent::GetResponse {
                                        key: Some(key),
                                        response:
                                            Ok(StoreResponse {
                                                state: Some(state),
                                                contract,
                                            }),
                                    }) => Some((key, state, contract)),
                                    _ => None,
                                };

                                if let Some((key, state, contract)) = local_contract {
                                    // We have the contract locally - re-seed the network
                                    tracing::info!(
                                        tx = %msg_id,
                                        contract = %key,
                                        phase = "reseed",
                                        "Subscribe: Network returned NotFound, re-hosting from local cache"
                                    );

                                    // Re-host to the network with our local copy
                                    if let Some(contract_code) = contract {
                                        let put_result = op_manager
                                            .notify_contract_handler(
                                                ContractHandlerEvent::PutQuery {
                                                    key,
                                                    state,
                                                    related_contracts: RelatedContracts::default(),
                                                    contract: Some(contract_code),
                                                },
                                            )
                                            .await;
                                        match put_result {
                                            Ok(ContractHandlerEvent::PutResponse {
                                                new_value: Ok(_),
                                                ..
                                            }) => {
                                                tracing::debug!(tx = %msg_id, %key, "Re-hosted contract to network");
                                                super::announce_contract_hosted(op_manager, &key)
                                                    .await;
                                            }
                                            _ => {
                                                tracing::warn!(tx = %msg_id, %key, "Failed to re-host contract");
                                            }
                                        }
                                    }

                                    // Complete the subscription successfully with local cache
                                    let own_loc = op_manager.ring.connection_manager.own_location();
                                    if let Some(event) = NetEventLog::subscribe_success(
                                        msg_id,
                                        &op_manager.ring,
                                        key,
                                        own_loc,
                                        None, // hop_count not tracked in subscribe
                                    ) {
                                        op_manager.ring.register_events(Either::Left(event)).await;
                                    }

                                    Ok(OperationResult::ContinueOp(OpEnum::Subscribe(
                                        SubscribeOp {
                                            id,
                                            state: SubscribeState::Completed(CompletedData { key }),
                                            requester_addr: None,
                                            requester_pub_key: None,
                                            is_renewal: self.is_renewal,
                                            stats: self.stats,
                                            ack_received: false,
                                            speculative_paths: 0,
                                        },
                                    )))
                                } else {
                                    // No local cache - subscription failed
                                    tracing::warn!(tx = %msg_id, %instance_id, phase = "not_found", "Subscribe failed - contract not found");

                                    // Notify subscribed clients that the subscription failed
                                    let reason = format!(
                                        "Subscription failed: contract {} not found in network",
                                        instance_id
                                    );
                                    if let Err(e) = op_manager
                                        .notify_contract_handler(
                                            ContractHandlerEvent::NotifySubscriptionError {
                                                key: *instance_id,
                                                reason,
                                            },
                                        )
                                        .await
                                    {
                                        tracing::debug!(
                                            contract = %instance_id,
                                            error = %e,
                                            "Failed to send subscription error to client notification channels"
                                        );
                                    }

                                    // Emit telemetry for subscription not found (relay nodes only)
                                    if self.requester_addr.is_some() {
                                        if let Some(event) = NetEventLog::subscribe_not_found(
                                            msg_id,
                                            &op_manager.ring,
                                            *instance_id,
                                            None, // hop_count not tracked in subscribe
                                        ) {
                                            op_manager
                                                .ring
                                                .register_events(Either::Left(event))
                                                .await;
                                        }
                                    }

                                    // Return op in Failed state - to_host_result() will return error
                                    Ok(OperationResult::ContinueOp(OpEnum::Subscribe(
                                        SubscribeOp {
                                            id,
                                            state: SubscribeState::Failed,
                                            requester_addr: None,
                                            requester_pub_key: None,
                                            is_renewal: self.is_renewal,
                                            stats: self.stats,
                                            ack_received: false,
                                            speculative_paths: 0,
                                        },
                                    )))
                                }
                            }
                        }
                    }
                }

                SubscribeMsg::Unsubscribe { id, instance_id } => {
                    tracing::debug!(
                        tx = %id,
                        %instance_id,
                        source_addr = ?source_addr,
                        "received unsubscribe notification"
                    );

                    // Resolve the sender's PeerKey
                    let sender_peer = self
                        .requester_pub_key
                        .as_ref()
                        .map(|pk| crate::ring::interest::PeerKey::from(pk.clone()))
                        .or_else(|| {
                            source_addr.and_then(|addr| {
                                op_manager
                                    .ring
                                    .connection_manager
                                    .get_peer_by_addr(addr)
                                    .map(|pkl| {
                                        crate::ring::interest::PeerKey::from(pkl.pub_key.clone())
                                    })
                            })
                        });

                    // Look up the full ContractKey from storage
                    if let Some(key) = super::has_contract(op_manager, *instance_id).await? {
                        if let Some(peer) = &sender_peer {
                            let was_downstream =
                                op_manager.ring.remove_downstream_subscriber(&key, peer);
                            let was_interested =
                                op_manager.interest_manager.remove_peer_interest(&key, peer);
                            // Only decrement downstream count if the peer was
                            // actually tracked, to stay in sync with the
                            // increment in register_downstream_subscriber.
                            if was_downstream || was_interested {
                                let lost_interest = op_manager
                                    .interest_manager
                                    .remove_downstream_subscriber(&key);
                                if lost_interest {
                                    super::broadcast_change_interests(
                                        op_manager,
                                        vec![],
                                        vec![key],
                                    )
                                    .await;
                                }
                            }
                        } else {
                            tracing::warn!(
                                tx = %id,
                                %instance_id,
                                source_addr = ?source_addr,
                                "Unsubscribe: could not resolve sender peer, downstream entry not removed"
                            );
                        }

                        // Chain propagation: if no more interest, unsubscribe upstream
                        if op_manager.ring.should_unsubscribe_upstream(&key) {
                            tracing::debug!(
                                tx = %id,
                                contract = %key,
                                "No remaining subscribers, propagating unsubscribe upstream"
                            );
                            op_manager.send_unsubscribe_upstream(&key).await;
                        } else {
                            tracing::debug!(
                                tx = %id,
                                contract = %key,
                                "Still have subscribers, not propagating unsubscribe"
                            );
                        }
                    } else {
                        tracing::debug!(
                            tx = %id,
                            %instance_id,
                            "Contract not found locally, ignoring unsubscribe"
                        );
                    }

                    Ok(OperationResult::Completed)
                }

                SubscribeMsg::ForwardingAck { id, instance_id } => {
                    // A downstream relay has acknowledged forwarding our request.
                    // Mark the op so the GC task knows the chain is alive.
                    tracing::debug!(
                        tx = %id,
                        %instance_id,
                        "Received forwarding ACK from downstream relay"
                    );
                    Ok(OperationResult::ContinueOp(OpEnum::Subscribe(
                        SubscribeOp {
                            ack_received: true,
                            ..self
                        },
                    )))
                }
            }
        })
    }
}

impl IsOperationCompleted for SubscribeOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, SubscribeState::Completed(_))
    }
}

#[cfg(test)]
mod tests;

/// Task-per-transaction client-initiated SUBSCRIBE path (#1454 Phase 2b).
/// First production consumer of `OpCtx::send_and_await`.
mod op_ctx_task;

pub(crate) use op_ctx_task::{run_client_subscribe, start_client_subscribe};

mod messages {
    use std::fmt::Display;

    use super::*;

    /// Result of a SUBSCRIBE operation - either subscription succeeded or contract was not found.
    ///
    /// This provides explicit semantics for "contract not found" rather than
    /// requiring interpretation of `subscribed: false` which could also mean other failures.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum SubscribeMsgResult {
        /// Subscription succeeded - includes full contract key
        Subscribed { key: ContractKey },
        /// Contract was not found after exhaustive search
        NotFound,
    }

    /// Subscribe operation messages.
    ///
    /// Uses hop-by-hop routing: each node stores `requester_addr` from the transport layer
    /// to route responses back. No `PeerKeyLocation` is embedded in wire messages.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum SubscribeMsg {
        /// Request to subscribe to a contract. Forwarded hop-by-hop toward contract location.
        Request {
            id: Transaction,
            /// Contract instance to subscribe to (full key not needed for routing)
            instance_id: ContractInstanceId,
            /// Hops to live - decremented at each hop
            htl: usize,
            /// Bloom filter tracking visited peers to prevent loops
            visited: super::super::VisitedPeers,
            /// Whether this is a renewal (requester already has contract state).
            /// If true, responder skips sending state to save bandwidth.
            is_renewal: bool,
        },
        /// Response for a SUBSCRIBE operation. Routed hop-by-hop back to originator.
        /// Uses instance_id for routing (always available from the request).
        /// The full ContractKey is only present in SubscribeMsgResult::Subscribed.
        Response {
            id: Transaction,
            instance_id: ContractInstanceId,
            result: SubscribeMsgResult,
        },
        /// Explicit unsubscribe notification sent upstream for fast cleanup.
        /// Fire-and-forget: does not require a response or existing operation state.
        Unsubscribe {
            id: Transaction,
            instance_id: ContractInstanceId,
        },

        /// Lightweight ACK sent by a relay peer back to its upstream when it forwards
        /// a SUBSCRIBE request to the next hop. Tells the upstream "I'm working on it"
        /// so the GC task can distinguish dead peers from slow multi-hop chains.
        /// Fire-and-forget — no response expected.
        ForwardingAck {
            id: Transaction,
            instance_id: ContractInstanceId,
        },
    }

    impl InnerMessage for SubscribeMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::Unsubscribe { id, .. }
                | Self::ForwardingAck { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { instance_id, .. }
                | Self::Response { instance_id, .. }
                | Self::Unsubscribe { instance_id, .. }
                | Self::ForwardingAck { instance_id, .. } => Some(Location::from(instance_id)),
            }
        }
    }

    impl Display for SubscribeMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request { instance_id, .. } => {
                    write!(f, "Subscribe::Request(id: {id}, contract: {instance_id})")
                }
                Self::Response {
                    instance_id,
                    result,
                    ..
                } => {
                    let result_str = match result {
                        SubscribeMsgResult::Subscribed { key } => format!("Subscribed({key})"),
                        SubscribeMsgResult::NotFound => "NotFound".to_string(),
                    };
                    write!(
                        f,
                        "Subscribe::Response(id: {id}, instance_id: {instance_id}, result: {result_str})"
                    )
                }
                Self::Unsubscribe { instance_id, .. } => {
                    write!(
                        f,
                        "Subscribe::Unsubscribe(id: {id}, contract: {instance_id})"
                    )
                }
                Self::ForwardingAck { instance_id, .. } => {
                    write!(
                        f,
                        "Subscribe::ForwardingAck(id: {id}, instance_id: {instance_id})"
                    )
                }
            }
        }
    }
}
