// #1454 phase 5 final (SUBSCRIBE slice) — the legacy state-machine path
// is retired but the type surface (SubscribeOp, SubscribeState,
// SubscribeStats, AwaitingResponseData/PrepareRequestData/CompletedData)
// is kept for test fixtures and operations-module re-exports. Module-
// level allow-dead because the surviving types are still load-bearing
// in tests and shared with `op_ctx_task::*`. Phase 6 retires the
// surface entirely.
#![allow(dead_code)]

use std::collections::HashSet;
use std::time::Instant;

use either::Either;

pub(crate) use self::messages::{SubscribeMsg, SubscribeMsgResult};
use super::{OpError, OpOutcome, OperationResult, get};
use crate::node::IsOperationCompleted;
use crate::ring::PeerKeyLocation;
use crate::tracing::NetEventLog;
use crate::{
    client_events::HostResult,
    message::{InnerMessage, NetMessage, Transaction},
    node::OpManager,
    ring::{Location, RingError},
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

/// Timeout for waiting on contract storage notification.
/// Used when a subscription arrives before the contract has been propagated via PUT.
pub(super) const CONTRACT_WAIT_TIMEOUT_MS: u64 = 2_000;

/// Wait for a contract to become available, using channel-based notification.
///
/// This handles the race condition where a subscription arrives before the contract
/// has been propagated via PUT. The flow is:
/// 1. Fast path: check if contract already exists
/// 2. Register notification waiter
/// 3. Check again (handles race between step 1 and 2)
/// 4. Wait for notification or timeout
/// 5. Final verification of actual state
pub(super) async fn wait_for_contract_with_timeout(
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

    // Start a GET operation to fetch the contract via the task-per-tx
    // sub-op driver. The receiver is dropped — caller relies on
    // `wait_for_contract_with_timeout` (storage poll + timeout) to
    // detect arrival, mirroring the legacy fire-and-forget shape.
    let (_tx, _rx) = get::op_ctx_task::start_sub_op_get(
        op_manager,
        instance_id,
        /* return_contract_code */ true,
    );

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
    ///
    /// Constructed only in test fixtures (`start_op` / `start_op_with_id`)
    /// after #1454 SUBSCRIBE migrations: production callers (client,
    /// renewal, executor, sub-op) drive subscribes through the
    /// task-per-tx machinery in `op_ctx_task.rs` and never push a
    /// `SubscribeOp` in the `PrepareRequest` state. Match arms in
    /// `process_message` are retained as a structural safety net.
    #[allow(dead_code)]
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
/// Test fixture only after #1454 SUBSCRIBE migrations: production
/// callers (client, renewal, executor, sub-op) all go through the
/// task-per-tx drivers in `op_ctx_task.rs` which build `SubscribeOp`s
/// inline.
#[cfg(test)]
pub(crate) fn start_op(instance_id: ContractInstanceId, is_renewal: bool) -> SubscribeOp {
    let id = Transaction::new::<SubscribeMsg>();
    SubscribeOp {
        id,
        state: SubscribeState::PrepareRequest(PrepareRequestData { id, instance_id }),
        requester_addr: None, // Local operation, we are the originator
        requester_pub_key: None,
        is_renewal,
        stats: None,
    }
}

/// Create a Subscribe operation with a specific transaction ID (for operation deduplication)
///
/// Test fixture only after #1454 SUBSCRIBE migrations.
#[cfg(test)]
pub(crate) fn start_op_with_id(
    instance_id: ContractInstanceId,
    id: Transaction,
    is_renewal: bool,
) -> SubscribeOp {
    SubscribeOp {
        id,
        state: SubscribeState::PrepareRequest(PrepareRequestData { id, instance_id }),
        requester_addr: None, // Local operation, we are the originator
        requester_pub_key: None,
        is_renewal,
        stats: None,
    }
}

/// Create a SubscribeOp for routing an Unsubscribe message to a target peer.
///
/// Test fixture only after #1454 phase 5 final (SUBSCRIBE slice).
/// Production callers now send Unsubscribe through `OpCtx::send_fire_and_forget`
/// directly from `OpManager::send_unsubscribe_upstream`, bypassing
/// `ops.subscribe` entirely.
#[cfg(test)]
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
    }
}

// `request_subscribe` was the legacy state-machine entry point that
// pushed a `SubscribeOp` into `OpManager.ops.subscribe` via
// `notify_op_change(_nonblocking)`. It was retired by #1454:
// client-initiated callers go through
// `op_ctx_task::run_client_subscribe`, renewals go through
// `op_ctx_task::run_renewal_subscribe`, and executor auto-subscribe
// goes through `op_ctx_task::run_executor_subscribe`. None of these
// push state into `ops.subscribe`. Relay-side intermediate-peer
// SUBSCRIBE state still flows through the legacy path via
// `process_message`, but no caller constructs a fresh `SubscribeOp`
// for it — the relay state arrives as a `SubscribeMsg::Request` from
// the wire and is handled by `start_relay_subscribe`.
//
// `start_op` and `start_op_with_id` remain for test fixtures.

/// Outcome of [`prepare_initial_request`]: the decision about how to originate
/// a subscribe request based on the node's current ring state and contract
/// availability.
///
/// This type exists so all task-per-tx subscribe entry points
/// (`op_ctx_task::run_client_subscribe`,
/// `op_ctx_task::run_renewal_subscribe`,
/// `op_ctx_task::run_executor_subscribe`) share the "which peer, or
/// local-complete, or give up?" decision logic without duplicating
/// `k_closest_potentially_hosting` + fallback + local-completion
/// handling. The legacy state-machine path (`request_subscribe`) used
/// the same helper before #1454 and is now retired.
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
/// All task-per-tx subscribe entry points
/// (`op_ctx_task::run_client_subscribe`, `run_renewal_subscribe`,
/// `run_executor_subscribe`) reuse the same ring lookup / fallback /
/// local-completion logic via this helper. The helper is pure modulo
/// telemetry emission: it calls `NetEventLog::subscribe_request` on
/// the `NetworkRequest` branch so all callers get identical event
/// logs, but it does not mutate `op_manager` state and does not push
/// any `SubscribeOp` into the per-op DashMap.
///
/// The decision branches:
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

/// Handle a fresh inbound `SubscribeMsg::Unsubscribe` from a peer.
///
/// Removes the sender's downstream subscriber tracking for the contract and
/// chains the unsubscribe upstream if no remaining interest is present.
///
/// Replaces the legacy `process_message::SubscribeMsg::Unsubscribe` arm
/// retired in #1454 phase 5 final (SUBSCRIBE slice). The node.rs dispatch
/// site calls this directly for inbound Unsubscribe variants instead of
/// routing through `handle_op_request<SubscribeOp>`.
pub(crate) async fn handle_unsubscribe_inbound(
    op_manager: &OpManager,
    tx: Transaction,
    instance_id: ContractInstanceId,
    source_addr: Option<std::net::SocketAddr>,
) {
    tracing::debug!(
        tx = %tx,
        %instance_id,
        ?source_addr,
        "received unsubscribe notification"
    );

    let sender_peer = source_addr.and_then(|addr| {
        op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(addr)
            .map(|pkl| crate::ring::interest::PeerKey::from(pkl.pub_key.clone()))
    });

    let key = match super::has_contract(op_manager, instance_id).await {
        Ok(Some(key)) => key,
        Ok(None) => {
            tracing::debug!(
                tx = %tx,
                %instance_id,
                "Contract not found locally, ignoring unsubscribe"
            );
            return;
        }
        Err(err) => {
            tracing::warn!(
                tx = %tx,
                %instance_id,
                %err,
                "Contract lookup failed while handling unsubscribe"
            );
            return;
        }
    };

    if let Some(peer) = &sender_peer {
        let was_downstream = op_manager.ring.remove_downstream_subscriber(&key, peer);
        let was_interested = op_manager.interest_manager.remove_peer_interest(&key, peer);
        // Only decrement downstream count if the peer was actually tracked,
        // to stay in sync with the increment in register_downstream_subscriber.
        if was_downstream || was_interested {
            let lost_interest = op_manager
                .interest_manager
                .remove_downstream_subscriber(&key);
            if lost_interest {
                super::broadcast_change_interests(op_manager, vec![], vec![key]).await;
            }
        }
    } else {
        tracing::warn!(
            tx = %tx,
            %instance_id,
            ?source_addr,
            "Unsubscribe: could not resolve sender peer, downstream entry not removed"
        );
    }

    if op_manager.ring.should_unsubscribe_upstream(&key) {
        tracing::debug!(
            tx = %tx,
            contract = %key,
            "No remaining subscribers, propagating unsubscribe upstream"
        );
        op_manager.send_unsubscribe_upstream(&key).await;
    } else {
        tracing::debug!(
            tx = %tx,
            contract = %key,
            "Still have subscribers, not propagating unsubscribe"
        );
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
pub(crate) mod op_ctx_task;

pub(crate) use op_ctx_task::{
    RenewalOutcome, run_client_subscribe, run_executor_subscribe, run_renewal_subscribe,
    start_client_subscribe,
};

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
