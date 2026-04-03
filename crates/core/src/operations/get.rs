use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;
use std::collections::HashSet;
use std::fmt::Display;
use std::pin::Pin;
use std::{future::Future, time::Instant};

use crate::client_events::HostResult;
use crate::config::GlobalExecutor;
use crate::node::IsOperationCompleted;
use crate::{
    contract::{ContractHandlerEvent, StoreResponse},
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager},
    operations::{OpInitialization, Operation},
    ring::{Location, PeerKeyLocation, RingError},
    tracing::{NetEventLog, OperationFailure, state_hash_full},
};
use either::Either;

use super::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};
use super::{OpEnum, OpError, OpOutcome, OperationResult, should_use_streaming};
use crate::transport::peer_connection::StreamId;

use self::messages::GetStreamingPayload;
pub(crate) use self::messages::{GetMsg, GetMsgResult};

/// Maximum number of retries to get values.
const MAX_RETRIES: usize = 10;

/// Maximum number of peer attempts at each hop level
const DEFAULT_MAX_BREADTH: usize = 3;

/// Minimum HTL for speculative retries.
///
/// Retries use a reduced HTL (capped at current_hop) to avoid full-depth
/// traversal storms. This floor ensures retries still reach peers 2-3 hops
/// away, which is the minimum useful search depth in any topology.
const MIN_RETRY_HTL: usize = 3;

pub(crate) fn start_op(
    instance_id: ContractInstanceId,
    fetch_contract: bool,
    subscribe: bool,
    blocking_subscribe: bool,
) -> GetOp {
    let contract_location = Location::from(&instance_id);
    let id = Transaction::new::<GetMsg>();
    tracing::debug!(tx = %id, "Requesting get contract {instance_id} @ loc({contract_location})");
    let state = Some(GetState::PrepareRequest(PrepareRequestData {
        instance_id,
        id,
        fetch_contract,
        subscribe,
        blocking_subscribe,
    }));
    GetOp {
        id,
        state,
        result: None,
        stats: Some(Box::new(GetStats {
            contract_location,
            next_peer: None,
            transfer_time: None,
            first_response_time: None,
        })),
        upstream_addr: None, // Local operation, no upstream peer
        local_fallback: None,
        auto_fetch: false,
        ack_received: false,
        speculative_paths: 0,
    }
}

/// Create a GET operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(
    instance_id: ContractInstanceId,
    fetch_contract: bool,
    subscribe: bool,
    blocking_subscribe: bool,
    id: Transaction,
) -> GetOp {
    let contract_location = Location::from(&instance_id);
    tracing::debug!(tx = %id, "Requesting get contract {instance_id} @ loc({contract_location}) with existing transaction ID");
    let state = Some(GetState::PrepareRequest(PrepareRequestData {
        instance_id,
        id,
        fetch_contract,
        subscribe,
        blocking_subscribe,
    }));
    GetOp {
        id,
        state,
        result: None,
        stats: Some(Box::new(GetStats {
            contract_location,
            next_peer: None,
            transfer_time: None,
            first_response_time: None,
        })),
        upstream_addr: None, // Local operation, no upstream peer
        local_fallback: None,
        auto_fetch: false,
        ack_received: false,
        speculative_paths: 0,
    }
}

/// Create a GET operation pre-targeted at a specific peer, bypassing normal routing.
/// Returns the operation and the message to send. Used for auto-fetching contracts
/// from peers known to have them (e.g., UPDATE senders).
pub(crate) fn start_targeted_op(
    instance_id: ContractInstanceId,
    target: PeerKeyLocation,
    max_hops_to_live: usize,
) -> (GetOp, GetMsg) {
    let contract_location = Location::from(&instance_id);
    let id = Transaction::new::<GetMsg>();
    tracing::debug!(
        tx = %id,
        "Requesting targeted get contract {instance_id} @ loc({contract_location})"
    );

    let visited = super::VisitedPeers::new(&id);
    let mut tried_peers = HashSet::new();
    if let Some(addr) = target.socket_addr() {
        tried_peers.insert(addr);
    }

    let state = Some(GetState::AwaitingResponse(AwaitingResponseData {
        instance_id,
        retries: 0,
        fetch_contract: true,
        requester: None,
        current_hop: max_hops_to_live,
        subscribe: false,
        blocking_subscribe: false,
        next_hop: target,
        tried_peers,
        alternatives: vec![],
        attempts_at_hop: 1,
        visited: visited.clone(),
    }));

    let msg = GetMsg::Request {
        id,
        instance_id,
        fetch_contract: true,
        htl: max_hops_to_live,
        visited,
    };

    let op = GetOp {
        id,
        state,
        result: None,
        stats: Some(Box::new(GetStats {
            contract_location,
            next_peer: None,
            transfer_time: None,
            first_response_time: None,
        })),
        upstream_addr: None,
        local_fallback: None,
        auto_fetch: true, // System-initiated, not a client request
        ack_received: false,
        speculative_paths: 0,
    };

    (op, msg)
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get(
    op_manager: &OpManager,
    get_op: GetOp,
    visited: super::VisitedPeers,
) -> Result<(), OpError> {
    let (mut candidates, id, instance_id_val, _fetch_contract, local_fallback) = if let Some(
        GetState::PrepareRequest(data),
    ) =
        &get_op.state
    {
        let instance_id = &data.instance_id;
        let id = &data.id;
        let fetch_contract = &data.fetch_contract;
        // Check local storage to have a fallback, but prefer network for freshness.
        // This implements "network first, local fallback" to ensure we get the latest
        // version of contracts rather than serving potentially stale cached data.
        tracing::debug!(
            tx = %id,
            %instance_id,
            "GET: Checking local storage for fallback before network query"
        );

        let get_result = op_manager
            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                instance_id: *instance_id,
                return_contract_code: *fetch_contract,
            })
            .await;

        let local_value = match get_result {
            Ok(ContractHandlerEvent::GetResponse {
                key: Some(key),
                response:
                    Ok(StoreResponse {
                        state: Some(state),
                        contract,
                    }),
            }) => {
                if *fetch_contract && contract.is_none() {
                    tracing::info!(
                        tx = %id,
                        %instance_id,
                        "GET: state available locally but contract code missing; will query peers"
                    );
                    None
                } else {
                    Some((key, state, contract))
                }
            }
            _ => None,
        };

        // Find peers to query - we prefer network over local cache for freshness
        let candidates = op_manager.ring.k_closest_potentially_hosting(
            instance_id,
            &visited,
            DEFAULT_MAX_BREADTH,
        );

        if candidates.is_empty() {
            // No peers available - use local cache if we have it
            if let Some((key, state, contract)) = local_value {
                tracing::info!(
                    tx = %id,
                    contract = %key,
                    phase = "complete",
                    "GET: No peers available, returning local cache"
                );

                let completed_op = GetOp {
                    id: *id,
                    state: Some(GetState::Finished(FinishedData { key })),
                    result: Some(GetResult {
                        key,
                        state,
                        contract,
                    }),
                    stats: get_op.stats,
                    upstream_addr: get_op.upstream_addr,
                    local_fallback: None,
                    auto_fetch: false,
                    ack_received: false,
                    speculative_paths: 0,
                };

                op_manager.push(*id, OpEnum::Get(completed_op)).await?;
                return Ok(());
            }

            // No peers AND no local cache - error
            tracing::warn!(
                tx = %id,
                contract = %instance_id,
                phase = "error",
                "GET: Contract not found locally and no peers available"
            );

            // Emit failure telemetry so this case is visible in production metrics
            if let Some(event) = NetEventLog::get_failure(
                id,
                &op_manager.ring,
                *instance_id,
                OperationFailure::NoPeersAvailable,
                None,
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }

            return Err(RingError::EmptyRing.into());
        }

        // Peers available - query network, keep local as fallback
        if local_value.is_some() {
            tracing::debug!(
                tx = %id,
                %instance_id,
                peer_count = candidates.len(),
                "GET: Have local cache, querying {} peer(s) for fresh version",
                candidates.len()
            );
        } else {
            tracing::debug!(
                tx = %id,
                %instance_id,
                peer_count = candidates.len(),
                "GET: No local cache, querying {} peer(s)",
                candidates.len()
            );
        }

        (candidates, *id, *instance_id, *fetch_contract, local_value)
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    // Take the first candidate as the target
    let target = candidates.remove(0);
    tracing::debug!(
        tx = %id,
        target = %target,
        "Preparing get contract request",
    );

    match get_op.state {
        Some(GetState::PrepareRequest(data)) => {
            let (fetch_contract, subscribe, blocking_subscribe) =
                (data.fetch_contract, data.subscribe, data.blocking_subscribe);
            let mut tried_peers = HashSet::new();
            if let Some(addr) = target.socket_addr() {
                tried_peers.insert(addr);
            }

            let new_state = Some(GetState::AwaitingResponse(AwaitingResponseData {
                instance_id: instance_id_val,
                retries: 0,
                fetch_contract,
                requester: None,
                current_hop: op_manager.ring.max_hops_to_live,
                subscribe,
                blocking_subscribe,
                next_hop: target.clone(),
                tried_peers,
                alternatives: candidates,
                attempts_at_hop: 1,
                visited: visited.clone(),
            }));

            let msg = GetMsg::Request {
                id,
                instance_id: instance_id_val,
                fetch_contract,
                htl: op_manager.ring.max_hops_to_live,
                visited,
            };

            let op = GetOp {
                id,
                state: new_state,
                result: None,
                stats: get_op.stats.map(|mut s| {
                    s.next_peer = Some(target.clone());
                    s
                }),
                upstream_addr: get_op.upstream_addr,
                local_fallback, // Store local cache for fallback if network returns NotFound
                auto_fetch: get_op.auto_fetch,
                ack_received: false,
                speculative_paths: 0,
            };

            // Emit get_request telemetry when initiating a GET operation
            if let Some(event) = NetEventLog::get_request(
                &id,
                &op_manager.ring,
                instance_id_val,
                target,
                op_manager.ring.max_hops_to_live,
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }

            op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Get(op))
                .await?;
        }
        _ => return Err(OpError::invalid_transition(get_op.id)),
    }
    Ok(())
}

// ── Type-state data structs ──────────────────────────────────────────────
//
// Each state in the GET state machine is a named struct with typed
// transition methods.  This gives compile-time guarantees:
//
//   ReceivedRequest ── (no transition methods, next state depends on message)
//   PrepareRequest ── into_awaiting_response() ──► AwaitingResponse
//   AwaitingResponse ── into_finished()         ──► Finished
//                    ── with_retry_peer()        ──► AwaitingResponse (retry at same hop)
//                    ── with_next_hop()           ──► AwaitingResponse (advance to next hop)
//
// Invalid transitions (e.g. Finished → PrepareRequest) are unrepresentable
// because the target type simply has no such method.

/// Data for the PrepareRequest state: originator preparing GET request.
#[derive(Debug)]
struct PrepareRequestData {
    instance_id: ContractInstanceId,
    id: Transaction,
    fetch_contract: bool,
    subscribe: bool,
    blocking_subscribe: bool,
}

impl PrepareRequestData {
    /// Transition to AwaitingResponse after sending the GET request.
    ///
    /// Encodes valid transition: PrepareRequest → AwaitingResponse.
    /// Carries forward instance_id, fetch_contract, subscribe, blocking_subscribe.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern
    fn into_awaiting_response(
        self,
        requester: Option<PeerKeyLocation>,
        next_hop: PeerKeyLocation,
        alternatives: Vec<PeerKeyLocation>,
        visited: super::VisitedPeers,
    ) -> AwaitingResponseData {
        AwaitingResponseData {
            instance_id: self.instance_id,
            requester,
            fetch_contract: self.fetch_contract,
            retries: 0,
            current_hop: 0,
            subscribe: self.subscribe,
            blocking_subscribe: self.blocking_subscribe,
            next_hop,
            tried_peers: HashSet::new(),
            alternatives,
            attempts_at_hop: 0,
            visited,
        }
    }
}

/// Data for the AwaitingResponse state: request sent, waiting for downstream peer.
///
/// This is the most complex state, tracking retry logic across multiple hops.
#[derive(Debug)]
struct AwaitingResponseData {
    /// Contract being fetched (by instance_id since we may not have full key yet)
    instance_id: ContractInstanceId,
    /// If specified the peer waiting for the response upstream
    requester: Option<PeerKeyLocation>,
    fetch_contract: bool,
    retries: usize,
    current_hop: usize,
    subscribe: bool,
    blocking_subscribe: bool,
    /// Peer we are currently trying to reach.
    /// Note: With connection-based routing, this is only used for state tracking,
    /// not for response routing (which uses upstream_addr instead).
    #[allow(dead_code)]
    next_hop: PeerKeyLocation,
    /// Peers we've already tried at this hop level
    tried_peers: HashSet<std::net::SocketAddr>,
    /// Alternative peers we could still try at this hop
    alternatives: Vec<PeerKeyLocation>,
    /// How many peers we've tried at this hop
    attempts_at_hop: usize,
    /// Bloom filter tracking visited peers across all hops
    visited: super::VisitedPeers,
}

impl AwaitingResponseData {
    /// Transition to Finished on successful GET response.
    ///
    /// Encodes valid transition: AwaitingResponse → Finished.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern
    fn into_finished(self, key: ContractKey) -> FinishedData {
        FinishedData { key }
    }
}

/// Data for the Finished state: GET operation completed successfully.
#[derive(Debug, Clone, Copy)]
struct FinishedData {
    key: ContractKey,
}

// ── State enum (wraps the typed structs) ─────────────────────────────────

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum GetState {
    /// A new petition for a get op received from another peer.
    /// Note: We use GetOp::upstream_addr for response routing (not PeerKeyLocation).
    ReceivedRequest,
    /// Preparing request for get op.
    PrepareRequest(PrepareRequestData),
    /// Awaiting response from petition.
    AwaitingResponse(AwaitingResponseData),
    /// Operation completed successfully
    Finished(FinishedData),
}

impl Display for GetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetState::ReceivedRequest => write!(f, "ReceivedRequest"),
            GetState::PrepareRequest(data) => {
                write!(
                    f,
                    "PrepareRequest(instance_id: {}, id: {}, fetch_contract: {}, subscribe: {})",
                    data.instance_id, data.id, data.fetch_contract, data.subscribe
                )
            }
            GetState::AwaitingResponse(data) => {
                write!(
                    f,
                    "AwaitingResponse(requester: {:?}, fetch_contract: {}, retries: {}, current_hop: {}, subscribe: {})",
                    data.requester,
                    data.fetch_contract,
                    data.retries,
                    data.current_hop,
                    data.subscribe
                )
            }
            GetState::Finished(data) => write!(f, "Finished(key: {})", data.key),
        }
    }
}

struct GetStats {
    /// Next peer in get path to be targeted
    next_peer: Option<PeerKeyLocation>,
    contract_location: Location,
    /// (start, end)
    first_response_time: Option<(Instant, Option<Instant>)>,
    /// (start, end)
    transfer_time: Option<(Instant, Option<Instant>)>,
}

#[derive(Clone)]
pub(crate) struct GetResult {
    key: ContractKey,
    pub state: WrappedState,
    pub contract: Option<ContractContainer>,
}

impl TryFrom<GetOp> for GetResult {
    type Error = OpError;

    fn try_from(value: GetOp) -> Result<Self, Self::Error> {
        match value.result {
            Some(r) => Ok(r),
            _ => Err(OpError::UnexpectedOpState),
        }
    }
}

pub(crate) struct GetOp {
    pub id: Transaction,
    state: Option<GetState>,
    pub(super) result: Option<GetResult>,
    stats: Option<Box<GetStats>>,
    /// The address we received this operation's message from.
    /// Used for connection-based routing: responses are sent back to this address.
    upstream_addr: Option<std::net::SocketAddr>,
    /// Local cached state to fall back to if network query fails or returns NotFound.
    /// This enables "network first, local fallback" behavior to ensure we get fresh data
    /// while still being able to serve cached content when the network is unavailable.
    local_fallback: Option<(ContractKey, WrappedState, Option<ContractContainer>)>,
    /// True when this GET was spawned internally by try_auto_fetch_contract,
    /// not by a client request. Used to avoid sending spurious timeout errors
    /// to a non-existent client.
    auto_fetch: bool,
    /// True when a downstream relay has acknowledged forwarding this request.
    /// Used by the GC task to distinguish "peer is dead" from "peer is working on it".
    pub(crate) ack_received: bool,
    /// Number of speculative parallel paths launched by the originator's GC task.
    /// Capped at MAX_SPECULATIVE_PATHS to bound network overhead.
    pub(crate) speculative_paths: u8,
}

impl GetOp {
    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        if let Some((
            GetResult {
                state, contract, ..
            },
            GetStats {
                next_peer: Some(target_peer),
                contract_location,
                first_response_time: Some((response_start, Some(response_end))),
                transfer_time: Some((transfer_start, Some(transfer_end))),
                ..
            },
        )) = self.result.as_ref().zip(self.stats.as_deref())
        {
            let payload_size = state.size()
                + contract
                    .as_ref()
                    .map(|c| c.data().len())
                    .unwrap_or_default();
            OpOutcome::ContractOpSuccess {
                target_peer,
                contract_location: *contract_location,
                payload_size,
                first_response_time: *response_end - *response_start,
                payload_transfer_time: *transfer_end - *transfer_start,
            }
        } else if self.result.is_none() {
            // No result — if we have stats with a target peer, report as failure
            if let Some(GetStats {
                next_peer: Some(target_peer),
                contract_location,
                ..
            }) = self.stats.as_deref()
            {
                OpOutcome::ContractOpFailure {
                    target_peer,
                    contract_location: *contract_location,
                }
            } else {
                OpOutcome::Incomplete
            }
        } else if let Some(GetStats {
            next_peer: Some(target_peer),
            contract_location,
            ..
        }) = self.stats.as_deref()
        {
            // Result present but timing incomplete (e.g. streaming completed without
            // full handshake timing). Report as untimed success so the router still
            // gets feedback instead of silently dropping the outcome.
            OpOutcome::ContractOpSuccessUntimed {
                target_peer,
                contract_location: *contract_location,
            }
        } else {
            OpOutcome::Incomplete
        }
    }

    /// Extract the contract instance ID from the operation state.
    pub(crate) fn instance_id(&self) -> Option<ContractInstanceId> {
        match &self.state {
            Some(GetState::PrepareRequest(data)) => Some(data.instance_id),
            Some(GetState::AwaitingResponse(data)) => Some(data.instance_id),
            _ => None,
        }
    }

    /// Returns true if this GET was initiated by a local client (not forwarded from
    /// a peer and not a system-initiated auto-fetch).
    pub(crate) fn is_client_initiated(&self) -> bool {
        if self.auto_fetch {
            return false;
        }
        match &self.state {
            Some(GetState::PrepareRequest(_)) => true,
            Some(GetState::AwaitingResponse(data)) => data.requester.is_none(),
            Some(GetState::ReceivedRequest) | Some(GetState::Finished(_)) | None => false,
        }
    }

    /// Extract routing failure info for timeout reporting.
    /// Returns `(target_peer, contract_location)` if stats are available.
    pub(crate) fn failure_routing_info(&self) -> Option<(PeerKeyLocation, Location)> {
        self.stats.as_deref().and_then(|stats| {
            stats
                .next_peer
                .as_ref()
                .map(|peer| (peer.clone(), stats.contract_location))
        })
    }

    /// Handle aborted outbound connections by directly retrying with alternative peers.
    pub(crate) async fn handle_abort(mut self, op_manager: &OpManager) -> Result<(), OpError> {
        if let Some(GetState::AwaitingResponse(AwaitingResponseData {
            instance_id,
            requester,
            fetch_contract,
            retries,
            current_hop,
            subscribe,
            blocking_subscribe,
            next_hop: failed_peer,
            mut tried_peers,
            mut alternatives,
            attempts_at_hop,
            visited,
        })) = self.state.take()
        {
            // Mark the failed peer as tried
            if let Some(addr) = failed_peer.socket_addr() {
                tried_peers.insert(addr);
            }

            // Try the next alternative if available
            if !alternatives.is_empty() && attempts_at_hop < DEFAULT_MAX_BREADTH {
                let next_target = alternatives.remove(0);

                tracing::debug!(
                    tx = %self.id,
                    %instance_id,
                    next_target = %next_target,
                    "GET: Connection aborted, trying alternative peer"
                );

                if let Some(addr) = next_target.socket_addr() {
                    tried_peers.insert(addr);
                }

                let new_state = Some(GetState::AwaitingResponse(AwaitingResponseData {
                    instance_id,
                    requester,
                    fetch_contract,
                    retries,
                    current_hop,
                    subscribe,
                    blocking_subscribe,
                    next_hop: next_target,
                    tried_peers,
                    alternatives,
                    attempts_at_hop: attempts_at_hop + 1,
                    visited: visited.clone(),
                }));

                // Note: handle_abort uses current_hop directly (not the attempts-based
                // reduction from retry_with_next_alternative). Connection aborts are
                // immediate failures, not timeout-based retries, so they don't contribute
                // to retry storms the same way. The bloom filter and tried_peers already
                // prevent cycling through the same peers. (#3570)
                let msg = GetMsg::Request {
                    id: self.id,
                    instance_id,
                    fetch_contract,
                    htl: current_hop,
                    visited,
                };

                let op = GetOp {
                    id: self.id,
                    state: new_state,
                    result: None,
                    stats: self.stats,
                    upstream_addr: self.upstream_addr,
                    local_fallback: self.local_fallback,
                    auto_fetch: self.auto_fetch,
                    ack_received: false,
                    speculative_paths: 0,
                };

                op_manager
                    .notify_op_change(NetMessage::from(msg), OpEnum::Get(op))
                    .await?;
                return Err(OpError::StatePushed);
            }

            // No alternatives - look for new candidates
            if retries < MAX_RETRIES {
                let mut new_visited = visited.clone();
                for addr in &tried_peers {
                    new_visited.mark_visited(*addr);
                }

                let mut new_candidates = op_manager.ring.k_closest_potentially_hosting(
                    &instance_id,
                    &new_visited,
                    DEFAULT_MAX_BREADTH,
                );

                if !new_candidates.is_empty() {
                    let next_target = new_candidates.remove(0);

                    tracing::debug!(
                        tx = %self.id,
                        %instance_id,
                        next_target = %next_target,
                        "GET: Connection aborted, found new candidate"
                    );

                    let mut new_tried_peers = HashSet::new();
                    if let Some(addr) = next_target.socket_addr() {
                        new_tried_peers.insert(addr);
                    }

                    let new_state = Some(GetState::AwaitingResponse(AwaitingResponseData {
                        instance_id,
                        requester,
                        fetch_contract,
                        retries: retries + 1,
                        current_hop,
                        subscribe,
                        blocking_subscribe,
                        next_hop: next_target,
                        tried_peers: new_tried_peers,
                        alternatives: new_candidates,
                        attempts_at_hop: 1,
                        visited: new_visited.clone(),
                    }));

                    let msg = GetMsg::Request {
                        id: self.id,
                        instance_id,
                        fetch_contract,
                        htl: current_hop,
                        visited: new_visited,
                    };

                    let op = GetOp {
                        id: self.id,
                        state: new_state,
                        result: None,
                        stats: self.stats,
                        upstream_addr: self.upstream_addr,
                        local_fallback: self.local_fallback,
                        auto_fetch: self.auto_fetch,
                        ack_received: false,
                        speculative_paths: 0,
                    };

                    op_manager
                        .notify_op_change(NetMessage::from(msg), OpEnum::Get(op))
                        .await?;
                    return Err(OpError::StatePushed);
                }
            }

            // No alternatives and no new candidates - check for local fallback
            // If we have a local cache, use it instead of failing entirely
            if let Some((key, state, contract)) = self.local_fallback {
                tracing::info!(
                    tx = %self.id,
                    contract = %key,
                    phase = "complete",
                    "GET: Connection aborted, no peers available - returning local cache"
                );

                let completed_op = GetOp {
                    id: self.id,
                    state: Some(GetState::Finished(FinishedData { key })),
                    result: Some(GetResult {
                        key,
                        state,
                        contract,
                    }),
                    stats: self.stats,
                    upstream_addr: self.upstream_addr,
                    local_fallback: None,
                    auto_fetch: false,
                    ack_received: false,
                    speculative_paths: 0,
                };

                op_manager.push(self.id, OpEnum::Get(completed_op)).await?;
                return Ok(());
            }

            // No local fallback either - give up
            if let Some(upstream) = self.upstream_addr {
                // Send NotFound response back to upstream requester instead of letting them timeout
                tracing::warn!(
                    tx = %self.id,
                    %instance_id,
                    %upstream,
                    phase = "not_found",
                    "GET: Connection aborted, no peers available - sending NotFound to upstream"
                );
                // Emit telemetry: relay returning NotFound after abort
                let hop_count = Some(op_manager.ring.max_hops_to_live.saturating_sub(current_hop));
                if let Some(event) =
                    NetEventLog::get_not_found(&self.id, &op_manager.ring, instance_id, hop_count)
                {
                    op_manager.ring.register_events(Either::Left(event)).await;
                }

                let response_op = GetOp {
                    id: self.id,
                    state: None,
                    result: None,
                    stats: self.stats,
                    upstream_addr: self.upstream_addr,
                    local_fallback: None,
                    auto_fetch: false,
                    ack_received: false,
                    speculative_paths: 0,
                };

                op_manager
                    .notify_op_change(
                        NetMessage::from(GetMsg::Response {
                            id: self.id,
                            instance_id,
                            result: GetMsgResult::NotFound,
                        }),
                        OpEnum::Get(response_op),
                    )
                    .await?;
                return Err(OpError::StatePushed);
            } else {
                // Original requester - operation fails locally
                tracing::warn!(
                    tx = %self.id,
                    %instance_id,
                    phase = "not_found",
                    "GET: Connection aborted, no peers available - local operation fails"
                );

                // Emit failure event with hop_count
                let hop_count = Some(op_manager.ring.max_hops_to_live.saturating_sub(current_hop));
                if let Some(event) = NetEventLog::get_failure(
                    &self.id,
                    &op_manager.ring,
                    instance_id,
                    OperationFailure::NoPeersAvailable,
                    hop_count,
                ) {
                    op_manager.ring.register_events(Either::Left(event)).await;
                }

                let failed_op = GetOp {
                    id: self.id,
                    state: None,
                    result: None,
                    stats: self.stats,
                    upstream_addr: self.upstream_addr,
                    local_fallback: None,
                    auto_fetch: false,
                    ack_received: false,
                    speculative_paths: 0,
                };
                op_manager.push(self.id, OpEnum::Get(failed_op)).await?;
                return Ok(());
            }
        }

        // If we weren't awaiting a response, just put the op back.
        // No retry needed; another handler may pick it up later.
        op_manager.push(self.id, OpEnum::Get(self)).await?;
        Ok(())
    }

    pub(super) fn finalized(&self) -> bool {
        self.result.is_some() && matches!(self.state, Some(GetState::Finished(_)))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        match &self.result {
            Some(GetResult {
                key,
                state,
                contract,
            }) => Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::GetResponse {
                    key: *key,
                    contract: contract.clone(),
                    state: state.clone(),
                },
            )),
            None => Err(ErrorKind::OperationError {
                cause: "get didn't finish successfully".into(),
            }
            .into()),
        }
    }

    /// Get the next hop address if this operation is in a state that needs to send
    /// an outbound message. Used for hop-by-hop routing.
    pub(crate) fn get_next_hop_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.state {
            Some(GetState::AwaitingResponse(data)) => data.next_hop.socket_addr(),
            _ => None,
        }
    }

    /// Get the current hop count (remaining HTL) for this operation.
    /// Returns None if the operation is not in AwaitingResponse state.
    pub(crate) fn get_current_hop(&self) -> Option<usize> {
        match &self.state {
            Some(GetState::AwaitingResponse(data)) => Some(data.current_hop),
            _ => None,
        }
    }

    /// Try to retry this GET operation by sending to the next alternative peer.
    /// If local alternatives are exhausted, `fallback_peers` provides additional
    /// candidates (e.g., all connected peers for DBF search).
    /// Returns `Ok((new_op, msg))` if a peer was available,
    /// `Err(self)` if no peers remain (caller gets ownership back).
    pub(crate) fn retry_with_next_alternative(
        mut self,
        max_hops_to_live: usize,
        fallback_peers: &[PeerKeyLocation],
    ) -> Result<(GetOp, GetMsg), Box<GetOp>> {
        let state = match self.state.take() {
            Some(s) => s,
            None => return Err(Box::new(self)),
        };
        match state {
            GetState::AwaitingResponse(mut data) => {
                // If local alternatives exhausted, inject fallback peers we haven't tried.
                // Filter through BOTH tried_peers (this hop) AND visited bloom filter
                // (all hops) to avoid retry storms cycling through the same peers (#3570).
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
                            "GET broadening search to all connected peers (DBF fallback)"
                        );
                    }
                }

                if data.alternatives.is_empty() {
                    self.state = Some(GetState::AwaitingResponse(data));
                    return Err(Box::new(self));
                }

                let next_target = data.alternatives.remove(0);
                let instance_id = data.instance_id;
                let fetch_contract = data.fetch_contract;
                if let Some(addr) = next_target.socket_addr() {
                    data.tried_peers.insert(addr);
                    // Mark in bloom filter so downstream peers won't forward back,
                    // and future retries won't select this peer again (#3570).
                    data.visited.mark_visited(addr);
                }
                tracing::info!(
                    tx = %self.id,
                    contract = %instance_id,
                    target = %next_target,
                    remaining_alternatives = data.alternatives.len(),
                    "GET retrying with alternative peer after timeout"
                );
                // Update stats to point to the new target so timeouts/failures
                // are reported against the correct peer (#3527).
                if let Some(ref mut s) = self.stats {
                    s.next_peer = Some(next_target.clone());
                }
                data.next_hop = next_target;
                data.attempts_at_hop += 1;
                let visited = data.visited.clone();

                // Reduce HTL on each retry to avoid full-depth traversal storms (#3570).
                // Halve the HTL for each retry attempt, floored at MIN_RETRY_HTL.
                // This limits the blast radius of retries while still allowing the
                // request to reach nearby contract holders.
                let retry_htl = (max_hops_to_live / (data.attempts_at_hop.max(1)))
                    .max(MIN_RETRY_HTL)
                    .min(max_hops_to_live);

                self.state = Some(GetState::AwaitingResponse(data));

                let msg = GetMsg::Request {
                    id: self.id,
                    instance_id,
                    fetch_contract,
                    htl: retry_htl,
                    visited,
                };

                Ok((self, msg))
            }
            other @ GetState::ReceivedRequest
            | other @ GetState::PrepareRequest(_)
            | other @ GetState::Finished(_) => {
                self.state = Some(other);
                Err(Box::new(self))
            }
        }
    }
}

impl Operation for GetOp {
    type Message = GetMsg;
    type Result = GetResult;

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Result<OpInitialization<Self>, OpError> {
        let tx = *msg.id();
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Get(get_op))) => {
                Ok(OpInitialization {
                    op: get_op,
                    source_addr,
                })
                // was an existing operation, other peer messaged back
            }
            Ok(Some(op)) => {
                if let Err(e) = op_manager.push(tx, op).await {
                    tracing::warn!(tx = %tx, error = %e, "failed to push mismatched op back");
                }
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                // Check if this is a response message - if so, the operation was likely
                // cleaned up due to timeout and we should not create a new operation
                if matches!(
                    msg,
                    GetMsg::Response { .. }
                        | GetMsg::ResponseStreaming { .. }
                        | GetMsg::ForwardingAck { .. }
                ) {
                    tracing::debug!(
                        tx = %tx,
                        phase = "load_or_init",
                        "GET response/ack arrived for non-existent operation (likely timed out)"
                    );
                    return Err(OpError::OpNotPresent(tx));
                }

                // new request to get a value for a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(GetState::ReceivedRequest),
                        id: tx,
                        result: None,
                        stats: None, // don't care about stats in target peers
                        upstream_addr: source_addr, // Connection-based routing: store who sent us this request
                        local_fallback: None,       // Remote requests don't have local fallback
                        auto_fetch: false,
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
            // Take fields early since self will be partially moved
            let mut local_fallback = self.local_fallback;
            let auto_fetch = self.auto_fetch;
            #[allow(unused_assignments)]
            let mut return_msg = None;
            #[allow(unused_assignments)]
            let mut new_state = None;
            let mut result = None;
            let mut stats = self.stats;
            let mut stream_data: Option<(StreamId, bytes::Bytes)> = None;

            // Look up sender's PeerKeyLocation from source address for logging/routing
            // This replaces the sender field that was previously embedded in messages
            let sender_from_addr = source_addr.and_then(|addr| {
                op_manager
                    .ring
                    .connection_manager
                    .get_peer_location_by_addr(addr)
            });

            match input {
                GetMsg::Request {
                    instance_id,
                    id,
                    fetch_contract,
                    htl,
                    visited,
                } => {
                    // Handle GET request - either initial or forwarded
                    let ring_max_htl = op_manager.ring.max_hops_to_live.max(1);
                    let htl = (*htl).min(ring_max_htl);
                    let id = *id;
                    let instance_id = *instance_id;
                    let fetch_contract = *fetch_contract;

                    // Use sender_from_addr for logging (falls back to source_addr if lookup fails)
                    let sender_display = sender_from_addr
                        .as_ref()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| {
                            source_addr
                                .map(|a| a.to_string())
                                .unwrap_or_else(|| "unknown".to_string())
                        });
                    tracing::info!(
                        tx = %id,
                        %instance_id,
                        peer_addr = %sender_display,
                        fetch_contract,
                        htl,
                        skip = ?visited,
                        phase = "request",
                        "GET Request received"
                    );

                    // Use sender_from_addr if available, but allow processing without it.
                    // The sender may not be in our ring if this is a transient connection
                    // or in simulation where direct sends bypass connection establishment.
                    // Routing uses upstream_addr (stored from source_addr), not sender.
                    let sender = sender_from_addr.clone();

                    // Check if operation is already completed
                    if matches!(self.state, Some(GetState::Finished(_))) {
                        tracing::debug!(
                            tx = %id,
                            "GET: Request received for already completed operation, ignoring duplicate request"
                        );
                        // Return the operation in its current state
                        new_state = self.state;
                        return_msg = None;
                        result = self.result;
                    } else if htl == 0 {
                        // HTL exhausted - send NotFound response
                        tracing::warn!(
                            tx = %id,
                            %instance_id,
                            peer_addr = %sender_display,
                            htl = 0,
                            phase = "not_found",
                            "GET Request exhausted HTL - sending NotFound response"
                        );
                        // Emit telemetry: relay returning NotFound due to HTL exhaustion
                        if let Some(event) = NetEventLog::get_not_found(
                            &id,
                            &op_manager.ring,
                            instance_id,
                            Some(op_manager.ring.max_hops_to_live.saturating_sub(htl)),
                        ) {
                            op_manager.ring.register_events(Either::Left(event)).await;
                        }
                        return build_op_result(GetOpResult {
                            id,
                            state: None,
                            msg: Some(GetMsg::Response {
                                id,
                                instance_id,
                                result: GetMsgResult::NotFound,
                            }),
                            result: None,
                            stats,
                            upstream_addr: self.upstream_addr,
                            stream_data: None,
                            local_fallback: None,
                            auto_fetch: false,
                        });
                    } else {
                        // Normal case: operation should be in ReceivedRequest or AwaitingResponse state
                        debug_assert!(matches!(
                            self.state,
                            Some(GetState::ReceivedRequest) | Some(GetState::AwaitingResponse(_))
                        ));
                        tracing::debug!(
                            tx = %id,
                            %instance_id,
                            htl,
                            "GET: Request processing in state {:?}",
                            self.state
                        );

                        // Initialize/update stats for tracking the operation
                        let this_peer = op_manager.ring.connection_manager.own_location();
                        if stats.is_none() {
                            stats = Some(Box::new(GetStats {
                                contract_location: Location::from(&instance_id),
                                next_peer: Some(this_peer.clone()),
                                transfer_time: None,
                                first_response_time: None,
                            }));
                        } else if let Some(s) = stats.as_mut() {
                            s.next_peer = Some(this_peer.clone());
                        }

                        // Update skip list with current peer address and sender address
                        // Restore hash keys after deserialization (they're derived from tx)
                        let mut new_visited = visited.clone().with_transaction(&id);
                        if let Some(addr) = this_peer.socket_addr() {
                            new_visited.mark_visited(addr);
                        }
                        // Also mark the sender to prevent routing back to them
                        // This is critical for preventing request cycles, especially when
                        // a node has limited connections (e.g., only connected to gateway)
                        if let Some(addr) = source_addr {
                            new_visited.mark_visited(addr);
                        }

                        // First check if we have the contract locally before forwarding
                        let get_result = op_manager
                            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                                instance_id,
                                return_contract_code: fetch_contract,
                            })
                            .await;

                        // GetResponse now returns key: Option<ContractKey>
                        let local_value = match get_result {
                            Ok(ContractHandlerEvent::GetResponse {
                                key: Some(key),
                                response:
                                    Ok(StoreResponse {
                                        state: Some(state),
                                        contract,
                                    }),
                            }) => {
                                if fetch_contract && contract.is_none() {
                                    tracing::info!(
                                        tx = %id,
                                        %instance_id,
                                        %this_peer,
                                        fetch_contract,
                                        "GET: state available locally but contract code missing; forwarding GET"
                                    );
                                    None
                                } else {
                                    Some((key, state, contract))
                                }
                            }
                            _ => None,
                        };

                        // Relay peers in ReceivedRequest: prefer fresh network state,
                        // local cache is fallback only. Store local value and forward.
                        let local_value = if self.upstream_addr.is_some()
                            && matches!(self.state, Some(GetState::ReceivedRequest))
                        {
                            if let Some(lv) = local_value {
                                tracing::debug!(
                                    tx = %id,
                                    "Relay peer deferring local cache, forwarding GET for fresh state"
                                );
                                local_fallback = Some(lv);
                            }
                            None
                        } else {
                            local_value
                        };

                        if let Some((key, state, contract)) = local_value {
                            // Contract found locally!
                            tracing::info!(
                                tx = %id,
                                contract = %key,
                                fetch_contract,
                                phase = "complete",
                                "GET: contract found locally"
                            );

                            // Register the GET requester's interest in this contract so that
                            // update broadcasts include them as a target. This is critical for
                            // partitioned topologies: the requester's auto-subscribe may route
                            // to a blocked peer instead of back to us, so we register interest
                            // proactively when serving the GET response.
                            if let Some(upstream_addr) = self.upstream_addr {
                                if let Some(pkl) = op_manager
                                    .ring
                                    .connection_manager
                                    .get_peer_by_addr(upstream_addr)
                                {
                                    let peer_key =
                                        crate::ring::interest::PeerKey::from(pkl.pub_key.clone());
                                    op_manager
                                        .interest_manager
                                        .register_peer_interest(&key, peer_key, None, false);
                                    tracing::debug!(
                                        tx = %id,
                                        contract = %key,
                                        requester = %upstream_addr,
                                        "Registered GET requester interest for update broadcasts"
                                    );
                                }
                            }

                            // Note: ResponseSent telemetry is emitted by from_outbound_msg()
                            // when the message is actually sent, avoiding duplicate events.

                            // Check if this is a forwarded request or a local request
                            // Use upstream_addr (the actual socket address) not requester (PeerKeyLocation lookup)
                            // because get_peer_location_by_addr() can fail for transient connections
                            match &self.state {
                                Some(GetState::ReceivedRequest) if self.upstream_addr.is_some() => {
                                    // This is a forwarded request - send result back to upstream
                                    tracing::debug!(tx = %id, "Returning contract {} to upstream", key);
                                    new_state = None;
                                    let store_response = StoreResponse {
                                        state: Some(state),
                                        contract,
                                    };
                                    let includes_contract = store_response.contract.is_some();
                                    let payload = GetStreamingPayload {
                                        key,
                                        value: store_response,
                                    };
                                    let payload_bytes =
                                        bincode::serialize(&payload).map_err(|e| {
                                            OpError::NotificationChannelError(format!(
                                                "Failed to serialize streaming payload: {e}"
                                            ))
                                        })?;
                                    let payload_size = payload_bytes.len();
                                    if should_use_streaming(
                                        op_manager.streaming_threshold,
                                        payload_size,
                                    ) {
                                        let sid = StreamId::next_operations();
                                        tracing::info!(
                                            tx = %id,
                                            stream_id = %sid,
                                            payload_size,
                                            "GET response using operations-level streaming"
                                        );
                                        return_msg = Some(GetMsg::ResponseStreaming {
                                            id,
                                            instance_id: *payload.key.id(),
                                            stream_id: sid,
                                            key: payload.key,
                                            total_size: payload_size as u64,
                                            includes_contract,
                                        });
                                        stream_data =
                                            Some((sid, bytes::Bytes::from(payload_bytes)));
                                    } else {
                                        return_msg = Some(GetMsg::Response {
                                            id,
                                            instance_id: *payload.key.id(),
                                            result: GetMsgResult::Found {
                                                key: payload.key,
                                                value: payload.value,
                                            },
                                        });
                                    }
                                }
                                Some(GetState::AwaitingResponse(_))
                                    if self.upstream_addr.is_some() =>
                                {
                                    // Forward contract to upstream
                                    new_state = None;
                                    tracing::debug!(tx = %id, "Returning contract {} to upstream", key);
                                    let store_response = StoreResponse {
                                        state: Some(state),
                                        contract,
                                    };
                                    let includes_contract = store_response.contract.is_some();
                                    let payload = GetStreamingPayload {
                                        key,
                                        value: store_response,
                                    };
                                    let payload_bytes =
                                        bincode::serialize(&payload).map_err(|e| {
                                            OpError::NotificationChannelError(format!(
                                                "Failed to serialize streaming payload: {e}"
                                            ))
                                        })?;
                                    let payload_size = payload_bytes.len();
                                    if should_use_streaming(
                                        op_manager.streaming_threshold,
                                        payload_size,
                                    ) {
                                        let sid = StreamId::next_operations();
                                        tracing::info!(
                                            tx = %id,
                                            stream_id = %sid,
                                            payload_size,
                                            "GET response using operations-level streaming"
                                        );
                                        return_msg = Some(GetMsg::ResponseStreaming {
                                            id,
                                            instance_id: *payload.key.id(),
                                            stream_id: sid,
                                            key: payload.key,
                                            total_size: payload_size as u64,
                                            includes_contract,
                                        });
                                        stream_data =
                                            Some((sid, bytes::Bytes::from(payload_bytes)));
                                    } else {
                                        return_msg = Some(GetMsg::Response {
                                            id,
                                            instance_id: *payload.key.id(),
                                            result: GetMsgResult::Found {
                                                key: payload.key,
                                                value: payload.value,
                                            },
                                        });
                                    }
                                }
                                Some(GetState::AwaitingResponse(AwaitingResponseData {
                                    requester: None,
                                    ..
                                })) => {
                                    // Operation completed for original requester
                                    tracing::debug!(
                                        tx = %id,
                                        "Completed operation, get response received for contract {key}"
                                    );
                                    new_state = Some(GetState::Finished(FinishedData { key }));
                                    return_msg = None;
                                    result = Some(GetResult {
                                        key,
                                        state,
                                        contract,
                                    });
                                }
                                _ => {
                                    // This is the original requester (locally initiated request)
                                    new_state = Some(GetState::Finished(FinishedData { key }));
                                    return_msg = None;
                                    result = Some(GetResult {
                                        key,
                                        state,
                                        contract,
                                    });
                                }
                            }
                        } else {
                            // Contract not found locally (or missing code), proceed with forwarding
                            tracing::debug!(
                                tx = %id,
                                %instance_id,
                                %this_peer,
                                sender = ?sender,
                                "Contract not found @ peer, forwarding to other peers"
                            );

                            // Send ForwardingAck to upstream peer before forwarding.
                            // This tells the upstream "I'm working on it" so the GC task
                            // can distinguish dead peers from slow multi-hop chains.
                            if let Some(upstream) = self.upstream_addr {
                                let ack =
                                    NetMessage::from(GetMsg::ForwardingAck { id, instance_id });
                                drop(conn_manager.send(upstream, ack).await);

                                // Emit telemetry for ForwardingAck sent (#3570 diagnostics)
                                let upstream_peer = op_manager
                                    .ring
                                    .connection_manager
                                    .get_peer_by_addr(upstream)
                                    .unwrap_or_else(|| {
                                        op_manager.ring.connection_manager.own_location()
                                    });
                                if let Some(event) = NetEventLog::get_forwarding_ack_sent(
                                    &id,
                                    &op_manager.ring,
                                    instance_id,
                                    upstream_peer,
                                ) {
                                    op_manager.ring.register_events(Either::Left(event)).await;
                                }
                            }

                            // Forward using standard routing helper
                            // Note: target is determined by routing, sender from source_addr
                            return try_forward_or_return(
                                id,
                                instance_id,
                                (htl, fetch_contract),
                                (this_peer, sender),
                                new_visited,
                                op_manager,
                                stats,
                                self.upstream_addr,
                                local_fallback,
                            )
                            .await;
                        }
                    }
                }
                GetMsg::Response {
                    id,
                    instance_id,
                    result: GetMsgResult::NotFound,
                } => {
                    let id = *id;
                    let instance_id = *instance_id;

                    // Use sender_from_addr for logging (optional - sender may not be in ring)
                    let sender = sender_from_addr.clone();

                    tracing::info!(
                        tx = %id,
                        %instance_id,
                        sender = ?sender,
                        phase = "not_found",
                        "GET NotFound response received"
                    );
                    // Handle case where contract was not found
                    let this_peer = op_manager.ring.connection_manager.own_location();
                    tracing::warn!(
                        tx = %id,
                        %instance_id,
                        peer_addr = %this_peer,
                        sender = ?sender,
                        phase = "retry",
                        "Contract not found at peer, retrying with other peers"
                    );

                    match self.state {
                        Some(GetState::AwaitingResponse(AwaitingResponseData {
                            fetch_contract,
                            retries,
                            requester,
                            current_hop,
                            subscribe,
                            blocking_subscribe,
                            mut tried_peers,
                            mut alternatives,
                            attempts_at_hop,
                            next_hop: _,
                            visited,
                            instance_id: state_instance_id,
                            ..
                        })) => {
                            // Use instance_id from state for consistency
                            let instance_id = state_instance_id;

                            // Add the failed peer to tried list
                            if let Some(addr) = sender.as_ref().and_then(|s| s.socket_addr()) {
                                tried_peers.insert(addr);
                            }

                            // First, check if we have alternatives at this hop level
                            if !alternatives.is_empty() && attempts_at_hop < DEFAULT_MAX_BREADTH {
                                // Try the next alternative
                                let next_target = alternatives.remove(0);

                                tracing::info!(
                                    tx = %id,
                                    %instance_id,
                                    peer_addr = %next_target,
                                    fetch_contract,
                                    attempts_at_hop = attempts_at_hop + 1,
                                    max_attempts = DEFAULT_MAX_BREADTH,
                                    tried = ?tried_peers,
                                    remaining_alternatives = ?alternatives,
                                    phase = "retry",
                                    "Trying alternative peer at same hop level"
                                );

                                return_msg = Some(GetMsg::Request {
                                    id,
                                    instance_id,
                                    fetch_contract,
                                    htl: current_hop,
                                    visited: visited.clone(),
                                });

                                // Update state with the new alternative being tried
                                if let Some(addr) = next_target.socket_addr() {
                                    tried_peers.insert(addr);
                                }
                                let updated_tried_peers = tried_peers.clone();
                                new_state =
                                    Some(GetState::AwaitingResponse(AwaitingResponseData {
                                        retries,
                                        fetch_contract,
                                        requester: requester.clone(),
                                        current_hop,
                                        subscribe,
                                        blocking_subscribe,
                                        tried_peers: updated_tried_peers.clone(),
                                        alternatives,
                                        attempts_at_hop: attempts_at_hop + 1,
                                        instance_id,
                                        next_hop: next_target,
                                        // Preserve the accumulated visited so future candidate
                                        // selection still avoids already-specified peers; tried_peers
                                        // tracks attempts at this hop.
                                        visited: visited.clone(),
                                    }));
                            } else if retries < MAX_RETRIES {
                                // No more alternatives at this hop, try finding new peers
                                let mut new_visited = visited.clone();
                                for addr in &tried_peers {
                                    new_visited.mark_visited(*addr);
                                }

                                // Get new candidates excluding all tried peers
                                let mut new_candidates =
                                    op_manager.ring.k_closest_potentially_hosting(
                                        &instance_id,
                                        &new_visited,
                                        DEFAULT_MAX_BREADTH,
                                    );

                                tracing::info!(
                                    tx = %id,
                                    %instance_id,
                                    new_candidates = ?new_candidates,
                                    skip = ?new_visited,
                                    htl = current_hop,
                                    retries = retries + 1,
                                    phase = "retry",
                                    "GET seeking new candidates after exhausted alternatives"
                                );

                                if !new_candidates.is_empty() {
                                    // Try with the best new peer
                                    let target = new_candidates.remove(0);
                                    return_msg = Some(GetMsg::Request {
                                        id,
                                        instance_id,
                                        fetch_contract,
                                        htl: current_hop,
                                        visited: new_visited.clone(),
                                    });

                                    // Reset for new round of attempts
                                    let mut new_tried_peers = HashSet::new();
                                    if let Some(addr) = target.socket_addr() {
                                        new_tried_peers.insert(addr);
                                    }

                                    new_state =
                                        Some(GetState::AwaitingResponse(AwaitingResponseData {
                                            retries: retries + 1,
                                            fetch_contract,
                                            requester: requester.clone(),
                                            current_hop,
                                            subscribe,
                                            blocking_subscribe,
                                            tried_peers: new_tried_peers,
                                            alternatives: new_candidates,
                                            attempts_at_hop: 1,
                                            instance_id,
                                            next_hop: target,
                                            visited: new_visited.clone(),
                                        }));
                                } else if let Some(requester_peer) = requester.clone() {
                                    // No more peers to try — serve local fallback if available
                                    if let Some((key, state, contract)) = local_fallback.take() {
                                        tracing::info!(
                                            tx = %id,
                                            contract = %key,
                                            "Relay serving local cache as fallback (network returned NotFound)"
                                        );
                                        return_msg = Some(build_fallback_found_response(
                                            id,
                                            instance_id,
                                            key,
                                            state,
                                            contract,
                                        ));
                                    } else {
                                        tracing::warn!(
                                            tx = %id,
                                            %instance_id,
                                            peer_addr = %this_peer,
                                            target = %requester_peer,
                                            tried = ?tried_peers,
                                            skip = ?new_visited,
                                            phase = "not_found",
                                            "No other peers found while trying to get the contract, returning NotFound to requester"
                                        );
                                        return_msg = Some(GetMsg::Response {
                                            id,
                                            instance_id,
                                            result: GetMsgResult::NotFound,
                                        });
                                    }
                                } else {
                                    // Original requester - check for local fallback before failing
                                    if let Some((key, state, contract)) = local_fallback.take() {
                                        tracing::info!(
                                            tx = %id,
                                            contract = %key,
                                            phase = "complete",
                                            "GET: Network returned NotFound, returning local cache and re-hosting to network"
                                        );

                                        // Complete with local cached version
                                        new_state = Some(GetState::Finished(FinishedData { key }));
                                        return_msg = None;
                                        result = Some(GetResult {
                                            key,
                                            state: state.clone(),
                                            contract: contract.clone(),
                                        });

                                        // Re-host to the network with our local copy
                                        if let Some(contract_code) = contract {
                                            tracing::info!(
                                                tx = %id,
                                                %key,
                                                "Re-publishing to network after NotFound response"
                                            );
                                            let put_result = op_manager
                                                .notify_contract_handler(
                                                    ContractHandlerEvent::PutQuery {
                                                        key,
                                                        state,
                                                        related_contracts:
                                                            RelatedContracts::default(),
                                                        contract: Some(contract_code),
                                                    },
                                                )
                                                .await;
                                            match put_result {
                                                Ok(ContractHandlerEvent::PutResponse {
                                                    new_value: Ok(_),
                                                    ..
                                                }) => {
                                                    tracing::debug!(tx = %id, %key, "Re-hosted contract to network");
                                                    super::announce_contract_hosted(
                                                        op_manager, &key,
                                                    )
                                                    .await;
                                                }
                                                _ => {
                                                    tracing::warn!(tx = %id, %key, "Failed to re-host contract");
                                                }
                                            }
                                        }
                                    } else {
                                        // No local fallback - operation failed
                                        tracing::error!(
                                            tx = %id,
                                            %instance_id,
                                            tried = ?tried_peers,
                                            skip = ?visited,
                                            phase = "not_found",
                                            "Failed getting contract, not found after max retries"
                                        );

                                        // Emit get_not_found telemetry
                                        let hop_count = Some(
                                            op_manager
                                                .ring
                                                .max_hops_to_live
                                                .saturating_sub(current_hop),
                                        );
                                        if let Some(event) = NetEventLog::get_not_found(
                                            &id,
                                            &op_manager.ring,
                                            instance_id,
                                            hop_count,
                                        ) {
                                            op_manager
                                                .ring
                                                .register_events(Either::Left(event))
                                                .await;
                                        }

                                        notify_get_failed(
                                            op_manager,
                                            id,
                                            &format!(
                                                "contract {instance_id} not found after exhausting all peers"
                                            ),
                                        );
                                        return_msg = None;
                                        new_state = None;
                                    }
                                }
                            } else {
                                // Max retries reached
                                tracing::error!(
                                    tx = %id,
                                    %instance_id,
                                    phase = "not_found",
                                    "Failed getting contract, reached max retries"
                                );

                                if let Some(requester_peer) = requester.clone() {
                                    // No more peers to try — serve local fallback if available
                                    if let Some((key, state, contract)) = local_fallback.take() {
                                        tracing::info!(
                                            tx = %id,
                                            contract = %key,
                                            "Relay serving local cache as fallback (max retries, network returned NotFound)"
                                        );
                                        return_msg = Some(build_fallback_found_response(
                                            id,
                                            instance_id,
                                            key,
                                            state,
                                            contract,
                                        ));
                                    } else {
                                        tracing::warn!(
                                            tx = %id,
                                            %instance_id,
                                            peer_addr = %this_peer,
                                            target = %requester_peer,
                                            tried = ?tried_peers,
                                            skip = ?visited,
                                            phase = "not_found",
                                            "No other peers found, returning NotFound to requester"
                                        );
                                        return_msg = Some(GetMsg::Response {
                                            id,
                                            instance_id,
                                            result: GetMsgResult::NotFound,
                                        });
                                    }
                                    new_state = None;
                                } else {
                                    // Original requester - check for local fallback before failing
                                    if let Some((key, state, contract)) = local_fallback.take() {
                                        tracing::info!(
                                            tx = %id,
                                            contract = %key,
                                            phase = "complete",
                                            "GET: Max retries reached, returning local cache and re-hosting to network"
                                        );

                                        // Complete with local cached version
                                        new_state = Some(GetState::Finished(FinishedData { key }));
                                        return_msg = None;
                                        result = Some(GetResult {
                                            key,
                                            state: state.clone(),
                                            contract: contract.clone(),
                                        });

                                        // Re-host to the network with our local copy
                                        if let Some(contract_code) = contract {
                                            tracing::info!(
                                                tx = %id,
                                                %key,
                                                "Re-publishing to network after NotFound (max retries)"
                                            );
                                            let put_result = op_manager
                                                .notify_contract_handler(
                                                    ContractHandlerEvent::PutQuery {
                                                        key,
                                                        state,
                                                        related_contracts:
                                                            RelatedContracts::default(),
                                                        contract: Some(contract_code),
                                                    },
                                                )
                                                .await;
                                            match put_result {
                                                Ok(ContractHandlerEvent::PutResponse {
                                                    new_value: Ok(_),
                                                    ..
                                                }) => {
                                                    tracing::debug!(tx = %id, %key, "Re-hosted contract to network");
                                                    super::announce_contract_hosted(
                                                        op_manager, &key,
                                                    )
                                                    .await;
                                                }
                                                _ => {
                                                    tracing::warn!(tx = %id, %key, "Failed to re-host contract");
                                                }
                                            }
                                        }
                                    } else {
                                        // No local fallback - operation failed
                                        tracing::error!(
                                            tx = %id,
                                            %instance_id,
                                            phase = "not_found",
                                            "Failed getting contract, reached max retries"
                                        );

                                        // Emit get_not_found telemetry
                                        let hop_count = Some(
                                            op_manager
                                                .ring
                                                .max_hops_to_live
                                                .saturating_sub(current_hop),
                                        );
                                        if let Some(event) = NetEventLog::get_not_found(
                                            &id,
                                            &op_manager.ring,
                                            instance_id,
                                            hop_count,
                                        ) {
                                            op_manager
                                                .ring
                                                .register_events(Either::Left(event))
                                                .await;
                                        }

                                        notify_get_failed(
                                            op_manager,
                                            id,
                                            &format!(
                                                "contract {instance_id} not found after max retries"
                                            ),
                                        );
                                        return_msg = None;
                                        new_state = None;
                                    }
                                }
                            }
                        }
                        Some(GetState::ReceivedRequest) => {
                            // Return NotFound to sender
                            tracing::debug!(tx = %id, %instance_id, sender = ?sender, "Returning NotFound to upstream");
                            new_state = None;
                            return_msg = Some(GetMsg::Response {
                                id,
                                instance_id,
                                result: GetMsgResult::NotFound,
                            });
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    };
                }
                GetMsg::Response {
                    id,
                    result:
                        GetMsgResult::Found {
                            key,
                            value:
                                StoreResponse {
                                    state: Some(value),
                                    contract,
                                },
                        },
                    ..
                } => {
                    let id = *id;
                    let key = *key;

                    // Use sender_from_addr for logging (optional - sender may not be in ring)
                    let sender = sender_from_addr.clone();

                    tracing::info!(tx = %id, contract = %key, sender = ?sender, state = ?self.state, phase = "response", "GET Response received with state");

                    // Check if contract is required
                    let require_contract = matches!(
                        self.state,
                        Some(GetState::AwaitingResponse(AwaitingResponseData {
                            fetch_contract: true,
                            ..
                        }))
                    );

                    // Get requester and current_hop from current state
                    let (requester, current_hop) =
                        if let Some(GetState::AwaitingResponse(data)) = self.state.as_ref() {
                            (data.requester.clone(), Some(data.current_hop))
                        } else {
                            return Err(OpError::UnexpectedOpState);
                        };

                    // Handle case where contract is required but not provided
                    if require_contract && contract.is_none() {
                        if let Some(_requester) = requester {
                            // no contract, consider this like an error ignoring the incoming update value
                            tracing::warn!(
                                tx = %id,
                                sender = ?sender,
                                "Contract not received from peer while required"
                            );

                            tracing::warn!(
                                tx = %id,
                                %key,
                                sender = ?sender,
                                "Contract not received while required, returning response to upstream",
                            );

                            // Forward NotFound to requester (contract was required but not provided)
                            // local_fallback is only for original requester, not for forwarding
                            op_manager
                                .notify_op_change(
                                    NetMessage::from(GetMsg::Response {
                                        id,
                                        instance_id: *key.id(),
                                        result: GetMsgResult::NotFound,
                                    }),
                                    OpEnum::Get(GetOp {
                                        id,
                                        state: self.state,
                                        result: None,
                                        stats,
                                        upstream_addr: self.upstream_addr,
                                        local_fallback: None,
                                        auto_fetch: false,
                                        ack_received: false,
                                        speculative_paths: 0,
                                    }),
                                )
                                .await?;
                            return Err(OpError::StatePushed);
                        }
                    }

                    // Check if this is the original requester (no upstream to forward to)
                    let is_original_requester = self.upstream_addr.is_none();

                    // Check if subscription was requested
                    let (subscribe_requested, blocking_sub) =
                        if let Some(GetState::AwaitingResponse(data)) = &self.state {
                            (data.subscribe, data.blocking_subscribe)
                        } else {
                            (false, false)
                        };

                    // Always cache contracts we encounter - LRU will handle eviction
                    let should_put = true;

                    // Put contract locally if needed
                    if should_put {
                        // First check if the local state matches the incoming state
                        // to avoid triggering validation errors in contracts that implement
                        // idempotency checks in their update_state() method (issue #2018)
                        let local_state = op_manager
                            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                                instance_id: *key.id(),
                                return_contract_code: false,
                            })
                            .await;

                        let state_matches = match local_state {
                            Ok(ContractHandlerEvent::GetResponse {
                                response:
                                    Ok(StoreResponse {
                                        state: Some(local), ..
                                    }),
                                ..
                            }) => {
                                // Compare the actual state bytes
                                local.as_ref() == value.as_ref()
                            }
                            _ => false, // No local state or error - we should try to cache
                        };

                        if state_matches {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "Local state matches network state, skipping redundant cache"
                            );

                            // BUG FIX (2026-01): ALWAYS refresh hosting status on GET.
                            // Previously, we only recorded access if !is_hosting_contract(),
                            // which meant re-GETs on cached contracts wouldn't refresh the
                            // hosting cache's TTL/LRU position, causing subscriptions to expire.
                            //
                            // record_get_access now returns is_new atomically, eliminating the
                            // TOCTOU race that existed when checking is_hosting_contract() first.
                            let access_result =
                                op_manager.ring.record_get_access(key, value.size() as u64);

                            // Clean up interest tracking for evicted contracts (always, even if already hosting)
                            let mut removed_contracts = Vec::new();
                            for evicted_key in &access_result.evicted {
                                if op_manager
                                    .interest_manager
                                    .unregister_local_hosting(evicted_key)
                                {
                                    removed_contracts.push(*evicted_key);
                                }
                            }

                            // Only do first-time hosting setup if newly hosting
                            if access_result.is_new {
                                tracing::debug!(tx = %id, %key, "Contract newly hosted");
                                super::announce_contract_hosted(op_manager, &key).await;

                                // Register local interest for delta-based sync
                                let became_interested =
                                    op_manager.interest_manager.register_local_hosting(&key);

                                // Broadcast interest changes to peers
                                let added = if became_interested { vec![key] } else { vec![] };
                                if !added.is_empty() || !removed_contracts.is_empty() {
                                    super::broadcast_change_interests(
                                        op_manager,
                                        added,
                                        removed_contracts,
                                    )
                                    .await;
                                }
                            } else {
                                tracing::debug!(tx = %id, %key, "Refreshed hosting status for already-hosted contract");
                                // Still broadcast eviction changes if any contracts were evicted
                                if !removed_contracts.is_empty() {
                                    super::broadcast_change_interests(
                                        op_manager,
                                        vec![],
                                        removed_contracts,
                                    )
                                    .await;
                                }
                            }

                            // Auto-subscribe to receive updates for this contract.
                            // Only the original requester subscribes — relay peers cache
                            // for durability but don't subscribe (no freshness obligation).
                            if crate::ring::AUTO_SUBSCRIBE_ON_GET && is_original_requester {
                                // Only start new subscription if not already subscribed
                                if access_result.is_new || !op_manager.ring.is_subscribed(&key) {
                                    let blocking = subscribe_requested && blocking_sub;
                                    let child_tx = super::start_subscription_request(
                                        op_manager, id, key, blocking,
                                    );
                                    tracing::debug!(tx = %id, %child_tx, %blocking, "started subscription");
                                }
                            }
                        } else {
                            // Only attempt to cache if we have the contract code.
                            // Without the code, we can't store the contract locally (issue #2306).
                            if contract.is_some() {
                                tracing::debug!(tx = %id, %key, %is_original_requester, %subscribe_requested, "Putting contract at executor - state differs from local cache");
                                let res = op_manager
                                    .notify_contract_handler(ContractHandlerEvent::PutQuery {
                                        key,
                                        state: value.clone(),
                                        related_contracts: RelatedContracts::default(), // fixme: i think we need to get the related contracts so the final put is ok
                                        contract: contract.clone(),
                                    })
                                    .await?;

                                match res {
                                    ContractHandlerEvent::PutResponse {
                                        new_value: Ok(_), ..
                                    } => {
                                        tracing::debug!(tx = %id, %key, "Contract put at executor");

                                        // BUG FIX (2026-01): ALWAYS refresh hosting status on GET.
                                        // This ensures re-GETs keep the hosting cache's TTL/LRU fresh.
                                        //
                                        // record_get_access now returns is_new atomically, eliminating the
                                        // TOCTOU race that existed when checking is_hosting_contract() first.
                                        tracing::debug!(tx = %id, %key, peer = ?op_manager.ring.connection_manager.get_own_addr(), "Recording contract access in hosting cache");
                                        let access_result = op_manager
                                            .ring
                                            .record_get_access(key, value.size() as u64);

                                        // Clean up interest tracking for evicted contracts (always, even if already hosting)
                                        let mut removed_contracts = Vec::new();
                                        for evicted_key in &access_result.evicted {
                                            if op_manager
                                                .interest_manager
                                                .unregister_local_hosting(evicted_key)
                                            {
                                                removed_contracts.push(*evicted_key);
                                            }
                                        }

                                        // Only do first-time hosting setup if newly hosting
                                        if access_result.is_new {
                                            super::announce_contract_hosted(op_manager, &key).await;

                                            // Register local interest for delta-based sync
                                            let became_interested = op_manager
                                                .interest_manager
                                                .register_local_hosting(&key);

                                            // Broadcast interest changes to peers
                                            let added =
                                                if became_interested { vec![key] } else { vec![] };
                                            if !added.is_empty() || !removed_contracts.is_empty() {
                                                super::broadcast_change_interests(
                                                    op_manager,
                                                    added,
                                                    removed_contracts,
                                                )
                                                .await;
                                            }
                                        } else {
                                            tracing::debug!(tx = %id, %key, "Refreshed hosting status for already-hosted contract");
                                            // Still broadcast eviction changes if any contracts were evicted
                                            if !removed_contracts.is_empty() {
                                                super::broadcast_change_interests(
                                                    op_manager,
                                                    vec![],
                                                    removed_contracts,
                                                )
                                                .await;
                                            }
                                        }

                                        // Auto-subscribe to receive updates for this contract.
                                        // Only the original requester subscribes — relay peers cache
                                        // for durability but don't subscribe (no freshness obligation).
                                        if crate::ring::AUTO_SUBSCRIBE_ON_GET
                                            && is_original_requester
                                        {
                                            // Only start new subscription if not already subscribed
                                            if access_result.is_new
                                                || !op_manager.ring.is_subscribed(&key)
                                            {
                                                let blocking = subscribe_requested && blocking_sub;
                                                let child_tx = super::start_subscription_request(
                                                    op_manager, id, key, blocking,
                                                );
                                                tracing::debug!(tx = %id, %child_tx, %blocking, "started subscription");
                                            }
                                        }
                                    }
                                    ContractHandlerEvent::PutResponse {
                                        new_value: Err(err),
                                        ..
                                    } => {
                                        // Local caching failed, but GET operation succeeded
                                        // Log warning and continue - caching is an optimization, not required
                                        tracing::warn!(
                                            tx = %id,
                                            %key,
                                            error = %err,
                                            %is_original_requester,
                                            "Failed to cache contract locally during GET - continuing with operation"
                                        );
                                        // Don't return error - the GET succeeded, caching is optional
                                        // Continue to process the GET result below
                                    }
                                    ContractHandlerEvent::DelegateRequest { .. }
                                    | ContractHandlerEvent::DelegateResponse(_)
                                    | ContractHandlerEvent::PutQuery { .. }
                                    | ContractHandlerEvent::GetQuery { .. }
                                    | ContractHandlerEvent::GetResponse { .. }
                                    | ContractHandlerEvent::UpdateQuery { .. }
                                    | ContractHandlerEvent::UpdateResponse { .. }
                                    | ContractHandlerEvent::UpdateNoChange { .. }
                                    | ContractHandlerEvent::RegisterSubscriberListener { .. }
                                    | ContractHandlerEvent::RegisterSubscriberListenerResponse
                                    | ContractHandlerEvent::QuerySubscriptions { .. }
                                    | ContractHandlerEvent::QuerySubscriptionsResponse
                                    | ContractHandlerEvent::GetSummaryQuery { .. }
                                    | ContractHandlerEvent::GetSummaryResponse { .. }
                                    | ContractHandlerEvent::GetDeltaQuery { .. }
                                    | ContractHandlerEvent::GetDeltaResponse { .. }
                                    | ContractHandlerEvent::NotifySubscriptionError { .. }
                                    | ContractHandlerEvent::NotifySubscriptionErrorResponse
                                    | ContractHandlerEvent::ClientDisconnect { .. } => {
                                        unreachable!(
                                            "PutQuery from Get operation should always return PutResponse"
                                        )
                                    }
                                }
                            } else {
                                // No contract code in GET response - can't cache or subscribe locally.
                                // Without the contract code, we can't:
                                // 1. Validate incoming state updates
                                // 2. Process delta updates from subscriptions
                                // Announcing as cached or subscribing would cause #2306 errors when
                                // updates arrive for a contract we can't process.
                                tracing::warn!(
                                    tx = %id,
                                    %key,
                                    "Cannot cache or subscribe to contract - no contract code in GET response"
                                );
                                // Don't announce cached or start subscription without contract code.
                                // The GET still succeeded - the client gets their state response.
                            }
                        }
                    }

                    // Process based on whether we're originator or forwarding
                    // Use upstream_addr for routing decision (not requester which can fail for transient connections)
                    // First validate we're in an expected state
                    match &self.state {
                        Some(GetState::AwaitingResponse(_)) | Some(GetState::ReceivedRequest) => {}
                        Some(other) => {
                            // Can't take ownership, so format the state for error reporting
                            return Err(OpError::invalid_transition_with_state(
                                self.id,
                                Box::new(format!("{}", other)),
                            ));
                        }
                        None => return Err(OpError::invalid_transition(self.id)),
                    }

                    // Now handle based on upstream_addr
                    if self.upstream_addr.is_none() {
                        // Original requester, operation completed successfully
                        tracing::info!(tx = %id, contract = %key, phase = "complete", "Get response received for contract at original requester");

                        // Emit get_success telemetry (only if sender is known)
                        let hop_count =
                            current_hop.map(|h| op_manager.ring.max_hops_to_live.saturating_sub(h));
                        let hash = Some(state_hash_full(value));
                        if let Some(sender_peer) = sender {
                            if let Some(event) = NetEventLog::get_success(
                                &id,
                                &op_manager.ring,
                                key,
                                sender_peer,
                                hop_count,
                                hash,
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }
                        }

                        new_state = Some(GetState::Finished(FinishedData { key }));
                        return_msg = None;
                        result = Some(GetResult {
                            key,
                            state: value.clone(),
                            contract: contract.clone(),
                        });
                    } else {
                        // Forward response to upstream. Opportunistically cache contract
                        // code so this relay peer can serve future GET requests with
                        // fetch_contract=true. We don't call announce_contract_hosted()
                        // here because relay peers should not advertise contracts they
                        // are not responsible for in the ring.
                        tracing::info!(tx = %id, contract = %key, phase = "response", "Get response received for contract at hop peer");
                        if let Some(code) = contract {
                            if !op_manager.ring.is_hosting_contract(&key) {
                                let om = op_manager.clone();
                                let cache_key = key;
                                let v = value.clone();
                                let c = code.clone();
                                GlobalExecutor::spawn(async move {
                                    let key_str = cache_key.to_string();
                                    let cached = matches!(
                                        om.notify_contract_handler(
                                            ContractHandlerEvent::PutQuery {
                                                key: cache_key,
                                                state: v,
                                                related_contracts: RelatedContracts::default(),
                                                contract: Some(c),
                                            }
                                        )
                                        .await,
                                        Ok(ContractHandlerEvent::PutResponse {
                                            new_value: Ok(_),
                                            ..
                                        })
                                    );
                                    tracing::info!(
                                        contract = %key_str,
                                        phase = "relay_cache",
                                        cached,
                                        "Relay peer contract code cache attempt"
                                    );
                                });
                            }
                        }
                        new_state = None;
                        // Check if the response payload exceeds the streaming
                        // threshold.  When it does, convert to ResponseStreaming
                        // so that upstream relays can use pipe_stream instead of
                        // serial store-and-forward (which adds 10-50 s per hop
                        // for large contracts like the River UI at ~814 KB).
                        let store_response = StoreResponse {
                            state: Some(value.clone()),
                            contract: contract.clone(),
                        };
                        let includes_contract = store_response.contract.is_some();
                        let payload = GetStreamingPayload {
                            key,
                            value: store_response,
                        };
                        let payload_bytes = bincode::serialize(&payload).map_err(|e| {
                            OpError::NotificationChannelError(format!(
                                "Failed to serialize streaming payload: {e}"
                            ))
                        })?;
                        let payload_size = payload_bytes.len();
                        if should_use_streaming(op_manager.streaming_threshold, payload_size) {
                            let sid = StreamId::next_operations();
                            tracing::info!(
                                tx = %id,
                                stream_id = %sid,
                                payload_size,
                                phase = "relay_streaming_forward",
                                "Relay converting non-streaming GET response to streaming for upstream"
                            );
                            return_msg = Some(GetMsg::ResponseStreaming {
                                id,
                                instance_id: *payload.key.id(),
                                stream_id: sid,
                                key: payload.key,
                                total_size: payload_size as u64,
                                includes_contract,
                            });
                            stream_data = Some((sid, bytes::Bytes::from(payload_bytes)));
                        } else {
                            return_msg = Some(GetMsg::Response {
                                id,
                                instance_id: *payload.key.id(),
                                result: GetMsgResult::Found {
                                    key: payload.key,
                                    value: payload.value,
                                },
                            });
                        }
                        tracing::debug!(tx = %id, %key, upstream = ?self.upstream_addr, "Returning contract to upstream");
                        result = Some(GetResult {
                            key,
                            state: value.clone(),
                            contract: contract.clone(),
                        });
                    }
                }
                // DEFENSIVE HANDLING: Found response with empty state
                //
                // This case should not occur in normal operation because:
                // 1. Contracts are always stored with their state
                // 2. If a peer has the contract, it should have state data
                //
                // However, we handle this defensively to avoid undefined behavior if:
                // - A malformed response is received from a peer
                // - There's a race condition during contract storage
                //
                // We treat this as NotFound because a Found response without usable
                // state data is equivalent to not finding the contract.
                GetMsg::Response {
                    id,
                    instance_id,
                    result:
                        GetMsgResult::Found {
                            value: StoreResponse { state: None, .. },
                            ..
                        },
                } => {
                    let id = *id;
                    let instance_id = *instance_id;

                    // Use sender_from_addr for logging (optional - sender may not be in ring)
                    let sender = sender_from_addr.clone();

                    tracing::warn!(
                        tx = %id,
                        %instance_id,
                        sender = ?sender,
                        phase = "response",
                        "GET Found response with empty state - treating as NotFound"
                    );

                    // Forward as NotFound since Found with empty state isn't useful
                    if self.upstream_addr.is_some() {
                        return_msg = Some(GetMsg::Response {
                            id,
                            instance_id,
                            result: GetMsgResult::NotFound,
                        });
                        new_state = None;
                    } else {
                        // Original requester - operation failed
                        return_msg = None;
                        new_state = None;
                        // result remains None to indicate failure
                    }
                }

                // Streaming GET response handler
                GetMsg::ResponseStreaming {
                    id: msg_id,
                    instance_id,
                    stream_id,
                    key,
                    total_size,
                    includes_contract,
                } => {
                    let id = *msg_id;
                    let key = *key;
                    let stream_id = *stream_id;
                    let includes_contract = *includes_contract;

                    tracing::info!(
                        tx = %id,
                        %instance_id,
                        contract = %key,
                        stream_id = %stream_id,
                        total_size,
                        includes_contract,
                        phase = "streaming_response",
                        "Processing GET ResponseStreaming"
                    );

                    // Step 1: Claim the stream from orphan registry (atomic dedup)
                    let peer_addr = match source_addr {
                        Some(addr) => addr,
                        None => {
                            tracing::error!(tx = %id, "source_addr missing for streaming GET response");
                            return Err(OpError::UnexpectedOpState);
                        }
                    };
                    let stream_handle = match op_manager
                        .orphan_stream_registry()
                        .claim_or_wait(peer_addr, stream_id, STREAM_CLAIM_TIMEOUT)
                        .await
                    {
                        Ok(handle) => handle,
                        Err(OrphanStreamError::AlreadyClaimed) => {
                            tracing::debug!(
                                tx = %id,
                                stream_id = %stream_id,
                                "GET ResponseStreaming skipped — stream already claimed (dedup)"
                            );
                            // Push the operation state back since load_or_init popped it.
                            // Without this, duplicate metadata messages (from embedded fragment #1)
                            // permanently lose the operation state, preventing subsequent
                            // ResponseStreaming from being matched to the operation.
                            if self.state.is_some() {
                                if let Err(e) = op_manager
                                    .push(
                                        id,
                                        OpEnum::Get(GetOp {
                                            id,
                                            state: self.state,
                                            result: self.result,
                                            stats,
                                            upstream_addr: self.upstream_addr,
                                            local_fallback,
                                            auto_fetch: false,
                                            ack_received: false,
                                            speculative_paths: 0,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push GET op state back after dedup");
                                }
                            }
                            return Err(OpError::OpNotPresent(id));
                        }
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to claim stream from orphan registry"
                            );
                            // Push the operation state back to prevent loss
                            if self.state.is_some() {
                                if let Err(e) = op_manager
                                    .push(
                                        id,
                                        OpEnum::Get(GetOp {
                                            id,
                                            state: self.state,
                                            result: self.result,
                                            stats,
                                            upstream_addr: self.upstream_addr,
                                            local_fallback,
                                            auto_fetch: false,
                                            ack_received: false,
                                            speculative_paths: 0,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push GET op state back after orphan claim failure");
                                }
                            }
                            return Err(OpError::OrphanStreamClaimFailed);
                        }
                    };

                    // Step 2: Check if we should pipe upstream BEFORE assembly
                    // This enables low-latency forwarding of responses
                    let is_original_requester = self.upstream_addr.is_none();

                    // Extract subscription flags from state (needed for auto-subscribe after caching)
                    let (subscribe_requested, blocking_sub) =
                        if let Some(GetState::AwaitingResponse(data)) = &self.state {
                            (data.subscribe, data.blocking_subscribe)
                        } else {
                            (false, false)
                        };
                    let piping_started = if !is_original_requester {
                        let upstream_addr = self
                            .upstream_addr
                            .expect("non-originator must have upstream");
                        // Check if streaming should be used based on the original stream size
                        if should_use_streaming(
                            op_manager.streaming_threshold,
                            *total_size as usize,
                        ) {
                            let outbound_sid = StreamId::next_operations();
                            let forked_handle = stream_handle.fork();

                            tracing::info!(
                                tx = %id,
                                inbound_stream_id = %stream_id,
                                outbound_stream_id = %outbound_sid,
                                total_size,
                                peer_addr = %upstream_addr,
                                "Starting piped stream forwarding of GET response to upstream"
                            );

                            // Send metadata message first
                            let pipe_metadata = GetMsg::ResponseStreaming {
                                id,
                                instance_id: *instance_id,
                                stream_id: outbound_sid,
                                key,
                                total_size: *total_size,
                                includes_contract,
                            };
                            let pipe_metadata_net: NetMessage = pipe_metadata.into();
                            // Serialize metadata for embedding in fragment #1 (fix #2757)
                            let embedded_metadata = match bincode::serialize(&pipe_metadata_net) {
                                Ok(bytes) => Some(bytes::Bytes::from(bytes)),
                                Err(e) => {
                                    tracing::warn!(
                                        tx = %id,
                                        error = %e,
                                        "Failed to serialize piped stream metadata for embedding"
                                    );
                                    None
                                }
                            };
                            conn_manager.send(upstream_addr, pipe_metadata_net).await?;

                            // Start piping (runs asynchronously in background)
                            conn_manager
                                .pipe_stream(
                                    upstream_addr,
                                    outbound_sid,
                                    forked_handle,
                                    embedded_metadata,
                                )
                                .await?;

                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    // Step 3: Wait for stream to complete and assemble data (for local caching)
                    let stream_data = match stream_handle.assemble().await {
                        Ok(data) => data,
                        Err(e) => {
                            if !is_original_requester {
                                // Relay node: fail immediately. The originator handles retry.
                                tracing::warn!(
                                    tx = %id,
                                    stream_id = %stream_id,
                                    error = %e,
                                    "Stream assembly failed on relay, sending Aborted upstream"
                                );
                                return Err(OpError::StreamCancelled);
                            }

                            // Originator: immediately retry with the next alternative peer
                            // instead of surfacing the error to the HTTP client.
                            let all_connected: Vec<_> = op_manager
                                .ring
                                .connection_manager
                                .get_connections_by_location()
                                .values()
                                .flat_map(|conns| conns.iter().map(|c| c.location.clone()))
                                .collect();
                            let max_htl = op_manager.ring.max_hops_to_live;

                            let retry_op = GetOp {
                                id: self.id,
                                state: self.state,
                                result: self.result,
                                stats,
                                upstream_addr: self.upstream_addr,
                                local_fallback,
                                auto_fetch,
                                ack_received: false,
                                speculative_paths: self.speculative_paths,
                            };

                            // Report routing failure for the stalled peer so the router
                            // learns to avoid it in future operations.
                            let stalled_peer_info = retry_op.failure_routing_info();

                            match retry_op.retry_with_next_alternative(max_htl, &all_connected) {
                                Ok((new_op, msg)) => {
                                    if let Some((peer, contract_location)) = stalled_peer_info {
                                        op_manager.ring.routing_finished(
                                            crate::router::RouteEvent {
                                                peer,
                                                contract_location,
                                                outcome: crate::router::RouteOutcome::Failure,
                                            },
                                        );
                                    }
                                    tracing::info!(
                                        tx = %id,
                                        stream_id = %stream_id,
                                        error = %e,
                                        "Stream assembly failed, retrying GET with next peer"
                                    );
                                    let target = new_op.get_next_hop_addr();
                                    return Ok(OperationResult::SendAndContinue {
                                        msg: NetMessage::from(msg),
                                        next_hop: target,
                                        state: OpEnum::Get(new_op),
                                        stream_data: None,
                                    });
                                }
                                Err(_exhausted_op) => {
                                    tracing::warn!(
                                        tx = %id,
                                        stream_id = %stream_id,
                                        error = %e,
                                        "Stream assembly failed, no alternative peers left"
                                    );
                                    return Err(OpError::StreamCancelled);
                                }
                            }
                        }
                    };

                    tracing::debug!(
                        tx = %id,
                        stream_id = %stream_id,
                        received_size = stream_data.len(),
                        expected_size = total_size,
                        "Stream data assembled"
                    );

                    // Step 3: Deserialize the streaming payload
                    let payload: messages::GetStreamingPayload =
                        match bincode::deserialize(&stream_data) {
                            Ok(p) => p,
                            Err(e) => {
                                tracing::error!(
                                    tx = %id,
                                    stream_id = %stream_id,
                                    error = %e,
                                    "Failed to deserialize streaming payload"
                                );
                                return Err(OpError::invalid_transition(self.id));
                            }
                        };

                    // Verify key matches
                    if payload.key != key {
                        tracing::error!(
                            tx = %id,
                            expected = %key,
                            actual = %payload.key,
                            "Contract key mismatch in streaming payload"
                        );
                        return Err(OpError::invalid_transition(self.id));
                    }

                    let value = payload.value;

                    // Get current hop for telemetry
                    let current_hop = if let Some(GetState::AwaitingResponse(data)) = &self.state {
                        let current_hop = &data.current_hop;
                        Some(*current_hop)
                    } else {
                        None
                    };

                    // Step 5: Cache the contract locally, announce hosting, and auto-subscribe.
                    // This mirrors the non-streaming Response path — without it, the node
                    // never registers as subscribed after a streaming GET, causing every
                    // subsequent GET to hit the network instead of serving from local cache.
                    if let Some(state) = &value.state {
                        let contract_to_cache = if includes_contract {
                            value.contract.clone()
                        } else {
                            None
                        };

                        // Record access atomically — returns whether this is a newly-hosted contract
                        let access_result =
                            op_manager.ring.record_get_access(key, state.size() as u64);

                        // Clean up interest tracking for evicted contracts
                        let mut removed_contracts = Vec::new();
                        for evicted_key in &access_result.evicted {
                            if op_manager
                                .interest_manager
                                .unregister_local_hosting(evicted_key)
                            {
                                removed_contracts.push(*evicted_key);
                            }
                        }

                        if access_result.is_new {
                            // First time hosting — cache the contract, then announce only on success.
                            // Mirrors the non-streaming "state differs" path which gates announce
                            // on PutResponse { new_value: Ok(_) }.
                            let put_ok = match op_manager
                                .notify_contract_handler(ContractHandlerEvent::PutQuery {
                                    key,
                                    state: state.clone(),
                                    related_contracts: RelatedContracts::default(),
                                    contract: contract_to_cache,
                                })
                                .await
                            {
                                Ok(ContractHandlerEvent::PutResponse {
                                    new_value: Ok(_), ..
                                }) => true,
                                Ok(ContractHandlerEvent::PutResponse {
                                    new_value: Err(err),
                                    ..
                                }) => {
                                    tracing::warn!(
                                        tx = %id, contract = %key, error = %err,
                                        "Streaming GET: PutQuery rejected by executor"
                                    );
                                    false
                                }
                                Ok(other) => {
                                    tracing::warn!(
                                        tx = %id, contract = %key,
                                        response = %other,
                                        "Streaming GET: unexpected PutQuery response"
                                    );
                                    false
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        contract = %key, error = %e,
                                        "failed to cache contract via PutQuery"
                                    );
                                    false
                                }
                            };

                            if put_ok {
                                tracing::debug!(tx = %id, %key, "Contract newly hosted (streaming)");
                                super::announce_contract_hosted(op_manager, &key).await;

                                let became_interested =
                                    op_manager.interest_manager.register_local_hosting(&key);
                                let added = if became_interested { vec![key] } else { vec![] };
                                if !added.is_empty() || !removed_contracts.is_empty() {
                                    super::broadcast_change_interests(
                                        op_manager,
                                        added,
                                        removed_contracts,
                                    )
                                    .await;
                                }

                                // Auto-subscribe only if we successfully cached the contract.
                                if crate::ring::AUTO_SUBSCRIBE_ON_GET
                                    && is_original_requester
                                    && !op_manager.ring.is_subscribed(&key)
                                {
                                    let blocking = subscribe_requested && blocking_sub;
                                    let child_tx = super::start_subscription_request(
                                        op_manager, id, key, blocking,
                                    );
                                    tracing::debug!(tx = %id, %child_tx, %blocking, "started subscription (streaming)");
                                }
                            } else if !removed_contracts.is_empty() {
                                super::broadcast_change_interests(
                                    op_manager,
                                    vec![],
                                    removed_contracts,
                                )
                                .await;
                            }
                        } else {
                            // Already hosting — refresh state (fire-and-forget since we already
                            // have a valid cached copy; failure just means we keep the old state).
                            if let Err(e) = op_manager
                                .notify_contract_handler(ContractHandlerEvent::PutQuery {
                                    key,
                                    state: state.clone(),
                                    related_contracts: RelatedContracts::default(),
                                    contract: contract_to_cache,
                                })
                                .await
                            {
                                tracing::warn!(contract = %key, error = %e, "failed to refresh contract state via PutQuery");
                            }

                            tracing::debug!(tx = %id, %key, "Refreshed hosting status for already-hosted contract (streaming)");
                            if !removed_contracts.is_empty() {
                                super::broadcast_change_interests(
                                    op_manager,
                                    vec![],
                                    removed_contracts,
                                )
                                .await;
                            }

                            // Auto-subscribe for already-hosted contracts that lost their subscription.
                            if crate::ring::AUTO_SUBSCRIBE_ON_GET
                                && is_original_requester
                                && !op_manager.ring.is_subscribed(&key)
                            {
                                let blocking = subscribe_requested && blocking_sub;
                                let child_tx = super::start_subscription_request(
                                    op_manager, id, key, blocking,
                                );
                                tracing::debug!(tx = %id, %child_tx, %blocking, "started subscription (streaming, re-subscribe)");
                            }
                        }
                    }

                    // Step 6: Emit telemetry
                    let hop_count =
                        current_hop.map(|h| op_manager.ring.max_hops_to_live.saturating_sub(h));
                    let hash = value.state.as_ref().map(state_hash_full);
                    if let Some(event) = NetEventLog::get_success(
                        &id,
                        &op_manager.ring,
                        key,
                        op_manager.ring.connection_manager.own_location(),
                        hop_count,
                        hash,
                    ) {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }

                    // Step 7: Build result and determine response
                    if is_original_requester {
                        // This is the original requester - operation complete
                        tracing::info!(
                            tx = %id,
                            contract = %key,
                            phase = "complete",
                            "Streaming GET complete (originator)"
                        );

                        // State must be present for a successful GET
                        let state = match value.state {
                            Some(s) => s,
                            None => {
                                tracing::error!(
                                    tx = %id,
                                    contract = %key,
                                    "Streaming GET response has no state"
                                );
                                return Err(OpError::invalid_transition(self.id));
                            }
                        };

                        result = Some(GetResult {
                            key,
                            state,
                            contract: value.contract,
                        });
                        new_state = Some(GetState::Finished(FinishedData { key }));
                        return_msg = None;
                    } else if piping_started {
                        // Piping is already underway - no need to send return_msg
                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            phase = "forward",
                            "GET response piping in progress to upstream"
                        );
                        return_msg = None;
                        new_state = None;
                    } else {
                        // Forward the response to upstream as a regular Response
                        // (streaming not used - payload below threshold)
                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            phase = "forward",
                            "Forwarding GET response as non-streaming to upstream"
                        );

                        return_msg = Some(GetMsg::Response {
                            id,
                            instance_id: *instance_id,
                            result: GetMsgResult::Found { key, value },
                        });
                        new_state = None;
                    }
                }

                // Streaming GET response acknowledgment handler
                GetMsg::ResponseStreamingAck {
                    id: msg_id,
                    stream_id,
                } => {
                    let id = *msg_id;
                    let stream_id = *stream_id;

                    // The acknowledgment confirms the stream was received.
                    // For now, we just log it and clean up.
                    tracing::info!(
                        tx = %id,
                        stream_id = %stream_id,
                        phase = "streaming_ack",
                        "Streaming GET response acknowledged"
                    );
                    new_state = None;
                    return_msg = None;
                }
                GetMsg::ForwardingAck { id, instance_id } => {
                    // A downstream relay has acknowledged forwarding our request.
                    // Mark the op so the GC task knows the chain is alive.
                    tracing::debug!(
                        tx = %id,
                        %instance_id,
                        "Received forwarding ACK from downstream relay"
                    );

                    // Emit telemetry for ForwardingAck received (#3570 diagnostics)
                    if let Some(event) = NetEventLog::get_forwarding_ack_received(
                        &self.id,
                        &op_manager.ring,
                        *instance_id,
                    ) {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }

                    return Ok(OperationResult::ContinueOp(OpEnum::Get(GetOp {
                        id: self.id,
                        state: self.state,
                        result: self.result,
                        stats,
                        upstream_addr: self.upstream_addr,
                        local_fallback,
                        auto_fetch,
                        ack_received: true,
                        speculative_paths: self.speculative_paths,
                    })));
                }
            }

            build_op_result(GetOpResult {
                id: self.id,
                state: new_state,
                msg: return_msg,
                result,
                stats,
                upstream_addr: self.upstream_addr,
                auto_fetch,
                stream_data,
                local_fallback,
            })
        })
    }
}

/// Build a `GetMsg::Response` with `Found` result from a local fallback cache entry.
fn build_fallback_found_response(
    id: Transaction,
    instance_id: ContractInstanceId,
    key: ContractKey,
    state: WrappedState,
    contract: Option<ContractContainer>,
) -> GetMsg {
    GetMsg::Response {
        id,
        instance_id,
        result: GetMsgResult::Found {
            key,
            value: StoreResponse {
                state: Some(state),
                contract,
            },
        },
    }
}

struct GetOpResult {
    id: Transaction,
    state: Option<GetState>,
    msg: Option<GetMsg>,
    result: Option<GetResult>,
    stats: Option<Box<GetStats>>,
    upstream_addr: Option<std::net::SocketAddr>,
    auto_fetch: bool,
    stream_data: Option<(StreamId, bytes::Bytes)>,
    local_fallback: Option<(ContractKey, WrappedState, Option<ContractContainer>)>,
}

fn build_op_result(params: GetOpResult) -> Result<OperationResult, OpError> {
    let GetOpResult {
        id,
        state,
        msg,
        result,
        stats,
        upstream_addr,
        auto_fetch,
        stream_data,
        local_fallback,
    } = params;
    // Determine the next hop for sending the message:
    // - For Response messages: route back to upstream_addr (who sent us the request)
    // - For Request messages being forwarded: use next_hop from state
    let next_hop = match (&msg, &state) {
        (Some(GetMsg::Response { .. }) | Some(GetMsg::ResponseStreaming { .. }), _) => {
            upstream_addr
        }
        (Some(GetMsg::Request { .. }), Some(GetState::AwaitingResponse(data))) => {
            data.next_hop.socket_addr()
        }
        _ => None,
    };

    let output_op = state.map(|state| GetOp {
        id,
        state: Some(state),
        result,
        stats,
        upstream_addr,
        local_fallback,
        auto_fetch,
        ack_received: false,
        speculative_paths: 0,
    });
    let return_msg = msg.map(NetMessage::from);
    let op_state = output_op.map(OpEnum::Get);
    Ok(match (return_msg, op_state) {
        (Some(msg), Some(state)) => OperationResult::SendAndContinue {
            msg,
            next_hop,
            state,
            stream_data,
        },
        (Some(msg), None) => OperationResult::SendAndComplete {
            msg,
            next_hop,
            stream_data,
        },
        (None, Some(state)) => OperationResult::ContinueOp(state),
        (None, None) => OperationResult::Completed,
    })
}

#[allow(clippy::too_many_arguments)]
async fn try_forward_or_return(
    id: Transaction,
    instance_id: ContractInstanceId,
    (htl, fetch_contract): (usize, bool),
    (this_peer, sender): (PeerKeyLocation, Option<PeerKeyLocation>),
    visited: super::VisitedPeers,
    op_manager: &OpManager,
    stats: Option<Box<GetStats>>,
    upstream_addr: Option<std::net::SocketAddr>,
    local_fallback: Option<(ContractKey, WrappedState, Option<ContractContainer>)>,
) -> Result<OperationResult, OpError> {
    tracing::warn!(
        tx = %id,
        %instance_id,
        peer_addr = %this_peer,
        sender = ?sender,
        phase = "forward",
        "Contract not found while processing a get request"
    );

    let mut new_visited = visited.clone();
    if let Some(addr) = this_peer.socket_addr() {
        new_visited.mark_visited(addr);
    }

    let new_htl = htl.saturating_sub(1);

    let (new_target, alternatives) = if new_htl == 0 {
        tracing::warn!(
            tx = %id,
            sender = ?sender,
            htl = 0,
            phase = "error",
            "The maximum hops have been exceeded, sending response back to the node"
        );
        (None, vec![])
    } else {
        let mut candidates = op_manager.ring.k_closest_potentially_hosting(
            &instance_id,
            &new_visited,
            DEFAULT_MAX_BREADTH,
        );

        if candidates.is_empty() {
            tracing::warn!(
                tx = %id,
                %instance_id,
                peer_addr = %this_peer,
                phase = "error",
                "No other peers found while trying to get the contract"
            );
            (None, vec![])
        } else {
            let target = candidates.remove(0);
            (Some(target), candidates)
        }
    };

    if let Some(target) = new_target {
        tracing::debug!(
            tx = %id,
            "Forwarding get request to {}",
            target
        );

        // Emit telemetry: relay is forwarding GET to next hop
        if let Some(event) =
            NetEventLog::get_request(&id, &op_manager.ring, instance_id, target.clone(), new_htl)
        {
            op_manager.ring.register_events(Either::Left(event)).await;
        }

        let mut tried_peers = HashSet::new();
        if let Some(addr) = target.socket_addr() {
            tried_peers.insert(addr);
        }

        // Forwarding nodes always use non-blocking subscriptions:
        // blocking_subscribe is a client-side preference that only
        // applies to the originator node.
        build_op_result(GetOpResult {
            id,
            state: Some(GetState::AwaitingResponse(AwaitingResponseData {
                instance_id,
                requester: sender,
                retries: 0,
                fetch_contract,
                current_hop: new_htl,
                subscribe: false,
                blocking_subscribe: false,
                next_hop: target.clone(),
                tried_peers,
                alternatives,
                attempts_at_hop: 1,
                visited: new_visited.clone(),
            })),
            msg: Some(GetMsg::Request {
                id,
                instance_id,
                fetch_contract,
                htl: new_htl,
                visited: new_visited,
            }),
            result: None,
            // Track the forward target so timeouts report to
            // PeerHealthTracker and the failure estimator (#3527).
            stats: stats.map(|mut s| {
                s.next_peer = Some(target.clone());
                s
            }),
            upstream_addr,
            stream_data: None,
            local_fallback,
            auto_fetch: false,
        })
    } else if upstream_addr.is_some() {
        // No targets found — check for local fallback before returning NotFound
        if let Some((key, state, contract)) = local_fallback {
            tracing::info!(
                tx = %id,
                contract = %key,
                "Relay serving local cache as fallback (no forwarding targets)"
            );
            let msg = build_fallback_found_response(id, instance_id, key, state, contract);
            build_op_result(GetOpResult {
                id,
                state: None,
                msg: Some(msg),
                result: None,
                stats,
                upstream_addr,
                stream_data: None,
                local_fallback: None,
                auto_fetch: false,
            })
        } else {
            tracing::warn!(
                tx = %id,
                %instance_id,
                phase = "not_found",
                "No peers to forward get request to, returning NotFound to upstream"
            );
            // Emit telemetry: relay returning NotFound (no forwarding targets)
            if let Some(event) =
                NetEventLog::get_not_found(&id, &op_manager.ring, instance_id, None)
            {
                op_manager.ring.register_events(Either::Left(event)).await;
            }
            build_op_result(GetOpResult {
                id,
                state: None,
                msg: Some(GetMsg::Response {
                    id,
                    instance_id,
                    result: GetMsgResult::NotFound,
                }),
                result: None,
                stats,
                upstream_addr,
                stream_data: None,
                local_fallback: None,
                auto_fetch: false,
            })
        }
    } else {
        // Original requester with no forwarding targets - operation fails locally
        tracing::warn!(
            tx = %id,
            %instance_id,
            phase = "not_found",
            "No peers to forward get request to and no upstream - local operation fails"
        );

        notify_get_failed(
            op_manager,
            id,
            &format!("contract {instance_id} not found, no peers available to forward"),
        );

        build_op_result(GetOpResult {
            id,
            state: None,
            msg: None,
            result: None,
            stats,
            upstream_addr,
            stream_data: None,
            local_fallback: None,
            auto_fetch: false,
        })
    }
}

impl IsOperationCompleted for GetOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(GetState::Finished(_)))
    }
}

/// Notify the client that a GET operation failed with the given reason.
fn notify_get_failed(op_manager: &OpManager, tx: Transaction, reason: &str) {
    op_manager.send_client_result(
        tx,
        Err(ErrorKind::OperationError {
            cause: reason.to_string().into(),
        }
        .into()),
    );
}

mod messages {
    use std::fmt::Display;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::transport::peer_connection::StreamId;

    /// Payload for streaming GET responses.
    /// Contains the result of a GET operation, serialized for streaming transfer.
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct GetStreamingPayload {
        pub key: ContractKey,
        pub value: StoreResponse,
    }

    /// Result of a GET operation - either the contract was found or it wasn't.
    ///
    /// This provides explicit semantics for "contract not found" rather than
    /// requiring interpretation of empty responses or timeouts.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum GetMsgResult {
        /// Contract was found - includes full key and value
        Found {
            key: ContractKey,
            value: StoreResponse,
        },
        /// Contract was not found after exhaustive search
        NotFound,
    }

    /// GET operation messages.
    ///
    /// Uses hop-by-hop routing: each node stores `upstream_addr` from the transport layer
    /// to route responses back. No `PeerKeyLocation` is embedded in wire messages.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum GetMsg {
        /// Request to retrieve a contract. Forwarded hop-by-hop toward contract location.
        /// Uses instance_id since the requester may not have the full key yet.
        Request {
            id: Transaction,
            instance_id: ContractInstanceId,
            fetch_contract: bool,
            /// Hops to live - decremented at each hop. When 0, stop forwarding.
            htl: usize,
            /// Bloom filter tracking visited peers to prevent loops.
            visited: super::super::VisitedPeers,
        },
        /// Response for a GET operation. Routed hop-by-hop back to originator.
        /// Uses instance_id for routing (always available from the request).
        /// The full ContractKey is only present in GetMsgResult::Found.
        Response {
            id: Transaction,
            instance_id: ContractInstanceId,
            result: GetMsgResult,
        },

        /// Streaming response for large contract data. Used when the response payload
        /// exceeds streaming_threshold (default 64KB). The actual data is sent via
        /// a separate stream identified by stream_id.
        ///
        /// This variant is only used when streaming is enabled in config.
        ResponseStreaming {
            id: Transaction,
            instance_id: ContractInstanceId,
            /// Identifies the stream carrying the response data
            stream_id: StreamId,
            /// Full contract key (known since we found the contract)
            key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
            /// Whether the response includes the contract code (not just state)
            includes_contract: bool,
        },

        /// Acknowledgment that a streaming response was received.
        /// Sent back to the responder to confirm stream completion.
        ResponseStreamingAck {
            id: Transaction,
            stream_id: StreamId,
        },

        /// Lightweight ACK sent by a relay peer back to its upstream when it forwards
        /// a GET request to the next hop. Tells the upstream "I'm working on it" so
        /// the GC task can distinguish dead peers from slow multi-hop chains.
        /// Fire-and-forget — no response expected.
        ForwardingAck {
            id: Transaction,
            instance_id: ContractInstanceId,
        },
    }

    impl InnerMessage for GetMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::ResponseStreaming { id, .. }
                | Self::ResponseStreamingAck { id, .. }
                | Self::ForwardingAck { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { instance_id, .. }
                | Self::Response { instance_id, .. }
                | Self::ResponseStreaming { instance_id, .. }
                | Self::ForwardingAck { instance_id, .. } => Some(Location::from(instance_id)),
                Self::ResponseStreamingAck { .. } => {
                    // Ack doesn't carry location info - routed via stream_id
                    None
                }
            }
        }
    }

    impl Display for GetMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request {
                    instance_id, htl, ..
                } => {
                    write!(
                        f,
                        "Get::Request(id: {id}, instance_id: {instance_id}, htl: {htl})"
                    )
                }
                Self::Response {
                    instance_id,
                    result,
                    ..
                } => {
                    let result_str = match result {
                        GetMsgResult::Found { key, .. } => format!("Found({key})"),
                        GetMsgResult::NotFound => "NotFound".to_string(),
                    };
                    write!(
                        f,
                        "Get::Response(id: {id}, instance_id: {instance_id}, result: {result_str})"
                    )
                }
                Self::ResponseStreaming {
                    instance_id,
                    stream_id,
                    key,
                    total_size,
                    ..
                } => {
                    write!(
                        f,
                        "Get::ResponseStreaming(id: {id}, instance_id: {instance_id}, key: {key}, stream: {stream_id}, size: {total_size})"
                    )
                }
                Self::ResponseStreamingAck { stream_id, .. } => {
                    write!(
                        f,
                        "Get::ResponseStreamingAck(id: {id}, stream: {stream_id})"
                    )
                }
                Self::ForwardingAck { instance_id, .. } => {
                    write!(
                        f,
                        "Get::ForwardingAck(id: {id}, instance_id: {instance_id})"
                    )
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::message::Transaction;
    use crate::operations::VisitedPeers;
    use crate::operations::test_utils::make_contract_key;

    fn make_get_op(state: Option<GetState>, result: Option<GetResult>) -> GetOp {
        GetOp {
            id: Transaction::new::<GetMsg>(),
            state,
            result,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        }
    }

    // Tests for finalized() method
    #[test]
    fn get_op_finalized_when_finished_with_result() {
        let key = make_contract_key(1);
        let result = GetResult {
            key,
            state: WrappedState::new(vec![1, 2, 3]),
            contract: None,
        };
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), Some(result));
        assert!(
            op.finalized(),
            "GetOp should be finalized when state is Finished and result is present"
        );
    }

    #[test]
    fn get_op_not_finalized_when_finished_without_result() {
        let key = make_contract_key(1);
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), None);
        assert!(
            !op.finalized(),
            "GetOp should not be finalized when state is Finished but result is None"
        );
    }

    #[test]
    fn get_op_not_finalized_when_received_request() {
        let op = make_get_op(Some(GetState::ReceivedRequest), None);
        assert!(
            !op.finalized(),
            "GetOp should not be finalized in ReceivedRequest state"
        );
    }

    #[test]
    fn get_op_not_finalized_when_state_is_none() {
        let op = make_get_op(None, None);
        assert!(
            !op.finalized(),
            "GetOp should not be finalized when state is None"
        );
    }

    // Tests for to_host_result() method
    #[test]
    fn get_op_to_host_result_success_when_result_present() {
        let key = make_contract_key(1);
        let state_data = WrappedState::new(vec![1, 2, 3]);
        let result = GetResult {
            key,
            state: state_data.clone(),
            contract: None,
        };
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), Some(result));
        let host_result = op.to_host_result();

        assert!(
            host_result.is_ok(),
            "to_host_result should return Ok when result is present"
        );

        if let Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::GetResponse {
                key: returned_key,
                state: returned_state,
                ..
            },
        )) = host_result
        {
            assert_eq!(returned_key, key, "Returned key should match");
            assert_eq!(returned_state, state_data, "Returned state should match");
        } else {
            panic!("Expected GetResponse");
        }
    }

    #[test]
    fn get_op_to_host_result_error_when_no_result() {
        let op = make_get_op(Some(GetState::ReceivedRequest), None);
        let result = op.to_host_result();
        assert!(
            result.is_err(),
            "to_host_result should return Err when result is None"
        );
    }

    // Tests for outcome() method - partial coverage since full stats require complex setup
    #[test]
    fn get_op_outcome_incomplete_without_stats() {
        let key = make_contract_key(1);
        let result = GetResult {
            key,
            state: WrappedState::new(vec![]),
            contract: None,
        };
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), Some(result));
        let outcome = op.outcome();

        assert!(matches!(outcome, OpOutcome::Incomplete));
    }

    // Tests for GetMsg helper methods
    #[test]
    fn get_msg_id_returns_transaction() {
        let tx = Transaction::new::<GetMsg>();
        let msg = GetMsg::Request {
            id: tx,
            instance_id: *make_contract_key(1).id(),
            fetch_contract: false,
            htl: 5,
            visited: VisitedPeers::new(&tx),
        };
        assert_eq!(*msg.id(), tx, "id() should return the transaction ID");
    }

    #[test]
    fn get_msg_display_formats_correctly() {
        let tx = Transaction::new::<GetMsg>();
        let msg = GetMsg::Request {
            id: tx,
            instance_id: *make_contract_key(1).id(),
            fetch_contract: false,
            htl: 5,
            visited: VisitedPeers::new(&tx),
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("Request"),
            "Display should contain message type name"
        );
    }

    // Tests for GetState Display
    #[test]
    fn get_state_display_received_request() {
        let state = GetState::ReceivedRequest;
        let display = format!("{}", state);
        assert!(
            display.contains("ReceivedRequest"),
            "Display should contain state name"
        );
    }

    #[test]
    fn get_state_display_finished() {
        let state = GetState::Finished(FinishedData {
            key: make_contract_key(1),
        });
        let display = format!("{}", state);
        assert!(
            display.contains("Finished"),
            "Display should contain state name"
        );
    }

    #[test]
    fn get_msg_response_found_display_formats_correctly() {
        let tx = Transaction::new::<GetMsg>();
        let key = make_contract_key(1);
        let msg = GetMsg::Response {
            id: tx,
            instance_id: *key.id(),
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: Some(WrappedState::new(vec![1, 2, 3])),
                    contract: None,
                },
            },
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("Response"),
            "Display should contain message type name"
        );
        assert!(
            display.contains("Found"),
            "Display should indicate Found result"
        );
    }

    #[test]
    fn get_msg_response_notfound_display_formats_correctly() {
        let tx = Transaction::new::<GetMsg>();
        let instance_id = *make_contract_key(1).id();
        let msg = GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::NotFound,
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("Response"),
            "Display should contain message type name"
        );
        assert!(
            display.contains("NotFound"),
            "Display should indicate NotFound result"
        );
    }

    #[test]
    fn get_msg_result_found_contains_key_and_value() {
        let key = make_contract_key(1);
        let state = WrappedState::new(vec![1, 2, 3]);
        let result = GetMsgResult::Found {
            key,
            value: StoreResponse {
                state: Some(state.clone()),
                contract: None,
            },
        };
        if let GetMsgResult::Found {
            key: found_key,
            value,
        } = result
        {
            assert_eq!(found_key, key);
            assert_eq!(value.state, Some(state));
        } else {
            panic!("Expected Found variant");
        }
    }

    #[test]
    fn get_msg_result_notfound_is_unit_variant() {
        let result = GetMsgResult::NotFound;
        assert!(
            matches!(result, GetMsgResult::NotFound),
            "NotFound should match NotFound"
        );
    }

    #[test]
    fn get_msg_response_requested_location_uses_instance_id() {
        let tx = Transaction::new::<GetMsg>();
        let key = make_contract_key(1);
        let instance_id = *key.id();

        // Test Found variant
        let msg_found = GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::Found {
                key,
                value: StoreResponse {
                    state: Some(WrappedState::new(vec![])),
                    contract: None,
                },
            },
        };
        let location_found = msg_found.requested_location();
        assert!(
            location_found.is_some(),
            "Response should have a requested location"
        );
        assert_eq!(
            location_found.unwrap(),
            Location::from(&instance_id),
            "Location should be derived from instance_id"
        );

        // Test NotFound variant
        let msg_notfound = GetMsg::Response {
            id: tx,
            instance_id,
            result: GetMsgResult::NotFound,
        };
        let location_notfound = msg_notfound.requested_location();
        assert!(
            location_notfound.is_some(),
            "NotFound Response should have a requested location"
        );
        assert_eq!(
            location_notfound.unwrap(),
            Location::from(&instance_id),
            "Location should be derived from instance_id for NotFound too"
        );
    }

    // Tests for blocking_subscribe propagation
    #[test]
    fn start_op_propagates_blocking_subscribe_true() {
        let instance_id = ContractInstanceId::new([1u8; 32]);
        let op = start_op(instance_id, true, true, true);
        match op.state {
            Some(GetState::PrepareRequest(data)) => {
                let blocking_subscribe = data.blocking_subscribe;
                assert!(
                    blocking_subscribe,
                    "blocking_subscribe should be true in PrepareRequest"
                );
            }
            other => panic!("Expected PrepareRequest state, got {:?}", other),
        }
    }

    #[test]
    fn start_op_with_id_propagates_blocking_subscribe_true() {
        let instance_id = ContractInstanceId::new([1u8; 32]);
        let tx = Transaction::new::<GetMsg>();
        let op = start_op_with_id(instance_id, true, true, true, tx);
        match op.state {
            Some(GetState::PrepareRequest(data)) => {
                let blocking_subscribe = data.blocking_subscribe;
                assert!(
                    blocking_subscribe,
                    "blocking_subscribe should be true in PrepareRequest via start_op_with_id"
                );
            }
            other => panic!("Expected PrepareRequest state, got {:?}", other),
        }
    }

    #[test]
    fn start_op_defaults_blocking_subscribe_false() {
        let instance_id = ContractInstanceId::new([1u8; 32]);
        let op = start_op(instance_id, true, true, false);
        match op.state {
            Some(GetState::PrepareRequest(data)) => {
                let blocking_subscribe = data.blocking_subscribe;
                assert!(
                    !blocking_subscribe,
                    "blocking_subscribe should be false by default"
                );
            }
            other => panic!("Expected PrepareRequest state, got {:?}", other),
        }
    }

    #[test]
    fn test_failure_outcome_for_get() {
        use crate::ring::{Location, PeerKeyLocation};
        use std::time::Duration;

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // GetOp with stats but no result → should return ContractOpFailure
        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: None,
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: None,
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };

        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpSuccessUntimed { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => panic!("Expected ContractOpFailure"),
        }

        // GetOp with no stats and no result → should return Incomplete
        let op_no_stats = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: None,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };
        assert!(
            matches!(op_no_stats.outcome(), OpOutcome::Incomplete),
            "GetOp with no stats should return Incomplete"
        );

        // GetOp with result + complete stats → should return ContractOpSuccess
        let now = Instant::now();
        let later = now + Duration::from_millis(50);
        let op_success = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: Some((now, Some(later))),
                transfer_time: Some((now, Some(later))),
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };
        assert!(
            matches!(op_success.outcome(), OpOutcome::ContractOpSuccess { .. }),
            "GetOp with result and complete stats should return ContractOpSuccess"
        );
    }

    // ============ Partial timing outcome tests (validates GET partial timing fix) ============

    /// Result + target_peer but incomplete timing → ContractOpSuccessUntimed.
    /// Before the fix, this returned Incomplete, so the router never got feedback.
    #[test]
    fn test_get_outcome_success_untimed_partial_timing() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let now = Instant::now();

        // Result present, target_peer set, first_response_time has start but no end
        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: Some((now, None)), // Started but not completed
                transfer_time: Some((now, None)),       // Started but not completed
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };

        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            other @ OpOutcome::ContractOpSuccess { .. }
            | other @ OpOutcome::ContractOpFailure { .. }
            | other @ OpOutcome::Incomplete
            | other @ OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed, got {other:?}")
            }
        }
    }

    /// Result + target_peer but no transfer_time at all → ContractOpSuccessUntimed.
    #[test]
    fn test_get_outcome_success_untimed_no_transfer_time() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: None,
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };

        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            other @ OpOutcome::ContractOpSuccess { .. }
            | other @ OpOutcome::ContractOpFailure { .. }
            | other @ OpOutcome::Incomplete
            | other @ OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed, got {other:?}")
            }
        }
    }

    /// Result but no target_peer → Incomplete (can't attribute to a peer).
    #[test]
    fn test_get_outcome_incomplete_result_no_peer() {
        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            result: Some(GetResult {
                key: make_contract_key(1),
                state: WrappedState::new(vec![1, 2, 3]),
                contract: None,
            }),
            stats: Some(Box::new(GetStats {
                next_peer: None,
                contract_location: Location::random(),
                first_response_time: None,
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };

        assert!(
            matches!(op.outcome(), OpOutcome::Incomplete),
            "Result with no target_peer should return Incomplete"
        );
    }

    /// No result + partial timing → still ContractOpFailure (regression guard).
    #[test]
    fn test_get_outcome_failure_no_result_partial_timing() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let now = Instant::now();

        let op = GetOp {
            id: Transaction::new::<GetMsg>(),
            state: None,
            result: None,
            stats: Some(Box::new(GetStats {
                next_peer: Some(target_peer.clone()),
                contract_location,
                first_response_time: Some((now, None)),
                transfer_time: None,
            })),
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };

        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            other @ OpOutcome::ContractOpSuccess { .. }
            | other @ OpOutcome::ContractOpSuccessUntimed { .. }
            | other @ OpOutcome::Incomplete
            | other @ OpOutcome::Irrelevant => panic!("Expected ContractOpFailure, got {other:?}"),
        }
    }

    #[test]
    fn test_build_fallback_found_response() {
        let id = Transaction::new::<GetMsg>();
        let key = make_contract_key(1);
        let instance_id = *key.id();
        let state = WrappedState::new(vec![1, 2, 3]);
        let msg = build_fallback_found_response(id, instance_id, key, state.clone(), None);
        match msg {
            GetMsg::Response {
                id: resp_id,
                instance_id: resp_iid,
                result: GetMsgResult::Found { key: k, value },
            } => {
                assert_eq!(resp_id, id);
                assert_eq!(resp_iid, instance_id);
                assert_eq!(k, key);
                assert_eq!(value.state, Some(state));
                assert!(value.contract.is_none());
            }
            other @ GetMsg::Request { .. }
            | other @ GetMsg::Response { .. }
            | other @ GetMsg::ResponseStreaming { .. }
            | other @ GetMsg::ResponseStreamingAck { .. }
            | other @ GetMsg::ForwardingAck { .. } => {
                panic!("Expected Found response, got {other:?}")
            }
        }
    }

    // === Tests for retry_with_next_alternative ===

    use crate::operations::test_utils::make_peer;

    fn make_awaiting_op(alternatives: Vec<PeerKeyLocation>, tried: &[PeerKeyLocation]) -> GetOp {
        let id = Transaction::new::<GetMsg>();
        let instance_id = ContractInstanceId::new([42u8; 32]);
        let visited = VisitedPeers::new(&id);
        let next_hop = make_peer(9000);
        let mut tried_peers = HashSet::new();
        for p in tried {
            if let Some(addr) = p.socket_addr() {
                tried_peers.insert(addr);
            }
        }
        if let Some(addr) = next_hop.socket_addr() {
            tried_peers.insert(addr);
        }
        GetOp {
            id,
            state: Some(GetState::AwaitingResponse(AwaitingResponseData {
                instance_id,
                retries: 0,
                fetch_contract: true,
                requester: None,
                current_hop: 7,
                subscribe: false,
                blocking_subscribe: false,
                next_hop,
                tried_peers,
                alternatives,
                attempts_at_hop: 1,
                visited,
            })),
            result: None,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        }
    }

    #[test]
    fn retry_with_local_alternative_succeeds() {
        let alt1 = make_peer(1001);
        let alt2 = make_peer(1002);
        let op = make_awaiting_op(vec![alt1.clone(), alt2], &[]);

        let result = op.retry_with_next_alternative(7, &[]);
        assert!(result.is_ok(), "Should succeed with local alternatives");

        let (new_op, msg) = result.unwrap_or_else(|_| panic!("expected Ok"));
        // The message should be a Request preserving original fetch_contract
        match &msg {
            GetMsg::Request { fetch_contract, .. } => {
                assert!(
                    *fetch_contract,
                    "retry must preserve original fetch_contract"
                );
            }
            GetMsg::Response { .. }
            | GetMsg::ResponseStreaming { .. }
            | GetMsg::ResponseStreamingAck { .. }
            | GetMsg::ForwardingAck { .. } => panic!("Expected GetMsg::Request"),
        }
        // Should have 1 alternative remaining
        if let Some(GetState::AwaitingResponse(data)) = &new_op.state {
            assert_eq!(data.alternatives.len(), 1);
            assert!(data.tried_peers.contains(&alt1.socket_addr().unwrap()));
        } else {
            panic!("Expected AwaitingResponse state");
        }
    }

    #[test]
    fn retry_exhausted_returns_err() {
        let op = make_awaiting_op(vec![], &[]);

        let result = op.retry_with_next_alternative(7, &[]);
        assert!(result.is_err(), "Should fail with no alternatives");

        // Verify op is returned intact
        let returned_op = match result {
            Err(op) => *op,
            Ok(_) => panic!("expected Err"),
        };
        assert!(
            matches!(&returned_op.state, Some(GetState::AwaitingResponse(_))),
            "State should be preserved"
        );
    }

    #[test]
    fn retry_dbf_fallback_injects_new_peers() {
        // No local alternatives, but provide fallback peers
        let fallback1 = make_peer(2001);
        let fallback2 = make_peer(2002);
        let op = make_awaiting_op(vec![], &[]);

        let result = op.retry_with_next_alternative(7, &[fallback1.clone(), fallback2]);
        assert!(result.is_ok(), "Should succeed with DBF fallback peers");

        let (new_op, _msg) = result.unwrap_or_else(|_| panic!("expected Ok"));
        if let Some(GetState::AwaitingResponse(data)) = &new_op.state {
            // One fallback used as next_hop, one remaining
            assert_eq!(data.alternatives.len(), 1);
            assert!(data.tried_peers.contains(&fallback1.socket_addr().unwrap()));
        } else {
            panic!("Expected AwaitingResponse state");
        }
    }

    #[test]
    fn retry_dbf_fallback_skips_already_tried() {
        let already_tried = make_peer(3001);
        let fresh_peer = make_peer(3002);
        let op = make_awaiting_op(vec![], std::slice::from_ref(&already_tried));

        let result = op.retry_with_next_alternative(7, &[already_tried, fresh_peer.clone()]);
        assert!(result.is_ok(), "Should find fresh_peer");

        let (new_op, _msg) = result.unwrap_or_else(|_| panic!("expected Ok"));
        if let Some(GetState::AwaitingResponse(data)) = &new_op.state {
            assert_eq!(data.alternatives.len(), 0, "Only one fresh peer injected");
            assert!(
                data.tried_peers
                    .contains(&fresh_peer.socket_addr().unwrap())
            );
        } else {
            panic!("Expected AwaitingResponse state");
        }
    }

    #[test]
    fn retry_dbf_all_tried_returns_err() {
        let tried1 = make_peer(4001);
        let tried2 = make_peer(4002);
        let op = make_awaiting_op(vec![], &[tried1.clone(), tried2.clone()]);

        let result = op.retry_with_next_alternative(7, &[tried1, tried2]);
        assert!(result.is_err(), "All fallback peers already tried");
    }

    /// Verify that retry HTL decreases with each attempt (#3570).
    ///
    /// At the originator, current_hop == max_hops_to_live, so the old code
    /// (htl: max_hops_to_live) always sent full-depth retries. The fix divides
    /// HTL by attempts_at_hop, creating progressively shorter retry chains.
    #[test]
    fn retry_htl_decreases_with_attempts() {
        let alt1 = make_peer(6001);
        let alt2 = make_peer(6002);
        let alt3 = make_peer(6003);
        let op = make_awaiting_op(vec![alt1, alt2, alt3], &[]);

        // make_awaiting_op sets attempts_at_hop=1, then retry increments to 2 before computing HTL.
        // First retry: attempts_at_hop becomes 2 → 10/2 = 5
        let (op, msg) = op
            .retry_with_next_alternative(10, &[])
            .unwrap_or_else(|_| panic!("retry 1 failed"));
        match &msg {
            GetMsg::Request { htl, .. } => {
                assert_eq!(
                    *htl, 5,
                    "First retry: 10/2=5 (attempts_at_hop incremented to 2)"
                );
            }
            other @ GetMsg::Response { .. }
            | other @ GetMsg::ResponseStreaming { .. }
            | other @ GetMsg::ResponseStreamingAck { .. }
            | other @ GetMsg::ForwardingAck { .. } => {
                panic!("Expected Request, got {other}")
            }
        }

        // Second retry: attempts_at_hop becomes 3 → 10/3 = 3
        let (op, msg) = op
            .retry_with_next_alternative(10, &[])
            .unwrap_or_else(|_| panic!("retry 2 failed"));
        match &msg {
            GetMsg::Request { htl, .. } => {
                assert_eq!(
                    *htl, MIN_RETRY_HTL,
                    "Second retry: 10/3=3 (clamped to MIN_RETRY_HTL)"
                );
            }
            other @ GetMsg::Response { .. }
            | other @ GetMsg::ResponseStreaming { .. }
            | other @ GetMsg::ResponseStreamingAck { .. }
            | other @ GetMsg::ForwardingAck { .. } => {
                panic!("Expected Request, got {other}")
            }
        }

        // Third retry: attempts_at_hop becomes 4 → 10/4 = 2 → clamped to MIN_RETRY_HTL=3
        let (_op, msg) = op
            .retry_with_next_alternative(10, &[])
            .unwrap_or_else(|_| panic!("retry 3 failed"));
        match &msg {
            GetMsg::Request { htl, .. } => {
                assert_eq!(
                    *htl, MIN_RETRY_HTL,
                    "Third retry: 10/4=2 → clamped to MIN_RETRY_HTL"
                );
            }
            other @ GetMsg::Response { .. }
            | other @ GetMsg::ResponseStreaming { .. }
            | other @ GetMsg::ResponseStreamingAck { .. }
            | other @ GetMsg::ForwardingAck { .. } => {
                panic!("Expected Request, got {other}")
            }
        }
    }

    /// Verify that MIN_RETRY_HTL floor is applied even when max_htl is low.
    #[test]
    fn retry_htl_floor_at_min() {
        let alt = make_peer(7001);
        let mut op = make_awaiting_op(vec![alt], &[]);
        // Simulate many attempts to push HTL below floor
        if let Some(GetState::AwaitingResponse(ref mut data)) = op.state {
            data.attempts_at_hop = 20;
        }

        let (_op, msg) = op
            .retry_with_next_alternative(10, &[])
            .unwrap_or_else(|_| panic!("retry failed"));
        match &msg {
            GetMsg::Request { htl, .. } => {
                assert!(
                    *htl >= MIN_RETRY_HTL,
                    "HTL {} should not fall below MIN_RETRY_HTL {}",
                    htl,
                    MIN_RETRY_HTL
                );
            }
            other @ GetMsg::Response { .. }
            | other @ GetMsg::ResponseStreaming { .. }
            | other @ GetMsg::ResponseStreamingAck { .. }
            | other @ GetMsg::ForwardingAck { .. } => {
                panic!("Expected Request, got {other}")
            }
        }
    }

    /// Verify that bloom-filter-visited peers are excluded from fallback injection (#3570).
    #[test]
    fn retry_dbf_fallback_skips_bloom_visited() {
        let visited_peer = make_peer(8001);
        let fresh_peer = make_peer(8002);

        // Create op with the visited peer marked in the bloom filter
        let mut op = make_awaiting_op(vec![], &[]);
        if let Some(GetState::AwaitingResponse(ref mut data)) = op.state {
            if let Some(addr) = visited_peer.socket_addr() {
                data.visited.mark_visited(addr);
            }
            // Remove visited_peer from tried_peers so only bloom filter catches it
            if let Some(addr) = visited_peer.socket_addr() {
                data.tried_peers.remove(&addr);
            }
        }

        let result = op.retry_with_next_alternative(7, &[visited_peer.clone(), fresh_peer.clone()]);
        assert!(
            result.is_ok(),
            "Should find fresh_peer despite visited_peer in bloom"
        );

        let (new_op, _msg) = result.unwrap_or_else(|_| panic!("retry failed"));
        if let Some(GetState::AwaitingResponse(data)) = &new_op.state {
            assert_eq!(
                data.alternatives.len(),
                0,
                "Only fresh_peer should be injected (visited_peer filtered by bloom)"
            );
        } else {
            panic!("Expected AwaitingResponse state");
        }
    }

    /// Verify that retry targets are marked in the bloom filter (#3570).
    #[test]
    fn retry_marks_target_in_bloom_filter() {
        let alt = make_peer(9001);
        let alt_addr = alt.socket_addr().unwrap();
        let op = make_awaiting_op(vec![alt], &[]);

        let (new_op, _msg) = op
            .retry_with_next_alternative(7, &[])
            .unwrap_or_else(|_| panic!("retry failed"));
        if let Some(GetState::AwaitingResponse(data)) = &new_op.state {
            assert!(
                data.visited.probably_visited(alt_addr),
                "Retry target should be marked in bloom filter"
            );
        } else {
            panic!("Expected AwaitingResponse state");
        }
    }

    #[test]
    fn retry_wrong_state_returns_err() {
        // PrepareRequest state
        let op = make_get_op(
            Some(GetState::PrepareRequest(PrepareRequestData {
                instance_id: ContractInstanceId::new([1u8; 32]),
                id: Transaction::new::<GetMsg>(),
                fetch_contract: true,
                subscribe: false,
                blocking_subscribe: false,
            })),
            None,
        );
        assert!(op.retry_with_next_alternative(7, &[]).is_err());

        // ReceivedRequest state
        let op = make_get_op(Some(GetState::ReceivedRequest), None);
        assert!(op.retry_with_next_alternative(7, &[]).is_err());

        // None state
        let op = make_get_op(None, None);
        assert!(op.retry_with_next_alternative(7, &[]).is_err());
    }

    // === Tests for start_targeted_op ===

    #[test]
    fn start_targeted_op_creates_correct_state() {
        let target = make_peer(5001);
        let instance_id = ContractInstanceId::new([99u8; 32]);

        let (op, msg) = start_targeted_op(instance_id, target.clone(), 10);

        // Should be auto_fetch
        assert!(op.auto_fetch, "Targeted op should be marked as auto_fetch");

        // Should be in AwaitingResponse state targeting the peer
        if let Some(GetState::AwaitingResponse(data)) = &op.state {
            assert_eq!(data.instance_id, instance_id);
            assert!(data.fetch_contract);
            assert!(data.requester.is_none());
            assert!(data.tried_peers.contains(&target.socket_addr().unwrap()));
            assert!(data.alternatives.is_empty());
        } else {
            panic!("Expected AwaitingResponse state");
        }

        // Message should be a Request with correct fields
        match msg {
            GetMsg::Request {
                instance_id: msg_id,
                fetch_contract,
                htl,
                ..
            } => {
                assert_eq!(msg_id, instance_id);
                assert!(fetch_contract);
                assert_eq!(htl, 10);
            }
            GetMsg::Response { .. }
            | GetMsg::ResponseStreaming { .. }
            | GetMsg::ResponseStreamingAck { .. }
            | GetMsg::ForwardingAck { .. } => {
                panic!("Expected Request message")
            }
        }
    }

    // === Tests for is_client_initiated ===

    #[test]
    fn is_client_initiated_true_for_prepare_request() {
        let op = make_get_op(
            Some(GetState::PrepareRequest(PrepareRequestData {
                instance_id: ContractInstanceId::new([1u8; 32]),
                id: Transaction::new::<GetMsg>(),
                fetch_contract: true,
                subscribe: false,
                blocking_subscribe: false,
            })),
            None,
        );
        assert!(op.is_client_initiated());
    }

    #[test]
    fn is_client_initiated_true_for_awaiting_no_requester() {
        let op = make_awaiting_op(vec![], &[]);
        assert!(op.is_client_initiated());
    }

    #[test]
    fn is_client_initiated_false_for_awaiting_with_requester() {
        let id = Transaction::new::<GetMsg>();
        let instance_id = ContractInstanceId::new([42u8; 32]);
        let visited = VisitedPeers::new(&id);
        let requester = make_peer(6001);
        let op = GetOp {
            id,
            state: Some(GetState::AwaitingResponse(AwaitingResponseData {
                instance_id,
                retries: 0,
                fetch_contract: true,
                requester: Some(requester),
                current_hop: 7,
                subscribe: false,
                blocking_subscribe: false,
                next_hop: make_peer(6002),
                tried_peers: HashSet::new(),
                alternatives: vec![],
                attempts_at_hop: 1,
                visited,
            })),
            result: None,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        };
        assert!(!op.is_client_initiated());
    }

    #[test]
    fn is_client_initiated_false_for_auto_fetch() {
        let target = make_peer(7001);
        let instance_id = ContractInstanceId::new([77u8; 32]);
        let (op, _msg) = start_targeted_op(instance_id, target, 10);
        // auto_fetch ops should NOT be client-initiated even though requester is None
        assert!(
            !op.is_client_initiated(),
            "Auto-fetch GET should not be client-initiated"
        );
    }

    #[test]
    fn is_client_initiated_false_for_other_states() {
        let op = make_get_op(Some(GetState::ReceivedRequest), None);
        assert!(!op.is_client_initiated());

        let key = make_contract_key(1);
        let op = make_get_op(Some(GetState::Finished(FinishedData { key })), None);
        assert!(!op.is_client_initiated());

        let op = make_get_op(None, None);
        assert!(!op.is_client_initiated());
    }

    // === Tests for GC task retry flow ===
    //
    // These tests reproduce the exact scenario from the garbage_cleanup_task in
    // op_state_manager.rs: a GET op has been stuck for >20s with no response,
    // and the GC task selects it for retry.
    //
    // The GC task uses:
    //   1. tx.elapsed() > GET_RETRY_THRESHOLD (20s ± 20% jitter)
    //   2. retry_with_next_alternative(max_htl, &all_connected)
    //
    // Without the fix (no GC retry logic), stuck GETs would sit until the 60s
    // OPERATION_TTL expires and then be silently removed — no retry, no client
    // notification.

    use crate::config::{GlobalRng, GlobalSimulationTime};

    /// Verify that a transaction created 25s ago would be selected as a
    /// retry candidate by the GC task's threshold check.
    ///
    /// Removing the GC retry logic from op_state_manager.rs would make this
    /// test's scenario moot — the elapsed check would never be performed
    /// and stuck GETs would silently expire.
    #[test]
    fn gc_retry_candidate_selected_after_threshold() {
        // Set deterministic simulation time
        GlobalSimulationTime::set_time_ms(1_700_000_000_000);
        GlobalRng::set_seed(0x6C01);

        // Create a transaction (records creation timestamp via current_time_ms)
        let tx = Transaction::new::<GetMsg>();
        // Advance simulation time by 25 seconds (past the 20s threshold).
        // Creation timestamp is ~1_700_000_000_000 (base + small counter offset).
        GlobalSimulationTime::set_time_ms(1_700_000_000_000 + 25_000);

        let elapsed = tx.elapsed();
        assert!(
            elapsed > std::time::Duration::from_secs(20),
            "Transaction should show >20s elapsed after time advancement, got {:?}",
            elapsed
        );

        // This is what the GC task checks: elapsed > threshold * (retry_count + 1)
        let threshold = std::time::Duration::from_secs(20);
        let retry_count = 0u32;
        let base = threshold * (retry_count + 1);
        // With jitter factor 0.8-1.2, the threshold ranges from 16s to 24s
        // At 25s elapsed, we're past even the worst-case jitter
        assert!(
            elapsed > base.mul_f64(1.2),
            "Should exceed even worst-case jittered threshold"
        );
    }

    /// Verify that a transaction created only 10s ago would NOT be selected
    /// as a retry candidate.
    #[test]
    fn gc_retry_candidate_not_selected_before_threshold() {
        GlobalSimulationTime::set_time_ms(1_700_000_000_000);
        GlobalRng::set_seed(0x6C02);

        let tx = Transaction::new::<GetMsg>();
        // Only 10 seconds — below the 20s threshold
        GlobalSimulationTime::set_time_ms(1_700_000_000_000 + 10_000);

        let elapsed = tx.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(20),
            "Transaction should show <20s elapsed, got {:?}",
            elapsed
        );
    }

    /// Full GC retry flow: stuck GET → time passes → retry with alternative.
    ///
    /// Reproduces the exact flow from garbage_cleanup_task:
    ///   1. GET op created, sent to peer, waiting for response
    ///   2. 25 seconds pass with no response
    ///   3. GC task selects it as retry candidate
    ///   4. retry_with_next_alternative produces a new Request message
    ///   5. Op is re-inserted with updated state
    ///
    /// Without the fix: step 4 doesn't exist — the op sits until TTL expiry.
    /// Removing retry_with_next_alternative → this test fails to compile.
    /// Removing the GC retry logic → step 3-5 never happen in production.
    #[test]
    fn gc_retry_full_flow_stuck_get_retries_with_alternative() {
        GlobalSimulationTime::set_time_ms(1_700_000_000_000);
        GlobalRng::set_seed(0x6C03);

        // Create a GET op with 2 alternative peers
        let alt1 = make_peer(4001);
        let alt2 = make_peer(4002);
        let op = make_awaiting_op(vec![alt1.clone(), alt2], &[]);
        let tx = op.id;

        // Simulate the op being stuck: advance time by 25 seconds
        GlobalSimulationTime::set_time_ms(1_700_000_000_000 + 25_000);

        // Verify the GC task would select this as a retry candidate
        assert!(
            tx.elapsed() > std::time::Duration::from_secs(20),
            "Op should be eligible for retry"
        );

        // GC task calls retry_with_next_alternative (this IS the fix)
        let max_htl = 7;
        let all_connected: Vec<PeerKeyLocation> = vec![]; // no DBF needed, has alternatives
        let result = op.retry_with_next_alternative(max_htl, &all_connected);

        // With the fix: produces a retry message targeting alt1
        let (new_op, msg) = result.unwrap_or_else(|_| {
            panic!("GC retry should produce a message when alternatives exist")
        });

        // Verify the retry message uses reduced HTL (not full max_htl).
        // make_awaiting_op sets attempts_at_hop=1, retry increments to 2 → 7/2=3.
        match &msg {
            GetMsg::Request { htl, .. } => {
                assert!(
                    *htl <= max_htl,
                    "Retry HTL ({}) should not exceed max_htl ({})",
                    htl,
                    max_htl
                );
                assert!(
                    *htl >= MIN_RETRY_HTL,
                    "Retry HTL ({}) should be at least MIN_RETRY_HTL ({})",
                    htl,
                    MIN_RETRY_HTL
                );
            }
            GetMsg::Response { .. }
            | GetMsg::ResponseStreaming { .. }
            | GetMsg::ResponseStreamingAck { .. }
            | GetMsg::ForwardingAck { .. } => panic!("Expected Request"),
        }

        // Verify the op state was updated: alt1 is now the next_hop, alt2 remains
        if let Some(GetState::AwaitingResponse(data)) = &new_op.state {
            assert_eq!(
                data.next_hop, alt1,
                "Should be targeting the first alternative"
            );
            assert_eq!(data.alternatives.len(), 1, "One alternative remaining");
            assert!(
                data.tried_peers.contains(&alt1.socket_addr().unwrap()),
                "alt1 should be in tried_peers"
            );
        } else {
            panic!("Expected AwaitingResponse state after retry");
        }
    }

    /// GC retry with DBF fallback: stuck GET, all alternatives exhausted,
    /// broadens search to all connected peers.
    ///
    /// Without the DBF fallback fix: when alternatives are empty, the retry
    /// returns Err and the op stays stuck until TTL expiry.
    /// With the fix: fallback_peers (from ring.get_connections_by_location)
    /// are injected as new alternatives.
    #[test]
    fn gc_retry_dbf_fallback_broadens_to_connected_peers() {
        GlobalSimulationTime::set_time_ms(1_700_000_000_000);
        GlobalRng::set_seed(0x6C04);

        // Create a GET op with NO alternatives (all exhausted)
        let op = make_awaiting_op(vec![], &[]);

        // Without DBF fallback: retry fails (no alternatives)
        let fallback_peers: Vec<PeerKeyLocation> = vec![];
        let op_clone_for_no_fallback = make_awaiting_op(vec![], &[]);
        let result = op_clone_for_no_fallback.retry_with_next_alternative(7, &fallback_peers);
        assert!(result.is_err(), "Without fallback peers, retry should fail");

        // WITH DBF fallback: provide connected peers that haven't been tried
        let fallback1 = make_peer(5001);
        let fallback2 = make_peer(5002);
        let fallback_peers = vec![fallback1.clone(), fallback2];

        let result = op.retry_with_next_alternative(7, &fallback_peers);
        let (new_op, msg) =
            result.unwrap_or_else(|_| panic!("DBF fallback should inject new peers and retry"));

        // Verify the retry targets a fallback peer
        assert!(matches!(msg, GetMsg::Request { .. }));

        if let Some(GetState::AwaitingResponse(data)) = &new_op.state {
            assert!(
                data.tried_peers.contains(&fallback1.socket_addr().unwrap()),
                "Fallback peer should be in tried_peers"
            );
            // One fallback was used, one remains as alternative
            assert_eq!(
                data.alternatives.len(),
                1,
                "Second fallback should remain as alternative"
            );
        } else {
            panic!("Expected AwaitingResponse state");
        }
    }

    /// Verify that successive GC retries advance the threshold multiplicatively.
    ///
    /// The GC task uses: threshold = GET_RETRY_THRESHOLD * (retry_count + 1)
    ///   retry 0: fires at ~20s
    ///   retry 1: fires at ~40s
    ///   retry 2: fires at ~60s (near TTL expiry)
    ///
    /// This prevents rapid-fire retries from overwhelming the network.
    #[test]
    fn gc_retry_threshold_scales_with_retry_count() {
        GlobalSimulationTime::set_time_ms(1_700_000_000_000);
        GlobalRng::set_seed(0x6C05);

        let tx = Transaction::new::<GetMsg>();
        // Transaction was created at ~1_700_000_000_000 (base + tiny counter offset)
        let base_ms = 1_700_000_000_000u64;

        // ACK_TIMEOUT is 3s (matches op_state_manager.rs GC task)
        let threshold = std::time::Duration::from_secs(3);

        // At 4s: retry_count=0 threshold=3s → eligible
        GlobalSimulationTime::set_time_ms(base_ms + 4_000);
        let base_0 = threshold * 1;
        assert!(tx.elapsed() > base_0, "First retry should fire at ~3s");

        // At 4s: retry_count=1 threshold=6s → NOT eligible
        let base_1 = threshold * 2;
        assert!(
            tx.elapsed() < base_1,
            "Second retry should NOT fire at 4s (needs ~6s)"
        );

        // At 7s: retry_count=1 threshold=6s → eligible
        GlobalSimulationTime::set_time_ms(base_ms + 7_000);
        assert!(tx.elapsed() > base_1, "Second retry should fire at ~6s");

        // At 7s: retry_count=2 threshold=9s → NOT eligible
        let base_2 = threshold * 3;
        assert!(
            tx.elapsed() < base_2,
            "Third retry should NOT fire at 7s (needs ~9s)"
        );
    }

    // === Tests for ForwardingAck serde round-trip ===

    #[test]
    fn forwarding_ack_serde_roundtrip() {
        let id = Transaction::new::<GetMsg>();
        let instance_id = ContractInstanceId::new([42; 32]);
        let msg = GetMsg::ForwardingAck { id, instance_id };

        let serialized = bincode::serialize(&msg).expect("serialize");
        let deserialized: GetMsg = bincode::deserialize(&serialized).expect("deserialize");

        match deserialized {
            GetMsg::ForwardingAck {
                id: deser_id,
                instance_id: deser_iid,
            } => {
                assert_eq!(deser_id, id);
                assert_eq!(deser_iid, instance_id);
            }
            other @ GetMsg::Request { .. }
            | other @ GetMsg::Response { .. }
            | other @ GetMsg::ResponseStreaming { .. }
            | other @ GetMsg::ResponseStreamingAck { .. } => {
                panic!("Expected ForwardingAck, got {other}")
            }
        }
    }

    // === Tests for ACK-aware GC retry logic ===
    //
    // These tests replicate the GC task's decision logic from op_state_manager.rs
    // to verify the retry/skip conditions at the unit level.

    /// Helper that replicates the GC task's retry eligibility check for a GET op.
    /// Returns true if the op would be selected as a retry candidate.
    fn gc_would_retry(op: &GetOp, retry_count: usize) -> bool {
        const ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
        const PROGRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(7);
        const MAX_SPECULATIVE_PATHS: u8 = 2;

        // Must be originator
        if !op.is_client_initiated() {
            return false;
        }
        // Capped speculative paths
        if op.speculative_paths >= MAX_SPECULATIVE_PATHS {
            return false;
        }
        let elapsed = op.id.elapsed();

        if op.ack_received {
            // ACK received — trust chain for PROGRESS_TIMEOUT, then re-enable (#3570)
            if elapsed <= PROGRESS_TIMEOUT {
                return false;
            }
            // Chain stalled past PROGRESS_TIMEOUT — eligible for retry
            let base = PROGRESS_TIMEOUT + ACK_TIMEOUT * (retry_count as u32);
            return elapsed > base;
        }

        // No ACK — check against ACK_TIMEOUT (without jitter for deterministic testing)
        let base = ACK_TIMEOUT * (retry_count as u32 + 1);
        elapsed > base
    }

    #[test]
    fn ack_received_prevents_gc_retry_within_progress_timeout() {
        use crate::config::GlobalSimulationTime;

        let base_ms = 1_700_000_000_000;
        GlobalSimulationTime::set_time_ms(base_ms);

        let mut op = make_awaiting_op(vec![make_peer(5001)], &[]);
        op.ack_received = true;

        // Advance time past ACK_TIMEOUT (3s) but within PROGRESS_TIMEOUT (7s)
        GlobalSimulationTime::set_time_ms(base_ms + 5_000);

        assert!(
            !gc_would_retry(&op, 0),
            "Op with ack_received should NOT be retried within PROGRESS_TIMEOUT"
        );
    }

    #[test]
    fn ack_received_allows_retry_after_progress_timeout() {
        use crate::config::GlobalSimulationTime;

        let base_ms = 1_700_000_000_000;
        GlobalSimulationTime::set_time_ms(base_ms);

        let mut op = make_awaiting_op(vec![make_peer(5001)], &[]);
        op.ack_received = true;

        // Advance time past PROGRESS_TIMEOUT (7s) + ACK_TIMEOUT (3s) for jitter headroom
        GlobalSimulationTime::set_time_ms(base_ms + 12_000);

        assert!(
            gc_would_retry(&op, 0),
            "Op with ack_received SHOULD be retried after PROGRESS_TIMEOUT (#3570)"
        );
    }

    #[test]
    fn speculative_paths_caps_at_max() {
        use crate::config::GlobalSimulationTime;

        let base_ms = 1_700_000_000_000;
        GlobalSimulationTime::set_time_ms(base_ms);

        let mut op = make_awaiting_op(vec![make_peer(5001), make_peer(5002)], &[]);
        op.speculative_paths = 2; // MAX_SPECULATIVE_PATHS

        GlobalSimulationTime::set_time_ms(base_ms + 10_000);

        assert!(
            !gc_would_retry(&op, 0),
            "Op at MAX_SPECULATIVE_PATHS should NOT be retried by GC"
        );
    }

    #[test]
    fn no_ack_allows_gc_retry_after_timeout() {
        use crate::config::GlobalSimulationTime;

        let base_ms = 1_700_000_000_000;
        GlobalSimulationTime::set_time_ms(base_ms);

        let op = make_awaiting_op(vec![make_peer(5001)], &[]);
        assert!(!op.ack_received);
        assert_eq!(op.speculative_paths, 0);

        // Before ACK_TIMEOUT — should NOT retry
        GlobalSimulationTime::set_time_ms(base_ms + 2_000);
        assert!(
            !gc_would_retry(&op, 0),
            "Op should NOT be retried before ACK_TIMEOUT (3s)"
        );

        // After ACK_TIMEOUT — SHOULD retry
        GlobalSimulationTime::set_time_ms(base_ms + 4_000);
        assert!(
            gc_would_retry(&op, 0),
            "Op without ACK should be retried after 3s"
        );

        // And retry should succeed since alternatives exist
        let result = op.retry_with_next_alternative(7, &[]);
        assert!(result.is_ok(), "Should retry with available alternative");
    }

    #[test]
    fn gc_retry_increments_speculative_paths_and_resets_ack() {
        use crate::config::GlobalSimulationTime;

        let base_ms = 1_700_000_000_000;
        GlobalSimulationTime::set_time_ms(base_ms);

        let op = make_awaiting_op(vec![make_peer(5001), make_peer(5002)], &[]);
        assert_eq!(op.speculative_paths, 0);
        assert!(!op.ack_received);

        // Simulate first retry (as GC task does)
        GlobalSimulationTime::set_time_ms(base_ms + 4_000);
        let (mut new_op, _msg) = op
            .retry_with_next_alternative(7, &[])
            .map_err(|_| "retry_with_next_alternative failed")
            .unwrap();
        new_op.speculative_paths += 1;
        new_op.ack_received = false;

        assert_eq!(new_op.speculative_paths, 1);
        assert!(!new_op.ack_received);

        // After ACK arrives on the new path
        new_op.ack_received = true;
        assert!(
            !gc_would_retry(&new_op, 1),
            "Op with ACK on new path should NOT be retried"
        );
    }

    // ── Intermediate node stats tracking tests (#3527) ─────────────────────

    use crate::ring::Location;

    fn make_get_op_with_stats(stats: Option<Box<GetStats>>) -> GetOp {
        GetOp {
            id: Transaction::new::<GetMsg>(),
            state: Some(GetState::ReceivedRequest),
            result: None,
            stats,
            upstream_addr: None,
            local_fallback: None,
            auto_fetch: false,
            ack_received: false,
            speculative_paths: 0,
        }
    }

    fn make_get_stats(
        target: PeerKeyLocation,
        contract_location: Location,
    ) -> Option<Box<GetStats>> {
        Some(Box::new(GetStats {
            next_peer: Some(target),
            contract_location,
            first_response_time: None,
            transfer_time: None,
        }))
    }

    /// Non-finalized GET with stats reports ContractOpFailure on timeout.
    #[test]
    fn test_get_failure_outcome_with_stats() {
        let target = make_peer(9001);
        let contract_location = Location::random();
        let op = make_get_op_with_stats(make_get_stats(target.clone(), contract_location));

        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer,
                contract_location: loc,
            } => {
                assert_eq!(target_peer, &target);
                assert_eq!(loc, contract_location);
            }
            other => panic!("Expected ContractOpFailure, got {other:?}"),
        }
    }

    /// Non-finalized GET without stats reports Incomplete.
    #[test]
    fn test_get_failure_outcome_without_stats() {
        let op = make_get_op_with_stats(None);
        assert!(
            matches!(op.outcome(), OpOutcome::Incomplete),
            "GET without stats should return Incomplete"
        );
    }

    /// failure_routing_info() returns correct peer and location from stats.
    #[test]
    fn test_get_failure_routing_info() {
        let target = make_peer(9002);
        let contract_location = Location::random();
        let op = make_get_op_with_stats(make_get_stats(target.clone(), contract_location));

        let (peer, loc) = op.failure_routing_info().expect("should have routing info");
        assert_eq!(peer, target);
        assert_eq!(loc, contract_location);
    }
}
