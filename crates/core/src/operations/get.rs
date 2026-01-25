use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;
use std::collections::HashSet;
use std::fmt::Display;
use std::pin::Pin;
use std::{future::Future, time::Instant};

use crate::client_events::HostResult;
use crate::node::IsOperationCompleted;
use crate::{
    contract::{ContractHandlerEvent, StoreResponse},
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager},
    operations::{OpInitialization, Operation},
    ring::{Location, PeerKeyLocation, RingError},
    tracing::{state_hash_full, NetEventLog, OperationFailure},
};
use either::Either;

use super::orphan_streams::STREAM_CLAIM_TIMEOUT;
use super::{OpEnum, OpError, OpOutcome, OperationResult};

pub(crate) use self::messages::{GetMsg, GetMsgResult};
// GetStreamingPayload is used for serialization by senders, not receivers

/// Maximum number of retries to get values.
const MAX_RETRIES: usize = 10;

/// Maximum number of peer attempts at each hop level
const DEFAULT_MAX_BREADTH: usize = 3;

pub(crate) fn start_op(
    instance_id: ContractInstanceId,
    fetch_contract: bool,
    subscribe: bool,
) -> GetOp {
    let contract_location = Location::from(&instance_id);
    let id = Transaction::new::<GetMsg>();
    tracing::debug!(tx = %id, "Requesting get contract {instance_id} @ loc({contract_location})");
    let state = Some(GetState::PrepareRequest {
        instance_id,
        id,
        fetch_contract,
        subscribe,
    });
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
    }
}

/// Create a GET operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(
    instance_id: ContractInstanceId,
    fetch_contract: bool,
    subscribe: bool,
    id: Transaction,
) -> GetOp {
    let contract_location = Location::from(&instance_id);
    tracing::debug!(tx = %id, "Requesting get contract {instance_id} @ loc({contract_location}) with existing transaction ID");
    let state = Some(GetState::PrepareRequest {
        instance_id,
        id,
        fetch_contract,
        subscribe,
    });
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
    }
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get(
    op_manager: &OpManager,
    get_op: GetOp,
    visited: super::VisitedPeers,
) -> Result<(), OpError> {
    let (mut candidates, id, instance_id_val, _fetch_contract, local_fallback) = if let Some(
        GetState::PrepareRequest {
            instance_id,
            id,
            fetch_contract,
            ..
        },
    ) =
        &get_op.state
    {
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
                    tracing::debug!(
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
        let candidates = op_manager.ring.k_closest_potentially_caching(
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
                    state: Some(GetState::Finished { key }),
                    result: Some(GetResult {
                        key,
                        state,
                        contract,
                    }),
                    stats: get_op.stats,
                    upstream_addr: get_op.upstream_addr,
                    local_fallback: None,
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
        Some(GetState::PrepareRequest {
            fetch_contract,
            instance_id: _,
            id: _,
            subscribe,
        }) => {
            let mut tried_peers = HashSet::new();
            if let Some(addr) = target.socket_addr() {
                tried_peers.insert(addr);
            }

            let new_state = Some(GetState::AwaitingResponse {
                instance_id: instance_id_val,
                retries: 0,
                fetch_contract,
                requester: None,
                current_hop: op_manager.ring.max_hops_to_live,
                subscribe,
                next_hop: target.clone(),
                tried_peers,
                alternatives: candidates,
                attempts_at_hop: 1,
                visited: visited.clone(),
            });

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

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum GetState {
    /// A new petition for a get op received from another peer.
    /// Note: We use GetOp::upstream_addr for response routing (not PeerKeyLocation).
    ReceivedRequest,
    /// Preparing request for get op.
    PrepareRequest {
        instance_id: ContractInstanceId,
        id: Transaction,
        fetch_contract: bool,
        subscribe: bool,
    },
    /// Awaiting response from petition.
    AwaitingResponse {
        /// Contract being fetched (by instance_id since we may not have full key yet)
        instance_id: ContractInstanceId,
        /// If specified the peer waiting for the response upstream
        requester: Option<PeerKeyLocation>,
        fetch_contract: bool,
        retries: usize,
        current_hop: usize,
        subscribe: bool,
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
    },
    /// Operation completed successfully
    Finished { key: ContractKey },
}

impl Display for GetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetState::ReceivedRequest => write!(f, "ReceivedRequest"),
            GetState::PrepareRequest {
                instance_id,
                id,
                fetch_contract,
                subscribe,
            } => {
                write!(
                    f,
                    "PrepareRequest(instance_id: {instance_id}, id: {id}, fetch_contract: {fetch_contract}, subscribe: {subscribe})"
                )
            }
            GetState::AwaitingResponse {
                requester,
                fetch_contract,
                retries,
                current_hop,
                subscribe,
                ..
            } => {
                write!(f, "AwaitingResponse(requester: {requester:?}, fetch_contract: {fetch_contract}, retries: {retries}, current_hop: {current_hop}, subscribe: {subscribe})")
            }
            GetState::Finished { key, .. } => write!(f, "Finished(key: {key})"),
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
        } else {
            OpOutcome::Incomplete
        }
    }

    /// Handle aborted outbound connections by directly retrying with alternative peers.
    pub(crate) async fn handle_abort(mut self, op_manager: &OpManager) -> Result<(), OpError> {
        if let Some(GetState::AwaitingResponse {
            instance_id,
            requester,
            fetch_contract,
            retries,
            current_hop,
            subscribe,
            next_hop: failed_peer,
            mut tried_peers,
            mut alternatives,
            attempts_at_hop,
            visited,
        }) = self.state.take()
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

                let new_state = Some(GetState::AwaitingResponse {
                    instance_id,
                    requester,
                    fetch_contract,
                    retries,
                    current_hop,
                    subscribe,
                    next_hop: next_target,
                    tried_peers,
                    alternatives,
                    attempts_at_hop: attempts_at_hop + 1,
                    visited: visited.clone(),
                });

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

                let mut new_candidates = op_manager.ring.k_closest_potentially_caching(
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

                    let new_state = Some(GetState::AwaitingResponse {
                        instance_id,
                        requester,
                        fetch_contract,
                        retries: retries + 1,
                        current_hop,
                        subscribe,
                        next_hop: next_target,
                        tried_peers: new_tried_peers,
                        alternatives: new_candidates,
                        attempts_at_hop: 1,
                        visited: new_visited.clone(),
                    });

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
                    state: Some(GetState::Finished { key }),
                    result: Some(GetResult {
                        key,
                        state,
                        contract,
                    }),
                    stats: self.stats,
                    upstream_addr: self.upstream_addr,
                    local_fallback: None,
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

                let response_op = GetOp {
                    id: self.id,
                    state: None,
                    result: None,
                    stats: self.stats,
                    upstream_addr: self.upstream_addr,
                    local_fallback: None,
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
        self.result.is_some() && matches!(self.state, Some(GetState::Finished { .. }))
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
            Some(GetState::AwaitingResponse { next_hop, .. }) => next_hop.socket_addr(),
            _ => None,
        }
    }

    /// Get the current hop count (remaining HTL) for this operation.
    /// Returns None if the operation is not in AwaitingResponse state.
    pub(crate) fn get_current_hop(&self) -> Option<usize> {
        match &self.state {
            Some(GetState::AwaitingResponse { current_hop, .. }) => Some(*current_hop),
            _ => None,
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
                let _ = op_manager.push(tx, op).await;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                // Check if this is a response message - if so, the operation was likely
                // cleaned up due to timeout and we should not create a new operation
                if matches!(msg, GetMsg::Response { .. }) {
                    tracing::debug!(
                        tx = %tx,
                        phase = "load_or_init",
                        "GET response arrived for non-existent operation (likely timed out)"
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
        _conn_manager: &'a mut NB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            // Take local_fallback early since self will be partially moved
            let mut local_fallback = self.local_fallback;
            #[allow(unused_assignments)]
            let mut return_msg = None;
            #[allow(unused_assignments)]
            let mut new_state = None;
            let mut result = None;
            let mut stats = self.stats;

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
                    if matches!(self.state, Some(GetState::Finished { .. })) {
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
                        return build_op_result(
                            id,
                            None,
                            Some(GetMsg::Response {
                                id,
                                instance_id,
                                result: GetMsgResult::NotFound,
                            }),
                            None,
                            stats,
                            self.upstream_addr,
                        );
                    } else {
                        // Normal case: operation should be in ReceivedRequest or AwaitingResponse state
                        debug_assert!(matches!(
                            self.state,
                            Some(GetState::ReceivedRequest)
                                | Some(GetState::AwaitingResponse { .. })
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
                                    tracing::debug!(
                                        tx = %id,
                                        %instance_id,
                                        %this_peer,
                                        "GET: state available locally but contract code missing; continuing search"
                                    );
                                    None
                                } else {
                                    Some((key, state, contract))
                                }
                            }
                            _ => None,
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

                            // Check if this is a forwarded request or a local request
                            // Use upstream_addr (the actual socket address) not requester (PeerKeyLocation lookup)
                            // because get_peer_location_by_addr() can fail for transient connections
                            match &self.state {
                                Some(GetState::ReceivedRequest) if self.upstream_addr.is_some() => {
                                    // This is a forwarded request - send result back to upstream
                                    tracing::debug!(tx = %id, "Returning contract {} to upstream", key);
                                    new_state = None;
                                    return_msg = Some(GetMsg::Response {
                                        id,
                                        instance_id: *key.id(),
                                        result: GetMsgResult::Found {
                                            key,
                                            value: StoreResponse {
                                                state: Some(state),
                                                contract,
                                            },
                                        },
                                    });
                                }
                                Some(GetState::AwaitingResponse { .. })
                                    if self.upstream_addr.is_some() =>
                                {
                                    // Forward contract to upstream
                                    new_state = None;
                                    tracing::debug!(tx = %id, "Returning contract {} to upstream", key);
                                    return_msg = Some(GetMsg::Response {
                                        id,
                                        instance_id: *key.id(),
                                        result: GetMsgResult::Found {
                                            key,
                                            value: StoreResponse {
                                                state: Some(state),
                                                contract,
                                            },
                                        },
                                    });
                                }
                                Some(GetState::AwaitingResponse {
                                    requester: None, ..
                                }) => {
                                    // Operation completed for original requester
                                    tracing::debug!(
                                        tx = %id,
                                        "Completed operation, get response received for contract {key}"
                                    );
                                    new_state = Some(GetState::Finished { key });
                                    return_msg = None;
                                    result = Some(GetResult {
                                        key,
                                        state,
                                        contract,
                                    });
                                }
                                _ => {
                                    // This is the original requester (locally initiated request)
                                    new_state = Some(GetState::Finished { key });
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
                        Some(GetState::AwaitingResponse {
                            fetch_contract,
                            retries,
                            requester,
                            current_hop,
                            subscribe,
                            mut tried_peers,
                            mut alternatives,
                            attempts_at_hop,
                            next_hop: _,
                            visited,
                            instance_id: state_instance_id,
                            ..
                        }) => {
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
                                new_state = Some(GetState::AwaitingResponse {
                                    retries,
                                    fetch_contract,
                                    requester: requester.clone(),
                                    current_hop,
                                    subscribe,
                                    tried_peers: updated_tried_peers.clone(),
                                    alternatives,
                                    attempts_at_hop: attempts_at_hop + 1,
                                    instance_id,
                                    next_hop: next_target,
                                    // Preserve the accumulated visited so future candidate
                                    // selection still avoids already-specified peers; tried_peers
                                    // tracks attempts at this hop.
                                    visited: visited.clone(),
                                });
                            } else if retries < MAX_RETRIES {
                                // No more alternatives at this hop, try finding new peers
                                let mut new_visited = visited.clone();
                                for addr in &tried_peers {
                                    new_visited.mark_visited(*addr);
                                }

                                // Get new candidates excluding all tried peers
                                let mut new_candidates =
                                    op_manager.ring.k_closest_potentially_caching(
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

                                    new_state = Some(GetState::AwaitingResponse {
                                        retries: retries + 1,
                                        fetch_contract,
                                        requester: requester.clone(),
                                        current_hop,
                                        subscribe,
                                        tried_peers: new_tried_peers,
                                        alternatives: new_candidates,
                                        attempts_at_hop: 1,
                                        instance_id,
                                        next_hop: target,
                                        visited: new_visited.clone(),
                                    });
                                } else if let Some(requester_peer) = requester.clone() {
                                    // No more peers to try, return NotFound to requester
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
                                } else {
                                    // Original requester - check for local fallback before failing
                                    if let Some((key, state, contract)) = local_fallback.take() {
                                        tracing::info!(
                                            tx = %id,
                                            contract = %key,
                                            phase = "complete",
                                            "GET: Network returned NotFound, returning local cache and re-seeding network"
                                        );

                                        // Complete with local cached version
                                        new_state = Some(GetState::Finished { key });
                                        return_msg = None;
                                        result = Some(GetResult {
                                            key,
                                            state: state.clone(),
                                            contract: contract.clone(),
                                        });

                                        // Re-seed the network with our local copy
                                        if let Some(contract_code) = contract {
                                            tracing::info!(
                                                tx = %id,
                                                %key,
                                                "Re-seeding network after NotFound response"
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
                                                    tracing::debug!(tx = %id, %key, "Re-seeded contract to network");
                                                    super::announce_contract_cached(
                                                        op_manager, &key,
                                                    )
                                                    .await;
                                                }
                                                _ => {
                                                    tracing::warn!(tx = %id, %key, "Failed to re-seed contract");
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

                                        // Set result to None - to_host_result will return an error
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
                                    // Return NotFound to requester
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
                                    new_state = None;
                                } else {
                                    // Original requester - check for local fallback before failing
                                    if let Some((key, state, contract)) = local_fallback.take() {
                                        tracing::info!(
                                            tx = %id,
                                            contract = %key,
                                            phase = "complete",
                                            "GET: Max retries reached, returning local cache and re-seeding network"
                                        );

                                        // Complete with local cached version
                                        new_state = Some(GetState::Finished { key });
                                        return_msg = None;
                                        result = Some(GetResult {
                                            key,
                                            state: state.clone(),
                                            contract: contract.clone(),
                                        });

                                        // Re-seed the network with our local copy
                                        if let Some(contract_code) = contract {
                                            tracing::info!(
                                                tx = %id,
                                                %key,
                                                "Re-seeding network after NotFound (max retries)"
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
                                                    tracing::debug!(tx = %id, %key, "Re-seeded contract to network");
                                                    super::announce_contract_cached(
                                                        op_manager, &key,
                                                    )
                                                    .await;
                                                }
                                                _ => {
                                                    tracing::warn!(tx = %id, %key, "Failed to re-seed contract");
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
                        Some(GetState::AwaitingResponse {
                            fetch_contract: true,
                            ..
                        })
                    );

                    // Get requester and current_hop from current state
                    let (requester, current_hop) = if let Some(GetState::AwaitingResponse {
                        requester,
                        current_hop,
                        ..
                    }) = self.state.as_ref()
                    {
                        (requester.clone(), Some(*current_hop))
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
                                    }),
                                )
                                .await?;
                            return Err(OpError::StatePushed);
                        }
                    }

                    // Check if this is the original requester (no upstream to forward to)
                    let is_original_requester = self.upstream_addr.is_none();

                    // Check if subscription was requested
                    let subscribe_requested =
                        if let Some(GetState::AwaitingResponse { subscribe, .. }) = &self.state {
                            *subscribe
                        } else {
                            false
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
                            // Previously, we only recorded access if !is_seeding_contract(),
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
                                    .unregister_local_seeding(evicted_key)
                                {
                                    removed_contracts.push(*evicted_key);
                                }
                            }

                            // Only do first-time hosting setup if newly hosting
                            if access_result.is_new {
                                tracing::debug!(tx = %id, %key, "Contract newly hosted");
                                super::announce_contract_cached(op_manager, &key).await;

                                // Register local interest for delta-based sync
                                let became_interested =
                                    op_manager.interest_manager.register_local_seeding(&key);

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

                            // Auto-subscribe to receive updates for this contract
                            // record_get_access already refreshed the hosting cache above
                            if crate::ring::AUTO_SUBSCRIBE_ON_GET {
                                // Only start new subscription if not already subscribed
                                if access_result.is_new || !op_manager.ring.is_subscribed(&key) {
                                    let child_tx = super::start_subscription_request(
                                        op_manager, id, key, false,
                                    );
                                    tracing::debug!(tx = %id, %child_tx, blocking = false, "started subscription");
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
                                    ContractHandlerEvent::PutResponse { new_value: Ok(_), .. } => {
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
                                                .unregister_local_seeding(evicted_key)
                                            {
                                                removed_contracts.push(*evicted_key);
                                            }
                                        }

                                        // Only do first-time hosting setup if newly hosting
                                        if access_result.is_new {
                                            super::announce_contract_cached(op_manager, &key).await;

                                            // Register local interest for delta-based sync
                                            let became_interested = op_manager
                                                .interest_manager
                                                .register_local_seeding(&key);

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

                                        // Auto-subscribe to receive updates for this contract
                                        // record_get_access already refreshed the hosting cache above
                                        if crate::ring::AUTO_SUBSCRIBE_ON_GET {
                                            // Only start new subscription if not already subscribed
                                            if access_result.is_new || !op_manager.ring.is_subscribed(&key) {
                                                let child_tx =
                                                    super::start_subscription_request(op_manager, id, key, false);
                                                tracing::debug!(tx = %id, %child_tx, blocking = false, "started subscription");
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
                                    _ => unreachable!(
                                        "PutQuery from Get operation should always return PutResponse"
                                    ),
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
                        Some(GetState::AwaitingResponse { .. })
                        | Some(GetState::ReceivedRequest) => {}
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

                        new_state = Some(GetState::Finished { key });
                        return_msg = None;
                        result = Some(GetResult {
                            key,
                            state: value.clone(),
                            contract: contract.clone(),
                        });
                    } else {
                        // Forward response to upstream
                        tracing::info!(tx = %id, contract = %key, phase = "response", "Get response received for contract at hop peer");
                        new_state = None;
                        return_msg = Some(GetMsg::Response {
                            id,
                            instance_id: *key.id(),
                            result: GetMsgResult::Found {
                                key,
                                value: StoreResponse {
                                    state: Some(value.clone()),
                                    contract: contract.clone(),
                                },
                            },
                        });
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

                    // Check if streaming is enabled at runtime
                    if !op_manager.streaming_enabled {
                        tracing::warn!(
                            tx = %id,
                            %instance_id,
                            contract = %key,
                            stream_id = %stream_id,
                            "GET ResponseStreaming received but streaming is disabled"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }

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

                    // Step 1: Claim the stream from orphan registry
                    let stream_handle = match op_manager
                        .orphan_stream_registry()
                        .claim_or_wait(stream_id, STREAM_CLAIM_TIMEOUT)
                        .await
                    {
                        Ok(handle) => handle,
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to claim stream from orphan registry"
                            );
                            return Err(OpError::OrphanStreamClaimFailed);
                        }
                    };

                    // Step 2: Wait for stream to complete and assemble data
                    let stream_data = match stream_handle.assemble().await {
                        Ok(data) => data,
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to assemble stream data"
                            );
                            return Err(OpError::StreamCancelled);
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

                    // Step 4: Check if this is the original requester
                    let is_original_requester = self.upstream_addr.is_none();

                    // Get current hop for telemetry
                    let current_hop =
                        if let Some(GetState::AwaitingResponse { current_hop, .. }) = &self.state {
                            Some(*current_hop)
                        } else {
                            None
                        };

                    // Step 5: Cache the contract locally (same as regular Response)
                    if let Some(state) = &value.state {
                        let contract_to_cache = if includes_contract {
                            value.contract.clone()
                        } else {
                            None
                        };

                        // Check if we already have this contract
                        let already_hosting = op_manager.ring.is_hosting_contract(&key);

                        if !already_hosting {
                            // Use put_query to cache the contract
                            let _ = op_manager
                                .notify_contract_handler(ContractHandlerEvent::PutQuery {
                                    key,
                                    state: state.clone(),
                                    related_contracts: RelatedContracts::default(),
                                    contract: contract_to_cache,
                                })
                                .await;
                        }

                        // BUG FIX (2026-01): ALWAYS refresh hosting status on GET.
                        // This keeps the TTL/LRU position fresh for already-hosted contracts.
                        op_manager.ring.record_get_access(key, state.size() as u64);
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
                        new_state = Some(GetState::Finished { key });
                        return_msg = None;
                    } else {
                        // Forward the response to upstream as a regular Response
                        // (streaming is only for large payloads, we've now received it)
                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            phase = "forward",
                            "Forwarding GET response to upstream"
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

                    // Check if streaming is enabled at runtime
                    if !op_manager.streaming_enabled {
                        tracing::warn!(
                            tx = %id,
                            stream_id = %stream_id,
                            "GET ResponseStreamingAck received but streaming is disabled"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }

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
            }

            build_op_result(
                self.id,
                new_state,
                return_msg,
                result,
                stats,
                self.upstream_addr,
            )
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<GetState>,
    msg: Option<GetMsg>,
    result: Option<GetResult>,
    stats: Option<Box<GetStats>>,
    upstream_addr: Option<std::net::SocketAddr>,
) -> Result<OperationResult, OpError> {
    // Determine the next hop for sending the message:
    // - For Response messages: route back to upstream_addr (who sent us the request)
    // - For Request messages being forwarded: use next_hop from state
    let next_hop = match (&msg, &state) {
        (Some(GetMsg::Response { .. }), _) => upstream_addr,
        (Some(GetMsg::Request { .. }), Some(GetState::AwaitingResponse { next_hop, .. })) => {
            next_hop.socket_addr()
        }
        _ => None,
    };

    let output_op = state.map(|state| GetOp {
        id,
        state: Some(state),
        result,
        stats,
        upstream_addr,
        local_fallback: None, // Forwarding operations don't have local fallback
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        next_hop,
        state: output_op.map(OpEnum::Get),
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
        let mut candidates = op_manager.ring.k_closest_potentially_caching(
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
        let mut tried_peers = HashSet::new();
        if let Some(addr) = target.socket_addr() {
            tried_peers.insert(addr);
        }

        build_op_result(
            id,
            Some(GetState::AwaitingResponse {
                instance_id,
                requester: sender,
                retries: 0,
                fetch_contract,
                current_hop: new_htl,
                subscribe: false,
                next_hop: target.clone(),
                tried_peers,
                alternatives,
                attempts_at_hop: 1,
                visited: new_visited.clone(),
            }),
            Some(GetMsg::Request {
                id,
                instance_id,
                fetch_contract,
                htl: new_htl,
                visited: new_visited,
            }),
            None,
            stats,
            upstream_addr,
        )
    } else if upstream_addr.is_some() {
        // No targets found and we don't have the contract - send NotFound back to requester
        tracing::warn!(
            tx = %id,
            %instance_id,
            phase = "not_found",
            "No peers to forward get request to, returning NotFound to upstream"
        );

        build_op_result(
            id,
            None,
            Some(GetMsg::Response {
                id,
                instance_id,
                result: GetMsgResult::NotFound,
            }),
            None,
            stats,
            upstream_addr,
        )
    } else {
        // Original requester with no forwarding targets - operation fails locally
        tracing::warn!(
            tx = %id,
            %instance_id,
            phase = "not_found",
            "No peers to forward get request to and no upstream - local operation fails"
        );

        build_op_result(id, None, None, None, stats, upstream_addr)
    }
}

impl IsOperationCompleted for GetOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(GetState::Finished { .. }))
    }
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
    }

    impl InnerMessage for GetMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::ResponseStreaming { id, .. }
                | Self::ResponseStreamingAck { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { instance_id, .. }
                | Self::Response { instance_id, .. }
                | Self::ResponseStreaming { instance_id, .. } => Some(Location::from(instance_id)),
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
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Transaction;
    use crate::operations::test_utils::make_contract_key;
    use crate::operations::VisitedPeers;

    fn make_get_op(state: Option<GetState>, result: Option<GetResult>) -> GetOp {
        GetOp {
            id: Transaction::new::<GetMsg>(),
            state,
            result,
            stats: None,
            upstream_addr: None,
            local_fallback: None,
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
        let op = make_get_op(Some(GetState::Finished { key }), Some(result));
        assert!(
            op.finalized(),
            "GetOp should be finalized when state is Finished and result is present"
        );
    }

    #[test]
    fn get_op_not_finalized_when_finished_without_result() {
        let key = make_contract_key(1);
        let op = make_get_op(Some(GetState::Finished { key }), None);
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
        let op = make_get_op(Some(GetState::Finished { key }), Some(result));
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
        let op = make_get_op(Some(GetState::Finished { key }), Some(result));
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
        let state = GetState::Finished {
            key: make_contract_key(1),
        };
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
}
