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
};

use super::{OpEnum, OpError, OpOutcome, OperationResult};

pub(crate) use self::messages::GetMsg;

/// Maximum number of retries to get values.
const MAX_RETRIES: usize = 10;

/// Maximum number of peer attempts at each hop level
const DEFAULT_MAX_BREADTH: usize = 3;

pub(crate) fn start_op(key: ContractKey, fetch_contract: bool, subscribe: bool) -> GetOp {
    let contract_location = Location::from(&key);
    let id = Transaction::new::<GetMsg>();
    tracing::debug!(tx = %id, "Requesting get contract {key} @ loc({contract_location})");
    let state = Some(GetState::PrepareRequest {
        key,
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
    }
}

/// Create a GET operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(
    key: ContractKey,
    fetch_contract: bool,
    subscribe: bool,
    id: Transaction,
) -> GetOp {
    let contract_location = Location::from(&key);
    tracing::debug!(tx = %id, "Requesting get contract {key} @ loc({contract_location}) with existing transaction ID");
    let state = Some(GetState::PrepareRequest {
        key,
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
    }
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get(
    op_manager: &OpManager,
    get_op: GetOp,
    skip_list: HashSet<std::net::SocketAddr>,
) -> Result<(), OpError> {
    let (mut candidates, id, key_val, _fetch_contract) = if let Some(GetState::PrepareRequest {
        key,
        id,
        fetch_contract,
        ..
    }) = &get_op.state
    {
        // CRITICAL: Always check local storage FIRST before querying peers.
        // This ensures that if a contract was just PUT to this node, a subsequent
        // GET from the same node will find it immediately rather than forwarding
        // to peers who don't have it yet.
        tracing::debug!(
            tx = %id,
            %key,
            "GET: Checking local storage first before peer lookup"
        );

        let get_result = op_manager
            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                key: *key,
                return_contract_code: *fetch_contract,
            })
            .await;

        let local_value = match get_result {
            Ok(ContractHandlerEvent::GetResponse {
                response:
                    Ok(StoreResponse {
                        state: Some(state),
                        contract,
                    }),
                ..
            }) => {
                if *fetch_contract && contract.is_none() {
                    tracing::debug!(
                        tx = %id,
                        %key,
                        "GET: state available locally but contract code missing; will query peers"
                    );
                    None
                } else {
                    Some((state, contract))
                }
            }
            _ => None,
        };

        if let Some((state, contract)) = local_value {
            // Contract found locally - complete the operation immediately
            tracing::info!(
                tx = %id,
                %key,
                "GET: contract found locally, returning immediately"
            );

            let completed_op = GetOp {
                id: *id,
                state: Some(GetState::Finished { key: *key }),
                result: Some(GetResult {
                    key: *key,
                    state,
                    contract,
                }),
                stats: get_op.stats,
                upstream_addr: get_op.upstream_addr,
            };

            op_manager.push(*id, OpEnum::Get(completed_op)).await?;
            return Ok(());
        }

        // Contract not found locally - find peers to query
        let candidates =
            op_manager
                .ring
                .k_closest_potentially_caching(key, &skip_list, DEFAULT_MAX_BREADTH);

        if candidates.is_empty() {
            // No peers available and contract not found locally
            tracing::warn!(
                tx = %id,
                %key,
                "GET: Contract not found locally and no peers available"
            );
            return Err(RingError::EmptyRing.into());
        }

        tracing::debug!(
            tx = %id,
            %key,
            peer_count = candidates.len(),
            "GET: Contract not found locally, will query {} peer(s)",
            candidates.len()
        );

        (candidates, *id, *key, *fetch_contract)
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
            key: _,
            id: _,
            subscribe,
        }) => {
            let mut tried_peers = HashSet::new();
            if let Some(addr) = target.socket_addr() {
                tried_peers.insert(addr);
            }

            let new_state = Some(GetState::AwaitingResponse {
                key: key_val,
                retries: 0,
                fetch_contract,
                requester: None,
                current_hop: op_manager.ring.max_hops_to_live,
                subscribe,
                current_target: target.clone(),
                tried_peers,
                alternatives: candidates,
                attempts_at_hop: 1,
                skip_list: skip_list.clone(),
            });

            let msg = GetMsg::Request {
                id,
                key: key_val,
                fetch_contract,
                htl: op_manager.ring.max_hops_to_live,
                skip_list,
            };

            let op = GetOp {
                id,
                state: new_state,
                result: None,
                stats: get_op.stats.map(|mut s| {
                    s.next_peer = Some(target);
                    s
                }),
                upstream_addr: get_op.upstream_addr,
            };

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
    /// The requester field stores who sent us this request, so we can send the result back.
    ReceivedRequest { requester: Option<PeerKeyLocation> },
    /// Preparing request for get op.
    PrepareRequest {
        key: ContractKey,
        id: Transaction,
        fetch_contract: bool,
        subscribe: bool,
    },
    /// Awaiting response from petition.
    AwaitingResponse {
        /// Contract being fetched
        key: ContractKey,
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
        current_target: PeerKeyLocation,
        /// Peers we've already tried at this hop level
        tried_peers: HashSet<std::net::SocketAddr>,
        /// Alternative peers we could still try at this hop
        alternatives: Vec<PeerKeyLocation>,
        /// How many peers we've tried at this hop
        attempts_at_hop: usize,
        /// Skip list used for the current hop
        skip_list: HashSet<std::net::SocketAddr>,
    },
    /// Operation completed successfully
    Finished { key: ContractKey },
}

impl Display for GetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetState::ReceivedRequest { .. } => write!(f, "ReceivedRequest"),
            GetState::PrepareRequest {
                key,
                id,
                fetch_contract,
                subscribe,
            } => {
                write!(
                    f,
                    "PrepareRequest(key: {key}, id: {id}, fetch_contract: {fetch_contract}, subscribe: {subscribe})"
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

    /// Handle aborted outbound connections by reusing the existing retry logic.
    pub(crate) async fn handle_abort(self, op_manager: &OpManager) -> Result<(), OpError> {
        if let Some(GetState::AwaitingResponse {
            key,
            current_target: _,
            skip_list: _,
            ..
        }) = &self.state
        {
            // We synthesize an empty Response back to ourselves to reuse the existing
            // fallback path that tries the next candidate. The state stays
            // AwaitingResponse so the retry logic can pick up from the stored
            // alternatives/skip list.
            let return_msg = GetMsg::Response {
                id: self.id,
                key: *key,
                value: StoreResponse {
                    state: None,
                    contract: None,
                },
            };

            op_manager
                .notify_op_change(NetMessage::from(return_msg), OpEnum::Get(self))
                .await?;
            return Err(OpError::StatePushed);
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

    /// Get the target address if this operation is in a state that needs to send
    /// an outbound message. Used for hop-by-hop routing.
    pub(crate) fn get_target_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.state {
            Some(GetState::AwaitingResponse { current_target, .. }) => current_target.socket_addr(),
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
                // new request to get a value for a contract, initialize the machine
                // Look up the requester's PeerKeyLocation from the source address
                // This replaces the sender field that was previously embedded in messages
                let requester = source_addr.and_then(|addr| {
                    op_manager
                        .ring
                        .connection_manager
                        .get_peer_location_by_addr(addr)
                });
                Ok(OpInitialization {
                    op: Self {
                        state: Some(GetState::ReceivedRequest { requester }),
                        id: tx,
                        result: None,
                        stats: None, // don't care about stats in target peers
                        upstream_addr: source_addr, // Connection-based routing: store who sent us this request
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
                    key,
                    id,
                    fetch_contract,
                    htl,
                    skip_list,
                } => {
                    // Handle GET request - either initial or forwarded
                    let ring_max_htl = op_manager.ring.max_hops_to_live.max(1);
                    let htl = (*htl).min(ring_max_htl);
                    let id = *id;
                    let key: ContractKey = *key;
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
                        %key,
                        sender = %sender_display,
                        fetch_contract,
                        htl,
                        skip = ?skip_list,
                        "GET: received Request"
                    );

                    // Use sender_from_addr (looked up from source_addr) instead of message field
                    let Some(sender) = sender_from_addr.clone() else {
                        tracing::warn!(
                            tx = %id,
                            %key,
                            "GET: Request without sender lookup - cannot process"
                        );
                        return Err(OpError::invalid_transition(self.id));
                    };

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
                        // HTL exhausted - return empty response
                        tracing::warn!(
                            tx = %id,
                            %key,
                            sender = %sender_display,
                            "Dropping GET Request with zero HTL"
                        );
                        return build_op_result(
                            id,
                            None,
                            Some(GetMsg::Response {
                                id,
                                key,
                                value: StoreResponse {
                                    state: None,
                                    contract: None,
                                },
                            }),
                            None,
                            stats,
                            self.upstream_addr,
                        );
                    } else {
                        // Normal case: operation should be in ReceivedRequest or AwaitingResponse state
                        debug_assert!(matches!(
                            self.state,
                            Some(GetState::ReceivedRequest { .. })
                                | Some(GetState::AwaitingResponse { .. })
                        ));
                        tracing::debug!(
                            tx = %id,
                            %key,
                            htl,
                            "GET: Request processing in state {:?}",
                            self.state
                        );

                        // Initialize/update stats for tracking the operation
                        let this_peer = op_manager.ring.connection_manager.own_location();
                        if stats.is_none() {
                            stats = Some(Box::new(GetStats {
                                contract_location: Location::from(&key),
                                next_peer: Some(this_peer.clone()),
                                transfer_time: None,
                                first_response_time: None,
                            }));
                        } else if let Some(s) = stats.as_mut() {
                            s.next_peer = Some(this_peer.clone());
                        }

                        // Update skip list with current peer address
                        let mut new_skip_list = skip_list.clone();
                        if let Some(addr) = this_peer.socket_addr() {
                            new_skip_list.insert(addr);
                        }

                        // First check if we have the contract locally before forwarding
                        let get_result = op_manager
                            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                                key,
                                return_contract_code: fetch_contract,
                            })
                            .await;

                        let local_value = match get_result {
                            Ok(ContractHandlerEvent::GetResponse {
                                response:
                                    Ok(StoreResponse {
                                        state: Some(state),
                                        contract,
                                    }),
                                ..
                            }) => {
                                if fetch_contract && contract.is_none() {
                                    tracing::debug!(
                                        tx = %id,
                                        %key,
                                        %this_peer,
                                        "GET: state available locally but contract code missing; continuing search"
                                    );
                                    None
                                } else {
                                    Some((state, contract))
                                }
                            }
                            _ => None,
                        };

                        if let Some((state, contract)) = local_value {
                            // Contract found locally!
                            tracing::info!(
                                tx = %id,
                                %key,
                                fetch_contract,
                                "GET: contract found locally"
                            );

                            // Check if this is a forwarded request or a local request
                            match &self.state {
                                Some(GetState::ReceivedRequest { requester })
                                    if requester.is_some() =>
                                {
                                    // This is a forwarded request - send result back to upstream
                                    tracing::debug!(tx = %id, "Returning contract {} to upstream", key);
                                    new_state = None;
                                    return_msg = Some(GetMsg::Response {
                                        id,
                                        key,
                                        value: StoreResponse {
                                            state: Some(state),
                                            contract,
                                        },
                                    });
                                }
                                Some(GetState::AwaitingResponse { requester, .. })
                                    if requester.is_some() =>
                                {
                                    // Forward contract to upstream
                                    new_state = None;
                                    tracing::debug!(tx = %id, "Returning contract {} to upstream", key);
                                    return_msg = Some(GetMsg::Response {
                                        id,
                                        key,
                                        value: StoreResponse {
                                            state: Some(state),
                                            contract,
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
                                %key,
                                %this_peer,
                                "Contract not found @ peer {}, forwarding to other peers",
                                sender
                            );

                            // Forward using standard routing helper
                            // Note: target is determined by routing, sender from source_addr
                            return try_forward_or_return(
                                id,
                                key,
                                (htl, fetch_contract),
                                (this_peer, sender.clone()),
                                new_skip_list,
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
                    key,
                    value: StoreResponse { state: None, .. },
                } => {
                    let id = *id;
                    let key = *key;

                    // Use sender_from_addr for logging
                    let Some(sender) = sender_from_addr.clone() else {
                        tracing::warn!(
                            tx = %id,
                            %key,
                            "GET: Response without sender lookup - cannot process"
                        );
                        return Err(OpError::invalid_transition(self.id));
                    };

                    tracing::info!(
                        tx = %id,
                        %key,
                        from = %sender,
                        "GET: Response received with empty value"
                    );
                    // Handle case where neither contract nor state was found
                    let this_peer = op_manager.ring.connection_manager.own_location();
                    tracing::warn!(
                        tx = %id,
                        %key,
                        %this_peer,
                        "Neither contract or contract value for contract found at peer {}, \
                        retrying with other peers",
                        sender
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
                            current_target: _,
                            skip_list,
                            ..
                        }) => {
                            // todo: register in the stats for the outcome of the op that failed to get a response from this peer

                            // Add the failed peer to tried list
                            if let Some(addr) = sender.socket_addr() {
                                tried_peers.insert(addr);
                            }

                            // First, check if we have alternatives at this hop level
                            if !alternatives.is_empty() && attempts_at_hop < DEFAULT_MAX_BREADTH {
                                // Try the next alternative
                                let next_target = alternatives.remove(0);

                                tracing::info!(
                                    tx = %id,
                                    %key,
                                    next_peer = %next_target,
                                    fetch_contract,
                                    attempts_at_hop = attempts_at_hop + 1,
                                    max_attempts = DEFAULT_MAX_BREADTH,
                                    tried = ?tried_peers,
                                    remaining_alternatives = ?alternatives,
                                    "Trying alternative peer at same hop level"
                                );

                                return_msg = Some(GetMsg::Request {
                                    id,
                                    key,
                                    fetch_contract,
                                    htl: current_hop,
                                    skip_list: tried_peers.clone(),
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
                                    key,
                                    current_target: next_target,
                                    // Preserve the accumulated skip_list so future candidate
                                    // selection still avoids already-specified peers; tried_peers
                                    // tracks attempts at this hop.
                                    skip_list: skip_list.clone(),
                                });
                            } else if retries < MAX_RETRIES {
                                // No more alternatives at this hop, try finding new peers
                                let mut new_skip_list = skip_list.clone();
                                new_skip_list.extend(tried_peers.clone());

                                // Get new candidates excluding all tried peers
                                let mut new_candidates =
                                    op_manager.ring.k_closest_potentially_caching(
                                        &key,
                                        &new_skip_list,
                                        DEFAULT_MAX_BREADTH,
                                    );

                                tracing::info!(
                                tx = %id,
                                %key,
                                new_candidates = ?new_candidates,
                                skip = ?new_skip_list,
                                hop = current_hop,
                                retries = retries + 1,
                                "GET seeking new candidates after exhausted alternatives"
                                );

                                if !new_candidates.is_empty() {
                                    // Try with the best new peer
                                    let target = new_candidates.remove(0);
                                    return_msg = Some(GetMsg::Request {
                                        id,
                                        key,
                                        fetch_contract,
                                        htl: current_hop,
                                        skip_list: new_skip_list.clone(),
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
                                        key,
                                        current_target: target,
                                        skip_list: new_skip_list.clone(),
                                    });
                                } else if let Some(requester_peer) = requester.clone() {
                                    // No more peers to try, return failure to requester
                                    tracing::warn!(
                                        tx = %id,
                                        %key,
                                        %this_peer,
                                        target = %requester_peer,
                                        tried = ?tried_peers,
                                        skip = ?new_skip_list,
                                        "No other peers found while trying to get the contract, returning response to requester"
                                    );
                                    return_msg = Some(GetMsg::Response {
                                        id,
                                        key,
                                        value: StoreResponse {
                                            state: None,
                                            contract: None,
                                        },
                                    });
                                } else {
                                    // Original requester, operation failed
                                    tracing::error!(
                                                            tx = %id,
                                    %key,
                                    tried = ?tried_peers,
                                    skip = ?skip_list,
                                    "Failed getting a value for contract {}, reached max retries",
                                    key
                                                        );
                                    return_msg = None;
                                    new_state = None;
                                    result = Some(GetResult {
                                        key,
                                        state: WrappedState::new(vec![]),
                                        contract: None,
                                    });
                                }
                            } else {
                                // Max retries reached
                                tracing::error!(
                                    tx = %id,
                                    "Failed getting a value for contract {}, reached max retries",
                                    key
                                );

                                if let Some(requester_peer) = requester.clone() {
                                    // Return failure to requester
                                    tracing::warn!(
                                        tx = %id,
                                        %key,
                                        %this_peer,
                                        target = %requester_peer,
                                        tried = ?tried_peers,
                                        skip = ?skip_list,
                                        "No other peers found while trying to get the contract, returning response to requester"
                                    );
                                    return_msg = Some(GetMsg::Response {
                                        id,
                                        key,
                                        value: StoreResponse {
                                            state: None,
                                            contract: None,
                                        },
                                    });
                                    new_state = None;
                                } else {
                                    // Original requester, operation failed
                                    tracing::error!(
                                        tx = %id,
                                        "Failed getting a value for contract {}, reached max retries",
                                        key
                                    );
                                    return_msg = None;
                                    new_state = None;
                                    result = Some(GetResult {
                                        key,
                                        state: WrappedState::new(vec![]),
                                        contract: None,
                                    });
                                }
                            }
                        }
                        Some(GetState::ReceivedRequest { .. }) => {
                            // Return failure to sender
                            tracing::debug!(tx = %id, "Returning contract {} to {}", key, sender);
                            new_state = None;
                            return_msg = Some(GetMsg::Response {
                                id,
                                key,
                                value: StoreResponse {
                                    state: None,
                                    contract: None,
                                },
                            });
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    };
                }
                GetMsg::Response {
                    id,
                    key,
                    value:
                        StoreResponse {
                            state: Some(value),
                            contract,
                        },
                } => {
                    let id = *id;
                    let key = *key;

                    // Use sender_from_addr for logging
                    let Some(sender) = sender_from_addr.clone() else {
                        tracing::warn!(
                            tx = %id,
                            %key,
                            "GET: Response without sender lookup - cannot process"
                        );
                        return Err(OpError::invalid_transition(self.id));
                    };

                    tracing::info!(tx = %id, %key, "GET: Response received with state: {:?}", self.state.as_ref().unwrap());

                    // Check if contract is required
                    let require_contract = matches!(
                        self.state,
                        Some(GetState::AwaitingResponse {
                            fetch_contract: true,
                            ..
                        })
                    );

                    // Get requester from current state
                    let requester = if let Some(GetState::AwaitingResponse { requester, .. }) =
                        self.state.as_ref()
                    {
                        requester.clone()
                    } else {
                        return Err(OpError::UnexpectedOpState);
                    };

                    // Handle case where contract is required but not provided
                    if require_contract && contract.is_none() {
                        if let Some(_requester) = requester {
                            // no contract, consider this like an error ignoring the incoming update value
                            tracing::warn!(
                                tx = %id,
                                "Contract not received from peer {} while required",
                                sender
                            );

                            tracing::warn!(
                                tx = %id,
                                %key,
                                at = %sender,
                                "Contract not received while required, returning response to upstream",
                            );

                            // Forward error to requester
                            op_manager
                                .notify_op_change(
                                    NetMessage::from(GetMsg::Response {
                                        id,
                                        key,
                                        value: StoreResponse {
                                            state: None,
                                            contract: None,
                                        },
                                    }),
                                    OpEnum::Get(GetOp {
                                        id,
                                        state: self.state,
                                        result: None,
                                        stats,
                                        upstream_addr: self.upstream_addr,
                                    }),
                                )
                                .await?;
                            return Err(OpError::StatePushed);
                        }
                    }

                    // Check if this is the original requester
                    let is_original_requester = matches!(
                        self.state,
                        Some(GetState::AwaitingResponse {
                            requester: None,
                            ..
                        })
                    );

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
                                key,
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
                            // State already cached and identical, mark as seeded if needed
                            if !op_manager.ring.is_seeding_contract(&key) {
                                tracing::debug!(tx = %id, %key, "Marking contract as seeded");
                                op_manager.ring.record_get_access(key, value.size() as u64);
                                super::announce_contract_cached(op_manager, &key).await;
                                let child_tx =
                                    super::start_subscription_request(op_manager, id, key);
                                tracing::debug!(tx = %id, %child_tx, "started subscription as child operation");
                            }
                        } else {
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
                                ContractHandlerEvent::PutResponse { new_value: Ok(_) } => {
                                    tracing::debug!(tx = %id, %key, "Contract put at executor");
                                    let is_subscribed_contract =
                                        op_manager.ring.is_seeding_contract(&key);

                                    // Start subscription if not already seeding
                                    if !is_subscribed_contract {
                                        tracing::debug!(tx = %id, %key, peer = ?op_manager.ring.connection_manager.get_own_addr(), "Contract not cached @ peer, caching");
                                        op_manager.ring.record_get_access(key, value.size() as u64);
                                        super::announce_contract_cached(op_manager, &key).await;

                                        let child_tx =
                                            super::start_subscription_request(op_manager, id, key);
                                        tracing::debug!(tx = %id, %child_tx, "started subscription as child operation");
                                    }
                                }
                                ContractHandlerEvent::PutResponse {
                                    new_value: Err(err),
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
                        }
                    }

                    // Process based on current state
                    match self.state {
                        Some(GetState::AwaitingResponse {
                            requester: None, ..
                        }) => {
                            // Original requester, operation completed successfully
                            tracing::info!(tx = %id, %key, "Get response received for contract at original requester");
                            new_state = Some(GetState::Finished { key });
                            return_msg = None;
                            result = Some(GetResult {
                                key,
                                state: value.clone(),
                                contract: contract.clone(),
                            });
                        }
                        Some(GetState::AwaitingResponse {
                            requester: Some(requester),
                            ..
                        }) => {
                            // Forward response to requester
                            tracing::info!(tx = %id, %key, "Get response received for contract at hop peer");
                            new_state = None;
                            return_msg = Some(GetMsg::Response {
                                id,
                                key,
                                value: StoreResponse {
                                    state: Some(value.clone()),
                                    contract: contract.clone(),
                                },
                            });
                            tracing::debug!(tx = %id, %key, target = %requester, "Returning contract to requester");
                            result = Some(GetResult {
                                key,
                                state: value.clone(),
                                contract: contract.clone(),
                            });
                        }
                        Some(GetState::ReceivedRequest { .. }) => {
                            // Return response to sender
                            tracing::info!(tx = %id, "Returning contract {} to {}", key, sender);
                            new_state = None;
                            return_msg = Some(GetMsg::Response {
                                id,
                                key,
                                value: StoreResponse {
                                    state: Some(value.clone()),
                                    contract: contract.clone(),
                                },
                            });
                        }
                        Some(other) => {
                            return Err(OpError::invalid_transition_with_state(
                                self.id,
                                Box::new(other),
                            ))
                        }
                        None => return Err(OpError::invalid_transition(self.id)),
                    };
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
    // Determine the target address for sending the message:
    // - For Response messages: route back to upstream_addr (who sent us the request)
    // - For Request messages being forwarded: use current_target from state
    let target_addr = match (&msg, &state) {
        (Some(GetMsg::Response { .. }), _) => upstream_addr,
        (Some(GetMsg::Request { .. }), Some(GetState::AwaitingResponse { current_target, .. })) => {
            current_target.socket_addr()
        }
        _ => None,
    };

    let output_op = state.map(|state| GetOp {
        id,
        state: Some(state),
        result,
        stats,
        upstream_addr,
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        target_addr,
        state: output_op.map(OpEnum::Get),
    })
}

#[allow(clippy::too_many_arguments)]
async fn try_forward_or_return(
    id: Transaction,
    key: ContractKey,
    (htl, fetch_contract): (usize, bool),
    (this_peer, sender): (PeerKeyLocation, PeerKeyLocation),
    skip_list: HashSet<std::net::SocketAddr>,
    op_manager: &OpManager,
    stats: Option<Box<GetStats>>,
    upstream_addr: Option<std::net::SocketAddr>,
) -> Result<OperationResult, OpError> {
    tracing::warn!(
        tx = %id,
        %key,
        this_peer = %this_peer,
        "Contract not found while processing a get request",
    );

    let mut new_skip_list = skip_list.clone();
    if let Some(addr) = this_peer.socket_addr() {
        new_skip_list.insert(addr);
    }

    let new_htl = htl.saturating_sub(1);

    let (new_target, alternatives) = if new_htl == 0 {
        tracing::warn!(
            tx = %id,
            sender = %sender,
            "The maximum hops have been exceeded, sending response back to the node",
        );
        (None, vec![])
    } else {
        let mut candidates = op_manager.ring.k_closest_potentially_caching(
            &key,
            &new_skip_list,
            DEFAULT_MAX_BREADTH,
        );

        if candidates.is_empty() {
            tracing::warn!(
                tx = %id,
                %key,
                this_peer = %this_peer,
                "No other peers found while trying to get the contract",
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
                key,
                requester: Some(sender),
                retries: 0,
                fetch_contract,
                current_hop: new_htl,
                subscribe: false,
                current_target: target.clone(),
                tried_peers,
                alternatives,
                attempts_at_hop: 1,
                skip_list: new_skip_list.clone(),
            }),
            Some(GetMsg::Request {
                id,
                key,
                fetch_contract,
                htl: new_htl,
                skip_list: new_skip_list,
            }),
            None,
            stats,
            upstream_addr,
        )
    } else {
        tracing::debug!(
            tx = %id,
            "Cannot find any other peers to forward the get request to, returning get response to {}",
            sender
        );

        build_op_result(
            id,
            None,
            Some(GetMsg::Response {
                key,
                id,
                value: StoreResponse {
                    state: None,
                    contract: None,
                },
            }),
            None,
            stats,
            upstream_addr,
        )
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

    /// GET operation messages.
    ///
    /// Uses hop-by-hop routing: each node stores `upstream_addr` from the transport layer
    /// to route responses back. No `PeerKeyLocation` is embedded in wire messages.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum GetMsg {
        /// Request to retrieve a contract. Forwarded hop-by-hop toward contract location.
        Request {
            id: Transaction,
            key: ContractKey,
            fetch_contract: bool,
            /// Hops to live - decremented at each hop. When 0, stop forwarding.
            htl: usize,
            skip_list: HashSet<std::net::SocketAddr>,
        },
        /// Response containing the contract data. Routed hop-by-hop back to originator.
        Response {
            id: Transaction,
            key: ContractKey,
            value: StoreResponse,
        },
    }

    impl InnerMessage for GetMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. } | Self::Response { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { key, .. } | Self::Response { key, .. } => {
                    Some(Location::from(key.id()))
                }
            }
        }
    }

    impl Display for GetMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request { key, htl, .. } => {
                    write!(f, "Get::Request(id: {id}, key: {key}, htl: {htl})")
                }
                Self::Response { key, .. } => write!(f, "Get::Response(id: {id}, key: {key})"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Transaction;
    use crate::operations::test_utils::make_contract_key;
    use std::collections::HashSet;

    fn make_get_op(state: Option<GetState>, result: Option<GetResult>) -> GetOp {
        GetOp {
            id: Transaction::new::<GetMsg>(),
            state,
            result,
            stats: None,
            upstream_addr: None,
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
        let op = make_get_op(Some(GetState::ReceivedRequest { requester: None }), None);
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
        let op = make_get_op(Some(GetState::ReceivedRequest { requester: None }), None);
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
            key: make_contract_key(1),
            fetch_contract: false,
            htl: 5,
            skip_list: HashSet::new(),
        };
        assert_eq!(*msg.id(), tx, "id() should return the transaction ID");
    }

    #[test]
    fn get_msg_display_formats_correctly() {
        let tx = Transaction::new::<GetMsg>();
        let msg = GetMsg::Request {
            id: tx,
            key: make_contract_key(1),
            fetch_contract: false,
            htl: 5,
            skip_list: HashSet::new(),
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
        let state = GetState::ReceivedRequest { requester: None };
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
}
