//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

pub(crate) use self::messages::{PutMsg, PutStreamingPayload};
use super::orphan_streams::{OrphanStreamError, STREAM_CLAIM_TIMEOUT};
use freenet_stdlib::{
    client_api::{ErrorKind, HostResponse},
    prelude::*,
};

use super::{
    put, should_use_streaming, OpEnum, OpError, OpInitialization, OpOutcome, Operation,
    OperationResult, VisitedPeers,
};

/// Maximum number of retry rounds when all alternatives at the current hop are exhausted.
const MAX_RETRIES: usize = 10;

/// Maximum number of alternative peers to try at each hop level before re-routing.
const DEFAULT_MAX_BREADTH: usize = 3;

/// Minimum HTL to use when retrying — prevents retries from being too shallow.
const MIN_RETRY_HTL: usize = 3;
use crate::node::IsOperationCompleted;
use crate::transport::peer_connection::StreamId;
use crate::{
    client_events::HostResult,
    contract::ContractHandlerEvent,
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager},
    ring::{KnownPeerKeyLocation, Location, PeerKeyLocation},
    tracing::{state_hash_full, NetEventLog, OperationFailure},
};
use either::Either;

/// Routing stats for put operations, used to report success/failure to the router.
struct PutStats {
    target_peer: PeerKeyLocation,
    contract_location: Location,
}

pub(crate) struct PutOp {
    pub id: Transaction,
    state: Option<PutState>,
    /// The address we received this operation's message from.
    /// Used for connection-based routing: responses are sent back to this address.
    upstream_addr: Option<std::net::SocketAddr>,
    /// Routing stats for reporting outcomes to the router.
    stats: Option<PutStats>,
    /// True when a downstream relay has acknowledged forwarding this request.
    /// Used by the GC task to distinguish "peer is dead" from "peer is working on it".
    pub(crate) ack_received: bool,
    /// Number of speculative parallel paths launched by the originator's GC task.
    /// Capped at MAX_SPECULATIVE_PATHS to bound network overhead.
    pub(crate) speculative_paths: u8,
}

impl PutOp {
    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        if self.finalized() {
            if let Some(ref stats) = self.stats {
                return OpOutcome::ContractOpSuccessUntimed {
                    target_peer: &stats.target_peer,
                    contract_location: stats.contract_location,
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

    /// Returns true if this PUT was initiated by a local client (not forwarded from a peer).
    pub(crate) fn is_client_initiated(&self) -> bool {
        self.upstream_addr.is_none()
    }

    /// Extract routing failure info for timeout reporting.
    pub(crate) fn failure_routing_info(&self) -> Option<(PeerKeyLocation, Location)> {
        self.stats
            .as_ref()
            .map(|s| (s.target_peer.clone(), s.contract_location))
    }

    pub(super) fn finalized(&self) -> bool {
        self.state.is_none() || matches!(self.state, Some(PutState::Finished(_)))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(PutState::Finished(data)) = &self.state {
            let key = &data.key;
            Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::PutResponse { key: *key },
            ))
        } else {
            Err(ErrorKind::OperationError {
                cause: "put didn't finish successfully".into(),
            }
            .into())
        }
    }

    /// Get the next hop address if this operation is in a state that needs to send
    /// an outbound message to a downstream peer.
    pub(crate) fn get_next_hop_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.state {
            Some(PutState::AwaitingResponse(data)) => data.next_hop,
            _ => None,
        }
    }

    /// Get the current HTL (remaining hops) for this operation.
    /// Returns None if the operation is not in AwaitingResponse state.
    pub(crate) fn get_current_htl(&self) -> Option<usize> {
        match &self.state {
            Some(PutState::AwaitingResponse(data)) => Some(data.current_htl),
            _ => None,
        }
    }

    /// Try the next alternative peer for a timed-out PUT operation.
    ///
    /// Returns `Ok((new_op, msg))` with the re-routed operation and message to send,
    /// or `Err(self)` if no alternatives remain.
    /// Try the next alternative peer for a timed-out PUT operation.
    ///
    /// Returns `Ok((new_op, msg))` with the re-routed operation and message to send,
    /// or `Err(self)` if no alternatives remain or no retry payload is available.
    ///
    /// Follows the same pattern as `GetOp::retry_with_next_alternative`:
    /// 1. Inject fallback peers if local alternatives exhausted
    /// 2. Pick next alternative, mark in tried_peers + bloom filter
    /// 3. Reduce HTL on each retry to limit blast radius
    /// 4. Build a new PutMsg::Request from the retained payload
    pub(crate) fn retry_with_next_alternative(
        mut self,
        max_hops_to_live: usize,
        fallback_peers: &[PeerKeyLocation],
    ) -> Result<(PutOp, PutMsg), Box<PutOp>> {
        let state = match self.state.take() {
            Some(s) => s,
            None => return Err(Box::new(self)),
        };
        match state {
            PutState::AwaitingResponse(mut data) => {
                // Can't retry without the contract data
                if data.retry_payload.is_none() {
                    self.state = Some(PutState::AwaitingResponse(data));
                    return Err(Box::new(self));
                }

                // If local alternatives exhausted, inject fallback peers we haven't tried.
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
                            contract = %data.contract_key,
                            new_candidates = data.alternatives.len(),
                            "PUT broadening search to all connected peers (DBF fallback)"
                        );
                    }
                }

                if data.alternatives.is_empty() {
                    self.state = Some(PutState::AwaitingResponse(data));
                    return Err(Box::new(self));
                }

                let next_target = data.alternatives.remove(0);
                let contract_key = data.contract_key;
                if let Some(addr) = next_target.socket_addr() {
                    data.tried_peers.insert(addr);
                    data.visited.mark_visited(addr);
                }
                tracing::info!(
                    tx = %self.id,
                    contract = %contract_key,
                    target = %next_target,
                    remaining_alternatives = data.alternatives.len(),
                    "PUT retrying with alternative peer after timeout"
                );
                data.next_hop = next_target.socket_addr();
                data.attempts_at_hop += 1;

                // Reduce HTL on each retry to avoid full-depth traversal storms.
                let retry_htl = (max_hops_to_live / (data.attempts_at_hop.max(1)))
                    .max(MIN_RETRY_HTL)
                    .min(max_hops_to_live);

                let payload = data.retry_payload.as_ref().unwrap();
                let skip_list = data.tried_peers.clone();

                let msg = PutMsg::Request {
                    id: self.id,
                    contract: payload.contract.clone(),
                    related_contracts: payload.related_contracts.clone(),
                    value: payload.value.clone(),
                    htl: retry_htl,
                    skip_list,
                };

                self.state = Some(PutState::AwaitingResponse(data));
                Ok((self, msg))
            }
            other => {
                self.state = Some(other);
                Err(Box::new(self))
            }
        }
    }

    /// Handle aborted connections by failing the operation immediately.
    ///
    /// PUT operations don't have alternative routes to try. When the connection
    /// drops, we notify the client of the failure so they can retry.
    pub(crate) async fn handle_abort(self, op_manager: &OpManager) -> Result<(), OpError> {
        tracing::warn!(
            tx = %self.id,
            "Put operation aborted due to connection failure"
        );

        // Extract key and current_htl from state if available
        let (key, current_htl) = match &self.state {
            Some(PutState::PrepareRequest(data)) => (Some(data.contract.key()), Some(data.htl)),
            Some(PutState::AwaitingResponse(data)) => (None, Some(data.current_htl)),
            Some(PutState::Finished(data)) => (Some(data.key), None),
            None => (None, None),
        };

        // Calculate hop_count: max_htl - current_htl
        let hop_count = current_htl.map(|htl| op_manager.ring.max_hops_to_live.saturating_sub(htl));

        // Emit failure event if we have the key
        if let Some(key) = key {
            if let Some(event) = NetEventLog::put_failure(
                &self.id,
                &op_manager.ring,
                key,
                OperationFailure::ConnectionDropped,
                hop_count,
            ) {
                op_manager.ring.register_events(Either::Left(event)).await;
            }
        }

        // Create an error result to notify the client
        let error_result: crate::client_events::HostResult =
            Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "Put operation failed: peer connection dropped".into(),
            }
            .into());

        // Send the error to the client via the result router.
        // Use try_send to avoid blocking the event loop (see channel-safety.md).
        if let Err(err) = op_manager
            .result_router_tx
            .try_send((self.id, error_result))
        {
            tracing::error!(
                tx = %self.id,
                error = %err,
                "Failed to send abort notification to client \
                 (result router channel full or closed)"
            );
        }

        // Mark the operation as completed so it's removed from tracking
        op_manager.completed(self.id);
        Ok(())
    }
}

impl IsOperationCompleted for PutOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(put::PutState::Finished(_)))
    }
}

pub(crate) struct PutResult {}

impl Operation for PutOp {
    type Message = PutMsg;
    type Result = PutResult;

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Result<OpInitialization<Self>, OpError> {
        let tx = *msg.id();
        tracing::debug!(
            tx = %tx,
            msg_type = %msg,
            phase = "load_or_init",
            "Attempting to load or initialize PUT operation"
        );

        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Put(put_op))) => {
                // was an existing operation, the other peer messaged back
                tracing::debug!(
                    tx = %tx,
                    state = %put_op.state.as_ref().map(|s| format!("{:?}", s)).unwrap_or_else(|| "None".to_string()),
                    phase = "load_or_init",
                    "Found existing PUT operation"
                );
                Ok(OpInitialization {
                    op: put_op,
                    source_addr,
                })
            }
            Ok(Some(op)) => {
                tracing::warn!(
                    tx = %tx,
                    phase = "load_or_init",
                    "Found operation with wrong type, pushing back"
                );
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
                    PutMsg::Response { .. } | PutMsg::ResponseStreaming { .. }
                ) {
                    tracing::debug!(
                        tx = %tx,
                        phase = "load_or_init",
                        "PUT response arrived for non-existent operation (likely timed out)"
                    );
                    return Err(OpError::OpNotPresent(tx));
                }

                // New incoming request - we're a forwarder or final node.
                // We don't need persistent state, just track upstream_addr for response routing.
                tracing::debug!(
                    tx = %tx,
                    source = ?source_addr,
                    phase = "load_or_init",
                    "New incoming request"
                );
                Ok(OpInitialization {
                    op: Self {
                        state: None, // No state needed for forwarding nodes
                        id: tx,
                        upstream_addr: source_addr, // Remember who to send response to
                        stats: None,
                        ack_received: false,
                        speculative_paths: 0,
                    },
                    source_addr,
                })
            }
            Err(err) => {
                tracing::error!(
                    tx = %tx,
                    error = %err,
                    phase = "load_or_init",
                    "Error popping operation"
                );
                Err(err.into())
            }
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
            let upstream_addr = self.upstream_addr;
            let mut stats = self.stats;
            let is_originator = upstream_addr.is_none();

            // Look up sender's PeerKeyLocation from source address for telemetry
            let sender_from_addr = source_addr.and_then(|addr| {
                op_manager
                    .ring
                    .connection_manager
                    .get_peer_location_by_addr(addr)
            });

            // Extract subscribe flags from state (only relevant for originator)
            let subscribe = match &self.state {
                Some(PutState::PrepareRequest(data)) => data.subscribe,
                Some(PutState::AwaitingResponse(data)) => data.subscribe,
                _ => false,
            };
            let blocking_subscribe = match &self.state {
                Some(PutState::PrepareRequest(data)) => data.blocking_subscribe,
                Some(PutState::AwaitingResponse(data)) => data.blocking_subscribe,
                _ => false,
            };

            match input {
                PutMsg::Request {
                    id: _msg_id,
                    contract,
                    related_contracts,
                    value,
                    htl,
                    skip_list,
                } => {
                    let key = contract.key();
                    let htl = *htl;

                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        htl,
                        is_originator,
                        subscribe,
                        phase = "request",
                        "Processing PUT Request"
                    );

                    // Check if we're already subscribed to this contract BEFORE storing
                    let was_hosting = op_manager.ring.is_hosting_contract(&key);

                    // Step 1: Store contract locally (all nodes cache)
                    // put_contract returns (merged_value, state_changed) where state_changed
                    // is true if the stored state actually changed (old != new).
                    let (merged_value, _state_changed) = put_contract(
                        op_manager,
                        key,
                        value.clone(),
                        related_contracts.clone(),
                        contract,
                    )
                    .await?;

                    // Mark as hosting if not already
                    if !was_hosting {
                        let evicted = op_manager
                            .ring
                            .host_contract(key, value.size() as u64, crate::ring::AccessType::Put)
                            .evicted;
                        super::announce_contract_hosted(op_manager, &key).await;

                        // Clean up interest tracking for evicted contracts
                        let mut removed_contracts = Vec::new();
                        for evicted_key in evicted {
                            if op_manager
                                .interest_manager
                                .unregister_local_hosting(&evicted_key)
                            {
                                removed_contracts.push(evicted_key);
                            }
                        }

                        // Register local interest for delta-based sync
                        let became_interested =
                            op_manager.interest_manager.register_local_hosting(&key);

                        // Broadcast interest changes to peers
                        let added = if became_interested { vec![key] } else { vec![] };
                        if !added.is_empty() || !removed_contracts.is_empty() {
                            super::broadcast_change_interests(op_manager, added, removed_contracts)
                                .await;
                        }
                    }

                    // Invariant: after storing and hosting, the contract MUST be in the hosting list.
                    debug_assert!(
                        op_manager.ring.is_hosting_contract(&key),
                        "PUT Request: contract {key} must be in hosting list after put_contract + host_contract"
                    );

                    // Network peer notification is now automatic via BroadcastStateChange
                    // event emitted by the executor when state changes. No manual triggering needed.

                    // Step 2: Determine if we should forward or respond
                    // Build skip list: include sender (upstream) and already-tried peers
                    let mut new_skip_list = skip_list.clone();
                    if let Some(addr) = upstream_addr {
                        new_skip_list.insert(addr);
                    }
                    // Add our own address to skip list
                    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
                        new_skip_list.insert(own_addr);
                    }

                    // Find next hop toward contract location
                    let next_hop = if htl > 0 {
                        op_manager
                            .ring
                            .closest_potentially_hosting(&key, &new_skip_list)
                    } else {
                        None
                    };

                    // Convert to KnownPeerKeyLocation for compile-time address guarantee
                    let next_peer_known =
                        next_hop.and_then(|p| KnownPeerKeyLocation::try_from(p).ok());
                    if let Some(next_peer) = next_peer_known {
                        // Forward to next hop
                        let next_addr = next_peer.socket_addr();

                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            peer_addr = %next_addr,
                            htl = htl - 1,
                            phase = "forward",
                            "Forwarding PUT to next hop"
                        );

                        // Emit put_request telemetry when forwarding
                        if let Some(event) = NetEventLog::put_request(
                            &id,
                            &op_manager.ring,
                            key,
                            PeerKeyLocation::from(next_peer.clone()),
                            htl.saturating_sub(1),
                        ) {
                            op_manager.ring.register_events(Either::Left(event)).await;
                        }

                        let new_htl = htl.saturating_sub(1);

                        // Check if we should use streaming for the forward
                        let payload = PutStreamingPayload {
                            contract: contract.clone(),
                            related_contracts: related_contracts.clone(),
                            value: merged_value,
                        };
                        let payload_bytes = bincode::serialize(&payload).map_err(|e| {
                            OpError::NotificationChannelError(format!(
                                "Failed to serialize streaming payload: {e}"
                            ))
                        })?;
                        let payload_size = payload_bytes.len();

                        let (forward_msg, stream_data) =
                            if should_use_streaming(op_manager.streaming_threshold, payload_size) {
                                let sid = StreamId::next_operations();
                                tracing::info!(
                                    tx = %id,
                                    stream_id = %sid,
                                    payload_size,
                                    "PUT request using operations-level streaming"
                                );
                                (
                                    PutMsg::RequestStreaming {
                                        id,
                                        stream_id: sid,
                                        contract_key: key,
                                        total_size: payload_size as u64,
                                        htl: new_htl,
                                        skip_list: new_skip_list,
                                        subscribe,
                                    },
                                    Some((sid, bytes::Bytes::from(payload_bytes))),
                                )
                            } else {
                                (
                                    PutMsg::Request {
                                        id,
                                        contract: payload.contract,
                                        related_contracts: payload.related_contracts,
                                        value: payload.value,
                                        htl: new_htl,
                                        skip_list: new_skip_list,
                                    },
                                    None,
                                )
                            };

                        // Transition to AwaitingResponse, preserving subscribe flags for originator
                        // Store next_hop so handle_notification_msg can route the message
                        let new_state = Some(PutState::AwaitingResponse(AwaitingResponseData {
                            subscribe,
                            blocking_subscribe,
                            next_hop: Some(next_addr),
                            current_htl: htl,
                            contract_key: key,
                            retries: 0,
                            tried_peers: {
                                let mut s = HashSet::new();
                                s.insert(next_addr);
                                s
                            },
                            alternatives: vec![],
                            attempts_at_hop: 1,
                            visited: VisitedPeers::default(),
                            // Originator retains payload for retry; relay peers don't need it
                            retry_payload: if is_originator {
                                Some(PutRetryPayload {
                                    contract: contract.clone(),
                                    related_contracts: related_contracts.clone(),
                                    value: value.clone(),
                                })
                            } else {
                                None
                            },
                        }));

                        stats = Some(PutStats {
                            target_peer: PeerKeyLocation::from(next_peer.clone()),
                            contract_location: Location::from(&key),
                        });

                        // Send ForwardingAck upstream before forwarding (fire-and-forget).
                        // Tells the upstream "I received the data, processing it" so the
                        // GC task can distinguish dead peers from slow multi-hop chains.
                        if let Some(upstream) = upstream_addr {
                            let ack = NetMessage::from(PutMsg::ForwardingAck {
                                id,
                                contract_key: key,
                            });
                            drop(conn_manager.send(upstream, ack).await);
                        }

                        Ok(OperationResult::SendAndContinue {
                            msg: NetMessage::from(forward_msg),
                            next_hop: Some(next_addr),
                            state: OpEnum::Put(PutOp {
                                id,
                                state: new_state,
                                upstream_addr,
                                stats,
                                ack_received: false,
                                speculative_paths: 0,
                            }),
                            stream_data,
                        })
                    } else {
                        // No next hop - we're the final destination (or htl exhausted)
                        tracing::info!(
                            tx = %id,
                            contract = %key,
                            phase = "complete",
                            "PUT complete at this node, sending response"
                        );

                        if is_originator {
                            // We're both originator and final destination
                            // Emit put_success telemetry with our own location as target
                            // hop_count is 0 since we stored locally
                            let own_location = op_manager.ring.connection_manager.own_location();
                            let hash = Some(state_hash_full(&merged_value));
                            let size = Some(merged_value.len());
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                Some(0), // Stored locally, 0 hops
                                hash,
                                size,
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }

                            // Start subscription if requested
                            if subscribe {
                                start_subscription_after_put(
                                    op_manager,
                                    id,
                                    key,
                                    blocking_subscribe,
                                )
                                .await;
                            }

                            Ok(OperationResult::ContinueOp(OpEnum::Put(PutOp {
                                id,
                                state: Some(PutState::Finished(FinishedData { key })),
                                upstream_addr: None,
                                stats,
                                ack_received: false,
                                speculative_paths: 0,
                            })))
                        } else {
                            // Non-originator target peer - emit put_success for convergence checking
                            // Without this event, the target peer's stored state won't be tracked,
                            // causing convergence checks to fail (they need contracts replicated
                            // across multiple peers). See issue #2680.
                            let own_location = op_manager.ring.connection_manager.own_location();
                            let hash = Some(state_hash_full(&merged_value));
                            let size = Some(merged_value.len());
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                None, // hop_count unknown without initial HTL
                                hash,
                                size,
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }

                            // Send response back to upstream
                            let response = PutMsg::Response { id, key };
                            let upstream =
                                upstream_addr.expect("non-originator must have upstream");

                            Ok(OperationResult::SendAndComplete {
                                msg: NetMessage::from(response),
                                next_hop: Some(upstream),
                                stream_data: None,
                            })
                        }
                    }
                }

                PutMsg::Response { id: _msg_id, key } => {
                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        is_originator,
                        subscribe,
                        phase = "response",
                        "PUT Response received"
                    );

                    if is_originator {
                        // We're the originator - operation complete!
                        tracing::info!(
                            tx = %id,
                            contract = %key,
                            elapsed_ms = id.elapsed().as_millis(),
                            phase = "complete",
                            "PUT operation completed successfully"
                        );

                        // Emit put_success telemetry
                        // Calculate hop_count from current_htl in state
                        let current_htl = match &self.state {
                            Some(PutState::AwaitingResponse(data)) => Some(data.current_htl),
                            _ => None,
                        };
                        let hop_count = current_htl
                            .map(|htl| op_manager.ring.max_hops_to_live.saturating_sub(htl));

                        if let Some(sender) = sender_from_addr.clone() {
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                *key,
                                sender,
                                hop_count,
                                None, // State not available in response
                                None, // Size not available in response
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }
                        }

                        // Start subscription if requested
                        if subscribe {
                            start_subscription_after_put(op_manager, id, *key, blocking_subscribe)
                                .await;
                        }

                        Ok(OperationResult::ContinueOp(OpEnum::Put(PutOp {
                            id,
                            state: Some(PutState::Finished(FinishedData { key: *key })),
                            upstream_addr: None,
                            stats,
                            ack_received: false,
                            speculative_paths: 0,
                        })))
                    } else {
                        // Forward response to our upstream
                        let upstream = upstream_addr.expect("non-originator must have upstream");

                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            peer_addr = %upstream,
                            phase = "response",
                            "Forwarding PUT Response to upstream"
                        );

                        let response = PutMsg::Response { id, key: *key };

                        Ok(OperationResult::SendAndComplete {
                            msg: NetMessage::from(response),
                            next_hop: Some(upstream),
                            stream_data: None,
                        })
                    }
                }

                // Streaming PUT request handler
                PutMsg::RequestStreaming {
                    id: _msg_id,
                    stream_id,
                    contract_key,
                    total_size,
                    htl,
                    skip_list,
                    subscribe: msg_subscribe,
                } => {
                    tracing::info!(
                        tx = %id,
                        contract = %contract_key,
                        stream_id = %stream_id,
                        total_size,
                        htl,
                        is_originator,
                        phase = "streaming_request",
                        "Processing PUT RequestStreaming"
                    );

                    // Step 1: Claim the stream from orphan registry (atomic dedup)
                    let peer_addr = match source_addr {
                        Some(addr) => addr,
                        None => {
                            tracing::error!(tx = %id, "source_addr missing for streaming PUT request");
                            return Err(OpError::UnexpectedOpState);
                        }
                    };
                    let stream_handle = match op_manager
                        .orphan_stream_registry()
                        .claim_or_wait(peer_addr, *stream_id, STREAM_CLAIM_TIMEOUT)
                        .await
                    {
                        Ok(handle) => handle,
                        Err(OrphanStreamError::AlreadyClaimed) => {
                            tracing::debug!(
                                tx = %id,
                                stream_id = %stream_id,
                                "PUT RequestStreaming skipped — stream already claimed (dedup)"
                            );
                            // Push the operation state back since load_or_init popped it.
                            // Without this, duplicate metadata messages (from embedded fragment #1)
                            // permanently lose the operation state, preventing ResponseStreaming
                            // from being matched to the operation.
                            if self.state.is_some() {
                                if let Err(e) = op_manager
                                    .push(
                                        id,
                                        OpEnum::Put(PutOp {
                                            id,
                                            state: self.state,
                                            upstream_addr,
                                            stats,
                                            ack_received: false,
                                            speculative_paths: 0,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push PUT op state back after dedup");
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
                                        OpEnum::Put(PutOp {
                                            id,
                                            state: self.state,
                                            upstream_addr,
                                            stats,
                                            ack_received: false,
                                            speculative_paths: 0,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push PUT op state back after orphan claim failure");
                                }
                            }
                            return Err(OpError::OrphanStreamClaimFailed);
                        }
                    };

                    // Step 2: Compute next hop BEFORE assembly (enables piped forwarding)
                    // Build skip list for routing
                    let htl = *htl;
                    let mut routing_skip_list = skip_list.clone();
                    if let Some(addr) = upstream_addr {
                        routing_skip_list.insert(addr);
                    }
                    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
                        routing_skip_list.insert(own_addr);
                    }

                    let next_hop = if htl > 0 {
                        op_manager
                            .ring
                            .closest_potentially_hosting(contract_key, &routing_skip_list)
                    } else {
                        None
                    };

                    let next_peer_known =
                        next_hop.and_then(|p| KnownPeerKeyLocation::try_from(p).ok());

                    // Step 3: Start piping if we have a next hop and streaming is appropriate
                    // Fork the handle and start forwarding BEFORE we assemble locally
                    let piping_started = if let Some(ref next_peer) = next_peer_known {
                        let next_addr = next_peer.socket_addr();
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
                                peer_addr = %next_addr,
                                "Starting piped stream forwarding to next hop"
                            );

                            // Send metadata message first
                            let pipe_metadata = PutMsg::RequestStreaming {
                                id,
                                stream_id: outbound_sid,
                                contract_key: *contract_key,
                                total_size: *total_size,
                                htl: htl.saturating_sub(1),
                                skip_list: routing_skip_list.clone(),
                                subscribe: *msg_subscribe,
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
                            // Send ForwardingAck upstream before piping (fire-and-forget).
                            if let Some(upstream) = upstream_addr {
                                let ack = NetMessage::from(PutMsg::ForwardingAck {
                                    id,
                                    contract_key: *contract_key,
                                });
                                drop(conn_manager.send(upstream, ack).await);
                            }

                            conn_manager.send(next_addr, pipe_metadata_net).await?;

                            // Start piping (runs asynchronously in background)
                            conn_manager
                                .pipe_stream(
                                    next_addr,
                                    outbound_sid,
                                    forked_handle,
                                    embedded_metadata,
                                )
                                .await?;

                            if let Some(event) = NetEventLog::put_request(
                                &id,
                                &op_manager.ring,
                                *contract_key,
                                PeerKeyLocation::from(next_peer.clone()),
                                htl.saturating_sub(1),
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }

                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    // Step 4: Wait for stream to complete and assemble data (for local storage)
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
                    let payload: PutStreamingPayload = match bincode::deserialize(&stream_data) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to deserialize streaming payload"
                            );
                            return Err(OpError::invalid_transition(id));
                        }
                    };

                    let contract = &payload.contract;
                    let value = payload.value;
                    let related_contracts = payload.related_contracts;
                    let key = contract.key();

                    // Verify contract key matches
                    if key != *contract_key {
                        tracing::error!(
                            tx = %id,
                            expected = %contract_key,
                            actual = %key,
                            "Contract key mismatch in streaming payload"
                        );
                        return Err(OpError::invalid_transition(id));
                    }

                    // Step 4: Store contract locally (same as regular Request)
                    let was_hosting = op_manager.ring.is_hosting_contract(&key);
                    let (merged_value, _state_changed) = put_contract(
                        op_manager,
                        key,
                        value.clone(),
                        related_contracts.clone(),
                        contract,
                    )
                    .await?;

                    // Mark as hosting if not already
                    if !was_hosting {
                        let evicted = op_manager
                            .ring
                            .host_contract(key, value.size() as u64, crate::ring::AccessType::Put)
                            .evicted;
                        super::announce_contract_hosted(op_manager, &key).await;

                        let mut removed_contracts = Vec::new();
                        for evicted_key in evicted {
                            if op_manager
                                .interest_manager
                                .unregister_local_hosting(&evicted_key)
                            {
                                removed_contracts.push(evicted_key);
                            }
                        }

                        let became_interested =
                            op_manager.interest_manager.register_local_hosting(&key);
                        let added = if became_interested { vec![key] } else { vec![] };
                        if !added.is_empty() || !removed_contracts.is_empty() {
                            super::broadcast_change_interests(op_manager, added, removed_contracts)
                                .await;
                        }
                    }

                    // Invariant: after storing and hosting, the contract MUST be in the hosting list.
                    debug_assert!(
                        op_manager.ring.is_hosting_contract(&key),
                        "PUT Streaming: contract {key} must be in hosting list after put_contract + host_contract"
                    );

                    // Step 5: Handle forwarding or final destination
                    if piping_started {
                        // Piping is already underway - just track state, no need to forward via return
                        let next_addr = next_peer_known
                            .as_ref()
                            .expect("piping_started implies next_peer_known")
                            .socket_addr();

                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            peer_addr = %next_addr,
                            htl = htl.saturating_sub(1),
                            phase = "forward",
                            "PUT piping in progress to next hop"
                        );

                        // Forwarding nodes always use non-blocking subscriptions:
                        // blocking_subscribe is a client-side preference that only
                        // applies to the originator node.
                        let new_state = Some(PutState::AwaitingResponse(AwaitingResponseData {
                            subscribe: *msg_subscribe,
                            blocking_subscribe: false,
                            next_hop: Some(next_addr),
                            current_htl: htl,
                            contract_key: key,
                            retries: 0,
                            tried_peers: {
                                let mut s = HashSet::new();
                                s.insert(next_addr);
                                s
                            },
                            alternatives: vec![],
                            attempts_at_hop: 1,
                            visited: VisitedPeers::default(),
                            retry_payload: None,
                        }));

                        stats = Some(PutStats {
                            target_peer: PeerKeyLocation::from(
                                next_peer_known
                                    .as_ref()
                                    .expect("piping_started implies next_peer_known")
                                    .clone(),
                            ),
                            contract_location: Location::from(&key),
                        });
                        // No msg or stream_data needed - piping handles forwarding
                        Ok(OperationResult::ContinueOp(OpEnum::Put(PutOp {
                            id,
                            state: new_state,
                            upstream_addr,
                            stats,
                            ack_received: false,
                            speculative_paths: 0,
                        })))
                    } else if let Some(next_peer) = next_peer_known {
                        // Next hop exists but piping didn't start (streaming not appropriate for size)
                        // Forward as non-streaming message
                        let next_addr = next_peer.socket_addr();

                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            peer_addr = %next_addr,
                            htl = htl - 1,
                            phase = "forward",
                            "Forwarding PUT (from streaming) as non-streaming to next hop"
                        );

                        if let Some(event) = NetEventLog::put_request(
                            &id,
                            &op_manager.ring,
                            key,
                            PeerKeyLocation::from(next_peer.clone()),
                            htl.saturating_sub(1),
                        ) {
                            op_manager.ring.register_events(Either::Left(event)).await;
                        }

                        let new_htl = htl.saturating_sub(1);
                        let forward_msg = PutMsg::Request {
                            id,
                            contract: contract.clone(),
                            related_contracts,
                            value: merged_value,
                            htl: new_htl,
                            skip_list: routing_skip_list,
                        };

                        // Forwarding nodes always use non-blocking subscriptions:
                        // blocking_subscribe is a client-side preference that only
                        // applies to the originator node.
                        let new_state = Some(PutState::AwaitingResponse(AwaitingResponseData {
                            subscribe: *msg_subscribe,
                            blocking_subscribe: false,
                            next_hop: Some(next_addr),
                            current_htl: htl,
                            contract_key: key,
                            retries: 0,
                            tried_peers: {
                                let mut s = HashSet::new();
                                s.insert(next_addr);
                                s
                            },
                            alternatives: vec![],
                            attempts_at_hop: 1,
                            visited: VisitedPeers::default(),
                            retry_payload: None,
                        }));

                        stats = Some(PutStats {
                            target_peer: PeerKeyLocation::from(next_peer.clone()),
                            contract_location: Location::from(&key),
                        });

                        // Send ForwardingAck upstream before forwarding (fire-and-forget).
                        if let Some(upstream) = upstream_addr {
                            let ack = NetMessage::from(PutMsg::ForwardingAck {
                                id,
                                contract_key: key,
                            });
                            drop(conn_manager.send(upstream, ack).await);
                        }

                        Ok(OperationResult::SendAndContinue {
                            msg: NetMessage::from(forward_msg),
                            next_hop: Some(next_addr),
                            state: OpEnum::Put(PutOp {
                                id,
                                state: new_state,
                                upstream_addr,
                                stats,
                                ack_received: false,
                                speculative_paths: 0,
                            }),
                            stream_data: None,
                        })
                    } else {
                        // No next hop - we're the final destination
                        tracing::info!(
                            tx = %id,
                            contract = %key,
                            phase = "complete",
                            "Streaming PUT complete at this node"
                        );

                        if is_originator {
                            let own_location = op_manager.ring.connection_manager.own_location();
                            let hash = Some(state_hash_full(&merged_value));
                            let size = Some(merged_value.len());
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                Some(0),
                                hash,
                                size,
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }

                            if *msg_subscribe {
                                start_subscription_after_put(
                                    op_manager,
                                    id,
                                    key,
                                    blocking_subscribe,
                                )
                                .await;
                            }

                            Ok(OperationResult::ContinueOp(OpEnum::Put(PutOp {
                                id,
                                state: Some(PutState::Finished(FinishedData { key })),
                                upstream_addr: None,
                                stats,
                                ack_received: false,
                                speculative_paths: 0,
                            })))
                        } else {
                            // Send ResponseStreaming back to upstream
                            let own_location = op_manager.ring.connection_manager.own_location();
                            let hash = Some(state_hash_full(&merged_value));
                            let size = Some(merged_value.len());
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                None,
                                hash,
                                size,
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }

                            let upstream =
                                upstream_addr.expect("non-originator must have upstream");
                            let response = PutMsg::ResponseStreaming {
                                id,
                                key,
                                continue_forwarding: false,
                            };

                            Ok(OperationResult::SendAndComplete {
                                msg: NetMessage::from(response),
                                next_hop: Some(upstream),
                                stream_data: None,
                            })
                        }
                    }
                }

                // Streaming PUT response handler
                PutMsg::ResponseStreaming {
                    id: _msg_id,
                    key,
                    continue_forwarding,
                } => {
                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        continue_forwarding,
                        is_originator,
                        phase = "streaming_response",
                        "Processing PUT ResponseStreaming"
                    );

                    // Extract current HTL for telemetry
                    let current_htl = match &self.state {
                        Some(PutState::AwaitingResponse(data)) => Some(data.current_htl),
                        _ => None,
                    };

                    if is_originator {
                        // We initiated the streaming PUT - operation complete
                        let hop_count = current_htl
                            .map(|htl| op_manager.ring.max_hops_to_live.saturating_sub(htl));

                        if let Some(event) = NetEventLog::put_success(
                            &id,
                            &op_manager.ring,
                            *key,
                            op_manager.ring.connection_manager.own_location(),
                            hop_count,
                            None, // No hash available in streaming response
                            None, // No size available in streaming response
                        ) {
                            op_manager.ring.register_events(Either::Left(event)).await;
                        }

                        // Start subscription if requested
                        if subscribe {
                            start_subscription_after_put(op_manager, id, *key, blocking_subscribe)
                                .await;
                        }

                        tracing::info!(
                            tx = %id,
                            contract = %key,
                            phase = "complete",
                            "Streaming PUT operation complete (originator)"
                        );

                        Ok(OperationResult::ContinueOp(OpEnum::Put(PutOp {
                            id,
                            state: Some(PutState::Finished(FinishedData { key: *key })),
                            upstream_addr: None,
                            stats,
                            ack_received: false,
                            speculative_paths: 0,
                        })))
                    } else {
                        // Forward response to upstream
                        let upstream = upstream_addr.expect("non-originator must have upstream");

                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            peer_addr = %upstream,
                            phase = "response",
                            "Forwarding PUT ResponseStreaming to upstream"
                        );

                        // Forward as regular Response for simplicity
                        // (streaming is only needed for large payloads, responses are small)
                        let response = PutMsg::Response { id, key: *key };

                        Ok(OperationResult::SendAndComplete {
                            msg: NetMessage::from(response),
                            next_hop: Some(upstream),
                            stream_data: None,
                        })
                    }
                }

                PutMsg::ForwardingAck { id, contract_key } => {
                    // A downstream relay acknowledged forwarding our PUT request.
                    // Mark the operation so the GC task knows the peer is alive.
                    tracing::debug!(
                        tx = %id,
                        contract = %contract_key,
                        phase = "ack",
                        "Received ForwardingAck from downstream"
                    );
                    // Continue waiting for the actual Response
                    Ok(OperationResult::ContinueOp(OpEnum::Put(PutOp {
                        id: *id,
                        state: self.state,
                        upstream_addr,
                        stats,
                        ack_received: true,
                        speculative_paths: self.speculative_paths,
                    })))
                }
            }
        })
    }
}

/// Helper to start subscription after PUT completes (only for originator)
///
/// The `blocking_subscription` parameter controls subscription behavior:
/// - When false (default): subscription completes asynchronously and PUT response
///   is sent immediately
/// - When true: PUT response waits for subscription to complete
///
/// This value comes from the client request's `blocking_subscribe` field
/// (`ContractRequest::Put`).
async fn start_subscription_after_put(
    op_manager: &OpManager,
    parent_tx: Transaction,
    key: ContractKey,
    blocking_subscription: bool,
) {
    // Note: This failed_parents check may be unnecessary since we only spawn the subscription
    // at PUT completion, so there's no earlier child operation that could have failed.
    // Keeping it as defensive check in case of race conditions not currently understood.
    if !op_manager.failed_parents().contains(&parent_tx) {
        let child_tx =
            super::start_subscription_request(op_manager, parent_tx, key, blocking_subscription);
        tracing::debug!(
            tx = %parent_tx,
            child_tx = %child_tx,
            contract = %key,
            blocking = blocking_subscription,
            phase = "subscribe",
            "Started subscription after PUT"
        );
    } else {
        tracing::warn!(
            tx = %parent_tx,
            contract = %key,
            phase = "subscribe",
            "Not starting subscription for failed parent PUT operation"
        );
    }
}

pub(crate) fn start_op(
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
) -> PutOp {
    let key = contract.key();
    let contract_location = Location::from(&key);
    tracing::debug!(contract_location = %contract_location, contract = %key, phase = "request", "Requesting put");

    let id = Transaction::new::<PutMsg>();
    let state = Some(PutState::PrepareRequest(PrepareRequestData {
        contract,
        related_contracts,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    }));

    PutOp {
        id,
        state,
        upstream_addr: None, // Local operation, no upstream peer
        stats: None,
        ack_received: false,
        speculative_paths: 0,
    }
}

/// Create a PUT operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
    blocking_subscribe: bool,
    id: Transaction,
) -> PutOp {
    let key = contract.key();
    let contract_location = Location::from(&key);
    tracing::debug!(contract_location = %contract_location, contract = %key, tx = %id, phase = "request", "Requesting put with existing transaction ID");

    let state = Some(PutState::PrepareRequest(PrepareRequestData {
        contract,
        related_contracts,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    }));

    PutOp {
        id,
        state,
        upstream_addr: None, // Local operation, no upstream peer
        stats: None,
        ack_received: false,
        speculative_paths: 0,
    }
}

// State machine for PUT operations.
//
// State transitions:
// - Originator: PrepareRequest → AwaitingResponse → Finished
// - Forwarder: (receives Request) → AwaitingResponse → (receives Response) → done
// - Final node: (receives Request) → stores contract → sends Response → done
//
// ── Type-state data structs ──────────────────────────────────────────────
//
// Each state in the PUT state machine is a named struct with typed
// transition methods.  This gives compile-time guarantees:
//
//   PrepareRequest ── into_awaiting_response() ──► AwaitingResponse
//   AwaitingResponse ── into_finished()         ──► Finished
//
// Invalid transitions (e.g. Finished → PrepareRequest) are unrepresentable
// because the target type simply has no such method.

/// Data for the PrepareRequest state: originator preparing initial PUT request.
#[derive(Debug, Clone)]
pub struct PrepareRequestData {
    pub contract: ContractContainer,
    pub related_contracts: RelatedContracts<'static>,
    pub value: WrappedState,
    pub htl: usize,
    /// If true, start a subscription after PUT completes
    pub subscribe: bool,
    /// If true, the PUT response waits for the subscription to complete
    pub blocking_subscribe: bool,
}

impl PrepareRequestData {
    /// Transition to AwaitingResponse after sending the PUT request.
    ///
    /// Encodes valid transition: PrepareRequest → AwaitingResponse.
    /// The subscribe/blocking_subscribe flags are carried forward.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern
    pub fn into_awaiting_response(
        self,
        next_hop: Option<std::net::SocketAddr>,
        contract_key: ContractKey,
        alternatives: Vec<PeerKeyLocation>,
        visited: VisitedPeers,
    ) -> AwaitingResponseData {
        let mut tried_peers = HashSet::new();
        if let Some(addr) = next_hop {
            tried_peers.insert(addr);
        }
        AwaitingResponseData {
            subscribe: self.subscribe,
            blocking_subscribe: self.blocking_subscribe,
            next_hop,
            current_htl: self.htl,
            contract_key,
            retries: 0,
            tried_peers,
            alternatives,
            attempts_at_hop: 1,
            visited,
            retry_payload: None,
        }
    }
}

/// Data for the AwaitingResponse state: waiting for downstream node's response.
#[derive(Debug, Clone)]
pub struct AwaitingResponseData {
    /// If true, start a subscription after PUT completes (originator only)
    pub subscribe: bool,
    /// If true, the PUT response waits for the subscription to complete
    pub blocking_subscribe: bool,
    /// Next hop address for routing the outbound message
    pub next_hop: Option<std::net::SocketAddr>,
    /// Current HTL (remaining hops) for hop_count calculation.
    pub current_htl: usize,
    /// Contract key for retry routing.
    pub contract_key: ContractKey,
    /// Number of retry rounds completed (up to MAX_RETRIES).
    pub retries: usize,
    /// Peers already tried at this hop level.
    pub tried_peers: HashSet<std::net::SocketAddr>,
    /// Fallback peers at the current hop, ranked by proximity.
    pub alternatives: Vec<PeerKeyLocation>,
    /// How many peers have been tried at this hop (for HTL reduction on retry).
    pub attempts_at_hop: usize,
    /// Bloom filter tracking peers visited across all hops (prevents re-routing).
    pub visited: VisitedPeers,
    /// Retained contract data for retry (originator only).
    /// None for relay peers since they don't need to retry with the full payload.
    pub retry_payload: Option<PutRetryPayload>,
}

/// Contract data retained by the originator for retry.
#[derive(Debug, Clone)]
pub struct PutRetryPayload {
    pub contract: ContractContainer,
    pub related_contracts: RelatedContracts<'static>,
    pub value: WrappedState,
}

impl AwaitingResponseData {
    /// Transition to Finished on successful PUT response.
    ///
    /// Encodes valid transition: AwaitingResponse → Finished.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern
    pub fn into_finished(self, key: ContractKey) -> FinishedData {
        FinishedData { key }
    }
}

/// Data for the Finished state: PUT operation completed successfully.
#[derive(Debug, Clone, Copy)]
pub struct FinishedData {
    pub key: ContractKey,
}

// ── State enum (wraps the typed structs) ─────────────────────────────────

/// Streaming flow (when enabled and payload > threshold):
/// State machine for PUT operations.
/// - Originator: PrepareRequest → AwaitingResponse → Finished
/// - Forwarder: ReceivedRequest → stores → sends Response → done
#[derive(Debug, Clone)]
pub enum PutState {
    /// Local originator preparing to send initial request.
    PrepareRequest(PrepareRequestData),
    /// Waiting for response from downstream node.
    AwaitingResponse(AwaitingResponseData),
    /// Operation completed successfully.
    Finished(FinishedData),
}

/// Request to insert/update a value into a contract.
/// Called when a client initiates a PUT operation.
pub(crate) async fn request_put(op_manager: &OpManager, put_op: PutOp) -> Result<(), OpError> {
    let (id, contract, value, related_contracts, htl, subscribe, blocking_subscribe) =
        match &put_op.state {
            Some(PutState::PrepareRequest(data)) => (
                put_op.id,
                data.contract.clone(),
                data.value.clone(),
                data.related_contracts.clone(),
                data.htl,
                data.subscribe,
                data.blocking_subscribe,
            ),
            _ => {
                tracing::error!(
                    tx = %put_op.id,
                    state = ?put_op.state,
                    phase = "error",
                    "request_put called with unexpected state"
                );
                return Err(OpError::UnexpectedOpState);
            }
        };

    let key = contract.key();

    tracing::info!(tx = %id, contract = %key, htl, subscribe, phase = "request", "Starting PUT operation");

    // Build initial skip list with our own address
    let mut skip_list = HashSet::new();
    if let Some(own_addr) = op_manager.ring.connection_manager.get_own_addr() {
        skip_list.insert(own_addr);
    }

    // Create the request message
    let msg = PutMsg::Request {
        id,
        contract,
        related_contracts,
        value,
        htl,
        skip_list,
    };

    // Transition to AwaitingResponse and send the message
    // Note: upstream_addr is None because we're the originator
    // next_hop is None initially - we process locally first then determine routing
    let new_op = PutOp {
        id,
        state: Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe,
            blocking_subscribe,
            next_hop: None,
            current_htl: htl,
            contract_key: key,
            retries: 0,
            tried_peers: HashSet::new(),
            alternatives: vec![],
            attempts_at_hop: 1,
            visited: VisitedPeers::default(),
            retry_payload: None,
        })),
        upstream_addr: None,
        stats: None,
        ack_received: false,
        speculative_paths: 0,
    };

    // Send through the operation processing pipeline
    op_manager
        .notify_op_change(NetMessage::from(msg), OpEnum::Put(new_op))
        .await?;

    Ok(())
}

/// Stores the contract state and returns (new_state, state_changed).
/// `state_changed` is true if the stored state was actually modified
/// (old state != new state), which is needed to trigger UPDATE propagation.
async fn put_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
    contract: &ContractContainer,
) -> Result<(WrappedState, bool), OpError> {
    // after the contract has been cached, push the update query
    match op_manager
        .notify_contract_handler(ContractHandlerEvent::PutQuery {
            key,
            state,
            related_contracts,
            contract: Some(contract.clone()),
        })
        .await
    {
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Ok(new_val),
            state_changed,
        }) => {
            // Notify any waiters that this contract has been stored
            op_manager.notify_contract_stored(&key);
            // Invariant: after a successful PUT, the stored state must be non-empty.
            // A successful PutResponse with an empty value indicates a contract handler bug.
            debug_assert!(
                new_val.size() > 0,
                "put_contract: stored state must be non-empty after successful PUT for contract {key}"
            );
            Ok((new_val, state_changed))
        }
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Err(err),
            ..
        }) => {
            tracing::error!(contract = %key, error = %err, phase = "error", "Failed to update contract value");
            Err(OpError::from(err))
            // TODO: not a valid value update, notify back to requester
        }
        Err(err) => Err(err.into()),
        Ok(_) => Err(OpError::UnexpectedOpState),
    }
}

mod messages {
    use std::{collections::HashSet, fmt::Display};

    use freenet_stdlib::prelude::*;
    use serde::{Deserialize, Serialize};

    use crate::message::{InnerMessage, Transaction};
    use crate::ring::Location;
    use crate::transport::peer_connection::StreamId;

    /// Payload for streaming PUT requests.
    /// This struct is serialized and sent via the stream, while the metadata
    /// is sent via the RequestStreaming message.
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct PutStreamingPayload {
        pub contract: ContractContainer,
        #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
        pub related_contracts: RelatedContracts<'static>,
        pub value: WrappedState,
    }

    /// PUT operation messages.
    ///
    /// The PUT operation stores a contract and its initial state in the network.
    /// It uses hop-by-hop routing: each node forwards toward the contract location
    /// and remembers where the request came from to route the response back.
    ///
    /// If a PUT reaches a node that is already subscribed to the contract and the
    /// merged state differs from the input, an Update operation is triggered to
    /// propagate the change to other subscribers.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum PutMsg {
        /// Request to store a contract. Forwarded hop-by-hop toward contract location.
        /// Each receiving node:
        /// 1. Stores the contract locally (caching)
        /// 2. Forwards to the next hop closer to contract location
        /// 3. Remembers upstream_addr to route the response back
        Request {
            id: Transaction,
            contract: ContractContainer,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
            /// Hops to live - decremented at each hop, request fails if reaches 0
            htl: usize,
            /// Addresses to skip when selecting next hop (prevents loops)
            skip_list: HashSet<std::net::SocketAddr>,
        },
        /// Response indicating the PUT completed. Routed hop-by-hop back to originator
        /// using each node's stored upstream_addr.
        Response { id: Transaction, key: ContractKey },

        /// Streaming request to store a large contract. Used when payload exceeds
        /// streaming_threshold (default 64KB). The actual data is sent via a separate
        /// stream identified by stream_id.
        ///
        /// This variant is only used when streaming is enabled in config.
        RequestStreaming {
            id: Transaction,
            /// Identifies the stream carrying the contract and state data
            stream_id: StreamId,
            /// Key of the contract being stored
            contract_key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
            /// Hops to live - decremented at each hop
            htl: usize,
            /// Addresses to skip when selecting next hop
            skip_list: HashSet<std::net::SocketAddr>,
            /// Whether to subscribe to updates after storing
            subscribe: bool,
        },

        /// Streaming response indicating PUT completed for a streaming request.
        /// Sent back to the originator after the stream has been fully received
        /// and the contract stored.
        ResponseStreaming {
            id: Transaction,
            key: ContractKey,
            /// Whether the receiving node should continue forwarding to other peers
            continue_forwarding: bool,
        },

        /// Lightweight ACK sent by a relay peer back to its upstream when it forwards
        /// a PUT request to the next hop. Tells the upstream "I received the data and
        /// am processing it" so the GC task can distinguish dead peers from slow
        /// multi-hop chains. Fire-and-forget — no response expected.
        ForwardingAck {
            id: Transaction,
            contract_key: ContractKey,
        },
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::RequestStreaming { id, .. }
                | Self::ResponseStreaming { id, .. }
                | Self::ForwardingAck { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { contract, .. } => Some(Location::from(contract.id())),
                Self::Response { key, .. } => Some(Location::from(key.id())),
                Self::RequestStreaming { contract_key, .. } => {
                    Some(Location::from(contract_key.id()))
                }
                Self::ResponseStreaming { key, .. } => Some(Location::from(key.id())),
                Self::ForwardingAck { contract_key, .. } => Some(Location::from(contract_key.id())),
            }
        }
    }

    impl Display for PutMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Request {
                    id, contract, htl, ..
                } => {
                    write!(
                        f,
                        "PutRequest(id: {}, key: {}, htl: {})",
                        id,
                        contract.key(),
                        htl
                    )
                }
                Self::Response { id, key } => {
                    write!(f, "PutResponse(id: {}, key: {})", id, key)
                }
                Self::RequestStreaming {
                    id,
                    stream_id,
                    contract_key,
                    total_size,
                    htl,
                    ..
                } => {
                    write!(
                        f,
                        "PutRequestStreaming(id: {}, key: {}, stream: {}, size: {}, htl: {})",
                        id, contract_key, stream_id, total_size, htl
                    )
                }
                Self::ResponseStreaming {
                    id,
                    key,
                    continue_forwarding,
                } => {
                    write!(
                        f,
                        "PutResponseStreaming(id: {}, key: {}, continue: {})",
                        id, key, continue_forwarding
                    )
                }
                Self::ForwardingAck { id, contract_key } => {
                    write!(f, "PutForwardingAck(id: {}, key: {})", id, contract_key)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Transaction;
    use crate::operations::test_utils::{make_contract_key, make_test_contract};

    fn make_put_op(state: Option<PutState>) -> PutOp {
        PutOp {
            id: Transaction::new::<PutMsg>(),
            state,
            upstream_addr: None,
            stats: None,
            ack_received: false,
            speculative_paths: 0,
        }
    }

    // Tests for finalized() method
    #[test]
    fn put_op_finalized_when_state_is_none() {
        let op = make_put_op(None);
        assert!(
            op.finalized(),
            "PutOp should be finalized when state is None"
        );
    }

    #[test]
    fn put_op_finalized_when_state_is_finished() {
        let op = make_put_op(Some(PutState::Finished(FinishedData {
            key: make_contract_key(1),
        })));
        assert!(
            op.finalized(),
            "PutOp should be finalized when state is Finished"
        );
    }

    #[test]
    fn put_op_not_finalized_when_awaiting_response() {
        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
            contract_key: make_contract_key(0),
            retries: 0,
            tried_peers: HashSet::new(),
            alternatives: vec![],
            attempts_at_hop: 1,
            visited: VisitedPeers::default(),
            retry_payload: None,
        })));
        assert!(
            !op.finalized(),
            "PutOp should not be finalized in AwaitingResponse state"
        );
    }

    // Tests for to_host_result() method
    #[test]
    fn put_op_to_host_result_success_when_finished() {
        let key = make_contract_key(1);
        let op = make_put_op(Some(PutState::Finished(FinishedData { key })));
        let result = op.to_host_result();
        assert!(
            result.is_ok(),
            "to_host_result should return Ok for Finished state"
        );

        if let Ok(HostResponse::ContractResponse(
            freenet_stdlib::client_api::ContractResponse::PutResponse { key: returned_key },
        )) = result
        {
            assert_eq!(returned_key, key, "Returned key should match");
        } else {
            panic!("Expected PutResponse");
        }
    }

    #[test]
    fn put_op_to_host_result_error_when_not_finished() {
        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
            contract_key: make_contract_key(0),
            retries: 0,
            tried_peers: HashSet::new(),
            alternatives: vec![],
            attempts_at_hop: 1,
            visited: VisitedPeers::default(),
            retry_payload: None,
        })));
        let result = op.to_host_result();
        assert!(
            result.is_err(),
            "to_host_result should return Err for non-Finished state"
        );
    }

    #[test]
    fn put_op_to_host_result_error_when_none() {
        let op = make_put_op(None);
        let result = op.to_host_result();
        assert!(
            result.is_err(),
            "to_host_result should return Err when state is None"
        );
    }

    // Tests for is_completed() trait method
    #[test]
    fn put_op_is_completed_when_finished() {
        let op = make_put_op(Some(PutState::Finished(FinishedData {
            key: make_contract_key(1),
        })));
        assert!(
            op.is_completed(),
            "is_completed should return true for Finished state"
        );
    }

    #[test]
    fn put_op_is_not_completed_when_in_progress() {
        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
            contract_key: make_contract_key(0),
            retries: 0,
            tried_peers: HashSet::new(),
            alternatives: vec![],
            attempts_at_hop: 1,
            visited: VisitedPeers::default(),
            retry_payload: None,
        })));
        assert!(
            !op.is_completed(),
            "is_completed should return false for AwaitingResponse state"
        );
    }

    // Tests for PutMsg helper methods
    #[test]
    fn put_msg_id_returns_transaction() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Response {
            id: tx,
            key: make_contract_key(1),
        };
        assert_eq!(*msg.id(), tx, "id() should return the transaction ID");
    }

    #[test]
    fn put_msg_display_formats_correctly() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Response {
            id: tx,
            key: make_contract_key(1),
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("PutResponse"),
            "Display should contain message type name"
        );
    }

    // Tests for blocking_subscribe propagation
    #[test]
    fn start_op_propagates_blocking_subscribe_true() {
        let contract = make_test_contract(&[1u8]);
        let op = start_op(
            contract,
            RelatedContracts::default(),
            WrappedState::new(vec![]),
            10,
            true, // subscribe
            true, // blocking_subscribe
        );
        match op.state {
            Some(PutState::PrepareRequest(data)) => {
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
        let contract = make_test_contract(&[1u8]);
        let tx = Transaction::new::<PutMsg>();
        let op = start_op_with_id(
            contract,
            RelatedContracts::default(),
            WrappedState::new(vec![]),
            10,
            true, // subscribe
            true, // blocking_subscribe
            tx,
        );
        match op.state {
            Some(PutState::PrepareRequest(data)) => {
                let blocking_subscribe = data.blocking_subscribe;
                assert!(
                    blocking_subscribe,
                    "blocking_subscribe should be true in PrepareRequest via start_op_with_id"
                );
            }
            other => panic!("Expected PrepareRequest state, got {:?}", other),
        }
    }

    // Tests for outcome() method
    #[test]
    fn put_op_outcome_success_untimed_when_finalized_with_stats() {
        use crate::operations::OpOutcome;
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            upstream_addr: None,
            stats: Some(PutStats {
                target_peer: target_peer.clone(),
                contract_location,
            }),
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
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpFailure { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed for finalized put with stats")
            }
        }
    }

    #[test]
    fn put_op_outcome_irrelevant_when_finalized_without_stats() {
        use crate::operations::OpOutcome;

        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::Finished(FinishedData {
                key: make_contract_key(1),
            })),
            upstream_addr: None,
            stats: None,
            ack_received: false,
            speculative_paths: 0,
        };
        assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
    }

    #[test]
    fn put_op_outcome_failure_when_not_finalized_with_stats() {
        use crate::operations::OpOutcome;
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::AwaitingResponse(AwaitingResponseData {
                subscribe: false,
                blocking_subscribe: false,
                next_hop: None,
                current_htl: 10,
                contract_key: make_contract_key(0),
                retries: 0,
                tried_peers: HashSet::new(),
                alternatives: vec![],
                attempts_at_hop: 1,
                visited: VisitedPeers::default(),
                retry_payload: None,
            })),
            upstream_addr: None,
            stats: Some(PutStats {
                target_peer: target_peer.clone(),
                contract_location,
            }),
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
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpFailure for non-finalized put with stats")
            }
        }
    }

    #[test]
    fn put_op_outcome_incomplete_when_not_finalized_without_stats() {
        use crate::operations::OpOutcome;

        let op = make_put_op(Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
            contract_key: make_contract_key(0),
            retries: 0,
            tried_peers: HashSet::new(),
            alternatives: vec![],
            attempts_at_hop: 1,
            visited: VisitedPeers::default(),
            retry_payload: None,
        })));
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
    }

    #[test]
    fn put_op_failure_routing_info_with_stats() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: None,
            upstream_addr: None,
            stats: Some(PutStats {
                target_peer: target_peer.clone(),
                contract_location,
            }),
            ack_received: false,
            speculative_paths: 0,
        };
        let info = op.failure_routing_info().expect("should have routing info");
        assert_eq!(info.0, target_peer);
        assert_eq!(info.1, contract_location);
    }

    #[test]
    fn put_op_failure_routing_info_without_stats() {
        let op = make_put_op(None);
        assert!(op.failure_routing_info().is_none());
    }

    #[test]
    fn start_op_defaults_blocking_subscribe_false() {
        let contract = make_test_contract(&[1u8]);
        let op = start_op(
            contract,
            RelatedContracts::default(),
            WrappedState::new(vec![]),
            10,
            true,  // subscribe
            false, // blocking_subscribe
        );
        match op.state {
            Some(PutState::PrepareRequest(data)) => {
                let blocking_subscribe = data.blocking_subscribe;
                assert!(
                    !blocking_subscribe,
                    "blocking_subscribe should be false by default"
                );
            }
            other => panic!("Expected PrepareRequest state, got {:?}", other),
        }
    }

    /// Verify that start_op creates a PutOp with stats=None initially.
    /// Stats should only be populated when a target peer is chosen during forwarding.
    #[test]
    fn start_op_creates_put_with_no_stats() {
        let contract = make_test_contract(&[1u8]);
        let op = start_op(
            contract,
            RelatedContracts::default(),
            WrappedState::new(vec![]),
            10,
            false,
            false,
        );
        assert!(
            op.stats.is_none(),
            "start_op should create PutOp with stats=None"
        );
        // Without stats, outcome should be Incomplete (not finalized, no stats)
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
    }

    /// Simulate a put operation lifecycle: initially no stats, then stats are set
    /// when forwarding, then state is Finished → outcome should be SuccessUntimed.
    #[test]
    fn put_op_stats_lifecycle_from_initial_to_finished() {
        use crate::ring::{Location, PeerKeyLocation};

        let target_peer = PeerKeyLocation::random();
        let contract_location = Location::random();

        // Step 1: Initial state - no stats, PrepareRequest
        let mut op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: Some(PutState::PrepareRequest(PrepareRequestData {
                contract: make_test_contract(&[1u8]),
                related_contracts: RelatedContracts::default(),
                value: WrappedState::new(vec![]),
                htl: 10,
                subscribe: false,
                blocking_subscribe: false,
            })),
            upstream_addr: None,
            stats: None,
            ack_received: false,
            speculative_paths: 0,
        };
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
        assert!(op.failure_routing_info().is_none());

        // Step 2: Stats are set when forwarding to next peer
        op.stats = Some(PutStats {
            target_peer: target_peer.clone(),
            contract_location,
        });
        op.state = Some(PutState::AwaitingResponse(AwaitingResponseData {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 9,
            contract_key: make_contract_key(0),
            retries: 0,
            tried_peers: HashSet::new(),
            alternatives: vec![],
            attempts_at_hop: 1,
            visited: VisitedPeers::default(),
            retry_payload: None,
        }));
        // Not finalized yet, but has stats → failure
        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer: peer, ..
            } => assert_eq!(*peer, target_peer),
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpSuccessUntimed { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpFailure for in-progress put with stats")
            }
        }
        assert!(op.failure_routing_info().is_some());

        // Step 3: Operation finishes successfully
        op.state = Some(PutState::Finished(FinishedData {
            key: make_contract_key(1),
        }));
        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer: peer,
                contract_location: loc,
            } => {
                assert_eq!(*peer, target_peer);
                assert_eq!(loc, contract_location);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpFailure { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed for finished put with stats")
            }
        }
    }

    /// Verify that a forwarding node (state=None, stats=None) returns Irrelevant
    /// since it is finalized (state is None) but has no routing decision to report.
    #[test]
    fn put_op_forwarding_node_outcome_is_irrelevant() {
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: None,
            upstream_addr: Some("127.0.0.1:12345".parse().unwrap()),
            stats: None,
            ack_received: false,
            speculative_paths: 0,
        };
        // state=None → finalized, stats=None → Irrelevant
        assert!(op.finalized());
        assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
    }

    #[test]
    fn is_client_initiated_true_when_no_upstream() {
        let op = make_put_op(None);
        assert!(op.is_client_initiated());
    }

    #[test]
    fn is_client_initiated_false_when_forwarded() {
        let op = PutOp {
            id: Transaction::new::<PutMsg>(),
            state: None,
            upstream_addr: Some("127.0.0.1:12345".parse().unwrap()),
            stats: None,
            ack_received: false,
            speculative_paths: 0,
        };
        assert!(!op.is_client_initiated());
    }
}
