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
    OperationResult,
};
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

pub(crate) struct PutOp {
    pub id: Transaction,
    state: Option<PutState>,
    /// The address we received this operation's message from.
    /// Used for connection-based routing: responses are sent back to this address.
    upstream_addr: Option<std::net::SocketAddr>,
}

impl PutOp {
    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        // todo: track in the future
        // match &self.stats {
        //     Some(PutStats {
        //         contract_location,
        //         payload_size,
        //         // first_response_time: Some((response_start, Some(response_end))),
        //         transfer_time: Some((transfer_start, Some(transfer_end))),
        //         target: Some(target),
        //         ..
        //     }) => {
        //         let payload_transfer_time: Duration = *transfer_end - *transfer_start;
        //         // in puts both times are equivalent since when the transfer is initialized
        //         // it already contains the payload
        //         let first_response_time = payload_transfer_time;
        //         OpOutcome::ContractOpSuccess {
        //             target_peer: target,
        //             contract_location: *contract_location,
        //             payload_size: *payload_size,
        //             payload_transfer_time,
        //             first_response_time,
        //         }
        //     }
        //     Some(_) => OpOutcome::Incomplete,
        //     None => OpOutcome::Irrelevant,
        // }
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        self.state.is_none() || matches!(self.state, Some(PutState::Finished { .. }))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(PutState::Finished { key }) = &self.state {
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
            Some(PutState::AwaitingResponse { next_hop, .. }) => *next_hop,
            _ => None,
        }
    }

    /// Get the current HTL (remaining hops) for this operation.
    /// Returns None if the operation is not in AwaitingResponse state.
    pub(crate) fn get_current_htl(&self) -> Option<usize> {
        match &self.state {
            Some(PutState::AwaitingResponse { current_htl, .. }) => Some(*current_htl),
            _ => None,
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
            Some(PutState::PrepareRequest { contract, htl, .. }) => {
                (Some(contract.key()), Some(*htl))
            }
            Some(PutState::AwaitingResponse { current_htl, .. }) => (None, Some(*current_htl)),
            Some(PutState::Finished { key }) => (Some(*key), None),
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

        // Send the error to the client via the result router
        if let Err(err) = op_manager
            .result_router_tx
            .send((self.id, error_result))
            .await
        {
            tracing::error!(
                tx = %self.id,
                error = %err,
                "Failed to send abort notification to client"
            );
        }

        // Mark the operation as completed so it's removed from tracking
        op_manager.completed(self.id);
        Ok(())
    }
}

impl IsOperationCompleted for PutOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(put::PutState::Finished { .. }))
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
                let _ = op_manager.push(tx, op).await;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                // Check if this is a response message - if so, the operation was likely
                // cleaned up due to timeout and we should not create a new operation
                if matches!(msg, PutMsg::Response { .. }) {
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
                Some(PutState::PrepareRequest { subscribe, .. }) => *subscribe,
                Some(PutState::AwaitingResponse { subscribe, .. }) => *subscribe,
                _ => false,
            };
            let blocking_subscribe = match &self.state {
                Some(PutState::PrepareRequest {
                    blocking_subscribe, ..
                }) => *blocking_subscribe,
                Some(PutState::AwaitingResponse {
                    blocking_subscribe, ..
                }) => *blocking_subscribe,
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
                    let was_seeding = op_manager.ring.is_seeding_contract(&key);

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

                    // Mark as seeding if not already
                    if !was_seeding {
                        let evicted = op_manager.ring.seed_contract(key, value.size() as u64);
                        super::announce_contract_cached(op_manager, &key).await;

                        // Clean up interest tracking for evicted contracts
                        let mut removed_contracts = Vec::new();
                        for evicted_key in evicted {
                            if op_manager
                                .interest_manager
                                .unregister_local_seeding(&evicted_key)
                            {
                                removed_contracts.push(evicted_key);
                            }
                        }

                        // Register local interest for delta-based sync
                        let became_interested =
                            op_manager.interest_manager.register_local_seeding(&key);

                        // Broadcast interest changes to peers
                        let added = if became_interested { vec![key] } else { vec![] };
                        if !added.is_empty() || !removed_contracts.is_empty() {
                            super::broadcast_change_interests(op_manager, added, removed_contracts)
                                .await;
                        }
                    }

                    // Invariant: after storing and seeding, the contract MUST be in the seed list.
                    debug_assert!(
                        op_manager.ring.is_seeding_contract(&key),
                        "PUT Request: contract {key} must be in seed list after put_contract + seed_contract"
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
                            .closest_potentially_caching(&key, &new_skip_list)
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

                        let (forward_msg, stream_data) = if should_use_streaming(
                            op_manager.streaming_enabled,
                            op_manager.streaming_threshold,
                            payload_size,
                        ) {
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
                        let new_state = Some(PutState::AwaitingResponse {
                            subscribe,
                            blocking_subscribe,
                            next_hop: Some(next_addr),
                            current_htl: htl,
                        });

                        Ok(OperationResult {
                            return_msg: Some(NetMessage::from(forward_msg)),
                            next_hop: Some(next_addr),
                            state: Some(OpEnum::Put(PutOp {
                                id,
                                state: new_state,
                                upstream_addr,
                            })),
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
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                Some(0), // Stored locally, 0 hops
                                hash,
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

                            Ok(OperationResult {
                                return_msg: None,
                                next_hop: None,
                                state: Some(OpEnum::Put(PutOp {
                                    id,
                                    state: Some(PutState::Finished { key }),
                                    upstream_addr: None,
                                })),
                                stream_data: None,
                            })
                        } else {
                            // Non-originator target peer - emit put_success for convergence checking
                            // Without this event, the target peer's stored state won't be tracked,
                            // causing convergence checks to fail (they need contracts replicated
                            // across multiple peers). See issue #2680.
                            let own_location = op_manager.ring.connection_manager.own_location();
                            let hash = Some(state_hash_full(&merged_value));
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                None, // hop_count unknown without initial HTL
                                hash,
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }

                            // Send response back to upstream
                            let response = PutMsg::Response { id, key };
                            let upstream =
                                upstream_addr.expect("non-originator must have upstream");

                            Ok(OperationResult {
                                return_msg: Some(NetMessage::from(response)),
                                next_hop: Some(upstream),
                                state: None, // Operation complete for this node
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
                            Some(PutState::AwaitingResponse { current_htl, .. }) => {
                                Some(*current_htl)
                            }
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
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }
                        }

                        // Start subscription if requested
                        if subscribe {
                            start_subscription_after_put(op_manager, id, *key, blocking_subscribe)
                                .await;
                        }

                        Ok(OperationResult {
                            return_msg: None,
                            next_hop: None,
                            state: Some(OpEnum::Put(PutOp {
                                id,
                                state: Some(PutState::Finished { key: *key }),
                                upstream_addr: None,
                            })),
                            stream_data: None,
                        })
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

                        Ok(OperationResult {
                            return_msg: Some(NetMessage::from(response)),
                            next_hop: Some(upstream),
                            state: None, // Operation complete for this node
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
                    // Check if streaming is enabled at runtime
                    if !op_manager.streaming_enabled {
                        tracing::warn!(
                            tx = %id,
                            contract = %contract_key,
                            stream_id = %stream_id,
                            "PUT RequestStreaming received but streaming is disabled"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }

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
                    let stream_handle = match op_manager
                        .orphan_stream_registry()
                        .claim_or_wait(*stream_id, STREAM_CLAIM_TIMEOUT)
                        .await
                    {
                        Ok(handle) => handle,
                        Err(OrphanStreamError::AlreadyClaimed) => {
                            tracing::debug!(
                                tx = %id,
                                stream_id = %stream_id,
                                "PUT RequestStreaming skipped — stream already claimed (dedup)"
                            );
                            return Err(OpError::OpNotPresent(id));
                        }
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
                            .closest_potentially_caching(contract_key, &routing_skip_list)
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
                            op_manager.streaming_enabled,
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
                    let was_seeding = op_manager.ring.is_seeding_contract(&key);
                    let (merged_value, _state_changed) = put_contract(
                        op_manager,
                        key,
                        value.clone(),
                        related_contracts.clone(),
                        contract,
                    )
                    .await?;

                    // Mark as seeding if not already
                    if !was_seeding {
                        let evicted = op_manager.ring.seed_contract(key, value.size() as u64);
                        super::announce_contract_cached(op_manager, &key).await;

                        let mut removed_contracts = Vec::new();
                        for evicted_key in evicted {
                            if op_manager
                                .interest_manager
                                .unregister_local_seeding(&evicted_key)
                            {
                                removed_contracts.push(evicted_key);
                            }
                        }

                        let became_interested =
                            op_manager.interest_manager.register_local_seeding(&key);
                        let added = if became_interested { vec![key] } else { vec![] };
                        if !added.is_empty() || !removed_contracts.is_empty() {
                            super::broadcast_change_interests(op_manager, added, removed_contracts)
                                .await;
                        }
                    }

                    // Invariant: after storing and seeding, the contract MUST be in the seed list.
                    debug_assert!(
                        op_manager.ring.is_seeding_contract(&key),
                        "PUT Streaming: contract {key} must be in seed list after put_contract + seed_contract"
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
                        let new_state = Some(PutState::AwaitingResponse {
                            subscribe: *msg_subscribe,
                            blocking_subscribe: false,
                            next_hop: Some(next_addr),
                            current_htl: htl,
                        });

                        // No return_msg or stream_data needed - piping handles forwarding
                        Ok(OperationResult {
                            return_msg: None,
                            next_hop: None, // Piping already sent to next_addr
                            state: Some(OpEnum::Put(PutOp {
                                id,
                                state: new_state,
                                upstream_addr,
                            })),
                            stream_data: None,
                        })
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
                        let new_state = Some(PutState::AwaitingResponse {
                            subscribe: *msg_subscribe,
                            blocking_subscribe: false,
                            next_hop: Some(next_addr),
                            current_htl: htl,
                        });

                        Ok(OperationResult {
                            return_msg: Some(NetMessage::from(forward_msg)),
                            next_hop: Some(next_addr),
                            state: Some(OpEnum::Put(PutOp {
                                id,
                                state: new_state,
                                upstream_addr,
                            })),
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
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                Some(0),
                                hash,
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

                            Ok(OperationResult {
                                return_msg: None,
                                next_hop: None,
                                state: Some(OpEnum::Put(PutOp {
                                    id,
                                    state: Some(PutState::Finished { key }),
                                    upstream_addr: None,
                                })),
                                stream_data: None,
                            })
                        } else {
                            // Send ResponseStreaming back to upstream
                            let own_location = op_manager.ring.connection_manager.own_location();
                            let hash = Some(state_hash_full(&merged_value));
                            if let Some(event) = NetEventLog::put_success(
                                &id,
                                &op_manager.ring,
                                key,
                                own_location,
                                None,
                                hash,
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

                            Ok(OperationResult {
                                return_msg: Some(NetMessage::from(response)),
                                next_hop: Some(upstream),
                                state: None,
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
                    // Check if streaming is enabled at runtime
                    if !op_manager.streaming_enabled {
                        tracing::warn!(
                            tx = %id,
                            contract = %key,
                            "PUT ResponseStreaming received but streaming is disabled"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }

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
                        Some(PutState::AwaitingResponse { current_htl, .. }) => Some(*current_htl),
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

                        Ok(OperationResult {
                            return_msg: None,
                            next_hop: None,
                            state: Some(OpEnum::Put(PutOp {
                                id,
                                state: Some(PutState::Finished { key: *key }),
                                upstream_addr: None,
                            })),
                            stream_data: None,
                        })
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

                        Ok(OperationResult {
                            return_msg: Some(NetMessage::from(response)),
                            next_hop: Some(upstream),
                            state: None, // Operation complete for this node
                            stream_data: None,
                        })
                    }
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
    let state = Some(PutState::PrepareRequest {
        contract,
        related_contracts,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    });

    PutOp {
        id,
        state,
        upstream_addr: None, // Local operation, no upstream peer
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

    let state = Some(PutState::PrepareRequest {
        contract,
        related_contracts,
        value,
        htl,
        subscribe,
        blocking_subscribe,
    });

    PutOp {
        id,
        state,
        upstream_addr: None, // Local operation, no upstream peer
    }
}

/// State machine for PUT operations.
///
/// State transitions:
/// - Originator: PrepareRequest → AwaitingResponse → Finished
/// - Forwarder: (receives Request) → AwaitingResponse → (receives Response) → done
/// - Final node: (receives Request) → stores contract → sends Response → done
///
/// Streaming flow (when enabled and payload > threshold):
/// State machine for PUT operations.
/// - Originator: PrepareRequest → AwaitingResponse → Finished
/// - Forwarder: ReceivedRequest → stores → sends Response → done
#[derive(Debug, Clone)]
pub enum PutState {
    /// Local originator preparing to send initial request.
    PrepareRequest {
        contract: ContractContainer,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
        htl: usize,
        /// If true, start a subscription after PUT completes
        subscribe: bool,
        /// If true, the PUT response waits for the subscription to complete
        blocking_subscribe: bool,
    },
    /// Waiting for response from downstream node.
    AwaitingResponse {
        /// If true, start a subscription after PUT completes (originator only)
        subscribe: bool,
        /// If true, the PUT response waits for the subscription to complete
        blocking_subscribe: bool,
        /// Next hop address for routing the outbound message
        next_hop: Option<std::net::SocketAddr>,
        /// Current HTL (remaining hops) for hop_count calculation.
        current_htl: usize,
    },
    /// Operation completed successfully.
    Finished { key: ContractKey },
}

/// Request to insert/update a value into a contract.
/// Called when a client initiates a PUT operation.
pub(crate) async fn request_put(op_manager: &OpManager, put_op: PutOp) -> Result<(), OpError> {
    let (id, contract, value, related_contracts, htl, subscribe, blocking_subscribe) =
        match &put_op.state {
            Some(PutState::PrepareRequest {
                contract,
                value,
                related_contracts,
                htl,
                subscribe,
                blocking_subscribe,
            }) => (
                put_op.id,
                contract.clone(),
                value.clone(),
                related_contracts.clone(),
                *htl,
                *subscribe,
                *blocking_subscribe,
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
        state: Some(PutState::AwaitingResponse {
            subscribe,
            blocking_subscribe,
            next_hop: None,
            current_htl: htl,
        }),
        upstream_addr: None,
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
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::RequestStreaming { id, .. }
                | Self::ResponseStreaming { id, .. } => id,
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
        let op = make_put_op(Some(PutState::Finished {
            key: make_contract_key(1),
        }));
        assert!(
            op.finalized(),
            "PutOp should be finalized when state is Finished"
        );
    }

    #[test]
    fn put_op_not_finalized_when_awaiting_response() {
        let op = make_put_op(Some(PutState::AwaitingResponse {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        }));
        assert!(
            !op.finalized(),
            "PutOp should not be finalized in AwaitingResponse state"
        );
    }

    // Tests for to_host_result() method
    #[test]
    fn put_op_to_host_result_success_when_finished() {
        let key = make_contract_key(1);
        let op = make_put_op(Some(PutState::Finished { key }));
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
        let op = make_put_op(Some(PutState::AwaitingResponse {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        }));
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
        let op = make_put_op(Some(PutState::Finished {
            key: make_contract_key(1),
        }));
        assert!(
            op.is_completed(),
            "is_completed should return true for Finished state"
        );
    }

    #[test]
    fn put_op_is_not_completed_when_in_progress() {
        let op = make_put_op(Some(PutState::AwaitingResponse {
            subscribe: false,
            blocking_subscribe: false,
            next_hop: None,
            current_htl: 10,
        }));
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
            Some(PutState::PrepareRequest {
                blocking_subscribe, ..
            }) => {
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
            Some(PutState::PrepareRequest {
                blocking_subscribe, ..
            }) => {
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
            Some(PutState::PrepareRequest {
                blocking_subscribe, ..
            }) => {
                assert!(
                    !blocking_subscribe,
                    "blocking_subscribe should be false by default"
                );
            }
            other => panic!("Expected PrepareRequest state, got {:?}", other),
        }
    }
}
