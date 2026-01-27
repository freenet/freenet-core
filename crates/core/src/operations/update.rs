use either::Either;
use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

pub(crate) use self::messages::{BroadcastStreamingPayload, UpdateMsg, UpdateStreamingPayload};
use super::{
    should_use_streaming, OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult,
};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{InnerMessage, NetMessage, NodeEvent, Transaction};
use crate::node::IsOperationCompleted;
use crate::ring::{Location, PeerKeyLocation, RingError};
use crate::transport::peer_connection::StreamId;
use crate::{
    client_events::HostResult,
    node::{NetworkBridge, OpManager},
    tracing::{state_hash_full, NetEventLog},
};
use std::net::SocketAddr;

pub(crate) struct UpdateOp {
    pub id: Transaction,
    pub(crate) state: Option<UpdateState>,
    stats: Option<UpdateStats>,
    /// The address we received this operation's message from.
    /// Used for connection-based routing: responses are sent back to this address.
    upstream_addr: Option<std::net::SocketAddr>,
}

impl UpdateOp {
    pub fn outcome(&self) -> OpOutcome<'_> {
        OpOutcome::Irrelevant
    }

    pub fn finalized(&self) -> bool {
        matches!(self.state, None | Some(UpdateState::Finished { .. }))
    }

    /// Get the next hop address for hop-by-hop routing.
    /// For UPDATE, this extracts the socket address from the stats.target field.
    pub fn get_next_hop_addr(&self) -> Option<std::net::SocketAddr> {
        self.stats
            .as_ref()
            .and_then(|s| s.target.as_ref())
            .and_then(|t| t.socket_addr())
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(UpdateState::Finished { key, summary }) = &self.state {
            tracing::debug!(
                "Creating UpdateResponse for transaction {} with key {} and summary length {}",
                self.id,
                key,
                summary.size()
            );
            Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::UpdateResponse {
                    key: *key,
                    summary: summary.clone(),
                },
            ))
        } else {
            tracing::error!(
                tx = %self.id,
                state = ?self.state,
                phase = "error",
                "UPDATE operation failed to finish successfully"
            );
            Err(ErrorKind::OperationError {
                cause: "update didn't finish successfully".into(),
            }
            .into())
        }
    }

    /// Handle aborted connections by failing the operation immediately.
    ///
    /// UPDATE operations don't have alternative routes to try. When the connection
    /// drops, we notify the client of the failure so they can retry.
    pub(crate) async fn handle_abort(self, op_manager: &OpManager) -> Result<(), OpError> {
        tracing::warn!(
            tx = %self.id,
            "Update operation aborted due to connection failure"
        );

        // Create an error result to notify the client
        let error_result: crate::client_events::HostResult =
            Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "Update operation failed: peer connection dropped".into(),
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

struct UpdateStats {
    target: Option<PeerKeyLocation>,
}

struct UpdateExecution {
    value: WrappedState,
    summary: StateSummary<'static>,
    changed: bool,
}

pub(crate) struct UpdateResult {}

impl TryFrom<UpdateOp> for UpdateResult {
    type Error = OpError;

    fn try_from(op: UpdateOp) -> Result<Self, Self::Error> {
        if matches!(op.state, None | Some(UpdateState::Finished { .. })) {
            Ok(UpdateResult {})
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

impl Operation for UpdateOp {
    type Message = UpdateMsg;
    type Result = UpdateResult;

    async fn load_or_init<'a>(
        op_manager: &'a crate::node::OpManager,
        msg: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Result<super::OpInitialization<Self>, OpError> {
        let tx = *msg.id();
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Update(update_op))) => {
                Ok(OpInitialization {
                    op: update_op,
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
                tracing::debug!(tx = %tx, ?source_addr, "initializing new op");
                Ok(OpInitialization {
                    op: Self {
                        state: Some(UpdateState::ReceivedRequest),
                        id: tx,
                        stats: None, // don't care about stats in target peers
                        upstream_addr: source_addr, // Connection-based routing: store who sent us this request
                    },
                    source_addr,
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    fn id(&self) -> &crate::message::Transaction {
        &self.id
    }

    fn process_message<'a, NB: NetworkBridge>(
        self,
        _conn_manager: &'a mut NB,
        op_manager: &'a crate::node::OpManager,
        input: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<super::OperationResult, OpError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let return_msg;
            let new_state;
            let stats = self.stats;
            // Track the next hop when forwarding RequestUpdate to another peer
            let mut forward_hop: Option<SocketAddr> = None;
            let mut stream_data: Option<(StreamId, bytes::Bytes)> = None;

            match input {
                UpdateMsg::RequestUpdate {
                    id,
                    key,
                    related_contracts,
                    value,
                } => {
                    // Use upstream_addr for routing decisions (not PeerKeyLocation lookup)
                    // because get_peer_location_by_addr() can fail for transient connections
                    let self_location = op_manager.ring.connection_manager.own_location();
                    let executing_addr = self_location.socket_addr();

                    tracing::debug!(
                        tx = %id,
                        %key,
                        executing_peer = ?executing_addr,
                        request_sender = ?source_addr,
                        "UPDATE RequestUpdate: processing request"
                    );

                    {
                        // First check if we have the contract locally and capture state for telemetry.
                        // NOTE: There's a theoretical TOCTOU race where state could change between this
                        // GetQuery and the update_contract() call. This is acceptable for telemetry
                        // purposes - the hashes provide debugging visibility, not correctness guarantees.
                        let state_before = match op_manager
                            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                                instance_id: *key.id(),
                                return_contract_code: false,
                            })
                            .await
                        {
                            Ok(ContractHandlerEvent::GetResponse {
                                response: Ok(StoreResponse { state: Some(s), .. }),
                                ..
                            }) => {
                                tracing::debug!(tx = %id, %key, "Contract exists locally, handling UPDATE");
                                Some(s)
                            }
                            _ => {
                                tracing::debug!(tx = %id, %key, "Contract not found locally");
                                None
                            }
                        };

                        if state_before.is_some() {
                            // We have the contract - handle UPDATE locally
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "Handling UPDATE locally - contract exists"
                            );

                            // Compute before hash for telemetry
                            let hash_before = state_before.as_ref().map(state_hash_full);

                            // Update contract locally
                            // Note: RequestUpdate sender should NOT be excluded from broadcast.
                            // Unlike BroadcastTo (where sender has the state), RequestUpdate sender
                            // is REQUESTING we apply an update and needs to receive the result
                            // via subscription to notify their client.
                            let UpdateExecution {
                                value: updated_value,
                                summary,
                                changed,
                                ..
                            } = update_contract(
                                op_manager,
                                *key,
                                UpdateData::State(State::from(value.clone())),
                                related_contracts.clone(),
                            )
                            .await?;

                            // Compute after hash for telemetry
                            let hash_after = Some(state_hash_full(&updated_value));

                            // Emit telemetry: UPDATE succeeded at this peer
                            // Use source_addr to get the requester's PeerKeyLocation
                            let requester_addr = source_addr.expect(
                                "remote UpdateMsg::RequestUpdate must have source_addr for telemetry",
                            );
                            if let Some(requester_pkl) = op_manager
                                .ring
                                .connection_manager
                                .get_peer_by_addr(requester_addr)
                            {
                                if let Some(event) = NetEventLog::update_success(
                                    id,
                                    &op_manager.ring,
                                    *key,
                                    requester_pkl,
                                    hash_before,
                                    hash_after.clone(),
                                ) {
                                    op_manager.ring.register_events(Either::Left(event)).await;
                                }
                            }

                            if !changed {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    "UPDATE yielded no state change, skipping broadcast"
                                );
                            } else {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    "UPDATE succeeded, state changed"
                                );
                            }

                            // Network peer propagation is automatic via BroadcastStateChange
                            // event emitted by the executor when state changes.
                            // Use upstream_addr to determine if we're the originator
                            if self.upstream_addr.is_none() {
                                new_state = Some(UpdateState::Finished {
                                    key: *key,
                                    summary: summary.clone(),
                                });
                            } else {
                                new_state = None;
                            }
                            return_msg = None;
                        } else {
                            // Contract not found locally - forward to another peer
                            let self_addr = op_manager.ring.connection_manager.peer_addr()?;
                            // Use source_addr directly instead of PeerKeyLocation lookup
                            let sender_addr = source_addr
                                .expect("remote UpdateMsg::RequestUpdate must have source_addr");
                            let skip_list = vec![self_addr, sender_addr];

                            let next_target = op_manager
                                .ring
                                .closest_potentially_caching(key, skip_list.as_slice());

                            if let Some(forward_target) = next_target {
                                let forward_addr = forward_target
                                    .socket_addr()
                                    .expect("forward target must have socket address");
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    next_peer = %forward_addr,
                                    "Forwarding UPDATE to peer that might have contract"
                                );

                                // Forward RequestUpdate to the next hop
                                // Check if we should use streaming for this payload
                                let payload = UpdateStreamingPayload {
                                    related_contracts: related_contracts.clone(),
                                    value: value.clone(),
                                };
                                let payload_bytes = bincode::serialize(&payload).map_err(|e| {
                                    OpError::NotificationChannelError(format!(
                                        "Failed to serialize UpdateStreamingPayload: {e}"
                                    ))
                                })?;
                                let payload_size = payload_bytes.len();

                                if should_use_streaming(
                                    op_manager.streaming_enabled,
                                    op_manager.streaming_threshold,
                                    payload_size,
                                ) {
                                    crate::config::GlobalTestMetrics::record_streaming_send();
                                    let sid = StreamId::next_operations();
                                    tracing::debug!(
                                        tx = %id,
                                        %key,
                                        stream_id = %sid,
                                        payload_size,
                                        "Using streaming for UPDATE RequestUpdate forward"
                                    );
                                    return_msg = Some(UpdateMsg::RequestUpdateStreaming {
                                        id: *id,
                                        stream_id: sid,
                                        key: *key,
                                        total_size: payload_size as u64,
                                    });
                                    stream_data = Some((sid, bytes::Bytes::from(payload_bytes)));
                                } else {
                                    crate::config::GlobalTestMetrics::record_inline_send();
                                    return_msg = Some(UpdateMsg::RequestUpdate {
                                        id: *id,
                                        key: *key,
                                        related_contracts: related_contracts.clone(),
                                        value: value.clone(),
                                    });
                                }
                                new_state = None;
                                // Track where to forward this message
                                forward_hop = Some(forward_addr);
                            } else {
                                // No peers available and we don't have the contract - log error
                                let candidates = op_manager
                                    .ring
                                    .k_closest_potentially_caching(key, skip_list.as_slice(), 5)
                                    .into_iter()
                                    .filter_map(|loc| loc.socket_addr())
                                    .map(|addr| format!("{:.8}", addr))
                                    .collect::<Vec<_>>();
                                let connection_count =
                                    op_manager.ring.connection_manager.num_connections();
                                tracing::error!(
                                    tx = %id,
                                    contract = %key,
                                    candidates = ?candidates,
                                    connection_count,
                                    peer_addr = ?sender_addr,
                                    phase = "error",
                                    "Cannot handle UPDATE: contract not found locally and no peers to forward to"
                                );
                                return Err(OpError::RingError(RingError::NoCachingPeers(
                                    *key.id(),
                                )));
                            }
                        }
                    }
                }
                UpdateMsg::BroadcastTo {
                    id,
                    key,
                    payload,
                    sender_summary_bytes,
                } => {
                    // Use source_addr directly instead of PeerKeyLocation lookup
                    let sender_addr = source_addr.expect("BroadcastTo requires source_addr");
                    let self_location = op_manager.ring.connection_manager.own_location();

                    // Convert sender's summary bytes to StateSummary
                    let sender_summary = StateSummary::from(sender_summary_bytes.clone());

                    // Update sender's cached summary in our interest manager
                    if let Some(sender_pkl) = op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(sender_addr)
                    {
                        let sender_key = crate::ring::PeerKey::from(sender_pkl.pub_key().clone());
                        op_manager.interest_manager.update_peer_summary(
                            key,
                            &sender_key,
                            Some(sender_summary.clone()),
                        );
                    }

                    // Convert payload to UpdateData
                    let update_data = match payload {
                        crate::message::DeltaOrFullState::Delta(bytes) => {
                            tracing::debug!(
                                contract = %key,
                                delta_size = bytes.len(),
                                "Received delta update"
                            );
                            UpdateData::Delta(StateDelta::from(bytes.clone()))
                        }
                        crate::message::DeltaOrFullState::FullState(bytes) => {
                            tracing::debug!(
                                contract = %key,
                                state_size = bytes.len(),
                                "Received full state update"
                            );
                            UpdateData::State(State::from(bytes.clone()))
                        }
                    };

                    // For telemetry, convert payload bytes to WrappedState
                    // (for deltas, we use the delta bytes as a placeholder)
                    let payload_bytes = match payload {
                        crate::message::DeltaOrFullState::FullState(bytes)
                        | crate::message::DeltaOrFullState::Delta(bytes) => bytes,
                    };
                    let state_for_telemetry = WrappedState::from(payload_bytes.clone());

                    // Emit telemetry: broadcast received from upstream peer
                    if let Some(requester_pkl) = op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(sender_addr)
                    {
                        if let Some(event) = NetEventLog::update_broadcast_received(
                            id,
                            &op_manager.ring,
                            *key,
                            requester_pkl,
                            state_for_telemetry.clone(),
                        ) {
                            op_manager.ring.register_events(Either::Left(event)).await;
                        }
                    }

                    tracing::debug!("Attempting contract value update - BroadcastTo - update");
                    let is_delta = matches!(payload, crate::message::DeltaOrFullState::Delta(_));
                    let update_result =
                        update_contract(op_manager, *key, update_data, RelatedContracts::default())
                            .await;

                    let UpdateExecution {
                        value: updated_value,
                        summary: _summary,
                        changed,
                        ..
                    } = match update_result {
                        Ok(result) => result,
                        Err(err) => {
                            if is_delta {
                                // Delta application failed - send ResyncRequest to get full state
                                // This is a critical debugging event for state divergence issues
                                tracing::warn!(
                                    tx = %id,
                                    contract = %key,
                                    sender = %sender_addr,
                                    error = %err,
                                    event = "delta_apply_failed",
                                    "Delta application failed, sending ResyncRequest to get full state"
                                );

                                // Clear cached summary for sender since it's out of sync
                                if let Some(sender_pkl) = op_manager
                                    .ring
                                    .connection_manager
                                    .get_peer_by_addr(sender_addr)
                                {
                                    let sender_key =
                                        crate::ring::PeerKey::from(sender_pkl.pub_key().clone());
                                    op_manager.interest_manager.update_peer_summary(
                                        key,
                                        &sender_key,
                                        None,
                                    );
                                }

                                // Send ResyncRequest to sender
                                tracing::info!(
                                    tx = %id,
                                    contract = %key,
                                    target = %sender_addr,
                                    event = "resync_request_sent",
                                    "Sending ResyncRequest to peer after delta failure"
                                );
                                let _ = op_manager
                                    .notify_node_event(
                                        crate::message::NodeEvent::SendInterestMessage {
                                            target: sender_addr,
                                            message:
                                                crate::message::InterestMessage::ResyncRequest {
                                                    key: *key,
                                                },
                                        },
                                    )
                                    .await;
                            }
                            return Err(err);
                        }
                    };
                    tracing::debug!("Contract successfully updated - BroadcastTo - update");

                    // NOTE: We intentionally do NOT refresh hosting TTL on UPDATE.
                    // Only GET and SUBSCRIBE should extend hosting lifetime.
                    // If UPDATE refreshed TTL, a malicious contract author could spam
                    // updates to keep their contract hosted everywhere, draining resources.

                    // Emit telemetry: broadcast applied with resulting state
                    if let Some(event) = NetEventLog::update_broadcast_applied(
                        id,
                        &op_manager.ring,
                        *key,
                        &state_for_telemetry,
                        &updated_value,
                        changed,
                    ) {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }

                    if !changed {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "BroadcastTo update produced no change, ending propagation"
                        );
                    } else {
                        tracing::debug!(
                            "Successfully updated contract {} @ {:?} - BroadcastTo",
                            key,
                            self_location.location()
                        );
                    }
                    // Network peer propagation is now automatic via BroadcastStateChange event
                    // emitted by the executor when state changes. No manual try_to_broadcast needed.
                    new_state = None;
                    return_msg = None;
                }
                UpdateMsg::Broadcasting {
                    id,
                    broadcast_to,
                    key,
                    ..
                } => {
                    // DEPRECATED: Broadcasting messages are no longer generated.
                    // Network peer propagation is now automatic via BroadcastStateChange event
                    // emitted by the executor when state changes.
                    // This handler is kept for backwards compatibility during rolling upgrades.
                    tracing::debug!(
                        tx = %id,
                        %key,
                        target_count = broadcast_to.len(),
                        "Received deprecated Broadcasting message - ignoring (propagation is automatic)"
                    );
                    new_state = None;
                    return_msg = None;
                }

                // ---- Streaming handlers ----
                UpdateMsg::RequestUpdateStreaming {
                    id,
                    stream_id,
                    key,
                    total_size,
                } => {
                    use crate::operations::orphan_streams::STREAM_CLAIM_TIMEOUT;

                    // Check if streaming is enabled at runtime
                    if !op_manager.streaming_enabled {
                        tracing::warn!(
                            tx = %id,
                            contract = %key,
                            stream_id = %stream_id,
                            "UPDATE RequestUpdateStreaming received but streaming is disabled"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }

                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        stream_id = %stream_id,
                        total_size,
                        "Processing UPDATE RequestUpdateStreaming"
                    );

                    // Step 1: Claim the stream from orphan registry
                    let stream_handle = match op_manager
                        .orphan_stream_registry()
                        .claim_or_wait(*stream_id, STREAM_CLAIM_TIMEOUT)
                        .await
                    {
                        Ok(handle) => handle,
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to claim stream from orphan registry for UPDATE"
                            );
                            return Err(OpError::OrphanStreamClaimFailed);
                        }
                    };

                    // Step 2: Wait for stream to complete and assemble data
                    let stream_data = match stream_handle.assemble().await {
                        Ok(data) => {
                            tracing::debug!(
                                tx = %id,
                                stream_id = %stream_id,
                                assembled_size = data.len(),
                                expected_size = total_size,
                                "Stream assembled for UPDATE"
                            );
                            data
                        }
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to assemble stream for UPDATE"
                            );
                            return Err(OpError::StreamCancelled);
                        }
                    };

                    // Step 3: Deserialize the streaming payload
                    let payload: UpdateStreamingPayload = match bincode::deserialize(&stream_data) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                error = %e,
                                "Failed to deserialize UpdateStreamingPayload"
                            );
                            return Err(OpError::invalid_transition(self.id));
                        }
                    };

                    let UpdateStreamingPayload {
                        related_contracts,
                        value,
                    } = payload;

                    // Step 4: Apply the update (same logic as RequestUpdate)
                    let self_location = op_manager.ring.connection_manager.own_location();
                    let executing_addr = self_location.socket_addr();

                    tracing::debug!(
                        tx = %id,
                        %key,
                        executing_peer = ?executing_addr,
                        request_sender = ?source_addr,
                        "UPDATE RequestUpdateStreaming: applying update"
                    );

                    // Update contract locally
                    let UpdateExecution {
                        value: updated_value,
                        summary,
                        changed,
                        ..
                    } = update_contract(
                        op_manager,
                        *key,
                        UpdateData::State(State::from(value.clone())),
                        related_contracts.clone(),
                    )
                    .await?;

                    // Emit telemetry
                    let hash_after = Some(state_hash_full(&updated_value));
                    if let Some(sender_addr) = source_addr {
                        if let Some(requester_pkl) = op_manager
                            .ring
                            .connection_manager
                            .get_peer_by_addr(sender_addr)
                        {
                            if let Some(event) = NetEventLog::update_success(
                                id,
                                &op_manager.ring,
                                *key,
                                requester_pkl,
                                None, // No before hash for streaming
                                hash_after.clone(),
                            ) {
                                op_manager.ring.register_events(Either::Left(event)).await;
                            }
                        }
                    }

                    if !changed {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "UPDATE streaming yielded no state change"
                        );
                    } else {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "UPDATE streaming succeeded, state changed"
                        );
                    }

                    // Network propagation is automatic via BroadcastStateChange
                    if self.upstream_addr.is_none() {
                        new_state = Some(UpdateState::Finished {
                            key: *key,
                            summary: summary.clone(),
                        });
                    } else {
                        new_state = None;
                    }
                    return_msg = None;
                }

                UpdateMsg::BroadcastToStreaming {
                    id,
                    stream_id,
                    key,
                    total_size,
                } => {
                    use crate::operations::orphan_streams::STREAM_CLAIM_TIMEOUT;

                    // Check if streaming is enabled at runtime
                    if !op_manager.streaming_enabled {
                        tracing::warn!(
                            tx = %id,
                            contract = %key,
                            stream_id = %stream_id,
                            "UPDATE BroadcastToStreaming received but streaming is disabled"
                        );
                        return Err(OpError::UnexpectedOpState);
                    }

                    let sender_addr = match source_addr {
                        Some(addr) => addr,
                        None => {
                            tracing::error!(
                                tx = %id,
                                contract = %key,
                                stream_id = %stream_id,
                                "BroadcastToStreaming received without source_addr"
                            );
                            return Err(OpError::UnexpectedOpState);
                        }
                    };

                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        stream_id = %stream_id,
                        total_size,
                        sender = %sender_addr,
                        "Processing UPDATE BroadcastToStreaming"
                    );

                    // Step 1: Claim the stream from orphan registry
                    let stream_handle = match op_manager
                        .orphan_stream_registry()
                        .claim_or_wait(*stream_id, STREAM_CLAIM_TIMEOUT)
                        .await
                    {
                        Ok(handle) => handle,
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to claim stream from orphan registry for broadcast"
                            );
                            return Err(OpError::OrphanStreamClaimFailed);
                        }
                    };

                    // Step 2: Wait for stream to complete and assemble data
                    let stream_data = match stream_handle.assemble().await {
                        Ok(data) => {
                            tracing::debug!(
                                tx = %id,
                                stream_id = %stream_id,
                                assembled_size = data.len(),
                                expected_size = total_size,
                                "Stream assembled for broadcast"
                            );
                            data
                        }
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to assemble stream for broadcast"
                            );
                            return Err(OpError::StreamCancelled);
                        }
                    };

                    // Step 3: Deserialize the broadcast streaming payload
                    let payload: BroadcastStreamingPayload =
                        match bincode::deserialize(&stream_data) {
                            Ok(p) => p,
                            Err(e) => {
                                tracing::error!(
                                    tx = %id,
                                    error = %e,
                                    "Failed to deserialize BroadcastStreamingPayload"
                                );
                                return Err(OpError::invalid_transition(self.id));
                            }
                        };

                    let BroadcastStreamingPayload {
                        state_bytes,
                        sender_summary_bytes,
                    } = payload;

                    // Step 4: Apply the update (same logic as BroadcastTo with FullState)
                    let sender_summary = StateSummary::from(sender_summary_bytes.clone());

                    // Update sender's cached summary in our interest manager
                    if let Some(sender_pkl) = op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(sender_addr)
                    {
                        let sender_key = crate::ring::PeerKey::from(sender_pkl.pub_key().clone());
                        op_manager.interest_manager.update_peer_summary(
                            key,
                            &sender_key,
                            Some(sender_summary.clone()),
                        );
                    }

                    let update_data = UpdateData::State(State::from(state_bytes.clone()));

                    // For telemetry
                    let state_for_telemetry = WrappedState::from(state_bytes.clone());

                    // Emit telemetry: broadcast received from upstream peer
                    if let Some(requester_pkl) = op_manager
                        .ring
                        .connection_manager
                        .get_peer_by_addr(sender_addr)
                    {
                        if let Some(event) = NetEventLog::update_broadcast_received(
                            id,
                            &op_manager.ring,
                            *key,
                            requester_pkl,
                            state_for_telemetry.clone(),
                        ) {
                            op_manager.ring.register_events(Either::Left(event)).await;
                        }
                    }

                    tracing::debug!("Attempting contract value update - BroadcastToStreaming");
                    let UpdateExecution {
                        value: updated_value,
                        summary: _summary,
                        changed,
                        ..
                    } = update_contract(op_manager, *key, update_data, RelatedContracts::default())
                        .await?;

                    tracing::debug!("Contract successfully updated - BroadcastToStreaming");

                    // NOTE: We intentionally do NOT refresh hosting TTL on UPDATE.
                    // Only GET and SUBSCRIBE should extend hosting lifetime.

                    // Emit telemetry: broadcast applied
                    if let Some(event) = NetEventLog::update_broadcast_applied(
                        id,
                        &op_manager.ring,
                        *key,
                        &state_for_telemetry,
                        &updated_value,
                        changed,
                    ) {
                        op_manager.ring.register_events(Either::Left(event)).await;
                    }

                    if !changed {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "BroadcastToStreaming update produced no change"
                        );
                    } else {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "Successfully updated contract via BroadcastToStreaming"
                        );
                    }

                    // Network peer propagation is automatic via BroadcastStateChange
                    new_state = None;
                    return_msg = None;
                }
            }

            build_op_result(
                self.id,
                new_state,
                return_msg,
                stats,
                self.upstream_addr,
                forward_hop,
                stream_data,
            )
        })
    }
}

impl OpManager {
    /// Get the list of network subscribers to broadcast an UPDATE to.
    ///
    /// **Architecture Note (Issue #2075):**
    /// This function returns only **network peer** subscribers, not local client subscriptions.
    /// Local clients receive updates through a separate path via the contract executor's
    /// `update_notifications` channels (see `send_update_notification` in runtime.rs).
    ///
    /// **Parameter `sender`:**
    /// The address of the peer that initiated or forwarded this UPDATE to us.
    /// - Used to filter out the sender from broadcast targets (avoid echo)
    /// - When sender equals our own address (local UPDATE initiation), we include ourselves
    ///   in proximity cache targets if we're seeding the contract
    ///
    /// # Hybrid Architecture (2026-01 Refactor)
    ///
    /// Updates are propagated via TWO sources:
    /// 1. Proximity cache: peers who have announced they seed this contract (fast, local knowledge)
    /// 2. Interest manager: peers who have expressed interest via the Interest/Summary protocol
    ///
    /// This hybrid approach ensures updates reach all interested peers even if CacheAnnounce
    /// messages haven't fully propagated yet.
    pub(crate) fn get_broadcast_targets_update(
        &self,
        key: &ContractKey,
        sender: &SocketAddr,
    ) -> Vec<PeerKeyLocation> {
        use std::collections::HashSet;

        let self_addr = self.ring.connection_manager.get_own_addr();
        let is_local_update_initiator = self_addr.as_ref().map(|me| me == sender).unwrap_or(false);

        let mut targets: HashSet<PeerKeyLocation> = HashSet::new();

        // Source 1: Proximity cache (peers who announced they seed this contract)
        let proximity_addrs = self.proximity_cache.neighbors_with_contract(key);
        let proximity_count = proximity_addrs.len();

        for addr in proximity_addrs {
            // Skip sender to avoid echo (unless we're the originator)
            if &addr == sender && !is_local_update_initiator {
                continue;
            }
            // Skip ourselves if not local originator
            if !is_local_update_initiator && self_addr.as_ref() == Some(&addr) {
                continue;
            }
            // Only include connected peers
            if let Some(pkl) = self.ring.connection_manager.get_peer_by_addr(addr) {
                targets.insert(pkl);
            }
        }

        // Source 2: Interest manager (peers who expressed interest via protocol)
        let interested_peers = self.interest_manager.get_interested_peers(key);
        let interest_count = interested_peers.len();

        for (peer_key, _interest) in interested_peers {
            // Look up peer by public key
            if let Some(pkl) = self
                .ring
                .connection_manager
                .get_peer_by_pub_key(&peer_key.0)
            {
                // Skip sender to avoid echo
                if let Some(pkl_addr) = pkl.socket_addr() {
                    if &pkl_addr == sender && !is_local_update_initiator {
                        continue;
                    }
                    // Skip ourselves
                    if !is_local_update_initiator && self_addr.as_ref() == Some(&pkl_addr) {
                        continue;
                    }
                }
                targets.insert(pkl);
            }
        }

        // Sort targets for deterministic iteration order
        let mut result: Vec<PeerKeyLocation> = targets.into_iter().collect();
        result.sort();

        // Trace update propagation for debugging
        if !result.is_empty() {
            tracing::info!(
                contract = %format!("{:.8}", key),
                peer_addr = %sender,
                targets = %result
                    .iter()
                    .filter_map(|s| s.socket_addr())
                    .map(|addr| format!("{:.8}", addr))
                    .collect::<Vec<_>>()
                    .join(","),
                count = result.len(),
                proximity_sources = proximity_count,
                interest_sources = interest_count,
                phase = "broadcast",
                "UPDATE_PROPAGATION"
            );
        } else {
            tracing::warn!(
                contract = %format!("{:.8}", key),
                peer_addr = %sender,
                self_addr = ?self_addr.map(|a| format!("{:.8}", a)),
                proximity_sources = proximity_count,
                interest_sources = interest_count,
                phase = "warning",
                "UPDATE_PROPAGATION: NO_TARGETS - update will not propagate further"
            );
        }

        result
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<UpdateState>,
    return_msg: Option<UpdateMsg>,
    stats: Option<UpdateStats>,
    upstream_addr: Option<std::net::SocketAddr>,
    forward_hop: Option<std::net::SocketAddr>,
    stream_data: Option<(StreamId, bytes::Bytes)>,
) -> Result<super::OperationResult, OpError> {
    // With hop-by-hop routing:
    // - forward_hop is set when forwarding RequestUpdate to the next peer
    // - Broadcasting messages have next_hop = None (they're processed locally)
    // - BroadcastTo uses explicit addresses via conn_manager.send()
    let next_hop = forward_hop;

    let output_op = state.map(|op| UpdateOp {
        id,
        state: Some(op),
        stats,
        upstream_addr,
    });
    let state = output_op.map(OpEnum::Update);
    Ok(OperationResult {
        return_msg: return_msg.map(NetMessage::from),
        next_hop,
        state,
        stream_data,
    })
}

/// Apply an update to a contract.
///
/// This function:
/// 1. Fetches the current state (for change detection)
/// 2. Calls UpdateQuery to merge the update and persist
/// 3. Returns the merged state, summary, and whether the state changed
///
/// The `update_data` parameter can be:
/// - `UpdateData::Delta(delta)` - A delta from the client, merged with current state
/// - `UpdateData::State(state)` - A full state from PUT or executor
async fn update_contract(
    op_manager: &OpManager,
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
) -> Result<UpdateExecution, OpError> {
    let previous_state = match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            instance_id: *key.id(),
            return_contract_code: false,
        })
        .await
    {
        Ok(ContractHandlerEvent::GetResponse {
            response: Ok(StoreResponse { state, .. }),
            ..
        }) => state,
        Ok(other) => {
            tracing::trace!(?other, %key, "Unexpected get response while preparing update summary");
            None
        }
        Err(err) => {
            tracing::debug!(%key, %err, "Failed to fetch existing contract state before update");
            None
        }
    };

    match op_manager
        .notify_contract_handler(ContractHandlerEvent::UpdateQuery {
            key,
            data: update_data.clone(),
            related_contracts,
        })
        .await
    {
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Ok(new_val),
            state_changed,
        }) => {
            let new_bytes = State::from(new_val.clone()).into_bytes();
            let summary = StateSummary::from(new_bytes);

            Ok(UpdateExecution {
                value: new_val,
                summary,
                changed: state_changed,
            })
        }
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Err(err),
            ..
        }) => {
            tracing::error!(contract = %key, error = %err, phase = "error", "Failed to update contract value");
            Err(err.into())
        }
        Ok(ContractHandlerEvent::UpdateNoChange { .. }) => {
            // Helper to extract state from UpdateData variants that contain state
            fn extract_state_from_update_data(
                update_data: &UpdateData<'static>,
            ) -> Option<WrappedState> {
                match update_data {
                    UpdateData::State(s) => Some(WrappedState::from(s.clone().into_bytes())),
                    UpdateData::StateAndDelta { state, .. }
                    | UpdateData::RelatedState { state, .. }
                    | UpdateData::RelatedStateAndDelta { state, .. } => {
                        Some(WrappedState::from(state.clone().into_bytes()))
                    }
                    UpdateData::Delta(_) | UpdateData::RelatedDelta { .. } => None,
                }
            }

            let resolved_state = match previous_state {
                Some(prev_state) => prev_state,
                None => {
                    // Try to fetch current state from store
                    let fetched_state = op_manager
                        .notify_contract_handler(ContractHandlerEvent::GetQuery {
                            instance_id: *key.id(),
                            return_contract_code: false,
                        })
                        .await
                        .ok()
                        .and_then(|event| match event {
                            ContractHandlerEvent::GetResponse {
                                response: Ok(StoreResponse { state, .. }),
                                ..
                            } => state,
                            _ => None,
                        });

                    match fetched_state {
                        Some(state) => state,
                        None => {
                            tracing::debug!(
                                %key,
                                "Fallback fetch for UpdateNoChange returned no state; trying to extract from update_data"
                            );
                            match extract_state_from_update_data(&update_data) {
                                Some(state) => state,
                                None => {
                                    tracing::error!(
                                        %key,
                                        "Cannot extract state from delta-only UpdateData in NoChange fallback"
                                    );
                                    return Err(OpError::UnexpectedOpState);
                                }
                            }
                        }
                    }
                }
            };

            let bytes = State::from(resolved_state.clone()).into_bytes();
            let summary = StateSummary::from(bytes);
            Ok(UpdateExecution {
                value: resolved_state,
                summary,
                changed: false,
            })
        }
        Err(err) => Err(err.into()),
        Ok(other) => {
            tracing::error!(event = ?other, contract = %key, phase = "error", "Unexpected event from contract handler during update");
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// This will be called from the node when processing an open request
// todo: new_state should be a delta when possible!
pub(crate) fn start_op(
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
) -> UpdateOp {
    let contract_location = Location::from(&key);
    tracing::debug!(%contract_location, %key, "Requesting update");
    let id = Transaction::new::<UpdateMsg>();

    let state = Some(UpdateState::PrepareRequest {
        key,
        related_contracts,
        update_data,
    });

    UpdateOp {
        id,
        state,
        stats: Some(UpdateStats { target: None }),
        upstream_addr: None, // Local operation, no upstream peer
    }
}

/// This will be called from the node when processing an open request with a specific transaction ID
pub(crate) fn start_op_with_id(
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
    id: Transaction,
) -> UpdateOp {
    let contract_location = Location::from(&key);
    tracing::debug!(%contract_location, %key, "Requesting update with transaction ID {}", id);

    let state = Some(UpdateState::PrepareRequest {
        key,
        related_contracts,
        update_data,
    });

    UpdateOp {
        id,
        state,
        stats: Some(UpdateStats { target: None }),
        upstream_addr: None, // Local operation, no upstream peer
    }
}

/// Entry point from node to operations logic
pub(crate) async fn request_update(
    op_manager: &OpManager,
    mut update_op: UpdateOp,
) -> Result<(), OpError> {
    // Extract the key and check if we need to handle this locally
    let (key, update_data, related_contracts) = if let Some(UpdateState::PrepareRequest {
        key,
        update_data,
        related_contracts,
    }) = update_op.state.take()
    {
        (key, update_data, related_contracts)
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    // Find the best peer to send this update to.
    // In the simplified architecture (2026-01 refactor), we use:
    // 1. Proximity cache - peers who have announced they seed this contract
    // 2. Ring-based routing - find closest potentially-caching peer
    let sender_addr = op_manager.ring.connection_manager.peer_addr()?;

    // Check proximity cache for neighbors that have announced caching this contract.
    // This is critical for peer-to-peer updates when peers are directly connected
    // but not explicitly subscribed (e.g., River chat rooms where both peers cache
    // the contract but haven't established a subscription tree).
    //
    // Note: The proximity cache is populated asynchronously via CacheAnnounce messages,
    // so there may be a brief race window after a peer caches a contract before its
    // neighbors receive the announcement. This is acceptable - the ring-based fallback
    // handles this case, and the proximity cache improves the common case where
    // announcements have propagated.
    let proximity_neighbors: Vec<_> = op_manager
        .proximity_cache
        .neighbors_with_contract(&key)
        .into_iter()
        .filter(|addr| addr != &sender_addr)
        .collect();

    let mut target_from_proximity = None;
    for addr in &proximity_neighbors {
        match op_manager.ring.connection_manager.get_peer_by_addr(*addr) {
            Some(peer) => {
                target_from_proximity = Some(peer);
                break;
            }
            None => {
                // Neighbor is in proximity cache but no longer connected.
                // This is normal during connection churn - the proximity cache
                // will be cleaned up when the disconnect is processed.
                tracing::debug!(
                    %key,
                    peer = %addr,
                    "UPDATE: Proximity cache neighbor not connected, trying next"
                );
            }
        }
    }

    let target = if let Some(proximity_neighbor) = target_from_proximity {
        // Use peer from proximity cache that announced having this contract.
        tracing::debug!(
            %key,
            target = ?proximity_neighbor.socket_addr(),
            proximity_neighbors_found = proximity_neighbors.len(),
            "UPDATE: Using proximity cache neighbor as target"
        );
        proximity_neighbor
    } else {
        // Find the best peer to send the update to based on ring location
        let remote_target = op_manager
            .ring
            .closest_potentially_caching(&key, [sender_addr].as_slice());

        if let Some(target) = remote_target {
            // Found a remote peer to send the update to
            target
        } else {
            // No remote peers available, handle locally
            tracing::debug!(
                "UPDATE: No remote peers available for contract {}, handling locally",
                key
            );

            let id = update_op.id;

            // Check if we're seeding this contract
            let is_seeding = op_manager.ring.is_seeding_contract(&key);
            let should_handle_update = is_seeding;

            if !should_handle_update {
                tracing::error!(
                    contract = %key,
                    phase = "error",
                    "UPDATE: Cannot update contract on isolated node - contract not seeded"
                );
                return Err(OpError::RingError(RingError::NoCachingPeers(*key.id())));
            }

            // Update the contract locally. This path is reached when:
            // 1. No remote peers are available (isolated node OR no suitable caching peers)
            // 2. We are seeding the contract (verified above)
            let UpdateExecution {
                value: _updated_value,
                summary,
                changed,
                ..
            } = update_contract(op_manager, key, update_data, related_contracts).await?;

            tracing::debug!(
                tx = %id,
                %key,
                "Successfully updated contract locally on isolated node"
            );

            if !changed {
                tracing::debug!(
                    tx = %id,
                    %key,
                    "Local update resulted in no change; finishing without broadcast"
                );
                deliver_update_result(op_manager, id, key, summary.clone()).await?;
                return Ok(());
            }

            deliver_update_result(op_manager, id, key, summary.clone()).await?;

            // Network peer propagation is automatic via BroadcastStateChange event
            // emitted by the executor when update_contract updated the state.
            // No manual try_to_broadcast needed - operation complete.
            tracing::debug!(
                tx = %id,
                %key,
                "UPDATE operation complete on isolated node"
            );
            return Ok(());
        }
    };

    // Normal case: we found a remote target
    // Apply the update locally first to ensure the initiating peer has the updated state
    let id = update_op.id;

    let target_addr = target
        .socket_addr()
        .expect("target must have socket address");
    tracing::debug!(
        tx = %id,
        %key,
        target_peer = %target_addr,
        "Applying UPDATE locally before forwarding to target peer"
    );

    // Apply update locally - this ensures the initiating peer serves the updated state
    // even if the remote UPDATE times out or fails
    let UpdateExecution {
        value: updated_value,
        summary,
        changed: _changed,
        ..
    } = update_contract(
        op_manager,
        key,
        update_data.clone(),
        related_contracts.clone(),
    )
    .await
    .map_err(|e| {
        tracing::error!(
            tx = %id,
            contract = %key,
            error = %e,
            phase = "error",
            "Failed to apply update locally before forwarding UPDATE"
        );
        e
    })?;

    tracing::debug!(
        tx = %id,
        %key,
        "Local update complete, now forwarding UPDATE to target peer"
    );

    if let Some(stats) = &mut update_op.stats {
        stats.target = Some(target.clone());
    }

    // Emit telemetry: UPDATE request initiated
    if let Some(event) = NetEventLog::update_request(&id, &op_manager.ring, key, target.clone()) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    let msg = UpdateMsg::RequestUpdate {
        id,
        key,
        related_contracts,
        value: updated_value, // Send the updated value, not the original
    };

    // Create operation state with target for hop-by-hop routing.
    // This allows peek_next_hop_addr to find the next hop address when
    // handle_notification_msg routes the outbound message.
    let op_state = UpdateOp {
        id,
        state: Some(UpdateState::ReceivedRequest),
        stats: Some(UpdateStats {
            target: Some(target),
        }),
        upstream_addr: None, // We're the originator
    };

    // Use notify_op_change to:
    // 1. Register the operation state (so peek_next_hop_addr can find the next hop)
    // 2. Send the message via the event loop (which routes via network bridge)
    op_manager
        .notify_op_change(NetMessage::from(msg), OpEnum::Update(op_state))
        .await?;

    // Deliver the UPDATE result to the client (fire-and-forget semantics).
    // NOTE: We do NOT call op_manager.completed() here because the operation
    // needs to remain in the state map until peek_next_hop_addr can route it.
    // The operation will be marked complete later when the message is processed.
    let op = UpdateOp {
        id,
        state: Some(UpdateState::Finished {
            key,
            summary: summary.clone(),
        }),
        stats: None,
        upstream_addr: None,
    };
    let host_result = op.to_host_result();
    op_manager
        .result_router_tx
        .send((id, host_result))
        .await
        .map_err(|error| {
            tracing::error!(tx = %id, error = %error, phase = "error", "Failed to send UPDATE result to result router");
            OpError::NotificationError
        })?;

    Ok(())
}

async fn deliver_update_result(
    op_manager: &OpManager,
    id: Transaction,
    key: ContractKey,
    summary: StateSummary<'static>,
) -> Result<(), OpError> {
    // NOTE: UPDATE is modeled as fire-and-forget: once the merge succeeds on the
    // seed/subscriber peer we surface success to the host immediately and allow
    // the broadcast fan-out to proceed asynchronously.
    let op = UpdateOp {
        id,
        state: Some(UpdateState::Finished {
            key,
            summary: summary.clone(),
        }),
        stats: None,
        upstream_addr: None, // Terminal state, no routing needed
    };

    let host_result = op.to_host_result();

    op_manager
        .result_router_tx
        .send((id, host_result))
        .await
        .map_err(|error| {
            tracing::error!(
                tx = %id,
                error = %error,
                phase = "error",
                "Failed to send UPDATE result to result router"
            );
            OpError::NotificationError
        })?;

    if let Err(error) = op_manager
        .to_event_listener
        .notifications_sender()
        .send(Either::Right(NodeEvent::TransactionCompleted(id)))
        .await
    {
        tracing::warn!(
            tx = %id,
            error = %error,
            phase = "error",
            "Failed to notify transaction completion for UPDATE"
        );
    }

    op_manager.completed(id);

    Ok(())
}

impl IsOperationCompleted for UpdateOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(UpdateState::Finished { .. }))
    }
}

mod messages {
    use std::fmt::Display;

    use freenet_stdlib::prelude::{ContractKey, RelatedContracts, WrappedState};
    use serde::{Deserialize, Serialize};

    use crate::{
        message::{InnerMessage, Transaction},
        ring::{Location, PeerKeyLocation},
        transport::peer_connection::StreamId,
    };

    /// Payload for streaming UPDATE requests.
    ///
    /// Contains the same data as RequestUpdate but serialized for streaming.
    /// The metadata (key, stream_id, total_size) is sent via RequestUpdateStreaming message.
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct UpdateStreamingPayload {
        #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
        pub related_contracts: RelatedContracts<'static>,
        pub value: WrappedState,
    }

    /// Payload for streaming broadcast updates.
    ///
    /// Contains full state for broadcasting to subscribers via streaming.
    /// Used when the full state is large (>streaming_threshold).
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct BroadcastStreamingPayload {
        /// Full contract state bytes
        pub state_bytes: Vec<u8>,
        /// Sender's current state summary bytes
        pub sender_summary_bytes: Vec<u8>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    /// Update operation messages.
    ///
    /// Uses hop-by-hop routing for request forwarding. Broadcasting to subscribers
    /// uses explicit addresses since there are multiple targets.
    pub(crate) enum UpdateMsg {
        /// Request to update a contract state. Forwarded hop-by-hop toward contract location.
        RequestUpdate {
            id: Transaction,
            key: ContractKey,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
        },
        /// Internal node instruction to track broadcasting progress.
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_value: WrappedState,
        },
        /// Broadcasting a change to a specific subscriber.
        ///
        /// Supports delta-based synchronization: when we know the peer's state summary,
        /// we send a delta instead of full state to reduce bandwidth.
        BroadcastTo {
            id: Transaction,
            key: ContractKey,
            /// The payload: either a delta (if we know peer's summary) or full state.
            payload: crate::message::DeltaOrFullState,
            /// Sender's current state summary bytes, so receiver can update their tracking.
            /// Use `StateSummary::from(sender_summary_bytes.clone())` to convert.
            sender_summary_bytes: Vec<u8>,
        },

        // ---- Streaming variants ----
        /// Streaming variant of RequestUpdate for large state updates.
        ///
        /// Used when the state size exceeds the streaming threshold (default 64KB).
        /// The actual state data is sent via a separate stream identified by stream_id.
        RequestUpdateStreaming {
            id: Transaction,
            /// Identifies the stream carrying the update payload
            stream_id: StreamId,
            /// Contract key being updated
            key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
        },

        /// Streaming variant of BroadcastTo for large full state broadcasts.
        ///
        /// Used when broadcasting full state (not delta) and the state size exceeds
        /// the streaming threshold. Deltas are typically small and use regular BroadcastTo.
        BroadcastToStreaming {
            id: Transaction,
            /// Identifies the stream carrying the broadcast payload
            stream_id: StreamId,
            /// Contract key being broadcast
            key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
        },
    }

    impl InnerMessage for UpdateMsg {
        fn id(&self) -> &Transaction {
            match self {
                UpdateMsg::RequestUpdate { id, .. }
                | UpdateMsg::Broadcasting { id, .. }
                | UpdateMsg::BroadcastTo { id, .. }
                | UpdateMsg::RequestUpdateStreaming { id, .. }
                | UpdateMsg::BroadcastToStreaming { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<crate::ring::Location> {
            match self {
                UpdateMsg::RequestUpdate { key, .. }
                | UpdateMsg::Broadcasting { key, .. }
                | UpdateMsg::BroadcastTo { key, .. }
                | UpdateMsg::RequestUpdateStreaming { key, .. }
                | UpdateMsg::BroadcastToStreaming { key, .. } => Some(Location::from(key.id())),
            }
        }
    }

    impl Display for UpdateMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                UpdateMsg::RequestUpdate { id, .. } => write!(f, "RequestUpdate(id: {id})"),
                UpdateMsg::Broadcasting { id, .. } => write!(f, "Broadcasting(id: {id})"),
                UpdateMsg::BroadcastTo { id, .. } => write!(f, "BroadcastTo(id: {id})"),
                UpdateMsg::RequestUpdateStreaming { id, stream_id, .. } => {
                    write!(f, "RequestUpdateStreaming(id: {id}, stream: {stream_id})")
                }
                UpdateMsg::BroadcastToStreaming { id, stream_id, .. } => {
                    write!(f, "BroadcastToStreaming(id: {id}, stream: {stream_id})")
                }
            }
        }
    }
}

/// State machine for UPDATE operations.
///
/// # Important: Updates are Fire-and-Forget
///
/// Updates spread through the subscription tree like a virus - there is no acknowledgment
/// or completion signal that propagates back. When a node receives an UPDATE:
/// 1. It applies the update locally
/// 2. It broadcasts to its downstream subscribers (if any)
/// 3. It does NOT wait for or expect any response from downstream
///
/// The `Finished` state only indicates that the LOCAL update was applied successfully.
/// It does NOT mean all subscribers have received the update - that's unknowable.
/// This is by design: waiting for tree-wide propagation would be impractical and
/// would create deadlocks in cyclic subscription topologies.
#[derive(Debug)]
pub enum UpdateState {
    /// Initial state when receiving an update request from another peer.
    ReceivedRequest,

    /// The update was applied locally. Used to signal completion to the client
    /// for client-initiated updates. Does NOT indicate network-wide propagation.
    Finished {
        key: ContractKey,
        summary: StateSummary<'static>,
    },

    /// Preparing to send an update request (client-initiated updates).
    PrepareRequest {
        key: ContractKey,
        related_contracts: RelatedContracts<'static>,
        /// The update data - can be a delta (from client) or full state (from PUT/executor).
        /// This is passed to update_contract which calls UpdateQuery to merge and persist.
        update_data: UpdateData<'static>,
    },
}
