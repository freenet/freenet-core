pub(crate) mod op_ctx_task;

use either::Either;
use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

pub(crate) use self::messages::{BroadcastStreamingPayload, UpdateMsg, UpdateStreamingPayload};
use super::{
    OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult, should_use_streaming,
};
use crate::contract::{ContractHandlerEvent, ExecutorError, StoreResponse};
use crate::message::{InnerMessage, NetMessage, NodeEvent, Transaction};
use crate::node::IsOperationCompleted;
use crate::ring::{Location, PeerKeyLocation, RingError};
use crate::transport::peer_connection::StreamId;
use crate::{
    client_events::HostResult,
    node::{NetworkBridge, OpManager},
    tracing::{NetEventLog, OperationFailure, state_hash_full},
};
use std::collections::VecDeque;
use std::net::SocketAddr;

use dashmap::DashMap;
use tokio::time::Instant;

/// Cache for deduplicating broadcast payloads.
///
/// When the same delta/state is broadcast to us by multiple peers (which is
/// expected in the gossip topology), we skip the expensive WASM merge call
/// for duplicates. Uses ahash for fast hashing of payload bytes.
pub(crate) struct BroadcastDedupCache {
    /// Per-contract dedup entries, newest at back.
    entries: DashMap<ContractKey, VecDeque<DedupEntry>>,
}

struct DedupEntry {
    delta_hash: u64,
    inserted_at: Instant,
}

/// Maximum entries per contract in the dedup cache.
const DEDUP_MAX_ENTRIES_PER_CONTRACT: usize = 64;

/// TTL for dedup entries — entries older than this are evicted.
const DEDUP_TTL: std::time::Duration = std::time::Duration::from_secs(60);

impl BroadcastDedupCache {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Check if this payload was already seen for this contract.
    /// If not, insert it and return `false` (not a duplicate).
    /// If yes, return `true` (duplicate — skip processing).
    ///
    /// `is_delta` distinguishes delta payloads from full state payloads so they
    /// don't collide in the hash space (a delta and full state could have the
    /// same bytes but represent different semantic operations).
    ///
    /// Note: Uses non-cryptographic ahash for speed. A hash collision would
    /// cause a legitimate update to be silently skipped, but the gossip
    /// protocol will deliver the data via another peer eventually.
    pub fn check_and_insert(
        &self,
        key: &ContractKey,
        payload_bytes: &[u8],
        is_delta: bool,
        now: Instant,
    ) -> bool {
        use ahash::AHasher;
        use std::hash::Hasher;

        let mut hasher = AHasher::default();
        // Include payload type discriminant to avoid delta/full-state collisions
        hasher.write_u8(if is_delta { 1 } else { 0 });
        hasher.write(payload_bytes);
        let delta_hash = hasher.finish();

        let mut entry = self.entries.entry(*key).or_default();
        let queue = entry.value_mut();

        // Evict expired entries from the front
        while let Some(front) = queue.front() {
            if now.duration_since(front.inserted_at) > DEDUP_TTL {
                queue.pop_front();
            } else {
                break;
            }
        }

        // Check if hash already exists
        if queue.iter().any(|e| e.delta_hash == delta_hash) {
            return true; // Duplicate
        }

        // Evict oldest if at capacity
        while queue.len() >= DEDUP_MAX_ENTRIES_PER_CONTRACT {
            queue.pop_front();
        }

        queue.push_back(DedupEntry {
            delta_hash,
            inserted_at: now,
        });

        false // Not a duplicate
    }
}

/// Result of `get_broadcast_targets_update()` with skip-reason counters
/// for broadcast delivery diagnostics (issue #3046).
pub(crate) struct BroadcastTargetResult {
    pub targets: Vec<PeerKeyLocation>,
    pub proximity_found: usize,
    pub proximity_resolve_failed: usize,
    pub interest_found: usize,
    pub interest_resolve_failed: usize,
    pub skipped_self: usize,
    pub skipped_sender: usize,
}

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
        if self.finalized() {
            if let Some(UpdateStats {
                target: Some(ref target),
                contract_location: Some(loc),
            }) = self.stats
            {
                return OpOutcome::ContractOpSuccessUntimed {
                    target_peer: target,
                    contract_location: loc,
                };
            }
            return OpOutcome::Irrelevant;
        }
        // Not completed — if we have stats with target+location, report as failure
        if let Some(UpdateStats {
            target: Some(ref target),
            contract_location: Some(loc),
        }) = self.stats
        {
            OpOutcome::ContractOpFailure {
                target_peer: target,
                contract_location: loc,
            }
        } else {
            OpOutcome::Incomplete
        }
    }

    /// Returns true if this UPDATE was initiated by a local client (not forwarded from a peer).
    pub(crate) fn is_client_initiated(&self) -> bool {
        self.upstream_addr.is_none()
    }

    /// Extract routing failure info for timeout reporting.
    pub(crate) fn failure_routing_info(&self) -> Option<(PeerKeyLocation, Location)> {
        match &self.stats {
            Some(UpdateStats {
                target: Some(target),
                contract_location: Some(loc),
            }) => Some((target.clone(), *loc)),
            _ => None,
        }
    }

    pub fn finalized(&self) -> bool {
        matches!(self.state, None | Some(UpdateState::Finished(_)))
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
        if let Some(UpdateState::Finished(data)) = &self.state {
            let (key, summary) = (&data.key, &data.summary);
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

struct UpdateStats {
    target: Option<PeerKeyLocation>,
    contract_location: Option<Location>,
}

pub(crate) struct UpdateExecution {
    pub(crate) value: WrappedState,
    pub(crate) summary: StateSummary<'static>,
    pub(crate) changed: bool,
}

pub(crate) struct UpdateResult {}

impl TryFrom<UpdateOp> for UpdateResult {
    type Error = OpError;

    fn try_from(op: UpdateOp) -> Result<Self, Self::Error> {
        if matches!(op.state, None | Some(UpdateState::Finished(_))) {
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
                if let Err(e) = op_manager.push(tx, op).await {
                    tracing::warn!(tx = %tx, error = %e, "failed to push mismatched op back");
                }
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
            let mut stats = self.stats;
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
                                    Some(updated_value.len()),
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
                                new_state = Some(UpdateState::Finished(FinishedData {
                                    key: *key,
                                    summary: summary.clone(),
                                }));
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
                                .closest_potentially_hosting(key, skip_list.as_slice());

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

                                // Emit telemetry: relay forwarding update request
                                if let Some(event) = NetEventLog::update_request(
                                    id,
                                    &op_manager.ring,
                                    *key,
                                    forward_target.clone(),
                                ) {
                                    op_manager.ring.register_events(Either::Left(event)).await;
                                }

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
                                    op_manager.streaming_threshold,
                                    payload_size,
                                ) {
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
                                    return_msg = Some(UpdateMsg::RequestUpdate {
                                        id: *id,
                                        key: *key,
                                        related_contracts: related_contracts.clone(),
                                        value: value.clone(),
                                    });
                                }
                                // Keep op alive in ReceivedRequest state so the GC
                                // task can detect timeouts and report failures.
                                // Without a persisted state, SendAndComplete
                                // would discard the op (and its stats) immediately.
                                new_state = Some(UpdateState::ReceivedRequest);
                                // Track where to forward this message
                                forward_hop = Some(forward_addr);

                                // Track the forward target so timeouts report to
                                // PeerHealthTracker and the failure estimator (#3527).
                                stats = Some(UpdateStats {
                                    target: Some(forward_target),
                                    contract_location: Some(Location::from(key)),
                                });
                            } else {
                                // No peers available and we don't have the contract - log error
                                let candidates = op_manager
                                    .ring
                                    .k_closest_potentially_hosting(key, skip_list.as_slice(), 5)
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
                                // Emit telemetry: update failure at relay
                                if let Some(event) = NetEventLog::update_failure(
                                    id,
                                    &op_manager.ring,
                                    *key,
                                    OperationFailure::NoPeersAvailable,
                                ) {
                                    op_manager.ring.register_events(Either::Left(event)).await;
                                }
                                return Err(OpError::RingError(RingError::NoHostingPeers(
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

                    // Dedup check: skip expensive WASM merge if we've already processed
                    // this exact payload recently (common when multiple peers broadcast
                    // the same delta to us).
                    let is_delta_payload =
                        matches!(payload, crate::message::DeltaOrFullState::Delta(_));
                    if op_manager.broadcast_dedup_cache.check_and_insert(
                        key,
                        payload_bytes,
                        is_delta_payload,
                        op_manager.interest_manager.now(),
                    ) {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "BroadcastTo skipped — duplicate payload (dedup cache hit)"
                        );
                        new_state = None;
                        return_msg = None;
                        return build_op_result(
                            self.id,
                            new_state,
                            return_msg,
                            stats,
                            self.upstream_addr,
                            forward_hop,
                            stream_data,
                        );
                    }

                    tracing::debug!("Attempting contract value update - BroadcastTo - update");
                    let is_delta = matches!(payload, crate::message::DeltaOrFullState::Delta(_));
                    let update_result =
                        update_contract(op_manager, *key, update_data, RelatedContracts::default())
                            .await;

                    let UpdateExecution {
                        value: updated_value,
                        summary: update_summary,
                        changed,
                        ..
                    } = match update_result {
                        Ok(result) => result,
                        Err(err) => {
                            // On benign stale-version rejection, nudge the sender's
                            // peer-summary cache of us toward Some(our_summary) so
                            // their next broadcast skips us. Helper gates internally
                            // on summary equality + per-contract throttle to avoid
                            // SyncStateToPeer loops and WASM amplification. See
                            // `send_summary_back_on_rejection` docs for the full
                            // rationale. Gated on `is_invalid_update_rejection`
                            // (not the broader `is_contract_exec_rejection`) so
                            // OOG/traps/validation failures don't fire summary-back.
                            if err.is_invalid_update_rejection() {
                                let op_mgr = op_manager.clone();
                                let contract_key = *key;
                                let sender_summary = sender_summary_bytes.clone();
                                tokio::spawn(async move {
                                    send_summary_back_on_rejection(
                                        &op_mgr,
                                        &contract_key,
                                        sender_addr,
                                        sender_summary,
                                    )
                                    .await;
                                });
                            }

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
                                if let Err(e) = op_manager
                                    .notify_node_event(
                                        crate::message::NodeEvent::SendInterestMessage {
                                            target: sender_addr,
                                            message:
                                                crate::message::InterestMessage::ResyncRequest {
                                                    key: *key,
                                                },
                                        },
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to send ResyncRequest");
                                }
                            } else if !err.is_contract_exec_rejection() {
                                // Full state update failed (not delta) — likely missing
                                // contract parameters. Trigger a self-healing GET.
                                // Skip if the merge function ran and rejected the update
                                // (e.g., stale version) — the contract code is present.
                                op_manager.try_auto_fetch_contract(key, sender_addr);
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

                        // Proactive summary notification: tell interested peers our state
                        // changed so they can update their cached summary of us. This
                        // reduces redundant broadcasts — peers who already sent us this
                        // data will see our summary matches and skip re-sending.
                        // Spawned as a background task to avoid blocking the operation.
                        {
                            let op_mgr = op_manager.clone();
                            let contract_key = *key;
                            let summary = update_summary.clone();
                            tokio::spawn(async move {
                                send_proactive_summary_notification(
                                    &op_mgr,
                                    &contract_key,
                                    sender_addr,
                                    summary,
                                )
                                .await;
                            });
                        }
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
                    use crate::operations::orphan_streams::{
                        OrphanStreamError, STREAM_CLAIM_TIMEOUT,
                    };

                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        stream_id = %stream_id,
                        total_size,
                        "Processing UPDATE RequestUpdateStreaming"
                    );

                    // Step 1: Claim the stream from orphan registry (atomic dedup)
                    let peer_addr = match source_addr {
                        Some(addr) => addr,
                        None => {
                            tracing::error!(tx = %id, "source_addr missing for streaming UPDATE request");
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
                                "UPDATE RequestUpdateStreaming skipped — stream already claimed (dedup)"
                            );
                            // Push the operation state back since load_or_init popped it.
                            // Without this, duplicate metadata messages (from embedded fragment #1)
                            // permanently lose the operation state.
                            if self.state.is_some() {
                                if let Err(e) = op_manager
                                    .push(
                                        *id,
                                        OpEnum::Update(UpdateOp {
                                            id: *id,
                                            state: self.state,
                                            stats,
                                            upstream_addr: self.upstream_addr,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push UPDATE op state back after dedup");
                                }
                            }
                            return Err(OpError::OpNotPresent(*id));
                        }
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to claim stream from orphan registry for UPDATE"
                            );
                            // Push the operation state back to prevent loss
                            if self.state.is_some() {
                                if let Err(e) = op_manager
                                    .push(
                                        *id,
                                        OpEnum::Update(UpdateOp {
                                            id: *id,
                                            state: self.state,
                                            stats,
                                            upstream_addr: self.upstream_addr,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push UPDATE op state back after orphan claim failure");
                                }
                            }
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
                                Some(updated_value.len()),
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
                        new_state = Some(UpdateState::Finished(FinishedData {
                            key: *key,
                            summary: summary.clone(),
                        }));
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
                    use crate::operations::orphan_streams::{
                        OrphanStreamError, STREAM_CLAIM_TIMEOUT,
                    };

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

                    // Step 1: Claim the stream from orphan registry (atomic dedup)
                    let stream_handle = match op_manager
                        .orphan_stream_registry()
                        .claim_or_wait(sender_addr, *stream_id, STREAM_CLAIM_TIMEOUT)
                        .await
                    {
                        Ok(handle) => handle,
                        Err(OrphanStreamError::AlreadyClaimed) => {
                            tracing::debug!(
                                tx = %id,
                                stream_id = %stream_id,
                                "UPDATE BroadcastToStreaming skipped — stream already claimed (dedup)"
                            );
                            // Push the operation state back since load_or_init popped it.
                            // Without this, duplicate metadata messages (from embedded fragment #1)
                            // permanently lose the operation state.
                            if self.state.is_some() {
                                if let Err(e) = op_manager
                                    .push(
                                        *id,
                                        OpEnum::Update(UpdateOp {
                                            id: *id,
                                            state: self.state,
                                            stats,
                                            upstream_addr: self.upstream_addr,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push UPDATE broadcast op state back after dedup");
                                }
                            }
                            return Err(OpError::OpNotPresent(*id));
                        }
                        Err(e) => {
                            tracing::error!(
                                tx = %id,
                                stream_id = %stream_id,
                                error = %e,
                                "Failed to claim stream from orphan registry for broadcast"
                            );
                            // Push the operation state back to prevent loss
                            if self.state.is_some() {
                                if let Err(e) = op_manager
                                    .push(
                                        *id,
                                        OpEnum::Update(UpdateOp {
                                            id: *id,
                                            state: self.state,
                                            stats,
                                            upstream_addr: self.upstream_addr,
                                        }),
                                    )
                                    .await
                                {
                                    tracing::warn!(tx = %id, error = %e, "failed to push UPDATE broadcast op state back after orphan claim failure");
                                }
                            }
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

                    // Dedup check for streaming broadcasts (always full state)
                    if op_manager.broadcast_dedup_cache.check_and_insert(
                        key,
                        &state_bytes,
                        false,
                        op_manager.interest_manager.now(),
                    ) {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "BroadcastToStreaming skipped — duplicate payload (dedup cache hit)"
                        );
                        new_state = None;
                        return_msg = None;
                        return build_op_result(
                            self.id,
                            new_state,
                            return_msg,
                            stats,
                            self.upstream_addr,
                            forward_hop,
                            None, // No stream data to forward for dedup'd broadcasts
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
                    match update_contract(
                        op_manager,
                        *key,
                        update_data,
                        RelatedContracts::default(),
                    )
                    .await
                    {
                        Ok(UpdateExecution {
                            value: updated_value,
                            summary: streaming_update_summary,
                            changed,
                            ..
                        }) => {
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
                                crate::node::network_status::record_update_received();
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    "Successfully updated contract via BroadcastToStreaming"
                                );

                                // Proactive summary notification (same as BroadcastTo)
                                {
                                    let op_mgr = op_manager.clone();
                                    let contract_key = *key;
                                    let summary = streaming_update_summary.clone();
                                    tokio::spawn(async move {
                                        send_proactive_summary_notification(
                                            &op_mgr,
                                            &contract_key,
                                            sender_addr,
                                            summary,
                                        )
                                        .await;
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            // See issue #3914 and `log_broadcast_to_streaming_failure`
                            // for the classification. The bool return is `true` when
                            // self-heal is needed (real failure, not a benign rejection).
                            if log_broadcast_to_streaming_failure(id, key, &err) {
                                op_manager.try_auto_fetch_contract(key, sender_addr);
                            } else if err.is_invalid_update_rejection() {
                                // Benign stale-version rejection: nudge the sender's
                                // peer-summary cache of us so the next broadcast takes
                                // the fast-path skip. See
                                // `send_summary_back_on_rejection` docs.
                                let op_mgr = op_manager.clone();
                                let contract_key = *key;
                                let sender_summary = sender_summary_bytes.clone();
                                tokio::spawn(async move {
                                    send_summary_back_on_rejection(
                                        &op_mgr,
                                        &contract_key,
                                        sender_addr,
                                        sender_summary,
                                    )
                                    .await;
                                });
                            }
                        }
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

/// Cooldown before retrying a self-healing contract fetch, in milliseconds.
/// 5 minutes: long enough to avoid hammering peers if the contract genuinely
/// doesn't exist, short enough to recover within a session if there was a
/// transient routing failure.
pub(crate) const CONTRACT_FETCH_COOLDOWN_MS: u64 = 300_000;

impl OpManager {
    /// Trigger a background GET when an UPDATE broadcast fails because the node
    /// doesn't have the contract's parameters (code + params). This self-heals
    /// the node by fetching the contract directly from the UPDATE sender, who
    /// is known to have the contract.
    ///
    /// Rate-limited: at most one fetch attempt per contract per 5 minutes.
    pub(crate) fn try_auto_fetch_contract(&self, key: &ContractKey, sender_addr: SocketAddr) {
        use crate::config::GlobalSimulationTime;

        let instance_id = *key.id();
        let now_ms = GlobalSimulationTime::read_time_ms();

        // Atomic rate-limit: try to insert a new entry. If one exists and hasn't
        // expired, bail out. Uses entry API to avoid TOCTOU between check and insert.
        {
            use dashmap::mapref::entry::Entry;
            match self.pending_contract_fetches.entry(instance_id) {
                Entry::Occupied(mut existing) => {
                    let elapsed_ms = now_ms.saturating_sub(*existing.get());
                    if elapsed_ms < CONTRACT_FETCH_COOLDOWN_MS {
                        return; // Still in cooldown
                    }
                    // Cooldown expired — update timestamp while still holding the lock
                    *existing.get_mut() = now_ms;
                }
                Entry::Vacant(slot) => {
                    slot.insert(now_ms);
                }
            }
        }

        // Look up the sender's PeerKeyLocation so we can target them directly
        let sender_pkl = match self.ring.connection_manager.get_peer_by_addr(sender_addr) {
            Some(pkl) => pkl,
            None => {
                tracing::debug!(
                    contract = %key,
                    sender = %sender_addr,
                    "Cannot auto-fetch: UPDATE sender not found in connection manager"
                );
                self.pending_contract_fetches.remove(&instance_id);
                return;
            }
        };

        tracing::info!(
            contract = %key,
            sender = %sender_addr,
            "Auto-fetching contract from UPDATE sender (missing parameters)"
        );

        // Create a GET operation that targets the sender directly
        let max_htl = self.ring.max_hops_to_live;
        let op_manager = self.clone();

        crate::config::GlobalExecutor::spawn(async move {
            let (targeted_op, msg) =
                super::get::start_targeted_op(instance_id, sender_pkl, max_htl);

            match op_manager
                .notify_op_change(
                    crate::message::NetMessage::from(msg),
                    super::OpEnum::Get(targeted_op),
                )
                .await
            {
                Ok(()) => {
                    tracing::info!(
                        contract = %instance_id,
                        target = %sender_addr,
                        "Auto-fetch GET sent directly to UPDATE sender"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        contract = %instance_id,
                        error = %e,
                        "Auto-fetch GET failed to send to UPDATE sender"
                    );
                    op_manager.pending_contract_fetches.remove(&instance_id);
                }
            }
        });
    }

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
    ///   in neighbor hosting targets if we're hosting the contract
    ///
    /// # Hybrid Architecture (2026-01 Refactor)
    ///
    /// Updates are propagated via TWO sources:
    /// 1. Neighbor hosting: peers who have announced they host this contract (fast, local knowledge)
    /// 2. Interest manager: peers who have expressed interest via the Interest/Summary protocol
    ///
    /// This hybrid approach ensures updates reach all interested peers even if HostingAnnounce
    /// messages haven't fully propagated yet.
    pub(crate) fn get_broadcast_targets_update(
        &self,
        key: &ContractKey,
        sender: &SocketAddr,
    ) -> BroadcastTargetResult {
        use std::collections::HashSet;

        let self_addr = self.ring.connection_manager.get_own_addr();
        let is_local_update_initiator = self_addr.as_ref().map(|me| me == sender).unwrap_or(false);

        let mut targets: HashSet<PeerKeyLocation> = HashSet::new();
        let mut proximity_resolve_failed: usize = 0;
        let mut interest_resolve_failed: usize = 0;
        let mut skipped_self: usize = 0;
        let mut skipped_sender: usize = 0;

        // Source 1: Proximity cache (peers who announced they seed this contract)
        // Returns TransportPublicKey (stable identity), resolve to PeerKeyLocation via pub_key lookup
        let proximity_pub_keys = self.neighbor_hosting.neighbors_with_contract(key);
        let proximity_found = proximity_pub_keys.len();

        for pub_key in proximity_pub_keys {
            // Resolve pub_key to PeerKeyLocation (which includes current address)
            if let Some(pkl) = self.ring.connection_manager.get_peer_by_pub_key(&pub_key) {
                // Skip sender to avoid echo (unless we're the originator)
                if let Some(pkl_addr) = pkl.socket_addr() {
                    if &pkl_addr == sender && !is_local_update_initiator {
                        skipped_sender += 1;
                        continue;
                    }
                    // Skip ourselves if not local originator
                    if !is_local_update_initiator && self_addr.as_ref() == Some(&pkl_addr) {
                        skipped_self += 1;
                        continue;
                    }
                }
                targets.insert(pkl);
            } else {
                proximity_resolve_failed += 1;
                tracing::warn!(
                    contract = %format!("{:.8}", key),
                    proximity_neighbor = %pub_key,
                    is_local = is_local_update_initiator,
                    phase = "target_lookup_failed",
                    "Proximity cache neighbor not found in connection manager"
                );
            }
        }

        // Source 2: Interest manager (peers who expressed interest via protocol)
        let interested_peers = self.interest_manager.get_interested_peers(key);
        let interest_found = interested_peers.len();

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
                        skipped_sender += 1;
                        continue;
                    }
                    // Skip ourselves
                    if !is_local_update_initiator && self_addr.as_ref() == Some(&pkl_addr) {
                        skipped_self += 1;
                        continue;
                    }
                }
                targets.insert(pkl);
            } else {
                interest_resolve_failed += 1;
                tracing::warn!(
                    contract = %format!("{:.8}", key),
                    interest_peer = %peer_key.0,
                    is_local = is_local_update_initiator,
                    phase = "target_lookup_failed",
                    "Interest manager peer not found in connection manager"
                );
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
                proximity_sources = proximity_found,
                interest_sources = interest_found,
                phase = "broadcast",
                "UPDATE_PROPAGATION"
            );
        } else {
            tracing::debug!(
                contract = %format!("{:.8}", key),
                peer_addr = %sender,
                self_addr = ?self_addr.map(|a| format!("{:.8}", a)),
                proximity_sources = proximity_found,
                interest_sources = interest_found,
                phase = "warning",
                "UPDATE_PROPAGATION: NO_TARGETS - update will not propagate further"
            );
        }

        BroadcastTargetResult {
            targets: result,
            proximity_found,
            proximity_resolve_failed,
            interest_found,
            interest_resolve_failed,
            skipped_self,
            skipped_sender,
        }
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
    let op_state = output_op.map(OpEnum::Update);
    let return_msg = return_msg.map(NetMessage::from);
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

/// Logs the failure outcome of `update_contract`.
///
/// Splits into two cases (issue #3914):
///
/// 1. The contract WASM merge function rejected the incoming state with a
///    typed `InvalidUpdate{,WithInfo}` error (e.g. "New state version 100
///    must be higher than current version 100"). On production gateways
///    this fires on every re-broadcast that misses the 60s dedup cache,
///    generating 80-130 ERROR/hr per gateway with no actionable signal.
///    Logged at INFO with `event="merge_rejected_invalid_update"`.
///
/// 2. Anything else (missing contract code, storage error, OOG, WASM trap,
///    timeout, internal bug) keeps the original ERROR level. The
///    discriminator is `is_invalid_update_rejection`, which matches the
///    contract-side `InvalidUpdate{,WithInfo}` cause string EXCLUSIVELY,
///    so runtime failures like OOG remain visible to operators.
fn log_update_contract_failure(key: &ContractKey, err: &ExecutorError) {
    if err.is_invalid_update_rejection() {
        tracing::info!(
            contract = %key,
            error = %err,
            event = "merge_rejected_invalid_update",
            "Update rejected by contract: incoming state invalid (likely stale rebroadcast), keeping local"
        );
    } else {
        tracing::error!(
            contract = %key,
            error = %err,
            phase = "error",
            "Failed to update contract value"
        );
    }
}

/// Logs the failure outcome of a `BroadcastToStreaming` relay attempt and
/// returns whether the caller should trigger a self-healing GET.
///
/// The two decisions (log severity, auto-fetch) use DIFFERENT predicates
/// because they answer different questions:
///
/// - Log severity uses `is_invalid_update_rejection` (narrow): only the
///   contract WASM's typed "invalid contract update" rejection counts as
///   benign. Out-of-gas, traps, timeouts stay at WARN even though the
///   contract code is present.
///
/// - Auto-fetch uses `is_contract_exec_rejection` (broad): any time the
///   contract code DID execute (whether successfully rejecting a stale
///   state or running out of gas), the code is present locally and a
///   self-heal GET would be wasted. Auto-fetch only fires for failures
///   where the contract is actually missing (e.g., missing parameters
///   after restart, storage error).
///
/// Returning `true` means "fetch missing contract code"; returning `false`
/// means "contract is present, skip auto-fetch".
///
/// Note: this helper takes `&OpError` while `log_update_contract_failure`
/// takes `&ExecutorError` because the two call sites have different error
/// types in scope (the streaming branch operates on the OpError already
/// produced by `update_contract`'s `Err(err.into())`).
fn log_broadcast_to_streaming_failure(tx: &Transaction, key: &ContractKey, err: &OpError) -> bool {
    if err.is_invalid_update_rejection() {
        tracing::info!(
            tx = %tx,
            %key,
            error = %err,
            event = "merge_rejected_invalid_update",
            "BroadcastToStreaming merge rejected: incoming state invalid (likely stale rebroadcast), keeping local"
        );
    } else {
        tracing::warn!(
            tx = %tx,
            %key,
            error = %err,
            "BroadcastToStreaming update skipped: contract not ready locally"
        );
    }
    // Preserves the pre-#3914 auto-fetch behavior: skip the self-heal
    // GET whenever the contract code is present (broader check than the
    // log-severity decision above).
    !err.is_contract_exec_rejection()
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
pub(crate) async fn update_contract(
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
            // Invariant: after a successful UPDATE, the resulting state must be non-empty.
            // A successful UpdateResponse with an empty value indicates a contract handler bug.
            debug_assert!(
                new_val.size() > 0,
                "update_contract: state must be non-empty after successful UPDATE for contract {key}"
            );
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
            log_update_contract_failure(&key, &err);
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
                    // `UpdateData` is `#[non_exhaustive]` since stdlib
                    // 0.6.0. We can't materialize state from an unknown
                    // variant, so return None and let the caller treat
                    // it as "no state to extract."
                    _ => None,
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
                            ContractHandlerEvent::DelegateRequest { .. }
                            | ContractHandlerEvent::DelegateResponse(_)
                            | ContractHandlerEvent::PutQuery { .. }
                            | ContractHandlerEvent::PutResponse { .. }
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
                            | ContractHandlerEvent::ClientDisconnect { .. } => None,
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

    let state = Some(UpdateState::PrepareRequest(PrepareRequestData {
        key,
        related_contracts,
        update_data,
    }));

    UpdateOp {
        id,
        state,
        stats: Some(UpdateStats {
            target: None,
            contract_location: Some(contract_location),
        }),
        upstream_addr: None, // Local operation, no upstream peer
    }
}

/// This will be called from the node when processing an open request with a specific transaction ID
#[allow(dead_code)] // Phase 4: client_events now uses op_ctx_task::start_client_update; kept for tests/future callers.
pub(crate) fn start_op_with_id(
    key: ContractKey,
    update_data: UpdateData<'static>,
    related_contracts: RelatedContracts<'static>,
    id: Transaction,
) -> UpdateOp {
    let contract_location = Location::from(&key);
    tracing::debug!(%contract_location, %key, "Requesting update with transaction ID {}", id);

    let state = Some(UpdateState::PrepareRequest(PrepareRequestData {
        key,
        related_contracts,
        update_data,
    }));

    UpdateOp {
        id,
        state,
        stats: Some(UpdateStats {
            target: None,
            contract_location: Some(contract_location),
        }),
        upstream_addr: None, // Local operation, no upstream peer
    }
}

/// Entry point from node to operations logic
pub(crate) async fn request_update(
    op_manager: &OpManager,
    mut update_op: UpdateOp,
) -> Result<(), OpError> {
    // Extract the key and check if we need to handle this locally
    let (key, update_data, related_contracts) =
        if let Some(UpdateState::PrepareRequest(data)) = update_op.state.take() {
            (data.key, data.update_data, data.related_contracts)
        } else {
            return Err(OpError::UnexpectedOpState);
        };

    // Find the best peer to send this update to.
    // In the simplified architecture (2026-01 refactor), we use:
    // 1. Neighbor hosting - peers who have announced they host this contract
    // 2. Ring-based routing - find closest potentially-hosting peer
    let sender_addr = op_manager.ring.connection_manager.peer_addr()?;

    // Check neighbor hosting info for neighbors that have announced hosting this contract.
    // This is critical for peer-to-peer updates when peers are directly connected
    // but not explicitly subscribed (e.g., River chat rooms where both peers cache
    // the contract but haven't established a subscription tree).
    //
    // Note: The neighbor hosting info is populated asynchronously via HostingAnnounce messages,
    // so there may be a brief race window after a peer hosts a contract before its
    // neighbors receive the announcement. This is acceptable - the ring-based fallback
    // handles this case, and the neighbor hosting info improves the common case where
    // announcements have propagated.
    let proximity_neighbors: Vec<_> = op_manager.neighbor_hosting.neighbors_with_contract(&key);

    let mut target_from_proximity = None;
    for pub_key in &proximity_neighbors {
        match op_manager
            .ring
            .connection_manager
            .get_peer_by_pub_key(pub_key)
        {
            Some(peer) => {
                // Skip ourselves
                if peer
                    .socket_addr()
                    .map(|a| a == sender_addr)
                    .unwrap_or(false)
                {
                    continue;
                }
                target_from_proximity = Some(peer);
                break;
            }
            None => {
                // Neighbor is in proximity cache but no longer connected.
                // This is normal during connection churn - the proximity cache
                // will be cleaned up when the disconnect is processed.
                tracing::debug!(
                    %key,
                    peer = %pub_key,
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
            .closest_potentially_hosting(&key, [sender_addr].as_slice());

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

            // Check if we're hosting this contract
            let is_hosting = op_manager.ring.is_hosting_contract(&key);
            let should_handle_update = is_hosting;

            if !should_handle_update {
                tracing::error!(
                    contract = %key,
                    phase = "error",
                    "UPDATE: Cannot update contract on isolated node - contract not hosted"
                );
                return Err(OpError::RingError(RingError::NoHostingPeers(*key.id())));
            }

            // Update the contract locally. This path is reached when:
            // 1. No remote peers are available (isolated node OR no suitable hosting peers)
            // 2. We are hosting the contract (verified above)
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
            contract_location: Some(Location::from(&key)),
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
        state: Some(UpdateState::Finished(FinishedData {
            key,
            summary: summary.clone(),
        })),
        stats: None,
        upstream_addr: None,
    };
    let host_result = op.to_host_result();
    // Use try_send to avoid blocking spawned executor tasks (see channel-safety.md).
    op_manager
        .result_router_tx
        .try_send((id, host_result))
        .map_err(|error| {
            tracing::error!(tx = %id, error = %error, phase = "error", "Failed to send UPDATE result to result router (channel full or closed)");
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
        state: Some(UpdateState::Finished(FinishedData {
            key,
            summary: summary.clone(),
        })),
        stats: None,
        upstream_addr: None, // Terminal state, no routing needed
    };

    let host_result = op.to_host_result();
    // Note: record_contract_updated is called in commit_state_update (the single
    // chokepoint for all state updates), so we don't call it here to avoid double-counting.

    // Use try_send to avoid blocking spawned executor tasks (see channel-safety.md).
    op_manager
        .result_router_tx
        .try_send((id, host_result))
        .map_err(|error| {
            tracing::error!(
                tx = %id,
                error = %error,
                phase = "error",
                "Failed to send UPDATE result to result router (channel full or closed)"
            );
            OpError::NotificationError
        })?;

    // Use try_send to avoid blocking (see channel-safety.md).
    if let Err(error) = op_manager
        .to_event_listener
        .notifications_sender()
        .try_send(Either::Right(NodeEvent::TransactionCompleted(id)))
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
        matches!(self.state, Some(UpdateState::Finished(_)))
    }
}

/// Send proactive summary notifications to interested peers after a successful
/// state change. This tells neighbors "my state just updated — here's my new
/// summary" so they can update their cached view of us and skip sending
/// redundant broadcasts.
///
/// Accepts the already-computed summary from `UpdateExecution` to avoid an
/// extra WASM `summarize_state` call.
pub(crate) async fn send_proactive_summary_notification(
    op_manager: &OpManager,
    key: &ContractKey,
    sender_addr: SocketAddr,
    summary: StateSummary<'static>,
) {
    use crate::message::{InterestMessage, SummaryEntry};
    use crate::ring::interest::contract_hash;

    // Throttle: at most one notification per contract per 100ms
    if !op_manager
        .interest_manager
        .should_send_summary_notification(key)
    {
        return;
    }

    // Build the Summaries message with our updated summary
    let hash = contract_hash(key);
    let message = InterestMessage::Summaries {
        entries: vec![SummaryEntry::from_summary(hash, Some(&summary))],
    };

    // Get interested peers and send to each (excluding the sender who just sent us the update)
    let interested = op_manager.interest_manager.get_interested_peers(key);
    let self_addr = op_manager.ring.connection_manager.get_own_addr();

    for (peer_key, _interest) in &interested {
        // Resolve peer to socket address
        let peer_addr = match op_manager
            .ring
            .connection_manager
            .get_peer_by_pub_key(&peer_key.0)
        {
            Some(pkl) => match pkl.socket_addr() {
                Some(addr) => addr,
                None => continue,
            },
            None => continue,
        };

        // Skip sender (they just gave us this data) and ourselves
        if peer_addr == sender_addr {
            continue;
        }
        if self_addr.as_ref() == Some(&peer_addr) {
            continue;
        }

        if let Err(e) = op_manager
            .notify_node_event(NodeEvent::SendInterestMessage {
                target: peer_addr,
                message: message.clone(),
            })
            .await
        {
            tracing::debug!(
                contract = %key,
                peer = %peer_addr,
                error = %e,
                "Failed to send proactive summary notification"
            );
        }
    }

    tracing::debug!(
        contract = %key,
        peer_count = interested.len(),
        "Sent proactive summary notifications after state change"
    );
}

/// Send our current summary to the peer whose broadcast we just rejected,
/// **only when the peer's included summary equals our own** — i.e. the
/// peer and we already agree on state, but the peer's cached view of
/// our summary is stale (`None` or out of date) so their send path
/// fell back to full-state at `broadcast_queue.rs:352-356` instead of
/// hitting the summaries-equal fast-path skip.
///
/// The gate on `sender_summary_bytes == our_summary` is load-bearing.
/// Without it, when our state is strictly ahead of the peer's and we
/// reject their stale broadcast, the `InterestMessage::Summaries`
/// handler at `node.rs:1791-1839` detects a mismatch and pushes the
/// peer's state back via `SyncStateToPeer` — which we then reject
/// again, creating a tight reject→summary→resync→reject loop bounded
/// only by the 60 s `BroadcastDedupCache` (and defeated entirely by
/// payload-byte variation). Restricting to the matching-summary case
/// makes this helper a pure convergence nudge: the `Summaries`
/// receiver's stale-detector returns `is_stale = false`, no
/// `SyncStateToPeer` fires, the only outcome is the sender's
/// peer-summary cache of us flipping from `None` → `Some(our_summary)`
/// so its next broadcast takes the fast-path skip.
///
/// Observed in production (nova, vega): ~80–130 rejections/hour per
/// gateway, all with `incoming_state_size == local_state_size` and
/// "version N == N" — i.e. exactly the same-summary case this helper
/// targets. The 5-min `Interests`/`Summaries` heartbeat eventually
/// populates the sender's cache; this shortcut closes the loop in one
/// round-trip instead of waiting for the next heartbeat tick.
///
/// Unlike `send_proactive_summary_notification` (success path, fans
/// out to all interested peers, explicitly excludes the sender), this
/// targets the single sender — on rejection, the sender is the ONE
/// peer whose cache we know is wrong about us.
///
/// Call sites MUST gate this on `err.is_invalid_update_rejection()`
/// (not the broader `is_contract_exec_rejection`): the stricter
/// predicate matches ONLY the benign "new state version not higher"
/// case, not OOG / `MaxComputeTimeExceeded` / WASM traps / validation
/// failures — which are attacker-inducible and shouldn't amplify into
/// extra messages.
pub(crate) async fn send_summary_back_on_rejection(
    op_manager: &OpManager,
    key: &ContractKey,
    target_addr: SocketAddr,
    sender_summary_bytes: Vec<u8>,
) {
    use crate::message::{InterestMessage, SummaryEntry};
    use crate::ring::interest::contract_hash;

    // Throttle BEFORE the WASM `summarize_state` call. Even with call
    // sites gated on `is_invalid_update_rejection`, a flood of
    // crafted-payload broadcasts that all produce benign rejections
    // would otherwise force one `summarize_state` call per rejection.
    // `should_send_summary_notification` caps at ~10 calls/sec/contract.
    //
    // Sharing the throttle map with `send_proactive_summary_notification`
    // is intentional: both paths emit the same `InterestMessage::Summaries`
    // shape, and the existing throttle's purpose (don't re-spam summary
    // updates for the same contract in bursts) applies to both callers.
    if !op_manager
        .interest_manager
        .should_send_summary_notification(key)
    {
        return;
    }

    let Some(our_summary) = op_manager
        .interest_manager
        .get_contract_summary(op_manager, key)
        .await
    else {
        tracing::debug!(
            contract = %key,
            peer = %target_addr,
            "Skipping summary-back on rejection — no local summary available"
        );
        return;
    };

    // Critical: only proceed when summaries match. See function docs for
    // the SyncStateToPeer-loop rationale. A differing summary means the
    // peer is genuinely out of sync, in which case the 5-min heartbeat
    // is the right convergence mechanism — firing Summaries here would
    // escalate into a per-rejection state ping-pong.
    if our_summary.as_ref() != sender_summary_bytes.as_slice() {
        tracing::debug!(
            contract = %key,
            peer = %target_addr,
            "Skipping summary-back on rejection — sender's summary differs \
             from ours (peer is genuinely out of sync; heartbeat will converge)"
        );
        return;
    }

    let hash = contract_hash(key);
    let message = InterestMessage::Summaries {
        entries: vec![SummaryEntry::from_summary(hash, Some(&our_summary))],
    };

    if let Err(e) = op_manager
        .notify_node_event(NodeEvent::SendInterestMessage {
            target: target_addr,
            message,
        })
        .await
    {
        // info! not debug! — debug! is stripped in release builds via
        // `tracing_max_level_info`, so a saturated event-loop channel
        // would fail silently in production.
        tracing::info!(
            contract = %key,
            peer = %target_addr,
            error = %e,
            "Failed to send summary-back after broadcast rejection"
        );
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

// State machine for UPDATE operations.
//
// # Important: Updates are Fire-and-Forget
//
// Updates spread through the subscription tree like a virus - there is no acknowledgment
// or completion signal that propagates back. When a node receives an UPDATE:
// 1. It applies the update locally
// 2. It broadcasts to its downstream subscribers (if any)
// 3. It does NOT wait for or expect any response from downstream
//
// ── Type-state data structs ──────────────────────────────────────────────
//
// Each state in the UPDATE state machine is a named struct with typed
// transition methods.  This gives compile-time guarantees:
//
//   ReceivedRequest ── (next state depends on whether contract is found locally)
//   PrepareRequest ── into_finished()  ──► Finished
//
// Invalid transitions (e.g. Finished → ReceivedRequest) are unrepresentable
// because the target type simply has no such method.

/// Data for the Finished state: update was applied locally.
///
/// The `Finished` state only indicates that the LOCAL update was applied successfully.
/// It does NOT mean all subscribers have received the update - that's unknowable.
#[derive(Debug)]
pub struct FinishedData {
    pub key: ContractKey,
    pub summary: StateSummary<'static>,
}

/// Data for the PrepareRequest state: client-initiated update being prepared.
#[derive(Debug)]
pub struct PrepareRequestData {
    pub key: ContractKey,
    pub related_contracts: RelatedContracts<'static>,
    /// The update data - can be a delta (from client) or full state (from PUT/executor).
    /// This is passed to update_contract which calls UpdateQuery to merge and persist.
    pub update_data: UpdateData<'static>,
}

impl PrepareRequestData {
    /// Transition to Finished after the update is applied locally.
    ///
    /// Encodes valid transition: PrepareRequest → Finished.
    #[allow(dead_code)] // Documents valid transition; see type-state pattern
    pub fn into_finished(self, summary: StateSummary<'static>) -> FinishedData {
        FinishedData {
            key: self.key,
            summary,
        }
    }
}

// ── State enum (wraps the typed structs) ─────────────────────────────────

#[derive(Debug)]
pub enum UpdateState {
    /// Initial state when receiving an update request from another peer.
    ReceivedRequest,
    /// The update was applied locally.
    Finished(FinishedData),
    /// Preparing to send an update request (client-initiated updates).
    PrepareRequest(PrepareRequestData),
}

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::operations::OpOutcome;
    use crate::operations::test_utils::make_contract_key;

    fn make_update_op(state: Option<UpdateState>, stats: Option<UpdateStats>) -> UpdateOp {
        UpdateOp {
            id: Transaction::new::<UpdateMsg>(),
            state,
            stats,
            upstream_addr: None,
        }
    }

    #[test]
    fn is_client_initiated_true_when_no_upstream() {
        let op = make_update_op(None, None);
        assert!(op.is_client_initiated());
    }

    #[test]
    fn is_client_initiated_false_when_forwarded() {
        let op = UpdateOp {
            id: Transaction::new::<UpdateMsg>(),
            state: None,
            stats: None,
            upstream_addr: Some("127.0.0.1:12345".parse().unwrap()),
        };
        assert!(!op.is_client_initiated());
    }

    #[test]
    fn update_op_outcome_success_untimed_when_finalized_with_full_stats() {
        let target = PeerKeyLocation::random();
        let loc = Location::random();
        let op = make_update_op(
            Some(UpdateState::Finished(FinishedData {
                key: make_contract_key(1),
                summary: StateSummary::from(vec![1u8]),
            })),
            Some(UpdateStats {
                target: Some(target.clone()),
                contract_location: Some(loc),
            }),
        );
        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer,
                contract_location,
            } => {
                assert_eq!(*target_peer, target);
                assert_eq!(contract_location, loc);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpFailure { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed for finalized update with full stats")
            }
        }
    }

    #[test]
    fn update_op_outcome_irrelevant_when_finalized_without_stats() {
        let op = make_update_op(
            Some(UpdateState::Finished(FinishedData {
                key: make_contract_key(1),
                summary: StateSummary::from(vec![1u8]),
            })),
            None,
        );
        assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
    }

    #[test]
    fn update_op_outcome_irrelevant_when_finalized_with_partial_stats() {
        // target is None — should fall through to Irrelevant
        let op = make_update_op(
            Some(UpdateState::Finished(FinishedData {
                key: make_contract_key(1),
                summary: StateSummary::from(vec![1u8]),
            })),
            Some(UpdateStats {
                target: None,
                contract_location: Some(Location::random()),
            }),
        );
        assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
    }

    #[test]
    fn update_op_outcome_failure_when_not_finalized_with_full_stats() {
        let target = PeerKeyLocation::random();
        let loc = Location::random();
        let op = make_update_op(
            Some(UpdateState::ReceivedRequest),
            Some(UpdateStats {
                target: Some(target.clone()),
                contract_location: Some(loc),
            }),
        );
        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer,
                contract_location,
            } => {
                assert_eq!(*target_peer, target);
                assert_eq!(contract_location, loc);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpSuccessUntimed { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpFailure for non-finalized update with full stats")
            }
        }
    }

    #[test]
    fn update_op_outcome_incomplete_when_not_finalized_without_stats() {
        let op = make_update_op(Some(UpdateState::ReceivedRequest), None);
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
    }

    #[test]
    fn update_op_failure_routing_info_with_full_stats() {
        let target = PeerKeyLocation::random();
        let loc = Location::random();
        let op = make_update_op(
            None,
            Some(UpdateStats {
                target: Some(target.clone()),
                contract_location: Some(loc),
            }),
        );
        let info = op.failure_routing_info().expect("should have routing info");
        assert_eq!(info.0, target);
        assert_eq!(info.1, loc);
    }

    #[test]
    fn update_op_failure_routing_info_without_stats() {
        let op = make_update_op(None, None);
        assert!(op.failure_routing_info().is_none());
    }

    #[test]
    fn update_op_failure_routing_info_with_partial_stats() {
        let op = make_update_op(
            None,
            Some(UpdateStats {
                target: None,
                contract_location: Some(Location::random()),
            }),
        );
        assert!(op.failure_routing_info().is_none());
    }

    /// Verify that start_op creates an UpdateOp with contract_location populated
    /// but target=None. The target is only set when the operation finds a peer
    /// to forward to.
    #[test]
    fn start_op_creates_update_with_partial_stats() {
        use crate::operations::test_utils::make_test_contract;
        let contract = make_test_contract(&[1u8]);
        let contract_key = contract.key();
        let contract_location = Location::from(&contract_key);
        let op = start_op(
            contract_key,
            UpdateData::State(WrappedState::new(vec![1u8]).into()),
            RelatedContracts::default(),
        );
        // Stats should exist with contract_location but no target
        let stats = op.stats.as_ref().expect("start_op should create stats");
        assert!(
            stats.target.is_none(),
            "target should be None before forwarding"
        );
        assert_eq!(
            stats.contract_location,
            Some(contract_location),
            "contract_location should be set from start_op"
        );
        // Outcome should be Incomplete since it has partial stats
        // (finalized=false because state=PrepareRequest, stats has no target)
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
    }

    /// Simulate the update operation lifecycle: partial stats → full stats → finished.
    /// This mirrors what happens when an update operation finds a forwarding target.
    #[test]
    fn update_op_stats_lifecycle_from_partial_to_complete() {
        let target = PeerKeyLocation::random();
        let loc = Location::random();

        // Step 1: Created with partial stats (no target yet)
        let mut op = make_update_op(
            Some(UpdateState::ReceivedRequest),
            Some(UpdateStats {
                target: None,
                contract_location: Some(loc),
            }),
        );
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
        assert!(op.failure_routing_info().is_none());

        // Step 2: Target found during forwarding
        op.stats = Some(UpdateStats {
            target: Some(target.clone()),
            contract_location: Some(loc),
        });
        // Still not finalized → ContractOpFailure (has full stats but not done)
        match op.outcome() {
            OpOutcome::ContractOpFailure {
                target_peer,
                contract_location,
            } => {
                assert_eq!(*target_peer, target);
                assert_eq!(contract_location, loc);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpSuccessUntimed { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpFailure for in-progress update with stats")
            }
        }
        assert!(op.failure_routing_info().is_some());

        // Step 3: Operation finishes
        op.state = Some(UpdateState::Finished(FinishedData {
            key: make_contract_key(1),
            summary: StateSummary::from(vec![1u8]),
        }));
        match op.outcome() {
            OpOutcome::ContractOpSuccessUntimed {
                target_peer,
                contract_location,
            } => {
                assert_eq!(*target_peer, target);
                assert_eq!(contract_location, loc);
            }
            OpOutcome::ContractOpSuccess { .. }
            | OpOutcome::ContractOpFailure { .. }
            | OpOutcome::Incomplete
            | OpOutcome::Irrelevant => {
                panic!("Expected ContractOpSuccessUntimed for finished update with stats")
            }
        }
    }

    /// Verify that contract_location=None in UpdateStats causes Incomplete/Irrelevant
    /// outcomes even when a target is set.
    #[test]
    fn update_op_outcome_with_target_but_no_contract_location() {
        let target = PeerKeyLocation::random();

        // Not finalized with target but no location → Incomplete
        let op = make_update_op(
            Some(UpdateState::ReceivedRequest),
            Some(UpdateStats {
                target: Some(target.clone()),
                contract_location: None,
            }),
        );
        assert!(matches!(op.outcome(), OpOutcome::Incomplete));
        assert!(op.failure_routing_info().is_none());

        // Finalized with target but no location → Irrelevant
        let op = make_update_op(
            Some(UpdateState::Finished(FinishedData {
                key: make_contract_key(1),
                summary: StateSummary::from(vec![1u8]),
            })),
            Some(UpdateStats {
                target: Some(target),
                contract_location: None,
            }),
        );
        assert!(matches!(op.outcome(), OpOutcome::Irrelevant));
    }

    // ── Intermediate node stats tracking tests (#3527) ─────────────────────

    use crate::operations::test_utils::make_peer;

    /// Non-finalized UPDATE with stats reports ContractOpFailure on timeout.
    #[test]
    fn test_update_failure_outcome_with_stats() {
        let target = make_peer(9001);
        let contract_location = Location::from(&make_contract_key(42));

        let op = make_update_op(
            Some(UpdateState::ReceivedRequest),
            Some(UpdateStats {
                target: Some(target.clone()),
                contract_location: Some(contract_location),
            }),
        );

        assert!(!op.finalized());
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

    /// Non-finalized UPDATE without stats reports Incomplete.
    #[test]
    fn test_update_failure_outcome_without_stats() {
        let op = make_update_op(Some(UpdateState::ReceivedRequest), None);

        assert!(!op.finalized());
        assert!(
            matches!(op.outcome(), OpOutcome::Incomplete),
            "UPDATE without stats should return Incomplete"
        );
    }

    /// failure_routing_info() returns correct peer and location from stats.
    #[test]
    fn test_update_failure_routing_info() {
        let target = make_peer(9002);
        let contract_location = Location::from(&make_contract_key(42));

        let op = make_update_op(
            Some(UpdateState::ReceivedRequest),
            Some(UpdateStats {
                target: Some(target.clone()),
                contract_location: Some(contract_location),
            }),
        );

        let (peer, loc) = op.failure_routing_info().expect("should have routing info");
        assert_eq!(peer, target);
        assert_eq!(loc, contract_location);
    }

    /// Regression tests for issue #3914: misleading ERROR/WARN log noise from
    /// benign WASM rejections of stale broadcast UPDATEs. The contract correctly
    /// rejects an incoming state at a version we already hold (a re-broadcast
    /// the dedup cache missed). On production gateways this generated 80-130
    /// ERROR-level lines per hour per gateway. The tests below pin both that
    /// the benign case is now INFO AND that real WASM failures (out-of-gas,
    /// max-compute-time, traps) stay at ERROR/WARN, so the predicate cannot
    /// silently broaden and hide real failures.
    mod log_severity {
        use super::*;
        use crate::contract::ExecutorError;
        use crate::test_utils::TestLogger;
        use freenet_stdlib::client_api::{ContractError as StdContractError, RequestError};

        // Constructors use `From<RequestError> for ExecutorError` (a public
        // impl) rather than the module-private `ExecutorError::request`, so
        // these helpers don't require widening any visibility for tests.
        fn invalid_update_rejection() -> ExecutorError {
            // Mirrors the production cause string exactly: stdlib's
            // `update_exec_error` prefixes "execution error: " and the
            // contract WASM's `InvalidUpdateWithInfo` Display produces
            // "invalid contract update, reason: ...".
            let req: RequestError = StdContractError::update_exec_error(
                make_contract_key(1),
                "invalid contract update, reason: New state version 100 must be higher than current version 100",
            )
            .into();
            req.into()
        }

        fn out_of_gas_failure() -> ExecutorError {
            // Real WASM fault: contract ran out of gas. Same `update_exec_error`
            // wrapper as the benign case (so the loose `is_contract_exec_rejection`
            // predicate matches both), but the cause string starts with
            // "execution error: The operation ran out of gas..." which the
            // tighter `is_invalid_update_rejection` predicate must REJECT.
            let req: RequestError = StdContractError::update_exec_error(
                make_contract_key(1),
                "The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation.",
            )
            .into();
            req.into()
        }

        fn missing_parameters_failure() -> ExecutorError {
            // Real failure case: contract not ready locally, auto-fetch needed.
            let req: RequestError = StdContractError::Update {
                key: make_contract_key(2),
                cause: "missing contract parameters".into(),
            }
            .into();
            req.into()
        }

        #[test]
        fn update_contract_failure_logs_info_for_invalid_update_rejection() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();

            log_update_contract_failure(&make_contract_key(1), &invalid_update_rejection());

            assert!(
                logger.contains("merge_rejected_invalid_update"),
                "expected event=merge_rejected_invalid_update in logs, got: {:?}",
                logger.logs()
            );
            assert!(
                logger.contains("INFO"),
                "expected INFO-level log for invalid-update rejection, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger.logs().iter().any(|l| l.contains("ERROR")),
                "invalid-update rejection must not produce ERROR-level logs, got: {:?}",
                logger.logs()
            );
        }

        #[test]
        fn update_contract_failure_logs_error_for_real_failure() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();

            log_update_contract_failure(&make_contract_key(2), &missing_parameters_failure());

            assert!(
                logger.contains("ERROR"),
                "real failures must remain ERROR-level, got: {:?}",
                logger.logs()
            );
            assert!(
                logger.contains("Failed to update contract value"),
                "expected ERROR message text, got: {:?}",
                logger.logs()
            );
        }

        /// CRITICAL: out-of-gas comes through the same `update_exec_error`
        /// wrapper as the benign rejection, so the loose
        /// `is_contract_exec_rejection` predicate matches it (used for the
        /// auto-fetch gate, where this is correct: contract code IS present).
        /// But for log severity, OOG is a real bug operators must see and
        /// MUST stay at ERROR. This test pins that.
        #[test]
        fn update_contract_failure_logs_error_for_out_of_gas() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();

            log_update_contract_failure(&make_contract_key(1), &out_of_gas_failure());

            assert!(
                logger.contains("ERROR"),
                "out-of-gas must remain ERROR-level (real WASM fault), got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("merge_rejected_invalid_update")),
                "out-of-gas must NOT be classified as a benign rejection, got: {:?}",
                logger.logs()
            );
        }

        #[test]
        fn broadcast_to_streaming_failure_logs_info_and_skips_auto_fetch_for_invalid_update() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = invalid_update_rejection().into();

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(1), &err);

            assert!(
                !needs_auto_fetch,
                "invalid-update rejection must NOT trigger self-heal auto-fetch (contract code is present)"
            );
            assert!(
                logger.contains("merge_rejected_invalid_update"),
                "expected event=merge_rejected_invalid_update in logs, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger.logs().iter().any(|l| l.contains("WARN")),
                "invalid-update rejection must not produce WARN-level logs (the old misleading 'contract not ready locally' line), got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("contract not ready locally")),
                "the misleading 'contract not ready locally' message must not appear for invalid-update rejections, got: {:?}",
                logger.logs()
            );
        }

        #[test]
        fn broadcast_to_streaming_failure_logs_warn_and_triggers_auto_fetch_for_real_failure() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = missing_parameters_failure().into();

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(2), &err);

            assert!(
                needs_auto_fetch,
                "real failures must trigger self-heal auto-fetch"
            );
            assert!(
                logger.contains("WARN"),
                "real failures remain WARN-level for the streaming branch, got: {:?}",
                logger.logs()
            );
            assert!(
                logger.contains("contract not ready locally"),
                "expected the WARN message text for real failure, got: {:?}",
                logger.logs()
            );
        }

        /// Mirror of `update_contract_failure_logs_error_for_out_of_gas` for
        /// the streaming branch: OOG must stay at WARN, AND auto-fetch must
        /// be SKIPPED because the contract code is present (broader predicate
        /// `is_contract_exec_rejection` correctly catches this case). The two
        /// decisions are deliberately decoupled by the helper.
        #[test]
        fn broadcast_to_streaming_failure_logs_warn_and_skips_auto_fetch_for_out_of_gas() {
            let logger = TestLogger::new().capture_logs().with_level("info").init();
            let tx = Transaction::new::<UpdateMsg>();
            let err: OpError = out_of_gas_failure().into();

            let needs_auto_fetch =
                log_broadcast_to_streaming_failure(&tx, &make_contract_key(1), &err);

            assert!(
                logger.contains("WARN"),
                "out-of-gas must remain WARN-level for the streaming branch, got: {:?}",
                logger.logs()
            );
            assert!(
                !logger
                    .logs()
                    .iter()
                    .any(|l| l.contains("merge_rejected_invalid_update")),
                "out-of-gas must NOT be classified as a benign rejection, got: {:?}",
                logger.logs()
            );
            assert!(
                !needs_auto_fetch,
                "out-of-gas must NOT trigger self-heal auto-fetch (contract code is present locally; the broader is_contract_exec_rejection predicate catches this case independently of log severity)"
            );
        }
    }

    /// Pin: `UpdateMsg::BroadcastTo` handler in `process_message` must spawn
    /// `send_summary_back_on_rejection` when the WASM merge rejects as an
    /// invalid-update rejection (stale version). Skipping this causes the
    /// sender to repeatedly full-state-broadcast identical content; see
    /// PR description for production evidence.
    #[test]
    fn legacy_broadcast_to_sends_summary_back_on_rejection() {
        let src = include_str!("update.rs");
        let bcast_arm_pos = src
            .find("UpdateMsg::BroadcastTo {")
            .expect("UpdateMsg::BroadcastTo match arm not found");
        // Scope to the Err(err) branch of this arm.
        let err_branch_start = src[bcast_arm_pos..]
            .find("Err(err) =>")
            .map(|p| bcast_arm_pos + p)
            .expect("BroadcastTo Err(err) branch not found");
        let branch = &src[err_branch_start..err_branch_start + 3500];

        assert!(
            branch.contains("err.is_invalid_update_rejection()"),
            "BroadcastTo rejection branch MUST gate summary-back on \
             is_invalid_update_rejection (not is_contract_exec_rejection): \
             the broader predicate matches OOG/traps which are \
             attacker-inducible and must not amplify into summary-back"
        );
        assert!(
            branch.contains("send_summary_back_on_rejection"),
            "send_summary_back_on_rejection call missing — stale-version \
             rejections in legacy BroadcastTo will keep amplifying"
        );
    }

    /// Pin: `UpdateMsg::BroadcastToStreaming` handler must spawn
    /// `send_summary_back_on_rejection` on the invalid-update rejection
    /// branch (the non-`try_auto_fetch_contract` branch).
    #[test]
    fn legacy_broadcast_to_streaming_sends_summary_back_on_rejection() {
        let src = include_str!("update.rs");
        let stream_arm_pos = src
            .find("UpdateMsg::BroadcastToStreaming")
            .expect("BroadcastToStreaming arm not found");
        let err_branch_start = src[stream_arm_pos..]
            .find("Err(err) =>")
            .map(|p| stream_arm_pos + p)
            .expect("BroadcastToStreaming Err(err) branch not found");
        let branch = &src[err_branch_start..err_branch_start + 3000];

        assert!(
            branch.contains("err.is_invalid_update_rejection()"),
            "BroadcastToStreaming Err branch must gate summary-back on \
             is_invalid_update_rejection"
        );
        assert!(
            branch.contains("send_summary_back_on_rejection"),
            "BroadcastToStreaming Err branch must spawn \
             send_summary_back_on_rejection on invalid-update rejection"
        );
        assert!(
            branch.contains("try_auto_fetch_contract"),
            "BroadcastToStreaming Err branch must still call \
             try_auto_fetch_contract on the non-rejection (missing-contract) path"
        );
    }

    /// Pin: `send_summary_back_on_rejection` helper MUST gate on
    /// `sender_summary_bytes` matching our current summary before sending
    /// `InterestMessage::Summaries`. Without this gate, a mismatch triggers
    /// `SyncStateToPeer` at `node.rs:1791-1839` which re-sends the sender's
    /// stale state back to us, creating a reject→summary→resync→reject
    /// loop. ALSO pins the throttle-before-WASM ordering to prevent
    /// attacker-induced `summarize_state` amplification under rejection
    /// floods. See the helper's docs and PR description.
    #[test]
    fn summary_back_helper_gates_on_summary_equality() {
        let src = include_str!("update.rs");
        let fn_start = src
            .find("pub(crate) async fn send_summary_back_on_rejection(")
            .expect("send_summary_back_on_rejection fn not found");
        let fn_end_offset = src[fn_start..]
            .find("\n}\n")
            .expect("send_summary_back_on_rejection fn close not found");
        let fn_body = &src[fn_start..fn_start + fn_end_offset];

        assert!(
            fn_body.contains("sender_summary_bytes"),
            "send_summary_back_on_rejection must take sender_summary_bytes \
             as a parameter"
        );

        // Pin throttle-before-WASM ordering. Without this, an attacker
        // inducing rejections can force one `summarize_state` WASM call per
        // rejection — the equality gate below guards only the outgoing send.
        let throttle_pos = fn_body
            .find("should_send_summary_notification")
            .expect("helper MUST call should_send_summary_notification (throttle)");
        let wasm_call_pos = fn_body
            .find("get_contract_summary")
            .expect("helper must call get_contract_summary to compute our_summary");
        assert!(
            throttle_pos < wasm_call_pos,
            "should_send_summary_notification MUST run before get_contract_summary \
             — otherwise attacker-induced rejections force unbounded WASM amplification"
        );

        // Pin the EXACT inequality direction: on difference → early return.
        // If the direction is reversed (== → early return, meaning "send only
        // when they differ"), the helper reintroduces the SyncStateToPeer
        // loop that this PR fixes.
        let inequality_check_pos = fn_body
            .find("our_summary.as_ref() != sender_summary_bytes.as_slice()")
            .expect(
                "helper MUST use `our_summary.as_ref() != sender_summary_bytes.as_slice()` \
                 as the gate condition (reversed direction reintroduces the SyncStateToPeer \
                 loop — see node.rs:1791-1839)",
            );
        let after_check = &fn_body[inequality_check_pos..];
        let return_pos = after_check.find("return;").expect(
            "gate must early-return on inequality (reversed direction would bypass \
             the SyncStateToPeer safeguard)",
        );
        let notify_pos = after_check
            .find("notify_node_event")
            .expect("helper must still call notify_node_event on the equality path");
        assert!(
            return_pos < notify_pos,
            "early return on inequality MUST precede the notify_node_event \
             send — otherwise mismatched summaries would still be transmitted"
        );
    }
}
