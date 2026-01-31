use std::future::Future;
use std::pin::Pin;

use either::Either;

pub(crate) use self::messages::{SubscribeMsg, SubscribeMsgResult};
use super::{get, OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::node::IsOperationCompleted;
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
use tokio::time::{sleep, Duration};

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
    let get_op = get::start_op(instance_id, true, false);
    let visited = super::VisitedPeers::new(&get_op.id);
    get::request_get(op_manager, get_op, visited).await?;

    // Wait for contract to arrive
    wait_for_contract_with_timeout(op_manager, instance_id, CONTRACT_WAIT_TIMEOUT_MS).await
}

#[derive(Debug)]
enum SubscribeState {
    /// Prepare the request to subscribe.
    PrepareRequest {
        id: Transaction,
        instance_id: ContractInstanceId,
        /// Whether this is a renewal (requester already has contract).
        is_renewal: bool,
    },
    /// Awaiting response from downstream peer.
    AwaitingResponse {
        /// The target we're sending to (for hop-by-hop routing)
        next_hop: Option<std::net::SocketAddr>,
    },
    /// Subscription completed.
    Completed { key: ContractKey },
}

pub(crate) struct SubscribeResult {}

impl TryFrom<SubscribeOp> for SubscribeResult {
    type Error = OpError;

    fn try_from(value: SubscribeOp) -> Result<Self, Self::Error> {
        if let Some(SubscribeState::Completed { .. }) = value.state {
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
    let state = Some(SubscribeState::PrepareRequest {
        id,
        instance_id,
        is_renewal,
    });
    SubscribeOp {
        id,
        state,
        requester_addr: None, // Local operation, we are the originator
        is_renewal,
    }
}

/// Create a Subscribe operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(
    instance_id: ContractInstanceId,
    id: Transaction,
    is_renewal: bool,
) -> SubscribeOp {
    let state = Some(SubscribeState::PrepareRequest {
        id,
        instance_id,
        is_renewal,
    });
    SubscribeOp {
        id,
        state,
        requester_addr: None, // Local operation, we are the originator
        is_renewal,
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
    let Some(SubscribeState::PrepareRequest {
        id,
        instance_id,
        is_renewal,
    }) = &sub_op.state
    else {
        return Err(OpError::UnexpectedOpState);
    };
    let is_renewal = *is_renewal;

    tracing::debug!(tx = %id, contract = %instance_id, is_renewal, "subscribe: request_subscribe invoked");

    let own_addr = match op_manager.ring.connection_manager.peer_addr() {
        Ok(addr) => addr,
        Err(_) => {
            // Peer hasn't joined the network yet - check if contract is available locally
            if let Some(key) = super::has_contract(op_manager, *instance_id).await? {
                tracing::info!(
                    tx = %id,
                    contract = %key,
                    phase = "local_complete",
                    "Peer not joined, but contract available locally - completing subscription locally"
                );
                return complete_local_subscription(op_manager, *id, key).await;
            }
            tracing::warn!(
                tx = %id,
                contract = %instance_id,
                phase = "peer_not_joined",
                "Cannot subscribe: peer has not joined network yet and contract not available locally"
            );
            return Err(RingError::PeerNotJoined.into());
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
    let mut visited = super::VisitedPeers::new(id);
    visited.mark_visited(own_addr);

    let candidates = op_manager
        .ring
        .k_closest_potentially_caching(instance_id, &visited, 3);

    // First try the best candidates from k_closest_potentially_caching.
    // If that returns empty, fall back to any available connection.
    // This ensures we join the subscription tree even when the routing algorithm
    // can't find ideal candidates (e.g., due to timing or location filtering).
    let target = if let Some(t) = candidates.first() {
        t.clone()
    } else {
        // k_closest_potentially_caching returned empty - try any connected peer as fallback.
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
                if let Some(key) = super::has_contract(op_manager, *instance_id).await? {
                    tracing::info!(
                        tx = %id,
                        contract = %key,
                        phase = "local_complete",
                        "Contract available locally and no network connections, completing subscription locally"
                    );
                    return complete_local_subscription(op_manager, *id, key).await;
                }
                tracing::warn!(tx = %id, contract = %instance_id, phase = "error", "No remote peers available for subscription");
                return Err(RingError::NoCachingPeers(*instance_id).into());
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
        target_peer = %target_addr,
        "subscribe: forwarding Request to target peer"
    );

    let msg = SubscribeMsg::Request {
        id: *id,
        instance_id: *instance_id,
        htl: op_manager.ring.max_hops_to_live,
        visited,
        is_renewal,
    };

    // Emit telemetry for subscribe request initiation
    if let Some(event) = NetEventLog::subscribe_request(
        id,
        &op_manager.ring,
        *instance_id,
        target.clone(),
        op_manager.ring.max_hops_to_live,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    let op = SubscribeOp {
        id: *id,
        state: Some(SubscribeState::AwaitingResponse {
            next_hop: Some(target_addr),
        }),
        requester_addr: None, // We're the originator
        is_renewal,
    };

    op_manager
        .notify_op_change(NetMessage::from(msg), OpEnum::Subscribe(op))
        .await?;

    Ok(())
}

/// Complete a **standalone** local subscription by notifying the client layer.
///
/// **IMPORTANT:** This function is ONLY used when no remote peers are available (standalone node).
/// For normal network subscriptions, the operation returns a `Completed` state and goes through
/// `handle_op_result`, which sends results via `result_router_tx` directly.
///
/// **Architecture Note (Issue #2075):**
/// Local client subscriptions are deliberately kept separate from network subscriptions:
/// - **Network subscriptions** are stored in `ring.seeding_manager.subscribers` and are used
///   for peer-to-peer UPDATE propagation between nodes
/// - **Local subscriptions** are managed by the contract executor via `update_notifications`
///   channels, which deliver `UpdateNotification` directly to WebSocket clients
async fn complete_local_subscription(
    op_manager: &OpManager,
    id: Transaction,
    key: ContractKey,
) -> Result<(), OpError> {
    tracing::debug!(
        %key,
        tx = %id,
        "Local subscription completed - client will receive updates via executor notification channel"
    );

    // Notify client layer that subscription is complete.
    // The actual update delivery happens through the executor's update_notifications
    // when contract state changes, not through network broadcast targets.
    op_manager
        .notify_node_event(crate::message::NodeEvent::LocalSubscribeComplete {
            tx: id,
            key,
            subscribed: true,
        })
        .await?;

    op_manager.completed(id);
    Ok(())
}

pub(crate) struct SubscribeOp {
    pub id: Transaction,
    state: Option<SubscribeState>,
    /// The address of the peer that requested this subscription.
    /// Used for routing responses back and registering them as downstream subscribers.
    requester_addr: Option<std::net::SocketAddr>,
    /// Whether this is a renewal (requester already has the contract).
    /// Preserved across request/response to avoid sending state to renewals.
    is_renewal: bool,
}

impl SubscribeOp {
    pub(super) fn outcome(&self) -> OpOutcome<'_> {
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, Some(SubscribeState::Completed { .. }))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(SubscribeState::Completed { key }) = self.state {
            Ok(HostResponse::ContractResponse(
                ContractResponse::SubscribeResponse {
                    key,
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
            Some(SubscribeState::AwaitingResponse { next_hop }) => *next_hop,
            _ => None,
        }
    }

    /// Handle aborted connections by failing the operation immediately.
    ///
    /// Unlike Get operations, Subscribe doesn't have alternative routes to try.
    /// The subscription follows the contract's location in the ring, so when
    /// the connection drops, we notify the client of the failure so they can retry.
    pub(crate) async fn handle_abort(self, op_manager: &OpManager) -> Result<(), OpError> {
        tracing::warn!(
            tx = %self.id,
            "Subscribe operation aborted due to connection failure"
        );

        // Create an error result to notify the client
        let error_result: crate::client_events::HostResult =
            Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "Subscribe operation failed: peer connection dropped".into(),
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

impl Operation for SubscribeOp {
    type Message = SubscribeMsg;
    type Result = SubscribeResult;

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Result<OpInitialization<Self>, OpError> {
        let id = *msg.id();
        let msg_type = if matches!(msg, SubscribeMsg::Request { .. }) {
            "Request"
        } else {
            "Response"
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
                let _ = op_manager.push(id, op).await;
                Err(OpError::OpNotPresent(id))
            }
            Ok(None) => {
                // Check if this is a response message - if so, the operation was likely
                // cleaned up due to timeout and we should not create a new operation
                if matches!(msg, SubscribeMsg::Response { .. }) {
                    tracing::debug!(
                        tx = %id,
                        phase = "load_or_init",
                        "SUBSCRIBE_OP_MISSING: response arrived for non-existent operation (likely timed out or race)"
                    );
                    return Err(OpError::OpNotPresent(id));
                }

                // New request from another peer - we're an intermediate/terminal node
                // Extract is_renewal from the Request message
                let is_renewal = match msg {
                    SubscribeMsg::Request { is_renewal, .. } => *is_renewal,
                    _ => false, // Response case handled above
                };
                Ok(OpInitialization {
                    op: Self {
                        id,
                        state: Some(SubscribeState::AwaitingResponse {
                            next_hop: None, // Will be determined during processing
                        }),
                        requester_addr: source_addr, // Store who sent us this request
                        is_renewal,
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
                            // Register the subscribing peer in the interest manager so that
                            // update broadcasts include them as a target immediately.
                            if let Some(pkl) = op_manager
                                .ring
                                .connection_manager
                                .get_peer_by_addr(requester_addr)
                            {
                                let peer_key =
                                    crate::ring::interest::PeerKey::from(pkl.pub_key.clone());
                                op_manager
                                    .interest_manager
                                    .register_peer_interest(&key, peer_key, None, false);
                            }
                            tracing::info!(tx = %id, contract = %key, is_renewal, phase = "response", "Subscription fulfilled, sending Response");
                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    instance_id: *instance_id,
                                    result: SubscribeMsgResult::Subscribed { key },
                                })),
                                next_hop: Some(requester_addr),
                                state: None,
                                stream_data: None,
                            });
                        } else {
                            // We're the originator and have the contract locally
                            tracing::info!(tx = %id, contract = %key, phase = "complete", "Subscribe completed (originator has contract locally)");
                            return Ok(OperationResult {
                                return_msg: None,
                                next_hop: None,
                                state: Some(OpEnum::Subscribe(SubscribeOp {
                                    id: *id,
                                    state: Some(SubscribeState::Completed { key }),
                                    requester_addr: None,
                                    is_renewal: self.is_renewal,
                                })),
                                stream_data: None,
                            });
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
                            // Register the subscribing peer in the interest manager so that
                            // update broadcasts include them as a target immediately.
                            if let Some(pkl) = op_manager
                                .ring
                                .connection_manager
                                .get_peer_by_addr(requester_addr)
                            {
                                let peer_key =
                                    crate::ring::interest::PeerKey::from(pkl.pub_key.clone());
                                op_manager
                                    .interest_manager
                                    .register_peer_interest(&key, peer_key, None, false);
                            }
                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    instance_id: *instance_id,
                                    result: SubscribeMsgResult::Subscribed { key },
                                })),
                                next_hop: Some(requester_addr),
                                state: None,
                                stream_data: None,
                            });
                        } else {
                            tracing::info!(tx = %id, contract = %key, phase = "complete", "Subscribe completed (originator, contract arrived after wait)");
                            return Ok(OperationResult {
                                return_msg: None,
                                next_hop: None,
                                state: Some(OpEnum::Subscribe(SubscribeOp {
                                    id: *id,
                                    state: Some(SubscribeState::Completed { key }),
                                    requester_addr: None,
                                    is_renewal: self.is_renewal,
                                })),
                                stream_data: None,
                            });
                        }
                    }

                    // Contract still not found - try to forward
                    if *htl == 0 {
                        tracing::warn!(tx = %id, contract = %instance_id, htl = 0, phase = "error", "Subscribe request exhausted HTL");
                        // Can't send a response without the full key - just return error
                        return Err(RingError::NoCachingPeers(*instance_id).into());
                    }

                    // Find next hop
                    let own_addr = op_manager.ring.connection_manager.peer_addr()?;
                    // Restore hash keys after deserialization (they're derived from tx)
                    let mut new_visited = visited.clone().with_transaction(id);
                    new_visited.mark_visited(own_addr);
                    if let Some(requester) = self.requester_addr {
                        new_visited.mark_visited(requester);
                    }

                    let candidates =
                        op_manager
                            .ring
                            .k_closest_potentially_caching(instance_id, &new_visited, 3);

                    let Some(next_hop) = candidates.first() else {
                        // No forward target
                        return Err(RingError::NoCachingPeers(*instance_id).into());
                    };

                    // Convert to KnownPeerKeyLocation for compile-time address guarantee
                    let next_hop_known = KnownPeerKeyLocation::try_from(next_hop)
                        .map_err(|e| {
                            tracing::error!(
                                tx = %id,
                                pub_key = %e.pub_key,
                                "INTERNAL ERROR: next_hop has unknown address - routing selected peer without address"
                            );
                            RingError::NoCachingPeers(*instance_id)
                        })?;
                    let next_addr = next_hop_known.socket_addr();
                    new_visited.mark_visited(next_addr);

                    tracing::debug!(tx = %id, %instance_id, next = %next_addr, is_renewal, "Forwarding subscribe request");

                    Ok(OperationResult {
                        return_msg: Some(NetMessage::from(SubscribeMsg::Request {
                            id: *id,
                            instance_id: *instance_id,
                            htl: htl.saturating_sub(1),
                            visited: new_visited,
                            is_renewal: *is_renewal,
                        })),
                        next_hop: Some(next_addr),
                        state: Some(OpEnum::Subscribe(SubscribeOp {
                            id: *id,
                            state: Some(SubscribeState::AwaitingResponse {
                                next_hop: None, // Already routing via next_hop in OperationResult
                            }),
                            requester_addr: self.requester_addr,
                            is_renewal: self.is_renewal,
                        })),
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

                            // In the simplified lease-based model (2026-01 refactor), we register
                            // our subscription locally. No upstream/downstream tree tracking.
                            op_manager.ring.subscribe(*key);
                            op_manager.ring.complete_subscription_request(key, true);

                            // Refresh hosting TTL on subscription success.
                            // This extends the hosting lifetime for contracts we're actively subscribed to.
                            // (Unlike UPDATE, SUBSCRIBE is user-initiated so safe to extend TTL)
                            op_manager.ring.touch_hosting(key);

                            tracing::info!(
                                tx = %msg_id,
                                contract = %format!("{:.8}", key),
                                "SUBSCRIPTION_ACCEPTED: registered lease-based subscription"
                            );

                            // Fetch contract if we don't have it.
                            // This is non-fatal - if it fails, we still continue with forwarding/completing
                            // the subscription. The contract will eventually arrive via UPDATE broadcasts.
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
                            // See: https://github.com/freenet/freenet-core/issues/XXX
                            super::announce_contract_cached(op_manager, key).await;

                            // Forward response to requester or complete
                            if let Some(requester_addr) = self.requester_addr {
                                // We're an intermediate node - forward response to the requester
                                // State is NOT sent here - requester gets state via GET, not SUBSCRIBE
                                tracing::debug!(tx = %msg_id, %key, requester = %requester_addr, "Forwarding Subscribed response to requester");
                                Ok(OperationResult {
                                    return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                        id: *msg_id,
                                        instance_id: *instance_id,
                                        result: SubscribeMsgResult::Subscribed { key: *key },
                                    })),
                                    next_hop: Some(requester_addr),
                                    state: None,
                                    stream_data: None,
                                })
                            } else {
                                // We're the originator - return completed state for handle_op_result
                                tracing::info!(tx = %msg_id, contract = %key, phase = "complete", "Subscribe completed (originator)");

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

                                Ok(OperationResult {
                                    return_msg: None,
                                    next_hop: None,
                                    state: Some(OpEnum::Subscribe(SubscribeOp {
                                        id,
                                        state: Some(SubscribeState::Completed { key: *key }),
                                        requester_addr: None,
                                        is_renewal: self.is_renewal,
                                    })),
                                    stream_data: None,
                                })
                            }
                        }
                        SubscribeMsgResult::NotFound => {
                            tracing::debug!(
                                tx = %msg_id,
                                %instance_id,
                                requester_addr = ?self.requester_addr,
                                source_addr = ?source_addr,
                                "subscribe: processing NotFound response"
                            );

                            // Forward NotFound response to requester or complete with failure
                            if let Some(requester_addr) = self.requester_addr {
                                // We're an intermediate node - forward NotFound to requester
                                tracing::debug!(tx = %msg_id, %instance_id, requester = %requester_addr, "Forwarding NotFound response to requester");
                                Ok(OperationResult {
                                    return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                        id: *msg_id,
                                        instance_id: *instance_id,
                                        result: SubscribeMsgResult::NotFound,
                                    })),
                                    next_hop: Some(requester_addr),
                                    state: None,
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
                                        "Subscribe: Network returned NotFound, re-seeding with local cache"
                                    );

                                    // Re-seed the network with our local copy
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
                                                tracing::debug!(tx = %msg_id, %key, "Re-seeded contract to network");
                                                super::announce_contract_cached(op_manager, &key)
                                                    .await;
                                            }
                                            _ => {
                                                tracing::warn!(tx = %msg_id, %key, "Failed to re-seed contract");
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

                                    Ok(OperationResult {
                                        return_msg: None,
                                        next_hop: None,
                                        state: Some(OpEnum::Subscribe(SubscribeOp {
                                            id,
                                            state: Some(SubscribeState::Completed { key }),
                                            requester_addr: None,
                                            is_renewal: self.is_renewal,
                                        })),
                                        stream_data: None,
                                    })
                                } else {
                                    // No local cache - subscription failed
                                    tracing::warn!(tx = %msg_id, %instance_id, phase = "not_found", "Subscribe failed - contract not found");

                                    // Emit telemetry for subscription not found
                                    if let Some(event) = NetEventLog::subscribe_not_found(
                                        msg_id,
                                        &op_manager.ring,
                                        *instance_id,
                                        None, // hop_count not tracked in subscribe
                                    ) {
                                        op_manager.ring.register_events(Either::Left(event)).await;
                                    }

                                    // Return op with no inner state - to_host_result() will return error
                                    Ok(OperationResult {
                                        return_msg: None,
                                        next_hop: None,
                                        state: Some(OpEnum::Subscribe(SubscribeOp {
                                            id,
                                            state: None,
                                            requester_addr: None,
                                            is_renewal: self.is_renewal,
                                        })),
                                        stream_data: None,
                                    })
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}

impl IsOperationCompleted for SubscribeOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(SubscribeState::Completed { .. }))
    }
}

#[cfg(test)]
mod tests;

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
    }

    impl InnerMessage for SubscribeMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. } | Self::Response { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { instance_id, .. } | Self::Response { instance_id, .. } => {
                    Some(Location::from(instance_id))
                }
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
            }
        }
    }
}
