use std::future::Future;
use std::pin::Pin;

pub(crate) use self::messages::{SubscribeMsg, SubscribeMsgResult};
use super::{get, OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::node::IsOperationCompleted;
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
    tokio::select! {
        _ = notifier => {}
        _ = sleep(Duration::from_millis(timeout_ms)) => {}
    };

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

pub(crate) fn start_op(instance_id: ContractInstanceId) -> SubscribeOp {
    let id = Transaction::new::<SubscribeMsg>();
    let state = Some(SubscribeState::PrepareRequest { id, instance_id });
    SubscribeOp {
        id,
        state,
        requester_addr: None, // Local operation, we are the originator
    }
}

/// Create a Subscribe operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(instance_id: ContractInstanceId, id: Transaction) -> SubscribeOp {
    let state = Some(SubscribeState::PrepareRequest { id, instance_id });
    SubscribeOp {
        id,
        state,
        requester_addr: None, // Local operation, we are the originator
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
    let Some(SubscribeState::PrepareRequest { id, instance_id }) = &sub_op.state else {
        return Err(OpError::UnexpectedOpState);
    };

    tracing::debug!(tx = %id, contract = %instance_id, "subscribe: request_subscribe invoked");

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
        let fallback_target = connections
            .values()
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
    };

    let op = SubscribeOp {
        id: *id,
        state: Some(SubscribeState::AwaitingResponse {
            next_hop: Some(target_addr),
        }),
        requester_addr: None, // We're the originator
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

        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Subscribe(subscribe_op))) => {
                // Existing operation - response from downstream peer
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
                // New request from another peer - we're an intermediate/terminal node
                Ok(OpInitialization {
                    op: Self {
                        id,
                        state: Some(SubscribeState::AwaitingResponse {
                            next_hop: None, // Will be determined during processing
                        }),
                        requester_addr: source_addr, // Store who sent us this request
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
                } => {
                    tracing::debug!(
                        tx = %id,
                        %instance_id,
                        htl,
                        requester_addr = ?self.requester_addr,
                        "subscribe: processing Request"
                    );

                    // Check if we have the contract
                    if let Some(key) = super::has_contract(op_manager, *instance_id).await? {
                        // We have the contract - register upstream as subscriber and respond
                        if let Some(requester_addr) = self.requester_addr {
                            // Register the upstream peer as downstream subscriber (they want updates FROM us)
                            if let Some(upstream_peer) = op_manager
                                .ring
                                .connection_manager
                                .get_peer_location_by_addr(requester_addr)
                            {
                                let _ = op_manager.ring.add_downstream(
                                    &key,
                                    upstream_peer,
                                    Some(requester_addr.into()),
                                );
                            }

                            tracing::info!(tx = %id, contract = %key, phase = "response", "Subscription fulfilled, sending Response");
                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    instance_id: *instance_id,
                                    result: SubscribeMsgResult::Subscribed { key },
                                })),
                                next_hop: Some(requester_addr),
                                state: None,
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
                                })),
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
                        // Contract arrived - handle same as above
                        if let Some(requester_addr) = self.requester_addr {
                            if let Some(upstream_peer) = op_manager
                                .ring
                                .connection_manager
                                .get_peer_location_by_addr(requester_addr)
                            {
                                let _ = op_manager.ring.add_downstream(
                                    &key,
                                    upstream_peer,
                                    Some(requester_addr.into()),
                                );
                            }

                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    instance_id: *instance_id,
                                    result: SubscribeMsgResult::Subscribed { key },
                                })),
                                next_hop: Some(requester_addr),
                                state: None,
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
                                })),
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

                    tracing::debug!(tx = %id, %instance_id, next = %next_addr, "Forwarding subscribe request");

                    Ok(OperationResult {
                        return_msg: Some(NetMessage::from(SubscribeMsg::Request {
                            id: *id,
                            instance_id: *instance_id,
                            htl: htl.saturating_sub(1),
                            visited: new_visited,
                        })),
                        next_hop: Some(next_addr),
                        state: Some(OpEnum::Subscribe(SubscribeOp {
                            id: *id,
                            state: Some(SubscribeState::AwaitingResponse {
                                next_hop: None, // Already routing via next_hop in OperationResult
                            }),
                            requester_addr: self.requester_addr,
                        })),
                    })
                }

                SubscribeMsg::Response {
                    id: msg_id,
                    instance_id,
                    result,
                } => {
                    match result {
                        SubscribeMsgResult::Subscribed { key } => {
                            tracing::debug!(
                                tx = %msg_id,
                                %key,
                                requester_addr = ?self.requester_addr,
                                source_addr = ?source_addr,
                                "subscribe: processing Subscribed response"
                            );

                            // Fetch contract if we don't have it
                            fetch_contract_if_missing(op_manager, *key.id()).await?;

                            // Register the sender as our upstream source
                            if let Some(sender_addr) = source_addr {
                                if let Some(sender_peer) = op_manager
                                    .ring
                                    .connection_manager
                                    .get_peer_location_by_addr(sender_addr)
                                {
                                    op_manager.ring.set_upstream(key, sender_peer.clone());
                                    tracing::debug!(
                                        tx = %msg_id,
                                        %key,
                                        upstream = %sender_addr,
                                        "subscribe: registered upstream source"
                                    );
                                }
                            }

                            // Forward response to requester or complete
                            if let Some(requester_addr) = self.requester_addr {
                                // We're an intermediate node - forward response to the requester
                                // Register them as downstream (they want updates from us)
                                if let Some(downstream_peer) = op_manager
                                    .ring
                                    .connection_manager
                                    .get_peer_location_by_addr(requester_addr)
                                {
                                    let _ = op_manager.ring.add_downstream(
                                        key,
                                        downstream_peer,
                                        Some(requester_addr.into()),
                                    );
                                    tracing::debug!(
                                        tx = %msg_id,
                                        %key,
                                        downstream = %requester_addr,
                                        "subscribe: registered requester as downstream subscriber"
                                    );
                                }

                                tracing::debug!(tx = %msg_id, %key, requester = %requester_addr, "Forwarding Subscribed response to requester");
                                Ok(OperationResult {
                                    return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                        id: *msg_id,
                                        instance_id: *instance_id,
                                        result: SubscribeMsgResult::Subscribed { key: *key },
                                    })),
                                    next_hop: Some(requester_addr),
                                    state: Some(OpEnum::Subscribe(SubscribeOp {
                                        id,
                                        state: Some(SubscribeState::Completed { key: *key }),
                                        requester_addr: None,
                                    })),
                                })
                            } else {
                                // We're the originator - return completed state for handle_op_result
                                tracing::info!(tx = %msg_id, contract = %key, phase = "complete", "Subscribe completed (originator)");
                                Ok(OperationResult {
                                    return_msg: None,
                                    next_hop: None,
                                    state: Some(OpEnum::Subscribe(SubscribeOp {
                                        id,
                                        state: Some(SubscribeState::Completed { key: *key }),
                                        requester_addr: None,
                                    })),
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
                                })
                            } else {
                                // We're the originator - subscription failed, contract not found
                                tracing::warn!(tx = %msg_id, %instance_id, phase = "not_found", "Subscribe failed - contract not found");
                                // Return op with no inner state - to_host_result() will return error
                                Ok(OperationResult {
                                    return_msg: None,
                                    next_hop: None,
                                    state: Some(OpEnum::Subscribe(SubscribeOp {
                                        id,
                                        state: None,
                                        requester_addr: None,
                                    })),
                                })
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
