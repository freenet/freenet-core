use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

pub(crate) use self::messages::SubscribeMsg;
use super::{get, OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::node::IsOperationCompleted;
use crate::{
    client_events::HostResult,
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager},
    ring::{Location, RingError},
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
    key: ContractKey,
    timeout_ms: u64,
) -> Result<bool, OpError> {
    // Fast path - contract already exists
    if super::has_contract(op_manager, key).await? {
        return Ok(true);
    }

    // Register waiter BEFORE second check to avoid race condition
    let notifier = op_manager.wait_for_contract(key);

    // Check again - contract may have arrived between first check and registration
    if super::has_contract(op_manager, key).await? {
        return Ok(true);
    }

    // Wait for notification or timeout (we don't care which triggers first)
    tokio::select! {
        _ = notifier => {}
        _ = sleep(Duration::from_millis(timeout_ms)) => {}
    };

    // Always verify actual state - don't trust notification alone
    super::has_contract(op_manager, key).await
}

async fn fetch_contract_if_missing(
    op_manager: &OpManager,
    key: ContractKey,
) -> Result<(), OpError> {
    if super::has_contract(op_manager, key).await? {
        return Ok(());
    }

    // Start a GET operation to fetch the contract
    let get_op = get::start_op(key, true, false);
    get::request_get(op_manager, get_op, HashSet::new()).await?;

    // Wait for contract to arrive
    wait_for_contract_with_timeout(op_manager, key, CONTRACT_WAIT_TIMEOUT_MS).await?;

    Ok(())
}
#[derive(Debug)]
enum SubscribeState {
    /// Prepare the request to subscribe.
    PrepareRequest { id: Transaction, key: ContractKey },
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

pub(crate) fn start_op(key: ContractKey) -> SubscribeOp {
    let id = Transaction::new::<SubscribeMsg>();
    let state = Some(SubscribeState::PrepareRequest { id, key });
    SubscribeOp {
        id,
        state,
        requester_addr: None, // Local operation, we are the originator
    }
}

/// Create a Subscribe operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(key: ContractKey, id: Transaction) -> SubscribeOp {
    let state = Some(SubscribeState::PrepareRequest { id, key });
    SubscribeOp {
        id,
        state,
        requester_addr: None, // Local operation, we are the originator
    }
}

/// Request to subscribe to value changes from a contract.
pub(crate) async fn request_subscribe(
    op_manager: &OpManager,
    sub_op: SubscribeOp,
) -> Result<(), OpError> {
    let Some(SubscribeState::PrepareRequest { id, key }) = &sub_op.state else {
        return Err(OpError::UnexpectedOpState);
    };

    let own_loc = op_manager.ring.connection_manager.own_location();
    let own_addr = own_loc
        .socket_addr()
        .expect("own location must have socket address");

    tracing::debug!(tx = %id, %key, "subscribe: request_subscribe invoked");

    // Check if we already have the contract locally
    let has_contract_locally = super::has_contract(op_manager, *key).await?;

    // IMPORTANT: Even if we have the contract locally (e.g., from PUT forwarding),
    // we MUST send a Subscribe::Request to the network to register ourselves as a
    // downstream subscriber in the subscription tree. Otherwise, when the contract
    // is updated at the source, we won't receive the update because we're not
    // registered in the upstream peer's subscriber list.
    //
    // The local subscription completion happens when we receive the Response back.
    // This ensures proper subscription tree management for update propagation.

    // Find a peer to forward the request to (needed even if we have contract locally)
    let mut skip_list: HashSet<std::net::SocketAddr> = HashSet::new();
    skip_list.insert(own_addr);

    let candidates = op_manager
        .ring
        .k_closest_potentially_caching(key, &skip_list, 3);

    let Some(target) = candidates.first() else {
        // No remote peers available - if we have contract locally, we can complete subscription locally
        // This handles the case of a standalone node or when we're the only node with the contract
        if has_contract_locally {
            tracing::info!(tx = %id, contract = %key, phase = "complete", "Contract available locally and no remote peers, completing subscription locally");
            return complete_local_subscription(op_manager, *id, *key).await;
        }
        tracing::warn!(tx = %id, contract = %key, phase = "error", "No remote peers available for subscription");
        return Err(RingError::NoCachingPeers(*key).into());
    };

    let target_addr = target
        .socket_addr()
        .expect("target must have socket address");
    skip_list.insert(target_addr);

    tracing::debug!(
        tx = %id,
        %key,
        target_peer = %target_addr,
        "subscribe: forwarding Request to target peer"
    );

    let msg = SubscribeMsg::Request {
        id: *id,
        key: *key,
        htl: op_manager.ring.max_hops_to_live,
        skip_list,
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
                    key,
                    htl,
                    skip_list,
                } => {
                    tracing::debug!(
                        tx = %id,
                        %key,
                        htl,
                        requester_addr = ?self.requester_addr,
                        "subscribe: processing Request"
                    );

                    // Check if we have the contract
                    if super::has_contract(op_manager, *key).await? {
                        // We have the contract - register upstream as subscriber and respond
                        if let Some(requester_addr) = self.requester_addr {
                            // Register the upstream peer as downstream subscriber (they want updates FROM us)
                            if let Some(upstream_peer) = op_manager
                                .ring
                                .connection_manager
                                .get_peer_location_by_addr(requester_addr)
                            {
                                let _ = op_manager.ring.add_downstream(
                                    key,
                                    upstream_peer,
                                    Some(requester_addr.into()),
                                );
                            }

                            tracing::info!(tx = %id, contract = %key, phase = "response", "Subscription fulfilled, sending Response");
                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    key: *key,
                                    subscribed: true,
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
                                    state: Some(SubscribeState::Completed { key: *key }),
                                    requester_addr: None,
                                })),
                            });
                        }
                    }

                    // Contract not found - wait briefly for in-flight PUT
                    if wait_for_contract_with_timeout(op_manager, *key, CONTRACT_WAIT_TIMEOUT_MS)
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
                                    key,
                                    upstream_peer,
                                    Some(requester_addr.into()),
                                );
                            }

                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    key: *key,
                                    subscribed: true,
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
                                    state: Some(SubscribeState::Completed { key: *key }),
                                    requester_addr: None,
                                })),
                            });
                        }
                    }

                    // Contract still not found - try to forward
                    if *htl == 0 {
                        tracing::warn!(tx = %id, contract = %key, htl = 0, phase = "error", "Subscribe request exhausted HTL");
                        if let Some(requester_addr) = self.requester_addr {
                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    key: *key,
                                    subscribed: false,
                                })),
                                next_hop: Some(requester_addr),
                                state: None,
                            });
                        }
                        return Err(RingError::NoCachingPeers(*key).into());
                    }

                    // Find next hop
                    let own_addr = op_manager
                        .ring
                        .connection_manager
                        .own_location()
                        .socket_addr()
                        .expect("own address");
                    let mut new_skip_list = skip_list.clone();
                    new_skip_list.insert(own_addr);
                    if let Some(requester) = self.requester_addr {
                        new_skip_list.insert(requester);
                    }

                    let candidates =
                        op_manager
                            .ring
                            .k_closest_potentially_caching(key, &new_skip_list, 3);

                    let Some(next_hop) = candidates.first() else {
                        // No forward target
                        if let Some(requester_addr) = self.requester_addr {
                            return Ok(OperationResult {
                                return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                    id: *id,
                                    key: *key,
                                    subscribed: false,
                                })),
                                next_hop: Some(requester_addr),
                                state: None,
                            });
                        }
                        return Err(RingError::NoCachingPeers(*key).into());
                    };

                    let next_addr = next_hop.socket_addr().expect("next hop address");
                    new_skip_list.insert(next_addr);

                    tracing::debug!(tx = %id, %key, next = %next_addr, "Forwarding subscribe request");

                    Ok(OperationResult {
                        return_msg: Some(NetMessage::from(SubscribeMsg::Request {
                            id: *id,
                            key: *key,
                            htl: htl.saturating_sub(1),
                            skip_list: new_skip_list,
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
                    key,
                    subscribed,
                } => {
                    tracing::debug!(
                        tx = %msg_id,
                        %key,
                        subscribed,
                        requester_addr = ?self.requester_addr,
                        source_addr = ?source_addr,
                        "subscribe: processing Response"
                    );

                    if *subscribed {
                        // Fetch contract if we don't have it
                        fetch_contract_if_missing(op_manager, *key).await?;

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
                    }

                    // Forward response to requester or complete
                    if let Some(requester_addr) = self.requester_addr {
                        // We're an intermediate node - forward response to the requester
                        // Register them as downstream (they want updates from us)
                        if *subscribed {
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
                        }

                        tracing::debug!(tx = %msg_id, %key, requester = %requester_addr, "Forwarding response to requester");
                        Ok(OperationResult {
                            return_msg: Some(NetMessage::from(SubscribeMsg::Response {
                                id: *msg_id,
                                key: *key,
                                subscribed: *subscribed,
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
                        if *subscribed {
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
                        } else {
                            tracing::warn!(tx = %msg_id, contract = %key, phase = "failed", "Subscribe failed (originator)");
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

    /// Subscribe operation messages.
    ///
    /// Uses hop-by-hop routing: each node stores `requester_addr` from the transport layer
    /// to route responses back. No `PeerKeyLocation` is embedded in wire messages.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum SubscribeMsg {
        /// Request to subscribe to a contract. Forwarded hop-by-hop toward contract location.
        Request {
            id: Transaction,
            key: ContractKey,
            /// Hops to live - decremented at each hop
            htl: usize,
            /// Addresses to skip when selecting next hop (prevents loops)
            skip_list: HashSet<std::net::SocketAddr>,
        },
        /// Response indicating subscription result. Routed hop-by-hop back to originator.
        Response {
            id: Transaction,
            key: ContractKey,
            subscribed: bool,
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
                Self::Request { key, .. } | Self::Response { key, .. } => {
                    Some(Location::from(key.id()))
                }
            }
        }
    }

    impl Display for SubscribeMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::Request { key, .. } => write!(f, "Subscribe::Request(id: {id}, key: {key})"),
                Self::Response {
                    key, subscribed, ..
                } => {
                    write!(
                        f,
                        "Subscribe::Response(id: {id}, key: {key}, subscribed: {subscribed})"
                    )
                }
            }
        }
    }
}
