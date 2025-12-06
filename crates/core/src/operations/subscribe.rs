use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

pub(crate) use self::messages::SubscribeMsg;
use super::{get, OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::node::IsOperationCompleted;
use crate::{
    client_events::HostResult,
    contract::{ContractHandlerEvent, StoreResponse},
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager},
    ring::{Location, PeerKeyLocation, RingError},
};
use freenet_stdlib::{
    client_api::{ContractResponse, ErrorKind, HostResponse},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

const MAX_RETRIES: usize = 10;
const LOCAL_FETCH_TIMEOUT_MS: u64 = 1_500;
const LOCAL_FETCH_POLL_INTERVAL_MS: u64 = 25;
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

fn subscribers_snapshot(op_manager: &OpManager, key: &ContractKey) -> Vec<String> {
    op_manager
        .ring
        .subscribers_of(key)
        .map(|subs| {
            subs.iter()
                .filter_map(|loc| loc.socket_addr())
                .map(|addr| format!("{:.8}", addr))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// Poll local storage for a short period until the fetched contract becomes available.
async fn wait_for_local_contract(
    op_manager: &OpManager,
    key: ContractKey,
) -> Result<bool, OpError> {
    let mut elapsed = 0;
    while elapsed < LOCAL_FETCH_TIMEOUT_MS {
        if super::has_contract(op_manager, key).await? {
            return Ok(true);
        }
        sleep(Duration::from_millis(LOCAL_FETCH_POLL_INTERVAL_MS)).await;
        elapsed += LOCAL_FETCH_POLL_INTERVAL_MS;
    }
    Ok(false)
}

async fn fetch_contract_if_missing(
    op_manager: &OpManager,
    key: ContractKey,
) -> Result<(), OpError> {
    if has_contract_with_code(op_manager, key).await? {
        return Ok(());
    }

    let get_op = get::start_op(key, true, false);
    get::request_get(op_manager, get_op, HashSet::new()).await?;

    if wait_for_local_contract(op_manager, key).await?
        && has_contract_with_code(op_manager, key).await?
    {
        Ok(())
    } else {
        Err(RingError::NoCachingPeers(key).into())
    }
}

async fn has_contract_with_code(op_manager: &OpManager, key: ContractKey) -> Result<bool, OpError> {
    match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            key,
            return_contract_code: true,
        })
        .await?
    {
        ContractHandlerEvent::GetResponse {
            response:
                Ok(StoreResponse {
                    state: Some(_),
                    contract: Some(_),
                }),
            ..
        } => Ok(true),
        _ => Ok(false),
    }
}
#[derive(Debug)]
enum SubscribeState {
    /// Prepare the request to subscribe.
    PrepareRequest {
        id: Transaction,
        key: ContractKey,
    },
    /// Received a request to subscribe to this network.
    ReceivedRequest,
    /// Awaitinh response from petition.
    AwaitingResponse {
        skip_list: HashSet<std::net::SocketAddr>,
        retries: usize,
        upstream_subscriber: Option<PeerKeyLocation>,
        current_hop: usize,
    },
    Completed {
        key: ContractKey,
    },
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
        upstream_addr: None, // Local operation, no upstream peer
    }
}

/// Create a Subscribe operation with a specific transaction ID (for operation deduplication)
pub(crate) fn start_op_with_id(key: ContractKey, id: Transaction) -> SubscribeOp {
    let state = Some(SubscribeState::PrepareRequest { id, key });
    SubscribeOp {
        id,
        state,
        upstream_addr: None, // Local operation, no upstream peer
    }
}

/// Request to subscribe to value changes from a contract.
pub(crate) async fn request_subscribe(
    op_manager: &OpManager,
    sub_op: SubscribeOp,
) -> Result<(), OpError> {
    if let Some(SubscribeState::PrepareRequest { id, key }) = &sub_op.state {
        let own_loc = op_manager.ring.connection_manager.own_location();
        let local_has_contract = super::has_contract(op_manager, *key).await?;

        let own_addr = own_loc
            .socket_addr()
            .expect("own location must have socket address");
        tracing::debug!(
            tx = %id,
            %key,
            subscriber_peer = %own_addr,
            local_has_contract,
            "subscribe: request_subscribe invoked"
        );

        let mut skip_list: HashSet<std::net::SocketAddr> = HashSet::new();
        skip_list.insert(own_addr);

        // Use k_closest_potentially_caching to try multiple candidates
        // Try up to 3 candidates
        let candidates = op_manager
            .ring
            .k_closest_potentially_caching(key, &skip_list, 3);

        if tracing::enabled!(tracing::Level::INFO) {
            let skip_display: Vec<String> = skip_list
                .iter()
                .map(|addr| format!("{:.8}", addr))
                .collect();
            let candidate_display: Vec<String> = candidates
                .iter()
                .filter_map(|cand| cand.socket_addr())
                .map(|addr| format!("{:.8}", addr))
                .collect();
            tracing::info!(
                tx = %id,
                %key,
                skip = ?skip_display,
                candidates = ?candidate_display,
                "subscribe: k_closest_potentially_caching results"
            );
        }

        let target = match candidates.first() {
            Some(peer) => peer.clone(),
            None => {
                // No remote peers available - rely on local contract if present.
                tracing::debug!(
                    %key,
                    "No remote peers available for subscription, checking locally"
                );

                if local_has_contract {
                    tracing::info!(
                        %key,
                        tx = %id,
                        "No remote peers, fulfilling subscription locally"
                    );
                    return complete_local_subscription(op_manager, *id, *key).await;
                } else {
                    let connection_count = op_manager.ring.connection_manager.num_connections();
                    let subscribers = op_manager
                        .ring
                        .subscribers_of(key)
                        .map(|subs| {
                            subs.value()
                                .iter()
                                .filter_map(|loc| loc.socket_addr())
                                .map(|addr| format!("{:.8}", addr))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    tracing::warn!(
                        %key,
                        tx = %id,
                        connection_count,
                        subscribers = ?subscribers,
                        "Contract not available locally and no remote peers"
                    );
                    return Err(RingError::NoCachingPeers(*key).into());
                }
            }
        };

        // Forward to remote peer
        let new_state = Some(SubscribeState::AwaitingResponse {
            skip_list,
            retries: 0,
            current_hop: op_manager.ring.max_hops_to_live,
            upstream_subscriber: None,
        });
        let target_addr = target
            .socket_addr()
            .expect("target must have socket address");
        tracing::debug!(
            tx = %id,
            %key,
            target_peer = %target_addr,
            target_location = ?target.location(),
            "subscribe: forwarding RequestSub to target peer"
        );
        // Create subscriber with PeerAddr::Unknown - the subscriber doesn't know their own
        // external address (especially behind NAT). The first recipient (gateway)
        // will fill this in from the packet source address.
        let subscriber = PeerKeyLocation::with_unknown_addr(own_loc.pub_key().clone());
        let msg = SubscribeMsg::RequestSub {
            id: *id,
            key: *key,
            target,
            subscriber,
        };
        let op = SubscribeOp {
            id: *id,
            state: new_state,
            upstream_addr: sub_op.upstream_addr,
        };
        op_manager
            .notify_op_change(NetMessage::from(msg), OpEnum::Subscribe(op))
            .await?;
    } else {
        return Err(OpError::UnexpectedOpState);
    }

    Ok(())
}

/// Complete a local subscription by notifying the client layer.
///
/// **Architecture Note (Issue #2075):**
/// Local client subscriptions are deliberately kept separate from network subscriptions:
/// - **Network subscriptions** are stored in `ring.seeding_manager.subscribers` and are used
///   for peer-to-peer UPDATE propagation between nodes
/// - **Local subscriptions** are managed by the contract executor via `update_notifications`
///   channels, which deliver `UpdateNotification` directly to WebSocket clients
///
/// This separation eliminates the need for workarounds like the previous `allow_self` hack
/// in `get_broadcast_targets_update()`, and ensures clean architectural boundaries between
/// the network layer (ops/) and the client layer (client_events/).
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
    /// The address we received this operation's message from.
    /// Used for connection-based routing: responses are sent back to this address.
    upstream_addr: Option<std::net::SocketAddr>,
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
                // was an existing operation, the other peer messaged back
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
                // new request to subscribe to a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(SubscribeState::ReceivedRequest),
                        id,
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
            // Look up sender's PeerKeyLocation from source address for logging/routing
            // This replaces the sender field that was previously embedded in messages
            let sender_from_addr = source_addr.and_then(|addr| {
                op_manager
                    .ring
                    .connection_manager
                    .get_peer_location_by_addr(addr)
            });

            let return_msg;
            let new_state;

            match input {
                SubscribeMsg::RequestSub {
                    id,
                    key,
                    target: _,
                    subscriber,
                } => {
                    // Fill in subscriber's external address from transport layer if unknown.
                    // This is the key step where the first recipient (gateway) determines the
                    // subscriber's external address from the actual packet source address.
                    // IMPORTANT: Must fill address BEFORE any .peer() calls to avoid panic.
                    let mut subscriber = subscriber.clone();

                    if subscriber.peer_addr.is_unknown() {
                        if let Some(addr) = source_addr {
                            subscriber.set_addr(addr);
                            tracing::debug!(
                                tx = %id,
                                %key,
                                subscriber_addr = %addr,
                                "subscribe: filled subscriber address from source_addr"
                            );
                        }
                    }

                    let subscriber_addr = subscriber
                        .socket_addr()
                        .expect("subscriber must have socket address after filling");
                    tracing::debug!(
                        tx = %id,
                        %key,
                        subscriber = %subscriber_addr,
                        source_addr = ?source_addr,
                        "subscribe: processing RequestSub"
                    );
                    let own_loc = op_manager.ring.connection_manager.own_location();

                    if !matches!(
                        self.state,
                        Some(SubscribeState::AwaitingResponse { .. })
                            | Some(SubscribeState::ReceivedRequest)
                    ) {
                        tracing::warn!(
                            tx = %id,
                            %key,
                            state = ?self.state,
                            "subscribe: RequestSub received in unexpected state"
                        );
                        return Err(OpError::invalid_transition(self.id));
                    }

                    if super::has_contract(op_manager, *key).await? {
                        let before_direct = subscribers_snapshot(op_manager, key);
                        tracing::info!(
                            tx = %id,
                            %key,
                            subscriber = %subscriber_addr,
                            subscribers_before = ?before_direct,
                            "subscribe: handling RequestSub locally (contract available)"
                        );

                        // Local registration - no upstream NAT address
                        if op_manager
                            .ring
                            .add_subscriber(key, subscriber.clone(), None)
                            .is_err()
                        {
                            tracing::warn!(
                                tx = %id,
                                %key,
                                subscriber = %subscriber_addr,
                                subscribers_before = ?before_direct,
                                "subscribe: direct registration failed (max subscribers reached)"
                            );
                            let return_msg = SubscribeMsg::ReturnSub {
                                id: *id,
                                key: *key,
                                target: subscriber.clone(),
                                subscribed: false,
                            };
                            return Ok(OperationResult {
                                target_addr: return_msg.target_addr(),
                                return_msg: Some(NetMessage::from(return_msg)),
                                state: None,
                            });
                        }

                        let after_direct = subscribers_snapshot(op_manager, key);
                        tracing::info!(
                            tx = %id,
                            %key,
                            subscriber = %subscriber_addr,
                            subscribers_after = ?after_direct,
                            "subscribe: registered direct subscriber (RequestSub)"
                        );

                        let own_addr = own_loc
                            .socket_addr()
                            .expect("own location must have socket address");
                        if subscriber_addr == own_addr {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "RequestSub originated locally; sending LocalSubscribeComplete"
                            );
                            if let Err(err) = op_manager
                                .notify_node_event(
                                    crate::message::NodeEvent::LocalSubscribeComplete {
                                        tx: *id,
                                        key: *key,
                                        subscribed: true,
                                    },
                                )
                                .await
                            {
                                tracing::error!(
                                    tx = %id,
                                    %key,
                                    error = %err,
                                    "Failed to send LocalSubscribeComplete event for RequestSub"
                                );
                                return Err(err);
                            }

                            return build_op_result(self.id, None, None, self.upstream_addr);
                        }

                        let return_msg = SubscribeMsg::ReturnSub {
                            id: *id,
                            key: *key,
                            target: subscriber.clone(),
                            subscribed: true,
                        };

                        return build_op_result(
                            self.id,
                            None,
                            Some(return_msg),
                            self.upstream_addr,
                        );
                    }

                    // Contract not found locally. Wait briefly in case a PUT is in flight.
                    tracing::debug!(
                        tx = %id,
                        %key,
                        "subscribe: contract not found, waiting for possible in-flight PUT"
                    );

                    // Wait for contract with timeout (handles race conditions internally)
                    if wait_for_contract_with_timeout(op_manager, *key, CONTRACT_WAIT_TIMEOUT_MS)
                        .await?
                    {
                        tracing::info!(
                            tx = %id,
                            %key,
                            "subscribe: contract arrived, handling locally"
                        );

                        // Contract exists, register subscription
                        if op_manager
                            .ring
                            .add_subscriber(key, subscriber.clone(), None)
                            .is_err()
                        {
                            let return_msg = SubscribeMsg::ReturnSub {
                                id: *id,
                                key: *key,
                                target: subscriber.clone(),
                                subscribed: false,
                            };
                            return Ok(OperationResult {
                                target_addr: return_msg.target_addr(),
                                return_msg: Some(NetMessage::from(return_msg)),
                                state: None,
                            });
                        }

                        let return_msg = SubscribeMsg::ReturnSub {
                            id: *id,
                            key: *key,
                            target: subscriber.clone(),
                            subscribed: true,
                        };
                        return build_op_result(
                            self.id,
                            None,
                            Some(return_msg),
                            self.upstream_addr,
                        );
                    }

                    // Contract still not found after waiting, try to forward
                    tracing::debug!(
                        tx = %id,
                        %key,
                        "subscribe: contract not found after waiting, attempting to forward"
                    );

                    let own_addr = own_loc
                        .socket_addr()
                        .expect("own location must have socket address");
                    let mut skip = HashSet::new();
                    skip.insert(subscriber_addr);
                    skip.insert(own_addr);

                    let forward_target = op_manager
                        .ring
                        .k_closest_potentially_caching(key, &skip, 3)
                        .into_iter()
                        .find(|candidate| {
                            candidate
                                .socket_addr()
                                .map(|addr| addr != own_addr)
                                .unwrap_or(false)
                        });

                    // If no forward target available, send ReturnSub(subscribed: false) back
                    // This allows the subscriber to complete locally if they have the contract
                    let forward_target = match forward_target {
                        Some(target) => target,
                        None => {
                            tracing::warn!(
                                tx = %id,
                                %key,
                                "subscribe: no forward target available, returning unsubscribed"
                            );
                            let return_msg = SubscribeMsg::ReturnSub {
                                id: *id,
                                key: *key,
                                target: subscriber.clone(),
                                subscribed: false,
                            };
                            return Ok(OperationResult {
                                target_addr: return_msg.target_addr(),
                                return_msg: Some(NetMessage::from(return_msg)),
                                state: None,
                            });
                        }
                    };

                    let forward_target_addr = forward_target
                        .socket_addr()
                        .expect("forward target must have socket address");
                    skip.insert(forward_target_addr);

                    new_state = self.state;
                    return_msg = Some(SubscribeMsg::SeekNode {
                        id: *id,
                        key: *key,
                        target: forward_target,
                        subscriber: subscriber.clone(),
                        skip_list: skip.clone(),
                        htl: op_manager.ring.max_hops_to_live.max(1),
                        retries: 0,
                    });
                }
                SubscribeMsg::SeekNode {
                    key,
                    id,
                    subscriber,
                    target,
                    skip_list,
                    htl,
                    retries,
                } => {
                    // Fill in subscriber's external address from transport layer if unknown.
                    // This is the key step where the recipient determines the subscriber's
                    // external address from the actual packet source address.
                    let mut subscriber = subscriber.clone();
                    if subscriber.peer_addr.is_unknown() {
                        if let Some(addr) = source_addr {
                            subscriber.set_addr(addr);
                            tracing::debug!(
                                tx = %id,
                                %key,
                                subscriber_addr = %addr,
                                "subscribe: filled SeekNode subscriber address from source_addr"
                            );
                        }
                    }

                    let ring_max_htl = op_manager.ring.max_hops_to_live.max(1);
                    let htl = (*htl).min(ring_max_htl);
                    let this_peer = op_manager.ring.connection_manager.own_location();
                    let return_not_subbed = || -> OperationResult {
                        let return_msg = SubscribeMsg::ReturnSub {
                            key: *key,
                            id: *id,
                            subscribed: false,
                            target: subscriber.clone(),
                        };
                        OperationResult {
                            target_addr: return_msg.target_addr(),
                            return_msg: Some(NetMessage::from(return_msg)),
                            state: None,
                        }
                    };

                    if htl == 0 {
                        let subscriber_addr = subscriber
                            .socket_addr()
                            .expect("subscriber must have socket address");
                        tracing::warn!(
                            tx = %id,
                            %key,
                            subscriber = %subscriber_addr,
                            "Dropping Subscribe SeekNode with zero HTL"
                        );
                        return Ok(return_not_subbed());
                    }

                    if !super::has_contract(op_manager, *key).await? {
                        tracing::debug!(tx = %id, %key, "Contract not found, trying other peer");

                        // Use k_closest_potentially_caching to try multiple candidates
                        let candidates = op_manager
                            .ring
                            .k_closest_potentially_caching(key, skip_list, 3);
                        if candidates.is_empty() {
                            let connection_count =
                                op_manager.ring.connection_manager.num_connections();
                            tracing::warn!(
                                tx = %id,
                                %key,
                                skip = ?skip_list,
                                connection_count,
                                "No remote peer available for forwarding"
                            );
                            tracing::info!(
                                tx = %id,
                                %key,
                                "Attempting to fetch contract locally before aborting subscribe"
                            );

                            let get_op = get::start_op(*key, true, false);
                            if let Err(fetch_err) =
                                get::request_get(op_manager, get_op, HashSet::new()).await
                            {
                                tracing::warn!(
                                    tx = %id,
                                    %key,
                                    error = %fetch_err,
                                    "Failed to fetch contract locally while handling subscribe"
                                );
                                return Ok(return_not_subbed());
                            }

                            if wait_for_local_contract(op_manager, *key).await? {
                                tracing::info!(
                                    tx = %id,
                                    %key,
                                    "Fetched contract locally while handling subscribe"
                                );
                            } else {
                                tracing::warn!(
                                    tx = %id,
                                    %key,
                                    "Contract still unavailable locally after fetch attempt"
                                );
                                return Ok(return_not_subbed());
                            }
                        } else {
                            let Some(new_target) = candidates.first() else {
                                return Ok(return_not_subbed());
                            };
                            let new_target = new_target.clone();
                            let new_htl = htl.saturating_sub(1);

                            if new_htl == 0 {
                                tracing::debug!(tx = %id, %key, "Max number of hops reached while trying to get contract");
                                return Ok(return_not_subbed());
                            }

                            let mut new_skip_list = skip_list.clone();
                            if let Some(target_addr) = target.socket_addr() {
                                new_skip_list.insert(target_addr);
                            }

                            let new_target_addr = new_target
                                .socket_addr()
                                .expect("new target must have socket address");
                            let subscriber_addr = subscriber
                                .socket_addr()
                                .expect("subscriber must have socket address");
                            tracing::info!(
                                tx = %id,
                                %key,
                                new_target = %new_target_addr,
                                upstream = %subscriber_addr,
                                "Forward request to peer"
                            );
                            tracing::debug!(
                                tx = %id,
                                %key,
                                candidates = ?candidates,
                                skip = ?new_skip_list,
                                "Forwarding seek to next candidate"
                            );
                            // Retry seek node when the contract to subscribe has not been found in this node
                            return build_op_result(
                                *id,
                                Some(SubscribeState::AwaitingResponse {
                                    skip_list: new_skip_list.clone(),
                                    retries: *retries,
                                    current_hop: new_htl,
                                    upstream_subscriber: Some(subscriber.clone()),
                                }),
                                // Use PeerAddr::Unknown - the subscriber doesn't know their own
                                // external address (especially behind NAT). The recipient will
                                // fill this in from the packet source address.
                                (SubscribeMsg::SeekNode {
                                    id: *id,
                                    key: *key,
                                    subscriber: PeerKeyLocation::with_unknown_addr(
                                        this_peer.pub_key().clone(),
                                    ),
                                    target: new_target,
                                    skip_list: new_skip_list,
                                    htl: new_htl,
                                    retries: *retries,
                                })
                                .into(),
                                self.upstream_addr,
                            );
                        }
                        // After fetch attempt we should now have the contract locally.
                    }

                    let before_direct = subscribers_snapshot(op_manager, key);
                    let subscriber_addr = subscriber
                        .socket_addr()
                        .expect("subscriber must have socket address");
                    tracing::info!(
                        tx = %id,
                        %key,
                        subscriber = %subscriber_addr,
                        subscribers_before = ?before_direct,
                        "subscribe: attempting to register direct subscriber"
                    );
                    // Local registration - no upstream NAT address
                    if op_manager
                        .ring
                        .add_subscriber(key, subscriber.clone(), None)
                        .is_err()
                    {
                        tracing::warn!(
                            tx = %id,
                            %key,
                            subscriber = %subscriber_addr,
                            subscribers_before = ?before_direct,
                            "subscribe: direct registration failed (max subscribers reached)"
                        );
                        // max number of subscribers for this contract reached
                        return Ok(return_not_subbed());
                    }
                    let after_direct = subscribers_snapshot(op_manager, key);
                    tracing::info!(
                        tx = %id,
                        %key,
                        subscriber = %subscriber_addr,
                        subscribers_after = ?after_direct,
                        "subscribe: registered direct subscriber"
                    );

                    match self.state {
                        Some(SubscribeState::ReceivedRequest) => {
                            tracing::info!(
                                tx = %id,
                                %key,
                                subscriber = %subscriber_addr,
                                "Peer successfully subscribed to contract",
                            );
                            new_state = None;
                            return_msg = Some(SubscribeMsg::ReturnSub {
                                target: subscriber.clone(),
                                id: *id,
                                key: *key,
                                subscribed: true,
                            });
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    }
                }
                SubscribeMsg::ReturnSub {
                    subscribed: false,
                    key,
                    target: _,
                    id,
                } => {
                    // Get sender from connection-based routing for skip list and logging
                    let sender = sender_from_addr
                        .clone()
                        .expect("ReturnSub requires source_addr");
                    let sender_addr = sender
                        .socket_addr()
                        .expect("sender must have socket address");
                    tracing::warn!(
                        tx = %id,
                        %key,
                        potential_provider = %sender_addr,
                        "Contract not found at potential subscription provider",
                    );
                    // will error out in case it has reached max number of retries
                    match self.state {
                        Some(SubscribeState::AwaitingResponse {
                            mut skip_list,
                            retries,
                            upstream_subscriber,
                            current_hop,
                        }) => {
                            if retries < MAX_RETRIES {
                                skip_list.insert(sender_addr);
                                // Use k_closest_potentially_caching to try multiple candidates
                                let candidates = op_manager
                                    .ring
                                    .k_closest_potentially_caching(key, &skip_list, 3);
                                if let Some(target) = candidates.first() {
                                    // Use PeerAddr::Unknown - the subscriber doesn't know their own
                                    // external address (especially behind NAT). The recipient will
                                    // fill this in from the packet source address.
                                    let own_loc = op_manager.ring.connection_manager.own_location();
                                    let subscriber = PeerKeyLocation::with_unknown_addr(
                                        own_loc.pub_key().clone(),
                                    );
                                    return_msg = Some(SubscribeMsg::SeekNode {
                                        id: *id,
                                        key: *key,
                                        subscriber,
                                        target: target.clone(),
                                        skip_list: skip_list.clone(),
                                        htl: current_hop,
                                        retries: retries + 1,
                                    });
                                } else {
                                    // No more candidates - try to complete locally as fallback
                                    if super::has_contract(op_manager, *key).await? {
                                        tracing::info!(
                                            tx = %id,
                                            %key,
                                            "No remote peers, completing subscription locally as fallback"
                                        );
                                        complete_local_subscription(op_manager, *id, *key).await?;
                                        return Ok(OperationResult {
                                            return_msg: None,
                                            target_addr: None,
                                            state: None,
                                        });
                                    }
                                    return Err(RingError::NoCachingPeers(*key).into());
                                }
                                new_state = Some(SubscribeState::AwaitingResponse {
                                    skip_list,
                                    retries: retries + 1,
                                    upstream_subscriber,
                                    current_hop,
                                });
                            } else {
                                return Err(OpError::MaxRetriesExceeded(
                                    *id,
                                    id.transaction_type(),
                                ));
                            }
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    }
                }
                SubscribeMsg::ReturnSub {
                    subscribed: true,
                    key,
                    id,
                    target,
                } => match self.state {
                    Some(SubscribeState::AwaitingResponse {
                        upstream_subscriber,
                        ..
                    }) => {
                        // Get sender from connection-based routing for logging
                        let sender = sender_from_addr
                            .clone()
                            .expect("ReturnSub requires source_addr");
                        fetch_contract_if_missing(op_manager, *key).await?;

                        let target_addr = target
                            .socket_addr()
                            .expect("target must have socket address");
                        let sender_addr = sender
                            .socket_addr()
                            .expect("sender must have socket address");
                        tracing::info!(
                            tx = %id,
                            %key,
                            this_peer = %target_addr,
                            provider = %sender_addr,
                            "Subscribed to contract"
                        );
                        tracing::info!(
                            tx = %id,
                            %key,
                            upstream = upstream_subscriber
                                .as_ref()
                                .and_then(|loc| loc.socket_addr())
                                .map(|addr| format!("{:.8}", addr))
                                .unwrap_or_else(|| "<none>".into()),
                            "Handling ReturnSub (subscribed=true)"
                        );
                        if let Some(upstream_subscriber) = upstream_subscriber.as_ref() {
                            let before_upstream = subscribers_snapshot(op_manager, key);
                            let upstream_addr = upstream_subscriber
                                .socket_addr()
                                .expect("upstream subscriber must have socket address");
                            tracing::info!(
                                tx = %id,
                                %key,
                                upstream = %upstream_addr,
                                subscribers_before = ?before_upstream,
                                "subscribe: attempting to register upstream link"
                            );
                            // Local registration - no upstream NAT address
                            if op_manager
                                .ring
                                .add_subscriber(key, upstream_subscriber.clone(), None)
                                .is_err()
                            {
                                tracing::warn!(
                                    tx = %id,
                                    %key,
                                    upstream = %upstream_addr,
                                    subscribers_before = ?before_upstream,
                                    "subscribe: upstream registration failed (max subscribers reached)"
                                );
                            } else {
                                let after_upstream = subscribers_snapshot(op_manager, key);
                                tracing::info!(
                                    tx = %id,
                                    %key,
                                    upstream = %upstream_addr,
                                    subscribers_after = ?after_upstream,
                                    "subscribe: registered upstream link"
                                );
                            }
                        }

                        let before_provider = subscribers_snapshot(op_manager, key);
                        tracing::info!(
                            tx = %id,
                            %key,
                            provider = %sender_addr,
                            subscribers_before = ?before_provider,
                            "subscribe: registering provider/subscription source"
                        );
                        // Local registration - no upstream NAT address
                        if op_manager
                            .ring
                            .add_subscriber(key, sender.clone(), None)
                            .is_err()
                        {
                            // concurrently it reached max number of subscribers for this contract
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "Max number of subscribers reached for contract"
                            );
                            return Err(OpError::UnexpectedOpState);
                        }
                        let after_provider = subscribers_snapshot(op_manager, key);
                        tracing::info!(
                            tx = %id,
                            %key,
                            provider = %sender_addr,
                            subscribers_after = ?after_provider,
                            "subscribe: registered provider/subscription source"
                        );

                        new_state = Some(SubscribeState::Completed { key: *key });
                        if let Some(upstream_subscriber) = upstream_subscriber {
                            let upstream_addr = upstream_subscriber
                                .socket_addr()
                                .expect("upstream subscriber must have socket address");
                            tracing::debug!(
                                tx = %id,
                                %key,
                                upstream_subscriber = %upstream_addr,
                                "Forwarding subscription to upstream subscriber"
                            );
                            return_msg = Some(SubscribeMsg::ReturnSub {
                                id: *id,
                                key: *key,
                                target: upstream_subscriber,
                                subscribed: true,
                            });
                        } else {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "No upstream subscriber, subscription completed"
                            );
                            return_msg = None;
                        }
                    }
                    _other => {
                        return Err(OpError::invalid_transition(self.id));
                    }
                },
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, self.upstream_addr)
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<SubscribeState>,
    msg: Option<SubscribeMsg>,
    upstream_addr: Option<std::net::SocketAddr>,
) -> Result<OperationResult, OpError> {
    // For response messages (ReturnSub), use upstream_addr directly for routing.
    // This is more reliable than extracting from the message's target field, which
    // may have been looked up from connection_manager (subject to race conditions).
    // For forward messages (SeekNode, RequestSub, FetchRouting), use the message's target.
    let target_addr = match &msg {
        Some(SubscribeMsg::ReturnSub { .. }) => upstream_addr,
        _ => msg.as_ref().and_then(|m| m.target_addr()),
    };

    let output_op = state.map(|state| SubscribeOp {
        id,
        state: Some(state),
        upstream_addr,
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        target_addr,
        state: output_op.map(OpEnum::Subscribe),
    })
}

impl IsOperationCompleted for SubscribeOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(SubscribeState::Completed { .. }))
    }
}

#[cfg(test)]
mod tests;

mod messages {
    use std::{borrow::Borrow, fmt::Display};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum SubscribeMsg {
        FetchRouting {
            id: Transaction,
            target: PeerKeyLocation,
        },
        RequestSub {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
            subscriber: PeerKeyLocation,
        },
        SeekNode {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
            subscriber: PeerKeyLocation,
            skip_list: HashSet<std::net::SocketAddr>,
            htl: usize,
            retries: usize,
        },
        ReturnSub {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
            subscribed: bool,
        },
    }

    impl InnerMessage for SubscribeMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                Self::FetchRouting { id, .. } => id,
                Self::RequestSub { id, .. } => id,
                Self::ReturnSub { id, .. } => id,
            }
        }

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            match self {
                Self::RequestSub { target, .. } => Some(target),
                Self::SeekNode { target, .. } => Some(target),
                Self::ReturnSub { target, .. } => Some(target),
                _ => None,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::SeekNode { key, .. } => Some(Location::from(key.id())),
                Self::RequestSub { key, .. } => Some(Location::from(key.id())),
                Self::ReturnSub { key, .. } => Some(Location::from(key.id())),
                _ => None,
            }
        }
    }

    impl SubscribeMsg {
        // sender() method removed - use connection-based routing via source_addr instead

        /// Returns the socket address of the target peer for routing.
        /// Used by OperationResult to determine where to send the message.
        pub fn target_addr(&self) -> Option<std::net::SocketAddr> {
            match self {
                Self::FetchRouting { target, .. }
                | Self::RequestSub { target, .. }
                | Self::SeekNode { target, .. }
                | Self::ReturnSub { target, .. } => target.socket_addr(),
            }
        }
    }

    impl Display for SubscribeMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {id})"),
                Self::FetchRouting { .. } => write!(f, "FetchRouting(id: {id})"),
                Self::RequestSub { .. } => write!(f, "RequestSub(id: {id})"),
                Self::ReturnSub { .. } => write!(f, "ReturnSub(id: {id})"),
            }
        }
    }
}
