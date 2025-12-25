//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

pub(crate) use self::messages::PutMsg;
use freenet_stdlib::{
    client_api::{ErrorKind, HostResponse},
    prelude::*,
};

use super::{put, OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::node::IsOperationCompleted;
use crate::{
    client_events::HostResult,
    contract::ContractHandlerEvent,
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager},
    ring::{KnownPeerKeyLocation, Location},
};

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

    /// Handle aborted connections by failing the operation immediately.
    ///
    /// PUT operations don't have alternative routes to try. When the connection
    /// drops, we notify the client of the failure so they can retry.
    pub(crate) async fn handle_abort(self, op_manager: &OpManager) -> Result<(), OpError> {
        tracing::warn!(
            tx = %self.id,
            "Put operation aborted due to connection failure"
        );

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
        _conn_manager: &'a mut NB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
        _source_addr: Option<std::net::SocketAddr>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let id = self.id;
            let upstream_addr = self.upstream_addr;
            let is_originator = upstream_addr.is_none();

            // Extract subscribe flag from state (only relevant for originator)
            let subscribe = match &self.state {
                Some(PutState::PrepareRequest { subscribe, .. }) => *subscribe,
                Some(PutState::AwaitingResponse { subscribe, .. }) => *subscribe,
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
                    let merged_value = put_contract(
                        op_manager,
                        key,
                        value.clone(),
                        related_contracts.clone(),
                        contract,
                    )
                    .await?;

                    // Mark as seeding if not already
                    if !was_seeding {
                        op_manager.ring.seed_contract(key, value.size() as u64);
                        super::announce_contract_cached(op_manager, &key).await;
                    }

                    // If we were already subscribed and the merged value differs from input,
                    // trigger an Update to propagate the change to other subscribers
                    let state_changed = merged_value.as_ref() != value.as_ref();
                    if was_seeding && state_changed {
                        tracing::debug!(
                            tx = %id,
                            contract = %key,
                            phase = "update_trigger",
                            "PUT on subscribed contract resulted in state change, triggering Update"
                        );
                        start_update_after_put(op_manager, id, key, merged_value.clone()).await;
                    }

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

                        let forward_msg = PutMsg::Request {
                            id,
                            contract: contract.clone(),
                            related_contracts: related_contracts.clone(),
                            value: merged_value,
                            htl: htl.saturating_sub(1),
                            skip_list: new_skip_list,
                        };

                        // Transition to AwaitingResponse, preserving subscribe flag for originator
                        // Store next_hop so handle_notification_msg can route the message
                        let new_state = Some(PutState::AwaitingResponse {
                            subscribe,
                            next_hop: Some(next_addr),
                        });

                        Ok(OperationResult {
                            return_msg: Some(NetMessage::from(forward_msg)),
                            next_hop: Some(next_addr),
                            state: Some(OpEnum::Put(PutOp {
                                id,
                                state: new_state,
                                upstream_addr,
                            })),
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
                            // Start subscription if requested
                            if subscribe {
                                start_subscription_after_put(op_manager, id, key).await;
                            }

                            Ok(OperationResult {
                                return_msg: None,
                                next_hop: None,
                                state: Some(OpEnum::Put(PutOp {
                                    id,
                                    state: Some(PutState::Finished { key }),
                                    upstream_addr: None,
                                })),
                            })
                        } else {
                            // Send response back to upstream
                            let response = PutMsg::Response { id, key };
                            let upstream =
                                upstream_addr.expect("non-originator must have upstream");

                            Ok(OperationResult {
                                return_msg: Some(NetMessage::from(response)),
                                next_hop: Some(upstream),
                                state: None, // Operation complete for this node
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

                        // Start subscription if requested
                        if subscribe {
                            start_subscription_after_put(op_manager, id, *key).await;
                        }

                        Ok(OperationResult {
                            return_msg: None,
                            next_hop: None,
                            state: Some(OpEnum::Put(PutOp {
                                id,
                                state: Some(PutState::Finished { key: *key }),
                                upstream_addr: None,
                            })),
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
                        })
                    }
                }
            }
        })
    }
}

/// Helper to start subscription after PUT completes (only for originator)
async fn start_subscription_after_put(
    op_manager: &OpManager,
    parent_tx: Transaction,
    key: ContractKey,
) {
    // Note: This failed_parents check may be unnecessary since we only spawn the subscription
    // at PUT completion, so there's no earlier child operation that could have failed.
    // Keeping it as defensive check in case of race conditions not currently understood.
    if !op_manager.failed_parents().contains(&parent_tx) {
        let child_tx = super::start_subscription_request(op_manager, parent_tx, key);
        tracing::debug!(
            tx = %parent_tx,
            child_tx = %child_tx,
            contract = %key,
            phase = "subscribe",
            "Started subscription as child operation after PUT"
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

/// Helper to start an Update operation when a PUT on a subscribed contract results in state change.
/// This propagates the merged state to other subscribers.
async fn start_update_after_put(
    op_manager: &OpManager,
    parent_tx: Transaction,
    key: ContractKey,
    new_state: WrappedState,
) {
    use super::update;

    let child_tx = Transaction::new_child_of::<update::UpdateMsg>(&parent_tx);
    op_manager.expect_and_register_sub_operation(parent_tx, child_tx);

    tracing::debug!(
        tx = %parent_tx,
        child_tx = %child_tx,
        contract = %key,
        phase = "update",
        "Starting Update as child operation after PUT changed subscribed contract state"
    );

    let op_manager_cloned = op_manager.clone();

    tokio::spawn(async move {
        tokio::task::yield_now().await;

        // Wrap the state as UpdateData::State for the update operation
        let update_data = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(new_state),
        );
        let update_op =
            update::start_op_with_id(key, update_data, RelatedContracts::default(), child_tx);

        match update::request_update(&op_manager_cloned, update_op).await {
            Ok(_) => {
                tracing::debug!(tx = %child_tx, parent_tx = %parent_tx, contract = %key, phase = "complete", "child Update completed");
            }
            Err(error) => {
                tracing::error!(tx = %parent_tx, child_tx = %child_tx, contract = %key, error = %error, phase = "error", "child Update failed");
                // Note: We don't propagate this failure to the parent PUT since the PUT itself
                // succeeded - the Update is best-effort propagation to subscribers
            }
        }
    });
}

pub(crate) fn start_op(
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
    subscribe: bool,
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
    },
    /// Waiting for response from downstream node.
    AwaitingResponse {
        /// If true, start a subscription after PUT completes (originator only)
        subscribe: bool,
        /// Next hop address for routing the outbound message
        next_hop: Option<std::net::SocketAddr>,
    },
    /// Operation completed successfully.
    Finished { key: ContractKey },
}

/// Request to insert/update a value into a contract.
/// Called when a client initiates a PUT operation.
pub(crate) async fn request_put(op_manager: &OpManager, put_op: PutOp) -> Result<(), OpError> {
    let (id, contract, value, related_contracts, htl, subscribe) = match &put_op.state {
        Some(PutState::PrepareRequest {
            contract,
            value,
            related_contracts,
            htl,
            subscribe,
        }) => (
            put_op.id,
            contract.clone(),
            value.clone(),
            related_contracts.clone(),
            *htl,
            *subscribe,
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
            next_hop: None,
        }),
        upstream_addr: None,
    };

    // Send through the operation processing pipeline
    op_manager
        .notify_op_change(NetMessage::from(msg), OpEnum::Put(new_op))
        .await?;

    Ok(())
}

async fn put_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
    contract: &ContractContainer,
) -> Result<WrappedState, OpError> {
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
        }) => {
            // Notify any waiters that this contract has been stored
            op_manager.notify_contract_stored(&key);
            Ok(new_val)
        }
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Err(err),
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
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. } | Self::Response { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { contract, .. } => Some(Location::from(contract.id())),
                Self::Response { key, .. } => Some(Location::from(key.id())),
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
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Transaction;
    use crate::operations::test_utils::make_contract_key;

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
            next_hop: None,
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
            next_hop: None,
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
            next_hop: None,
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
}
