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
    message::{InnerMessage, NetMessage, NetMessageV1, Transaction},
    node::{NetworkBridge, OpManager, PeerId},
    ring::{Location, PeerKeyLocation},
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
            "PutOp::load_or_init: Attempting to load or initialize operation"
        );

        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Put(put_op))) => {
                // was an existing operation, the other peer messaged back
                tracing::debug!(
                    tx = %tx,
                    state = %put_op.state.as_ref().map(|s| format!("{:?}", s)).unwrap_or_else(|| "None".to_string()),
                    "PutOp::load_or_init: Found existing PUT operation"
                );
                Ok(OpInitialization {
                    op: put_op,
                    source_addr,
                })
            }
            Ok(Some(op)) => {
                tracing::warn!(
                    tx = %tx,
                    "PutOp::load_or_init: Found operation with wrong type, pushing back"
                );
                let _ = op_manager.push(tx, op).await;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                // new request to put a new value for a contract, initialize the machine
                tracing::debug!(
                    tx = %tx,
                    "PutOp::load_or_init: No existing operation found, initializing new ReceivedRequest"
                );
                Ok(OpInitialization {
                    op: Self {
                        state: Some(PutState::ReceivedRequest),
                        id: tx,
                        upstream_addr: source_addr, // Connection-based routing: store who sent us this request
                    },
                    source_addr,
                })
            }
            Err(err) => {
                tracing::error!(
                    tx = %tx,
                    error = %err,
                    "PutOp::load_or_init: Error popping operation"
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
                PutMsg::RequestPut {
                    id,
                    origin,
                    contract,
                    related_contracts,
                    value,
                    htl,
                    target: _,
                } => {
                    // Fill in origin's external address from transport layer if unknown.
                    // This is the key step where the first recipient determines the
                    // origin's external address from the actual packet source address.
                    let mut origin = origin.clone();
                    if origin.peer_addr.is_unknown() {
                        let addr = source_addr
                            .expect("RequestPut with unknown origin address requires source_addr");
                        origin.set_addr(addr);
                        tracing::debug!(
                            tx = %id,
                            origin_addr = %addr,
                            "put: filled RequestPut origin address from source_addr"
                        );
                    }

                    // Get the contract key and own location
                    let key = contract.key();
                    let own_location = op_manager.ring.connection_manager.own_location();
                    // Use origin (from message) instead of sender_from_addr (from connection lookup).
                    // The origin has the correct pub_key and its address is filled from source_addr.
                    // Connection lookup can return wrong identity due to race condition where
                    // transport connection arrives before ExpectPeerConnection is processed.
                    let prev_sender = origin.clone();

                    tracing::info!(
                        "Requesting put for contract {} from {} to {}",
                        key,
                        prev_sender.peer(),
                        own_location.peer()
                    );

                    let subscribe = match &self.state {
                        Some(PutState::PrepareRequest { subscribe, .. }) => *subscribe,
                        Some(PutState::AwaitingResponse { subscribe, .. }) => *subscribe,
                        _ => false,
                    };

                    // Check if we're the initiator of this PUT operation
                    // We only cache locally when either WE initiate the PUT, or when forwarding just of the peer should be seeding
                    let should_seed = match &self.state {
                        Some(PutState::PrepareRequest { .. }) => true,
                        Some(PutState::AwaitingResponse { upstream, .. }) => {
                            upstream.is_none() || op_manager.ring.should_seed(&key)
                        }
                        _ => op_manager.ring.should_seed(&key),
                    };

                    let modified_value = if should_seed {
                        // Cache locally when initiating a PUT. This ensures:
                        // 1. Nodes with no connections can still cache contracts
                        // 2. Nodes that are the optimal location cache locally
                        // 3. Initiators always have the data they're putting (per Nacho's requirement)
                        // 4. States are properly merged (Freenet states are commutative monoids)
                        let is_already_seeding = op_manager.ring.is_seeding_contract(&key);

                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %prev_sender.peer(),
                            is_already_seeding,
                            "Processing local PUT in initiating node"
                        );

                        // DEBUG: Log contract key/hash details before put_contract
                        tracing::debug!(
                            "DEBUG PUT: Before put_contract - key={}, key.code_hash={:?}, contract.key={}, contract.key.code_hash={:?}, code_len={}",
                            key,
                            key.code_hash(),
                            contract.key(),
                            contract.key().code_hash(),
                            contract.data().len()
                        );

                        // Always call put_contract to ensure proper state merging
                        // Since Freenet states are commutative monoids, merging is always safe
                        // and necessary to maintain consistency
                        let result = put_contract(
                            op_manager,
                            key,
                            value.clone(),
                            related_contracts.clone(),
                            contract,
                        )
                        .await?;

                        // Mark as seeded locally if not already
                        if !is_already_seeding {
                            op_manager.ring.seed_contract(key);
                            tracing::debug!(
                                tx = %id,
                                %key,
                                peer = %prev_sender.peer(),
                                "Marked contract as seeding locally"
                            );
                        }

                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %prev_sender.peer(),
                            was_already_seeding = is_already_seeding,
                            "Successfully processed contract locally with merge"
                        );

                        result
                    } else {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %prev_sender.peer(),
                            "Not initiator, skipping local caching"
                        );
                        value.clone()
                    };

                    // Determine next forwarding target - find peers closer to the contract location
                    // Don't reuse the target from RequestPut as that's US (the current processing peer)
                    let skip = [&prev_sender.peer()];
                    let next_target = op_manager
                        .ring
                        .closest_potentially_caching(&key, skip.as_slice());

                    tracing::info!(
                        tx = %id,
                        %key,
                        next_target = ?next_target,
                        skip = ?skip,
                        "PUT seek evaluating next forwarding target"
                    );

                    if let Some(forward_target) = next_target {
                        // Create a SeekNode message to forward to the next hop
                        return_msg = Some(PutMsg::SeekNode {
                            id: *id,
                            origin: origin.clone(),
                            target: forward_target,
                            value: modified_value.clone(),
                            contract: contract.clone(),
                            related_contracts: related_contracts.clone(),
                            htl: *htl,
                        });

                        // When we're the origin node we already seeded the contract locally.
                        // Treat downstream SuccessfulPut messages as best-effort so River is unblocked.
                        if origin.peer() == own_location.peer() {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "Origin node finishing PUT without waiting for SuccessfulPut ack"
                            );

                            if subscribe {
                                if !op_manager.failed_parents().contains(id) {
                                    let child_tx =
                                        super::start_subscription_request(op_manager, *id, key);
                                    tracing::debug!(
                                        tx = %id,
                                        %child_tx,
                                        "started subscription as child operation"
                                    );
                                } else {
                                    tracing::warn!(
                                        tx = %id,
                                        "not starting subscription for failed parent operation"
                                    );
                                }
                            }

                            new_state = Some(PutState::Finished { key });
                        } else {
                            // Transition to AwaitingResponse state to handle future SuccessfulPut messages
                            new_state = Some(PutState::AwaitingResponse {
                                key,
                                upstream: Some(prev_sender.clone()),
                                contract: contract.clone(),
                                state: modified_value,
                                subscribe,
                                origin: origin.clone(),
                            });
                        }
                    } else {
                        // No other peers to forward to - we're the final destination
                        tracing::warn!(
                            tx = %id,
                            %key,
                            skip = ?skip,
                            "No peers to forward to after local processing - completing PUT locally"
                        );

                        // Send SuccessfulPut back to the sender (upstream node)
                        return_msg = Some(PutMsg::SuccessfulPut {
                            id: *id,
                            target: prev_sender.clone(),
                            key,
                            origin: origin.clone(),
                        });

                        // Mark operation as finished
                        new_state = Some(PutState::Finished { key });
                    }
                }
                PutMsg::SeekNode {
                    id,
                    value,
                    contract,
                    related_contracts,
                    htl,
                    target: _,
                    origin,
                } => {
                    // Fill in origin's external address from transport layer if unknown.
                    // This is the key step where the recipient determines the
                    // origin's external address from the actual packet source address.
                    let mut origin = origin.clone();
                    if origin.peer_addr.is_unknown() {
                        if let Some(addr) = source_addr {
                            origin.set_addr(addr);
                            tracing::debug!(
                                tx = %id,
                                origin_addr = %addr,
                                "put: filled SeekNode origin address from source_addr"
                            );
                        }
                    }

                    // Get sender from connection-based routing
                    let sender = sender_from_addr
                        .clone()
                        .expect("SeekNode requires source_addr");
                    // Get the contract key and check if we should handle it
                    let key = contract.key();
                    let is_subscribed_contract = op_manager.ring.is_seeding_contract(&key);
                    let should_seed = op_manager.ring.should_seed(&key);
                    let should_handle_locally = !is_subscribed_contract && should_seed;

                    tracing::debug!(
                        tx = %id,
                        %key,
                        target = %op_manager.ring.connection_manager.own_location().peer(),
                        sender = %sender.peer(),
                        "Putting contract at target peer",
                    );

                    // Determine if this is the last hop or if we should forward
                    let last_hop = if let Some(new_htl) = htl.checked_sub(1) {
                        // Forward changes to nodes closer to the contract location
                        forward_put(
                            op_manager,
                            conn_manager,
                            contract,
                            value.clone(),
                            *id,
                            new_htl,
                            HashSet::from([sender.peer().clone()]),
                            origin.clone(),
                        )
                        .await
                    } else {
                        // Last hop, no more forwarding
                        true
                    };

                    // Handle local storage and subscription if needed
                    if should_handle_locally {
                        // Store contract locally
                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
                            "Caching contract locally as it's not already seeded"
                        );

                        tracing::debug!(tx = %id, "Attempting contract value put");
                        // We don't need to capture the return value here since the value
                        // has already been processed at the initiating node
                        let _ = put_contract(
                            op_manager,
                            key,
                            value.clone(),
                            related_contracts.clone(),
                            contract,
                        )
                        .await?;

                        let own_location = op_manager.ring.connection_manager.own_location();
                        tracing::debug!(
                            tx = %id,
                            "Successfully put value for contract {} @ {:?}",
                            key,
                            own_location.location
                        );

                        // Start subscription
                        let mut skip_list = HashSet::new();
                        skip_list.insert(sender.peer().clone());

                        // Add ourselves to skip list if not the last hop
                        if !last_hop {
                            skip_list.insert(own_location.peer().clone());
                        }

                        let child_tx =
                            super::start_subscription_request_internal(op_manager, *id, key, false);
                        tracing::debug!(tx = %id, %child_tx, "started subscription as child operation");
                        op_manager.ring.seed_contract(key);

                        true
                    } else {
                        false
                    };

                    // Broadcast changes to subscribers
                    let broadcast_to = op_manager.get_broadcast_targets(&key, &sender.peer());
                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        origin.clone(),
                        (broadcast_to, sender.clone()),
                        key,
                        (contract.clone(), value.clone()),
                        self.upstream_addr,
                    )
                    .await
                    {
                        Ok((state, msg)) => {
                            new_state = state;
                            return_msg = msg;
                        }
                        Err(err) => return Err(err),
                    }
                }
                PutMsg::BroadcastTo {
                    id,
                    key,
                    new_value,
                    contract,
                    origin,
                    ..
                } => {
                    // Get sender from connection-based routing
                    let sender = sender_from_addr
                        .clone()
                        .expect("BroadcastTo requires source_addr");
                    // Get own location
                    let target = op_manager.ring.connection_manager.own_location();

                    // Update the contract locally
                    tracing::debug!(tx = %id, %key, "Attempting contract value update");
                    let updated_value = put_contract(
                        op_manager,
                        *key,
                        new_value.clone(),
                        RelatedContracts::default(),
                        contract,
                    )
                    .await?;
                    tracing::debug!(tx = %id, %key, "Contract successfully updated");

                    // Broadcast changes to subscribers
                    let broadcast_to = op_manager.get_broadcast_targets(key, &sender.peer());
                    tracing::debug!(
                        tx = %id,
                        %key,
                        location = ?target.location,
                        "Successfully updated contract value"
                    );

                    // Try to broadcast the changes
                    match try_to_broadcast(
                        *id,
                        false,
                        op_manager,
                        self.state,
                        origin.clone(),
                        (broadcast_to, sender.clone()),
                        *key,
                        (contract.clone(), updated_value),
                        self.upstream_addr,
                    )
                    .await
                    {
                        Ok((state, msg)) => {
                            new_state = state;
                            return_msg = msg;
                        }
                        Err(err) => return Err(err),
                    }
                }
                PutMsg::Broadcasting {
                    id,
                    broadcasted_to,
                    broadcast_to,
                    key,
                    new_value,
                    contract,
                    upstream,
                    origin,
                    ..
                } => {
                    // Get own location and initialize counter
                    let sender = op_manager.ring.connection_manager.own_location();
                    let mut broadcasted_to = *broadcasted_to;

                    if upstream.peer() == sender.peer() {
                        // Originator reached the subscription tree. This path should be filtered
                        // out by the deduplication layer, so treat it as a warning if it happens
                        // to help surface potential bugs.
                        tracing::warn!(
                            tx = %id,
                            %key,
                            "PUT originator re-entered broadcast loop; dedup should have completed"
                        );
                        new_state = Some(PutState::Finished { key: *key });
                    } else {
                        // Notify the upstream hop right away so the request
                        // path does not wait for the broadcast to finish.
                        let ack = PutMsg::SuccessfulPut {
                            id: *id,
                            target: upstream.clone(),
                            key: *key,
                            origin: origin.clone(),
                        };

                        tracing::trace!(
                            tx = %id,
                            %key,
                            upstream = %upstream.peer(),
                            "Forwarding SuccessfulPut upstream before broadcast"
                        );

                        conn_manager
                            .send(upstream.addr(), NetMessage::from(ack))
                            .await?;
                        new_state = None;
                    }

                    // Broadcast to all peers in parallel
                    let mut broadcasting = Vec::with_capacity(broadcast_to.len());
                    for peer in broadcast_to.iter() {
                        let msg = PutMsg::BroadcastTo {
                            id: *id,
                            key: *key,
                            new_value: new_value.clone(),
                            origin: origin.clone(),
                            contract: contract.clone(),
                            target: peer.clone(),
                        };
                        let f = conn_manager.send(peer.addr(), msg.into());
                        broadcasting.push(f);
                    }

                    // Collect errors from broadcasting
                    let error_futures = futures::future::join_all(broadcasting)
                        .await
                        .into_iter()
                        .enumerate()
                        .filter_map(|(p, err)| {
                            if let Err(err) = err {
                                Some((p, err))
                            } else {
                                None
                            }
                        });

                    // Handle failed broadcasts
                    let mut incorrect_results = 0;
                    for (peer_num, err) in error_futures {
                        // Remove the failed peers in reverse order
                        let peer = broadcast_to.get(peer_num).unwrap();
                        tracing::warn!(
                            "failed broadcasting put change to {} with error {}; dropping connection",
                            peer.peer(),
                            err
                        );
                        // todo: review this, maybe we should just dropping this subscription
                        conn_manager.drop_connection(peer.addr()).await?;
                        incorrect_results += 1;
                    }

                    // Update broadcast count and log success
                    broadcasted_to += broadcast_to.len() - incorrect_results;
                    tracing::debug!(
                        "Successfully broadcasted put into contract {key} to {broadcasted_to} peers"
                    );

                    return_msg = None;
                }
                PutMsg::SuccessfulPut { id, .. } => {
                    tracing::debug!(
                        tx = %id,
                        current_state = ?self.state,
                        "PutOp::process_message: handling SuccessfulPut"
                    );
                    match self.state {
                        Some(PutState::AwaitingResponse {
                            key,
                            upstream,
                            contract,
                            state,
                            subscribe,
                            origin: state_origin,
                        }) => {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                subscribe = subscribe,
                                "Processing LocalPut with subscribe flag"
                            );

                            // Check if already stored before any operations
                            let is_seeding_contract = op_manager.ring.is_seeding_contract(&key);

                            // Only store the contract locally if not already seeded
                            if !is_seeding_contract {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                    "Storing contract locally after successful put"
                                );

                                // Store the contract locally
                                put_contract(
                                    op_manager,
                                    key,
                                    state.clone(),
                                    RelatedContracts::default(),
                                    &contract.clone(),
                                )
                                .await?;

                                // Always seed the contract locally after a successful put
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                    "Adding contract to local seed list"
                                );
                                op_manager.ring.seed_contract(key);
                            } else {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                    "Contract already seeded locally, skipping duplicate caching"
                                );
                            }

                            tracing::info!(
                                tx = %id,
                                %key,
                                this_peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                "Peer completed contract value put",
                            );

                            new_state = Some(PutState::Finished { key });

                            if subscribe {
                                // Check if this parent has already failed due to a previous child failure
                                if !op_manager.failed_parents().contains(id) {
                                    tracing::debug!(
                                        tx = %id,
                                        %key,
                                        "starting child subscription for PUT operation"
                                    );
                                    let child_tx = super::start_subscription_request_internal(
                                        op_manager, *id, key, false,
                                    );
                                    tracing::debug!(tx = %id, %child_tx, "started subscription as child operation");
                                } else {
                                    tracing::warn!(
                                        tx = %id,
                                        "not starting subscription for failed parent operation"
                                    );
                                }
                            }

                            // Forward success message upstream if needed
                            if let Some(upstream_peer) = upstream.clone() {
                                tracing::trace!(
                                    tx = %id,
                                    %key,
                                    upstream = %upstream_peer.peer(),
                                    "PutOp::process_message: Forwarding SuccessfulPut upstream"
                                );
                                return_msg = Some(PutMsg::SuccessfulPut {
                                    id: *id,
                                    target: upstream_peer,
                                    key,
                                    origin: state_origin.clone(),
                                });
                            } else {
                                tracing::trace!(
                                    tx = %id,
                                    %key,
                                    "PutOp::process_message: SuccessfulPut originated locally; no upstream"
                                );
                                return_msg = None;
                            }
                        }
                        Some(PutState::Finished { .. }) => {
                            // Operation already completed - this is a duplicate SuccessfulPut message
                            // This can happen when multiple peers send success confirmations
                            tracing::debug!(
                                tx = %id,
                                "Received duplicate SuccessfulPut for already completed operation, ignoring"
                            );
                            new_state = None; // Mark for completion
                            return_msg = None;
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    };
                }
                PutMsg::PutForward {
                    id,
                    contract,
                    new_value,
                    htl,
                    skip_list,
                    origin,
                    ..
                } => {
                    // Get sender from connection-based routing
                    let sender = sender_from_addr
                        .clone()
                        .expect("PutForward requires source_addr");
                    let max_htl = op_manager.ring.max_hops_to_live.max(1);
                    let htl_value = (*htl).min(max_htl);
                    if htl_value == 0 {
                        tracing::warn!(
                            tx = %id,
                            %contract,
                            sender = %sender.peer(),
                            "Discarding PutForward with zero HTL"
                        );
                        return Ok(OperationResult {
                            return_msg: None,
                            target_addr: None,
                            state: None,
                        });
                    }
                    // Get contract key and own location
                    let key = contract.key();
                    let peer_loc = op_manager.ring.connection_manager.own_location();
                    let is_seeding_contract = op_manager.ring.is_seeding_contract(&key);
                    let should_seed = op_manager.ring.should_seed(&key);
                    let should_handle_locally = should_seed && !is_seeding_contract;

                    tracing::debug!(
                        tx = %id,
                        %key,
                        this_peer = %peer_loc.peer(),
                        "Forwarding changes, trying to put the contract"
                    );

                    // Put the contract locally if needed
                    let already_put = if should_handle_locally {
                        tracing::debug!(tx = %id, %key, "Seeding contract locally");
                        put_contract(
                            op_manager,
                            key,
                            new_value.clone(),
                            RelatedContracts::default(),
                            contract,
                        )
                        .await?;
                        true
                    } else {
                        false
                    };

                    // Determine if this is the last hop and handle forwarding
                    let last_hop = if let Some(new_htl) = htl_value.checked_sub(1) {
                        // Create updated skip list
                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.insert(sender.peer().clone());

                        // Forward to closer peers
                        let put_here = forward_put(
                            op_manager,
                            conn_manager,
                            contract,
                            new_value.clone(),
                            *id,
                            new_htl,
                            new_skip_list.clone(),
                            origin.clone(),
                        )
                        .await;

                        put_here
                    } else {
                        // Last hop, no more forwarding
                        true
                    };

                    // Handle subscription and local storage if this is the last hop
                    if last_hop && should_handle_locally {
                        // Put the contract if not already done
                        if !already_put {
                            put_contract(
                                op_manager,
                                key,
                                new_value.clone(),
                                RelatedContracts::default(),
                                contract,
                            )
                            .await?;
                        }

                        // Start subscription and handle dropped contracts
                        let (dropped_contract, old_subscribers) = {
                            let child_tx = super::start_subscription_request_internal(
                                op_manager, *id, key, false,
                            );
                            tracing::debug!(tx = %id, %child_tx, "started subscription as child operation");
                            op_manager.ring.seed_contract(key)
                        };

                        // Notify subscribers of dropped contracts
                        if let Some(dropped_key) = dropped_contract {
                            for subscriber in old_subscribers {
                                conn_manager
                                    .send(
                                        subscriber.addr(),
                                        NetMessage::V1(NetMessageV1::Unsubscribed {
                                            transaction: Transaction::new::<PutMsg>(),
                                            key: dropped_key,
                                            from: op_manager
                                                .ring
                                                .connection_manager
                                                .get_peer_key()
                                                .unwrap(),
                                        }),
                                    )
                                    .await?;
                            }
                        }
                    } else if last_hop && !already_put {
                        // Last hop but not handling locally, still need to put
                        put_contract(
                            op_manager,
                            key,
                            new_value.clone(),
                            RelatedContracts::default(),
                            contract,
                        )
                        .await?;
                    }

                    // Broadcast changes to subscribers
                    let broadcast_to = op_manager.get_broadcast_targets(&key, &sender.peer());
                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        origin.clone(),
                        (broadcast_to, sender.clone()),
                        key,
                        (contract.clone(), new_value.clone()),
                        self.upstream_addr,
                    )
                    .await
                    {
                        Ok((state, msg)) => {
                            new_state = state;
                            return_msg = msg;
                        }
                        Err(err) => return Err(err),
                    }
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, self.upstream_addr)
        })
    }
}

impl OpManager {
    fn get_broadcast_targets(&self, key: &ContractKey, sender: &PeerId) -> Vec<PeerKeyLocation> {
        let subscribers = self
            .ring
            .subscribers_of(key)
            .map(|subs| {
                subs.value()
                    .iter()
                    .filter(|pk| &pk.peer() != sender)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        subscribers
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<PutState>,
    msg: Option<PutMsg>,
    upstream_addr: Option<std::net::SocketAddr>,
) -> Result<OperationResult, OpError> {
    // Extract target address from the message for routing
    let target_addr = msg.as_ref().and_then(|m| m.target_addr());

    let output_op = state.map(|op| PutOp {
        id,
        state: Some(op),
        upstream_addr,
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        target_addr,
        state: output_op.map(OpEnum::Put),
    })
}

#[allow(clippy::too_many_arguments)]
async fn try_to_broadcast(
    id: Transaction,
    last_hop: bool,
    op_manager: &OpManager,
    state: Option<PutState>,
    origin: PeerKeyLocation,
    (broadcast_to, upstream): (Vec<PeerKeyLocation>, PeerKeyLocation),
    key: ContractKey,
    (contract, new_value): (ContractContainer, WrappedState),
    upstream_addr: Option<std::net::SocketAddr>,
) -> Result<(Option<PutState>, Option<PutMsg>), OpError> {
    let new_state;
    let return_msg;

    let subscribe = match &state {
        Some(PutState::AwaitingResponse { subscribe, .. }) => *subscribe,
        _ => false,
    };

    let preserved_upstream = match &state {
        Some(PutState::AwaitingResponse {
            upstream: Some(existing),
            ..
        }) => Some(existing.clone()),
        _ => None,
    };

    match state {
        // Handle initiating node that's also the target (single node or targeting self)
        Some(PutState::AwaitingResponse {
            upstream: None,
            subscribe,
            ..
        }) if broadcast_to.is_empty() && last_hop => {
            // We're the initiating node and the target - operation complete
            tracing::debug!(
                "PUT operation complete - initiating node is also target (broadcast_to empty, last hop)"
            );

            // NOTE: We do NOT start a network subscription here when we're the target.
            // Client subscriptions are handled independently through the WebSocket API
            // and contract executor, not through the ring operations layer.
            // See: https://github.com/freenet/freenet-core/issues/1782
            if subscribe {
                tracing::debug!(
                    "Subscription requested but not starting network subscription (we are the target). \
                     Client subscriptions are handled through WebSocket/contract executor layer"
                );
            }

            new_state = Some(PutState::Finished { key });
            return_msg = None;
        }
        Some(
            PutState::ReceivedRequest
            | PutState::BroadcastOngoing
            | PutState::AwaitingResponse { .. },
        ) => {
            if broadcast_to.is_empty() && !last_hop {
                // broadcast complete
                tracing::debug!(
                    "Empty broadcast list while updating value for contract {}",
                    key
                );
                // means the whole tx finished so can return early
                let upstream_for_completion = preserved_upstream
                    .clone()
                    .or_else(|| Some(upstream.clone()));
                new_state = Some(PutState::AwaitingResponse {
                    key,
                    upstream: upstream_for_completion,
                    contract: contract.clone(), // No longer optional
                    state: new_value.clone(),
                    subscribe,
                    origin: origin.clone(),
                });
                return_msg = None;
            } else if !broadcast_to.is_empty() {
                tracing::debug!("Callback to start broadcasting to other nodes");
                new_state = Some(PutState::BroadcastOngoing);
                return_msg = Some(PutMsg::Broadcasting {
                    id,
                    new_value,
                    broadcasted_to: 0,
                    broadcast_to,
                    key,
                    contract,
                    upstream,
                    origin: origin.clone(),
                });

                let op = PutOp {
                    id,
                    state: new_state,
                    upstream_addr,
                };
                op_manager
                    .notify_op_change(NetMessage::from(return_msg.unwrap()), OpEnum::Put(op))
                    .await?;
                return Err(OpError::StatePushed);
            } else {
                new_state = None;
                return_msg = Some(PutMsg::SuccessfulPut {
                    id,
                    target: upstream,
                    key,
                    origin,
                });
            }
        }
        _ => return Err(OpError::invalid_transition(id)),
    };

    Ok((new_state, return_msg))
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
    tracing::debug!(%contract_location, %key, "Requesting put");

    let id = Transaction::new::<PutMsg>();
    // let payload_size = contract.data().len();
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
    tracing::debug!(%contract_location, %key, tx = %id, "Requesting put with existing transaction ID");

    // let payload_size = contract.data().len();
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

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum PutState {
    ReceivedRequest,
    /// Preparing request for put op.
    PrepareRequest {
        contract: ContractContainer,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
        htl: usize,
        subscribe: bool,
    },
    /// Awaiting response from petition.
    AwaitingResponse {
        key: ContractKey,
        upstream: Option<PeerKeyLocation>,
        contract: ContractContainer,
        state: WrappedState,
        subscribe: bool,
        origin: PeerKeyLocation,
    },
    /// Broadcasting changes to subscribers.
    BroadcastOngoing,
    /// Operation completed.
    Finished {
        key: ContractKey,
    },
}

/// Request to insert/update a value into a contract.
pub(crate) async fn request_put(op_manager: &OpManager, mut put_op: PutOp) -> Result<(), OpError> {
    // Process PrepareRequest state and transition to next state
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
            tracing::error!(tx = %put_op.id, op_state = ?put_op.state, "request_put called with unexpected state, expected PrepareRequest");
            return Err(OpError::UnexpectedOpState);
        }
    };

    let key = contract.key();
    let own_location = op_manager.ring.connection_manager.own_location();

    // Find the optimal target for this contract
    let target = op_manager
        .ring
        .closest_potentially_caching(&key, [&own_location.peer()].as_slice());

    tracing::debug!(
        tx = %id,
        %key,
        target_found = target.is_some(),
        target_peer = ?target.as_ref().map(|t| t.peer().to_string()),
        "Determined PUT routing target"
    );

    // No other peers found - handle locally
    if target.is_none() {
        tracing::debug!(tx = %id, %key, "No other peers available, handling put operation locally");

        // Store the contract locally
        let updated_value =
            put_contract(op_manager, key, value, related_contracts.clone(), &contract).await?;

        // Always seed the contract locally after a successful put
        tracing::debug!(
            tx = %id,
            %key,
            peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
            "Adding contract to local seed list"
        );
        op_manager.ring.seed_contract(key);

        // Determine which peers need to be notified and broadcast the update
        let broadcast_to = op_manager.get_broadcast_targets(&key, &own_location.peer());

        if broadcast_to.is_empty() {
            // No peers to broadcast to - operation complete
            tracing::debug!(tx = %id, %key, "No broadcast targets, completing operation");

            // Set up state for SuccessfulPut message handling
            put_op.state = Some(PutState::AwaitingResponse {
                key,
                upstream: None,
                contract: contract.clone(),
                state: updated_value.clone(),
                subscribe,
                origin: own_location.clone(),
            });

            // Create a SuccessfulPut message to trigger the completion handling
            let success_msg = PutMsg::SuccessfulPut {
                id,
                target: own_location.clone(),
                key,
                origin: own_location.clone(),
            };

            // Use notify_op_change to trigger the completion handling
            op_manager
                .notify_op_change(NetMessage::from(success_msg), OpEnum::Put(put_op))
                .await?;

            return Ok(());
        } else {
            // Broadcast to subscribers
            let sender = own_location.clone();
            let broadcast_state = Some(PutState::ReceivedRequest);

            let (new_state, return_msg) = try_to_broadcast(
                id,
                false,
                op_manager,
                broadcast_state,
                own_location.clone(),
                (broadcast_to, sender),
                key,
                (contract.clone(), updated_value),
                put_op.upstream_addr,
            )
            .await?;

            put_op.state = new_state;

            if let Some(msg) = return_msg {
                op_manager
                    .notify_op_change(NetMessage::from(msg), OpEnum::Put(put_op))
                    .await?;
            } else {
                // Complete the operation locally if no further messages needed
                put_op.state = Some(PutState::Finished { key });
            }
        }

        return Ok(());
    }

    // At least one peer found - cache locally first, then forward to network
    let target_peer = target.unwrap();

    tracing::debug!(
        tx = %id,
        %key,
        target_peer = %target_peer.peer(),
        target_location = ?target_peer.location,
        "Caching state locally before forwarding PUT to target peer"
    );

    // Cache the contract state locally before forwarding
    // This ensures the publishing node has immediate access to the new state
    let updated_value = put_contract(
        op_manager,
        key,
        value.clone(),
        related_contracts.clone(),
        &contract,
    )
    .await
    .map_err(|e| {
        tracing::error!(
            tx = %id,
            %key,
            error = %e,
            "Failed to cache state locally before forwarding PUT"
        );
        e
    })?;

    tracing::debug!(
        tx = %id,
        %key,
        "Local cache updated, now forwarding PUT to target peer"
    );

    put_op.state = Some(PutState::AwaitingResponse {
        key,
        upstream: None,
        contract: contract.clone(),
        state: updated_value.clone(),
        subscribe,
        origin: own_location.clone(),
    });

    // Create RequestPut message and forward to target peer
    // Use PeerAddr::Unknown for origin - the sender doesn't know their own
    // external address (especially behind NAT). The first recipient will
    // fill this in from the packet source address.
    let origin_for_msg = PeerKeyLocation::with_unknown_addr(own_location.pub_key().clone());
    let msg = PutMsg::RequestPut {
        id,
        origin: origin_for_msg,
        contract,
        related_contracts,
        value: updated_value,
        htl,
        target: target_peer,
    };

    tracing::debug!(
        tx = %id,
        %key,
        "Calling notify_op_change to send PUT message to network"
    );

    // Use notify_op_change to trigger the operation processing
    // This will cause the operation to be processed through process_message for network propagation
    op_manager
        .notify_op_change(NetMessage::from(msg), OpEnum::Put(put_op))
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
        }) => Ok(new_val),
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Err(err),
        }) => {
            tracing::error!(%key, "Failed to update contract value: {}", err);
            Err(OpError::from(err))
            // TODO: not a valid value update, notify back to requester
        }
        Err(err) => Err(err.into()),
        Ok(_) => Err(OpError::UnexpectedOpState),
    }
}

/// Forwards the put request to a peer which is closer to the assigned contract location if possible.
/// If is not possible to forward the request, then this peer is the final target and should store the contract.
/// It returns whether this peer should be storing the contract or not.
///
/// This operation is "fire and forget" and the node does not keep track if is successful or not.
#[allow(clippy::too_many_arguments)]
async fn forward_put<CB>(
    op_manager: &OpManager,
    conn_manager: &CB,
    contract: &ContractContainer,
    new_value: WrappedState,
    id: Transaction,
    htl: usize,
    skip_list: HashSet<PeerId>,
    origin: PeerKeyLocation,
) -> bool
where
    CB: NetworkBridge,
{
    let key = contract.key();
    let contract_loc = Location::from(&key);
    let max_htl = op_manager.ring.max_hops_to_live.max(1);
    let capped_htl = htl.min(max_htl);
    if capped_htl == 0 {
        tracing::warn!(
            tx = %id,
            %key,
            skip = ?skip_list,
            "Discarding PutForward with zero HTL after sanitization"
        );
        return true;
    }
    let target_peer = op_manager
        .ring
        .closest_potentially_caching(&key, &skip_list);
    let own_pkloc = op_manager.ring.connection_manager.own_location();
    let Some(own_loc) = own_pkloc.location else {
        tracing::warn!(
            tx = %id,
            %key,
            skip = ?skip_list,
            "Not forwarding PUT – own ring location not assigned yet; caching locally"
        );
        return true;
    };

    tracing::info!(
        tx = %id,
        %key,
        contract_location = %contract_loc.0,
        own_location = %own_loc.0,
        skip_list = ?skip_list,
        "Evaluating PUT forwarding decision"
    );

    if let Some(peer) = target_peer {
        let other_loc = peer.location.as_ref().expect("infallible");
        let other_distance = contract_loc.distance(other_loc);
        let self_distance = contract_loc.distance(own_loc);

        tracing::info!(
            tx = %id,
            %key,
            target_peer = %peer.peer(),
            target_location = %other_loc.0,
            target_distance = ?other_distance,
            self_distance = ?self_distance,
            skip_list = ?skip_list,
            "Found potential forward target"
        );

        if peer.peer() == own_pkloc.peer() {
            tracing::info!(
                tx = %id,
                %key,
                skip_list = ?skip_list,
                "Not forwarding - candidate peer resolves to self"
            );
            return true;
        }

        if htl == 0 {
            tracing::info!(
                tx = %id,
                %key,
                target_peer = %peer.peer(),
                "HTL exhausted - storing locally"
            );
            return true;
        }

        let mut updated_skip_list = skip_list.clone();
        updated_skip_list.insert(own_pkloc.peer().clone());

        if other_distance < self_distance {
            tracing::info!(
                tx = %id,
                %key,
                from_peer = %own_pkloc.peer(),
                to_peer = %peer.peer(),
                contract_location = %contract_loc.0,
                from_location = %own_loc.0,
                to_location = %other_loc.0,
                skip_list = ?updated_skip_list,
                "Forwarding PUT to closer peer"
            );
        } else {
            tracing::info!(
                tx = %id,
                %key,
                from_peer = %own_pkloc.peer(),
                to_peer = %peer.peer(),
                contract_location = %contract_loc.0,
                from_location = %own_loc.0,
                to_location = %other_loc.0,
                skip_list = ?updated_skip_list,
                "Forwarding PUT to peer despite non-improving distance (avoiding local minimum)"
            );
        }

        let _ = conn_manager
            .send(
                peer.addr(),
                (PutMsg::PutForward {
                    id,
                    target: peer.clone(),
                    origin,
                    contract: contract.clone(),
                    new_value: new_value.clone(),
                    htl: capped_htl,
                    skip_list: updated_skip_list,
                })
                .into(),
            )
            .await;
        return false;
    } else {
        tracing::info!(
            tx = %id,
            %key,
            skip_list = ?skip_list,
            "No peers available for forwarding - caching locally"
        );
    }
    true
}

mod messages {
    use std::{borrow::Borrow, fmt::Display};

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum PutMsg {
        /// Internal node instruction to find a route to the target node.
        RequestPut {
            id: Transaction,
            origin: PeerKeyLocation,
            contract: ContractContainer,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
            /// max hops to live
            htl: usize,
            target: PeerKeyLocation,
        },
        /// Internal node instruction to await the result of a put.
        AwaitPut { id: Transaction },
        /// Forward a contract and it's latest value to an other node
        PutForward {
            id: Transaction,
            target: PeerKeyLocation,
            origin: PeerKeyLocation,
            contract: ContractContainer,
            new_value: WrappedState,
            /// current htl, reduced by one at each hop
            htl: usize,
            skip_list: HashSet<PeerId>,
        },
        /// Value successfully inserted/updated.
        SuccessfulPut {
            id: Transaction,
            target: PeerKeyLocation,
            key: ContractKey,
            origin: PeerKeyLocation,
        },
        /// Target the node which is closest to the key
        SeekNode {
            id: Transaction,
            target: PeerKeyLocation,
            origin: PeerKeyLocation,
            value: WrappedState,
            contract: ContractContainer,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            /// max hops to live
            htl: usize,
        },
        /// Internal node instruction that  a change (either a first time insert or an update).
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_value: WrappedState,
            contract: ContractContainer,
            upstream: PeerKeyLocation,
            origin: PeerKeyLocation,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            origin: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            contract: ContractContainer,
            target: PeerKeyLocation,
        },
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                Self::RequestPut { id, .. } => id,
                Self::Broadcasting { id, .. } => id,
                Self::SuccessfulPut { id, .. } => id,
                Self::PutForward { id, .. } => id,
                Self::AwaitPut { id } => id,
                Self::BroadcastTo { id, .. } => id,
            }
        }

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::RequestPut { target, .. } => Some(target),
                Self::SuccessfulPut { target, .. } => Some(target),
                Self::PutForward { target, .. } => Some(target),
                Self::BroadcastTo { target, .. } => Some(target),
                _ => None,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::SeekNode { contract, .. } => Some(Location::from(contract.id())),
                Self::RequestPut { contract, .. } => Some(Location::from(contract.id())),
                Self::Broadcasting { key, .. } => Some(Location::from(key.id())),
                Self::PutForward { contract, .. } => Some(Location::from(contract.id())),
                Self::BroadcastTo { key, .. } => Some(Location::from(key.id())),
                _ => None,
            }
        }
    }

    impl PutMsg {
        // sender() method removed - use connection-based routing via source_addr instead

        /// Returns the socket address of the target peer for routing.
        /// Used by OperationResult to determine where to send the message.
        pub fn target_addr(&self) -> Option<std::net::SocketAddr> {
            match self {
                Self::SeekNode { target, .. }
                | Self::RequestPut { target, .. }
                | Self::SuccessfulPut { target, .. }
                | Self::PutForward { target, .. }
                | Self::BroadcastTo { target, .. } => target.socket_addr(),
                // AwaitPut and Broadcasting are internal messages, no network target
                Self::AwaitPut { .. } | Self::Broadcasting { .. } => None,
            }
        }
    }

    impl Display for PutMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {id})"),
                Self::RequestPut { .. } => write!(f, "RequestPut(id: {id})"),
                Self::Broadcasting { .. } => write!(f, "Broadcasting(id: {id})"),
                Self::SuccessfulPut { .. } => write!(f, "SuccessfulPut(id: {id})"),
                Self::PutForward { .. } => write!(f, "PutForward(id: {id})"),
                Self::AwaitPut { .. } => write!(f, "AwaitPut(id: {id})"),
                Self::BroadcastTo { .. } => write!(f, "BroadcastTo(id: {id})"),
            }
        }
    }
}
