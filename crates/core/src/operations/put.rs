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
    ) -> Result<OpInitialization<Self>, OpError> {
        let mut sender: Option<PeerId> = None;
        if let Some(peer_key_loc) = msg.sender().cloned() {
            sender = Some(peer_key_loc.peer);
        };

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
                Ok(OpInitialization { op: put_op, sender })
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
                    },
                    sender,
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
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let new_state;

            match input {
                PutMsg::RequestPut {
                    id,
                    sender,
                    contract,
                    related_contracts,
                    value,
                    htl,
                    target,
                } => {
                    // Get the contract key and own location
                    let key = contract.key();
                    let own_location = op_manager.ring.connection_manager.own_location();

                    tracing::info!(
                        "Requesting put for contract {} from {} to {}",
                        key,
                        sender.peer,
                        target.peer
                    );

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
                            peer = %sender.peer,
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
                                peer = %sender.peer,
                                "Marked contract as seeding locally"
                            );
                        }

                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %sender.peer,
                            was_already_seeding = is_already_seeding,
                            "Successfully processed contract locally with merge"
                        );

                        result
                    } else {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %sender.peer,
                            "Not initiator, skipping local caching"
                        );
                        value.clone()
                    };

                    // Determine next forwarding target - find peers closer to the contract location
                    // Don't reuse the target from RequestPut as that's US (the current processing peer)
                    let next_target = op_manager
                        .ring
                        .closest_potentially_caching(&key, [&sender.peer].as_slice());

                    if let Some(forward_target) = next_target {
                        // Create a SeekNode message to forward to the next hop
                        return_msg = Some(PutMsg::SeekNode {
                            id: *id,
                            sender: sender.clone(),
                            target: forward_target,
                            value: modified_value.clone(),
                            contract: contract.clone(),
                            related_contracts: related_contracts.clone(),
                            htl: *htl,
                        });

                        // Transition to AwaitingResponse state to handle future SuccessfulPut messages
                        new_state = Some(PutState::AwaitingResponse {
                            key,
                            upstream: Some(sender.clone()),
                            contract: contract.clone(),
                            state: modified_value,
                            subscribe: false,
                        });
                    } else {
                        // No other peers to forward to - we're the final destination
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "No peers to forward to - handling PUT completion locally, sending SuccessfulPut back to sender"
                        );

                        // Send SuccessfulPut back to the sender (upstream node)
                        return_msg = Some(PutMsg::SuccessfulPut {
                            id: *id,
                            target: sender.clone(),
                            key,
                            sender: own_location,
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
                    target,
                    sender,
                } => {
                    // Get the contract key and check if we should handle it
                    let key = contract.key();
                    let is_subscribed_contract = op_manager.ring.is_seeding_contract(&key);
                    let should_seed = op_manager.ring.should_seed(&key);
                    let should_handle_locally = !is_subscribed_contract && should_seed;

                    tracing::debug!(
                        tx = %id,
                        %key,
                        target = %target.peer,
                        sender = %sender.peer,
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
                            HashSet::from([sender.peer.clone()]),
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

                        tracing::debug!(
                            tx = %id,
                            "Successfully put value for contract {} @ {:?}",
                            key,
                            target.location
                        );

                        // Start subscription
                        let mut skip_list = HashSet::new();
                        skip_list.insert(sender.peer.clone());

                        // Add target to skip list if not the last hop
                        if !last_hop {
                            skip_list.insert(target.peer.clone());
                        }

                        super::start_subscription_request(op_manager, key).await;
                        op_manager.ring.seed_contract(key);

                        true
                    } else {
                        false
                    };

                    // Broadcast changes to subscribers
                    let broadcast_to = op_manager.get_broadcast_targets(&key, &sender.peer);
                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        (broadcast_to, sender.clone()),
                        key,
                        (contract.clone(), value.clone()),
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
                    sender,
                    ..
                } => {
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
                    let broadcast_to = op_manager.get_broadcast_targets(key, &sender.peer);
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
                        (broadcast_to, sender.clone()),
                        *key,
                        (contract.clone(), updated_value),
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
                    ..
                } => {
                    // Get own location and initialize counter
                    let sender = op_manager.ring.connection_manager.own_location();
                    let mut broadcasted_to = *broadcasted_to;

                    if upstream.peer == sender.peer {
                        // Originator reached the subscription tree. We can
                        // complete the local operation immediately.
                        tracing::trace!(
                            tx = %id,
                            %key,
                            "PUT originator reached subscription tree; completing locally"
                        );
                        new_state = Some(PutState::Finished { key: *key });
                    } else {
                        // Notify the upstream hop right away so the request
                        // path does not wait for the broadcast to finish.
                        let ack = PutMsg::SuccessfulPut {
                            id: *id,
                            target: upstream.clone(),
                            key: *key,
                            sender: sender.clone(),
                        };

                        tracing::trace!(
                            tx = %id,
                            %key,
                            upstream = %upstream.peer,
                            "Forwarding SuccessfulPut upstream before broadcast"
                        );

                        conn_manager
                            .send(&upstream.peer, NetMessage::from(ack))
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
                            sender: sender.clone(),
                            contract: contract.clone(),
                            target: peer.clone(),
                        };
                        let f = conn_manager.send(&peer.peer, msg.into());
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
                            peer.peer,
                            err
                        );
                        // todo: review this, maybe we should just dropping this subscription
                        conn_manager.drop_connection(&peer.peer).await?;
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
                    match self.state {
                        Some(PutState::AwaitingResponse {
                            key,
                            upstream,
                            contract,
                            state,
                            subscribe,
                        }) => {
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

                            // Start subscription if the contract is already seeded and the user requested it
                            if subscribe && is_seeding_contract {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                    "Starting subscription request"
                                );
                                // TODO: Make put operation atomic by linking it to the completion of this subscription request.
                                // Currently we can't link one transaction to another transaction's result, which would be needed
                                // to make this fully atomic. This should be addressed in a future refactoring.
                                super::start_subscription_request(op_manager, key).await;
                            }

                            tracing::info!(
                                tx = %id,
                                %key,
                                this_peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(),
                                "Peer completed contract value put",
                            );

                            // Mark operation as finished
                            new_state = Some(PutState::Finished { key });

                            // Forward success message upstream if needed
                            if let Some(upstream) = upstream {
                                return_msg = Some(PutMsg::SuccessfulPut {
                                    id: *id,
                                    target: upstream,
                                    key,
                                    sender: op_manager.ring.connection_manager.own_location(),
                                });
                            } else {
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
                    sender,
                    skip_list,
                    ..
                } => {
                    // Get contract key and own location
                    let key = contract.key();
                    let peer_loc = op_manager.ring.connection_manager.own_location();
                    let is_seeding_contract = op_manager.ring.is_seeding_contract(&key);
                    let should_seed = op_manager.ring.should_seed(&key);
                    let should_handle_locally = should_seed && !is_seeding_contract;

                    tracing::debug!(
                        tx = %id,
                        %key,
                        this_peer = %peer_loc.peer,
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
                    let last_hop = if let Some(new_htl) = htl.checked_sub(1) {
                        // Create updated skip list
                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.insert(sender.peer.clone());

                        // Forward to closer peers
                        let put_here = forward_put(
                            op_manager,
                            conn_manager,
                            contract,
                            new_value.clone(),
                            *id,
                            new_htl,
                            new_skip_list.clone(),
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
                            super::start_subscription_request(op_manager, key).await;
                            op_manager.ring.seed_contract(key)
                        };

                        // Notify subscribers of dropped contracts
                        if let Some(dropped_key) = dropped_contract {
                            for subscriber in old_subscribers {
                                conn_manager
                                    .send(
                                        &subscriber.peer,
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
                    let broadcast_to = op_manager.get_broadcast_targets(&key, &sender.peer);
                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        (broadcast_to, sender.clone()),
                        key,
                        (contract.clone(), new_value.clone()),
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

            build_op_result(self.id, new_state, return_msg)
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
                    .filter(|pk| &pk.peer != sender)
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
) -> Result<OperationResult, OpError> {
    let output_op = state.map(|op| PutOp {
        id,
        state: Some(op),
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        state: output_op.map(OpEnum::Put),
    })
}

async fn try_to_broadcast(
    id: Transaction,
    last_hop: bool,
    op_manager: &OpManager,
    state: Option<PutState>,
    (broadcast_to, upstream): (Vec<PeerKeyLocation>, PeerKeyLocation),
    key: ContractKey,
    (contract, new_value): (ContractContainer, WrappedState),
) -> Result<(Option<PutState>, Option<PutMsg>), OpError> {
    let new_state;
    let return_msg;

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
                new_state = Some(PutState::AwaitingResponse {
                    key,
                    upstream: Some(upstream),
                    contract: contract.clone(), // No longer optional
                    state: new_value.clone(),
                    subscribe: false,
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
                    sender: op_manager.ring.connection_manager.own_location(),
                });

                let op = PutOp {
                    id,
                    state: new_state,
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
                    sender: op_manager.ring.connection_manager.own_location(),
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

    PutOp { id, state }
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

    PutOp { id, state }
}

#[derive(Debug)]
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
        .closest_potentially_caching(&key, [&own_location.peer].as_slice());

    tracing::debug!(
        tx = %id,
        %key,
        target_found = target.is_some(),
        target_peer = ?target.as_ref().map(|t| t.peer.to_string()),
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
        let broadcast_to = op_manager.get_broadcast_targets(&key, &own_location.peer);

        if broadcast_to.is_empty() {
            // No peers to broadcast to - operation complete
            tracing::debug!(tx = %id, %key, "No broadcast targets, completing operation");

            // Set up state for SuccessfulPut message handling
            put_op.state = Some(PutState::AwaitingResponse {
                key,
                upstream: None,
                contract: contract.clone(),
                state: updated_value.clone(),
                subscribe: false,
            });

            // Create a SuccessfulPut message to trigger the completion handling
            let success_msg = PutMsg::SuccessfulPut {
                id,
                target: own_location.clone(),
                key,
                sender: own_location.clone(),
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
                (broadcast_to, sender),
                key,
                (contract.clone(), updated_value),
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
        target_peer = %target_peer.peer,
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
    });

    // Create RequestPut message and forward to target peer
    let msg = PutMsg::RequestPut {
        id,
        sender: own_location,
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
async fn forward_put<CB>(
    op_manager: &OpManager,
    conn_manager: &CB,
    contract: &ContractContainer,
    new_value: WrappedState,
    id: Transaction,
    htl: usize,
    skip_list: HashSet<PeerId>,
) -> bool
where
    CB: NetworkBridge,
{
    let key = contract.key();
    let contract_loc = Location::from(&key);
    let target_peer = op_manager
        .ring
        .closest_potentially_caching(&key, &skip_list);
    let own_pkloc = op_manager.ring.connection_manager.own_location();
    let own_loc = own_pkloc.location.expect("infallible");

    tracing::debug!(
        tx = %id,
        %key,
        contract_location = %contract_loc.0,
        own_location = %own_loc.0,
        skip_list_size = skip_list.len(),
        "Evaluating PUT forwarding decision"
    );

    if let Some(peer) = target_peer {
        let other_loc = peer.location.as_ref().expect("infallible");
        let other_distance = contract_loc.distance(other_loc);
        let self_distance = contract_loc.distance(own_loc);

        tracing::debug!(
            tx = %id,
            %key,
            target_peer = %peer.peer,
            target_location = %other_loc.0,
            target_distance = ?other_distance,
            self_distance = ?self_distance,
            "Found potential forward target"
        );

        if other_distance < self_distance {
            // forward the contract towards this node since it is indeed closer to the contract location
            // and forget about it, no need to keep track of this op or wait for response
            tracing::info!(
                tx = %id,
                %key,
                from_peer = %own_pkloc.peer,
                to_peer = %peer.peer,
                contract_location = %contract_loc.0,
                from_location = %own_loc.0,
                to_location = %other_loc.0,
                "Forwarding PUT to closer peer"
            );

            let _ = conn_manager
                .send(
                    &peer.peer,
                    (PutMsg::PutForward {
                        id,
                        sender: own_pkloc,
                        target: peer.clone(),
                        contract: contract.clone(),
                        new_value: new_value.clone(),
                        htl,
                        skip_list,
                    })
                    .into(),
                )
                .await;
            return false;
        } else {
            tracing::debug!(
                tx = %id,
                %key,
                "Not forwarding - this peer is closest"
            );
        }
    } else {
        tracing::debug!(
            tx = %id,
            %key,
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
            sender: PeerKeyLocation,
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
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
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
            sender: PeerKeyLocation,
        },
        /// Target the node which is closest to the key
        SeekNode {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
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
            sender: PeerKeyLocation,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
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
        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { sender, .. } => Some(sender),
                Self::BroadcastTo { sender, .. } => Some(sender),
                _ => None,
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
