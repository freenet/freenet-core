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
    ring::{Location, PeerKeyLocation, RingError},
};

pub(crate) struct PutOp {
    pub id: Transaction,
    state: Option<PutState>,
    stats: Option<PutStats>,
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

struct PutStats {
    target: Option<PeerKeyLocation>,
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
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Put(put_op))) => {
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization { op: put_op, sender })
            }
            Ok(Some(op)) => {
                let _ = op_manager.push(tx, op).await;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                // new request to put a new value for a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(PutState::ReceivedRequest),
                        stats: None, // don't care for stats in the target peers
                        id: tx,
                    },
                    sender,
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
        conn_manager: &'a mut NB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let new_state;
            let stats = self.stats;
            let old_state = format!("{:?}", self.state); // Capture before moving

            match input {
                PutMsg::RequestPut {
                    id,
                    contract,
                    related_contracts,
                    value,
                    htl,
                    target,
                } => {
                    // Get the contract key and own location
                    let key = contract.key();
                    let sender = op_manager.ring.connection_manager.own_location();

                    tracing::info!(
                        "Requesting put for contract {} from {} to {}",
                        key,
                        sender.peer,
                        target.peer
                    );

                    // Cache the contract locally BEFORE propagating to network
                    // This ensures the initiating node has immediate access to the contract
                    // and prevents data loss if the network propagation fails
                    let should_seed = op_manager.ring.should_seed(&key);
                    let is_already_seeding = op_manager.ring.is_seeding_contract(&key);

                    if should_seed && !is_already_seeding {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %sender.peer,
                            "Caching contract locally in initiating node before propagation"
                        );

                        // Put the contract locally first
                        put_contract(
                            op_manager,
                            key,
                            value.clone(),
                            related_contracts.clone(),
                            contract,
                        )
                        .await?;

                        // Mark as seeded locally
                        op_manager.ring.seed_contract(key);

                        tracing::debug!(
                            tx = %id,
                            %key,
                            peer = %sender.peer,
                            "Successfully cached contract locally before propagation"
                        );
                    }

                    // Create a SeekNode message to find the target node
                    // fixme: this node should filter out incoming redundant puts since is the one initiating the request
                    return_msg = Some(PutMsg::SeekNode {
                        id: *id,
                        sender,
                        target: target.clone(),
                        value: value.clone(),
                        contract: contract.clone(),
                        related_contracts: related_contracts.clone(),
                        htl: *htl,
                    });

                    // No changes to state yet, still in AwaitResponse state
                    new_state = self.state;
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

                    tracing::info!(
                        tx = %id,
                        %key,
                        is_subscribed_contract,
                        should_seed,
                        should_handle_locally,
                        "PUT_SEEDING_DECISION: Evaluating if node should cache contract"
                    );

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
                        put_contract(
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

                        super::start_subscription_request(op_manager, key, false, skip_list).await;
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

                    // Subscriber nodes have been notified of the change, the operation is completed
                    tracing::info!(
                        tx = %id,
                        key = %key,
                        target = %upstream.peer,
                        "PUT_SUCCESS_SEND: Broadcasting complete, sending SuccessfulPut upstream"
                    );
                    return_msg = Some(PutMsg::SuccessfulPut {
                        id: *id,
                        target: upstream.clone(),
                        key: *key,
                        sender,
                    });
                    new_state = None;
                }
                PutMsg::SuccessfulPut { id, .. } => {
                    tracing::info!(
                        tx = %id,
                        current_state = ?self.state,
                        "PUT_SUCCESS_MSG: Received SuccessfulPut message"
                    );
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
                                super::start_subscription_request(
                                    op_manager,
                                    key,
                                    false,
                                    HashSet::new(),
                                )
                                .await;
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
                                tracing::info!(
                                    tx = %id,
                                    key = %key,
                                    target = %upstream.peer,
                                    "PUT_SUCCESS_SEND: Contract stored, sending SuccessfulPut upstream"
                                );
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
                    let (last_hop, new_skip_list) = if let Some(new_htl) = htl.checked_sub(1) {
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

                        (put_here, new_skip_list)
                    } else {
                        // Last hop, no more forwarding
                        (true, skip_list.clone())
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
                            super::start_subscription_request(
                                op_manager,
                                key,
                                true,
                                new_skip_list.clone(),
                            )
                            .await;
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

            tracing::info!(
                tx = %self.id,
                old_state = %old_state,
                new_state = ?new_state,
                has_return_msg = return_msg.is_some(),
                "PUT_STATE_TRANSITION: PUT operation state change"
            );

            build_op_result(self.id, new_state, return_msg, stats)
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
    stats: Option<PutStats>,
) -> Result<OperationResult, OpError> {
    let output_op = state.map(|op| PutOp {
        id,
        state: Some(op),
        stats,
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
        Some(PutState::ReceivedRequest | PutState::BroadcastOngoing) => {
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
                    stats: None,
                };
                op_manager
                    .notify_op_change(NetMessage::from(return_msg.unwrap()), OpEnum::Put(op))
                    .await?;
                return Err(OpError::StatePushed);
            } else {
                new_state = None;
                tracing::info!(
                    tx = %id,
                    key = %key,
                    target = %upstream.peer,
                    "PUT_SUCCESS_SEND: Final hop complete, sending SuccessfulPut upstream"
                );
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

    PutOp {
        id,
        state,
        stats: Some(PutStats { target: None }),
    }
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
    let (key, ..) = if let Some(PutState::PrepareRequest { contract, .. }) = &put_op.state {
        (contract.key(), contract.clone())
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    let sender = op_manager.ring.connection_manager.own_location();

    // the initial request must provide:
    // - a peer as close as possible to the contract location
    // - and the value to put
    let target = op_manager
        .ring
        .closest_potentially_caching(&key, [&sender.peer].as_slice())
        .into_iter()
        .next()
        .ok_or(RingError::EmptyRing)?;

    let id = put_op.id;
    if let Some(stats) = &mut put_op.stats {
        stats.target = Some(target.clone());
    }

    match put_op.state {
        Some(PutState::PrepareRequest {
            contract,
            related_contracts,
            value,
            htl,
            subscribe,
        }) => {
            let new_state = Some(PutState::AwaitingResponse {
                key,
                upstream: None,
                contract: contract.clone(),
                state: value.clone(),
                subscribe,
            });
            let msg = PutMsg::RequestPut {
                id,
                contract,
                related_contracts,
                value,
                htl,
                target: target.clone(),
            };

            let op = PutOp {
                id,
                state: new_state,
                stats: put_op.stats,
            };

            op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Put(op))
                .await?;
        }
        _ => return Err(OpError::invalid_transition(put_op.id)),
    }

    Ok(())
}

async fn put_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
    contract: &ContractContainer,
) -> Result<WrappedState, OpError> {
    tracing::info!(
        %key,
        state_size = state.size(),
        "PUT_CONTRACT_START: Attempting to cache contract locally"
    );

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
            tracing::info!(
                %key,
                new_state_size = new_val.size(),
                "PUT_CONTRACT_SUCCESS: Contract cached successfully"
            );
            Ok(new_val)
        }
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Err(err),
        }) => {
            tracing::error!(%key, error = %err, "PUT_CONTRACT_ERROR: Failed to update contract value");
            Err(OpError::from(err))
            // TODO: not a valid value update, notify back to requester
        }
        Err(err) => {
            tracing::error!(%key, error = %err, "PUT_CONTRACT_ERROR: Contract handler error");
            Err(err.into())
        }
        Ok(other) => {
            tracing::error!(%key, response = ?other, "PUT_CONTRACT_ERROR: Unexpected contract handler response");
            Err(OpError::UnexpectedOpState)
        }
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
    let forward_to = op_manager
        .ring
        .closest_potentially_caching(&key, &skip_list);
    let own_pkloc = op_manager.ring.connection_manager.own_location();
    let own_loc = own_pkloc.location.expect("infallible");
    if let Some(peer) = forward_to {
        let other_loc = peer.location.as_ref().expect("infallible");
        let other_distance = contract_loc.distance(other_loc);
        let self_distance = contract_loc.distance(own_loc);
        if other_distance < self_distance {
            // forward the contract towards this node since it is indeed closer to the contract location
            // and forget about it, no need to keep track of this op or wait for response
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
        }
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
