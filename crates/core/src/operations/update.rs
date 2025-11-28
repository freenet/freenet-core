// TODO: complete update logic in the network
use either::Either;
use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;

pub(crate) use self::messages::UpdateMsg;
use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::contract::{ContractHandlerEvent, StoreResponse};
use crate::message::{InnerMessage, NetMessage, NodeEvent, Transaction};
use crate::node::IsOperationCompleted;
use crate::ring::{Location, PeerKeyLocation, RingError};
use crate::{
    client_events::HostResult,
    node::{NetworkBridge, OpManager, PeerId},
};

pub(crate) struct UpdateOp {
    pub id: Transaction,
    pub(crate) state: Option<UpdateState>,
    stats: Option<UpdateStats>,
}

impl UpdateOp {
    pub fn outcome(&self) -> OpOutcome<'_> {
        OpOutcome::Irrelevant
    }

    pub fn finalized(&self) -> bool {
        matches!(self.state, None | Some(UpdateState::Finished { .. }))
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
                "UPDATE operation {} failed to finish successfully, current state: {:?}",
                self.id,
                self.state
            );
            Err(ErrorKind::OperationError {
                cause: "update didn't finish successfully".into(),
            }
            .into())
        }
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
        conn_manager: &'a mut NB,
        op_manager: &'a crate::node::OpManager,
        input: &'a Self::Message,
        _source_addr: Option<std::net::SocketAddr>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<super::OperationResult, OpError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let return_msg;
            let new_state;
            let stats = self.stats;

            match input {
                UpdateMsg::RequestUpdate {
                    id,
                    key,
                    sender: request_sender,
                    target,
                    related_contracts,
                    value,
                } => {
                    let self_location = op_manager.ring.connection_manager.own_location();

                    tracing::debug!(
                        tx = %id,
                        %key,
                        executing_peer = %self_location.peer(),
                        request_sender = %request_sender.peer(),
                        target_peer = %target.peer(),
                        "UPDATE RequestUpdate: executing_peer={} received request from={} targeting={}",
                        self_location.peer(),
                        request_sender.peer(),
                        target.peer()
                    );

                    // If target is not us, this message is meant for another peer
                    // This can happen when the initiator processes its own message before sending to network
                    if target.peer() != self_location.peer() {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            our_peer = %self_location.peer(),
                            target_peer = %target.peer(),
                            "RequestUpdate target is not us - message will be routed to target"
                        );
                        // Keep current state, message will be sent to target peer via network
                        return_msg = None;
                        new_state = self.state;
                    } else {
                        // Target is us - process the request
                        // Determine if this is a local request (from our own client) or remote request
                        let is_local_request =
                            matches!(&self.state, Some(UpdateState::PrepareRequest { .. }));
                        let upstream = if is_local_request {
                            None // No upstream - we are the initiator
                        } else {
                            Some(request_sender.clone()) // Upstream is the peer that sent us this request
                        };

                        // First check if we have the contract locally
                        let has_contract = match op_manager
                            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                                key: *key,
                                return_contract_code: false,
                            })
                            .await
                        {
                            Ok(ContractHandlerEvent::GetResponse {
                                response: Ok(StoreResponse { state: Some(_), .. }),
                                ..
                            }) => {
                                tracing::debug!(tx = %id, %key, "Contract exists locally, handling UPDATE");
                                true
                            }
                            _ => {
                                tracing::debug!(tx = %id, %key, "Contract not found locally");
                                false
                            }
                        };

                        if has_contract {
                            // We have the contract - handle UPDATE locally
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "Handling UPDATE locally - contract exists"
                            );

                            // Update contract locally
                            let UpdateExecution {
                                value: updated_value,
                                summary,
                                changed,
                            } = update_contract(
                                op_manager,
                                *key,
                                value.clone(),
                                related_contracts.clone(),
                            )
                            .await?;

                            if !changed {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    "UPDATE yielded no state change, skipping broadcast"
                                );

                                if upstream.is_none() {
                                    new_state = Some(UpdateState::Finished {
                                        key: *key,
                                        summary: summary.clone(),
                                    });
                                } else {
                                    new_state = None;
                                }

                                return_msg = None;
                            } else {
                                // Get broadcast targets for propagating UPDATE to subscribers
                                let broadcast_to = op_manager
                                    .get_broadcast_targets_update(key, &request_sender.peer());

                                if broadcast_to.is_empty() {
                                    tracing::debug!(
                                        tx = %id,
                                        %key,
                                        "No broadcast targets, completing UPDATE locally"
                                    );

                                    if upstream.is_none() {
                                        new_state = Some(UpdateState::Finished {
                                            key: *key,
                                            summary: summary.clone(),
                                        });
                                    } else {
                                        new_state = None;
                                    }

                                    return_msg = None;
                                } else {
                                    // Broadcast to other peers
                                    match try_to_broadcast(
                                        *id,
                                        true, // last_hop - we're handling locally
                                        op_manager,
                                        self.state,
                                        (broadcast_to, request_sender.clone()),
                                        *key,
                                        updated_value.clone(),
                                        false,
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
                            }
                        } else {
                            // Contract not found locally - forward to another peer
                            let next_target = op_manager.ring.closest_potentially_caching(
                                key,
                                [&self_location.peer(), &request_sender.peer()].as_slice(),
                            );

                            if let Some(forward_target) = next_target {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    next_peer = %forward_target.peer(),
                                    "Forwarding UPDATE to peer that might have contract"
                                );

                                // Create a SeekNode message to forward to the next hop
                                return_msg = Some(UpdateMsg::SeekNode {
                                    id: *id,
                                    sender: self_location.clone(),
                                    target: forward_target,
                                    value: value.clone(),
                                    key: *key,
                                    related_contracts: related_contracts.clone(),
                                });
                                new_state = None;
                            } else {
                                // No peers available and we don't have the contract - capture context
                                let skip_list = [&self_location.peer(), &request_sender.peer()];
                                let subscribers = op_manager
                                    .ring
                                    .subscribers_of(key)
                                    .map(|subs| {
                                        subs.value()
                                            .iter()
                                            .map(|loc| format!("{:.8}", loc.peer()))
                                            .collect::<Vec<_>>()
                                    })
                                    .unwrap_or_default();
                                let candidates = op_manager
                                    .ring
                                    .k_closest_potentially_caching(key, skip_list.as_slice(), 5)
                                    .into_iter()
                                    .map(|loc| format!("{:.8}", loc.peer()))
                                    .collect::<Vec<_>>();
                                let connection_count =
                                    op_manager.ring.connection_manager.num_connections();
                                tracing::error!(
                                    tx = %id,
                                    %key,
                                    subscribers = ?subscribers,
                                    candidates = ?candidates,
                                    connection_count,
                                    request_sender = %request_sender.peer(),
                                    "Cannot handle UPDATE: contract not found locally and no peers to forward to"
                                );
                                return Err(OpError::RingError(RingError::NoCachingPeers(*key)));
                            }
                        }
                    }
                }
                UpdateMsg::SeekNode {
                    id,
                    value,
                    key,
                    related_contracts,
                    target,
                    sender,
                } => {
                    // Check if we have the contract locally
                    let has_contract = match op_manager
                        .notify_contract_handler(ContractHandlerEvent::GetQuery {
                            key: *key,
                            return_contract_code: false,
                        })
                        .await
                    {
                        Ok(ContractHandlerEvent::GetResponse {
                            response: Ok(StoreResponse { state: Some(_), .. }),
                            ..
                        }) => {
                            tracing::debug!(tx = %id, %key, "Contract exists locally for SeekNode UPDATE");
                            true
                        }
                        _ => {
                            tracing::debug!(tx = %id, %key, "Contract not found locally for SeekNode UPDATE");
                            false
                        }
                    };

                    if has_contract {
                        tracing::debug!("Contract found locally - handling UPDATE");
                        let UpdateExecution {
                            value: updated_value,
                            summary: _summary, // summary not used here yet
                            changed,
                        } = update_contract(
                            op_manager,
                            *key,
                            value.clone(),
                            related_contracts.clone(),
                        )
                        .await?;
                        tracing::debug!(
                            tx = %id,
                            "Successfully updated a value for contract {} @ {:?} - update",
                            key,
                            target.location
                        );

                        if !changed {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                "SeekNode update produced no change, stopping propagation"
                            );
                            new_state = None;
                            return_msg = None;
                        } else {
                            // Get broadcast targets
                            let broadcast_to =
                                op_manager.get_broadcast_targets_update(key, &sender.peer());

                            // If no peers to broadcast to, nothing else to do
                            if broadcast_to.is_empty() {
                                tracing::debug!(
                                    tx = %id,
                                    %key,
                                    "No broadcast targets for SeekNode - completing locally"
                                );
                                new_state = None;
                                return_msg = None;
                            } else {
                                // Have peers to broadcast to - use try_to_broadcast
                                match try_to_broadcast(
                                    *id,
                                    true,
                                    op_manager,
                                    self.state,
                                    (broadcast_to, sender.clone()),
                                    *key,
                                    updated_value.clone(),
                                    false,
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
                        }
                    } else {
                        // Contract not found - forward to another peer
                        let self_location = op_manager.ring.connection_manager.own_location();
                        let next_target = op_manager.ring.closest_potentially_caching(
                            key,
                            [&sender.peer(), &self_location.peer()].as_slice(),
                        );

                        if let Some(forward_target) = next_target {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                next_peer = %forward_target.peer(),
                                "Contract not found - forwarding SeekNode to next peer"
                            );

                            // Forward SeekNode to the next peer
                            return_msg = Some(UpdateMsg::SeekNode {
                                id: *id,
                                sender: self_location.clone(),
                                target: forward_target,
                                value: value.clone(),
                                key: *key,
                                related_contracts: related_contracts.clone(),
                            });
                            new_state = None;
                        } else {
                            // No more peers to try - capture context for diagnostics
                            let skip_list = [&sender.peer(), &self_location.peer()];
                            let subscribers = op_manager
                                .ring
                                .subscribers_of(key)
                                .map(|subs| {
                                    subs.value()
                                        .iter()
                                        .map(|loc| format!("{:.8}", loc.peer()))
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();
                            let candidates = op_manager
                                .ring
                                .k_closest_potentially_caching(key, skip_list.as_slice(), 5)
                                .into_iter()
                                .map(|loc| format!("{:.8}", loc.peer()))
                                .collect::<Vec<_>>();
                            let connection_count =
                                op_manager.ring.connection_manager.num_connections();
                            tracing::error!(
                                tx = %id,
                                %key,
                                subscribers = ?subscribers,
                                candidates = ?candidates,
                                connection_count,
                                sender = %sender.peer(),
                                "Cannot handle UPDATE SeekNode: contract not found and no peers to forward to"
                            );
                            return Err(OpError::RingError(RingError::NoCachingPeers(*key)));
                        }
                    }
                }
                UpdateMsg::BroadcastTo {
                    id,
                    key,
                    new_value,
                    sender,
                    target,
                } => {
                    tracing::debug!("Attempting contract value update - BroadcastTo - update");
                    let UpdateExecution {
                        value: updated_value,
                        summary: _summary,
                        changed,
                    } = update_contract(
                        op_manager,
                        *key,
                        new_value.clone(),
                        RelatedContracts::default(),
                    )
                    .await?;
                    tracing::debug!("Contract successfully updated - BroadcastTo - update");

                    if !changed {
                        tracing::debug!(
                            tx = %id,
                            %key,
                            "BroadcastTo update produced no change, ending propagation"
                        );
                        new_state = None;
                        return_msg = None;
                    } else {
                        let broadcast_to =
                            op_manager.get_broadcast_targets_update(key, &sender.peer());

                        tracing::debug!(
                            "Successfully updated a value for contract {} @ {:?} - BroadcastTo - update",
                            key,
                            target.location
                        );

                        match try_to_broadcast(
                            *id,
                            false,
                            op_manager,
                            self.state,
                            (broadcast_to, sender.clone()),
                            *key,
                            updated_value.clone(),
                            true,
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
                }
                UpdateMsg::Broadcasting {
                    id,
                    broadcast_to,
                    broadcasted_to,
                    key,
                    new_value,
                    upstream: _upstream,
                    ..
                } => {
                    let sender = op_manager.ring.connection_manager.own_location();
                    let mut broadcasted_to = *broadcasted_to;

                    let mut broadcasting = Vec::with_capacity(broadcast_to.len());

                    for peer in broadcast_to.iter() {
                        let msg = UpdateMsg::BroadcastTo {
                            id: *id,
                            key: *key,
                            new_value: new_value.clone(),
                            sender: sender.clone(),
                            target: peer.clone(),
                        };
                        let f = conn_manager.send(peer.addr(), msg.into());
                        broadcasting.push(f);
                    }
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

                    let mut incorrect_results = 0;
                    for (peer_num, err) in error_futures {
                        // remove the failed peers in reverse order
                        let peer = broadcast_to.get(peer_num).unwrap();
                        tracing::warn!(
                            "failed broadcasting update change to {} with error {}; dropping connection",
                            peer.peer(),
                            err
                        );
                        // TODO: review this, maybe we should just dropping this subscription
                        conn_manager.drop_connection(peer.addr()).await?;
                        incorrect_results += 1;
                    }

                    broadcasted_to += broadcast_to.len() - incorrect_results;
                    tracing::debug!(
                        "Successfully broadcasted update contract {key} to {broadcasted_to} peers - Broadcasting"
                    );

                    // Subscriber nodes have been notified of the change
                    new_state = None;
                    return_msg = None;
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, stats)
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_to_broadcast(
    id: Transaction,
    last_hop: bool,
    op_manager: &OpManager,
    state: Option<UpdateState>,
    (broadcast_to, upstream): (Vec<PeerKeyLocation>, PeerKeyLocation),
    key: ContractKey,
    new_value: WrappedState,
    is_from_a_broadcasted_to_peer: bool,
) -> Result<(Option<UpdateState>, Option<UpdateMsg>), OpError> {
    let new_state;
    let return_msg;

    match state {
        Some(UpdateState::ReceivedRequest | UpdateState::BroadcastOngoing) => {
            if broadcast_to.is_empty() && !last_hop {
                tracing::debug!(
                    "Empty broadcast list while updating value for contract {} - try_to_broadcast",
                    key
                );

                return_msg = None;
                new_state = None;

                if is_from_a_broadcasted_to_peer {
                    return Ok((new_state, return_msg));
                }
            } else if !broadcast_to.is_empty() {
                tracing::debug!(
                    "Callback to start broadcasting to other nodes. List size {}",
                    broadcast_to.len()
                );
                new_state = Some(UpdateState::BroadcastOngoing);

                return_msg = Some(UpdateMsg::Broadcasting {
                    id,
                    new_value,
                    broadcasted_to: 0,
                    broadcast_to,
                    key,
                    upstream,
                    sender: op_manager.ring.connection_manager.own_location(),
                });
            } else {
                new_state = None;
                return_msg = None;
            }
        }
        _ => return Err(OpError::invalid_transition(id)),
    };

    Ok((new_state, return_msg))
}

impl OpManager {
    pub(crate) fn get_broadcast_targets_update(
        &self,
        key: &ContractKey,
        sender: &PeerId,
    ) -> Vec<PeerKeyLocation> {
        let subscribers = self
            .ring
            .subscribers_of(key)
            .map(|subs| {
                let self_peer = self.ring.connection_manager.get_peer_key();
                let allow_self = self_peer.as_ref().map(|me| me == sender).unwrap_or(false);
                subs.value()
                    .iter()
                    .filter(|pk| {
                        // Allow the sender (or ourselves) to stay in the broadcast list when we're
                        // originating the UPDATE so local auto-subscribes still receive events.
                        let is_sender = &pk.peer() == sender;
                        let is_self = self_peer.as_ref() == Some(&pk.peer());
                        if is_sender || is_self {
                            allow_self
                        } else {
                            true
                        }
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // Trace update propagation for debugging
        if !subscribers.is_empty() {
            tracing::info!(
                "UPDATE_PROPAGATION: contract={:.8} from={} targets={} count={}",
                key,
                sender,
                subscribers
                    .iter()
                    .map(|s| format!("{:.8}", s.peer()))
                    .collect::<Vec<_>>()
                    .join(","),
                subscribers.len()
            );
        } else {
            let own_peer = self.ring.connection_manager.get_peer_key();
            let skip_slice = std::slice::from_ref(sender);
            let fallback_candidates = self
                .ring
                .k_closest_potentially_caching(key, skip_slice, 5)
                .into_iter()
                .map(|candidate| format!("{:.8}", candidate.peer()))
                .collect::<Vec<_>>();

            tracing::warn!(
                "UPDATE_PROPAGATION: contract={:.8} from={} NO_TARGETS - update will not propagate (self={:?}, fallback_candidates={:?})",
                key,
                sender,
                own_peer.map(|p| format!("{:.8}", p)),
                fallback_candidates
            );
        }

        subscribers
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<UpdateState>,
    return_msg: Option<UpdateMsg>,
    stats: Option<UpdateStats>,
) -> Result<super::OperationResult, OpError> {
    let output_op = state.map(|op| UpdateOp {
        id,
        state: Some(op),
        stats,
    });
    let state = output_op.map(OpEnum::Update);
    Ok(OperationResult {
        return_msg: return_msg.map(NetMessage::from),
        target_addr: None,
        state,
    })
}

async fn update_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
) -> Result<UpdateExecution, OpError> {
    let previous_state = match op_manager
        .notify_contract_handler(ContractHandlerEvent::GetQuery {
            key,
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

    let update_data = UpdateData::State(State::from(state.clone()));
    match op_manager
        .notify_contract_handler(ContractHandlerEvent::UpdateQuery {
            key,
            data: update_data,
            related_contracts,
        })
        .await
    {
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Ok(new_val),
        }) => {
            let new_bytes = State::from(new_val.clone()).into_bytes();
            let summary = StateSummary::from(new_bytes.clone());
            let changed = match previous_state.as_ref() {
                Some(prev_state) => {
                    let prev_bytes = State::from(prev_state.clone()).into_bytes();
                    prev_bytes != new_bytes
                }
                None => true,
            };

            // NOTE: change detection currently relies on byte-level equality. Contracts that
            // produce semantically identical states with different encodings must normalize their
            // serialization (e.g., sort map keys) to avoid redundant broadcasts.

            Ok(UpdateExecution {
                value: new_val,
                summary,
                changed,
            })
        }
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Err(err),
        }) => {
            tracing::error!(%key, %err, "Failed to update contract value");
            Err(OpError::UnexpectedOpState)
        }
        Ok(ContractHandlerEvent::UpdateNoChange { .. }) => {
            let resolved_state = match previous_state {
                Some(prev_state) => prev_state,
                None => {
                    match op_manager
                        .notify_contract_handler(ContractHandlerEvent::GetQuery {
                            key,
                            return_contract_code: false,
                        })
                        .await
                    {
                        Ok(ContractHandlerEvent::GetResponse {
                            response:
                                Ok(StoreResponse {
                                    state: Some(current),
                                    ..
                                }),
                            ..
                        }) => current,
                        Ok(other) => {
                            tracing::debug!(
                                ?other,
                                %key,
                                "Fallback fetch for UpdateNoChange returned no state; using requested state"
                            );
                            state.clone()
                        }
                        Err(err) => {
                            tracing::debug!(
                                %key,
                                %err,
                                "Fallback fetch for UpdateNoChange failed; using requested state"
                            );
                            state.clone()
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
            tracing::error!(?other, %key, "Unexpected event from contract handler during update");
            Err(OpError::UnexpectedOpState)
        }
    }
}

/// This will be called from the node when processing an open request
// todo: new_state should be a delta when possible!
pub(crate) fn start_op(
    key: ContractKey,
    new_state: WrappedState,
    related_contracts: RelatedContracts<'static>,
) -> UpdateOp {
    let contract_location = Location::from(&key);
    tracing::debug!(%contract_location, %key, "Requesting update");
    let id = Transaction::new::<UpdateMsg>();
    // let payload_size = contract.data().len();

    let state = Some(UpdateState::PrepareRequest {
        key,
        related_contracts,
        value: new_state,
    });

    UpdateOp {
        id,
        state,
        stats: Some(UpdateStats { target: None }),
    }
}

/// This will be called from the node when processing an open request with a specific transaction ID
pub(crate) fn start_op_with_id(
    key: ContractKey,
    new_state: WrappedState,
    related_contracts: RelatedContracts<'static>,
    id: Transaction,
) -> UpdateOp {
    let contract_location = Location::from(&key);
    tracing::debug!(%contract_location, %key, "Requesting update with transaction ID {}", id);
    // let payload_size = contract.data().len();

    let state = Some(UpdateState::PrepareRequest {
        key,
        related_contracts,
        value: new_state,
    });

    UpdateOp {
        id,
        state,
        stats: Some(UpdateStats { target: None }),
    }
}

/// Entry point from node to operations logic
pub(crate) async fn request_update(
    op_manager: &OpManager,
    mut update_op: UpdateOp,
) -> Result<(), OpError> {
    // Extract the key and check if we need to handle this locally
    let (key, value, related_contracts) = if let Some(UpdateState::PrepareRequest {
        key,
        value,
        related_contracts,
    }) = update_op.state.take()
    {
        (key, value, related_contracts)
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    let sender = op_manager.ring.connection_manager.own_location();

    // the initial request must provide:
    // - a peer as close as possible to the contract location
    // - and the value to update
    let target_from_subscribers = if let Some(subscribers) = op_manager.ring.subscribers_of(&key) {
        // Clone and filter out self from subscribers to prevent self-targeting
        let mut filtered_subscribers: Vec<_> = subscribers
            .iter()
            .filter(|sub| sub.peer() != sender.peer())
            .cloned()
            .collect();
        filtered_subscribers.pop()
    } else {
        None
    };

    let target = if let Some(remote_subscriber) = target_from_subscribers {
        remote_subscriber
    } else {
        // Find the best peer to send the update to
        let remote_target = op_manager
            .ring
            .closest_potentially_caching(&key, [sender.peer().clone()].as_slice());

        if let Some(target) = remote_target {
            // Subscribe to the contract
            op_manager
                .ring
                .add_subscriber(&key, sender.clone())
                .map_err(|_| RingError::NoCachingPeers(key))?;

            target
        } else {
            // No remote peers available, handle locally
            tracing::debug!(
                "UPDATE: No remote peers available for contract {}, handling locally",
                key
            );

            let id = update_op.id;

            // Check if we're seeding or subscribed to this contract
            let is_seeding = op_manager.ring.is_seeding_contract(&key);
            let has_subscribers = op_manager.ring.subscribers_of(&key).is_some();
            let should_handle_update = is_seeding || has_subscribers;

            if !should_handle_update {
                tracing::error!(
                    "UPDATE: Cannot update contract {} on isolated node - contract not seeded and no subscribers",
                    key
                );
                return Err(OpError::RingError(RingError::NoCachingPeers(key)));
            }

            // Update the contract locally. This path is reached when:
            // 1. No remote peers are available (isolated node OR no suitable caching peers)
            // 2. Either seeding the contract OR has subscribers (verified above)
            // Note: This handles both truly isolated nodes and nodes where subscribers exist
            // but no suitable remote caching peer was found.
            let UpdateExecution {
                value: updated_value,
                summary,
                changed,
            } = update_contract(op_manager, key, value, related_contracts).await?;

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

            // Check if there are any subscribers to broadcast to
            let broadcast_to = op_manager.get_broadcast_targets_update(&key, &sender.peer());

            deliver_update_result(op_manager, id, key, summary.clone()).await?;

            if broadcast_to.is_empty() {
                // No subscribers - operation complete
                tracing::debug!(
                    tx = %id,
                    %key,
                    "No broadcast targets, completing UPDATE operation locally"
                );
                return Ok(());
            } else {
                // There are subscribers - broadcast the update
                tracing::debug!(
                    tx = %id,
                    %key,
                    subscribers = broadcast_to.len(),
                    "Broadcasting UPDATE to subscribers on isolated node"
                );

                let broadcast_state = Some(UpdateState::ReceivedRequest);

                let (_new_state, return_msg) = try_to_broadcast(
                    id,
                    false,
                    op_manager,
                    broadcast_state,
                    (broadcast_to, sender.clone()),
                    key,
                    updated_value,
                    false,
                )
                .await?;

                if let Some(msg) = return_msg {
                    op_manager
                        .to_event_listener
                        .notifications_sender()
                        .send(Either::Left(NetMessage::from(msg)))
                        .await
                        .map_err(|error| {
                            tracing::error!(
                                tx = %id,
                                %error,
                                "Failed to enqueue UPDATE broadcast message"
                            );
                            OpError::NotificationError
                        })?;
                }

                return Ok(());
            }
        }
    };

    // Normal case: we found a remote target
    // Apply the update locally first to ensure the initiating peer has the updated state
    let id = update_op.id;

    tracing::debug!(
        tx = %id,
        %key,
        target_peer = %target.peer(),
        "Applying UPDATE locally before forwarding to target peer"
    );

    // Apply update locally - this ensures the initiating peer serves the updated state
    // even if the remote UPDATE times out or fails
    let UpdateExecution {
        value: updated_value,
        summary,
        changed: _changed,
    } = update_contract(op_manager, key, value.clone(), related_contracts.clone())
        .await
        .map_err(|e| {
            tracing::error!(
                tx = %id,
                %key,
                error = %e,
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

    deliver_update_result(op_manager, id, key, summary.clone()).await?;
    let msg = UpdateMsg::RequestUpdate {
        id,
        key,
        sender,
        related_contracts,
        target,
        value: updated_value, // Send the updated value, not the original
    };

    op_manager
        .to_event_listener
        .notifications_sender()
        .send(Either::Left(NetMessage::from(msg)))
        .await
        .map_err(|error| {
            tracing::error!(
                tx = %id,
                %error,
                "Failed to enqueue UPDATE RequestUpdate message"
            );
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
    };

    let host_result = op.to_host_result();

    op_manager
        .result_router_tx
        .send((id, host_result))
        .await
        .map_err(|error| {
            tracing::error!(
                tx = %id,
                %error,
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
            %error,
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
    use std::{borrow::Borrow, fmt::Display};

    use freenet_stdlib::prelude::{ContractKey, RelatedContracts, WrappedState};
    use serde::{Deserialize, Serialize};

    use crate::{
        message::{InnerMessage, Transaction},
        ring::{Location, PeerKeyLocation},
    };

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum UpdateMsg {
        RequestUpdate {
            id: Transaction,
            key: ContractKey,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
        },
        AwaitUpdate {
            id: Transaction,
        },
        SeekNode {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            value: WrappedState,
            key: ContractKey,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
        },
        /// Internal node instruction that  a change (either a first time insert or an update).
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_value: WrappedState,
            //contract: ContractContainer,
            upstream: PeerKeyLocation,
            sender: PeerKeyLocation,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            target: PeerKeyLocation,
        },
    }

    impl InnerMessage for UpdateMsg {
        fn id(&self) -> &Transaction {
            match self {
                UpdateMsg::RequestUpdate { id, .. } => id,
                UpdateMsg::AwaitUpdate { id, .. } => id,
                UpdateMsg::SeekNode { id, .. } => id,
                UpdateMsg::Broadcasting { id, .. } => id,
                UpdateMsg::BroadcastTo { id, .. } => id,
            }
        }

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            match self {
                UpdateMsg::RequestUpdate { target, .. } => Some(target),
                UpdateMsg::SeekNode { target, .. } => Some(target),
                UpdateMsg::BroadcastTo { target, .. } => Some(target),
                _ => None,
            }
        }

        fn requested_location(&self) -> Option<crate::ring::Location> {
            match self {
                UpdateMsg::RequestUpdate { key, .. } => Some(Location::from(key.id())),
                UpdateMsg::SeekNode { key, .. } => Some(Location::from(key.id())),
                UpdateMsg::Broadcasting { key, .. } => Some(Location::from(key.id())),
                UpdateMsg::BroadcastTo { key, .. } => Some(Location::from(key.id())),
                _ => None,
            }
        }
    }

    impl UpdateMsg {
        #[allow(dead_code)]
        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::RequestUpdate { sender, .. } => Some(sender),
                Self::SeekNode { sender, .. } => Some(sender),
                Self::BroadcastTo { sender, .. } => Some(sender),
                _ => None,
            }
        }
    }

    impl Display for UpdateMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                UpdateMsg::RequestUpdate { id, .. } => write!(f, "RequestUpdate(id: {id})"),
                UpdateMsg::AwaitUpdate { id } => write!(f, "AwaitUpdate(id: {id})"),
                UpdateMsg::SeekNode { id, .. } => write!(f, "SeekNode(id: {id})"),
                UpdateMsg::Broadcasting { id, .. } => write!(f, "Broadcasting(id: {id})"),
                UpdateMsg::BroadcastTo { id, .. } => write!(f, "BroadcastTo(id: {id})"),
            }
        }
    }
}

#[derive(Debug)]
pub enum UpdateState {
    ReceivedRequest,
    Finished {
        key: ContractKey,
        summary: StateSummary<'static>,
    },
    PrepareRequest {
        key: ContractKey,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
    },
    BroadcastOngoing,
}
