// TODO: complete update logic in the network
use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;
use std::time::Duration;

pub(crate) use self::messages::UpdateMsg;
use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::contract::{ContractHandlerEvent, ExecutorError};
use crate::message::{InnerMessage, NetMessage, Transaction};
use crate::node::IsOperationCompleted;
use crate::ring::{Location, PeerKeyLocation, RingError};
use crate::{
    client_events::HostResult,
    node::{NetworkBridge, OpManager, PeerId},
};

const MAX_RETRIES: usize = 10;
const BASE_DELAY_MS: u64 = 100;
const MAX_DELAY_MS: u64 = 5000;

pub(crate) struct UpdateOp {
    pub id: Transaction,
    pub(crate) state: Option<UpdateState>,
    stats: Option<UpdateStats>,
}

impl UpdateOp {
    pub fn outcome(&self) -> OpOutcome {
        OpOutcome::Irrelevant
    }

    pub fn finalized(&self) -> bool {
        matches!(self.state, None | Some(UpdateState::Finished { .. }))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(UpdateState::Finished { key, summary }) = &self.state {
            Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::UpdateResponse {
                    key: *key,
                    summary: summary.clone(),
                },
            ))
        } else {
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
    ) -> Result<super::OpInitialization<Self>, OpError> {
        let mut sender: Option<PeerId> = None;
        if let Some(peer_key_loc) = msg.sender().cloned() {
            sender = Some(peer_key_loc.peer);
        };
        let tx = *msg.id();
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Update(update_op))) => {
                // Check if we need to retry an AwaitingResponse state
                if let Some(UpdateState::AwaitingResponse { key, upstream, retry_count }) = &update_op.state {
                    if let UpdateMsg::AwaitUpdate { .. } = msg {
                        if *retry_count < MAX_RETRIES {
                            // This is a retry for an AwaitingResponse state
                            tracing::debug!(
                                "Processing retry for AwaitingResponse state for contract {} (retry {}/{})",
                                key,
                                retry_count + 1,
                                MAX_RETRIES
                            );
                            
                            let new_op = Self {
                                state: Some(UpdateState::AwaitingResponse {
                                    key: *key,
                                    upstream: upstream.clone(),
                                    retry_count: retry_count + 1,
                                }),
                                id: tx,
                                stats: update_op.stats.clone(),
                            };
                            
                            return Ok(OpInitialization {
                                op: new_op,
                                sender,
                            });
                        } else {
                            tracing::warn!(
                                "Maximum retries ({}) reached for AwaitingResponse state for contract {}",
                                MAX_RETRIES,
                                key
                            );
                        }
                    }
                }
                
                Ok(OpInitialization {
                    op: update_op,
                    sender,
                })
                // was an existing operation, other peer messaged back
            }
            Ok(Some(op)) => {
                let _ = op_manager.push(tx, op).await;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                // new request to get a value for a contract, initialize the machine
                tracing::debug!(tx = %tx, sender = ?sender, "initializing new op");
                Ok(OpInitialization {
                    op: Self {
                        state: Some(UpdateState::ReceivedRequest { retry_count: 0 }),
                        id: tx,
                        stats: None, // don't care about stats in target peers
                    },
                    sender,
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
        // _client_id: Option<ClientId>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<super::OperationResult, OpError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let return_msg;
            let new_state;
            let stats = self.stats;

            if let Some(UpdateState::AwaitingResponse { key, upstream, retry_count }) = &self.state {
                if let UpdateMsg::AwaitUpdate { .. } = input {
                    if *retry_count < MAX_RETRIES {
                        let delay_ms = std::cmp::min(
                            BASE_DELAY_MS * (1 << retry_count), // Exponential backoff: BASE_DELAY_MS * 2^retry_count
                            MAX_DELAY_MS,
                        );
                        
                        tracing::debug!(
                            "Retrying update request for contract {} due to timeout (retry {}/{}), delaying for {}ms",
                            key,
                            retry_count + 1,
                            MAX_RETRIES,
                            delay_ms
                        );
                        
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        
                        if let Some(target) = upstream {
                            let sender = op_manager.ring.connection_manager.own_location();
                            
                            let msg = UpdateMsg::SeekNode {
                                id: self.id,
                                sender: sender.clone(),
                                target: target.clone(),
                                value: WrappedState::default(), // We don't have the original value, but the target should have it
                                key: *key,
                                related_contracts: RelatedContracts::default(),
                            };
                            
                            match conn_manager.send(&target.peer, msg.into()).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        "Successfully sent retry update request for contract {} (retry {}/{})",
                                        key,
                                        retry_count + 1,
                                        MAX_RETRIES
                                    );
                                    
                                    new_state = Some(UpdateState::AwaitingResponse {
                                        key: *key,
                                        upstream: Some(target.clone()),
                                        retry_count: retry_count + 1,
                                    });
                                    
                                    return_msg = None;
                                    
                                    return Ok(OperationResult {
                                        return_msg: None,
                                        state: Some(OpEnum::Update(UpdateOp {
                                            id: self.id,
                                            state: new_state,
                                            stats,
                                        })),
                                    });
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        "Failed to send retry update request for contract {}: {} (retry {}/{})",
                                        key,
                                        err,
                                        retry_count + 1,
                                        MAX_RETRIES
                                    );
                                    
                                    let retry_op = UpdateOp {
                                        id: self.id,
                                        state: Some(UpdateState::AwaitingResponse {
                                            key: *key,
                                            upstream: upstream.clone(),
                                            retry_count: retry_count + 1,
                                        }),
                                        stats,
                                    };
                                    
                                    op_manager
                                        .notify_op_change(
                                            NetMessage::from(UpdateMsg::AwaitUpdate { id: self.id }),
                                            OpEnum::Update(retry_op),
                                        )
                                        .await?;
                                    
                                    return Err(OpError::StatePushed);
                                }
                            }
                        } else {
                            // This is a client-initiated update, we need to find a new target
                            let sender = op_manager.ring.connection_manager.own_location();
                            
                            let target = if let Some(location) = op_manager.ring.subscribers_of(key) {
                                location
                                    .clone()
                                    .pop()
                                    .ok_or(OpError::RingError(RingError::NoLocation))?
                            } else {
                                let closest = op_manager
                                    .ring
                                    .closest_potentially_caching(key, [sender.peer.clone()].as_slice())
                                    .into_iter()
                                    .next()
                                    .ok_or_else(|| RingError::EmptyRing)?;
                                
                                closest
                            };
                            
                            let msg = UpdateMsg::SeekNode {
                                id: self.id,
                                sender: sender.clone(),
                                target: target.clone(),
                                value: WrappedState::default(), // We don't have the original value, but the target should have it
                                key: *key,
                                related_contracts: RelatedContracts::default(),
                            };
                            
                            match conn_manager.send(&target.peer, msg.into()).await {
                                Ok(_) => {
                                    tracing::debug!(
                                        "Successfully sent retry update request to new target for contract {} (retry {}/{})",
                                        key,
                                        retry_count + 1,
                                        MAX_RETRIES
                                    );
                                    
                                    new_state = Some(UpdateState::AwaitingResponse {
                                        key: *key,
                                        upstream: None,
                                        retry_count: retry_count + 1,
                                    });
                                    
                                    return_msg = None;
                                    
                                    return Ok(OperationResult {
                                        return_msg: None,
                                        state: Some(OpEnum::Update(UpdateOp {
                                            id: self.id,
                                            state: new_state,
                                            stats,
                                        })),
                                    });
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        "Failed to send retry update request to new target for contract {}: {} (retry {}/{})",
                                        key,
                                        err,
                                        retry_count + 1,
                                        MAX_RETRIES
                                    );
                                    
                                    let retry_op = UpdateOp {
                                        id: self.id,
                                        state: Some(UpdateState::AwaitingResponse {
                                            key: *key,
                                            upstream: None,
                                            retry_count: retry_count + 1,
                                        }),
                                        stats,
                                    };
                                    
                                    op_manager
                                        .notify_op_change(
                                            NetMessage::from(UpdateMsg::AwaitUpdate { id: self.id }),
                                            OpEnum::Update(retry_op),
                                        )
                                        .await?;
                                    
                                    return Err(OpError::StatePushed);
                                }
                            }
                        }
                    } else {
                        tracing::warn!(
                            "Maximum retries ({}) reached for AwaitingResponse state for contract {}",
                            MAX_RETRIES,
                            key
                        );
                        
                        return Err(OpError::MaxRetriesExceeded(self.id, crate::message::TransactionType::Update));
                    }
                }
            }

            match input {
                UpdateMsg::RequestUpdate {
                    id,
                    key,
                    target,
                    related_contracts,
                    value,
                } => {
                    let sender = op_manager.ring.connection_manager.own_location();

                    tracing::debug!(
                        "Requesting update for contract {} from {} to {}",
                        key,
                        sender.peer,
                        target.peer
                    );

                    return_msg = Some(UpdateMsg::SeekNode {
                        id: *id,
                        sender,
                        target: target.clone(),
                        value: value.clone(),
                        key: *key,
                        related_contracts: related_contracts.clone(),
                    });

                    // no changes to state yet, still in AwaitResponse state
                    new_state = self.state;
                }
                UpdateMsg::SeekNode {
                    id,
                    value,
                    key,
                    related_contracts,
                    target,
                    sender,
                } => {
                    let is_subscribed_contract = op_manager.ring.is_seeding_contract(key);

                    tracing::debug!(
                        tx = %id,
                        %key,
                        target = %target.peer,
                        sender = %sender.peer,
                        "Updating contract at target peer",
                    );

                    let broadcast_to = op_manager.get_broadcast_targets_update(key, &sender.peer);

                    if is_subscribed_contract {
                        tracing::debug!("Peer is subscribed to contract. About to update it");
                        update_contract(op_manager, *key, value.clone(), related_contracts.clone())
                            .await?;
                        tracing::debug!(
                            tx = %id,
                            "Successfully updated a value for contract {} @ {:?} - update",
                            key,
                            target.location
                        );
                    } else {
                        tracing::debug!("contract not found in this peer. Should throw an error");
                        return Err(OpError::RingError(RingError::NoCachingPeers(*key)));
                    }

                    match try_to_broadcast(
                        *id,
                        true,
                        op_manager,
                        self.state,
                        (broadcast_to, sender.clone()),
                        *key,
                        value.clone(),
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
                UpdateMsg::BroadcastTo {
                    id,
                    key,
                    new_value,
                    sender,
                    target,
                } => {
                    if let Some(UpdateState::AwaitingResponse { .. }) = self.state {
                        tracing::debug!("Trying to broadcast to a peer that was the initiator of the op because it received the client request, or is in the middle of a seek node process");
                        return Err(OpError::StatePushed);
                    }

                    tracing::debug!("Attempting contract value update - BroadcastTo - update");
                    let new_value = update_contract(
                        op_manager,
                        *key,
                        new_value.clone(),
                        RelatedContracts::default(),
                    )
                    .await?;
                    tracing::debug!("Contract successfully updated - BroadcastTo - update");

                    let broadcast_to = op_manager.get_broadcast_targets_update(key, &sender.peer);

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
                        new_value,
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
                UpdateMsg::Broadcasting {
                    id,
                    broadcast_to,
                    broadcasted_to,
                    key,
                    new_value,
                    upstream,
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
                        let f = conn_manager.send(&peer.peer, msg.into());
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
                    let mut failed_peers = Vec::new();

                    for (peer_num, err) in error_futures {
                        let peer = broadcast_to.get(peer_num).unwrap();
                        tracing::warn!(
                            "failed broadcasting update change to {} with error {}; will retry",
                            peer.peer,
                            err
                        );

                        failed_peers.push(peer.clone());
                        incorrect_results += 1;
                    }

                    if !failed_peers.is_empty() && incorrect_results > 0 {
                        tracing::debug!(
                            "Setting up retry for {} failed peers out of {}",
                            incorrect_results,
                            broadcast_to.len()
                        );

                        new_state = Some(UpdateState::RetryingBroadcast {
                            key: *key,
                            retry_count: 0,
                            failed_peers,
                            upstream: upstream.clone(),
                            new_value: new_value.clone(),
                        });

                        let op = UpdateOp {
                            id: *id,
                            state: new_state.clone(),
                            stats: None,
                        };

                        op_manager
                            .notify_op_change(
                                NetMessage::from(UpdateMsg::AwaitUpdate { id: *id }),
                                OpEnum::Update(op),
                            )
                            .await?;

                        return Err(OpError::StatePushed);
                    }

                    broadcasted_to += broadcast_to.len() - incorrect_results;
                    tracing::debug!(
                        "Successfully broadcasted update contract {key} to {broadcasted_to} peers - Broadcasting"
                    );

                    let raw_state = State::from(new_value.clone());

                    let summary = StateSummary::from(raw_state.into_bytes());

                    // Subscriber nodes have been notified of the change, the operation is complete
                    return_msg = Some(UpdateMsg::SuccessfulUpdate {
                        id: *id,
                        target: upstream.clone(),
                        summary,
                        sender: sender.clone(),
                        key: *key,
                    });

                    new_state = None;
                }
                UpdateMsg::SuccessfulUpdate { id, summary, .. } => {
                    match self.state {
                        Some(UpdateState::AwaitingResponse {
                            key,
                            upstream,
                            retry_count: _,
                        }) => {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                this_peer = ?op_manager.ring.connection_manager.get_peer_key(),
                                "Peer completed contract value update - SuccessfulUpdate",
                            );

                            new_state = Some(UpdateState::Finished {
                                key,
                                summary: summary.clone(),
                            });
                            if let Some(upstream) = upstream {
                                return_msg = Some(UpdateMsg::SuccessfulUpdate {
                                    id: *id,
                                    target: upstream,
                                    summary: summary.clone(),
                                    key,
                                    sender: op_manager.ring.connection_manager.own_location(),
                                });
                            } else {
                                // this means op finalized
                                return_msg = None;
                            }
                        }
                        _ => {
                            tracing::error!(
                                state = ?self.state,
                                "invalid transition in UpdateMsg::SuccessfulUpdate -> match self.state"
                            );

                            return Err(OpError::invalid_transition(self.id));
                        }
                    };
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
        Some(
            UpdateState::ReceivedRequest { retry_count }
            | UpdateState::BroadcastOngoing { retry_count },
        ) => {
            if broadcast_to.is_empty() && !last_hop {
                // broadcast complete
                tracing::debug!(
                    "Empty broadcast list while updating value for contract {} - try_to_broadcast",
                    key
                );

                return_msg = None;

                if is_from_a_broadcasted_to_peer {
                    new_state = None;
                    return Ok((new_state, return_msg));
                }

                // means the whole tx finished so can return early
                new_state = Some(UpdateState::AwaitingResponse {
                    key,
                    upstream: Some(upstream),
                    retry_count: 0,
                });
            } else if !broadcast_to.is_empty() {
                tracing::debug!(
                    "Callback to start broadcasting to other nodes. List size {}",
                    broadcast_to.len()
                );
                new_state = Some(UpdateState::BroadcastOngoing { retry_count });

                return_msg = Some(UpdateMsg::Broadcasting {
                    id,
                    new_value,
                    broadcasted_to: 0,
                    broadcast_to,
                    key,
                    upstream,
                    sender: op_manager.ring.connection_manager.own_location(),
                });

                let op = UpdateOp {
                    id,
                    state: new_state,
                    stats: None,
                };
                op_manager
                    .notify_op_change(NetMessage::from(return_msg.unwrap()), OpEnum::Update(op))
                    .await?;
                return Err(OpError::StatePushed);
            } else {
                let raw_state = State::from(new_value);

                let summary = StateSummary::from(raw_state.into_bytes());

                new_state = None;
                return_msg = Some(UpdateMsg::SuccessfulUpdate {
                    id,
                    target: upstream,
                    summary,
                    key,
                    sender: op_manager.ring.connection_manager.own_location(),
                });
            }
        }
        Some(UpdateState::RetryingBroadcast {
            key,
            retry_count,
            failed_peers,
            upstream: retry_upstream,
            new_value: retry_value,
        }) => {
            if retry_count >= MAX_RETRIES {
                tracing::warn!(
                    "Maximum retries ({}) reached for broadcasting update to contract {}",
                    MAX_RETRIES,
                    key
                );

                let raw_state = State::from(retry_value);
                let summary = StateSummary::from(raw_state.into_bytes());

                new_state = None;
                return_msg = Some(UpdateMsg::SuccessfulUpdate {
                    id,
                    target: retry_upstream,
                    summary,
                    key,
                    sender: op_manager.ring.connection_manager.own_location(),
                });
            } else {
                let delay_ms = std::cmp::min(
                    BASE_DELAY_MS * (1 << retry_count), // Exponential backoff: BASE_DELAY_MS * 2^retry_count
                    MAX_DELAY_MS,
                );

                tracing::debug!(
                    "Retrying broadcast for contract {} (retry {}/{}), delaying for {}ms",
                    key,
                    retry_count + 1,
                    MAX_RETRIES,
                    delay_ms
                );

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;

                let sender = op_manager.ring.connection_manager.own_location();

                let mut failed_broadcasts = Vec::new();

                for (i, peer) in failed_peers.iter().enumerate() {
                    let msg = UpdateMsg::BroadcastTo {
                        id,
                        key,
                        new_value: retry_value.clone(),
                        sender: sender.clone(),
                        target: peer.clone(),
                    };

                    match op_manager
                        .notify_op_change(
                            NetMessage::from(msg),
                            OpEnum::Update(UpdateOp {
                                id,
                                state: Some(UpdateState::RetryingBroadcast {
                                    key,
                                    retry_count,
                                    failed_peers: failed_peers.clone(),
                                    upstream: retry_upstream.clone(),
                                    new_value: retry_value.clone(),
                                }),
                                stats: None,
                            }),
                        )
                        .await
                    {
                        Ok(_) => {
                            tracing::debug!("Successfully sent retry broadcast to {}", peer.peer);
                        }
                        Err(err) => {
                            tracing::warn!(
                                "Failed to send retry broadcast to {}: {}",
                                peer.peer,
                                err
                            );
                            failed_broadcasts.push((i, err));
                        }
                    }
                }

                let mut still_failed_peers = Vec::new();
                let incorrect_results = failed_broadcasts.len();

                for (peer_num, err) in failed_broadcasts {
                    let peer = failed_peers.get(peer_num).unwrap();
                    tracing::warn!(
                        "Failed broadcasting update change to {} with error {} (retry {}/{})",
                        peer.peer,
                        err,
                        retry_count + 1,
                        MAX_RETRIES
                    );

                    still_failed_peers.push(peer.clone());
                }

                let successful_broadcasts = failed_peers.len() - incorrect_results;
                tracing::debug!(
                    "Successfully broadcasted update contract {key} to {successful_broadcasts} peers on retry {}/{}",
                    retry_count + 1,
                    MAX_RETRIES
                );

                if still_failed_peers.is_empty() {
                    let raw_state = State::from(retry_value);
                    let summary = StateSummary::from(raw_state.into_bytes());

                    new_state = None;
                    return_msg = Some(UpdateMsg::SuccessfulUpdate {
                        id,
                        target: retry_upstream,
                        summary,
                        key,
                        sender: op_manager.ring.connection_manager.own_location(),
                    });
                } else {
                    new_state = Some(UpdateState::RetryingBroadcast {
                        key,
                        retry_count: retry_count + 1,
                        failed_peers: still_failed_peers,
                        upstream: retry_upstream,
                        new_value: retry_value,
                    });

                    return_msg = None;

                    let op = UpdateOp {
                        id,
                        state: new_state.clone(),
                        stats: None,
                    };

                    op_manager
                        .notify_op_change(
                            NetMessage::from(UpdateMsg::AwaitUpdate { id }),
                            OpEnum::Update(op),
                        )
                        .await?;

                    return Err(OpError::StatePushed);
                }
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
        state,
    })
}

async fn update_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
) -> Result<WrappedState, OpError> {
    let update_data = UpdateData::Delta(StateDelta::from(state.as_ref().to_vec()));
    tracing::debug!("Using Delta update for contract {}", key);

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
        }) => Ok(new_val),
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Err(_rr),
        }) => {
            tracing::error!("Failed to update contract value");
            Err(OpError::UnexpectedOpState)
        }
        Err(err) => Err(err.into()),
        Ok(_) => Err(OpError::UnexpectedOpState),
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

/// Entry point from node to operations logic
pub(crate) async fn request_update(
    op_manager: &OpManager,
    mut update_op: UpdateOp,
) -> Result<(), OpError> {
    let (key, state_type) = match &update_op.state {
        Some(UpdateState::PrepareRequest { key, .. }) => (key, "PrepareRequest"),
        Some(UpdateState::RetryingRequest { key, retry_count, .. }) => {
            if *retry_count >= MAX_RETRIES {
                tracing::warn!(
                    "Maximum retries ({}) reached for initial update request to contract {}",
                    MAX_RETRIES,
                    key
                );
                return Err(OpError::MaxRetriesExceeded(update_op.id, crate::message::TransactionType::Update));
            }
            (key, "RetryingRequest")
        }
        _ => return Err(OpError::UnexpectedOpState),
    };

    let sender = op_manager.ring.connection_manager.own_location();

    // the initial request must provide:
    // - a peer as close as possible to the contract location
    // - and the value to update
    let target = if let Some(location) = op_manager.ring.subscribers_of(key) {
        location
            .clone()
            .pop()
            .ok_or(OpError::RingError(RingError::NoLocation))?
    } else {
        let closest = op_manager
            .ring
            .closest_potentially_caching(key, [sender.peer.clone()].as_slice())
            .into_iter()
            .next()
            .ok_or_else(|| RingError::EmptyRing)?;

        op_manager
            .ring
            .add_subscriber(key, sender)
            .map_err(|_| RingError::NoCachingPeers(*key))?;

        closest
    };

    let id = update_op.id;
    if let Some(stats) = &mut update_op.stats {
        stats.target = Some(target.clone());
    }

    match update_op.state {
        Some(UpdateState::PrepareRequest {
            key,
            value,
            related_contracts,
        }) => {
            let new_state = Some(UpdateState::AwaitingResponse {
                key,
                upstream: None,
                retry_count: 0,
            });
            let msg = UpdateMsg::RequestUpdate {
                id,
                key,
                related_contracts,
                target: target.clone(),
                value: value.clone(),
            };

            let op = UpdateOp {
                state: new_state,
                id,
                stats: update_op.stats,
            };

            match op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Update(op))
                .await
            {
                Ok(_) => {
                    tracing::debug!("Successfully sent initial update request for contract {}", key);
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to send initial update request for contract {}: {}. Will retry.",
                        key,
                        err
                    );
                    
                    let retry_state = Some(UpdateState::RetryingRequest {
                        key,
                        target,
                        related_contracts,
                        value,
                        retry_count: 0,
                    });
                    
                    let retry_op = UpdateOp {
                        state: retry_state,
                        id,
                        stats: update_op.stats,
                    };
                    
                    op_manager
                        .notify_op_change(
                            NetMessage::from(UpdateMsg::AwaitUpdate { id }),
                            OpEnum::Update(retry_op),
                        )
                        .await?;
                }
            }
        }
        Some(UpdateState::RetryingRequest {
            key,
            target: retry_target,
            related_contracts,
            value,
            retry_count,
        }) => {
            let delay_ms = std::cmp::min(
                BASE_DELAY_MS * (1 << retry_count), // Exponential backoff: BASE_DELAY_MS * 2^retry_count
                MAX_DELAY_MS,
            );
            
            tracing::debug!(
                "Retrying initial update request for contract {} (retry {}/{}), delaying for {}ms",
                key,
                retry_count + 1,
                MAX_RETRIES,
                delay_ms
            );
            
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            
            let new_state = Some(UpdateState::AwaitingResponse {
                key,
                upstream: None,
                retry_count: 0,
            });
            
            let msg = UpdateMsg::RequestUpdate {
                id,
                key,
                related_contracts,
                target: retry_target,
                value,
            };
            
            let op = UpdateOp {
                state: new_state,
                id,
                stats: update_op.stats,
            };
            
            match op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Update(op))
                .await
            {
                Ok(_) => {
                    tracing::debug!(
                        "Successfully sent retry update request for contract {} (retry {}/{})",
                        key,
                        retry_count + 1,
                        MAX_RETRIES
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to send retry update request for contract {}: {} (retry {}/{}). Will retry again.",
                        key,
                        err,
                        retry_count + 1,
                        MAX_RETRIES
                    );
                    
                    let retry_state = Some(UpdateState::RetryingRequest {
                        key,
                        target: retry_target,
                        related_contracts,
                        value,
                        retry_count: retry_count + 1,
                    });
                    
                    let retry_op = UpdateOp {
                        state: retry_state,
                        id,
                        stats: update_op.stats,
                    };
                    
                    op_manager
                        .notify_op_change(
                            NetMessage::from(UpdateMsg::AwaitUpdate { id }),
                            OpEnum::Update(retry_op),
                        )
                        .await?;
                }
            }
        }
        _ => return Err(OpError::invalid_transition(update_op.id)),
    };

    Ok(())
}

impl IsOperationCompleted for UpdateOp {
    fn is_completed(&self) -> bool {
        matches!(self.state, Some(UpdateState::Finished { .. }))
    }
}

mod messages {
    use std::{borrow::Borrow, fmt::Display};

    use freenet_stdlib::prelude::{ContractKey, RelatedContracts, StateSummary, WrappedState};
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
            target: PeerKeyLocation,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
        },
        /// Value successfully inserted/updated.
        SuccessfulUpdate {
            id: Transaction,
            target: PeerKeyLocation,
            #[serde(deserialize_with = "StateSummary::deser_state_summary")]
            summary: StateSummary<'static>,
            sender: PeerKeyLocation,
            key: ContractKey,
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
                UpdateMsg::SuccessfulUpdate { id, .. } => id,
                UpdateMsg::AwaitUpdate { id, .. } => id,
                UpdateMsg::SeekNode { id, .. } => id,
                UpdateMsg::Broadcasting { id, .. } => id,
                UpdateMsg::BroadcastTo { id, .. } => id,
            }
        }

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            match self {
                UpdateMsg::RequestUpdate { target, .. } => Some(target),
                UpdateMsg::SuccessfulUpdate { target, .. } => Some(target),
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
        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
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
                UpdateMsg::SuccessfulUpdate { id, .. } => write!(f, "SuccessfulUpdate(id: {id})"),
                UpdateMsg::AwaitUpdate { id } => write!(f, "AwaitUpdate(id: {id})"),
                UpdateMsg::SeekNode { id, .. } => write!(f, "SeekNode(id: {id})"),
                UpdateMsg::Broadcasting { id, .. } => write!(f, "Broadcasting(id: {id})"),
                UpdateMsg::BroadcastTo { id, .. } => write!(f, "BroadcastTo(id: {id})"),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum UpdateState {
    ReceivedRequest {
        retry_count: usize,
    },
    AwaitingResponse {
        key: ContractKey,
        upstream: Option<PeerKeyLocation>,
        retry_count: usize,
    },
    Finished {
        key: ContractKey,
        summary: StateSummary<'static>,
    },
    PrepareRequest {
        key: ContractKey,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
    },
    BroadcastOngoing {
        retry_count: usize,
    },
    RetryingBroadcast {
        key: ContractKey,
        retry_count: usize,
        failed_peers: Vec<PeerKeyLocation>,
        upstream: PeerKeyLocation,
        new_value: WrappedState,
    },
    RetryingRequest {
        key: ContractKey,
        target: PeerKeyLocation,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
        retry_count: usize,
    },
}
