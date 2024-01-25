use std::time::Instant;

use freenet_stdlib::client_api::{ErrorKind, HostResponse};
// TODO: complete update logic in the network
use freenet_stdlib::prelude::*;
use futures::future::BoxFuture;
use futures::FutureExt;

use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::contract::ContractHandlerEvent;
use crate::message::{InnerMessage, NetMessage, Transaction};
use crate::ring::{Location, PeerKeyLocation, RingError};
use crate::{
    client_events::{ClientId, HostResult},
    node::{NetworkBridge, OpManager, PeerId},
};

pub(crate) use self::messages::UpdateMsg;

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
        self.stats
            .as_ref()
            .map(|s| matches!(s.step, RecordingStats::Completed))
            .unwrap_or(false)
            || matches!(self.state, Some(UpdateState::Finished { .. }))
    }

    pub(super) fn record_transfer(&mut self) {
        if let Some(stats) = self.stats.as_mut() {
            match stats.step {
                RecordingStats::Uninitialized => {
                    stats.transfer_time = Some((Instant::now(), None));
                    stats.step = RecordingStats::InitUpdate;
                }
                RecordingStats::InitUpdate => {
                    if let Some((_, e)) = stats.transfer_time.as_mut() {
                        *e = Some(Instant::now());
                    }
                    stats.step = RecordingStats::Completed;
                }
                RecordingStats::Completed => {}
            }
        }
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(UpdateState::Finished { key, summary }) = &self.state {
            Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::UpdateResponse {
                    key: key.clone(),
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
    // contract_location: Location,
    // payload_size: usize,
    // /// (start, end)
    // first_response_time: Option<(Instant, Option<Instant>)>,
    /// (start, end)
    transfer_time: Option<(Instant, Option<Instant>)>,
    target: Option<PeerKeyLocation>,
    step: RecordingStats,
}

/// While timing, at what particular step we are now.
#[derive(Clone, Copy, Default)]
enum RecordingStats {
    #[default]
    Uninitialized,
    InitUpdate,
    Completed,
}

pub(crate) struct UpdateResult {}

impl TryFrom<UpdateOp> for UpdateResult {
    type Error = OpError;

    fn try_from(op: UpdateOp) -> Result<Self, Self::Error> {
        if let Some(true) = op
            .stats
            .map(|s| matches!(s.step, RecordingStats::Completed))
        {
            Ok(UpdateResult {})
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

impl Operation for UpdateOp {
    type Message = UpdateMsg;
    type Result = UpdateResult;

    fn load_or_init<'a>(
        op_manager: &'a crate::node::OpManager,
        msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<super::OpInitialization<Self>, OpError>> {
        async move {
            let mut sender: Option<PeerId> = None;
            if let Some(peer_key_loc) = msg.sender().cloned() {
                sender = Some(peer_key_loc.peer);
            };
            let tx = *msg.id();
            match op_manager.pop(msg.id()) {
                Ok(Some(OpEnum::Update(update_op))) => {
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
                            state: Some(UpdateState::ReceivedRequest),
                            id: tx,
                            stats: None, // don't care about stats in target peers
                        },
                        sender,
                    })
                }
                Err(err) => Err(err.into()),
            }
        }
        .boxed()
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

            match input {
                UpdateMsg::RequestUpdate {
                    id,
                    key,
                    target,
                    related_contracts,
                    value,
                    htl,
                } => {
                    let sender = op_manager.ring.own_location();

                    tracing::debug!(
                        "Requesting update for contract {} from {} to {}",
                        key,
                        sender.peer,
                        target.peer
                    );

                    // fixme: this node should filter out incoming redundant puts since is the one initiating the request
                    return_msg = Some(UpdateMsg::SeekNode {
                        id: *id,
                        sender,
                        target: *target,
                        value: value.clone(),
                        key: key.clone(),
                        related_contracts: related_contracts.clone(),
                        htl: *htl,
                    });

                    tracing::debug!(state = ?self.state);

                    // no changes to state yet, still in AwaitResponse state
                    new_state = self.state;
                }
                UpdateMsg::SeekNode {
                    id,
                    value,
                    key,
                    related_contracts,
                    htl,
                    target,
                    sender,
                } => {
                    let is_subscribed_contract = op_manager.ring.is_subscribed_to_contract(&key);

                    let this_peer = op_manager.ring.own_location();
                    tracing::debug!(
                        tx = %id,
                        %key,
                        target = %target.peer,
                        "Updating contract at target peer",
                    );

                    let broadcast_to = op_manager.get_broadcast_targets_update(&key, &sender.peer);

                    let mut new_htl = *htl;

                    let last_hop = if let Some(inner_new_htl) = htl.checked_sub(1) {
                        new_htl = inner_new_htl;
                        false
                    } else {
                        tracing::error!("Last hop if it doesnt find the contract to update this should throw an error");
                        true
                    };

                    if is_subscribed_contract
                    // || op_manager
                    //     .ring
                    //     .within_subscribing_distance(&Location::from(key))
                    {
                        tracing::debug!("peer is subscribed to contract. About to update it");
                        update_contract(
                            op_manager,
                            key.clone(),
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
                    } else {
                        tracing::debug!(%key, "is not suscribed to contract at {:?}", target.location);
                        tracing::debug!("just forwarding update to next peer");

                        if last_hop {
                            tracing::error!(
                                "last hop found. Cannot forward update. Should throw an error"
                            );
                        } else {
                            let mut skip_list = vec![sender.peer];
                            // broadcast_to.iter().for_each(|t| skip_list.push(t.peer));
                            forward_update(
                                op_manager,
                                conn_manager,
                                key.clone(),
                                value.clone(),
                                *id,
                                new_htl,
                                skip_list,
                                broadcast_to.iter().map(|t| t.peer).collect(),
                            )
                            .await;
                        }
                    }

                    let mut next_already_broadcasted_to = vec![];

                    broadcast_to.iter().for_each(|t| {
                        // fix error 54 - BroadcastTo -> state = AwaitingResponse
                        // next_already_broadcasted_to.push(t.peer);
                    });

                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        (broadcast_to, *sender),
                        key.clone(),
                        value.clone(),
                        next_already_broadcasted_to,
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
                    already_broadcasted_to,
                } => {
                    if let Some(UpdateState::AwaitingResponse { .. }) = self.state {
                        tracing::debug!("error 54 - trying to broadcast to a peer that was the initiator of the op because it received the client request, or is in the middle of a seek node or forward process");
                        return Err(OpError::StatePushed);
                    };

                    let target = op_manager.ring.own_location();

                    tracing::debug!("Attempting contract value update - BroadcastTo - update");
                    let new_value = update_contract(
                        op_manager,
                        key.clone(),
                        new_value.clone(),
                        RelatedContracts::default(),
                    )
                    .await?;
                    tracing::debug!("Contract successfully updated - BroadcastTo - update");

                    tracing::debug!(state = ?self.state);

                    let broadcast_to = op_manager.get_broadcast_targets_update(key, &sender.peer);

                    let mut next_already_broadcasted_to = already_broadcasted_to.clone();
                    let mut next_already_broadcasted_to = vec![];

                    let mut broadcast_to_next = vec![];

                    // this filter already sent broadcast peer by other peers
                    broadcast_to.into_iter().for_each(|t| {
                        if already_broadcasted_to.contains(&t.peer) {
                            // tracing::warn!(
                            //     %target.peer, "error 54 - about to add to targets an already broadcasted to peer"
                            // );
                        } else {
                            broadcast_to_next.push(t);
                            // fix error 54 - BroadcastTo -> state = AwaitingResponse
                            //next_already_broadcasted_to.push(t.peer);
                        }
                    });

                    tracing::debug!(
                        "Successfully updated a value for contract {} @ {:?} - BroadcastTo - update",
                        key,
                        target.location
                    );

                    if self.state.is_none() {
                        tracing::debug!(op_id = %self.id, "try_to_broadcast_2 - self state is None - BroadcastTo - update");
                    }

                    if let Some(UpdateState::AwaitingResponse { .. }) = self.state {
                        tracing::debug!(state = ?self.state, op_id = %self.id, "try_to_broadcast_2 awaitingresponse - BroadcastTo - update");
                    }

                    match try_to_broadcast(
                        *id,
                        false,
                        op_manager,
                        self.state,
                        (broadcast_to_next, *sender),
                        key.clone(),
                        new_value,
                        next_already_broadcasted_to.to_vec(),
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
                    already_broadcasted_to,
                } => {
                    let sender = op_manager.ring.own_location();
                    let mut broadcasted_to = *broadcasted_to;

                    let mut broadcasting = Vec::with_capacity(broadcast_to.len());

                    let mut next_already_broadcasted_to = already_broadcasted_to.clone();
                    let mut next_already_broadcasted_to = vec![];

                    broadcast_to.iter().for_each(|t| {
                        if already_broadcasted_to.contains(&t.peer) {
                            // fix error 54 - BroadcastTo -> state = AwaitingResponse
                            // next_already_broadcasted_to.push(t.peer);
                        }
                    });

                    for peer in broadcast_to.iter() {
                        if already_broadcasted_to.contains(&peer.peer) {
                            // tracing::error!(%peer.peer,
                            //     "error 54 - about to broadcast update to an already broadcasted peer - Broadcasting"
                            // );
                            // fix error 54 - BroadcastTo -> state = AwaitingResponse
                            // continue;
                        }

                        let msg = UpdateMsg::BroadcastTo {
                            id: *id,
                            key: key.clone(),
                            new_value: new_value.clone(),
                            sender,
                            already_broadcasted_to: next_already_broadcasted_to.to_vec(),
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
                    for (peer_num, err) in error_futures {
                        // remove the failed peers in reverse order
                        let peer = broadcast_to.get(peer_num).unwrap();
                        tracing::warn!(
                            "failed broadcasting update change to {} with error {}; dropping connection",
                            peer.peer,
                            err
                        );
                        // TODO: review this, maybe we should just dropping this subscription
                        conn_manager.drop_connection(&peer.peer).await?;
                        incorrect_results += 1;
                    }

                    broadcasted_to += broadcast_to.len() - incorrect_results;
                    tracing::debug!(
                        "Successfully broadcasted update contract {key} to {broadcasted_to} peers - Broadcasting"
                    );

                    let raw_state = State::from(new_value.clone());

                    let summary = StateSummary::from(raw_state.into_bytes());

                    // Subscriber nodes have been notified of the change, the operation is complete
                    // tracing::debug!();
                    return_msg = Some(UpdateMsg::SuccessfulUpdate {
                        id: *id,
                        target: *upstream,
                        summary,
                    });

                    new_state = None;
                    // new_state = Some(UpdateState::AwaitingResponse {
                    //     key: key.clone(),
                    //     upstream: Some(*upstream),
                    // });
                }
                UpdateMsg::SuccessfulUpdate { id, summary, .. } => {
                    tracing::debug!(tx = %id, state = ?self.state, "in SuccessfulUpdate");

                    match self.state {
                        Some(UpdateState::AwaitingResponse { key, upstream }) => {
                            tracing::debug!("State == AwaitingResponse");
                            let is_subscribed_contract =
                                op_manager.ring.is_subscribed_to_contract(&key);
                            // if !is_subscribed_contract
                            //     && op_manager
                            //         .ring
                            //         .within_subscribing_distance(&Location::from(&key))
                            // {
                            //     tracing::debug!(tx = %id, %key, peer = %op_manager.ring.peer_key, "Contract not cached @ peer, caching");
                            //     super::start_subscription_request(op_manager, key.clone(), true)
                            //         .await;
                            // }
                            tracing::debug!(
                                tx = %id,
                                %key,
                                this_peer = %op_manager.ring.peer_key,
                                "Peer completed contract value update - SuccessfulUpdate",
                            );
                            tracing::debug!(upstream = ?upstream, "set state to Finished so upstream can close its tx");
                            new_state = Some(UpdateState::Finished {
                                key,
                                summary: summary.clone(),
                            });
                            if let Some(upstream) = upstream {
                                return_msg = Some(UpdateMsg::SuccessfulUpdate {
                                    id: *id,
                                    target: upstream,
                                    summary: summary.clone(),
                                });
                            } else {
                                // this means op finalized
                                return_msg = None;
                            }
                        }
                        _ => {
                            tracing::debug!(
                                state = ?self.state,
                                "invalid transition in UpdateMsg::SuccessfulUpdate -> match self.state"
                            );

                            return Err(OpError::invalid_transition(self.id));
                        }
                    };
                }
                UpdateMsg::UpdateForward {
                    id,
                    key,
                    new_value,
                    htl,
                    sender,
                    skip_list,
                    already_broadcasted_to,
                } => {
                    let peer_loc = op_manager.ring.own_location();

                    tracing::debug!(
                        %key,
                        this_peer = % peer_loc.peer,
                        "Forwarding changes, trying to update the contract - update"
                    );

                    let is_subscribed_contract = op_manager.ring.is_subscribed_to_contract(&key);
                    let within_caching_dist = op_manager
                        .ring
                        .within_subscribing_distance(&Location::from(key));

                    if is_subscribed_contract {
                        // after the contract has been cached, push the update query
                        update_contract(
                            op_manager,
                            key.clone(),
                            new_value.clone(),
                            RelatedContracts::default(),
                        )
                        .await?;
                    } else {
                        tracing::debug!(%key, "is not suscribed to contract at {:?}", peer_loc.location);
                    }

                    let broadcast_to = op_manager.get_broadcast_targets_update(&key, &sender.peer);

                    let mut next_already_broadcasted_to = already_broadcasted_to.clone();
                    let mut next_already_broadcasted_to = vec![];

                    let mut broadcast_to_next = vec![];

                    // this filter already sent broadcast peer by other peers
                    broadcast_to.into_iter().for_each(|t| {
                        if already_broadcasted_to.contains(&t.peer) {
                            // tracing::warn!(
                            //     %peer_loc.peer, "error 54 - about to add to targets an already broadcasted to peer"
                            // );
                        } else {
                            broadcast_to_next.push(t);
                        }
                        //next_already_broadcasted_to.push(t.peer);
                    });

                    // if successful, forward to the next closest peers (if any)
                    let last_hop = if let Some(new_htl) = htl.checked_sub(1) {
                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.push(sender.peer);
                        next_already_broadcasted_to
                            .clone()
                            .into_iter()
                            .for_each(|t| new_skip_list.push(t));

                        // only hop forward if there are closer peers
                        let put_here = forward_update(
                            op_manager,
                            conn_manager,
                            key.clone(),
                            new_value.clone(),
                            *id,
                            new_htl,
                            new_skip_list,
                            next_already_broadcasted_to.clone(),
                        )
                        .await;
                        // if put_here && !is_subscribed_contract {
                        //     // if already subscribed the value was already put and merging succeeded
                        //     update_contract(
                        //         op_manager,
                        //         key.clone(),
                        //         new_value.clone(),
                        //         RelatedContracts::default(),
                        //     )
                        //     .await?;
                        // }
                        put_here
                    } else {
                        // should put in this location, no hops left
                        // update_contract(
                        //     op_manager,
                        //     key.clone(),
                        //     new_value.clone(),
                        //     RelatedContracts::default(),
                        // )
                        // .await?;
                        tracing::error!("didnt find the contract to update and no hops left. This should throw an error");
                        true
                    };

                    tracing::debug!("\njust before try_to_broadcast_3");
                    tracing::debug!(state = ?self.state, op_id = %self.id);
                    // tracing::debug!(broadcast_list = ?broadcast_to);

                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        (broadcast_to_next, *sender),
                        key.clone(),
                        new_value.clone(),
                        next_already_broadcasted_to.to_vec(),
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
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, stats)
        })
    }
}

async fn try_to_broadcast(
    id: Transaction,
    last_hop: bool,
    op_manager: &OpManager,
    state: Option<UpdateState>,
    (broadcast_to, upstream): (Vec<PeerKeyLocation>, PeerKeyLocation),
    key: ContractKey,
    new_value: WrappedState,
    already_broadcasted_to: Vec<PeerId>,
    is_from_a_broadcasted_to_peer: bool,
) -> Result<(Option<UpdateState>, Option<UpdateMsg>), OpError> {
    let new_state;
    let return_msg;

    match state {
        Some(UpdateState::ReceivedRequest | UpdateState::BroadcastOngoing { .. }) => {
            if broadcast_to.is_empty() && !last_hop {
                // broadcast complete
                tracing::debug!(
                    "Empty broadcast list while updating value for contract {} - try_to_broadcast",
                    key
                );

                return_msg = None;

                if is_from_a_broadcasted_to_peer {
                    // let raw_state = State::from(new_value);

                    // let summary = StateSummary::from(raw_state.into_bytes());

                    new_state = None;
                    return Ok((new_state, return_msg));
                }

                // means the whole tx finished so can return early
                new_state = Some(UpdateState::AwaitingResponse {
                    key,
                    upstream: Some(upstream),
                });
            } else if !broadcast_to.is_empty() {
                tracing::debug!("Callback to start broadcasting to other nodes - try_to_broadcast");
                new_state = Some(UpdateState::BroadcastOngoing);

                tracing::debug!(
                    "broadcast_to list size {} before broadcasting",
                    broadcast_to.len()
                );
                for peer in broadcast_to.iter() {
                    tracing::warn!("to {}", peer.peer);
                }
                tracing::debug!("end broadcast_to list before broadcasting - try_to_broadcast");

                return_msg = Some(UpdateMsg::Broadcasting {
                    id,
                    new_value,
                    broadcasted_to: 0,
                    broadcast_to,
                    key,
                    upstream,
                    already_broadcasted_to,
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

                // tracing::debug!(op_id = %id, "state op changed to None");

                new_state = None;
                return_msg = Some(UpdateMsg::SuccessfulUpdate {
                    id,
                    target: upstream,
                    summary,
                });

                tracing::debug!("set tx msg to SuccessfulUpdate - try_to_broadcast");
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
        let mut subscribers = self
            .ring
            .subscribers_of(key)
            .map(|subs| {
                subs.value()
                    .iter()
                    .filter(|pk| &pk.peer != sender)
                    .copied()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(peer) = self.ring.subscribed_to_contract(key) {
            if &peer.peer == sender {
                // tracing::warn!("1 - about to add sender peer to broadcast targets");
            } else if subscribers.contains(&peer) {
                // tracing::warn!("2 - about to add already added peer to broadcast targets");
            } else {
                subscribers.push(peer);
            }
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
    let mut state_is_none = false;
    if state.as_ref().is_none() {
        tracing::debug!("state is none - build_op_result 1");
        state_is_none = true;
    }

    let output_op = Some(UpdateOp { id, state, stats });

    let op_enum_update = output_op.map(OpEnum::Update);

    if state_is_none {
        if let Some(OpEnum::Update(op)) = &op_enum_update {
            // tracing::debug!(state = ?op.state, "state is Some error - build_op_result 2");
        };

        if let None = &op_enum_update {
            tracing::debug!("op_enum_update is None ok - build_op_result 3");
        }
    }

    Ok(OperationResult {
        return_msg: return_msg.map(NetMessage::from),
        state: {
            if state_is_none {
                None
            } else {
                op_enum_update
            }
        }, // state: output_op.map(OpEnum::Update),
    })
}

async fn forward_update<NB: NetworkBridge>(
    op_manager: &OpManager,
    conn_manager: &mut NB,
    key: ContractKey,
    new_value: WrappedState,
    id: Transaction,
    htl: usize,
    skip_list: Vec<PeerId>,
    already_broadcasted_to: Vec<PeerId>,
) -> bool {
    let contract_loc = Location::from(&key);
    let forward_to = op_manager
        .ring
        .closest_potentially_caching(&key, &*skip_list);
    let own_pkloc = op_manager.ring.own_location();
    let own_loc = own_pkloc.location.expect("infallible");
    if let Some(peer) = forward_to {
        let other_loc = peer.location.as_ref().expect("infallible");
        let other_distance = contract_loc.distance(other_loc);
        let self_distance = contract_loc.distance(own_loc);
        if other_distance < self_distance {
            // forward the contract towards this node since it is indeed closer to the contract location
            // and forget about it, no need to keep track of this op or wait for response
            if already_broadcasted_to.contains(&peer.peer) {
                // tracing::error!(%peer.peer,
                //     "error 54 - about to forward update to an already broadcasted peer"
                // );
            }

            let _ = conn_manager
                .send(
                    &peer.peer,
                    (UpdateMsg::UpdateForward {
                        id,
                        key,
                        sender: own_pkloc,
                        new_value: new_value.clone(),
                        htl,
                        skip_list,
                        already_broadcasted_to,
                    })
                    .into(),
                )
                .await;
            return false;
        }
    }
    true
}

async fn update_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
) -> Result<WrappedState, OpError> {
    match op_manager
        .notify_contract_handler(ContractHandlerEvent::UpdateQuery {
            key,
            state,
            related_contracts,
        })
        .await
    {
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Ok(new_val),
        }) => Ok(new_val),
        Ok(ContractHandlerEvent::UpdateResponse {
            new_value: Err(_err),
        }) => {
            // return Err(OpError::from(ContractError::StorageError(err)));
            todo!("not a valid value update, notify back to requester")
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
    htl: usize,
) -> UpdateOp {
    let contract_location = Location::from(&key);
    tracing::debug!(%contract_location, %key, "Requesting update");
    let id = Transaction::new::<UpdateMsg>();
    // let payload_size = contract.data().len();

    let state = Some(UpdateState::PrepareRequest {
        key,
        related_contracts,
        value: new_state,
        htl,
    });

    UpdateOp {
        id,
        state,
        stats: Some(UpdateStats {
            // contract_location,
            // payload_size,
            target: None,
            // first_response_time: None,
            transfer_time: None,
            step: Default::default(),
        }),
    }
}

/// Entry point from node to operations logic
pub(crate) async fn request_update(
    op_manager: &OpManager,
    mut update_op: UpdateOp,
) -> Result<(), OpError> {
    let key = if let Some(UpdateState::PrepareRequest { key, .. }) = &update_op.state {
        key
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    let sender = op_manager.ring.own_location();

    // the initial request must provide:
    // - a peer as close as possible to the contract location
    // - and the value to update
    let target = op_manager
        .ring
        .closest_potentially_caching(&key, [&sender.peer].as_slice())
        .into_iter()
        .next()
        .ok_or(RingError::EmptyRing)?;

    let id = update_op.id;
    if let Some(stats) = &mut update_op.stats {
        stats.target = Some(target);
    }

    match update_op.state {
        Some(UpdateState::PrepareRequest {
            key,
            value,
            htl,
            related_contracts,
        }) => {
            let new_state = Some(UpdateState::AwaitingResponse {
                key: key.clone(),
                upstream: None,
            });
            let msg = UpdateMsg::RequestUpdate {
                id,
                key,
                related_contracts,
                htl,
                target,
                value,
            };

            let op = UpdateOp {
                state: new_state,
                id,
                stats: update_op.stats,
            };

            op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Update(op))
                .await?;
        }
        _ => return Err(OpError::invalid_transition(update_op.id)),
    };

    Ok(())
}

mod messages {
    use std::fmt::Display;

    use freenet_stdlib::prelude::{
        ContractContainer, ContractKey, RelatedContracts, StateSummary, WrappedState,
    };
    use serde::{Deserialize, Serialize};

    use crate::{
        message::{InnerMessage, Transaction},
        node::PeerId,
        ring::{Location, PeerKeyLocation},
    };

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum UpdateMsg {
        RequestUpdate {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
            htl: usize,
        },
        /// Forward a contract and it's latest value to an other node
        UpdateForward {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            /// current htl, reduced by one at each hop
            htl: usize,
            skip_list: Vec<PeerId>,
            already_broadcasted_to: Vec<PeerId>,
        },
        /// Value successfully inserted/updated.
        SuccessfulUpdate {
            id: Transaction,
            target: PeerKeyLocation,
            #[serde(deserialize_with = "StateSummary::deser_state_summary")]
            summary: StateSummary<'static>,
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
            //contract: ContractContainer,
            upstream: PeerKeyLocation,
            already_broadcasted_to: Vec<PeerId>,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            already_broadcasted_to: Vec<PeerId>, //`contract: ContractContainer,
        },
    }

    impl InnerMessage for UpdateMsg {
        fn id(&self) -> &Transaction {
            match self {
                UpdateMsg::RequestUpdate { id, .. } => id,
                UpdateMsg::UpdateForward { id, .. } => id,
                UpdateMsg::SuccessfulUpdate { id, .. } => id,
                UpdateMsg::AwaitUpdate { id, .. } => id,
                UpdateMsg::SeekNode { id, .. } => id,
                UpdateMsg::Broadcasting { id, .. } => id,
                UpdateMsg::BroadcastTo { id, .. } => id,
            }
        }

        fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                UpdateMsg::RequestUpdate { target, .. } => Some(target),
                UpdateMsg::SuccessfulUpdate { target, .. } => Some(target),
                UpdateMsg::SeekNode { target, .. } => Some(target),
                _ => None,
            }
        }

        fn terminal(&self) -> bool {
            use UpdateMsg::*;
            matches!(
                self,
                SuccessfulUpdate { .. } | UpdateForward { .. } | SeekNode { .. }
            )
        }

        fn requested_location(&self) -> Option<crate::ring::Location> {
            match self {
                UpdateMsg::RequestUpdate { key, .. } => Some(Location::from(key.id())),
                UpdateMsg::UpdateForward { key, .. } => Some(Location::from(key.id())),
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
            let id = self.id();
            match self {
                UpdateMsg::RequestUpdate { id, .. } => write!(f, "RequestUpdate(id: {id})"),
                UpdateMsg::UpdateForward { id, .. } => write!(f, "UpdateForward(id: {id})"),
                UpdateMsg::SuccessfulUpdate { id, .. } => write!(f, "SuccessfulUpdate(id: {id})"),
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
    AwaitingResponse {
        key: ContractKey,
        upstream: Option<PeerKeyLocation>,
    },
    Finished {
        key: ContractKey,
        summary: StateSummary<'static>,
    },
    PrepareRequest {
        key: ContractKey,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
        htl: usize,
    },
    BroadcastOngoing,
}
