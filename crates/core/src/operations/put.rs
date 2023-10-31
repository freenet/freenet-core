//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

use std::future::Future;
use std::pin::Pin;
use std::{collections::HashSet, time::Instant};

pub(crate) use self::messages::PutMsg;
use freenet_stdlib::prelude::*;
use futures::future::BoxFuture;
use futures::FutureExt;

use super::{OpEnum, OpError, OpOutcome, OperationResult};
use crate::{
    client_events::ClientId,
    contract::ContractHandlerEvent,
    message::{InnerMessage, Message, Transaction},
    node::{NetworkBridge, OpManager, PeerKey},
    operations::{op_trait::Operation, OpInitialization},
    ring::{Location, PeerKeyLocation, RingError},
};

pub(crate) struct PutOp {
    id: Transaction,
    state: Option<PutState>,
    stats: Option<PutStats>,
}

impl PutOp {
    pub(super) fn outcome(&self) -> OpOutcome {
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
        self.stats
            .as_ref()
            .map(|s| matches!(s.step, RecordingStats::Completed))
            .unwrap_or(false)
    }

    pub(super) fn record_transfer(&mut self) {
        if let Some(stats) = self.stats.as_mut() {
            match stats.step {
                RecordingStats::Uninitialized => {
                    stats.transfer_time = Some((Instant::now(), None));
                    stats.step = RecordingStats::InitPut;
                }
                RecordingStats::InitPut => {
                    if let Some((_, e)) = stats.transfer_time.as_mut() {
                        *e = Some(Instant::now());
                    }
                    stats.step = RecordingStats::Completed;
                }
                RecordingStats::Completed => {}
            }
        }
    }
}

struct PutStats {
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
    InitPut,
    Completed,
}

pub(crate) struct PutResult {}

impl TryFrom<PutOp> for PutResult {
    type Error = OpError;

    fn try_from(op: PutOp) -> Result<Self, Self::Error> {
        if let Some(true) = op
            .stats
            .map(|s| matches!(s.step, RecordingStats::Completed))
        {
            Ok(PutResult {})
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

impl Operation for PutOp {
    type Message = PutMsg;
    type Result = PutResult;

    fn load_or_init<'a>(
        op_storage: &'a OpManager,
        msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<OpInitialization<Self>, OpError>> {
        async move {
            let mut sender: Option<PeerKey> = None;
            if let Some(peer_key_loc) = msg.sender().cloned() {
                sender = Some(peer_key_loc.peer);
            };

            let tx = *msg.id();
            match op_storage.pop(msg.id()) {
                Ok(Some(OpEnum::Put(put_op))) => {
                    // was an existing operation, the other peer messaged back
                    Ok(OpInitialization { op: put_op, sender })
                }
                Ok(Some(op)) => {
                    let _ = op_storage.push(tx, op).await;
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
        .boxed()
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a, NB: NetworkBridge>(
        self,
        conn_manager: &'a mut NB,
        op_storage: &'a OpManager,
        input: &'a Self::Message,
        client_id: Option<ClientId>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let new_state;
            let stats = self.stats;

            match input {
                PutMsg::RequestPut {
                    id,
                    contract,
                    related_contracts,
                    value,
                    htl,
                    target,
                } => {
                    let sender = op_storage.ring.own_location();

                    let key = contract.key();
                    tracing::debug!(
                        "Rquesting put for contract {} from {} to {}",
                        key,
                        sender.peer,
                        target.peer
                    );

                    return_msg = Some(PutMsg::SeekNode {
                        id: *id,
                        sender,
                        target: *target,
                        value: value.clone(),
                        contract: contract.clone(),
                        related_contracts: related_contracts.clone(),
                        htl: *htl,
                        skip_list: vec![sender.peer],
                    });

                    // no changes to state yet, still in AwaitResponse state
                    new_state = self.state;
                }
                PutMsg::SeekNode {
                    id,
                    sender,
                    value,
                    contract,
                    related_contracts,
                    htl,
                    target,
                    skip_list,
                } => {
                    let key = contract.key();
                    let is_cached_contract = op_storage.ring.is_contract_cached(&key);

                    tracing::debug!(
                        tx = %id,
                        "Puttting contract {} at target peer {}",
                        key,
                        target.peer,
                    );

                    if !is_cached_contract
                        && op_storage
                            .ring
                            .within_caching_distance(&Location::from(&key))
                    {
                        tracing::debug!(tx = %id, "Contract `{}` not cached @ peer {}", key, target.peer);
                        match try_to_cache_contract(op_storage, contract, &key, client_id).await {
                            Ok(_) => {}
                            Err(err) => return Err(err),
                        }
                    } else if !is_cached_contract {
                        // FIXME
                        // in this case forward to a closer node to the target location and just wait for a response
                        // to give back to requesting peer
                        tracing::warn!(
                            tx = %id,
                            "Contract {} not found while processing info, forwarding",
                            key
                        );
                    }

                    // after the contract has been cached, push the update query
                    tracing::debug!(tx = %id, "Attempting contract value update");
                    let parameters = contract.params();
                    let new_value = put_contract(
                        op_storage,
                        key.clone(),
                        value.clone(),
                        related_contracts.clone(),
                        parameters,
                        client_id,
                    )
                    .await?;
                    tracing::debug!(tx = %id, "Contract successfully updated");
                    // if the change was successful, communicate this back to the requestor and broadcast the change
                    conn_manager
                        .send(
                            &sender.peer,
                            (PutMsg::SuccessfulUpdate {
                                id: *id,
                                new_value: new_value.clone(),
                            })
                            .into(),
                        )
                        .await?;

                    let mut skip_list = skip_list.clone();
                    skip_list.push(target.peer);

                    if let Some(new_htl) = htl.checked_sub(1) {
                        // forward changes in the contract to nodes closer to the contract location, if possible
                        forward_changes(
                            op_storage,
                            conn_manager,
                            contract,
                            new_value.clone(),
                            *id,
                            new_htl,
                            skip_list.as_slice(),
                        )
                        .await;
                    }

                    let broadcast_to = op_storage
                        .ring
                        .subscribers_of(&key)
                        .map(|i| i.value().to_vec())
                        .unwrap_or_default();
                    tracing::debug!(
                        tx = %id,
                        "Successfully updated a value for contract {} @ {:?}",
                        key,
                        target.location
                    );

                    match try_to_broadcast(
                        (*id, client_id),
                        op_storage,
                        self.state,
                        broadcast_to,
                        key.clone(),
                        (contract.params(), new_value),
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
                    parameters,
                    sender,
                    sender_subscribers,
                } => {
                    let target = op_storage.ring.own_location();

                    tracing::debug!("Attempting contract value update");
                    let new_value = put_contract(
                        op_storage,
                        key.clone(),
                        new_value.clone(),
                        RelatedContracts::default(),
                        parameters.clone(),
                        client_id,
                    )
                    .await?;
                    tracing::debug!("Contract successfully updated");

                    let broadcast_to = op_storage
                        .ring
                        .subscribers_of(key)
                        .map(|i| {
                            // Avoid already broadcast nodes and sender from broadcasting
                            let mut subscribers: Vec<PeerKeyLocation> = i.value().to_vec();
                            let mut avoid_list: HashSet<PeerKey> =
                                sender_subscribers.iter().map(|pl| pl.peer).collect();
                            avoid_list.insert(sender.peer);
                            subscribers.retain(|s| !avoid_list.contains(&s.peer));
                            subscribers
                        })
                        .unwrap_or_default();
                    tracing::debug!(
                        "Successfully updated a value for contract {} @ {:?}",
                        key,
                        target.location
                    );

                    match try_to_broadcast(
                        (*id, client_id),
                        op_storage,
                        self.state,
                        broadcast_to,
                        key.clone(),
                        (parameters.clone(), new_value),
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
                    broadcast_to,
                    broadcasted_to,
                    key,
                    new_value,
                    parameters,
                } => {
                    let sender = op_storage.ring.own_location();
                    let mut broadcasted_to = *broadcasted_to;

                    let mut broadcasting = Vec::with_capacity(broadcast_to.len());
                    let mut filtered_broadcast = broadcast_to
                        .iter()
                        .filter(|pk| pk.peer != sender.peer)
                        .collect::<Vec<_>>();
                    for peer in filtered_broadcast.iter() {
                        let msg = PutMsg::BroadcastTo {
                            id: *id,
                            key: key.clone(),
                            new_value: new_value.clone(),
                            sender,
                            sender_subscribers: broadcast_to.clone(),
                            parameters: parameters.clone(),
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
                        })
                        .rev();

                    let mut incorrect_results = 0;
                    for (peer_num, err) in error_futures {
                        // remove the failed peers in reverse order
                        let peer = filtered_broadcast.remove(peer_num);
                        tracing::warn!(
                            "failed broadcasting put change to {} with error {}; dropping connection",
                            peer.peer,
                            err
                        );
                        conn_manager.drop_connection(&peer.peer).await?;
                        incorrect_results += 1;
                    }

                    broadcasted_to += broadcast_to.len() - incorrect_results;
                    tracing::debug!(
                        "successfully broadcasted put into contract {key} to {broadcasted_to} peers"
                    );

                    // Subscriber nodes have been notified of the change, the operation is completed
                    op_storage.completed(*id);
                    return_msg = None;
                    new_state = None;
                }
                PutMsg::SuccessfulUpdate { id, .. } => {
                    match self.state {
                        Some(PutState::AwaitingResponse { contract, .. }) => {
                            tracing::debug!("Successfully updated value for {}", contract,);
                            op_storage.completed(*id);
                            new_state = None;
                            return_msg = None;
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };
                    tracing::debug!(
                        "Peer {} completed contract value put",
                        op_storage.ring.peer_key
                    );
                }
                PutMsg::PutForward {
                    id,
                    contract,
                    new_value,
                    htl,
                    skip_list,
                } => {
                    let key = contract.key();
                    let peer_loc = op_storage.ring.own_location();

                    tracing::debug!(
                        "Forwarding changes at {}, trying put the contract {}",
                        peer_loc.peer,
                        key
                    );

                    let cached_contract = op_storage.ring.is_contract_cached(&key);
                    let within_caching_dist = op_storage
                        .ring
                        .within_caching_distance(&Location::from(&key));
                    if !cached_contract && within_caching_dist {
                        match try_to_cache_contract(op_storage, contract, &key, client_id).await {
                            Ok(_) => {}
                            Err(err) => return Err(err),
                        }
                    } else if !within_caching_dist {
                        // not a contract this node cares about; do nothing
                        return Ok(OperationResult {
                            return_msg: None,
                            state: None,
                        });
                    }
                    // after the contract has been cached, push the update query
                    let new_value = put_contract(
                        op_storage,
                        key,
                        new_value.clone(),
                        RelatedContracts::default(),
                        contract.params(),
                        client_id,
                    )
                    .await?;

                    // update skip list
                    let mut skip_list = skip_list.clone();
                    skip_list.push(peer_loc.peer);

                    // if successful, forward to the next closest peers (if any)
                    if let Some(new_htl) = htl.checked_sub(1) {
                        forward_changes(
                            op_storage,
                            conn_manager,
                            contract,
                            new_value,
                            *id,
                            new_htl,
                            skip_list.as_slice(),
                        )
                        .await;
                    }
                    op_storage.completed(*id);
                    return_msg = None;
                    new_state = None;
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, stats)
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<PutState>,
    msg: Option<PutMsg>,
    stats: Option<PutStats>,
) -> Result<OperationResult, OpError> {
    let output_op = Some(PutOp { id, state, stats });
    Ok(OperationResult {
        return_msg: msg.map(Message::from),
        state: output_op.map(OpEnum::Put),
    })
}

pub(super) async fn try_to_cache_contract<'a>(
    op_storage: &'a OpManager,
    contract: &ContractContainer,
    key: &ContractKey,
    client_id: Option<ClientId>,
) -> Result<(), OpError> {
    // this node does not have the contract, so instead store the contract and execute the put op.
    let res = op_storage
        .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()), client_id)
        .await?;
    if let ContractHandlerEvent::CacheResult(Ok(_)) = res {
        op_storage.ring.contract_cached(key);
        tracing::debug!("Contract successfully cached");
        Ok(())
    } else {
        tracing::error!(
            "Contract handler returned wrong event when trying to cache contract, this should not happen!"
        );
        Err(OpError::UnexpectedOpState)
    }
}

async fn try_to_broadcast(
    (id, client_id): (Transaction, Option<ClientId>),
    op_storage: &OpManager,
    state: Option<PutState>,
    broadcast_to: Vec<PeerKeyLocation>,
    key: ContractKey,
    (parameters, new_value): (Parameters<'static>, WrappedState),
) -> Result<(Option<PutState>, Option<PutMsg>), OpError> {
    let new_state;
    let return_msg;

    match state {
        Some(PutState::ReceivedRequest | PutState::BroadcastOngoing { .. }) => {
            if broadcast_to.is_empty() {
                // broadcast complete
                tracing::debug!(
                    "Empty broadcast list while updating value for contract {}",
                    key
                );
                // means the whole tx finished so can return early
                new_state = None;
                return_msg = Some(PutMsg::SuccessfulUpdate { id, new_value });
            } else {
                tracing::debug!("Callback to start broadcasting to other nodes");
                new_state = Some(PutState::BroadcastOngoing);
                return_msg = Some(PutMsg::Broadcasting {
                    id,
                    new_value,
                    parameters,
                    broadcasted_to: 0,
                    broadcast_to,
                    key,
                });

                let op = PutOp {
                    id,
                    state: new_state,
                    stats: None,
                };
                op_storage
                    .notify_op_change(
                        Message::from(return_msg.unwrap()),
                        OpEnum::Put(op),
                        client_id,
                    )
                    .await?;
                return Err(OpError::StatePushed);
            }
        }
        _ => return Err(OpError::InvalidStateTransition(id)),
    };

    Ok((new_state, return_msg))
}

pub(crate) fn start_op(
    contract: ContractContainer,
    related_contracts: RelatedContracts<'static>,
    value: WrappedState,
    htl: usize,
) -> PutOp {
    let key = contract.key();
    let contract_location = Location::from(&key);
    tracing::debug!(
        "Requesting put to contract {} @ loc({contract_location})",
        key,
    );

    let id = Transaction::new::<PutMsg>();
    // let payload_size = contract.data().len();
    let state = Some(PutState::PrepareRequest {
        contract,
        related_contracts,
        value,
        htl,
    });

    PutOp {
        id,
        state,
        stats: Some(PutStats {
            // contract_location,
            // payload_size,
            target: None,
            // first_response_time: None,
            transfer_time: None,
            step: Default::default(),
        }),
    }
}

enum PutState {
    ReceivedRequest,
    PrepareRequest {
        contract: ContractContainer,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
        htl: usize,
    },
    AwaitingResponse {
        contract: ContractKey,
    },
    BroadcastOngoing,
}

/// Request to insert/update a value into a contract.
pub(crate) async fn request_put(
    op_storage: &OpManager,
    mut put_op: PutOp,
    client_id: Option<ClientId>,
) -> Result<(), OpError> {
    let key = if let Some(PutState::PrepareRequest { contract, .. }) = &put_op.state {
        contract.key()
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    let sender = op_storage.ring.own_location();

    // the initial request must provide:
    // - a peer as close as possible to the contract location
    // - and the value to put
    let target = op_storage
        .ring
        .closest_caching(&key, &[sender.peer])
        .into_iter()
        .next()
        .ok_or(RingError::EmptyRing)?;

    let id = put_op.id;
    if let Some(stats) = &mut put_op.stats {
        stats.target = Some(target);
    }

    match put_op.state {
        Some(PutState::PrepareRequest {
            contract,
            value,
            htl,
            related_contracts,
        }) => {
            let key = contract.key();
            let new_state = Some(PutState::AwaitingResponse { contract: key });
            let msg = PutMsg::RequestPut {
                id,
                contract,
                related_contracts,
                value,
                htl,
                target,
            };

            let op = PutOp {
                state: new_state,
                id,
                stats: put_op.stats,
            };

            op_storage
                .notify_op_change(Message::from(msg), OpEnum::Put(op), client_id)
                .await?;
        }
        _ => return Err(OpError::InvalidStateTransition(put_op.id)),
    };

    Ok(())
}

async fn put_contract(
    op_storage: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
    parameters: Parameters<'static>,
    client_id: Option<ClientId>,
) -> Result<WrappedState, OpError> {
    // after the contract has been cached, push the update query
    match op_storage
        .notify_contract_handler(
            ContractHandlerEvent::PutQuery {
                key,
                state,
                related_contracts,
                parameters: Some(parameters),
            },
            client_id,
        )
        .await
    {
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Ok(new_val),
        }) => Ok(new_val),
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Err(_err),
        }) => {
            // return Err(OpError::from(ContractError::StorageError(err)));
            todo!("not a valid value update, notify back to requester")
        }
        Err(err) => Err(err.into()),
        Ok(_) => Err(OpError::UnexpectedOpState),
    }
}

// TODO: keep track of who is supposed to have the contract, and only send if necessary
// since sending the contract over and over, will be expensive; this can be done via subscriptions
/// Communicate changes in the contract to other peers nearby the contract location.
/// This operation is "fire and forget" and the node does not keep track if is successful or not.
async fn forward_changes<CB>(
    op_storage: &OpManager,
    conn_manager: &CB,
    contract: &ContractContainer,
    new_value: WrappedState,
    id: Transaction,
    htl: usize,
    skip_list: &[PeerKey],
) where
    CB: NetworkBridge,
{
    let key = contract.key();
    let contract_loc = Location::from(&key);
    let forward_to = op_storage.ring.closest_caching(&key, skip_list);
    let own_loc = op_storage.ring.own_location().location.expect("infallible");
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
                        contract: contract.clone(),
                        new_value: new_value.clone(),
                        htl,
                        skip_list: skip_list.to_vec(),
                    })
                    .into(),
                )
                .await;
        }
    }
}

mod messages {
    use std::fmt::Display;

    use super::*;

    use crate::message::InnerMessage;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum PutMsg {
        /// Initialize the put operation by routing the value
        RouteValue {
            id: Transaction,
            htl: usize,
            target: PeerKeyLocation,
        },
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
            contract: ContractContainer,
            new_value: WrappedState,
            /// current htl, reduced by one at each hop
            htl: usize,
            skip_list: Vec<PeerKey>,
        },
        /// Value successfully inserted/updated.
        SuccessfulUpdate {
            id: Transaction,
            new_value: WrappedState,
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
            // FIXME: remove skip list once we deduplicate at top msg handling level
            // using this is a tmp workaround until (https://github.com/freenet/freenet-core/issues/13) is done
            skip_list: Vec<PeerKey>,
        },
        /// Internal node instruction that  a change (either a first time insert or an update).
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_value: WrappedState,
            #[serde(deserialize_with = "Parameters::deser_params")]
            parameters: Parameters<'static>,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            #[serde(deserialize_with = "Parameters::deser_params")]
            parameters: Parameters<'static>,
            sender_subscribers: Vec<PeerKeyLocation>,
        },
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                Self::RouteValue { id, .. } => id,
                Self::RequestPut { id, .. } => id,
                Self::Broadcasting { id, .. } => id,
                Self::SuccessfulUpdate { id, .. } => id,
                Self::PutForward { id, .. } => id,
                Self::AwaitPut { id } => id,
                Self::BroadcastTo { id, .. } => id,
            }
        }

        fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::RequestPut { target, .. } => Some(target),
                _ => None,
            }
        }

        fn terminal(&self) -> bool {
            use PutMsg::*;
            matches!(
                self,
                SuccessfulUpdate { .. } | SeekNode { .. } | PutForward { .. }
            )
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
                Self::RouteValue { .. } => write!(f, "RouteValue(id: {id})"),
                Self::RequestPut { .. } => write!(f, "RequestPut(id: {id})"),
                Self::Broadcasting { .. } => write!(f, "Broadcasting(id: {id})"),
                Self::SuccessfulUpdate { .. } => write!(f, "SusscessfulUpdate(id: {id})"),
                Self::PutForward { .. } => write!(f, "PutForward(id: {id})"),
                Self::AwaitPut { .. } => write!(f, "AwaitPut(id: {id})"),
                Self::BroadcastTo { .. } => write!(f, "BroadcastTo(id: {id})"),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, time::Duration};

    use freenet_stdlib::client_api::ContractRequest;
    use freenet_stdlib::prelude::*;

    use crate::node::tests::{NodeSpecification, SimNetwork};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn successful_put_op_between_nodes() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 2usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1kb();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let key = contract.key().clone();
        let contract_val: WrappedState = gen.arbitrary()?;
        let new_value = WrappedState::new(Vec::from_iter(gen.arbitrary::<[u8; 20]>().unwrap()));

        let mut sim_nw = SimNetwork::new(
            "successful_put_op_between_nodes",
            NUM_GW,
            NUM_NODES,
            2,
            1,
            3,
            2,
        )
        .await;
        let mut locations = sim_nw.get_locations_by_node();
        let node0_loc = locations.remove(&"node-0".into()).unwrap();
        let node1_loc = locations.remove(&"node-1".into()).unwrap();

        // both own the contract, and one triggers an update
        let node_0 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone())),
                contract_val.clone(),
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::from_iter([(key.clone(), vec![node1_loc])]),
        };

        let node_1 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone())),
                contract_val.clone(),
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::from_iter([(key.clone(), vec![node0_loc])]),
        };

        let put_event = ContractRequest::Put {
            contract: ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone())),
            state: new_value.clone(),
            related_contracts: Default::default(),
        }
        .into();

        let gw_0 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone())),
                contract_val,
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::from_iter([(1, put_event)]),
            contract_subscribers: HashMap::new(),
        };

        // establish network
        let put_specs = HashMap::from_iter([
            ("node-0".into(), node_0),
            ("node-1".into(), node_1),
            ("gateway-0".into(), gw_0),
        ]);

        sim_nw.start_with_spec(put_specs).await;
        sim_nw.check_connectivity(Duration::from_secs(3)).await?;

        // trigger the put op @ gw-0
        sim_nw
            .trigger_event("gateway-0", 1, Some(Duration::from_millis(200)))
            .await?;
        assert!(sim_nw.has_put_contract("gateway-0", &key, &new_value));
        assert!(sim_nw.event_listener.contract_broadcasted(&key));
        Ok(())
    }
}
