//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

use std::future::Future;
use std::pin::Pin;

pub(crate) use self::messages::PutMsg;
use freenet_stdlib::{
    client_api::{ErrorKind, HostResponse},
    prelude::*,
};

use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
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
        self.state.is_none()
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(PutState::Finished { key }) = &self.state {
            Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::PutResponse { key: key.clone() },
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

            match input {
                PutMsg::RequestPut {
                    id,
                    contract,
                    related_contracts,
                    value,
                    htl,
                    target,
                } => {
                    let sender = op_manager.ring.own_location();

                    let key = contract.key();
                    tracing::debug!(
                        "Requesting put for contract {} from {} to {}",
                        key,
                        sender.peer,
                        target.peer
                    );

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

                    // no changes to state yet, still in AwaitResponse state
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
                    let key = contract.key();
                    let is_subscribed_contract = op_manager.ring.is_seeding_contract(&key);

                    tracing::debug!(
                        tx = %id,
                        %key,
                        target = %target.peer,
                        "Puttting contract at target peer",
                    );

                    if is_subscribed_contract || op_manager.ring.should_seed(&key) {
                        tracing::debug!(tx = %id, "Attempting contract value update");
                        put_contract(
                            op_manager,
                            key.clone(),
                            value.clone(),
                            related_contracts.clone(),
                            contract,
                        )
                        .await?;
                        tracing::debug!(
                            tx = %id,
                            "Successfully updated a value for contract {} @ {:?}",
                            key,
                            target.location
                        );
                    }

                    let last_hop = if let Some(new_htl) = htl.checked_sub(1) {
                        // forward changes in the contract to nodes closer to the contract location, if possible
                        let put_here = forward_put(
                            op_manager,
                            conn_manager,
                            contract,
                            value.clone(),
                            *id,
                            new_htl,
                            vec![sender.peer.clone()],
                        )
                        .await;
                        if put_here && !is_subscribed_contract {
                            // if already subscribed the value was already put and merging succeeded
                            put_contract(
                                op_manager,
                                key.clone(),
                                value.clone(),
                                RelatedContracts::default(),
                                contract,
                            )
                            .await?;
                        }
                        put_here
                    } else {
                        // should put in this location, no hops left
                        put_contract(
                            op_manager,
                            key.clone(),
                            value.clone(),
                            RelatedContracts::default(),
                            contract,
                        )
                        .await?;
                        true
                    };

                    let broadcast_to = op_manager.get_broadcast_targets(&key, &sender.peer);

                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        (broadcast_to, sender.clone()),
                        key.clone(),
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
                } => {
                    let target = op_manager.ring.own_location();

                    tracing::debug!("Attempting contract value update");
                    let new_value = put_contract(
                        op_manager,
                        key.clone(),
                        new_value.clone(),
                        RelatedContracts::default(),
                        contract,
                    )
                    .await?;
                    tracing::debug!("Contract successfully updated");

                    let broadcast_to = op_manager.get_broadcast_targets(key, &sender.peer);
                    tracing::debug!(
                        "Successfully updated a value for contract {} @ {:?}",
                        key,
                        target.location
                    );

                    match try_to_broadcast(
                        *id,
                        false,
                        op_manager,
                        self.state,
                        (broadcast_to, sender.clone()),
                        key.clone(),
                        (contract.clone(), new_value),
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
                    contract,
                    upstream,
                } => {
                    let sender = op_manager.ring.own_location();
                    let mut broadcasted_to = *broadcasted_to;

                    let mut broadcasting = Vec::with_capacity(broadcast_to.len());
                    for peer in broadcast_to.iter() {
                        let msg = PutMsg::BroadcastTo {
                            id: *id,
                            key: key.clone(),
                            new_value: new_value.clone(),
                            sender: sender.clone(),
                            contract: contract.clone(),
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
                            "failed broadcasting put change to {} with error {}; dropping connection",
                            peer.peer,
                            err
                        );
                        // todo: review this, maybe we should just dropping this subscription
                        conn_manager.drop_connection(&peer.peer).await?;
                        incorrect_results += 1;
                    }

                    broadcasted_to += broadcast_to.len() - incorrect_results;
                    tracing::debug!(
                        "Successfully broadcasted put into contract {key} to {broadcasted_to} peers"
                    );

                    // Subscriber nodes have been notified of the change, the operation is completed
                    return_msg = Some(PutMsg::SuccessfulPut {
                        id: *id,
                        target: upstream.clone(),
                        key: key.clone(),
                    });
                    new_state = None;
                }
                PutMsg::SuccessfulPut { id, .. } => {
                    match self.state {
                        Some(PutState::AwaitingResponse { key, upstream }) => {
                            let is_subscribed_contract = op_manager.ring.is_seeding_contract(&key);
                            if !is_subscribed_contract && op_manager.ring.should_seed(&key) {
                                tracing::debug!(tx = %id, %key, peer = %op_manager.ring.get_peer_key().unwrap(), "Contract not cached @ peer, caching");
                                super::start_subscription_request(op_manager, key.clone(), true)
                                    .await;
                            }
                            tracing::info!(
                                tx = %id,
                                %key,
                                this_peer = %op_manager.ring.get_peer_key().unwrap(),
                                "Peer completed contract value put",
                            );
                            new_state = Some(PutState::Finished { key: key.clone() });
                            if let Some(upstream) = upstream {
                                return_msg = Some(PutMsg::SuccessfulPut {
                                    id: *id,
                                    target: upstream,
                                    key,
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
                } => {
                    let key = contract.key();
                    let peer_loc = op_manager.ring.own_location();

                    tracing::debug!(
                        %key,
                        this_peer = % peer_loc.peer,
                        "Forwarding changes, trying put the contract"
                    );

                    let should_seed = op_manager.ring.should_seed(&key);
                    if should_seed {
                        // after the contract has been cached, push the update query
                        put_contract(
                            op_manager,
                            key.clone(),
                            new_value.clone(),
                            RelatedContracts::default(),
                            contract,
                        )
                        .await?;
                    }

                    // if successful, forward to the next closest peers (if any)
                    let last_hop = if let Some(new_htl) = htl.checked_sub(1) {
                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.push(sender.peer.clone());
                        // only hop forward if there are closer peers
                        let put_here = forward_put(
                            op_manager,
                            conn_manager,
                            contract,
                            new_value.clone(),
                            *id,
                            new_htl,
                            new_skip_list,
                        )
                        .await;
                        let is_seeding_contract = op_manager.ring.is_seeding_contract(&key);
                        if put_here && !is_seeding_contract && should_seed {
                            // if already subscribed the value was already put and merging succeeded
                            put_contract(
                                op_manager,
                                key.clone(),
                                new_value.clone(),
                                RelatedContracts::default(),
                                contract,
                            )
                            .await?;
                            let (dropped_contract, old_subscribers) =
                                op_manager.ring.seed_contract(key.clone());
                            if let Some(key) = dropped_contract {
                                for subscriber in old_subscribers {
                                    conn_manager
                                        .send(
                                            &subscriber.peer,
                                            NetMessage::V1(NetMessageV1::Unsubscribed {
                                                transaction: Transaction::new::<PutMsg>(),
                                                key: key.clone(),
                                                from: op_manager.ring.get_peer_key().unwrap(),
                                            }),
                                        )
                                        .await?;
                                }
                            }
                        }
                        put_here
                    } else {
                        // should put in this location, no hops left
                        put_contract(
                            op_manager,
                            key.clone(),
                            new_value.clone(),
                            RelatedContracts::default(),
                            contract,
                        )
                        .await?;
                        true
                    };

                    let broadcast_to = op_manager.get_broadcast_targets(&key, &sender.peer);
                    match try_to_broadcast(
                        *id,
                        last_hop,
                        op_manager,
                        self.state,
                        (broadcast_to, sender.clone()),
                        key.clone(),
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
    let output_op = Some(PutOp { id, state, stats });
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
        Some(PutState::ReceivedRequest | PutState::BroadcastOngoing { .. }) => {
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
                return_msg = Some(PutMsg::SuccessfulPut {
                    id,
                    target: upstream,
                    key: key.clone(),
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
    });

    PutOp {
        id,
        state,
        stats: Some(PutStats { target: None }),
    }
}

pub enum PutState {
    ReceivedRequest,
    PrepareRequest {
        contract: ContractContainer,
        related_contracts: RelatedContracts<'static>,
        value: WrappedState,
        htl: usize,
    },
    AwaitingResponse {
        key: ContractKey,
        upstream: Option<PeerKeyLocation>,
    },
    BroadcastOngoing,
    Finished {
        key: ContractKey,
    },
}

/// Request to insert/update a value into a contract.
pub(crate) async fn request_put(op_manager: &OpManager, mut put_op: PutOp) -> Result<(), OpError> {
    let key = if let Some(PutState::PrepareRequest { contract, .. }) = &put_op.state {
        contract.key()
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    let sender = op_manager.ring.own_location();

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
            value,
            htl,
            related_contracts,
        }) => {
            let key = contract.key();
            let new_state = Some(PutState::AwaitingResponse {
                key,
                upstream: None,
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
                state: new_state,
                id,
                stats: put_op.stats,
            };

            op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Put(op))
                .await?;
        }
        _ => return Err(OpError::invalid_transition(put_op.id)),
    };

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
            new_value: Err(_err),
        }) => {
            // return Err(OpError::from(ContractError::StorageError(err)));
            todo!("not a valid value update, notify back to requester")
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
    skip_list: Vec<PeerId>,
) -> bool
where
    CB: NetworkBridge,
{
    let key = contract.key();
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
            let _ = conn_manager
                .send(
                    &peer.peer,
                    (PutMsg::PutForward {
                        id,
                        sender: own_pkloc,
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

    #[derive(Debug, Serialize, Deserialize)]
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
            contract: ContractContainer,
            new_value: WrappedState,
            /// current htl, reduced by one at each hop
            htl: usize,
            skip_list: Vec<PeerId>,
        },
        /// Value successfully inserted/updated.
        SuccessfulPut {
            id: Transaction,
            target: PeerKeyLocation,
            key: ContractKey,
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
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            contract: ContractContainer,
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
                _ => None,
            }
        }

        fn terminal(&self) -> bool {
            use PutMsg::*;
            matches!(
                self,
                SuccessfulPut { .. } | SeekNode { .. } | PutForward { .. }
            )
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
                Self::SuccessfulPut { .. } => write!(f, "SusscessfulUpdate(id: {id})"),
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

    use crate::node::testing_impl::{NodeSpecification, SimNetwork};

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
        let node0_loc = locations.remove(&"node-1".into()).unwrap();
        let node1_loc = locations.remove(&"node-2".into()).unwrap();

        // both own the contract, and one triggers an update
        let node_1 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone())),
                contract_val.clone(),
                false,
            )],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
        };

        let node_2 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone())),
                contract_val.clone(),
                false,
            )],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
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
                false,
            )],
            events_to_generate: HashMap::from_iter([(1, put_event)]),
            contract_subscribers: HashMap::from_iter([(key.clone(), vec![node0_loc, node1_loc])]),
        };

        // establish network
        let put_specs = HashMap::from_iter([
            ("node-1".into(), node_1),
            ("node-2".into(), node_2),
            ("gateway-0".into(), gw_0),
        ]);

        sim_nw.start_with_spec(put_specs).await;
        sim_nw.check_connectivity(Duration::from_secs(3))?;

        // trigger the put op @ gw-0
        sim_nw
            .trigger_event("gateway-0", 1, Some(Duration::from_secs(1)))
            .await?;
        assert!(sim_nw.has_put_contract("gateway-0", &key));
        assert!(sim_nw.event_listener.contract_broadcasted(&key));
        Ok(())
    }
}
