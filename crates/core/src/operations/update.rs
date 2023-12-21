use std::time::Instant;

// TODO: complete update logic in the network
use freenet_stdlib::prelude::*;
use futures::future::BoxFuture;
use futures::FutureExt;

use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation};
use crate::message::{InnerMessage, Transaction};
use crate::ring::{Location, PeerKeyLocation};
use crate::{
    client_events::{ClientId, HostResult},
    node::{NetworkBridge, OpManager, PeerId},
};

pub(crate) use self::messages::UpdateMsg;

pub(crate) struct UpdateOp {
    pub id: Transaction,
    state: Option<UpdateState>,
    stats: Option<UpdateStats>,
}

impl UpdateOp {
    pub fn outcome(&self) -> OpOutcome {
        OpOutcome::Irrelevant
    }

    pub fn finalized(&self) -> bool {
        todo!()
    }

    pub fn record_transfer(&mut self) {}

    pub(super) fn to_host_result(&self) -> HostResult {
        todo!()
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

    fn try_from(_value: UpdateOp) -> Result<Self, Self::Error> {
        todo!()
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
        input: &Self::Message,
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
                    contract,
                    target,
                    key,
                    new_state,
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
                    return_msg = Some(UpdateMsg::SeekNode {
                        id: *id,
                        sender,
                        target: *target,
                        new_state: new_state.clone(),
                        contract: contract.clone(),
                        htl: *htl,
                    });

                    // no changes to state yet, still in AwaitResponse state
                    new_state = self.state;
                }
                UpdateMsg::SeekNode {
                    id,
                    new_state,
                    contract,
                    htl,
                    target,
                    sender,
                } => {
                    let key = contract.key();
                    let is_subscribed_contract = op_manager.ring.is_subscribed_to_contract(&key);

                    tracing::debug!(
                        tx = %id,
                        %key,
                        target = %target.peer,
                        "Puttting contract at target peer",
                    );

                    if is_subscribed_contract
                        || op_manager
                            .ring
                            .within_subscribing_distance(&Location::from(&key))
                    {
                        tracing::debug!(tx = %id, "Attempting contract value update");
                        put_contract(op_manager, key.clone(), value.clone(), contract).await?;
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
                            vec![sender.peer],
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
                        (broadcast_to, *sender),
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
                UpdateMsg::BroadcastTo {
                    id,
                    key,
                    new_state,
                    contract,
                    sender,
                } => {
                    let target = op_manager.ring.own_location();

                    tracing::debug!("Attempting contract value update");
                    let new_value = put_contract(
                        op_manager,
                        key.clone(),
                        new_state.clone(),
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
                        (broadcast_to, *sender),
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
                UpdateMsg::Broadcasting {
                    id,
                    broadcast_to,
                    broadcasted_to,
                    key,
                    new_state,
                    contract,
                    upstream,
                } => {
                    let sender = op_manager.ring.own_location();
                    let mut broadcasted_to = *broadcasted_to;

                    let mut broadcasting = Vec::with_capacity(broadcast_to.len());
                    for peer in broadcast_to.iter() {
                        let msg = UpdateMsg::BroadcastTo {
                            id: *id,
                            key: key.clone(),
                            new_state: new_state.clone(),
                            sender,
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
                    return_msg = Some(UpdateMsg::SuccessfulPut {
                        id: *id,
                        target: *upstream,
                    });
                    new_state = None;
                }
                UpdateMsg::SuccessfulUpdate { id, .. } => {
                    match self.state {
                        Some(UpdateState::AwaitingResponse { key, upstream }) => {
                            let is_subscribed_contract =
                                op_manager.ring.is_subscribed_to_contract(&key);
                            if !is_subscribed_contract
                                && op_manager
                                    .ring
                                    .within_subscribing_distance(&Location::from(&key))
                            {
                                tracing::debug!(tx = %id, %key, peer = %op_manager.ring.peer_key, "Contract not cached @ peer, caching");
                                super::start_subscription_request(op_manager, key.clone(), true)
                                    .await;
                            }
                            tracing::info!(
                                tx = %id,
                                %key,
                                this_peer = %op_manager.ring.peer_key,
                                "Peer completed contract value put",
                            );
                            new_state = Some(UpdateState::Finished { key });
                            if let Some(upstream) = upstream {
                                return_msg = Some(UpdateMsg::SuccessfulPut {
                                    id: *id,
                                    target: upstream,
                                });
                            } else {
                                return_msg = None;
                            }
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    };
                }
                UpdateMsg::UpdateForward {
                    id,
                    contract,
                    new_state,
                    htl,
                    sender,
                } => {
                    let key = contract.key();
                    let peer_loc = op_manager.ring.own_location();

                    tracing::debug!(
                        %key,
                        this_peer = % peer_loc.peer,
                        "Forwarding changes, trying put the contract"
                    );

                    let is_subscribed_contract = op_manager.ring.is_subscribed_to_contract(&key);
                    let within_caching_dist = op_manager
                        .ring
                        .within_subscribing_distance(&Location::from(&key));
                    if is_subscribed_contract || within_caching_dist {
                        // after the contract has been cached, push the update query
                        update_contract(
                            op_manager,
                            key.clone(),
                            new_state.clone(),
                            RelatedContracts::default(),
                            contract,
                        )
                        .await?;
                    }

                    // if successful, forward to the next closest peers (if any)
                    let last_hop = if let Some(new_htl) = htl.checked_sub(1) {
                        // only hop forward if there are closer peers
                        let put_here = forward_update(
                            op_manager,
                            conn_manager,
                            contract,
                            new_state.clone(),
                            *id,
                            new_htl,
                        )
                        .await;
                        if put_here && !is_subscribed_contract {
                            // if already subscribed the value was already put and merging succeeded
                            update_contract(
                                op_manager,
                                key.clone(),
                                new_state.clone(),
                                RelatedContracts::default(),
                                contract,
                            )
                            .await?;
                        }
                        put_here
                    } else {
                        // should put in this location, no hops left
                        update_contract(
                            op_manager,
                            key.clone(),
                            new_state.clone(),
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
                        (broadcast_to, *sender),
                        key.clone(),
                        (contract.clone(), new_state.clone()),
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

async fn forward_update<NB: NetworkBridge>(
    op_manager: &OpManager,
    conn_manager: &mut NB,
    contract: &ContractContainer,
    clone: WrappedState,
    id: Transaction,
    new_htl: usize,
) -> bool {
    todo!()
}

async fn update_contract(
    op_manager: &OpManager,
    clone_1: ContractKey,
    clone_2: WrappedState,
    default: RelatedContracts<'_>,
    contract: &ContractContainer,
) -> _ {
    todo!()
}

// todo: new_state should be a delta when possible!
pub(crate) fn start_op(_key: ContractKey, _new_state: WrappedState, _htl: usize) -> UpdateOp {
    todo!()
}

pub(crate) async fn request_update(
    _op_manager: &OpManager,
    _update_op: UpdateOp,
    _client_id: Option<ClientId>,
) -> Result<(), OpError> {
    todo!()
}

mod messages {
    use std::fmt::Display;

    use freenet_stdlib::prelude::{ContractContainer, ContractKey, WrappedState};
    use serde::{Deserialize, Serialize};

    use crate::{
        message::{InnerMessage, Transaction},
        ring::PeerKeyLocation,
    };

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum UpdateMsg {
        RequestUpdate {
            id: Transaction,
            contract: ContractContainer,
            target: PeerKeyLocation,
            key: ContractKey,
            new_state: WrappedState,
        },
        /// Forward a contract and it's latest value to an other node
        UpdateForward {
            id: Transaction,
            sender: PeerKeyLocation,
            contract: ContractContainer,
            new_state: WrappedState,
            /// current htl, reduced by one at each hop
            htl: usize,
        },
        /// Value successfully inserted/updated.
        SuccessfulUpdate {
            id: Transaction,
            target: PeerKeyLocation,
        },
        AwaitUpdate {
            id: Transaction,
        },
        SeekNode {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            new_state: WrappedState,
            contract: ContractContainer,
            /// max hops to live
            htl: usize,
        },
        /// Internal node instruction that  a change (either a first time insert or an update).
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_state: WrappedState,
            contract: ContractContainer,
            upstream: PeerKeyLocation,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_state: WrappedState,
            contract: ContractContainer,
        },
    }

    impl InnerMessage for UpdateMsg {
        fn id(&self) -> &Transaction {
            todo!()
        }

        fn target(&self) -> Option<&PeerKeyLocation> {
            todo!()
        }

        fn terminal(&self) -> bool {
            todo!()
        }

        fn requested_location(&self) -> Option<crate::ring::Location> {
            todo!()
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
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            todo!()
        }
    }
}

#[derive(Debug)]
enum UpdateState {
    ReceivedRequest,
    AwaitingResponse {
        key: ContractKey,
        upstream: PeerKeyLocation,
    },
    Finished {
        key: ContractKey,
    },
}
