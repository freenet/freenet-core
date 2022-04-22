//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

use std::collections::HashSet;
use std::time::Duration;

use locutus_runtime::prelude::ContractKey;

use crate::{
    config::PEER_TIMEOUT,
    contract::{ContractError, ContractHandlerEvent},
    message::{Message, Transaction, TxType},
    node::{ConnectionBridge, OpManager, PeerKey},
    ring::{Location, PeerKeyLocation, RingError},
    WrappedContract, WrappedState,
};

pub(crate) use self::messages::PutMsg;

use super::{
    handle_op_result,
    state_machine::{StateMachine, StateMachineImpl},
    OpError, Operation, OperationResult,
};

pub(crate) struct PutOp {
    sm: StateMachine<PutOpSm>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl PutOp {
    pub fn start_op(
        contract: WrappedContract,
        value: WrappedState,
        htl: usize,
        peer: &PeerKey,
    ) -> Self {
        log::debug!(
            "Requesting put to contract {} @ loc({})",
            contract.key(),
            Location::from(&contract.key())
        );

        let id = Transaction::new(<PutMsg as TxType>::tx_type_id(), peer);
        let sm = StateMachine::from_state(
            PutState::PrepareRequest {
                id,
                contract,
                value,
                htl,
            },
            id,
        );
        PutOp {
            sm,
            _ttl: PEER_TIMEOUT,
        }
    }

    pub fn id(&self) -> Transaction {
        self.sm.id
    }
}

struct PutOpSm;

impl StateMachineImpl for PutOpSm {
    type Input = PutMsg;

    type State = PutState;

    type Output = PutMsg;

    fn state_transition_from_input(state: Self::State, input: Self::Input) -> Option<Self::State> {
        // FIXME: receive broadcast msg when awaiting state
        match (state, input) {
            // state changed for the initial requesting node
            (PutState::AwaitingResponse { contract }, PutMsg::RequestPut { .. }) => {
                Some(PutState::AwaitingResponse { contract })
            }
            (PutState::AwaitingResponse { contract, .. }, PutMsg::SuccessfulUpdate { .. }) => {
                log::debug!("Successfully updated value for {}", contract,);
                Some(PutState::Done)
            }
            _ => None,
        }
    }

    fn state_transition(state: &mut Self::State, input: &mut Self::Input) -> Option<Self::State> {
        match (state, input) {
            // state changed for the initial requesting node
            (PutState::PrepareRequest { contract, .. }, PutMsg::RouteValue { .. }) => {
                Some(PutState::AwaitingResponse {
                    contract: contract.key(),
                })
            }
            // state changes for the target node
            (
                PutState::ReceivedRequest | PutState::BroadcastOngoing { .. },
                PutMsg::Broadcasting { broadcast_to, .. },
            ) => {
                if broadcast_to.is_empty() {
                    // broadcast complete
                    Some(PutState::BroadcastComplete)
                } else {
                    Some(PutState::BroadcastOngoing)
                }
            }
            _ => None,
        }
    }

    fn output_from_input(state: Self::State, input: Self::Input) -> Option<Self::Output> {
        match (state, input) {
            // output from requester
            (
                PutState::PrepareRequest {
                    contract,
                    value,
                    htl,
                    ..
                },
                PutMsg::RouteValue { id, target, .. },
            ) => Some(PutMsg::RequestPut {
                id,
                contract,
                value,
                htl,
                target,
            }),
            (
                PutState::PrepareRequest { .. },
                PutMsg::SeekNode {
                    id,
                    target,
                    sender,
                    contract,
                    value,
                    htl,
                    skip_list,
                },
            ) => Some(PutMsg::SeekNode {
                id,
                target,
                sender,
                contract,
                value,
                htl,
                skip_list,
            }),
            // output from initial target
            (
                PutState::ReceivedRequest | PutState::BroadcastOngoing { .. },
                PutMsg::Broadcasting {
                    id,
                    new_value,
                    broadcasted_to,
                    broadcast_to,
                    key,
                },
            ) => {
                if broadcast_to.is_empty() {
                    Some(PutMsg::SuccessfulUpdate { id, new_value })
                } else {
                    Some(PutMsg::Broadcasting {
                        id,
                        new_value,
                        broadcasted_to,
                        broadcast_to,
                        key,
                    })
                }
            }
            _ => None,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
enum PutState {
    ReceivedRequest,
    PrepareRequest {
        id: Transaction,
        contract: WrappedContract,
        value: WrappedState,
        htl: usize,
    },
    AwaitingResponse {
        contract: ContractKey,
    },
    Done,
    BroadcastOngoing,
    BroadcastComplete,
}

impl PutState {
    fn id(&self) -> &Transaction {
        match self {
            Self::PrepareRequest { id, .. } => id,
            _ => unreachable!(),
        }
    }
}

/// Request to insert/update a value into a contract.
pub(crate) async fn request_put<CErr>(
    op_storage: &OpManager<CErr>,
    mut put_op: PutOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let key = if let PutState::PrepareRequest { contract, .. } = put_op.sm.state() {
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
        .closest_caching(&key, 1, &[sender.peer])
        .into_iter()
        .next()
        .ok_or_else(|| OpError::from(RingError::EmptyRing))?;

    let id = *put_op.sm.state().id();
    if let Some(req_put) = put_op.sm.consume_to_output(PutMsg::RouteValue {
        id,
        htl: op_storage.ring.max_hops_to_live,
        target,
    })? {
        op_storage
            .notify_op_change(Message::from(req_put), Operation::Put(put_op))
            .await?;
    } else {
        return Err(OpError::UnexpectedOpState);
    }
    Ok(())
}

pub(crate) async fn handle_put_request<CB, CErr>(
    op_storage: &OpManager<CErr>,
    conn_manager: &mut CB,
    put_op: PutMsg,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    OpError<CErr>: From<ContractError<CErr>>,
    CErr: std::error::Error,
{
    let sender;
    let tx = *put_op.id();
    let result = match op_storage.pop(put_op.id()) {
        Some(Operation::Put(state)) => {
            sender = put_op.sender().cloned();
            // was an existing operation, the other peer messaged back
            update_state(conn_manager, state, put_op, op_storage).await
        }
        Some(_) => return Err(OpError::OpNotPresent(tx)),
        None => {
            sender = put_op.sender().cloned();
            // new request to put a new value for a contract, initialize the machine
            let machine = PutOp {
                sm: StateMachine::from_state(PutState::ReceivedRequest, tx),
                _ttl: PEER_TIMEOUT,
            };
            update_state(conn_manager, machine, put_op, op_storage).await
        }
    };

    handle_op_result(
        op_storage,
        conn_manager,
        result.map_err(|err| (err, tx)),
        sender.map(|p| p.peer),
    )
    .await
}

async fn update_state<CB, CErr>(
    conn_manager: &mut CB,
    mut state: PutOp,
    other_host_msg: PutMsg,
    op_storage: &OpManager<CErr>,
) -> Result<OperationResult, OpError<CErr>>
where
    CB: ConnectionBridge,
    OpError<CErr>: From<ContractError<CErr>>,
    CErr: std::error::Error,
{
    let return_msg;
    let new_state;
    match other_host_msg {
        PutMsg::RequestPut {
            id,
            contract,
            value,
            htl,
            target,
        } => {
            let sender = op_storage.ring.own_location();

            log::debug!(
                "Performing a RequestPut for contract {} from {} to {}",
                contract.key(),
                sender.peer,
                target.peer
            );

            return_msg = Some(Message::from(PutMsg::SeekNode {
                id,
                sender,
                target,
                value,
                contract,
                htl,
                skip_list: vec![sender.peer],
            }));

            // no changes to state yet, still in AwaitResponse state
            new_state = Some(state);
        }
        PutMsg::SeekNode {
            id,
            sender,
            value,
            contract,
            htl,
            target,
            mut skip_list,
        } => {
            let key = contract.key();
            let cached_contract = op_storage.ring.is_contract_cached(&key);

            log::debug!(
                "Performing a SeekNode at {}, trying put the contract {}",
                target.peer,
                key
            );

            if !cached_contract
                && op_storage
                    .ring
                    .within_caching_distance(&Location::from(&key))
            {
                log::debug!("Contract `{}` not cached @ peer {}", key, target.peer);
                // this node does not have the contract, so instead store the contract and execute the put op.
                let res = op_storage
                    .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()))
                    .await?;
                if let ContractHandlerEvent::CacheResult(Ok(_)) = res {
                    op_storage.ring.contract_cached(key);
                    log::debug!("Contract successfully cached");
                } else {
                    log::error!(
                        "Contract handler returned wrong event when trying to cache contract, this should not happen!"
                    );
                    return Err(OpError::UnexpectedOpState);
                }
            } else if !cached_contract {
                // in this case forward to a closer node to the target location and just wait for a response
                // to give back to requesting peer
                // FIXME
                log::warn!(
                    "Contract {} not found while processing info, forwarding",
                    key
                );
            }

            // after the contract has been cached, push the update query
            log::debug!("Attempting contract value update");
            let new_value = put_contract(op_storage, key, value).await?;
            log::debug!("Contract successfully updated");
            // if the change was successful, communicate this back to the requestor and broadcast the change
            conn_manager
                .send(
                    &sender.peer,
                    (PutMsg::SuccessfulUpdate {
                        id,
                        new_value: new_value.clone(),
                    })
                    .into(),
                )
                .await?;
            skip_list.push(target.peer);

            if let Some(new_htl) = htl.checked_sub(1) {
                // forward changes in the contract to nodes closer to the contract location, if possible
                forward_changes(
                    op_storage,
                    conn_manager,
                    &contract,
                    new_value.clone(),
                    id,
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
            log::debug!(
                "Successfully updated a value for contract {} @ {:?}",
                key,
                target.location
            );

            let internal_cb = state
                .sm
                .consume_to_output(PutMsg::Broadcasting {
                    id,
                    broadcast_to,
                    broadcasted_to: 0,
                    key,
                    new_value,
                })?
                .ok_or(OpError::InvalidStateTransition(id))?;

            if let PutMsg::SuccessfulUpdate { .. } = internal_cb {
                log::debug!(
                    "Empty broadcast list while updating value for contract {}",
                    key
                );
                // means the whole tx finished so can return early
                return_msg = Some(internal_cb.into());
                new_state = None;
            } else {
                op_storage
                    .notify_op_change(internal_cb.into(), Operation::Put(state))
                    .await?;
                return Err(OpError::StatePushed);
            }
        }
        PutMsg::BroadcastTo {
            id,
            key,
            new_value,
            sender,
            sender_subscribers,
        } => {
            let target = op_storage.ring.own_location();

            log::debug!("Attempting contract value update");
            let new_value = put_contract(op_storage, key, new_value).await?;
            log::debug!("Contract successfully updated");

            let broadcast_to = op_storage
                .ring
                .subscribers_of(&key)
                .map(|i| {
                    // Avoid already broadcast nodes and sender from broadcasting
                    let mut subscribers: Vec<PeerKeyLocation> = i.value().to_vec();
                    let mut avoid_list: HashSet<PeerKey> =
                        sender_subscribers.into_iter().map(|pl| pl.peer).collect();
                    avoid_list.insert(sender.peer);
                    subscribers.retain(|s| !avoid_list.contains(&s.peer));
                    subscribers
                })
                .unwrap_or_default();
            log::debug!(
                "Successfully updated a value for contract {} @ {:?}",
                key,
                target.location
            );

            let internal_cb = state
                .sm
                .consume_to_output(PutMsg::Broadcasting {
                    id,
                    broadcast_to,
                    broadcasted_to: 0,
                    key,
                    new_value,
                })?
                .ok_or(OpError::InvalidStateTransition(id))?;

            if let PutMsg::SuccessfulUpdate { .. } = internal_cb {
                log::debug!(
                    "Empty broadcast list while updating value for contract {}",
                    key
                );
                // means the whole tx finished so can return early
                return_msg = Some(internal_cb.into());
                new_state = None;
            } else {
                log::debug!("Callback to start broadcasting to other nodes");
                op_storage
                    .notify_op_change(internal_cb.into(), Operation::Put(state))
                    .await?;
                return Err(OpError::StatePushed);
            }
        }
        PutMsg::Broadcasting {
            id,
            mut broadcast_to,
            mut broadcasted_to,
            key,
            new_value,
        } => {
            let sender = op_storage.ring.own_location();
            let msg = PutMsg::BroadcastTo {
                id,
                key,
                new_value: new_value.clone(),
                sender,
                sender_subscribers: broadcast_to.clone(),
            };

            let mut broadcasting = Vec::with_capacity(broadcast_to.len());
            for peer in &broadcast_to {
                let f = conn_manager.send(&peer.peer, msg.clone().into());
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
                let peer = broadcast_to.remove(peer_num);
                log::warn!(
                    "failed broadcasting put change to {} with error {}; dropping connection",
                    peer.peer,
                    err
                );
                conn_manager.drop_connection(&peer.peer).await?;
                incorrect_results += 1;
            }

            broadcasted_to += broadcast_to.len() - incorrect_results;
            log::debug!(
                "successfully broadcasted put into contract {} to {} peers",
                key,
                broadcasted_to
            );

            // Subscriber nodes have been notified of the change, the operation is completed
            return_msg = None;
            new_state = None;
        }
        PutMsg::SuccessfulUpdate { id, new_value } => {
            return_msg = state
                .sm
                .consume_to_state(PutMsg::SuccessfulUpdate { id, new_value })?
                .map(Message::from);
            log::debug!(
                "Peer {} completed contract value put",
                op_storage.ring.peer_key
            );
            new_state = None;
        }
        PutMsg::PutForward {
            id,
            contract,
            new_value,
            htl,
            mut skip_list,
        } => {
            let key = contract.key();
            let cached_contract = op_storage.ring.is_contract_cached(&key);
            let within_caching_dist = op_storage
                .ring
                .within_caching_distance(&Location::from(&key));
            if !cached_contract && within_caching_dist {
                // this node does not have the contract, so instead store the contract and execute the put op.
                let res = op_storage
                    .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()))
                    .await?;
                if let ContractHandlerEvent::CacheResult(Ok(_)) = res {
                    op_storage.ring.contract_cached(key);
                    log::debug!("Contract successfully cached");
                } else {
                    log::error!(
                            "Contract handler returned wrong event when trying to cache contract, this should not happen!"
                        );
                    return Err(OpError::UnexpectedOpState);
                }
            } else if !within_caching_dist {
                // not a contract this node cares about; do nothing
                return Ok(OperationResult {
                    return_msg: None,
                    state: None,
                });
            }
            // after the contract has been cached, push the update query
            let new_value = put_contract(op_storage, key, new_value).await?;

            //update skip list
            let peer_loc = op_storage.ring.own_location();
            skip_list.push(peer_loc.peer);

            // if sucessful, forward to the next closest peers (if any)
            if let Some(new_htl) = htl.checked_sub(1) {
                forward_changes(
                    op_storage,
                    conn_manager,
                    &contract,
                    new_value,
                    id,
                    new_htl,
                    skip_list.as_slice(),
                )
                .await;
            }
            return_msg = None;
            new_state = None;
        }
        _ => return Err(OpError::UnexpectedOpState),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::Put),
    })
}

async fn put_contract<CErr>(
    op_storage: &OpManager<CErr>,
    key: ContractKey,
    value: WrappedState,
) -> Result<WrappedState, OpError<CErr>>
where
    CErr: std::error::Error,
{
    // after the contract has been cached, push the update query
    match op_storage
        .notify_contract_handler(ContractHandlerEvent::PushQuery { key, value })
        .await
    {
        Ok(ContractHandlerEvent::PushResponse {
            new_value: Ok(new_val),
        }) => Ok(new_val),
        Ok(ContractHandlerEvent::PushResponse {
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
async fn forward_changes<CErr, CB>(
    op_storage: &OpManager<CErr>,
    conn_manager: &CB,
    contract: &WrappedContract,
    new_value: WrappedState,
    id: Transaction,
    htl: usize,
    skip_list: &[PeerKey],
) where
    CErr: std::error::Error,
    CB: ConnectionBridge,
{
    let key = contract.key();
    let contract_loc = Location::from(&key);
    let forward_to = op_storage.ring.closest_caching(&key, 1, skip_list);
    let own_loc = op_storage.ring.own_location().location.expect("infallible");
    for peer in forward_to {
        let other_loc = peer.location.as_ref().expect("infallible");
        let other_distance = contract_loc.distance(other_loc);
        let self_distance = contract_loc.distance(&own_loc);
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

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
            contract: WrappedContract,
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
            contract: WrappedContract,
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
            contract: WrappedContract,
            /// max hops to live
            htl: usize,
            // FIXME: remove skip list once we deduplicate at top msg handling level
            // using this is a tmp workaround until (https://github.com/freenet/locutus/issues/13) is done
            skip_list: Vec<PeerKey>,
        },
        /// Internal node instruction that  a change (either a first time insert or an update).
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_value: WrappedState,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            sender_subscribers: Vec<PeerKeyLocation>,
        },
    }

    impl PutMsg {
        pub fn id(&self) -> &Transaction {
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

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { sender, .. } => Some(sender),
                Self::BroadcastTo { sender, .. } => Some(sender),
                _ => None,
            }
        }

        pub fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::RequestPut { target, .. } => Some(target),
                _ => None,
            }
        }

        pub fn terminal(&self) -> bool {
            use PutMsg::*;
            matches!(
                self,
                SuccessfulUpdate { .. } | SeekNode { .. } | PutForward { .. }
            )
        }
    }

    impl Display for PutMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {})", id),
                Self::RouteValue { .. } => write!(f, "RouteValue(id: {})", id),
                Self::RequestPut { .. } => write!(f, "RequestPut(id: {})", id),
                Self::Broadcasting { .. } => write!(f, "Broadcasting(id: {})", id),
                Self::SuccessfulUpdate { .. } => write!(f, "SusscessfulUpdate(id: {})", id),
                Self::PutForward { .. } => write!(f, "PutForward(id: {})", id),
                Self::AwaitPut { .. } => write!(f, "AwaitPut(id: {})", id),
                Self::BroadcastTo { .. } => write!(f, "BroadcastTo(id: {})", id),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        client_events::ClientRequest,
        contract::SimStoreError,
        node::test::{check_connectivity, NodeSpecification, SimNetwork},
    };

    use super::*;

    type Err = OpError<SimStoreError>;

    #[test]
    fn successful_put_op_seq() -> Result<(), anyhow::Error> {
        let peer = PeerKey::random();
        let id = Transaction::new(<PutMsg as TxType>::tx_type_id(), &peer);
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let target_loc = PeerKeyLocation {
            location: Some(Location::random()),
            peer: PeerKey::random(),
        };

        let mut requester = PutOp::start_op(
            contract.clone(),
            WrappedState::new(vec![0, 1, 2, 3]),
            0,
            &peer,
        )
        .sm;
        let mut target = StateMachine::<PutOpSm>::from_state(PutState::ReceivedRequest, id);

        let req_msg = requester
            .consume_to_output::<Err>(PutMsg::RouteValue {
                id,
                htl: 0,
                target: target_loc,
            })?
            .ok_or_else(|| anyhow::anyhow!("no msg"))?;
        let expected = PutMsg::RequestPut {
            id,
            contract: contract.clone(),
            value: WrappedState::new(vec![0, 1, 2, 3]),
            htl: 0,
            target: target_loc,
        };
        assert_eq!(req_msg, expected);
        assert_eq!(
            requester.state(),
            &PutState::AwaitingResponse {
                contract: contract.key()
            }
        );

        let res_msg = target
            .consume_to_output::<Err>(PutMsg::Broadcasting {
                id,
                broadcast_to: vec![],
                broadcasted_to: 0,
                key: contract.key(),
                new_value: WrappedState::new(vec![4, 3, 2, 1]),
            })?
            .ok_or_else(|| anyhow::anyhow!("no output"))?;
        let expected = PutMsg::SuccessfulUpdate {
            id,
            new_value: WrappedState::new(vec![4, 3, 2, 1]),
        };
        assert_eq!(target.state(), &PutState::BroadcastComplete);
        assert_eq!(res_msg, expected);

        let finished = requester.consume_to_state::<SimStoreError>(res_msg)?;
        assert_eq!(target.state(), &PutState::BroadcastComplete);
        assert!(finished.is_none());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn successful_put_op_between_nodes() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 2usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let key = contract.key();
        let contract_val: WrappedState = gen.arbitrary()?;
        let new_value = WrappedState::new(Vec::from_iter(gen.arbitrary::<[u8; 20]>().unwrap()));

        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 2);
        let mut locations = sim_nodes.get_locations_by_node();
        let node0_loc = locations.remove("node-0").unwrap();
        let node1_loc = locations.remove("node-1").unwrap();

        // both own the contract, and one triggers an update
        let node_0 = NodeSpecification {
            owned_contracts: vec![(contract.clone(), contract_val.clone())],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::from_iter([(contract.key(), vec![node1_loc])]),
        };

        let node_1 = NodeSpecification {
            owned_contracts: vec![(contract.clone(), contract_val.clone())],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::from_iter([(contract.key(), vec![node0_loc])]),
        };

        let put_event = ClientRequest::Put {
            contract: contract.clone(),
            state: new_value.clone(),
        };

        let gw_0 = NodeSpecification {
            owned_contracts: vec![(contract, contract_val)],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::from_iter([(1, put_event)]),
            contract_subscribers: HashMap::new(),
        };

        // establish network
        let put_specs = HashMap::from_iter([
            ("node-0".to_string(), node_0),
            ("node-1".to_string(), node_1),
            ("gateway-0".to_string(), gw_0),
        ]);

        sim_nodes.build_with_specs(put_specs).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await?;

        // trigger the put op @ gw-0, this
        sim_nodes
            .trigger_event("gateway-0", 1, Some(Duration::from_secs(3)))
            .await?;
        assert!(sim_nodes.has_put_contract("gateway-0", &key, &new_value));
        assert!(sim_nodes.event_listener.contract_broadcasted(&key));
        Ok(())
    }
}
