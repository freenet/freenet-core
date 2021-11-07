//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.
// FIXME: should allow to do partial value updates

use std::time::Duration;

use crate::{
    config::PEER_TIMEOUT,
    conn_manager::ConnectionBridge,
    contract::{Contract, ContractError, ContractHandlerEvent, ContractKey, ContractValue},
    message::{Message, Transaction, TxType},
    node::OpManager,
    ring::{Location, PeerKeyLocation, RingError},
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
    pub fn start_op(contract: Contract, value: ContractValue, htl: usize) -> Self {
        log::debug!(
            "Requesting put to contract {} @ loc({})",
            contract.key(),
            Location::from(contract.key())
        );

        let id = Transaction::new(<PutMsg as TxType>::tx_type_id());
        let sm = StateMachine::from_state(PutState::PrepareRequest {
            id,
            contract,
            value,
            htl,
        });
        PutOp {
            sm,
            _ttl: PEER_TIMEOUT,
        }
    }
}

struct PutOpSm;

impl StateMachineImpl for PutOpSm {
    type Input = PutMsg;

    type State = PutState;

    type Output = PutMsg;

    fn state_transition_from_input(state: Self::State, input: Self::Input) -> Option<Self::State> {
        match (state, input) {
            // state changed for the initial requesting node
            (PutState::AwaitingResponse { contract }, PutMsg::RequestPut { .. }) => {
                Some(PutState::AwaitingResponse { contract })
            }
            (PutState::AwaitingResponse { contract, .. }, PutMsg::SuccessfulUpdate { .. }) => {
                log::debug!("Successfully updated value for {}", contract);
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
                    id,
                    htl,
                },
                PutMsg::RouteValue { target, .. },
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
                },
            ) => Some(PutMsg::SeekNode {
                id,
                target,
                sender,
                contract,
                value,
                htl,
            }),
            // output from initial target
            (
                PutState::ReceivedRequest | PutState::BroadcastOngoing { .. },
                PutMsg::Broadcasting {
                    id,
                    new_value,
                    broadcast_to,
                    ..
                },
            ) => {
                if broadcast_to.is_empty() {
                    Some(PutMsg::SuccessfulUpdate { id, new_value })
                } else {
                    None
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
        contract: Contract,
        value: ContractValue,
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
        return Err(OpError::InvalidStateTransition);
    };

    // the initial request must provide:
    // - a peer as close as possible to the contract location
    // - and the value to put
    let target = op_storage
        .ring
        .closest_caching(&key, 1, &[])
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
            .notify_change(Message::from(req_put), Operation::Put(put_op))
            .await?;
    } else {
        return Err(OpError::InvalidStateTransition);
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
        Some(_) => return Err(OpError::TxUpdateFailure(tx)),
        None => {
            sender = put_op.sender().cloned();
            // new request to put a new value for a contract, initialize the machine
            let machine = PutOp {
                sm: StateMachine::from_state(PutState::ReceivedRequest),
                _ttl: PEER_TIMEOUT,
            };
            update_state(conn_manager, machine, put_op, op_storage).await
        }
    };

    handle_op_result(
        op_storage,
        conn_manager,
        result.map_err(|err| (err, tx)),
        sender,
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
            return_msg = state
                .sm
                .consume_to_state(PutMsg::RequestPut {
                    id,
                    contract,
                    value,
                    htl,
                    target,
                })?
                .map(Message::from);
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
        } => {
            let key = contract.key();
            let cached_contract = op_storage.ring.contract_exists(&key);
            if !cached_contract && op_storage.ring.within_caching_distance(&key.location()) {
                // this node does not have the contract, so instead store the contract and execute the put op.
                op_storage
                    .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()))
                    .await?;
            } else if !cached_contract {
                // in this case forward to a closer node to the target location and just wait for a response
                // to give back to requesting peer
                todo!()
            }

            // after the contract has been cached, push the update query
            let new_value = put_contract(op_storage, key, value).await?;
            // if the change was successful, communicate this back to the requestor and broadcast the change
            conn_manager
                .send(
                    sender,
                    (PutMsg::SuccessfulUpdate {
                        id,
                        new_value: new_value.clone(),
                    })
                    .into(),
                )
                .await?;

            if let Some(new_htl) = htl.checked_sub(1) {
                // forward changes in the contract to nodes closer to the contract location, if possible
                forward_changes(
                    op_storage,
                    conn_manager,
                    &contract,
                    new_value.clone(),
                    id,
                    new_htl,
                )
                .await;
            }

            let broadcast_to = op_storage
                .ring
                .subscribers_of(&key)
                .ok_or(ContractError::ContractNotFound(key))?
                .iter()
                .cloned()
                .collect();
            log::debug!(
                "Successfully updated a value for contract {} @ {:?}",
                key,
                target.location
            );

            if let Some(msg) = state.sm.consume_to_state(PutMsg::Broadcasting {
                id,
                broadcast_to,
                broadcasted_to: 0,
                key,
                new_value,
            })? {
                op_storage
                    .notify_change(msg.into(), Operation::Put(state))
                    .await?;
                return Err(OpError::StatePushed);
            }
            return_msg = None;
            new_state = Some(state);
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
            };

            let mut broadcasting = Vec::with_capacity(broadcast_to.len());
            for peer in &broadcast_to {
                let f = conn_manager.send(*peer, msg.clone().into());
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
                conn_manager.drop_connection(peer.peer);
                incorrect_results += 1;
            }

            broadcasted_to += broadcast_to.len() - incorrect_results;
            log::debug!(
                "successfully broadcasted put into contract {} to {} peers",
                key,
                broadcasted_to
            );

            return_msg = state
                .sm
                .consume_to_state(PutMsg::Broadcasting {
                    id,
                    broadcasted_to,
                    broadcast_to,
                    key,
                    new_value,
                })?
                .map(Message::from);
            new_state = None;
            if &PutState::BroadcastComplete != state.sm.state() {
                return Err(OpError::InvalidStateTransition);
            }
        }
        PutMsg::SuccessfulUpdate { id, new_value } => {
            return_msg = state
                .sm
                .consume_to_state(PutMsg::SuccessfulUpdate { id, new_value })?
                .map(Message::from);
            new_state = None;
        }
        PutMsg::PutForward {
            id,
            contract,
            new_value,
            htl,
        } => {
            let key = contract.key();
            let cached_contract = op_storage.ring.contract_exists(&key);
            let within_caching_dist = op_storage.ring.within_caching_distance(&key.location());
            if !cached_contract && within_caching_dist {
                // this node does not have the contract, so instead store the contract and execute the put op.
                op_storage
                    .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()))
                    .await?;
            } else if !within_caching_dist {
                // not a contract this node cares about; do nothing
                return Ok(OperationResult {
                    return_msg: None,
                    state: None,
                });
            }
            // after the contract has been cached, push the update query
            let new_value = put_contract(op_storage, key, new_value).await?;
            // if sucessful, forward to the next closest peers (if any)
            if let Some(new_htl) = htl.checked_sub(1) {
                forward_changes(op_storage, conn_manager, &contract, new_value, id, new_htl).await;
            }
            return_msg = None;
            new_state = None;
        }
        _ => return Err(OpError::InvalidStateTransition),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::Put),
    })
}

async fn put_contract<CErr>(
    op_storage: &OpManager<CErr>,
    key: ContractKey,
    value: ContractValue,
) -> Result<ContractValue, OpError<CErr>>
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
        Ok(_) => Err(OpError::InvalidStateTransition),
    }
}

// TODO: keep track of who is supposed to have the contract, and only send if necessary
// since sending the contract over and over, will be expensive; this can be done via subscriptions
/// Communicate changes in the contract to other peers nearby the contract location.
/// This operation is "fire and forget" and the node does not keep track if is successful or not.
async fn forward_changes<CErr, CB>(
    op_storage: &OpManager<CErr>,
    conn_manager: &CB,
    contract: &Contract,
    new_value: ContractValue,
    id: Transaction,
    htl: usize,
) where
    CErr: std::error::Error,
    CB: ConnectionBridge,
{
    let key = contract.key();
    let contract_loc = key.location();
    let forward_to = op_storage.ring.closest_caching(&key, 1, &[]);
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
                    peer,
                    (PutMsg::PutForward {
                        id,
                        contract: contract.clone(),
                        new_value: new_value.clone(),
                        htl,
                    })
                    .into(),
                )
                .await;
        }
    }
}

mod messages {
    use std::fmt::Display;

    use crate::contract::ContractValue;

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
            contract: Contract,
            value: ContractValue,
            /// max hops to live
            htl: usize,
            target: PeerKeyLocation,
        },
        /// Internal node instruction to await the result of a put.
        AwaitPut { id: Transaction },
        /// Forward a contract and it's latest value to an other node
        PutForward {
            id: Transaction,
            contract: Contract,
            new_value: ContractValue,
            /// current htl, reduced by one at each hop
            htl: usize,
        },
        /// Value successfully inserted/updated.
        SuccessfulUpdate {
            id: Transaction,
            new_value: ContractValue,
        },
        /// Target the node which is closest to the key
        SeekNode {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            value: ContractValue,
            contract: Contract,
            /// max hops to live
            htl: usize,
        },
        /// Internal node instruction that  a change (either a first time insert or an update).
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_value: ContractValue,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: ContractValue,
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
    use crate::{conn_manager::PeerKey, node::SimStorageError};

    use super::*;

    type Err = OpError<SimStorageError>;

    #[test]
    fn successful_put_op_seq() -> Result<(), anyhow::Error> {
        let id = Transaction::new(<PutMsg as TxType>::tx_type_id());
        let bytes = crate::test_utils::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: Contract = gen.arbitrary()?;
        let target_loc = PeerKeyLocation {
            location: Some(Location::random()),
            peer: PeerKey::random(),
        };

        let mut requester =
            PutOp::start_op(contract.clone(), ContractValue::new(vec![0, 1, 2, 3]), 0).sm;
        let mut target = StateMachine::<PutOpSm>::from_state(PutState::ReceivedRequest);

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
            value: ContractValue::new(vec![0, 1, 2, 3]),
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
                new_value: ContractValue::new(vec![4, 3, 2, 1]),
            })?
            .ok_or_else(|| anyhow::anyhow!("no output"))?;
        let expected = PutMsg::SuccessfulUpdate {
            id,
            new_value: ContractValue::new(vec![4, 3, 2, 1]),
        };
        assert_eq!(target.state(), &PutState::BroadcastComplete);
        assert_eq!(res_msg, expected);

        let finished = requester.consume_to_state::<SimStorageError>(res_msg)?;
        assert_eq!(target.state(), &PutState::BroadcastComplete);
        assert!(finished.is_none());
        Ok(())
    }
}
