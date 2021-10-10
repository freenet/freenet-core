//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

use std::time::Duration;

use rust_fsm::{StateMachine, StateMachineImpl};

use crate::{
    config::PEER_TIMEOUT_SECS,
    conn_manager::{ConnectionBridge, PeerKeyLocation},
    contract::{Contract, ContractError, ContractHandlerEvent},
    message::{GetTxType, Message, Transaction},
    node::{OpExecError, OpManager},
    ring::{Location, RingError},
};

pub(crate) use self::messages::PutMsg;

use super::{handle_op_result, OpError, Operation, OperationResult};

pub(crate) type ContractPutValue = Vec<u8>;

pub(crate) struct PutOp {
    sm: StateMachine<PutOpSM>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl PutOp {
    pub fn start_op(contract: Contract, value: Vec<u8>) -> Self {
        log::debug!(
            "Requesting put to contract {} @ loc({})",
            hex::encode(contract.key().bytes()),
            Location::from(contract.key())
        );
        let id = Transaction::new(<PutMsg as GetTxType>::tx_type_id());
        let sm = StateMachine::from_state(PutState::Requesting {
            id,
            contract,
            value,
        });
        PutOp {
            sm,
            _ttl: Duration::from_secs(PEER_TIMEOUT_SECS),
        }
    }
}

struct PutOpSM;

impl StateMachineImpl for PutOpSM {
    type Input = PutMsg;

    type State = PutState;

    type Output = PutMsg;

    const INITIAL_STATE: Self::State = PutState::Initializing;

    fn transition(state: &Self::State, input: &Self::Input) -> Option<Self::State> {
        match (state, input) {
            (
                PutState::Requesting {
                    contract,
                    value,
                    id,
                },
                PutMsg::RouteValue { .. },
            ) => Some(PutState::AwaitAnswer {
                id: *id,
                contract: contract.clone(),
                value: value.clone(),
            }),
            (
                PutState::Initializing,
                PutMsg::SeekNode {
                    id,
                    sender,
                    contract,
                    ..
                },
            ) => {
                log::debug!(
                    "Received petition({}) to put value for contract {} from {} ",
                    id,
                    hex::encode(contract.key().bytes()),
                    sender.peer,
                );
                Some(PutState::AwaitingBroadcast { id: *id })
            }
            (
                PutState::Initializing,
                PutMsg::AwaitingBroadcast {
                    id,
                    broadcast_to,
                    broadcasted_to,
                },
            ) => {
                if *broadcasted_to >= broadcast_to.len() {
                    todo!()
                } else {
                    Some(PutState::Initializing)
                }
            }
            _ => None,
        }
    }

    fn output(state: &Self::State, input: &Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (
                PutState::Requesting {
                    contract,
                    value,
                    id,
                },
                PutMsg::RouteValue { .. },
            ) => Some(PutMsg::RequestPut {
                id: *id,
                contract: contract.clone(),
                value: value.clone(),
            }),
            _ => None,
        }
    }
}

enum PutState {
    Initializing,
    Requesting {
        id: Transaction,
        contract: Contract,
        value: ContractPutValue,
    },
    AwaitAnswer {
        id: Transaction,
        contract: Contract,
        value: ContractPutValue,
    },
    AwaitingBroadcast {
        id: Transaction,
    },
}

impl PutState {
    fn is_requesting(&self) -> bool {
        matches!(self, Self::Requesting { .. })
    }

    fn id(&self) -> &Transaction {
        todo!()
    }
}

/// Request to insert/update a value into a contract.
pub(crate) async fn request_put<CErr>(
    op_storage: &OpManager<CErr>,
    mut put_op: PutOp,
) -> Result<(), OpError<CErr>> {
    if !put_op.sm.state().is_requesting() {
        return Err(OpError::IllegalStateTransition);
    };

    if let Some(req_put) = put_op.sm.consume(&PutMsg::RouteValue {
        id: *put_op.sm.state().id(),
    })? {
        op_storage
            .notify_change(Message::from(req_put), Operation::Put(put_op))
            .await?;
    } else {
        return Err(OpError::IllegalStateTransition);
    }
    Ok(())
}

pub(crate) async fn handle_put_response<CB, CErr>(
    op_storage: &OpManager<CErr>,
    conn_manager: &mut CB,
    put_op: PutMsg,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    OpError<CErr>: From<ContractError<CErr>>,
{
    let sender;
    let tx = *put_op.id();
    let result = match op_storage.pop(put_op.id()) {
        Some(Operation::Put(state)) => {
            sender = put_op.sender().cloned();
            // was an existing operation, the other peer messaged back
            update_state(conn_manager, state, put_op, op_storage).await
        }
        Some(_) => return Err(OpExecError::TxUpdateFailure(tx).into()),
        None => {
            sender = put_op.sender().cloned();
            // new request to join from this node, initialize the machine
            let machine = PutOp {
                sm: StateMachine::new(),
                _ttl: Duration::from_millis(PEER_TIMEOUT_SECS),
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
{
    let return_msg;
    let new_state;
    match other_host_msg {
        PutMsg::RequestPut {
            id,
            contract,
            value,
        } => {
            // find the closest node to the location of the contract
            let target = if let Some(potential_target) = op_storage
                .ring
                .routing(&contract.key().location(), 1)
                .into_iter()
                .next()
            {
                potential_target
            } else {
                return Err(RingError::EmptyRing.into());
            };
            // the initial request must provide:
            // - a peer as close as possible to the contract location
            // - and the value to put

            return_msg = Some(
                (PutMsg::SeekNode {
                    id,
                    target,
                    sender: op_storage
                        .ring
                        .own_location()
                        .map(|location| PeerKeyLocation {
                            location: Some(location),
                            peer: conn_manager.peer_key(),
                        })
                        .ok_or_else(|| {
                            <OpError<CErr> as From<RingError>>::from(RingError::NoLocationAssigned)
                        })?,
                    contract,
                    value,
                })
                .into(),
            );
            // no changes to state yet, still in AwaitResponse state
            new_state = Some(state);
        }
        PutMsg::SeekNode {
            id,
            sender,
            target,
            value,
            contract,
        } => {
            let contract_loc = contract.assigned_location();
            let cached_contract = op_storage.ring.has_contract(&contract.key());
            if cached_contract {
                match op_storage
                    .notify_contract_handler(ContractHandlerEvent::PushQuery {
                        key: contract.key(),
                        value,
                    })
                    .await
                {
                    Ok(ContractHandlerEvent::PushResponse) => {
                        // 1.1. if ok then return success
                        // 1.2. propagate/broadcast changes
                    }
                    Err(ContractError::StorageError(_err)) => {
                        // 2. if failed communicate failure
                    }
                    _ => unreachable!(),
                }
            }

            if !cached_contract && op_storage.ring.within_caching_distance(&contract_loc) {
                // this node does not have the contract, so instead store the contract and execute the put op.
                op_storage
                    .notify_contract_handler(ContractHandlerEvent::Cache(contract))
                    .await?;

                // if the change was successful broadcast the change
                let broadcast_to = op_storage.ring.routing(&contract_loc, 10);
                state.sm.consume(&PutMsg::AwaitingBroadcast {
                    id,
                    broadcasted_to: 0,
                    broadcast_to,
                })?;

                op_storage
                    .notify_change(
                        Message::Put(PutMsg::Broadcast { id }),
                        Operation::Put(state),
                    )
                    .await?;
                return Err(OpError::StatePushed);
            }

            if let Some(PeerKeyLocation {
                location: Some(other_loc),
                peer,
            }) = op_storage.ring.routing(&contract_loc, 1).into_iter().next()
            {
                if let Some(own_loc) = op_storage.ring.own_location() {
                    let other_distance = contract_loc.distance(&other_loc);
                    let self_distance = contract_loc.distance(&own_loc);
                    if other_distance < self_distance {
                        // forward the contract towards this node since it is indeed closer
                        // to the contract location

                        todo!()
                    }
                }
            }
            todo!()
        }
        PutMsg::AwaitingBroadcast {
            id,
            broadcast_to,
            broadcasted_to,
        } => {
            todo!()
        }
        _ => return Err(OpError::IllegalStateTransition),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::Put),
    })
}

mod messages {
    use crate::conn_manager::PeerKeyLocation;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub(crate) enum PutMsg {
        /// Initialize the put operation by routing the value
        RouteValue { id: Transaction },
        /// Internal node instruction to find a route to the target node.
        RequestPut {
            id: Transaction,
            contract: Contract,
            value: ContractPutValue,
        },
        /// Target the node which is closest to the key
        SeekNode {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            value: ContractPutValue,
            contract: Contract,
        },
        ///
        AwaitingBroadcast {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
        },
        /// Broadcast a change (either a first time insert or an update).
        Broadcast { id: Transaction },
    }

    impl PutMsg {
        pub fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                Self::RouteValue { id } => id,
                Self::RequestPut { id, .. } => id,
                Self::Broadcast { id } => id,
                Self::AwaitingBroadcast { id, .. } => id,
            }
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { sender, .. } => Some(sender),
                _ => None,
            }
        }

        pub fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                _ => None,
            }
        }
    }
}
