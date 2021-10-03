use rust_fsm::{StateMachine, StateMachineImpl};

use crate::{
    conn_manager::{ConnectionBridge, PeerKeyLocation},
    contract::{Contract, ContractKey},
    message::{GetTxType, Message, Transaction},
    node::{OpExecError, OpStateStorage},
    ring::{Location, Ring, RingError},
};

pub(crate) use self::messages::PutMsg;

use super::{OpError, Operation, OperationResult};

type ContractPutValue = Vec<u8>;

pub(crate) struct PutOp(StateMachine<PutOpSM>);

impl PutOp {
    pub fn start_op(contract: &Contract, value: Vec<u8>) -> Self {
        log::debug!(
            "Requesting put to contract {} @ loc({})",
            hex::encode(contract.key().bytes()),
            Location::from(contract.key())
        );
        let id = Transaction::new(<PutMsg as GetTxType>::tx_type_id());
        let state = StateMachine::from_state(PutState::Requesting {
            id,
            key: contract.key(),
            value,
        });
        PutOp(state)
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
            (PutState::Requesting { key, value, id }, PutMsg::RouteValue { .. }) => {
                Some(PutState::AwaitAnswer {
                    id: *id,
                    key: *key,
                    value: value.clone(),
                })
            }
            (PutState::Initializing, PutMsg::SeekNode { .. }) => {
                todo!()
            }
            _ => None,
        }
    }

    fn output(state: &Self::State, input: &Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (PutState::Requesting { key, value, id }, PutMsg::RouteValue { .. }) => {
                Some(PutMsg::RequestPut {
                    id: *id,
                    key: *key,
                    value: value.clone(),
                })
            }
            _ => None,
        }
    }
}

enum PutState {
    Initializing,
    Requesting {
        id: Transaction,
        key: ContractKey,
        value: ContractPutValue,
    },
    AwaitAnswer {
        id: Transaction,
        key: ContractKey,
        value: ContractPutValue,
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
pub(crate) async fn request_put(
    op_storage: &OpStateStorage,
    mut put_op: PutOp,
) -> Result<(), OpError> {
    if !put_op.0.state().is_requesting() {
        return Err(OpError::IllegalStateTransition);
    };

    if let Some(req_put) = put_op.0.consume(&PutMsg::RouteValue {
        id: *put_op.0.state().id(),
    })? {
        op_storage.notify_change(Message::from(req_put)).await?;
    } else {
        return Err(OpError::IllegalStateTransition);
    }
    Ok(())
}

// TODO: deduplicate the handle functions between different ops, are pretty much the same
pub(crate) async fn handle_put_response<CB>(
    op_storage: &OpStateStorage,
    conn_manager: &mut CB,
    put_op: PutMsg,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    let sender;
    let tx = *put_op.id();
    let result = match op_storage.pop(put_op.id()) {
        Some(Operation::Put(state)) => {
            sender = put_op.sender().cloned();
            // was an existing operation, the other peer messaged back
            update_state(conn_manager, state, put_op, &op_storage.ring).await
        }
        Some(_) => return Err(OpExecError::TxUpdateFailure(tx).into()),
        None => {
            sender = put_op.sender().cloned();
            // new request to join from this node, initialize the machine
            let machine = PutOp(StateMachine::new());
            update_state(conn_manager, machine, put_op, &op_storage.ring).await
        }
    };

    match result {
        Err(err) => {
            log::error!("error while processing put request: {}", err);
            if let Some(sender) = sender {
                conn_manager.send(&sender, Message::Canceled(tx)).await?;
            }
            return Err(err);
        }
        _ => todo!(),
    }

    Ok(())
}

async fn update_state<CB>(
    conn_manager: &mut CB,
    mut state: PutOp,
    other_host_msg: PutMsg,
    ring: &Ring,
) -> Result<OperationResult<PutOp>, OpError>
where
    CB: ConnectionBridge,
{
    let return_msg;
    let new_state;
    match other_host_msg {
        PutMsg::RequestPut { id, key, value } => {
            // find the closest node to the location of the contract
            let target = if let Some((_, potential_target)) = ring.routing(&key.location()) {
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
                    sender: ring
                        .own_location()
                        .map(|location| PeerKeyLocation {
                            location: Some(location),
                            peer: conn_manager.peer_key(),
                        })
                        .ok_or_else(|| OpError::from(RingError::NoLocationAssigned))?,
                    key,
                    value,
                })
                .into(),
            );
            // no changes to state yet, still in AwaitResponse state
            new_state = Some(state);
        }
        _ => return Err(OpError::IllegalStateTransition),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state,
    })
}

mod messages {
    use crate::conn_manager::PeerKeyLocation;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum PutMsg {
        /// Initialize the put operation by routing the value
        RouteValue { id: Transaction },
        /// Internal node instruction to find a route to the target node.
        RequestPut {
            id: Transaction,
            key: ContractKey,
            value: ContractPutValue,
        },
        /// Target the node which is closest to the key
        SeekNode {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            key: ContractKey,
            value: ContractPutValue,
        },
    }

    impl PutMsg {
        pub fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                PutMsg::RouteValue { id } => id,
                PutMsg::RequestPut { id, .. } => id,
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
