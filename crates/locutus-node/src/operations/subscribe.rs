use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    config::PEER_TIMEOUT,
    conn_manager::{ConnectionBridge, PeerKeyLocation},
    contract::{ContractError, ContractKey},
    message::{GetTxType, Transaction},
    node::OpManager,
};

use super::{
    handle_op_result,
    state_machine::{StateMachine, StateMachineImpl},
    OpError, Operation, OperationResult,
};

pub(crate) use self::messages::SubscribeMsg;

pub(crate) struct SubscribeOp {
    sm: StateMachine<SubscribeOpSM>,
    _ttl: Duration,
}

impl SubscribeOp {
    pub fn start_op(key: ContractKey) -> Self {
        let id = Transaction::new(<SubscribeMsg as GetTxType>::tx_type_id());
        let sm = StateMachine::from_state(SubscribeState::PrepareRequest { id });
        SubscribeOp {
            sm,
            _ttl: PEER_TIMEOUT,
        }
    }
}

struct SubscribeOpSM;

impl StateMachineImpl for SubscribeOpSM {
    type Input = SubscribeMsg;

    type State = SubscribeState;

    type Output = SubscribeMsg;
}

enum SubscribeState {
    /// Prepare the request to subscribe.
    PrepareRequest { id: Transaction },
}
/// Request to subscribe to value changes from a contract.
pub(crate) async fn request_subscribe<CErr>(
    op_storage: &OpManager<CErr>,
    mut get_op: SubscribeOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    todo!()
}

pub(crate) async fn handle_subscribe_response<CB, CErr>(
    op_storage: &OpManager<CErr>,
    conn_manager: &mut CB,
    subscribe_op: SubscribeMsg,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    OpError<CErr>: From<ContractError<CErr>>,
    CErr: std::error::Error,
{
    let sender;
    let tx = *subscribe_op.id();
    let result = match op_storage.pop(subscribe_op.id()) {
        Some(Operation::Subscribe(state)) => {
            sender = subscribe_op.sender().cloned();
            // was an existing operation, the other peer messaged back
            update_state(conn_manager, state, subscribe_op, op_storage).await
        }
        Some(_) => return Err(OpError::TxUpdateFailure(tx)),
        None => {
            sender = subscribe_op.sender().cloned();
            // new request to subcribe to a contract, initialize the machine
            let machine = SubscribeOp {
                sm: StateMachine::from_state(SubscribeState::PrepareRequest { id: tx }),
                _ttl: PEER_TIMEOUT,
            };
            update_state(conn_manager, machine, subscribe_op, op_storage).await
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
    mut state: SubscribeOp,
    other_host_msg: SubscribeMsg,
    op_storage: &OpManager<CErr>,
) -> Result<OperationResult, OpError<CErr>>
where
    CB: ConnectionBridge,
    OpError<CErr>: From<ContractError<CErr>>,
    CErr: std::error::Error,
{
    todo!()
}

mod messages {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub(crate) enum SubscribeMsg {
        SeekNode { id: Transaction },
    }

    impl SubscribeMsg {
        pub(crate) fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id } => id,
            }
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                _ => None,
            }
        }
    }
}
