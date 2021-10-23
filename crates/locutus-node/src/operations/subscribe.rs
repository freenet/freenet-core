use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    config::PEER_TIMEOUT,
    conn_manager::ConnectionBridge,
    contract::{ContractError, ContractKey},
    message::{GetTxType, Transaction},
    node::OpManager,
};

use super::{
    state_machine::{StateMachine, StateMachineImpl},
    OpError,
};

pub(crate) struct SubscribeOp {
    sm: StateMachine<SubscribeOpSM>,
    _ttl: Duration,
}

impl SubscribeOp {
    /// Maximum number of retries to get values.
    const MAX_RETRIES: usize = 10;

    pub fn start_op(key: ContractKey) -> Self {
        let id = Transaction::new(<SubscribeMsg as GetTxType>::tx_type_id());
        let sm = StateMachine::from_state(SubscribeState::Request {});
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
    Request,
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
    // let sender;
    // let tx = *subscribe_op.id();
    // let result = match op_storage.pop(subscribe_op.id()) {
    //     Some(Operation::Get(state)) => {
    // 	sender = subscribe_op.sender().cloned();
    // 	// was an existing operation, the other peer messaged back
    // 	update_state(conn_manager, state, subscribe_op, op_storage).await
    //     }
    //     Some(_) => return Err(OpError::TxUpdateFailure(tx)),
    //     None => {
    // 	sender = subscribe_op.sender().cloned();
    // 	// new request to join from this node, initialize the machine
    // 	let machine = GetOp {
    // 	    sm: StateMachine::from_state(GetState::Initializing),
    // 	    _ttl: PEER_TIMEOUT,
    // 	};
    // 	update_state(conn_manager, machine, subscribe_op, op_storage).await
    //     }
    // };

    // handle_op_result(
    //     op_storage,
    //     conn_manager,
    //     result.map_err(|err| (err, tx)),
    //     sender,
    // )
    // .await
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum SubscribeMsg {}

impl SubscribeMsg {
    pub(crate) fn id(&self) -> &Transaction {
        todo!()
    }
}
