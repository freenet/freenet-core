use crate::{
    conn_manager::ConnectionBridge, contract::ContractKey, message::Transaction, node::OpManager,
};

pub(crate) use self::messages::GetMsg;

use super::{
    state_machine::{StateMachine, StateMachineImpl},
    OpError,
};

/// This is just a placeholder for now!
pub(crate) struct GetOp(StateMachine<GetOpSM>);

impl GetOp {
    pub fn start_op(key: ContractKey) -> Self {
        let state = StateMachine::from_state(GetState::Requesting { key });
        GetOp(state)
    }
}

struct GetOpSM;

impl StateMachineImpl for GetOpSM {
    type Input = GetMsg;

    type State = GetState;

    type Output = GetMsg;

    fn state_transition_from_input(state: Self::State, input: Self::Input) -> Option<Self::State> {
        match (state, input) {
            (GetState::Initializing, GetMsg::FetchRouting { key }) => {
                Some(GetState::Requesting { key })
            }
            _ => None,
        }
    }

    fn output_from_input_as_ref(state: &Self::State, input: &Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (GetState::Initializing, GetMsg::FetchRouting { key }) => {
                Some(GetMsg::FetchRouting { key: *key })
            }
            _ => None,
        }
    }
}

enum GetState {
    Initializing,
    Requesting { key: ContractKey },
}

pub(crate) async fn handle_get_response<CB, CErr>(
    _op_storage: &OpManager<CErr>,
    _conn_manager: &mut CB,
    _get_op: GetMsg,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
{
    todo!()
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get<CErr>(
    _op_storage: &OpManager<CErr>,
    _get_op: GetOp,
) -> Result<(), OpError<CErr>> {
    // the initial request must provide:
    // - a location in the network where the contract resides
    // - and the value to get
    todo!()
}

mod messages {
    use crate::contract::ContractKey;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum GetMsg {
        FetchRouting { key: ContractKey },
    }

    impl GetMsg {
        pub fn id(&self) -> &Transaction {
            todo!()
        }
    }
}
