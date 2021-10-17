use std::time::Duration;

use crate::{
    config::PEER_TIMEOUT,
    conn_manager::ConnectionBridge,
    contract::{ContractError, ContractHandlerEvent, ContractKey},
    message::{GetTxType, Message, Transaction},
    node::OpManager,
    ring::RingError,
};

pub(crate) use self::messages::GetMsg;

use super::{
    handle_op_result,
    state_machine::{StateMachine, StateMachineImpl},
    OpError, Operation, OperationResult,
};

/// This is just a placeholder for now!
pub(crate) struct GetOp {
    sm: StateMachine<GetOpSM>,
    _ttl: Duration,
}

impl GetOp {
    pub fn start_op(key: ContractKey) -> Self {
        let id = Transaction::new(<GetMsg as GetTxType>::tx_type_id());
        let sm = StateMachine::from_state(GetState::Request { key, id });
        GetOp {
            sm,
            _ttl: PEER_TIMEOUT,
        }
    }
}

struct GetOpSM;

impl StateMachineImpl for GetOpSM {
    type Input = GetMsg;

    type State = GetState;

    type Output = GetMsg;

    fn state_transition(state: &Self::State, input: &Self::Input) -> Option<Self::State> {
        match (state, input) {
            (GetState::Request { .. }, GetMsg::FetchRouting { .. }) => {
                Some(GetState::AwaitingResponse)
            }
            (GetState::Initializing, GetMsg::ReturnGet { .. }) => Some(GetState::Completed),
            _ => None,
        }
    }

    fn output_from_input(state: Self::State, input: Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (GetState::Request { key, id }, GetMsg::FetchRouting { target, .. }) => {
                Some(GetMsg::RequestGet { key, target, id })
            }
            (
                GetState::Initializing,
                GetMsg::ReturnGet {
                    id,
                    key,
                    value,
                    target,
                },
            ) => Some(GetMsg::ReturnGet {
                id,
                key,
                value,
                target,
            }),
            _ => None,
        }
    }
}

enum GetState {
    Initializing,
    Request { key: ContractKey, id: Transaction },
    AwaitingResponse,
    Completed,
}

pub(crate) async fn handle_get_response<CB, CErr>(
    op_storage: &OpManager<CErr>,
    conn_manager: &mut CB,
    get_op: GetMsg,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    OpError<CErr>: From<ContractError<CErr>>,
    CErr: std::error::Error,
{
    let sender;
    let tx = *get_op.id();
    let result = match op_storage.pop(get_op.id()) {
        Some(Operation::Get(state)) => {
            sender = get_op.sender().cloned();
            // was an existing operation, the other peer messaged back
            update_state(conn_manager, state, get_op, op_storage).await
        }
        Some(_) => return Err(OpError::TxUpdateFailure(tx)),
        None => {
            sender = get_op.sender().cloned();
            // new request to join from this node, initialize the machine
            let machine = GetOp {
                sm: StateMachine::from_state(GetState::Initializing),
                _ttl: PEER_TIMEOUT,
            };
            update_state(conn_manager, machine, get_op, op_storage).await
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
    mut state: GetOp,
    other_host_msg: GetMsg,
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
        GetMsg::RequestGet { key, id, target } => {
            new_state = Some(state);
            return_msg = Some(Message::from(GetMsg::SeekNode { key, id, target }));
        }
        GetMsg::SeekNode { key, id, target } => {
            if !op_storage.ring.has_contract(&key) {
                // this node does not have the contract, return an error to the requester
                log::info!("Contract {} not found while processing info", key);
                new_state = None;
                todo!()
            }
            // FIXME: fetch_contract should be communicated by the requester
            if let ContractHandlerEvent::FetchResponse {
                response: value,
                key: returned_key,
            } = op_storage
                .notify_contract_handler(ContractHandlerEvent::FetchQuery {
                    key,
                    fetch_contract: false,
                })
                .await?
            {
                if returned_key != key {
                    return Err(OpError::IllegalStateTransition);
                }
                return_msg = state
                    .sm
                    .consume_to_output(GetMsg::ReturnGet {
                        key,
                        id,
                        value: value.map_err(ContractError::from)?,
                        target,
                    })?
                    .map(Message::from);
                new_state = Some(state);
            } else {
                return Err(OpError::IllegalStateTransition);
            }
        }
        _ => return Err(OpError::IllegalStateTransition),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::Get),
    })
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get<CErr>(
    op_storage: &OpManager<CErr>,
    mut get_op: GetOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let (target, id) = if let GetState::Request { key, id } = get_op.sm.state() {
        // the initial request must provide:
        // - a location in the network where the contract resides
        // - and the key of the contract value to get
        (
            op_storage
                .ring
                .closest_caching(key, 1)
                .into_iter()
                .next()
                .ok_or_else(|| OpError::from(RingError::EmptyRing))?,
            *id,
        )
    } else {
        return Err(OpError::IllegalStateTransition);
    };
    if let Some(req_get) = get_op
        .sm
        .consume_to_output(GetMsg::FetchRouting { target, id })?
    {
        op_storage
            .notify_change(Message::from(req_get), Operation::Get(get_op))
            .await?;
    }
    Ok(())
}

mod messages {
    use crate::{conn_manager::PeerKeyLocation, contract::StoreResponse};

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum GetMsg {
        /// Internal node call to route to a peer close to the contract.
        FetchRouting {
            id: Transaction,
            target: PeerKeyLocation,
        },
        RequestGet {
            id: Transaction,
            target: PeerKeyLocation,
            key: ContractKey,
        },
        SeekNode {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
        },
        ReturnGet {
            id: Transaction,
            key: ContractKey,
            value: StoreResponse,
            target: PeerKeyLocation,
        },
    }

    impl GetMsg {
        pub fn id(&self) -> &Transaction {
            match self {
                Self::FetchRouting { id, .. } => id,
                Self::RequestGet { id, .. } => id,
                Self::SeekNode { id, .. } => id,
                Self::ReturnGet { id, .. } => id,
            }
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::ReturnGet { target, .. } => Some(target),
                _ => None,
            }
        }
    }
}
