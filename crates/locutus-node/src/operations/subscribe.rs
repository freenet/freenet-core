use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    config::PEER_TIMEOUT,
    conn_manager::{ConnectionBridge, PeerKey},
    contract::{ContractError, ContractKey},
    message::{GetTxType, Message, Transaction},
    node::OpManager,
    ring::{PeerKeyLocation, RingError},
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
        let sm = StateMachine::from_state(SubscribeState::PrepareRequest { id, key });
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

    fn state_transition(state: &Self::State, input: &Self::Input) -> Option<Self::State> {
        match (state, input) {
            (SubscribeState::PrepareRequest { .. }, SubscribeMsg::FetchRouting { .. }) => {
                Some(SubscribeState::AwaitingResponse {
                    skip_list: vec![],
                    retries: 0,
                })
            }
            _ => None,
        }
    }

    fn output_from_input(state: Self::State, input: Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (
                SubscribeState::PrepareRequest { id, key, .. },
                SubscribeMsg::FetchRouting { target, .. },
            ) => Some(SubscribeMsg::RequestSub { id, key, target }),
            _ => None,
        }
    }
}

enum SubscribeState {
    /// Prepare the request to subscribe.
    PrepareRequest { id: Transaction, key: ContractKey },
    /// Received a request to subscribe to this network.
    ReceivedRequest,
    /// Awaitinh response from petition.
    AwaitingResponse {
        skip_list: Vec<PeerKey>,
        retries: usize,
    },
}

/// Request to subscribe to value changes from a contract.
pub(crate) async fn request_subscribe<CErr>(
    op_storage: &OpManager<CErr>,
    mut sub_op: SubscribeOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let (target, id) = if let SubscribeState::PrepareRequest { id, key } = sub_op.sm.state() {
        if !op_storage.ring.has_contract(key) {
            return Err(OpError::ContractError(ContractError::ContractNotFound(
                *key,
            )));
        }
        (
            op_storage
                .ring
                .closest_caching(key, 1, &[])
                .into_iter()
                .next()
                .ok_or_else(|| OpError::from(RingError::EmptyRing))?,
            *id,
        )
    } else {
        return Err(OpError::IllegalStateTransition);
    };

    if let Some(req_sub) = sub_op
        .sm
        .consume_to_output(SubscribeMsg::FetchRouting { target, id })?
    {
        op_storage
            .notify_change(Message::from(req_sub), Operation::Subscribe(sub_op))
            .await?;
    }
    Ok(())
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
                sm: StateMachine::from_state(SubscribeState::ReceivedRequest),
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
    let new_state;
    let return_msg;
    match other_host_msg {
        SubscribeMsg::RequestSub { id, key, target } => {
            // fast tracked from the request_sub func
            debug_assert!(matches!(
                state.sm.state(),
                SubscribeState::AwaitingResponse { .. }
            ));
            let sender = PeerKeyLocation {
                peer: conn_manager.peer_key(),
                location: op_storage.ring.own_location(),
            };
            new_state = Some(state);
            return_msg = Some(Message::from(SubscribeMsg::SeekNode {
                id,
                key,
                target,
                subscriber: sender,
            }));
        }
        SubscribeMsg::SeekNode {
            key,
            id,
            target,
            subscriber,
        } => {
            let sender = PeerKeyLocation {
                peer: conn_manager.peer_key(),
                location: op_storage.ring.own_location(),
            };

            if !op_storage.ring.has_contract(&key) {
                //FIXME: should try forward to someone else who may have it first
                // this node does not have the contract, return a void result to the requester
                log::info!("Contract {} not found while processing info", key);
                return Ok(OperationResult {
                    return_msg: Some(Message::from(SubscribeMsg::ReturnSub {
                        key,
                        id,
                        subscribed: false,
                        sender,
                    })),
                    state: None,
                });
            }

            op_storage.ring.add_subscriber(key.clone(), subscriber);

            todo!()
        }
        _ => return Err(OpError::IllegalStateTransition),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::Subscribe),
    })
}

mod messages {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub(crate) enum SubscribeMsg {
        FetchRouting {
            id: Transaction,
            target: PeerKeyLocation,
        },
        RequestSub {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
        },
        SeekNode {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
            subscriber: PeerKeyLocation,
        },
        ReturnSub {
            id: Transaction,
            key: ContractKey,
            sender: PeerKeyLocation,
            subscribed: bool,
        },
    }

    impl SubscribeMsg {
        pub(crate) fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                Self::FetchRouting { id, .. } => id,
                Self::RequestSub { id, .. } => id,
                Self::ReturnSub { id, .. } => id,
            }
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                _ => None,
            }
        }
    }
}
