use std::time::Duration;

use crate::{
    config::PEER_TIMEOUT,
    conn_manager::{ConnectionBridge, PeerKey, PeerKeyLocation},
    contract::{ContractError, ContractHandlerEvent, ContractKey, StoreResponse},
    message::{GetTxType, Message, Transaction},
    node::OpManager,
    ring::RingError,
};

use super::{
    handle_op_result,
    state_machine::{StateMachine, StateMachineImpl},
    OpError, Operation, OperationResult,
};

pub(crate) use self::messages::GetMsg;

pub(crate) struct GetOp {
    sm: StateMachine<GetOpSM>,
    _ttl: Duration,
}

impl GetOp {
    /// Maximum number of retries to get values.
    const MAX_RETRIES: usize = 10;

    pub fn start_op(key: ContractKey, fetch_contract: bool) -> Self {
        let id = Transaction::new(<GetMsg as GetTxType>::tx_type_id());
        let sm = StateMachine::from_state(GetState::PrepareRequest {
            key,
            id,
            fetch_contract,
        });
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
            // states of the requester
            (GetState::PrepareRequest { fetch_contract, .. }, GetMsg::FetchRouting { .. }) => {
                Some(GetState::AwaitingResponse {
                    skip_list: vec![],
                    retries: 0,
                    fetch_contract: *fetch_contract,
                })
            }
            (
                GetState::AwaitingResponse { .. },
                GetMsg::ReturnGet {
                    key,
                    value: StoreResponse { value: Some(_), .. },
                    ..
                },
            ) => {
                log::info!("Get response received for contract {}", key);
                Some(GetState::Completed)
            }
            // states of the petitioner
            (GetState::ReceiveRequest, GetMsg::ReturnGet { .. }) => Some(GetState::Completed),
            _ => None,
        }
    }

    fn state_transition_from_input(state: Self::State, input: Self::Input) -> Option<Self::State> {
        match (state, input) {
            (
                GetState::AwaitingResponse {
                    mut skip_list,
                    retries,
                    fetch_contract,
                    ..
                },
                GetMsg::ReturnGet {
                    sender,
                    value:
                        StoreResponse {
                            value: None,
                            contract: None,
                        },
                    ..
                },
            ) if retries < GetOp::MAX_RETRIES => {
                // no respose received from this peer, so skip it in the next iteration
                skip_list.push(sender.peer);
                Some(GetState::AwaitingResponse {
                    skip_list,
                    retries,
                    fetch_contract,
                })
            }
            (
                GetState::AwaitingResponse { .. },
                GetMsg::ReturnGet {
                    key,
                    value:
                        StoreResponse {
                            value: None,
                            contract: None,
                        },
                    ..
                },
            ) => {
                log::info!("Failed getting a value for contract {}", key);
                None
            }
            _ => None,
        }
    }

    fn output_from_input(state: Self::State, input: Self::Input) -> Option<Self::Output> {
        match (state, input) {
            (
                GetState::PrepareRequest {
                    key,
                    id,
                    fetch_contract,
                },
                GetMsg::FetchRouting { target, .. },
            ) => Some(GetMsg::RequestGet {
                key,
                target,
                id,
                fetch_contract,
            }),
            (
                GetState::ReceiveRequest,
                GetMsg::ReturnGet {
                    id,
                    key,
                    value,
                    sender,
                },
            ) => Some(GetMsg::ReturnGet {
                id,
                key,
                value,
                sender,
            }),
            _ => None,
        }
    }
}

enum GetState {
    /// A new petition for a get op.
    ReceiveRequest,
    /// Preparing request for get op.
    PrepareRequest {
        key: ContractKey,
        id: Transaction,
        fetch_contract: bool,
    },
    /// Awaiting response from petition.
    AwaitingResponse {
        skip_list: Vec<PeerKey>,
        retries: usize,
        fetch_contract: bool,
    },
    /// Transaction complete.
    Completed,
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get<CErr>(
    op_storage: &OpManager<CErr>,
    mut get_op: GetOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let (target, id) = if let GetState::PrepareRequest { key, id, .. } = get_op.sm.state() {
        // the initial request must provide:
        // - a location in the network where the contract resides
        // - and the key of the contract value to get
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

pub(crate) async fn handle_get_request<CB, CErr>(
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
            // new request to get a value for a contract, initialize the machine
            let machine = GetOp {
                sm: StateMachine::from_state(GetState::ReceiveRequest),
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
        GetMsg::RequestGet {
            key,
            id,
            target,
            fetch_contract,
        } => {
            new_state = Some(state);
            return_msg = Some(Message::from(GetMsg::SeekNode {
                key,
                id,
                target,
                fetch_contract,
            }));
        }
        GetMsg::SeekNode {
            key,
            id,
            fetch_contract,
            ..
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
                    return_msg: Some(Message::from(GetMsg::ReturnGet {
                        key,
                        id,
                        value: StoreResponse {
                            value: None,
                            contract: None,
                        },
                        sender,
                    })),
                    state: None,
                });
            }
            if let ContractHandlerEvent::FetchResponse {
                response: value,
                key: returned_key,
            } = op_storage
                .notify_contract_handler(ContractHandlerEvent::FetchQuery {
                    key,
                    fetch_contract,
                })
                .await?
            {
                if returned_key != key {
                    return Err(OpError::IllegalStateTransition);
                }

                match &value {
                    Ok(StoreResponse {
                        value: Some(_),
                        contract: None,
                    }) => return Err(ContractError::ContractNotFound(key).into()),
                    _ => {}
                }

                return_msg = state
                    .sm
                    .consume_to_output(GetMsg::ReturnGet {
                        key,
                        id,
                        value: value.map_err(ContractError::from)?,
                        sender,
                    })?
                    .map(Message::from);
                new_state = Some(state);
            } else {
                return Err(OpError::IllegalStateTransition);
            }
        }
        GetMsg::ReturnGet {
            id,
            key,
            value:
                StoreResponse {
                    value: None,
                    contract: None,
                },
            sender,
            ..
        } => {
            log::info!(
                "Contract value for {} not available, retrying with other peers.",
                key
            );
            // will error out in case
            state.sm.consume_to_state(GetMsg::ReturnGet {
                id,
                key,
                sender,
                value: StoreResponse {
                    value: None,
                    contract: None,
                },
            })?;
            if let GetState::AwaitingResponse {
                skip_list,
                fetch_contract,
                retries,
                ..
            } = state.sm.state()
            {
                if let Some(target) = op_storage
                    .ring
                    .closest_caching(&key, 1, skip_list)
                    .into_iter()
                    .next()
                {
                    return_msg = Some(Message::from(GetMsg::SeekNode {
                        id,
                        key,
                        target,
                        fetch_contract: *fetch_contract,
                    }));
                    *retries += 1;
                    new_state = Some(state);
                } else {
                    return Err(OpError::IllegalStateTransition);
                }
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
            fetch_contract: bool,
        },
        SeekNode {
            id: Transaction,
            key: ContractKey,
            target: PeerKeyLocation,
            fetch_contract: bool,
        },
        ReturnGet {
            id: Transaction,
            key: ContractKey,
            value: StoreResponse,
            sender: PeerKeyLocation,
        },
        Retry {
            num_retries: usize,
        },
    }

    impl GetMsg {
        pub fn id(&self) -> &Transaction {
            match self {
                Self::FetchRouting { id, .. } => id,
                Self::RequestGet { id, .. } => id,
                Self::SeekNode { id, .. } => id,
                Self::ReturnGet { id, .. } => id,
                Self::Retry { .. } => unimplemented!(),
            }
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                _ => None,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn successful_get_op_seq() -> Result<(), anyhow::Error> {
        Ok(())
    }
}
