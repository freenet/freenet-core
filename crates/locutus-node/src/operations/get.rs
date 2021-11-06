use std::time::Duration;

use crate::{
    config::PEER_TIMEOUT,
    conn_manager::{ConnectionBridge, PeerKey},
    contract::{ContractError, ContractHandlerEvent, ContractKey, StoreResponse},
    message::{Message, Transaction, TxType},
    node::OpManager,
    ring::{PeerKeyLocation, RingError},
};

use super::{
    handle_op_result,
    state_machine::{StateMachine, StateMachineImpl},
    OpError, Operation, OperationResult,
};

pub(crate) use self::messages::GetMsg;

pub(crate) struct GetOp {
    sm: StateMachine<GetOpSm>,
    _ttl: Duration,
}

impl GetOp {
    /// Maximum number of retries to get values.
    const MAX_RETRIES: usize = 10;

    pub fn start_op(key: ContractKey, fetch_contract: bool) -> Self {
        let id = Transaction::new(<GetMsg as TxType>::tx_type_id());
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

struct GetOpSm;

impl StateMachineImpl for GetOpSm {
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
                GetState::AwaitingResponse {
                    fetch_contract: false,
                    ..
                },
                GetMsg::ReturnGet {
                    key,
                    value: StoreResponse { value: Some(_), .. },
                    ..
                },
            ) => {
                log::debug!("Get response received for contract {}", key);
                Some(GetState::Completed)
            }
            (
                GetState::AwaitingResponse {
                    fetch_contract: true,
                    ..
                },
                GetMsg::ReturnGet {
                    key,
                    value:
                        StoreResponse {
                            value: Some(_),
                            contract: Some(_),
                            ..
                        },
                    ..
                },
            ) => {
                log::debug!("Get response received for contract {}", key);
                Some(GetState::Completed)
            }
            (
                GetState::AwaitingResponse {
                    fetch_contract: true,
                    ..
                },
                GetMsg::ReturnGet {
                    key,
                    value:
                        StoreResponse {
                            value: Some(_),
                            contract: None,
                            ..
                        },
                    ..
                },
            ) => {
                log::error!(
                    "Get response received for contract {}, but the contract wasn't returned!",
                    key
                );
                // error out, should not be possible
                None
            }
            // states of the receiver
            (GetState::ReceivedRequest, GetMsg::ReturnGet { .. }) => Some(GetState::Completed),
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
                    key,
                    ..
                },
            ) => {
                if retries < GetOp::MAX_RETRIES {
                    // no respose received from this peer, so skip it in the next iteration
                    skip_list.push(sender.peer);
                    Some(GetState::AwaitingResponse {
                        skip_list,
                        retries: retries + 1,
                        fetch_contract,
                    })
                } else {
                    log::error!(
                        "Failed getting a value for contract {}, reached max retries",
                        key
                    );
                    None
                }
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
                GetState::ReceivedRequest,
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
    ReceivedRequest,
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
                sm: StateMachine::from_state(GetState::ReceivedRequest),
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
    _conn_manager: &mut CB,
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
            // fast tracked from the request_get func
            debug_assert!(matches!(
                state.sm.state(),
                GetState::AwaitingResponse { .. }
            ));
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
            let sender = op_storage.ring.own_location();
            if !op_storage.ring.contract_exists(&key) {
                //FIXME: should try forward to someone else who may have it first
                // this node does not have the contract, return a void result to the requester
                log::info!("Contract {} not found while processing a get request", key);
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
                    // shouldn't be a reachable path
                    log::error!(
                        "contract retrieved ({}) and asked ({}) are not the same",
                        returned_key,
                        key
                    );
                    return Err(OpError::IllegalStateTransition);
                }

                match &value {
                    Ok(StoreResponse {
                        value: None,
                        contract: None,
                    }) => return Err(ContractError::ContractNotFound(key).into()),
                    Ok(StoreResponse {
                        value: Some(_),
                        contract: None,
                    }) if fetch_contract => return Err(ContractError::ContractNotFound(key).into()),
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
                "Contract value for {} not available from peer {}, retrying with other peers.",
                sender.peer,
                key
            );
            // will error out in case it has reached max number of retries
            state
                .sm
                .consume_to_state(GetMsg::ReturnGet {
                    id,
                    key,
                    sender,
                    value: StoreResponse {
                        value: None,
                        contract: None,
                    },
                })
                .map_err(|_: OpError<CErr>| OpError::MaxRetriesExceeded(id, "get".to_owned()))?;
            if let GetState::AwaitingResponse {
                skip_list,
                fetch_contract,
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
                    new_state = Some(state);
                } else {
                    return Err(RingError::NoCachingPeers(key).into());
                }
            } else {
                return Err(OpError::IllegalStateTransition);
            }
        }
        GetMsg::ReturnGet {
            key,
            value:
                StoreResponse {
                    value: Some(value),
                    contract,
                },
            id,
            sender,
        } => {
            let require_contract = matches!(
                state.sm.state(),
                GetState::AwaitingResponse {
                    fetch_contract: true,
                    ..
                }
            );

            // received a response with a contract value
            if require_contract {
                if let Some(contract) = &contract {
                    // store contract first
                    op_storage
                        .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()))
                        .await?;
                } else {
                    // no contract, consider this like an error ignoring the incoming update value
                    op_storage
                        .notify_change(
                            Message::from(GetMsg::ReturnGet {
                                id,
                                key,
                                value: StoreResponse {
                                    value: None,
                                    contract: None,
                                },
                                sender,
                            }),
                            Operation::Get(state),
                        )
                        .await?;
                    return Err(OpError::StatePushed);
                }
            }

            op_storage
                .notify_contract_handler(ContractHandlerEvent::PushQuery {
                    key,
                    value: value.clone(),
                })
                .await?;

            return_msg = state
                .sm
                .consume_to_output(GetMsg::ReturnGet {
                    key,
                    value: StoreResponse {
                        value: Some(value),
                        contract,
                    },
                    id,
                    sender,
                })?
                .map(Message::from);
            new_state = None;
        }
        _ => return Err(OpError::IllegalStateTransition),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::Get),
    })
}

mod messages {
    use std::fmt::Display;

    use crate::contract::StoreResponse;

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
                _ => None,
            }
        }
    }

    impl Display for GetMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::FetchRouting { .. } => write!(f, "FetchRouting(id: {})", id),
                Self::RequestGet { .. } => write!(f, "RequestGet(id: {})", id),
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {})", id),
                Self::ReturnGet { .. } => write!(f, "ReturnGet(id: {})", id),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        contract::{Contract, ContractValue},
        node::SimStorageError,
        ring::Location,
    };

    use super::*;

    type Err = OpError<SimStorageError>;

    #[test]
    fn successful_get_op_seq() -> Result<(), anyhow::Error> {
        let id = Transaction::new(<GetMsg as TxType>::tx_type_id());
        let bytes = crate::test_utils::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: Contract = gen.arbitrary()?;
        let target_loc = PeerKeyLocation {
            location: Some(Location::random()),
            peer: PeerKey::random(),
        };

        let mut requester = GetOp::start_op(contract.key(), true).sm;
        let mut target = StateMachine::<GetOpSm>::from_state(GetState::ReceivedRequest);

        let req_msg = requester
            .consume_to_output::<Err>(GetMsg::FetchRouting {
                id,
                target: target_loc,
            })?
            .ok_or(anyhow::anyhow!("no msg"))?;
        assert!(matches!(req_msg, GetMsg::RequestGet { .. }));
        assert!(matches!(
            requester.state(),
            GetState::AwaitingResponse { .. }
        ));

        assert!(matches!(target.state(), GetState::ReceivedRequest));
        let res_msg = target
            .consume_to_output::<Err>(GetMsg::ReturnGet {
                key: contract.key(),
                id,
                value: StoreResponse {
                    contract: Some(contract),
                    value: Some(ContractValue::new(b"abc".to_vec())),
                },
                sender: target_loc.clone(),
            })?
            .ok_or(anyhow::anyhow!("no msg"))?;
        assert!(matches!(target.state(), GetState::Completed));
        assert!(matches!(res_msg, GetMsg::ReturnGet { .. }));

        let res_msg = requester.consume_to_output::<Err>(res_msg)?;
        assert!(res_msg.is_none());

        Ok(())
    }
}
