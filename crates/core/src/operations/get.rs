use freenet_stdlib::client_api::{ErrorKind, HostResponse};
use freenet_stdlib::prelude::*;
use std::fmt::Display;
use std::pin::Pin;
use std::{future::Future, time::Instant};

use crate::client_events::HostResult;
use crate::{
    contract::{ContractHandlerEvent, StoreResponse},
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager, PeerId},
    operations::{OpInitialization, Operation},
    ring::{Location, PeerKeyLocation, RingError},
};

use super::{OpEnum, OpError, OpOutcome, OperationResult};

pub(crate) use self::messages::GetMsg;

/// Maximum number of retries to get values.
const MAX_RETRIES: usize = 10;

pub(crate) fn start_op(key: ContractKey, fetch_contract: bool) -> GetOp {
    let contract_location = Location::from(&key);
    let id = Transaction::new::<GetMsg>();
    tracing::debug!(tx = %id, "Requesting get contract {key} @ loc({contract_location})");
    let state = Some(GetState::PrepareRequest {
        key,
        id,
        fetch_contract,
    });
    GetOp {
        id,
        state,
        result: None,
        stats: Some(Box::new(GetStats {
            contract_location,
            next_peer: None,
            transfer_time: None,
            first_response_time: None,
        })),
    }
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get(
    op_manager: &OpManager,
    get_op: GetOp,
    skip_list: Vec<PeerId>,
) -> Result<(), OpError> {
    let (target, id) = if let Some(GetState::PrepareRequest { key, id, .. }) = &get_op.state {
        // the initial request must provide:
        // - a location in the network where the contract resides
        // - and the key of the contract value to get
        (
            op_manager
                .ring
                .closest_potentially_caching(key, skip_list.as_slice())
                .into_iter()
                .next()
                .ok_or(RingError::EmptyRing)?,
            *id,
        )
    } else {
        return Err(OpError::UnexpectedOpState);
    };
    tracing::debug!(
        tx = %id,
        target = %target.peer,
        "Preparing get contract request",
    );

    match get_op.state {
        Some(GetState::PrepareRequest {
            fetch_contract,
            key,
            id,
            ..
        }) => {
            let new_state = Some(GetState::AwaitingResponse {
                retries: 0,
                fetch_contract,
                requester: None,
                current_hop: op_manager.ring.max_hops_to_live,
            });

            let msg = GetMsg::RequestGet {
                id,
                key,
                target: target.clone(),
                fetch_contract,
                skip_list,
            };

            let op = GetOp {
                id,
                state: new_state,
                result: None,
                stats: get_op.stats.map(|mut s| {
                    s.next_peer = Some(target);
                    s
                }),
            };

            op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Get(op))
                .await?;
        }
        _ => return Err(OpError::invalid_transition(get_op.id)),
    }
    Ok(())
}

#[derive(Debug)]
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
        /// If specified the peer waiting for the response upstream
        requester: Option<PeerKeyLocation>,
        fetch_contract: bool,
        retries: usize,
        current_hop: usize,
    },
}

impl Display for GetState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetState::ReceivedRequest => write!(f, "ReceivedRequest"),
            GetState::PrepareRequest {
                key,
                id,
                fetch_contract,
            } => {
                write!(
                    f,
                    "PrepareRequest(key: {}, id: {}, fetch_contract: {})",
                    key, id, fetch_contract
                )
            }
            GetState::AwaitingResponse {
                requester,
                fetch_contract,
                retries,
                current_hop,
            } => {
                write!(f, "AwaitingResponse(requester: {:?}, fetch_contract: {}, retries: {}, current_hop: {})", requester, fetch_contract, retries, current_hop)
            }
        }
    }
}

struct GetStats {
    /// Next peer in get path to be targeted
    next_peer: Option<PeerKeyLocation>,
    contract_location: Location,
    /// (start, end)
    first_response_time: Option<(Instant, Option<Instant>)>,
    /// (start, end)
    transfer_time: Option<(Instant, Option<Instant>)>,
}

#[derive(Clone)]
pub(crate) struct GetResult {
    key: ContractKey,
    pub state: WrappedState,
    pub contract: Option<ContractContainer>,
}

impl TryFrom<GetOp> for GetResult {
    type Error = OpError;

    fn try_from(value: GetOp) -> Result<Self, Self::Error> {
        match value.result {
            Some(r) => Ok(r),
            _ => Err(OpError::UnexpectedOpState),
        }
    }
}

pub(crate) struct GetOp {
    pub id: Transaction,
    state: Option<GetState>,
    pub(super) result: Option<GetResult>,
    stats: Option<Box<GetStats>>,
}

impl GetOp {
    pub(super) fn outcome(&self) -> OpOutcome {
        if let Some((
            GetResult {
                state, contract, ..
            },
            GetStats {
                next_peer: Some(target_peer),
                contract_location,
                first_response_time: Some((response_start, Some(response_end))),
                transfer_time: Some((transfer_start, Some(transfer_end))),
                ..
            },
        )) = self.result.as_ref().zip(self.stats.as_deref())
        {
            let payload_size = state.size()
                + contract
                    .as_ref()
                    .map(|c| c.data().len())
                    .unwrap_or_default();
            OpOutcome::ContractOpSuccess {
                target_peer,
                contract_location: *contract_location,
                payload_size,
                first_response_time: *response_end - *response_start,
                payload_transfer_time: *transfer_end - *transfer_start,
            }
        } else {
            OpOutcome::Incomplete
        }
    }

    pub(super) fn finalized(&self) -> bool {
        self.result.is_some()
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        match &self.result {
            Some(GetResult {
                key,
                state,
                contract,
            }) => Ok(HostResponse::ContractResponse(
                freenet_stdlib::client_api::ContractResponse::GetResponse {
                    key: *key,
                    contract: contract.clone(),
                    state: state.clone(),
                },
            )),
            None => Err(ErrorKind::OperationError {
                cause: "get didn't finish successfully".into(),
            }
            .into()),
        }
    }
}

impl Operation for GetOp {
    type Message = GetMsg;
    type Result = GetResult;

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
        let mut sender: Option<PeerId> = None;
        if let Some(peer_key_loc) = msg.sender().cloned() {
            sender = Some(peer_key_loc.peer);
        };
        let tx = *msg.id();
        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Get(get_op))) => {
                Ok(OpInitialization { op: get_op, sender })
                // was an existing operation, other peer messaged back
            }
            Ok(Some(op)) => {
                let _ = op_manager.push(tx, op).await;
                Err(OpError::OpNotPresent(tx))
            }
            Ok(None) => {
                // new request to get a value for a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(GetState::ReceivedRequest),
                        id: tx,
                        result: None,
                        stats: None, // don't care about stats in target peers
                    },
                    sender,
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a, NB: NetworkBridge>(
        self,
        _conn_manager: &'a mut NB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let new_state;
            let mut result = None;
            let mut stats = self.stats;

            match input {
                GetMsg::RequestGet {
                    key,
                    id,
                    target,
                    fetch_contract,
                    skip_list,
                } => {
                    // fast tracked from the request_get func
                    debug_assert!(matches!(
                        self.state,
                        Some(GetState::AwaitingResponse { .. })
                    ));
                    tracing::info!(tx = %id, %key, target = %target.peer, "Seek contract");
                    new_state = self.state;
                    stats = Some(Box::new(GetStats {
                        contract_location: Location::from(key),
                        next_peer: None,
                        transfer_time: None,
                        first_response_time: None,
                    }));
                    let own_loc = op_manager.ring.connection_manager.own_location();
                    let mut new_skip_list = skip_list.clone();
                    new_skip_list.push(own_loc.peer.clone());
                    return_msg = Some(GetMsg::SeekNode {
                        key: *key,
                        id: *id,
                        target: target.clone(),
                        sender: own_loc.clone(),
                        fetch_contract: *fetch_contract,
                        htl: op_manager.ring.max_hops_to_live,
                        skip_list: new_skip_list,
                    });
                }
                GetMsg::SeekNode {
                    key,
                    id,
                    fetch_contract,
                    sender,
                    target,
                    htl,
                    skip_list,
                } => {
                    let htl = *htl;
                    let id = *id;
                    let key: ContractKey = *key;
                    let fetch_contract = *fetch_contract;
                    let this_peer = target.clone();

                    if let Some(s) = stats.as_mut() {
                        s.next_peer = Some(this_peer.clone());
                    }

                    let mut new_skip_list = skip_list.clone();
                    new_skip_list.push(this_peer.clone().peer);

                    let get_result = op_manager
                        .notify_contract_handler(ContractHandlerEvent::GetQuery {
                            key,
                            return_contract_code: fetch_contract,
                        })
                        .await;

                    let (returned_key, contract, state) = match get_result {
                        Ok(ContractHandlerEvent::GetResponse {
                            key,
                            response:
                                Ok(StoreResponse {
                                    state: Some(state),
                                    contract,
                                }),
                        }) => (key, contract, state),
                        _ => {
                            tracing::debug!(
                                tx = %id,
                                %key,
                                %this_peer,
                                "Contract not found @ peer {}, retrying with other peers",
                                sender.peer
                            );
                            return try_forward_or_return(
                                id,
                                key,
                                (htl, fetch_contract),
                                (this_peer, sender.clone()),
                                skip_list,
                                op_manager,
                                stats,
                            )
                            .await;
                        }
                    };

                    tracing::debug!(tx = %id, "Contract {returned_key} found @ peer {}", target.peer);

                    match self.state {
                        Some(GetState::AwaitingResponse { requester, .. }) => {
                            if let Some(requester) = requester {
                                new_state = None;
                                tracing::debug!(tx = %id, "Returning contract {} to {}", key, sender.peer);
                                return_msg = Some(GetMsg::ReturnGet {
                                    id,
                                    key,
                                    value: StoreResponse {
                                        state: Some(state),
                                        contract,
                                    },
                                    sender: target.clone(),
                                    target: requester,
                                    skip_list: skip_list.clone(),
                                });
                            } else {
                                tracing::debug!(
                                    tx = %id,
                                    "Completed operation, get response received for contract {key}"
                                );
                                // Completed op
                                new_state = None;
                                return_msg = None;
                            }
                        }
                        Some(GetState::ReceivedRequest) => {
                            new_state = None;
                            tracing::debug!(tx = %id, "Returning contract {} to {}", key, sender.peer);
                            return_msg = Some(GetMsg::ReturnGet {
                                id,
                                key,
                                value: StoreResponse {
                                    state: Some(state),
                                    contract,
                                },
                                sender: target.clone(),
                                target: sender.clone(),
                                skip_list: skip_list.clone(),
                            });
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    }
                }
                GetMsg::ReturnGet {
                    id,
                    key,
                    value: StoreResponse { state: None, .. },
                    sender,
                    target,
                    skip_list,
                } => {
                    let this_peer = target;
                    tracing::warn!(
                        tx = %id,
                        %key,
                        %this_peer,
                        "Neither contract or contract value for contract found at peer {}, \
                        retrying with other peers",
                        sender.peer
                    );

                    match self.state {
                        Some(GetState::AwaitingResponse {
                            fetch_contract,
                            retries,
                            requester,
                            current_hop,
                        }) => {
                            // todo: register in the stats for the outcome of the op that failed to get a response from this peer
                            if retries < MAX_RETRIES {
                                // no response received from this peer, so skip it in the next iteration
                                let mut new_skip_list = skip_list.clone();
                                new_skip_list.push(target.peer.clone());
                                if let Some(target) = op_manager
                                    .ring
                                    .closest_potentially_caching(key, new_skip_list.as_slice())
                                    .into_iter()
                                    .next()
                                {
                                    return_msg = Some(GetMsg::SeekNode {
                                        id: *id,
                                        key: *key,
                                        target,
                                        sender: this_peer.clone(),
                                        fetch_contract,
                                        htl: current_hop,
                                        skip_list: new_skip_list.clone(),
                                    });
                                } else if let Some(requester_peer) = requester.clone() {
                                    tracing::warn!(
                                        tx = %id,
                                        %key,
                                        %this_peer,
                                        target = %requester_peer,
                                        "No other peers found while trying to get the contract, returning response to requester"
                                    );
                                    return_msg = Some(GetMsg::ReturnGet {
                                        id: *id,
                                        key: *key,
                                        value: StoreResponse {
                                            state: None,
                                            contract: None,
                                        },
                                        sender: this_peer.clone(),
                                        target: requester_peer,
                                        skip_list: new_skip_list.clone(),
                                    });
                                } else {
                                    tracing::error!(
                                        tx = %id,
                                        "Failed getting a value for contract {}, reached max retries",
                                        key
                                    );
                                    return_msg = None;
                                    result = Some(GetResult {
                                        key: *key,
                                        state: WrappedState::new(vec![]),
                                        contract: None,
                                    });
                                }
                                new_state = Some(GetState::AwaitingResponse {
                                    retries: retries + 1,
                                    fetch_contract,
                                    requester,
                                    current_hop,
                                });
                            } else {
                                tracing::error!(
                                    tx = %id,
                                    "Failed getting a value for contract {}, reached max retries",
                                    key
                                );
                                if let Some(requester_peer) = requester.clone() {
                                    tracing::warn!(
                                        tx = %id,
                                        %key,
                                        %this_peer,
                                        target = %requester_peer,
                                        "No other peers found while trying to get the contract, returning response to requester"
                                    );
                                    return_msg = Some(GetMsg::ReturnGet {
                                        id: *id,
                                        key: *key,
                                        value: StoreResponse {
                                            state: None,
                                            contract: None,
                                        },
                                        sender: this_peer.clone(),
                                        target: requester_peer,
                                        skip_list: skip_list.clone(),
                                    });
                                    new_state = None;
                                } else {
                                    tracing::error!(
                                        tx = %id,
                                        "Failed getting a value for contract {}, reached max retries",
                                        key
                                    );
                                    return_msg = None;
                                    new_state = Some(GetState::AwaitingResponse {
                                        retries: retries + 1,
                                        fetch_contract,
                                        requester,
                                        current_hop,
                                    });
                                    result = Some(GetResult {
                                        key: *key,
                                        state: WrappedState::new(vec![]),
                                        contract: None,
                                    });
                                }
                            }
                        }
                        Some(GetState::ReceivedRequest) => {
                            tracing::debug!(tx = %id, "Returning contract {} to {}", key, sender.peer);
                            new_state = None;
                            return_msg = Some(GetMsg::ReturnGet {
                                id: *id,
                                key: *key,
                                value: StoreResponse {
                                    state: None,
                                    contract: None,
                                },
                                sender: this_peer.clone(),
                                target: sender.clone(),
                                skip_list: skip_list.clone(),
                            });
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    };
                }
                GetMsg::ReturnGet {
                    id,
                    key,
                    value:
                        StoreResponse {
                            state: Some(value),
                            contract,
                        },
                    sender,
                    target,
                    skip_list,
                } => {
                    let id = *id;
                    let key = *key;

                    tracing::info!(tx = %id, %key, "Received get response with state: {:?}", self.state.as_ref().unwrap());
                    let require_contract = matches!(
                        self.state,
                        Some(GetState::AwaitingResponse {
                            fetch_contract: true,
                            ..
                        })
                    );

                    let requester = if let Some(GetState::AwaitingResponse { requester, .. }) =
                        self.state.as_ref()
                    {
                        requester.clone()
                    } else {
                        return Err(OpError::UnexpectedOpState);
                    };

                    // received a response with a contract value
                    if require_contract && contract.is_none() && requester.is_some() {
                        // no contract, consider this like an error ignoring the incoming update value
                        tracing::warn!(
                            tx = %id,
                            "Contract not received from peer {} while required",
                            sender.peer
                        );

                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.push(sender.peer.clone());

                        let requester = requester.unwrap();

                        tracing::warn!(
                            tx = %id,
                            %key,
                            at = %sender.peer,
                            target = %requester,
                            "Contract not received while required, returning response to requester",
                        );

                        op_manager
                            .notify_op_change(
                                NetMessage::from(GetMsg::ReturnGet {
                                    id,
                                    key,
                                    value: StoreResponse {
                                        state: None,
                                        contract: None,
                                    },
                                    sender: sender.clone(),
                                    target: requester.clone(),
                                    skip_list: new_skip_list,
                                }),
                                OpEnum::Get(GetOp {
                                    id,
                                    state: self.state,
                                    result: None,
                                    stats,
                                }),
                            )
                            .await?;
                        return Err(OpError::StatePushed);
                    }

                    let is_original_requester = matches!(
                        self.state,
                        Some(GetState::AwaitingResponse {
                            requester: None,
                            ..
                        })
                    );
                    let should_subscribe = op_manager.ring.should_seed(&key);
                    // TODO: In case of original requester, we should check if is possible to cache the contract
                    let should_put = is_original_requester || should_subscribe;

                    if should_put {
                        tracing::debug!(tx = %id, %key, %is_original_requester, %should_subscribe, "Putting contract at executor");
                        let res = op_manager
                            .notify_contract_handler(ContractHandlerEvent::PutQuery {
                                key,
                                state: value.clone(),
                                related_contracts: RelatedContracts::default(), // fixme: i think we need to get the related contracts so the final put is ok
                                contract: contract.clone(),
                            })
                            .await?;
                        match res {
                            ContractHandlerEvent::PutResponse { new_value: Ok(_) } => {
                                tracing::debug!(tx = %id, %key, "Contract put at executor");
                                let is_subscribed_contract =
                                    op_manager.ring.is_seeding_contract(&key);
                                if !is_subscribed_contract && should_subscribe {
                                    tracing::debug!(tx = %id, %key, peer = %op_manager.ring.connection_manager.get_peer_key().unwrap(), "Contract not cached @ peer, caching");
                                    op_manager.ring.seed_contract(key);
                                    let mut new_skip_list = skip_list.clone();
                                    new_skip_list.push(sender.peer.clone());
                                    super::start_subscription_request(
                                        op_manager,
                                        key,
                                        false,
                                        new_skip_list,
                                    )
                                    .await;
                                }
                            }
                            ContractHandlerEvent::PutResponse {
                                new_value: Err(err),
                            } => {
                                if is_original_requester {
                                    tracing::debug!(tx = %id, error = %err, "Failed put at executor");
                                    return Err(OpError::ExecutorError(err));
                                } else {
                                    let mut new_skip_list = skip_list.clone();
                                    new_skip_list.push(sender.peer.clone());

                                    let requester = requester.unwrap();

                                    tracing::warn!(
                                        tx = %id,
                                        %key,
                                        %sender.peer,
                                        target = %requester,
                                        "Failed put at executor, returning response to requester",
                                    );

                                    op_manager
                                        .notify_op_change(
                                            NetMessage::from(GetMsg::ReturnGet {
                                                id,
                                                key,
                                                value: StoreResponse {
                                                    state: None,
                                                    contract: None,
                                                },
                                                sender: sender.clone(),
                                                target: requester.clone(),
                                                skip_list: new_skip_list,
                                            }),
                                            OpEnum::Get(GetOp {
                                                id,
                                                state: self.state,
                                                result: None,
                                                stats,
                                            }),
                                        )
                                        .await?;
                                    return Err(OpError::StatePushed);
                                }
                            }
                            _ => unreachable!(),
                        }
                    }

                    match self.state {
                        Some(GetState::AwaitingResponse {
                            requester: None, ..
                        }) => {
                            tracing::info!(tx = %id, %key, "Get response received for contract at original requester");
                            new_state = None;
                            return_msg = None;
                            result = Some(GetResult {
                                key,
                                state: value.clone(),
                                contract: contract.clone(),
                            });
                        }
                        Some(GetState::AwaitingResponse {
                            requester: Some(requester),
                            ..
                        }) => {
                            tracing::info!(tx = %id, %key, "Get response received for contract at hop peer");
                            new_state = None;
                            return_msg = Some(GetMsg::ReturnGet {
                                id,
                                key,
                                value: StoreResponse {
                                    state: Some(value.clone()),
                                    contract: contract.clone(),
                                },
                                sender: target.clone(),
                                target: requester.clone(),
                                skip_list: skip_list.clone(),
                            });
                            tracing::debug!(tx = %id, %key, target = %requester, "Returning contract to requester");
                            result = Some(GetResult {
                                key,
                                state: value.clone(),
                                contract: contract.clone(),
                            });
                        }
                        Some(GetState::ReceivedRequest) => {
                            tracing::info!(tx = %id, "Returning contract {} to {}", key, sender.peer);
                            new_state = None;
                            return_msg = Some(GetMsg::ReturnGet {
                                id,
                                key,
                                value: StoreResponse {
                                    state: Some(value.clone()),
                                    contract: contract.clone(),
                                },
                                sender: target.clone(),
                                target: sender.clone(),
                                skip_list: skip_list.clone(),
                            });
                        }
                        Some(other) => {
                            return Err(OpError::invalid_transition_with_state(
                                self.id,
                                Box::new(other),
                            ))
                        }
                        None => return Err(OpError::invalid_transition(self.id)),
                    };
                }
            }

            build_op_result(self.id, new_state, return_msg, result, stats)
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<GetState>,
    msg: Option<GetMsg>,
    result: Option<GetResult>,
    stats: Option<Box<GetStats>>,
) -> Result<OperationResult, OpError> {
    let output_op = state.map(|state| GetOp {
        id,
        state: Some(state),
        result,
        stats,
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        state: output_op.map(OpEnum::Get),
    })
}

async fn try_forward_or_return(
    id: Transaction,
    key: ContractKey,
    (htl, fetch_contract): (usize, bool),
    (this_peer, sender): (PeerKeyLocation, PeerKeyLocation),
    skip_list: &[PeerId],
    op_manager: &OpManager,
    stats: Option<Box<GetStats>>,
) -> Result<OperationResult, OpError> {
    tracing::warn!(
        tx = %id,
        %key,
        this_peer = %this_peer.peer,
        "Contract not found while processing a get request",
    );

    let mut new_skip_list = skip_list.to_vec();
    new_skip_list.push(this_peer.peer.clone());

    let new_htl = htl - 1;

    let new_target = if new_htl == 0 {
        tracing::warn!(
            tx = %id,
            sender = %sender.peer,
            "The maximum hops have been exceeded, sending response back to the node",
        );
        None
    } else {
        match op_manager
            .ring
            .closest_potentially_caching(&key, new_skip_list.as_slice())
        {
            Some(target) => Some(target),
            None => {
                tracing::warn!(
                    tx = %id,
                    %key,
                    this_peer = %this_peer.peer,
                    "No other peers found while trying to get the contract",
                );
                None
            }
        }
    };

    if let Some(target) = new_target {
        tracing::debug!(
            tx = %id,
            "Forwarding get request to {}",
            target.peer
        );
        build_op_result(
            id,
            Some(GetState::AwaitingResponse {
                requester: Some(sender),
                retries: 0,
                fetch_contract,
                current_hop: new_htl,
            }),
            Some(GetMsg::SeekNode {
                id,
                key,
                fetch_contract,
                sender: this_peer,
                target,
                htl: new_htl,
                skip_list: new_skip_list,
            }),
            None,
            stats,
        )
    } else {
        tracing::debug!(
            tx = %id,
            "Cannot find any other peers to forward the get request to, returning get response to {}",
            sender.peer
        );

        build_op_result(
            id,
            None,
            Some(GetMsg::ReturnGet {
                key,
                id,
                value: StoreResponse {
                    state: None,
                    contract: None,
                },
                sender: op_manager.ring.connection_manager.own_location(),
                target: sender,
                skip_list: new_skip_list,
            }),
            None,
            stats,
        )
    }
}

mod messages {
    use std::{borrow::Borrow, fmt::Display};

    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum GetMsg {
        RequestGet {
            id: Transaction,
            target: PeerKeyLocation,
            key: ContractKey,
            fetch_contract: bool,
            skip_list: Vec<PeerId>,
        },
        SeekNode {
            id: Transaction,
            key: ContractKey,
            fetch_contract: bool,
            target: PeerKeyLocation,
            sender: PeerKeyLocation,
            htl: usize,
            skip_list: Vec<PeerId>,
        },
        ReturnGet {
            id: Transaction,
            key: ContractKey,
            value: StoreResponse,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            skip_list: Vec<PeerId>,
        },
    }

    impl InnerMessage for GetMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::RequestGet { id, .. } => id,
                Self::SeekNode { id, .. } => id,
                Self::ReturnGet { id, .. } => id,
            }
        }

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::RequestGet { target, .. } => Some(target),
                Self::ReturnGet { target, .. } => Some(target),
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                GetMsg::RequestGet { key, .. } => Some(Location::from(key.id())),
                GetMsg::SeekNode { key, .. } => Some(Location::from(key.id())),
                GetMsg::ReturnGet { key, .. } => Some(Location::from(key.id())),
            }
        }
    }

    impl GetMsg {
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
                Self::RequestGet { .. } => write!(f, "RequestGet(id: {id})"),
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {id})"),
                Self::ReturnGet { .. } => write!(f, "ReturnGet(id: {id})"),
            }
        }
    }
}
