use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use locutus_runtime::ContractKey;

use crate::message::InnerMessage;
use crate::operations::op_trait::Operation;
use crate::operations::OpInitialization;
use crate::{
    config::PEER_TIMEOUT,
    contract::{ContractError, ContractHandlerEvent, StoreResponse},
    message::{Message, Transaction, TxType},
    node::{ConnectionBridge, OpManager, PeerKey},
    ring::{Location, PeerKeyLocation, RingError},
};

use super::{OpEnum, OpError, OperationResult};

pub(crate) use self::messages::GetMsg;

/// Maximum number of retries to get values.
const MAX_RETRIES: usize = 10;
/// Maximum number of hops performed while trying to perform a get (a hop will be performed
/// when the current node cannot perform a get for whichever reason, eg. being out of the caching
/// distance for the contract)
const MAX_GET_RETRY_HOPS: usize = 1;

pub(crate) struct GetOp {
    id: Transaction,
    state: Option<GetState>,
    _ttl: Duration,
}

impl<CErr, CB: ConnectionBridge> Operation<CErr, CB> for GetOp
where
    CErr: std::error::Error + Send + Sync,
    CB: std::marker::Send,
{
    type Message = GetMsg;
    type Error = OpError<CErr>;

    fn load_or_init(
        op_storage: &OpManager<CErr>,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError<CErr>> {
        let mut sender: Option<PeerKey> = None;
        if let Some(peer_key_loc) = msg.sender().cloned() {
            sender = Some(peer_key_loc.peer);
        };
        let tx = *msg.id();
        let result = match op_storage.pop(msg.id()) {
            Some(OpEnum::Get(get_op)) => {
                Ok(OpInitialization { op: get_op, sender })
                // was an existing operation, the other peer messaged back
            }
            Some(_) => return Err(OpError::OpNotPresent(tx)),
            None => {
                // new request to get a value for a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(GetState::ReceivedRequest),
                        id: tx,
                        _ttl: PEER_TIMEOUT,
                    },
                    sender,
                })
            }
        };

        result
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a>(
        self,
        conn_manager: &'a mut CB,
        op_storage: &'a OpManager<CErr>,
        input: Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, Self::Error>> + Send + 'a>> {
        Box::pin(async move {
            let return_msg;
            let new_state;

            match input {
                GetMsg::RequestGet {
                    key,
                    id,
                    target,
                    fetch_contract,
                } => {
                    // fast tracked from the request_get func
                    debug_assert!(matches!(
                        self.state,
                        Some(GetState::AwaitingResponse { .. })
                    ));
                    tracing::debug!("Seek contract {} @ {} (tx: {})", key, target.peer, id);
                    new_state = self.state;
                    return_msg = Some(GetMsg::SeekNode {
                        key,
                        id,
                        target,
                        sender: op_storage.ring.own_location(),
                        fetch_contract,
                        htl: MAX_GET_RETRY_HOPS,
                    });
                }
                GetMsg::SeekNode {
                    key,
                    id,
                    fetch_contract,
                    sender,
                    target,
                    htl,
                } => {
                    let is_cached_contract = op_storage.ring.is_contract_cached(&key);
                    if !is_cached_contract {
                        tracing::warn!(
                            "Contract `{}` not found while processing a get request at node @ {}",
                            key,
                            target.peer
                        );

                        if htl == 0 {
                            tracing::warn!(
                                "The maximum HOPS number has been exceeded, sending the error \
                                 back to the node @ {}",
                                sender.peer
                            );

                            return build_op_result(
                                self.id,
                                None,
                                Some(GetMsg::ReturnGet {
                                    key,
                                    id,
                                    value: StoreResponse {
                                        state: None,
                                        contract: None,
                                    },
                                    sender: op_storage.ring.own_location(),
                                    target: sender, // return to requester
                                }),
                                self._ttl,
                            );
                        }

                        let new_htl = htl - 1;
                        let new_target =
                            op_storage.ring.closest_caching(&key, 1, &[sender.peer])[0];

                        continue_seeking(
                            conn_manager,
                            &new_target,
                            (GetMsg::SeekNode {
                                id,
                                key,
                                fetch_contract,
                                sender,
                                target: new_target,
                                htl: new_htl,
                            })
                            .into(),
                        )
                        .await?;

                        return_msg = None;
                        new_state = None;
                    } else if let ContractHandlerEvent::FetchResponse {
                        response: value,
                        key: returned_key,
                    } = op_storage
                        .notify_contract_handler(ContractHandlerEvent::FetchQuery {
                            key: key.clone(),
                            fetch_contract,
                        })
                        .await?
                    {
                        match check_contract_found(
                            key.clone(),
                            id,
                            fetch_contract,
                            &value,
                            returned_key.clone(),
                        ) {
                            Ok(_) => {}
                            Err(err) => return Err(err),
                        }

                        tracing::debug!("Contract {returned_key} found @ peer {}", target.peer);

                        match self.state {
                            Some(GetState::AwaitingResponse { .. }) => {
                                tracing::debug!(
                                    "Completed operation, Get response received for contract {key}"
                                );
                                // Completed op
                                new_state = None;
                                return_msg = None;
                            }
                            Some(GetState::ReceivedRequest) => {
                                tracing::debug!("Returning contract {} to {}", key, sender.peer);
                                new_state = None;
                                return_msg = Some(GetMsg::ReturnGet {
                                    id,
                                    key,
                                    value: value.unwrap(),
                                    sender: target,
                                    target: sender,
                                });
                            }
                            _ => return Err(OpError::InvalidStateTransition(self.id)),
                        };
                    } else {
                        return Err(OpError::InvalidStateTransition(id));
                    }
                }
                GetMsg::ReturnGet {
                    id,
                    key,
                    value:
                        StoreResponse {
                            state: None,
                            contract: None,
                        },
                    sender,
                    target,
                    ..
                } => {
                    let this_loc = target;
                    tracing::warn!(
                        "Neither contract or contract value for contract `{}` found at peer {}, \
                        retrying with other peers",
                        key,
                        sender.peer
                    );

                    match self.state {
                        Some(GetState::AwaitingResponse {
                            mut skip_list,
                            retries,
                            fetch_contract,
                            ..
                        }) => {
                            if retries < MAX_RETRIES {
                                // no response received from this peer, so skip it in the next iteration
                                skip_list.push(target.peer);
                                if let Some(target) = op_storage
                                    .ring
                                    .closest_caching(&key, 1, skip_list.as_slice())
                                    .into_iter()
                                    .next()
                                {
                                    return_msg = Some(GetMsg::SeekNode {
                                        id,
                                        key,
                                        target,
                                        sender: this_loc,
                                        fetch_contract,
                                        htl: MAX_GET_RETRY_HOPS,
                                    });
                                } else {
                                    return Err(RingError::NoCachingPeers(key).into());
                                }
                                new_state = Some(GetState::AwaitingResponse {
                                    skip_list,
                                    retries: retries + 1,
                                    fetch_contract,
                                });
                            } else {
                                tracing::error!(
                                    "Failed getting a value for contract {}, reached max retries",
                                    key
                                );
                                return Err(OpError::MaxRetriesExceeded(id, "get".to_owned()));
                            }
                        }
                        Some(GetState::ReceivedRequest) => {
                            tracing::debug!("Returning contract {} to {}", key, sender.peer);
                            new_state = None;
                            return_msg = Some(GetMsg::ReturnGet {
                                id,
                                key,
                                value: StoreResponse {
                                    state: None,
                                    contract: None,
                                },
                                sender,
                                target,
                            });
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };
                }
                GetMsg::ReturnGet {
                    key,
                    value:
                        StoreResponse {
                            state: Some(value),
                            contract,
                        },
                    id,
                    sender,
                    target,
                } => {
                    let require_contract = matches!(
                        self.state,
                        Some(GetState::AwaitingResponse {
                            fetch_contract: true,
                            ..
                        })
                    );

                    // received a response with a contract value
                    if require_contract {
                        if let Some(contract) = &contract {
                            // store contract first
                            op_storage
                                .notify_contract_handler(ContractHandlerEvent::Cache(
                                    contract.clone(),
                                ))
                                .await?;
                            let key = contract.key();
                            tracing::debug!("Contract `{}` successfully put", key);
                        } else {
                            // no contract, consider this like an error ignoring the incoming update value
                            tracing::warn!(
                                "Contract not received from peer {} while requested",
                                sender.peer
                            );

                            let op = GetOp {
                                id,
                                state: self.state,
                                _ttl: self._ttl,
                            };

                            op_storage
                                .notify_op_change(
                                    Message::from(GetMsg::ReturnGet {
                                        id,
                                        key,
                                        value: StoreResponse {
                                            state: None,
                                            contract: None,
                                        },
                                        sender,
                                        target,
                                    }),
                                    OpEnum::Get(op),
                                )
                                .await?;
                            return Err(OpError::StatePushed);
                        }
                    }

                    op_storage
                        .notify_contract_handler(ContractHandlerEvent::PushQuery {
                            key: key.clone(),
                            state: value.clone(),
                        })
                        .await?;

                    match self.state {
                        Some(GetState::AwaitingResponse { fetch_contract, .. }) => {
                            if fetch_contract && contract.is_none() {
                                tracing::error!(
                                    "Get response received for contract {key}, but the contract wasn't returned"
                                );
                                new_state = None;
                                return_msg = None;
                            } else {
                                tracing::debug!("Get response received for contract {}", key);
                                new_state = None;
                                return_msg = None;
                            }
                        }
                        Some(GetState::ReceivedRequest) => {
                            tracing::debug!("Returning contract {} to {}", key, sender.peer);
                            new_state = None;
                            return_msg = Some(GetMsg::ReturnGet {
                                id,
                                key,
                                value: StoreResponse {
                                    state: None,
                                    contract: None,
                                },
                                sender,
                                target,
                            });
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, self._ttl)
        })
    }
}

fn build_op_result<CErr: std::error::Error>(
    id: Transaction,
    state: Option<GetState>,
    msg: Option<GetMsg>,
    ttl: Duration,
) -> Result<OperationResult, OpError<CErr>> {
    let output_op = Some(GetOp {
        id,
        state,
        _ttl: ttl,
    });
    Ok(OperationResult {
        return_msg: msg.map(Message::from),
        state: output_op.map(OpEnum::Get),
    })
}

async fn continue_seeking<CErr: std::error::Error, CB: ConnectionBridge>(
    conn_manager: &mut CB,
    new_target: &PeerKeyLocation,
    retry_msg: Message,
) -> Result<(), OpError<CErr>> {
    tracing::info!(
        "Retrying to get the contract from node @ {}",
        new_target.peer
    );

    conn_manager.send(&new_target.peer, retry_msg).await?;

    Ok(())
}

fn check_contract_found<CErr: std::error::Error>(
    key: ContractKey,
    id: Transaction,
    fetch_contract: bool,
    value: &Result<StoreResponse, CErr>,
    returned_key: ContractKey,
) -> Result<(), OpError<CErr>> {
    if returned_key != key {
        // shouldn't be a reachable path
        tracing::error!(
            "contract retrieved ({}) and asked ({}) are not the same",
            returned_key,
            key
        );
        return Err(OpError::InvalidStateTransition(id));
    }

    match &value {
        Ok(StoreResponse {
            state: None,
            contract: None,
        }) => Err(OpError::ContractError(ContractError::ContractNotFound(key))),
        Ok(StoreResponse {
            state: Some(_),
            contract: None,
        }) if fetch_contract => Err(OpError::ContractError(ContractError::ContractNotFound(key))),
        _ => Ok(()),
    }
}

pub(crate) fn start_op(key: ContractKey, fetch_contract: bool, id: &PeerKey) -> GetOp {
    tracing::debug!(
        "Requesting get contract {} @ loc({})",
        key,
        Location::from(&key)
    );

    let id = Transaction::new(<GetMsg as TxType>::tx_type_id(), id);
    let state = Some(GetState::PrepareRequest {
        key,
        id,
        fetch_contract,
    });
    GetOp {
        id,
        state,
        _ttl: PEER_TIMEOUT,
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
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
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get<CErr>(
    op_storage: &OpManager<CErr>,
    get_op: GetOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let (target, id) = if let Some(GetState::PrepareRequest { key, id, .. }) = get_op.state.clone()
    {
        // the initial request must provide:
        // - a location in the network where the contract resides
        // - and the key of the contract value to get
        (
            op_storage
                .ring
                .closest_caching(&key, 1, &[])
                .into_iter()
                .next()
                .ok_or(RingError::EmptyRing)?,
            id,
        )
    } else {
        return Err(OpError::UnexpectedOpState);
    };
    tracing::debug!(
        "Preparing get contract request to {} (tx: {})",
        target.peer,
        id
    );

    match get_op.state.clone() {
        Some(GetState::PrepareRequest {
            fetch_contract,
            key,
            id,
            ..
        }) => {
            let new_state = Some(GetState::AwaitingResponse {
                skip_list: vec![],
                retries: 0,
                fetch_contract,
            });

            let msg = Some(GetMsg::RequestGet {
                key,
                target,
                id,
                fetch_contract,
            });

            let op = GetOp {
                id,
                state: new_state,
                _ttl: get_op._ttl,
            };

            op_storage
                .notify_op_change(msg.map(Message::from).unwrap(), OpEnum::Get(op))
                .await?;
        }
        _ => return Err(OpError::InvalidStateTransition(get_op.id)),
    }
    Ok(())
}

mod messages {
    use std::fmt::Display;

    use crate::{contract::StoreResponse, message::InnerMessage};

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
            fetch_contract: bool,
            target: PeerKeyLocation,
            sender: PeerKeyLocation,
            htl: usize,
        },
        ReturnGet {
            id: Transaction,
            key: ContractKey,
            value: StoreResponse,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
        },
    }

    impl InnerMessage for GetMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::FetchRouting { id, .. } => id,
                Self::RequestGet { id, .. } => id,
                Self::SeekNode { id, .. } => id,
                Self::ReturnGet { id, .. } => id,
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

        pub fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::FetchRouting { target, .. } => Some(target),
                Self::SeekNode { target, .. } => Some(target),
                Self::RequestGet { target, .. } => Some(target),
                Self::ReturnGet { target, .. } => Some(target),
            }
        }

        pub fn terminal(&self) -> bool {
            use GetMsg::*;
            matches!(self, ReturnGet { .. } | SeekNode { .. })
        }
    }

    impl Display for GetMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::FetchRouting { .. } => write!(f, "FetchRouting(id: {id})"),
                Self::RequestGet { .. } => write!(f, "RequestGet(id: {id})"),
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {id})"),
                Self::ReturnGet { .. } => write!(f, "ReturnGet(id: {id})"),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use locutus_runtime::{ContractContainer, WasmAPIVersion};
    use locutus_stdlib::client_api::ContractRequest;
    use std::collections::HashMap;

    use super::*;
    use crate::{
        node::test::{check_connectivity, NodeSpecification, SimNetwork},
        WrappedContract, WrappedState,
    };

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn successful_get_op_between_nodes() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 1usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let contract_val: WrappedState = gen.arbitrary()?;
        let key = contract.key().clone();
        let get_event = ContractRequest::Get {
            key: key.clone(),
            fetch_contract: true,
        }
        .into();
        let node_0 = NodeSpecification {
            owned_contracts: vec![],
            non_owned_contracts: vec![key.clone()],
            events_to_generate: HashMap::from_iter([(1, get_event)]),
            contract_subscribers: HashMap::new(),
        };

        let gw_0 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(WasmAPIVersion::V1(contract)),
                contract_val,
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
        };

        let get_specs = HashMap::from_iter([
            ("node-0".to_string(), node_0),
            ("gateway-0".to_string(), gw_0),
        ]);

        // establish network
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 2);
        sim_nodes.build_with_specs(get_specs).await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await?;

        // trigger get @ node-0, which does not own the contract
        sim_nodes
            .trigger_event("node-0", 1, Some(Duration::from_millis(100)))
            .await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(sim_nodes.has_got_contract("node-0", &key));
        Ok(())
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn contract_not_found() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 2usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let key = contract.key().clone();

        let get_event = ContractRequest::Get {
            key: key.clone(),
            fetch_contract: false,
        }
        .into();
        let node_1 = NodeSpecification {
            owned_contracts: vec![],
            non_owned_contracts: vec![key.clone()],
            events_to_generate: HashMap::from_iter([(1, get_event)]),
            contract_subscribers: HashMap::new(),
        };

        let get_specs = HashMap::from_iter([("node-1".to_string(), node_1)]);

        // establish network
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 2);
        sim_nodes.build_with_specs(get_specs).await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await?;

        // trigger get @ node-1, which does not own the contract
        sim_nodes
            .trigger_event("node-1", 1, Some(Duration::from_millis(100)))
            .await?;
        assert!(!sim_nodes.has_got_contract("node-1", &key));
        Ok(())
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn contract_found_after_retry() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 2usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let contract_val: WrappedState = gen.arbitrary()?;
        let key = contract.key().clone();

        let get_event = ContractRequest::Get {
            key: key.clone(),
            fetch_contract: false,
        }
        .into();

        let node_0 = NodeSpecification {
            owned_contracts: vec![],
            non_owned_contracts: vec![key.clone()],
            events_to_generate: HashMap::from_iter([(1, get_event)]),
            contract_subscribers: HashMap::new(),
        };

        let node_1 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(WasmAPIVersion::V1(contract)),
                contract_val,
            )],
            non_owned_contracts: vec![key.clone()],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
        };

        let gw_0 = NodeSpecification {
            owned_contracts: vec![],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
        };

        let get_specs = HashMap::from_iter([
            ("node-0".to_string(), node_0),
            ("node-1".to_string(), node_1),
            ("gateway-0".to_string(), gw_0),
        ]);

        // establish network
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 3);
        sim_nodes.build_with_specs(get_specs).await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await?;

        sim_nodes
            .trigger_event("node-0", 1, Some(Duration::from_millis(500)))
            .await?;
        assert!(sim_nodes.has_got_contract("node-0", &key));
        Ok(())
    }
}
