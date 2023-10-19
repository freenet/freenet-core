use std::pin::Pin;
use std::time::Duration;
use std::{future::Future, time::Instant};

use freenet_stdlib::prelude::*;

use crate::{
    client_events::ClientId,
    config::PEER_TIMEOUT,
    contract::{ContractError, ContractHandlerEvent, StoreResponse},
    message::{InnerMessage, Message, Transaction},
    node::{ConnectionBridge, OpManager, PeerKey},
    operations::{op_trait::Operation, OpInitialization},
    ring::{Location, PeerKeyLocation, RingError},
    DynError,
};

use super::{OpEnum, OpError, OpOutcome, OperationResult};

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
    result: Option<GetResult>,
    stats: Option<GetStats>,
    _ttl: Duration,
}

struct GetStats {
    caching_peer: Option<PeerKeyLocation>,
    contract_location: Location,
    /// (start, end)
    first_response_time: Option<(Instant, Option<Instant>)>,
    /// (start, end)
    transfer_time: Option<(Instant, Option<Instant>)>,
    step: RecordingStats,
}

/// While timing, at what particular step we are now.
#[derive(Clone, Copy, Default)]
enum RecordingStats {
    #[default]
    Uninitialized,
    InitGet,
    TransferNotStarted,
    TransferStarted,
    Completed,
}

impl GetOp {
    pub(super) fn outcome(&self) -> OpOutcome {
        if let Some((
            GetResult { state, contract },
            GetStats {
                caching_peer: Some(target_peer),
                contract_location,
                first_response_time: Some((response_start, Some(response_end))),
                transfer_time: Some((transfer_start, Some(transfer_end))),
                ..
            },
        )) = self.result.as_ref().zip(self.stats.as_ref())
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
        self.stats
            .as_ref()
            .map(|s| s.transfer_time.is_some())
            .unwrap_or(false)
    }

    pub(super) fn record_transfer(&mut self) {
        if let Some(stats) = self.stats.as_mut() {
            match stats.step {
                RecordingStats::Uninitialized => {
                    stats.first_response_time = Some((Instant::now(), None));
                    stats.step = RecordingStats::InitGet;
                }
                RecordingStats::InitGet => {
                    if let Some((_, e)) = stats.first_response_time.as_mut() {
                        *e = Some(Instant::now());
                    }
                    stats.step = RecordingStats::TransferNotStarted;
                }
                RecordingStats::TransferNotStarted => {
                    stats.transfer_time = Some((Instant::now(), None));
                    stats.step = RecordingStats::TransferStarted;
                }
                RecordingStats::TransferStarted => {
                    if let Some((_, e)) = stats.transfer_time.as_mut() {
                        *e = Some(Instant::now());
                    }
                    stats.step = RecordingStats::Completed;
                }
                RecordingStats::Completed => {}
            }
        }
    }
}

pub(crate) struct GetResult {
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

impl Operation for GetOp {
    type Message = GetMsg;
    type Result = GetResult;

    fn load_or_init(
        op_storage: &OpManager,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
        let mut sender: Option<PeerKey> = None;
        if let Some(peer_key_loc) = msg.sender().cloned() {
            sender = Some(peer_key_loc.peer);
        };
        let tx = *msg.id();
        let result = match op_storage.pop(msg.id()) {
            Some(OpEnum::Get(get_op)) => {
                Ok(OpInitialization { op: get_op, sender })
                // was an existing operation, other peer messaged back
            }
            Some(_) => return Err(OpError::OpNotPresent(tx)),
            None => {
                // new request to get a value for a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(GetState::ReceivedRequest),
                        id: tx,
                        result: None,
                        stats: None, // don't care about stats in target peers
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

    fn process_message<'a, CB: ConnectionBridge>(
        self,
        conn_manager: &'a mut CB,
        op_storage: &'a OpManager,
        input: Self::Message,
        client_id: Option<ClientId>,
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
                } => {
                    // fast tracked from the request_get func
                    debug_assert!(matches!(
                        self.state,
                        Some(GetState::AwaitingResponse { .. })
                    ));
                    tracing::debug!(tx = %id, "Seek contract {} @ {}", key, target.peer);
                    new_state = self.state;
                    stats = Some(GetStats {
                        contract_location: Location::from(&key),
                        caching_peer: None,
                        transfer_time: None,
                        first_response_time: None,
                        step: Default::default(),
                    });
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
                    if let Some(s) = stats.as_mut() {
                        s.caching_peer = Some(target);
                    }

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
                                None,
                                stats,
                                self._ttl,
                            );
                        }

                        let new_htl = htl - 1;
                        let Some(new_target) =
                            op_storage.ring.closest_caching(&key, &[sender.peer])
                        else {
                            tracing::warn!("no peer found while trying getting contract {key}");
                            return Err(OpError::RingError(RingError::NoCachingPeers(key)));
                        };

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
                    } else if let ContractHandlerEvent::GetResponse {
                        key: returned_key,
                        response: value,
                    } = op_storage
                        .notify_contract_handler(
                            ContractHandlerEvent::GetQuery {
                                key: key.clone(),
                                fetch_contract,
                            },
                            client_id,
                        )
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
                                let value = match value {
                                    Ok(res) => res,
                                    Err(err) => {
                                        tracing::error!("error: {err}");
                                        return Err(OpError::ExecutorError(err));
                                    }
                                };
                                return_msg = Some(GetMsg::ReturnGet {
                                    id,
                                    key,
                                    value,
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
                            // todo: register in the stats for the outcome of the op that failed to get a response from this peer
                            if retries < MAX_RETRIES {
                                // no response received from this peer, so skip it in the next iteration
                                skip_list.push(target.peer);
                                if let Some(target) = op_storage
                                    .ring
                                    .closest_caching(&key, skip_list.as_slice())
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
                                return Err(OpError::MaxRetriesExceeded(id, id.tx_type()));
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
                    id,
                    key,
                    value:
                        StoreResponse {
                            state: Some(value),
                            contract,
                        },
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
                                .notify_contract_handler(
                                    ContractHandlerEvent::Cache(contract.clone()),
                                    client_id,
                                )
                                .await?;
                            let key = contract.key();
                            tracing::debug!("Contract `{}` successfully cached", key);
                        } else {
                            // no contract, consider this like an error ignoring the incoming update value
                            tracing::warn!(
                                "Contract not received from peer {} while requested",
                                sender.peer
                            );

                            let op = GetOp {
                                id,
                                state: self.state,
                                result: None,
                                _ttl: self._ttl,
                                stats,
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
                                    None,
                                )
                                .await?;
                            return Err(OpError::StatePushed);
                        }
                    }

                    let parameters = contract.as_ref().map(|c| c.params());
                    op_storage
                        .notify_contract_handler(
                            ContractHandlerEvent::PutQuery {
                                key: key.clone(),
                                state: value.clone(),
                                parameters,
                            },
                            client_id,
                        )
                        .await?;

                    match self.state {
                        Some(GetState::AwaitingResponse { fetch_contract, .. }) => {
                            if fetch_contract && contract.is_none() {
                                tracing::error!(
                                    "Get response received for contract {key}, but the contract wasn't returned"
                                );
                                new_state = None;
                                return_msg = None;
                                result = Some(GetResult {
                                    state: value.clone(),
                                    contract,
                                });
                            } else {
                                tracing::debug!("Get response received for contract {}", key);
                                new_state = None;
                                return_msg = None;
                                result = Some(GetResult {
                                    state: value.clone(),
                                    contract,
                                });
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

            build_op_result(self.id, new_state, return_msg, result, stats, self._ttl)
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<GetState>,
    msg: Option<GetMsg>,
    result: Option<GetResult>,
    stats: Option<GetStats>,
    ttl: Duration,
) -> Result<OperationResult, OpError> {
    let output_op = Some(GetOp {
        id,
        state,
        result,
        stats,
        _ttl: ttl,
    });
    Ok(OperationResult {
        return_msg: msg.map(Message::from),
        state: output_op.map(OpEnum::Get),
    })
}

async fn continue_seeking<CB: ConnectionBridge>(
    conn_manager: &mut CB,
    new_target: &PeerKeyLocation,
    retry_msg: Message,
) -> Result<(), OpError> {
    tracing::info!(
        "Retrying to get the contract from node @ {}",
        new_target.peer
    );

    conn_manager.send(&new_target.peer, retry_msg).await?;

    Ok(())
}

fn check_contract_found(
    key: ContractKey,
    id: Transaction,
    fetch_contract: bool,
    value: &Result<StoreResponse, DynError>,
    returned_key: ContractKey,
) -> Result<(), OpError> {
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

pub(crate) fn start_op(key: ContractKey, fetch_contract: bool) -> GetOp {
    let contract_location = Location::from(&key);
    tracing::debug!("Requesting get contract {} @ loc({contract_location})", key,);

    let id = Transaction::new::<GetMsg>();
    let state = Some(GetState::PrepareRequest {
        key,
        id,
        fetch_contract,
    });
    GetOp {
        id,
        state,
        result: None,
        stats: Some(GetStats {
            contract_location,
            caching_peer: None,
            transfer_time: None,
            first_response_time: None,
            step: Default::default(),
        }),
        _ttl: PEER_TIMEOUT,
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
}

/// Request to get the current value from a contract.
pub(crate) async fn request_get(
    op_storage: &OpManager,
    get_op: GetOp,
    client_id: Option<ClientId>,
) -> Result<(), OpError> {
    let (target, id) = if let Some(GetState::PrepareRequest { key, id, .. }) = &get_op.state {
        // the initial request must provide:
        // - a location in the network where the contract resides
        // - and the key of the contract value to get
        (
            op_storage
                .ring
                .closest_caching(key, &[])
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
        "Preparing get contract request to {}",
        target.peer,
    );

    match get_op.state {
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

            let msg = GetMsg::RequestGet {
                id,
                key,
                target,
                fetch_contract,
            };

            let op = GetOp {
                id,
                state: new_state,
                result: None,
                stats: get_op.stats.map(|mut s| {
                    s.caching_peer = Some(target);
                    s
                }),
                _ttl: get_op._ttl,
            };

            op_storage
                .notify_op_change(Message::from(msg), OpEnum::Get(op), client_id)
                .await?;
        }
        _ => return Err(OpError::InvalidStateTransition(get_op.id)),
    }
    Ok(())
}

mod messages {
    use std::fmt::Display;

    use serde::{Deserialize, Serialize};

    use crate::{contract::StoreResponse, message::InnerMessage};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum GetMsg {
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
                Self::RequestGet { id, .. } => id,
                Self::SeekNode { id, .. } => id,
                Self::ReturnGet { id, .. } => id,
            }
        }

        fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::RequestGet { target, .. } => Some(target),
                Self::ReturnGet { target, .. } => Some(target),
            }
        }

        fn terminal(&self) -> bool {
            use GetMsg::*;
            matches!(self, ReturnGet { .. })
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

#[cfg(test)]
mod test {
    use freenet_stdlib::client_api::ContractRequest;
    use std::collections::HashMap;

    use super::*;
    use crate::node::tests::{NodeSpecification, SimNetwork};

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
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract)),
                contract_val,
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
        };

        let get_specs = HashMap::from_iter([("node-0".into(), node_0), ("gateway-0".into(), gw_0)]);

        // establish network
        let mut sim_nw = SimNetwork::new(
            "successful_get_op_between_nodes",
            NUM_GW,
            NUM_NODES,
            3,
            2,
            4,
            2,
        )
        .await;
        sim_nw.start_with_spec(get_specs).await;
        sim_nw.check_connectivity(Duration::from_secs(3)).await?;

        // trigger get @ node-0, which does not own the contract
        sim_nw
            .trigger_event(&"node-0".into(), 1, Some(Duration::from_millis(100)))
            .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(sim_nw.has_got_contract(&"node-0".into(), &key));
        Ok(())
    }

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

        let get_specs = HashMap::from_iter([("node-1".into(), node_1)]);

        // establish network
        let mut sim_nw =
            SimNetwork::new("get_contract_not_found", NUM_GW, NUM_NODES, 3, 2, 4, 2).await;
        sim_nw.start_with_spec(get_specs).await;
        sim_nw.check_connectivity(Duration::from_secs(3)).await?;

        // trigger get @ node-1, which does not own the contract
        sim_nw
            .trigger_event(&"node-1".into(), 1, Some(Duration::from_millis(100)))
            .await?;
        assert!(!sim_nw.has_got_contract(&"node-1".into(), &key));
        Ok(())
    }

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
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract)),
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
            ("node-0".into(), node_0),
            ("node-1".into(), node_1),
            ("gateway-0".into(), gw_0),
        ]);

        // establish network
        let mut sim_nw = SimNetwork::new(
            "get_contract_found_after_retry",
            NUM_GW,
            NUM_NODES,
            3,
            2,
            4,
            3,
        )
        .await;
        sim_nw.start_with_spec(get_specs).await;
        sim_nw.check_connectivity(Duration::from_secs(3)).await?;

        sim_nw
            .trigger_event(&"node-0".into(), 1, Some(Duration::from_millis(500)))
            .await?;
        assert!(sim_nw.has_got_contract(&"node-0".into(), &key));
        Ok(())
    }
}
