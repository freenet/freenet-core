use std::future::Future;
use std::pin::Pin;

use freenet_stdlib::{
    client_api::{ErrorKind, HostResponse},
    prelude::*,
};
use serde::{Deserialize, Serialize};

use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::{
    client_events::HostResult,
    contract::ContractError,
    message::{InnerMessage, NetMessage, Transaction},
    node::{NetworkBridge, OpManager, PeerId},
    ring::{Location, PeerKeyLocation, RingError},
};

pub(crate) use self::messages::SubscribeMsg;

const MAX_RETRIES: usize = 10;

#[derive(Debug)]
enum SubscribeState {
    /// Prepare the request to subscribe.
    PrepareRequest {
        id: Transaction,
        key: ContractKey,
    },
    /// Received a request to subscribe to this network.
    ReceivedRequest,
    /// Awaitinh response from petition.
    AwaitingResponse {
        skip_list: Vec<PeerId>,
        retries: usize,
        upstream_subscriber: Option<PeerKeyLocation>,
        current_hop: usize,
    },
    Completed {},
}

pub(crate) struct SubscribeResult {}

impl TryFrom<SubscribeOp> for SubscribeResult {
    type Error = OpError;

    fn try_from(value: SubscribeOp) -> Result<Self, Self::Error> {
        if let Some(SubscribeState::Completed {}) = value.state {
            Ok(SubscribeResult {})
        } else {
            Err(OpError::UnexpectedOpState)
        }
    }
}

pub(crate) fn start_op(key: ContractKey) -> SubscribeOp {
    let id = Transaction::new::<SubscribeMsg>();
    let state = Some(SubscribeState::PrepareRequest { id, key });
    SubscribeOp { id, state }
}

/// Request to subscribe to value changes from a contract.
pub(crate) async fn request_subscribe(
    op_manager: &OpManager,
    sub_op: SubscribeOp,
) -> Result<(), OpError> {
    let (target, _id) = if let Some(SubscribeState::PrepareRequest { id, key }) = &sub_op.state {
        if !super::has_contract(op_manager, key.clone()).await? {
            return Err(OpError::ContractError(ContractError::ContractNotFound(
                key.clone(),
            )));
        }
        const EMPTY: &[PeerId] = &[];
        (
            op_manager
                .ring
                .closest_potentially_caching(key, EMPTY)
                .into_iter()
                .next()
                .ok_or_else(|| RingError::NoCachingPeers(key.clone()))?,
            *id,
        )
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    match sub_op.state {
        Some(SubscribeState::PrepareRequest { id, key, .. }) => {
            let new_state = Some(SubscribeState::AwaitingResponse {
                skip_list: vec![],
                retries: 0,
                current_hop: op_manager.ring.max_hops_to_live,
                upstream_subscriber: None,
            });
            let msg = SubscribeMsg::RequestSub { id, key, target };
            let op = SubscribeOp {
                id,
                state: new_state,
            };
            op_manager
                .notify_op_change(NetMessage::from(msg), OpEnum::Subscribe(op))
                .await?;
        }
        _ => return Err(OpError::invalid_transition(sub_op.id)),
    }

    Ok(())
}

pub(crate) struct SubscribeOp {
    pub id: Transaction,
    state: Option<SubscribeState>,
}

impl SubscribeOp {
    pub(super) fn outcome(&self) -> OpOutcome {
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, Some(SubscribeState::Completed { .. }))
    }

    pub(super) fn to_host_result(&self) -> HostResult {
        if let Some(SubscribeState::Completed {}) = self.state {
            Ok(HostResponse::Ok)
        } else {
            Err(ErrorKind::OperationError {
                cause: "subscribe didn't finish successfully".into(),
            }
            .into())
        }
    }
}

impl Operation for SubscribeOp {
    type Message = SubscribeMsg;
    type Result = SubscribeResult;

    async fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> Result<OpInitialization<Self>, OpError> {
        let mut sender: Option<PeerId> = None;
        if let Some(peer_key_loc) = msg.sender().cloned() {
            sender = Some(peer_key_loc.peer);
        };
        let id = *msg.id();

        match op_manager.pop(msg.id()) {
            Ok(Some(OpEnum::Subscribe(subscribe_op))) => {
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization {
                    op: subscribe_op,
                    sender,
                })
            }
            Ok(Some(op)) => {
                let _ = op_manager.push(id, op).await;
                Err(OpError::OpNotPresent(id))
            }
            Ok(None) => {
                // new request to subcribe to a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(SubscribeState::ReceivedRequest),
                        id,
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

            match input {
                SubscribeMsg::RequestSub { id, key, target } => {
                    // fast tracked from the request_sub func
                    debug_assert!(matches!(
                        self.state,
                        Some(SubscribeState::AwaitingResponse { .. })
                    ));
                    let sender = op_manager.ring.own_location();
                    new_state = self.state;
                    return_msg = Some(SubscribeMsg::SeekNode {
                        id: *id,
                        key: key.clone(),
                        target: target.clone(),
                        subscriber: sender.clone(),
                        skip_list: vec![sender.peer],
                        htl: op_manager.ring.max_hops_to_live,
                        retries: 0,
                    });
                }
                SubscribeMsg::SeekNode {
                    key,
                    id,
                    subscriber,
                    target,
                    skip_list,
                    htl,
                    retries,
                } => {
                    let this_peer = op_manager.ring.own_location();
                    let return_not_subbed = || -> OperationResult {
                        OperationResult {
                            return_msg: Some(NetMessage::from(SubscribeMsg::ReturnSub {
                                key: key.clone(),
                                id: *id,
                                subscribed: false,
                                sender: this_peer.clone(),
                                target: subscriber.clone(),
                            })),
                            state: None,
                        }
                    };

                    if !super::has_contract(op_manager, key.clone()).await? {
                        tracing::debug!(tx = %id, %key, "Contract not found, trying other peer");

                        let Some(new_target) = op_manager
                            .ring
                            .closest_potentially_caching(key, skip_list.as_slice())
                        else {
                            tracing::warn!(tx = %id, %key, "No target peer found while trying getting contract");
                            return Ok(return_not_subbed());
                        };
                        let new_htl = htl - 1;

                        if new_htl == 0 {
                            tracing::debug!(tx = %id, %key, "Max number of hops reached while trying to get contract");
                            return Ok(return_not_subbed());
                        }

                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.push(target.peer.clone());

                        tracing::debug!(tx = %id, new_target = %new_target.peer, "Forward request to peer");
                        // Retry seek node when the contract to subscribe has not been found in this node
                        return build_op_result(
                            *id,
                            Some(SubscribeState::AwaitingResponse {
                                skip_list: new_skip_list.clone(),
                                retries: *retries,
                                current_hop: new_htl,
                                upstream_subscriber: Some(subscriber.clone()),
                            }),
                            (SubscribeMsg::SeekNode {
                                id: *id,
                                key: key.clone(),
                                subscriber: this_peer,
                                target: new_target,
                                skip_list: new_skip_list,
                                htl: new_htl,
                                retries: *retries,
                            })
                            .into(),
                        );
                    }

                    if op_manager
                        .ring
                        .add_subscriber(key, subscriber.clone())
                        .is_err()
                    {
                        tracing::debug!(tx = %id, %key, "Max number of subscribers reached for contract");
                        // max number of subscribers for this contract reached
                        return Ok(return_not_subbed());
                    }

                    match self.state {
                        Some(SubscribeState::ReceivedRequest) => {
                            tracing::info!(
                                tx = %id,
                                %key,
                                subscriber = % subscriber.peer,
                                "Peer successfully subscribed to contract",
                            );
                            new_state = None;
                            return_msg = Some(SubscribeMsg::ReturnSub {
                                sender: target.clone(),
                                target: subscriber.clone(),
                                id: *id,
                                key: key.clone(),
                                subscribed: true,
                            });
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    }
                }
                SubscribeMsg::ReturnSub {
                    subscribed: false,
                    key,
                    sender,
                    target: _,
                    id,
                } => {
                    tracing::warn!(
                        tx = %id,
                        %key,
                        potential_provider = %sender.peer,
                        "Contract not found at potential subscription provider",
                    );
                    // will error out in case it has reached max number of retries
                    match self.state {
                        Some(SubscribeState::AwaitingResponse {
                            mut skip_list,
                            retries,
                            upstream_subscriber,
                            current_hop,
                        }) => {
                            if retries < MAX_RETRIES {
                                skip_list.push(sender.peer.clone());
                                if let Some(target) = op_manager
                                    .ring
                                    .closest_potentially_caching(key, skip_list.as_slice())
                                    .into_iter()
                                    .next()
                                {
                                    let subscriber = op_manager.ring.own_location();
                                    return_msg = Some(SubscribeMsg::SeekNode {
                                        id: *id,
                                        key: key.clone(),
                                        subscriber,
                                        target,
                                        skip_list: skip_list.clone(),
                                        htl: current_hop,
                                        retries: retries + 1,
                                    });
                                } else {
                                    return Err(RingError::NoCachingPeers(key.clone()).into());
                                }
                                new_state = Some(SubscribeState::AwaitingResponse {
                                    skip_list,
                                    retries: retries + 1,
                                    upstream_subscriber,
                                    current_hop,
                                });
                            } else {
                                return Err(OpError::MaxRetriesExceeded(
                                    *id,
                                    id.transaction_type(),
                                ));
                            }
                        }
                        _ => return Err(OpError::invalid_transition(self.id)),
                    }
                }
                SubscribeMsg::ReturnSub {
                    subscribed: true,
                    key,
                    sender,
                    id,
                    target,
                    ..
                } => match self.state {
                    Some(SubscribeState::AwaitingResponse {
                        upstream_subscriber,
                        ..
                    }) => {
                        tracing::info!(
                            tx = %id,
                            %key,
                            this_peer = %target.peer,
                            provider = %sender.peer,
                            "Subscribed to contract"
                        );
                        op_manager.ring.register_subscription(key, sender.clone());

                        new_state = Some(SubscribeState::Completed {});
                        if let Some(upstream_subscriber) = upstream_subscriber {
                            return_msg = Some(SubscribeMsg::ReturnSub {
                                id: *id,
                                key: key.clone(),
                                sender: target.clone(),
                                target: upstream_subscriber,
                                subscribed: true,
                            });
                        } else {
                            return_msg = None;
                        }
                    }
                    _other => {
                        return Err(OpError::invalid_transition(self.id));
                    }
                },
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg)
        })
    }
}

fn build_op_result(
    id: Transaction,
    state: Option<SubscribeState>,
    msg: Option<SubscribeMsg>,
) -> Result<OperationResult, OpError> {
    let output_op = state.map(|state| SubscribeOp {
        id,
        state: Some(state),
    });
    Ok(OperationResult {
        return_msg: msg.map(NetMessage::from),
        state: output_op.map(OpEnum::Subscribe),
    })
}

mod messages {
    use std::{borrow::Borrow, fmt::Display};

    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
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
            skip_list: Vec<PeerId>,
            htl: usize,
            retries: usize,
        },
        ReturnSub {
            id: Transaction,
            key: ContractKey,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            subscribed: bool,
        },
    }

    impl InnerMessage for SubscribeMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                Self::FetchRouting { id, .. } => id,
                Self::RequestSub { id, .. } => id,
                Self::ReturnSub { id, .. } => id,
            }
        }

        fn target(&self) -> Option<impl Borrow<PeerKeyLocation>> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::ReturnSub { target, .. } => Some(target),
                _ => None,
            }
        }

        fn terminal(&self) -> bool {
            use SubscribeMsg::*;
            matches!(self, ReturnSub { .. } | SeekNode { .. })
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::SeekNode { key, .. } => Some(Location::from(key.id())),
                Self::RequestSub { key, .. } => Some(Location::from(key.id())),
                Self::ReturnSub { key, .. } => Some(Location::from(key.id())),
                _ => None,
            }
        }
    }

    impl SubscribeMsg {
        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::ReturnSub { sender, .. } => Some(sender),
                _ => None,
            }
        }
    }

    impl Display for SubscribeMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {id})"),
                Self::FetchRouting { .. } => write!(f, "FetchRouting(id: {id})"),
                Self::RequestSub { .. } => write!(f, "RequestSub(id: {id})"),
                Self::ReturnSub { .. } => write!(f, "ReturnSub(id: {id})"),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, time::Duration};

    use freenet_stdlib::client_api::ContractRequest;

    use super::*;
    use crate::node::testing_impl::{NodeSpecification, SimNetwork};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_subscribe_op_between_nodes() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 4usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1kb();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let contract_val: WrappedState = gen.arbitrary()?;
        let contract_key: ContractKey = contract.key().clone();

        let event = ContractRequest::Subscribe {
            key: contract_key.clone(),
            summary: None,
        }
        .into();
        let first_node = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract)),
                contract_val,
                true,
            )],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
        };
        let second_node = NodeSpecification {
            owned_contracts: Vec::new(),
            events_to_generate: HashMap::from_iter([(1, event)]),
            contract_subscribers: HashMap::new(),
        };

        let subscribe_specs = HashMap::from_iter([
            ("node-1".into(), first_node),
            ("node-2".into(), second_node),
        ]);
        let mut sim_nw = SimNetwork::new(
            "successful_subscribe_op_between_nodes",
            NUM_GW,
            NUM_NODES,
            4,
            3,
            5,
            2,
        )
        .await;
        sim_nw.start_with_spec(subscribe_specs).await;
        sim_nw.check_connectivity(Duration::from_secs(3))?;
        sim_nw
            .trigger_event("node-2", 1, Some(Duration::from_secs(1)))
            .await?;
        assert!(sim_nw.has_got_contract("node-2", &contract_key));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(sim_nw.is_subscribed_to_contract("node-2", &contract_key));
        Ok(())
    }
}
