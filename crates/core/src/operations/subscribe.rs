use std::future::Future;
use std::pin::Pin;

use freenet_stdlib::prelude::*;
use futures::{future::BoxFuture, FutureExt};
use serde::{Deserialize, Serialize};

use super::{OpEnum, OpError, OpInitialization, OpOutcome, Operation, OperationResult};
use crate::{
    client_events::ClientId,
    contract::ContractError,
    message::{InnerMessage, Message, Transaction},
    node::{NetworkBridge, OpManager, PeerKey},
    ring::{Location, PeerKeyLocation, RingError},
};

pub(crate) use self::messages::SubscribeMsg;

const MAX_RETRIES: usize = 10;

pub(crate) struct SubscribeOp {
    id: Transaction,
    state: Option<SubscribeState>,
}

impl SubscribeOp {
    pub(super) fn outcome(&self) -> OpOutcome {
        OpOutcome::Irrelevant
    }

    pub(super) fn finalized(&self) -> bool {
        matches!(self.state, Some(SubscribeState::Completed))
    }

    pub(super) fn record_transfer(&mut self) {}
}

pub(crate) struct SubscribeResult {}

impl TryFrom<SubscribeOp> for SubscribeResult {
    type Error = OpError;

    fn try_from(value: SubscribeOp) -> Result<Self, Self::Error> {
        value
            .finalized()
            .then_some(SubscribeResult {})
            .ok_or(OpError::UnexpectedOpState)
    }
}

impl Operation for SubscribeOp {
    type Message = SubscribeMsg;
    type Result = SubscribeResult;

    fn load_or_init<'a>(
        op_storage: &'a OpManager,
        msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<OpInitialization<Self>, OpError>> {
        async move {
            let mut sender: Option<PeerKey> = None;
            if let Some(peer_key_loc) = msg.sender().cloned() {
                sender = Some(peer_key_loc.peer);
            };
            let id = *msg.id();

            match op_storage.pop(msg.id()) {
                Ok(Some(OpEnum::Subscribe(subscribe_op))) => {
                    // was an existing operation, the other peer messaged back
                    Ok(OpInitialization {
                        op: subscribe_op,
                        sender,
                    })
                }
                Ok(Some(op)) => {
                    let _ = op_storage.push(id, op).await;
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
        .boxed()
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message<'a, NB: NetworkBridge>(
        self,
        conn_manager: &'a mut NB,
        op_storage: &'a OpManager,
        input: &'a Self::Message,
        client_id: Option<ClientId>,
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
                    let sender = op_storage.ring.own_location();
                    new_state = self.state;
                    return_msg = Some(SubscribeMsg::SeekNode {
                        id: *id,
                        key: key.clone(),
                        target: *target,
                        subscriber: sender,
                        skip_list: vec![sender.peer],
                        htl: 0,
                    });
                }
                SubscribeMsg::SeekNode {
                    key,
                    id,
                    subscriber,
                    target,
                    skip_list,
                    htl,
                } => {
                    let sender = op_storage.ring.own_location();
                    let return_err = || -> OperationResult {
                        OperationResult {
                            return_msg: Some(Message::from(SubscribeMsg::ReturnSub {
                                key: key.clone(),
                                id: *id,
                                subscribed: false,
                                sender,
                                target: *subscriber,
                            })),
                            state: None,
                        }
                    };

                    if !op_storage.ring.is_contract_cached(key) {
                        tracing::debug!(tx = %id, "Contract {} not found at {}, trying other peer", key, target.peer);

                        let Some(new_target) = op_storage
                            .ring
                            .closest_caching(key, [&sender.peer].as_slice())
                        else {
                            tracing::warn!(tx = %id, "No peer found while trying getting contract {key}");
                            return Err(OpError::RingError(RingError::NoCachingPeers(key.clone())));
                        };
                        let new_htl = htl + 1;

                        if new_htl > MAX_RETRIES {
                            return Ok(return_err());
                        }

                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.push(target.peer);

                        tracing::debug!(tx = %id, "Forward request to peer: {}", new_target.peer);
                        // Retry seek node when the contract to subscribe has not been found in this node
                        conn_manager
                            .send(
                                &new_target.peer,
                                (SubscribeMsg::SeekNode {
                                    id: *id,
                                    key: key.clone(),
                                    subscriber: *subscriber,
                                    target: new_target,
                                    skip_list: new_skip_list.clone(),
                                    htl: new_htl,
                                })
                                .into(),
                            )
                            .await?;
                    } else if op_storage.ring.add_subscriber(key, *subscriber).is_err() {
                        // max number of subscribers for this contract reached
                        return Ok(return_err());
                    }

                    match self.state {
                        Some(SubscribeState::ReceivedRequest) => {
                            tracing::debug!(
                                tx = %id,
                                "Peer {} successfully subscribed to contract {key}",
                                subscriber.peer,
                            );
                            new_state = None;
                            // TODO review behaviour, if the contract is not cached should return subscribed false?
                            return_msg = Some(SubscribeMsg::ReturnSub {
                                sender: *target,
                                target: *subscriber,
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
                        "Contract `{key}` not found at potential subscription provider {}",
                        sender.peer
                    );
                    // will error out in case it has reached max number of retries
                    match self.state {
                        Some(SubscribeState::AwaitingResponse {
                            mut skip_list,
                            retries,
                            ..
                        }) => {
                            if retries < MAX_RETRIES {
                                skip_list.push(sender.peer);
                                if let Some(target) = op_storage
                                    .ring
                                    .closest_caching(key, skip_list.as_slice())
                                    .into_iter()
                                    .next()
                                {
                                    let subscriber = op_storage.ring.own_location();
                                    return_msg = Some(SubscribeMsg::SeekNode {
                                        id: *id,
                                        key: key.clone(),
                                        subscriber,
                                        target,
                                        skip_list: vec![target.peer],
                                        htl: 0,
                                    });
                                } else {
                                    return Err(RingError::NoCachingPeers(key.clone()).into());
                                }
                                new_state = Some(SubscribeState::AwaitingResponse {
                                    skip_list,
                                    retries: retries + 1,
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
                } => {
                    match self.state {
                        Some(SubscribeState::AwaitingResponse { .. }) => {
                            tracing::debug!(
                                tx = %id,
                                target = ?target.peer,
                                this = ?op_storage.ring.own_location().peer,
                                "Subscribed to `{key}` at provider {}", sender.peer
                            );
                            op_storage.ring.add_subscription(key.clone());
                            // todo: should inform back to the network event loop in case a client
                            // is waiting for response
                            let _ = client_id;
                            new_state = Some(SubscribeState::Completed);
                            return_msg = None;
                        }
                        _other => {
                            return Err(OpError::invalid_transition(self.id));
                        }
                    }
                }
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
        return_msg: msg.map(Message::from),
        state: output_op.map(OpEnum::Subscribe),
    })
}

pub(crate) fn start_op(key: ContractKey) -> SubscribeOp {
    let id = Transaction::new::<SubscribeMsg>();
    let state = Some(SubscribeState::PrepareRequest { id, key });
    SubscribeOp { id, state }
}

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
        skip_list: Vec<PeerKey>,
        retries: usize,
    },
    Completed,
}

/// Request to subscribe to value changes from a contract.
pub(crate) async fn request_subscribe(
    op_storage: &OpManager,
    sub_op: SubscribeOp,
    client_id: Option<ClientId>,
) -> Result<(), OpError> {
    let (target, _id) = if let Some(SubscribeState::PrepareRequest { id, key }) = &sub_op.state {
        if !op_storage.ring.is_contract_cached(key) {
            return Err(OpError::ContractError(ContractError::ContractNotFound(
                key.clone(),
            )));
        }
        const EMPTY: &[PeerKey] = &[];
        (
            op_storage
                .ring
                .closest_caching(key, EMPTY)
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
            });
            let msg = SubscribeMsg::RequestSub { id, key, target };
            let op = SubscribeOp {
                id,
                state: new_state,
            };
            op_storage
                .notify_op_change(Message::from(msg), OpEnum::Subscribe(op), client_id)
                .await?;
        }
        _ => return Err(OpError::invalid_transition(sub_op.id)),
    }

    Ok(())
}

mod messages {
    use crate::message::InnerMessage;
    use std::fmt::Display;

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
            skip_list: Vec<PeerKey>,
            htl: usize,
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

        fn target(&self) -> Option<&PeerKeyLocation> {
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
    use crate::node::tests::{NodeSpecification, SimNetwork};

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
            ("node-0".into(), first_node),
            ("node-1".into(), second_node),
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
        sim_nw.check_connectivity(Duration::from_secs(3)).await?;
        sim_nw
            .trigger_event("node-1", 1, Some(Duration::from_secs(1)))
            .await?;
        assert!(sim_nw.has_got_contract("node-1", &contract_key));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(sim_nw.is_subscribed_to_contract("node-1", &contract_key));
        Ok(())
    }
}
