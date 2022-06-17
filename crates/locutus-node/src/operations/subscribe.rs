use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use locutus_runtime::prelude::*;
use serde::{Deserialize, Serialize};

use crate::operations::op_trait::Operation;
use crate::operations::OpInitialization;
use crate::{
    config::PEER_TIMEOUT,
    contract::ContractError,
    message::{Message, Transaction, TxType},
    node::{ConnectionBridge, OpManager, PeerKey},
    ring::{PeerKeyLocation, RingError},
};

use super::{OpEnum, OpError, OperationResult};

pub(crate) use self::messages::SubscribeMsg;

const MAX_RETRIES: usize = 10;

pub(crate) struct SubscribeOp {
    id: Transaction,
    state: Option<SubscribeState>,
    _ttl: Duration,
}

impl<CErr, CB: ConnectionBridge> Operation<CErr, CB> for SubscribeOp
where
    CErr: std::error::Error + Send,
{
    type Message = SubscribeMsg;
    type Error = OpError<CErr>;

    fn load_or_init(
        op_storage: &OpManager<CErr>,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError<CErr>> {
        let mut sender: Option<PeerKey> = None;
        if let Some(peer_key_loc) = msg.sender().cloned() {
            sender = Some(peer_key_loc.peer);
        };
        let id = *msg.id();

        let result = match op_storage.pop(msg.id()) {
            Some(OpEnum::Subscribe(subscribe_op)) => {
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization {
                    op: subscribe_op,
                    sender,
                })
            }
            Some(_) => return Err(OpError::OpNotPresent(id)),
            None => {
                // new request to subcribe to a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(SubscribeState::ReceivedRequest),
                        id,
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
                SubscribeMsg::RequestSub { id, key, target } => {
                    // fast tracked from the request_sub func
                    debug_assert!(matches!(
                        self.state,
                        Some(SubscribeState::AwaitingResponse { .. })
                    ));
                    let sender = op_storage.ring.own_location();
                    new_state = self.state;
                    return_msg = Some(SubscribeMsg::SeekNode {
                        id,
                        key,
                        target,
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
                                key,
                                id,
                                subscribed: false,
                                sender,
                                target: subscriber,
                            })),
                            state: None,
                        }
                    };

                    if !op_storage.ring.is_contract_cached(&key) {
                        log::info!("Contract {} not found while processing info", key);
                        log::info!("Trying to found the contract from another node");

                        let new_target =
                            op_storage.ring.closest_caching(&key, 1, &[sender.peer])[0];
                        let new_htl = htl + 1;

                        if new_htl > MAX_RETRIES {
                            return Ok(return_err());
                        }

                        let mut new_skip_list = skip_list.clone();
                        new_skip_list.push(target.peer);

                        // Retry seek node when the contract to subscribe has not been found in this node
                        conn_manager
                            .send(
                                &new_target.peer,
                                (SubscribeMsg::SeekNode {
                                    id,
                                    key,
                                    subscriber,
                                    target: new_target,
                                    skip_list: new_skip_list.clone(),
                                    htl: new_htl,
                                })
                                .into(),
                            )
                            .await?;
                    } else if op_storage.ring.add_subscriber(key, subscriber).is_err() {
                        // max number of subscribers for this contract reached
                        return Ok(return_err());
                    }

                    match self.state {
                        Some(SubscribeState::ReceivedRequest) => {
                            log::info!(
                                "Peer {} successfully subscribed to contract {}",
                                subscriber.peer,
                                key
                            );
                            new_state = Some(SubscribeState::Completed);
                            // TODO review behaviour, if the contract is not cached should return subscribed false?
                            return_msg = Some(SubscribeMsg::ReturnSub {
                                sender: target,
                                target: subscriber,
                                id,
                                key,
                                subscribed: true,
                            });
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }
                }
                SubscribeMsg::ReturnSub {
                    subscribed: false,
                    key,
                    sender,
                    target: _,
                    id,
                } => {
                    log::warn!(
                        "Contract `{}` not found at potential subscription provider {}",
                        key,
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
                                    .closest_caching(&key, 1, skip_list.as_slice())
                                    .into_iter()
                                    .next()
                                {
                                    let subscriber = op_storage.ring.own_location();
                                    return_msg = Some(SubscribeMsg::SeekNode {
                                        id,
                                        key,
                                        subscriber,
                                        target,
                                        skip_list: vec![target.peer],
                                        htl: 0,
                                    });
                                } else {
                                    return Err(RingError::NoCachingPeers(key).into());
                                }
                                new_state = Some(SubscribeState::AwaitingResponse {
                                    skip_list,
                                    retries: retries + 1,
                                });
                            } else {
                                return Err(OpError::MaxRetriesExceeded(id, "sub".to_owned()));
                            }
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }
                }
                SubscribeMsg::ReturnSub {
                    subscribed: true,
                    key,
                    sender,
                    target: _,
                    id: _,
                } => {
                    log::warn!(
                        "Subscribed to `{}` not found at potential subscription provider {}",
                        key,
                        sender.peer
                    );
                    op_storage.ring.add_subscription(key);

                    match self.state {
                        Some(SubscribeState::AwaitingResponse { .. }) => {
                            new_state = None;
                            return_msg = None;
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    }
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, self._ttl)
        })
    }
}

fn build_op_result<CErr: std::error::Error>(
    id: Transaction,
    state: Option<SubscribeState>,
    msg: Option<SubscribeMsg>,
    ttl: Duration,
) -> Result<OperationResult, OpError<CErr>> {
    let output_op = Some(SubscribeOp {
        id,
        state,
        _ttl: ttl,
    });
    Ok(OperationResult {
        return_msg: msg.map(Message::from),
        state: output_op.map(OpEnum::Subscribe),
    })
}

pub(crate) fn start_op(key: ContractKey, peer: &PeerKey) -> SubscribeOp {
    let id = Transaction::new(<SubscribeMsg as TxType>::tx_type_id(), peer);
    let state = Some(SubscribeState::PrepareRequest { id, key });
    SubscribeOp {
        id,
        state,
        _ttl: PEER_TIMEOUT,
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
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
pub(crate) async fn request_subscribe<CErr>(
    op_storage: &OpManager<CErr>,
    sub_op: SubscribeOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let (target, _id) =
        if let Some(SubscribeState::PrepareRequest { id, key }) = sub_op.state.clone() {
            if !op_storage.ring.is_contract_cached(&key) {
                return Err(OpError::ContractError(ContractError::ContractNotFound(key)));
            }
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

    match sub_op.state.clone() {
        Some(SubscribeState::PrepareRequest { id, key, .. }) => {
            let new_state = Some(SubscribeState::AwaitingResponse {
                skip_list: vec![],
                retries: 0,
            });
            let msg = Some(SubscribeMsg::RequestSub { id, key, target });
            let op = SubscribeOp {
                id,
                state: new_state,
                _ttl: sub_op._ttl,
            };
            op_storage
                .notify_op_change(msg.map(Message::from).unwrap(), OpEnum::Subscribe(op))
                .await?;
        }
        _ => return Err(OpError::InvalidStateTransition(sub_op.id)),
    }

    Ok(())
}

mod messages {
    use crate::message::InnerMessage;
    use std::fmt::Display;

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
                Self::ReturnSub { sender, .. } => Some(sender),
                _ => None,
            }
        }

        pub fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::ReturnSub { target, .. } => Some(target),
                _ => None,
            }
        }

        pub fn terminal(&self) -> bool {
            use SubscribeMsg::*;
            matches!(self, ReturnSub { .. } | SeekNode { .. })
        }
    }

    impl Display for SubscribeMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {})", id),
                Self::FetchRouting { .. } => write!(f, "FetchRouting(id: {})", id),
                Self::RequestSub { .. } => write!(f, "RequestSub(id: {})", id),
                Self::ReturnSub { .. } => write!(f, "ReturnSub(id: {})", id),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        client_events::ClientRequest,
        node::test::{check_connectivity, NodeSpecification, SimNetwork},
        WrappedContract, WrappedState,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_subscribe_op_between_nodes() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 4usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let contract_val: WrappedState = gen.arbitrary()?;
        let contract_key: ContractKey = *contract.key();

        let (updates, _) = tokio::sync::mpsc::unbounded_channel();
        let event = ClientRequest::Subscribe {
            key: contract_key,
            updates,
            peer: PeerKey::random(),
        };
        let first_node = NodeSpecification {
            owned_contracts: Vec::new(),
            non_owned_contracts: vec![contract_key],
            events_to_generate: HashMap::from_iter([(1, event)]),
            contract_subscribers: HashMap::new(),
        };

        let second_node = NodeSpecification {
            owned_contracts: vec![(contract, contract_val)],
            non_owned_contracts: Vec::new(),
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::new(),
        };

        let subscribe_specs = HashMap::from_iter([
            ("node-0".to_string(), first_node),
            ("node-1".to_string(), second_node),
        ]);
        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 2);
        sim_nodes.build_with_specs(subscribe_specs).await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await?;

        Ok(())
    }
}
