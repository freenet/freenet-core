use std::time::Duration;

use locutus_runtime::ContractKey;
use serde::{Deserialize, Serialize};

use crate::{
    config::PEER_TIMEOUT,
    contract::ContractError,
    message::{Message, Transaction, TxType},
    node::{ConnectionBridge, OpManager, PeerKey},
    ring::{PeerKeyLocation, RingError},
};

use super::{
    handle_op_result,
    state_machine::{StateMachine, StateMachineImpl},
    OpError, Operation, OperationResult,
};

pub(crate) use self::messages::SubscribeMsg;

pub(crate) struct SubscribeOp {
    sm: StateMachine<SubscribeOpSm>,
    _ttl: Duration,
}

impl SubscribeOp {
    const MAX_RETRIES: usize = 10;

    pub fn start_op(key: ContractKey, peer: &PeerKey) -> Self {
        let id = Transaction::new(<SubscribeMsg as TxType>::tx_type_id(), peer);
        let sm = StateMachine::from_state(SubscribeState::PrepareRequest { id, key }, id);
        SubscribeOp {
            sm,
            _ttl: PEER_TIMEOUT,
        }
    }

    pub fn id(&self) -> Transaction {
        self.sm.id
    }
}

struct SubscribeOpSm;

impl StateMachineImpl for SubscribeOpSm {
    type Input = SubscribeMsg;

    type State = SubscribeState;

    type Output = SubscribeMsg;

    fn state_transition(state: &mut Self::State, input: &mut Self::Input) -> Option<Self::State> {
        match (state, input) {
            (SubscribeState::PrepareRequest { .. }, SubscribeMsg::FetchRouting { .. }) => {
                Some(SubscribeState::AwaitingResponse {
                    skip_list: vec![],
                    retries: 0,
                })
            }
            (SubscribeState::ReceivedRequest, SubscribeMsg::SeekNode { .. }) => {
                Some(SubscribeState::Completed)
            }
            (
                SubscribeState::AwaitingResponse { .. },
                SubscribeMsg::ReturnSub {
                    subscribed: true, ..
                },
            ) => Some(SubscribeState::Completed),
            _ => None,
        }
    }

    fn state_transition_from_input(state: Self::State, input: Self::Input) -> Option<Self::State> {
        match (state, input) {
            (
                SubscribeState::AwaitingResponse {
                    mut skip_list,
                    retries,
                },
                SubscribeMsg::ReturnSub {
                    sender,
                    subscribed: false,
                    ..
                },
            ) => {
                if retries < SubscribeOp::MAX_RETRIES {
                    skip_list.push(sender.peer);
                    Some(SubscribeState::AwaitingResponse {
                        skip_list,
                        retries: retries + 1,
                    })
                } else {
                    None
                }
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
            (
                SubscribeState::ReceivedRequest,
                SubscribeMsg::SeekNode {
                    id,
                    key,
                    target,
                    subscriber,
                    ..
                },
            ) => {
                log::info!(
                    "Peer {} successfully subscribed to contract {}",
                    subscriber.peer,
                    key
                );
                Some(SubscribeMsg::ReturnSub {
                    sender: target,
                    target: subscriber,
                    id,
                    key,
                    subscribed: true,
                })
            }
            _ => None,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
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
    mut sub_op: SubscribeOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let (target, id) = if let SubscribeState::PrepareRequest { id, key } = sub_op.sm.state() {
        if !op_storage.ring.is_contract_cached(key) {
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
        return Err(OpError::UnexpectedOpState);
    };

    if let Some(req_sub) = sub_op
        .sm
        .consume_to_output(SubscribeMsg::FetchRouting { target, id })?
    {
        op_storage
            .notify_op_change(Message::from(req_sub), Operation::Subscribe(sub_op))
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
        Some(_) => return Err(OpError::OpNotPresent(tx)),
        None => {
            sender = subscribe_op.sender().cloned();
            // new request to subcribe to a contract, initialize the machine
            let machine = SubscribeOp {
                sm: StateMachine::from_state(SubscribeState::ReceivedRequest, tx),
                _ttl: PEER_TIMEOUT,
            };
            update_state(conn_manager, machine, subscribe_op, op_storage).await
        }
    };

    handle_op_result(
        op_storage,
        conn_manager,
        result.map_err(|err| (err, tx)),
        sender.map(|p| p.peer),
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
            let sender = op_storage.ring.own_location();
            new_state = Some(state);
            return_msg = Some(Message::from(SubscribeMsg::SeekNode {
                id,
                key,
                target,
                subscriber: sender,
                skip_list: vec![sender.peer],
                htl: 0,
            }));
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

                let new_target = op_storage.ring.closest_caching(&key, 1, &[sender.peer])[0];
                let new_htl = htl + 1;

                if new_htl > SubscribeOp::MAX_RETRIES {
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

                return_msg = state
                    .sm
                    .consume_to_state(SubscribeMsg::SeekNode {
                        id,
                        key,
                        subscriber,
                        target,
                        skip_list,
                        htl,
                    })?
                    .map(Message::from);
                new_state = Some(state);
            } else {
                if op_storage.ring.add_subscriber(key, subscriber).is_err() {
                    // max number of subscribers for this contract reached
                    return Ok(return_err());
                }
                new_state = None;
                return_msg = state
                    .sm
                    .consume_to_output(SubscribeMsg::SeekNode {
                        id,
                        key,
                        target,
                        subscriber,
                        skip_list,
                        htl,
                    })?
                    .map(Message::from);
            }
        }
        SubscribeMsg::ReturnSub {
            subscribed: false,
            key,
            sender,
            target,
            id,
        } => {
            log::warn!(
                "Contract `{}` not found at potential subscription provider {}",
                key,
                sender.peer
            );
            // will error out in case it has reached max number of retries
            state
                .sm
                .consume_to_state(SubscribeMsg::ReturnSub {
                    subscribed: false,
                    key,
                    sender,
                    target,
                    id,
                })
                .map_err(|_: OpError<CErr>| OpError::MaxRetriesExceeded(id, "sub".to_owned()))?;
            if let SubscribeState::AwaitingResponse { skip_list, .. } = state.sm.state() {
                if let Some(target) = op_storage
                    .ring
                    .closest_caching(&key, 1, skip_list)
                    .into_iter()
                    .next()
                {
                    let subscriber = op_storage.ring.own_location();
                    return_msg = Some(Message::from(SubscribeMsg::SeekNode {
                        id,
                        key,
                        subscriber,
                        target,
                        skip_list: vec![target.peer],
                        htl: 0,
                    }));
                    new_state = Some(state);
                } else {
                    return Err(RingError::NoCachingPeers(key).into());
                }
            } else {
                return Err(OpError::InvalidStateTransition(id));
            }
        }
        SubscribeMsg::ReturnSub {
            subscribed: true,
            key,
            sender,
            target,
            id,
        } => {
            log::warn!(
                "Subscribed to `{}` not found at potential subscription provider {}",
                key,
                sender.peer
            );
            op_storage.ring.add_subscription(key);
            return_msg = state
                .sm
                .consume_to_output(SubscribeMsg::ReturnSub {
                    subscribed: true,
                    key,
                    sender,
                    target,
                    id,
                })?
                .map(Message::from);
            new_state = None;
        }
        _ => return Err(OpError::UnexpectedOpState),
    }
    Ok(OperationResult {
        return_msg,
        state: new_state.map(Operation::Subscribe),
    })
}

mod messages {
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
    use locutus_runtime::{Contract, ContractState};

    use super::*;
    use crate::{
        client_events::ClientRequest,
        contract::SimStoreError,
        node::test::{check_connectivity, NodeSpecification, SimNetwork},
        ring::Location,
    };
    use std::collections::HashMap;

    #[test]
    fn successful_subscribe_op_seq() -> Result<(), anyhow::Error> {
        let peer = PeerKey::random();
        let id = Transaction::new(<SubscribeMsg as TxType>::tx_type_id(), &peer);
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: Contract = gen.arbitrary()?;

        let key = contract.key();

        let subscriber_loc = PeerKeyLocation {
            location: Some(Location::random()),
            peer: PeerKey::random(),
        };
        let first_target_loc = PeerKeyLocation {
            location: Some(Location::random()),
            peer: PeerKey::random(),
        };
        let second_target_loc = PeerKeyLocation {
            location: Some(Location::random()),
            peer: PeerKey::random(),
        };

        let mut subscriber = SubscribeOp::start_op(key, &peer).sm;
        let mut target =
            StateMachine::<SubscribeOpSm>::from_state(SubscribeState::ReceivedRequest, id);

        subscriber.consume_to_output::<OpError<SimStoreError>>(SubscribeMsg::FetchRouting {
            id,
            target: first_target_loc,
        })?;

        assert_eq!(
            subscriber.state(),
            &SubscribeState::AwaitingResponse {
                skip_list: vec![],
                retries: 0
            }
        );

        let res_msg = target
            .consume_to_output::<OpError<SimStoreError>>(SubscribeMsg::SeekNode {
                id,
                key,
                target: first_target_loc,
                subscriber: subscriber_loc,
                skip_list: vec![subscriber_loc.peer],
                htl: 0,
            })?
            .ok_or_else(|| anyhow::anyhow!("no output"))?;

        let expected_msg = SubscribeMsg::ReturnSub {
            id,
            key,
            sender: first_target_loc,
            target: subscriber_loc,
            subscribed: true,
        };

        assert_eq!(target.state(), &SubscribeState::Completed);
        assert_eq!(res_msg, expected_msg);

        subscriber.consume_to_output::<OpError<SimStoreError>>(SubscribeMsg::ReturnSub {
            id,
            key,
            sender: first_target_loc,
            target: subscriber_loc,
            subscribed: true,
        })?;

        assert_eq!(subscriber.state(), &SubscribeState::Completed);

        target = StateMachine::<SubscribeOpSm>::from_state(SubscribeState::ReceivedRequest, id);
        target.consume_to_output::<OpError<SimStoreError>>(SubscribeMsg::SeekNode {
            id,
            key,
            target: second_target_loc,
            subscriber: subscriber_loc,
            skip_list: vec![subscriber_loc.peer, first_target_loc.peer],
            htl: 0,
        })?;

        assert_eq!(target.state(), &SubscribeState::Completed);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn successful_subscribe_op_between_nodes() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 4usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: Contract = gen.arbitrary()?;
        let contract_val: ContractState = gen.arbitrary()?;
        let contract_key: ContractKey = contract.key();

        let event = ClientRequest::Subscribe { key: contract_key };
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
