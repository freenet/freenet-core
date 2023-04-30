//! A contract is PUT within a location distance, this entails that all nodes within
//! a given radius will cache a copy of the contract and it's current value,
//! as well as will broadcast updates to the contract value to all subscribers.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

pub(crate) use self::messages::PutMsg;
use locutus_runtime::{prelude::ContractKey, ContractContainer};

use super::{OpEnum, OpError, OperationResult};
use crate::{
    config::PEER_TIMEOUT,
    contract::ContractHandlerEvent,
    message::{InnerMessage, Message, Transaction, TxType},
    node::{ConnectionBridge, OpManager, PeerKey},
    operations::{op_trait::Operation, OpInitialization},
    ring::{Location, PeerKeyLocation, RingError},
    WrappedState,
};

pub(crate) struct PutOp {
    id: Transaction,
    state: Option<PutState>,
    /// time left until time out, when this reaches zero it will be removed from the state
    _ttl: Duration,
}

impl<CErr, CB: ConnectionBridge> Operation<CErr, CB> for PutOp
where
    CErr: std::error::Error + Send + Sync,
{
    type Message = PutMsg;
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
            Some(OpEnum::Put(put_op)) => {
                // was an existing operation, the other peer messaged back
                Ok(OpInitialization { op: put_op, sender })
            }
            Some(_) => return Err(OpError::OpNotPresent(tx)),
            None => {
                // new request to put a new value for a contract, initialize the machine
                Ok(OpInitialization {
                    op: Self {
                        state: Some(PutState::ReceivedRequest),
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
                PutMsg::RequestPut {
                    id,
                    contract,
                    value,
                    htl,
                    target,
                } => {
                    let sender = op_storage.ring.own_location();

                    let key = contract.key();
                    tracing::debug!(
                        "Performing a RequestPut for contract {} from {} to {}",
                        key,
                        sender.peer,
                        target.peer
                    );

                    return_msg = Some(PutMsg::SeekNode {
                        id,
                        sender,
                        target,
                        value,
                        contract,
                        htl,
                        skip_list: vec![sender.peer],
                    });

                    // no changes to state yet, still in AwaitResponse state
                    new_state = self.state;
                }
                PutMsg::SeekNode {
                    id,
                    sender,
                    value,
                    contract,
                    htl,
                    target,
                    mut skip_list,
                } => {
                    let key = contract.key();
                    let is_cached_contract = op_storage.ring.is_contract_cached(&key);

                    tracing::debug!(
                        "Performing a SeekNode at {}, trying put the contract {}",
                        target.peer,
                        key
                    );

                    if !is_cached_contract
                        && op_storage
                            .ring
                            .within_caching_distance(&Location::from(&key))
                    {
                        tracing::debug!("Contract `{}` not cached @ peer {}", key, target.peer);
                        match try_to_cache_contract(op_storage, &contract, &key).await {
                            Ok(_) => {}
                            Err(err) => return Err(err),
                        }
                    } else if !is_cached_contract {
                        // in this case forward to a closer node to the target location and just wait for a response
                        // to give back to requesting peer
                        // FIXME
                        tracing::warn!(
                            "Contract {} not found while processing info, forwarding",
                            key
                        );
                    }

                    // after the contract has been cached, push the update query
                    tracing::debug!("Attempting contract value update");
                    let new_value = put_contract(op_storage, key.clone(), value).await?;
                    tracing::debug!("Contract successfully updated");
                    // if the change was successful, communicate this back to the requestor and broadcast the change
                    conn_manager
                        .send(
                            &sender.peer,
                            (PutMsg::SuccessfulUpdate {
                                id,
                                new_value: new_value.clone(),
                            })
                            .into(),
                        )
                        .await?;
                    skip_list.push(target.peer);

                    if let Some(new_htl) = htl.checked_sub(1) {
                        // forward changes in the contract to nodes closer to the contract location, if possible
                        forward_changes(
                            op_storage,
                            conn_manager,
                            &contract,
                            new_value.clone(),
                            id,
                            new_htl,
                            skip_list.as_slice(),
                        )
                        .await;
                    }

                    let broadcast_to = op_storage
                        .ring
                        .subscribers_of(&key)
                        .map(|i| i.value().to_vec())
                        .unwrap_or_default();
                    tracing::debug!(
                        "Successfully updated a value for contract {} @ {:?}",
                        key,
                        target.location
                    );

                    match try_to_broadcast(
                        id,
                        op_storage,
                        self.state,
                        broadcast_to,
                        key.clone(),
                        new_value,
                        self._ttl,
                    )
                    .await
                    {
                        Ok((state, msg)) => {
                            new_state = state;
                            return_msg = msg;
                        }
                        Err(err) => return Err(err),
                    }
                }
                PutMsg::BroadcastTo {
                    id,
                    key,
                    new_value,
                    sender,
                    sender_subscribers,
                } => {
                    let target = op_storage.ring.own_location();

                    tracing::debug!("Attempting contract value update");
                    let new_value = put_contract(op_storage, key.clone(), new_value).await?;
                    tracing::debug!("Contract successfully updated");

                    let broadcast_to = op_storage
                        .ring
                        .subscribers_of(&key)
                        .map(|i| {
                            // Avoid already broadcast nodes and sender from broadcasting
                            let mut subscribers: Vec<PeerKeyLocation> = i.value().to_vec();
                            let mut avoid_list: HashSet<PeerKey> =
                                sender_subscribers.into_iter().map(|pl| pl.peer).collect();
                            avoid_list.insert(sender.peer);
                            subscribers.retain(|s| !avoid_list.contains(&s.peer));
                            subscribers
                        })
                        .unwrap_or_default();
                    tracing::debug!(
                        "Successfully updated a value for contract {} @ {:?}",
                        key,
                        target.location
                    );

                    match try_to_broadcast(
                        id,
                        op_storage,
                        self.state,
                        broadcast_to,
                        key,
                        new_value,
                        self._ttl,
                    )
                    .await
                    {
                        Ok((state, msg)) => {
                            new_state = state;
                            return_msg = msg;
                        }
                        Err(err) => return Err(err),
                    }
                }
                PutMsg::Broadcasting {
                    id,
                    mut broadcast_to,
                    mut broadcasted_to,
                    key,
                    new_value,
                } => {
                    let sender = op_storage.ring.own_location();
                    let msg = PutMsg::BroadcastTo {
                        id,
                        key: key.clone(),
                        new_value: new_value.clone(),
                        sender,
                        sender_subscribers: broadcast_to.clone(),
                    };

                    let mut broadcasting = Vec::with_capacity(broadcast_to.len());
                    for peer in &broadcast_to {
                        let f = conn_manager.send(&peer.peer, msg.clone().into());
                        broadcasting.push(f);
                    }
                    let error_futures = futures::future::join_all(broadcasting)
                        .await
                        .into_iter()
                        .enumerate()
                        .filter_map(|(p, err)| {
                            if let Err(err) = err {
                                Some((p, err))
                            } else {
                                None
                            }
                        })
                        .rev();

                    let mut incorrect_results = 0;
                    for (peer_num, err) in error_futures {
                        // remove the failed peers in reverse order
                        let peer = broadcast_to.remove(peer_num);
                        tracing::warn!(
                            "failed broadcasting put change to {} with error {}; dropping connection",
                            peer.peer,
                            err
                        );
                        conn_manager.drop_connection(&peer.peer).await?;
                        incorrect_results += 1;
                    }

                    broadcasted_to += broadcast_to.len() - incorrect_results;
                    tracing::debug!(
                        "successfully broadcasted put into contract {key} to {broadcasted_to} peers"
                    );

                    // Subscriber nodes have been notified of the change, the operation is completed
                    return_msg = None;
                    new_state = None;
                }
                PutMsg::SuccessfulUpdate { .. } => {
                    match self.state {
                        Some(PutState::AwaitingResponse { contract, .. }) => {
                            tracing::debug!("Successfully updated value for {}", contract,);
                            new_state = None;
                            return_msg = None;
                        }
                        _ => return Err(OpError::InvalidStateTransition(self.id)),
                    };
                    tracing::debug!(
                        "Peer {} completed contract value put",
                        op_storage.ring.peer_key
                    );
                }
                PutMsg::PutForward {
                    id,
                    contract,
                    new_value,
                    htl,
                    mut skip_list,
                } => {
                    let key = contract.key();
                    let peer_loc = op_storage.ring.own_location();

                    tracing::debug!(
                        "Forwarding changes at {}, trying put the contract {}",
                        peer_loc.peer,
                        key
                    );

                    let cached_contract = op_storage.ring.is_contract_cached(&key);
                    let within_caching_dist = op_storage
                        .ring
                        .within_caching_distance(&Location::from(&key));
                    if !cached_contract && within_caching_dist {
                        match try_to_cache_contract(op_storage, &contract, &key).await {
                            Ok(_) => {}
                            Err(err) => return Err(err),
                        }
                    } else if !within_caching_dist {
                        // not a contract this node cares about; do nothing
                        return Ok(OperationResult {
                            return_msg: None,
                            state: None,
                        });
                    }
                    // after the contract has been cached, push the update query
                    let new_value = put_contract(op_storage, key, new_value).await?;

                    //update skip list
                    skip_list.push(peer_loc.peer);

                    // if successful, forward to the next closest peers (if any)
                    if let Some(new_htl) = htl.checked_sub(1) {
                        forward_changes(
                            op_storage,
                            conn_manager,
                            &contract,
                            new_value,
                            id,
                            new_htl,
                            skip_list.as_slice(),
                        )
                        .await;
                    }
                    return_msg = None;
                    new_state = None;
                }
                _ => return Err(OpError::UnexpectedOpState),
            }

            build_op_result(self.id, new_state, return_msg, self._ttl)
        })
    }
}

fn build_op_result<CErr: std::error::Error>(
    id: Transaction,
    state: Option<PutState>,
    msg: Option<PutMsg>,
    ttl: Duration,
) -> Result<OperationResult, OpError<CErr>> {
    let output_op = Some(PutOp {
        id,
        state,
        _ttl: ttl,
    });
    Ok(OperationResult {
        return_msg: msg.map(Message::from),
        state: output_op.map(OpEnum::Put),
    })
}

async fn try_to_cache_contract<'a, CErr: std::error::Error>(
    op_storage: &'a OpManager<CErr>,
    contract: &ContractContainer,
    key: &ContractKey,
) -> Result<(), OpError<CErr>> {
    // this node does not have the contract, so instead store the contract and execute the put op.
    let res = op_storage
        .notify_contract_handler(ContractHandlerEvent::Cache(contract.clone()))
        .await?;
    if let ContractHandlerEvent::CacheResult(Ok(_)) = res {
        op_storage.ring.contract_cached(key);
        tracing::debug!("Contract successfully cached");
        Ok(())
    } else {
        tracing::error!(
            "Contract handler returned wrong event when trying to cache contract, this should not happen!"
        );
        Err(OpError::UnexpectedOpState)
    }
}

async fn try_to_broadcast<CErr: std::error::Error>(
    id: Transaction,
    op_storage: &OpManager<CErr>,
    state: Option<PutState>,
    broadcast_to: Vec<PeerKeyLocation>,
    key: ContractKey,
    new_value: WrappedState,
    ttl: Duration,
) -> Result<(Option<PutState>, Option<PutMsg>), OpError<CErr>> {
    let new_state;
    let return_msg;

    match state {
        Some(PutState::ReceivedRequest | PutState::BroadcastOngoing { .. }) => {
            if broadcast_to.is_empty() {
                // broadcast complete
                tracing::debug!(
                    "Empty broadcast list while updating value for contract {}",
                    key
                );
                // means the whole tx finished so can return early
                new_state = None;
                return_msg = Some(PutMsg::SuccessfulUpdate { id, new_value });
            } else {
                tracing::debug!("Callback to start broadcasting to other nodes");
                new_state = Some(PutState::BroadcastOngoing);
                return_msg = Some(PutMsg::Broadcasting {
                    id,
                    new_value,
                    broadcasted_to: 0,
                    broadcast_to,
                    key,
                });

                let op = PutOp {
                    id,
                    state: new_state,
                    _ttl: ttl,
                };
                op_storage
                    .notify_op_change(Message::from(return_msg.unwrap()), OpEnum::Put(op))
                    .await?;
                return Err(OpError::StatePushed);
            }
        }
        _ => return Err(OpError::InvalidStateTransition(id)),
    };

    Ok((new_state, return_msg))
}

pub(crate) fn start_op(
    contract: ContractContainer,
    value: WrappedState,
    htl: usize,
    peer: &PeerKey,
) -> PutOp {
    let key = contract.key();
    tracing::debug!(
        "Requesting put to contract {} @ loc({})",
        key,
        Location::from(&key)
    );

    let id = Transaction::new(<PutMsg as TxType>::tx_type_id(), peer);
    let state = Some(PutState::PrepareRequest {
        id,
        contract,
        value,
        htl,
    });

    PutOp {
        id,
        state,
        _ttl: PEER_TIMEOUT,
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
enum PutState {
    ReceivedRequest,
    PrepareRequest {
        id: Transaction,
        contract: ContractContainer,
        value: WrappedState,
        htl: usize,
    },
    AwaitingResponse {
        contract: ContractKey,
    },
    BroadcastOngoing,
}

/// Request to insert/update a value into a contract.
pub(crate) async fn request_put<CErr>(
    op_storage: &OpManager<CErr>,
    put_op: PutOp,
) -> Result<(), OpError<CErr>>
where
    CErr: std::error::Error,
{
    let key = if let Some(PutState::PrepareRequest { contract, .. }) = put_op.state.clone() {
        contract.key()
    } else {
        return Err(OpError::UnexpectedOpState);
    };

    let sender = op_storage.ring.own_location();

    // the initial request must provide:
    // - a peer as close as possible to the contract location
    // - and the value to put
    let target = op_storage
        .ring
        .closest_caching(&key, 1, &[sender.peer])
        .into_iter()
        .next()
        .ok_or(RingError::EmptyRing)?;

    let id = put_op.id;

    match put_op.state.clone() {
        Some(PutState::PrepareRequest {
            contract,
            value,
            htl,
            ..
        }) => {
            let key = contract.key();
            let new_state = Some(PutState::AwaitingResponse { contract: key });
            let msg = Some(PutMsg::RequestPut {
                id,
                contract,
                value,
                htl,
                target,
            });

            let op = PutOp {
                state: new_state,
                id,
                _ttl: put_op._ttl,
            };

            op_storage
                .notify_op_change(msg.map(Message::from).unwrap(), OpEnum::Put(op))
                .await?;
        }
        _ => return Err(OpError::InvalidStateTransition(put_op.id)),
    };

    Ok(())
}

async fn put_contract<CErr>(
    op_storage: &OpManager<CErr>,
    key: ContractKey,
    state: WrappedState,
) -> Result<WrappedState, OpError<CErr>>
where
    CErr: std::error::Error,
{
    // after the contract has been cached, push the update query
    match op_storage
        .notify_contract_handler(ContractHandlerEvent::PushQuery { key, state })
        .await
    {
        Ok(ContractHandlerEvent::PushResponse {
            new_value: Ok(new_val),
        }) => Ok(new_val),
        Ok(ContractHandlerEvent::PushResponse {
            new_value: Err(_err),
        }) => {
            // return Err(OpError::from(ContractError::StorageError(err)));
            todo!("not a valid value update, notify back to requester")
        }
        Err(err) => Err(err.into()),
        Ok(_) => Err(OpError::UnexpectedOpState),
    }
}

// TODO: keep track of who is supposed to have the contract, and only send if necessary
// since sending the contract over and over, will be expensive; this can be done via subscriptions
/// Communicate changes in the contract to other peers nearby the contract location.
/// This operation is "fire and forget" and the node does not keep track if is successful or not.
async fn forward_changes<CErr, CB>(
    op_storage: &OpManager<CErr>,
    conn_manager: &CB,
    contract: &ContractContainer,
    new_value: WrappedState,
    id: Transaction,
    htl: usize,
    skip_list: &[PeerKey],
) where
    CErr: std::error::Error,
    CB: ConnectionBridge,
{
    let key = contract.key();
    let contract_loc = Location::from(&key);
    let forward_to = op_storage.ring.closest_caching(&key, 1, skip_list);
    let own_loc = op_storage.ring.own_location().location.expect("infallible");
    for peer in forward_to {
        let other_loc = peer.location.as_ref().expect("infallible");
        let other_distance = contract_loc.distance(other_loc);
        let self_distance = contract_loc.distance(own_loc);
        if other_distance < self_distance {
            // forward the contract towards this node since it is indeed closer to the contract location
            // and forget about it, no need to keep track of this op or wait for response
            let _ = conn_manager
                .send(
                    &peer.peer,
                    (PutMsg::PutForward {
                        id,
                        contract: contract.clone(),
                        new_value: new_value.clone(),
                        htl,
                        skip_list: skip_list.to_vec(),
                    })
                    .into(),
                )
                .await;
        }
    }
}

mod messages {
    use std::fmt::Display;

    use super::*;

    use crate::message::InnerMessage;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum PutMsg {
        /// Initialize the put operation by routing the value
        RouteValue {
            id: Transaction,
            htl: usize,
            target: PeerKeyLocation,
        },
        /// Internal node instruction to find a route to the target node.
        RequestPut {
            id: Transaction,
            contract: ContractContainer,
            value: WrappedState,
            /// max hops to live
            htl: usize,
            target: PeerKeyLocation,
        },
        /// Internal node instruction to await the result of a put.
        AwaitPut { id: Transaction },
        /// Forward a contract and it's latest value to an other node
        PutForward {
            id: Transaction,
            contract: ContractContainer,
            new_value: WrappedState,
            /// current htl, reduced by one at each hop
            htl: usize,
            skip_list: Vec<PeerKey>,
        },
        /// Value successfully inserted/updated.
        SuccessfulUpdate {
            id: Transaction,
            new_value: WrappedState,
        },
        /// Target the node which is closest to the key
        SeekNode {
            id: Transaction,
            sender: PeerKeyLocation,
            target: PeerKeyLocation,
            value: WrappedState,
            contract: ContractContainer,
            /// max hops to live
            htl: usize,
            // FIXME: remove skip list once we deduplicate at top msg handling level
            // using this is a tmp workaround until (https://github.com/freenet/locutus/issues/13) is done
            skip_list: Vec<PeerKey>,
        },
        /// Internal node instruction that  a change (either a first time insert or an update).
        Broadcasting {
            id: Transaction,
            broadcasted_to: usize,
            broadcast_to: Vec<PeerKeyLocation>,
            key: ContractKey,
            new_value: WrappedState,
        },
        /// Broadcasting a change to a peer, which then will relay the changes to other peers.
        BroadcastTo {
            id: Transaction,
            sender: PeerKeyLocation,
            key: ContractKey,
            new_value: WrappedState,
            sender_subscribers: Vec<PeerKeyLocation>,
        },
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::SeekNode { id, .. } => id,
                Self::RouteValue { id, .. } => id,
                Self::RequestPut { id, .. } => id,
                Self::Broadcasting { id, .. } => id,
                Self::SuccessfulUpdate { id, .. } => id,
                Self::PutForward { id, .. } => id,
                Self::AwaitPut { id } => id,
                Self::BroadcastTo { id, .. } => id,
            }
        }
    }

    impl PutMsg {
        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { sender, .. } => Some(sender),
                Self::BroadcastTo { sender, .. } => Some(sender),
                _ => None,
            }
        }

        pub fn target(&self) -> Option<&PeerKeyLocation> {
            match self {
                Self::SeekNode { target, .. } => Some(target),
                Self::RequestPut { target, .. } => Some(target),
                _ => None,
            }
        }

        pub fn terminal(&self) -> bool {
            use PutMsg::*;
            matches!(
                self,
                SuccessfulUpdate { .. } | SeekNode { .. } | PutForward { .. }
            )
        }
    }

    impl Display for PutMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let id = self.id();
            match self {
                Self::SeekNode { .. } => write!(f, "SeekNode(id: {id})"),
                Self::RouteValue { .. } => write!(f, "RouteValue(id: {id})"),
                Self::RequestPut { .. } => write!(f, "RequestPut(id: {id})"),
                Self::Broadcasting { .. } => write!(f, "Broadcasting(id: {id})"),
                Self::SuccessfulUpdate { .. } => write!(f, "SusscessfulUpdate(id: {id})"),
                Self::PutForward { .. } => write!(f, "PutForward(id: {id})"),
                Self::AwaitPut { .. } => write!(f, "AwaitPut(id: {id})"),
                Self::BroadcastTo { .. } => write!(f, "BroadcastTo(id: {id})"),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use locutus_runtime::{WasmAPIVersion, WrappedContract};
    use locutus_stdlib::client_api::ContractRequest;
    use std::collections::HashMap;

    use crate::node::test::{check_connectivity, NodeSpecification, SimNetwork};

    use super::*;

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn successful_put_op_between_nodes() -> Result<(), anyhow::Error> {
        const NUM_NODES: usize = 2usize;
        const NUM_GW: usize = 1usize;

        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = gen.arbitrary()?;
        let key = contract.key().clone();
        let contract_val: WrappedState = gen.arbitrary()?;
        let new_value = WrappedState::new(Vec::from_iter(gen.arbitrary::<[u8; 20]>().unwrap()));

        let mut sim_nodes = SimNetwork::new(NUM_GW, NUM_NODES, 3, 2, 4, 2);
        let mut locations = sim_nodes.get_locations_by_node();
        let node0_loc = locations.remove("node-0").unwrap();
        let node1_loc = locations.remove("node-1").unwrap();

        // both own the contract, and one triggers an update
        let node_0 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(WasmAPIVersion::V1(contract.clone())),
                contract_val.clone(),
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::from_iter([(key.clone(), vec![node1_loc])]),
        };

        let node_1 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(WasmAPIVersion::V1(contract.clone())),
                contract_val.clone(),
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::new(),
            contract_subscribers: HashMap::from_iter([(key.clone(), vec![node0_loc])]),
        };

        let put_event = ContractRequest::Put {
            contract: ContractContainer::Wasm(WasmAPIVersion::V1(contract.clone())),
            state: new_value.clone(),
            related_contracts: Default::default(),
        }
        .into();

        let gw_0 = NodeSpecification {
            owned_contracts: vec![(
                ContractContainer::Wasm(WasmAPIVersion::V1(contract.clone())),
                contract_val,
            )],
            non_owned_contracts: vec![],
            events_to_generate: HashMap::from_iter([(1, put_event)]),
            contract_subscribers: HashMap::new(),
        };

        // establish network
        let put_specs = HashMap::from_iter([
            ("node-0".to_string(), node_0),
            ("node-1".to_string(), node_1),
            ("gateway-0".to_string(), gw_0),
        ]);

        sim_nodes.build_with_specs(put_specs).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        check_connectivity(&sim_nodes, NUM_NODES, Duration::from_secs(3)).await?;

        // trigger the put op @ gw-0, this
        sim_nodes
            .trigger_event("gateway-0", 1, Some(Duration::from_secs(3)))
            .await?;
        assert!(sim_nodes.has_put_contract("gateway-0", &key, &new_value));
        assert!(sim_nodes.event_listener.contract_broadcasted(&key));
        Ok(())
    }
}
