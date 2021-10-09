use std::sync::Arc;

use tokio::sync::mpsc::{self, Receiver};

use crate::{
    conn_manager::{in_memory::MemoryConnManager, ConnectionBridge, PeerKey, PeerKeyLocation},
    contract::ContractError,
    contract_store::{
        test::MemoryContractHandler, ContractHandler, ContractHandlerChannel, ContractHandlerEvent,
    },
    message::{GetTxType, Message, Transaction},
    operations::{
        get,
        join_ring::{self, JoinRingMsg},
        put,
    },
    ring::Ring,
    user_events::{UserEvent, UserEventsProxy},
    NodeConfig,
};

use super::{op_state::OpStateStorage, InitPeerNode};

pub(crate) struct NodeInMemory {
    peer: PeerKey,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: Receiver<Message>,
    pub conn_manager: MemoryConnManager,
    pub op_storage: Arc<OpStateStorage<String>>,
}

impl NodeInMemory {
    /// Buils an in-memory node. Does nothing upon construction,
    pub fn build(config: NodeConfig) -> Result<Self, &'static str> {
        if (config.local_ip.is_none() || config.local_port.is_none())
            && config.remote_nodes.is_empty()
        {
            return Err("At least one remote gateway is required to join an existing network for non-gateway nodes.");
        }
        let peer = PeerKey::from(config.local_key.public());
        let conn_manager = MemoryConnManager::new(true, peer, None);
        let gateways = config
            .remote_nodes
            .into_iter()
            .filter_map(|node| {
                let InitPeerNode {
                    identifier,
                    location,
                    ..
                } = node;
                location.zip(identifier).map(|(loc, id)| PeerKeyLocation {
                    peer: PeerKey(id),
                    location: Some(loc),
                })
            })
            .collect();
        let mut ring = Ring::new();
        if let Some(max_hops_to_live) = config.max_hops_to_live {
            ring.with_max_hops(max_hops_to_live);
        }
        if let Some(rnd_if_htl_above) = config.rnd_if_htl_above {
            ring.with_rnd_walk_above(rnd_if_htl_above);
        }
        let (notification_tx, notification_channel) = mpsc::channel(100);
        let ch_handler = ContractHandlerChannel::new();
        let op_storage = Arc::new(OpStateStorage::new(
            ring,
            notification_tx,
            ch_handler.clone(),
        ));
        let contract_handler = MemoryContractHandler::new(ch_handler);

        tokio::spawn(contract_handling(contract_handler));

        Ok(NodeInMemory {
            peer,
            conn_manager,
            op_storage,
            gateways,
            notification_channel,
        })
    }

    pub async fn join_ring(&mut self) -> Result<(), ()> {
        // FIXME: this iteration should be shuffled, must write an extension iterator shuffle items "in place"
        // the idea here is to limit the amount of gateways being contacted that's why shuffling is required
        for gateway in &self.gateways {
            let tx_id = Transaction::new(<JoinRingMsg as GetTxType>::tx_type_id());
            // initiate join action action per each gateway
            let op = join_ring::JoinRingOp::initial_request(
                self.peer,
                *gateway,
                self.op_storage.ring.max_hops_to_live,
            );
            join_ring::join_ring_request(tx_id, &self.op_storage, &mut self.conn_manager, op)
                .await
                .unwrap();
        }
        Ok(())
    }

    /// Starts listening to incoming events. Will attempt to join the ring if any gateways have been provided.
    pub async fn listen_on<UsrEv>(&mut self, user_events: UsrEv) -> Result<(), ()>
    where
        UsrEv: UserEventsProxy + Send + Sync + 'static,
    {
        self.join_ring().await?;
        tokio::spawn(user_event_handling(self.op_storage.clone(), user_events));

        // loop for processings messages
        loop {
            let msg = tokio::select! {
                msg = self.conn_manager.recv() => { msg }
                msg = self.notification_channel.recv() => if let Some(msg) = msg {
                    Ok(msg)
                } else {
                    break Err(());
                }
            };
            match msg {
                Ok(msg) => match msg {
                    Message::JoinRing(op) => {
                        join_ring::handle_join_ring(&self.op_storage, &mut self.conn_manager, op)
                            .await
                            .unwrap();
                    }
                    Message::Put(op) => {
                        put::handle_put_response(&self.op_storage, &mut self.conn_manager, op)
                            .await
                            .unwrap();
                    }
                    Message::Get(op) => {
                        get::handle_get_response(&self.op_storage, &mut self.conn_manager, op)
                            .await
                            .unwrap();
                    }
                    Message::Canceled(_) => todo!(),
                },
                Err(_) => return Err(()),
            }
        }
    }
}

async fn contract_handling<CH, Err>(mut contract_handler: CH) -> Result<(), ContractError<Err>>
where
    CH: ContractHandler<Error = Err>,
{
    loop {
        let res = contract_handler.channel().recv_from_listeners().await?;
        match res {
            (id, ContractHandlerEvent::FetchQuery(key)) => {
                let contract = contract_handler.fetch_contract(&key).await;
                contract_handler
                    .channel()
                    .send_to_listeners(id, ContractHandlerEvent::FetchResponse { key, contract })
                    .await
            }
            (id, ContractHandlerEvent::Cache(contract)) => {
                match contract_handler.store_contract(contract).await {
                    Ok(_) => {
                        contract_handler
                            .channel()
                            .send_to_listeners(id, ContractHandlerEvent::CacheResult(Ok(())))
                            .await;
                    }
                    Err(err) => {
                        contract_handler
                            .channel()
                            .send_to_listeners(id, ContractHandlerEvent::CacheResult(Err(err)))
                            .await;
                    }
                }
            }
            (id, ContractHandlerEvent::PushQuery { key, value }) => {
                match contract_handler.put_value(&key).await {
                    Ok(value) => {}
                    Err(err) => {}
                }
            }
            _ => unreachable!(),
        }
    }
}

/// Process user events.
async fn user_event_handling<UsrEv, CErr>(
    op_storage: Arc<OpStateStorage<CErr>>,
    mut user_events: UsrEv,
) where
    UsrEv: UserEventsProxy + Send + Sync + 'static,
    CErr: std::fmt::Debug,
{
    loop {
        match user_events.recv().await {
            UserEvent::Put { value, contract } => {
                // Initialize a put op.
                let op = put::PutOp::start_op(contract, value);
                put::request_put(&op_storage, op).await.unwrap();
            }
            UserEvent::Get { key, contract } => {
                // Initialize a get op.
                let op = get::GetOp::start_op(key);
                get::request_get(&op_storage, op).await.unwrap();
            }
        }
    }
}
