use crate::{
    conn_manager::{in_memory::MemoryConnManager, ConnectionBridge, PeerKey, PeerKeyLocation},
    message::{Message, Transaction, TransactionType},
    operations::{
        get,
        join_ring::{self, JoinRingMsg},
        put,
    },
    ring::Ring,
    user_events::{test_utils::MemoryEventsGen, UserEvent, UserEventsProxy},
    NodeConfig,
};

use super::{op_state::OpStateStorage, InitPeerNode};

pub(crate) struct NodeInMemory<Ev = MemoryEventsGen>
where
    Ev: UserEventsProxy,
{
    peer: PeerKey,
    gateways: Vec<PeerKeyLocation>,
    pub conn_manager: MemoryConnManager,
    pub op_storage: OpStateStorage,
    user_events: Ev,
}

impl NodeInMemory {
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
        let op_storage = OpStateStorage::new(ring);
        Ok(NodeInMemory {
            peer,
            conn_manager,
            op_storage,
            gateways,
            user_events: MemoryEventsGen {},
        })
    }

    pub async fn join_ring(&mut self) -> Result<(), ()> {
        // FIXME: this iteration should be shuffled, must write an extension iterator shuffle items "in place"
        // the idea here is to limit the amount of gateways being contacted that's why shuffling is required
        for gateway in &self.gateways {
            let tx_id = Transaction::new(<JoinRingMsg as TransactionType>::tx_type_id());
            // initiate join action action per each gateway
            let op = join_ring::JoinRingOp::initial_request(
                self.peer,
                *gateway,
                self.op_storage.ring.max_hops_to_live,
            );
            join_ring::join_ring_request(tx_id, &mut self.op_storage, &mut self.conn_manager, op)
                .await
                .unwrap();
        }
        Ok(())
    }

    /// Starts listening to incoming messages, only allowed to be called directly when this node
    /// already joined the network.
    pub async fn listen_on(&mut self) -> Result<(), ()> {
        self.join_ring().await?;
        loop {
            tokio::select! {
                msg = self.conn_manager.recv() => {
                    match msg {
                        Ok(msg) => match msg {
                            Message::JoinRing(op) => {
                                join_ring::handle_join_ring(&mut self.op_storage, &mut self.conn_manager, op)
                                    .await
                                    .unwrap();
                            }
                            Message::Put(op) => {
                                put::handle_put_response(&mut self.op_storage, &mut self.conn_manager, op)
                                    .await
                                    .unwrap();
                            }
                            Message::Get(op) => {
                                get::handle_get_response(&mut self.op_storage, &mut self.conn_manager, op)
                                .await
                                .unwrap();
                            }
                            Message::Canceled(_) => todo!(),
                        },
                        Err(_) => break Err(()),
                    }
                }
                usr_event = self.user_events.recv() => {
                    match usr_event {
                        UserEvent::Put { key, value, contract } => {
                            // Initialize a put op.
                            let op = put::PutOp::new(key, value);
                            put::request_put(&mut self.op_storage, &mut self.conn_manager, op).await.unwrap();
                        }
                        UserEvent::Get { key, contract } => {
                            // Initialize a get op.
                            let op = get::GetOp::new(key);
                            get::request_get(&mut self.op_storage, &mut self.conn_manager, op).await.unwrap();
                        }
                    }
                }
            }
        }
    }
}
