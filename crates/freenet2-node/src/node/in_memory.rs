use crate::{
    conn_manager::{in_memory::MemoryConnManager, ConnectionBridge, PeerKeyLocation},
    message::Message,
    operations::join_ring::{self, JoinRingOp},
    NodeConfig, PeerKey,
};

use super::op_state::OpStateStorage;

pub(super) struct InMemory {
    peer: PeerKey,
    listening: bool,
    conn_manager: MemoryConnManager,
    op_storage: OpStateStorage,
    gateways: Vec<PeerKeyLocation>,
}

impl InMemory {
    pub fn build(config: NodeConfig) -> Result<Self, &'static str> {
        if (config.local_ip.is_none() || config.local_port.is_none())
            && config.remote_nodes.is_empty()
        {
            return Err("At least one remote gateway is required to join an existing network for non-gateway nodes.");
        }
        let peer = PeerKey::from(config.local_key.public());
        let conn_manager = MemoryConnManager::new(true, peer, None);
        Ok(InMemory {
            peer,
            listening: true,
            conn_manager,
            op_storage: OpStateStorage::new(),
            gateways: vec![],
        })
    }

    pub async fn start(&mut self) -> Result<(), ()> {
        if self.listening {
            // cannot start if already listening; meaning that this node already joined
            // the ring or is a gateway
            return Err(());
        }

        // FIXME: this iteration should be shuffled, must write an extension iterator shuffle items "in place"
        // the idea here is to limit the amount of gateways being contacted that's why shuffling is required
        for gateway in &self.gateways {
            // initiate join action action per each gateway
            let op = JoinRingOp::new(
                PeerKeyLocation {
                    peer: self.peer,
                    location: None,
                },
                *gateway,
            );
            join_ring::initial_join_request(&mut self.op_storage, &mut self.conn_manager, op)
                .await
                .unwrap();
        }

        Err(())
    }

    /// Starts listening to incoming messages, only allowed to be called directly when this node
    /// already joined the network.
    pub async fn listen_on(&mut self) -> Result<(), ()> {
        if !self.listening {
            return Err(());
        }

        loop {
            match self.conn_manager.recv().await {
                Ok(msg) => match msg {
                    Message::JoinRing(join_op) => {
                        join_ring::join_ring(&mut self.op_storage, &mut self.conn_manager, join_op)
                            .await
                            .unwrap();
                    }
                    Message::Canceled(_) => todo!(),
                    // old:
                    Message::ProbeRequest(_, _) => todo!(),
                    Message::ProbeResponse(_, _) => todo!(),
                },
                Err(_) => break Err(()),
            }
        }
    }
}
