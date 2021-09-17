use crate::{
    conn_manager::{in_memory::MemoryConnManager, ConnectionBridge},
    message::Message,
    operations::join_ring,
    NodeConfig, PeerKey,
};

use super::op_state::OpStateStorage;

pub(super) struct InMemory {
    peer: PeerKey,
    listening: bool,
    conn_manager: MemoryConnManager,
    op_storage: OpStateStorage,
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
        })
    }

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
                    // old:
                    Message::ProbeRequest(_, _) => todo!(),
                    Message::ProbeResponse(_, _) => todo!(),
                    Message::Canceled(_) => todo!(),
                },
                Err(_) => break Err(()),
            }
        }
    }
}
