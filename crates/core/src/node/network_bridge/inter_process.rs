use crate::{dev_tool::PeerKey, message::NetMessage};

use super::NetworkBridge;

pub(in crate::node) struct InterProcessConnManager {}

#[async_trait::async_trait]
impl NetworkBridge for InterProcessConnManager {
    async fn send(&self, target: &PeerKey, msg: NetMessage) -> super::ConnResult<()> {
        todo!()
    }

    async fn add_connection(&mut self, _peer: PeerKey) -> super::ConnResult<()> {
        Ok(())
    }

    async fn drop_connection(&mut self, _peer: &PeerKey) -> super::ConnResult<()> {
        Ok(())
    }
}
