use futures::future::BoxFuture;

use crate::{
    message::NetMessage,
    node::{testing_impl::NetworkBridgeExt, PeerId},
};

use super::{ConnectionError, NetworkBridge};

#[derive(Clone)]
pub(in crate::node) struct InterProcessConnManager {}

impl InterProcessConnManager {
    pub fn new() -> Self {
        Self {}
    }
}

impl NetworkBridgeExt for InterProcessConnManager {
    fn recv(&mut self) -> BoxFuture<Result<NetMessage, ConnectionError>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl NetworkBridge for InterProcessConnManager {
    async fn send(&self, target: &PeerId, msg: NetMessage) -> super::ConnResult<()> {
        todo!()
    }

    async fn add_connection(&mut self, _peer: PeerId) -> super::ConnResult<()> {
        Ok(())
    }

    async fn drop_connection(&mut self, _peer: &PeerId) -> super::ConnResult<()> {
        Ok(())
    }
}
