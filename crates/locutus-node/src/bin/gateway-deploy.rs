use anyhow::anyhow;
use libp2p::identity::Keypair;
use locutus_node::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let key = Keypair::generate_ed25519();
    let mut config = NodeConfig::new([Box::new(Temp)]);
    config.with_key(key);
    let node = config.build()?;
    node.run().await.map_err(|_| anyhow!("failed to start"))
}

struct Temp;

#[async_trait::async_trait]
impl ClientEventsProxy for Temp {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    async fn recv(&mut self) -> Result<(ClientId, ClientRequest), ClientError> {
        todo!()
    }

    /// Sends a response from the host to the client application.
    async fn send(
        &mut self,
        id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Result<(), ClientError> {
        todo!()
    }

    fn cloned(&self) -> BoxedClient {
        todo!()
    }
}
