use anyhow::anyhow;
use libp2p::identity::Keypair;
use locutus_node::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let key = Keypair::generate_ed25519();
    let mut config = NodeConfig::default();
    config.with_key(key);
    let node = config.build()?;

    let user_events = UserEvents;
    node.run(user_events)
        .await
        .map_err(|_| anyhow!("failed to start"))
}

struct UserEvents;

#[async_trait::async_trait]
impl ClientEventsProxy for UserEvents {
    type Error = String;

    async fn recv(&mut self) -> Result<ClientRequest, Self::Error> {
        todo!()
    }

    async fn send(&mut self, _response: HostResponse) -> Result<HostResponse, Self::Error> {
        todo!()
    }
}
