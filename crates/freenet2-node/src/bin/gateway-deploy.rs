use libp2p::identity::Keypair;
use locutus_node::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = Keypair::generate_ed25519();
    let mut node = NodeConfig::default().with_key(key).build_libp2p()?;
    node.listen_on().await.map_err(|_| "failed to start".into())
}
