use libp2p::identity::Keypair;
use locutus_node::*;

#[tokio::test(flavor = "multi_thread")]
async fn start_node() -> Result<(), Box<dyn std::error::Error>> {
    let key = Keypair::generate_ed25519();
    let mut node = NodeConfig::default().with_key(key).build_in_memory()?;
    node.listen_on().map_err(|_| "failed to start".into())
}
