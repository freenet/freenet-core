use freenet2_node::*;
use libp2p::identity::Keypair;

#[tokio::main]
async fn main() -> Result<(), impl std::error::Error> {
    let key = Keypair::generate_ed25519();
    let loc = Location::random();
    let peer = PeerKey::from(key.public());
    let mut conn_manager = MemoryConnManager::new(true, peer, Some(loc));
    conn_manager.start()
}
