//! Serves a new contract so is available for browsing.

use std::{fs::File, io::Read, path::PathBuf};

use locutus_node::WrappedState;

pub fn test_state() -> Result<WrappedState, std::io::Error> {
    // todo: package this dinamically
    const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(CRATE_DIR).join("examples/encoded_state");
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(WrappedState::new(bytes))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _test_state = test_state()?;
    // todo: load this state and contract in the local node stores so they are available
    http_gw::local_node::set_local_node().await
}
