//! Serves a new contract so is available for browsing.

use std::{fs::File, io::Read, path::PathBuf, sync::Arc};

use locutus_node::{either::Either, WrappedState};
use locutus_runtime::{ContractCode, WrappedContract};

pub fn test_state() -> Result<WrappedState, std::io::Error> {
    const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(CRATE_DIR).join("examples/encoded_state");
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(WrappedState::new(bytes))
}

pub fn test_contract() -> Result<WrappedContract<'static>, std::io::Error> {
    const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(CRATE_DIR).join("examples/test_web_contract.wasm");
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(WrappedContract::new(
        Arc::new(ContractCode::from(bytes)),
        [].as_ref().into(),
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "local")]
    {
        let state = test_state()?;
        let contract = test_contract()?;
        let mut local_node = http_gw::local_node::config_node().await?;
        if let Err(err) = local_node
            .handle_request(locutus_node::ClientRequest::Put { contract, state })
            .await
        {
            match err {
                Either::Left(err) => log::error!("{err}"),
                Either::Right(err) => log::error!("{err}"),
            }
        }
        http_gw::local_node::set_local_node(local_node).await?;
    }
    #[cfg(not(feature = "local"))]
    {
        panic!("only allowed if local feature is enabled");
    }
    Ok(())
}
