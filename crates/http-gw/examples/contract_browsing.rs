//! Serves a new contract so is available for browsing.

use std::{fs::File, io::Read, path::PathBuf, sync::Arc};

use locutus_node::{SqlitePool, WrappedState};
use locutus_runtime::{ContractCode, StateDelta, StateStore, WrappedContract};
use rand::{prelude::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use warp::Rejection;

const MAX_SIZE: i64 = 10 * 1024 * 1024;
const MAX_MEM_CACHE: u32 = 10_000_000;

pub fn test_state() -> Result<WrappedState, std::io::Error> {
    const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(CRATE_DIR).join("examples/encoded_state");
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    Ok(WrappedState::new(bytes))
}

pub fn test_contract(
    public_key: ed25519_dalek::PublicKey,
) -> Result<WrappedContract<'static>, std::io::Error> {
    const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from(CRATE_DIR).join("examples/test_web_contract.wasm");
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;

    #[derive(Serialize)]
    struct Verification {
        public_key: ed25519_dalek::PublicKey,
    }
    let params = serde_json::to_vec(&Verification { public_key }).unwrap();

    Ok(WrappedContract::new(
        Arc::new(ContractCode::from(bytes)),
        params.into(),
    ))
}

#[derive(Serialize, Deserialize)]
struct Message {
    author: String,
    date: chrono::DateTime<chrono::Utc>,
    title: String,
    content: String,
    #[serde(default = "Message::modded")]
    mod_msg: bool,
    signature: Option<ed25519_dalek::Signature>,
}

impl Message {
    fn modded() -> bool {
        false
    }
}

// TODO: we probably need a new function in the contract that
//       allows specifying this hook actions before sending a message
fn sign_up(raw_delta: Vec<u8>) -> Result<StateDelta<'static>, Rejection> {
    use ed25519_dalek::Signer;
    let messages: Vec<Message> = serde_json::from_slice(&raw_delta).unwrap();
    let mut rng = StdRng::from_seed([1u8; 32]);
    let keypair = ed25519_dalek::Keypair::generate(&mut rng);
    let mut r = Vec::with_capacity(messages.len());
    for mut m in messages {
        let signature = keypair.sign(m.content.as_bytes());
        m.signature = Some(signature);
        r.push(m);
    }
    Ok(serde_json::to_vec(&r).unwrap().into())
}

#[cfg(feature = "local")]
async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use http_gw::HttpGateway;
    use locutus_dev::ContractStore;

    let mut rng = StdRng::from_seed([1u8; 32]);
    let keypair = ed25519_dalek::Keypair::generate(&mut rng);

    let state = test_state()?;
    let contract = test_contract(keypair.public)?;

    let mut package = http_gw::unpack_state(state.as_ref(), contract.key())?;
    // fixme: a workaround
    HttpGateway::store_web(&mut package, contract.key())?;
    let state = package.state;
    log::info!("loading contract {} in local node", contract.key().encode());
    log::info!(
        "initializing state with content:\n{}",
        serde_json::from_slice::<serde_json::Value>(state.as_ref())?
    );
    let tmp_path = std::env::temp_dir().join("locutus");
    let contract_store = ContractStore::new(tmp_path.join("contracts"), MAX_SIZE);
    let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
    let mut local_node =
        locutus_dev::LocalNode::new(contract_store.clone(), state_store.clone()).await?;
    local_node.preload(contract, state).await;
    http_gw::local_node::set_local_node(local_node, sign_up).await
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(not(feature = "local"))]
    {
        panic!("only allowed if local feature is enabled");
    }
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    // env_logger::Builder::from_default_env()
    //     .format_module_path(true)
    //     .filter_level(log::LevelFilter::Info)
    //     .init();

    #[allow(unused_variables)]
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    #[cfg(feature = "local")]
    {
        rt.block_on(run())?;
    }

    Ok(())
}
