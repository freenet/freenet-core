//! Serves a new contract so is available for browsing.

use std::{
    fs::File,
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};

use locutus_core::locutus_runtime::{ContractContainer, WasmAPIVersion};
use locutus_core::{
    libp2p::identity::ed25519::PublicKey,
    locutus_runtime::{ContractCode, StateStore, WrappedContract},
    Config, SqlitePool, WrappedState,
};
use serde::Serialize;
use tracing::metadata::LevelFilter;
use tracing_subscriber::EnvFilter;

const MAX_SIZE: i64 = 10 * 1024 * 1024;
const MAX_MEM_CACHE: u32 = 10_000_000;
const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");

struct WebBundle {
    posts_contract: ContractContainer,
    posts_state: WrappedState,
    web_contract: ContractContainer,
    web_state: WrappedState,
}

fn test_web(public_key: PublicKey) -> Result<WebBundle, std::io::Error> {
    fn get_posts_contract(
        _public_key: PublicKey,
    ) -> std::io::Result<(ContractContainer, WrappedState)> {
        let path = PathBuf::from(CRATE_DIR).join("examples/freenet_microblogging_posts.wasm");
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;

        #[derive(Serialize)]
        struct Verification {
            public_key: Vec<u8>,
        }
        let params = serde_json::to_vec(&Verification { public_key: vec![] }).unwrap();
        let contract = ContractContainer::Wasm(WasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(bytes)),
            params.into(),
        )));

        let path = PathBuf::from(CRATE_DIR).join("examples/freenet_microblogging_posts");
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;

        Ok((contract, bytes.into()))
    }

    fn get_web_contract() -> std::io::Result<(ContractContainer, WrappedState)> {
        let path = PathBuf::from(CRATE_DIR).join("examples/freenet_microblogging_web.wasm");
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;

        let contract = ContractContainer::Wasm(WasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(bytes)),
            [].as_ref().into(),
        )));

        let path = PathBuf::from(CRATE_DIR).join("examples/freenet_microblogging_web");
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;

        Ok((contract, bytes.into()))
    }

    let (posts_contract, initial_state) = get_posts_contract(public_key)?;
    let (web_contract, web_content) = get_web_contract()?;

    Ok(WebBundle {
        posts_contract,
        posts_state: initial_state,
        web_contract,
        web_state: web_content,
    })
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use locutus::HttpGateway;
    use locutus_core::{
        libp2p::identity::ed25519::Keypair, locutus_runtime::ContractStore, Executor,
    };

    let keypair = Keypair::generate();
    let bundle = test_web(keypair.public())?;
    log::info!(
        "loading web contract {} in local node",
        bundle.web_contract.key().encoded_contract_id()
    );
    log::info!(
        "loading posts contract {} in local node",
        bundle.posts_contract.key().encoded_contract_id()
    );

    let contract_dir = Config::get_conf().config_paths.local_contracts_dir();
    let contract_store = ContractStore::new(contract_dir, MAX_SIZE)?;
    let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
    let mut local_node = Executor::new(contract_store, state_store, || {
        locutus_core::util::set_cleanup_on_exit().unwrap();
    })
    .await?;
    let id = HttpGateway::next_client_id();
    local_node
        .preload(
            id,
            bundle.posts_contract,
            bundle.posts_state,
            Default::default(),
        )
        .await;
    local_node
        .preload(
            id,
            bundle.web_contract,
            bundle.web_state,
            Default::default(),
        )
        .await;
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50509);
    locutus::local_node::run_local_node(local_node, socket).await
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_level(true)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(run())?;

    Ok(())
}
