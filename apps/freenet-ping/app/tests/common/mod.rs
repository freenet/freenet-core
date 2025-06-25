#![allow(unused)]
use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use freenet_ping_app::ping_client::{
    wait_for_get_response, wait_for_put_response, wait_for_subscribe_response,
};
use freenet_ping_types::{Ping, PingContractOptions};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse, WebApi},
    prelude::*,
};
use futures::{future::BoxFuture, FutureExt};
use rand::{random, Rng, SeedableRng};
use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::{
    collections::HashSet,
    io,
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};
use tokio::{select, time::sleep};
use tokio_tungstenite::connect_async;
use tracing::{info, span, Instrument, Level};

use serde::{Deserialize, Serialize};

const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";

pub static RNG: once_cell::sync::Lazy<Mutex<rand::rngs::StdRng>> =
    once_cell::sync::Lazy::new(|| {
        Mutex::new(rand::rngs::StdRng::from_seed(
            *b"0102030405060708090a0b0c0d0e0f10",
        ))
    });

#[derive(Debug)]
pub struct PresetConfig {
    pub temp_dir: tempfile::TempDir,
}

pub fn get_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

pub fn get_free_socket_addr() -> Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::await_holding_lock)]
pub async fn base_node_test_config(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
    ws_api_port: u16,
    data_dir_suffix: &str,
    base_tmp_dir: Option<&Path>,
    blocked_addresses: Option<Vec<SocketAddr>>,
) -> Result<(ConfigArgs, PresetConfig)> {
    let mut rng = RNG.lock().unwrap();
    base_node_test_config_with_rng(
        is_gateway,
        gateways,
        public_port,
        ws_api_port,
        data_dir_suffix,
        base_tmp_dir,
        blocked_addresses,
        &mut rng,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn base_node_test_config_with_rng(
    is_gateway: bool,
    gateways: Vec<String>,
    public_port: Option<u16>,
    ws_api_port: u16,
    data_dir_suffix: &str,
    base_tmp_dir: Option<&Path>,
    blocked_addresses: Option<Vec<SocketAddr>>,
    rng: &mut rand::rngs::StdRng,
) -> Result<(ConfigArgs, PresetConfig)> {
    if is_gateway {
        assert!(public_port.is_some());
    }

    let temp_dir = if let Some(base) = base_tmp_dir {
        tempfile::tempdir_in(base)?
    } else {
        tempfile::Builder::new().prefix(data_dir_suffix).tempdir()?
    };

    let key = TransportKeypair::new_with_rng(rng);
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;
    key.public().save(temp_dir.path().join("public.pem"))?;

    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_api_port),
        },
        network_api: NetworkArgs {
            public_address: Some(Ipv4Addr::LOCALHOST.into()),
            public_port,
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(gateways),
            location: Some(rng.gen()),
            ignore_protocol_checking: true,
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: public_port, // if None, node will pick a free one or use default
            bandwidth_limit: None,
            blocked_addresses,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir.path().to_path_buf()),
            data_dir: Some(temp_dir.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };
    Ok((config, PresetConfig { temp_dir }))
}

pub fn gw_config_from_path(port: u16, path: &Path) -> Result<InlineGwConfig> {
    gw_config_from_path_with_rng(port, path, &mut RNG.lock().unwrap())
}

pub fn gw_config_from_path_with_rng(
    port: u16,
    path: &Path,
    rng: &mut rand::rngs::StdRng,
) -> Result<InlineGwConfig> {
    Ok(InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, port).into(),
        location: Some(rng.gen()),
        public_key_path: path.join("public.pem"),
    })
}

pub fn ping_states_equal(a: &Ping, b: &Ping) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for key in a.keys() {
        if !b.contains_key(key) {
            return false;
        }
    }
    true
}

pub const PACKAGE_DIR: &str = env!("CARGO_MANIFEST_DIR");
pub const PATH_TO_CONTRACT: &str = "../contracts/ping";
const WASM_FILE_NAME: &str = "freenet-ping-contract";
pub const APP_TAG: &str = "ping-app";

pub async fn connect_ws_client(ws_port: u16) -> Result<WebApi> {
    let uri = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        ws_port
    );
    let (stream, _) = connect_async(&uri).await?;
    Ok(WebApi::start(stream))
}

/// Builds and packages a contract or delegate.
///
/// This tool will build the WASM contract or delegate and publish it to the network.
#[derive(clap::Parser, Clone, Debug)]
pub struct BuildToolConfig {
    /// Compile the contract or delegate with specific features.
    #[arg(long)]
    pub(crate) features: Option<String>,

    // /// Compile the contract or delegate with a specific API version.
    // #[arg(long, value_parser = parse_version, default_value_t=Version::new(0, 0, 1))]
    // pub(crate) version: Version,
    /// Output object type.
    #[arg(long, value_enum, default_value_t=PackageType::default())]
    pub(crate) package_type: PackageType,

    /// Compile in debug mode instead of release.
    #[arg(long)]
    pub(crate) debug: bool,
}

#[derive(Default, Debug, Clone, Copy, ValueEnum)]
pub(crate) enum PackageType {
    #[default]
    Contract,
    Delegate,
}

impl PackageType {
    pub fn feature(&self) -> &'static str {
        match self {
            PackageType::Contract => "freenet-main-contract",
            PackageType::Delegate => "freenet-main-delegate",
        }
    }
}

impl std::fmt::Display for PackageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackageType::Contract => write!(f, "contract"),
            PackageType::Delegate => write!(f, "delegate"),
        }
    }
}

pub fn load_contract(
    contract_path: &PathBuf,
    params: Parameters<'static>,
) -> anyhow::Result<ContractContainer> {
    let contract_bytes = WrappedContract::new(
        Arc::new(ContractCode::from(compile_contract(contract_path)?)),
        params,
    );
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_bytes));
    Ok(contract)
}

const WASM_TARGET: &str = "wasm32-unknown-unknown";
fn compile_options(cli_config: &BuildToolConfig) -> impl Iterator<Item = String> {
    let release: &[&str] = if cli_config.debug {
        &[]
    } else {
        &["--release"]
    };
    let feature_list = cli_config
        .features
        .iter()
        .flat_map(|s| {
            s.split(',')
                .filter(|p| *p != cli_config.package_type.feature())
        })
        .chain([cli_config.package_type.feature()]);
    let features = [
        "--features".to_string(),
        feature_list.collect::<Vec<_>>().join(","),
    ];
    features
        .into_iter()
        .chain(release.iter().map(|s| s.to_string()))
}
// TODO: refactor so we share the implementation with fdev (need to extract to )
fn compile_contract(contract_path: &PathBuf) -> anyhow::Result<Vec<u8>> {
    println!("module path: {contract_path:?}");
    let target = std::env::var(TARGET_DIR_VAR)
        .map_err(|_| anyhow::anyhow!("CARGO_TARGET_DIR should be set"))?;
    println!("trying to compile the test contract, target: {target}");

    compile_rust_wasm_lib(
        &BuildToolConfig {
            features: None,
            package_type: PackageType::Contract,
            debug: true,
        },
        contract_path,
    )?;

    let output_file = Path::new(&target)
        .join(WASM_TARGET)
        .join("debug")
        .join(WASM_FILE_NAME.replace('-', "_"))
        .with_extension("wasm");
    println!("output file: {output_file:?}");
    Ok(std::fs::read(output_file)?)
}

fn compile_rust_wasm_lib(cli_config: &BuildToolConfig, work_dir: &Path) -> anyhow::Result<()> {
    const RUST_TARGET_ARGS: &[&str] = &["build", "--lib", "--target"];
    use std::io::IsTerminal;
    let comp_opts = compile_options(cli_config).collect::<Vec<_>>();
    let cmd_args = if std::io::stdout().is_terminal() && std::io::stderr().is_terminal() {
        RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([WASM_TARGET, "--color", "always"])
            .chain(comp_opts.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
    } else {
        RUST_TARGET_ARGS
            .iter()
            .copied()
            .chain([WASM_TARGET])
            .chain(comp_opts.iter().map(|s| s.as_str()))
            .collect::<Vec<_>>()
    };

    let package_type = cli_config.package_type;
    println!("Compiling {package_type} with rust");
    let child = Command::new("cargo")
        .args(&cmd_args)
        .current_dir(work_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            eprintln!("Error while executing cargo command: {e}");
            anyhow::anyhow!("Error while executing cargo command: {e}")
        })?;
    pipe_std_streams(child)?;
    Ok(())
}

pub(crate) fn pipe_std_streams(mut child: Child) -> anyhow::Result<()> {
    let c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let c_stderr = child.stderr.take().expect("Failed to open command stderr");

    let write_child_stderr = move || -> anyhow::Result<()> {
        use std::io::BufRead;
        let mut stderr = io::stderr();
        let reader = std::io::BufReader::new(c_stderr);
        for line in reader.lines() {
            let line = line?;
            writeln!(stderr, "{}", line)?;
        }
        Ok(())
    };

    let write_child_stdout = move || -> anyhow::Result<()> {
        use std::io::BufRead;
        let mut stdout = io::stdout();
        let reader = std::io::BufReader::new(c_stdout);
        for line in reader.lines() {
            let line = line?;
            writeln!(stdout, "{}", line)?;
        }
        Ok(())
    };
    std::thread::spawn(write_child_stdout);
    std::thread::spawn(write_child_stderr);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    anyhow::bail!("exit with status: {status}");
                }
                break;
            }
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(())
}

pub async fn deploy_contract(
    client: &mut WebApi,
    initial_ping_state: Ping,
    options: &PingContractOptions,
    subscribe: bool,
) -> Result<ContractKey> {
    let path_to_code = PathBuf::from(PACKAGE_DIR).join(PATH_TO_CONTRACT);
    let code = std::fs::read(path_to_code)?;
    let params = Parameters::from(serde_json::to_vec(options)?);
    let container = ContractContainer::try_from((code, &params))?;
    let contract_key = container.key();

    let wrapped_state = WrappedState::new(serde_json::to_vec(&initial_ping_state)?);

    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: container,
            state: wrapped_state,
            related_contracts: RelatedContracts::new(),
            subscribe,
        }))
        .await?;
    wait_for_put_response(client, &contract_key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to deploy contract: {}", e))
}

pub async fn subscribe_to_contract(client: &mut WebApi, key: ContractKey) -> Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key,
            summary: None,
        }))
        .await?;
    wait_for_subscribe_response(client, &key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to subscribe to contract: {}", e))
}

pub async fn get_contract_state(
    client: &mut WebApi,
    key: ContractKey,
    fetch_contract: bool,
) -> Result<Ping> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code: fetch_contract,
            subscribe: false,
        }))
        .await?;
    wait_for_get_response(client, &key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get contract state: {}", e))
}

pub async fn update_contract_state(
    client: &mut WebApi,
    key: ContractKey,
    delta: Ping,
) -> Result<()> {
    let delta_bytes = serde_json::to_vec(&delta)?;
    client
        .send(ClientRequest::ContractOp(ContractRequest::Update {
            key,
            data: UpdateData::Delta(StateDelta::from(delta_bytes)),
        }))
        .await?;
    // Note: Update typically doesn't have a direct response confirming the update itself,
    // propagation is checked by subsequent Gets or via subscription updates.
    Ok(())
}

pub async fn get_all_ping_states(
    client_gw: &mut WebApi,
    client_node1: &mut WebApi,
    client_node2: &mut WebApi,
    key: ContractKey,
) -> Result<(Ping, Ping, Ping)> {
    tracing::debug!("Querying all nodes for current state (key: {})...", key);

    client_gw
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code: false,
            subscribe: false,
        }))
        .await?;

    client_node1
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code: false,
            subscribe: false,
        }))
        .await?;

    client_node2
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code: false,
            subscribe: false,
        }))
        .await?;

    let state_gw = tokio::time::timeout(
        Duration::from_secs(15),
        wait_for_get_response(client_gw, &key),
    )
    .await
    .map_err(|_| anyhow!("Gateway get request timed out"))?;

    let state_node1 = tokio::time::timeout(
        Duration::from_secs(15),
        wait_for_get_response(client_node1, &key),
    )
    .await
    .map_err(|_| anyhow!("Node1 get request timed out"))?;

    let state_node2 = tokio::time::timeout(
        Duration::from_secs(15),
        wait_for_get_response(client_node2, &key),
    )
    .await
    .map_err(|_| anyhow!("Node2 get request timed out"))?;

    let ping_gw = state_gw.map_err(|e| anyhow!("Failed to get gateway state: {}", e))?;
    let ping_node1 = state_node1.map_err(|e| anyhow!("Failed to get node1 state: {}", e))?;
    let ping_node2 = state_node2.map_err(|e| anyhow!("Failed to get node2 state: {}", e))?;

    tracing::debug!(
        "Received states: GW: {:?}, N1: {:?}, N2: {:?}",
        ping_gw.keys(),
        ping_node1.keys(),
        ping_node2.keys()
    );

    Ok((ping_gw, ping_node1, ping_node2))
}
