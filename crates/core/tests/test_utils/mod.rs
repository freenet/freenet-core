use std::{
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use clap::ValueEnum;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, WebApi},
    prelude::*,
};
use testresult::TestResult;

const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";

pub async fn make_put(
    client: &mut WebApi,
    state: WrappedState,
    contract: ContractContainer,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: contract.clone(),
            state: state.clone(),
            related_contracts: RelatedContracts::default(),
        }))
        .await?;
    Ok(())
}

pub async fn make_update(
    client: &mut WebApi,
    key: ContractKey,
    state: WrappedState,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Update {
            key,
            data: UpdateData::State(State::from(state)),
        }))
        .await?;
    Ok(())
}

pub async fn make_get(
    client: &mut WebApi,
    key: ContractKey,
    return_contract_code: bool,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code,
        }))
        .await?;
    Ok(())
}

pub fn load_contract(name: &str, params: Parameters<'static>) -> anyhow::Result<ContractContainer> {
    let contract_bytes = WrappedContract::new(
        Arc::new(ContractCode::from(compile_contract(name)?)),
        params,
    );
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_bytes));
    Ok(contract)
}

// TODO: refactor so we share the implementation with fdev (need to extract to )
fn compile_contract(name: &str) -> anyhow::Result<Vec<u8>> {
    let contract_path = {
        const CRATE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/");
        let contracts = PathBuf::from(CRATE_DIR);
        contracts.join(name)
    };

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
        &contract_path,
    )?;

    let output_file = Path::new(&target)
        .join(WASM_TARGET)
        .join("debug")
        .join(name.replace('-', "_"))
        .with_extension("wasm");
    println!("output file: {output_file:?}");
    Ok(std::fs::read(output_file)?)
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
        let mut stderr = io::stderr();
        for b in c_stderr.bytes() {
            let b = b?;
            stderr.write_all(&[b])?;
        }
        Ok(())
    };

    let write_child_stdout = move || -> anyhow::Result<()> {
        let mut stdout = io::stdout();
        for b in c_stdout.bytes() {
            let b = b?;
            stdout.write_all(&[b])?;
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

pub async fn verify_contract_exists(dir: &Path, key: ContractKey) -> anyhow::Result<bool> {
    let code_hash = key.encoded_code_hash().unwrap_or_else(|| {
        panic!("Contract key does not have a code hash");
    });
    let contract_path = dir.join("contracts").join(code_hash);
    Ok(tokio::fs::metadata(contract_path).await.is_ok())
}

#[test]
fn test_compile_contract() -> TestResult {
    let contract = compile_contract("test-contract-integration")?;
    assert!(!contract.is_empty());
    Ok(())
}