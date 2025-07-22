#![allow(clippy::unbuffered_bytes)]
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
use serde::{Deserialize, Serialize};

use crate::util::workspace::get_workspace_target_dir;

pub fn with_tracing<T>(f: impl FnOnce() -> T) -> T {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_line_number(true)
        .with_file(true)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::with_default(subscriber, f)
}

pub async fn make_put(
    client: &mut WebApi,
    state: WrappedState,
    contract: ContractContainer,
    subscribe: bool,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Put {
            contract: contract.clone(),
            state: state.clone(),
            related_contracts: RelatedContracts::default(),
            subscribe,
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

pub async fn make_subscribe(client: &mut WebApi, key: ContractKey) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Subscribe {
            key,
            summary: None,
        }))
        .await?;
    Ok(())
}

pub async fn make_get(
    client: &mut WebApi,
    key: ContractKey,
    return_contract_code: bool,
    subscribe: bool,
) -> anyhow::Result<()> {
    client
        .send(ClientRequest::ContractOp(ContractRequest::Get {
            key,
            return_contract_code,
            subscribe,
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

pub fn load_delegate(name: &str, params: Parameters<'static>) -> anyhow::Result<DelegateContainer> {
    let delegate_bytes = compile_delegate(name)?;
    let delegate_code = DelegateCode::from(delegate_bytes);
    let delegate = Delegate::from((&delegate_code, &params));
    let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate));
    Ok(delegate)
}

// TODO: refactor so we share the implementation with fdev (need to extract to )
fn compile_contract(name: &str) -> anyhow::Result<Vec<u8>> {
    let contract_path = {
        const CRATE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/");
        let contracts = PathBuf::from(CRATE_DIR);
        contracts.join(name)
    };

    println!("module path: {contract_path:?}");
    let target = get_workspace_target_dir();
    println!(
        "trying to compile the test contract, target: {}",
        target.display()
    );

    compile_rust_wasm_lib(
        &BuildToolConfig {
            features: None,
            package_type: PackageType::Contract,
            debug: true,
        },
        &contract_path,
    )?;

    let output_file = target
        .join(WASM_TARGET)
        .join("debug")
        .join(name.replace('-', "_"))
        .with_extension("wasm");
    println!("output file: {output_file:?}");
    Ok(std::fs::read(output_file)?)
}

fn compile_delegate(name: &str) -> anyhow::Result<Vec<u8>> {
    let delegate_path = {
        const CRATE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tests/");
        let delegates = PathBuf::from(CRATE_DIR);
        delegates.join(name)
    };

    println!("delegate path: {delegate_path:?}");

    // Check if the delegate directory exists
    if !delegate_path.exists() {
        return Err(anyhow::anyhow!(
            "Delegate directory does not exist: {delegate_path:?}"
        ));
    }

    let target = get_workspace_target_dir();
    println!(
        "trying to compile the test delegate, target: {}",
        target.display()
    );

    compile_rust_wasm_lib(
        &BuildToolConfig {
            features: None,
            package_type: PackageType::Delegate,
            debug: false,
        },
        &delegate_path,
    )?;

    let output_file = target
        .join(WASM_TARGET)
        .join("release")
        .join(name.replace('-', "_"))
        .with_extension("wasm");
    println!("output file: {output_file:?}");

    // Check if output file exists before reading
    if !output_file.exists() {
        return Err(anyhow::anyhow!(
            "Compiled WASM file not found at: {output_file:?}"
        ));
    }

    let wasm_data = std::fs::read(&output_file)
        .map_err(|e| anyhow::anyhow!("Failed to read output file {output_file:?}: {e}"))?;
    println!("WASM size: {} bytes", wasm_data.len());

    Ok(wasm_data)
}

pub const WASM_TARGET: &str = "wasm32-unknown-unknown";

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

pub fn compile_rust_wasm_lib(cli_config: &BuildToolConfig, work_dir: &Path) -> anyhow::Result<()> {
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

    // Set CARGO_TARGET_DIR if not already set to ensure consistent output location
    let mut command = Command::new("cargo");
    if std::env::var("CARGO_TARGET_DIR").is_err() {
        command.env("CARGO_TARGET_DIR", get_workspace_target_dir());
    }

    let child = command
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
    pub features: Option<String>,

    // /// Compile the contract or delegate with a specific API version.
    // #[arg(long, value_parser = parse_version, default_value_t=Version::new(0, 0, 1))]
    // pub(crate) version: Version,
    /// Output object type.
    #[arg(long, value_enum, default_value_t=PackageType::default())]
    pub package_type: PackageType,

    /// Compile in debug mode instead of release.
    #[arg(long)]
    pub debug: bool,
}

#[derive(Default, Debug, Clone, Copy, ValueEnum)]
pub enum PackageType {
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

// Test data structures for contract operations

/// Data model representing a todo list for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TodoList {
    /// List of tasks
    pub tasks: Vec<Task>,
    /// State version for concurrency control
    pub version: u64,
}

/// Data model representing a task for testing
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Task {
    /// Unique task identifier
    pub id: u64,
    /// Task title
    pub title: String,
    /// Task description
    pub description: String,
    /// Completion status
    pub completed: bool,
    /// Priority (1-5, where 5 is highest)
    pub priority: u8,
}

/// Operations that can be performed on tasks
#[derive(Serialize, Deserialize, Debug)]
pub enum TodoOperation {
    /// Add a new task
    Add(Task),
    /// Update an existing task
    Update(Task),
    /// Remove a task by ID
    Remove(u64),
    /// Mark a task as completed
    Complete(u64),
}

/// Creates an empty todo list for testing
pub fn create_empty_todo_list() -> Vec<u8> {
    let todo_list = TodoList {
        tasks: Vec::new(),
        version: 0,
    };

    serde_json::to_vec(&todo_list).unwrap_or_default()
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_compile_contract() -> testresult::TestResult {
        let contract = compile_contract("test-contract-integration")?;
        assert!(!contract.is_empty());
        Ok(())
    }
}
