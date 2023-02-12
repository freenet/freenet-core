use std::path::PathBuf;

use crate::local_node::LocalNodeCliConfig;
use locutus_core::OperationMode;
use locutus_stdlib::prelude::Version;

#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Development Tool")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.3")]
pub struct Config {
    #[clap(subcommand)]
    pub sub_command: SubCommand,
    #[clap(flatten)]
    pub additional: BaseConfig,
}

#[derive(clap::Parser, Clone)]
pub struct BaseConfig {
    /// Overrides the default data directory where Locutus contract files are stored.
    pub(crate) contract_data_dir: Option<PathBuf>,
    /// Node operation mode.
    #[clap(value_enum, default_value_t=OperationMode::Local)]
    pub(crate) mode: OperationMode,
}

#[derive(clap::Subcommand, Clone)]
pub enum SubCommand {
    RunLocal(LocalNodeCliConfig),
    Build(BuildToolCliConfig),
    New(NewPackageCliConfig),
    Publish(PutConfig),
    Execute(RunCliConfig),
}

/// Node CLI
///
/// This tool allows the execution of commands against the local node
/// and is intended to be used for development and automated workflows.
#[derive(clap::Parser, Clone)]
pub struct RunCliConfig {
    /// Command to execute.
    #[clap(subcommand)]
    pub command: NodeCommand,
}

#[derive(clap::Subcommand, Clone)]
pub enum NodeCommand {
    Put(PutConfig),
    Update(UpdateConfig),
}

/// Updates a contract in the network.
#[derive(clap::Parser, Clone)]
pub struct UpdateConfig {
    /// Contract id of the contract being updated in Base58 format.
    pub(crate) key: String,
    /// A path to the update/delta being pushed to the contract.
    pub(crate) delta: PathBuf,
    /// Whether this contract will be updated in the network or is just a dry run
    /// to be executed in local mode only. By default puts are performed in local.
    pub(crate) release: bool,
}

/// Publishes a new contract to the network.
#[derive(clap::Parser, Clone)]
pub struct PutConfig {
    /// A path to the compiled WASM code file.
    #[clap(long)]
    pub(crate) code: PathBuf,
    /// A path to the file parameters for the contract. If not specified, the contract
    /// will be published with empty parameters.
    #[clap(long)]
    pub(crate) parameters: Option<PathBuf>,
    /// A path to the initial state for the contract being published.
    #[clap(long)]
    pub(crate) state: PathBuf,
    /// Whether this contract will be released into the network or is just a dry run
    /// to be executed in local mode only. By default puts are performed in local.
    #[clap(long)]
    pub(crate) release: bool,
    /// A path to a JSON file listing the related contracts.
    #[clap(long)]
    pub(crate) related_contracts: Option<PathBuf>,
}

/// Builds and packages a contract.
///
/// This tool will build the WASM contract and publish it to the network.
#[derive(clap::Parser, Clone, Debug)]
pub struct BuildToolCliConfig {
    /// Compile the contract with WASI extension enabled (useful for debugging).
    #[clap(long)]
    pub(crate) wasi: bool,

    /// Compile the contract with a specific version.
    #[clap(long, value_parser = parse_version, default_value_t=Version::new(0, 0, 1))]
    pub(crate) version: Version,
}

impl Default for BuildToolCliConfig {
    fn default() -> Self {
        Self {
            wasi: false,
            version: Version::new(0, 0, 1),
        }
    }
}

fn parse_version(src: &str) -> Result<Version, String> {
    Version::parse(src).map_err(|e| e.to_string())
}

/// Create a new Locutus contract and/or app.
#[derive(clap::Parser, Clone)]
pub struct NewPackageCliConfig {
    #[clap(id = "type", value_enum)]
    pub(crate) kind: ContractKind,
}

#[derive(clap::ValueEnum, Clone)]
pub(crate) enum ContractKind {
    /// A web app container contract.
    WebApp,
    /// An standard contract.
    Contract,
}
