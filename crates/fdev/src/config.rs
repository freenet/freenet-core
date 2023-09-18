use std::{fmt::Display, path::PathBuf};

use crate::{commands::PutType, local_node::LocalNodeCliConfig};
use clap::ValueEnum;
use freenet_core::OperationMode;
use semver::Version;

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
    #[arg(long)]
    pub(crate) contract_data_dir: Option<PathBuf>,
    /// Overrides the default data directory where Locutus delegate files are stored.
    #[arg(long)]
    pub(crate) delegate_data_dir: Option<PathBuf>,
    /// Overrides the default data directory where Locutus secret files are stored.
    #[arg(long)]
    pub(crate) secret_data_dir: Option<PathBuf>,
    /// Node operation mode.
    #[arg(value_enum, default_value_t=OperationMode::Local)]
    pub(crate) mode: OperationMode,
}

#[derive(clap::Subcommand, Clone)]
pub enum SubCommand {
    RunLocal(LocalNodeCliConfig),
    Build(BuildToolCliConfig),
    New(NewPackageCliConfig),
    Publish(PutConfig),
    Execute(RunCliConfig),
    Inspect(crate::inspect::InspectCliConfig),
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

/// Publishes a new contract or delegate to the network.
// todo: make some of this options exclusive depending on the value of `package_type`
#[derive(clap::Parser, Clone)]
pub struct PutConfig {
    /// A path to the compiled WASM code file. This must be a valid packaged contract or component,
    /// (built using the `ldt` tool). Not an arbitrary WASM file.
    #[arg(long)]
    pub(crate) code: PathBuf,
    /// A path to the file parameters for the contract/delegate. If not specified, will be published
    /// with empty parameters.
    #[arg(long)]
    pub(crate) parameters: Option<PathBuf>,
    /// Whether this contract will be released into the network or is just a dry run
    /// to be executed in local mode only. By default puts are performed in local.
    #[arg(long)]
    pub(crate) release: bool,
    /// Type of put to perform.
    #[clap(subcommand)]
    pub(crate) package_type: PutType,
}

/// Builds and packages a contract.
///
/// This tool will build the WASM contract or delegate and publish it to the network.
#[derive(clap::Parser, Clone, Debug)]
pub struct BuildToolCliConfig {
    /// Compile the contract or delegate with specific features.
    #[arg(long)]
    pub(crate) features: Option<String>,

    /// Compile the contract or delegate with a specific API version.
    #[arg(long, value_parser = parse_version, default_value_t=Version::new(0, 0, 1))]
    pub(crate) version: Version,

    #[arg(long, value_enum, default_value_t=PackageType::default())]
    pub(crate) package_type: PackageType,
}

#[derive(Default, Debug, Clone, Copy, ValueEnum)]
pub(crate) enum PackageType {
    #[default]
    Contract,
    Delegate,
}

impl Display for PackageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackageType::Contract => write!(f, "contract"),
            PackageType::Delegate => write!(f, "delegate"),
        }
    }
}

impl Default for BuildToolCliConfig {
    fn default() -> Self {
        Self {
            features: None,
            version: Version::new(0, 0, 1),
            package_type: PackageType::default(),
        }
    }
}

fn parse_version(src: &str) -> Result<Version, String> {
    Version::parse(src).map_err(|e| e.to_string())
}

/// Create a new Locutus contract and/or app.
#[derive(clap::Parser, Clone)]
pub struct NewPackageCliConfig {
    #[arg(id = "type", value_enum)]
    pub(crate) kind: ContractKind,
}

#[derive(clap::ValueEnum, Clone)]
pub(crate) enum ContractKind {
    /// A web app container contract.
    WebApp,
    /// An standard contract.
    Contract,
}
