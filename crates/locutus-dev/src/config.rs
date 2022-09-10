use std::path::PathBuf;

use crate::local_node::LocalNodeCliConfig;

#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Development Environment")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
pub struct Config {
    #[clap(subcommand)]
    pub sub_command: SubCommand,
    #[clap(flatten)]
    pub additional: BaseConfig,
}

#[derive(clap::Parser, Clone)]
pub struct BaseConfig {
    /// Overrides the default data directory where Locutus files are stored.
    pub(crate) data_dir: Option<PathBuf>,
}

#[derive(clap::Subcommand, Clone)]
pub enum SubCommand {
    RunLocal(LocalNodeCliConfig),
    Build(BuildToolCliConfig),
    New(NewPackageCliConfig),
    Publish(PutConfig),
    Execute(RunCliConfig),
}

#[derive(clap::Parser, Clone)]
pub struct RunCliConfig {
    #[clap(subcommand)]
    pub command: NodeCommand,
}

#[derive(clap::Subcommand, Clone)]
pub enum NodeCommand {
    Put(PutConfig),
    Update(UpdateConfig),
}

#[derive(clap::Parser, Clone)]
pub struct UpdateConfig {
    pub(crate) key: String,
    pub(crate) delta: PathBuf,
    pub(crate) release: bool,
}

#[derive(clap::Parser, Clone)]
pub struct PutConfig {
    #[clap(long)]
    pub(crate) code: PathBuf,
    #[clap(long)]
    pub(crate) parameters: Option<PathBuf>,
    #[clap(long)]
    pub(crate) state: PathBuf,
    #[clap(long)]
    pub(crate) release: bool,
}

/// Locutus Build Tool
#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Build Tool")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
pub struct BuildToolCliConfig {}

#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Contract Package Manager")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
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
