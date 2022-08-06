use std::path::PathBuf;

use clap::ArgGroup;

const DEFAULT_MAX_CONTRACT_SIZE: i64 = 50 * 1024 * 1024;

#[derive(clap::ArgEnum, Clone, Copy, Debug)]
pub enum DeserializationFmt {
    #[cfg(feature = "json")]
    Json,
    #[cfg(feature = "messagepack")]
    MessagePack,
}

#[derive(clap::ArgEnum, Clone, Copy, Debug)]
pub enum ContractType {
    View,
    Model,
}

#[derive(clap::Subcommand, Clone)]
pub enum SubCommand {
    RunLocal(LocalNodeConfig),
    Build(StateConfig),
}

#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Development Environment")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
pub struct Config {
    #[clap(subcommand)]
    pub sub_command: SubCommand,
}

/// A CLI utility for testing out contracts against a Locutus local node.
///
#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Local Development Node Environment")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
#[clap(group(
    ArgGroup::new("output")
        .required(true)
        .args(&["output-file", "terminal-output"])
))]
pub struct LocalNodeConfig {
    /// Cleanups all state which was created locally during execution
    #[clap(long, requires = "fmt")]
    pub clean_exit: bool,
    /// Path to the contract to be loaded.
    #[clap(parse(from_os_str))]
    pub contract: PathBuf,
    /// Path to the file containing the parameters for this contract. If not set the default parameters will be empty.
    #[clap(long = "parameters", parse(from_os_str))]
    pub params: Option<PathBuf>,
    /// Path to the input file to read from on command.
    #[clap(short, long, parse(from_os_str), value_name = "INPUT_FILE")]
    pub input_file: PathBuf,
    /// Deserialization format, requires feature flags enabled.
    #[clap(
        short,
        long = "deserialization-format",
        arg_enum,
        group = "fmt",
        value_name = "FORMAT"
    )]
    pub ser_format: Option<DeserializationFmt>,
    /// Disable TUI mode (run only though CLI commands)
    #[clap(long)]
    pub disable_tui_mode: bool,
    /// Path to output file
    #[clap(short, long, parse(from_os_str), value_name = "OUTPUT_FILE")]
    pub output_file: Option<PathBuf>,
    /// Terminal output
    #[clap(long, requires = "fmt")]
    pub terminal_output: bool,
    /// Max contract size
    #[clap(long, env = "LOCUTUS_MAX_CONTRACT_SIZE", default_value_t = DEFAULT_MAX_CONTRACT_SIZE)]
    pub max_contract_size: i64,
}

/// A CLI utility for build contract states.
///
#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Contract Packaging Tool")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
pub struct StateConfig {
    #[clap(long, parse(from_os_str), value_name = "INPUT_METADATA_PATH")]
    pub input_metadata_path: Option<PathBuf>,
    #[clap(long, parse(from_os_str), value_name = "INPUT_STATE_PATH")]
    pub input_state_path: PathBuf,
    #[clap(long, parse(from_os_str), value_name = "OUTPUT_PATH")]
    pub output_file: PathBuf,
    #[clap(long = "contract-type", arg_enum, group = "type", value_name = "TYPE")]
    pub contract_type: ContractType,
}
