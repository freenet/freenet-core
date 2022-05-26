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

/// A CLI utility for testing out contracts against a Locutus local node.
///
#[derive(clap::Parser, Clone)]
#[clap(name = "Locutus Contract Development Environment")]
#[clap(author = "The Freenet Project Inc.")]
#[clap(version = "0.0.1")]
#[clap(group(
    ArgGroup::new("output")
        .required(true)
        .args(&["output-file", "terminal-output"])
))]
pub struct Config {
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
