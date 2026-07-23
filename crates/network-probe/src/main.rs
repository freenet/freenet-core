//! Nightly probe against the production Freenet network (#4665).
//!
//! Runs as a normal network client: PUTs contracts through an existing
//! gateway's WebSocket API, then boots a fresh ephemeral peer (empty data
//! dir, so it cannot hold any replica) and GETs everything back through it,
//! including contracts published by previous runs (24h / 48h / 7d retention
//! windows). Talks only to public node APIs; never links freenet-core.

mod client;
mod contracts;
mod ephemeral;
mod manifest;
mod report;
mod scenarios;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "network-probe", about, version)]
enum Cli {
    /// Cross-peer PUT/GET + retention scenario (the nightly run).
    PutGet(PutGetArgs),
    /// Run an isolated local gateway for testing the probe on one machine.
    /// Prints the `--gateway-spec` value to pass to `put-get`.
    LocalGateway(LocalGatewayArgs),
}

#[derive(clap::Args, Debug)]
pub struct PutGetArgs {
    /// Base websocket URL of the gateway node the PUTs go through.
    #[arg(long, default_value = "ws://127.0.0.1:7509")]
    pub gateway_ws: String,

    /// Path of the persistent manifest recording previous runs' contracts.
    #[arg(long, default_value = "network-probe-manifest.json")]
    pub manifest: PathBuf,

    /// `freenet` binary used to boot the ephemeral getter node.
    #[arg(long, default_value = "freenet")]
    pub freenet_bin: PathBuf,

    /// Directory of the probe contract crate (compiled to WASM at startup).
    #[arg(long, default_value = "tests/test-contract-integration")]
    pub contract_dir: PathBuf,

    /// UDP network port for the ephemeral node (32177 is reserved on nova).
    #[arg(long, default_value_t = 32177)]
    pub ephemeral_network_port: u16,

    /// Local websocket API port for the ephemeral node.
    #[arg(long, default_value_t = 7519)]
    pub ephemeral_ws_port: u16,

    /// Gateway(s) for the ephemeral node, as "ip:port,hex-pubkey". May be
    /// repeated. When set, the node skips the remote gateway index and uses
    /// only these (local testing); when empty it bootstraps like any
    /// production peer.
    #[arg(long)]
    pub gateway_spec: Vec<String>,

    /// Number of small contracts to PUT (a ~1 MB one is always added).
    #[arg(long, default_value_t = 3)]
    pub small_contracts: usize,

    /// Per-operation deadline, seconds. No retries by design: an operation
    /// that only succeeds on retry is the regression the probe exists to catch.
    #[arg(long, default_value_t = 120)]
    pub op_timeout_secs: u64,

    /// Deadline for the ephemeral node to join the ring, seconds.
    #[arg(long, default_value_t = 120)]
    pub join_timeout_secs: u64,

    /// Settle time between the PUTs and booting the getter node, seconds.
    #[arg(long, default_value_t = 10)]
    pub settle_secs: u64,
}

#[derive(clap::Args, Debug)]
pub struct LocalGatewayArgs {
    /// `freenet` binary to run.
    #[arg(long, default_value = "freenet")]
    pub freenet_bin: PathBuf,

    /// UDP network port for the local gateway.
    #[arg(long, default_value_t = 31338)]
    pub network_port: u16,

    /// Websocket API port (7509 matches put-get's --gateway-ws default).
    #[arg(long, default_value_t = 7509)]
    pub ws_port: u16,

    /// Data/config directory. Defaults to a temp dir removed on exit.
    #[arg(long)]
    pub dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli {
        Cli::PutGet(args) => scenarios::put_get::run(args).await,
        Cli::LocalGateway(args) => ephemeral::run_local_gateway(args).await.map(|()| true),
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("network-probe: fatal: {e:#}");
            ExitCode::FAILURE
        }
    }
}
