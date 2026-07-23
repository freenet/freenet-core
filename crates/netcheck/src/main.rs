//! Synthetic end-to-end check against the live Freenet network (#4665).
//!
//! Runs as a normal network client: PUTs contracts through one gateway's
//! WebSocket API, then boots a fresh ephemeral peer (empty data dir, so it
//! cannot hold any replica) that joins through a *different* gateway and
//! GETs everything back, including contracts published by previous runs
//! (24h / 48h / 7d retention windows). Talks only to public node APIs;
//! never links freenet-core.

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
#[command(name = "netcheck", about, version)]
enum Cli {
    /// Cross-gateway PUT/GET + retention scenario (the nightly run).
    PutGet(PutGetArgs),
    /// Run an isolated local gateway for testing netcheck on one machine.
    /// Prints the `--gateway-spec` value to pass to `put-get`.
    LocalGateway(LocalGatewayArgs),
}

#[derive(clap::Args, Debug)]
pub struct PutGetArgs {
    /// Base websocket URL of the gateway node the PUTs go through.
    #[arg(long, default_value = "ws://127.0.0.1:7509")]
    pub gateway_ws: String,

    /// Path of the persistent manifest recording previous runs' contracts.
    #[arg(long, default_value = "netcheck-manifest.json")]
    pub manifest: PathBuf,

    /// `freenet` binary used to boot the ephemeral getter node.
    #[arg(long, default_value = "freenet")]
    pub freenet_bin: PathBuf,

    /// Directory of the check's contract crate (compiled to WASM at startup).
    #[arg(long, default_value = "tests/test-contract-integration")]
    pub contract_dir: PathBuf,

    /// UDP network port for the ephemeral node (32177 is reserved on nova).
    #[arg(long, default_value_t = 32177)]
    pub ephemeral_network_port: u16,

    /// Local websocket API port for the ephemeral node.
    #[arg(long, default_value_t = 7519)]
    pub ephemeral_ws_port: u16,

    /// Working directory for the ephemeral node. Defaults to a temp dir that
    /// is removed on exit; on a shared host point it at a known path so a
    /// killed run leaves no anonymous directory behind.
    #[arg(long)]
    pub ephemeral_dir: Option<PathBuf>,

    /// Gateway(s) the ephemeral node joins through, as "ip:port,hex-pubkey".
    /// May be repeated. When set, the node skips the remote gateway index and
    /// uses only these.
    ///
    /// In production this MUST name a gateway OTHER than the one the PUTs go
    /// through (`--gateway-ws`): a GET answered by the node that just stored
    /// the PUT proves transfer, not findability. Left empty, the node
    /// bootstraps from the public index and may pick either gateway, so the
    /// check silently degrades — the report records which peers it actually
    /// connected to.
    #[arg(long)]
    pub gateway_spec: Vec<String>,

    /// Number of small contracts to PUT (a ~1 MB one is always added).
    #[arg(long, default_value_t = 3)]
    pub small_contracts: usize,

    /// Per-operation deadline, seconds. No retries by design: an operation
    /// that only succeeds on retry is the regression netcheck exists to catch.
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

    /// Other gateway(s) this one connects to, as "ip:port,hex-pubkey".
    /// Two linked local gateways reproduce the production topology on one
    /// machine: PUT through the first, join the ephemeral peer through the
    /// second, so the GETs are actually routed instead of being answered by
    /// the node that stored them.
    #[arg(long)]
    pub peer_gateway: Vec<String>,
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
            eprintln!("netcheck: fatal: {e:#}");
            ExitCode::FAILURE
        }
    }
}
