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
mod emit;
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
    /// Cross-gateway PUT/GET + retention scenario, both phases from one host.
    /// For local testing; the nightly runs `put` and `get` on two hosts.
    PutGet(PutGetArgs),
    /// The PUT phase alone: PUT this run's contracts, record them in the
    /// manifest, and write the handoff `get` reads. Exits 0 once the handoff
    /// is written, even if a PUT failed: `get` reports it.
    Put(PutCmd),
    /// The GET phase alone: boot the ephemeral getter, GET everything the
    /// handoff names, and print the whole run's report (PUTs included).
    Get(GetCmd),
    /// Run an isolated local gateway for testing netcheck on one machine.
    /// Prints the `--gateway-spec` value to pass to `put-get`.
    LocalGateway(LocalGatewayArgs),
    /// Read a run's report on stdin and publish it to an OTLP collector.
    Emit(EmitArgs),
}

#[derive(clap::Args, Debug)]
pub struct EmitArgs {
    /// Base URL of the OTLP/HTTP collector.
    #[arg(long, default_value = "http://127.0.0.1:4318")]
    pub endpoint: String,

    /// Where the check ran from: a hostname, `ci`, a datacentre.
    #[arg(long, default_value = "unknown")]
    pub vantage: String,

    /// Fallback only. The scenario is read from the report's run line
    /// whenever it has one, so a check cannot be filed under another's name
    /// because a flag was forgotten.
    #[arg(long, default_value = "put_get")]
    pub scenario: String,

    /// The check's exit status, so a run that never started is published as
    /// an error rather than a silent pass.
    #[arg(long, default_value_t = 0)]
    pub exit_code: i32,

    /// Print the payload instead of sending it.
    #[arg(long)]
    pub dry_run: bool,
}

/// What the PUT phase needs: the gateway, and the state that carries
/// retention across nights.
#[derive(clap::Args, Debug)]
pub struct PutArgs {
    /// Base websocket URL of the gateway node the PUTs go through.
    #[arg(long, default_value = "ws://127.0.0.1:7509")]
    pub gateway_ws: String,

    /// Path of the persistent manifest recording previous runs' contracts.
    #[arg(long, default_value = "netcheck-manifest.json")]
    pub manifest: PathBuf,

    /// Directory of the check's contract crate (compiled to WASM at startup).
    #[arg(long, default_value = "tests/test-contract-integration")]
    pub contract_dir: PathBuf,

    /// Number of small contracts to PUT (a ~1 MB one is always added).
    #[arg(long, default_value_t = 3)]
    pub small_contracts: usize,
}

/// What the GET phase needs: the ephemeral getter node and how it joins.
#[derive(clap::Args, Debug)]
pub struct GetArgs {
    /// `freenet` binary used to boot the ephemeral getter node.
    #[arg(long, default_value = "freenet")]
    pub freenet_bin: PathBuf,

    /// UDP network port for the ephemeral node.
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
    /// check silently degrades: the report records which peers it actually
    /// connected to.
    #[arg(long)]
    pub gateway_spec: Vec<String>,

    /// Deadline for the ephemeral node to join the ring, seconds. Joined means
    /// at least one ring connection, not just the gateway handshake. Missing it
    /// ends the run before any GET. If every PUT succeeded, the run is published
    /// as an error (the check could not run) rather than a findability failure;
    /// a failed PUT still makes it a failure.
    #[arg(long, default_value_t = 120)]
    pub join_timeout_secs: u64,
}

#[derive(clap::Args, Debug)]
pub struct PutGetArgs {
    #[command(flatten)]
    pub put: PutArgs,

    #[command(flatten)]
    pub get: GetArgs,

    /// Per-operation deadline, seconds. No retries by design: an operation
    /// that only succeeds on retry is the regression netcheck exists to catch.
    #[arg(long, default_value_t = 120)]
    pub op_timeout_secs: u64,

    /// Settle time between the PUTs and booting the getter node, seconds.
    #[arg(long, default_value_t = 10)]
    pub settle_secs: u64,
}

#[derive(clap::Args, Debug)]
pub struct PutCmd {
    #[command(flatten)]
    pub put: PutArgs,

    /// Per-operation deadline, seconds. No retries by design.
    #[arg(long, default_value_t = 120)]
    pub op_timeout_secs: u64,

    /// Where to write the handoff file `get` reads.
    #[arg(long)]
    pub handoff: PathBuf,

    /// Name of the host the PUTs run on, recorded on each PUT record. The
    /// published report comes from the `get` host, under its vantage.
    #[arg(long)]
    pub vantage: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct GetCmd {
    #[command(flatten)]
    pub get: GetArgs,

    /// Per-operation deadline, seconds. No retries by design.
    #[arg(long, default_value_t = 120)]
    pub op_timeout_secs: u64,

    /// Handoff file written by `put`.
    #[arg(long)]
    pub handoff: PathBuf,
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
        Cli::Put(args) => scenarios::put_get::run_put(args).await,
        Cli::Get(args) => scenarios::put_get::run_get(args).await,
        Cli::LocalGateway(args) => ephemeral::run_local_gateway(args).await.map(|()| true),
        Cli::Emit(args) => emit::run(args).await,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_keeps_its_flags_after_the_split() {
        // Splitting the args into PUT and GET groups must not rename a flag:
        // local runs and anyone's notes use these spellings.
        let cli = Cli::try_parse_from([
            "netcheck",
            "put-get",
            "--gateway-ws",
            "ws://127.0.0.1:7509",
            "--manifest",
            "m.json",
            "--freenet-bin",
            "/usr/local/bin/freenet",
            "--ephemeral-dir",
            "run",
            "--ephemeral-network-port",
            "32177",
            "--ephemeral-ws-port",
            "7519",
            "--gateway-spec",
            "1.2.3.4:31338,abcd",
            "--op-timeout-secs",
            "60",
            "--join-timeout-secs",
            "90",
            "--settle-secs",
            "5",
        ])
        .expect("put-get flags parse");
        let Cli::PutGet(args) = cli else {
            panic!("parsed as another subcommand")
        };
        assert_eq!(args.put.manifest, PathBuf::from("m.json"));
        assert_eq!(
            args.get.gateway_spec,
            vec!["1.2.3.4:31338,abcd".to_string()]
        );
        assert_eq!(args.get.join_timeout_secs, 90);
        assert_eq!(args.op_timeout_secs, 60);
    }

    #[test]
    fn each_phase_takes_only_its_own_flags() {
        let Cli::Put(put) = Cli::try_parse_from([
            "netcheck",
            "put",
            "--manifest",
            "m.json",
            "--handoff",
            "h.json",
            "--vantage",
            "nova",
        ])
        .expect("put parses") else {
            panic!("parsed as another subcommand")
        };
        assert_eq!(put.handoff, PathBuf::from("h.json"));
        assert_eq!(put.vantage.as_deref(), Some("nova"));

        let Cli::Get(get) = Cli::try_parse_from([
            "netcheck",
            "get",
            "--handoff",
            "h.json",
            "--gateway-spec",
            "1.2.3.4:31338,abcd",
        ])
        .expect("get parses") else {
            panic!("parsed as another subcommand")
        };
        assert_eq!(get.get.gateway_spec.len(), 1);

        // The manifest belongs to the PUT host; a GET that accepted it would
        // invite running the phase that cannot keep it. Each call is
        // otherwise complete, so the only possible error is the stray flag.
        for (args, stray) in [
            (
                ["netcheck", "get", "--handoff", "h", "--manifest", "m"],
                "--manifest",
            ),
            (
                ["netcheck", "put", "--handoff", "h", "--gateway-spec", "g"],
                "--gateway-spec",
            ),
        ] {
            let err = Cli::try_parse_from(args).expect_err("a flag of the other phase");
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "{args:?} must be refused for {stray}, not for something else: {err}"
            );
        }
    }
}
