//! Boots the ephemeral getter node (and, for local testing, an isolated
//! gateway) as `freenet` child processes. The ephemeral node starts from an
//! empty data dir every run, so it cannot hold any replica and its GETs are
//! forced to route over the network — the whole point of netcheck.
//!
//! The node is addressed only through its own dirs and ports, and killed by
//! process handle, never by name: on a host that also runs a real node,
//! netcheck must be incapable of touching it.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

use crate::LocalGatewayArgs;

pub struct EphemeralNode {
    child: Child,
    pub ws_base: String,
    dir: PathBuf,
    /// Kept alive so the temp dir outlives the node when no explicit
    /// `--ephemeral-dir` was given.
    _tmp: Option<tempfile::TempDir>,
}

/// Version of the `freenet` binary running the ephemeral node. Recorded in
/// the report: it separates "the network broke" from "last night's release
/// broke", and cannot be recovered after the fact.
pub async fn binary_version(freenet_bin: &Path) -> String {
    match Command::new(freenet_bin).arg("--version").output().await {
        Ok(out) => String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string(),
        Err(e) => format!("(unknown: {e})"),
    }
}

pub async fn spawn_ephemeral(
    freenet_bin: &Path,
    network_port: u16,
    ws_port: u16,
    gateway_specs: &[String],
    explicit_dir: Option<&Path>,
) -> Result<EphemeralNode> {
    let (dir, tmp) = match explicit_dir {
        Some(d) => {
            std::fs::create_dir_all(d)
                .with_context(|| format!("creating ephemeral node dir {}", d.display()))?;
            (d.to_path_buf(), None)
        }
        None => {
            let tmp = tempfile::tempdir().context("creating ephemeral node dir")?;
            (tmp.path().to_path_buf(), Some(tmp))
        }
    };
    let log = std::fs::File::create(dir.join("node.log"))?;

    let mut cmd = Command::new(freenet_bin);
    cmd.arg("network")
        .args(["--network-port", &network_port.to_string()])
        .args(["--ws-api-address", "127.0.0.1"])
        .args(["--ws-api-port", &ws_port.to_string()])
        // Lives for minutes: detecting a new release and exiting 42 mid-run
        // would fail the check for a reason unrelated to the network.
        .arg("--disable-auto-update")
        .arg("--config-dir")
        .arg(&dir)
        .arg("--data-dir")
        .arg(&dir);
    // With explicit gateways the node skips the remote index and joins only
    // through those; with none it bootstraps from the public index.
    if !gateway_specs.is_empty() {
        cmd.arg("--skip-load-from-network");
        for spec in gateway_specs {
            cmd.args(["--gateway", spec]);
        }
    }
    cmd.stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true);

    let child = cmd
        .spawn()
        .with_context(|| format!("spawning {} network", freenet_bin.display()))?;
    Ok(EphemeralNode {
        child,
        ws_base: format!("ws://127.0.0.1:{ws_port}"),
        dir,
        _tmp: tmp,
    })
}

impl EphemeralNode {
    /// Last lines of the node's log, for failure diagnostics.
    pub fn log_tail(&self) -> String {
        let path = self.dir.join("node.log");
        match std::fs::read_to_string(&path) {
            Ok(s) => {
                let lines: Vec<&str> = s.lines().collect();
                let start = lines.len().saturating_sub(40);
                lines[start..].join("\n")
            }
            Err(e) => format!("(could not read node log: {e})"),
        }
    }

    pub async fn shutdown(mut self) {
        let _ = self.child.kill().await;
    }
}

/// Run a local gateway in the foreground for local testing. Generates a
/// transport keypair (the on-disk format is the hex-encoded 32-byte X25519
/// secret) and prints the "ip:port,hex-pubkey" spec other nodes need to
/// bootstrap against it. With `--peer-gateway` it links to another local
/// gateway instead of running isolated.
pub async fn run_local_gateway(args: LocalGatewayArgs) -> Result<()> {
    let tmp;
    let dir: &Path = match &args.dir {
        Some(d) => {
            std::fs::create_dir_all(d)?;
            d
        }
        None => {
            tmp = tempfile::tempdir()?;
            tmp.path()
        }
    };

    let secret = x25519_dalek::StaticSecret::from(rand::random::<[u8; 32]>());
    let public = x25519_dalek::PublicKey::from(&secret);
    let key_path = dir.join("gw-transport-keypair");
    std::fs::write(&key_path, hex::encode(secret.to_bytes()))?;

    let spec = format!(
        "127.0.0.1:{},{}",
        args.network_port,
        hex::encode(public.as_bytes())
    );
    println!("local gateway starting.");
    println!("  ws api:        ws://127.0.0.1:{}", args.ws_port);
    println!("  --gateway-spec \"{spec}\"");
    println!("run netcheck in another terminal, e.g.:");
    println!(
        "  cargo run -p netcheck -- put-get --gateway-spec \"{spec}\" \
         --ephemeral-network-port 31400"
    );

    let mut cmd = Command::new(&args.freenet_bin);
    cmd.arg("network")
        .arg("--is-gateway")
        .arg("--skip-load-from-network")
        .arg("--disable-auto-update")
        .arg("--transport-keypair")
        .arg(&key_path)
        .args(["--network-port", &args.network_port.to_string()])
        .args(["--public-network-address", "127.0.0.1"])
        .args(["--public-network-port", &args.network_port.to_string()])
        .args(["--ws-api-address", "127.0.0.1"])
        .args(["--ws-api-port", &args.ws_port.to_string()])
        .arg("--config-dir")
        .arg(dir)
        .arg("--data-dir")
        .arg(dir);
    for peer in &args.peer_gateway {
        cmd.args(["--gateway", peer]);
    }

    let status = cmd
        .status()
        .await
        .with_context(|| format!("running {} network", args.freenet_bin.display()))?;

    // Wait a beat so a crash-on-boot is visible before the temp dir goes away.
    tokio::time::sleep(Duration::from_millis(100)).await;
    anyhow::ensure!(status.success(), "local gateway exited with {status}");
    Ok(())
}
