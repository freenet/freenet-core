//! Boots the ephemeral getter node (and, for local testing, an isolated
//! gateway) as `freenet` child processes. The ephemeral node starts from an
//! empty data dir every run, so it cannot hold any replica and its GETs are
//! forced to route over the network — the whole point of the probe.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

use crate::LocalGatewayArgs;

pub struct EphemeralNode {
    child: Child,
    pub ws_base: String,
    dir: tempfile::TempDir,
}

pub async fn spawn_ephemeral(
    freenet_bin: &Path,
    network_port: u16,
    ws_port: u16,
    gateway_specs: &[String],
) -> Result<EphemeralNode> {
    let dir = tempfile::tempdir().context("creating ephemeral node dir")?;
    let log = std::fs::File::create(dir.path().join("node.log"))?;

    let mut cmd = Command::new(freenet_bin);
    cmd.arg("network")
        .args(["--network-port", &network_port.to_string()])
        .args(["--ws-api-address", "127.0.0.1"])
        .args(["--ws-api-port", &ws_port.to_string()])
        .arg("--config-dir")
        .arg(dir.path())
        .arg("--data-dir")
        .arg(dir.path());
    // With explicit gateways (local testing) skip the remote gateway index;
    // with none, bootstrap from the index like any production peer.
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
    })
}

impl EphemeralNode {
    /// Last lines of the node's log, for failure diagnostics.
    pub fn log_tail(&self) -> String {
        let path = self.dir.path().join("node.log");
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

/// Run an isolated local gateway in the foreground for probe testing.
/// Generates a transport keypair (the on-disk format is the hex-encoded
/// 32-byte X25519 secret) and prints the "ip:port,hex-pubkey" spec the
/// ephemeral node needs to bootstrap against it.
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
    println!("run the probe in another terminal, e.g.:");
    println!(
        "  cargo run -p network-probe -- put-get --gateway-spec \"{spec}\" \
         --ephemeral-network-port 31400"
    );

    let status = Command::new(&args.freenet_bin)
        .arg("network")
        .arg("--is-gateway")
        .arg("--skip-load-from-network")
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
        .arg(dir)
        .status()
        .await
        .with_context(|| format!("running {} network", args.freenet_bin.display()))?;

    // Wait a beat so a crash-on-boot is visible before the temp dir goes away.
    tokio::time::sleep(Duration::from_millis(100)).await;
    anyhow::ensure!(status.success(), "local gateway exited with {status}");
    Ok(())
}
