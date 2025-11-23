//! Minimal riverctl propagation smoke test to reproduce intermittent "missing contract" errors.
//!
//! Run manually (ignored in CI):
//! `cargo test -p freenet --test river_smoke -- --ignored --nocapture`

use anyhow::{anyhow, bail, Context, Result};
use freenet_test_network::{BuildProfile, FreenetBinary, TestNetwork};
use regex::Regex;
use std::path::PathBuf;
use tokio::time::{sleep, Duration};
use which::which;

const ITERATIONS: usize = 5;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual-only: reproduces riverctl missing contract race"]
async fn river_missing_contract_smoke() -> Result<()> {
    let riverctl = which("riverctl").context("riverctl not found in PATH")?;

    for iter in 1..=ITERATIONS {
        println!("=== iteration {iter}/{ITERATIONS} ===");
        let network = TestNetwork::builder()
            .gateways(1)
            .peers(1)
            .preserve_temp_dirs_on_failure(true)
            .preserve_temp_dirs_on_success(true)
            .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
            .build()
            .await
            .context("start test network")?;

        let gw_url = format!("{}?encodingProtocol=native", network.gateway(0).ws_url());
        let peer_url = format!("{}?encodingProtocol=native", network.peer(0).ws_url());

        let mut session = RiverSession::initialize(riverctl.clone(), gw_url, peer_url).await?;
        match session.send_message("smoke test").await {
            Ok(_) => {
                println!(
                    "iteration {iter} succeeded (run_root={})",
                    network.run_root().display()
                );
            }
            Err(err) => {
                println!(
                    "iteration {iter} failed: {err} (run_root={})",
                    network.run_root().display()
                );
                bail!(err);
            }
        }

        // Give the OS a moment to free ports between iterations.
        sleep(Duration::from_secs(2)).await;
    }

    Ok(())
}

struct RiverSession {
    riverctl: PathBuf,
    room_key: String,
    invite_regex: Regex,
    room_regex: Regex,
    alice_url: String,
    bob_url: String,
}

impl RiverSession {
    async fn initialize(riverctl: PathBuf, alice_url: String, bob_url: String) -> Result<Self> {
        let mut session = Self {
            riverctl,
            room_key: String::new(),
            invite_regex: Regex::new(r"[A-Za-z0-9+/=]{40,}")?,
            room_regex: Regex::new(r"[A-Za-z0-9]{40,}")?,
            alice_url,
            bob_url,
        };
        session.setup_room().await?;
        Ok(session)
    }

    async fn setup_room(&mut self) -> Result<()> {
        // No retries—we want to catch propagation races.
        let create_output = self
            .run_riverctl(
                RiverUser::Alice,
                &[
                    "room",
                    "create",
                    "--name",
                    "river-smoke",
                    "--nickname",
                    "Alice",
                ],
            )
            .await?;
        self.room_key = self
            .room_regex
            .find(&create_output)
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("failed to parse room owner key from riverctl output"))?;

        let invite_output = self
            .run_riverctl(
                RiverUser::Alice,
                &["invite", "create", self.room_key.as_str()],
            )
            .await?;
        let invitation_code = self
            .invite_regex
            .find_iter(&invite_output)
            .filter(|m| m.as_str() != self.room_key)
            .last()
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("failed to parse invitation code from riverctl output"))?;

        self.run_riverctl(
            RiverUser::Bob,
            &["invite", "accept", &invitation_code, "--nickname", "Bob"],
        )
        .await?;

        Ok(())
    }

    async fn send_message(&mut self, body: &str) -> Result<()> {
        self.run_riverctl(
            RiverUser::Alice,
            &["message", "send", self.room_key.as_str(), body],
        )
        .await
        .map(|_| ())
    }

    async fn run_riverctl(&self, user: RiverUser, args: &[&str]) -> Result<String> {
        let url = match user {
            RiverUser::Alice => &self.alice_url,
            RiverUser::Bob => &self.bob_url,
        };

        let output = tokio::process::Command::new(&self.riverctl)
            .arg("--node-url")
            .arg(url)
            .args(args)
            .output()
            .await
            .context("failed to execute riverctl command")?;

        if output.status.success() {
            return Ok(String::from_utf8_lossy(&output.stdout).to_string());
        }

        Err(anyhow!(
            "riverctl {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

#[derive(Clone, Copy, Debug)]
enum RiverUser {
    Alice,
    Bob,
}
