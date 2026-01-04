#![cfg(feature = "test-network")]
//! Minimal riverctl propagation smoke test to reproduce intermittent "missing contract" errors.
//!
//! This intentionally runs outside CI (ignored) so it can be executed manually when debugging
//! contract propagation races:
//! ```text
//! cargo test -p freenet --test river_smoke -- --ignored
//! ```

mod common;

use anyhow::{bail, Context, Result};
use common::RiverSession;
use freenet_test_network::{BuildProfile, FreenetBinary, TestNetwork};
use tokio::time::{sleep, Duration};
use tracing::info;

const ITERATIONS: usize = 5;

#[test_log::test(tokio::test(flavor = "current_thread", start_paused = true))]
#[ignore = "manual-only: reproduces riverctl missing contract race"]
async fn river_missing_contract_smoke() -> Result<()> {
    let riverctl = which::which("riverctl").context("riverctl not found in PATH")?;

    for iter in 1..=ITERATIONS {
        info!("=== iteration {iter}/{ITERATIONS} ===");
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

        // Use no-retry mode to catch propagation races
        let session = RiverSession::initialize_no_retry(riverctl.clone(), gw_url, peer_url).await?;
        match session
            .send_message(common::RiverUser::Alice, "smoke test")
            .await
        {
            Ok(_) => {
                info!(
                    "iteration {iter} succeeded (run_root={})",
                    network.run_root().display()
                );
            }
            Err(err) => {
                info!(
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
