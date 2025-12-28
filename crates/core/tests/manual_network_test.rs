#![cfg(feature = "test-network")]
//! Manual test to inspect network logs

use freenet_test_network::{BuildProfile, FreenetBinary, TestNetwork};
use std::time::Duration;
use tracing::info;

#[test_log::test(tokio::test)]
#[ignore] // Run manually with: cargo test manual_network_test -- --ignored
async fn manual_network_test() {
    let network = TestNetwork::builder()
        .gateways(1)
        .peers(1)
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .require_connectivity(0.5)
        .connectivity_timeout(Duration::from_secs(10)) // Short timeout so we can inspect quickly
        .preserve_temp_dirs_on_failure(true)
        .build()
        .await;

    match network {
        Ok(ref net) => {
            info!("=== Network Started ===");
            info!("Gateway: {}", net.gateway(0).ws_url());
            info!("Peer: {}", net.peer(0).ws_url());

            // Print all logs
            if let Ok(logs) = net.read_logs() {
                info!("=== Logs ===");
                for entry in logs {
                    info!(
                        "[{}] {}: {}",
                        entry.peer_id,
                        entry.level.as_deref().unwrap_or("INFO"),
                        entry.message
                    );
                }
            }

            // Keep network alive for inspection
            info!("Network is running. Press Ctrl+C to exit.");
            tokio::time::sleep(Duration::from_secs(300)).await;
        }
        Err(e) => {
            tracing::error!("Network failed: {:?}", e);
            // Try to read logs anyway if temp dirs still exist
        }
    }
}
