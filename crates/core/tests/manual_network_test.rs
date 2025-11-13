//! Manual test to inspect network logs

use freenet_test_network::{BuildProfile, FreenetBinary, TestNetwork};
use std::time::Duration;

#[tokio::test]
#[ignore] // Run manually with: cargo test manual_network_test -- --ignored --nocapture
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
            println!("\n=== Network Started ===");
            println!("Gateway: {}", net.gateway(0).ws_url());
            println!("Peer: {}", net.peer(0).ws_url());

            // Print all logs
            if let Ok(logs) = net.read_logs() {
                println!("\n=== Logs ===");
                for entry in logs {
                    println!(
                        "[{}] {}: {}",
                        entry.peer_id,
                        entry.level.as_deref().unwrap_or("INFO"),
                        entry.message
                    );
                }
            }

            // Keep network alive for inspection
            println!("\nNetwork is running. Press Ctrl+C to exit.");
            tokio::time::sleep(Duration::from_secs(300)).await;
        }
        Err(e) => {
            eprintln!("\n✗ Network failed: {:?}", e);
            // Try to read logs anyway if temp dirs still exist
        }
    }
}
