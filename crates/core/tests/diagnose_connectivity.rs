//! Diagnostic test to understand connectivity failures

use freenet_test_network::{BuildProfile, FreenetBinary, TestNetwork};
use std::time::Duration;

#[tokio::test]
async fn diagnose_connectivity_failure() {
    // Build network with more relaxed settings
    let result = TestNetwork::builder()
        .gateways(1)
        .peers(2)
        .binary(FreenetBinary::CurrentCrate(BuildProfile::Debug))
        .require_connectivity(0.5) // Lower threshold - just need 50%
        .connectivity_timeout(Duration::from_secs(60)) // Longer timeout
        .preserve_temp_dirs_on_failure(true)
        .build()
        .await;

    match result {
        Ok(network) => {
            println!("\n✓ Network started successfully!");

            // Print network info
            println!("\nNetwork topology:");
            println!("  Gateway: {}", network.gateway(0).ws_url());
            for i in 0..2 {
                println!("  Peer {}: {}", i, network.peer(i).ws_url());
            }

            // Read and print logs
            println!("\n=== Network Logs ===");
            if let Ok(logs) = network.read_logs() {
                for entry in logs.iter().take(200) {
                    println!(
                        "[{}] {}: {}",
                        entry.peer_id,
                        entry.level.as_deref().unwrap_or("INFO"),
                        entry.message
                    );
                }
                println!("\n(Showing first 200 log lines, total: {})", logs.len());
            }
        }
        Err(e) => {
            eprintln!("\n✗ Network failed to start: {:?}", e);
            panic!("Network startup failed - see logs above");
        }
    }
}
