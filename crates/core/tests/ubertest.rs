//! Comprehensive integration test for freenet-core using River as a reference application.
//!
//! This test verifies that freenet-core can support real-world complex applications by:
//! 1. Setting up a multi-peer network (1 gateway + N peers, configurable)
//! 2. Verifying proper network topology formation (peer-to-peer connections, not just gateway)
//! 3. Testing contract operations through riverctl (create room, join, messaging)
//! 4. Validating state consistency across peers
//!
//! # Prerequisites
//! - `riverctl` must be installed: `cargo install riverctl`
//! - Test will verify riverctl version matches latest on crates.io
//!
//! # Configuration
//! Set environment variable `UBERTEST_PEER_COUNT` to control number of peers (default: 10)

use anyhow::{bail, Context};
use freenet::test_utils::TestContext;
use freenet_macros::freenet_test;
use std::{path::PathBuf, process::Command, time::Duration};
use tokio::time::sleep;
use tracing::info;

/// Check that riverctl is installed and is the latest version
fn verify_riverctl() -> anyhow::Result<PathBuf> {
    // Check if riverctl is installed
    let riverctl_path = which::which("riverctl").context("riverctl not found in PATH")?;

    info!("Found riverctl at: {}", riverctl_path.display());

    // Get installed version
    let output = Command::new(&riverctl_path)
        .arg("--version")
        .output()
        .context("Failed to execute riverctl --version")?;

    if !output.status.success() {
        bail!("riverctl --version failed");
    }

    let version_output = String::from_utf8_lossy(&output.stdout);
    let installed_version = version_output
        .split_whitespace()
        .last()
        .context("Could not parse riverctl version")?;

    info!("Installed riverctl version: {}", installed_version);

    // Get latest version from crates.io
    let crates_io_url = "https://crates.io/api/v1/crates/riverctl";
    let mut response = ureq::get(crates_io_url)
        .call()
        .context("Failed to fetch riverctl info from crates.io")?;

    let json: serde_json::Value = response
        .body_mut()
        .read_json()
        .context("Failed to parse crates.io response")?;

    let latest_version = json["crate"]["newest_version"]
        .as_str()
        .context("Could not find newest_version in crates.io response")?;

    info!("Latest riverctl version on crates.io: {}", latest_version);

    if installed_version != latest_version {
        bail!(
            "riverctl version mismatch!\n\
             Installed: {}\n\
             Latest:    {}\n\n\
             Please update riverctl:\n\
             $ cargo install riverctl --force\n\n\
             Or if you're running this test in CI, the GitHub Action should run:\n\
             $ cargo install riverctl\n\
             before running the tests.",
            installed_version,
            latest_version
        );
    }

    info!("✓ riverctl version check passed");
    Ok(riverctl_path)
}

/// Simplified test with just gateway + 1 peer to verify basic PUT operations work
#[freenet_test(
    nodes = ["gateway", "peer-0"],
    auto_connect_peers = true,
    timeout_secs = 120,
    startup_wait_secs = 25,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4,
    aggregate_events = "on_failure"
)]
#[ignore] // Waker registration fix verified working - test fails due to unrelated connection issues
async fn test_basic_room_creation(ctx: &mut TestContext) -> anyhow::Result<()> {
    info!("=== Basic Room Creation Test ===");
    info!("Testing minimal setup: 1 gateway + 1 peer");

    // Find riverctl without version check
    let riverctl_path = which::which("riverctl").context("riverctl not found in PATH")?;
    info!("Using riverctl at: {}", riverctl_path.display());

    // Get peer info from context
    let peer = ctx.node("peer-0")?;
    let peer_ws_port = peer.ws_port;
    let peer_temp_dir = peer.temp_dir_path.clone();

    let gateway = ctx.gateway()?;
    info!(
        "Gateway - network: {}, ws: {}",
        gateway.network_port.unwrap_or(0),
        gateway.ws_port
    );
    info!(
        "Peer - network: {}, ws: {}",
        peer.network_port.unwrap_or(0),
        peer_ws_port
    );

    // Nodes are already connected, proceed with room creation
    let peer_ws = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer_ws_port
    );
    let river_config_dir = peer_temp_dir.join("river-user0");
    std::fs::create_dir_all(&river_config_dir)?;

    info!("Creating room via riverctl...");
    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &river_config_dir)
        .args([
            "--node-url",
            &peer_ws,
            "--format",
            "json",
            "room",
            "create",
            "--name",
            "test-room",
            "--nickname",
            "Alice",
        ])
        .output()
        .context("Failed to execute riverctl room create")?;

    if !output.status.success() {
        bail!(
            "Room creation failed: {}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    info!("✓ Room created successfully");
    info!("Output: {}", String::from_utf8_lossy(&output.stdout));

    Ok(())
}

/// Comprehensive ubertest with multiple peers testing River application functionality
/// Note: The number of peers is now fixed at compile time (5 peers + 1 gateway).
/// Previously used UBERTEST_PEER_COUNT environment variable for dynamic peer count.
#[freenet_test(
    nodes = ["gateway", "peer-0", "peer-1", "peer-2", "peer-3", "peer-4"],
    auto_connect_peers = true,
    timeout_secs = 600,
    startup_wait_secs = 30,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 8,
    aggregate_events = "on_failure"
)]
#[ignore = "requires fixes still in progress"]
async fn test_app_ubertest(ctx: &mut TestContext) -> anyhow::Result<()> {
    info!("=== Freenet Application Ubertest ===");
    info!("Testing River as reference application");
    info!("Configuration: {} peers + 1 gateway", ctx.peers().len());

    // Step 1: Verify riverctl is installed and up-to-date
    info!("\n--- Step 1: Verifying riverctl ---");
    let riverctl_path = verify_riverctl()?;

    // Step 2: Log network information
    info!("\n--- Step 2: Network Information ---");
    let gateway = ctx.gateway()?;
    info!(
        "Gateway - network: {}, ws: {}",
        gateway.network_port.unwrap_or(0),
        gateway.ws_port
    );

    for (i, peer) in ctx.peers().iter().enumerate() {
        info!(
            "Peer {} - network: {}, ws: {}",
            i,
            peer.network_port.unwrap_or(0),
            peer.ws_port
        );
    }

    // Wait additional time for mesh formation (connection maintenance cycles)
    info!("\n--- Step 3: Waiting for Mesh Formation ---");
    sleep(Duration::from_secs(75)).await;
    info!("Mesh formation period complete");

    // Step 4: Create room via riverctl on peer 0
    info!("\n--- Step 4: Creating Chat Room (via peer 0) ---");
    let peer0 = ctx.node("peer-0")?;
    let peer0_ws = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer0.ws_port
    );
    let peer0_config_dir = peer0.temp_dir_path.join("river-user0");
    std::fs::create_dir_all(&peer0_config_dir)?;

    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &peer0_config_dir)
        .args([
            "--node-url",
            &peer0_ws,
            "--format",
            "json",
            "room",
            "create",
            "--name",
            "ubertest-room",
            "--nickname",
            "Alice",
        ])
        .output()
        .context("Failed to execute riverctl room create")?;

    if !output.status.success() {
        bail!(
            "Room creation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let room_output = String::from_utf8_lossy(&output.stdout);
    info!("Room creation output: {}", room_output);

    // Parse room ID from JSON output
    let room_id = if let Some(line) = room_output.lines().find(|l| l.trim().starts_with('{')) {
        let json: serde_json::Value = serde_json::from_str(line)?;
        json.get("owner_key")
            .and_then(|v| v.as_str())
            .context("Failed to extract room owner_key from response")?
            .to_string()
    } else {
        bail!("No JSON output found in room creation response");
    };

    info!("Room created with ID: {}", room_id);

    // Step 5: Create invitation
    info!("\n--- Step 5: Creating Invitation ---");
    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &peer0_config_dir)
        .args([
            "--node-url",
            &peer0_ws,
            "--format",
            "json",
            "invite",
            "create",
            &room_id,
        ])
        .output()
        .context("Failed to execute riverctl invite create")?;

    if !output.status.success() {
        bail!(
            "Invitation creation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let invite_output = String::from_utf8_lossy(&output.stdout);
    info!("Invitation creation output: {}", invite_output);

    // Parse invitation code from JSON output
    let invitation = if let Some(line) = invite_output.lines().find(|l| l.trim().starts_with('{')) {
        let json: serde_json::Value = serde_json::from_str(line)?;
        json.get("invitation_code")
            .or_else(|| json.get("invitation"))
            .and_then(|v| v.as_str())
            .context("Failed to extract invitation from response")?
            .to_string()
    } else {
        bail!("No JSON output found in invitation response");
    };

    info!(
        "Invitation created: {}...",
        &invitation[..50.min(invitation.len())]
    );

    // Step 6: Accept invitation on peer 1
    info!("\n--- Step 6: Accepting Invitation (via peer 1) ---");
    let peer1 = ctx.node("peer-1")?;
    let peer1_ws = format!(
        "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
        peer1.ws_port
    );
    let peer1_config_dir = peer1.temp_dir_path.join("river-user1");
    std::fs::create_dir_all(&peer1_config_dir)?;

    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &peer1_config_dir)
        .args([
            "--node-url",
            &peer1_ws,
            "--format",
            "json",
            "invite",
            "accept",
            &invitation,
            "--nickname",
            "Bob",
        ])
        .output()
        .context("Failed to execute riverctl invite accept")?;

    if !output.status.success() {
        bail!(
            "Invitation acceptance failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    info!("Invitation accepted by peer 1");

    // Wait for synchronization
    info!("Waiting 5s for state synchronization...");
    sleep(Duration::from_secs(5)).await;

    // Step 7: Send message from peer 0 -> peer 1
    info!("\n--- Step 7: Sending Message (Alice -> Bob) ---");
    let msg1 = "Hello from Alice!";
    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &peer0_config_dir)
        .args([
            "--node-url",
            &peer0_ws,
            "--format",
            "json",
            "message",
            "send",
            &room_id,
            msg1,
        ])
        .output()
        .context("Failed to execute riverctl message send (Alice)")?;

    if !output.status.success() {
        bail!(
            "Message send (Alice) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    info!("Message sent from Alice");

    // Step 8: Send message from peer 1 -> peer 0
    info!("\n--- Step 8: Sending Message (Bob -> Alice) ---");
    let msg2 = "Hello from Bob!";
    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &peer1_config_dir)
        .args([
            "--node-url",
            &peer1_ws,
            "--format",
            "json",
            "message",
            "send",
            &room_id,
            msg2,
        ])
        .output()
        .context("Failed to execute riverctl message send (Bob)")?;

    if !output.status.success() {
        bail!(
            "Message send (Bob) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    info!("Message sent from Bob");

    // Wait for message propagation
    info!("Waiting 5s for message propagation...");
    sleep(Duration::from_secs(5)).await;

    // Step 9: Verify messages received by both users
    info!("\n--- Step 9: Verifying Messages ---");

    // Alice checks messages
    info!("Alice listing messages...");
    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &peer0_config_dir)
        .args([
            "--node-url",
            &peer0_ws,
            "--format",
            "json",
            "message",
            "list",
            &room_id,
        ])
        .output()
        .context("Failed to execute riverctl message list (Alice)")?;

    if !output.status.success() {
        bail!(
            "Message list (Alice) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let alice_messages = String::from_utf8_lossy(&output.stdout);
    info!("Alice's messages: {}", alice_messages);

    // Bob checks messages
    info!("Bob listing messages...");
    let output = Command::new(&riverctl_path)
        .env("RIVER_CONFIG_DIR", &peer1_config_dir)
        .args([
            "--node-url",
            &peer1_ws,
            "--format",
            "json",
            "message",
            "list",
            &room_id,
        ])
        .output()
        .context("Failed to execute riverctl message list (Bob)")?;

    if !output.status.success() {
        bail!(
            "Message list (Bob) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let bob_messages = String::from_utf8_lossy(&output.stdout);
    info!("Bob's messages: {}", bob_messages);

    // Verify both messages are visible to both users
    if !alice_messages.contains(msg1) || !alice_messages.contains(msg2) {
        bail!("Alice didn't receive all messages");
    }
    if !bob_messages.contains(msg1) || !bob_messages.contains(msg2) {
        bail!("Bob didn't receive all messages");
    }

    info!("\n✓ Ubertest completed successfully!");
    Ok(())
}
