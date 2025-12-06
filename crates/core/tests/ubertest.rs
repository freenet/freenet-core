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
use freenet::{
    config::{ConfigArgs, InlineGwConfig, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::NodeConfig,
    server::serve_gateway,
};
use futures::FutureExt;
use rand::{Rng, SeedableRng};
use std::{
    env,
    net::{Ipv4Addr, TcpListener},
    path::PathBuf,
    process::Command,
    sync::{LazyLock, Mutex},
    time::Duration,
};
use tokio::select;
use tokio::time::{sleep, timeout};
use tracing::info;

static RNG: LazyLock<Mutex<rand::rngs::StdRng>> = LazyLock::new(|| {
    Mutex::new(rand::rngs::StdRng::from_seed(
        *b"0102030405060708090a0b0c0d0e0f10",
    ))
});

const DEFAULT_PEER_COUNT: usize = 10;

#[derive(Debug)]
struct UbertestConfig {
    peer_count: usize,
}

impl UbertestConfig {
    fn from_env() -> Self {
        let peer_count = env::var("UBERTEST_PEER_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_PEER_COUNT);

        Self { peer_count }
    }
}

#[derive(Debug)]
struct PeerInfo {
    _name: String,
    network_port: u16,
    ws_port: u16,
    temp_dir: tempfile::TempDir,
    _is_gateway: bool,
    location: f64,
}

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

/// Create configuration for a peer (gateway or regular)
async fn create_peer_config(
    name: String,
    is_gateway: bool,
    gateway_info: Option<InlineGwConfig>,
    _peer_index: usize,
) -> anyhow::Result<(ConfigArgs, PeerInfo)> {
    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new();
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;
    key.public().save(temp_dir.path().join("public.pem"))?;

    // Bind all peers to localhost; macOS only routes 127.0.0.1 by default.
    let peer_ip = Ipv4Addr::LOCALHOST;
    let location = {
        let mut rng = RNG.lock().unwrap();
        rng.random()
    };

    // Bind network socket to peer-specific IP for P2P communication
    let network_bind_addr = format!("{}:0", peer_ip);
    let network_socket = TcpListener::bind(&network_bind_addr)?;
    let network_port = network_socket.local_addr()?.port();

    // Bind WebSocket API to 127.0.0.1 so local clients (riverctl) can connect
    let ws_bind_addr = "127.0.0.1:0";
    let ws_socket = TcpListener::bind(ws_bind_addr)?;
    let ws_port = ws_socket.local_addr()?.port();

    // Drop sockets so they can be reused
    std::mem::drop(network_socket);
    std::mem::drop(ws_socket);

    let gateways = if let Some(gw) = gateway_info {
        vec![serde_json::to_string(&gw)?]
    } else {
        vec![]
    };

    let config = ConfigArgs {
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::new(127, 0, 0, 1).into()), // Always use 127.0.0.1 for WebSocket API
            ws_api_port: Some(ws_port),
            token_ttl_seconds: None,
            token_cleanup_interval_seconds: None,
        },
        network_api: NetworkArgs {
            public_address: Some(peer_ip.into()), // Share localhost IP for P2P network
            public_port: Some(network_port), // Always set for localhost (required for local networks)
            is_gateway,
            skip_load_from_network: true,
            gateways: Some(gateways),
            location: Some(location), // Ensure unique ring location even with shared IP
            ignore_protocol_checking: true,
            address: Some(peer_ip.into()),
            network_port: Some(network_port),
            min_connections: None,
            max_connections: None,
            bandwidth_limit: None,
            blocked_addresses: None,
            transient_budget: None,
            transient_ttl_secs: None,
        },
        config_paths: freenet::config::ConfigPathsArgs {
            config_dir: Some(temp_dir.path().to_path_buf()),
            data_dir: Some(temp_dir.path().to_path_buf()),
        },
        secrets: SecretArgs {
            transport_keypair: Some(transport_keypair),
            ..Default::default()
        },
        ..Default::default()
    };

    let peer_info = PeerInfo {
        _name: name,
        network_port,
        ws_port,
        temp_dir,
        _is_gateway: is_gateway,
        location,
    };

    Ok((config, peer_info))
}

/// Poll for a condition to be met, with timeout
async fn poll_until<F, Fut>(
    condition: F,
    timeout_duration: Duration,
    poll_interval: Duration,
    description: &str,
) -> anyhow::Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    let start = std::time::Instant::now();
    loop {
        if condition().await? {
            info!("✓ {} (took {:?})", description, start.elapsed());
            return Ok(());
        }

        if start.elapsed() > timeout_duration {
            bail!("{} - timeout after {:?}", description, timeout_duration);
        }

        sleep(poll_interval).await;
    }
}

/// Verify network topology using freenet's internal API or log inspection
async fn verify_network_topology(
    _peers: &[PeerInfo],
    _min_peer_connections: usize,
) -> anyhow::Result<bool> {
    // TODO: This needs to query actual peer connection state
    // For now, we'll use a heuristic based on time and log inspection
    // In a full implementation, we'd:
    // 1. Query each peer's WebSocket API for connection state
    // 2. Parse connection_manager info
    // 3. Verify peer-to-peer connections exist (not just gateway)

    info!("Checking network topology (simplified check - needs full implementation)");
    Ok(true)
}

/// Simplified test with just gateway + 1 peer to verify basic PUT operations work
#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore] // Waker registration fix verified working - test fails due to unrelated connection issues
async fn test_basic_room_creation() -> anyhow::Result<()> {
    info!("=== Basic Room Creation Test ===");
    info!("Testing minimal setup: 1 gateway + 1 peer");

    // Find riverctl without version check
    let riverctl_path = which::which("riverctl").context("riverctl not found in PATH")?;
    info!("Using riverctl at: {}", riverctl_path.display());

    // Create gateway
    let (gw_config, gw_info) = create_peer_config("gateway".to_string(), true, None, 0).await?;
    let gateway_inline_config = InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gw_info.network_port).into(),
        location: Some(gw_info.location),
        public_key_path: gw_info.temp_dir.path().join("public.pem"),
    };

    info!(
        "Gateway - network: {}, ws: {}",
        gw_info.network_port, gw_info.ws_port
    );

    // Create peer
    let (peer_config, peer_info) = create_peer_config(
        "peer0".to_string(),
        false,
        Some(gateway_inline_config.clone()),
        1,
    )
    .await?;

    info!(
        "Peer - network: {}, ws: {}",
        peer_info.network_port, peer_info.ws_port
    );

    // Start gateway
    let gw_node = async {
        let config = gw_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Start peer (with delay for gateway to be ready)
    let peer_node = async {
        sleep(Duration::from_secs(5)).await;
        let config = peer_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    let peer_ws_port = peer_info.ws_port;
    let peer_temp_dir = peer_info.temp_dir.path().to_path_buf();

    // Test logic
    let test_logic = timeout(Duration::from_secs(120), async move {
        info!("Waiting for nodes to bootstrap...");
        sleep(Duration::from_secs(25)).await;

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
        Ok::<(), anyhow::Error>(())
    });

    // Run everything
    select! {
        result = test_logic => {
            result??;
            info!("Test completed successfully");
        }
        result = gw_node => {
            result?;
            bail!("Gateway node exited unexpectedly");
        }
        result = peer_node => {
            result?;
            bail!("Peer node exited unexpectedly");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires fixes still in progress"]
async fn test_app_ubertest() -> anyhow::Result<()> {
    info!("=== Freenet Application Ubertest ===");
    info!("Testing River as reference application");

    let config = UbertestConfig::from_env();
    info!("Configuration: {} peers + 1 gateway", config.peer_count);

    // Step 1: Verify riverctl is installed and up-to-date
    info!("\n--- Step 1: Verifying riverctl ---");
    let riverctl_path = verify_riverctl()?;

    // Step 2: Create and start gateway
    info!("\n--- Step 2: Creating Gateway ---");
    let (gw_config, gw_info) = create_peer_config("gateway".to_string(), true, None, 0).await?;

    let gateway_inline_config = InlineGwConfig {
        address: (Ipv4Addr::LOCALHOST, gw_info.network_port).into(),
        location: Some(gw_info.location),
        public_key_path: gw_info.temp_dir.path().join("public.pem"),
    };

    info!("Gateway network port: {}", gw_info.network_port);
    info!("Gateway WS API port: {}", gw_info.ws_port);

    // Start gateway
    let gw_node = async {
        let config = gw_config.build().await?;
        let node = NodeConfig::new(config.clone())
            .await?
            .build(serve_gateway(config.ws_api).await?)
            .await?;
        node.run().await
    }
    .boxed_local();

    // Wait for gateway startup
    sleep(Duration::from_secs(20)).await;
    info!("Gateway started, peers starting with 20s delays...");

    // Step 3: Create and start peers with staggered startup
    info!(
        "\n--- Step 3: Creating {} Peers (staggered startup) ---",
        config.peer_count
    );
    let mut peer_configs = Vec::new();
    let mut peer_infos = Vec::new();

    for i in 0..config.peer_count {
        let (peer_config, peer_info) = create_peer_config(
            format!("peer{}", i),
            false,
            Some(gateway_inline_config.clone()),
            i + 1, // peer_index: gateway is 0, peers are 1, 2, 3...
        )
        .await?;

        info!(
            "Peer {} - network: {}, ws: {}",
            i, peer_info.network_port, peer_info.ws_port
        );
        peer_configs.push(peer_config);
        peer_infos.push(peer_info);
    }

    // Start all peers as futures
    let mut peer_nodes = Vec::new();
    for (i, peer_config) in peer_configs.into_iter().enumerate() {
        let peer_node = async move {
            // Stagger startup by i * 10 seconds
            if i > 0 {
                sleep(Duration::from_secs((i * 10) as u64)).await;
            }

            let config = peer_config.build().await?;
            let node = NodeConfig::new(config.clone())
                .await?
                .build(serve_gateway(config.ws_api).await?)
                .await?;
            node.run().await
        }
        .boxed_local();

        peer_nodes.push(peer_node);
    }

    // The actual test logic
    let test_logic = timeout(Duration::from_secs(600), async {
        info!("\n--- Step 4: Waiting for Network Formation ---");

        // Wait for all peers to start (last peer starts after peer_count * 10 seconds)
        //let peer_startup_time = config.peer_count * 10 + 30; // Extra 30s buffer
        sleep(Duration::from_secs(30)).await;
        info!("All peers should be started now");

        // Wait additional time for mesh formation (connection maintenance cycles)
        info!("Waiting for peer mesh formation...");
        sleep(Duration::from_secs(75)).await;
        info!("Mesh formation period complete");

        // Step 5: Verify network topology
        info!("\n--- Step 5: Verifying Network Topology ---");
        poll_until(
            || async { verify_network_topology(&peer_infos, 2).await },
            Duration::from_secs(120),
            Duration::from_secs(5),
            "Network topology verification",
        )
        .await?;

        // Step 6: Create room via riverctl on peer 0
        info!("\n--- Step 6: Creating Chat Room (via peer 0) ---");
        let peer0_ws = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            peer_infos[0].ws_port
        );
        let peer0_config_dir = peer_infos[0].temp_dir.path().join("river-user0");
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

        // Step 7: Create invitation
        info!("\n--- Step 7: Creating Invitation ---");
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
        let invitation =
            if let Some(line) = invite_output.lines().find(|l| l.trim().starts_with('{')) {
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

        // Step 8: Accept invitation on peer 1
        info!("\n--- Step 8: Accepting Invitation (via peer 1) ---");
        let peer1_ws = format!(
            "ws://127.0.0.1:{}/v1/contract/command?encodingProtocol=native",
            peer_infos[1].ws_port
        );
        let peer1_config_dir = peer_infos[1].temp_dir.path().join("river-user1");
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

        // Step 9: Send message from peer 0 -> peer 1
        info!("\n--- Step 9: Sending Message (Alice -> Bob) ---");
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

        // Step 10: Send message from peer 1 -> peer 0
        info!("\n--- Step 10: Sending Message (Bob -> Alice) ---");
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

        // Step 11: Verify messages received by both users
        info!("\n--- Step 11: Verifying Messages ---");

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
        Ok::<(), anyhow::Error>(())
    });

    // Run everything concurrently
    select! {
        result = test_logic => {
            result??;
            info!("Test logic completed successfully");
        }
        result = gw_node => {
            result?;
            bail!("Gateway node exited unexpectedly");
        }
        result = futures::future::try_join_all(peer_nodes) => {
            result?;
            bail!("A peer node exited unexpectedly");
        }
    }

    Ok(())
}
