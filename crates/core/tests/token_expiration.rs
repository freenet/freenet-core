//! Integration tests for authentication token expiration configuration.
//!
//! These tests verify that:
//! - Token TTL and cleanup interval can be configured
//! - Configuration is properly passed through to the cleanup task
//! - Short TTL values work correctly (for testing purposes)

use freenet::{
    config::{ConfigArgs, NetworkArgs, SecretArgs, WebsocketApiArgs},
    dev_tool::TransportKeypair,
    local_node::OperationMode,
};
use std::net::{Ipv4Addr, TcpListener};
use testresult::TestResult;
use tracing::{info, span, Instrument, Level};

/// Creates a local node configuration with custom token TTL and cleanup interval.
async fn create_test_config(
    token_ttl_seconds: u64,
    cleanup_interval_seconds: u64,
) -> anyhow::Result<(ConfigArgs, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let key = TransportKeypair::new();
    let transport_keypair = temp_dir.path().join("private.pem");
    key.save(&transport_keypair)?;

    let ws_socket = TcpListener::bind("127.0.0.1:0")?;
    let network_socket = TcpListener::bind("127.0.0.1:0")?;

    let config = ConfigArgs {
        mode: Some(OperationMode::Local),
        ws_api: WebsocketApiArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            ws_api_port: Some(ws_socket.local_addr()?.port()),
            token_ttl_seconds: Some(token_ttl_seconds),
            token_cleanup_interval_seconds: Some(cleanup_interval_seconds),
        },
        network_api: NetworkArgs {
            address: Some(Ipv4Addr::LOCALHOST.into()),
            network_port: Some(network_socket.local_addr()?.port()),
            transient_budget: None,
            transient_ttl_secs: None,
            ..Default::default()
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

    Ok((config, temp_dir))
}

/// Test that token configuration values are properly accepted and applied.
/// This verifies that the cleanup task can be configured with short intervals for testing.
#[test_log::test(tokio::test)]
async fn test_token_configuration() -> TestResult {
    let span = span!(Level::INFO, "test_token_configuration");
    async move {
        // Configure very short TTL (2 seconds) and cleanup interval (1 second)
        const TOKEN_TTL_SECS: u64 = 2;
        const CLEANUP_INTERVAL_SECS: u64 = 1;

        info!("Creating test configuration with custom token TTL and cleanup interval");
        let (config, _temp_dir) = create_test_config(TOKEN_TTL_SECS, CLEANUP_INTERVAL_SECS).await?;

        // Verify configuration was accepted
        assert_eq!(
            config.ws_api.token_ttl_seconds,
            Some(TOKEN_TTL_SECS),
            "Token TTL should be configured"
        );
        assert_eq!(
            config.ws_api.token_cleanup_interval_seconds,
            Some(CLEANUP_INTERVAL_SECS),
            "Token cleanup interval should be configured"
        );

        info!("Building node configuration");
        let built_config = config.build().await?;

        // Verify configuration was properly applied
        assert_eq!(
            built_config.ws_api.token_ttl_seconds, TOKEN_TTL_SECS,
            "Built config should have correct token TTL"
        );
        assert_eq!(
            built_config.ws_api.token_cleanup_interval_seconds, CLEANUP_INTERVAL_SECS,
            "Built config should have correct cleanup interval"
        );

        info!("✓ Configuration accepted custom token values");
        info!("✓ Token TTL: {} seconds", TOKEN_TTL_SECS);
        info!("✓ Cleanup interval: {} seconds", CLEANUP_INTERVAL_SECS);

        Ok(())
    }
    .instrument(span)
    .await
}

/// Test that default token configuration values are used when not specified.
#[test_log::test(tokio::test)]
async fn test_default_token_configuration() -> TestResult {
    let span = span!(Level::INFO, "test_default_token_configuration");
    async move {
        info!("Creating test configuration without custom token settings");

        let temp_dir = tempfile::tempdir()?;
        let key = TransportKeypair::new();
        let transport_keypair = temp_dir.path().join("private.pem");
        key.save(&transport_keypair)?;

        let ws_socket = TcpListener::bind("127.0.0.1:0")?;
        let network_socket = TcpListener::bind("127.0.0.1:0")?;

        let config = ConfigArgs {
            mode: Some(OperationMode::Local),
            ws_api: WebsocketApiArgs {
                address: Some(Ipv4Addr::LOCALHOST.into()),
                ws_api_port: Some(ws_socket.local_addr()?.port()),
                // Don't specify token_ttl_seconds or token_cleanup_interval_seconds
                // to test default values
                token_ttl_seconds: None,
                token_cleanup_interval_seconds: None,
            },
            network_api: NetworkArgs {
                address: Some(Ipv4Addr::LOCALHOST.into()),
                network_port: Some(network_socket.local_addr()?.port()),
                transient_budget: None,
                transient_ttl_secs: None,
                ..Default::default()
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

        info!("Building node configuration");
        let built_config = config.build().await?;

        // Verify default values are applied
        assert_eq!(
            built_config.ws_api.token_ttl_seconds,
            86400, // 24 hours
            "Default token TTL should be 24 hours (86400 seconds)"
        );
        assert_eq!(
            built_config.ws_api.token_cleanup_interval_seconds,
            300, // 5 minutes
            "Default cleanup interval should be 5 minutes (300 seconds)"
        );

        info!("✓ Default token configuration values applied correctly");
        info!("  - Token TTL: 86400 seconds (24 hours)");
        info!("  - Cleanup interval: 300 seconds (5 minutes)");

        Ok(())
    }
    .instrument(span)
    .await
}

/// Test that the cleanup task actually removes expired tokens from the map.
/// This test exercises the full token expiration lifecycle.
#[test_log::test(tokio::test)]
async fn test_token_cleanup_removes_expired_tokens() -> TestResult {
    use freenet::{
        config::WebsocketApiConfig,
        dev_tool::{AuthToken, ClientId},
        server::{serve_gateway_for_test, AttestedContract},
        test_utils,
    };
    use std::time::Duration;
    use tokio::time::sleep;

    let span = span!(Level::INFO, "test_token_cleanup_removes_expired_tokens");
    async move {
        // Configure very short TTL (2 seconds) and cleanup interval (1 second) for fast testing
        const TOKEN_TTL_SECS: u64 = 2;
        const CLEANUP_INTERVAL_SECS: u64 = 1;

        info!(
            "Starting gateway with short token TTL ({} seconds) and cleanup interval ({} seconds)",
            TOKEN_TTL_SECS, CLEANUP_INTERVAL_SECS
        );

        let ws_socket = TcpListener::bind("127.0.0.1:0")?;
        let ws_port = ws_socket.local_addr()?.port();
        // Drop the socket to release the port before the gateway binds to it
        drop(ws_socket);

        let config = WebsocketApiConfig {
            address: Ipv4Addr::LOCALHOST.into(),
            port: ws_port,
            token_ttl_seconds: TOKEN_TTL_SECS,
            token_cleanup_interval_seconds: CLEANUP_INTERVAL_SECS,
        };

        // Start the gateway server (which spawns the cleanup task)
        let (gw, _ws_proxy) = serve_gateway_for_test(config).await?;

        // Access the attested_contracts map via the test-only method
        let attested_contracts = gw.attested_contracts();

        info!("Creating test tokens and contract IDs");

        // Create some test tokens
        let token1 = AuthToken::generate();
        let token2 = AuthToken::generate();
        let token3 = AuthToken::generate();

        // Load actual contracts to get valid contract IDs
        let contract1 = test_utils::load_contract("test-contract-integration", vec![].into())?;
        let contract2 = test_utils::load_contract("test-contract-integration", vec![1u8].into())?;
        let contract3 = test_utils::load_contract("test-contract-integration", vec![2u8].into())?;

        let contract_id1 = contract1.key().into();
        let contract_id2 = contract2.key().into();
        let contract_id3 = contract3.key().into();

        // Create test client IDs
        let client_id1 = ClientId::next();
        let client_id2 = ClientId::next();
        let client_id3 = ClientId::next();

        // Insert tokens into the map
        attested_contracts.insert(
            token1.clone(),
            AttestedContract::new(contract_id1, client_id1),
        );
        attested_contracts.insert(
            token2.clone(),
            AttestedContract::new(contract_id2, client_id2),
        );
        attested_contracts.insert(
            token3.clone(),
            AttestedContract::new(contract_id3, client_id3),
        );

        info!("Inserted 3 tokens into attested_contracts map");
        assert_eq!(
            attested_contracts.len(),
            3,
            "Should have 3 tokens before expiration"
        );

        // Wait for tokens to expire: TTL (2s) + cleanup interval (1s) + buffer (1s) = 4s
        let wait_time = Duration::from_secs(TOKEN_TTL_SECS + CLEANUP_INTERVAL_SECS + 1);
        info!(
            "Waiting {} seconds for tokens to expire and cleanup task to run",
            wait_time.as_secs()
        );
        sleep(wait_time).await;

        // Check that tokens have been removed
        let remaining_count = attested_contracts.len();
        info!("After cleanup: {} tokens remaining", remaining_count);

        assert_eq!(
            remaining_count, 0,
            "All tokens should be removed after expiration"
        );

        // Verify individual tokens are gone
        assert!(
            !attested_contracts.contains_key(&token1),
            "Token 1 should be removed"
        );
        assert!(
            !attested_contracts.contains_key(&token2),
            "Token 2 should be removed"
        );
        assert!(
            !attested_contracts.contains_key(&token3),
            "Token 3 should be removed"
        );

        info!("✓ Cleanup task successfully removed all expired tokens");

        Ok(())
    }
    .instrument(span)
    .await
}
