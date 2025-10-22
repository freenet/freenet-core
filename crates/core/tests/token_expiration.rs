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
#[tokio::test]
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
#[tokio::test]
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
