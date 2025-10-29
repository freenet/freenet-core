//! Example test demonstrating the #[freenet_test] macro usage.
//!
//! This test shows how the macro automatically handles:
//! - TestLogger initialization
//! - Multi-node setup (gateway + peers)
//! - TestContext creation
//! - Event aggregation and failure reporting

use freenet::test_utils::{TestContext, TestResult};
use freenet_macros::freenet_test;

/// Simple test with just a gateway node
#[freenet_test(nodes = ["gateway"])]
async fn test_single_node(ctx: &mut TestContext) -> TestResult {
    // The macro has already set up:
    // - TestLogger with JSON format
    // - Gateway node configuration
    // - TestContext with node info

    // Access gateway node info
    let gateway = ctx.gateway()?;
    tracing::info!(
        "Gateway running on ws_port={}, network_port={:?}",
        gateway.ws_port,
        gateway.network_port
    );

    assert!(gateway.is_gateway);
    assert!(gateway.network_port.is_some());

    Ok(())
}

/// Test with multiple nodes
#[freenet_test(
    nodes = ["gateway", "peer-1", "peer-2"],
    timeout_secs = 120,
    startup_wait_secs = 10
)]
async fn test_multi_node(ctx: &mut TestContext) -> TestResult {
    // Access individual nodes
    let gateway = ctx.node("gateway")?;
    let peer1 = ctx.node("peer-1")?;
    let peer2 = ctx.node("peer-2")?;

    tracing::info!("Gateway: ws_port={}", gateway.ws_port);
    tracing::info!("Peer 1: ws_port={}", peer1.ws_port);
    tracing::info!("Peer 2: ws_port={}", peer2.ws_port);

    // Verify node configuration
    assert!(gateway.is_gateway);
    assert!(!peer1.is_gateway);
    assert!(!peer2.is_gateway);

    // Gateway has network port, peers don't
    assert!(gateway.network_port.is_some());
    assert!(peer1.network_port.is_none());
    assert!(peer2.network_port.is_none());

    // All nodes have unique locations
    assert_ne!(gateway.location, peer1.location);
    assert_ne!(gateway.location, peer2.location);
    assert_ne!(peer1.location, peer2.location);

    Ok(())
}

/// Test that demonstrates event aggregation on failure
#[freenet_test(
    nodes = ["gateway", "peer-1"],
    aggregate_events = "on_failure"
)]
async fn test_with_event_aggregation(ctx: &mut TestContext) -> TestResult {
    // If this test fails, the macro will automatically:
    // 1. Aggregate events from all nodes
    // 2. Generate a comprehensive failure report
    // 3. Show event statistics and timeline

    let gateway = ctx.gateway()?;
    tracing::info!("Test running with gateway on port {}", gateway.ws_port);

    // Get event log paths
    let gw_log = ctx.event_log_path("gateway")?;
    let peer_log = ctx.event_log_path("peer-1")?;

    tracing::info!("Gateway event log: {:?}", gw_log);
    tracing::info!("Peer event log: {:?}", peer_log);

    Ok(())
}

/// Test that always shows event aggregation
#[freenet_test(
    nodes = ["gateway"],
    aggregate_events = "always"
)]
async fn test_always_aggregate(ctx: &mut TestContext) -> TestResult {
    // This test will show event aggregation even on success
    let gateway = ctx.gateway()?;
    tracing::info!("Running test with gateway {}", gateway.ws_port);
    Ok(())
}

/// Test with custom tokio configuration
#[freenet_test(
    nodes = ["gateway"],
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 8,
    timeout_secs = 60
)]
async fn test_custom_tokio_config(ctx: &mut TestContext) -> TestResult {
    // This test runs on a multi-threaded runtime with 8 worker threads
    let gateway = ctx.gateway()?;
    tracing::info!(
        "Test with custom tokio config - gateway on port {}",
        gateway.ws_port
    );

    // Verify we're on a multi-threaded runtime by checking if we can spawn tasks
    let handle = tokio::spawn(async {
        tracing::info!("Spawned task is running");
        42
    });

    let result = handle.await?;
    assert_eq!(result, 42);

    Ok(())
}

/// Test with single-threaded runtime
#[freenet_test(
    nodes = ["gateway"],
    tokio_flavor = "current_thread",
    timeout_secs = 60,
    startup_wait_secs = 10
)]
async fn test_current_thread_runtime(ctx: &mut TestContext) -> TestResult {
    // This test runs on a single-threaded runtime
    let gateway = ctx.gateway()?;
    tracing::info!(
        "Test with current_thread runtime - gateway on port {}",
        gateway.ws_port
    );
    Ok(())
}
