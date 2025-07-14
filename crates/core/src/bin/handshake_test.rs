#!/usr/bin/env rust
//! Minimal handshake test to isolate where Freenet handshake protocol fails
//!
//! This binary progressively tests handshake steps:
//! 1. Send basic UDP packet (like transport-test)
//! 2. Send packet with Freenet header structure
//! 3. Send actual initial handshake packet
//!
//! Usage:
//!   handshake-test --remote 136.62.52.28:31337  # Test against ziggy
//!   handshake-test --remote 100.27.151.80:31337  # Test against vega

use clap::Parser;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

#[derive(Parser)]
struct Args {
    /// Remote gateway address to test against
    #[clap(long)]
    remote: SocketAddr,

    /// Local address to bind to
    #[clap(long, default_value = "0.0.0.0:0")]
    local: SocketAddr,

    /// Enable verbose logging
    #[clap(long)]
    verbose: bool,
}

async fn test_basic_udp(socket: &UdpSocket, remote: SocketAddr) -> anyhow::Result<()> {
    info!("Phase 1: Testing basic UDP connectivity");

    let test_packet = b"HANDSHAKE_TEST_BASIC_UDP";

    for i in 1..=3 {
        debug!("Sending basic UDP packet #{}", i);

        let start = Instant::now();
        socket.send_to(test_packet, remote).await?;

        // Try to receive any response (with short timeout)
        match timeout(
            Duration::from_millis(1000),
            socket.recv_from(&mut [0u8; 1024]),
        )
        .await
        {
            Ok(Ok((len, addr))) => {
                info!(
                    "✓ Received response from {}: {} bytes (took {:?})",
                    addr,
                    len,
                    start.elapsed()
                );
            }
            Ok(Err(e)) => {
                warn!("✗ UDP error: {}", e);
            }
            Err(_) => {
                debug!("✗ No response to basic UDP packet #{} (expected)", i);
            }
        }

        sleep(Duration::from_millis(500)).await;
    }

    info!("Phase 1 complete: Basic UDP connectivity tested");
    Ok(())
}

async fn test_freenet_header(socket: &UdpSocket, remote: SocketAddr) -> anyhow::Result<()> {
    info!("Phase 2: Testing packet with Freenet-like header");

    // Create a packet that looks somewhat like a Freenet packet structure
    // but is clearly a test packet
    let mut test_packet = Vec::new();
    test_packet.extend_from_slice(b"FNET"); // Fake Freenet magic
    test_packet.extend_from_slice(&1u32.to_be_bytes()); // Version
    test_packet.extend_from_slice(&0u32.to_be_bytes()); // Test packet type
    test_packet.extend_from_slice(b"HANDSHAKE_TEST_HEADER"); // Payload

    for i in 1..=3 {
        debug!("Sending Freenet-header test packet #{}", i);

        let start = Instant::now();
        socket.send_to(&test_packet, remote).await?;

        // Try to receive any response
        match timeout(
            Duration::from_millis(2000),
            socket.recv_from(&mut [0u8; 1024]),
        )
        .await
        {
            Ok(Ok((len, addr))) => {
                info!(
                    "✓ Received response from {}: {} bytes (took {:?})",
                    addr,
                    len,
                    start.elapsed()
                );
                return Ok(()); // Got a response, this is interesting!
            }
            Ok(Err(e)) => {
                warn!("✗ UDP error: {}", e);
            }
            Err(_) => {
                debug!("✗ No response to header test packet #{}", i);
            }
        }

        sleep(Duration::from_millis(500)).await;
    }

    info!("Phase 2 complete: Freenet header test (no responses expected)");
    Ok(())
}

async fn test_connection_attempt_logging(
    socket: &UdpSocket,
    remote: SocketAddr,
) -> anyhow::Result<()> {
    info!("Phase 3: Testing rapid connection attempts (to trigger handshake errors)");

    // Send multiple "connection-like" packets quickly to see if we can trigger
    // the same handshake errors we see in the full Freenet logs
    let test_packet = b"FREENET_CONNECTION_TEST_RAPID_ATTEMPTS";

    let start_time = Instant::now();
    for i in 1..=10 {
        debug!("Sending rapid connection attempt #{}", i);
        socket.send_to(test_packet, remote).await?;

        // Very short delay to simulate rapid connection attempts
        sleep(Duration::from_millis(100)).await;
    }

    // Wait a bit to see if any delayed responses come in
    info!("Waiting for any delayed responses...");
    for _ in 0..5 {
        match timeout(
            Duration::from_millis(1000),
            socket.recv_from(&mut [0u8; 1024]),
        )
        .await
        {
            Ok(Ok((len, addr))) => {
                info!(
                    "✓ Delayed response from {}: {} bytes (total time: {:?})",
                    addr,
                    len,
                    start_time.elapsed()
                );
            }
            Ok(Err(e)) => {
                warn!("✗ UDP error: {}", e);
                break;
            }
            Err(_) => {
                debug!("No delayed response");
                break;
            }
        }
    }

    info!("Phase 3 complete: Rapid connection attempts tested");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing_subscriber::fmt()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(true)
        .init();

    info!("Starting handshake test");
    info!("Local bind: {}", args.local);
    info!("Remote target: {}", args.remote);

    // Bind UDP socket
    let socket = UdpSocket::bind(args.local).await?;
    let local_addr = socket.local_addr()?;
    info!("UDP socket bound successfully to {}", local_addr);

    // Run test phases
    test_basic_udp(&socket, args.remote).await?;
    test_freenet_header(&socket, args.remote).await?;
    test_connection_attempt_logging(&socket, args.remote).await?;

    info!("All handshake test phases complete");
    info!("Next step: Monitor gateway logs for any evidence of receiving our test packets");

    Ok(())
}
