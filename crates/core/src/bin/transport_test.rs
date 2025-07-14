//! Minimal transport test for debugging connection stability
//!
//! This creates a simple UDP client/server to test connection patterns
//! similar to Freenet's transport layer, focusing on reproducing the
//! 30-second timeout issue.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::net::UdpSocket;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "transport-test")]
#[command(about = "Minimal transport layer test for debugging connection stability")]
struct Args {
    /// Run as server (listener) or client (connector)
    #[arg(long)]
    server: bool,

    /// Address to bind to (server) or connect to (client)
    #[arg(long, default_value = "0.0.0.0:31337")]
    address: String,

    /// For client mode: remote server address
    #[arg(long)]
    remote: Option<String>,

    /// Keep-alive interval in seconds
    #[arg(long, default_value = "10")]
    keepalive: u64,

    /// Enable verbose logging
    #[arg(long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    let filter = if args.verbose {
        "transport_test=trace,freenet=trace"
    } else {
        "transport_test=info,freenet=info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(true)
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .init();

    let addr: SocketAddr = args.address.parse()?;

    if args.server {
        run_server(addr, args.keepalive).await
    } else {
        let remote = args
            .remote
            .ok_or_else(|| anyhow::anyhow!("--remote required in client mode"))?;
        let remote_addr: SocketAddr = remote.parse()?;
        run_client(addr, remote_addr, args.keepalive).await
    }
}

async fn run_server(bind_addr: SocketAddr, keepalive_interval: u64) -> anyhow::Result<()> {
    info!("Starting transport test SERVER on {}", bind_addr);
    info!("Keep-alive interval: {}s", keepalive_interval);

    let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
    info!("UDP socket bound successfully");

    // Track connection state
    let mut clients = std::collections::HashMap::new();
    let start_time = Instant::now();

    // Stats
    let messages_received = Arc::new(AtomicU64::new(0));

    // Buffer for receiving
    let mut buf = vec![0u8; 65536];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((len, from)) => {
                let msg_count = messages_received.fetch_add(1, Ordering::Relaxed) + 1;
                let now = Instant::now();
                let uptime = start_time.elapsed().as_secs();

                // Parse message type
                if len >= 4 {
                    let msg_type = &buf[0..4];
                    match msg_type {
                        b"KEEP" => {
                            // Track last keep-alive time
                            let last_seen = clients.entry(from).or_insert(now);
                            let since_last = now.duration_since(*last_seen).as_secs_f64();
                            *last_seen = now;

                            info!(
                                "Keep-alive from {} | Time since last: {:.1}s | Total msgs: {} | Uptime: {}s",
                                from, since_last, msg_count, uptime
                            );

                            // Echo back
                            if let Err(e) = socket.send_to(b"ACKK", from).await {
                                error!("Failed to send keep-alive ACK: {}", e);
                            }
                        }
                        b"DATA" => {
                            info!(
                                "Data message from {} | {} bytes | Uptime: {}s",
                                from, len, uptime
                            );

                            // Echo back
                            if let Err(e) = socket.send_to(b"ACKD", from).await {
                                error!("Failed to send data ACK: {}", e);
                            }
                        }
                        _ => {
                            warn!(
                                "Unknown message type from {}: {:?}",
                                from,
                                &buf[0..4.min(len)]
                            );
                        }
                    }
                }

                // Check for timed-out clients (simulate 30s timeout)
                let timeout_threshold = Duration::from_secs(30);
                clients.retain(|addr, last_seen| {
                    let elapsed = now.duration_since(*last_seen);
                    if elapsed > timeout_threshold {
                        warn!(
                            "Client {} timed out (no message for {}s)",
                            addr,
                            elapsed.as_secs()
                        );
                        false
                    } else {
                        true
                    }
                });
            }
            Err(e) => {
                error!("Error receiving packet: {}", e);
            }
        }
    }
}

async fn run_client(
    bind_addr: SocketAddr,
    remote_addr: SocketAddr,
    keepalive_interval: u64,
) -> anyhow::Result<()> {
    info!("Starting transport test CLIENT");
    info!("Local bind: {}", bind_addr);
    info!("Remote server: {}", remote_addr);
    info!("Keep-alive interval: {}s", keepalive_interval);

    let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
    info!("UDP socket bound successfully");

    // Connection state
    let connected = Arc::new(AtomicBool::new(false));
    let last_received = Arc::new(parking_lot::Mutex::new(Instant::now()));
    let start_time = Instant::now();

    // Stats
    let keepalives_sent = Arc::new(AtomicU64::new(0));
    let keepalives_acked = Arc::new(AtomicU64::new(0));

    // Keep-alive task
    let socket_send = socket.clone();
    let ka_sent = keepalives_sent.clone();
    let ka_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(keepalive_interval));

        loop {
            ticker.tick().await;

            let count = ka_sent.fetch_add(1, Ordering::Relaxed) + 1;
            let uptime = start_time.elapsed().as_secs();

            info!("Sending keep-alive #{} | Uptime: {}s", count, uptime);

            match socket_send.send_to(b"KEEP", remote_addr).await {
                Ok(_) => {
                    debug!("Keep-alive sent successfully");
                }
                Err(e) => {
                    error!("Failed to send keep-alive: {}", e);
                }
            }
        }
    });

    // Receive task
    let socket_recv = socket.clone();
    let last_received_clone = last_received.clone();
    let connected_clone = connected.clone();
    let recv_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 65536];

        loop {
            match socket_recv.recv_from(&mut buf).await {
                Ok((len, from)) => {
                    if from != remote_addr {
                        warn!("Received packet from unexpected source: {}", from);
                        continue;
                    }

                    let now = Instant::now();
                    *last_received_clone.lock() = now;

                    if !connected_clone.load(Ordering::Relaxed) {
                        connected_clone.store(true, Ordering::Relaxed);
                        info!("Connection established with {}", from);
                    }

                    if len >= 4 {
                        let msg_type = &buf[0..4];
                        match msg_type {
                            b"ACKK" => {
                                let acked = keepalives_acked.fetch_add(1, Ordering::Relaxed) + 1;
                                let sent = keepalives_sent.load(Ordering::Relaxed);
                                let uptime = start_time.elapsed().as_secs();

                                info!(
                                    "Keep-alive ACK received | Sent: {} Acked: {} | Uptime: {}s",
                                    sent, acked, uptime
                                );
                            }
                            b"ACKD" => {
                                debug!("Data ACK received");
                            }
                            _ => {
                                warn!("Unknown message type: {:?}", &buf[0..4.min(len)]);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving packet: {}", e);
                }
            }
        }
    });

    // Monitor task - check for timeout
    let monitor_task = tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));

        loop {
            ticker.tick().await;

            if connected.load(Ordering::Relaxed) {
                let elapsed = last_received.lock().elapsed();
                if elapsed > Duration::from_secs(30) {
                    error!(
                        "CONNECTION TIMEOUT: No packet received for {}s (30s threshold exceeded)",
                        elapsed.as_secs()
                    );
                    connected.store(false, Ordering::Relaxed);
                }
            }
        }
    });

    // Wait for tasks
    tokio::select! {
        _ = ka_task => warn!("Keep-alive task ended"),
        _ = recv_task => warn!("Receive task ended"),
        _ = monitor_task => warn!("Monitor task ended"),
    }

    Ok(())
}
