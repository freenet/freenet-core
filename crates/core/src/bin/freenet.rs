#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use freenet::{
    config::{Config, ConfigArgs, GlobalExecutor},
    local_node::{Executor, NodeConfig, OperationMode},
    run_local_node, run_network_node,
    server::serve_client_api,
};
use std::sync::Arc;

mod commands;
use commands::{service::ServiceCommand, update::UpdateCommand};

/// Freenet - A distributed, decentralized, and censorship-resistant platform
#[derive(Parser, Debug)]
#[command(name = "freenet")]
#[command(about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    config: ConfigArgs,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the node in network mode (default if no subcommand specified)
    Network {
        #[command(flatten)]
        config: ConfigArgs,
    },
    /// Run the node in local mode
    Local {
        #[command(flatten)]
        config: ConfigArgs,
    },
    /// Manage the Freenet system service
    #[command(subcommand)]
    Service(ServiceCommand),
    /// Update Freenet to the latest version
    Update(UpdateCommand),
}

/// Build metadata embedded at compile time
mod build_info {
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const GIT_COMMIT: &str = env!("GIT_COMMIT_HASH");
    pub const GIT_DIRTY: &str = env!("GIT_DIRTY");
    pub const BUILD_TIMESTAMP: &str = env!("BUILD_TIMESTAMP");
}

async fn run(config: Config) -> anyhow::Result<()> {
    // Log build info on startup - critical for correlating logs with code version
    tracing::info!(
        version = build_info::VERSION,
        git_commit = %format!("{}{}", build_info::GIT_COMMIT, build_info::GIT_DIRTY),
        build_timestamp = build_info::BUILD_TIMESTAMP,
        "Freenet node starting"
    );

    match config.mode {
        OperationMode::Local => run_local(config).await,
        OperationMode::Network => run_network(config).await,
    }
}

async fn run_local(config: Config) -> anyhow::Result<()> {
    tracing::info!("Starting freenet node in local mode");
    let socket = config.ws_api;

    let executor = Executor::from_config_local(Arc::new(config))
        .await
        .map_err(anyhow::Error::msg)?;

    run_local_node(executor, socket)
        .await
        .map_err(anyhow::Error::msg)
}

async fn run_network(config: Config) -> anyhow::Result<()> {
    tracing::info!("Starting freenet node in network mode");

    // Check if another freenet process is already using the WS API port.
    // This gives a clear error message instead of a generic "address in use".
    check_for_existing_process(&config);

    let clients = serve_client_api(config.ws_api)
        .await
        .with_context(|| "failed to start HTTP/WebSocket client API")?;
    tracing::info!("Initializing node configuration");

    let node_config = NodeConfig::new(config)
        .await
        .with_context(|| "failed while loading node config")?;

    let node = node_config
        .build(clients)
        .await
        .with_context(|| "failed while building the node")?;

    // Get shutdown handle before starting the node
    let shutdown_handle = node.shutdown_handle();

    // Run node with signal handling for graceful shutdown
    run_network_node_with_signals(node, shutdown_handle).await
}

/// Run the network node with signal handling for graceful shutdown.
///
/// This function handles SIGTERM and SIGINT (Ctrl+C) to trigger graceful shutdown,
/// allowing the node to properly close peer connections and clean up resources.
///
/// It also monitors for version mismatches with other peers (especially the gateway).
/// When a mismatch is detected, it checks GitHub to verify a newer version exists
/// before returning an UpdateNeededError (which causes exit code 42).
async fn run_network_node_with_signals(
    node: freenet::Node,
    shutdown_handle: freenet::ShutdownHandle,
) -> anyhow::Result<()> {
    use commands::auto_update::{
        check_if_update_available, clear_version_mismatch, get_open_connection_count,
        has_reached_max_backoff, has_version_mismatch, reset_backoff, version_mismatch_generation,
        UpdateCheckResult, UpdateNeededError,
    };
    use tokio::signal;

    // Set up SIGTERM handler for Unix systems
    #[cfg(unix)]
    let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;

    // Spawn a task to listen for shutdown signals and trigger graceful shutdown
    let signal_task = {
        let shutdown_handle = shutdown_handle.clone();
        GlobalExecutor::spawn(async move {
            #[cfg(unix)]
            let shutdown_reason = tokio::select! {
                _ = signal::ctrl_c() => "received SIGINT (Ctrl+C)",
                _ = sigterm.recv() => "received SIGTERM",
            };

            #[cfg(not(unix))]
            let shutdown_reason = {
                let _ = signal::ctrl_c().await;
                "received SIGINT (Ctrl+C)"
            };

            tracing::info!(reason = shutdown_reason, "Initiating graceful shutdown");
            shutdown_handle.shutdown().await;
        })
    };

    // Spawn a task to handle SIGUSR1 for on-demand jemalloc heap profile dumps.
    // Send SIGUSR1 to dump a heap profile: kill -USR1 <pid>
    #[cfg(all(unix, feature = "jemalloc-prof"))]
    let heap_dump_task = {
        let mut sigusr1 = signal::unix::signal(signal::unix::SignalKind::user_defined1())
            .context("failed to install SIGUSR1 handler")?;
        GlobalExecutor::spawn(async move {
            loop {
                sigusr1.recv().await;
                let timestamp = chrono::Utc::now().format("%Y%m%d-%H%M%S");
                let path = format!("/tmp/freenet-heap.{timestamp}.heap");
                tracing::info!(%path, "SIGUSR1 received, dumping heap profile");
                match std::ffi::CString::new(path.as_str()) {
                    Ok(c_path) => {
                        // prof.dump mallctl expects a *const c_char (pointer to filename)
                        let ptr: *const libc::c_char = c_path.as_ptr();
                        // SAFETY: `ptr` points to a valid null-terminated C string
                        // (`c_path` is alive for the duration of the call), and
                        // "prof.dump" is a valid jemalloc mallctl key that accepts
                        // a `*const c_char` pointer to a filename.
                        let result = unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", ptr) };
                        match result {
                            Ok(()) => tracing::info!(%path, "Heap profile dumped"),
                            Err(e) => tracing::error!(error = %e, "Failed to dump heap profile"),
                        }
                    }
                    Err(e) => tracing::error!(error = %e, "Invalid heap dump path"),
                }
            }
        })
    };

    // Monitor for version mismatches and check for updates (#3204).
    // Mitigations against peers getting stuck on old versions:
    //   1. Reset backoff on fresh mismatch (prevents stale disk state)
    //   2. Exit code 42 at max backoff + 0 connections (trusts gateway signal)
    //   3. Hard timeout: 6h isolated with mismatch forces exit regardless
    //   4. Disabled for dirty builds — `freenet update` replaces the binary with a
    //      prebuilt release, which would discard local modifications (#3245)
    let (update_tx, mut update_rx) = tokio::sync::oneshot::channel::<String>();
    let auto_update_disabled = !build_info::GIT_DIRTY.is_empty();
    let update_check_task = GlobalExecutor::spawn(async move {
        use std::time::Instant;

        if auto_update_disabled {
            tracing::info!(
                git_dirty = build_info::GIT_DIRTY,
                "Auto-update disabled: this is a dirty (locally modified) build. \
                 Run `freenet update` manually if needed."
            );
            // Don't exit — just sit idle forever so the oneshot channel stays open
            std::future::pending::<()>().await;
            return;
        }

        const HARD_EXIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(6 * 3600);

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        let mut last_mismatch_generation = version_mismatch_generation();
        let mut isolated_mismatch_since: Option<Instant> = None;

        loop {
            interval.tick().await;

            // Reset backoff when a fresh mismatch signal arrives.
            let current_generation = version_mismatch_generation();
            if current_generation != last_mismatch_generation {
                last_mismatch_generation = current_generation;
                reset_backoff();
                tracing::info!(
                    generation = current_generation,
                    "Fresh version mismatch — reset update check backoff"
                );
            }

            if has_version_mismatch() {
                let open_connections = get_open_connection_count();

                // Track how long we've been isolated with a version mismatch.
                if open_connections == 0 {
                    isolated_mismatch_since.get_or_insert_with(Instant::now);
                } else {
                    isolated_mismatch_since = None;
                }

                // Hard timeout: force exit if isolated with mismatch for too long.
                if let Some(since) = isolated_mismatch_since {
                    if since.elapsed() > HARD_EXIT_TIMEOUT {
                        tracing::error!(
                            isolated_secs = since.elapsed().as_secs(),
                            "Isolated with version mismatch >6h — forcing exit for auto-update"
                        );
                        clear_version_mismatch();
                        #[allow(clippy::let_underscore_must_use)]
                        let _ = update_tx.send("unknown (hard timeout)".to_string());
                        return;
                    }
                }

                tracing::info!("Version mismatch detected, checking GitHub for updates...");

                match check_if_update_available(build_info::VERSION).await {
                    UpdateCheckResult::UpdateAvailable(new_version) => {
                        clear_version_mismatch();
                        tracing::info!(
                            new_version = %new_version,
                            "Newer version confirmed on GitHub, triggering auto-update"
                        );
                        #[allow(clippy::let_underscore_must_use)]
                        let _ = update_tx.send(new_version);
                        return;
                    }
                    UpdateCheckResult::Skipped if has_reached_max_backoff() => {
                        // Re-read connection count after the await — it may
                        // have changed during the GitHub HTTP request.
                        let open_connections = get_open_connection_count();
                        if open_connections == 0 {
                            tracing::warn!(
                                "Max backoff + 0 connections — \
                                 trusting gateway version signal, exiting for auto-update"
                            );
                            clear_version_mismatch();
                            #[allow(clippy::let_underscore_must_use)]
                            let _ = update_tx.send("unknown (gateway mismatch)".to_string());
                            return;
                        }
                        tracing::info!(
                            open_connections,
                            "Max backoff reached but node has connections — \
                             clearing version mismatch flag"
                        );
                        clear_version_mismatch();
                    }
                    UpdateCheckResult::Skipped => {}
                }
            } else {
                isolated_mismatch_since = None;
            }
        }
    });

    // Run the node - it will exit when it receives the shutdown signal or an update is needed
    let result = tokio::select! {
        r = run_network_node(node) => r,
        new_version = &mut update_rx => {
            match new_version {
                Ok(version) => {
                    tracing::info!(version = %version, "Initiating graceful shutdown for auto-update");
                    // Trigger graceful shutdown before exiting with update error.
                    // This properly closes peer connections instead of just dropping them.
                    shutdown_handle.shutdown().await;
                    Err(UpdateNeededError { new_version: version }.into())
                }
                Err(_) => {
                    // Channel closed without sending, shouldn't happen
                    Ok(())
                }
            }
        }
    };

    // Clean up tasks
    signal_task.abort();
    update_check_task.abort();
    #[cfg(all(unix, feature = "jemalloc-prof"))]
    heap_dump_task.abort();

    // Allow time for channels to drain and tasks to clean up.
    // 100ms was insufficient; 2s gives spawned tasks time to notice cancellation
    // and complete their cleanup without being forcefully killed by SIGKILL.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    if result.is_ok() {
        tracing::info!("Graceful shutdown complete");
    }

    result
}

/// Log a warning if another freenet process is already listening on the WS API port.
///
/// This is a best-effort check — it tries to connect to the port before we bind it.
/// If another process is there, we warn loudly so the user knows why startup will fail.
/// The actual binding still happens in `serve_client_api` where `SO_REUSEADDR` handles
/// TIME_WAIT sockets and the error message handles truly conflicting processes.
fn check_for_existing_process(config: &Config) {
    use std::net::{SocketAddr, TcpStream};
    use std::time::Duration;

    let addr = SocketAddr::from((config.ws_api.address, config.ws_api.port));
    if TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok() {
        let pid = find_process_on_port(config.ws_api.port);
        // Log as warn, not error — this is advisory. The bind call in
        // serve_client_api() is the authoritative check and will produce
        // the actual error if the port is still occupied.
        if let Some(pid) = pid {
            tracing::warn!(
                port = config.ws_api.port,
                pid = pid,
                "Another process (PID {pid}) is already listening on port {}. \
                 If freenet is installed as a service, use 'freenet service stop' before \
                 running manually. Otherwise use 'kill {pid}' to stop it.",
                config.ws_api.port
            );
        } else {
            tracing::warn!(
                port = config.ws_api.port,
                "Port {} is already in use by another process. \
                 If freenet is installed as a service, use 'freenet service stop' before \
                 running manually.",
                config.ws_api.port
            );
        }
    }
}

/// Try to find the PID of the process listening on the given port.
/// Returns None if we can't determine it (non-Linux, no permissions, etc.).
fn find_process_on_port(port: u16) -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        // Parse /proc/net/tcp and /proc/net/tcp6 to find the listening socket,
        // then find its inode owner. This avoids requiring external tools like lsof or ss.
        let port_hex = format!("{:04X}", port);

        // Search both IPv4 and IPv6 socket tables
        let target_inode = find_listening_inode("/proc/net/tcp", &port_hex)
            .or_else(|| find_listening_inode("/proc/net/tcp6", &port_hex))?;

        // Scan /proc/*/fd/ to find which process owns this inode
        let expected_link = format!("socket:[{target_inode}]");
        let proc_dir = std::fs::read_dir("/proc").ok()?;
        for entry in proc_dir.filter_map(|e| e.ok()) {
            let pid_str = entry.file_name();
            let pid_str = pid_str.to_string_lossy();
            if !pid_str.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            let fd_dir = format!("/proc/{pid_str}/fd");
            if let Ok(fds) = std::fs::read_dir(&fd_dir) {
                for fd in fds.filter_map(|e| e.ok()) {
                    if let Ok(link) = std::fs::read_link(fd.path()) {
                        if link.to_string_lossy() == expected_link {
                            return pid_str.parse().ok();
                        }
                    }
                }
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = port;
        None
    }
}

/// Search a /proc/net/tcp{,6} file for a LISTEN socket on the given port.
/// Returns the inode number as a string if found.
#[cfg(target_os = "linux")]
fn find_listening_inode(proc_path: &str, port_hex: &str) -> Option<String> {
    let contents = std::fs::read_to_string(proc_path).ok()?;
    parse_listening_inode(&contents, port_hex)
}

/// Parse /proc/net/tcp{,6} content for a LISTEN socket on the given hex port.
///
/// The format has a header line followed by socket entries. Each entry has
/// whitespace-separated fields:
///   [0]=sl [1]=local_address [2]=rem_address [3]=st [4]=tx_queue:rx_queue
///   [5]=tr:tm->when [6]=retrnsmt [7]=uid [8]=timeout [9]=inode ...
///
/// State 0A = TCP_LISTEN. The local_address format is hex_ip:hex_port.
#[cfg(target_os = "linux")]
fn parse_listening_inode(contents: &str, port_hex: &str) -> Option<String> {
    for line in contents.lines().skip(1) {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 10 {
            continue;
        }
        if fields[3] == "0A" {
            if let Some(addr_port) = fields[1].rsplit_once(':') {
                if addr_port.1 == port_hex {
                    return Some(fields[9].to_string());
                }
            }
        }
    }
    None
}

fn run_node(config_args: ConfigArgs) -> anyhow::Result<()> {
    if config_args.version {
        println!(
            "Freenet version: {} ({}{})",
            config_args.current_version(),
            build_info::GIT_COMMIT,
            build_info::GIT_DIRTY
        );
        println!("Build timestamp: {}", build_info::BUILD_TIMESTAMP);
        return Ok(());
    }

    // Calculate blocking threads: use CLI arg, or default (2x CPU cores, clamped to 4-32)
    let max_blocking_threads = config_args.max_blocking_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| (n.get() * 2).clamp(4, 32))
            .unwrap_or(8)
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
        )
        .max_blocking_threads(max_blocking_threads)
        // Name threads to distinguish main runtime from any rogue runtimes
        // Rogue runtimes would use default "tokio-runtime-w" name
        .thread_name("freenet-main")
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let config = config_args.build().await?;
        freenet::config::set_logger(None, None, config.paths().log_dir());
        // The logger is needed before this info which is why it's here instead of above
        tracing::info!(
            max_blocking_threads,
            "Tokio runtime configured with bounded blocking thread pool"
        );
        run(config).await
    })?;

    Ok(())
}

fn freenet_main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Service(cmd)) => {
            let rt = tokio::runtime::Runtime::new()?;
            let config = rt.block_on(cli.config.build())?;

            cmd.run(
                build_info::VERSION,
                build_info::GIT_COMMIT,
                build_info::GIT_DIRTY,
                build_info::BUILD_TIMESTAMP,
                config.paths(),
            )
        }
        Some(Command::Update(cmd)) => cmd.run(build_info::VERSION),
        Some(Command::Network { mut config }) => {
            config.mode = Some(OperationMode::Network);
            run_node(config)
        }
        Some(Command::Local { mut config }) => {
            config.mode = Some(OperationMode::Local);
            run_node(config)
        }
        None => {
            // Default behavior: run with the config from top-level args
            run_node(cli.config)
        }
    }
}

fn main() {
    use commands::auto_update::{UpdateNeededError, EXIT_CODE_UPDATE_NEEDED};

    match freenet_main() {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            // Check if this is an "update needed" error from auto-update detection
            if e.downcast_ref::<UpdateNeededError>().is_some() {
                eprintln!("Update needed, exiting for service wrapper to handle update...");
                std::process::exit(EXIT_CODE_UPDATE_NEEDED);
            }
            eprintln!("Error: {e:?}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use super::parse_listening_inode;

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_listening_inode_ipv4() {
        // Real /proc/net/tcp format with a LISTEN socket on port 7509 (0x1D55)
        let content = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000:1D55 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 54321 1 0000000000000000 100 0 0 10 0
   1: 0100007F:0035 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 11111 1 0000000000000000 100 0 0 10 0
   2: 00000000:1D55 0100007F:E234 01 00000000:00000000 00:00000000 00000000  1000        0 99999 1 0000000000000000 100 0 0 10 0";

        // Should find the LISTEN (0A) socket on port 7509, not the ESTABLISHED (01) one
        assert_eq!(
            parse_listening_inode(content, "1D55"),
            Some("54321".to_string())
        );
        // Port 53 (0x0035) is also listening
        assert_eq!(
            parse_listening_inode(content, "0035"),
            Some("11111".to_string())
        );
        // Port 8080 (0x1F90) is not present
        assert_eq!(parse_listening_inode(content, "1F90"), None);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_listening_inode_ipv6() {
        // /proc/net/tcp6 format — IPv6 addresses are 32 hex chars
        let content = "\
  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000000000000000000000000000:1D55 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 67890 1 0000000000000000 100 0 0 10 0";

        assert_eq!(
            parse_listening_inode(content, "1D55"),
            Some("67890".to_string())
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_listening_inode_short_line() {
        // Lines with fewer than 10 fields should be skipped
        let content = "\
  sl  local_address rem_address   st
   0: 00000000:1D55 00000000:0000 0A";

        assert_eq!(parse_listening_inode(content, "1D55"), None);
    }
}
