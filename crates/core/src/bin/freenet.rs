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
use commands::{
    secrets_cmd::SecretsCliConfig, service::ServiceCommand, uninstall::UninstallCommand,
    update::UpdateCommand,
};

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
    ///
    /// NOTE ON AUTO-UPDATE: a node detects new releases and exits with code 42
    /// to request an update, but it does NOT update itself. Applying the update
    /// requires a supervisor that catches exit code 42 and runs `freenet update`
    /// before restarting — this is set up by `freenet service install` (systemd
    /// on Linux, a launchd wrapper on macOS, the tray wrapper on Windows).
    ///
    /// A bare `freenet network` run has no such supervisor: it will detect an
    /// update, exit, and NOT be restarted, so it stays on its current version.
    /// To keep a hand-run node current, either run `freenet update` yourself when
    /// prompted in the logs, or install Freenet as a service. Dirty/dev builds
    /// disable auto-update entirely (it would clobber local changes).
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
    /// Completely uninstall Freenet (service, binaries, and optionally data)
    Uninstall(UninstallCommand),
    /// Manage the node KEK (Key Encryption Key) backend.
    ///
    /// The KEK is the master key from which every per-delegate DEK is
    /// derived via HKDF. Subcommands report status, rotate, or migrate
    /// the KEK between backends (OS keyring / systemd credential / file).
    Secrets(SecretsCliConfig),
}

/// Build metadata embedded at compile time
mod build_info {
    pub const VERSION: &str = env!("CARGO_PKG_VERSION");
    pub const GIT_COMMIT: &str = env!("GIT_COMMIT_HASH");
    pub const GIT_DIRTY: &str = env!("GIT_DIRTY");
    pub const BUILD_TIMESTAMP: &str = env!("BUILD_TIMESTAMP");
}

/// Raise the process's `RLIMIT_NOFILE` (open file-descriptor) soft limit up to
/// its hard limit, so a busy node does not hit `EMFILE` ("No file descriptors
/// available", os error 24).
///
/// WHY: the WASM module cache holds up to ~1024 compiled contracts, each backed
/// by a `memfd`, plus AOF segment files, UDP sockets, the WS API, etc. systemd's
/// `DefaultLimitNOFILE` soft limit is typically 1024, and freenet never raised
/// it, so a busy gateway exhausted file descriptors. The resulting `EMFILE`
/// killed a monitored background task (`refresh_router` opening an AOF segment),
/// which the `BackgroundTaskMonitor` treats as node-fatal — producing a
/// systemd crash-loop. Raising the soft limit to the hard limit at startup gives
/// the node the FD headroom the kernel already permits, without operator
/// intervention or a unit-file change.
///
/// Best-effort and non-fatal: on any error we `warn!` and continue with the
/// inherited limit rather than failing startup. No-op on non-unix.
#[cfg(unix)]
fn raise_fd_limit() {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` with a valid resource id and a properly initialized,
    // exclusively-borrowed `rlimit` out-param is sound; we check the return code
    // before reading the struct.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) } != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(error = %err, "failed to read RLIMIT_NOFILE; leaving fd limit unchanged");
        return;
    }

    // `rlim_t` is `u64` on linux-gnu but `i64`/`u64` varies across unix targets;
    // normalize to `u64` for logging. The cast is redundant on the CI target,
    // hence the scoped allow rather than a bare `as u64` that trips clippy.
    #[allow(clippy::unnecessary_cast)]
    let previous_soft = limits.rlim_cur as u64;
    #[allow(clippy::unnecessary_cast)]
    let hard = limits.rlim_max as u64;

    if previous_soft >= hard {
        // Already at (or above) the hard limit — nothing to raise.
        tracing::info!(
            soft = previous_soft,
            hard,
            "RLIMIT_NOFILE soft limit already at hard limit"
        );
        return;
    }

    limits.rlim_cur = limits.rlim_max;
    // SAFETY: `setrlimit` with a valid resource id and an exclusively-borrowed,
    // fully-initialized `rlimit` whose soft limit (rlim_cur) does not exceed the
    // hard limit (rlim_max) is sound.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limits) } != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(
            error = %err,
            soft = previous_soft,
            hard,
            "failed to raise RLIMIT_NOFILE soft limit; leaving fd limit unchanged"
        );
        return;
    }

    tracing::info!(
        soft = hard,
        hard,
        previous_soft,
        "raised RLIMIT_NOFILE soft limit"
    );
}

#[cfg(not(unix))]
fn raise_fd_limit() {
    // No RLIMIT_NOFILE concept on non-unix targets; nothing to do.
}

async fn run(config: Config) -> anyhow::Result<()> {
    // Raise the open-fd soft limit before the node starts serving, so a busy
    // node does not crash-loop on EMFILE (see `raise_fd_limit` rustdoc).
    raise_fd_limit();

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
    let socket = config.ws_api.clone();

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
    // If so, bail out immediately instead of proceeding to bind and fail.
    // This prevents systemd restart loops when another instance is running.
    check_for_existing_process(&config)?;

    let clients = serve_client_api(config.ws_api.clone())
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
        UPDATE_REPOLL_INTERVAL, UPDATE_REPOLL_JITTER_FRACTION, UpdateCheckResult,
        UpdateNeededError, check_if_update_available, clear_version_mismatch,
        get_open_connection_count, has_reached_max_backoff, has_version_mismatch,
        jittered_repoll_interval, reset_backoff, should_attempt_update, startup_update_check,
        version_mismatch_generation,
    };
    use freenet::transport::{clear_urgent_update, get_highest_seen_version, is_urgent_update};
    use tokio::signal;

    // #4549: this is the real node process (not a test / embedded use), so enable the
    // fast-exit watchdog — a fatal network-event-listener exit will abort the process
    // (code 42) for a prompt systemd restart + auto-update, instead of risking a hang
    // in teardown that leaves the node network-dead and spinning.
    freenet::enable_abort_on_fatal_listener_exit();

    // #4551: only emit the distinct fast-crash exit code (45) for a sub-60s fatal
    // exit when the supervising unit actually understands it. The regenerated Freenet
    // systemd unit advertises that via SYSTEMD_FAST_CRASH_ENV_VAR (it keeps 45 out of
    // SuccessExitStatus, sets StartLimitAction=none, and runs `freenet update` on 42
    // OR 45). A node running this binary under an OLD/custom systemd unit (auto-updated
    // but not reinstalled), under the macOS/Windows run-wrapper (knows only 42), or
    // unsupervised must keep exiting 42 so its existing exit-42 self-heal/limiting
    // still works — so the marker, not a broad systemd heuristic, is the gate.
    if std::env::var_os(commands::auto_update::SYSTEMD_FAST_CRASH_ENV_VAR).is_some() {
        freenet::enable_fast_crash_exit_code();
    }

    // #4604: this is the real node process, so let the contract storage layer
    // exit-for-restart if redb gets poisoned by a transient I/O error (e.g. a disk
    // EIO / filesystem csum failure). Without this the node stays "running" while
    // every contract GET/PUT/UPDATE fails forever; with it, the supervisor restarts
    // the node with a fresh database handle. The exit code reuses the fatal-listener
    // decision above, so a database that re-poisons on every boot (persistent
    // corruption) is bounded by the same crash-loop protection rather than looping.
    freenet::enable_abort_on_redb_poison();

    // #4073 crash-loop auto-rollback: once this (possibly freshly auto-updated)
    // node has run healthily for the commit window, clear any post-update
    // probation marker so ordinary later crashes never trigger a rollback. The
    // window mirrors MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT — a node that survives
    // it has cleared the same fast-crash boundary. Fire-and-forget: if the node
    // crashes before the window elapses, the task dies with the process and the
    // probation marker stays armed (so the supervisor's post-stop `freenet
    // update` can count the crash and eventually roll back).
    let commit_probation_task = {
        let version = build_info::VERSION.to_string();
        GlobalExecutor::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(
                commands::rollback::COMMIT_HEALTHY_UPTIME_SECS,
            ))
            .await;
            commands::rollback::commit_probation(&version);
        })
    };

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
    //
    // Four update triggers (checked each 60s tick):
    //   1. Urgent update: remote's min_compatible > our version → verify with GitHub, exit immediately
    //   2. Decentralized discovery: highest_seen_version > our version → start stagger timer,
    //      verify with GitHub when timer expires
    //   3. Legacy mismatch: existing backoff-based mechanism (fallback)
    //   4. Periodic re-poll (#4073): re-run the direct startup GitHub check on a
    //      recurring, jittered ~6h schedule so a long-running node still notices a
    //      new release without a restart — even when every peer is on the same old
    //      version (so triggers 1-3 never fire). Detection-only: feeds the SAME
    //      exit-42 path.
    //
    // GitHub verification before exit-42 is always required — decentralized discovery
    // tells us *when* to check, GitHub confirms *what* to install.
    //
    // Disabled for dirty builds — `freenet update` replaces the binary with a
    // prebuilt release, which would discard local modifications (#3245)
    let (update_tx, mut update_rx) = tokio::sync::oneshot::channel::<String>();
    let auto_update_disabled = !build_info::GIT_DIRTY.is_empty();
    let update_check_task = GlobalExecutor::spawn(async move {
        use std::time::Instant;

        if auto_update_disabled {
            // Loud, operator-visible (issue #4580): a dirty/dev build never even
            // checks for updates, so this node will stay on its current version
            // indefinitely with no further signal. That is the intended
            // behaviour for a local build (auto-update would clobber local
            // changes, #3245), but it must be surfaced rather than silent.
            tracing::warn!(
                git_dirty = build_info::GIT_DIRTY,
                "Auto-update is DISABLED for this dirty (locally modified) build: this node will \
                 NOT detect or apply updates and will stay on version {} until you act. This is \
                 expected for a local build. Run `freenet update` manually to switch to the \
                 latest release, or install a clean release build to re-enable auto-update.",
                build_info::VERSION,
            );
            std::future::pending::<()>().await;
            return;
        }

        // --- Startup update check (#3864) ---
        //
        // Ask GitHub directly, once at boot, whether a newer release exists.
        // This closes the "offline-for-days transient peer" gap where a node
        // that has fallen out of the compatible-version window has no way to
        // discover the new release via peer handshake (handshakes with an
        // incompatible peer may never complete), so the peer-signal-driven
        // update loop below never fires.
        //
        // Cross-platform: on Linux (systemd ExecStopPost), macOS (wrapper
        // script), and Windows (wrapper loop), a successful update is
        // propagated through `update_tx` → graceful shutdown → exit 42 →
        // the service manager runs `freenet update --quiet` and restarts
        // the freshly installed binary.
        //
        // Small jitter (0-60s) avoids a thundering-herd GitHub API hit when
        // many nodes restart together (e.g. post-outage). Jitter lives in
        // the caller so the helper stays pure and unit-testable.
        //
        // Fail-open: any GitHub / parse error returns None and the node
        // continues booting normally into the peer-signal loop below.
        let startup_jitter_secs = freenet::config::GlobalRng::random_u64() % 60;
        if startup_jitter_secs > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(startup_jitter_secs)).await;
        }
        tracing::info!(
            current = build_info::VERSION,
            jitter_secs = startup_jitter_secs,
            "Startup update check against GitHub"
        );
        if let Some(new_version) = startup_update_check(build_info::VERSION).await {
            // #4073: don't auto-update to a version that is locally BLOCKED — a
            // crash-loop known-bad pin OR a version that has repeatedly failed to
            // install (checksum / signature / download / extract). The installer
            // would refuse it anyway; gating here avoids a needless exit-42
            // restart cycle and is what stops the failed-install loop.
            if commands::rollback::is_version_pinned_bad(&new_version)
                || commands::rollback::is_version_install_gated(&new_version)
            {
                tracing::warn!(
                    new_version = %new_version,
                    "Startup check: newer version is locally blocked (crash-loop known-bad pin or \
                     repeated install failures); not triggering auto-update (#4073)"
                );
            } else {
                tracing::info!(
                    new_version = %new_version,
                    "Startup check: newer version on GitHub, triggering auto-update"
                );
                #[allow(clippy::let_underscore_must_use)]
                let _ = update_tx.send(new_version);
                return;
            }
        }
        tracing::debug!("Startup update check: no newer version found");

        /// Parse our version string into a (major, minor, patch) tuple for comparison.
        fn parse_our_version() -> Option<(u8, u8, u16)> {
            let parts: Vec<&str> = build_info::VERSION.split('.').collect();
            if parts.len() < 3 {
                return None;
            }
            let major: u8 = parts[0].parse().ok()?;
            let minor: u8 = parts[1].parse().ok()?;
            // Patch may have pre-release suffix
            let patch_str = parts[2].split('-').next()?;
            let patch: u16 = patch_str.parse().ok()?;
            Some((major, minor, patch))
        }

        const HARD_EXIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(6 * 3600);
        // Stagger timer: random delay 0-4 hours before updating on decentralized discovery.
        const MAX_STAGGER_SECS: u64 = 4 * 3600;
        // After a stagger check returns Skipped (GitHub unreachable or no update),
        // wait at least this long before re-arming. Prevents polling GitHub every
        // few hours indefinitely when peers report a version that isn't on GitHub yet.
        const STAGGER_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

        // Compute the delay until the next periodic GitHub re-poll, jittering the
        // base interval by ±UPDATE_REPOLL_JITTER_FRACTION using GlobalRng so nodes
        // that booted together do not poll (and restart) in lockstep.
        let repoll_delay = || {
            let rand_unit = freenet::config::GlobalRng::random_range(0.0_f64..1.0);
            jittered_repoll_interval(
                UPDATE_REPOLL_INTERVAL,
                UPDATE_REPOLL_JITTER_FRACTION,
                rand_unit,
            )
        };

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        let mut last_mismatch_generation = version_mismatch_generation();
        let mut isolated_mismatch_since: Option<Instant> = None;
        let mut stagger_deadline: Option<Instant> = None;
        let mut stagger_cooldown_until: Option<Instant> = None;
        // First periodic re-poll is one (jittered) interval out; the boot-time
        // startup_update_check above already covered "right now".
        let mut next_repoll = Instant::now() + repoll_delay();
        let our_version = parse_our_version();

        loop {
            interval.tick().await;

            // --- Priority 1: Urgent update (breaking change) ---
            if is_urgent_update() {
                tracing::warn!("Urgent update needed (remote's min_compatible > our version)");
                match check_if_update_available(build_info::VERSION).await {
                    UpdateCheckResult::UpdateAvailable(new_version) => {
                        clear_version_mismatch();
                        clear_urgent_update();
                        tracing::info!(
                            new_version = %new_version,
                            "Urgent update confirmed on GitHub, triggering immediate auto-update"
                        );
                        #[allow(clippy::let_underscore_must_use)]
                        let _ = update_tx.send(new_version);
                        return;
                    }
                    UpdateCheckResult::Skipped => {
                        // GitHub not reachable or no update yet; will retry next tick
                    }
                    UpdateCheckResult::RateLimited => {
                        // #4073: our own GitHub-poll rate limiter denied the
                        // check. Do nothing and retry when the bucket refills —
                        // do NOT exit 42 (the supervisor's `freenet update`
                        // shares the empty bucket and would no-op).
                    }
                    UpdateCheckResult::PinnedKnownBad => {
                        // #4073: the only newer release is pinned known-bad on
                        // this node after a crash-loop rollback. Stop trying to
                        // exit for it (the updater would only refuse it). Clear
                        // the driving signals; they re-arm on the next peer
                        // handshake, which will pick up a later, strictly-newer fix.
                        clear_urgent_update();
                        clear_version_mismatch();
                        tracing::warn!(
                            "Urgent update target is pinned known-bad after a crash-loop \
                             rollback; staying put (#4073)"
                        );
                    }
                }
            }

            // --- Priority 2: Decentralized version discovery (staggered) ---
            if let (Some(our_ver), Some(highest)) = (our_version, get_highest_seen_version()) {
                if highest > our_ver {
                    // Don't re-arm stagger if we're in cooldown after a failed check.
                    let in_cooldown =
                        stagger_cooldown_until.is_some_and(|until| Instant::now() < until);

                    if stagger_deadline.is_none() && !in_cooldown {
                        let stagger_secs =
                            freenet::config::GlobalRng::random_u64() % MAX_STAGGER_SECS;
                        let deadline =
                            Instant::now() + std::time::Duration::from_secs(stagger_secs);
                        tracing::info!(
                            highest_seen = %format!("{}.{}.{}", highest.0, highest.1, highest.2),
                            our_version = build_info::VERSION,
                            stagger_secs,
                            "Newer version discovered via peer handshake, stagger timer started"
                        );
                        stagger_deadline = Some(deadline);
                    }

                    if let Some(deadline) = stagger_deadline {
                        if Instant::now() >= deadline {
                            tracing::info!("Stagger timer expired, checking GitHub for updates");
                            match check_if_update_available(build_info::VERSION).await {
                                UpdateCheckResult::UpdateAvailable(new_version) => {
                                    clear_version_mismatch();
                                    clear_urgent_update();
                                    tracing::info!(
                                        new_version = %new_version,
                                        "Update confirmed on GitHub after stagger, triggering auto-update"
                                    );
                                    #[allow(clippy::let_underscore_must_use)]
                                    let _ = update_tx.send(new_version);
                                    return;
                                }
                                UpdateCheckResult::Skipped => {
                                    // GitHub unreachable or no update yet. Enter cooldown
                                    // to avoid re-arming every tick (highest > our_ver is
                                    // permanently true once set).
                                    stagger_deadline = None;
                                    stagger_cooldown_until =
                                        Some(Instant::now() + STAGGER_COOLDOWN);
                                }
                                UpdateCheckResult::RateLimited => {
                                    // #4073: rate-limited by our own bucket — keep the
                                    // stagger deadline armed (do NOT enter the long
                                    // cooldown) so we re-check promptly once the bucket
                                    // refills, without hitting GitHub meanwhile.
                                }
                                UpdateCheckResult::PinnedKnownBad => {
                                    // #4073: discovered version is pinned known-bad
                                    // after a crash-loop rollback. Drop the stagger
                                    // and cool down (don't re-arm every tick); a
                                    // later strictly-newer fix will re-trigger.
                                    stagger_deadline = None;
                                    stagger_cooldown_until =
                                        Some(Instant::now() + STAGGER_COOLDOWN);
                                    tracing::warn!(
                                        "Staggered update target is pinned known-bad after a \
                                         crash-loop rollback; cooling down (#4073)"
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // --- Priority 3: Legacy mismatch-based update (fallback) ---
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
                    UpdateCheckResult::RateLimited => {
                        // #4073: our own GitHub-poll rate limiter denied the
                        // check. Crucially do NOT fall through to the max-backoff
                        // "trust the gateway, exit 42" fallback above (this is a
                        // separate match arm, so a RateLimited result never
                        // reaches it): the supervisor's `freenet update` shares
                        // the same empty bucket and would just exit "already up to
                        // date", so exiting here would be a pointless restart loop.
                        // Keep the mismatch flag and retry once the bucket refills.
                    }
                    UpdateCheckResult::PinnedKnownBad => {
                        // #4073: the gateway-advertised newer release is pinned
                        // known-bad after a crash-loop rollback. Clear the
                        // mismatch so the legacy "max-backoff + 0 connections ->
                        // exit 42" / hard-timeout fallbacks never fire for a
                        // version the updater will only refuse — that would be a
                        // slow exit-42 restart loop. The flag re-arms on the next
                        // mismatch handshake, catching a later strictly-newer fix.
                        clear_version_mismatch();
                        isolated_mismatch_since = None;
                        tracing::warn!(
                            "Version-mismatch update target is pinned known-bad after a \
                             crash-loop rollback; staying put (#4073)"
                        );
                    }
                }
            } else {
                isolated_mismatch_since = None;
            }

            // --- Priority 4: Periodic direct GitHub re-poll (#4073) ---
            //
            // Independent of any peer signal: re-run the SAME one-shot startup
            // GitHub check on a recurring, jittered schedule so a node that has
            // been up for a long time still notices a new release without a
            // restart. Before this, once the boot-time startup check ran, all
            // further detection depended on a peer signal (priorities 1-3); if an
            // entire network sat on the same old version, no node re-checked GitHub
            // and a freshly published release was never picked up.
            //
            // Detection-only: a discovered update feeds the SAME `update_tx` →
            // graceful-shutdown → exit-42 → `freenet update` path as every trigger
            // above — identical to the startup check — so all existing
            // apply/verify/signing safety (checksum fail-closed #4586, signature
            // verify #4587, crash-loop bounding #4551/#4588) applies unchanged.
            //
            // Fail-open + self-throttling: `startup_update_check` returns None on
            // any GitHub/parse/network error, so a failed poll (rate-limit,
            // outage) simply retries at the next interval (~6h) — it never hammers
            // the API or crashes the node. At 6h ± 25% the minimum interval is
            // ~4.5h, far under GitHub's unauthenticated 60 req/hr/IP limit. The
            // 60s tick advances `next_repoll`, so only one re-poll runs per
            // interval and (being sequential in this single task) it never
            // overlaps an in-flight check.
            if Instant::now() >= next_repoll {
                next_repoll = Instant::now() + repoll_delay();
                // Respect the persistent auto-update failure lockout (#3934).
                // When installs keep failing (e.g. a non-writable binary path)
                // the failure counter reaches MAX_UPDATE_FAILURES and the
                // peer-signal triggers (priorities 1-3) stop exiting for
                // auto-update via `check_if_update_available`. The periodic
                // re-poll MUST honor the same lockout — otherwise a locked-out
                // long-running node would exit-42 every interval and make the
                // supervisor rerun the same failing update, reintroducing the
                // exact loop the lockout exists to stop. (The boot-time startup
                // check bypasses the lockout, but only once per restart; a
                // *recurring* bypass is the regression.) A successful manual
                // `freenet update` clears the counter and re-enables this path.
                if should_attempt_update() {
                    tracing::debug!(
                        current = build_info::VERSION,
                        "Periodic re-poll: checking GitHub directly for a newer release"
                    );
                    if let Some(new_version) = startup_update_check(build_info::VERSION).await {
                        // #4073 (rebase onto #4591/#4593): mirror the boot-time
                        // startup check — never exit-42 to a version that is
                        // locally BLOCKED (crash-loop known-bad pin OR repeatedly
                        // failed install: checksum / signature / download /
                        // extract). The installer would refuse it anyway, so an
                        // exit-42 here is a pointless restart cycle; gating it is
                        // what keeps the failed-install loop stopped. (Rate-limit
                        // is already honored upstream: this path reaches GitHub
                        // through `get_latest_version`, which consumes a node
                        // token and returns None here when the bucket is empty.)
                        if commands::rollback::is_version_pinned_bad(&new_version)
                            || commands::rollback::is_version_install_gated(&new_version)
                        {
                            tracing::warn!(
                                new_version = %new_version,
                                "Periodic re-poll: newer version is locally blocked (crash-loop \
                                 known-bad pin or repeated install failures); not triggering \
                                 auto-update (#4073)"
                            );
                        } else {
                            tracing::info!(
                                new_version = %new_version,
                                "Periodic re-poll: newer version on GitHub, triggering auto-update"
                            );
                            #[allow(clippy::let_underscore_must_use)]
                            let _ = update_tx.send(new_version);
                            return;
                        }
                    }
                } else {
                    tracing::debug!(
                        "Periodic re-poll: skipped — auto-update locked out after repeated \
                         failed installs (#3934); run `freenet update` to recover"
                    );
                }
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
    commit_probation_task.abort();
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

/// Exit code when another freenet instance is already running.
/// Listed in RestartPreventExitStatus and SuccessExitStatus in the systemd
/// service file so systemd does not restart and does not count it as a failure.
const EXIT_CODE_ALREADY_RUNNING: i32 = 43;

/// Error returned when another freenet process already occupies the WS API port.
#[derive(Debug, thiserror::Error)]
#[error("another freenet instance is already running")]
struct AlreadyRunningError;

fn check_for_existing_process(config: &Config) -> anyhow::Result<()> {
    use std::net::{SocketAddr, TcpStream};
    use std::time::Duration;

    let addr = SocketAddr::from((config.ws_api.address, config.ws_api.port));
    if TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok() {
        let pid = find_process_on_port(config.ws_api.port);
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
        return Err(AlreadyRunningError.into());
    }
    Ok(())
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
        // Surface the early-`main` umask override now that tracing is
        // installed — operators with a custom `UMask=` see the change.
        log_umask_override();
        run(config).await
    })?;

    Ok(())
}

fn freenet_main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::Service(cmd)) => {
            // Build only ConfigPaths (directory layout), not the full Config
            // which triggers a remote gateway fetch that fails on fresh
            // installs or before the network is ready (see #3717).
            let config_paths =
                std::sync::Arc::new(cli.config.config_paths.build(cli.config.id.as_deref())?);

            cmd.run(
                build_info::VERSION,
                build_info::GIT_COMMIT,
                build_info::GIT_DIRTY,
                build_info::BUILD_TIMESTAMP,
                config_paths,
            )
        }
        Some(Command::Update(cmd)) => cmd.run(build_info::VERSION),
        Some(Command::Uninstall(cmd)) => cmd.run(),
        Some(Command::Secrets(cfg)) => {
            // CLI utility; uses simple current-thread runtime (no
            // multi-thread / blocking-pool tuning needed for IO-light
            // KEK marker reads + status print).
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(commands::secrets_cmd::run(cfg))
        }
        Some(Command::Network { mut config }) => {
            config.mode = Some(OperationMode::Network);
            run_node(config)
        }
        Some(Command::Local { mut config }) => {
            config.mode = Some(OperationMode::Local);
            run_node(config)
        }
        None => {
            // Handle --version before the setup wizard so it works in CI
            // and headless environments where a GUI dialog would hang.
            if cli.config.version {
                return run_node(cli.config);
            }
            // On Windows, if not installed, show setup wizard before starting.
            // If already installed, start the background service (with tray icon)
            // instead of running in console mode — this is what users expect when
            // double-clicking freenet.exe or running it from the command line.
            if commands::setup_wizard::maybe_show_setup_wizard()? {
                return Ok(());
            }
            // Default behavior: run with the config from top-level args
            run_node(cli.config)
        }
    }
}

/// Stores the operator's umask at the moment `set_secure_umask` ran,
/// so a `tracing::info!` emitted later (after the logger is installed
/// in `run_node`) can surface the override in operator logs. Only used
/// for observability; the load-bearing behaviour is the `libc::umask`
/// call itself.
#[cfg(unix)]
static PRIOR_UMASK: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// Defense-in-depth umask tightening for the secrets subsystem.
///
/// PR #4195 / issue #4141 added explicit `OpenOptions::mode(0o600)` at
/// every known secret-writing call site. Setting the process umask to
/// `0o077` here is the belt-and-suspenders companion: any future
/// `File::create` added in the secrets subsystem that forgets to go
/// through `create_owner_only` will still land at `0o600` (and any
/// `fs::create_dir` at `0o700`) rather than silently inheriting the
/// operator's umask (typically `0o022` → world-readable).
///
/// MUST run before any thread is spawned. `umask(2)` is per-thread on
/// macOS — child threads inherit their creator's umask at thread-create
/// time, so a worker thread spawned before this call would keep the
/// (default, permissive) inherited umask. On Linux umask is per-process
/// so the constraint is weaker, but the same call site is correct for
/// both platforms.
///
/// See: issue #4196.
#[cfg(unix)]
fn set_secure_umask() {
    // SAFETY: `umask(2)` is a thread-safe POSIX syscall that simply
    // installs a new mask and returns the prior value.
    let prior = unsafe { libc::umask(0o077) };
    // `set` is a no-op if a prior call already populated the cell
    // (e.g. when a test exercises `set_secure_umask` directly after
    // `main` already ran in the same process). The first writer wins;
    // that's fine — the value is purely informational.
    #[allow(clippy::let_underscore_must_use)]
    let _ = PRIOR_UMASK.set(prior as u32);
}

#[cfg(not(unix))]
fn set_secure_umask() {}

/// Log the umask override at INFO once the tracing subscriber is up.
/// Operators who configured a non-default umask via systemd `UMask=`,
/// a wrapper script, or a login shell see the override in normal logs
/// instead of having to guess from on-disk file modes.
#[cfg(unix)]
fn log_umask_override() {
    if let Some(prior) = PRIOR_UMASK.get() {
        tracing::info!(
            prior_umask = format_args!("{:#05o}", prior),
            new_umask = format_args!("{:#05o}", 0o077),
            "tightened process umask for secrets defense-in-depth (issue #4196)"
        );
    }
}

#[cfg(not(unix))]
fn log_umask_override() {}

fn main() {
    // Defense-in-depth: tighten umask BEFORE the tokio runtime spawns
    // any worker threads. See `set_secure_umask` and issue #4196.
    set_secure_umask();

    use commands::auto_update::{
        EXIT_CODE_UPDATE_NEEDED, SupervisorStatus, UpdateNeededError, supervisor_status,
    };

    match freenet_main() {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            // Check if this is an "update needed" error from auto-update detection
            if let Some(update) = e.downcast_ref::<UpdateNeededError>() {
                // Auto-update is supervised-install-only: the node never replaces
                // its own binary. It exits 42 and relies on a supervisor (systemd
                // ExecStopPost, the macOS launchd wrapper, or the Windows/Linux
                // in-process run-wrapper loop) to run `freenet update` and restart
                // it. A bare `freenet network` run has no supervisor, so without a
                // loud warning the update is detected and then silently never
                // applied (issue #4580). Scale the message by whether we have any
                // evidence of a supervisor.
                match supervisor_status() {
                    SupervisorStatus::Supervised => {
                        tracing::info!(
                            new_version = %update.new_version,
                            "Update {} detected; exiting with code {EXIT_CODE_UPDATE_NEEDED} for \
                             the service supervisor to apply it and restart.",
                            update.new_version,
                        );
                        eprintln!("Update needed, exiting for service wrapper to handle update...");
                    }
                    SupervisorStatus::SupervisedUnverified => {
                        // Under systemd but without our authoritative marker: the
                        // unit may or may not have the exit-42 → `freenet update`
                        // ExecStopPost hook (e.g. a custom or pre-marker unit).
                        // Warn, but more softly than the bare-CLI case, and tell
                        // the operator how to confirm.
                        tracing::warn!(
                            new_version = %update.new_version,
                            "Update {} detected; exiting with code {EXIT_CODE_UPDATE_NEEDED}. This \
                             process is under systemd but its unit was not marked with \
                             FREENET_SUPERVISED, so we cannot confirm it has the exit-42 → \
                             `freenet update` hook. If updates do not apply automatically, \
                             reinstall the service with `freenet service install` (or add an \
                             ExecStopPost that runs `freenet update` on exit 42).",
                            update.new_version,
                        );
                        eprintln!(
                            "Update {} detected, exiting with code {EXIT_CODE_UPDATE_NEEDED} for a \
                             service supervisor to apply it. If updates do not apply, reinstall \
                             with `freenet service install`.",
                            update.new_version,
                        );
                    }
                    SupervisorStatus::Unsupervised => {
                        tracing::error!(
                            new_version = %update.new_version,
                            "Update {} was detected, but this process does NOT appear to be \
                             running under a Freenet service supervisor. Auto-update only works \
                             when Freenet is installed as a service (`freenet service install`): \
                             the node exits with code {EXIT_CODE_UPDATE_NEEDED} expecting the \
                             supervisor to run `freenet update` and restart it. A bare \
                             `freenet network` run will now EXIT WITHOUT UPDATING and will not be \
                             restarted. Run `freenet update` and relaunch, or install Freenet as \
                             a service so future updates apply automatically.",
                            update.new_version,
                        );
                        eprintln!(
                            "WARNING: update {} detected but no service supervisor was found. \
                             Exiting with code {EXIT_CODE_UPDATE_NEEDED} WITHOUT applying the \
                             update. Run `freenet update` and relaunch, or install Freenet as a \
                             service (`freenet service install`) so updates apply automatically.",
                            update.new_version,
                        );
                    }
                }
                std::process::exit(EXIT_CODE_UPDATE_NEEDED);
            }
            // Another instance is already running — exit cleanly so systemd
            // does not enter a restart loop.
            if e.downcast_ref::<AlreadyRunningError>().is_some() {
                eprintln!(
                    "Another freenet instance is already running. \
                     Exiting without error to avoid restart loop."
                );
                std::process::exit(EXIT_CODE_ALREADY_RUNNING);
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
    #[cfg(unix)]
    fn raise_fd_limit_does_not_lower_soft_limit() {
        // Normalize `rlim_t` (u64 on linux-gnu, varies elsewhere) to u64 for
        // arithmetic / comparison without tripping clippy on the redundant cast.
        #[allow(clippy::unnecessary_cast)]
        fn as_u64(v: libc::rlim_t) -> u64 {
            v as u64
        }

        let mut before = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit with a valid resource id and an initialized,
        // exclusively-borrowed out-param is sound; we check the return code.
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut before) };
        assert_eq!(rc, 0, "getrlimit should succeed");

        let orig_soft = as_u64(before.rlim_cur);
        let hard = as_u64(before.rlim_max);

        // To exercise the ACTUAL raise branch (not the soft==hard no-op that is
        // the common CI case), first LOWER our soft limit to a finite value
        // strictly below hard, then call `raise_fd_limit()` and assert it raised
        // back up to hard. Lowering one's OWN soft limit never requires
        // privilege, but if the sandbox forbids even that we fall back to the
        // weaker "no-decrease" assertion so the test is never flaky.
        let lowered_to: Option<u64> = if hard != as_u64(libc::RLIM_INFINITY) && hard > 1 {
            // Pick a finite target strictly below hard: min(orig_soft, hard/2),
            // floored at 1 so we never request 0 fds.
            let target = orig_soft.min(hard / 2).max(1);
            // Guard: only meaningful if we can land strictly below hard.
            if target < hard {
                let lowered = libc::rlimit {
                    rlim_cur: target as libc::rlim_t,
                    rlim_max: before.rlim_max,
                };
                // SAFETY: setrlimit with a valid resource id and a fully
                // initialized rlimit whose soft (target < hard) does not exceed
                // the unchanged hard limit is sound.
                let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) };
                if rc == 0 { Some(target) } else { None }
            } else {
                None
            }
        } else {
            None
        };

        // Must not panic and must be safe to call.
        super::raise_fd_limit();

        let mut after = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: same invariants as the `before` read above.
        let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut after) };
        assert_eq!(rc, 0, "getrlimit should succeed after raising");
        let after_soft = as_u64(after.rlim_cur);
        let after_hard = as_u64(after.rlim_max);

        match lowered_to {
            Some(target) => {
                // We genuinely lowered the soft limit, so `raise_fd_limit` had a
                // real raise to perform. It must have raised the soft limit back
                // up to the hard limit (and at minimum strictly above where we
                // lowered it to).
                assert_eq!(
                    after_soft, after_hard,
                    "raise_fd_limit must raise the soft limit up to the hard limit \
                     (lowered_to={target}, hard={after_hard}, after_soft={after_soft})"
                );
                assert!(
                    after_soft > target,
                    "raise_fd_limit must strictly increase the lowered soft limit: \
                     lowered_to={target}, after={after_soft}"
                );
            }
            None => {
                // Could not lower (already minimal, infinite hard limit, or
                // sandbox forbade it): fall back to the no-decrease invariant.
                assert!(
                    after_soft >= orig_soft,
                    "soft fd limit must not decrease: before={orig_soft}, after={after_soft}"
                );
            }
        }

        // The soft limit must never exceed the hard limit (an invariant the OS
        // enforces, but we assert it to catch a logic error in the helper).
        assert!(
            after_soft <= after_hard,
            "soft fd limit must not exceed hard limit: soft={after_soft}, hard={after_hard}"
        );

        // Restore the original soft limit so we don't leak a changed limit into
        // other tests sharing this process. Best-effort; ignore failures.
        let restore = libc::rlimit {
            rlim_cur: before.rlim_cur,
            rlim_max: before.rlim_max,
        };
        // SAFETY: restoring the exact limits we read at entry is sound.
        let _ = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &restore) };
    }

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

    /// Regression test for issue #4196: a plain `File::create` from
    /// inside a `tokio::spawn`'d task on a tokio worker thread must
    /// land at mode `0o600` after `set_secure_umask` runs, even though
    /// the call site does NOT go through the `create_owner_only`
    /// helper. This is the defense-in-depth guarantee the umask
    /// provides for any future secret-writing call that forgets the
    /// explicit `OpenOptions::mode(0o600)`.
    #[cfg(unix)]
    #[test]
    fn umask_persists_into_tokio_worker_thread() {
        use std::os::unix::fs::PermissionsExt;

        // Force a permissive baseline umask so we are actually testing
        // that `set_secure_umask` tightens it (not that the test
        // harness already happened to start at 0o077). Capture the
        // prior value so we can restore it before returning — leaking
        // a tight umask into the rest of the test binary would mask
        // bugs in other tests' file-permission assertions.
        // SAFETY: `umask(2)` is a thread-safe POSIX syscall.
        let prior = unsafe { libc::umask(0o022) };

        // Sanity-check the baseline by creating a file BEFORE
        // tightening: under `umask(0o022)` it should land at `0o644`.
        let temp = tempfile::tempdir().expect("tempdir");
        let baseline_path = temp.path().join("baseline-default-umask");
        std::fs::File::create(&baseline_path).expect("create baseline");
        let baseline_mode = std::fs::metadata(&baseline_path)
            .expect("stat baseline")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            baseline_mode, 0o644,
            "baseline file under umask(0o022) should be 0o644, got {baseline_mode:o}; \
             test setup is invalid"
        );

        // Function under test: tighten the umask, then build a fresh
        // multi-thread runtime so that the worker threads inherit the
        // new umask at pthread-create time (macOS-correct).
        super::set_secure_umask();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build runtime");

        let worker_file_path = temp.path().join("created-from-tokio-worker");
        let worker_dir_path = temp.path().join("created-from-tokio-worker-dir");
        let worker_file_for_task = worker_file_path.clone();
        let worker_dir_for_task = worker_dir_path.clone();
        rt.block_on(async move {
            tokio::spawn(async move {
                // Deliberately use plain `File::create` / `create_dir`
                // — NOT `create_owner_only` / `ensure_owner_only_dir`
                // — to exercise the umask path on both file and
                // directory creates. `0o600` for files and `0o700`
                // for dirs is the contract the docs commit to in
                // `docs/secrets-at-rest.md`.
                std::fs::File::create(&worker_file_for_task).expect("create file from worker");
                std::fs::create_dir(&worker_dir_for_task).expect("create dir from worker");
            })
            .await
            .expect("worker task");
        });

        let worker_file_mode = std::fs::metadata(&worker_file_path)
            .expect("stat worker file")
            .permissions()
            .mode()
            & 0o777;
        let worker_dir_mode = std::fs::metadata(&worker_dir_path)
            .expect("stat worker dir")
            .permissions()
            .mode()
            & 0o777;

        // Restore the prior umask on the test-runner thread BEFORE
        // asserting so a failure does not leak a tightened mask into
        // subsequent tests. We deliberately do NOT restore on the
        // tokio worker threads: macOS umask is per-thread, but the
        // runtime is dropped at the end of this block, so the worker
        // threads exit before any other test can observe their state.
        // SAFETY: `umask(2)` is a thread-safe POSIX syscall.
        unsafe {
            libc::umask(prior);
        }

        assert_eq!(
            worker_file_mode, 0o600,
            "file created from tokio worker after set_secure_umask() should be 0o600, \
             got {worker_file_mode:o}"
        );
        assert_eq!(
            worker_dir_mode, 0o700,
            "directory created from tokio worker after set_secure_umask() should be 0o700, \
             got {worker_dir_mode:o}"
        );
    }

    /// Strip Rust `//` line comments from source text so a source-scrape pin
    /// matches only on actual code, not on a comment that happens to mention the
    /// searched token (addresses the "passes with commented-out code" brittleness
    /// the Codex review flagged). Crude but sufficient: we only feed it our own
    /// source and never have `//` inside a string literal on the matched lines.
    fn strip_line_comments(src: &str) -> String {
        src.lines()
            .map(|line| line.split_once("//").map(|(code, _)| code).unwrap_or(line))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Source-scrape pin for issue #4580: the exit-42 handler in `main()` MUST
    /// branch on supervisor status and escalate to a loud `error!` on the
    /// unsupervised path. A bare `freenet network` run that detects an update,
    /// exits 42, and dies without applying it is exactly the silent failure this
    /// PR makes diagnosable; a regression that drops the branch (back to a bare
    /// "exiting for service wrapper to handle update") would re-hide it.
    #[test]
    fn exit_42_path_warns_loudly_when_unsupervised() {
        let src = strip_line_comments(include_str!("freenet.rs"));
        let (_, after_branch) = src
            .split_once("if let Some(update) = e.downcast_ref::<UpdateNeededError>() {")
            .expect("exit-42 branch not found in main()");
        let (branch_body, _) = after_branch
            .split_once("std::process::exit(EXIT_CODE_UPDATE_NEEDED);")
            .expect("could not locate end of exit-42 branch");
        assert!(
            branch_body.contains("SupervisorStatus::Unsupervised"),
            "exit-42 handler must distinguish the unsupervised case (#4580)"
        );
        assert!(
            branch_body.contains("tracing::error!"),
            "exit-42 handler must log at error! level on the unsupervised path \
             so the silent 'detected update, exited, never applied' failure is \
             diagnosable (#4580)"
        );
    }

    /// Source-scrape pin for #4580: the dirty-build branch that disables
    /// auto-update entirely must be operator-visible (`warn!`), not a quiet
    /// `info!`. A dirty build never checks for updates, so the only signal an
    /// operator gets is this line.
    #[test]
    fn dirty_build_disables_update_loudly() {
        let src = strip_line_comments(include_str!("freenet.rs"));
        let (_, after_guard) = src
            .split_once("if auto_update_disabled {")
            .expect("dirty-build guard not found");
        let (guard_body, _) = after_guard
            .split_once("std::future::pending::<()>().await;")
            .expect("could not locate end of dirty-build guard");
        assert!(
            guard_body.contains("tracing::warn!"),
            "the dirty-build auto-update-disabled branch must log at warn! so it \
             is operator-visible (#4580)"
        );
    }

    /// #4073: a version pinned known-bad after a crash-loop rollback must never
    /// cause the node to exit for auto-update — the updater would only refuse it,
    /// producing a slow exit-42 restart loop (the Codex finding on the reworked
    /// PR). Every `UpdateCheckResult::PinnedKnownBad` arm in the update loop must
    /// therefore handle the case WITHOUT calling `update_tx.send` (the only thing
    /// that drives the exit-42 update path).
    #[test]
    fn pinned_known_bad_never_triggers_exit_42() {
        let src = strip_line_comments(include_str!("freenet.rs"));
        // Build the needle from fragments so this test's OWN source text does not
        // self-match the contiguous arm marker (include_str! pulls in this file).
        let needle = concat!("UpdateCheckResult::", "PinnedKnownBad =>");
        let arms: Vec<&str> = src.split(needle).skip(1).collect();
        assert_eq!(
            arms.len(),
            3,
            "expected a PinnedKnownBad arm in each of the 3 update-trigger \
             priorities (urgent / stagger / legacy mismatch) (#4073)"
        );
        for (i, arm) in arms.iter().enumerate() {
            // Bound each arm to before the next match's first arm (capped at 400
            // chars) so a later UpdateAvailable send can't leak into the window.
            let end = arm
                .find("UpdateCheckResult::")
                .unwrap_or(arm.len())
                .min(400);
            let body = &arm[..end];
            assert!(
                !body.contains("update_tx.send"),
                "PinnedKnownBad arm #{i} must NOT send an update — exiting 42 to a \
                 pinned version would loop (#4073)"
            );
        }
        // The legacy-mismatch arm must also clear the mismatch flag so the
        // max-backoff / hard-timeout exit-42 fallbacks never fire.
        assert!(
            src.contains("Version-mismatch update target is pinned known-bad"),
            "the legacy-mismatch PinnedKnownBad arm must clear the mismatch flag (#4073)"
        );
    }

    /// #4073: a `RateLimited` update-check result (our own GitHub-poll bucket was
    /// empty) must NEVER drive an exit-42 update. Routing it through the
    /// `Skipped` path would let the legacy "max-backoff + 0 connections -> exit
    /// 42" gateway-trust fallback fire on a poll we deliberately skipped, and
    /// since the supervisor-side `freenet update` shares the same empty bucket it
    /// would just no-op — a pointless restart loop. So every `RateLimited` arm
    /// must be its own arm that does NOT call `update_tx.send`.
    #[test]
    fn rate_limited_never_triggers_exit_42() {
        let src = strip_line_comments(include_str!("freenet.rs"));
        let needle = concat!("UpdateCheckResult::", "RateLimited =>");
        let arms: Vec<&str> = src.split(needle).skip(1).collect();
        assert_eq!(
            arms.len(),
            3,
            "expected a RateLimited arm in each of the 3 update-trigger priorities \
             (urgent / stagger / legacy mismatch) (#4073)"
        );
        for (i, arm) in arms.iter().enumerate() {
            let end = arm
                .find("UpdateCheckResult::")
                .unwrap_or(arm.len())
                .min(400);
            let body = &arm[..end];
            assert!(
                !body.contains("update_tx.send"),
                "RateLimited arm #{i} must NOT send an update — exiting 42 while \
                 rate-limited would loop (#4073)"
            );
        }
    }

    /// Source-scrape pin for #4580: each Freenet supervisor must set the
    /// `FREENET_SUPERVISED` marker on the `freenet network` child it spawns, so
    /// the node can tell it is supervised and log calmly instead of erroring on
    /// the exit-42 path. These run on every platform (we scrape source text, not
    /// the cfg-gated generators) so a regression on any one platform's path is
    /// caught by CI regardless of the runner OS.
    #[test]
    fn supervisors_set_supervised_marker() {
        // The in-process wrapper loop (Windows + Linux service run-wrapper).
        let wrapper_src = include_str!("commands/service/wrapper.rs");
        assert!(
            wrapper_src.contains("SUPERVISED_ENV_VAR"),
            "the in-process wrapper must set the FREENET_SUPERVISED marker on the \
             network child it spawns (#4580)"
        );

        // The macOS launchd wrapper script.
        let macos_src = include_str!("commands/service/macos.rs");
        assert!(
            macos_src.contains("supervised_env = super::super::auto_update::SUPERVISED_ENV_VAR")
                && macos_src.contains("export {supervised_env}=1"),
            "the macOS wrapper script must export the FREENET_SUPERVISED marker \
             so the launchd-supervised node detects its supervisor (#4580)"
        );

        // Both systemd unit templates (user + system).
        let linux_src = include_str!("commands/service/linux.rs");
        let marker_count = linux_src
            .matches("Environment=FREENET_SUPERVISED=1")
            .count();
        assert!(
            marker_count >= 2,
            "both systemd unit templates must set Environment=FREENET_SUPERVISED=1 \
             (#4580); found {marker_count}"
        );
    }

    /// Source-scrape pin (#4073 / Codex P2): the periodic re-poll MUST gate on
    /// `should_attempt_update()` so it honors the persistent auto-update failure
    /// lockout (#3934). Without the gate, a locked-out long-running node (e.g. a
    /// non-writable binary path that makes every install fail) would exit-42 once
    /// per re-poll interval and make the supervisor rerun the same failing
    /// update, reintroducing the loop the lockout exists to stop. The boot-time
    /// startup check bypasses the lockout, but only once per restart; the
    /// recurring re-poll must not.
    #[test]
    fn periodic_repoll_respects_update_lockout() {
        let src = strip_line_comments(include_str!("freenet.rs"));
        // `should_attempt_update()` (with parens) appears only at the re-poll
        // gate; the bare name without parens is the `use` import. The re-poll's
        // GitHub call is the LAST `startup_update_check(...)` in the file (the
        // first is the boot-time startup check).
        let gate = src.find("should_attempt_update()").expect(
            "periodic re-poll must gate on should_attempt_update() to honor the \
             #3934 auto-update failure lockout",
        );
        let repoll_check = src
            .rfind("startup_update_check(build_info::VERSION).await")
            .expect("periodic re-poll startup_update_check call not found");
        assert!(
            gate < repoll_check,
            "the should_attempt_update() lockout gate must precede the periodic \
             re-poll's startup_update_check call, so a locked-out node does not \
             exit-42 in a loop (#4073 / #3934)"
        );
    }
}
