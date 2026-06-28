use std::{convert::Infallible, sync::Arc, time::Duration};

use futures::{FutureExt, future::BoxFuture};
use tokio::task::JoinHandle;
use tracing::Instrument;

use super::{
    NetEventRegister, PeerId,
    network_bridge::{
        EventLoopExitReason, EventLoopNotificationsReceiver,
        event_loop_notification_channel_with_capacity, p2p_protoc::P2pConnManager,
    },
};
use crate::{
    client_events::client_event_handling,
    ring::{ConnectionManager, Location},
};
use crate::{
    client_events::{BoxedClient, combinator::ClientEventsCombinator},
    config::GlobalExecutor,
    contract::{self, ContractHandler, ContractHandlerChannel, WaitingResolution},
    message::NodeEvent,
    node::NodeConfig,
    operations::connect,
};

use super::{OpManager, background_task_monitor::BackgroundTaskMonitor};

/// Exit code requested when a node that had been up for a *healthy* amount of
/// time (>= [`MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT`]) dies on a fatal
/// (non-graceful) listener condition (#4549). `42` == `EXIT_CODE_UPDATE_NEEDED`
/// (see `crates/core/src/bin/commands/auto_update.rs`) and is matched by the
/// GENERATED systemd unit template (`crates/core/src/bin/commands/service/linux.rs`,
/// `[ "$EXIT_STATUS" = "42" ]`). Its `ExecStopPost` hook runs `freenet update`, so a
/// wedged OLD binary that ran fine for a while auto-upgrades to a fixed version (the
/// self-heal path that rolls a fix out), and `Restart=always` (`RestartSec=10`)
/// restarts it.
///
/// The unit also lists `42` in `SuccessExitStatus`, so it is treated as a clean exit
/// and does NOT count toward the `[Unit]`-section `StartLimitBurst`. That is correct
/// for a long-uptime node (a real fault / update), but it would let a *boot-wedge*
/// loop restart forever — so a fatal exit that happens too soon after start uses
/// [`FAST_CRASH_EXIT_CODE`] instead (see [`fatal_listener_exit_code`]).
///
/// Caveat (macOS): the macOS service wrapper treats a non-zero `freenet update`
/// result (the no-op "already up to date" exit) as a failure and applies restart
/// backoff. Production gateways are Linux, so this only slows restart on macOS.
const FATAL_LISTENER_EXIT_CODE: i32 = 42;

/// Exit code for a fatal listener exit that happened too soon after start
/// (uptime < [`MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT`]) to be a healthy node hitting
/// an update / transient fault — i.e. a fast crash or boot-wedge (#4551).
///
/// It is deliberately DISTINCT from [`FATAL_LISTENER_EXIT_CODE`] (42) precisely so
/// the systemd unit's `SuccessExitStatus=42 43` does NOT whitelist it: systemd then
/// counts each fast-crash restart toward `StartLimitBurst`, so a tight loop trips
/// the (now correctly `[Unit]`-placed) limiter and the unit is STOPPED instead of
/// looping every ~10s forever. The boot-crash self-heal is NOT lost: the systemd
/// unit's `ExecStopPost` runs `freenet update` on exit 42 OR 45, so a boot-crash
/// that a newer release fixes still upgrades (the `freenet update` child is a
/// separate process that can succeed even when `freenet network` crashes at
/// startup). Counting and self-heal are independent.
///
/// Emitted ONLY when the binary opted in via [`enable_fast_crash_exit_code`] — which
/// happens solely under a regenerated Freenet systemd unit that advertises 45 support
/// (see [`EMIT_FAST_CRASH_EXIT_CODE`]). Under the macOS/Windows in-process run-wrapper
/// (understands only exit 42; self-heals + bounds via its own backoff/50-cap), an old
/// or custom systemd unit, or unsupervised, the node keeps emitting 42 so its existing
/// self-heal is preserved.
///
/// NOT `44`: that is `EXIT_CODE_BUNDLE_UPDATE_STAGED`, an internal `freenet update`
/// code never emitted by `freenet network`.
const FAST_CRASH_EXIT_CODE: i32 = 45;

/// Minimum uptime for a fatal listener exit to be treated as a "ran healthily,
/// then died" event eligible for the burst-exempt auto-update self-heal path
/// (exit [`FATAL_LISTENER_EXIT_CODE`] = 42). A fatal exit BELOW this threshold is
/// classified as a fast crash / boot-wedge and uses the counted
/// [`FAST_CRASH_EXIT_CODE`] instead so a tight loop is rate-limited.
///
/// 60s is comfortably longer than normal node startup (bind sockets, load
/// contracts, connect to gateways) yet far below any "ran for a while" duration,
/// so genuine boot-wedges fall below it while healthy nodes that later die fall
/// above it. The exact value is not safety-critical: the `StartLimitBurst=5` /
/// `StartLimitIntervalSec=120` cap only stops a *rapid* loop, and a legitimate
/// single restart (update, reboot, one-off crash) never approaches 5-in-120s
/// regardless of which code it used.
const MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT: Duration = Duration::from_secs(60);

/// Choose the process exit code for a fatal (non-graceful) network-listener exit,
/// based on how long the node had been up and whether the supervisor understands the
/// distinct fast-crash code (#4551).
///
/// - `fast_crash_enabled` && uptime < [`MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT`] →
///   [`FAST_CRASH_EXIT_CODE`] (45): a counted failure, so a tight boot-wedge loop
///   trips `StartLimitBurst` and the unit stops. The unit's `ExecStopPost` still
///   runs `freenet update` on 45, so a fixable boot-crash self-heals.
/// - otherwise → [`FATAL_LISTENER_EXIT_CODE`] (42): burst-exempt, fires the
///   `freenet update` self-heal hook.
///
/// `fast_crash_enabled` comes from [`EMIT_FAST_CRASH_EXIT_CODE`], which the binary
/// turns on ONLY under a regenerated Freenet systemd unit (the only supervisor that
/// handles 45). See that flag's docs for why every other supervisor must keep 42.
///
/// Extracted as a pure function so the decision can be unit-tested without the
/// surrounding `process::exit` (per `.claude/rules/deployment.md`).
fn fatal_listener_exit_code(uptime: Duration, fast_crash_enabled: bool) -> i32 {
    if fast_crash_enabled && uptime < MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT {
        FAST_CRASH_EXIT_CODE
    } else {
        FATAL_LISTENER_EXIT_CODE
    }
}

/// When `true`, a sub-[`MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT`] fatal listener exit
/// uses the distinct [`FAST_CRASH_EXIT_CODE`] (45) instead of the default
/// [`FATAL_LISTENER_EXIT_CODE`] (42). Off by default; the binary opts in (via
/// [`enable_fast_crash_exit_code`]) ONLY when the supervisor is known to understand
/// exit 45 — i.e. the freshly-generated Freenet systemd unit, detected by the
/// supervisor marker it sets.
///
/// Gating is essential: exit 45 only helps under the regenerated systemd unit, where
/// `SuccessExitStatus` excludes it (so it counts toward `StartLimitBurst`) and
/// `ExecStopPost` runs `freenet update` on 42 OR 45 (boot-crash self-heal). Under any
/// other supervisor, emitting 45 would be a generic failure that LOSES the exit-42
/// self-heal: the macOS/Windows in-process run-wrapper only knows 42 (it self-heals
/// and bounds the loop via its own backoff and 50-failure cap); an OLD or custom
/// systemd unit (e.g. a node auto-updated to this binary but whose unit file was not
/// regenerated) lacks the 45 handling; an unsupervised run is moot. So the default
/// (flag off) keeps emitting 42 everywhere.
static EMIT_FAST_CRASH_EXIT_CODE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Opt in to the distinct [`FAST_CRASH_EXIT_CODE`] for fast boot-crashes (#4551).
/// Call ONLY from the production node entry point, and ONLY after confirming the
/// supervising systemd unit advertises support for exit 45 (see the entry point's
/// marker check). See [`EMIT_FAST_CRASH_EXIT_CODE`].
pub fn enable_fast_crash_exit_code() {
    EMIT_FAST_CRASH_EXIT_CODE.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// When `true`, a fatal (non-graceful) exit of the network event listener aborts
/// the whole process with [`FATAL_LISTENER_EXIT_CODE`] instead of unwinding through
/// the normal teardown/return path (#4549).
///
/// In production that unwind path could hang for tens of minutes while the node sat
/// network-dead (the listener had already logged its exit), so systemd never
/// restarted it and the auto-update hook never fired — the node just spun. A prompt
/// `process::exit` converts that dark window into a quick restart.
///
/// **Off by default.** It is enabled only by the real `freenet` binary
/// ([`enable_abort_on_fatal_listener_exit`]) so that simulation / integration tests
/// and library embedders never have a fatal listener exit kill their host process.
static ABORT_ON_FATAL_LISTENER_EXIT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Enable the #4549 fast-exit behaviour: a fatal (non-graceful) network event
/// listener exit will abort the process with [`FATAL_LISTENER_EXIT_CODE`] for a
/// prompt service-manager restart, rather than risk hanging in teardown. Call once
/// from the production node entry point. See [`ABORT_ON_FATAL_LISTENER_EXIT`].
pub fn enable_abort_on_fatal_listener_exit() {
    ABORT_ON_FATAL_LISTENER_EXIT.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Whether a network-event-listener exit error is a *graceful* shutdown (a clean,
/// intended exit) rather than a fatal wedge that warrants a process restart.
///
/// Graceful shutdown (`NodeEvent::Disconnect`, e.g. SIGTERM or auto-update) returns
/// [`EventLoopExitReason::GracefulShutdown`]; every other listener-exit error
/// (UDP-listener death, unexpected stream end, a handler/transport error) is fatal.
fn listener_exit_is_graceful(err: &anyhow::Error) -> bool {
    err.downcast_ref::<EventLoopExitReason>()
        .is_some_and(|reason| matches!(reason, EventLoopExitReason::GracefulShutdown))
}

pub(crate) struct NodeP2P {
    pub(crate) op_manager: Arc<OpManager>,
    pub(super) conn_manager: P2pConnManager,
    pub(super) peer_id: Option<PeerId>,
    pub(super) is_gateway: bool,
    /// used for testing with deterministic location
    pub(super) location: Option<Location>,
    notification_channel: EventLoopNotificationsReceiver,
    client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
    node_controller: tokio::sync::mpsc::Receiver<NodeEvent>,
    should_try_connect: bool,
    client_events_task: BoxFuture<'static, anyhow::Error>,
    contract_executor_task: BoxFuture<'static, anyhow::Error>,
    /// Abort handles for the detached client-events and contract-executor tasks.
    /// `run_node` selects on the tasks' result futures (which only *observe*
    /// completion); aborting via these handles on the way out is what actually
    /// stops the tasks, dropping their `op_manager` clones and the contract
    /// executor's redb `Database` handle.
    ///
    /// Aborting the client-events task tears down the WebSocket server through a
    /// load-bearing channel-close chain (NOT a direct drop): abort drops the
    /// `ClientEventsCombinator` → drops the per-slot `Sender` it holds →
    /// `client_fn`'s `rx.recv()` returns `None` → `client_fn` breaks its loop →
    /// drops the `WebSocketProxy` → the proxy's `AbortOnDrop` fires, aborting the
    /// detached `axum::serve` server + token-cleanup tasks. The "client_fn breaks
    /// on rx close" step is the load-bearing link.
    ///
    /// Without these aborts, those tasks keep running — and holding the redb file
    /// lock and the bound ports — after a graceful shutdown. See issue #4401.
    detached_task_aborts: Vec<tokio::task::AbortHandle>,
    initial_join_task: Option<JoinHandle<()>>,
    session_actor_task: JoinHandle<()>,
    result_router_task: JoinHandle<()>,
    /// Monitor for background tasks spawned during node construction (Ring, OpManager, etc.)
    background_task_monitor: BackgroundTaskMonitor,
}

/// Fires the node's shutdown teardown on *every* `run_node` exit path — both the
/// normal post-event-loop return and any early `?`/return during setup (e.g. a
/// failed initial join). Without a Drop-based guard the teardown only ran on the
/// happy path, so an early return would leave the detached executor task running
/// and the redb file lock held for the process lifetime (issue #4401).
///
/// On drop it:
///   1. triggers the ring's background-task shutdown token so the long-lived
///      `Ring::new` loops (governance reaper, interest heartbeat, connection
///      maintenance, telemetry) stop promptly instead of waiting out their
///      longest sleep (issue #4278),
///   2. aborts the stored detached task handles (executor + client-events, plus
///      the session-actor / result-router / initial-join / aggressive-connect
///      tasks once they exist), and
///   3. clears the ring's redb `Storage` clones via `clear_redb_storage()`.
///
/// ## Exhaustive redb `Arc<Database>` holder set (verified for #4401)
///
/// The redb file lock releases only once *all* of these drop. Aborting the
/// executor task drops (1)+(2); `clear_redb_storage()` drops (3)+(4):
///   1. `RuntimePool::shared_state_store` — the one shared `StateStore<Storage>`.
///   2. Each `Executor<Runtime>` in the pool's `runtimes` Vec — every executor's
///      contract/state/delegate/secret stores clone the same `Storage`.
///      Both (1) and (2) live inside `NetworkContractHandler`, which is owned by
///      the detached **contract-executor task** (`contract_executor_abort`).
///   3. `ring.hosting_manager.storage`.
///   4. `ring.broken_invariants.storage`.
///
/// The session-actor, result-router, client-events, and background-monitor tasks
/// hold channels / in-memory maps only — verified to hold **no** `Storage` /
/// `Arc<Database>` clone. If a future change moves a `Storage`/`Arc<Database>`
/// clone into any other long-lived task, it MUST either be aborted here or its
/// handle dropped on shutdown, or the lock will never release.
struct ShutdownTeardown {
    aborts: Vec<tokio::task::AbortHandle>,
    ring: Arc<crate::ring::Ring>,
    fired: bool,
}

impl ShutdownTeardown {
    fn new(aborts: Vec<tokio::task::AbortHandle>, ring: Arc<crate::ring::Ring>) -> Self {
        Self {
            aborts,
            ring,
            fired: false,
        }
    }

    /// Track an abort handle for a task spawned after the guard was created
    /// (e.g. the initial-join and aggressive-connect tasks).
    fn track(&mut self, handle: tokio::task::AbortHandle) {
        self.aborts.push(handle);
    }
}

impl Drop for ShutdownTeardown {
    fn drop(&mut self) {
        if self.fired {
            return;
        }
        self.fired = true;
        // Signal the Ring's long-lived background tasks (governance reaper,
        // interest heartbeat, connection maintenance, telemetry loops, etc.)
        // to stop. They race this token against their sleeps via
        // `tokio::select!`, so a graceful shutdown returns promptly instead of
        // waiting up to ~5 minutes for the longest outstanding sleep to elapse.
        // Unlike the abort handles below, these tasks are only *observed* by
        // the BackgroundTaskMonitor (their JoinHandles are consumed there, not
        // aborted), so the cancellation token is the mechanism that actually
        // stops them. See issue #4278.
        self.ring.trigger_shutdown();
        for abort in &self.aborts {
            abort.abort();
        }
        // Drop the ring's clones of the redb `Storage` handle so the only
        // remaining references live in the (now-aborted) executor task. These
        // are the hosting-metadata and broken-invariants persistence handles
        // wired in `NetworkContractHandler::build`.
        //
        // NOTE: lock release is ASYNCHRONOUS. `AbortHandle::abort()` only
        // *signals* cancellation — the executor task's redb clones drop when the
        // runtime next polls that task to completion, which can lag by any
        // in-flight `block_in_place` WASM execution. So the OS file lock is not
        // guaranteed free the instant `run_node` returns; a caller reopening the
        // same data dir should tolerate a brief delay (the #4401 regression test
        // polls up to 10s).
        self.ring.clear_redb_storage();
    }
}

impl NodeP2P {
    /// Aggressively wait for connections during startup to avoid on-demand delays.
    /// This is an associated function that can be spawned as a task to run concurrently
    /// with the event listener. Without the event listener running, connection
    /// handshakes won't be processed.
    async fn aggressive_initial_connections_impl(
        op_manager: &Arc<OpManager>,
        min_connections: usize,
    ) {
        tracing::info!(
            "Starting aggressive connection acquisition phase (target: {} connections)",
            min_connections
        );

        // For small networks, we want to ensure all nodes discover each other quickly
        // to avoid the 10+ second delays on first GET operations
        let start = tokio::time::Instant::now();
        let max_duration = Duration::from_secs(10);
        let mut last_connection_count = 0;

        while start.elapsed() < max_duration {
            // Cooperative yielding for CI environments with limited CPU cores
            // This is critical - the event listener needs CPU time to process handshakes
            tokio::task::yield_now().await;

            let current_connections = op_manager.ring.open_connections();

            // If we've reached our target, we're done
            if current_connections >= min_connections {
                tracing::info!(
                    "Reached minimum connections target: {}/{}",
                    current_connections,
                    min_connections
                );
                break;
            }

            // Log progress when connection count changes
            if current_connections != last_connection_count {
                tracing::info!(
                    "Connection progress: {}/{} (elapsed: {}s)",
                    current_connections,
                    min_connections,
                    start.elapsed().as_secs()
                );
                last_connection_count = current_connections;
            } else {
                tracing::debug!(
                    "Current connections: {}/{}, waiting for more peers (elapsed: {}s)",
                    current_connections,
                    min_connections,
                    start.elapsed().as_secs()
                );
            }

            // Check more frequently at the beginning to detect quick connections
            let sleep_duration = if start.elapsed() < Duration::from_secs(3) {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(500)
            };
            tokio::time::sleep(sleep_duration).await;
        }

        let final_connections = op_manager.ring.open_connections();
        tracing::info!(
            "Aggressive connection phase complete. Final connections: {}/{} (took {}s)",
            final_connections,
            min_connections,
            start.elapsed().as_secs()
        );
    }

    pub(super) async fn run_node(mut self) -> anyhow::Result<Infallible> {
        // Record the start time for uptime tracking in shutdown event
        let start_time = tokio::time::Instant::now();

        // Install the shutdown teardown guard BEFORE any fallible setup so the
        // detached executor/client-events tasks are aborted and the ring's redb
        // `Storage` clones are dropped on EVERY exit path — including an early
        // `?` return (e.g. a failed initial join). The session-actor and
        // result-router abort handles are folded in too so those tasks don't
        // leak on an early return; later-spawned tasks (initial-join,
        // aggressive-connect) are added via `track`. See issue #4401.
        let mut teardown = {
            let mut aborts = std::mem::take(&mut self.detached_task_aborts);
            aborts.push(self.session_actor_task.abort_handle());
            aborts.push(self.result_router_task.abort_handle());
            ShutdownTeardown::new(aborts, self.op_manager.ring.clone())
        };

        // Initialize network status tracking for the connecting page diagnostics
        let gateway_addrs: std::collections::HashSet<std::net::SocketAddr> = self
            .conn_manager
            .gateways
            .iter()
            .filter_map(|g| g.socket_addr())
            .collect();
        super::network_status::init(
            self.conn_manager.listening_port(),
            gateway_addrs,
            crate::config::PCK_VERSION.to_string(),
        );
        // Wire the dashboard's subscription view directly to canonical
        // ring state so the panel doesn't drift the way the legacy
        // `record_subscription` mirror did.
        let ring = self.op_manager.ring.clone();
        super::network_status::set_subscription_provider(std::sync::Arc::new({
            let ring = ring.clone();
            move || ring.dashboard_subscription_snapshot()
        }));
        // Same pattern for the per-contract governance snapshot —
        // dashboard reads live state from the GovernanceManager
        // populated by the meter and HostingManager wiring.
        super::network_status::set_governance_provider(std::sync::Arc::new(move || {
            ring.dashboard_governance_snapshot()
        }));
        // Same pattern for the contract ban list (#4302) — dashboard
        // reads the canonical `Ring::contract_ban_list` so the count
        // tile + entry list can't drift from a mirrored counter.
        let ban_list_ring = self.op_manager.ring.clone();
        super::network_status::set_ban_list_provider(std::sync::Arc::new(move || {
            ban_list_ring.dashboard_ban_list_snapshot()
        }));

        // Wire live ring stats for the dashboard: connection count +
        // hosted contracts + own public key, read on every homepage
        // request.
        let ring_stats = self.op_manager.ring.clone();
        super::network_status::set_ring_stats_provider(std::sync::Arc::new(move || {
            let (peer_id, own_pub_key) = {
                let pk = ring_stats.connection_manager.own_location();
                let key_bytes = pk.pub_key().as_bytes();
                let peer_id = pk.pub_key().to_string(); // 12-byte Display
                let own_pub_key = bs58::encode(key_bytes).into_string(); // full 32-byte
                (peer_id, own_pub_key)
            };
            let rate_limiter = &ring_stats.update_rate_limiter;
            super::network_status::RingStatsSnapshot {
                connection_count: ring_stats.connection_manager.connection_count() as u32,
                hosted_contracts: ring_stats.hosting_contracts_count() as u32,
                peer_id,
                own_pub_key,
                updates_accepted: rate_limiter.accepted_total(),
                updates_rate_limited: rate_limiter.rejected_total(),
                updates_capacity_dropped: rate_limiter.capacity_rejected_total(),
            }
        }));

        // Emit peer startup event
        if let Some(event) = crate::tracing::NetEventLog::peer_startup(
            &self.op_manager.ring,
            crate::config::PCK_VERSION.to_string(),
            None, // git_commit - not available in library, only in binary
            None, // git_dirty - not available in library, only in binary
        ) {
            use either::Either;
            self.op_manager
                .ring
                .register_events(Either::Left(event))
                .await;
            tracing::info!(
                version = crate::config::PCK_VERSION,
                is_gateway = self.op_manager.ring.is_gateway(),
                "Peer startup event emitted"
            );
        }

        if self.should_try_connect {
            let join_handle = connect::initial_join_procedure(
                self.op_manager.clone(),
                &self.conn_manager.gateways,
            )
            .await?;
            self.initial_join_task = Some(join_handle);

            // Note: We don't run aggressive_initial_connections here because
            // the event listener hasn't started yet. The connect requests from
            // initial_join_procedure are queued but won't be processed until
            // the event listener runs. Instead, we'll run the aggressive
            // connection phase concurrently with the event listener below.
        }

        // The initial-join task (spawned above when connecting) outlives this
        // setup phase too; fold its abort handle into the teardown guard so an
        // early return below also stops it. The `JoinHandle` itself is detached
        // (dropping it does not cancel the task) — the guard's abort handle is
        // what tears it down.
        if let Some(join_handle) = self.initial_join_task.take() {
            teardown.track(join_handle.abort_handle());
        }

        // Spawn aggressive connection task to run concurrently with event listener.
        // This is needed because connection handshakes are processed by the event
        // listener, so we can't block waiting for connections before it starts.
        if self.should_try_connect {
            let op_manager = self.op_manager.clone();
            let min_connections = op_manager.ring.connection_manager.min_connections;
            let aggressive_conn_task = GlobalExecutor::spawn(async move {
                Self::aggressive_initial_connections_impl(&op_manager, min_connections).await;
            });
            teardown.track(aggressive_conn_task.abort_handle());
        }

        let f = self.conn_manager.run_event_listener(
            self.op_manager.clone(),
            self.client_wait_for_transaction,
            self.notification_channel,
            self.node_controller,
        );

        // Monitor spawned infrastructure tasks (session actor, result router).
        // If any of these panics or exits unexpectedly, the node runs degraded with no
        // logs or detection. Combine into a single future that produces an error.
        // Their abort handles already live in the teardown guard (folded in at
        // the top of `run_node`), so the JoinHandles can be moved into the future
        // here without losing the ability to cancel them on shutdown.
        let infra_monitor = {
            let mut session_handle = self.session_actor_task;
            let mut router_handle = self.result_router_task;
            async move {
                fn join_result_to_error(
                    name: &str,
                    r: Result<(), tokio::task::JoinError>,
                ) -> anyhow::Error {
                    match r {
                        Err(e) if e.is_panic() => anyhow::anyhow!("{name} panicked: {e}"),
                        Err(e) => anyhow::anyhow!("{name} task failed: {e}"),
                        Ok(()) => anyhow::anyhow!("{name} exited unexpectedly"),
                    }
                }
                let e: anyhow::Error = tokio::select! {
                    biased;
                    r = &mut session_handle => join_result_to_error("Session actor", r),
                    r = &mut router_handle => join_result_to_error("Result router", r),
                };
                e
            }
        };

        // Monitor background tasks registered during node construction
        // (Ring maintenance, garbage cleanup, etc.)
        let background_monitor = self.background_task_monitor.wait_for_any_exit();

        let result = crate::deterministic_select! {
            r = f => {
               let Err(e) = r;
               // #4549: on a fatal (non-graceful) listener exit, abort the process
               // immediately rather than risk hanging in the teardown/return path
               // below (in production the node sat network-dead and spun for ~37min
               // before finally exiting, so systemd never restarted it). A graceful
               // shutdown (Disconnect / SIGTERM / auto-update) must still fall through
               // to the clean teardown. Gated so only the real binary force-exits.
               if !listener_exit_is_graceful(&e)
                   && ABORT_ON_FATAL_LISTENER_EXIT.load(std::sync::atomic::Ordering::Relaxed)
               {
                   // #4551: pick 42 (burst-exempt, fires the auto-update self-heal
                   // hook) for a node that ran healthily before dying, vs the counted
                   // FAST_CRASH_EXIT_CODE for a fast crash / boot-wedge so a tight loop
                   // trips the unit's StartLimitBurst and is stopped instead of looping.
                   // The fast-crash code is emitted only when the binary opted in (a
                   // regenerated systemd unit that handles 45); otherwise 42 is kept so
                   // an old unit / the mac/Windows run-wrapper still self-heals.
                   let uptime = start_time.elapsed();
                   let exit_code = fatal_listener_exit_code(
                       uptime,
                       EMIT_FAST_CRASH_EXIT_CODE.load(std::sync::atomic::Ordering::Relaxed),
                   );
                   eprintln!("CRITICAL: Network event listener exited (fatal): {e}");
                   tracing::error!(
                       error = %e,
                       exit_code,
                       uptime_secs = uptime.as_secs(),
                       fast_crash = exit_code == FAST_CRASH_EXIT_CODE,
                       "Network event listener exited unexpectedly; forcing immediate process \
                        exit so the service manager restarts the node promptly and (for a \
                        long-uptime exit) the auto-update hook can fire (avoids the \
                        wedged-but-alive dark window, #4549; crash-loop bounding, #4551)"
                   );
                   std::process::exit(exit_code);
               }
               eprintln!("CRITICAL: Network event listener exited: {e}");
               tracing::error!("Network event listener exited: {}", e);
               Err(e)
            },
            e = self.client_events_task => {
                eprintln!("CRITICAL: Client events task exited: {e}");
                tracing::error!("Client events task exited: {:?}", e);
                Err(e)
            },
            e = self.contract_executor_task => {
                eprintln!("CRITICAL: Contract executor task exited: {e}");
                tracing::error!("Contract executor task exited: {:?}", e);
                Err(e)
            },
            e = infra_monitor => {
                eprintln!("CRITICAL: Infrastructure task exited: {e}");
                tracing::error!("Infrastructure task exited: {:?}", e);
                Err(e)
            },
            e = background_monitor => {
                eprintln!("CRITICAL: Background task exited: {e}");
                tracing::error!("Background task exited: {:?}", e);
                Err(e)
            },
        };

        // Tear down the detached executor + client-events tasks and clear the
        // ring's redb `Storage` clones (issue #4401).
        //
        // `deterministic_select!` above only *observed* one of these futures
        // completing — the other detached tasks are still running on the
        // process-wide `GlobalExecutor`. Until they stop they keep their
        // `op_manager` clones, the `ClientEventsCombinator` (which, via the
        // channel-close chain documented on `detached_task_aborts`, owns the
        // WebSocket `axum::serve` server), and the contract executor's redb
        // `Database` handle alive — so a graceful shutdown would leave the
        // on-disk redb file locked and a stale WS server answering requests.
        //
        // The teardown now runs through `ShutdownTeardown::drop`, which also
        // fires on any early `?`/return during setup above. Fire it explicitly
        // here (idempotent) so the tasks are aborted and the redb `Storage`
        // clones dropped BEFORE we emit the shutdown event below, preserving the
        // original ordering on the happy path. Lock release is asynchronous (see
        // the guard's docs): abort only signals, so the executor task's redb
        // clones drop when the runtime next polls it to completion.
        drop(teardown);

        // Emit peer shutdown event
        let (graceful, reason) = match &result {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        if let Some(event) = crate::tracing::NetEventLog::peer_shutdown(
            &self.op_manager.ring,
            graceful,
            reason.clone(),
            start_time,
        ) {
            use either::Either;
            self.op_manager
                .ring
                .register_events(Either::Left(event))
                .await;
            tracing::info!(
                graceful,
                reason = reason.as_deref().unwrap_or("clean exit"),
                uptime_secs = start_time.elapsed().as_secs(),
                "Peer shutdown event emitted"
            );
        }

        result
    }

    /// Build a new node and return it along with a shutdown sender.
    ///
    /// The shutdown sender can be used to trigger graceful shutdown by sending
    /// `NodeEvent::Disconnect`.
    pub(crate) async fn build<CH, const CLIENTS: usize, ER>(
        config: NodeConfig,
        clients: [BoxedClient; CLIENTS],
        event_register: ER,
        ch_builder: CH::Builder,
    ) -> anyhow::Result<(Self, tokio::sync::mpsc::Sender<NodeEvent>)>
    where
        CH: ContractHandler + Send + 'static,
        ER: NetEventRegister + Clone,
    {
        let channel_capacity = config.config.network_api.event_loop_channel_capacity;
        let (notification_channel, notification_tx) =
            event_loop_notification_channel_with_capacity(channel_capacity);
        let (mut ch_outbound, ch_inbound, wait_for_event) = contract::contract_handler_channel();
        let (client_responses, cli_response_sender) = contract::client_responses_channel();

        // Prepare session adapter channel for actor-based client management
        let (session_tx, session_rx) = tokio::sync::mpsc::channel(1000);

        // Install session adapter in contract handler
        ch_outbound.with_session_adapter(session_tx.clone());

        // Create result router channel for dual-path result delivery
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(1000);

        // Spawn Session Actor
        use crate::client_events::session_actor::SessionActor;
        let session_actor = SessionActor::new(session_rx, cli_response_sender.clone());
        let session_actor_task = GlobalExecutor::spawn(async move {
            tracing::info!("Session actor starting");
            session_actor.run().await;
            tracing::warn!("Session actor stopped");
        });

        // Spawn ResultRouter task
        use crate::client_events::result_router::ResultRouter;
        let router = ResultRouter::new(result_router_rx, session_tx.clone());
        let result_router_task = GlobalExecutor::spawn(async move {
            tracing::info!("Result router starting");
            router.run().await;
            tracing::warn!("Result router stopped");
        });

        tracing::info!("Actor-based client management infrastructure installed with result router");

        let background_task_monitor = BackgroundTaskMonitor::new();
        // Phase 1 / Phase 1.5 of the outer-loop rate-controller RFC
        // (#4074): start the cross-connection RTT shadow aggregator
        // and the out-of-band reference-path probe. Both are pure
        // observation — never read by the production data path.
        //
        // The local peer id is tagged onto every emitted event so the
        // collector can disaggregate samples by reporting node. We
        // construct it as `PeerId::new(pub_key, addr).to_string()` to
        // match the *format* used by other OTLP events (set elsewhere
        // from `KnownPeerKeyLocation::Display`).
        //
        // Caveat: for non-gateway nodes the external `own_addr` is
        // not known at build time, so we fall back to the listener
        // address (typically `0.0.0.0:<port>`). Other OTLP events
        // emitted later read the up-to-date `Ring::own_location()`,
        // so cross-correlation with the rest of the event stream by
        // peer_id alone works for gateways but is best-effort for
        // leaf nodes until they learn their external address. Phase
        // 1.5 accepts this; a refresh path is tracked in #4294.
        //
        // Gate on telemetry being enabled AND reference-ping being
        // explicitly opted in: when telemetry is off
        // (`telemetry-enabled = false`) or in test environments
        // (detected by `--id` flag), `TelemetryReporter::new` returns
        // `None` and `send_standalone_event_with_peer_id` silently
        // drops events. The shadow aggregator is cheap to leave
        // running (no I/O), but reference-ping issues a real UDP DNS
        // query at 1Hz, so we must not fire that traffic by default.
        //
        // `reference_ping_enabled` defaults to `false`; production
        // gateway configs set it to `true` via
        // `telemetry.reference-ping-enabled = true` in the config
        // file (or `FREENET_REFERENCE_PING_ENABLED=true`). This keeps
        // CI integration tests (which build `NodeConfig` directly
        // without `--id`, so `is_test_environment` stays false) from
        // accidentally firing DNS traffic and perturbing
        // timing-sensitive multi-node tests. It also avoids
        // surprising opt-out operators with default Cloudflare hits.
        let reference_ping_enabled = config.config.telemetry.enabled
            && !config.config.telemetry.is_test_environment
            && config.config.telemetry.reference_ping_enabled;
        // Phase 1.6 OS-interface-tx probe (#4074): same opt-in gating as
        // reference-ping (telemetry on + not a test env + flag set). It
        // reads /proc/net/dev at 1Hz, so it must not fire on dev machines
        // or in CI by default.
        let iface_tx_enabled = config.config.telemetry.enabled
            && !config.config.telemetry.is_test_environment
            && config.config.telemetry.iface_tx_enabled;
        let local_peer_id = config.local_peer_id_string();
        crate::transport::rolling_rtt_stats::spawn_aggregator(
            local_peer_id.clone(),
            &background_task_monitor,
        );
        // Phase 1.6 demand + outbound-class aggregators (#4074). Both are
        // always-on like the RTT aggregator: they only read atomics + the
        // bandwidth handle and emit one OTLP event per second each (no
        // I/O). Observation only.
        crate::transport::shadow_demand::spawn_demand_aggregator(
            local_peer_id.clone(),
            &background_task_monitor,
        );
        crate::transport::shadow_demand::spawn_outbound_class_aggregator(
            local_peer_id.clone(),
            &background_task_monitor,
        );
        if reference_ping_enabled {
            crate::transport::reference_ping::spawn_reference_ping(
                local_peer_id.clone(),
                crate::transport::reference_ping::DEFAULT_REFERENCE_TARGET,
                &background_task_monitor,
            );
        }
        if iface_tx_enabled {
            crate::transport::shadow_iface_tx::spawn_iface_tx_monitor(
                local_peer_id,
                &background_task_monitor,
            );
        }
        let connection_manager = ConnectionManager::new(&config);
        let op_manager = Arc::new(OpManager::new(
            notification_tx,
            ch_outbound,
            &config,
            event_register.clone(),
            connection_manager,
            result_router_tx,
            &background_task_monitor,
        )?);
        op_manager.ring.attach_op_manager(&op_manager);

        let contract_handler = CH::build(ch_inbound, op_manager.clone(), ch_builder)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let conn_manager =
            P2pConnManager::build(&config, op_manager.clone(), event_register).await?;

        let parent_span = tracing::Span::current();
        let contract_executor_handle = GlobalExecutor::spawn({
            let task = async move {
                tracing::info!("Contract executor task starting");
                let result = contract::contract_handling(
                    contract_handler,
                    crate::contract::user_input::DashboardPrompter::new(
                        crate::contract::user_input::pending_prompts(),
                        config.config.ws_api.port,
                    ),
                )
                .await;
                match &result {
                    Ok(_) => tracing::warn!("Contract executor task exiting normally (unexpected)"),
                    Err(e) => tracing::error!("Contract executor task exiting with error: {e}"),
                }
                result
            };
            task.instrument(tracing::info_span!(parent: parent_span.clone(), "contract_handling"))
        });
        // Retain the abort handle so shutdown can stop the executor task and
        // drop its redb `Database` clone (issue #4401).
        let contract_executor_abort = contract_executor_handle.abort_handle();
        let contract_executor_task = contract_executor_handle
            .map(|r| match r {
                Ok(Err(e)) => anyhow::anyhow!("Error in contract handling task: {e}"),
                Ok(Ok(_)) => anyhow::anyhow!("Contract handling task exited unexpectedly"),
                Err(e) => anyhow::anyhow!(e),
            })
            .boxed();
        // Wire each client proxy's HTTP layer to this node's op_manager BEFORE
        // the combinator consumes them (the combinator moves each boxed client
        // into a spawned task, after which it can no longer be reached). Only
        // the HTTP/WS proxies act on this — it routes the hosted-mode export
        // endpoint to the executor (P3-live of #4381). Per-node, so concurrent
        // in-process nodes never clobber each other.
        for client in clients.iter() {
            // Pass `Arc<OpManager>` behind `&dyn Any` (the trait keeps the
            // `pub(crate)` OpManager out of its public signature; the proxy
            // downcasts).
            client.set_op_manager(&op_manager as &dyn std::any::Any);
        }
        // Slot 0 = HTTP client API, Slot 1 = WebSocket proxy (from serve_client_api).
        let clients = ClientEventsCombinator::new(clients).with_slot_names(&["http", "websocket"]);
        // Create node controller channel with capacity for shutdown signal
        // We clone the sender to return it for external shutdown triggering
        let (node_controller_tx, node_controller_rx) = tokio::sync::mpsc::channel(1);
        let shutdown_tx = node_controller_tx.clone();
        let client_events_handle = GlobalExecutor::spawn({
            let op_manager_clone = op_manager.clone();
            let task = async move {
                tracing::info!("Client events task starting");
                let result = client_event_handling(
                    op_manager_clone,
                    clients,
                    client_responses,
                    node_controller_tx,
                )
                .await;
                tracing::warn!("Client events task exiting (unexpected)");
                result
            };
            task.instrument(tracing::info_span!(parent: parent_span, "client_event_handling"))
        });
        // Retain the abort handle so shutdown can stop this task and drop the
        // `ClientEventsCombinator` it owns. That drops the combinator's per-slot
        // `Sender`, which closes `client_fn`'s `rx`; `client_fn` then breaks its
        // loop and drops the `WebSocketProxy`, whose `AbortOnDrop` tears down the
        // detached `axum::serve` server + token-cleanup tasks (issue #4401). See
        // `detached_task_aborts` for the full channel-close chain.
        let client_events_abort = client_events_handle.abort_handle();
        let client_events_task = client_events_handle
            .map(|r| match r {
                Ok(_) => anyhow::anyhow!("Client event handling task exited unexpectedly"),
                Err(e) => anyhow::anyhow!(e),
            })
            .boxed();

        Ok((
            NodeP2P {
                conn_manager,
                notification_channel,
                client_wait_for_transaction: wait_for_event,
                op_manager,
                node_controller: node_controller_rx,
                should_try_connect: config.should_connect,
                peer_id: None, // PeerId removed - using PeerKeyLocation instead
                is_gateway: config.is_gateway,
                location: config.location,
                client_events_task,
                contract_executor_task,
                detached_task_aborts: vec![client_events_abort, contract_executor_abort],
                initial_join_task: None,
                session_actor_task,
                result_router_task,
                background_task_monitor,
            },
            shutdown_tx,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #4549: the fast-exit watchdog (`process::exit(42)` on a fatal listener exit)
    /// is gated by `listener_exit_is_graceful`. A regression in this predicate is
    /// dangerous in BOTH directions: classifying a graceful shutdown as fatal would
    /// force-exit the process on every clean SIGTERM / auto-update (turning exit 0
    /// into a spurious exit-42 + auto-update on every stop); classifying a real
    /// wedge as graceful would leave the node spinning network-dead (the bug we are
    /// fixing). Pin both directions.
    #[test]
    fn listener_exit_graceful_classification() {
        // Graceful shutdown (NodeEvent::Disconnect via SIGTERM / auto-update).
        assert!(
            listener_exit_is_graceful(&EventLoopExitReason::GracefulShutdown.into()),
            "GracefulShutdown must be treated as a clean exit (no force-exit)"
        );

        // Unexpected stream end is fatal — the node must fast-exit and restart.
        assert!(
            !listener_exit_is_graceful(&EventLoopExitReason::UnexpectedStreamEnd.into()),
            "UnexpectedStreamEnd must be treated as a fatal wedge"
        );

        // Arbitrary listener errors (UDP-listener death, handler/transport errors)
        // are all fatal.
        assert!(!listener_exit_is_graceful(&anyhow::anyhow!(
            "UDP listen task exited unexpectedly — transport layer is dead"
        )));

        // The real #4549 production error is the TYPED
        // `ContractError::NoEvHandlerResponse` (not a string stand-in) — it must
        // also classify as fatal so the wedge triggers a restart.
        assert!(!listener_exit_is_graceful(
            &crate::contract::ContractError::NoEvHandlerResponse.into()
        ));
    }

    /// The fast-exit flag must be OFF by default so simulation / integration tests
    /// and library embedders never have a fatal listener exit kill their process.
    /// Only the real binary opts in via `enable_abort_on_fatal_listener_exit`.
    #[test]
    fn fatal_listener_fast_exit_is_opt_in() {
        // NOTE: deliberately does NOT call `enable_abort_on_fatal_listener_exit()`
        // (a process-global flip would affect other tests in the same binary).
        assert!(
            !ABORT_ON_FATAL_LISTENER_EXIT.load(std::sync::atomic::Ordering::Relaxed),
            "fatal-listener fast-exit must be opt-in (off unless the binary enables it)"
        );
        // #4551: the distinct fast-crash exit code (45) must ALSO be opt-in — off
        // unless the binary detects a 45-aware systemd unit. A default-on flip would
        // emit 45 under the mac/Windows wrapper or an old unit, losing the exit-42
        // self-heal.
        assert!(
            !EMIT_FAST_CRASH_EXIT_CODE.load(std::sync::atomic::Ordering::Relaxed),
            "fast-crash exit code must be opt-in (off unless the binary enables it)"
        );
        assert_eq!(
            FATAL_LISTENER_EXIT_CODE, 42,
            "must match EXIT_CODE_UPDATE_NEEDED so the systemd ExecStopPost auto-update \
             hook fires on a wedge"
        );
    }

    /// #4551: the fast-crash exit code must be distinct from the update code so the
    /// systemd unit's `SuccessExitStatus=42 43` does NOT whitelist it — otherwise a
    /// boot-wedge loop would be treated as a clean exit and never trip
    /// `StartLimitBurst`. Also pin that it does not collide with the other declared
    /// exit codes (43 = already-running, 44 = bundle-update-staged).
    #[test]
    fn fast_crash_exit_code_is_distinct_and_counted() {
        assert_eq!(
            FAST_CRASH_EXIT_CODE, 45,
            "fast-crash code is 45 (45 must stay OUT of the unit's SuccessExitStatus \
             so it counts toward StartLimitBurst)"
        );
        assert_ne!(
            FAST_CRASH_EXIT_CODE, FATAL_LISTENER_EXIT_CODE,
            "a fast crash must NOT reuse the burst-exempt update code 42"
        );
        // 43 = EXIT_CODE_ALREADY_RUNNING (RestartPreventExitStatus), 44 =
        // EXIT_CODE_BUNDLE_UPDATE_STAGED. The fast-crash code must avoid both.
        assert_ne!(
            FAST_CRASH_EXIT_CODE, 43,
            "must not collide with already-running (43)"
        );
        assert_ne!(
            FAST_CRASH_EXIT_CODE, 44,
            "must not collide with bundle-update-staged (44)"
        );
    }

    /// #4551: with fast-crash enabled (a 45-aware systemd unit), a fatal listener exit
    /// that happens *soon* after start is a fast crash / boot-wedge — it must use the
    /// counted [`FAST_CRASH_EXIT_CODE`] so a tight loop trips the limiter. An exit after
    /// a *healthy* uptime keeps the burst-exempt update code 42 so a real fault/update
    /// self-heals and is never rate-limited.
    #[test]
    fn fatal_listener_exit_code_distinguishes_fast_crash_from_healthy_uptime() {
        // Fast crash / boot-wedge: well under the healthy-uptime threshold.
        for secs in [0u64, 1, 10, 30, 59] {
            assert_eq!(
                fatal_listener_exit_code(Duration::from_secs(secs), true),
                FAST_CRASH_EXIT_CODE,
                "with fast-crash enabled, a fatal exit after only {secs}s uptime is a \
                 fast crash → counted code"
            );
        }
        // Boundary: exactly the threshold counts as healthy (>= is update-exit).
        assert_eq!(
            fatal_listener_exit_code(MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT, true),
            FATAL_LISTENER_EXIT_CODE,
            "uptime exactly at the threshold is treated as a healthy-then-died exit"
        );
        // Healthy uptime: the node ran for a while before dying.
        for secs in [60u64, 120, 3600, 86_400] {
            assert_eq!(
                fatal_listener_exit_code(Duration::from_secs(secs), true),
                FATAL_LISTENER_EXIT_CODE,
                "a fatal exit after {secs}s uptime keeps the burst-exempt update code"
            );
        }
    }

    /// #4551 (Codex P2): the fast-crash exit code 45 is meaningful ONLY under a
    /// regenerated systemd unit that handles it (fast-crash DISABLED otherwise). Under
    /// the macOS/Windows run-wrapper, an OLD/custom systemd unit (e.g. a node
    /// auto-updated to this binary but not reinstalled), or unsupervised, the node must
    /// keep emitting 42 — those supervisors self-heal on 42 (and the wrapper bounds the
    /// loop with its own backoff + 50-failure cap), whereas a 45 is a generic crash that
    /// loses the self-heal. So when fast-crash is disabled, every uptime — including a
    /// fast boot-crash — must map to [`FATAL_LISTENER_EXIT_CODE`] (42).
    #[test]
    fn fatal_listener_exit_code_uses_update_code_when_fast_crash_disabled() {
        for secs in [0u64, 1, 10, 30, 59, 60, 120, 3600, 86_400] {
            assert_eq!(
                fatal_listener_exit_code(Duration::from_secs(secs), false),
                FATAL_LISTENER_EXIT_CODE,
                "with fast-crash disabled, a fatal exit after {secs}s must keep exit 42 \
                 (an old unit / the run-wrapper only understands 42); 45 would lose the \
                 self-heal"
            );
        }
        // The boundary value is also the update code when fast-crash is disabled.
        assert_eq!(
            fatal_listener_exit_code(MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT, false),
            FATAL_LISTENER_EXIT_CODE,
        );
    }

    /// Verify that a spawned task that panics is detected via JoinHandle.
    #[tokio::test]
    async fn test_join_handle_detects_panic() {
        let handle: JoinHandle<()> = tokio::spawn(async {
            panic!("intentional test panic");
        });
        let result = handle.await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.is_panic(),
            "JoinError should indicate a panic, got: {err}"
        );
    }

    /// Verify that a spawned task that returns cleanly produces Ok.
    #[tokio::test]
    async fn test_join_handle_detects_clean_exit() {
        let handle: JoinHandle<()> = tokio::spawn(async {
            // Clean return
        });
        let result = handle.await;
        assert!(result.is_ok(), "Clean task exit should produce Ok");
    }

    /// Three tasks: 2 sleeping, 1 panics after short delay. Verify tokio::select!
    /// triggers on the panicked task and returns a panic error.
    #[tokio::test]
    async fn test_select_catches_first_panicked_task() {
        let mut h1: JoinHandle<()> = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let mut h2: JoinHandle<()> = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let mut h3: JoinHandle<()> = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            panic!("task 3 panicked");
        });

        let result: anyhow::Result<()> = tokio::select! {
            biased;
            r = &mut h1 => match r {
                Err(e) if e.is_panic() => Err(anyhow::anyhow!("task 1 panicked: {e}")),
                Err(e) => Err(anyhow::anyhow!("task 1 failed: {e}")),
                Ok(()) => Err(anyhow::anyhow!("task 1 exited")),
            },
            r = &mut h2 => match r {
                Err(e) if e.is_panic() => Err(anyhow::anyhow!("task 2 panicked: {e}")),
                Err(e) => Err(anyhow::anyhow!("task 2 failed: {e}")),
                Ok(()) => Err(anyhow::anyhow!("task 2 exited")),
            },
            r = &mut h3 => match r {
                Err(e) if e.is_panic() => Err(anyhow::anyhow!("task 3 panicked: {e}")),
                Err(e) => Err(anyhow::anyhow!("task 3 failed: {e}")),
                Ok(()) => Err(anyhow::anyhow!("task 3 exited")),
            },
        };

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("task 3 panicked"),
            "Should catch the panicking task, got: {err_msg}"
        );

        // Clean up the sleeping tasks
        h1.abort();
        h2.abort();
    }
}
