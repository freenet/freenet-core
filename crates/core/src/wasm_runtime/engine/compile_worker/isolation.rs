//! The worker machinery of one node (Linux): the decision layer between the
//! node's compile path and the worker process.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use super::CodeKind;
use super::accepted::{AcceptedModules, Stamp};
pub(crate) use super::budget::HostLimits;
use super::budget::{Denied, LimitsSource, MemoryBudget, Reservation};
use super::caps::{self, Policy};
use super::metrics::{CompileMetrics, Outcome};
use super::protocol::{self, Caps, FLAG_CONSUME_FUEL, Identity};
use super::supervisor::{self, BusyReason, Launcher, Verdict};
use super::worker;
use crate::config::CompileIsolationMode;
use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};
use crate::util::time_source::{DynTimeSource, InstantTimeSrc, TimeSource};
use crate::wasm_runtime::engine::WasmError;
use crate::wasm_runtime::engine::compile_limits::{self, CompileRefusal};
use crate::wasm_runtime::engine::compile_verdicts::CompileVerdictCache;
use crate::wasm_runtime::engine::wasmtime_engine::compile_concurrency;

/// What the node was asked to do.
#[derive(Debug, Clone)]
pub(crate) struct IsolationConfig {
    pub mode: CompileIsolationMode,
    /// Whether delegate code is covered in `enforce` mode (hosted mode).
    pub isolate_delegates: bool,
    /// Where to keep the accepted-modules file; `None` keeps it in memory only.
    pub accepted_dir: Option<PathBuf>,
    /// Whether the node's engine consumes fuel (part of the engine identity).
    pub consume_fuel: bool,
}

impl Default for IsolationConfig {
    fn default() -> Self {
        Self {
            mode: CompileIsolationMode::Off,
            isolate_delegates: false,
            accepted_dir: None,
            consume_fuel: false,
        }
    }
}

fn host_limits_now() -> HostLimits {
    let cg = caps::read_own_limits();
    let memory_limit = cg.memory_max.unwrap_or_else(|| {
        std::fs::read_to_string("/proc/meminfo")
            .ok()
            .as_deref()
            .and_then(caps::parse_mem_total)
            .unwrap_or(u64::MAX / 4)
    });
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    // Current anonymous resident memory, not the lifetime peak and not the file
    // pages (a mapped database file is reclaimable cache, not memory a worker
    // competes with). Total resident memory stands in where the kernel does not
    // report the split.
    let kib = |key: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|r| r.split_whitespace().next()?.parse::<u64>().ok())
    };
    let node_rss = kib("RssAnon:").or_else(|| kib("VmRSS:")).unwrap_or(0) * 1024;
    HostLimits {
        memory_limit,
        node_rss,
        cores: cg.cpu_cores,
        limit_unreadable: cg.memory_unreadable,
    }
}

/// How long a module whose own check keeps failing (a timeout or a crash that is
/// not a cap signature: outcomes the module itself causes) is not tried again:
/// one minute after the first failure, doubling to thirty. Without it a module
/// that makes every worker fail would start one per request. Environmental
/// outcomes (an outside kill, a failed spawn, a worker that could not install
/// its limits, no headroom) never land here, because they say nothing about the
/// module. They open the global breaker.
const BACKOFF_FIRST: Duration = Duration::from_secs(60);
const BACKOFF_MAX: Duration = Duration::from_secs(30 * 60);
const BACKOFF_CAPACITY: usize = 256;

/// The global breaker: while it is open every worker start is refused
/// transiently. It opens for one second on the first environmental failure and
/// doubles on each further one, to a minute; the first success closes it.
const BREAKER_FIRST: Duration = Duration::from_secs(1);
const BREAKER_MAX: Duration = Duration::from_secs(60);

/// The longest a compile waits for a worker slot or for memory.
const SLOT_WAIT_MAX: Duration = Duration::from_secs(10);

/// What the startup probe assumes the node's own resident memory will be: the
/// larger of this and its current memory. A node that has just started is small
/// and grows into its caches; a unit whose limit cannot hold a grown node plus
/// the floor of a worker is marked degraded at startup instead of flapping later.
const PROBE_NODE_ALLOWANCE: u64 = 1536 << 20;

/// Margin on the worker's reported peak when it is kept as the memory a later
/// in-process compile of the module reserves: peak x 5/4.
const REQUIREMENT_MARGIN_NUM: u64 = 5;
const REQUIREMENT_MARGIN_DEN: u64 = 4;

/// The memory in MiB to remember for a module whose worker peaked at
/// `vm_hwm_kib`: the peak with a margin, rounded up, never 0 (0 means unknown).
fn requirement_mib(vm_hwm_kib: u64) -> u32 {
    let kib = vm_hwm_kib.saturating_mul(REQUIREMENT_MARGIN_NUM) / REQUIREMENT_MARGIN_DEN;
    u32::try_from(kib.div_ceil(1024).max(1)).unwrap_or(u32::MAX)
}

/// Dry-run workers that may run at once, beside the node's own compiles.
const DRY_RUN_MAX_INFLIGHT: usize = 1;

/// How often the degraded state is logged at error level.
const DEGRADED_LOG_EVERY: Duration = Duration::from_secs(60);

/// The worker machinery of one node.
#[derive(Clone)]
pub(crate) struct Isolation(Arc<Inner>);

struct Inner {
    mode: CompileIsolationMode,
    isolate_delegates: bool,
    launcher: Option<Launcher>,
    /// Cleared by a failed startup probe: isolation is then off for the run.
    available: AtomicBool,
    policy: Policy,
    budget: MemoryBudget,
    accepted: AcceptedModules,
    identity: Identity,
    caps_digest: [u8; 16],
    metrics: Arc<CompileMetrics>,
    /// Every deadline below (back-off, breaker, log throttles, reservation and
    /// stall timings) reads this clock, not the system clock directly. Only the
    /// worker's wall limit and the memory wait, which time a real child process
    /// and a real condition variable, use the system clock (see `supervisor.rs`
    /// and `budget.rs`).
    time: DynTimeSource,
    /// Per-module back-off (60 s doubling to 30 min, with the jitter
    /// `TrackedBackoff` applies), bounded to `BACKOFF_CAPACITY` modules.
    backoff: Mutex<TrackedBackoff<[u8; 32]>>,
    /// The global breaker: one unit key, 1 s doubling to 60 s with jitter.
    breaker: Mutex<TrackedBackoff<()>>,
    dry_run_inflight: AtomicUsize,
    degraded_logged: Mutex<Option<Instant>>,
    headroom_logged: Mutex<Option<Instant>>,
    /// True while a compile was last refused for want of headroom: the
    /// `refusing_for_memory` gauge and status banner are up for that reason, and a
    /// later grant clears them. The flag and both published values change under
    /// this one lock, so the published state cannot disagree with the flag.
    refusing_for_memory: Mutex<bool>,
}

/// Held for the span of one compile that went through [`Isolation::admit`].
pub(crate) struct Ticket {
    iso: Option<Arc<Inner>>,
    _reservation: Option<Reservation>,
    /// Set when the worker accepted the module: remembered once the node's own
    /// compile has also succeeded.
    worker_accepted: Option<([u8; 32], u32)>,
    started: Option<Instant>,
}

impl Ticket {
    pub(crate) fn none() -> Self {
        Ticket {
            iso: None,
            _reservation: None,
            worker_accepted: None,
            started: None,
        }
    }

    /// The node's own compile has finished.
    pub(crate) fn finish(self, compiled: bool) {
        let Some(iso) = &self.iso else { return };
        if let Some(started) = self.started {
            iso.metrics
                .stall_ms
                .record(iso.time.now().duration_since(started).as_millis() as u64);
        }
        if compiled {
            if let Some((hash, mib)) = self.worker_accepted {
                if iso.accepted.insert(hash, mib) {
                    iso.metrics.set_accepted_set_size(iso.accepted.len());
                }
            }
        }
    }
}

fn caps_digest(policy: &Policy, engine_id: u64) -> [u8; 16] {
    let text = format!(
        "{policy:?}|{}|{:x?}",
        protocol::PROTOCOL_VERSION,
        compile_limits::limits_fingerprint(engine_id)
    );
    let mut d = [0u8; 16];
    d.copy_from_slice(&blake3::hash(text.as_bytes()).as_bytes()[..16]);
    d
}

/// A module that is valid and does nothing: what the startup probe sends.
const PROBE_MODULE: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

/// What one worker run came to, after the counters and logs were written.
enum Judged {
    Accepted {
        /// The memory in MiB a later in-process compile of this module reserves.
        requirement_mib: u32,
    },
    /// Deterministic and attributable to the module: remembered when enforcing.
    Refused(CompileRefusal),
    /// No answer about the module. `would_refuse`: a dry-run counts it as a
    /// module the worker would not have let through.
    Transient {
        message: &'static str,
        would_refuse: bool,
    },
    NotStarted,
}

impl Isolation {
    /// The production setup for `config`: the node's own binary as the worker.
    /// With a mode other than `off` this runs the startup probe.
    pub(crate) fn for_node(
        test_seam: Option<(Launcher, Policy)>,
        config: &IsolationConfig,
        engine_id: u64,
        metrics: Arc<CompileMetrics>,
    ) -> Self {
        // A test seam also fixes the host's limits: the test process's own
        // cgroup and resident memory say nothing about the case under test.
        let limits: LimitsSource = if test_seam.is_some() {
            Arc::new(|| HostLimits {
                memory_limit: 64 << 30,
                node_rss: 0,
                cores: None,
                limit_unreadable: false,
            })
        } else {
            Arc::new(host_limits_now)
        };
        let (launcher, policy) = match (config.mode, test_seam) {
            (CompileIsolationMode::Off, _) => (None, Policy::default()),
            (_, Some((launcher, policy))) => (Some(launcher), policy),
            (_, None) => (Launcher::production(), Policy::default()),
        };
        let this = Self::build(config, engine_id, launcher, policy, limits, metrics);
        this.probe();
        this
    }

    pub(crate) fn build(
        config: &IsolationConfig,
        engine_id: u64,
        launcher: Option<Launcher>,
        policy: Policy,
        limits: LimitsSource,
        metrics: Arc<CompileMetrics>,
    ) -> Self {
        let digest = caps_digest(&policy, engine_id);
        let stamp = Stamp {
            engine_compat_id: engine_id,
            caps_digest: digest,
        };
        let accepted = match (&config.accepted_dir, config.mode) {
            (_, CompileIsolationMode::Off) => AcceptedModules::in_memory(stamp),
            (Some(dir), _) => AcceptedModules::open(dir, stamp),
            (None, _) => AcceptedModules::in_memory(stamp),
        };
        metrics.set_accepted_set_size(accepted.len());
        let max_workers = policy.max_gate.min(compile_concurrency()).max(1);
        Self(Arc::new(Inner {
            mode: config.mode,
            isolate_delegates: config.isolate_delegates,
            launcher,
            available: AtomicBool::new(true),
            policy,
            budget: MemoryBudget::new(policy, max_workers, limits),
            accepted,
            identity: Identity {
                engine_compat_id: engine_id,
                build_id: worker::build_id(),
                flags: if config.consume_fuel {
                    FLAG_CONSUME_FUEL
                } else {
                    0
                },
            },
            caps_digest: digest,
            metrics,
            time: Arc::new(InstantTimeSrc::new()),
            backoff: Mutex::new(TrackedBackoff::new(
                ExponentialBackoff::new(BACKOFF_FIRST, BACKOFF_MAX),
                BACKOFF_CAPACITY,
            )),
            breaker: Mutex::new(TrackedBackoff::new(
                ExponentialBackoff::new(BREAKER_FIRST, BREAKER_MAX),
                1,
            )),
            dry_run_inflight: AtomicUsize::new(0),
            degraded_logged: Mutex::new(None),
            headroom_logged: Mutex::new(None),
            refusing_for_memory: Mutex::new(false),
        }))
    }

    pub(crate) fn metrics(&self) -> &Arc<CompileMetrics> {
        &self.0.metrics
    }

    /// Run a worker on a trivial module with the production limits. If no worker
    /// can ever start here (no `/proc`, a container that forbids the limits, a
    /// build mismatch, or not even the floor of memory under the service limit),
    /// isolation is off for this run, decided at startup. The node stays available, and says so loudly: see
    /// [`Isolation::note_degraded`].
    pub(crate) fn probe(&self) {
        let inner = &self.0;
        if inner.mode == CompileIsolationMode::Off {
            return;
        }
        let ok = match &inner.launcher {
            None => false,
            Some(launcher) => {
                let h = inner.budget.host();
                // Memory too small for even the floor is a probe failure: with
                // nothing running, no worker could ever start here. The node is
                // judged at its expected size, not the small size of a node that
                // has just started.
                let node = h.node_rss.max(PROBE_NODE_ALLOWANCE);
                // A cgroup whose memory limit cannot be read is not treated as
                // having none: the budget would be host RAM, not the service limit.
                match caps::worker_cap(&inner.policy, h.memory_limit, node, 0) {
                    _ if h.limit_unreadable => false,
                    None => false,
                    Some(data_bytes) => {
                        let caps = Caps {
                            cpu_ms: inner.policy.cpu_ms,
                            data_bytes,
                            wall_ms: caps::wall_ms(&inner.policy, h.cores),
                        };
                        matches!(
                            supervisor::run_worker_once(
                                launcher,
                                &inner.identity,
                                &caps,
                                &PROBE_MODULE
                            )
                            .map(|r| supervisor::classify(&r, &caps)),
                            Ok(Verdict::Accepted { .. })
                        )
                    }
                }
            }
        };
        if !ok {
            inner.available.store(false, Ordering::Relaxed);
            inner.metrics.count(Outcome::ProbeFailed);
            inner.metrics.set_degraded(true);
            crate::node::network_status::set_compile_isolation_degraded(true);
            self.note_degraded();
        }
    }

    /// Isolation is requested but not running: say so at error level at most once
    /// a minute (it is called on startup and on every compile that goes
    /// unchecked), count the unchecked compile, and leave the gauge and the node
    /// status set.
    fn note_degraded(&self) {
        let inner = &self.0;
        inner.metrics.count(Outcome::Degraded);
        let mut last = inner
            .degraded_logged
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = inner.time.now();
        if last.is_none_or(|t| now.duration_since(t) >= DEGRADED_LOG_EVERY) {
            *last = Some(now);
            tracing::error!(
                mode = ?inner.mode,
                "compile isolation is requested but cannot run on this node (the startup \
                 probe failed, the service memory limit could not be read, or too little \
                 memory is left under the service limit): contract \
                 code is being compiled WITHOUT the hard CPU and memory limits. See the \
                 compile_isolation_degraded gauge and the node status page"
            );
        }
    }

    /// The verdict cache key. A memory refusal depends on the cap the worker was
    /// granted (which follows the node's headroom), so its key includes the cap in
    /// bytes; every other refusal is independent of it.
    fn worker_key(&self, input_hash: &[u8; 32], cap: Option<u64>) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        h.update(b"compile-worker-verdict");
        h.update(&self.0.caps_digest);
        h.update(input_hash);
        if let Some(cap) = cap {
            h.update(b"data-cap");
            h.update(&cap.to_le_bytes());
        }
        *h.finalize().as_bytes()
    }

    /// Whether the always-on coarse acceptance check (`compile_limits`) applies
    /// to this kind of code: contract code always; delegate code only on hosted
    /// nodes, the same rule as the worker. A delegate on any other node is
    /// installed by the node's own client and is not checked.
    pub(crate) fn coarse_check_applies(&self, kind: CodeKind) -> bool {
        match kind {
            CodeKind::Contract => true,
            CodeKind::Delegate => self.0.isolate_delegates,
        }
    }

    /// Whether this kind of code goes through the worker under the current mode,
    /// and whether a refusal takes effect.
    fn plan(&self, kind: CodeKind) -> (bool, bool) {
        let inner = &self.0;
        match (inner.mode, kind) {
            (CompileIsolationMode::Off, _) => (false, false),
            (CompileIsolationMode::DryRun, _) => (true, false),
            (CompileIsolationMode::Enforce, CodeKind::Contract) => (true, true),
            (CompileIsolationMode::Enforce, CodeKind::Delegate) => {
                (inner.isolate_delegates, inner.isolate_delegates)
            }
        }
    }

    /// Judge `code` before the node compiles it. `Err` means the node must not
    /// compile it. The returned ticket is held until the node's own compile ends.
    ///
    /// In `dry-run` this returns at once: the node's compile is never delayed and
    /// holds nothing of the worker's, which runs on a thread of its own and only
    /// records what it found.
    pub(crate) fn admit(
        &self,
        input_hash: &[u8; 32],
        code: &[u8],
        kind: CodeKind,
        verdicts: &CompileVerdictCache,
    ) -> Result<Ticket, WasmError> {
        let inner = &self.0;
        let (run, enforce) = self.plan(kind);
        if !run {
            return Ok(Ticket::none());
        }
        if !inner.available.load(Ordering::Relaxed) {
            self.note_degraded();
            return Ok(Ticket::none());
        }
        let started = inner.time.now();
        inner.metrics.module_bytes.record(code.len() as u64);
        if inner.accepted.contains(input_hash) {
            inner.metrics.accepted_set_lookup(true);
            inner.metrics.count(Outcome::AcceptedSkip);
            // Accepted is not free: the node still compiles it in this process,
            // so a large one reserves memory like a worker does. Dry-run changes
            // nothing about the node's compile.
            return if enforce {
                self.in_process_ticket(input_hash, started)
            } else {
                Ok(Ticket::none())
            };
        }
        inner.metrics.accepted_set_lookup(false);
        if !enforce {
            self.start_dry_run(*input_hash, code);
            return Ok(Ticket::none());
        }
        let key = self.worker_key(input_hash, None);
        if let Some(refusal) = verdicts.get(&key) {
            inner.metrics.count_cached_refusal(&refusal);
            return Err(WasmError::CompileRefused(refusal));
        }
        // A remembered memory refusal at the cap this compile would get.
        if let Some(cap) = inner.budget.peek_worker_cap() {
            if let Some(refusal) = verdicts.get(&self.worker_key(input_hash, Some(cap))) {
                inner.metrics.count_cached_refusal(&refusal);
                return Err(WasmError::CompileRefused(refusal));
            }
        }
        if self.in_backoff(input_hash) {
            inner.metrics.count(Outcome::BackoffSkip);
            return Err(WasmError::Compile(
                "checking this module recently failed; try again later".into(),
            ));
        }
        if self.breaker_open() {
            inner.metrics.count(Outcome::Busy);
            return Err(WasmError::Compile(
                "the module check is unavailable just now; try again later".into(),
            ));
        }

        let wait_from = inner.time.now();
        let reservation = match inner.budget.acquire_worker(self.max_wait()) {
            Ok(r) => r,
            Err(denied) => return Err(self.not_enough_memory(denied)),
        };
        // The breaker may have opened while this caller waited for a slot: queued
        // callers must not start workers during the cooldown.
        if self.breaker_open() {
            drop(reservation);
            inner.metrics.count(Outcome::Busy);
            return Err(WasmError::Compile(
                "the module check is unavailable just now; try again later".into(),
            ));
        }
        self.headroom_recovered();
        inner
            .metrics
            .gate_wait_ms
            .record(inner.time.now().duration_since(wait_from).as_millis() as u64);

        let caps = self.caps_for(&reservation);
        let memory_key = self.worker_key(input_hash, Some(caps.data_bytes));
        if let Some(refusal) = verdicts.get(&memory_key) {
            drop(reservation);
            inner.metrics.count_cached_refusal(&refusal);
            return Err(WasmError::CompileRefused(refusal));
        }
        match self.judge(input_hash, code, &caps) {
            Judged::Accepted { requirement_mib } => Ok(Ticket {
                iso: Some(inner.clone()),
                _reservation: Some(reservation),
                worker_accepted: Some((*input_hash, requirement_mib)),
                started: Some(started),
            }),
            Judged::Refused(refusal) => {
                // Local log only: the module identity never goes to telemetry.
                tracing::info!(
                    code_hash = %blake3::Hash::from(*input_hash),
                    size_bucket = log2_bucket(code.len()),
                    reason = %refusal,
                    "compile check refused this module"
                );
                let key = if matches!(refusal, CompileRefusal::OverMemory) {
                    memory_key
                } else {
                    key
                };
                verdicts.insert(key, refusal.clone());
                Err(WasmError::CompileRefused(refusal))
            }
            Judged::Transient { message, .. } => Err(WasmError::Compile(message.into())),
            Judged::NotStarted => Err(WasmError::Compile(
                "could not start the module check".into(),
            )),
        }
    }

    /// The longest a compile waits for a slot or for memory: ten seconds, or the
    /// worker's own wall limit when that is shorter.
    fn max_wait(&self) -> Duration {
        Duration::from_millis(self.0.policy.wall_ms).min(SLOT_WAIT_MAX)
    }

    fn caps_for(&self, reservation: &Reservation) -> Caps {
        let h = self.0.budget.host();
        Caps {
            cpu_ms: self.0.policy.cpu_ms,
            data_bytes: reservation.bytes(),
            wall_ms: caps::wall_ms(&self.0.policy, h.cores),
        }
    }

    /// The refusal for a compile that found no memory: transient, never remembered.
    /// Too little headroom is a state of the node, not of the module: it raises
    /// the refusing-for-memory gauge and the status banner (cleared by the next grant), logs
    /// at error level at most once a minute, and opens the global breaker so
    /// that requests do not each wait for memory.
    fn not_enough_memory(&self, denied: Denied) -> WasmError {
        self.0.metrics.count(Outcome::Busy);
        match denied {
            Denied::TooSmall { available } => {
                self.note_headroom_degraded(available);
                self.breaker_trip();
                WasmError::Compile("not enough memory to check this module".into())
            }
            Denied::TimedOut => {
                tracing::warn!("timed out waiting for memory to check a module");
                WasmError::Compile("the module check is busy; try again".into())
            }
        }
    }

    fn note_headroom_degraded(&self, available: u64) {
        let inner = &self.0;
        {
            let mut refusing = inner
                .refusing_for_memory
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !*refusing {
                *refusing = true;
                self.publish_refusing_for_memory(true);
            }
        }
        let mut last = inner
            .headroom_logged
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = inner.time.now();
        if last.is_none_or(|t| now.duration_since(t) >= DEGRADED_LOG_EVERY) {
            *last = Some(now);
            tracing::error!(
                available,
                floor = inner.policy.data_floor,
                "not enough memory is left under the service limit to check a module: new \
                 contract code is being refused until the node's memory falls (enforce needs \
                 MemoryMax of at least 4 GiB, or none). See the \
                 compile_isolation_refusing_for_memory gauge"
            );
        }
    }

    /// A grant was made: the headroom is back, so the runtime degradation raised
    /// by [`Self::note_headroom_degraded`] ends.
    fn headroom_recovered(&self) {
        let inner = &self.0;
        let mut refusing = inner
            .refusing_for_memory
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *refusing {
            *refusing = false;
            self.publish_refusing_for_memory(false);
        }
    }

    /// Publish the gauge and the status flag. Called only with the
    /// `refusing_for_memory` lock held, so the two never lag the flag.
    fn publish_refusing_for_memory(&self, refusing: bool) {
        #[cfg(test)]
        tests_hook::between_flag_and_publish(self.id_for_tests());
        self.0.metrics.set_refusing_for_memory(refusing);
        crate::node::network_status::set_compile_isolation_refusing_for_memory(refusing);
    }

    fn breaker_open(&self) -> bool {
        let b = self.0.breaker.lock().unwrap_or_else(|e| e.into_inner());
        b.is_in_backoff(&())
    }

    /// An environmental failure: no worker could give an answer about any module.
    /// Failures that arrive while the breaker is already open are the same event.
    fn breaker_trip(&self) {
        let mut b = self.0.breaker.lock().unwrap_or_else(|e| e.into_inner());
        if b.is_in_backoff(&()) {
            return;
        }
        b.record_failure(());
    }

    /// A worker answered about a module: the machinery works.
    fn breaker_close(&self) {
        let mut b = self.0.breaker.lock().unwrap_or_else(|e| e.into_inner());
        b.record_success(&());
    }

    /// The memory an in-process compile of an accepted module reserves: what was
    /// recorded when it was accepted (at least the floor, at most the ceiling),
    /// or the ceiling for a record that carries no figure.
    fn in_process_requirement(&self, input_hash: &[u8; 32]) -> u64 {
        let p = &self.0.policy;
        match self.0.accepted.requirement_mib(input_hash) {
            Some(mib) if mib > 0 => (u64::from(mib) << 20).clamp(p.data_floor, p.data_ceiling),
            _ => p.data_ceiling,
        }
    }

    /// An in-process compile of a module the worker accepted earlier.
    fn in_process_ticket(
        &self,
        input_hash: &[u8; 32],
        started: Instant,
    ) -> Result<Ticket, WasmError> {
        let inner = &self.0;
        match inner
            .budget
            .acquire_in_process(self.in_process_requirement(input_hash), self.max_wait())
        {
            Ok(r) => {
                self.headroom_recovered();
                Ok(Ticket {
                    iso: Some(inner.clone()),
                    _reservation: Some(r),
                    worker_accepted: None,
                    started: Some(started),
                })
            }
            Err(denied) => Err(self.not_enough_memory(denied)),
        }
    }

    /// One worker run on `code` under `caps`: counters, logs and back-off are
    /// written here, and what it means for the module is returned.
    fn judge(&self, input_hash: &[u8; 32], code: &[u8], caps: &Caps) -> Judged {
        let inner = &self.0;
        let Some(launcher) = &inner.launcher else {
            return Judged::NotStarted;
        };
        let report = match supervisor::run_worker_once(launcher, &inner.identity, caps, code) {
            Ok(r) => r,
            Err(e) => {
                inner.metrics.count(Outcome::SpawnFailed);
                self.breaker_trip();
                tracing::error!(error = %e.0, "could not start the compile worker");
                return Judged::NotStarted;
            }
        };
        inner
            .metrics
            .worker_wall_ms
            .record(report.wall.as_millis() as u64);
        let code_hash = blake3::Hash::from(*input_hash);
        match supervisor::classify(&report, caps) {
            Verdict::Accepted { vm_hwm_kib, cpu_ms } => {
                inner.metrics.worker_cpu_ms.record(cpu_ms);
                inner.metrics.worker_rss_mib.record(vm_hwm_kib / 1024);
                inner.metrics.count(Outcome::Accepted);
                self.breaker_close();
                self.clear_backoff(input_hash);
                Judged::Accepted {
                    requirement_mib: requirement_mib(vm_hwm_kib),
                }
            }
            Verdict::Refused(refusal) => {
                // Only the worker's own refusals have counters of their own; any
                // other reason (an invalid module, a scan refusal) counts together.
                #[allow(clippy::wildcard_enum_match_arm)]
                let outcome = match refusal {
                    CompileRefusal::OverCpu => Outcome::OverCpu,
                    CompileRefusal::OverMemory => Outcome::OverMemory,
                    _ => Outcome::RefusedFeature,
                };
                inner.metrics.count(outcome);
                self.breaker_close();
                self.clear_backoff(input_hash);
                Judged::Refused(refusal)
            }
            Verdict::Timeout => {
                inner.metrics.count(Outcome::Timeout);
                self.breaker_close();
                self.note_backoff(*input_hash);
                tracing::info!(
                    code_hash = %code_hash,
                    size_bucket = log2_bucket(code.len()),
                    "compile worker timed out on a module"
                );
                Judged::Transient {
                    message: "checking this module timed out",
                    would_refuse: true,
                }
            }
            Verdict::Crashed => {
                inner.metrics.count(Outcome::Crashed);
                self.breaker_close();
                self.note_backoff(*input_hash);
                tracing::warn!(
                    code_hash = %code_hash,
                    size_bucket = log2_bucket(code.len()),
                    end = ?report.end,
                    "the compile worker ended abnormally on a module"
                );
                Judged::Transient {
                    message: "checking this module failed; try again later",
                    would_refuse: true,
                }
            }
            Verdict::Interrupted => {
                inner.metrics.count(Outcome::Busy);
                self.breaker_trip();
                Judged::Transient {
                    message: "the module check was interrupted",
                    would_refuse: false,
                }
            }
            Verdict::Busy(BusyReason::LimitsNotInstalled) => {
                inner.metrics.count(Outcome::InfraFailure);
                self.breaker_trip();
                tracing::error!(
                    reason = %report
                        .response
                        .as_ref()
                        .map_or("", |r| r.reason.as_str()),
                    "the compile worker could not install its limits, so it judged nothing"
                );
                Judged::Transient {
                    message: "the module check could not run here",
                    would_refuse: false,
                }
            }
            Verdict::Busy(reason) => {
                inner.metrics.count(Outcome::Busy);
                self.breaker_trip();
                tracing::warn!(?reason, "the compile worker gave no verdict");
                Judged::Transient {
                    message: "the module check could not complete",
                    would_refuse: false,
                }
            }
        }
    }

    /// Dry-run: a worker on a thread of its own, if the dry-run slot and the
    /// memory budget allow. Never waits and never delays the node's compile; a
    /// busy slot or a full budget skips the module, counted.
    ///
    /// The worker's grant ignores the node's own concurrent compile of the same
    /// module, which is unreserved in dry-run, so peak memory can approach the
    /// limit. That is by design: the worker runs with `oom_score_adj` 1000, so
    /// the kernel chooses it as the victim, which is an outside kill and a
    /// transient outcome.
    fn start_dry_run(&self, input_hash: [u8; 32], code: &[u8]) {
        let inner = &self.0;
        let skip = |this: &Self| this.0.metrics.count(Outcome::DryRunSkipped);
        if self.in_backoff(&input_hash) || self.breaker_open() {
            return skip(self);
        }
        if inner.dry_run_inflight.fetch_add(1, Ordering::AcqRel) >= DRY_RUN_MAX_INFLIGHT {
            inner.dry_run_inflight.fetch_sub(1, Ordering::AcqRel);
            return skip(self);
        }
        let Ok(reservation) = inner.budget.acquire_worker(Duration::ZERO) else {
            inner.dry_run_inflight.fetch_sub(1, Ordering::AcqRel);
            return skip(self);
        };
        let this = self.clone();
        let code = code.to_vec();
        let spawned = std::thread::Builder::new()
            .name("compile-worker-dry-run".into())
            .spawn(move || {
                let caps = this.caps_for(&reservation);
                let started = this.0.time.now();
                let judged = this.judge(&input_hash, &code, &caps);
                let inner = &this.0;
                match judged {
                    Judged::Accepted { requirement_mib } => {
                        if inner.accepted.insert(input_hash, requirement_mib) {
                            inner.metrics.set_accepted_set_size(inner.accepted.len());
                        }
                    }
                    Judged::Refused(refusal) => {
                        tracing::info!(
                            code_hash = %blake3::Hash::from(input_hash),
                            size_bucket = log2_bucket(code.len()),
                            reason = %refusal,
                            enforced = false,
                            "compile check would refuse this module"
                        );
                        inner.metrics.count(Outcome::WouldRefuse);
                    }
                    Judged::Transient { would_refuse, .. } => {
                        if would_refuse {
                            inner.metrics.count(Outcome::WouldRefuse);
                        }
                    }
                    Judged::NotStarted => {}
                }
                inner
                    .metrics
                    .stall_ms
                    .record(inner.time.now().duration_since(started).as_millis() as u64);
                drop(reservation);
                inner.dry_run_inflight.fetch_sub(1, Ordering::AcqRel);
            });
        if spawned.is_err() {
            // The closure (and the reservation in it) was dropped with the error.
            inner.dry_run_inflight.fetch_sub(1, Ordering::AcqRel);
            skip(self);
        }
    }

    fn note_backoff(&self, hash: [u8; 32]) {
        let mut map = self.0.backoff.lock().unwrap_or_else(|e| e.into_inner());
        // A module that has been quiet for a whole maximum back-off after its
        // retry window ended starts over.
        map.cleanup_after_retry_grace();
        map.record_failure(hash);
    }

    fn in_backoff(&self, hash: &[u8; 32]) -> bool {
        let map = self.0.backoff.lock().unwrap_or_else(|e| e.into_inner());
        map.is_in_backoff(hash)
    }

    fn clear_backoff(&self, hash: &[u8; 32]) {
        self.0
            .backoff
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .record_success(hash);
    }

    #[cfg(test)]
    pub(crate) fn accepted_len(&self) -> usize {
        self.0.accepted.len()
    }

    /// Wait until no dry-run worker is running (tests).
    #[cfg(test)]
    pub(crate) fn wait_for_dry_runs(&self) {
        let deadline = Instant::now() + Duration::from_secs(120);
        while self.0.dry_run_inflight.load(Ordering::Acquire) > 0 {
            assert!(Instant::now() < deadline, "a dry-run worker never ended");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Record `hash` as accepted with a requirement of `mib` MiB (0 = no figure,
    /// as a record of the earlier file format).
    #[cfg(test)]
    pub(crate) fn accept_for_tests(&self, hash: [u8; 32], mib: u32) {
        self.0.accepted.insert(hash, mib);
    }

    /// The requirement recorded for `hash`, in MiB.
    #[cfg(test)]
    pub(crate) fn requirement_for_tests(&self, hash: &[u8; 32]) -> Option<u32> {
        self.0.accepted.requirement_mib(hash)
    }

    #[cfg(test)]
    pub(crate) fn budget(&self) -> &MemoryBudget {
        &self.0.budget
    }

    #[cfg(test)]
    pub(crate) fn is_degraded(&self) -> bool {
        !self.0.available.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn note_failure_for_tests(&self, hash: [u8; 32]) {
        self.note_backoff(hash);
    }

    #[cfg(test)]
    pub(crate) fn backoff_left_for_tests(&self, hash: &[u8; 32]) -> Option<Duration> {
        let map = self.0.backoff.lock().unwrap_or_else(|e| e.into_inner());
        map.remaining_backoff(hash)
    }

    #[cfg(test)]
    pub(crate) fn backoff_len_for_tests(&self) -> usize {
        self.0
            .backoff
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    #[cfg(test)]
    pub(crate) fn id_for_tests(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    #[cfg(test)]
    pub(crate) fn breaker_trip_for_tests(&self) {
        self.breaker_trip();
    }

    #[cfg(test)]
    pub(crate) fn headroom_degraded_for_tests(&self, available: u64) {
        self.note_headroom_degraded(available);
    }

    #[cfg(test)]
    pub(crate) fn headroom_recovered_for_tests(&self) {
        self.headroom_recovered();
    }

    #[cfg(test)]
    pub(crate) fn refusing_for_memory_for_tests(&self) -> bool {
        *self
            .0
            .refusing_for_memory
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    #[cfg(test)]
    pub(crate) fn in_backoff_for_tests(&self, hash: &[u8; 32]) -> bool {
        self.in_backoff(hash)
    }
}

/// `size` as a log2 bucket (the number of significant bits), the only form in
/// which a size is logged next to a hash.
fn log2_bucket(size: usize) -> u32 {
    usize::BITS - size.leading_zeros()
}

/// A pause between a flag change and its publication, for one isolation, so a
/// test can interleave two transitions deterministically.
#[cfg(test)]
pub(crate) mod tests_hook {
    use std::sync::{Arc, Mutex};

    type Hook = Arc<dyn Fn() + Send + Sync>;
    static HOOK: Mutex<Option<(usize, Hook)>> = Mutex::new(None);

    pub(crate) fn set(id: usize, f: Option<Hook>) {
        *HOOK.lock().unwrap_or_else(|e| e.into_inner()) = f.map(|f| (id, f));
    }

    pub(super) fn between_flag_and_publish(id: usize) {
        let h = HOOK.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some((hid, f)) = h {
            if hid == id {
                f();
            }
        }
    }
}

#[cfg(test)]
mod breaker_tests {
    use super::*;

    fn iso() -> Isolation {
        Isolation::build(
            &IsolationConfig::default(),
            0,
            None,
            Policy::default(),
            Arc::new(|| HostLimits {
                memory_limit: 64 << 30,
                node_rss: 0,
                cores: None,
                limit_unreadable: false,
            }),
            Arc::new(CompileMetrics::default()),
        )
    }

    fn left(i: &Isolation) -> Duration {
        let b = i.0.breaker.lock().unwrap();
        b.remaining_backoff(&()).unwrap_or(Duration::ZERO)
    }

    fn expire(i: &Isolation) {
        i.0.breaker
            .lock()
            .unwrap()
            .set_retry_after(&(), Instant::now());
    }

    /// `left` is within the +-20% jitter of `nominal` (less a little for the
    /// time the test itself took).
    fn within_jitter(left: Duration, nominal: Duration) -> bool {
        left <= nominal.mul_f64(1.2) && left + Duration::from_millis(500) >= nominal.mul_f64(0.8)
    }

    #[test]
    fn the_breaker_doubles_to_a_minute_ignores_a_failure_while_open_and_closes_on_success() {
        let i = iso();
        assert!(!i.breaker_open());
        i.breaker_trip();
        assert!(i.breaker_open());
        assert!(within_jitter(left(&i), Duration::from_secs(1)));
        // The same event reported by a second in-flight worker does not double it.
        i.breaker_trip();
        assert_eq!(i.0.breaker.lock().unwrap().failure_count(&()), 1);
        let mut want = 2;
        for _ in 0..9 {
            expire(&i);
            i.breaker_trip();
            let l = left(&i);
            let want_d = Duration::from_secs(want.min(60));
            assert!(within_jitter(l, want_d), "{l:?} {want_d:?}");
            want *= 2;
        }
        assert!(left(&i) > Duration::from_secs(47), "capped near a minute");
        i.breaker_close();
        assert!(!i.breaker_open());
        i.breaker_trip();
        assert!(
            within_jitter(left(&i), Duration::from_secs(1)),
            "starts again at one second"
        );
    }

    /// A module at the ceiling keeps its failure history for a whole maximum
    /// back-off after its retry window ends, so the delay stays at the ceiling
    /// instead of restarting at the first delay.
    #[test]
    fn a_module_at_the_ceiling_keeps_its_history_through_the_retry_window() {
        let i = iso();
        let hash = [7u8; 32];
        let mut last = Duration::ZERO;
        for round in 0..14 {
            i.note_backoff(hash);
            let delay =
                i.0.backoff
                    .lock()
                    .unwrap()
                    .remaining_backoff(&hash)
                    .expect("in back-off");
            assert!(delay + Duration::from_millis(500) >= last.mul_f64(0.8 / 1.2));
            last = delay;
            // The window ends; the retry fails again a little later.
            i.0.backoff
                .lock()
                .unwrap()
                .set_retry_after(&hash, Instant::now() - Duration::from_secs(1 + round));
        }
        assert!(
            within_jitter(last, BACKOFF_MAX),
            "grew to the ceiling and stayed: {last:?}"
        );
        // Quiet for longer than a maximum back-off after the window ended: forgotten.
        i.0.backoff
            .lock()
            .unwrap()
            .set_retry_after(&hash, Instant::now() - BACKOFF_MAX - Duration::from_secs(5));
        i.note_backoff(hash);
        let after =
            i.0.backoff
                .lock()
                .unwrap()
                .remaining_backoff(&hash)
                .unwrap();
        assert!(within_jitter(after, BACKOFF_FIRST), "{after:?}");
    }
}

#[cfg(test)]
mod requirement_tests {
    use super::requirement_mib;

    #[test]
    fn the_requirement_is_the_peak_with_a_margin_rounded_up() {
        assert_eq!(requirement_mib(0), 1, "never 0, which means unknown");
        assert_eq!(requirement_mib(1024 * 100), 125);
        assert_eq!(requirement_mib(1024 * 100 + 1), 126);
        assert_eq!(requirement_mib(u64::MAX), u32::MAX);
    }
}
