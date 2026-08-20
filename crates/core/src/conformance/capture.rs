//! Operator-enabled capture of real contract traffic, for offline replay.
//!
//! This is the RFC's Phase 1 capture path: a way to get representative execution
//! data off an ordinary peer so the verifier can be exercised against real
//! contracts before any of it influences the network. Without it, the only material
//! available is fixtures written by whoever wrote the checks, which is exactly the
//! material least likely to contain a surprise.
//!
//! # Rules this path obeys, in order of importance
//!
//! **It must never affect contract operation.** The executor's only interaction is
//! a `try_send` on a bounded channel and, when that channel is full, a counter
//! increment. No lock, no await, no fallible I/O on the hot path. If the writer
//! falls behind, observations are dropped and counted — capture losing data is
//! always preferable to synchronization stalling behind it.
//!
//! **It is off unless deliberately switched on.** Enabled only by setting
//! `FREENET_CONFORMANCE_CAPTURE_DIR`, read once at startup. There is no config-file
//! key on purpose: this is a diagnostic path, and a persisted setting is one someone
//! turns on and forgets. The RFC leaves the mechanism open and asks for the least
//! intrusive one.
//!
//! **It is bounded.** Per-contract sampling goes through [`ContractSampler`], which
//! caps bytes rather than item counts, so a contract with large states cannot grow
//! the corpus without limit. The number of contracts tracked is capped too.
//!
//! # Privacy
//!
//! A capture contains real application state, including values no longer current
//! anywhere else. Treat a capture directory as sensitive: it is not published, not
//! uploaded, and should not outlive the analysis it was collected for.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use tokio::sync::mpsc;

use super::bundle::ReplayBundle;
use super::sampler::{Admission, ContractSampler, SamplerConfig};

/// Environment variable that switches capture on and says where to write.
pub const CAPTURE_DIR_ENV: &str = "FREENET_CONFORMANCE_CAPTURE_DIR";

/// Optional per-contract byte budget override for a capture run.
///
/// The sampler's default is sized for ordinary state. Measured against the live
/// River room, whose states are ~356 KB, the default 4 MiB holds barely a dozen
/// states however long the node runs — so a long collection produces a corpus no
/// deeper than a short one, and "no violations found" then rests on three or four
/// states. That is a much weaker statement than the same words applied to a
/// small-state contract, and the difference is invisible in the output.
///
/// This is an operator knob for a deliberate diagnostic run, not a new default:
/// raising it costs disk and memory on the node doing the capturing.
pub const CAPTURE_MAX_BYTES_ENV: &str = "FREENET_CONFORMANCE_CAPTURE_MAX_BYTES";

/// Sampler configuration for this capture run.
fn sampler_config() -> SamplerConfig {
    sampler_config_from(std::env::var(CAPTURE_MAX_BYTES_ENV).ok().as_deref())
}

/// Split out from [`sampler_config`] so the parsing is testable without mutating
/// process-global environment state, which other tests running in the same
/// process would see.
fn sampler_config_from(raw: Option<&str>) -> SamplerConfig {
    let mut config = SamplerConfig::default();
    if let Some(bytes) = raw
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|bytes| *bytes > 0)
    {
        config.max_bytes = bytes;
        // Track the total in BOTH directions. `max` here was a bug: lowering the
        // budget below the shipped default left the per-state ceiling above the
        // whole budget (4 KiB total against a 1 MiB ceiling), and one state was then
        // free to exclude every other sample — the exact thing the ceiling exists to
        // prevent. Worse, states were then refused as `NoBudget` rather than
        // `TooLarge`, so the "retained nothing for this contract" warning named the
        // wrong cause.
        config.max_state_bytes = (bytes / 4).max(1);
    }
    config
}

/// How many observations may queue before the executor starts dropping them.
///
/// Small on purpose. A deep queue would hide a writer that cannot keep up, and the
/// honest failure for this path is a visible drop count rather than latent memory
/// growth behind the merge path.
const OBSERVATION_QUEUE: usize = 256;

/// Upper bound on bytes queued but not yet sampled.
///
/// The item cap alone is not a memory bound. Each `Observation` owns full copies of
/// the base, incoming and result states, so 256 queued observations of a contract
/// with multi-megabyte states is gigabytes in flight — on a node whose hosted-set
/// budget is fighting for a fraction of that, and with none of it visible to the
/// memory accounting. Capture is a diagnostic; it must not be able to OOM the node
/// it is diagnosing.
///
/// 8 MiB holds a useful burst of ordinary states and several of the largest observed
/// in the wild (~356 KB), while being small enough that the answer to "could capture
/// exhaust memory" is no by construction rather than by argument.
const MAX_QUEUED_BYTES: usize = 8 * 1024 * 1024;

/// Budgets for related-contract state, derived from the sampler's own budgets.
///
/// Related state gets its OWN allowance rather than sharing the per-contract sample
/// budget: sharing would let large related state crowd out the very samples the
/// related state exists to make checkable. But the allowance must SCALE with the
/// sampler's, and until #5320's follow-up it did not - it was a hardcoded 512 KiB,
/// used as both the per-state cap and the total.
///
/// That number was quietly deciding what could be checked at all. States on the live
/// network run to ~356 KiB, so TWO related contracts already exceeded the total and
/// the second was silently discarded; `FREENET_CONFORMANCE_CAPTURE_MAX_BYTES` raised
/// the sampler's budget but never reached this one. Measured consequence: 1,567 of
/// 5,856 replayed cases reached no verdict for want of related state, and five
/// contracts produced no verdict on ANY case.
///
/// Why that is a correctness problem and not a resource trade-off: a contract that
/// depends on a large related contract becomes permanently unjudgeable, and an
/// unjudgeable contract reads as a clean one. Depending on related state must not be
/// a way to escape conformance checking. The RFC's adversarial corpus names this
/// class directly - "attempts to make violations hard to trigger".
///
/// So: the same per-state ceiling and the same per-contract total the sampler uses,
/// both of which the operator can already raise for a diagnostic run.
///
/// # This raises the DEFAULT footprint, not just the override's reach
///
/// Worth stating plainly rather than leaving to be discovered. At
/// `SamplerConfig::default()` the related-state total per tracked contract goes from a
/// flat 512 KiB to `max_bytes` (4 MiB), and the per-state ceiling from 512 KiB to
/// `max_state_bytes` (1 MiB) - so a node that sets no environment variable at all still
/// carries more. Node-wide worst case at `MAX_TRACKED_CONTRACTS` is bounded by
/// `64 * (max_bytes + max_bytes)`, related state plus samples.
///
/// Accepted because capture does not run unless an operator sets
/// `FREENET_CONFORMANCE_CAPTURE_DIR`, `MAX_RELATED_CONTRACTS` still bounds the count,
/// and the alternative is what this comment exists to describe: a budget too small to
/// judge the contracts it was collected for.
fn related_budgets(config: &SamplerConfig) -> (usize, usize) {
    (config.max_state_bytes, config.max_bytes)
}

/// Upper bound on distinct related contracts retained per tracked contract.
const MAX_RELATED_CONTRACTS: usize = 8;

/// Upper bound on contracts sampled concurrently.
const MAX_TRACKED_CONTRACTS: usize = 64;

/// How often to retry drawing focus from the hosted set before it is registered.
///
/// Short, because until it succeeds a focus-scoped peer is sampling against an empty
/// focus set and therefore recording nothing.
const HOSTED_SOURCE_WARMUP: std::time::Duration = std::time::Duration::from_secs(5);

/// How many warm-up draws to make before giving up.
///
/// The arm normally switches off on its first successful draw, so this only matters
/// when no hosted source is EVER registered - which is reachable, not hypothetical:
/// `freenet` calls `capture::global()` (spawning this task) before it checks whether
/// the daemon is disabled, and a disabled daemon idles forever without ever building a
/// `Ring`. Without a cap the ticker would fire every five seconds for days, while the
/// constant above claimed to be self-terminating. A minute of retries is far longer
/// than node startup needs.
const HOSTED_SOURCE_WARMUP_ATTEMPTS: u32 = 12;

/// How many observations may accumulate before a flush, regardless of the clock.
///
/// The timer alone leaves the whole interval exposed, and worse for a short run:
/// the writer is a detached task whose handle is dropped, so runtime shutdown aborts
/// it rather than letting it finish, and the sender lives in a process-wide static so
/// `recv()` never returns `None` to trigger the closing flush. A node that runs for
/// less than one interval therefore writes nothing at all.
///
/// Bounding by work as well as by time means what is at risk is a known quantity of
/// observations rather than however many happened to arrive in a minute. This does
/// not make shutdown safe — it makes the loss small and predictable, which is the
/// honest fix available without reaching into the node's shutdown path.
const FLUSH_EVERY_OBSERVATIONS: usize = 32;

/// How often the worker flushes bundles to disk.
const FLUSH_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// One observed merge, copied out of the executor.
///
/// Owned bytes: the executor must not be kept waiting on the writer, so nothing here
/// borrows from it.
#[derive(Debug)]
pub struct Observation {
    pub contract: ContractInstanceId,
    pub code_hash: [u8; 32],
    pub parameters: Vec<u8>,
    pub base_state: Vec<u8>,
    pub incoming_state: Option<Vec<u8>>,
    pub delta: Option<Vec<u8>>,
    pub result_state: Vec<u8>,
    /// State of other contracts this merge referenced.
    ///
    /// A contract whose `validate_state` needs another contract's state cannot be
    /// checked at all without it: the verifier reports
    /// [`Inconclusive::RelatedRequired`](super::property::Inconclusive) and reaches
    /// no verdict. That is honest but useless, and it applies to a whole class of
    /// contract rather than to an unlucky one. The states arrive alongside the
    /// update the contract is being asked to apply, so recording them costs a copy
    /// and no lookup.
    pub related: Vec<(ContractInstanceId, Vec<u8>)>,
}

impl Observation {
    /// Bytes this observation owns, for the queue's byte budget.
    fn queued_bytes(&self) -> usize {
        self.parameters.len()
            + self.base_state.len()
            + self.incoming_state.as_ref().map_or(0, Vec::len)
            + self.delta.as_ref().map_or(0, Vec::len)
            + self.result_state.len()
            + self
                .related
                .iter()
                .map(|(_, state)| state.len())
                .sum::<usize>()
    }
}

/// What the executor hands to the capture writer.
///
/// Two kinds, because related-contract state reaches the executor by two unrelated
/// routes and only one of them travels with a transition.
///
/// A contract that needs related state to UPDATE gets it pushed into `updates` as
/// `UpdateData::RelatedState` by the executor's retry loop, so it arrives inside the
/// transition itself. A contract that needs related state only to VALIDATE never
/// produces that: the state is resolved separately in
/// `fetch_related_for_validation_network`, after the transition has already been
/// observed. Capturing only the first route left the second class permanently
/// unjudgeable - every replayed case dead-ends at `Inconclusive::RelatedRequired`,
/// which reads exactly like a clean result (#5376).
#[derive(Debug)]
pub(crate) enum CaptureMsg {
    /// A `base + update -> result` step, the material a replay actually checks.
    Transition(Box<Observation>),
    /// Related-contract state resolved while VALIDATING a contract, carried on its
    /// own because it arrives after the transition and belongs to no single update.
    ///
    /// Merged into a contract that is ALREADY tracked; it never creates a new entry.
    /// Related state without any states of its own is not a corpus, and admitting it
    /// would spend a tracking slot on something no case can be built from.
    Related {
        contract: ContractInstanceId,
        related: Vec<(ContractInstanceId, Vec<u8>)>,
    },
}

impl CaptureMsg {
    fn queued_bytes(&self) -> usize {
        match self {
            CaptureMsg::Transition(observation) => observation.queued_bytes(),
            CaptureMsg::Related { related, .. } => {
                related.iter().map(|(_, state)| state.len()).sum()
            }
        }
    }
}

/// The executor's end of the capture path.
///
/// Cloning is cheap; the executor holds one and does nothing else with it.
#[derive(Clone)]
pub struct CaptureHandle {
    tx: mpsc::Sender<CaptureMsg>,
    dropped: Arc<AtomicU64>,
    /// Bytes admitted to the queue and not yet sampled.
    ///
    /// Approximate under concurrency: two threads can both observe room and both
    /// admit, so the bound can be overshot by one observation per racing thread.
    /// That is deliberate — the alternative is a lock on the merge path, and the
    /// overshoot is bounded and small, which is all this needs to be.
    queued_bytes: Arc<AtomicUsize>,
}

impl CaptureHandle {
    /// Offer an observation, building it only if there is somewhere to put it.
    ///
    /// The closure is what copies the states out of the executor, and it runs only
    /// after a queue slot is secured. That ordering is the point: an `Observation`
    /// owns full copies of the base, incoming and result states, so on a contract
    /// with 356 KB states it costs about a megabyte of allocate-and-copy to build.
    /// Building it first and then discovering the queue is full would make the DROP
    /// path the most expensive path — precisely under the load that causes drops,
    /// and on the merge path, which is the hottest path a contract touches.
    ///
    /// Never blocks, never fails visibly.
    /// Record related-contract state resolved during validation.
    ///
    /// Same discipline as [`observe_with`](Self::observe_with): refuse on bytes before
    /// copying anything, `try_reserve` rather than await, drop and count rather than
    /// block. Validation-time related state can be another contract's whole state, so
    /// the measure-first ordering matters at least as much here.
    ///
    /// Only reached when a contract actually returns `RequestRelated` from
    /// `validate_state`, which is rare - so this costs nothing on the ordinary path.
    pub fn observe_related_with(
        &self,
        contract: ContractInstanceId,
        size_hint: usize,
        build: impl FnOnce() -> Vec<(ContractInstanceId, Vec<u8>)>,
    ) {
        let queued = self.queued_bytes.load(Ordering::Relaxed);
        if size_hint > MAX_QUEUED_BYTES || queued.saturating_add(size_hint) > MAX_QUEUED_BYTES {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        match self.tx.try_reserve() {
            Ok(permit) => {
                let related = build();
                let msg = CaptureMsg::Related { contract, related };
                self.queued_bytes
                    .fetch_add(msg.queued_bytes(), Ordering::Relaxed);
                permit.send(msg);
            }
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn observe_with(&self, size_hint: usize, build: impl FnOnce() -> Observation) {
        // Refuse on bytes BEFORE reserving or copying. `size_hint` is computed from
        // the executor's own slices, so this decision costs no allocation at all.
        // A single observation larger than the whole budget can never be admitted;
        // saying so here keeps one huge contract from starving every other.
        let queued = self.queued_bytes.load(Ordering::Relaxed);
        if size_hint > MAX_QUEUED_BYTES || queued.saturating_add(size_hint) > MAX_QUEUED_BYTES {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        match self.tx.try_reserve() {
            Ok(permit) => {
                let msg = CaptureMsg::Transition(Box::new(build()));
                // Charge what was actually built, not the estimate.
                self.queued_bytes
                    .fetch_add(msg.queued_bytes(), Ordering::Relaxed);
                permit.send(msg);
            }
            Err(_) => {
                // Queue full or writer gone. Count it and carry on without paying
                // for the copies: a stalled capture must never become a stalled
                // merge, and it should not tax one either.
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Offer an already-built observation. Never blocks, never fails visibly.
    ///
    /// Prefer [`observe_with`](Self::observe_with) from the merge path, where the
    /// copies are worth avoiding when the queue is full.
    pub fn observe(&self, observation: Observation) {
        let msg = CaptureMsg::Transition(Box::new(observation));
        let bytes = msg.queued_bytes();
        match self.tx.try_send(msg) {
            Ok(()) => {
                self.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Where this node keeps contract WASM, for shadow-mode probing.
///
/// Set by the executor, which is the component that knows it, and read by the capture
/// task, which needs it but is started from a different place at a different time.
/// A `OnceLock` rather than a constructor argument so neither has to care which of
/// them runs first: the capture task reads it on every probe tick, so a store
/// registered after capture started is picked up on the next tick rather than lost.
///
/// Absent means shadow mode can select focus contracts but cannot execute them, which
/// is reported as `skipped_no_code` rather than passing for a clean result.
///
/// # Limitation: one store per PROCESS, not per node
///
/// A production node is one process with one `Config`, so first-caller-wins is exact.
/// The DST/SimNetwork harness is not: it runs many simulated peers in one process, and
/// if shadow mode were ever exercised across more than one of them, every peer after
/// the first would silently probe against the first peer's contract directory, find
/// nothing, and report `skipped_no_code` — a clean-looking result that means nothing,
/// which is precisely the failure class this module's counters exist to expose. That
/// is not reachable today (registration is skipped entirely unless capture is on, and
/// capture is a single-peer diagnostic opt-in), but a multi-peer simulation of shadow
/// mode would need this keyed by node identity rather than held as a bare global.
static CONTRACT_STORE: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

/// Tell the conformance machinery where contract WASM lives. Idempotent; the first
/// caller wins, and later callers with a different path are ignored rather than
/// racing.
pub fn set_contract_store(path: PathBuf) {
    // Nothing reads this unless capture is enabled, and registering regardless has a
    // cost that is not obvious: `crates/core` runs many nodes in ONE process (the
    // SimNetwork/DST harness) and builds many executors over different temp dirs in a
    // single `cargo test` binary. First-caller-wins would then warn on essentially
    // every executor after the first, turning a warning meant to catch a real
    // production misconfiguration into routine noise in exactly the runner where
    // cross-test interference is visible at all (see `.claude/rules/testing.md`).
    if global().is_none() {
        return;
    }
    if let Err(rejected) = CONTRACT_STORE.set(path) {
        // First caller wins. Two callers agreeing is the normal case (several
        // executors, one Config) and is silent; two callers DISAGREEING would send
        // every probe to look for WASM in a directory the node does not write to, and
        // the only symptom would be a rising `skipped_no_code` that reads as "we host
        // nothing interesting".
        if CONTRACT_STORE.get() != Some(&rejected) {
            tracing::warn!(
                registered = %CONTRACT_STORE.get().map(|p| p.display().to_string()).unwrap_or_default(),
                rejected = %rejected.display(),
                "conflicting contract-store paths registered for conformance; probes \
                 will look in the first one"
            );
        }
    }
}

pub(crate) fn contract_store() -> Option<&'static PathBuf> {
    CONTRACT_STORE.get()
}

/// How the peer enumerates the contracts it currently hosts.
///
/// The RFC selects focus contracts from *what the peer hosts*, not from what the
/// sampler happens to have collected. Those are different sets, and the difference is
/// not cosmetic: a sampler-sourced candidate pool can only ever offer contracts that
/// already arrived through it, so once it reached its tracking cap the peer became
/// permanently unable to focus on anything new (#5366 - observed live as
/// `tracking_at_cap=true` on every tick for hours). Sourcing candidates from the
/// hosting cache makes the horizon the peer's actual hosted set, and makes sampling a
/// consequence of focus rather than its precondition.
///
/// Registered by the ring, which owns the hosting cache; read by the capture task,
/// which is started elsewhere. The closure holds a `Weak`, so registering it never
/// keeps a ring alive past node teardown; a dead ring reads as "no candidates", which
/// surfaces as `focused=0` rather than as a stale set.
///
/// Absent means no source was registered, which is reported per tick rather than
/// silently falling back - see [`super::shadow::CandidateSource`].
type HostedContractsFn = Box<dyn Fn() -> Vec<ContractInstanceId> + Send + Sync>;
static HOSTED_CONTRACTS: std::sync::OnceLock<HostedContractsFn> = std::sync::OnceLock::new();

/// Tell the conformance machinery how to list hosted contracts. First caller wins.
pub fn set_hosted_contracts_source(source: HostedContractsFn) {
    // Deliberately NOT gated on `global()`, unlike `set_contract_store`. That gate
    // exists there to keep a conflict WARNING quiet in the many-nodes-in-one-process
    // test harness; this function logs conflicts at debug, so it buys nothing - and it
    // costs correctness, because `capture::start` is public: an embedder starting
    // capture explicitly rather than from the environment would have registration
    // refused, leaving focus permanently empty and every observation discarded.
    // Registering when nothing reads it is free.
    if HOSTED_CONTRACTS.set(source).is_err() {
        // Unlike the contract store there is nothing useful to compare - two closures
        // are not comparable - so this cannot distinguish "same ring twice" from "two
        // different rings". It is logged at debug because in the one process where it
        // can happen (the simulation harness) it is expected, and in production a
        // second ring in one process would have larger problems than this.
        tracing::debug!("hosted-contract source already registered for conformance");
    }
}

pub(crate) fn hosted_contracts() -> Option<Vec<ContractInstanceId>> {
    HOSTED_CONTRACTS.get().map(|source| source())
}

static CAPTURE: std::sync::OnceLock<Option<CaptureHandle>> = std::sync::OnceLock::new();

/// The process-wide capture handle, or `None` when capture is off.
///
/// A global rather than a field on `Executor` deliberately: this is a diagnostic
/// path that should not appear in the signature of every executor constructor, and
/// after initialization reading it is a single atomic load, which is what the merge
/// path can afford. Initialized once, from the environment, and never mutated.
pub fn global() -> Option<&'static CaptureHandle> {
    CAPTURE.get_or_init(start_from_env).as_ref()
}

/// Start capture if the environment asks for it.
///
/// Returns `None` when the variable is unset, which is the normal case and the
/// default for every node that has not been deliberately configured otherwise.
pub fn start_from_env() -> Option<CaptureHandle> {
    let dir = capture_dir_from(std::env::var(CAPTURE_DIR_ENV).ok().as_deref())?;
    match start(dir) {
        Ok(handle) => Some(handle),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "conformance capture requested but could not start; continuing without it"
            );
            None
        }
    }
}

/// Decide where to capture, from the raw environment value.
///
/// Split out so the decision is testable without mutating process-global
/// environment state. `set_var` races any concurrent `getenv` anywhere in the
/// process, and sibling tests in this very module call `TempDir::new()`, which
/// reads `TMPDIR` — the process-global interference class `.claude/rules/testing.md`
/// documents, and one that per-process test isolation hides rather than prevents.
fn capture_dir_from(raw: Option<&str>) -> Option<PathBuf> {
    let raw = raw?;
    if raw.trim().is_empty() {
        return None;
    }
    Some(PathBuf::from(raw))
}

/// Start the capture writer against an explicit directory.
pub fn start(dir: PathBuf) -> std::io::Result<CaptureHandle> {
    // The writer is a tokio task, so there has to be a runtime to spawn it on.
    // Refusing here rather than panicking keeps a capture misconfiguration from
    // taking down a node that would otherwise run fine without capture.
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(std::io::Error::other(
            "conformance capture must be started from within a tokio runtime",
        ));
    }
    std::fs::create_dir_all(&dir)?;
    let (tx, rx) = mpsc::channel(OBSERVATION_QUEUE);
    let dropped = Arc::new(AtomicU64::new(0));
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let handle = CaptureHandle {
        tx,
        dropped: dropped.clone(),
        queued_bytes: queued_bytes.clone(),
    };

    tracing::info!(
        directory = %dir.display(),
        "conformance capture enabled: recording contract merges for offline replay"
    );
    tokio::spawn(run_writer(dir, rx, dropped, queued_bytes));
    Ok(handle)
}

async fn run_writer(
    dir: PathBuf,
    mut rx: mpsc::Receiver<CaptureMsg>,
    dropped: Arc<AtomicU64>,
    queued_bytes: Arc<AtomicUsize>,
) {
    // Resume from what is already on disk.
    //
    // Without this a restart silently DESTROYS the corpus: the worker starts with
    // empty samplers and the first flush overwrites each bundle with whatever few
    // states it has seen since boot. A capture is only interesting because it
    // accumulates diversity over hours, so losing it on every restart would make a
    // long collection worth roughly as much as a short one — and it would look
    // fine, because the file is still there and still recent.
    let mut samplers = reload(&dir);
    if !samplers.is_empty() {
        tracing::info!(
            contracts = samplers.len(),
            "conformance capture resumed from existing bundles"
        );
    }
    let mut since_flush = 0usize;
    // Shadow mode rides the capture task: it already owns the samples, and it is
    // already off the merge path, which is the property that matters most. The
    // mode is `default()` — Shadow — and there is deliberately no way to reach
    // `Enforce` from here.
    let mut shadow = crate::conformance::shadow::ShadowRunner::new(
        &dir,
        crate::conformance::policy::EnforcementMode::default(),
    );
    let mut probe = tokio::time::interval(crate::conformance::shadow::PROBE_INTERVAL);
    // At most one probe at a time, held here so the select loop can decline to start
    // another while one is running.
    //
    // Deliberately not joined or aborted when the channel closes and this task
    // returns. A detached probe finishes on its own and drops its scratch directory,
    // and `panic = "abort"` is off so it unwinds and cleans up even if it panics. The
    // one case that does leak is the node's real shutdown path, which calls
    // `std::process::exit` and so runs no destructors: a probe in flight at that exact
    // moment leaves one temp directory behind, with roughly a probe-duration /
    // PROBE_INTERVAL chance per restart. Left as-is rather than blocking shutdown on
    // best-effort diagnostic work, which is the wrong trade in the other direction.
    let mut in_flight: Option<
        tokio::task::JoinHandle<(
            crate::conformance::shadow::ShadowReport,
            Vec<crate::conformance::shadow::Finding>,
        )>,
    > = None;
    // The first tick of a tokio interval fires immediately. Probing a sampler that
    // has just been reloaded and holds nothing useful wastes a probe and, worse,
    // reports a clean run for contracts nobody looked at yet.
    probe.reset();
    // Same as `flush` below, and it matters MORE here. The probe arm carries an
    // `if in_flight.is_none()` guard, and a false-guarded select arm is not polled at
    // all, so a probe that outran its interval would leave ticks queued and, on the
    // default Burst behaviour, fire them back to back the moment the guard cleared.
    // Delay makes a missed tick mean "next one is a full interval from now", which is
    // what a best-effort background job should do.
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut flush = tokio::time::interval(FLUSH_EVERY);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Focus is computed once up front, not left empty until the first probe tick.
    // Under focus-scoped sampling an empty focus set records NOTHING, so deferring
    // this would throw away every observation in the first probe interval - and the
    // interval is fifteen minutes, which on a restarting peer is a real hole in the
    // corpus rather than a rounding error.
    //
    // Computing it here is necessary but NOT sufficient, and the reason is worth
    // stating because it is the opposite of what it looks like: this task is spawned
    // BY `global()`, which `set_hosted_contracts_source` calls on its own first line
    // to decide whether to register at all. So the writer starts one line before the
    // hosted source is installed, and `set_contract_store` triggers the same thing
    // earlier still. The startup computation therefore usually runs with no hosted
    // source, falls back to an empty sampler, and installs an empty focus - exactly
    // the hole it was added to close. The warm-up ticker below re-runs it until the
    // source appears, then stops.
    // Re-runs focus until the hosted source shows up, then never fires again. Cheap
    // (one `OnceLock` read per tick, and the arm is disabled once satisfied) and
    // self-terminating, rather than re-deriving focus on every observation - which
    // would take the ring's hosting-cache read lock on the merge path's queue drain.
    let mut warmup = tokio::time::interval(HOSTED_SOURCE_WARMUP);
    warmup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Seeded from the real draw below, never left at `Default`. The first version left
    // it defaulted, and because `CandidateSource` then defaulted to `Hosted` the
    // warm-up guard read "already on the hosted set" before any draw had happened - so
    // the retry arm was never polled once and the startup gap it exists to close stayed
    // wide open. The default is now the pessimistic variant too, but seeding this
    // correctly is the fix; the default is only the backstop.
    let mut last_focus;
    let mut warmup_attempts = 0u32;
    // Focused contracts with no samples yet, carried across the probe so the finished
    // tick can report them rather than silently omitting them from `focused`.
    let mut awaiting_samples = 0usize;
    let wide = wide_capture_requested();
    let startup_focus = shadow.focus(
        hosted_contracts(),
        &samplers.keys().copied().collect::<Vec<_>>(),
    );
    let mut scope = if wide {
        SamplingScope::Wide
    } else {
        SamplingScope::Focused(startup_focus.selected.iter().copied().collect())
    };
    last_focus = startup_focus;

    loop {
        tokio::select! {
            received = rx.recv() => {
                let Some(msg) = received else { break };
                // Release the byte credit as the message leaves the queue, so the
                // budget measures what is actually in flight rather than everything
                // ever admitted.
                queued_bytes.fetch_sub(msg.queued_bytes(), Ordering::Relaxed);
                let observation = match msg {
                    CaptureMsg::Transition(observation) => *observation,
                    CaptureMsg::Related { contract, related } => {
                        record_related(&mut samplers, &scope, contract, &related);
                        since_flush += 1;
                        if since_flush >= FLUSH_EVERY_OBSERVATIONS {
                            write_all(&dir, &samplers, dropped.load(Ordering::Relaxed)).await;
                            since_flush = 0;
                        }
                        continue;
                    }
                };
                if let Some(evicted) = record(&mut samplers, &scope, observation) {
                    // Dropping the entry alone orphans the file: eviction fires on
                    // every rotation once the map is full, so the directory would
                    // grow without bound and `reload` could resurrect a long-evicted
                    // contract ahead of the current focus set. Best-effort - a failed
                    // unlink must never disturb capture, let alone the merge path.
                    let path = bundle_path(&dir, &evicted);
                    if let Err(err) = tokio::fs::remove_file(&path).await {
                        if err.kind() != std::io::ErrorKind::NotFound {
                            tracing::debug!(
                                path = %path.display(),
                                error = %err,
                                "could not remove an evicted conformance bundle"
                            );
                        }
                    }
                }
                since_flush += 1;
                if since_flush >= FLUSH_EVERY_OBSERVATIONS {
                    write_all(&dir, &samplers, dropped.load(Ordering::Relaxed)).await;
                    since_flush = 0;
                }
            }
            // Disabled the moment focus has actually been drawn from the hosted set.
            // `Wide` never consults focus, so it never needs this either.
            _ = warmup.tick(),
                if crate::conformance::shadow::needs_hosted_warmup(
                    wide, &last_focus, warmup_attempts, HOSTED_SOURCE_WARMUP_ATTEMPTS) => {
                warmup_attempts += 1;
                let focus = shadow.focus(
                    hosted_contracts(),
                    &samplers.keys().copied().collect::<Vec<_>>(),
                );
                if focus.source == crate::conformance::shadow::CandidateSource::Hosted {
                    tracing::debug!(
                        candidates = focus.candidates,
                        focused = focus.selected.len(),
                        "conformance capture picked up the hosted-contract source"
                    );
                }
                scope = SamplingScope::Focused(focus.selected.iter().copied().collect());
                last_focus = focus;
            }
            _ = flush.tick() => {
                write_all(&dir, &samplers, dropped.load(Ordering::Relaxed)).await;
                since_flush = 0;
            }
            // Only start a probe when none is in flight. A probe that overran its
            // interval must not have a second one stacked on top of it: that is how a
            // slow contract turns a bounded background job into unbounded concurrent
            // WASM execution.
            _ = probe.tick(), if in_flight.is_none() => {
                // Selection borrows the samplers and is cheap; probing owns its copies
                // and is not. Only the cheap half runs here, so the queue keeps
                // draining while WASM executes on another task.
                shadow.advance();
                let focus = shadow.focus(
                    hosted_contracts(),
                    &samplers.keys().copied().collect::<Vec<_>>(),
                );
                // Recomputed every tick so a rotation, or a change in what the peer
                // hosts, takes effect on sampling immediately rather than at the next
                // restart. Wide mode ignores focus by definition.
                if !matches!(scope, SamplingScope::Wide) {
                    scope = SamplingScope::Focused(focus.selected.iter().copied().collect());
                }
                last_focus = focus.clone();
                let (work, awaiting) = shadow.select(&focus, &samplers);
                awaiting_samples = awaiting;
                if work.is_empty() {
                    // Still reported. A tick that selected nothing and a tick that
                    // found nothing are the same shape in a findings-only log, and
                    // this phase exists to tell those apart — "no violations" from a
                    // peer that never had a contract to look at is not evidence about
                    // any contract.
                    tracing::info!(
                        epoch = shadow.epoch(),
                        tracked = samplers.len(),
                        candidates = focus.candidates,
                        candidate_source = focus.source.as_str(),
                        focused = focus.selected.len(),
                        scope = scope.as_str(),
                        "conformance shadow tick selected nothing to probe"
                    );
                } else {
                    let store = contract_store().cloned();
                    let mode = shadow.mode();
                    // An ordinary task: `probe` puts its own WASM execution on a
                    // blocking thread internally (see `shadow::probe_one`). Doing the
                    // hop there rather than here keeps the async work — building the
                    // oracle — on the runtime, and avoids driving a future with
                    // `Handle::block_on` from a blocking thread, which is subtle on a
                    // current-thread runtime whose driver lives elsewhere.
                    in_flight = Some(tokio::spawn(crate::conformance::shadow::probe(
                        work, store, mode,
                    )));
                }
            }
            Some(finished) = async {
                match in_flight.as_mut() {
                    Some(handle) => Some(handle.await),
                    None => None,
                }
            }, if in_flight.is_some() => {
                in_flight = None;
                let (mut report, findings) = match finished {
                    Ok(result) => result,
                    Err(err) => {
                        // A panicking probe must not take the capture task with it,
                        // and must not pass silently either: capture would carry on
                        // looking healthy while nothing was ever checked.
                        tracing::warn!(error = %err, "conformance shadow probe failed");
                        continue;
                    }
                };
                // Focus picked these; they simply had nothing to check yet. Counted
                // in both, so `focused` is the size of the focus set rather than the
                // size of the subset that happened to have samples.
                report.focused += awaiting_samples;
                report.skipped_no_samples += awaiting_samples;
                shadow.record(&findings);
                // Reported even when nothing was checked. A shadow period that finds
                // nothing and a shadow period that never ran look identical in a
                // findings-only log, and telling them apart is most of what this
                // phase is for.
                tracing::info!(
                    epoch = shadow.epoch(),
                    // How much sample storage is in use. This used to describe focus
                    // REACH, because focus selected out of this very map; it no longer
                    // does (see `candidates` below), and the map now evicts. Kept as a
                    // storage metric: at the cap, every new focus pick costs an older
                    // contract's accumulated sample.
                    tracked = samplers.len(),
                    tracking_at_cap = samplers.len() >= MAX_TRACKED_CONTRACTS,
                    // Where focus could reach this tick. `candidates` is the size of
                    // the pool focus chose from; when `candidate_source` is `hosted`
                    // that is the peer's hosted set, which is the whole point of the
                    // #5366 fix. A tick reading `candidate_source=sampler` means no
                    // hosted source was registered and reach has silently narrowed
                    // back to whatever the sampler already held.
                    candidates = last_focus.candidates,
                    candidate_source = last_focus.source.as_str(),
                    scope = scope.as_str(),
                    focused = report.focused,
                    probed = report.probed,
                    cases = report.cases,
                    inconclusive = report.inconclusive,
                    would_remove = report.would_remove,
                    reported = report.reported,
                    skipped_no_code = report.skipped_no_code,
                    skipped_no_samples = report.skipped_no_samples,
                    timed_out = report.timed_out,
                    "conformance shadow tick"
                );
            }
        }
    }

    // Channel closed: the node is going away. Write what we have.
    write_all(&dir, &samplers, dropped.load(Ordering::Relaxed)).await;
}

pub(crate) struct TrackedContract {
    sampler: ContractSampler,
    code_hash: [u8; 32],
    parameters: Vec<u8>,
    /// Related-contract state this contract offered that capture would not keep.
    ///
    /// Load-bearing for reading a corpus honestly: a bundle whose related map is
    /// empty because nothing was ever needed and one whose related state was refused
    /// look identical, and only the second explains why a replay can reach no verdict.
    pub(crate) refused_related: RelatedRefusals,
    /// Observations the sampler refused because a state exceeded its per-state
    /// ceiling.
    ///
    /// Kept because the alternative is a silent exclusion. A contract whose states
    /// are all oversized produces a bundle holding nothing, and replaying it says
    /// only "the corpus is empty" — which reads as "this contract never merged
    /// anything" when the truth is the opposite: it merged constantly and every
    /// observation was refused. The count comes from the filter that does the
    /// refusing rather than being inferred later from an empty result, because an
    /// empty corpus has several possible causes and they need telling apart.
    refused_too_large: u64,
    /// Most recent state seen for each related contract this one referenced.
    ///
    /// Keyed by instance, so a contract that references the same related contract
    /// repeatedly keeps one entry rather than a history: the verifier needs one
    /// state it can execute against, not every state that ever passed through.
    ///
    /// # Why latest-wins is sound, and what it costs
    ///
    /// `to_corpus` attaches whatever related state is held here to EVERY case,
    /// including transitions sampled hours earlier when the related contract held
    /// something else. That looks like the temporal-mismatch shape of the delta false
    /// positive this work memorialises, where the fix was provenance. It is not, and
    /// the difference is worth stating so nobody re-derives the wrong conclusion:
    ///
    /// `verify_case` validates EVERY input state against `case.related` before any
    /// property runs, and that is the SAME related state the property then uses. So
    /// the only way to reach a violation is `validate(A, R)` and `validate(B, R)` both
    /// Valid while `validate(merge(A, B), R)` is Invalid — the contract emitting a
    /// state it rejects, under a related state it accepted both inputs against. `R`
    /// was observed on this node, so it is reachable, and related contracts propagate
    /// independently of this one, so a peer holding `A` can be seeing `R` when `B`
    /// arrives. A contract that couples its own validity to the related state has that
    /// coupling enforced in the input check, which degrades to
    /// `Inconclusive::InputNotValid` rather than accusing. Deltas had no such gate,
    /// which is precisely why they needed provenance and this does not.
    ///
    /// What latest-wins does cost is COVERAGE, not soundness. When the one state held
    /// here does not validate the inputs, the case reaches no verdict at all. If
    /// shadow-mode data shows related-dependent contracts sitting at Inconclusive in
    /// bulk, the cheap answer is to hold the last few distinct related states and let
    /// the verifier try each, rather than a per-transition snapshot.
    related: HashMap<ContractInstanceId, Vec<u8>>,
}

/// Rebuild samplers from the bundles already in the capture directory.
///
/// Replays each stored transition back through the sampler rather than trying to
/// restore its internal strata: the bundle is the portable format and does not
/// carry the sampler's private structure, and re-observing is both simpler and
/// self-correcting — anything the current configuration would no longer admit is
/// simply not re-admitted.
fn reload(dir: &Path) -> HashMap<ContractInstanceId, TrackedContract> {
    let mut samplers = HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return samplers;
    };

    // Sorted, because this loop stops at MAX_TRACKED_CONTRACTS and `read_dir` order is
    // undefined. Unsorted, WHICH contracts survive a restart is decided by whatever the
    // filesystem happens to enumerate first - and with eviction now deleting bundles,
    // a directory can legitimately hold entries in any order. Sorting does not make the
    // choice smart (nothing here knows the current focus set), but it makes it
    // reproducible, so a peer that restarts twice reloads the same corpus twice.
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("bundle") {
            continue;
        }
        let bundle = match ReplayBundle::read_from(&path) {
            Ok(bundle) => bundle,
            Err(err) => {
                // A corrupt or half-written bundle must not stop the node from
                // capturing; it just means that contract starts over.
                tracing::warn!(error = %err, path = %path.display(), "skipping unreadable capture bundle");
                continue;
            }
        };
        let (Some(instance), Some(code_hash)) = (bundle.instance, bundle.code_hash) else {
            continue;
        };
        if samplers.len() >= MAX_TRACKED_CONTRACTS {
            break;
        }

        let mut sampler = ContractSampler::new(sampler_config());
        for state in &bundle.states {
            sampler.observe_state(state);
        }
        for transition in &bundle.transitions {
            sampler.observe_transition(
                &transition.base_state,
                transition.incoming_state.as_deref(),
                transition.delta.as_deref(),
                transition.summary.as_deref(),
                &transition.result_state,
            );
        }
        // Read before the struct takes ownership of `sampler`.
        let (max_related_state, max_related_total) = related_budgets(sampler.config());
        let mut reload_refusals = RelatedRefusals::default();
        samplers.insert(
            instance,
            TrackedContract {
                sampler,
                code_hash,
                related: {
                    let mut restored = HashMap::new();
                    // Re-admitted under THIS process's budgets, not the ones that
                    // wrote the bundle: an operator who raised the budget to make a
                    // contract checkable must not have a corpus written under the old
                    // one silently re-truncated to it.
                    admit_related(
                        &mut restored,
                        &bundle.related,
                        &mut reload_refusals,
                        max_related_state,
                        max_related_total,
                    );
                    restored
                },
                parameters: bundle.parameters,
                // Not persisted in the bundle: this counts what THIS process refused,
                // and a reloaded corpus has none of its own yet.
                refused_too_large: 0,
                // Related refusals ARE carried, because reload genuinely makes them.
                //
                // An earlier version discarded them into a throwaway local, with a
                // comment claiming "a reloaded corpus has no refusals of its own yet".
                // That was false on its own terms: reload re-admits under THIS
                // process's budgets, so a run that captured under a raised budget and
                // then restarted under the default has its related state trimmed right
                // here - silently, with the next flush writing "0 refused" over the
                // evidence. That is the exact invariant this type exists to hold.
                refused_related: reload_refusals,
            },
        );
    }
    samplers
}

/// Which contracts the sampler will keep samples for.
///
/// The RFC samples the *focus* contracts: "while a contract is in the focus set, the
/// peer records a bounded, diverse sample of the states and update context it
/// naturally observes". Sampling everything and then choosing focus from what was
/// collected is the inversion that produced #5366, and it also makes an ordinary
/// peer's corpus grow with its traffic rather than with its focus.
#[derive(Debug, Clone)]
pub(crate) enum SamplingScope {
    /// Record only for the contracts currently in focus. The production shape: the
    /// corpus stays proportional to the focus set, not to what the peer happens to
    /// route.
    Focused(std::collections::HashSet<ContractInstanceId>),
    /// Record for everything observed, up to the tracking cap.
    ///
    /// Developer corpus-building only, behind [`CAPTURE_WIDE_ENV`]. This is how the
    /// corpora that found every violation so far were gathered - offline replay needs
    /// many contracts from one peer, which is precisely what a production peer should
    /// not be doing.
    Wide,
}

impl SamplingScope {
    /// Whether this scope admits `contract` as a newly tracked contract.
    fn admits(&self, contract: &ContractInstanceId) -> bool {
        match self {
            SamplingScope::Focused(focus) => focus.contains(contract),
            SamplingScope::Wide => true,
        }
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            SamplingScope::Focused(_) => "focused",
            SamplingScope::Wide => "wide",
        }
    }
}

/// Set to `1`/`true` to sample every contract observed rather than only the focus set.
///
/// Developer-only. It exists because offline replay - which is what actually found
/// the deployed violations - needs a broad corpus from a single peer, and a peer
/// obeying the RFC's focus-scoped sampling will never build one.
pub const CAPTURE_WIDE_ENV: &str = "FREENET_CONFORMANCE_CAPTURE_WIDE";

fn wide_capture_requested() -> bool {
    matches!(
        std::env::var(CAPTURE_WIDE_ENV).ok().as_deref(),
        Some("1") | Some("true")
    )
}

/// Where a contract's persisted bundle lives. One definition, so an eviction cannot
/// delete a path the writer never wrote.
pub(crate) fn bundle_path(dir: &Path, instance: &ContractInstanceId) -> PathBuf {
    dir.join(format!("{instance}.bundle"))
}

/// Fold validation-resolved related state into a contract already being tracked.
///
/// Deliberately does NOT create a tracked entry. Related state with no states of its
/// own cannot produce a single case, so admitting it would spend one of
/// `MAX_TRACKED_CONTRACTS` slots on something no replay can use — and on a peer at its
/// cap that costs a contract that CAN be judged.
///
/// Scope is honoured the same way `record` honours it: an out-of-focus contract is
/// retained but no longer collects, and that has to include this route or "sampling
/// follows focus" would be true of transitions and quietly false of related state.
pub(crate) fn record_related(
    samplers: &mut HashMap<ContractInstanceId, TrackedContract>,
    scope: &SamplingScope,
    contract: ContractInstanceId,
    related: &[(ContractInstanceId, Vec<u8>)],
) {
    if !scope.admits(&contract) {
        return;
    }
    let Some(tracked) = samplers.get_mut(&contract) else {
        return;
    };
    let (max_state, max_total) = related_budgets(tracked.sampler.config());
    admit_related(
        &mut tracked.related,
        related,
        &mut tracked.refused_related,
        max_state,
        max_total,
    );
}

/// Fold one observation into the sampler map.
///
/// Returns the contract evicted to make room, if any. The caller must delete that
/// contract's persisted bundle: dropping it from this map alone leaves the `.bundle`
/// file behind forever, and since eviction happens on every rotation once the map is
/// full, those orphans accumulate without bound - which also lets `reload` resurrect a
/// long-evicted contract ahead of the current focus set on the next restart.
#[must_use = "the evicted contract's bundle must be deleted, or it is orphaned on disk"]
pub(crate) fn record(
    samplers: &mut HashMap<ContractInstanceId, TrackedContract>,
    scope: &SamplingScope,
    observation: Observation,
) -> Option<ContractInstanceId> {
    let known = samplers.contains_key(&observation.contract);
    if !scope.admits(&observation.contract) {
        // Not in focus, so not COLLECTED - whether or not it is already tracked.
        //
        // The first version of this gated only new admissions (`!known && !admits`),
        // which let any contract that had ever been focused keep absorbing every
        // observation it saw, forever. With focus rotating every couple of hours and a
        // 64-entry map, steady state was 64 contracts collecting rather than the two
        // in focus - so "sampling follows focus" held for a few days after a clean
        // deploy and then quietly stopped being true. The RFC's allowance is that a
        // sample "may outlive a focus period for a BOUNDED time so returning to a
        // contract does not always start from zero"; that is about RETAINING what was
        // collected, not about continuing to collect, and there was no bound at all.
        //
        // Retention is preserved: an out-of-focus entry stays in the map, and stays on
        // disk, until eviction needs its slot. It simply stops growing.
        return None;
    }
    let mut evicted = None;
    if !known && samplers.len() >= MAX_TRACKED_CONTRACTS {
        // At the tracking cap with a contract that focus has actually selected.
        //
        // Refusing here is what caused #5366: the map fills, admission stops, and no
        // amount of rotation can ever get the current focus contract sampled - the
        // peer keeps probing whatever it captured first, forever, while reporting
        // healthy ticks. Under focus-scoped sampling that failure returns in a worse
        // form, because rotation guarantees the map fills with contracts that are no
        // longer in focus.
        //
        // So evict a contract that is NOT in focus to make room. Deterministic (lowest
        // id) rather than random so behaviour is reproducible in tests; the sample
        // being discarded belongs to a contract nothing is looking at.
        let evictable = match scope {
            SamplingScope::Focused(focus) => samplers
                .keys()
                .filter(|id| !focus.contains(id))
                .min_by(|a, b| a.as_bytes().cmp(b.as_bytes()))
                .copied(),
            // Wide mode has no focus to protect and is a developer path where the cap
            // is the intended stopping point: a corpus that silently rolled over would
            // make "what this peer saw" depend on eviction order.
            SamplingScope::Wide => None,
        };
        match evictable {
            Some(victim) => {
                samplers.remove(&victim);
                evicted = Some(victim);
            }
            None => {
                // Two very different situations reach here, and warning for both says
                // something false about one of them.
                //
                // Wide mode maps to `None` unconditionally: refuse-at-the-cap IS its
                // design, the steady state of any long developer capture run, and it
                // has no focus set to speak of. Warning there fires continuously and
                // calls the newcomer "focused", which it is not.
                //
                // Focused mode reaching `None` means every tracked contract is in
                // focus with the map full. Unreachable while MAX_FOCUS_CONTRACTS (2)
                // sits far below MAX_TRACKED_CONTRACTS (64) - but that is a documented
                // operational tunable, and raising it without revisiting the cap would
                // start discarding exactly the focus-selected observations this
                // eviction rule exists to protect. That one is worth a warning.
                if matches!(scope, SamplingScope::Focused(_)) {
                    tracing::warn!(
                        contract = %observation.contract,
                        tracked = samplers.len(),
                        "conformance capture could not make room for a focused \
                         contract: every tracked contract is in focus"
                    );
                }
                return None;
            }
        }
    }

    let tracked = samplers
        .entry(observation.contract)
        .or_insert_with(|| TrackedContract {
            sampler: ContractSampler::new(sampler_config()),
            code_hash: observation.code_hash,
            parameters: observation.parameters.clone(),
            refused_too_large: 0,
            refused_related: RelatedRefusals::default(),
            related: HashMap::new(),
        });

    // The sampler's own budgets, so raising FREENET_CONFORMANCE_CAPTURE_MAX_BYTES for
    // a diagnostic run reaches the related state too. Read into locals first: the
    // config lives behind `tracked.sampler` and `related` is a sibling field.
    let (max_related_state, max_related_total) = related_budgets(tracked.sampler.config());
    admit_related(
        &mut tracked.related,
        &observation.related,
        &mut tracked.refused_related,
        max_related_state,
        max_related_total,
    );

    let admission = tracked.sampler.observe_transition(
        &observation.base_state,
        observation.incoming_state.as_deref(),
        observation.delta.as_deref(),
        None,
        &observation.result_state,
    );
    if matches!(admission, Admission::TooLarge) {
        tracked.refused_too_large += 1;
    }

    evicted
}

/// Related state a contract offered and capture would not keep.
///
/// Counted, never silently dropped. Three `continue`s used to discard related state
/// with no record at all, which made an EMPTY related map indistinguishable from
/// "this contract never needed any" - and the difference decides whether a later
/// replay can reach a verdict. The main sampler already counts its refusals
/// (`refused_too_large`); this is the same discipline applied to the context those
/// samples need in order to mean anything.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelatedRefusals {
    /// A single related state exceeded the per-state ceiling.
    pub too_large: u64,
    /// Keeping it would have exceeded the per-contract related total.
    pub over_budget: u64,
    /// Already holding [`MAX_RELATED_CONTRACTS`] distinct related contracts.
    pub no_slot: u64,
}

impl RelatedRefusals {
    pub(crate) fn total(&self) -> u64 {
        self.too_large + self.over_budget + self.no_slot
    }
}

/// Admit related-contract states into a tracked contract, within the allowance.
///
/// Shared by `record` and `reload` on purpose. Everything else a reload restores goes
/// back through the sampler's own admission checks, so it is self-correcting: a
/// bundle written under looser limits is trimmed to what the current build allows.
/// Related state was the one field that skipped that and was collected raw, so a
/// bundle from a looser build — or an edited one, since nothing bounds the vector on
/// disk — loaded straight past today's limits. One function, called from both paths,
/// is what stops the two rules drifting again.
///
/// Both bounds refuse rather than evict: a contract referencing a hundred others must
/// not be able to churn this map, and states already retained are more useful than an
/// arbitrary newcomer.
fn admit_related(
    held: &mut HashMap<ContractInstanceId, Vec<u8>>,
    offered: &[(ContractInstanceId, Vec<u8>)],
    refused: &mut RelatedRefusals,
    max_state_bytes: usize,
    max_total_bytes: usize,
) {
    for (instance, state) in offered {
        if state.len() > max_state_bytes {
            refused.too_large += 1;
            continue;
        }
        if !held.contains_key(instance) && held.len() >= MAX_RELATED_CONTRACTS {
            refused.no_slot += 1;
            continue;
        }
        let total: usize = held.values().map(Vec::len).sum();
        // Subtracting what this entry already holds cannot underflow: `replacing` is
        // non-zero only when the instance is already a key, in which case its length
        // is part of `total`.
        let replacing = held.get(instance).map_or(0, Vec::len);
        if total - replacing + state.len() > max_total_bytes {
            refused.over_budget += 1;
            continue;
        }
        held.insert(*instance, state.clone());
    }
}

/// Build the replay bundle for one tracked contract.
///
/// Shared by the periodic flush and by shadow-mode probing, which must check exactly
/// what a later offline replay would check. Two constructions would be two chances to
/// disagree about what a corpus contains, and the disagreement would be invisible:
/// both would produce a plausible bundle, and only a finding that reproduced offline
/// but not in shadow (or the reverse) would reveal it.
pub(crate) fn bundle_for(
    instance: ContractInstanceId,
    tracked: &TrackedContract,
) -> crate::conformance::bundle::ReplayBundle {
    // No embedded code: the WASM lives in the node's contract store and would
    // multiply the size of every bundle. The code hash identifies it, and
    // `ReplayBundle::resolve_code` verifies whatever is supplied at replay time
    // against that hash — so a bundle can never be replayed against the wrong
    // contract even though it does not carry the contract.
    let mut bundle =
        tracked
            .sampler
            .to_bundle(None, Some(tracked.code_hash), tracked.parameters.clone());
    bundle.instance = Some(instance);
    // Carried so a replay can execute a contract whose validity depends on
    // another contract. `to_corpus` turns these back into `RelatedContracts`.
    // Sorted by instance, so a bundle's bytes do not depend on hash iteration
    // order. The corpus is written repeatedly and compared across runs; a file
    // that differs only by map ordering wastes everyone's time.
    let mut related: Vec<(ContractInstanceId, Vec<u8>)> = tracked
        .related
        .iter()
        .map(|(id, state)| (*id, state.clone()))
        .collect();
    related.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    bundle.related = related;
    bundle
}

/// Flush every tracked contract's bundle.
///
/// Async because a flush is real I/O: up to 64 bundles of up to the per-contract
/// budget each, which was measured at 25 MB on a live capture. Doing that with
/// `std::fs::write` parked a tokio worker for the duration, and it is exactly
/// during that stall that the observation queue fills and observations are
/// dropped — the flush was starving the thing it exists to record.
async fn write_all(
    dir: &Path,
    samplers: &HashMap<ContractInstanceId, TrackedContract>,
    dropped: u64,
) {
    for (instance, tracked) in samplers {
        let mut bundle = bundle_for(*instance, tracked);
        let refused = tracked.refused_related;
        bundle.note = Some(format!(
            "captured by freenet {} ({} observation(s) dropped node-wide){}",
            env!("CARGO_PKG_VERSION"),
            dropped,
            // Carried INTO the corpus, not just logged. A replay reads the bundle
            // long after the node's logs have rotated, and "no related state" versus
            // "related state was refused" is the difference between a contract that
            // needs none and one this corpus cannot judge.
            if refused.total() == 0 {
                String::new()
            } else {
                // Built by concatenation, not a `\`-continued literal: a wrapped
                // literal bakes its own source indentation into every bundle note a
                // reader ever sees, and rustfmt does not touch string contents.
                let mut extra = format!(
                    " (related state refused: {} too large, {} over budget, {} no slot.",
                    refused.too_large, refused.over_budget, refused.no_slot,
                );
                extra.push_str(" This corpus may be unable to reach a verdict for this contract.");
                if refused.too_large > 0 || refused.over_budget > 0 {
                    extra.push_str(&format!(" Raise {CAPTURE_MAX_BYTES_ENV} to capture it."));
                }
                if refused.no_slot > 0 {
                    // Raising the byte budget cannot help here: the slot limit is a
                    // compile-time constant, so do not send a reader after that knob.
                    extra.push_str(&format!(
                        " {} related contract(s) exceeded the {}-contract limit, which no configuration can raise.",
                        refused.no_slot, MAX_RELATED_CONTRACTS,
                    ));
                }
                extra.push(')');
                extra
            },
        ));

        // Warned as well as recorded. A contract whose related state was refused is
        // not merely under-sampled: it becomes UNJUDGEABLE, and an unjudgeable
        // contract reads exactly like a clean one. Depending on related state must
        // not be a way to escape conformance checking, so the one signal that says it
        // happened cannot be silent.
        if refused.total() > 0 {
            tracing::warn!(
                contract = %instance,
                too_large = refused.too_large,
                over_budget = refused.over_budget,
                no_slot = refused.no_slot,
                related_held = tracked.related.len(),
                // Only suggest the byte budget when the byte budget is what bound.
                // `no_slot` is the MAX_RELATED_CONTRACTS limit, a compile-time
                // constant, and sending an operator to re-run with a bigger budget
                // that cannot possibly help is worse than saying nothing.
                // Must describe BOTH causes when both fired. Collapsing to the byte
                // budget alone sends an operator to raise it, re-run, and get an
                // incomplete corpus again with no explanation - and a large, popular
                // related contract is exactly the case that trips both at once.
                remedy = match (
                    refused.too_large > 0 || refused.over_budget > 0,
                    refused.no_slot > 0,
                ) {
                    (true, true) => "raise the byte budget; some refusals are also \
                                     capped by the related-contract COUNT limit, which \
                                     is not configurable",
                    (true, false) => "raise the byte budget",
                    (false, true) => "none: the related-contract COUNT limit bound, \
                                      which is not configurable",
                    (false, false) => "none",
                },
                byte_budget_env = CAPTURE_MAX_BYTES_ENV,
                "conformance capture refused related-contract state; replays of this \
                 contract may reach no verdict"
            );
        }

        // A bundle with no states is worse than no bundle: it looks like evidence
        // and replays as "the corpus is empty", which invites the reader to
        // conclude the contract was quiet. Say what actually happened instead.
        if bundle.states.is_empty() {
            tracing::warn!(
                contract = %instance,
                refused_too_large = tracked.refused_too_large,
                "conformance capture retained nothing for this contract: its states \
                 exceed the per-state ceiling. Raise \
                 FREENET_CONFORMANCE_CAPTURE_MAX_BYTES to sample it."
            );
            continue;
        }

        let path = bundle_path(dir, instance);
        // Encode on this task (pure CPU, no syscall) and hand only the bytes to the
        // async write, so the syscalls yield rather than parking the worker.
        match bundle.encode() {
            Ok(bytes) => {
                // Same atomic replacement as `ReplayBundle::write_to`, async: write
                // beside the bundle and rename over it, so a crash mid-flush leaves
                // the previous corpus intact rather than a truncated file.
                let temporary = path.with_extension("bundle.tmp");
                let write_then_rename = async {
                    tokio::fs::write(&temporary, bytes).await?;
                    tokio::fs::rename(&temporary, &path).await
                };
                if let Err(err) = write_then_rename.await {
                    drop(tokio::fs::remove_file(&temporary).await);
                    // Capture failing to write must not escalate. Log and move on.
                    tracing::warn!(
                        error = %err,
                        path = %path.display(),
                        "could not write capture bundle"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    path = %path.display(),
                    "could not encode capture bundle"
                );
            }
        }
    }

    if dropped > 0 {
        tracing::info!(
            dropped,
            contracts = samplers.len(),
            "conformance capture flushed (dropped count is observations the writer could not keep up with)"
        );
    }
}

/// Extract the code hash a contract key was derived from.
///
/// Carried in the bundle so a replay can be checked against the contract it was
/// actually observed on, without embedding the WASM in every capture file.
pub fn code_hash_of(key: &ContractKey) -> [u8; 32] {
    let mut out = [0u8; 32];
    let bytes: &[u8] = key.code_hash().as_ref();
    let len = out.len().min(bytes.len());
    out[..len].copy_from_slice(&bytes[..len]);
    out
}

/// The executor must tell conformance where contract WASM lives.
///
/// Without that one call every shadow probe resolves no code, so the peer selects
/// focus contracts, executes nothing, and reports no violations — for the same reason
/// a peer with no contracts reports none. The counters distinguish it (`skipped_no_code`
/// rises), but nothing FAILS, so a refactor that drops the registration turns the whole
/// mechanism off while leaving every test green and the logs superficially healthy.
///
/// A source scrape rather than a behavioural test because the alternative is standing
/// up a full executor to observe a `OnceLock` being set, which would test tokio more
/// than it tests this.
/// Source pin on the executor emitting validation-resolved related state.
///
/// No unit test reaches this call site: it lives in
/// `fetch_related_for_validation_network`, which needs a runtime, a state store and a
/// contract that actually returns `RequestRelated`. So the one thing a test CAN check
/// is that the call is still there.
///
/// Worth pinning rather than trusting, because deleting it is silent in the worst way.
/// Nothing fails, no counter moves, and no warning fires — the corpus simply stops
/// carrying related state for the contracts that need it, and every replay of those
/// contracts comes back `Inconclusive`, which reads exactly like a clean result. That
/// is #5376, and it went unnoticed until a replay of the whole corpus showed 9 of 54
/// contracts reaching no verdict on any of 2,474 cases.
#[cfg(test)]
mod validation_related_capture_pin {
    /// Slice `fetch_related_for_validation_network`'s body by counting braces to its
    /// own closing one.
    ///
    /// Brace-counting rather than "up to the next `fn`": a region ended on a guessed
    /// anchor silently widens when the following item is not the shape assumed, and a
    /// widened region here would swallow unrelated executor code and pass vacuously.
    fn fetch_related_body() -> &'static str {
        let src = include_str!("../contract/executor/runtime/contract_ops.rs");
        let start = src
            .find("async fn fetch_related_for_validation_network(")
            .expect("fetch_related_for_validation_network not found in contract_ops.rs");
        let after = &src[start..];
        let open = after.find('{').expect("function has no body");
        let mut depth = 0usize;
        for (offset, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &after[..open + offset + 1];
                    }
                }
                _ => {}
            }
        }
        panic!("fetch_related_for_validation_network's body is not brace-balanced");
    }

    /// Whole-line comments stripped: the block above this call site names
    /// `observe_related_with` in prose, and a pin that its own explanation can satisfy
    /// is not a pin.
    fn code_only() -> String {
        fetch_related_body()
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn the_executor_captures_validation_resolved_related_state() {
        let body = code_only();
        assert!(
            body.contains("observe_related_with("),
            "the executor no longer hands validation-resolved related state to \
             capture, so contracts whose VALIDITY depends on another contract go back \
             to being unjudgeable — and an unjudgeable contract reads as a clean one"
        );
    }

    /// It must be emitted BEFORE the map is consumed, or there is nothing to read.
    #[test]
    fn the_capture_call_precedes_the_map_being_consumed() {
        let body = code_only();
        let call = body
            .find("observe_related_with(")
            .expect("capture call missing; the sibling pin covers that");
        let consumed = body
            .find("RelatedContracts::from(related_map)")
            .expect("related_map is no longer converted; this pin needs rewriting");
        assert!(
            call < consumed,
            "the capture call moved after `related_map` is consumed, so it can only \
             ever observe an empty map"
        );
    }
}

#[cfg(test)]
mod contract_store_registration_pin {
    /// Slice `get_runtime_stores`' body, from its signature to the next item. A
    /// missing anchor panics rather than silently widening the region to the rest of
    /// the file, which is how a source pin quietly stops testing anything.
    /// Slice `get_runtime_stores`' body by counting braces to its own closing one.
    ///
    /// The first version of this ended the region at the next `pub(crate) fn` / `fn`
    /// signature, which does not match this file: the item after `get_runtime_stores`
    /// is a plain `pub fn`. So the region ran on for ~350 lines, through several
    /// unrelated helpers and into `#[cfg(test)] mod tests`. It was not vacuous — the
    /// symbol occurs once in the file — but it was one added mention away from being
    /// so, in a comment or an unrelated helper, and the doc claimed a bound it did not
    /// have. Brace counting has no such dependency on what happens to come next.
    fn get_runtime_stores_body() -> &'static str {
        let src = include_str!("../contract/executor.rs");
        let start = src
            .find("    pub(crate) fn get_runtime_stores(")
            .expect("get_runtime_stores not found in executor.rs");
        let after = &src[start..];
        let open = after.find('{').expect("get_runtime_stores has no body");
        let mut depth = 0usize;
        for (offset, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &after[..open + offset + 1];
                    }
                }
                _ => {}
            }
        }
        panic!("get_runtime_stores' body is not brace-balanced");
    }

    /// Slice `Ring::new`'s body by counting braces to its own closing one.
    ///
    /// Same discipline as `get_runtime_stores_body` above and for the same reason: a
    /// region ended at "the next `fn`" silently widens when the following item is not
    /// the shape the pin assumed, and a widened region can match the pin's own
    /// assertion text and pass vacuously.
    fn ring_new_body() -> &'static str {
        let src = include_str!("../ring.rs");
        let start = src
            .find("    pub fn new<ER: NetEventRegister>(")
            .expect("Ring::new not found in ring.rs");
        let after = &src[start..];
        let open = after.find('{').expect("Ring::new has no body");
        let mut depth = 0usize;
        for (offset, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &after[..open + offset + 1];
                    }
                }
                _ => {}
            }
        }
        panic!("Ring::new's body is not brace-balanced");
    }

    /// The ring must register how to enumerate hosted contracts.
    ///
    /// Without it, focus falls back to the sampler keys and silently reinstates the
    /// #5366 horizon: the peer keeps probing, keeps logging clean ticks, and can never
    /// reach a contract it did not happen to observe early. The fallback is reported
    /// per tick (`candidate_source=sampler`), but nothing in a unit test would notice
    /// the registration going away, because no unit test builds a `Ring`.
    #[test]
    fn the_ring_registers_its_hosted_contracts_for_conformance() {
        let body = ring_new_body();
        assert!(
            body.contains("set_hosted_contracts_source("),
            "the ring no longer tells conformance how to list hosted contracts, so \
             focus selection falls back to whatever the sampler already held (#5366)"
        );
    }

    /// The registration must not hold the ring alive.
    ///
    /// The closure lives in a process-global that outlives the node. A strong `Arc`
    /// there would leak an entire ring, its caches and its background-task handles
    /// past teardown - and in the simulation harness, one per simulated peer.
    #[test]
    fn the_hosted_contracts_source_holds_only_a_weak_reference() {
        let body = ring_new_body();
        let start = body
            .find("set_hosted_contracts_source(")
            .expect("registration missing; the sibling pin covers that");
        let before = &body[..start];
        assert!(
            before.contains("Arc::downgrade(&ring)"),
            "the hosted-contract source does not downgrade the ring first, so the \
             process-global closure may be keeping the ring alive after teardown"
        );
    }

    #[test]
    fn the_executor_registers_the_contract_store_for_conformance() {
        let body = get_runtime_stores_body();
        assert!(
            body.contains("set_contract_store("),
            "the executor no longer registers its contract store with conformance, so \
             every shadow probe will fail to resolve code and the mechanism reports \
             nothing while looking healthy"
        );
    }
}

/// Source pins on the probe's wiring inside `run_writer`.
///
/// These guard two regressions that no behavioural test in this module would catch,
/// because every shadow test drives `select` / `probe` / `record` directly rather than
/// through the writer's select loop. Both regressions leave the mechanism working and
/// every test green, which is the whole reason they are pinned rather than trusted:
///
/// 1. `tokio::spawn` instead of `spawn_blocking` puts synchronous WASM execution back
///    on the async runtime. `.claude/rules/contracts.md` forbids exactly that, and on a
///    single-vCPU node — sized by `available_parallelism()`, a supported deployment —
///    there is one worker thread for it to compete with.
/// 2. Dropping the `in_flight.is_none()` guard lets probes stack, turning a bounded
///    background job into unbounded concurrent WASM execution.
///
/// Stated plainly: these are pins, not behavioural tests. They prove the call site
/// still says what it should, not that the scheduling behaves.
#[cfg(test)]
mod probe_wiring_pins {
    /// Slice `run_writer`'s body by counting braces to its own closing one, so the
    /// region cannot silently widen into whatever happens to follow it.
    fn run_writer_body() -> &'static str {
        let src = include_str!("capture.rs");
        let start = src
            .find("async fn run_writer(")
            .expect("run_writer not found");
        let after = &src[start..];
        let open = after.find('{').expect("run_writer has no body");
        let mut depth = 0usize;
        for (offset, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &after[..open + offset + 1];
                    }
                }
                _ => {}
            }
        }
        panic!("run_writer's body is not brace-balanced");
    }

    /// `run_writer_body` with whole-line `//` comments removed, so a pin asserts on
    /// CODE rather than on prose that happens to name the same thing.
    ///
    /// This is not hypothetical tidiness. Mutation-testing these pins caught exactly
    /// that: the one-probe-at-a-time pin passed with the guard deleted, because the
    /// comment a few lines above the guard quotes `in_flight.is_none()` while
    /// explaining why it exists. The pin was matching the explanation of the thing it
    /// was supposed to be guarding. `full_state_version_gate_pins::upsert_code_only`
    /// in the executor solves the same problem the same way, and its reasoning applies
    /// here too: stripping only whole-line comments can produce a false FAILURE but
    /// never a false pass, because a real call's identifier cannot sit on a line whose
    /// `trim_start()` begins with `//`.
    fn run_writer_code_only() -> String {
        run_writer_body()
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The probe's WASM must run on a blocking thread. The hop lives inside
    /// `shadow::probe_one`, not here, so this pin reads that module rather than
    /// `run_writer` — a pin that checked the wrong file would be worse than none.
    #[test]
    fn the_probe_runs_off_the_async_runtime() {
        let src = include_str!("shadow.rs");
        let body = src
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            body.contains("spawn_blocking("),
            "the conformance probe no longer runs on a blocking thread. It executes \
             contract WASM synchronously, so on the async runtime it competes with \
             the event loop — and a node sized by available_parallelism() has one \
             worker thread on a single-vCPU host. See .claude/rules/contracts.md"
        );
    }

    /// The writer must delete an evicted contract's persisted bundle.
    ///
    /// `record` returns the victim and is `#[must_use]`, so ignoring it is a compile
    /// error - but `let _ = ...` silences that, and a refactor reaching for it would
    /// reintroduce unbounded orphan files with no test failing.
    #[test]
    fn an_evicted_contract_has_its_bundle_removed() {
        let body = run_writer_code_only();
        // Bound to the EVICTED binding, not merely to `remove_file` appearing
        // somewhere: the writer already unlinks its own `.tmp` staging file on a failed
        // write, so a bare substring check passes even if the deletion is aimed at the
        // wrong contract entirely.
        assert!(
            body.contains("bundle_path(&dir, &evicted)"),
            "the writer no longer derives the deleted path from the evicted contract, \
             so eviction either orphans a bundle or deletes the wrong one"
        );
        assert!(
            body.contains("remove_file(&path)"),
            "the writer no longer deletes evicted bundles, so every rotation past the \
             tracking cap orphans a file that nothing will ever clean up"
        );
    }

    /// Focus must be drawn before the select loop, and retried until the hosted source
    /// exists.
    ///
    /// Both halves are load-bearing and neither is reachable from a test, because
    /// nothing calls `run_writer`. Under focus-scoped sampling an empty focus set
    /// records NOTHING, and this task is spawned BY `global()` from inside
    /// `set_hosted_contracts_source` - one line before the source is registered - so
    /// the startup draw alone reliably misses it and the peer records nothing until the
    /// first probe tick fifteen minutes later.
    #[test]
    fn focus_is_drawn_at_startup_and_retried_until_the_hosted_source_arrives() {
        let body = run_writer_code_only();
        let loop_start = body.find("loop {").expect("run_writer has no select loop");
        assert!(
            body[..loop_start].contains(".focus("),
            "focus is no longer computed before the select loop, so a focus-scoped \
             peer records nothing for the first probe interval after every restart"
        );
        // The ARM, not merely the constant: deleting the select arm leaves
        // `let mut warmup = interval(HOSTED_SOURCE_WARMUP)` behind, so a pin on the
        // constant's name passes with the retry gone - confirmed by mutation.
        assert!(
            body.contains("warmup.tick()"),
            "the hosted-source warm-up retry arm is gone; the startup draw alone runs \
             before the ring registers its hosted contracts and therefore misses it"
        );
    }

    /// A focus contract still warming up must be counted, not dropped from the tick.
    ///
    /// `focused` is derived from the probe's work length, so a focus contract with no
    /// samples yet vanished entirely: two selected with samples for one reported
    /// `focused=1, skipped_no_samples=0`, which reads as "one contract, fully checked".
    /// Warm-up is the normal state now that focus draws from the hosted set rather than
    /// from what was already sampled, so this hid the common case.
    #[test]
    fn contracts_awaiting_samples_are_counted_in_the_tick() {
        let body = run_writer_code_only();
        assert!(
            body.contains("report.focused += awaiting_samples"),
            "focus contracts still awaiting samples no longer count toward `focused`, \
             so a warming-up focus set reports as a smaller, fully-checked one"
        );
        assert!(
            body.contains("report.skipped_no_samples += awaiting_samples"),
            "focus contracts still awaiting samples are no longer reported as skipped"
        );
    }

    #[test]
    fn at_most_one_probe_runs_at_a_time() {
        let body = run_writer_code_only();
        assert!(
            body.contains("in_flight.is_none()"),
            "the probe tick arm is no longer guarded on there being no probe in \
             flight, so a probe that overran its interval would have another stacked \
             on top of it — unbounded concurrent WASM execution from a job whose \
             entire justification is that it is bounded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The related-state budgets the sampler config in force for tests implies.
    ///
    /// Was a bare `MAX_RELATED_BYTES` constant. It is now derived from the sampler
    /// config so a raised capture budget reaches related state too, and these tests
    /// have to ask the same question the code does rather than a fixed number that
    /// could drift away from it.
    fn test_related_budgets() -> (usize, usize) {
        related_budgets(&sampler_config())
    }

    fn instance(n: u8) -> ContractInstanceId {
        ContractInstanceId::new([n; 32])
    }

    fn observation_for(contract: ContractInstanceId) -> Observation {
        Observation {
            contract,
            code_hash: [2; 32],
            parameters: vec![3],
            base_state: vec![1, 2],
            incoming_state: Some(vec![2, 3]),
            delta: None,
            result_state: vec![1, 2, 3],
            related: Vec::new(),
        }
    }

    fn focused_on(ids: &[u8]) -> SamplingScope {
        SamplingScope::Focused(ids.iter().copied().map(instance).collect())
    }

    /// Sampling follows focus: a contract nobody is watching is not recorded.
    ///
    /// This is the RFC's shape ("while a contract is in the focus set, the peer
    /// records a bounded, diverse sample") and it is what keeps an ordinary peer's
    /// corpus proportional to its focus set rather than to its traffic.
    #[test]
    fn focused_sampling_records_only_the_focus_set() {
        let mut samplers = HashMap::new();
        let scope = focused_on(&[1]);

        let _evicted = record(&mut samplers, &scope, observation_for(instance(1)));
        let _evicted = record(&mut samplers, &scope, observation_for(instance(2)));

        assert!(
            samplers.contains_key(&instance(1)),
            "the focused contract was not sampled"
        );
        assert!(
            !samplers.contains_key(&instance(2)),
            "an unfocused contract was sampled: sampling is not following focus"
        );
    }

    /// A restart keeps a NAMED set of contracts, not whatever the filesystem lists first.
    ///
    /// `reload` stops at `MAX_TRACKED_CONTRACTS`, and `read_dir` order is undefined, so
    /// without a sort which contracts survive a restart is decided by enumeration
    /// order. Every other reload test writes a single bundle, so the truncation path
    /// they cover is the one where truncation never happens - deleting the sort left
    /// all of them green.
    ///
    /// Asserting "two reloads agree" does NOT test this either, and that version of
    /// this test was vacuous: `read_dir` is stable across calls within one process even
    /// though its order is unspecified, so both reloads agreed with the sort deleted.
    /// What the sort actually guarantees is WHICH set survives - the lexicographically
    /// smallest filenames - and that holds only by luck under ext4's hash order.
    #[test]
    fn a_restart_reloads_a_deterministic_set_when_there_are_more_bundles_than_slots() {
        let dir = tempfile::TempDir::new().expect("tempdir");

        // More bundles than slots, so truncation actually runs.
        let extra = 12usize;
        let mut written = Vec::new();
        for i in 0..(MAX_TRACKED_CONTRACTS + extra) {
            let instance = ContractInstanceId::new([(i % 251) as u8; 32]);
            written.push(instance);
            let mut bundle = super::super::bundle::ReplayBundle::new(vec![9, 9], vec![3]);
            bundle.instance = Some(instance);
            bundle.states = vec![vec![1, 2], vec![2, 3]];
            bundle
                .write_to(&bundle_path(dir.path(), &instance))
                .expect("write bundle");
        }

        // What the sort promises: the lexicographically smallest bundle filenames.
        let mut expected: Vec<String> = written.iter().map(|id| format!("{id}.bundle")).collect();
        expected.sort();
        expected.truncate(MAX_TRACKED_CONTRACTS);

        let mut kept: Vec<String> = reload(dir.path())
            .into_keys()
            .map(|id| format!("{id}.bundle"))
            .collect();
        kept.sort();

        assert_eq!(
            kept.len(),
            MAX_TRACKED_CONTRACTS,
            "fixture did not exceed the cap, so truncation never ran"
        );
        assert_eq!(
            kept, expected,
            "reload kept a different set than the ordering promises, so which samples \
             survive a restart depends on filesystem enumeration order"
        );
    }

    /// Reload counts the related state IT refuses.
    ///
    /// Reload re-admits under the CURRENT process's budgets, so a run that captured
    /// under a raised budget and then restarted under a smaller one has its related
    /// state trimmed at reload. An earlier version threw that count away into a
    /// throwaway local, so the next flush wrote "0 refused" over the evidence - the
    /// exact silent truncation this counter exists to prevent, in the one path that
    /// most needs it.
    #[test]
    fn reload_reports_related_state_it_had_to_refuse() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let instance = ContractInstanceId::new([3; 32]);
        let (max_state, _) = related_budgets(&sampler_config());

        // A bundle carrying more related contracts than the slot limit allows, which
        // binds regardless of how the byte budgets are configured.
        let mut bundle = super::super::bundle::ReplayBundle::new(vec![9, 9], vec![3]);
        bundle.instance = Some(instance);
        bundle.states = vec![vec![1, 2], vec![2, 3]];
        bundle.related = (0..(MAX_RELATED_CONTRACTS + 4))
            .map(|i| (ContractInstanceId::new([i as u8; 32]), vec![i as u8; 16]))
            .collect();
        assert!(
            max_state > 16,
            "fixture states must be individually admissible"
        );
        bundle
            .write_to(&bundle_path(dir.path(), &instance))
            .expect("write bundle");

        let samplers = reload(dir.path());
        let tracked = samplers
            .get(&instance)
            .expect("bundle should have reloaded");

        assert_eq!(
            tracked.related.len(),
            MAX_RELATED_CONTRACTS,
            "reload did not trim to the slot limit, so nothing was refused"
        );
        assert_eq!(
            tracked.refused_related.no_slot, 4,
            "reload trimmed related state without counting it, so the next flush will \
             record a corpus as complete when it is not"
        );
    }

    /// Each reloaded bundle gets ITS OWN refusal count, not a running total.
    ///
    /// The direct regression for the worse bug the first fix could have introduced.
    /// `reload_refusals` has to be declared inside the per-bundle loop; hoisted above
    /// it, every contract after the first inherits the earlier contracts' refusals and
    /// the counter starts lying in the opposite direction - overstating rather than
    /// hiding, which is worse, because an overstated count sends someone hunting a
    /// truncation that never happened.
    ///
    /// The existing multi-bundle reload test cannot catch this: its bundles carry no
    /// related state at all, so no bundle refuses anything.
    #[test]
    fn each_reloaded_bundle_counts_only_its_own_refusals() {
        let dir = tempfile::TempDir::new().expect("tempdir");

        // Two bundles, each over the slot limit by a DIFFERENT amount, so a shared
        // counter cannot coincidentally produce the right answer for both.
        let over_by = [3usize, 5usize];
        let instances = [
            ContractInstanceId::new([200; 32]),
            ContractInstanceId::new([201; 32]),
        ];
        for (instance, extra) in instances.iter().zip(over_by) {
            let mut bundle = super::super::bundle::ReplayBundle::new(vec![9, 9], vec![3]);
            bundle.instance = Some(*instance);
            bundle.states = vec![vec![1, 2], vec![2, 3]];
            bundle.related = (0..(MAX_RELATED_CONTRACTS + extra))
                .map(|i| (ContractInstanceId::new([i as u8; 32]), vec![i as u8; 16]))
                .collect();
            bundle
                .write_to(&bundle_path(dir.path(), instance))
                .expect("write bundle");
        }

        let samplers = reload(dir.path());

        for (instance, extra) in instances.iter().zip(over_by) {
            let tracked = samplers.get(instance).expect("bundle should have reloaded");
            assert_eq!(
                tracked.refused_related.no_slot, extra as u64,
                "{instance} reported a refusal count that is not its own; a shared \
                 counter across bundles makes every contract after the first overstate"
            );
        }
    }

    /// The related budget SCALES with the sampler budget an operator sets.
    ///
    /// It used to be a hardcoded 512 KiB, used as both the per-state and the total
    /// cap, and `FREENET_CONFORMANCE_CAPTURE_MAX_BYTES` never reached it. Live states
    /// run to ~356 KiB, so two related contracts already blew the total and the second
    /// was discarded - which is why 1,567 of 5,856 replayed cases reached no verdict.
    #[test]
    fn the_related_budget_follows_the_sampler_budget() {
        let small = sampler_config_from(Some("1048576"));
        let large = sampler_config_from(Some("16777216"));

        let (_, small_total) = related_budgets(&small);
        let (_, large_total) = related_budgets(&large);

        assert!(
            large_total > small_total,
            "raising the capture budget did not raise the related-state budget, so a \
             contract that needs large related state stays unjudgeable however the \
             operator configures the run ({small_total} vs {large_total})"
        );
        assert_eq!(large_total, large.max_bytes);
    }

    /// Related state offered but not kept is COUNTED, never silently dropped.
    ///
    /// The whole point: an empty related map and a refused one look identical in a
    /// corpus, and only the second explains why a replay reaches no verdict. A
    /// contract that depends on related state must not be able to become quietly
    /// unjudgeable - that would make "depends on a big related contract" a way to
    /// escape conformance checking entirely.
    #[test]
    fn refused_related_state_is_counted_rather_than_dropped_silently() {
        let config = sampler_config();
        let (max_state, _) = related_budgets(&config);
        let mut held = HashMap::new();
        let mut refused = RelatedRefusals::default();

        // One oversized state.
        admit_related(
            &mut held,
            &[(instance(9), vec![0u8; max_state + 1])],
            &mut refused,
            max_state,
            config.max_bytes,
        );

        assert!(held.is_empty(), "an oversized related state was admitted");
        assert_eq!(
            refused.too_large, 1,
            "an oversized related state was dropped without being counted"
        );
        assert_eq!(refused.total(), 1);
    }

    /// Exceeding the TOTAL budget is counted under its own reason.
    #[test]
    fn related_state_over_the_total_budget_is_counted_separately() {
        let mut held = HashMap::new();
        let mut refused = RelatedRefusals::default();
        // Per-state cap generous, total cap tight: the second offer must not fit.
        let (max_state, max_total) = (1024, 1024);

        admit_related(
            &mut held,
            &[(instance(1), vec![0u8; 800]), (instance(2), vec![0u8; 800])],
            &mut refused,
            max_state,
            max_total,
        );

        assert_eq!(held.len(), 1, "both states fit, so the budget never bound");
        assert_eq!(
            refused.over_budget, 1,
            "a related state refused for budget was not counted"
        );
        assert_eq!(refused.too_large, 0, "counted under the wrong reason");
    }

    /// Running out of slots is counted under its own reason too.
    #[test]
    fn related_state_beyond_the_slot_limit_is_counted_separately() {
        let mut held = HashMap::new();
        let mut refused = RelatedRefusals::default();
        let offered: Vec<_> = (0..(MAX_RELATED_CONTRACTS + 3))
            .map(|i| (instance(i as u8), vec![0u8; 8]))
            .collect();

        admit_related(&mut held, &offered, &mut refused, 4096, 1 << 20);

        assert_eq!(held.len(), MAX_RELATED_CONTRACTS);
        assert_eq!(
            refused.no_slot, 3,
            "related contracts beyond the slot limit were dropped uncounted"
        );
    }

    /// A bundle whose related state was refused SAYS SO, in the bundle itself.
    ///
    /// The logs rotate; the corpus is replayed later and elsewhere. If the refusal
    /// only ever reached a log line, a reader of the bundle would see an empty
    /// related map and conclude the contract needed none.
    #[tokio::test]
    async fn a_bundle_records_that_related_state_was_refused() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut samplers = HashMap::new();
        let watched = instance(1);

        let mut obs = observation_for(watched);
        let (max_state, _) = related_budgets(&sampler_config());
        obs.related = vec![(instance(2), vec![0u8; max_state + 1])];
        let _evicted = record(&mut samplers, &SamplingScope::Wide, obs);

        assert_eq!(
            samplers[&watched].refused_related.too_large, 1,
            "fixture did not trigger a refusal, so this proves nothing"
        );

        write_all(dir.path(), &samplers, 0).await;

        let bundle =
            super::super::bundle::ReplayBundle::read_from(&bundle_path(dir.path(), &watched))
                .expect("bundle should have been written");
        let note = bundle.note.unwrap_or_default();
        assert!(
            note.contains("related state refused"),
            "the bundle does not record that related state was refused, so a replay \
             cannot tell 'needed none' from 'could not keep it': {note}"
        );
    }

    /// Validation-resolved related state reaches the tracked contract.
    ///
    /// The #5376 regression. Related state arrives by two routes and only one travels
    /// with a transition; without this path a contract whose VALIDITY depends on
    /// another contract is permanently unjudgeable, because every replayed case
    /// dead-ends at `Inconclusive::RelatedRequired` and that reads like a clean run.
    #[test]
    fn validation_resolved_related_state_is_merged_into_the_tracked_contract() {
        let mut samplers = HashMap::new();
        let watched = instance(1);
        let scope = focused_on(&[1]);

        // The contract is tracked from an ordinary transition that carried NO related
        // state - which is the real shape: the update did not need any, only the
        // validation did.
        let mut obs = observation_for(watched);
        obs.related = Vec::new();
        let _evicted = record(&mut samplers, &scope, obs);
        assert!(
            samplers[&watched].related.is_empty(),
            "fixture started with related state, so this proves nothing"
        );

        record_related(
            &mut samplers,
            &scope,
            watched,
            &[(instance(2), vec![7u8; 32])],
        );

        assert_eq!(
            samplers[&watched].related.len(),
            1,
            "validation-resolved related state never reached the tracked contract, so \
             a replay of it cannot reach a verdict"
        );
    }

    /// It does NOT create a tracked entry for an unknown contract.
    ///
    /// Related state with no states of its own cannot produce a single case, so
    /// admitting it would spend one of MAX_TRACKED_CONTRACTS on something no replay
    /// can use - and on a peer at its cap that displaces a contract that CAN be judged.
    #[test]
    fn validation_related_state_does_not_create_an_untracked_contract() {
        let mut samplers = HashMap::new();
        let stranger = instance(9);

        record_related(
            &mut samplers,
            &focused_on(&[9]),
            stranger,
            &[(instance(2), vec![7u8; 32])],
        );

        assert!(
            samplers.is_empty(),
            "related state alone created a tracked entry, spending a slot on a \
             contract no case can be built from"
        );
    }

    /// Sampling follows focus for it too.
    ///
    /// Otherwise "sampling follows focus" would be true of transitions and quietly
    /// false of related state, which is the kind of half-true invariant that is worse
    /// than none.
    #[test]
    fn validation_related_state_is_not_collected_out_of_focus() {
        let mut samplers = HashMap::new();
        let watched = instance(1);

        let _evicted = record(&mut samplers, &focused_on(&[1]), observation_for(watched));
        // Rotate away, then let validation-resolved state arrive for it.
        record_related(
            &mut samplers,
            &focused_on(&[2]),
            watched,
            &[(instance(3), vec![7u8; 32])],
        );

        assert!(
            samplers[&watched].related.is_empty(),
            "an out-of-focus contract kept collecting related state"
        );
    }

    /// Sampling follows focus for contracts ALREADY tracked, not just new ones.
    ///
    /// The first version gated only new admissions, so any contract that had ever been
    /// focused kept absorbing observations forever. With focus rotating every couple of
    /// hours into a 64-entry map, steady state was 64 contracts collecting rather than
    /// the two in focus - the narrow behaviour held for a few days after a clean deploy
    /// and then quietly stopped being true.
    ///
    /// Counts `total_seen` rather than distinct states, so the assertion cannot be
    /// satisfied by deduplication happening to hide a sampler that is still collecting.
    #[test]
    fn a_contract_that_leaves_focus_stops_collecting() {
        let mut samplers = HashMap::new();
        let watched = instance(1);

        let _evicted = record(&mut samplers, &focused_on(&[1]), observation_for(watched));
        let before = samplers
            .get(&watched)
            .expect("focused contract was not sampled")
            .sampler
            .total_seen();
        assert!(
            before > 0,
            "fixture recorded nothing, so this proves nothing"
        );

        // Rotate it out of focus, then keep sending it traffic with distinct states.
        let elsewhere = focused_on(&[2]);
        for n in 0..8u8 {
            let mut obs = observation_for(watched);
            obs.result_state = vec![n, 9, 9];
            let _evicted = record(&mut samplers, &elsewhere, obs);
        }

        let after = samplers
            .get(&watched)
            .expect("an out-of-focus contract must be RETAINED, only not collected")
            .sampler
            .total_seen();
        assert_eq!(
            before, after,
            "a contract kept collecting after it left the focus set"
        );
    }

    /// Retention survives defocus even though collection stops - the RFC's "may outlive
    /// a focus period ... so returning to a contract does not always start from zero".
    #[test]
    fn a_contract_that_leaves_focus_keeps_what_it_already_collected() {
        let mut samplers = HashMap::new();
        let watched = instance(1);
        let _evicted = record(&mut samplers, &focused_on(&[1]), observation_for(watched));

        // Traffic MUST keep arriving for the defocused contract. Without it the
        // not-admitted branch never executes, and a version of `record` that discarded
        // the entry on defocus would pass this test unchanged - confirmed by mutation.
        let elsewhere = focused_on(&[2]);
        for n in 0..4u8 {
            let mut obs = observation_for(watched);
            obs.result_state = vec![n, 4, 4];
            let _evicted = record(&mut samplers, &elsewhere, obs);
        }

        assert!(
            samplers.contains_key(&watched),
            "defocusing a contract discarded its accumulated sample"
        );
    }

    /// Eviction names its victim so the caller can delete the persisted bundle.
    ///
    /// Removing the entry alone orphans the file. Eviction fires on every rotation once
    /// the map is full, so those orphans accumulate without bound - and `reload` can
    /// then resurrect a long-evicted contract ahead of the current focus set.
    #[test]
    fn eviction_reports_the_victim_so_its_bundle_can_be_deleted() {
        let mut samplers = HashMap::new();
        for i in 0..MAX_TRACKED_CONTRACTS {
            let id = ContractInstanceId::new([(i % 251) as u8; 32]);
            let _evicted = record(&mut samplers, &SamplingScope::Wide, observation_for(id));
        }
        assert_eq!(samplers.len(), MAX_TRACKED_CONTRACTS);

        let newcomer = ContractInstanceId::new([255; 32]);
        let scope = SamplingScope::Focused([newcomer].into_iter().collect());
        let evicted = record(&mut samplers, &scope, observation_for(newcomer));

        let victim = evicted.expect("eviction happened but reported no victim to clean up");
        assert!(
            !samplers.contains_key(&victim),
            "the reported victim is still tracked"
        );
        assert_ne!(
            victim, newcomer,
            "eviction reported the newcomer as its own victim"
        );
    }

    /// The victim is specifically the LOWEST-id non-focused contract.
    ///
    /// Asserting only "a non-focused contract was evicted" does not pin this: the
    /// earlier tests protected a contract that already sat at the minimum id, so
    /// swapping `min_by` for `max_by` passed them both.
    #[test]
    fn eviction_takes_the_lowest_id_contract_not_in_focus() {
        let mut samplers = HashMap::new();
        for i in 0..MAX_TRACKED_CONTRACTS {
            let id = ContractInstanceId::new([(i % 251) as u8; 32]);
            let _evicted = record(&mut samplers, &SamplingScope::Wide, observation_for(id));
        }
        assert_eq!(samplers.len(), MAX_TRACKED_CONTRACTS);

        // Protect the lowest id, so the expected victim is the SECOND lowest. Under a
        // max_by rule the victim would be the highest instead.
        let mut ids: Vec<_> = samplers.keys().copied().collect();
        ids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let protected = ids[0];
        let expected_victim = ids[1];
        let newcomer = ContractInstanceId::new([255; 32]);

        let scope = SamplingScope::Focused([protected, newcomer].into_iter().collect());
        let evicted = record(&mut samplers, &scope, observation_for(newcomer));

        assert_eq!(
            evicted,
            Some(expected_victim),
            "eviction did not take the lowest-id non-focused contract"
        );
        assert!(
            samplers.contains_key(&protected),
            "a focused contract was evicted"
        );
    }

    /// Wide mode is the developer corpus path and deliberately ignores focus.
    #[test]
    fn wide_sampling_records_contracts_outside_the_focus_set() {
        let mut samplers = HashMap::new();
        let _evicted = record(
            &mut samplers,
            &SamplingScope::Wide,
            observation_for(instance(2)),
        );
        assert!(
            samplers.contains_key(&instance(2)),
            "wide capture refused a contract, so no corpus can be built"
        );
    }

    /// The #5366 regression, in the form focus-scoped sampling would take.
    ///
    /// A full tracking map must not be able to lock the CURRENT focus contract out of
    /// being sampled. Rotation guarantees the map fills with contracts that are no
    /// longer in focus, so a refuse-the-newcomer rule means that after enough
    /// rotations a peer probes only what it captured first - forever, while its tick
    /// lines still read healthy. Room is made by evicting something nothing is
    /// watching.
    #[test]
    fn a_full_map_still_admits_the_contract_focus_selected() {
        let mut samplers = HashMap::new();
        // Fill to the cap with contracts that are not in focus.
        for i in 0..MAX_TRACKED_CONTRACTS {
            let id = ContractInstanceId::new([(i % 251) as u8; 32]);
            let _evicted = record(&mut samplers, &SamplingScope::Wide, observation_for(id));
        }
        // Distinct ids only, otherwise this fills nothing and proves nothing.
        assert_eq!(
            samplers.len(),
            MAX_TRACKED_CONTRACTS,
            "fixture did not reach the cap, so the admission path under test never ran"
        );

        let newcomer = ContractInstanceId::new([255; 32]);
        assert!(
            !samplers.contains_key(&newcomer),
            "fixture already tracked the newcomer"
        );
        let scope = SamplingScope::Focused([newcomer].into_iter().collect());
        let _evicted = record(&mut samplers, &scope, observation_for(newcomer));

        assert!(
            samplers.contains_key(&newcomer),
            "a full map locked out the contract focus actually selected (#5366)"
        );
        assert!(
            samplers.len() <= MAX_TRACKED_CONTRACTS,
            "admission grew the map past its cap: {}",
            samplers.len()
        );
    }

    /// Eviction to make room must never take a contract that IS in focus - that would
    /// trade one starvation for another, discarding the sample being actively built.
    #[test]
    fn making_room_never_evicts_a_focused_contract() {
        let mut samplers = HashMap::new();
        for i in 0..MAX_TRACKED_CONTRACTS {
            let id = ContractInstanceId::new([(i % 251) as u8; 32]);
            let _evicted = record(&mut samplers, &SamplingScope::Wide, observation_for(id));
        }
        assert_eq!(samplers.len(), MAX_TRACKED_CONTRACTS);

        // Focus on one contract already tracked, plus one that is not.
        let held = *samplers
            .keys()
            .min_by(|a, b| a.as_bytes().cmp(b.as_bytes()))
            .expect("fixture is empty");
        let newcomer = ContractInstanceId::new([255; 32]);
        let scope = SamplingScope::Focused([held, newcomer].into_iter().collect());

        let _evicted = record(&mut samplers, &scope, observation_for(newcomer));

        assert!(
            samplers.contains_key(&held),
            "eviction took a focused contract - the lowest id is exactly what the \
             deterministic victim rule would pick if it did not skip focus"
        );
        assert!(
            samplers.contains_key(&newcomer),
            "newcomer was not admitted"
        );
    }

    /// Wide mode keeps the old bounded-refusal behaviour: a developer corpus whose
    /// contents depended on eviction order would stop meaning "what this peer saw".
    ///
    /// Asserting only that the map stopped at its cap does NOT test this - a map that
    /// evicts one entry per admission also sits exactly at the cap forever. The first
    /// version of this test did exactly that and passed under the rollover it was
    /// written to forbid. What separates the two is WHICH contracts survive: refusal
    /// keeps the earliest, rollover keeps the latest.
    #[test]
    fn wide_sampling_stops_at_the_cap_rather_than_rolling_over() {
        let mut samplers = HashMap::new();
        let first = ContractInstanceId::new([0; 32]);
        for i in 0..(MAX_TRACKED_CONTRACTS + 20) {
            let id = ContractInstanceId::new([(i % 251) as u8; 32]);
            let _evicted = record(&mut samplers, &SamplingScope::Wide, observation_for(id));
        }
        assert_eq!(
            samplers.len(),
            MAX_TRACKED_CONTRACTS,
            "wide capture did not stop at its cap"
        );
        assert!(
            samplers.contains_key(&first),
            "wide capture dropped the earliest contract it saw, so the corpus now \
             depends on eviction order rather than on what the peer observed"
        );
        let last = ContractInstanceId::new([(MAX_TRACKED_CONTRACTS + 19) as u8; 32]);
        assert!(
            !samplers.contains_key(&last),
            "wide capture admitted a contract past its cap"
        );
    }

    fn observation() -> Observation {
        Observation {
            contract: ContractInstanceId::new([1; 32]),
            code_hash: [2; 32],
            parameters: vec![3],
            base_state: vec![1, 2],
            incoming_state: Some(vec![2, 3]),
            delta: None,
            result_state: vec![1, 2, 3],
            related: Vec::new(),
        }
    }

    /// The load-bearing property for running this on a live node: when the writer
    /// cannot keep up, the executor is not made to wait. Observations are dropped
    /// and counted instead, because a stalled capture must never become a stalled
    /// merge.
    #[tokio::test]
    async fn a_full_queue_drops_rather_than_blocking() {
        let (tx, _rx) = mpsc::channel(1);
        let handle = CaptureHandle {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            queued_bytes: Arc::new(AtomicUsize::new(0)),
        };

        // One fits in the buffer; the rest cannot, and must not block.
        for _ in 0..64 {
            handle.observe(observation());
        }

        assert!(
            handle.dropped() >= 60,
            "expected the overflow to be dropped and counted, saw {}",
            handle.dropped()
        );
    }

    /// A dead writer must be just as harmless as a slow one.
    #[tokio::test]
    async fn a_closed_receiver_does_not_panic_the_caller() {
        let (tx, rx) = mpsc::channel(4);
        drop(rx);
        let handle = CaptureHandle {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            queued_bytes: Arc::new(AtomicUsize::new(0)),
        };
        handle.observe(observation());
        assert_eq!(handle.dropped(), 1);
    }

    /// Capture is off unless asked for. If this ever returns a handle from an
    /// unset environment, every node in the network starts recording user state.
    #[test]
    fn capture_is_off_when_the_environment_does_not_ask_for_it() {
        // Tests the decision, not the environment. The previous version called
        // `set_var`/`remove_var` with a SAFETY note reasoning about this variable
        // having no other readers — but the hazard is not this variable: `setenv`
        // races any concurrent `getenv` in the process, and sibling tests here call
        // `TempDir::new()`, which reads `TMPDIR`.
        assert!(
            capture_dir_from(None).is_none(),
            "an unset environment must leave capture off; otherwise every node in \
             the network starts recording user state"
        );
        for blank in ["", "   ", "\t\n"] {
            assert!(
                capture_dir_from(Some(blank)).is_none(),
                "a blank setting ({blank:?}) must not enable capture"
            );
        }
        assert_eq!(
            capture_dir_from(Some("/tmp/somewhere")),
            Some(PathBuf::from("/tmp/somewhere")),
            "an explicit directory must be honoured, or the knob does nothing"
        );
    }

    /// Bundles must name the contract they came from, or they cannot be replayed
    /// safely: `resolve_code` refuses a bundle with no code hash precisely so a
    /// corpus can never be checked against an unrelated WASM.
    /// The queue is bounded by BYTES, not just by item count.
    ///
    /// The item cap alone is not a memory bound: 256 queued observations of a
    /// contract with multi-megabyte states is gigabytes in flight, invisible to the
    /// node's memory accounting. Capture is a diagnostic and must not be able to OOM
    /// the node it is diagnosing.
    ///
    /// Checked before building, so an over-budget observation costs no allocation
    /// either — asserted by counting builds, since the drop is otherwise invisible.
    #[tokio::test]
    async fn the_queue_refuses_more_bytes_than_its_budget() {
        // Plenty of item capacity, so any refusal here is the byte budget's doing.
        let (tx, _rx) = mpsc::channel(64);
        let handle = CaptureHandle {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            queued_bytes: Arc::new(AtomicUsize::new(0)),
        };

        let builds = std::cell::Cell::new(0usize);
        let huge = || {
            builds.set(builds.get() + 1);
            let mut obs = observation();
            obs.base_state = vec![0u8; MAX_QUEUED_BYTES + 1];
            obs
        };

        // One observation larger than the entire budget can never be admitted, and
        // must not be built to find that out.
        handle.observe_with(MAX_QUEUED_BYTES + 1, huge);
        assert_eq!(
            builds.get(),
            0,
            "an observation bigger than the whole budget must be refused without \
             being built; otherwise one huge contract pays for itself in full"
        );
        assert_eq!(handle.dropped(), 1);

        // Filling the budget with in-range observations must also start refusing,
        // while item capacity remains.
        let each = MAX_QUEUED_BYTES / 4;
        let admitted = std::cell::Cell::new(0usize);
        for _ in 0..8 {
            handle.observe_with(each, || {
                admitted.set(admitted.get() + 1);
                let mut obs = observation();
                obs.base_state = vec![0u8; each];
                obs
            });
        }
        assert!(
            admitted.get() <= 4,
            "the byte budget should have stopped admissions at about four of these, \
             built {} instead",
            admitted.get()
        );
        assert!(
            handle.dropped() >= 4,
            "the refusals should be counted, saw {}",
            handle.dropped()
        );
    }

    /// Related-contract state survives capture and comes back out of the bundle.
    ///
    /// A contract whose `validate_state` depends on another contract cannot be
    /// checked at all without that state: the verifier reaches no verdict and says
    /// `RelatedRequired`. That is honest, and it applies to a whole class of contract
    /// rather than to an unlucky one, so the capture path has to carry it.
    #[tokio::test]
    async fn related_contract_state_is_captured_and_replayable() {
        let related_id = ContractInstanceId::new([9; 32]);
        let mut observed = observation();
        observed.related = vec![(related_id, vec![42, 43])];

        let mut samplers = HashMap::new();
        let _evicted = record(&mut samplers, &SamplingScope::Wide, observed);

        let dir = tempfile::TempDir::new().expect("tempdir");
        write_all(dir.path(), &samplers, 0).await;

        let path = dir
            .path()
            .join(format!("{}.bundle", ContractInstanceId::new([1; 32])));
        let bundle = super::super::bundle::ReplayBundle::read_from(&path).expect("read back");
        assert_eq!(
            bundle.related,
            vec![(related_id, vec![42, 43])],
            "the bundle must carry the related state the merge referenced"
        );

        // And it has to arrive where the verifier looks for it, not merely be stored.
        let corpus = bundle.to_corpus();
        assert!(
            corpus.related.states().any(|(id, state)| {
                *id == related_id && state.as_ref().map(|s| s.as_ref()) == Some(&[42u8, 43][..])
            }),
            "related state must reach the corpus as RelatedContracts, or a contract \
             that needs it still cannot be executed"
        );
    }

    /// The TOTAL byte allowance holds, not just the per-entry one.
    ///
    /// Review found the total check was deletable with every other test still green:
    /// the oversized-entry case is caught by the per-entry check, and the count tests
    /// used states small enough that their sum never approached the limit. Without
    /// the total, eight entries just under the per-entry bound would hold eight times
    /// the intended allowance for one contract, and that multiplies by every tracked
    /// contract.
    ///
    /// This also exercises the replacement path, where an existing entry's bytes must
    /// be discounted from the total rather than double-counted.
    #[tokio::test]
    async fn the_total_related_byte_allowance_holds() {
        let mut samplers = HashMap::new();
        let (max_state, max_total) = test_related_budgets();

        // Each entry sits exactly AT the per-state ceiling, so every one is
        // individually admissible and only the running total can refuse them.
        //
        // This used to divide the TOTAL budget into thirds, which was correct while
        // one constant served as both caps. Once they became distinct
        // (`max_state_bytes` is a quarter of `max_bytes`), a third of the total
        // exceeded the per-state cap, so every entry was refused as `TooLarge`, the
        // map stayed empty, and the assertions below passed without the total-budget
        // path ever running.
        let chunk = max_state;
        let entries = (max_total / chunk) + 1;
        assert!(
            entries >= 2,
            "fixture needs at least two entries to test a TOTAL bound"
        );
        for i in 0..entries {
            let mut observed = observation();
            observed.related = vec![(ContractInstanceId::new([i as u8; 32]), vec![7u8; chunk])];
            let _evicted = record(&mut samplers, &SamplingScope::Wide, observed);
        }

        let refused = samplers
            .values()
            .next()
            .expect("one contract tracked")
            .refused_related;
        assert_eq!(
            refused.too_large, 0,
            "entries were refused on the PER-STATE cap, so the total was never tested"
        );
        assert!(
            refused.over_budget > 0,
            "nothing was refused on the total, so the allowance never bound"
        );

        let tracked = samplers.values().next().expect("one contract tracked");
        let held: usize = tracked.related.values().map(Vec::len).sum();
        assert!(
            held <= test_related_budgets().1,
            "related state totalled {held} bytes against an allowance of {}",
            test_related_budgets().1
        );
        assert!(
            tracked.related.len() < entries,
            "all {entries} entries were admitted, so the total allowance is not binding"
        );

        // Replacing an existing entry must discount what it already held: offering
        // the same id again with the same size must not push the total over.
        let before = tracked.related.len();
        let mut again = observation();
        again.related = vec![(ContractInstanceId::new([0; 32]), vec![0; chunk])];
        let _evicted = record(&mut samplers, &SamplingScope::Wide, again);
        let tracked = samplers.values().next().expect("one contract tracked");
        assert_eq!(
            tracked.related.len(),
            before,
            "replacing an entry changed the number held, so its bytes were not \
             discounted from the total"
        );
    }

    /// A bundle on disk cannot smuggle more related state than today's bounds allow.
    ///
    /// Everything else a reload restores goes back through the sampler's admission
    /// checks, so it is self-correcting: a corpus written by a build with looser
    /// limits is trimmed to what this build permits. Related state was the one field
    /// collected raw, and nothing bounds that vector on disk, so a bundle from a
    /// looser build — or an edited one — loaded straight past the current limits.
    #[tokio::test]
    async fn a_reloaded_bundle_cannot_exceed_the_related_bounds() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let instance = ContractInstanceId::new([1; 32]);

        // Hand-build a bundle carrying far more related state than the bounds allow,
        // as a looser build or an edit would produce.
        let mut bundle = super::super::bundle::ReplayBundle::new(vec![9, 9], vec![3]);
        bundle.instance = Some(instance);
        bundle.states = vec![vec![1, 2], vec![2, 3]];
        bundle.related = (0..(MAX_RELATED_CONTRACTS + 6))
            .map(|i| (ContractInstanceId::new([i as u8; 32]), vec![i as u8; 32]))
            .collect();
        bundle
            .write_to(&dir.path().join(format!("{instance}.bundle")))
            .expect("write bundle");

        let samplers = reload(dir.path());
        let tracked = samplers
            .values()
            .next()
            .expect("the bundle should have been reloaded");

        assert!(
            tracked.related.len() <= MAX_RELATED_CONTRACTS,
            "reload admitted {} related contracts, over the cap of {}",
            tracked.related.len(),
            MAX_RELATED_CONTRACTS
        );
        assert!(
            tracked.related.values().map(Vec::len).sum::<usize>() <= test_related_budgets().1,
            "reload admitted more related bytes than the allowance"
        );
    }

    /// Related state is bounded, and refuses rather than evicting.
    ///
    /// It has its own allowance rather than sharing the sample budget: sharing would
    /// let a contract with large related state crowd out the very samples the related
    /// state exists to make checkable.
    #[tokio::test]
    async fn related_contract_state_is_bounded() {
        let mut samplers = HashMap::new();

        // More distinct related contracts than the cap allows.
        for i in 0..(MAX_RELATED_CONTRACTS + 4) {
            let mut observed = observation();
            observed.related = vec![(ContractInstanceId::new([i as u8; 32]), vec![i as u8; 16])];
            let _evicted = record(&mut samplers, &SamplingScope::Wide, observed);
        }
        let tracked = samplers.values().next().expect("one contract tracked");
        assert!(
            tracked.related.len() <= MAX_RELATED_CONTRACTS,
            "related contracts must be capped, held {}",
            tracked.related.len()
        );

        // A single oversized related state is refused outright rather than
        // displacing everything already held.
        let held_before = tracked.related.len();
        let mut huge = observation();
        huge.related = vec![(
            ContractInstanceId::new([200; 32]),
            vec![0u8; test_related_budgets().0 + 1],
        )];
        let _evicted = record(&mut samplers, &SamplingScope::Wide, huge);
        let tracked = samplers.values().next().expect("one contract tracked");
        assert_eq!(
            tracked.related.len(),
            held_before,
            "an oversized related state must be refused, not admitted or swapped in"
        );
        assert!(
            tracked.related.values().map(Vec::len).sum::<usize>() <= test_related_budgets().1,
            "the related-state allowance must hold"
        );
    }

    /// A full queue must skip the copies, not merely discard them afterwards.
    ///
    /// An `Observation` owns full copies of the base, incoming and result states, so
    /// building one on a contract with large states costs about a megabyte of
    /// allocate-and-copy. If the queue is checked only after that work, the drop path
    /// becomes the most expensive path — on the merge path, and exactly under the load
    /// that causes drops. Counting the builds is the only way to see the difference:
    /// the observable outcome (dropped counter increments) is identical either way.
    #[tokio::test]
    async fn a_full_queue_skips_building_the_observation_entirely() {
        let (tx, rx) = mpsc::channel(1);
        let handle = CaptureHandle {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            queued_bytes: Arc::new(AtomicUsize::new(0)),
        };

        let builds = std::cell::Cell::new(0usize);
        let build = || {
            builds.set(builds.get() + 1);
            observation()
        };

        // First offer fits the one-slot queue.
        handle.observe_with(observation().queued_bytes(), build);
        assert_eq!(builds.get(), 1, "the first observation should be built");
        assert_eq!(handle.dropped(), 0);

        // Queue is now full: the closure must not run at all.
        handle.observe_with(observation().queued_bytes(), build);
        assert_eq!(
            builds.get(),
            1,
            "a full queue must not pay for the copies; the closure ran anyway"
        );
        assert_eq!(handle.dropped(), 1, "the drop must still be counted");

        drop(rx);
        // Receiver gone: still no build, still counted.
        handle.observe_with(observation().queued_bytes(), build);
        assert_eq!(builds.get(), 1, "a dead writer must not pay for the copies");
        assert_eq!(handle.dropped(), 2);
    }

    #[tokio::test]
    async fn a_written_bundle_identifies_its_contract() {
        let mut samplers = HashMap::new();
        let _evicted = record(&mut samplers, &SamplingScope::Wide, observation());

        let dir = tempfile::TempDir::new().expect("tempdir");
        write_all(dir.path(), &samplers, 0).await;

        let path = dir
            .path()
            .join(format!("{}.bundle", ContractInstanceId::new([1; 32])));
        let bundle = super::super::bundle::ReplayBundle::read_from(&path).expect("read back");
        assert_eq!(bundle.code_hash, Some([2; 32]));
        assert_eq!(bundle.parameters, vec![3]);
        assert!(bundle.instance.is_some());
        assert!(
            bundle.resolve_code(Some(vec![9, 9])).is_err(),
            "a bundle must refuse code that does not match the contract it recorded"
        );
    }

    /// A contract whose states all exceed the per-state ceiling must not leave a
    /// bundle behind.
    ///
    /// Found on a live capture: one contract produced a 200-byte bundle holding no
    /// states at all, and replaying it reported only "the corpus is empty". That
    /// reads as "this contract never merged anything", when in fact it merged
    /// constantly and every observation was refused for size. An empty file that
    /// looks like evidence is worse than no file, because it answers a question it
    /// never actually examined.
    #[tokio::test]
    async fn a_contract_whose_states_are_all_oversized_leaves_no_misleading_bundle() {
        let mut samplers = HashMap::new();

        let mut oversized = observation();
        let ceiling = SamplerConfig::default().max_state_bytes;
        oversized.base_state = vec![7; ceiling + 1];
        oversized.incoming_state = Some(vec![8; ceiling + 1]);
        oversized.result_state = vec![9; ceiling + 1];
        let _evicted = record(&mut samplers, &SamplingScope::Wide, oversized);

        assert_eq!(
            samplers
                .values()
                .map(|tracked| tracked.refused_too_large)
                .sum::<u64>(),
            1,
            "the refusal must be counted where it happens, not inferred afterwards"
        );

        let dir = tempfile::TempDir::new().expect("tempdir");
        write_all(dir.path(), &samplers, 0).await;

        let path = dir
            .path()
            .join(format!("{}.bundle", ContractInstanceId::new([1; 32])));
        assert!(
            !path.exists(),
            "a bundle holding no states must not be written: it replays as an \
             empty corpus and invites the reader to conclude the contract was quiet"
        );
    }

    /// Regression: a restart must not destroy the corpus.
    ///
    /// The worker previously started with empty samplers, so the first flush after
    /// a restart overwrote each bundle with only what had been seen since boot. A
    /// capture is worth having because it accumulates diversity over hours, and this
    /// failure is invisible from outside — the file is still there, still recent,
    /// just thinner. Found by measuring a real capture rather than by any test,
    /// which is why this one exists.
    #[tokio::test]
    async fn a_restart_resumes_from_what_is_already_on_disk() {
        let dir = tempfile::TempDir::new().expect("tempdir");

        // First run: observe several distinct states.
        let mut samplers = HashMap::new();
        for i in 0..6u8 {
            let mut obs = observation();
            obs.base_state = vec![i; 16];
            obs.result_state = vec![i; 17];
            let _evicted = record(&mut samplers, &SamplingScope::Wide, obs);
        }
        write_all(dir.path(), &samplers, 0).await;
        let before = samplers
            .values()
            .next()
            .expect("one contract")
            .sampler
            .distinct_states();
        assert!(before > 1, "fixture did not accumulate anything to lose");

        // Restart: a fresh worker reloading the same directory.
        let resumed = reload(dir.path());
        let after = resumed
            .values()
            .next()
            .expect("contract should have been reloaded")
            .sampler
            .distinct_states();

        assert_eq!(
            after, before,
            "restart lost sampled states: had {before}, resumed with {after}"
        );
        assert_eq!(
            resumed.values().next().unwrap().code_hash,
            [2; 32],
            "restart lost the contract identity, so the corpus could no longer be \
             verified against the WASM it came from"
        );
    }

    /// The test above exercises `reload` directly, which is NOT enough on its own:
    /// the actual bug was that `run_writer` never called it, and reverting that one
    /// line left the test green. So the wiring is pinned separately.
    ///
    /// `run_writer` is a long-lived task driving a channel and a timer, so calling
    /// it from a unit test would mean orchestrating a shutdown to observe the
    /// result. A source pin buys the same guarantee for a fraction of the
    /// complexity, and the thing being guarded is precisely a one-line call site.
    #[test]
    fn the_writer_actually_resumes_on_startup() {
        let src = include_str!("capture.rs");
        let start = src
            .find("async fn run_writer(")
            .expect("run_writer not found");
        let after = &src[start..];
        // Anchored on the name, not on the visibility keyword in front of it: this
        // pin already broke once because `TrackedContract` gained `pub(crate)`, which
        // says nothing about what the pin is guarding. It still `expect`s rather than
        // defaulting, so a genuinely moved anchor fails loudly instead of silently
        // widening the region to the rest of the file.
        let end = after
            .find("struct TrackedContract")
            .expect("run_writer no longer precedes TrackedContract");
        let body = &after[..end];
        assert!(
            body.contains("reload(&dir)"),
            "run_writer no longer reloads existing bundles on startup, so a node \
             restart will overwrite each capture with only what it has seen since \
             boot — silently, because the file is still there and still recent"
        );
    }

    /// An unreadable bundle must not stop the node capturing everything else.
    #[test]
    fn a_corrupt_bundle_is_skipped_rather_than_fatal() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        std::fs::write(dir.path().join("junk.bundle"), b"not a bundle at all").expect("write");
        assert!(reload(dir.path()).is_empty());
    }

    /// The number of contracts tracked is bounded, so a node hosting thousands
    /// cannot grow the capture without limit.
    #[test]
    fn the_number_of_tracked_contracts_is_bounded() {
        let mut samplers = HashMap::new();
        for i in 0..(MAX_TRACKED_CONTRACTS + 50) {
            let mut obs = observation();
            obs.contract = ContractInstanceId::new([(i % 251) as u8; 32]);
            let _evicted = record(&mut samplers, &SamplingScope::Wide, obs);
        }
        assert!(samplers.len() <= MAX_TRACKED_CONTRACTS);
    }

    #[test]
    fn the_byte_budget_override_is_honoured_and_bad_input_falls_back() {
        let default = sampler_config_from(None);

        let raised = sampler_config_from(Some(" 33554432 "));
        assert_eq!(raised.max_bytes, 33_554_432, "override should be applied");
        assert!(
            raised.max_state_bytes >= default.max_state_bytes,
            "raising the total budget must never lower the per-state ceiling"
        );
        assert!(
            raised.max_state_bytes < raised.max_bytes,
            "a per-state ceiling at or above the whole budget would let one state \
             evict every other sample"
        );

        // The lowering direction, which an earlier version got backwards: it used
        // `max`, so a budget below the shipped default left the ceiling ABOVE the
        // whole budget. One state could then exclude every other sample, and states
        // were refused as `NoBudget` rather than `TooLarge`, which made the
        // "retained nothing" warning name the wrong cause.
        let lowered = sampler_config_from(Some("4096"));
        assert_eq!(lowered.max_bytes, 4096);
        assert!(
            lowered.max_state_bytes < lowered.max_bytes,
            "lowering the total budget must lower the per-state ceiling with it: \
             ceiling {} against a total of {}",
            lowered.max_state_bytes,
            lowered.max_bytes
        );
        assert!(
            lowered.max_state_bytes >= 1,
            "the ceiling must never reach zero, which would admit nothing at all"
        );

        // Anything unparseable or zero leaves the shipped default in place rather
        // than silently disabling capture, which a `0` budget would do.
        for bad in ["", "0", "lots", "-1", "4MB"] {
            assert_eq!(
                sampler_config_from(Some(bad)).max_bytes,
                default.max_bytes,
                "{bad:?} should fall back to the default budget"
            );
        }
    }
}
