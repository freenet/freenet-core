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
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Upper bound on contracts sampled concurrently.
const MAX_TRACKED_CONTRACTS: usize = 64;

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
}

/// The executor's end of the capture path.
///
/// Cloning is cheap; the executor holds one and does nothing else with it.
#[derive(Clone)]
pub struct CaptureHandle {
    tx: mpsc::Sender<Observation>,
    dropped: Arc<AtomicU64>,
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
    pub fn observe_with(&self, build: impl FnOnce() -> Observation) {
        match self.tx.try_reserve() {
            Ok(permit) => permit.send(build()),
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
        if self.tx.try_send(observation).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
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
    let dir = std::env::var(CAPTURE_DIR_ENV).ok()?;
    if dir.trim().is_empty() {
        return None;
    }
    match start(PathBuf::from(dir)) {
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
    let handle = CaptureHandle {
        tx,
        dropped: dropped.clone(),
    };

    tracing::info!(
        directory = %dir.display(),
        "conformance capture enabled: recording contract merges for offline replay"
    );
    tokio::spawn(run_writer(dir, rx, dropped));
    Ok(handle)
}

async fn run_writer(dir: PathBuf, mut rx: mpsc::Receiver<Observation>, dropped: Arc<AtomicU64>) {
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
    let mut flush = tokio::time::interval(FLUSH_EVERY);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            received = rx.recv() => {
                let Some(observation) = received else { break };
                record(&mut samplers, observation);
            }
            _ = flush.tick() => {
                write_all(&dir, &samplers, dropped.load(Ordering::Relaxed)).await;
            }
        }
    }

    // Channel closed: the node is going away. Write what we have.
    write_all(&dir, &samplers, dropped.load(Ordering::Relaxed)).await;
}

struct TrackedContract {
    sampler: ContractSampler,
    code_hash: [u8; 32],
    parameters: Vec<u8>,
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

    for entry in entries.flatten() {
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
        samplers.insert(
            instance,
            TrackedContract {
                sampler,
                code_hash,
                parameters: bundle.parameters,
                // Not persisted in the bundle: this counts what THIS process
                // refused, and a reloaded corpus has no refusals of its own yet.
                refused_too_large: 0,
            },
        );
    }
    samplers
}

fn record(samplers: &mut HashMap<ContractInstanceId, TrackedContract>, observation: Observation) {
    if !samplers.contains_key(&observation.contract) && samplers.len() >= MAX_TRACKED_CONTRACTS {
        // Already tracking as many as allowed. Ignoring the newcomer is the bounded
        // choice; the alternative is evicting an accumulated sample, which throws
        // away the diversity this is collecting.
        return;
    }

    let tracked = samplers
        .entry(observation.contract)
        .or_insert_with(|| TrackedContract {
            sampler: ContractSampler::new(sampler_config()),
            code_hash: observation.code_hash,
            parameters: observation.parameters.clone(),
            refused_too_large: 0,
        });

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
        // No embedded code: the WASM lives in the node's contract store and would
        // multiply the size of every bundle. The code hash identifies it, and
        // `ReplayBundle::resolve_code` verifies whatever is supplied at replay time
        // against that hash — so a bundle can never be replayed against the wrong
        // contract even though it does not carry the contract.
        let mut bundle =
            tracked
                .sampler
                .to_bundle(None, Some(tracked.code_hash), tracked.parameters.clone());
        bundle.instance = Some(*instance);
        bundle.note = Some(format!(
            "captured by freenet {} ({} observation(s) dropped node-wide)",
            env!("CARGO_PKG_VERSION"),
            dropped,
        ));

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

        let path = dir.join(format!("{instance}.bundle"));
        // Encode on this task (pure CPU, no syscall) and hand only the bytes to the
        // async write, so the syscalls yield rather than parking the worker.
        match bundle.encode() {
            Ok(bytes) => {
                if let Err(err) = tokio::fs::write(&path, bytes).await {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn observation() -> Observation {
        Observation {
            contract: ContractInstanceId::new([1; 32]),
            code_hash: [2; 32],
            parameters: vec![3],
            base_state: vec![1, 2],
            incoming_state: Some(vec![2, 3]),
            delta: None,
            result_state: vec![1, 2, 3],
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
        };
        handle.observe(observation());
        assert_eq!(handle.dropped(), 1);
    }

    /// Capture is off unless asked for. If this ever returns a handle from an
    /// unset environment, every node in the network starts recording user state.
    #[test]
    fn capture_is_off_when_the_environment_does_not_ask_for_it() {
        // SAFETY: this test is the only reader or writer of this variable, and it
        // does not spawn threads, so no other thread can observe the environment
        // mid-mutation.
        unsafe {
            std::env::remove_var(CAPTURE_DIR_ENV);
        }
        assert!(start_from_env().is_none());

        // SAFETY: as above.
        unsafe {
            std::env::set_var(CAPTURE_DIR_ENV, "   ");
        }
        assert!(
            start_from_env().is_none(),
            "a blank setting must not enable capture"
        );
        // SAFETY: as above.
        unsafe {
            std::env::remove_var(CAPTURE_DIR_ENV);
        }
    }

    /// Bundles must name the contract they came from, or they cannot be replayed
    /// safely: `resolve_code` refuses a bundle with no code hash precisely so a
    /// corpus can never be checked against an unrelated WASM.
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
        };

        let builds = std::cell::Cell::new(0usize);
        let build = || {
            builds.set(builds.get() + 1);
            observation()
        };

        // First offer fits the one-slot queue.
        handle.observe_with(build);
        assert_eq!(builds.get(), 1, "the first observation should be built");
        assert_eq!(handle.dropped(), 0);

        // Queue is now full: the closure must not run at all.
        handle.observe_with(build);
        assert_eq!(
            builds.get(),
            1,
            "a full queue must not pay for the copies; the closure ran anyway"
        );
        assert_eq!(handle.dropped(), 1, "the drop must still be counted");

        drop(rx);
        // Receiver gone: still no build, still counted.
        handle.observe_with(build);
        assert_eq!(builds.get(), 1, "a dead writer must not pay for the copies");
        assert_eq!(handle.dropped(), 2);
    }

    #[tokio::test]
    async fn a_written_bundle_identifies_its_contract() {
        let mut samplers = HashMap::new();
        record(&mut samplers, observation());

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
        record(&mut samplers, oversized);

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
            record(&mut samplers, obs);
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
        let end = after
            .find("\nstruct TrackedContract")
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
            record(&mut samplers, obs);
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
