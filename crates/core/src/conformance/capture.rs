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

use super::sampler::{ContractSampler, SamplerConfig};

/// Environment variable that switches capture on and says where to write.
pub const CAPTURE_DIR_ENV: &str = "FREENET_CONFORMANCE_CAPTURE_DIR";

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
    /// Offer an observation. Never blocks, never fails visibly.
    pub fn observe(&self, observation: Observation) {
        if self.tx.try_send(observation).is_err() {
            // Queue full or writer gone. Count it and carry on: a stalled capture
            // must never become a stalled merge.
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
    let mut samplers: HashMap<ContractInstanceId, TrackedContract> = HashMap::new();
    let mut flush = tokio::time::interval(FLUSH_EVERY);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            received = rx.recv() => {
                let Some(observation) = received else { break };
                record(&mut samplers, observation);
            }
            _ = flush.tick() => {
                write_all(&dir, &samplers, dropped.load(Ordering::Relaxed));
            }
        }
    }

    // Channel closed: the node is going away. Write what we have.
    write_all(&dir, &samplers, dropped.load(Ordering::Relaxed));
}

struct TrackedContract {
    sampler: ContractSampler,
    code_hash: [u8; 32],
    parameters: Vec<u8>,
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
            sampler: ContractSampler::new(SamplerConfig::default()),
            code_hash: observation.code_hash,
            parameters: observation.parameters.clone(),
        });

    tracked.sampler.observe_transition(
        &observation.base_state,
        observation.incoming_state.as_deref(),
        observation.delta.as_deref(),
        None,
        &observation.result_state,
    );
}

fn write_all(dir: &Path, samplers: &HashMap<ContractInstanceId, TrackedContract>, dropped: u64) {
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

        let path = dir.join(format!("{instance}.bundle"));
        if let Err(err) = bundle.write_to(&path) {
            // Capture failing to write must not escalate. Log and move on.
            tracing::warn!(error = %err, path = %path.display(), "could not write capture bundle");
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
    #[test]
    fn a_written_bundle_identifies_its_contract() {
        let mut samplers = HashMap::new();
        record(&mut samplers, observation());

        let dir = tempfile::TempDir::new().expect("tempdir");
        write_all(dir.path(), &samplers, 0);

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
}
