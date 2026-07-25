//! Which fan-out arm chose a broadcast payload, and how many bytes it put on
//! the wire.
//!
//! ## Why
//!
//! A production measurement on 2026-07-24 (posted to #3335) found that the
//! fleet sends ~60 GB per 3 h, that ~85 % of byte-weighted broadcast work is
//! on contracts holding >= 500 KB of state, and that 97.9 % of it lands on
//! contracts whose applies change nothing. What it could NOT determine is
//! *why* those large states go out whole, because
//! [`broadcast_to_single_peer`] has **four distinct paths that send FULL
//! STATE** and none of them were separately instrumented:
//!
//! 1. [`PayloadArm::FullDeltaSuppressed`] — the `delta_incompat` memo is armed
//!    (#4904): the contract is known to reject every delta, so full state is
//!    sent deliberately.
//! 2. [`PayloadArm::FullNotEfficient`] — the wire-efficiency gate
//!    ([`crate::ring::interest::is_delta_efficient`]) refused *before*
//!    computing anything, because the peer's summary is >= 50 % of our state
//!    size. Note the fallback is full state, which is never smaller than the
//!    delta the gate declined to compute.
//! 3. [`PayloadArm::FullComputeFailed`] — the contract's WASM failed, timed
//!    out, or answered unexpectedly.
//! 4. [`PayloadArm::FullNoSummary`] — we hold no summary for one side of the
//!    pair, so there is nothing to diff against (first sync, or a summary
//!    cleared by a delta-apply failure).
//!
//! Those four have completely different remedies, so knowing which one emits
//! the bytes is the difference between a targeted fix and a guess. Deltas are
//! counted too ([`PayloadArm::Delta`]) so the mix is a ratio rather than a
//! bare count.
//!
//! ## What is counted
//!
//! Bytes are recorded at the **real-delivery** sites only — the same points
//! that charge [`ResourceType::BroadcastFanoutCost`][rt] — so a dropped,
//! timed-out, or failed-to-enqueue send never inflates the mix with phantom
//! fan-out. This deliberately mirrors the #4903 review round-3 Fix 4
//! accounting rule; see `broadcast_queue::broadcast_to_single_peer`.
//!
//! [rt]: crate::topology::meter::ResourceType::BroadcastFanoutCost
//!
//! ## Cost
//!
//! One short uncontended mutex acquire per **delivered broadcast** (not per
//! packet), covering a handful of integer adds and at most one bounded
//! `HashMap` touch. Everything else happens in the aggregator task, and the
//! hot path only ever WRITES. The lock is what makes a rollup a consistent
//! snapshot; see [`PayloadMix`] for why per-field atomics were not enough.
//!
//! [`broadcast_to_single_peer`]: super::broadcast_queue::broadcast_to_single_peer

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;

use crate::node::background_task_monitor::BackgroundTaskMonitor;

/// Rollup cadence. Broadcasts are far less frequent than packets, so this is
/// a minute rather than the 1 Hz sampling the `shadow_demand` aggregators use;
/// one event per minute per node is a negligible addition to the telemetry
/// volume this is meant to explain.
const ROLLUP_WINDOW: Duration = Duration::from_secs(60);

/// Per-contract attribution cap for one window.
///
/// Bounded because the key is contract-controlled: any peer can fan out a
/// contract we host, so an unbounded map here would be an amplification
/// vector (see `.claude/rules/code-style.md`, "per-key collections"). Entries
/// beyond the cap are still counted in the per-arm totals; only their
/// per-contract attribution is dropped, and both the count and the BYTE total
/// of those unattributed sends are reported (`attribution_dropped_sends` /
/// `attribution_dropped_bytes`) so a truncated window is never mistaken for a
/// complete one.
const MAX_TRACKED_CONTRACTS: usize = 256;

/// How many contracts the emitted rollup names.
const TOP_CONTRACTS_REPORTED: usize = 10;

/// Which arm of the fan-out's payload selection produced a broadcast.
///
/// See the module docs for what each arm means and why the split matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PayloadArm {
    /// A delta was computed and sent.
    Delta,
    /// Full state: the `delta_incompat` memo is armed for this contract.
    FullDeltaSuppressed,
    /// Full state: the wire-efficiency gate refused to compute a delta.
    FullNotEfficient,
    /// Full state: delta computation was attempted and failed.
    FullComputeFailed,
    /// Full state: no cached summary for one side of the pair.
    FullNoSummary,
}

impl PayloadArm {
    /// Every arm, in reporting order. Exhaustive by construction: the
    /// `match` in [`PayloadArm::index`] fails to compile if a variant is
    /// added without being listed here.
    pub(crate) const ALL: [PayloadArm; 5] = [
        PayloadArm::Delta,
        PayloadArm::FullDeltaSuppressed,
        PayloadArm::FullNotEfficient,
        PayloadArm::FullComputeFailed,
        PayloadArm::FullNoSummary,
    ];

    const COUNT: usize = Self::ALL.len();

    const fn index(self) -> usize {
        match self {
            PayloadArm::Delta => 0,
            PayloadArm::FullDeltaSuppressed => 1,
            PayloadArm::FullNotEfficient => 2,
            PayloadArm::FullComputeFailed => 3,
            PayloadArm::FullNoSummary => 4,
        }
    }

    /// Stable wire label. Used as the telemetry field prefix, so changing one
    /// breaks existing dashboard queries.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            PayloadArm::Delta => "delta",
            PayloadArm::FullDeltaSuppressed => "full_delta_suppressed",
            PayloadArm::FullNotEfficient => "full_not_efficient",
            PayloadArm::FullComputeFailed => "full_compute_failed",
            PayloadArm::FullNoSummary => "full_no_summary",
        }
    }

    /// Whether this arm put a whole contract state on the wire.
    pub(crate) const fn is_full_state(self) -> bool {
        !matches!(self, PayloadArm::Delta)
    }
}

/// Process-wide payload-mix accumulator. See the module docs.
pub(crate) static BROADCAST_PAYLOAD_MIX: LazyLock<PayloadMix> = LazyLock::new(PayloadMix::new);

/// One rollup window's counters.
///
/// Every field advances together under a single lock so a rollup takes a
/// consistent snapshot; see [`PayloadMix`].
struct Window {
    sends: [u64; PayloadArm::COUNT],
    bytes: [u64; PayloadArm::COUNT],
    /// Per-contract full-state byte attribution, bounded at
    /// [`MAX_TRACKED_CONTRACTS`].
    contract_full_state_bytes: HashMap<ContractInstanceId, u64>,
    /// Full-state sends that could not be attributed to a contract because
    /// the cap was already reached. This counts SENDS, not distinct
    /// contracts: one over-cap contract broadcasting 1,000 times contributes
    /// 1,000. Naming it for what it counts avoids the "1,000 contracts were
    /// dropped" misreading; distinct-contract cardinality would need an
    /// unbounded set, which is exactly what the cap exists to prevent.
    attribution_dropped_sends: u64,
    /// Bytes behind those unattributed sends. This is the field that says
    /// how much of `full_state_bytes` the top-N list cannot account for, so
    /// a truncated window is never mistaken for a complete one.
    attribution_dropped_bytes: u64,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            sends: [0; PayloadArm::COUNT],
            bytes: [0; PayloadArm::COUNT],
            contract_full_state_bytes: HashMap::new(),
            attribution_dropped_sends: 0,
            attribution_dropped_bytes: 0,
        }
    }
}

/// Per-arm send/byte counters plus bounded per-contract full-state
/// attribution.
///
/// All state lives on the struct rather than in free statics so tests can
/// instantiate an isolated accumulator: a shared global would make the unit
/// tests order-dependent and intermittently failing, which this repo treats
/// as a broken test, not an acceptable one.
///
/// ## Why one mutex rather than per-field atomics
///
/// The first version used relaxed atomics plus a `DashMap`. External review
/// caught two defects that are both really the same defect: a rollup drained
/// the arm counters and the contract map at different instants, so (a) an
/// increment landing between sampling a map entry and clearing it was lost
/// outright, and (b) a single broadcast could be counted in one window for
/// the aggregate fields and the next for the per-contract fields, leaving
/// `top_contracts_by_full_state_bytes` unable to reconcile against
/// `full_state_bytes`. For an accuracy-measurement feature that is a real
/// bug, not a rounding detail.
///
/// A single lock covering the whole window makes record and drain atomic
/// with respect to each other, which is the property the measurement needs.
/// The cost is acceptable because this is per *delivered broadcast*, not per
/// packet: an uncontended `parking_lot::Mutex` acquire is on the order of a
/// few atomic operations, and the surrounding send path has already done WASM
/// delta computation and a network write. Note this is the documented
/// exception in `.claude/rules/code-style.md` to preferring `DashMap` — we
/// need an atomic read-modify-write across MULTIPLE keys (all arms plus the
/// contract map) in one transaction, which is precisely when a global lock is
/// required.
pub(crate) struct PayloadMix {
    window: Mutex<Window>,
}

impl PayloadMix {
    fn new() -> Self {
        Self {
            window: Mutex::new(Window::default()),
        }
    }

    /// Record one **delivered** broadcast.
    ///
    /// Call this only where [`ResourceType::BroadcastFanoutCost`][rt] is
    /// charged, so the mix and the cost axis agree on what "sent" means.
    ///
    /// [rt]: crate::topology::meter::ResourceType::BroadcastFanoutCost
    pub(crate) fn record_delivered(
        &self,
        arm: PayloadArm,
        contract: &ContractInstanceId,
        payload_bytes: usize,
    ) {
        let bytes = payload_bytes as u64;
        let idx = arm.index();
        let mut w = self.window.lock();
        // Saturating throughout: a wrapped counter would silently report a
        // tiny number for the heaviest contract, which is the opposite of
        // what this measurement is for.
        w.sends[idx] = w.sends[idx].saturating_add(1);
        w.bytes[idx] = w.bytes[idx].saturating_add(bytes);
        if arm.is_full_state() {
            if let Some(tally) = w.contract_full_state_bytes.get_mut(contract) {
                *tally = tally.saturating_add(bytes);
            } else if w.contract_full_state_bytes.len() < MAX_TRACKED_CONTRACTS {
                w.contract_full_state_bytes.insert(*contract, bytes);
            } else {
                w.attribution_dropped_sends = w.attribution_dropped_sends.saturating_add(1);
                w.attribution_dropped_bytes = w.attribution_dropped_bytes.saturating_add(bytes);
            }
        }
    }

    /// Atomically take the current window, leaving a fresh empty one.
    ///
    /// One lock acquisition covers every field, so the aggregate counters and
    /// the per-contract tallies always describe the same set of broadcasts.
    fn take_window(&self) -> Window {
        std::mem::take(&mut *self.window.lock())
    }
}

impl Window {
    /// Per-arm `(sends, bytes)` in [`PayloadArm::ALL`] order.
    fn arms(&self) -> Vec<(PayloadArm, u64, u64)> {
        PayloadArm::ALL
            .iter()
            .map(|arm| {
                let idx = arm.index();
                (*arm, self.sends[idx], self.bytes[idx])
            })
            .collect()
    }

    /// The top [`TOP_CONTRACTS_REPORTED`] contracts by full-state bytes.
    fn top_contracts(&self) -> Vec<(ContractInstanceId, u64)> {
        let mut tallies: Vec<(ContractInstanceId, u64)> = self
            .contract_full_state_bytes
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();
        // `ContractInstanceId` has no `Ord`, so tie-break on its raw bytes: the
        // reported top-N must be stable across nodes and windows, otherwise a tie
        // reorders on every rollup and looks like churn.
        tallies.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        tallies.truncate(TOP_CONTRACTS_REPORTED);
        tallies
    }
}

/// Build the `broadcast_payload_mix` rollup JSON.
///
/// Pure so the schema is unit-testable without the telemetry sender, matching
/// the `shadow_demand` rollup builders.
fn payload_mix_json(
    arms: &[(PayloadArm, u64, u64)],
    contracts: &[(ContractInstanceId, u64)],
    attribution_dropped_sends: u64,
    attribution_dropped_bytes: u64,
    window_secs: u64,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    let mut total_sends = 0u64;
    let mut total_bytes = 0u64;
    let mut full_state_bytes = 0u64;
    for (arm, sends, bytes) in arms {
        obj.insert(format!("{}_sends", arm.label()), (*sends).into());
        obj.insert(format!("{}_bytes", arm.label()), (*bytes).into());
        total_sends += sends;
        total_bytes += bytes;
        if arm.is_full_state() {
            full_state_bytes += bytes;
        }
    }
    obj.insert("total_sends".into(), total_sends.into());
    obj.insert("total_bytes".into(), total_bytes.into());
    obj.insert("full_state_bytes".into(), full_state_bytes.into());
    // The headline ratio: of everything we actually put on the wire, how much
    // was a whole state rather than a diff.
    let full_state_share = if total_bytes == 0 {
        0.0
    } else {
        full_state_bytes as f64 / total_bytes as f64
    };
    obj.insert(
        "full_state_byte_share".into(),
        serde_json::Number::from_f64(full_state_share)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
    );
    obj.insert(
        "top_contracts_by_full_state_bytes".into(),
        serde_json::Value::Array(
            contracts
                .iter()
                .map(
                    |(id, bytes)| serde_json::json!({ "contract": id.to_string(), "bytes": bytes }),
                )
                .collect(),
        ),
    );
    // Sends (not distinct contracts) that missed attribution, and the bytes
    // behind them: `attribution_dropped_bytes` is what tells an analyst how
    // much of `full_state_bytes` the top-N list cannot account for.
    obj.insert(
        "attribution_dropped_sends".into(),
        attribution_dropped_sends.into(),
    );
    obj.insert(
        "attribution_dropped_bytes".into(),
        attribution_dropped_bytes.into(),
    );
    obj.insert("window_secs".into(), window_secs.into());
    serde_json::Value::Object(obj)
}

/// Emit one `broadcast_payload_mix` rollup and reset the window.
///
/// Returns the payload so callers (and tests) can inspect what was sent.
pub(crate) fn emit_payload_mix_rollup(local_peer_id: &str, window_secs: u64) -> serde_json::Value {
    // ONE atomic take: the arm counters and the per-contract tallies describe
    // exactly the same set of broadcasts, so the top-N list always reconciles
    // against `full_state_bytes`.
    let window = BROADCAST_PAYLOAD_MIX.take_window();
    let payload = payload_mix_json(
        &window.arms(),
        &window.top_contracts(),
        window.attribution_dropped_sends,
        window.attribution_dropped_bytes,
        window_secs,
    );
    crate::tracing::telemetry::send_standalone_shadow_event_with_peer_id(
        "broadcast_payload_mix",
        local_peer_id,
        payload.clone(),
    );
    payload
}

/// Spawn the `broadcast_payload_mix` aggregator and register it with the
/// [`BackgroundTaskMonitor`].
///
/// Always-on and cheap: it only swaps atomics and drains a bounded map once
/// per [`ROLLUP_WINDOW`]. Observation only — nothing reads these counters to
/// make a decision, and nothing on the hot path ever reads them at all.
pub(crate) fn spawn_payload_mix_aggregator(local_peer_id: String, monitor: &BackgroundTaskMonitor) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ROLLUP_WINDOW);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip the immediate first tick
        loop {
            ticker.tick().await;
            emit_payload_mix_rollup(&local_peer_id, ROLLUP_WINDOW.as_secs());
        }
    });
    monitor.register("broadcast_payload_mix_aggregator", handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    /// Taking the window leaves the accumulator empty so consecutive rollups
    /// report windows, not lifetime totals.
    #[test]
    fn take_window_resets_the_window() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::Delta, &contract(1), 100);
        let first = mix.take_window().arms();
        assert_eq!(first[PayloadArm::Delta.index()].1, 1);
        assert_eq!(first[PayloadArm::Delta.index()].2, 100);
        let second = mix.take_window().arms();
        assert!(
            second
                .iter()
                .all(|(_, sends, bytes)| *sends == 0 && *bytes == 0),
            "second take must be empty, got {second:?}"
        );
    }

    /// Every arm lands in its own bucket — a mis-indexed arm would silently
    /// attribute bytes to the wrong cause, which is the entire point of this
    /// module.
    #[test]
    fn each_arm_counts_separately() {
        let mix = PayloadMix::new();
        for (i, arm) in PayloadArm::ALL.iter().enumerate() {
            for _ in 0..=i {
                mix.record_delivered(*arm, &contract(i as u8), 10);
            }
        }
        let drained = mix.take_window().arms();
        for (i, (arm, sends, bytes)) in drained.iter().enumerate() {
            assert_eq!(*arm, PayloadArm::ALL[i]);
            assert_eq!(*sends, i as u64 + 1, "wrong send count for {arm:?}");
            assert_eq!(*bytes, (i as u64 + 1) * 10, "wrong byte count for {arm:?}");
        }
    }

    /// The aggregate arm counters and the per-contract tallies must always
    /// describe the SAME set of broadcasts.
    ///
    /// Regression for the external-review finding: the first version drained
    /// the arm counters and the contract map at two different instants, so a
    /// broadcast landing in between was counted in one window for
    /// `full_state_bytes` and the next for the per-contract list (and an
    /// increment racing the map `clear()` was lost outright). This asserts the
    /// invariant an analyst actually relies on: the top-N list reconciles
    /// against the full-state total.
    #[test]
    fn per_contract_tallies_reconcile_with_arm_totals() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::FullNotEfficient, &contract(1), 500);
        mix.record_delivered(PayloadArm::FullNoSummary, &contract(2), 300);
        mix.record_delivered(PayloadArm::FullDeltaSuppressed, &contract(1), 200);
        mix.record_delivered(PayloadArm::Delta, &contract(3), 50); // not full state

        let window = mix.take_window();
        let full_state_total: u64 = window
            .arms()
            .iter()
            .filter(|(arm, _, _)| arm.is_full_state())
            .map(|(_, _, bytes)| bytes)
            .sum();
        let attributed: u64 = window.contract_full_state_bytes.values().sum();
        assert_eq!(
            full_state_total, 1000,
            "full-state arm bytes should exclude the delta send"
        );
        assert_eq!(
            attributed + window.attribution_dropped_bytes,
            full_state_total,
            "every full-state byte must be either attributed to a contract or \
             counted as dropped attribution — otherwise the top-N list cannot \
             be reconciled against full_state_bytes"
        );
        // Contract 1 accumulated across two different full-state arms.
        assert_eq!(window.contract_full_state_bytes[&contract(1)], 700);
        assert_eq!(window.contract_full_state_bytes[&contract(2)], 300);
    }

    /// Concurrent recorders racing a rollup must not lose or double-count
    /// bytes: every recorded byte lands in exactly one window.
    #[test]
    fn concurrent_records_racing_a_rollup_conserve_bytes() {
        use std::sync::Arc;

        const THREADS: usize = 8;
        const PER_THREAD: usize = 500;

        let mix = Arc::new(PayloadMix::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // A rollup loop running concurrently with the writers, accumulating
        // what it drains.
        let drained_total = Arc::new(Mutex::new(0u64));
        let drainer = {
            let mix = Arc::clone(&mix);
            let stop = Arc::clone(&stop);
            let drained_total = Arc::clone(&drained_total);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let w = mix.take_window();
                    let sum: u64 = w.arms().iter().map(|(_, _, b)| b).sum();
                    *drained_total.lock() += sum;
                    std::thread::yield_now();
                }
            })
        };

        let writers: Vec<_> = (0..THREADS)
            .map(|t| {
                let mix = Arc::clone(&mix);
                std::thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        mix.record_delivered(PayloadArm::FullNotEfficient, &contract(t as u8), 7);
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        drainer.join().unwrap();

        // Whatever the final drainer pass missed is still in the accumulator.
        let leftover: u64 = mix.take_window().arms().iter().map(|(_, _, b)| b).sum();
        let total = *drained_total.lock() + leftover;
        assert_eq!(
            total,
            (THREADS * PER_THREAD * 7) as u64,
            "bytes were lost or double-counted across a concurrent rollover"
        );
    }

    /// Only full-state arms are counted as full-state bytes; a delta-heavy
    /// node must not look like it is flooding whole states.
    #[test]
    fn full_state_share_excludes_deltas() {
        let arms = vec![
            (PayloadArm::Delta, 3, 300),
            (PayloadArm::FullDeltaSuppressed, 0, 0),
            (PayloadArm::FullNotEfficient, 1, 700),
            (PayloadArm::FullComputeFailed, 0, 0),
            (PayloadArm::FullNoSummary, 0, 0),
        ];
        let json = payload_mix_json(&arms, &[], 0, 0, 60);
        assert_eq!(json["total_bytes"], 1000);
        assert_eq!(json["full_state_bytes"], 700);
        assert_eq!(json["full_state_byte_share"], 0.7);
        assert_eq!(json["delta_sends"], 3);
        assert_eq!(json["full_not_efficient_bytes"], 700);
    }

    /// A window with no traffic must emit 0.0, not NaN — `Number::from_f64`
    /// rejects NaN and the field would silently become null.
    #[test]
    fn empty_window_reports_zero_share_not_nan() {
        let arms: Vec<_> = PayloadArm::ALL.iter().map(|a| (*a, 0, 0)).collect();
        let json = payload_mix_json(&arms, &[], 0, 0, 60);
        assert_eq!(json["full_state_byte_share"], 0.0);
        assert_eq!(json["total_bytes"], 0);
    }

    /// Per-contract attribution is capped, and the overflow is reported as
    /// SENDS and BYTES rather than silently truncated.
    ///
    /// The counter deliberately counts sends, not distinct contracts: an
    /// external reviewer flagged that the original `contracts_dropped` name
    /// implied cardinality while the code incremented per send, so one
    /// over-cap contract broadcasting 1,000 times read as "1,000 contracts
    /// dropped". Tracking true cardinality would need an unbounded set, which
    /// is what the cap exists to prevent, so the field is named for what it
    /// measures and paired with the byte total that makes it actionable.
    #[test]
    fn contract_attribution_is_bounded_and_reports_overflow() {
        let mix = PayloadMix::new();
        for i in 0..(MAX_TRACKED_CONTRACTS + 20) {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            mix.record_delivered(
                PayloadArm::FullNotEfficient,
                &ContractInstanceId::new(raw),
                5,
            );
        }
        let window = mix.take_window();
        assert!(
            window.contract_full_state_bytes.len() <= MAX_TRACKED_CONTRACTS,
            "attribution map exceeded its cap: {}",
            window.contract_full_state_bytes.len()
        );
        assert_eq!(
            window.attribution_dropped_sends, 20,
            "overflow must be reported, not dropped silently"
        );
        assert_eq!(
            window.attribution_dropped_bytes, 100,
            "the bytes behind unattributed sends must be reported too"
        );
        assert!(window.top_contracts().len() <= TOP_CONTRACTS_REPORTED);
        // Taking the window resets everything, so the next window starts clean
        // rather than re-reporting this window's overflow.
        let next = mix.take_window();
        assert!(next.contract_full_state_bytes.is_empty());
        assert_eq!(next.attribution_dropped_sends, 0);
        assert_eq!(next.attribution_dropped_bytes, 0);
    }

    /// One over-cap contract sending repeatedly inflates the SEND count, not
    /// a contract count — the distinction the field name now makes explicit.
    #[test]
    fn repeated_sends_from_one_over_cap_contract_count_as_sends() {
        let mix = PayloadMix::new();
        for i in 0..MAX_TRACKED_CONTRACTS {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            mix.record_delivered(
                PayloadArm::FullNotEfficient,
                &ContractInstanceId::new(raw),
                1,
            );
        }
        // A single additional contract, broadcasting many times.
        let mut raw = [9u8; 32];
        raw[31] = 7;
        let over_cap = ContractInstanceId::new(raw);
        for _ in 0..1000 {
            mix.record_delivered(PayloadArm::FullNotEfficient, &over_cap, 3);
        }
        let window = mix.take_window();
        assert_eq!(window.attribution_dropped_sends, 1000);
        assert_eq!(window.attribution_dropped_bytes, 3000);
        assert!(!window.contract_full_state_bytes.contains_key(&over_cap));
    }

    /// Ties break deterministically on contract id so the reported top-N is
    /// stable across nodes and windows.
    #[test]
    fn top_contracts_sort_is_deterministic() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::FullNoSummary, &contract(9), 100);
        mix.record_delivered(PayloadArm::FullNoSummary, &contract(2), 100);
        mix.record_delivered(PayloadArm::FullNoSummary, &contract(5), 500);
        let top = mix.take_window().top_contracts();
        assert_eq!(top[0].0, contract(5), "largest first");
        assert_eq!(top[1].0, contract(2), "tie broken by contract id");
        assert_eq!(top[2].0, contract(9));
    }

    /// Deltas must never be attributed to a contract's full-state tally.
    #[test]
    fn delta_sends_are_not_attributed_as_full_state() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::Delta, &contract(7), 1234);
        let window = mix.take_window();
        assert!(
            window.contract_full_state_bytes.is_empty(),
            "delta bytes leaked into full-state attribution: {:?}",
            window.contract_full_state_bytes
        );
        // ...but the delta itself is still counted in the per-arm totals.
        assert_eq!(window.arms()[PayloadArm::Delta.index()].2, 1234);
    }

    /// Every full-state arm attributes to the contract; this is what names
    /// the offending contracts in the rollup.
    #[test]
    fn every_full_state_arm_attributes_to_its_contract() {
        for arm in PayloadArm::ALL.iter().filter(|a| a.is_full_state()) {
            let mix = PayloadMix::new();
            mix.record_delivered(*arm, &contract(3), 99);
            let top = mix.take_window().top_contracts();
            assert_eq!(
                top,
                vec![(contract(3), 99)],
                "{arm:?} must attribute its full-state bytes to the contract"
            );
        }
    }
}
