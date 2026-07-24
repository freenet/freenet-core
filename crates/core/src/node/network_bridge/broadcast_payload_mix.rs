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
//! [`broadcast_to_single_peer`] has **three distinct paths that send FULL
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
//! One relaxed atomic add per delivered broadcast on the hot path, plus at
//! most one bounded `DashMap` touch for per-contract attribution. Everything
//! else happens in the aggregator task. Same shape as
//! [`crate::transport::shadow_demand`]; the hot path only ever WRITES.
//!
//! [`broadcast_to_single_peer`]: super::broadcast_queue::broadcast_to_single_peer

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;

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
/// per-contract attribution is dropped, and the count of dropped contracts is
/// reported so a truncated window is never mistaken for a complete one.
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

/// Per-arm send/byte counters plus bounded per-contract full-state
/// attribution.
///
/// All state lives on the struct rather than in free statics so tests can
/// instantiate an isolated accumulator: a shared global would make the unit
/// tests order-dependent and intermittently failing, which this repo treats
/// as a broken test, not an acceptable one.
pub(crate) struct PayloadMix {
    sends: [AtomicU64; PayloadArm::COUNT],
    bytes: [AtomicU64; PayloadArm::COUNT],
    /// Per-contract full-state byte attribution for the current window,
    /// bounded at [`MAX_TRACKED_CONTRACTS`].
    contract_full_state_bytes: DashMap<ContractInstanceId, AtomicU64>,
    /// Contracts whose attribution was dropped this window because the cap
    /// was already reached.
    contracts_dropped: AtomicU64,
}

impl PayloadMix {
    fn new() -> Self {
        Self {
            sends: std::array::from_fn(|_| AtomicU64::new(0)),
            bytes: std::array::from_fn(|_| AtomicU64::new(0)),
            contract_full_state_bytes: DashMap::new(),
            contracts_dropped: AtomicU64::new(0),
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
        let idx = arm.index();
        self.sends[idx].fetch_add(1, Ordering::Relaxed);
        self.bytes[idx].fetch_add(payload_bytes as u64, Ordering::Relaxed);
        if arm.is_full_state() {
            self.attribute_full_state(contract, payload_bytes as u64);
        }
    }

    /// Add `bytes` to a contract's full-state tally, respecting the cap.
    fn attribute_full_state(&self, contract: &ContractInstanceId, bytes: u64) {
        if let Some(entry) = self.contract_full_state_bytes.get(contract) {
            entry.fetch_add(bytes, Ordering::Relaxed);
            return;
        }
        // `len()` is a racy read, so the cap is approximate under concurrency —
        // acceptable for telemetry, and it can only ever overshoot by the number
        // of threads racing here, never grow unboundedly.
        if self.contract_full_state_bytes.len() >= MAX_TRACKED_CONTRACTS {
            self.contracts_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.contract_full_state_bytes
            .entry(*contract)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Drain the per-contract tallies, returning the top
    /// [`TOP_CONTRACTS_REPORTED`] by full-state bytes plus the dropped count.
    fn drain_contracts(&self) -> (Vec<(ContractInstanceId, u64)>, u64) {
        let mut tallies: Vec<(ContractInstanceId, u64)> = self
            .contract_full_state_bytes
            .iter()
            .map(|e| (*e.key(), e.value().load(Ordering::Relaxed)))
            .collect();
        self.contract_full_state_bytes.clear();
        let dropped = self.contracts_dropped.swap(0, Ordering::Relaxed);
        // `ContractInstanceId` has no `Ord`, so tie-break on its raw bytes: the
        // reported top-N must be stable across nodes and windows, otherwise a tie
        // reorders on every rollup and looks like churn.
        tallies.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        tallies.truncate(TOP_CONTRACTS_REPORTED);
        (tallies, dropped)
    }

    /// Drain the counters, returning per-arm `(sends, bytes)` in
    /// [`PayloadArm::ALL`] order. Resets to zero so each rollup reports one
    /// window rather than a lifetime total.
    fn drain(&self) -> Vec<(PayloadArm, u64, u64)> {
        PayloadArm::ALL
            .iter()
            .map(|arm| {
                let idx = arm.index();
                (
                    *arm,
                    self.sends[idx].swap(0, Ordering::Relaxed),
                    self.bytes[idx].swap(0, Ordering::Relaxed),
                )
            })
            .collect()
    }
}

/// Build the `broadcast_payload_mix` rollup JSON.
///
/// Pure so the schema is unit-testable without the telemetry sender, matching
/// the `shadow_demand` rollup builders.
fn payload_mix_json(
    arms: &[(PayloadArm, u64, u64)],
    contracts: &[(ContractInstanceId, u64)],
    contracts_dropped: u64,
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
    obj.insert("contracts_dropped".into(), contracts_dropped.into());
    obj.insert("window_secs".into(), window_secs.into());
    serde_json::Value::Object(obj)
}

/// Emit one `broadcast_payload_mix` rollup and reset the window.
///
/// Returns the payload so callers (and tests) can inspect what was sent.
pub(crate) fn emit_payload_mix_rollup(local_peer_id: &str, window_secs: u64) -> serde_json::Value {
    let arms = BROADCAST_PAYLOAD_MIX.drain();
    let (contracts, dropped) = BROADCAST_PAYLOAD_MIX.drain_contracts();
    let payload = payload_mix_json(&arms, &contracts, dropped, window_secs);
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

    /// Draining leaves the accumulator empty so consecutive rollups report
    /// windows, not lifetime totals.
    #[test]
    fn drain_resets_the_window() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::Delta, &contract(1), 100);
        let first = mix.drain();
        assert_eq!(first[PayloadArm::Delta.index()].1, 1);
        assert_eq!(first[PayloadArm::Delta.index()].2, 100);
        let second = mix.drain();
        assert!(
            second
                .iter()
                .all(|(_, sends, bytes)| *sends == 0 && *bytes == 0),
            "second drain must be empty, got {second:?}"
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
        let drained = mix.drain();
        for (i, (arm, sends, bytes)) in drained.iter().enumerate() {
            assert_eq!(*arm, PayloadArm::ALL[i]);
            assert_eq!(*sends, i as u64 + 1, "wrong send count for {arm:?}");
            assert_eq!(*bytes, (i as u64 + 1) * 10, "wrong byte count for {arm:?}");
        }
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
        let json = payload_mix_json(&arms, &[], 0, 60);
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
        let json = payload_mix_json(&arms, &[], 0, 60);
        assert_eq!(json["full_state_byte_share"], 0.0);
        assert_eq!(json["total_bytes"], 0);
    }

    /// Per-contract attribution is capped, and the overflow is reported
    /// rather than silently truncated.
    #[test]
    fn contract_attribution_is_bounded_and_reports_overflow() {
        let mix = PayloadMix::new();
        for i in 0..(MAX_TRACKED_CONTRACTS + 20) {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            mix.attribute_full_state(&ContractInstanceId::new(raw), 5);
        }
        assert!(
            mix.contract_full_state_bytes.len() <= MAX_TRACKED_CONTRACTS,
            "attribution map exceeded its cap: {}",
            mix.contract_full_state_bytes.len()
        );
        let (top, dropped) = mix.drain_contracts();
        assert_eq!(
            dropped, 20,
            "overflow must be reported, not dropped silently"
        );
        assert!(top.len() <= TOP_CONTRACTS_REPORTED);
        assert!(mix.contract_full_state_bytes.is_empty(), "drain must clear");
        // Draining also resets the dropped counter, so the next window starts
        // clean rather than re-reporting this window's overflow.
        let (_, dropped_again) = mix.drain_contracts();
        assert_eq!(dropped_again, 0, "dropped counter must reset on drain");
    }

    /// Ties break deterministically on contract id so the reported top-N is
    /// stable across nodes and windows.
    #[test]
    fn top_contracts_sort_is_deterministic() {
        let mix = PayloadMix::new();
        mix.attribute_full_state(&contract(9), 100);
        mix.attribute_full_state(&contract(2), 100);
        mix.attribute_full_state(&contract(5), 500);
        let (top, _) = mix.drain_contracts();
        assert_eq!(top[0].0, contract(5), "largest first");
        assert_eq!(top[1].0, contract(2), "tie broken by contract id");
        assert_eq!(top[2].0, contract(9));
    }

    /// Deltas must never be attributed to a contract's full-state tally.
    #[test]
    fn delta_sends_are_not_attributed_as_full_state() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::Delta, &contract(7), 1234);
        let (top, _) = mix.drain_contracts();
        assert!(
            top.is_empty(),
            "delta bytes leaked into full-state attribution: {top:?}"
        );
        // ...but the delta itself is still counted in the per-arm totals.
        let arms = mix.drain();
        assert_eq!(arms[PayloadArm::Delta.index()].2, 1234);
    }

    /// Every full-state arm attributes to the contract; this is what names
    /// the offending contracts in the rollup.
    #[test]
    fn every_full_state_arm_attributes_to_its_contract() {
        for arm in PayloadArm::ALL.iter().filter(|a| a.is_full_state()) {
            let mix = PayloadMix::new();
            mix.record_delivered(*arm, &contract(3), 99);
            let (top, _) = mix.drain_contracts();
            assert_eq!(
                top,
                vec![(contract(3), 99)],
                "{arm:?} must attribute its full-state bytes to the contract"
            );
        }
    }
}
