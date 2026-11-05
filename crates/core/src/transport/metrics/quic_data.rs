//! Telemetry of QUIC data paths (#5940, e5b-8).
//!
//! Node-wide counts of what the paths' data sends and receives did, gathered from
//! the per-path counters (`QuicPathStats`) where each is recorded, so a count is
//! made once, at the decision. Exported per period in `transport_snapshot`, each
//! count only when non-zero and the gauge always. A family is the keys of one
//! label list under one prefix; no other key starts with a family's prefix, or
//! with the prefix its sibling families share (`quic_data_`), so summing either
//! never adds two kinds of count:
//!
//! | Key | What |
//! |---|---|
//! | `quic_data_send_<kind>_<result>` | data sends by kind and how they ended |
//! | `quic_data_event_<event>` | short overflow to UDP, refusals, stall closes, lost UDP sends |
//! | `quic_data_stop_<code>` | stops the peer sent to data sends, by code |
//! | `quic_inbound_<outcome>` | every `QuicInboundOutcome`, node-wide |
//! | `quic_enabled_data_paths` | paths where both sides set the data bit |
//! | `quic_live_data_paths` | paths carrying data now (a gauge, not reset, sent every period, 0 included); a path the peer refused data on is not one |
//! | `quic_sends`, `quic_open_uni_waited` | streams a path tried to open (data and control), and those whose open had to wait for a stream credit |
//! | `quic_streamed_sends`, `quic_stream_permit_waited` | streamed-send permits taken, and those that waited |
//! | `quic_bytes_sent`, `quic_bytes_received` | UDP payload bytes of QUIC paths, from quinn's counts |
//! | `quic_session_ended_with_data_path_<age>` | sessions that ended under a data path, by the path's age |
//! | `quic_streams_sampled_paths`, `quic_streams_open_ge_<n>`, `quic_streams_peak_max` | per path and period, the peak of inbound streams read at once: the paths sampled, those whose peak reached each level, and the highest peak |
//!
//! The waits are the step-3 stream-limit bars: `quic_open_uni_waited` over
//! `quic_sends` and `quic_stream_permit_waited` over `quic_streamed_sends` below 0.5%, and
//! `quic_streams_open_ge_48` below 0.2% of `quic_streams_sampled_paths`. Both
//! wait counts are of attempts: a send that waits and then times out is counted.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::QuicPathAge;
use crate::transport::quic::path::{
    QuicInboundOutcome, QuicSendEvent, QuicSendResult, SendKind, stop_code,
};

/// The gauge's key, uploaded in every snapshot ([`QuicDataSnapshot::upload_keys`]).
const LIVE_DATA_PATHS: &str = "quic_live_data_paths";

/// The levels `quic_streams_open_ge_<n>` reports, up to the streamed-send limit
/// (`quic::config::STREAMED_SEND_LIMIT`), which is the bar.
pub(crate) const QUIC_STREAMS_OPEN_THRESHOLDS: [u32; 4] = [8, 16, 32, 48];

/// The streams-open counts of one period, under one lock with its period, as the
/// connection-level inbound concurrency counts are.
#[derive(Debug, Default, Clone, Copy)]
struct StreamsOpenCounts {
    paths_sampled: u32,
    ge: [u32; QUIC_STREAMS_OPEN_THRESHOLDS.len()],
    max: u32,
}

/// One path's peak of inbound streams read at once in the current period. Owned
/// by the path's receive task.
#[derive(Debug, Default)]
pub(crate) struct StreamsOpenPeak {
    /// The period the peak belongs to; 0 before the first sample.
    epoch: u64,
    peak: u32,
}

/// The node-wide counts. A field of `TransportMetrics`.
#[derive(Debug)]
pub(crate) struct QuicDataCounters {
    sends: [[AtomicU64; QuicSendResult::COUNT]; SendKind::ALL.len()],
    events: [AtomicU64; QuicSendEvent::COUNT],
    stops: [AtomicU64; stop_code::ALL.len()],
    inbound: [AtomicU64; QuicInboundOutcome::COUNT],
    data_enabled: AtomicU64,
    /// A gauge: paths with a data route now.
    data_paths: AtomicU64,
    sends_opened: AtomicU64,
    open_uni_waited: AtomicU64,
    permit_takes: AtomicU64,
    permit_waited: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    session_ended: [AtomicU64; QuicPathAge::COUNT],
    streams_epoch: AtomicU64,
    streams: Mutex<StreamsOpenCounts>,
}

impl QuicDataCounters {
    pub(crate) fn new() -> Self {
        let zero = || AtomicU64::new(0);
        Self {
            sends: std::array::from_fn(|_| std::array::from_fn(|_| zero())),
            events: std::array::from_fn(|_| zero()),
            stops: std::array::from_fn(|_| zero()),
            inbound: std::array::from_fn(|_| zero()),
            data_enabled: zero(),
            data_paths: zero(),
            sends_opened: zero(),
            open_uni_waited: zero(),
            permit_takes: zero(),
            permit_waited: zero(),
            bytes_sent: zero(),
            bytes_received: zero(),
            session_ended: std::array::from_fn(|_| zero()),
            streams_epoch: AtomicU64::new(1),
            streams: Mutex::new(StreamsOpenCounts::default()),
        }
    }

    pub(crate) fn record_send(&self, kind: SendKind, result: QuicSendResult) {
        self.sends[kind as usize][result as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_event(&self, event: QuicSendEvent) {
        self.events[event as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A stop from the peer; a code this node does not know is not counted here.
    pub(crate) fn record_stop(&self, code: u64) {
        if let Some(index) = stop_code::ALL.iter().position(|c| u64::from(*c) == code) {
            self.stops[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_inbound(&self, outcome: QuicInboundOutcome) {
        self.inbound[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_data_enabled(&self) {
        self.data_enabled.fetch_add(1, Ordering::Relaxed);
    }

    /// A path took a data route. Pair with [`Self::data_path_ended`].
    pub(crate) fn data_path_started(&self) {
        self.data_paths.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn data_path_ended(&self) {
        // A failed update means the gauge was already 0: nothing to undo.
        if self
            .data_paths
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_err()
        {
            tracing::debug!("quic_live_data_paths was already 0 when a data path ended");
        }
    }

    /// A stream was opened on a path, data or control; `waited` when the open did
    /// not complete on its first poll, so the peer's stream credit was used up.
    pub(crate) fn record_open(&self, waited: bool) {
        self.sends_opened.fetch_add(1, Ordering::Relaxed);
        if waited {
            self.open_uni_waited.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A streamed send took a permit; `waited` when none was free at once.
    pub(crate) fn record_permit(&self, waited: bool) {
        self.permit_takes.fetch_add(1, Ordering::Relaxed);
        if waited {
            self.permit_waited.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// UDP payload bytes a path sent and received since its last report.
    pub(crate) fn record_bytes(&self, sent: u64, received: u64) {
        self.bytes_sent.fetch_add(sent, Ordering::Relaxed);
        self.bytes_received.fetch_add(received, Ordering::Relaxed);
    }

    /// A session ended while its path carried data, `age` after it became active.
    pub(crate) fn record_session_ended_with_data_path(&self, age: std::time::Duration) {
        self.session_ended[QuicPathAge::bucket(age) as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A path reads `open` inbound streams at once. Counted per path and period as
    /// `TransportMetrics::record_inbound_concurrency` counts connections: a path
    /// is sampled once a period and counted once in each level its peak reaches,
    /// and a sample that is no new peak in a period already counted takes no lock.
    pub(crate) fn record_streams_open(&self, path: &mut StreamsOpenPeak, open: u32) {
        if path.epoch == self.streams_epoch.load(Ordering::Relaxed) && open <= path.peak {
            return;
        }
        let mut counts = self.streams.lock();
        let epoch = self.streams_epoch.load(Ordering::Relaxed);
        if path.epoch != epoch {
            path.epoch = epoch;
            path.peak = 0;
            counts.paths_sampled += 1;
        }
        if open <= path.peak {
            return;
        }
        for (count, &level) in counts.ge.iter_mut().zip(&QUIC_STREAMS_OPEN_THRESHOLDS) {
            if path.peak < level && level <= open {
                *count += 1;
            }
        }
        path.peak = open;
        counts.max = counts.max.max(open);
    }

    /// The period's counts, reset when `reset`. The gauge is read, never reset.
    pub(crate) fn snapshot(&self, reset: bool) -> QuicDataSnapshot {
        let get = |a: &AtomicU64| {
            if reset {
                a.swap(0, Ordering::Relaxed)
            } else {
                a.load(Ordering::Relaxed)
            }
        };
        let streams = {
            let mut counts = self.streams.lock();
            if reset {
                self.streams_epoch.fetch_add(1, Ordering::Relaxed);
                std::mem::take(&mut *counts)
            } else {
                *counts
            }
        };
        QuicDataSnapshot {
            sends: self.sends.iter().flatten().map(get).collect(),
            events: self.events.iter().map(get).collect(),
            stops: self.stops.iter().map(get).collect(),
            inbound: self.inbound.iter().map(get).collect(),
            data_enabled: get(&self.data_enabled),
            data_paths: self.data_paths.load(Ordering::Relaxed),
            sends_opened: get(&self.sends_opened),
            open_uni_waited: get(&self.open_uni_waited),
            permit_takes: get(&self.permit_takes),
            permit_waited: get(&self.permit_waited),
            bytes_sent: get(&self.bytes_sent),
            bytes_received: get(&self.bytes_received),
            session_ended_with_data_path: self.session_ended.iter().map(get).collect(),
            streams_open_paths_sampled: streams.paths_sampled,
            streams_open_ge: streams.ge.to_vec(),
            streams_open_max: streams.max,
        }
    }
}

/// The QUIC data counts of one period. Vectors, in the order of their label lists,
/// so a family can pass 32 entries; [`Self::keys`] names every value.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct QuicDataSnapshot {
    /// By `SendKind`, then `QuicSendResult`.
    pub sends: Vec<u64>,
    pub events: Vec<u64>,
    /// In the order of `stop_code::ALL`.
    pub stops: Vec<u64>,
    pub inbound: Vec<u64>,
    pub data_enabled: u64,
    pub data_paths: u64,
    pub sends_opened: u64,
    pub open_uni_waited: u64,
    pub permit_takes: u64,
    pub permit_waited: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    /// By `QuicPathAge`.
    pub session_ended_with_data_path: Vec<u64>,
    pub streams_open_paths_sampled: u32,
    /// By `QUIC_STREAMS_OPEN_THRESHOLDS`.
    pub streams_open_ge: Vec<u32>,
    pub streams_open_max: u32,
}

impl QuicDataSnapshot {
    /// Every key with its value, in a fixed order. A value with no label (a
    /// vector longer than its list, which only a decoded foreign snapshot has) is
    /// left out.
    pub fn keys(&self) -> Vec<(String, u64)> {
        self.keys_by_family()
            .into_iter()
            .map(|(_, key, value)| (key, value))
            .collect()
    }

    /// [`Self::keys`], each with the prefix of the family it is in, or `None` for
    /// a key of its own. The prefixes the keys are built from, so a test can check
    /// them against every key.
    pub(crate) fn keys_by_family(&self) -> Vec<(Option<&'static str>, String, u64)> {
        // Destructured, so a new field cannot go unexported.
        let Self {
            sends,
            events,
            stops,
            inbound,
            data_enabled,
            data_paths,
            sends_opened,
            open_uni_waited,
            permit_takes,
            permit_waited,
            bytes_sent,
            bytes_received,
            session_ended_with_data_path,
            streams_open_paths_sampled,
            streams_open_ge,
            streams_open_max,
        } = self;
        let mut keys = Vec::new();
        let mut family = |prefix: &'static str, labels: Vec<String>, values: Vec<u64>| {
            keys.extend(
                labels
                    .into_iter()
                    .zip(values)
                    .map(|(label, value)| (Some(prefix), format!("{prefix}{label}"), value)),
            );
        };
        family(
            "quic_data_send_",
            SendKind::ALL
                .iter()
                .flat_map(|kind| {
                    QuicSendResult::ALL
                        .iter()
                        .map(move |result| format!("{}_{}", kind.label(), result.label()))
                })
                .collect(),
            sends.clone(),
        );
        family(
            "quic_data_event_",
            QuicSendEvent::ALL
                .iter()
                .map(|e| e.label().to_string())
                .collect(),
            events.clone(),
        );
        family(
            "quic_data_stop_",
            stop_code::ALL
                .iter()
                .map(|code| stop_code::label(*code).to_string())
                .collect(),
            stops.clone(),
        );
        family(
            "quic_inbound_",
            QuicInboundOutcome::ALL
                .iter()
                .map(|o| o.label().to_string())
                .collect(),
            inbound.clone(),
        );
        family(
            "quic_session_ended_with_data_path_",
            QuicPathAge::ALL
                .iter()
                .map(|a| a.label().to_string())
                .collect(),
            session_ended_with_data_path.clone(),
        );
        family(
            "quic_streams_open_ge_",
            QUIC_STREAMS_OPEN_THRESHOLDS
                .iter()
                .map(|level| level.to_string())
                .collect(),
            streams_open_ge.iter().map(|n| u64::from(*n)).collect(),
        );
        for (key, value) in [
            ("quic_enabled_data_paths", *data_enabled),
            (LIVE_DATA_PATHS, *data_paths),
            ("quic_sends", *sends_opened),
            ("quic_open_uni_waited", *open_uni_waited),
            ("quic_streamed_sends", *permit_takes),
            ("quic_stream_permit_waited", *permit_waited),
            ("quic_bytes_sent", *bytes_sent),
            ("quic_bytes_received", *bytes_received),
            (
                "quic_streams_sampled_paths",
                u64::from(*streams_open_paths_sampled),
            ),
            ("quic_streams_peak_max", u64::from(*streams_open_max)),
        ] {
            keys.push((None, key.to_string(), value));
        }
        keys
    }

    /// The keys the snapshot uploads: every count that is not 0, and the gauge
    /// always, so a gauge of 0 is told apart from one not reported.
    pub fn upload_keys(&self) -> impl Iterator<Item = (String, u64)> {
        self.keys()
            .into_iter()
            .filter(|(key, value)| *value != 0 || key == LIVE_DATA_PATHS)
    }

    /// Whether the period counted anything. The gauge alone is not activity, as
    /// `quic_paths_active` is not.
    pub(super) fn has_activity(&self) -> bool {
        let QuicDataSnapshot {
            sends,
            events,
            stops,
            inbound,
            data_enabled,
            data_paths: _,
            sends_opened,
            open_uni_waited,
            permit_takes,
            permit_waited,
            bytes_sent,
            bytes_received,
            session_ended_with_data_path,
            streams_open_paths_sampled,
            streams_open_ge: _,
            streams_open_max: _,
        } = self;
        let any = |values: &[u64]| values.iter().any(|n| *n != 0);
        any(sends)
            || any(events)
            || any(stops)
            || any(inbound)
            || any(session_ended_with_data_path)
            || [
                *data_enabled,
                *sends_opened,
                *open_uni_waited,
                *permit_takes,
                *permit_waited,
                *bytes_sent,
                *bytes_received,
                u64::from(*streams_open_paths_sampled),
            ]
            .iter()
            .any(|n| *n != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each family is counted where it is recorded, read back under its own keys
    /// with its own values, and reset by the snapshot, except the gauge.
    #[test]
    fn each_count_reaches_its_key_and_resets() {
        let counters = QuicDataCounters::new();
        for (i, kind) in SendKind::ALL.iter().enumerate() {
            for (j, result) in QuicSendResult::ALL.iter().enumerate() {
                for _ in 0..(i * 10 + j + 1) {
                    counters.record_send(*kind, *result);
                }
            }
        }
        counters.record_event(QuicSendEvent::StalledClosed);
        counters.record_stop(u64::from(stop_code::NOT_ACCEPTING_DATA));
        counters.record_stop(0xff);
        counters.record_inbound(QuicInboundOutcome::ShortDroppedFull);
        counters.record_data_enabled();
        counters.data_path_started();
        counters.data_path_started();
        counters.data_path_ended();
        counters.record_open(false);
        counters.record_open(true);
        counters.record_permit(true);
        counters.record_bytes(100, 200);
        counters.record_session_ended_with_data_path(std::time::Duration::from_secs(90));
        let mut path = StreamsOpenPeak::default();
        counters.record_streams_open(&mut path, 20);
        counters.record_streams_open(&mut path, 10);

        let snap = counters.snapshot(true);
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_data_send_short_sent"], 1);
        assert_eq!(keys["quic_data_send_large_short_maybe_delivered"], 12);
        assert_eq!(keys["quic_data_send_pipe_dropped"], 35);
        assert_eq!(keys["quic_data_event_stalled_closed"], 1);
        assert_eq!(keys["quic_data_stop_not_accepting_data"], 1);
        assert_eq!(keys["quic_inbound_short_dropped_full"], 1);
        assert_eq!(keys["quic_enabled_data_paths"], 1);
        assert_eq!(keys["quic_live_data_paths"], 1);
        assert_eq!(keys["quic_sends"], 2);
        assert_eq!(keys["quic_open_uni_waited"], 1);
        assert_eq!(keys["quic_streamed_sends"], 1);
        assert_eq!(keys["quic_stream_permit_waited"], 1);
        assert_eq!(keys["quic_bytes_sent"], 100);
        assert_eq!(keys["quic_bytes_received"], 200);
        assert_eq!(keys["quic_session_ended_with_data_path_lt_10m"], 1);
        assert_eq!(keys["quic_streams_sampled_paths"], 1);
        assert_eq!(keys["quic_streams_open_ge_16"], 1);
        assert_eq!(keys["quic_streams_open_ge_32"], 0);
        assert_eq!(keys["quic_streams_peak_max"], 20);
        assert!(snap.has_activity());

        let after = counters.snapshot(true);
        assert_eq!(after.data_paths, 1, "the gauge is not reset");
        assert!(!after.has_activity(), "a gauge alone is not activity");
        assert!(
            after
                .upload_keys()
                .all(|(key, _)| key == "quic_live_data_paths")
        );
        // A new period counts the path again.
        counters.record_streams_open(&mut path, 5);
        assert_eq!(counters.snapshot(true).streams_open_paths_sampled, 1);
    }

    /// The gauge goes out in every snapshot, 0 included, so 0 is told apart from
    /// not reported; a count of 0 does not.
    #[test]
    fn the_gauge_is_uploaded_at_zero_and_counts_are_not() {
        let counters = QuicDataCounters::new();
        let uploaded: Vec<_> = counters.snapshot(true).upload_keys().collect();
        assert_eq!(uploaded, vec![("quic_live_data_paths".to_string(), 0)]);
        counters.data_path_started();
        counters.record_open(false);
        let uploaded: std::collections::BTreeMap<_, _> =
            counters.snapshot(true).upload_keys().collect();
        assert_eq!(uploaded.len(), 2, "{uploaded:?}");
        assert_eq!(uploaded["quic_live_data_paths"], 1);
        assert_eq!(uploaded["quic_sends"], 1);
    }

    /// The keys with the family each is in, from `keys_by_family`; every key the
    /// rest of the snapshot has, from its serde fields; and the prefix of each of
    /// its fields that is an array, which goes out as a key per label
    /// (`quic_upgrade_<outcome>`).
    #[allow(clippy::type_complexity)]
    fn all_keys() -> (
        Vec<(Option<&'static str>, String)>,
        Vec<String>,
        Vec<String>,
    ) {
        let ours = QuicDataCounters::new()
            .snapshot(false)
            .keys_by_family()
            .into_iter()
            .map(|(family, key, _)| (family, key))
            .collect();
        let json = serde_json::to_value(crate::transport::metrics::TransportSnapshot::default())
            .expect("serializes");
        let map = json.as_object().expect("an object");
        let others = map
            .keys()
            .filter(|key| *key != "quic_data")
            .cloned()
            .collect();
        let arrays = map
            .iter()
            .filter(|(_, value)| value.is_array())
            .map(|(key, _)| format!("{key}_"))
            .collect();
        (ours, others, arrays)
    }

    /// A key's prefixes that end at a `_` and have two words or more: `quic_data_`
    /// and `quic_data_send_` of `quic_data_send_short_sent`.
    fn stems(key: &str) -> impl Iterator<Item = &str> {
        key.match_indices('_')
            .map(move |(at, _)| &key[..=at])
            .skip(1)
    }

    /// Every key is distinct, and none is under a family it is not in: not under
    /// another family's prefix, nor under the prefix sibling families share
    /// (`quic_data_` of the send, event and stop families), nor under one of the
    /// snapshot's arrays (`quic_upgrade_`) or a prefix two of its other keys share
    /// (`quic_paths_`). The families come from the keys and the other keys from
    /// the snapshot, so a new key or family is checked without editing a list.
    #[test]
    fn no_key_is_under_another_family() {
        let (ours, others, arrays) = all_keys();
        let keys: Vec<&str> = ours
            .iter()
            .map(|(_, key)| key.as_str())
            .chain(others.iter().map(String::as_str))
            .collect();
        let distinct: std::collections::BTreeSet<_> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len(), "a key is used twice");

        let families: std::collections::BTreeSet<&str> =
            ours.iter().filter_map(|(family, _)| *family).collect();
        // A family's prefix, and each prefix it shares with a sibling, with the
        // families under it.
        let mut claimed: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
        for family in &families {
            claimed.entry(family).or_default().push(family);
            for parent in stems(family).filter(|stem| stem != family) {
                if families
                    .iter()
                    .any(|other| other != family && other.starts_with(parent))
                {
                    claimed.entry(parent).or_default().push(family);
                }
            }
        }
        for (key_family, key) in &ours {
            for (prefix, owners) in &claimed {
                if key.starts_with(prefix) {
                    assert!(
                        key_family.is_some_and(|f| owners.contains(&f)),
                        "{key} is under {prefix}, of {owners:?}"
                    );
                }
            }
        }
        for key in &others {
            for prefix in claimed.keys() {
                assert!(
                    !key.starts_with(prefix),
                    "the snapshot's {key} is under {prefix}"
                );
            }
        }
        // The prefixes two of the snapshot's own keys share.
        let mut shared: std::collections::BTreeMap<&str, u32> = Default::default();
        for key in &others {
            for stem in stems(key) {
                *shared.entry(stem).or_default() += 1;
            }
        }
        let taken = shared
            .iter()
            .filter(|(_, n)| **n >= 2)
            .map(|(stem, _)| *stem)
            .chain(arrays.iter().map(String::as_str));
        for stem in taken {
            for (_, key) in &ours {
                assert!(
                    !key.starts_with(stem),
                    "{key} is under the snapshot's {stem}"
                );
            }
        }
    }

    /// A path's peak counts once in each level it reaches, a level at its exact
    /// value included, and a new period counts it again from 0.
    #[test]
    fn a_peak_counts_once_per_level_and_period() {
        let counters = QuicDataCounters::new();
        let mut rising = StreamsOpenPeak::default();
        for open in [20, 40, 40] {
            counters.record_streams_open(&mut rising, open);
        }
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 1);
        assert_eq!(
            snap.streams_open_ge,
            vec![1, 1, 1, 0],
            "8, 16, 32 and not 48"
        );
        assert_eq!(snap.streams_open_max, 40);

        let mut at_16 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_16, 16);
        let mut at_48 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_48, 48);
        let mut at_7 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_7, 7);
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 3);
        assert_eq!(snap.streams_open_ge, vec![2, 2, 1, 1]);
        assert_eq!(snap.streams_open_max, 48);

        // After the reset the rising path starts again: 9 is a new peak.
        counters.record_streams_open(&mut rising, 9);
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 1);
        assert_eq!(snap.streams_open_ge, vec![1, 0, 0, 0]);
        assert_eq!(snap.streams_open_max, 9);
    }

    /// The snapshot round-trips through bincode, as the event log stores it.
    #[test]
    fn snapshot_round_trips_through_bincode() {
        let counters = QuicDataCounters::new();
        counters.record_send(SendKind::Stream, QuicSendResult::FailedUdp);
        counters.record_inbound(QuicInboundOutcome::StreamRefusedBudget);
        let snap = counters.snapshot(true);
        let back: QuicDataSnapshot =
            bincode::deserialize(&bincode::serialize(&snap).unwrap()).unwrap();
        assert_eq!(back, snap);
    }
}
