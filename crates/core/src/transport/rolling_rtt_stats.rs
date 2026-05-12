//! Rolling per-peer RTT statistics for shadow telemetry.
//!
//! This module implements Phase 1 of the outer-loop rate controller RFC
//! (issue #4074): per-connection rolling RTT windows that an aggregator
//! reads once per second to compute cross-connection inflation.
//!
//! The structure tracks two sliding windows of RTT samples per peer:
//!
//! - `BASELINE_WINDOW` (5 min): the minimum RTT across this window is used
//!   as the "uncongested baseline" for that peer.
//! - `RECENT_WINDOW` (10 s): the median RTT across this window is the
//!   "current" RTT estimate, robust to a few outlier samples.
//!
//! `inflation = recent_median - baseline_min` is the per-peer signal. The
//! cross-peer median of these is the controller's contention signal, but
//! this module deliberately stops at exposing the per-peer numbers; no
//! decisions are taken here.
//!
//! Phase 1 is observation only; nothing in this file feeds back into the
//! congestion controller.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use dashmap::DashMap;

use crate::simulation::TimeSource;

/// Baseline window for the rolling minimum RTT. The RFC uses 5 min as a
/// starting guess; the actual value should be revisited once Phase 1
/// telemetry shows how stable real paths are.
const BASELINE_WINDOW: Duration = Duration::from_secs(300);

/// Recent window for the rolling median RTT. Matches the RFC's "median
/// RTT over last 10 s" definition.
const RECENT_WINDOW: Duration = Duration::from_secs(10);

/// Hard upper bound on stored samples per peer. At a sustained 50
/// samples/sec this covers the full 5-min baseline window; busier peers
/// will lose the oldest samples first. The cap exists so a runaway
/// sender cannot push per-peer memory above a few hundred KB.
const MAX_SAMPLES: usize = 16_384;

/// Snapshot of one peer's rolling RTT state. Cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RttSnapshot {
    /// Minimum RTT observed in the baseline window. `None` if the window
    /// has no samples yet.
    pub baseline_min: Option<Duration>,
    /// Median RTT in the recent window. `None` if the recent window has
    /// no samples.
    pub recent_median: Option<Duration>,
    /// Inflation = `recent_median - baseline_min`, saturating at zero.
    /// `None` if either input is `None`.
    pub inflation: Option<Duration>,
    /// Count of samples in the baseline window after pruning.
    pub baseline_samples: usize,
    /// Count of samples in the recent window.
    pub recent_samples: usize,
}

pub(crate) struct RollingRttStats<T: TimeSource> {
    time_source: T,
    inner: parking_lot::Mutex<Inner>,
}

struct Inner {
    /// (timestamp_nanos, rtt_nanos) in ascending timestamp order.
    /// Front is oldest.
    samples: VecDeque<(u64, u64)>,
}

impl<T: TimeSource> RollingRttStats<T> {
    pub(crate) fn new(time_source: T) -> Self {
        Self {
            time_source,
            inner: parking_lot::Mutex::new(Inner {
                samples: VecDeque::with_capacity(1024),
            }),
        }
    }

    /// Record one RTT sample using the embedded time source for the
    /// timestamp. Called from the ACK-processing site once per ACK that
    /// produced a non-retransmitted RTT measurement.
    pub(crate) fn record(&self, rtt: Duration) {
        let now_nanos = self.time_source.now_nanos();
        let rtt_nanos = rtt.as_nanos().min(u64::MAX as u128) as u64;
        let cutoff = now_nanos.saturating_sub(BASELINE_WINDOW.as_nanos() as u64);

        let mut inner = self.inner.lock();
        inner.samples.push_back((now_nanos, rtt_nanos));

        // Drop samples older than the baseline window.
        while let Some(&(ts, _)) = inner.samples.front() {
            if ts < cutoff {
                inner.samples.pop_front();
            } else {
                break;
            }
        }
        // Enforce the hard memory cap.
        while inner.samples.len() > MAX_SAMPLES {
            inner.samples.pop_front();
        }
    }

    /// Take a snapshot of the rolling statistics. Returns `None` if the
    /// baseline window has no samples.
    ///
    /// This is the only read API. Both the cross-connection aggregator
    /// (1 Hz) and any debug introspection go through it. Acquires the
    /// mutex once and computes both window summaries in a single pass.
    pub(crate) fn snapshot(&self) -> Option<RttSnapshot> {
        let now_nanos = self.time_source.now_nanos();
        let baseline_cutoff = now_nanos.saturating_sub(BASELINE_WINDOW.as_nanos() as u64);
        let recent_cutoff = now_nanos.saturating_sub(RECENT_WINDOW.as_nanos() as u64);

        let inner = self.inner.lock();
        if inner.samples.is_empty() {
            return None;
        }

        let mut baseline_min: Option<u64> = None;
        let mut baseline_samples = 0usize;
        let mut recent: Vec<u64> = Vec::new();
        for &(ts, rtt) in inner.samples.iter() {
            if ts < baseline_cutoff {
                continue;
            }
            baseline_samples += 1;
            baseline_min = Some(match baseline_min {
                Some(m) => m.min(rtt),
                None => rtt,
            });
            if ts >= recent_cutoff {
                recent.push(rtt);
            }
        }
        if baseline_samples == 0 {
            return None;
        }

        let recent_samples = recent.len();
        let recent_median = if recent.is_empty() {
            None
        } else {
            recent.sort_unstable();
            // Upper-middle for even-length windows; matches the RFC's
            // "median RTT over last 10 s" wording and is robust to a
            // single low outlier.
            Some(Duration::from_nanos(recent[recent.len() / 2]))
        };

        let baseline_min_dur = baseline_min.map(Duration::from_nanos);
        let inflation = match (baseline_min_dur, recent_median) {
            (Some(b), Some(r)) => Some(r.saturating_sub(b)),
            _ => None,
        };

        Some(RttSnapshot {
            baseline_min: baseline_min_dur,
            recent_median,
            inflation,
            baseline_samples,
            recent_samples,
        })
    }

    /// Number of samples currently retained. Test-only; production reads
    /// go through `snapshot`.
    #[cfg(test)]
    pub(crate) fn stored_samples(&self) -> usize {
        self.inner.lock().samples.len()
    }
}

/// Type-erased view over a per-peer RTT stats source. Lets the global
/// registry hold `RollingRttStats<T>` for arbitrary `T: TimeSource`
/// without leaking the type parameter to the aggregator.
pub(crate) trait RttSnapshotProvider: Send + Sync {
    fn snapshot(&self) -> Option<RttSnapshot>;
}

impl<T: TimeSource> RttSnapshotProvider for RollingRttStats<T> {
    fn snapshot(&self) -> Option<RttSnapshot> {
        Self::snapshot(self)
    }
}

/// Process-wide registry of per-peer rolling RTT stats. Populated by
/// `RollingRttStatsHandle::new` on connection establishment and pruned
/// when the handle drops. The cross-connection aggregator reads this
/// registry once per second; nothing in the controller path writes to
/// it.
pub(crate) static SHADOW_RTT_REGISTRY: LazyLock<DashMap<SocketAddr, Arc<dyn RttSnapshotProvider>>> =
    LazyLock::new(DashMap::new);

/// RAII handle that owns a peer's `RollingRttStats` and registers it
/// with the global shadow registry for the lifetime of the connection.
///
/// On drop the entry is removed *only* if the registry still points at
/// this exact `Arc`, otherwise a newer connection to the same address
/// has already replaced our entry and we leave it alone.
pub(crate) struct RollingRttStatsHandle<T: TimeSource> {
    stats: Arc<RollingRttStats<T>>,
    erased: Arc<dyn RttSnapshotProvider>,
    remote_addr: SocketAddr,
}

impl<T: TimeSource> RollingRttStatsHandle<T> {
    pub(crate) fn new(remote_addr: SocketAddr, time_source: T) -> Self {
        let stats = Arc::new(RollingRttStats::new(time_source));
        let erased: Arc<dyn RttSnapshotProvider> = stats.clone();
        SHADOW_RTT_REGISTRY.insert(remote_addr, erased.clone());
        ensure_aggregator_running();
        Self {
            stats,
            erased,
            remote_addr,
        }
    }

    pub(crate) fn record(&self, rtt: Duration) {
        self.stats.record(rtt);
    }
}

impl<T: TimeSource> Drop for RollingRttStatsHandle<T> {
    fn drop(&mut self) {
        SHADOW_RTT_REGISTRY.remove_if(&self.remote_addr, |_, current| {
            Arc::ptr_eq(current, &self.erased)
        });
    }
}

/// Snapshot every live per-peer entry. Skips peers whose baseline
/// window is still empty.
pub(crate) fn registry_snapshot() -> Vec<(SocketAddr, RttSnapshot)> {
    SHADOW_RTT_REGISTRY
        .iter()
        .filter_map(|kv| kv.value().snapshot().map(|s| (*kv.key(), s)))
        .collect()
}

/// Cross-connection median RTT inflation across all live peers with a
/// `recent_median` (i.e. peers that have sent recent traffic). Returns
/// `None` if no peer has a defined inflation, which is the case at cold
/// start or on a fully idle node.
///
/// The median, not mean, is used so a single peer with a routing event
/// or genuinely contended path cannot push the signal on its own, the
/// RFC's "self-fulfilling re-route" trap.
pub(crate) fn cross_connection_median_inflation() -> Option<Duration> {
    let mut inflations: Vec<u64> = registry_snapshot()
        .into_iter()
        .filter_map(|(_, s)| s.inflation.map(|d| d.as_nanos() as u64))
        .collect();
    if inflations.is_empty() {
        return None;
    }
    inflations.sort_unstable();
    Some(Duration::from_nanos(inflations[inflations.len() / 2]))
}

/// Period at which the aggregator wakes up to log a snapshot of the
/// cross-connection signal. The RFC controller would consult the same
/// signal at this cadence; Phase 1 just emits it as telemetry.
const AGGREGATOR_INTERVAL: Duration = Duration::from_secs(1);

static AGGREGATOR_STARTED: std::sync::Once = std::sync::Once::new();

/// Idempotently spawn the cross-connection aggregator task.
///
/// Called from `RollingRttStatsHandle::new`, so the first connection
/// established on a node bootstraps the aggregator. In test contexts
/// with no current tokio runtime (e.g. plain `#[test]` units), the
/// aggregator is silently skipped, the registry itself still works
/// for direct inspection.
fn ensure_aggregator_running() {
    AGGREGATOR_STARTED.call_once(|| {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        handle.spawn(async {
            let mut ticker = tokio::time::interval(AGGREGATOR_INTERVAL);
            // Skip the immediate-tick on construction; the first
            // useful sample is one period out.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                emit_aggregate_snapshot();
            }
        });
    });
}

/// Emit one tracing event summarising the current cross-connection
/// state. Logged at `info` because Phase 1 must reach production
/// telemetry; debug! would be compiled out of release builds.
fn emit_aggregate_snapshot() {
    let snap = registry_snapshot();
    let active_peers = snap.len();
    if active_peers == 0 {
        return;
    }
    let peers_with_recent = snap
        .iter()
        .filter(|(_, s)| s.recent_median.is_some())
        .count();
    let median_inflation_us = cross_connection_median_inflation().map(|d| d.as_micros() as u64);
    // Per-peer detail at trace so dashboards can reconstruct the full
    // picture without paying for it in every release log.
    for (addr, s) in &snap {
        tracing::trace!(
            target: "freenet::transport::shadow_rtt",
            peer = %addr,
            baseline_min_us = s.baseline_min.map(|d| d.as_micros() as u64),
            recent_median_us = s.recent_median.map(|d| d.as_micros() as u64),
            inflation_us = s.inflation.map(|d| d.as_micros() as u64),
            baseline_samples = s.baseline_samples,
            recent_samples = s.recent_samples,
            "shadow_rtt_per_peer"
        );
    }
    tracing::info!(
        target: "freenet::transport::shadow_rtt",
        active_peers,
        peers_with_recent,
        median_inflation_us,
        "shadow_rtt_aggregate"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;

    fn dur_ms(ms: u64) -> Duration {
        Duration::from_millis(ms)
    }

    fn new_stats() -> (VirtualTime, RollingRttStats<VirtualTime>) {
        let ts = VirtualTime::new();
        let stats = RollingRttStats::new(ts.clone());
        (ts, stats)
    }

    #[test]
    fn empty_snapshot_is_none() {
        let (_ts, stats) = new_stats();
        assert!(stats.snapshot().is_none());
    }

    #[test]
    fn baseline_min_tracks_minimum() {
        let (ts, stats) = new_stats();
        for ms in [50u64, 80, 40, 60, 100] {
            stats.record(dur_ms(ms));
            ts.advance(Duration::from_millis(100));
        }
        let snap = stats.snapshot().expect("snapshot");
        assert_eq!(snap.baseline_min, Some(dur_ms(40)));
    }

    #[test]
    fn samples_older_than_baseline_window_are_dropped() {
        let (ts, stats) = new_stats();
        // Old sample with a very low RTT, should be evicted before we
        // measure baseline.
        stats.record(dur_ms(10));
        // Advance past the baseline window.
        ts.advance(BASELINE_WINDOW + Duration::from_secs(1));
        // Fresh samples after the window.
        for ms in [80u64, 90, 100] {
            stats.record(dur_ms(ms));
            ts.advance(Duration::from_millis(100));
        }
        let snap = stats.snapshot().expect("snapshot");
        assert_eq!(
            snap.baseline_min,
            Some(dur_ms(80)),
            "the 10 ms outlier should have aged out"
        );
        assert_eq!(snap.baseline_samples, 3);
    }

    #[test]
    fn recent_median_uses_only_last_ten_seconds() {
        let (ts, stats) = new_stats();
        // Old samples (well outside the 10s recent window).
        for ms in [40u64, 45, 50] {
            stats.record(dur_ms(ms));
            ts.advance(Duration::from_millis(100));
        }
        ts.advance(Duration::from_secs(60));
        // Recent burst.
        for ms in [120u64, 130, 140] {
            stats.record(dur_ms(ms));
            ts.advance(Duration::from_millis(100));
        }
        let snap = stats.snapshot().expect("snapshot");
        assert_eq!(snap.recent_median, Some(dur_ms(130)));
        assert_eq!(snap.recent_samples, 3);
        // Baseline window still sees the old low samples.
        assert_eq!(snap.baseline_min, Some(dur_ms(40)));
    }

    #[test]
    fn inflation_is_recent_minus_baseline() {
        let (ts, stats) = new_stats();
        // Establish a clean baseline.
        for _ in 0..5 {
            stats.record(dur_ms(50));
            ts.advance(Duration::from_millis(200));
        }
        // 30s later, inflate.
        ts.advance(Duration::from_secs(30));
        for _ in 0..5 {
            stats.record(dur_ms(150));
            ts.advance(Duration::from_millis(200));
        }
        let snap = stats.snapshot().expect("snapshot");
        assert_eq!(snap.baseline_min, Some(dur_ms(50)));
        assert_eq!(snap.recent_median, Some(dur_ms(150)));
        assert_eq!(snap.inflation, Some(dur_ms(100)));
    }

    #[test]
    fn inflation_saturates_at_zero_when_recent_below_baseline() {
        // After a baseline window of bad RTTs the path improves; recent
        // is *better* than baseline. inflation must not go negative
        // (Duration cannot represent negatives), saturating_sub gives 0.
        let (ts, stats) = new_stats();
        for _ in 0..5 {
            stats.record(dur_ms(200));
            ts.advance(Duration::from_millis(200));
        }
        ts.advance(Duration::from_secs(30));
        for _ in 0..5 {
            stats.record(dur_ms(50));
            ts.advance(Duration::from_millis(200));
        }
        let snap = stats.snapshot().expect("snapshot");
        assert_eq!(snap.inflation, Some(Duration::ZERO));
    }

    #[test]
    fn recent_window_can_be_empty_while_baseline_has_samples() {
        let (ts, stats) = new_stats();
        for ms in [40u64, 45, 50] {
            stats.record(dur_ms(ms));
            ts.advance(Duration::from_millis(100));
        }
        // Push past the 10s recent window but stay within baseline.
        ts.advance(Duration::from_secs(60));
        let snap = stats.snapshot().expect("snapshot");
        assert_eq!(snap.recent_median, None);
        assert!(snap.baseline_min.is_some());
        assert_eq!(snap.inflation, None);
    }

    #[test]
    fn memory_cap_drops_oldest_samples_first() {
        let (ts, stats) = new_stats();
        // Record more than the cap, spaced so none age out via the time
        // window, pruning here is purely from the MAX_SAMPLES cap.
        // Use a tight cadence so all fit inside BASELINE_WINDOW.
        let total = MAX_SAMPLES + 50;
        // Mark the first 10 with a distinctly low RTT so we can prove
        // they were the ones evicted.
        for i in 0..total {
            let rtt = if i < 10 { dur_ms(10) } else { dur_ms(100) };
            stats.record(rtt);
            // Stay well inside the 5min window.
            ts.advance(Duration::from_micros(100));
        }
        assert_eq!(stats.stored_samples(), MAX_SAMPLES);
        let snap = stats.snapshot().expect("snapshot");
        assert_eq!(
            snap.baseline_min,
            Some(dur_ms(100)),
            "the early dur_ms(10) samples should have been evicted by the cap"
        );
    }

    #[test]
    fn snapshot_returns_none_if_only_pre_baseline_samples_remain() {
        let (ts, stats) = new_stats();
        stats.record(dur_ms(50));
        // Advance past the baseline window without recording anything new.
        ts.advance(BASELINE_WINDOW + Duration::from_secs(1));
        // No new sample to trigger eviction of the stale entry, but
        // snapshot must still ignore it.
        let snap = stats.snapshot();
        assert!(
            snap.is_none(),
            "stale-only samples should not yield a snapshot"
        );
    }

    // Registry / handle tests use a unique IP per test to stay
    // independent under nextest's per-process isolation. They reach
    // into the global SHADOW_RTT_REGISTRY, so a per-test addr keeps
    // them safe even when a single process happens to run more than
    // one of them.

    fn unique_addr(octet: u8, port: u16) -> SocketAddr {
        use std::net::Ipv4Addr;
        SocketAddr::new(Ipv4Addr::new(192, 0, 2, octet).into(), port)
    }

    #[test]
    fn handle_registers_and_deregisters() {
        let addr = unique_addr(1, 50001);
        let ts = VirtualTime::new();
        let handle = RollingRttStatsHandle::new(addr, ts.clone());
        assert!(SHADOW_RTT_REGISTRY.contains_key(&addr));

        // Snapshot through the registry returns None until samples land.
        let pre = registry_snapshot();
        assert!(pre.iter().all(|(a, _)| *a != addr));

        handle.record(dur_ms(50));
        ts.advance(Duration::from_millis(200));
        handle.record(dur_ms(60));

        let mid = registry_snapshot();
        assert!(mid.iter().any(|(a, _)| *a == addr));

        drop(handle);
        assert!(!SHADOW_RTT_REGISTRY.contains_key(&addr));
    }

    #[test]
    fn drop_does_not_remove_replacement_entry() {
        // Models a reconnect: the second handle takes over the slot
        // before the first handle is dropped. The first handle's drop
        // must NOT evict the second handle's registration.
        let addr = unique_addr(2, 50002);
        let ts = VirtualTime::new();
        let h1 = RollingRttStatsHandle::new(addr, ts.clone());
        let h2 = RollingRttStatsHandle::new(addr, ts.clone());

        // The registry now points at h2.
        assert!(SHADOW_RTT_REGISTRY.contains_key(&addr));
        drop(h1);
        assert!(
            SHADOW_RTT_REGISTRY.contains_key(&addr),
            "h1's drop must not evict h2"
        );
        drop(h2);
        assert!(!SHADOW_RTT_REGISTRY.contains_key(&addr));
    }

    #[test]
    fn cross_connection_median_is_robust_to_one_outlier() {
        // Three peers with ~zero inflation; one peer with very high
        // inflation. The median computed over only our peers must
        // ignore the outlier. We filter to our own addresses so this
        // test is independent of any other test's registry entries
        // running concurrently.
        let ts = VirtualTime::new();
        let addrs: Vec<SocketAddr> = (10..14u8)
            .map(|i| unique_addr(i, 50100 + i as u16))
            .collect();
        let peers: Vec<_> = addrs
            .iter()
            .map(|a| RollingRttStatsHandle::new(*a, ts.clone()))
            .collect();

        // Establish a clean baseline on every peer.
        for h in &peers {
            for _ in 0..3 {
                h.record(dur_ms(50));
            }
        }
        ts.advance(Duration::from_secs(30));
        // Three peers stay near 50ms recent; one inflates to 500ms.
        for (i, h) in peers.iter().enumerate() {
            let recent = if i == 3 { dur_ms(500) } else { dur_ms(55) };
            for _ in 0..5 {
                h.record(recent);
            }
        }

        let mut my_inflations: Vec<u64> = registry_snapshot()
            .into_iter()
            .filter(|(a, _)| addrs.contains(a))
            .filter_map(|(_, s)| s.inflation.map(|d| d.as_nanos() as u64))
            .collect();
        my_inflations.sort_unstable();
        assert_eq!(
            my_inflations.len(),
            4,
            "all four peers should have inflation"
        );
        let median = Duration::from_nanos(my_inflations[my_inflations.len() / 2]);
        // Inflations across our peers are ~[5, 5, 5, 450] ms. The
        // upper-middle median is ~5 ms; anything below 50 ms proves
        // the outlier didn't drive the signal.
        assert!(
            median < dur_ms(50),
            "median {median:?} should be near baseline, not pulled by the 500ms outlier"
        );

        // Drop everything for hygiene.
        drop(peers);
    }
}
