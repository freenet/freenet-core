//! Counters and histograms for the compile path.
//!
//! Aggregate numbers only. Nothing here names a contract, a code hash or a peer,
//! and sizes appear only as log2 buckets, so a snapshot cannot identify an app or
//! a user. (Module identity goes to the node's own log file, never to telemetry.)

// Several of these are only reached by the Linux worker code.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Buckets of a [`Log2Histogram`]: bucket `i` counts values with `i` significant
/// bits (bucket 0 is the value 0), the last bucket everything larger.
pub(crate) const HISTOGRAM_BUCKETS: usize = 24;

#[derive(Debug)]
pub(crate) struct Log2Histogram {
    buckets: [AtomicU64; HISTOGRAM_BUCKETS],
}

impl Default for Log2Histogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Log2Histogram {
    pub(crate) fn record(&self, value: u64) {
        let bits = (u64::BITS - value.leading_zeros()) as usize;
        self.buckets[bits.min(HISTOGRAM_BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> [u64; HISTOGRAM_BUCKETS] {
        std::array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed))
    }
}

/// One verdict of the compile path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The worker accepted the module.
    Accepted,
    /// The module was already in the accepted set; no worker ran.
    AcceptedSkip,
    /// Refused by the scan before any worker: byte limit.
    RefusedSize,
    /// Refused by the scan: text format or not WebAssembly binary.
    RefusedText,
    /// Refused as invalid or outside the accepted feature set.
    RefusedFeature,
    /// Refused by a coarse structural limit of the scan.
    RefusedLimit,
    /// The worker's CPU cap.
    OverCpu,
    /// The worker's memory cap.
    OverMemory,
    /// The worker crashed on the module.
    Crashed,
    /// The worker hit the wall limit.
    Timeout,
    /// No verdict could be had (spawn, protocol, budget, outside kill).
    Busy,
    SpawnFailed,
    /// Dry-run: the worker would have refused this module (any reason).
    WouldRefuse,
    /// The startup probe failed and isolation went off for the run.
    ProbeFailed,
    /// Dry-run: no worker was started because one was already running, or the
    /// memory budget had no room, or the module is in its failure back-off.
    DryRunSkipped,
    /// Enforce: the module is in its failure back-off, so no worker was started.
    BackoffSkip,
    /// The worker could not install a limit it was asked to; it judged nothing.
    InfraFailure,
    /// A compile that went unchecked because isolation is requested but is not
    /// running on this node (see [`CompileMetrics::set_degraded`]).
    Degraded,
    /// A request answered from the verdict cache with a refusal, by the reason
    /// the module was first refused for. Each repeat request counts here; the
    /// first refusal counts under its own reason above.
    CachedRefusedSize,
    CachedRefusedText,
    CachedRefusedFeature,
    CachedRefusedLimit,
    CachedOverCpu,
    CachedOverMemory,
}

const OUTCOMES: usize = 24;

impl Outcome {
    pub(crate) const ALL: [Outcome; OUTCOMES] = [
        Self::Accepted,
        Self::AcceptedSkip,
        Self::RefusedSize,
        Self::RefusedText,
        Self::RefusedFeature,
        Self::RefusedLimit,
        Self::OverCpu,
        Self::OverMemory,
        Self::Crashed,
        Self::Timeout,
        Self::Busy,
        Self::SpawnFailed,
        Self::WouldRefuse,
        Self::ProbeFailed,
        Self::DryRunSkipped,
        Self::BackoffSkip,
        Self::InfraFailure,
        Self::Degraded,
        Self::CachedRefusedSize,
        Self::CachedRefusedText,
        Self::CachedRefusedFeature,
        Self::CachedRefusedLimit,
        Self::CachedOverCpu,
        Self::CachedOverMemory,
    ];

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::AcceptedSkip => "accepted_skip",
            Self::RefusedSize => "refused_size",
            Self::RefusedText => "refused_text",
            Self::RefusedFeature => "refused_feature",
            Self::RefusedLimit => "refused_limit",
            Self::OverCpu => "over_cpu",
            Self::OverMemory => "over_memory",
            Self::Crashed => "crashed",
            Self::Timeout => "timeout",
            Self::Busy => "busy",
            Self::SpawnFailed => "spawn_failed",
            Self::WouldRefuse => "would_refuse",
            Self::ProbeFailed => "probe_failed",
            Self::DryRunSkipped => "dry_run_skipped",
            Self::BackoffSkip => "backoff_skip",
            Self::InfraFailure => "infra_failure",
            Self::Degraded => "degraded",
            Self::CachedRefusedSize => "cached_refused_size",
            Self::CachedRefusedText => "cached_refused_text",
            Self::CachedRefusedFeature => "cached_refused_feature",
            Self::CachedRefusedLimit => "cached_refused_limit",
            Self::CachedOverCpu => "cached_over_cpu",
            Self::CachedOverMemory => "cached_over_memory",
        }
    }

    fn index(self) -> usize {
        Self::ALL.iter().position(|o| *o == self).expect("listed")
    }
}

/// The node's compile counters.
#[derive(Debug, Default)]
pub(crate) struct CompileMetrics {
    outcomes: [AtomicU64; OUTCOMES],
    /// Worker wall time, ms.
    pub(crate) worker_wall_ms: Log2Histogram,
    /// Worker CPU time as the worker reported it, ms.
    pub(crate) worker_cpu_ms: Log2Histogram,
    /// Worker peak resident memory as the worker reported it, MiB.
    pub(crate) worker_rss_mib: Log2Histogram,
    /// Time a compile waited for a worker slot, ms.
    pub(crate) gate_wait_ms: Log2Histogram,
    /// Time the compile call held its caller, spawn to in-process finish, ms.
    pub(crate) stall_ms: Log2Histogram,
    /// Compiled module size (input bytes), log2 buckets only.
    pub(crate) module_bytes: Log2Histogram,
    accepted_file_hits: AtomicU64,
    accepted_file_misses: AtomicU64,
    accepted_set_size: AtomicU64,
    /// 1 while isolation was requested but is not running.
    isolation_degraded: AtomicU64,
    /// 1 while compiles are being refused transiently for want of memory headroom
    /// (isolation stays on).
    isolation_refusing_for_memory: AtomicU64,
}

/// A read of [`CompileMetrics`] for telemetry.
#[derive(Debug, Clone)]
pub(crate) struct CompileMetricsSnapshot {
    pub outcomes: [u64; OUTCOMES],
    pub worker_wall_ms: [u64; HISTOGRAM_BUCKETS],
    pub worker_cpu_ms: [u64; HISTOGRAM_BUCKETS],
    pub worker_rss_mib: [u64; HISTOGRAM_BUCKETS],
    pub gate_wait_ms: [u64; HISTOGRAM_BUCKETS],
    pub stall_ms: [u64; HISTOGRAM_BUCKETS],
    pub module_bytes: [u64; HISTOGRAM_BUCKETS],
    pub accepted_file_hits: u64,
    pub accepted_file_misses: u64,
    pub accepted_set_size: u64,
    pub isolation_degraded: u64,
    pub isolation_refusing_for_memory: u64,
}

impl CompileMetricsSnapshot {
    pub(crate) fn outcome(&self, o: Outcome) -> u64 {
        self.outcomes[o.index()]
    }
}

impl CompileMetrics {
    pub(crate) fn count(&self, o: Outcome) {
        self.outcomes[o.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Count a request that the verdict cache answered with `refusal`, under
    /// the reason it carries.
    pub(crate) fn count_cached_refusal(
        &self,
        refusal: &super::super::compile_limits::CompileRefusal,
    ) {
        use super::super::compile_limits::CompileRefusal as R;
        // Every scan limit counts together, as it does for the first refusal.
        #[allow(clippy::wildcard_enum_match_arm)]
        let outcome = match refusal {
            R::ModuleTooLarge { .. } => Outcome::CachedRefusedSize,
            R::NotBinary(_) => Outcome::CachedRefusedText,
            R::Malformed(_) => Outcome::CachedRefusedFeature,
            R::OverCpu => Outcome::CachedOverCpu,
            R::OverMemory => Outcome::CachedOverMemory,
            _ => Outcome::CachedRefusedLimit,
        };
        self.count(outcome);
    }

    pub(crate) fn accepted_set_lookup(&self, hit: bool) {
        let c = if hit {
            &self.accepted_file_hits
        } else {
            &self.accepted_file_misses
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_accepted_set_size(&self, n: usize) {
        self.accepted_set_size.store(n as u64, Ordering::Relaxed);
    }

    /// Record whether isolation was requested but is not running. The gauge is
    /// the `compile_isolation_degraded` figure of the telemetry snapshot; the
    /// node's status page reads the same state.
    pub(crate) fn set_degraded(&self, degraded: bool) {
        self.isolation_degraded
            .store(u64::from(degraded), Ordering::Relaxed);
    }

    /// Record whether compiles are being refused transiently for want of memory
    /// headroom. Isolation is still on; this is the
    /// `compile_isolation_refusing_for_memory` figure of the telemetry snapshot.
    pub(crate) fn set_refusing_for_memory(&self, refusing: bool) {
        self.isolation_refusing_for_memory
            .store(u64::from(refusing), Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> CompileMetricsSnapshot {
        CompileMetricsSnapshot {
            outcomes: std::array::from_fn(|i| self.outcomes[i].load(Ordering::Relaxed)),
            worker_wall_ms: self.worker_wall_ms.snapshot(),
            worker_cpu_ms: self.worker_cpu_ms.snapshot(),
            worker_rss_mib: self.worker_rss_mib.snapshot(),
            gate_wait_ms: self.gate_wait_ms.snapshot(),
            stall_ms: self.stall_ms.snapshot(),
            module_bytes: self.module_bytes.snapshot(),
            accepted_file_hits: self.accepted_file_hits.load(Ordering::Relaxed),
            accepted_file_misses: self.accepted_file_misses.load(Ordering::Relaxed),
            accepted_set_size: self.accepted_set_size.load(Ordering::Relaxed),
            isolation_degraded: self.isolation_degraded.load(Ordering::Relaxed),
            isolation_refusing_for_memory: self
                .isolation_refusing_for_memory
                .load(Ordering::Relaxed),
        }
    }
}

/// What goes into the node's periodic telemetry snapshot: the counters and
/// log2 histograms of [`CompileMetrics`], nothing per module.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct CompileTelemetry {
    /// Verdict counts by name (see [`Outcome::name`]); monotonic.
    pub outcomes: BTreeMap<String, u64>,
    pub worker_wall_ms_log2: Vec<u64>,
    pub worker_cpu_ms_log2: Vec<u64>,
    pub worker_rss_mib_log2: Vec<u64>,
    pub gate_wait_ms_log2: Vec<u64>,
    pub stall_ms_log2: Vec<u64>,
    pub module_bytes_log2: Vec<u64>,
    pub accepted_set_hits: u64,
    pub accepted_set_misses: u64,
    pub accepted_set_size: u64,
    /// Gauge `compile_isolation_degraded`: 1 while isolation was requested but is
    /// not running (the startup probe failed), 0 otherwise.
    #[serde(default)]
    pub compile_isolation_degraded: u64,
    /// Gauge `compile_isolation_refusing_for_memory`: 1 while isolation is running
    /// but compiles are being refused transiently for want of memory headroom.
    #[serde(default)]
    pub compile_isolation_refusing_for_memory: u64,
}

impl CompileMetricsSnapshot {
    pub(crate) fn telemetry(&self) -> CompileTelemetry {
        CompileTelemetry {
            outcomes: Outcome::ALL
                .iter()
                .map(|o| (o.name().to_string(), self.outcome(*o)))
                .collect(),
            worker_wall_ms_log2: self.worker_wall_ms.to_vec(),
            worker_cpu_ms_log2: self.worker_cpu_ms.to_vec(),
            worker_rss_mib_log2: self.worker_rss_mib.to_vec(),
            gate_wait_ms_log2: self.gate_wait_ms.to_vec(),
            stall_ms_log2: self.stall_ms.to_vec(),
            module_bytes_log2: self.module_bytes.to_vec(),
            accepted_set_hits: self.accepted_file_hits,
            accepted_set_misses: self.accepted_file_misses,
            accepted_set_size: self.accepted_set_size,
            compile_isolation_degraded: self.isolation_degraded,
            compile_isolation_refusing_for_memory: self.isolation_refusing_for_memory,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_log2() {
        let h = Log2Histogram::default();
        for v in [0, 1, 2, 3, 4, 1000, u64::MAX] {
            h.record(v);
        }
        let s = h.snapshot();
        assert_eq!(s[0], 1, "zero");
        assert_eq!(s[1], 1, "one");
        assert_eq!(s[2], 2, "two and three");
        assert_eq!(s[3], 1, "four");
        assert_eq!(s[10], 1, "1000 has ten bits");
        assert_eq!(s[HISTOGRAM_BUCKETS - 1], 1, "overflow bucket");
    }

    #[test]
    fn every_outcome_has_its_own_counter_and_a_unique_name() {
        let m = CompileMetrics::default();
        for o in Outcome::ALL {
            m.count(o);
        }
        let s = m.snapshot();
        for o in Outcome::ALL {
            assert_eq!(s.outcome(o), 1, "{}", o.name());
        }
        let mut names: Vec<_> = Outcome::ALL.iter().map(|o| o.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), OUTCOMES);
    }
}
