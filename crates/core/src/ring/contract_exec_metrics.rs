//! Contract-exec WASM observability (#5168 sibling; #4440 / #4473 / #4610 /
//! #5238 storm lineage).
//!
//! # Why this exists
//!
//! Every rate figure quoted in the recurring "summarize storm" investigations
//! (#4473 ~40/sec, #4610 ~70-80/sec, #5040 ~49-57/sec, #5238 ~50-95/sec) came
//! from the DROPPED-LINE counter of the tracing rate limiter
//! (`crate::util::rate_limit_layer`), which caps LOG EVENTS at 30/sec per
//! callsite and has no effect whatsoever on work done. Worse, the span it
//! counts (`contract.rs`'s `info_span!("summarize_contract_state")`) is opened
//! at HANDLER ENTRY, so it fires identically for a state-hash cache HIT and for
//! a real WASM `summarize_state` invocation.
//!
//! Those two are separated by orders of magnitude in cost — the cache is the
//! entire reason the observed rate is survivable — so the headline number could
//! not distinguish "this peer is burning CPU in WASM" from "this peer is
//! serving a warm cache". Five PRs in five weeks bounded or gated call sites
//! against that undifferentiated number.
//!
//! These counters close that gap. They are recorded BY the code that makes each
//! decision (never re-derived by a caller from set sizes — see the "metric
//! describing a filtering decision" row in `bug-prevention-patterns.md`), and
//! they PARTITION each cached path exactly, so no consumer ever has to subtract
//! one counter from another to get an answer:
//!
//! ```text
//! calls to bridged_summarize_contract_state
//!   = summarize_fast_hits + summarize_reload_hits + summarize_wasm_calls
//! calls to bridged_get_contract_state_delta
//!   = delta_fast_hits     + delta_reload_hits     + delta_wasm_calls
//!
//! total WASM summarize_state invocations = summarize_wasm_calls + summarize_wasm_uncached
//! total WASM get_state_delta invocations = delta_wasm_calls     + delta_wasm_uncached
//! ```
//!
//! (Both partitions hold only for a call that RETURNS; an error return — state
//! missing, params missing, WASM trap — exits without recording a terminal arm.
//! Those are already loud in their own right, and a silent counter drift would
//! be worse than a small under-count.)
//!
//! # What each counter answers
//!
//! - `*_fast_hits` — the state-change detector held a hash for the contract's
//!   current state AND a summary/delta was cached against exactly that hash.
//!   Neither the state nor the WASM module was touched. This is the arm that
//!   makes a 50/sec anti-entropy cadence cheap.
//! - `*_reload_hits` — the detector was cold (restart, eviction) so the full
//!   state was loaded and hashed, but the cache already held an entry for the
//!   recomputed hash. Real I/O and hashing, no WASM. A high value here means the
//!   detector is being invalidated more than the cache is.
//! - `*_wasm_calls` — the WASM module actually ran, from the cached path. This
//!   is the expensive work every storm fix was trying to bound.
//! - `*_wasm_uncached` — the WASM module ran from a call site that has NO cache
//!   in front of it (the PUT/UPDATE summary sites in `contract_ops`, the local
//!   client-notification delta fan-out). Counted separately so the cached-path
//!   partition above stays exact while the WASM TOTAL stays honest.
//!
//! # Cost
//!
//! One `Relaxed` `fetch_add` on a `u64` per contract-handler call — a few ns,
//! uncontended (the contract-handling loop has concurrency exactly 1, enforced
//! by `&mut self`). The fast path takes exactly one increment; the slow paths
//! take one more, alongside a state load and a WASM invocation that dwarf it.
//! Nothing allocates and nothing locks.
//!
//! # How this is read
//!
//! Constructed once per node in `Ring::new` and shared via `Arc` (per-node, not
//! a process global, so unit tests stay isolated — the #4488 rationale that
//! `ModuleCacheMetrics` was moved off a global for). The executor increments it
//! through `op_manager.ring`; `emit_router_snapshot_telemetry` reads it on the
//! existing 5-minute `router_snapshot` cadence and emits both the monotonic
//! lifetime totals (collector-side differencing) and the per-window deltas for
//! the four headline arms, so a single snapshot answers "was this peer's
//! summarize load cache hits or real WASM work" without a stateful reader.
//!
//! Note the deliberate choice of an EXISTING telemetry stream: `router_snapshot`
//! already carries this class of per-node monotonic counter
//! (`contract_module_cache_evictions_total`, `broadcast_stream_*_total`), so
//! these fields cost no shadow-rollup budget slot (`MAX_SHADOW_EVENTS_PER_SECOND`
//! is 6 with one slot of headroom) and land in a `tracing::info!` line that
//! survives `release_max_level_info` for local `journalctl` reading.

use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative lifetime counts of contract-exec WASM invocations and the cache
/// hits that elided them. See the module docs for the partition and cost.
#[derive(Debug, Default)]
pub(crate) struct ContractExecMetrics {
    /// `bridged_summarize_contract_state` returned a cached summary from the
    /// fast path: detector hash present and matching a cached summary. No state
    /// load, no hash, no WASM.
    summarize_fast_hits: AtomicU64,
    /// `bridged_summarize_contract_state` reached the slow path (state loaded
    /// and hashed) but the summary cache already held an entry for the
    /// recomputed hash, so the WASM call was still elided.
    summarize_reload_hits: AtomicU64,
    /// WASM `summarize_state` ran from the CACHED path
    /// (`bridged_summarize_contract_state`) — a true cache miss.
    summarize_wasm_calls: AtomicU64,
    /// WASM `summarize_state` ran from a call site with no cache in front of it
    /// (the PUT/UPDATE response-summary sites in `contract_ops`).
    summarize_wasm_uncached: AtomicU64,
    /// `bridged_get_contract_state_delta` returned a cached delta from the fast
    /// path: detector hash present and a delta cached for that exact
    /// (state, peer-summary) pair.
    delta_fast_hits: AtomicU64,
    /// `bridged_get_contract_state_delta` reached the slow path (state loaded
    /// and hashed) but the delta cache already held the recomputed key.
    delta_reload_hits: AtomicU64,
    /// WASM `get_state_delta` ran from the CACHED path
    /// (`bridged_get_contract_state_delta`) — a true cache miss.
    delta_wasm_calls: AtomicU64,
    /// WASM `get_state_delta` ran from a call site with no cache in front of it
    /// (the per-subscriber local client-notification fan-out).
    delta_wasm_uncached: AtomicU64,
}

/// A point-in-time read of [`ContractExecMetrics`] for telemetry emission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ContractExecSnapshot {
    pub summarize_fast_hits: u64,
    pub summarize_reload_hits: u64,
    pub summarize_wasm_calls: u64,
    pub summarize_wasm_uncached: u64,
    pub delta_fast_hits: u64,
    pub delta_reload_hits: u64,
    pub delta_wasm_calls: u64,
    pub delta_wasm_uncached: u64,
}

impl ContractExecMetrics {
    /// Record a summary served from the fast path (no state load, no WASM).
    #[inline]
    pub(crate) fn record_summarize_fast_hit(&self) {
        self.summarize_fast_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a summary served from the cache after a state reload+rehash.
    #[inline]
    pub(crate) fn record_summarize_reload_hit(&self) {
        self.summarize_reload_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WASM `summarize_state` invocation on the cached path.
    #[inline]
    pub(crate) fn record_summarize_wasm_call(&self) {
        self.summarize_wasm_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WASM `summarize_state` invocation on an uncached call site.
    #[inline]
    pub(crate) fn record_summarize_wasm_uncached(&self) {
        self.summarize_wasm_uncached.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delta served from the fast path (no state load, no WASM).
    #[inline]
    pub(crate) fn record_delta_fast_hit(&self) {
        self.delta_fast_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a delta served from the cache after a state reload+rehash.
    #[inline]
    pub(crate) fn record_delta_reload_hit(&self) {
        self.delta_reload_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WASM `get_state_delta` invocation on the cached path.
    #[inline]
    pub(crate) fn record_delta_wasm_call(&self) {
        self.delta_wasm_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WASM `get_state_delta` invocation on an uncached call site.
    #[inline]
    pub(crate) fn record_delta_wasm_uncached(&self) {
        self.delta_wasm_uncached.fetch_add(1, Ordering::Relaxed);
    }

    /// Read all counters for telemetry.
    pub(crate) fn snapshot(&self) -> ContractExecSnapshot {
        ContractExecSnapshot {
            summarize_fast_hits: self.summarize_fast_hits.load(Ordering::Relaxed),
            summarize_reload_hits: self.summarize_reload_hits.load(Ordering::Relaxed),
            summarize_wasm_calls: self.summarize_wasm_calls.load(Ordering::Relaxed),
            summarize_wasm_uncached: self.summarize_wasm_uncached.load(Ordering::Relaxed),
            delta_fast_hits: self.delta_fast_hits.load(Ordering::Relaxed),
            delta_reload_hits: self.delta_reload_hits.load(Ordering::Relaxed),
            delta_wasm_calls: self.delta_wasm_calls.load(Ordering::Relaxed),
            delta_wasm_uncached: self.delta_wasm_uncached.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_recorder_lands_on_its_own_counter() {
        let m = ContractExecMetrics::default();
        assert_eq!(m.snapshot(), ContractExecSnapshot::default());

        // Distinct counts per arm: a copy-paste slip that made two recorders
        // share a field would still produce a plausible non-zero snapshot, so
        // the multiplicities have to differ for the assertions to bite.
        m.record_summarize_fast_hit();
        for _ in 0..2 {
            m.record_summarize_reload_hit();
        }
        for _ in 0..3 {
            m.record_summarize_wasm_call();
        }
        for _ in 0..4 {
            m.record_summarize_wasm_uncached();
        }
        for _ in 0..5 {
            m.record_delta_fast_hit();
        }
        for _ in 0..6 {
            m.record_delta_reload_hit();
        }
        for _ in 0..7 {
            m.record_delta_wasm_call();
        }
        for _ in 0..8 {
            m.record_delta_wasm_uncached();
        }

        let s = m.snapshot();
        assert_eq!(s.summarize_fast_hits, 1);
        assert_eq!(s.summarize_reload_hits, 2);
        assert_eq!(s.summarize_wasm_calls, 3);
        assert_eq!(s.summarize_wasm_uncached, 4);
        assert_eq!(s.delta_fast_hits, 5);
        assert_eq!(s.delta_reload_hits, 6);
        assert_eq!(s.delta_wasm_calls, 7);
        assert_eq!(s.delta_wasm_uncached, 8);
    }

    #[test]
    fn snapshot_is_non_destructive() {
        let m = ContractExecMetrics::default();
        m.record_summarize_wasm_call();
        assert_eq!(m.snapshot().summarize_wasm_calls, 1);
        // Unlike the drained rollup windows (`outbound_message_mix`,
        // `broadcast_payload_mix`), these are monotonic lifetime totals:
        // reading must not reset them, or a dropped telemetry event would lose
        // that interval permanently.
        assert_eq!(m.snapshot().summarize_wasm_calls, 1);
        m.record_summarize_wasm_call();
        assert_eq!(m.snapshot().summarize_wasm_calls, 2);
    }
}
