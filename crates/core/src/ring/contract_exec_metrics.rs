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
//! executor-mediated summarize_state invocations
//!   = summarize_wasm_calls + summarize_wasm_uncached
//! executor-mediated get_state_delta invocations
//!   = delta_wasm_calls     + delta_wasm_uncached
//! ```
//!
//! Note the qualifier on the last two: **executor-mediated**, not "total". These
//! arms count WASM driven through the contract executor, which is every
//! invocation on the request and fan-out paths. They are deliberately NOT a
//! whole-process total — see exception 5.
//!
//! Five exceptions to the partition, all deliberate:
//!
//! 1. **A call that never returns records nothing.** An early error — state
//!    missing, params missing — exits before any terminal arm. So does a
//!    cancelled future: both cached functions `await` a state load before the
//!    slow-path arms are reached.
//! 2. **A WASM trap still counts as a WASM call.** The `*_wasm_calls` arms are
//!    recorded immediately BEFORE the invocation they describe, so a trapping
//!    or timing-out `summarize_state` increments the arm and then returns
//!    `Err`. That is the intended sign: these arms answer "how much WASM work
//!    is this peer attempting", and a trap costs the same CPU up to the trap.
//!    It does mean the arms count ATTEMPTS, so during a trap storm
//!    `*_wasm_calls` exceeds the successful returns. The hit arms have no
//!    fallible step after them and so count only successes.
//! 3. **An executor with no `OpManager` records nothing at all** (unit-test and
//!    local-only executors have no `Ring` to attribute the work to). Every
//!    production executor is built by `RuntimePool` with an `OpManager`.
//! 4. **A snapshot can land mid-call.** See [`ContractExecSnapshot`].
//! 5. **The conformance oracle's WASM is not counted.**
//!    `conformance::runtime_oracle::RuntimeOracle` calls `Runtime`'s
//!    `summarize_state` / `get_state_delta` directly rather than through the
//!    executor, and it runs on a real node (`conformance::shadow`'s opt-in probe
//!    loop constructs one per focus contract). Its work is therefore real WASM
//!    execution that these arms do not see. That is why the identity above says
//!    "executor-mediated" rather than "total": the oracle is a diagnostic
//!    probe whose cost belongs to the conformance budget, not to the request
//!    path these counters are read to size. If the oracle ever moves onto the
//!    executor, fold it in and delete this exception — but do NOT quietly
//!    restore the word "total" while a second uncounted WASM caller exists.
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
//! One `Relaxed` `fetch_add` on a `u64` per contract-handler call — a few ns.
//! The fast path takes exactly one increment; the slow paths take one more,
//! alongside a state load and a WASM invocation that dwarf it. Nothing
//! allocates and nothing locks.
//!
//! The two CACHED entry points are reached only through `RuntimePool`'s
//! `&mut self` methods, so they are serialized on the contract-handling loop and
//! the atomics are uncontended there. The `*_uncached` sites are NOT all on that
//! loop — the `contract_ops` PUT/UPDATE sites are reachable from
//! `run_local_node`, and `executor_impl.rs` documents that off-loop work can
//! hold an executor — so treat "uncontended" as the common case, not a
//! guarantee. `fetch_add` is correct under any contention; `Relaxed` is
//! sufficient because each counter is read on its own and publishes no other
//! memory alongside it.
//!
//! # How this is read
//!
//! Constructed once per node in `Ring::new` and shared via `Arc` (per-node, not
//! a process global, so unit tests stay isolated — the #4488 rationale that
//! `ModuleCacheMetrics` was moved off a global for). The executor increments it
//! through `op_manager.ring`; `emit_router_snapshot_telemetry` reads it on the
//! existing 5-minute `router_snapshot` cadence and emits, for EVERY arm, both
//! the monotonic lifetime total (collector-side differencing) and the per-window
//! delta, so a single snapshot answers "was this peer's summarize load cache
//! hits or real WASM work" without a stateful reader. All eight, not a headline
//! subset: mixing 5-minute deltas with lifetime totals under parallel names on
//! one log line invites reading them as comparable magnitudes, and the arm that
//! would be understated that way is `delta_wasm_uncached` — the fan-out delta
//! with no cache in front of it, which can dominate on a client-facing node.
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
///
/// The eight loads are independent and `Relaxed`, so a snapshot taken while the
/// contract loop is running can land between two arms of one call and violate
/// the module-level partition identity by one. That is fine for a rate and for
/// the collector's differencing; do NOT build an alert that asserts the identity
/// holds exactly on a single snapshot, it would flap.
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

impl ContractExecSnapshot {
    /// Difference this snapshot against `prev` field-by-field, advancing `prev`
    /// to this snapshot. Returns the per-window deltas.
    ///
    /// This exists as ONE function rather than eight hand-written
    /// `window_delta(ce.x, &mut prev.x)` calls at the emit site, because a
    /// hand-written set is exactly where a cross-wiring — differencing one arm
    /// against another arm's previous value — hides. Such a slip emits a
    /// plausible number that is pure noise, and the whole point of these
    /// counters is that a plausible-but-wrong number is worse than none. Here
    /// the correspondence is structural and `each_field_differences_its_own_twin`
    /// pins it with distinct per-field values.
    ///
    /// `saturating_sub` can only mask a decrease, and these counters are
    /// monotonic for a process lifetime (nothing resets them — see
    /// `snapshot_is_non_destructive`), so a non-zero mask means the caller
    /// passed a `prev` from a different source.
    pub(crate) fn window_deltas(&self, prev: &mut ContractExecSnapshot) -> ContractExecSnapshot {
        let deltas = ContractExecSnapshot {
            summarize_fast_hits: self
                .summarize_fast_hits
                .saturating_sub(prev.summarize_fast_hits),
            summarize_reload_hits: self
                .summarize_reload_hits
                .saturating_sub(prev.summarize_reload_hits),
            summarize_wasm_calls: self
                .summarize_wasm_calls
                .saturating_sub(prev.summarize_wasm_calls),
            summarize_wasm_uncached: self
                .summarize_wasm_uncached
                .saturating_sub(prev.summarize_wasm_uncached),
            delta_fast_hits: self.delta_fast_hits.saturating_sub(prev.delta_fast_hits),
            delta_reload_hits: self
                .delta_reload_hits
                .saturating_sub(prev.delta_reload_hits),
            delta_wasm_calls: self.delta_wasm_calls.saturating_sub(prev.delta_wasm_calls),
            delta_wasm_uncached: self
                .delta_wasm_uncached
                .saturating_sub(prev.delta_wasm_uncached),
        };
        *prev = *self;
        deltas
    }
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

    /// Every per-window delta must difference its OWN twin. A cross-wiring here
    /// emits the difference of two unrelated arms — a plausible number that is
    /// pure noise — which is the failure these counters exist to prevent, not
    /// to commit. Distinct per-field values, so a swap cannot pass.
    #[test]
    fn each_field_differences_its_own_twin() {
        let mut prev = ContractExecSnapshot {
            summarize_fast_hits: 10,
            summarize_reload_hits: 20,
            summarize_wasm_calls: 30,
            summarize_wasm_uncached: 40,
            delta_fast_hits: 50,
            delta_reload_hits: 60,
            delta_wasm_calls: 70,
            delta_wasm_uncached: 80,
        };
        let now = ContractExecSnapshot {
            summarize_fast_hits: 11,
            summarize_reload_hits: 22,
            summarize_wasm_calls: 33,
            summarize_wasm_uncached: 44,
            delta_fast_hits: 55,
            delta_reload_hits: 66,
            delta_wasm_calls: 77,
            delta_wasm_uncached: 88,
        };

        let d = now.window_deltas(&mut prev);
        assert_eq!(d.summarize_fast_hits, 1);
        assert_eq!(d.summarize_reload_hits, 2);
        assert_eq!(d.summarize_wasm_calls, 3);
        assert_eq!(d.summarize_wasm_uncached, 4);
        assert_eq!(d.delta_fast_hits, 5);
        assert_eq!(d.delta_reload_hits, 6);
        assert_eq!(d.delta_wasm_calls, 7);
        assert_eq!(d.delta_wasm_uncached, 8);

        // `prev` advanced to `now`, so an immediately repeated window is all
        // zeroes rather than re-reporting the same work.
        assert_eq!(prev, now);
        assert_eq!(
            now.window_deltas(&mut prev),
            ContractExecSnapshot::default()
        );
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
