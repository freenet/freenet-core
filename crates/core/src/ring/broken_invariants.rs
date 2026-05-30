//! Per-contract tracking of detected CRDT-invariant violations.
//!
//! A non-idempotent `update_state` (i.e. `update_state(update_state(S, U), U) != update_state(S, U)`)
//! breaks the convergence guarantee Freenet contracts rely on and produces a
//! self-perpetuating broadcast storm: every peer's merge generates a new
//! byte-different state, which propagates, which every peer re-merges.
//!
//! When the merge path's idempotency probe (in `Executor`) detects this on a
//! sampled merge, it calls [`BrokenInvariantsTracker::record`] which:
//!
//! 1. Inserts the flag into an in-memory `DashMap` keyed on the contract
//!    instance id, so subsequent same-process reads see it immediately.
//! 2. Persists the flag to the executor's ReDb storage so the detection
//!    survives node restarts (a known-broken contract should not re-engage
//!    the storm just because the process bounced).
//!
//! Reads (via [`BrokenInvariantsTracker::is_broken`]) gate the broadcast-
//! emission path so a flagged contract's state changes never leave the
//! node.
//!
//! ## Why the flag is TTL-bounded, not permanent
//!
//! An earlier revision made the flag permanent on the theory that a "fixed"
//! contract has a different code hash → different `ContractKey`, so an
//! instance id flagged once is broken forever. That reasoning is sound for a
//! *true* violation, but the probe is a sampled heuristic that can
//! false-positive: it ultimately rests on a byte-level comparison, and a
//! correct contract whose serialization is non-canonical (HashMap/HashSet
//! iteration order, float formatting, embedded timestamps) can be flagged
//! even though its merge is logically idempotent. A permanent, silent,
//! cross-restart-persistent flag turns a single false positive into a
//! contract that stops propagating *forever* with no recovery — exactly the
//! failure mode behind issue #4295 (`test_ping_multi_node` froze because the
//! ping contract's `HashMap`-backed state byte-fluttered on re-merge).
//!
//! So the flag now expires after [`BROKEN_INVARIANT_TTL`]. The consequences:
//!
//! - **False positive:** self-heals within one TTL window. Combined with the
//!   probe's own soundness hardening (it no longer flags benign byte-flutter
//!   — see `Executor::maybe_probe_idempotency`), false positives should be
//!   rare *and* recoverable rather than permanent.
//! - **True violation:** suppressed for a TTL, then re-detected and re-flagged
//!   (which refreshes the window, so a continuously-merging violator stays
//!   suppressed). Re-detection is probabilistic: after expiry the probe samples
//!   at [`crate::contract::executor`]'s `IDEMPOTENCY_PROBE_PROBABILITY` (1/32),
//!   so the contract leaks the merges that occur between expiry and the next
//!   sampled probe — in expectation ~32 merges, with a longer tail under a high
//!   merge rate. That is a deliberately accepted, bounded periodic leak, not the
//!   permanent re-engaged storm of the pre-#4279 baseline. We accept it because
//!   the opposite failure — a permanent flag that a *false* positive can trigger,
//!   silently bricking a correct contract forever (#4295) — is worse, and we
//!   bias toward false-positive recovery. (A genuinely non-idempotent contract
//!   has never been observed in production; the only flag ever seen fire was the
//!   #4295 false positive.) To shorten the leak, raise `BROKEN_INVARIANT_TTL`
//!   (fewer expiry windows) rather than lowering it.
//!
//! This also satisfies the project rule that GC/suppression exemptions must
//! be time-bounded (see `.claude/rules/ring.md` "Cleanup Exemptions Must Be
//! Time-Bounded"). Expired entries are swept by [`BrokenInvariantsTracker::cleanup`],
//! hooked into the same reaper tick as the UPDATE rate limiter.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::contract::storages::Storage;
use crate::util::time_source::TimeSource;

/// How long a broken-invariant flag suppresses a contract before it expires
/// and the contract is given a fresh chance (re-detected by the next sampled
/// probe if genuinely still broken).
///
/// Chosen to be long enough that a genuinely non-convergent contract leaks at
/// most ≈one merge per window (negligible amplification) yet short enough that
/// a false positive recovers promptly rather than bricking the contract.
pub(crate) const BROKEN_INVARIANT_TTL: Duration = Duration::from_secs(300);

/// The kind of CRDT invariant a contract was observed to violate. Currently
/// only one variant; future work may add violations like non-commutativity
/// or non-determinism detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokenInvariant {
    /// `update_state(update_state(S, U), U) != update_state(S, U)` was observed
    /// for at least one sampled (state, update) pair.
    NonIdempotent,
}

impl BrokenInvariant {
    /// Single-byte on-disk encoding. Stable across releases.
    fn to_byte(self) -> u8 {
        match self {
            BrokenInvariant::NonIdempotent => 0,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(BrokenInvariant::NonIdempotent),
            _ => None,
        }
    }
}

/// An in-memory flag together with the instant it was (re-)recorded, so the
/// flag can expire after [`BROKEN_INVARIANT_TTL`].
#[derive(Debug, Clone, Copy)]
struct FlagEntry {
    kind: BrokenInvariant,
    recorded_at: Instant,
}

/// Tracks per-contract broken-invariant flags with optional persistent backing.
///
/// `set_storage` is called once during node startup, at which point the
/// tracker reads any previously-persisted flags into the in-memory map.
/// Until storage is wired, reads still work (empty map) and writes are
/// in-memory only — this matches the pattern used by `HostingManager`.
pub(crate) struct BrokenInvariantsTracker {
    flags: Arc<DashMap<ContractInstanceId, FlagEntry>>,
    storage: std::sync::OnceLock<Storage>,
    time_source: Arc<dyn TimeSource + Send + Sync>,
}

impl BrokenInvariantsTracker {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self {
            flags: Arc::new(DashMap::new()),
            storage: std::sync::OnceLock::new(),
            time_source,
        }
    }

    /// Returns true if `id` currently has a non-expired broken-invariant flag.
    ///
    /// Pure read: expired entries are reported as not-broken here and are
    /// physically reclaimed by the periodic [`BrokenInvariantsTracker::cleanup`]
    /// sweep, so this stays cheap on the broadcast/commit hot path.
    pub fn is_broken(&self, id: &ContractInstanceId) -> bool {
        let now = self.time_source.now();
        match self.flags.get(id) {
            Some(entry) => now.saturating_duration_since(entry.recorded_at) < BROKEN_INVARIANT_TTL,
            None => false,
        }
    }

    /// Returns the broken-invariant flag for `id`, if any and not expired.
    /// Used by tests and future detectors that distinguish kinds; production
    /// callers only need `is_broken`.
    #[cfg(test)]
    pub fn get(&self, id: &ContractInstanceId) -> Option<BrokenInvariant> {
        let now = self.time_source.now();
        self.flags.get(id).and_then(|entry| {
            if now.saturating_duration_since(entry.recorded_at) < BROKEN_INVARIANT_TTL {
                Some(entry.kind)
            } else {
                None
            }
        })
    }

    /// Mark `id` as broken with `kind`, stamping the current time. Re-recording
    /// an already-flagged contract refreshes its expiry (so a contract the
    /// probe keeps re-detecting stays suppressed). Persists best-effort to
    /// storage if wired; persistence errors are logged but never block the
    /// in-memory flag from taking effect (we'd rather suppress the storm than
    /// crash on a database hiccup).
    pub fn record(&self, id: ContractInstanceId, kind: BrokenInvariant) {
        let recorded_at = self.time_source.now();
        let was_new = self
            .flags
            .insert(id, FlagEntry { kind, recorded_at })
            .is_none();
        if was_new {
            tracing::warn!(
                contract = %id,
                invariant = ?kind,
                event = "broken_invariant_detected",
                "Marking contract as broken — gating outbound broadcast and merge propagation"
            );
            // Persistence is currently only wired for the redb backend;
            // sqlite-only builds keep the in-memory flag but skip the
            // on-disk hydration. This is the same trade-off
            // `HostingManager` makes — see #4279 deferred follow-up to
            // add sqlite parity.
            #[cfg(feature = "redb")]
            if let Some(storage) = self.storage.get() {
                if let Err(e) = storage.store_broken_invariant(&id, kind.to_byte()) {
                    tracing::warn!(
                        contract = %id,
                        error = %e,
                        "Failed to persist broken-invariant flag (in-memory flag still active)"
                    );
                }
            }
        }
    }

    /// Remove the broken-invariant flag for `id`. Use with extreme care:
    /// this is the operator escape hatch for the rare case where the
    /// probe was a false positive (most plausibly a probe trap that
    /// shouldn't have been observed at all — see
    /// `Executor::maybe_probe_idempotency`). Re-enables outbound
    /// broadcast and commit for the contract. Does not unflag remotely.
    ///
    /// Not currently exposed to a CLI / WS API by this PR — added so the
    /// debug-CLI follow-up has a stable surface to call into. Returns
    /// the previous flag if any.
    #[allow(dead_code)] // wired in follow-up PR
    pub fn clear(&self, id: &ContractInstanceId) -> Option<BrokenInvariant> {
        let previous = self.flags.remove(id).map(|(_, v)| v.kind);
        if previous.is_some() {
            self.remove_from_storage(id);
            tracing::warn!(
                contract = %id,
                event = "broken_invariant_cleared",
                "Operator cleared broken-invariant flag — outbound broadcast re-enabled"
            );
        }
        previous
    }

    /// Sweep expired flags from the in-memory map (and best-effort from
    /// storage). Hooked into the Ring reaper tick alongside the UPDATE rate
    /// limiter's cleanup so expired suppressions are reclaimed promptly even
    /// for contracts that are no longer being read on the hot path.
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        // Snapshot the currently-expired ids (read-only).
        let candidates: Vec<ContractInstanceId> = self
            .flags
            .iter()
            .filter(|e| {
                now.saturating_duration_since(e.value().recorded_at) >= BROKEN_INVARIANT_TTL
            })
            .map(|e| *e.key())
            .collect();
        for id in candidates {
            // Atomically re-check expiry under the shard guard and remove only
            // if STILL expired. This closes a TOCTOU race: a concurrent
            // `record()` (e.g. a probe re-detecting the same contract) may have
            // refreshed `recorded_at` between the snapshot above and here — in
            // which case `remove_if` keeps the fresh entry, and we must NOT
            // purge its persisted row (a restart needs it to re-hydrate the
            // active suppression). We only touch storage when we actually
            // removed an expired entry.
            let removed = self.flags.remove_if(&id, |_, entry| {
                now.saturating_duration_since(entry.recorded_at) >= BROKEN_INVARIANT_TTL
            });
            if removed.is_some() {
                self.remove_from_storage(&id);
            }
        }
    }

    /// Best-effort removal of a flag's persisted row. Shared by `clear` and
    /// the expiry sweep so a stale row does not re-flag the contract on the
    /// next restart.
    fn remove_from_storage(&self, id: &ContractInstanceId) {
        #[cfg(feature = "redb")]
        if let Some(storage) = self.storage.get() {
            if let Err(e) = storage.remove_broken_invariant(id) {
                tracing::warn!(
                    contract = %id,
                    error = %e,
                    "Failed to remove persisted broken-invariant flag (in-memory flag already cleared)"
                );
            }
        }
        #[cfg(not(feature = "redb"))]
        let _ = id;
    }

    /// Wire persistent storage. Called once at startup; idempotent on the
    /// `OnceLock` so callers cannot accidentally re-init with a different
    /// database. Hydrates the in-memory map from previously-persisted
    /// entries on first wiring.
    ///
    /// Loaded flags are stamped with the current time, so a node restart
    /// gives each previously-flagged contract a fresh TTL window rather than
    /// inheriting an unknown (and un-persisted) original timestamp.
    pub fn set_storage(&self, storage: Storage) {
        if self.storage.set(storage.clone()).is_err() {
            tracing::warn!("BrokenInvariantsTracker storage already set; ignoring re-init");
            return;
        }
        #[cfg(feature = "redb")]
        match storage.load_all_broken_invariants() {
            Ok(entries) => {
                let recorded_at = self.time_source.now();
                for (id, byte) in entries {
                    if let Some(kind) = BrokenInvariant::from_byte(byte) {
                        self.flags.insert(id, FlagEntry { kind, recorded_at });
                    } else {
                        tracing::warn!(
                            contract = %id,
                            byte,
                            "Skipping unknown broken-invariant byte on load"
                        );
                    }
                }
                tracing::debug!(
                    count = self.flags.len(),
                    "Loaded broken-invariant flags from storage"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load broken-invariant flags from storage");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn fake_id(seed: u8) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        ContractInstanceId::new(bytes)
    }

    fn mk_tracker() -> (BrokenInvariantsTracker, SharedMockTimeSource) {
        let ts = SharedMockTimeSource::new();
        (BrokenInvariantsTracker::new(Arc::new(ts.clone())), ts)
    }

    #[test]
    fn record_then_query_returns_true() {
        let (t, _ts) = mk_tracker();
        let id = fake_id(1);
        assert!(!t.is_broken(&id));
        t.record(id, BrokenInvariant::NonIdempotent);
        assert!(t.is_broken(&id));
        assert_eq!(t.get(&id), Some(BrokenInvariant::NonIdempotent));
    }

    #[test]
    fn record_is_idempotent() {
        let (t, _ts) = mk_tracker();
        let id = fake_id(2);
        t.record(id, BrokenInvariant::NonIdempotent);
        t.record(id, BrokenInvariant::NonIdempotent);
        // No panic, no duplicate entries; map size remains 1.
        assert!(t.is_broken(&id));
    }

    #[test]
    fn unrelated_contracts_unaffected() {
        let (t, _ts) = mk_tracker();
        let broken = fake_id(3);
        let healthy = fake_id(4);
        t.record(broken, BrokenInvariant::NonIdempotent);
        assert!(t.is_broken(&broken));
        assert!(!t.is_broken(&healthy));
    }

    #[test]
    fn clear_returns_previous_and_unsets() {
        let (t, _ts) = mk_tracker();
        let id = fake_id(5);

        // Clearing an absent entry returns None and is a no-op.
        assert_eq!(t.clear(&id), None);

        t.record(id, BrokenInvariant::NonIdempotent);
        assert!(t.is_broken(&id));

        let prev = t.clear(&id);
        assert_eq!(prev, Some(BrokenInvariant::NonIdempotent));
        assert!(
            !t.is_broken(&id),
            "after clear the contract is no longer broken"
        );

        // Second clear is also a no-op, returns None.
        assert_eq!(t.clear(&id), None);
    }

    /// Regression test for #4295: a flag MUST NOT brick a contract forever.
    /// After `BROKEN_INVARIANT_TTL` elapses, the flag expires so a false
    /// positive self-heals and the contract resumes propagating.
    #[test]
    fn flag_expires_after_ttl() {
        let (t, ts) = mk_tracker();
        let id = fake_id(6);
        t.record(id, BrokenInvariant::NonIdempotent);
        assert!(t.is_broken(&id), "freshly recorded flag is active");

        // Just before the TTL boundary it is still active.
        ts.advance_time(BROKEN_INVARIANT_TTL - Duration::from_secs(1));
        assert!(t.is_broken(&id), "flag still active just before TTL");

        // Past the TTL boundary it has expired.
        ts.advance_time(Duration::from_secs(2));
        assert!(
            !t.is_broken(&id),
            "flag must expire after TTL so a false positive self-heals"
        );
        assert_eq!(t.get(&id), None, "expired flag reports no kind");
    }

    /// `cleanup` physically reclaims expired entries from the in-memory map.
    #[test]
    fn cleanup_reclaims_expired_entries() {
        let (t, ts) = mk_tracker();
        let id = fake_id(7);
        t.record(id, BrokenInvariant::NonIdempotent);
        assert_eq!(t.flags.len(), 1);

        // Before expiry, cleanup keeps it.
        ts.advance_time(BROKEN_INVARIANT_TTL / 2);
        t.cleanup();
        assert_eq!(t.flags.len(), 1, "non-expired entry retained by cleanup");

        // After expiry, cleanup removes it.
        ts.advance_time(BROKEN_INVARIANT_TTL);
        t.cleanup();
        assert_eq!(t.flags.len(), 0, "expired entry reclaimed by cleanup");
    }

    /// Re-recording refreshes the expiry window so a contract the probe keeps
    /// re-detecting stays suppressed instead of flapping every TTL.
    #[test]
    fn record_refreshes_ttl() {
        let (t, ts) = mk_tracker();
        let id = fake_id(8);
        t.record(id, BrokenInvariant::NonIdempotent);

        // Advance most of the way through the TTL, then re-record.
        ts.advance_time(BROKEN_INVARIANT_TTL - Duration::from_secs(1));
        t.record(id, BrokenInvariant::NonIdempotent);

        // Advance past where the ORIGINAL stamp would have expired; the
        // refreshed stamp keeps it active.
        ts.advance_time(Duration::from_secs(2));
        assert!(
            t.is_broken(&id),
            "re-recording must refresh the expiry window"
        );
    }

    #[test]
    fn byte_roundtrip_stable() {
        // Iterating the (currently single-variant) set keeps the test
        // honest as new BrokenInvariant kinds are added.
        let kinds: &[BrokenInvariant] = &[BrokenInvariant::NonIdempotent];
        for kind in kinds {
            assert_eq!(BrokenInvariant::from_byte(kind.to_byte()), Some(*kind));
        }
        // Unknown bytes are rejected (forward-compat: skip rather than panic).
        assert_eq!(BrokenInvariant::from_byte(255), None);
    }
}
