//! Per-contract tracking of detected CRDT-invariant violations.
//!
//! A non-idempotent `update_state` (i.e. `update_state(update_state(S, U), U) != update_state(S, U)`)
//! breaks the convergence guarantee Freenet contracts rely on and produces a
//! self-perpetuating broadcast storm: every peer's merge generates a new
//! byte-different state, which propagates, which every peer re-merges.
//!
//! Two detectors in `Executor` feed this tracker: the sampled re-apply
//! probe (`maybe_probe_idempotency`, 1/32 of pure-`State` merges) and the
//! deterministic identical-input probe
//! (`probe_identical_input_idempotency`): when an incoming full-`State`
//! payload is byte-identical to the stored state, `update_state(S, State(S))`
//! must reach a fixpoint by CvRDT lattice-join semantics, so a
//! cooldown-bounded ([`IDENTITY_PROBE_COOLDOWN`]) re-apply sequence whose
//! byte MULTISET keeps changing is deterministic proof of non-idempotency
//! (no sampling; a contract that canonicalizes once and then stabilizes is
//! NOT flagged).
//!
//! When a detector fires, it calls [`BrokenInvariantsTracker::record`] which:
//!
//! 1. Inserts the flag into an in-memory `DashMap` keyed on the contract
//!    instance id, so subsequent same-process reads see it immediately.
//! 2. Persists the flag to the executor's ReDb storage so the detection
//!    survives node restarts (a known-broken contract should not re-engage
//!    the storm just because the process bounced).
//!
//! Reads (via [`BrokenInvariantsTracker::is_broken`]) gate the executor's
//! commit + broadcast-emission paths AND the full-state egress paths
//! (`ResyncResponse` in node.rs, `SyncStateToPeer` and the
//! `handle_broadcast_state_change` fan-out in p2p_protoc/broadcast.rs) so a
//! flagged contract's state never leaves the node.
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
//! ## Escalating TTL on re-detection (#4903 review B-tweak)
//!
//! A fixed TTL turns a genuinely non-idempotent contract into a limit-cycle
//! OSCILLATOR once #4902's egress gates suppress it: the flagged node goes
//! dark for one TTL, the flag expires, the node re-enters the still-divergent
//! mesh, a cascade burst (sized by the drift accumulated while it was dark)
//! re-flags it, and the cycle repeats — raising per-peer emissions well above
//! steady participation. To damp this, the TTL ESCALATES on re-detection:
//! first detection stays at [`BROKEN_INVARIANT_TTL`], and each re-detection
//! within the re-flag window doubles it (capped at
//! [`BROKEN_INVARIANT_TTL_CAP`]). A genuinely-broken contract therefore
//! converges to mostly-dark — its re-entry duty cycle collapses geometrically,
//! drift shrinks as fewer peers advance it, and the oscillation dies out. A
//! contract that stops being re-detected for a full quiet window resets to
//! baseline, and a contract flagged exactly once (the #4295 false positive)
//! never escalates at all. See [`BrokenInvariantsTracker::record`].
//!
//! - **True violation:** suppressed for a TTL, then re-detected and re-flagged
//!   with an ESCALATED (doubled) TTL, so a continuously-oscillating violator's
//!   dark window grows each cycle until it is effectively silent. Re-detection
//!   is probabilistic: after expiry the probe samples
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

/// Baseline (FIRST-detection) suppression TTL: how long a freshly-flagged
/// contract is suppressed before its flag expires and it is given a fresh
/// chance (re-detected by the next sampled probe if genuinely still broken).
///
/// Chosen to be long enough that a genuinely non-convergent contract leaks at
/// most ≈one merge per window (negligible amplification) yet short enough that
/// a false positive recovers promptly rather than bricking the contract. A
/// contract flagged exactly ONCE (the #4295 false-positive shape) is
/// suppressed for exactly this long and then recovers — escalation (below)
/// never touches it, so this preserves the pre-escalation false-positive
/// conservatism unchanged.
pub(crate) const BROKEN_INVARIANT_TTL: Duration = Duration::from_secs(300);

/// Ceiling for the escalated suppression TTL (#4903 review B-tweak, stops the
/// #4902 egress oscillation). A contract that keeps being RE-detected after
/// each expiry doubles its TTL toward this cap; past it the TTL stops growing.
/// Six hours: long enough that a genuinely-broken contract's re-entry
/// oscillation collapses to a negligible duty cycle, bounded so even a
/// (vanishingly unlikely) escalated false positive still self-heals within a
/// working day.
pub(crate) const BROKEN_INVARIANT_TTL_CAP: Duration = Duration::from_secs(6 * 60 * 60);

/// Re-flag window multiplier (#4903 review B-tweak). Measured from the last
/// detection, the window `[ttl, REFLAG_WINDOW_MULT × ttl)` is the "still
/// oscillating" band: a re-detection inside it ESCALATES (doubles) the TTL,
/// because the contract went dark for its TTL and then re-entered the mesh and
/// was caught again. A re-detection at or beyond `REFLAG_WINDOW_MULT × ttl`
/// means the contract stayed quiet for an escalated TTL PLUS a full window, so
/// escalation RESETS to baseline (it is no longer oscillating — genuinely
/// fixed, or the flag was a one-off false positive). This multiplier is also
/// the age at which [`BrokenInvariantsTracker::cleanup`] reclaims an idle
/// entry, so the escalation memory outlives the suppression window (allowing a
/// prompt re-detection to escalate rather than restart) but stays bounded.
const REFLAG_WINDOW_MULT: u32 = 2;

/// Per-contract cooldown for the deterministic identical-input idempotency
/// probe (`Executor::probe_identical_input_idempotency`). The #4151
/// identical-push fast path exists because byte-identical re-pushes are the
/// dominant dedup-miss case; re-running the merge on EVERY one would
/// reintroduce the WASM cost that fix removed. One probe per contract per
/// cooldown keeps detection deterministic (a violating contract is caught
/// on the first identical apply after each cooldown — no 1/32 sampling) at
/// a bounded cost.
pub(crate) const IDENTITY_PROBE_COOLDOWN: Duration = Duration::from_secs(60);

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

/// An in-memory flag together with the instant it was (re-)recorded and the
/// CURRENT (possibly escalated) suppression TTL for this detection, so the
/// flag can expire after its own `ttl` and the escalation policy can grow that
/// TTL on repeated re-detection (#4903 review B-tweak).
#[derive(Debug, Clone, Copy)]
struct FlagEntry {
    kind: BrokenInvariant,
    recorded_at: Instant,
    /// Suppression TTL for THIS detection cycle. Starts at
    /// [`BROKEN_INVARIANT_TTL`] on first detection and doubles (capped at
    /// [`BROKEN_INVARIANT_TTL_CAP`]) on each re-detection within the re-flag
    /// window; resets to baseline after a full quiet window. In-memory only —
    /// a restart re-hydrates flags at the baseline TTL.
    ttl: Duration,
}

/// Tracks per-contract broken-invariant flags with optional persistent backing.
///
/// `set_storage` is called once during node startup, at which point the
/// tracker reads any previously-persisted flags into the in-memory map.
/// Until storage is wired, reads still work (empty map) and writes are
/// in-memory only — this matches the pattern used by `HostingManager`.
pub(crate) struct BrokenInvariantsTracker {
    flags: Arc<DashMap<ContractInstanceId, FlagEntry>>,
    /// Last-claim timestamps for the deterministic identical-input probe
    /// (see [`IDENTITY_PROBE_COOLDOWN`]). Swept by [`Self::cleanup`];
    /// bounded in practice by the set of locally-stored contracts (a claim
    /// requires an upsert against stored state).
    identity_probe_claims: DashMap<ContractInstanceId, Instant>,
    // `RwLock<Option<Storage>>` (mirroring `HostingManager`) rather than a
    // `OnceLock` so the handle can be dropped on shutdown via `clear_storage`,
    // releasing its redb `Database` clone (issue #4401). None of the readers
    // are on a hot path: only `record`/`remove_from_storage`/`set_storage`
    // touch the handle, all off the broadcast/commit fast path.
    storage: parking_lot::RwLock<Option<Storage>>,
    time_source: Arc<dyn TimeSource + Send + Sync>,
}

impl BrokenInvariantsTracker {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self {
            flags: Arc::new(DashMap::new()),
            identity_probe_claims: DashMap::new(),
            storage: parking_lot::RwLock::new(None),
            time_source,
        }
    }

    /// Claim a slot for the deterministic identical-input idempotency probe
    /// for `id`. Returns true (and stamps the claim) if no probe ran within
    /// the last [`IDENTITY_PROBE_COOLDOWN`]; false while the cooldown is
    /// pending. The claim is consumed regardless of the probe's outcome, so
    /// an inconclusive probe (merge error, no state output) still waits out
    /// the cooldown before re-trying.
    pub fn try_claim_identity_probe(&self, id: &ContractInstanceId) -> bool {
        use dashmap::mapref::entry::Entry;
        let now = self.time_source.now();
        match self.identity_probe_claims.entry(*id) {
            Entry::Occupied(mut o) => {
                if now.saturating_duration_since(*o.get()) >= IDENTITY_PROBE_COOLDOWN {
                    *o.get_mut() = now;
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(v) => {
                v.insert(now);
                true
            }
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
            Some(entry) => now.saturating_duration_since(entry.recorded_at) < entry.ttl,
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
            if now.saturating_duration_since(entry.recorded_at) < entry.ttl {
                Some(entry.kind)
            } else {
                None
            }
        })
    }

    /// The current (possibly escalated) suppression TTL for `id`, if flagged.
    /// Test-only accessor for the escalation-policy tests.
    #[cfg(test)]
    pub fn current_ttl(&self, id: &ContractInstanceId) -> Option<Duration> {
        self.flags.get(id).map(|entry| entry.ttl)
    }

    /// Mark `id` as broken with `kind`, stamping the current time and applying
    /// the escalating-TTL policy (#4903 review B-tweak).
    ///
    /// - **First detection** (no live entry): baseline [`BROKEN_INVARIANT_TTL`].
    /// - **Re-detection within the re-flag window** (`[ttl, REFLAG_WINDOW_MULT ×
    ///   ttl)` since the last detection): ESCALATE — double the TTL, capped at
    ///   [`BROKEN_INVARIANT_TTL_CAP`]. This is a contract that went dark for its
    ///   TTL, re-entered the mesh, and was caught again — the #4902 oscillator.
    ///   Escalating collapses its re-entry duty cycle geometrically.
    /// - **Re-detection while still suppressed** (`< ttl`): refresh the stamp
    ///   at the SAME TTL (same detection cycle; not a new oscillation).
    /// - **Re-detection after a full quiet window** (`≥ REFLAG_WINDOW_MULT ×
    ///   ttl`): RESET to baseline — no longer oscillating.
    ///
    /// A contract flagged exactly ONCE (the #4295 false-positive shape) never
    /// re-detects, so it never escalates: its behavior is identical to the
    /// pre-escalation baseline.
    ///
    /// Persists best-effort to storage on first detection if wired; persistence
    /// errors are logged but never block the in-memory flag (we'd rather
    /// suppress the storm than crash on a database hiccup). The escalation level
    /// is in-memory only — the persisted row records just the flag kind, so a
    /// restart re-hydrates at the baseline TTL.
    pub fn record(&self, id: ContractInstanceId, kind: BrokenInvariant) {
        use dashmap::mapref::entry::Entry;
        let now = self.time_source.now();

        // Decide the new TTL under the shard guard, then release it before any
        // logging / persistence (never hold a DashMap guard across the storage
        // lock).
        enum Outcome {
            New,
            Escalated(Duration),
            Refreshed,
        }
        let outcome = match self.flags.entry(id) {
            Entry::Occupied(mut o) => {
                let elapsed = now.saturating_duration_since(o.get().recorded_at);
                let ttl = o.get().ttl;
                let reflag_window = ttl * REFLAG_WINDOW_MULT;
                let (new_ttl, outcome) = if elapsed >= reflag_window {
                    // Quiet for an escalated TTL plus a full window → reset.
                    (BROKEN_INVARIANT_TTL, Outcome::Refreshed)
                } else if elapsed >= ttl {
                    // Re-detected after expiry, within the re-flag window →
                    // escalate (double, capped).
                    let escalated = (ttl * 2).min(BROKEN_INVARIANT_TTL_CAP);
                    (escalated, Outcome::Escalated(escalated))
                } else {
                    // Still within the active suppression window → same cycle.
                    (ttl, Outcome::Refreshed)
                };
                *o.get_mut() = FlagEntry {
                    kind,
                    recorded_at: now,
                    ttl: new_ttl,
                };
                outcome
            }
            Entry::Vacant(v) => {
                v.insert(FlagEntry {
                    kind,
                    recorded_at: now,
                    ttl: BROKEN_INVARIANT_TTL,
                });
                Outcome::New
            }
        };

        match outcome {
            Outcome::New => {
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
                if let Some(storage) = self.storage.read().as_ref() {
                    if let Err(e) = storage.store_broken_invariant(&id, kind.to_byte()) {
                        tracing::warn!(
                            contract = %id,
                            error = %e,
                            "Failed to persist broken-invariant flag (in-memory flag still active)"
                        );
                    }
                }
            }
            Outcome::Escalated(new_ttl) => {
                tracing::debug!(
                    contract = %id,
                    escalated_ttl_secs = new_ttl.as_secs(),
                    event = "broken_invariant_ttl_escalated",
                    "Re-detected broken contract — escalating suppression TTL (#4902 egress oscillation)"
                );
            }
            Outcome::Refreshed => {}
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
        // Reclaim an entry only once it is past its RESET boundary
        // (`REFLAG_WINDOW_MULT × ttl` since the last detection), not merely
        // past its suppression TTL: between `ttl` and `REFLAG_WINDOW_MULT ×
        // ttl` the flag no longer suppresses (`is_broken` is false) but the
        // entry is retained as escalation memory, so a prompt re-detection can
        // ESCALATE rather than restart at baseline (#4903 review B-tweak).
        // Snapshot the reclaimable ids (read-only).
        let reclaimable = |entry: &FlagEntry| {
            now.saturating_duration_since(entry.recorded_at) >= entry.ttl * REFLAG_WINDOW_MULT
        };
        let candidates: Vec<ContractInstanceId> = self
            .flags
            .iter()
            .filter(|e| reclaimable(e.value()))
            .map(|e| *e.key())
            .collect();
        for id in candidates {
            // Atomically re-check under the shard guard and remove only if
            // STILL reclaimable. This closes a TOCTOU race: a concurrent
            // `record()` (e.g. a probe re-detecting the same contract) may have
            // refreshed `recorded_at`/escalated `ttl` between the snapshot above
            // and here — in which case `remove_if` keeps the fresh entry, and we
            // must NOT purge its persisted row (a restart needs it to re-hydrate
            // the active suppression). We only touch storage when we actually
            // removed a reclaimable entry.
            let removed = self.flags.remove_if(&id, |_, entry| reclaimable(entry));
            if removed.is_some() {
                self.remove_from_storage(&id);
            }
        }

        // Identity-probe claims are pure cooldown stamps — drop any that
        // have aged past the cooldown (they'd grant the next claim anyway),
        // keeping the map bounded.
        self.identity_probe_claims
            .retain(|_, t| now.saturating_duration_since(*t) < IDENTITY_PROBE_COOLDOWN);
    }

    /// Best-effort removal of a flag's persisted row. Shared by `clear` and
    /// the expiry sweep so a stale row does not re-flag the contract on the
    /// next restart.
    fn remove_from_storage(&self, id: &ContractInstanceId) {
        #[cfg(feature = "redb")]
        if let Some(storage) = self.storage.read().as_ref() {
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

    /// Wire persistent storage. Called once at startup; ignores re-init so
    /// callers cannot accidentally swap in a different database. Hydrates the
    /// in-memory map from previously-persisted entries on first wiring.
    ///
    /// Loaded flags are stamped with the current time, so a node restart
    /// gives each previously-flagged contract a fresh TTL window rather than
    /// inheriting an unknown (and un-persisted) original timestamp.
    pub fn set_storage(&self, storage: Storage) {
        {
            let mut slot = self.storage.write();
            if slot.is_some() {
                tracing::warn!("BrokenInvariantsTracker storage already set; ignoring re-init");
                return;
            }
            *slot = Some(storage.clone());
        }
        #[cfg(feature = "redb")]
        match storage.load_all_broken_invariants() {
            Ok(entries) => {
                let recorded_at = self.time_source.now();
                for (id, byte) in entries {
                    if let Some(kind) = BrokenInvariant::from_byte(byte) {
                        // Escalation state is in-memory only, so a reload starts
                        // each flag at the baseline TTL (#4903 review B-tweak):
                        // a restart resets any prior escalation, and the
                        // contract re-escalates from scratch if it keeps
                        // oscillating.
                        self.flags.insert(
                            id,
                            FlagEntry {
                                kind,
                                recorded_at,
                                ttl: BROKEN_INVARIANT_TTL,
                            },
                        );
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

    /// Drop the storage handle so its redb `Database` clone is released. Called
    /// on node shutdown to help free the on-disk file lock (issue #4401). The
    /// in-memory flags are left intact — only the persistence backing is
    /// dropped, since the node is going away.
    ///
    /// Runs on the shutdown thread (`run_node`'s teardown) while `record` /
    /// `remove_from_storage` run on the detached executor task. The write lock
    /// taken here may briefly wait on an in-flight redb write held by those
    /// readers, but cannot deadlock: the `RwLock` is non-reentrant and the
    /// writer/readers are distinct tasks.
    pub(crate) fn clear_storage(&self) {
        *self.storage.write() = None;
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

    /// `cleanup` physically reclaims an entry only once it is past its RESET
    /// boundary (`REFLAG_WINDOW_MULT × ttl`), not merely past its suppression
    /// TTL: between the two it is retained as escalation memory so a prompt
    /// re-detection can escalate rather than restart at baseline.
    #[test]
    fn cleanup_reclaims_entries_past_reset_boundary() {
        let (t, ts) = mk_tracker();
        let id = fake_id(7);
        t.record(id, BrokenInvariant::NonIdempotent);
        assert_eq!(t.flags.len(), 1);

        // Suppression has expired (past ttl) but we are still inside the
        // reset boundary (< 2 × ttl): the entry is retained as escalation
        // memory even though it no longer suppresses.
        ts.advance_time(BROKEN_INVARIANT_TTL + Duration::from_secs(1));
        assert!(
            !t.is_broken(&id),
            "past its ttl the flag no longer suppresses"
        );
        t.cleanup();
        assert_eq!(
            t.flags.len(),
            1,
            "entry retained as escalation memory until the reset boundary"
        );

        // Past the reset boundary (2 × ttl), cleanup reclaims it.
        ts.advance_time(BROKEN_INVARIANT_TTL);
        t.cleanup();
        assert_eq!(
            t.flags.len(),
            0,
            "entry past the reset boundary reclaimed by cleanup"
        );
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

    /// The identity-probe claim is granted at most once per cooldown per
    /// contract, independently per contract, and the claim map is swept
    /// once stamps age past the cooldown (bounded-collection hygiene).
    #[test]
    fn identity_probe_claim_respects_cooldown() {
        let (t, ts) = mk_tracker();
        let a = fake_id(9);
        let b = fake_id(10);

        assert!(t.try_claim_identity_probe(&a), "first claim granted");
        assert!(
            !t.try_claim_identity_probe(&a),
            "second claim within cooldown denied"
        );
        assert!(
            t.try_claim_identity_probe(&b),
            "unrelated contract unaffected"
        );

        ts.advance_time(IDENTITY_PROBE_COOLDOWN);
        assert!(
            t.try_claim_identity_probe(&a),
            "claim granted again after the cooldown"
        );

        // Sweep drops stale stamps so the map stays bounded.
        ts.advance_time(IDENTITY_PROBE_COOLDOWN);
        t.cleanup();
        assert!(t.identity_probe_claims.is_empty(), "stale claims swept");
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

    // ===== Escalating-TTL policy (#4903 review B-tweak, #4902 oscillation) =====

    /// (i) The FIRST detection uses exactly the baseline TTL and suppresses for
    /// exactly that long — unchanged from the pre-escalation behavior, so the
    /// #4295 false-positive conservatism is preserved.
    #[test]
    fn first_flag_uses_baseline_ttl() {
        let (t, ts) = mk_tracker();
        let id = fake_id(20);
        t.record(id, BrokenInvariant::NonIdempotent);
        assert_eq!(
            t.current_ttl(&id),
            Some(BROKEN_INVARIANT_TTL),
            "first flag = baseline TTL"
        );
        ts.advance_time(BROKEN_INVARIANT_TTL - Duration::from_secs(1));
        assert!(t.is_broken(&id), "suppressed right up to the baseline TTL");
        ts.advance_time(Duration::from_secs(2));
        assert!(
            !t.is_broken(&id),
            "first flag expires after exactly the baseline TTL"
        );
    }

    /// (ii) Each re-detection within the re-flag window doubles the TTL,
    /// geometrically, up to the cap — then stays pinned at the cap.
    #[test]
    fn re_detection_escalates_ttl_geometrically_to_cap() {
        let (t, ts) = mk_tracker();
        let id = fake_id(21);
        t.record(id, BrokenInvariant::NonIdempotent);

        let mut expected = BROKEN_INVARIANT_TTL;
        assert_eq!(t.current_ttl(&id), Some(expected));
        for cycle in 0..12 {
            let ttl = t.current_ttl(&id).unwrap();
            // Go dark for the whole TTL, then re-detect 1s into the re-flag
            // window (a fresh oscillation cycle).
            ts.advance_time(ttl + Duration::from_secs(1));
            assert!(!t.is_broken(&id), "flag expired before re-detection");
            t.record(id, BrokenInvariant::NonIdempotent);
            expected = (expected * 2).min(BROKEN_INVARIANT_TTL_CAP);
            assert_eq!(
                t.current_ttl(&id),
                Some(expected),
                "re-detection at cycle {cycle} must double the TTL (capped)"
            );
        }
        // The early doublings are exactly 300 → 600 → 1200 → 2400 …
        // and the sequence converged to the cap.
        assert_eq!(t.current_ttl(&id), Some(BROKEN_INVARIANT_TTL_CAP));
    }

    /// (iii) A re-detection AFTER a full quiet window (≥ 2× the current TTL
    /// with no re-detection) resets escalation to baseline — the contract is
    /// no longer oscillating.
    #[test]
    fn quiet_window_resets_escalation_to_baseline() {
        let (t, ts) = mk_tracker();
        let id = fake_id(22);
        t.record(id, BrokenInvariant::NonIdempotent);
        // Escalate once: 300 → 600.
        ts.advance_time(BROKEN_INVARIANT_TTL + Duration::from_secs(1));
        t.record(id, BrokenInvariant::NonIdempotent);
        assert_eq!(t.current_ttl(&id), Some(BROKEN_INVARIANT_TTL * 2));

        // Now go quiet for a full reset window (≥ 2× the current TTL) with no
        // re-detection, then re-detect: escalation resets to baseline.
        let ttl = t.current_ttl(&id).unwrap();
        ts.advance_time(ttl * 2 + Duration::from_secs(1));
        t.record(id, BrokenInvariant::NonIdempotent);
        assert_eq!(
            t.current_ttl(&id),
            Some(BROKEN_INVARIANT_TTL),
            "a re-detection after a full quiet window resets to baseline"
        );
    }

    /// (v) A contract flagged exactly ONCE (the #4295 false-positive shape) is
    /// never re-detected, so it never escalates: it self-heals after one
    /// baseline TTL and is reclaimed at the reset boundary.
    #[test]
    fn single_false_positive_never_escalates() {
        let (t, ts) = mk_tracker();
        let id = fake_id(23);
        t.record(id, BrokenInvariant::NonIdempotent);
        assert_eq!(t.current_ttl(&id), Some(BROKEN_INVARIANT_TTL));

        // Self-heals after one baseline TTL; the entry is retained (as
        // escalation memory) but the TTL is still baseline — no escalation.
        ts.advance_time(BROKEN_INVARIANT_TTL + Duration::from_secs(1));
        assert!(!t.is_broken(&id), "false positive self-heals after one TTL");
        assert_eq!(
            t.current_ttl(&id),
            Some(BROKEN_INVARIANT_TTL),
            "no re-detection means no escalation"
        );

        // Past the reset boundary, cleanup reclaims it entirely.
        ts.advance_time(BROKEN_INVARIANT_TTL);
        t.cleanup();
        assert!(
            t.current_ttl(&id).is_none(),
            "reclaimed after the reset boundary"
        );
    }

    /// (iv) A simulated oscillator (dark / re-enter / re-flag cycles) whose
    /// TTL escalates converges to a vanishing emission duty cycle. The model:
    /// each cycle the host is dark for its (escalating) TTL, then re-enters the
    /// mesh for a CONSTANT re-detection delay (bounded by the 1/32 probe
    /// cadence, independent of the dark-window length) before being re-flagged.
    /// The re-entry cascade's size is proportional to the drift accumulated
    /// during the dark window WHILE OTHER hosts are active — and in a fleet
    /// escalating in lockstep the active-other fraction is itself the shrinking
    /// duty cycle, so per-cycle emission ≈ dark_window × duty_cycle, which is
    /// bounded (≈ the constant re-detection delay). We assert the dark fraction
    /// → ~1, the TTL reaches the cap, and cumulative emission is far below the
    /// fixed-TTL baseline over the same horizon.
    #[test]
    fn oscillator_escalation_collapses_duty_cycle() {
        let (t, ts) = mk_tracker();
        let id = fake_id(24);
        let redetect_delay = Duration::from_secs(10);

        t.record(id, BrokenInvariant::NonIdempotent);

        let mut total_dark = Duration::ZERO;
        let mut total_active = Duration::ZERO;
        let mut total_emission = 0.0_f64;

        for _ in 0..20 {
            let ttl = t.current_ttl(&id).unwrap();
            // Dark for the whole TTL (suppressed).
            assert!(
                t.is_broken(&id),
                "suppressed at the start of the dark window"
            );
            total_dark += ttl;
            ts.advance_time(ttl);
            assert!(
                !t.is_broken(&id),
                "flag expired at the end of the dark window"
            );

            // Re-entry: active for the constant re-detection delay, accruing a
            // cascade ∝ dark_window × (fleet active fraction ≈ this cycle's
            // duty cycle).
            let cycle_len = ttl + redetect_delay;
            let active_fraction = redetect_delay.as_secs_f64() / cycle_len.as_secs_f64();
            total_emission += ttl.as_secs_f64() * active_fraction;
            total_active += redetect_delay;
            ts.advance_time(redetect_delay);
            t.record(id, BrokenInvariant::NonIdempotent);
        }

        // Dark fraction → ~1 (duty cycle → ~0).
        let dark_fraction =
            total_dark.as_secs_f64() / (total_dark.as_secs_f64() + total_active.as_secs_f64());
        assert!(
            dark_fraction > 0.98,
            "dark fraction must converge toward 1 (duty cycle collapses), got {dark_fraction}"
        );

        // Escalation is monotonic to the ceiling.
        assert_eq!(t.current_ttl(&id), Some(BROKEN_INVARIANT_TTL_CAP));

        // Cumulative re-entry emission is bounded: far below the fixed-TTL
        // baseline (which re-cascades every baseline TTL) over the SAME horizon.
        let horizon = total_dark + total_active;
        let baseline_cycle = BROKEN_INVARIANT_TTL + redetect_delay;
        let baseline_cycles = horizon.as_secs_f64() / baseline_cycle.as_secs_f64();
        let baseline_active_fraction = redetect_delay.as_secs_f64() / baseline_cycle.as_secs_f64();
        let baseline_emission =
            baseline_cycles * BROKEN_INVARIANT_TTL.as_secs_f64() * baseline_active_fraction;
        assert!(
            total_emission < baseline_emission / 5.0,
            "escalation must slash cumulative re-entry cascade emission vs the \
             fixed-TTL baseline (escalated {total_emission:.1}, baseline {baseline_emission:.1})"
        );
    }
}
