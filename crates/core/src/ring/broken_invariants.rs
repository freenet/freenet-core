//! Per-contract tracking of detected CRDT-invariant violations.
//!
//! A non-idempotent `update_state` (i.e. `update_state(update_state(S, U), U) != update_state(S, U)`)
//! breaks the convergence guarantee Freenet contracts rely on and produces a
//! self-perpetuating broadcast storm: every peer's merge generates a new
//! byte-different state, which propagates, which every peer re-merges.
//!
//! Two detectors feed this tracker (both in `Executor`, see
//! `executor_impl.rs`):
//!
//! 1. The **sampled re-apply probe** (`maybe_probe_idempotency`): with
//!    probability `IDEMPOTENCY_PROBE_PROBABILITY` (1/32) a merge of a
//!    pure-`State` batch is re-applied and the outputs compared.
//! 2. The **deterministic identical-input probe**
//!    (`probe_identical_input_idempotency`): when an incoming full-`State`
//!    payload is byte-identical to the stored state, `update_state(S, State(S))`
//!    must be a no-op by CvRDT lattice-join semantics — re-running the merge
//!    once per [`IDENTITY_PROBE_COOLDOWN`] and seeing a byte-MULTISET change
//!    is deterministic proof of non-idempotency (no sampling).
//!
//! A positive detection calls [`BrokenInvariantsTracker::record`] (via
//! `Ring::record_broken_invariant`), which:
//!
//! 1. Inserts the flag into an in-memory `DashMap` keyed on the contract
//!    instance id, so subsequent same-process reads see it immediately.
//! 2. Persists the flag (and the escalation count below) to the executor's
//!    ReDb storage so the detection survives node restarts.
//! 3. Counts the *flag episode* toward the escalation ledger (below).
//!
//! Reads (via [`BrokenInvariantsTracker::is_broken`]) gate the broadcast-
//! emission path, the executor's commit path, and the full-state egress
//! paths (`ResyncResponse` in node.rs, `SyncStateToPeer` in
//! p2p_protoc/broadcast.rs) so a flagged contract's state changes never
//! leave the node.
//!
//! ## Why the flag is TTL-bounded, not permanent
//!
//! An earlier revision made the flag permanent on the theory that a "fixed"
//! contract has a different code hash → different `ContractKey`, so an
//! instance id flagged once is broken forever. That reasoning is sound for a
//! *true* violation, but the probe is a heuristic that can false-positive:
//! it ultimately rests on a byte-level comparison, and a correct contract
//! whose serialization is non-canonical (HashMap/HashSet iteration order,
//! float formatting, embedded timestamps) can be flagged even though its
//! merge is logically idempotent. A permanent, silent, cross-restart-
//! persistent flag turns a single false positive into a contract that stops
//! propagating *forever* with no recovery — exactly the failure mode behind
//! issue #4295 (`test_ping_multi_node` froze because the ping contract's
//! `HashMap`-backed state byte-fluttered on re-merge).
//!
//! So the flag expires after [`BROKEN_INVARIANT_TTL`]: a false positive
//! self-heals within one TTL window, while a genuine violation is
//! re-detected after expiry (deterministically by the identical-input
//! probe, probabilistically by the sampled probe).
//!
//! ## Escalation to a durable ban (the quarantine)
//!
//! The TTL alone only DAMPENS a genuinely non-idempotent contract: it is
//! per-node, expires in minutes, and across ~58 co-hosts somebody is always
//! unflagged, so the broadcast echo survives (the production storm root
//! cause this module now closes). The escalation ledger makes repeated
//! detection durable:
//!
//! - Each *flag episode* — a `record` that transitions the flag from
//!   inactive to active (a fresh detection, at most one per TTL per
//!   contract) — increments a per-contract counter. Re-records while the
//!   flag is already active refresh the TTL but do NOT count, so a hot
//!   caller can never inflate the ledger.
//! - [`FLAGS_PER_ESCALATION`] episodes within [`ESCALATION_FLAG_WINDOW`]
//!   complete one **escalation** (and reset the episode window).
//! - At [`ESCALATIONS_FOR_BAN`] escalations the contract is fed to the
//!   [`ContractBanList`] with [`BanReason::NonIdempotent`] for
//!   [`NON_IDEMPOTENT_BAN_TTL`]; the existing wire-dispatch gates then drop
//!   the contract's PUT/GET/UPDATE/SUBSCRIBE entirely.
//!
//! The thresholds are deliberately conservative: with the 5-minute flag TTL
//! a contract must be re-detected ~every TTL for over an hour before the
//! first ban lands, so a one-off (or even recurring-but-sparse) false
//! positive never bans. Escalation counts persist across restarts (see
//! [`BrokenInvariantsTracker::set_storage`]) and past flag expiry, bounded
//! by [`ESCALATION_RETENTION`] — an absolute age cap so the ledger cannot
//! become a permanent GC blind spot (`.claude/rules/ring.md`, "Cleanup
//! Exemptions Must Be Time-Bounded").
//!
//! Expired entries are swept by [`BrokenInvariantsTracker::cleanup`],
//! hooked into the same reaper tick as the UPDATE rate limiter.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::contract::storages::Storage;
use crate::ring::contract_ban_list::{BanReason, ContractBanList};
use crate::util::time_source::TimeSource;

/// How long a broken-invariant flag suppresses a contract before it expires
/// and the contract is given a fresh chance (re-detected by the next probe
/// if genuinely still broken).
///
/// Chosen to be long enough that a genuinely non-convergent contract leaks at
/// most ≈one merge per window (negligible amplification) yet short enough that
/// a false positive recovers promptly rather than bricking the contract.
pub(crate) const BROKEN_INVARIANT_TTL: Duration = Duration::from_secs(300);

/// Sliding (tumbling) window within which flag episodes are counted toward
/// an escalation. Must comfortably exceed
/// `FLAGS_PER_ESCALATION × BROKEN_INVARIANT_TTL` (25 min) so a contract
/// that is re-detected every TTL actually accumulates the episodes.
pub(crate) const ESCALATION_FLAG_WINDOW: Duration = Duration::from_secs(3600);

/// Flag episodes within [`ESCALATION_FLAG_WINDOW`] that complete one
/// escalation. Episodes are at most one per [`BROKEN_INVARIANT_TTL`], so
/// one escalation represents ≥ ~20 minutes of sustained re-detection.
pub(crate) const FLAGS_PER_ESCALATION: u32 = 5;

/// Escalations at which the contract is fed to the [`ContractBanList`].
/// Combined with the above, the first ban requires ~15 distinct detection
/// episodes spread over more than an hour — a one-off false positive (or a
/// handful of them) can never ban.
pub(crate) const ESCALATIONS_FOR_BAN: u32 = 3;

/// How long an escalation-triggered ban lasts. Deliberately much longer
/// than the flag TTL: by the time a contract reaches the ban threshold it
/// has proven non-idempotent ~15 separate times, so the residual
/// false-positive risk is negligible and the quarantine should outlast the
/// storm. The ban expires on its own (the ban list is TTL-bounded), and the
/// persisted escalation ledger re-arms it quickly if the echo resumes.
pub(crate) const NON_IDEMPOTENT_BAN_TTL: Duration = Duration::from_secs(24 * 3600);

/// Absolute age bound on the escalation ledger: an entry whose most recent
/// flag is older than this is dropped entirely (memory + storage), even if
/// it had accumulated escalations. Keeps the "survives flag expiry"
/// exemption time-bounded per the ring GC rules. Longer than
/// [`NON_IDEMPOTENT_BAN_TTL`] so the ledger survives a full ban (during
/// which no flags occur, because dispatch drops the contract's traffic) and
/// an echo that resumes right after ban expiry re-escalates against the
/// remembered count instead of starting from zero.
pub(crate) const ESCALATION_RETENTION: Duration = Duration::from_secs(48 * 3600);

/// Per-contract cooldown for the deterministic identical-input idempotency
/// probe (`Executor::probe_identical_input_idempotency`). The #4151
/// fast path exists because identical re-pushes are the dominant dedup-miss
/// case; re-running the merge on EVERY one would reintroduce the WASM cost
/// that fix removed. One probe per contract per cooldown keeps detection
/// deterministic (a violating contract is caught on the first identical
/// apply after each cooldown — no 1/32 sampling) at a bounded cost.
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

/// An in-memory flag together with the instant it was (re-)recorded (so the
/// active flag can expire after [`BROKEN_INVARIANT_TTL`]) and the durable
/// escalation ledger that outlives individual flag activations.
#[derive(Debug, Clone, Copy)]
struct FlagEntry {
    kind: BrokenInvariant,
    /// Most recent flag episode. The flag is ACTIVE while
    /// `now - recorded_at < BROKEN_INVARIANT_TTL`.
    recorded_at: Instant,
    /// Start of the current episode-counting window.
    window_started_at: Instant,
    /// Flag episodes counted in the current window. Reset when the window
    /// expires or an escalation completes.
    flags_in_window: u32,
    /// Completed escalations. Persisted; survives flag expiry and restarts
    /// (bounded by [`ESCALATION_RETENTION`]).
    escalations: u32,
}

/// What a [`BrokenInvariantsTracker::record`] call did — consumed by
/// [`BrokenInvariantsTracker::record_and_escalate`] to decide whether to
/// feed the ban list, and by tests.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordOutcome {
    /// The flag transitioned inactive → active (a fresh detection episode).
    /// False for a re-record while the flag was still active (TTL refresh
    /// only — episodes are never inflated by hot callers).
    pub newly_flagged: bool,
    /// This record completed an escalation ([`FLAGS_PER_ESCALATION`]
    /// episodes accumulated within the window).
    pub escalated: bool,
    /// Total escalations after this record.
    pub escalations: u32,
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

    /// Mark `id` as broken with `kind`, stamping the current time.
    ///
    /// A record while the flag is already ACTIVE refreshes its expiry only
    /// (so a contract a probe keeps re-detecting stays suppressed) — it does
    /// NOT count toward the escalation ledger, so a hot caller can never
    /// inflate it. A record that transitions the flag inactive → active is a
    /// *flag episode*: it increments the windowed episode count and may
    /// complete an escalation (see module docs).
    ///
    /// Persists best-effort to storage if wired; persistence errors are
    /// logged but never block the in-memory flag from taking effect (we'd
    /// rather suppress the storm than crash on a database hiccup).
    pub fn record(&self, id: ContractInstanceId, kind: BrokenInvariant) -> RecordOutcome {
        use dashmap::mapref::entry::Entry;
        let now = self.time_source.now();
        let outcome = match self.flags.entry(id) {
            Entry::Vacant(v) => {
                let mut entry = FlagEntry {
                    kind,
                    recorded_at: now,
                    window_started_at: now,
                    flags_in_window: 1,
                    escalations: 0,
                };
                let escalated = Self::maybe_complete_escalation(&mut entry, now);
                let escalations = entry.escalations;
                v.insert(entry);
                RecordOutcome {
                    newly_flagged: true,
                    escalated,
                    escalations,
                }
            }
            Entry::Occupied(mut o) => {
                let entry = o.get_mut();
                let was_active =
                    now.saturating_duration_since(entry.recorded_at) < BROKEN_INVARIANT_TTL;
                entry.kind = kind;
                entry.recorded_at = now;
                if was_active {
                    // TTL refresh only — not a new detection episode.
                    RecordOutcome {
                        newly_flagged: false,
                        escalated: false,
                        escalations: entry.escalations,
                    }
                } else {
                    // Fresh episode. Tumbling window: if the previous window
                    // has lapsed, restart the count.
                    if now.saturating_duration_since(entry.window_started_at)
                        >= ESCALATION_FLAG_WINDOW
                    {
                        entry.window_started_at = now;
                        entry.flags_in_window = 0;
                    }
                    entry.flags_in_window += 1;
                    let escalated = Self::maybe_complete_escalation(entry, now);
                    RecordOutcome {
                        newly_flagged: true,
                        escalated,
                        escalations: entry.escalations,
                    }
                }
            }
        };

        if outcome.newly_flagged {
            tracing::warn!(
                contract = %id,
                invariant = ?kind,
                escalations = outcome.escalations,
                event = "broken_invariant_detected",
                "Marking contract as broken — gating outbound broadcast, merge \
                 propagation, and full-state egress (resync/heal)"
            );
            // Persistence is currently only wired for the redb backend;
            // sqlite-only builds keep the in-memory flag but skip the
            // on-disk hydration. This is the same trade-off
            // `HostingManager` makes — see #4279 deferred follow-up to
            // add sqlite parity. At most one write per flag episode
            // (bounded by BROKEN_INVARIANT_TTL per contract).
            #[cfg(feature = "redb")]
            if let Some(storage) = self.storage.read().as_ref() {
                if let Err(e) =
                    storage.store_broken_invariant(&id, kind.to_byte(), outcome.escalations)
                {
                    tracing::warn!(
                        contract = %id,
                        error = %e,
                        "Failed to persist broken-invariant flag (in-memory flag still active)"
                    );
                }
            }
        }
        outcome
    }

    /// If the entry's windowed episode count reached
    /// [`FLAGS_PER_ESCALATION`], complete an escalation: bump the durable
    /// escalation count and restart the episode window. Returns whether an
    /// escalation completed.
    fn maybe_complete_escalation(entry: &mut FlagEntry, now: Instant) -> bool {
        if entry.flags_in_window >= FLAGS_PER_ESCALATION {
            entry.escalations = entry.escalations.saturating_add(1);
            entry.flags_in_window = 0;
            entry.window_started_at = now;
            true
        } else {
            false
        }
    }

    /// [`Self::record`] plus the durable-quarantine escalation: when this
    /// record completes an escalation and the total reaches
    /// [`ESCALATIONS_FOR_BAN`], feed the contract to the ban list with
    /// [`BanReason::NonIdempotent`] for [`NON_IDEMPOTENT_BAN_TTL`]. Each
    /// FURTHER escalation past the threshold re-bans (extends the expiry),
    /// so a violator that outlives one ban window is re-quarantined as soon
    /// as it re-accumulates a window of episodes.
    ///
    /// This is the single production entry point (`Ring::record_broken_invariant`
    /// delegates here); `record` stays separately callable for tests.
    pub fn record_and_escalate(
        &self,
        ban_list: &ContractBanList,
        id: ContractInstanceId,
        kind: BrokenInvariant,
    ) -> RecordOutcome {
        let outcome = self.record(id, kind);
        if outcome.escalated {
            tracing::warn!(
                contract = %id,
                escalations = outcome.escalations,
                ban_threshold = ESCALATIONS_FOR_BAN,
                event = "broken_invariant_escalated",
                "Repeated non-idempotency detections escalated"
            );
            if outcome.escalations >= ESCALATIONS_FOR_BAN {
                let expires_at = self.time_source.now() + NON_IDEMPOTENT_BAN_TTL;
                let ban_outcome = ban_list.ban(id, expires_at, BanReason::NonIdempotent);
                tracing::warn!(
                    contract = %id,
                    escalations = outcome.escalations,
                    ban_ttl_secs = NON_IDEMPOTENT_BAN_TTL.as_secs(),
                    ?ban_outcome,
                    event = "non_idempotent_contract_banned",
                    "Contract durably quarantined: repeated CRDT-idempotency \
                     violations escalated to the ban list — inbound \
                     PUT/GET/UPDATE/SUBSCRIBE will be dropped at dispatch"
                );
            }
        }
        outcome
    }

    /// Re-arm bans for contracts whose (persisted, hydrated) escalation
    /// count already crossed [`ESCALATIONS_FOR_BAN`]. Called by
    /// `Ring::set_broken_invariants_storage` right after hydration: the ban
    /// list itself is in-memory only, so without this a restart would lift
    /// the quarantine until the contract re-escalated from the wire.
    pub fn reban_offenders(&self, ban_list: &ContractBanList) {
        let now = self.time_source.now();
        let mut rebanned = 0usize;
        for entry in self.flags.iter() {
            if entry.value().escalations >= ESCALATIONS_FOR_BAN {
                ban_list.ban(
                    *entry.key(),
                    now + NON_IDEMPOTENT_BAN_TTL,
                    BanReason::NonIdempotent,
                );
                rebanned += 1;
            }
        }
        if rebanned > 0 {
            tracing::warn!(
                count = rebanned,
                event = "non_idempotent_bans_rearmed",
                "Re-armed durable quarantine bans from persisted escalation counts"
            );
        }
    }

    /// Remove the broken-invariant flag AND escalation ledger for `id`. Use
    /// with extreme care: this is the operator escape hatch for the rare
    /// case where the probe was a false positive (most plausibly a probe
    /// trap that shouldn't have been observed at all — see
    /// `Executor::maybe_probe_idempotency`). Re-enables outbound
    /// broadcast and commit for the contract. Does not unflag remotely and
    /// does NOT lift an already-applied ban (the ban list has its own TTL
    /// and operator surface).
    ///
    /// Not currently exposed to a CLI / WS API by this PR — added so the
    /// debug-CLI follow-up has a stable surface to call into. Returns
    /// the previous flag if any.
    #[allow(dead_code)] // wired in follow-up PR
    pub fn clear(&self, id: &ContractInstanceId) -> Option<BrokenInvariant> {
        let previous = self.flags.remove(id).map(|(_, v)| v.kind);
        self.identity_probe_claims.remove(id);
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

    /// True when the entry carries no live state at `now`: flag expired,
    /// episode window lapsed (or empty), and escalation ledger past
    /// retention (or empty). Only such entries are reclaimed — an entry
    /// with a lapsed flag but a live episode window or escalation ledger
    /// must SURVIVE so re-detections keep accumulating toward the ban
    /// (removing it at flag expiry would reset the count every TTL and the
    /// quarantine could never trigger). Every survival condition is
    /// time-bounded (window ≤ [`ESCALATION_FLAG_WINDOW`], ledger ≤
    /// [`ESCALATION_RETENTION`]) per the ring GC rules.
    fn reclaimable(entry: &FlagEntry, now: Instant) -> bool {
        let flag_expired = now.saturating_duration_since(entry.recorded_at) >= BROKEN_INVARIANT_TTL;
        let window_lapsed = entry.flags_in_window == 0
            || now.saturating_duration_since(entry.window_started_at) >= ESCALATION_FLAG_WINDOW;
        let ledger_lapsed = entry.escalations == 0
            || now.saturating_duration_since(entry.recorded_at) >= ESCALATION_RETENTION;
        flag_expired && window_lapsed && ledger_lapsed
    }

    /// Sweep reclaimable entries from the in-memory map (and best-effort
    /// from storage), plus stale identity-probe claims. Hooked into the
    /// Ring reaper tick alongside the UPDATE rate limiter's cleanup so
    /// expired suppressions are reclaimed promptly even for contracts that
    /// are no longer being read on the hot path.
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        // Snapshot the currently-reclaimable ids (read-only).
        let candidates: Vec<ContractInstanceId> = self
            .flags
            .iter()
            .filter(|e| Self::reclaimable(e.value(), now))
            .map(|e| *e.key())
            .collect();
        for id in candidates {
            // Atomically re-check under the shard guard and remove only if
            // STILL reclaimable. This closes a TOCTOU race: a concurrent
            // `record()` (e.g. a probe re-detecting the same contract) may
            // have refreshed the entry between the snapshot above and here —
            // in which case `remove_if` keeps the fresh entry, and we must
            // NOT purge its persisted row (a restart needs it to re-hydrate
            // the active suppression / escalation ledger). We only touch
            // storage when we actually removed an entry.
            let removed = self
                .flags
                .remove_if(&id, |_, entry| Self::reclaimable(entry, now));
            if removed.is_some() {
                self.remove_from_storage(&id);
            }
        }

        // Identity-probe claims are pure cooldown stamps — drop any that
        // have aged past the cooldown (they'd grant the next claim anyway).
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
    /// inheriting an unknown (and un-persisted) original timestamp. The
    /// persisted escalation count is restored as-is (the durable half of the
    /// quarantine); the episode window restarts empty.
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
                for (id, byte, escalations) in entries {
                    if let Some(kind) = BrokenInvariant::from_byte(byte) {
                        self.flags.insert(
                            id,
                            FlagEntry {
                                kind,
                                recorded_at,
                                window_started_at: recorded_at,
                                flags_in_window: 0,
                                escalations,
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

    fn mk_ban_list(ts: &SharedMockTimeSource) -> ContractBanList {
        ContractBanList::new(Arc::new(ts.clone()))
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

    /// `cleanup` physically reclaims entries once ALL their state has
    /// lapsed. An expired flag whose episode window is still live must
    /// SURVIVE the sweep (otherwise the escalation ledger would reset
    /// every TTL and the durable quarantine could never trigger) — but it
    /// must report not-broken, and once the episode window lapses too the
    /// entry is reclaimed. (Semantics extended by the durable-quarantine
    /// escalation ledger; the pre-escalation behavior removed entries at
    /// flag expiry.)
    #[test]
    fn cleanup_reclaims_entries_once_window_and_ledger_lapse() {
        let (t, ts) = mk_tracker();
        let id = fake_id(7);
        t.record(id, BrokenInvariant::NonIdempotent);
        assert_eq!(t.flags.len(), 1);

        // Before flag expiry, cleanup keeps it.
        ts.advance_time(BROKEN_INVARIANT_TTL / 2);
        t.cleanup();
        assert_eq!(t.flags.len(), 1, "non-expired entry retained by cleanup");

        // After flag expiry but within the episode window: the flag is no
        // longer active, but the entry survives so re-detections keep
        // counting toward an escalation.
        ts.advance_time(BROKEN_INVARIANT_TTL);
        t.cleanup();
        assert_eq!(
            t.flags.len(),
            1,
            "entry with a live episode window survives flag expiry"
        );
        assert!(!t.is_broken(&id), "expired flag reports not-broken");

        // Once the episode window lapses too (no escalations were ever
        // completed), the entry is reclaimed.
        ts.advance_time(ESCALATION_FLAG_WINDOW);
        t.cleanup();
        assert_eq!(
            t.flags.len(),
            0,
            "entry reclaimed once flag, window, and ledger have all lapsed"
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

    // ================= escalation / durable-quarantine tests =================

    /// Drive one counted flag episode: record, then advance past the flag
    /// TTL so the next record is a fresh episode.
    fn one_episode(
        t: &BrokenInvariantsTracker,
        ban_list: &ContractBanList,
        ts: &SharedMockTimeSource,
        id: ContractInstanceId,
    ) -> RecordOutcome {
        let outcome = t.record_and_escalate(ban_list, id, BrokenInvariant::NonIdempotent);
        ts.advance_time(BROKEN_INVARIANT_TTL);
        outcome
    }

    /// A record while the flag is still ACTIVE refreshes the TTL but does
    /// NOT count as a new episode — a hot caller (or a detector bug that
    /// skipped the is_broken precheck) can never inflate the escalation
    /// ledger.
    #[test]
    fn active_flag_re_records_do_not_count_episodes() {
        let (t, _ts) = mk_tracker();
        let id = fake_id(10);

        let first = t.record(id, BrokenInvariant::NonIdempotent);
        assert!(first.newly_flagged);

        for _ in 0..20 {
            let outcome = t.record(id, BrokenInvariant::NonIdempotent);
            assert!(
                !outcome.newly_flagged,
                "active-flag re-record is not an episode"
            );
            assert!(!outcome.escalated);
            assert_eq!(outcome.escalations, 0);
        }
        let entry = t.flags.get(&id).expect("entry exists");
        assert_eq!(
            entry.flags_in_window, 1,
            "episode count must not move on active-flag re-records"
        );
    }

    /// FLAGS_PER_ESCALATION distinct episodes within the window complete
    /// exactly one escalation; the episode before the threshold does not.
    #[test]
    fn episodes_within_window_complete_one_escalation() {
        let (t, ts) = mk_tracker();
        let ban_list = mk_ban_list(&ts);
        let id = fake_id(11);

        for i in 1..FLAGS_PER_ESCALATION {
            let outcome = one_episode(&t, &ban_list, &ts, id);
            assert!(outcome.newly_flagged, "episode {i} counts");
            assert!(!outcome.escalated, "episode {i} is below the threshold");
            assert_eq!(outcome.escalations, 0);
        }
        let outcome = one_episode(&t, &ban_list, &ts, id);
        assert!(
            outcome.escalated,
            "threshold episode completes an escalation"
        );
        assert_eq!(outcome.escalations, 1);
        assert!(
            !ban_list.is_banned(&id),
            "one escalation is below the ban threshold — no ban"
        );
    }

    /// Episodes separated by more than the window do NOT accumulate: the
    /// tumbling window restarts, so sparse false positives never escalate.
    #[test]
    fn window_expiry_resets_episode_count() {
        let (t, ts) = mk_tracker();
        let ban_list = mk_ban_list(&ts);
        let id = fake_id(12);

        // Four episodes (one short of the threshold)…
        for _ in 0..(FLAGS_PER_ESCALATION - 1) {
            one_episode(&t, &ban_list, &ts, id);
        }
        // …then a gap longer than the window.
        ts.advance_time(ESCALATION_FLAG_WINDOW);

        // The next episode starts a FRESH window: no escalation.
        let outcome = one_episode(&t, &ban_list, &ts, id);
        assert!(
            !outcome.escalated,
            "an episode after a window gap must start a fresh count, not escalate"
        );
        assert_eq!(outcome.escalations, 0);
    }

    /// The full quarantine path: sustained re-flagging escalates to a
    /// durable ban after exactly ESCALATIONS_FOR_BAN × FLAGS_PER_ESCALATION
    /// counted episodes — and not one episode sooner. This is the
    /// regression pin for the production storm: detection existed but fed
    /// nothing durable, so ~58 co-hosts kept the echo alive.
    #[test]
    fn sustained_reflagging_escalates_to_durable_ban() {
        let (t, ts) = mk_tracker();
        let ban_list = mk_ban_list(&ts);
        let id = fake_id(13);

        let total = ESCALATIONS_FOR_BAN * FLAGS_PER_ESCALATION;
        for i in 1..total {
            one_episode(&t, &ban_list, &ts, id);
            assert!(
                !ban_list.is_banned(&id),
                "episode {i}/{total}: must NOT be banned before the final threshold episode"
            );
        }
        let outcome = one_episode(&t, &ban_list, &ts, id);
        assert!(outcome.escalated);
        assert_eq!(outcome.escalations, ESCALATIONS_FOR_BAN);
        assert!(
            ban_list.is_banned(&id),
            "the {total}th counted episode must land the durable ban"
        );
    }

    /// A one-off false positive never bans: a single flag episode leaves no
    /// escalations, is suppressed only for one TTL, and the whole entry
    /// evaporates once the episode window lapses.
    #[test]
    fn one_off_false_positive_never_bans_and_evaporates() {
        let (t, ts) = mk_tracker();
        let ban_list = mk_ban_list(&ts);
        let id = fake_id(14);

        let outcome = t.record_and_escalate(&ban_list, id, BrokenInvariant::NonIdempotent);
        assert!(outcome.newly_flagged);
        assert!(!outcome.escalated);
        assert!(!ban_list.is_banned(&id), "a single flag must never ban");

        // Suppression self-heals after the TTL…
        ts.advance_time(BROKEN_INVARIANT_TTL + Duration::from_secs(1));
        assert!(!t.is_broken(&id));

        // …and the entry (with its episode count) evaporates once the
        // window lapses, leaving no trace to accumulate against.
        ts.advance_time(ESCALATION_FLAG_WINDOW);
        t.cleanup();
        assert_eq!(t.flags.len(), 0, "false-positive entry fully reclaimed");
        assert!(!ban_list.is_banned(&id));
    }

    /// The escalation ledger must survive flag expiry AND the cleanup
    /// sweep (that survival is what makes the quarantine durable), but it
    /// is time-bounded: once the most recent flag is older than
    /// ESCALATION_RETENTION the entry is reclaimed entirely.
    #[test]
    fn escalation_ledger_survives_sweeps_until_retention() {
        let (t, ts) = mk_tracker();
        let ban_list = mk_ban_list(&ts);
        let id = fake_id(15);

        // Complete one escalation.
        for _ in 0..FLAGS_PER_ESCALATION {
            one_episode(&t, &ban_list, &ts, id);
        }
        assert_eq!(t.flags.get(&id).expect("entry").escalations, 1);

        // Flag long expired, window empty (reset by the escalation) — but
        // the ledger keeps the entry alive through cleanup.
        ts.advance_time(Duration::from_secs(3600));
        t.cleanup();
        assert_eq!(
            t.flags.get(&id).expect("ledger entry survives").escalations,
            1,
            "escalation ledger must survive flag expiry and cleanup"
        );
        assert!(!t.is_broken(&id));

        // Past the absolute retention bound the entry is reclaimed.
        ts.advance_time(ESCALATION_RETENTION);
        t.cleanup();
        assert_eq!(
            t.flags.len(),
            0,
            "escalation ledger must be reclaimed after ESCALATION_RETENTION"
        );
    }

    /// Post-threshold escalations re-ban (extend the quarantine) rather
    /// than being ignored, so a violator that outlives one ban window is
    /// re-quarantined as soon as it re-accumulates a window of episodes.
    #[test]
    fn escalations_past_threshold_reban() {
        let (t, ts) = mk_tracker();
        let ban_list = mk_ban_list(&ts);
        let id = fake_id(16);

        for _ in 0..(ESCALATIONS_FOR_BAN * FLAGS_PER_ESCALATION) {
            one_episode(&t, &ban_list, &ts, id);
        }
        assert!(ban_list.is_banned(&id));

        // Simulate the ban lapsing (e.g. TTL expiry) while the ledger
        // remembers the offender.
        ban_list.unban(&id);
        assert!(!ban_list.is_banned(&id));

        // One more full window of episodes → 4th escalation → re-banned.
        for _ in 0..FLAGS_PER_ESCALATION {
            one_episode(&t, &ban_list, &ts, id);
        }
        assert!(
            ban_list.is_banned(&id),
            "an escalation past the threshold must re-arm the ban"
        );
    }

    /// `reban_offenders` re-arms the (in-memory-only) ban list from the
    /// tracker's ledger — the restart path: hydration restores escalation
    /// counts, then Ring calls this so a quarantined contract does not get
    /// a free window after a process bounce.
    #[test]
    fn reban_offenders_rearms_threshold_contracts_only() {
        let (t, ts) = mk_tracker();
        let ban_list = mk_ban_list(&ts);
        let offender = fake_id(17);
        let minor = fake_id(18);

        // Offender crosses the ban threshold; minor accrues one escalation.
        for _ in 0..(ESCALATIONS_FOR_BAN * FLAGS_PER_ESCALATION) {
            one_episode(&t, &ban_list, &ts, offender);
        }
        for _ in 0..FLAGS_PER_ESCALATION {
            one_episode(&t, &ban_list, &ts, minor);
        }

        // Fresh ban list = the post-restart state (bans are in-memory only).
        let fresh = mk_ban_list(&ts);
        assert!(!fresh.is_banned(&offender));

        t.reban_offenders(&fresh);
        assert!(
            fresh.is_banned(&offender),
            "persisted-threshold offender must be re-banned after restart"
        );
        assert!(
            !fresh.is_banned(&minor),
            "sub-threshold contract must NOT be banned by the re-arm sweep"
        );
    }

    /// The identity-probe claim is granted at most once per cooldown per
    /// contract, and independently per contract.
    #[test]
    fn identity_probe_claim_respects_cooldown() {
        let (t, ts) = mk_tracker();
        let a = fake_id(19);
        let b = fake_id(20);

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
}
