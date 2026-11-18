//! Deferred re-broadcast store for fresh-contract PUTs that found no targets.
//!
//! ## Why this exists (issue #4359)
//!
//! When a brand-new contract id is PUT, the node applies the state locally and
//! emits a `BroadcastStateChange` to fan it out to interested peers. For a
//! never-before-seen id, no peer has subscribed/announced interest yet, so the
//! initial broadcast resolves **zero** targets. The fan-out handler retries a
//! few times over ~6 s (`MAX_BROADCAST_RETRIES` × `BROADCAST_RETRY_BASE_DELAY`)
//! and then **gives up permanently**. But a fresh id's interest/subscription
//! resolves on a *much* longer timescale (the second-PUT-of-the-same-id case in
//! #4359 propagated only ~31 s after the PUT). The give-up window is shorter
//! than the interest-resolve latency, so the state silently lands
//! locally-hosted only: the originating node's GET looks healthy while every
//! other node GETs `NotFound`.
//!
//! Rather than lengthen the give-up timeout (a band-aid that still races), this
//! store remembers that a contract exhausted its retry budget with no targets
//! and still owes a fan-out. It records only the contract id and the stash
//! time, never the state bytes. When the *first viable broadcast target* for
//! that contract appears later, the entry is drained, the CURRENT local state
//! is read, and it is re-emitted as a single
//! `BroadcastStateChange { is_reemit: true }` — which now finds the
//! freshly-appeared target and propagates. If the contract is no longer held
//! locally at that point the broadcast is skipped and counted: this node must
//! not re-serve a copy it no longer hosts (hosting-invariants, invariant 1).
//!
//! ## First-viable-target signal set (issue #4359 re-review, MUST-FIX 1)
//!
//! Since #4642 step 9, `get_broadcast_targets_update` resolves targets from a
//! SINGLE source — the proximity cache of advertised co-hosts (Source 1). So
//! the meaningful flush signal is:
//!
//! * **Source 1 — proximity cache**: when a neighbor newly announces it hosts a
//!   contract we also host (`NeighborHosting` overlap path), it becomes a
//!   `neighbors_with_contract` target and the flush fires, re-emitting the
//!   stashed broadcast so it now reaches the freshly-advertised co-host.
//!
//! The interest-triggered flush call sites
//! ([`OpManager::flush_pending_broadcast_on_interest`], invoked from every
//! `register_peer_interest` that returns "new" — downstream subscriber,
//! originator upstream, GET-piggyback, remote GET `subscribe=false`, and the
//! Interests/ChangeInterests sync handlers) are LEFT IN PLACE but are now only
//! an eager nudge: an interested-but-not-yet-advertised peer is NOT a broadcast
//! target anymore (Source 2 was removed), so such a flush re-emits, finds no
//! target, and simply re-stashes (or lets the entry expire at its TTL). It is
//! harmless and cheap; a re-emit only actually propagates once the peer
//! advertises (Source 1) — live, or reconciled by the anti-entropy heartbeat.
//!
//! That set is complete: a never-seen id can only gain a broadcast target by a
//! connected neighbor advertising it hosts the id (Source 1), whether that
//! advertisement arrives live or is reconciled by the ~5-min InterestSync
//! anti-entropy heartbeat (`ring.rs::interest_heartbeat`, #4722). The give-up
//! path additionally re-checks targets immediately after stashing to close the
//! stash-after-flush lost-wakeup race. The flush re-reads the *current* local
//! state (not the stale give-up-time bytes) so a newer locally-applied UPDATE
//! is never clobbered.
//!
//! ## Bounding (per `.claude/rules/code-style.md` per-key-collection rule and
//! the AGENTS.md "cleanup exemptions must be time-bounded" rule)
//!
//! Stashes come from this node's own broadcast give-up, which fires for any
//! state it stores and fans out, including a remote peer's PUT stored under
//! every-hop hosting, so remote peers can influence which ids are stashed. The
//! store therefore holds NO state bytes (an earlier revision held full states,
//! up to `MAX_STATE_SIZE` each, which a count cap does not bound: #5551).
//! Memory is bounded by [`MAX_PENDING_BROADCASTS`] x (contract id + timestamp +
//! LRU node), about a hundred kilobytes in total, whatever the state sizes.
//!
//! * **Count**: at most [`MAX_PENDING_BROADCASTS`] ids. An insert at capacity
//!   evicts the least-recently-stashed id (counted, and logged at `info!` on
//!   powers of two so a flood is visible in release builds). An evicted id
//!   loses its deferred broadcast until anti-entropy repairs it, so eviction is
//!   a cost, but one that costs the attacker as many distinct ids as the cap.
//! * **Age (TTL)**: an entry older than [`PENDING_BROADCAST_TTL_MS`] is never
//!   re-broadcast. Expired entries are dropped eagerly, on every
//!   [`PendingBroadcastStore::stash`] and every
//!   [`PendingBroadcastStore::contains`] (called from the interest-activity
//!   paths), not only when the same contract is later taken.
//!
//! Entries are evicted and expired in stash order (re-stashing an id refreshes
//! its position and timestamp without growing the store). If the system clock
//! steps backwards, an expired entry can sit behind a newer one until the clock
//! catches up; the count cap still bounds it.
//!
//! Timestamps come from [`GlobalSimulationTime`](crate::config::GlobalSimulationTime)
//! so the TTL is deterministic under the simulation clock (DST).
//!
//! ## Cross-thread clock consistency (issue #4359 re-review, SHOULD-FIX 5)
//!
//! `stash` records `inserted_at_ms` on the event-loop task while `take`'s TTL
//! check reads the clock on the subscribe/op-driver task. `GlobalSimulationTime`
//! is *thread-local*, so in principle those two reads could come from different
//! clock bases. This is benign for both run modes:
//!
//! * **Production**: no simulation time is set, so both reads fall back to the
//!   process-global, monotonic `SystemTime` — a single shared clock.
//! * **DST**: the deterministic/turmoil harness runs on a
//!   `new_current_thread()` single-threaded paused-time runtime
//!   (`simulation_integration.rs::create_runtime`), so the event loop and the
//!   subscribe driver — and any `tokio::spawn`ed give-up retry / flush task —
//!   all execute on the SAME thread and therefore share one thread-local
//!   simulation clock. The clocks cannot diverge.
//!
//! The TTL is a coarse 5-minute liveness bound, not a fine-grained deadline, so
//! even a hypothetical small skew could only shift an eviction by a tolerance
//! far below the window. No shared/atomic clock is needed here.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

use freenet_stdlib::prelude::{ContractInstanceId, WrappedState};
use lru::LruCache;
use parking_lot::Mutex;

use crate::config::GlobalSimulationTime;

/// Maximum number of distinct contracts with a deferred broadcast pending. The
/// store holds only ids and timestamps, so this bounds memory at roughly
/// `1024 x (32-byte id + 8-byte timestamp + LRU node)`. Raising it only costs
/// that much per entry; it exists so a churn of fresh ids cannot grow the map
/// without bound, and so a flood must supply this many distinct ids to evict a
/// legitimate entry.
const MAX_PENDING_BROADCASTS: usize = 1024;

/// How long a deferred broadcast stays eligible for re-emission. After this the
/// entry is dropped without re-broadcasting: if no subscriber/interest appeared
/// within the window, the fresh PUT is treated as locally-hosted-only and the
/// stash is released. 5 minutes is well past the observed ~31 s
/// interest-resolve latency for fresh ids (#4359) with a wide safety margin,
/// while still bounding how long an un-propagated entry lingers.
const PENDING_BROADCAST_TTL_MS: u64 = 5 * 60 * 1_000;

/// Log on the 1st, 2nd, 4th, 8th, ... occurrence so a sustained event is
/// visible in release builds without a log line per occurrence.
fn is_log_point(count: u64) -> bool {
    count.is_power_of_two()
}

/// Outcome of reading the CURRENT local state for a contract at flush time.
/// The distinction decides whether a drained entry is dropped or kept: only a
/// definitive [`StateRead::NotHeld`] may drop the deferred broadcast.
pub(crate) enum StateRead {
    /// The contract is held locally; broadcast this state.
    Held(WrappedState),
    /// The contract handler answered that no state is held. Skip and count.
    NotHeld,
    /// The read itself failed (timeout, queue refusal, dropped channel, store
    /// error). Says nothing about whether the state is held, so the entry is
    /// re-stashed rather than lost.
    Error,
}

/// Bounded store of contracts whose broadcast found no targets and that await
/// the first interested peer to re-fan-out to. See module docs.
pub(crate) struct PendingBroadcastStore {
    /// Contract id -> stash time (ms). Least-recently-stashed first.
    entries: Mutex<LruCache<ContractInstanceId, u64>>,
    /// Ids evicted to honor the count cap (lifetime).
    evicted_at_capacity: AtomicU64,
    /// Ids dropped because their TTL elapsed before a flush (lifetime).
    expired: AtomicU64,
    /// Flushes skipped because the contract was no longer held locally (lifetime).
    flush_skipped_not_held: AtomicU64,
    /// Flushes whose state read failed and whose entry was re-stashed (lifetime).
    read_failed_restashed: AtomicU64,
}

impl PendingBroadcastStore {
    pub(crate) fn new() -> Self {
        Self::with_capacity(MAX_PENDING_BROADCASTS)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("capacity is non-zero"),
            )),
            evicted_at_capacity: AtomicU64::new(0),
            expired: AtomicU64::new(0),
            flush_skipped_not_held: AtomicU64::new(0),
            read_failed_restashed: AtomicU64::new(0),
        }
    }

    /// Record that `contract` owes a deferred broadcast. Re-stashing an id
    /// refreshes its timestamp and position without growing the store. Drops
    /// expired entries and enforces the count cap on the way in.
    pub(crate) fn stash(&self, contract: ContractInstanceId) {
        let now_ms = GlobalSimulationTime::read_time_ms();
        let mut entries = self.entries.lock();
        self.drop_expired(&mut entries, now_ms);
        if let Some((displaced, _)) = entries.push(contract, now_ms) {
            // `push` also returns the previous value for the SAME key; only a
            // different key is a capacity eviction.
            if displaced != contract {
                let n = self.evicted_at_capacity.fetch_add(1, Ordering::Relaxed) + 1;
                if is_log_point(n) {
                    tracing::info!(
                        phase = "pending_broadcast_evicted",
                        evicted_total = n,
                        capacity = MAX_PENDING_BROADCASTS,
                        "deferred fresh-contract broadcast evicted at the count cap; \
                         that contract waits for anti-entropy instead of the eager re-emit"
                    );
                }
            }
        }
    }

    /// Whether an unexpired entry exists for `contract`. Used as a cheap
    /// fast-path guard so the flush can skip an async contract-handler read on
    /// the common case where nothing is stashed. Also drops expired entries, so
    /// interest activity releases stale ids without waiting for a `take`. Does
    /// not change eviction order.
    pub(crate) fn contains(&self, contract: &ContractInstanceId) -> bool {
        let now_ms = GlobalSimulationTime::read_time_ms();
        let mut entries = self.entries.lock();
        self.drop_expired(&mut entries, now_ms);
        // The sweep above is positional (oldest stash first). A re-stashed
        // entry keeps its old stamp at the newest end, so check this entry's
        // own stamp: expired means absent, and it is dropped here.
        let expired = match entries.peek(contract) {
            None => return false,
            Some(at) => now_ms.saturating_sub(*at) >= PENDING_BROADCAST_TTL_MS,
        };
        if expired {
            entries.pop(contract);
            self.note_expired();
            return false;
        }
        true
    }

    /// Remove the pending entry for `contract`, returning `true` only if one
    /// existed and had not expired. Called when the first interested
    /// peer/subscriber appears so the caller can re-emit
    /// `BroadcastStateChange`. An expired entry is removed and `false` is
    /// returned (no re-broadcast).
    pub(crate) fn take(&self, contract: &ContractInstanceId) -> bool {
        self.take_stamped(contract).is_some()
    }

    /// Like [`Self::take`], returning the original stash time so a caller whose
    /// follow-up step fails transiently can put the entry back with
    /// [`Self::restash_after_failed_read`] without extending its TTL.
    pub(crate) fn take_stamped(&self, contract: &ContractInstanceId) -> Option<u64> {
        let inserted_at_ms = self.entries.lock().pop(contract)?;
        let now_ms = GlobalSimulationTime::read_time_ms();
        if now_ms.saturating_sub(inserted_at_ms) >= PENDING_BROADCAST_TTL_MS {
            self.note_expired();
            return None;
        }
        Some(inserted_at_ms)
    }

    /// Put an entry back after a flush's state read failed transiently,
    /// keeping its ORIGINAL stash time, so repeated failures cannot keep it
    /// alive past [`PENDING_BROADCAST_TTL_MS`]. It re-enters at the newest end
    /// of the eviction order with an older timestamp, which can delay (never
    /// skip) its expiry sweep until older entries ahead of it are gone; `take`
    /// still refuses it once expired.
    pub(crate) fn restash_after_failed_read(
        &self,
        contract: ContractInstanceId,
        stashed_at_ms: u64,
    ) {
        let n = self.read_failed_restashed.fetch_add(1, Ordering::Relaxed) + 1;
        if is_log_point(n) {
            tracing::info!(
                phase = "pending_broadcast_read_failed",
                contract = %contract,
                read_failed_total = n,
                "deferred fresh-contract broadcast: current-state read failed; entry re-stashed"
            );
        }
        let mut entries = self.entries.lock();
        // Insert only if absent. While the read was awaiting, another stash of
        // this id may have been made; its fresher stamp must win, not be
        // shortened by this older one.
        if entries.contains(&contract) {
            return;
        }
        if let Some((displaced, _)) = entries.push(contract, stashed_at_ms) {
            if displaced != contract {
                self.evicted_at_capacity.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn note_expired(&self) {
        let n = self.expired.fetch_add(1, Ordering::Relaxed) + 1;
        if is_log_point(n) {
            tracing::info!(
                phase = "pending_broadcast_expired",
                expired_total = n,
                "deferred fresh-contract broadcast expired without an interested peer"
            );
        }
    }

    /// Record that a flush found its contract no longer held locally and
    /// skipped the broadcast. A node must not re-serve a copy it no longer
    /// hosts, so this is correct behaviour, but a high rate would mean
    /// stashes are being outlived by eviction.
    pub(crate) fn note_flush_skipped_not_held(&self, contract: &ContractInstanceId) {
        let n = self.flush_skipped_not_held.fetch_add(1, Ordering::Relaxed) + 1;
        if is_log_point(n) {
            tracing::info!(
                phase = "pending_broadcast_flush_skipped",
                contract = %contract,
                skipped_total = n,
                "deferred fresh-contract broadcast skipped: contract no longer held locally"
            );
        }
    }

    /// Pop expired entries from the oldest end. Entries are stashed in
    /// timestamp order, so the scan stops at the first unexpired one and costs
    /// O(expired).
    fn drop_expired(&self, entries: &mut LruCache<ContractInstanceId, u64>, now_ms: u64) {
        while entries
            .peek_lru()
            .is_some_and(|(_, at)| now_ms.saturating_sub(*at) >= PENDING_BROADCAST_TTL_MS)
        {
            entries.pop_lru();
            self.note_expired();
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().len()
    }

    // The getters below are the store's stats surface. No diagnostics snapshot
    // consumes them yet, so outside tests they have no caller.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn evicted_at_capacity(&self) -> u64 {
        self.evicted_at_capacity.load(Ordering::Relaxed)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn expired(&self) -> u64 {
        self.expired.load(Ordering::Relaxed)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn flush_skipped_not_held(&self) -> u64 {
        self.flush_skipped_not_held.load(Ordering::Relaxed)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn read_failed_restashed(&self) -> u64 {
        self.read_failed_restashed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(seed: u8) -> ContractInstanceId {
        ContractInstanceId::new([seed; 32])
    }

    fn cid_n(i: u64) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&i.to_le_bytes());
        ContractInstanceId::new(bytes)
    }

    #[test]
    fn stash_then_take_returns_true_and_removes_entry() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        assert_eq!(store.len(), 1);
        assert!(store.take(&cid(1)), "pending entry present");
        assert_eq!(store.len(), 0, "take must remove the entry");
        assert!(!store.take(&cid(1)), "second take yields nothing");
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn contains_reflects_membership_without_removing() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        assert!(!store.contains(&cid(1)), "absent before stash");
        store.stash(cid(1));
        assert!(store.contains(&cid(1)), "present after stash");
        assert!(store.contains(&cid(1)), "contains is non-destructive");
        assert!(store.take(&cid(1)), "take still finds it");
        assert!(!store.contains(&cid(1)), "absent after take");
        GlobalSimulationTime::clear_time();
    }

    /// Re-stashing the same id refreshes its timestamp and does not grow the
    /// store or count as an eviction.
    #[test]
    fn restash_refreshes_timestamp_without_growing() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS - 1);
        store.stash(cid(1));
        assert_eq!(store.len(), 1, "re-stash must not duplicate the contract");
        assert_eq!(
            store.evicted_at_capacity(),
            0,
            "a re-stash is not an eviction"
        );
        // Past the ORIGINAL stash's TTL but within the refreshed one.
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        assert!(
            store.contains(&cid(1)),
            "timestamp must have been refreshed"
        );
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn expired_entry_is_not_rebroadcast_and_is_counted() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        assert!(
            !store.take(&cid(1)),
            "an entry past its TTL must not be re-broadcast"
        );
        assert_eq!(store.expired(), 1);
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn prune_drops_expired_on_stash() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        store.stash(cid(2));
        assert_eq!(store.len(), 1, "stale entry must be pruned on stash");
        assert_eq!(store.expired(), 1);
        assert!(!store.take(&cid(1)));
        assert!(store.take(&cid(2)));
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn stash_only_expires_entries_older_than_the_ttl() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS / 2);
        store.stash(cid(2));
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        store.stash(cid(3));
        assert!(!store.take(&cid(1)), "oldest expired");
        assert!(store.take(&cid(2)), "younger entry survives");
        assert!(store.take(&cid(3)));
        GlobalSimulationTime::clear_time();
    }

    /// Expiry must not wait for the same contract to be taken: `contains`
    /// (the interest-activity fast path) releases expired entries.
    #[test]
    fn contains_drops_expired_entries() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        store.stash(cid(2));
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        assert!(!store.contains(&cid(3)), "unrelated probe");
        assert_eq!(store.len(), 0, "expired entries released by contains");
        assert_eq!(store.expired(), 2);
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn contains_reports_an_expired_entry_as_absent() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS - 1);
        assert!(store.contains(&cid(1)), "still live just inside the TTL");
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS);
        assert!(!store.contains(&cid(1)), "expired at the TTL boundary");
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn size_cap_evicts_oldest() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        for i in 0..MAX_PENDING_BROADCASTS as u64 {
            GlobalSimulationTime::set_time_ms(i);
            store.stash(cid_n(i));
        }
        assert_eq!(store.len(), MAX_PENDING_BROADCASTS);
        assert_eq!(store.evicted_at_capacity(), 0);

        GlobalSimulationTime::set_time_ms(MAX_PENDING_BROADCASTS as u64);
        store.stash(cid(0xFF));
        assert_eq!(store.len(), MAX_PENDING_BROADCASTS, "cap holds");
        assert_eq!(store.evicted_at_capacity(), 1);
        assert!(
            !store.take(&cid_n(0)),
            "oldest entry must have been evicted"
        );
        assert!(store.take(&cid(0xFF)), "new entry retained");
        GlobalSimulationTime::clear_time();
    }

    /// Victim behaviour, pinned. A legitimate pending entry is evicted only
    /// after a flood supplies a full cap of DISTINCT newer ids; a flood of
    /// fewer ids, or of one id repeated, does not touch it. Stashed states'
    /// sizes are irrelevant now because none are stored, so one large remote
    /// PUT costs the attacker exactly one slot (the earlier byte-bounded design
    /// let one or two 50 MiB states evict everything).
    #[test]
    fn a_flood_must_supply_a_full_cap_of_distinct_ids_to_evict_a_legit_entry() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        let legit = cid(0xAA);
        store.stash(legit);

        // A repeated id, however often, takes one slot.
        for t in 1..=10_000u64 {
            GlobalSimulationTime::set_time_ms(t % 1_000);
            store.stash(cid(0xBB));
        }
        assert!(store.contains(&legit), "repeating one id evicts nothing");
        assert_eq!(store.evicted_at_capacity(), 0);

        // cap - 2 further distinct ids still leave `legit` resident (legit +
        // the repeated id + this many = cap - 0 would be full; stay under it).
        GlobalSimulationTime::set_time_ms(2_000);
        for i in 0..(MAX_PENDING_BROADCASTS as u64 - 2) {
            store.stash(cid_n(i));
        }
        assert_eq!(store.len(), MAX_PENDING_BROADCASTS);
        assert!(
            store.contains(&legit),
            "store exactly full, nothing evicted yet"
        );
        assert_eq!(store.evicted_at_capacity(), 0);

        // One more distinct id evicts the least-recently-stashed, which is
        // `legit`, because the flood refreshed everything else.
        store.stash(cid_n(1_000_000));
        assert!(!store.contains(&legit), "oldest entry is the victim");
        assert_eq!(store.evicted_at_capacity(), 1);
        GlobalSimulationTime::clear_time();
    }

    /// A failed read re-stashes with the ORIGINAL stamp: it is kept for a later
    /// flush, but repeated failures cannot extend its life past the TTL.
    #[test]
    fn restash_after_failed_read_keeps_the_original_ttl() {
        GlobalSimulationTime::set_time_ms(1_000);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        let stamp = store.take_stamped(&cid(1)).expect("entry present");
        assert_eq!(stamp, 1_000);
        GlobalSimulationTime::set_time_ms(1_000 + PENDING_BROADCAST_TTL_MS - 1);
        store.restash_after_failed_read(cid(1), stamp);
        assert_eq!(store.read_failed_restashed(), 1);
        assert!(store.contains(&cid(1)), "kept for a later flush");
        GlobalSimulationTime::set_time_ms(1_000 + PENDING_BROADCAST_TTL_MS);
        assert!(
            !store.contains(&cid(1)),
            "TTL still counts from the first stash"
        );
        GlobalSimulationTime::clear_time();
    }

    /// A concurrent stash made while the read was awaiting must not be
    /// overwritten by the re-stash (which carries an older stamp).
    #[test]
    fn restash_after_failed_read_does_not_overwrite_a_concurrent_stash() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        let stamp = store.take_stamped(&cid(1)).expect("entry present");
        // A fresh stash of the same id lands while the read is in flight.
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS - 1);
        store.stash(cid(1));
        store.restash_after_failed_read(cid(1), stamp);
        // Past the old stamp's TTL; the fresher stash must still be live.
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        assert!(
            store.contains(&cid(1)),
            "the concurrent fresher stash must keep its own stamp"
        );
        GlobalSimulationTime::clear_time();
    }

    /// A re-stashed entry sits at the newest end with an old stamp, behind the
    /// positional sweep. `contains` must still report it expired and drop it.
    #[test]
    fn contains_checks_the_entry_s_own_stamp_after_a_restash() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1));
        let stamp = store.take_stamped(&cid(1)).expect("entry present");
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS - 1);
        store.stash(cid(2));
        store.restash_after_failed_read(cid(1), stamp);
        assert_eq!(store.len(), 2);
        // cid(2) is still live, so the positional sweep stops at it and never
        // reaches the older-stamped cid(1) behind it.
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        assert!(
            !store.contains(&cid(1)),
            "expired entry must read as absent"
        );
        assert_eq!(store.len(), 1, "and be dropped");
        assert!(store.contains(&cid(2)));
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn flush_skipped_not_held_is_counted() {
        let store = PendingBroadcastStore::new();
        store.note_flush_skipped_not_held(&cid(1));
        store.note_flush_skipped_not_held(&cid(1));
        assert_eq!(store.flush_skipped_not_held(), 2);
    }
}
