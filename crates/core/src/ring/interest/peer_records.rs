//! One contract's neighbour interest records, with each distinct neighbour
//! summary stored once (#5786).
//!
//! Neighbours that are in sync report byte-identical summaries for the same
//! contract (River rooms are the main case: ~33 KB per room, the same bytes
//! from every in-sync member). Storing one copy per neighbour made the bytes
//! held for a contract grow with its neighbour count instead of with the number
//! of distinct versions in play, which #5779's counted hosting budget would
//! have charged against the node (#5647).
//!
//! [`ContractPeers`] owns both the records and an intern table mapping summary
//! bytes to one shared allocation plus the number of records holding it. The
//! summary field of [`PeerInterest`] is private to this module, so every write
//! to it goes through a [`ContractPeers`] method that keeps the holder counts
//! exact: a table entry exists exactly while at least one record holds those
//! bytes. The table lives inside the same `interested_peers` value as the
//! records, so it is updated under the contract's existing shard guard and adds
//! no cross-map lock ordering; when the contract's value is removed, its table
//! goes with it.
//!
//! Reads are unchanged: [`ContractPeers`] dereferences to the record map, and
//! [`PeerInterest::summary`] returns the shared bytes.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use freenet_stdlib::prelude::StateSummary;
use tokio::time::Instant;

use super::{
    INTEREST_TTL, NeverPopulatedOrigin, PeerKey, SummaryMissingReason, contract_summary_cap,
};

/// Tracking information for a peer's interest in a specific contract.
#[derive(Clone, Debug)]
pub struct PeerInterest {
    /// The peer's current state summary. `None` if interested but has no state
    /// yet. Shared with every other record of the same contract holding the
    /// same bytes, and private so that only [`ContractPeers`] can change it.
    summary: Option<Arc<StateSummary<'static>>>,

    /// Why [`Self::summary`] is absent. Stale (and unread) whenever `summary`
    /// is `Some` — always read it via [`Self::summary_missing_reason`], which
    /// returns `None` in that case rather than a misleading last-clear cause.
    summary_absence: SummaryMissingReason,

    /// Diagnostic-only provenance for the current NeverPopulated epoch.
    pub(super) never_populated_origin: NeverPopulatedOrigin,

    /// Start time and send-attempt count for that epoch.
    pub(super) never_populated_since: Instant,
    pub(super) never_populated_send_starts: u32,

    /// When this interest entry was last refreshed.
    /// Used for TTL-based expiration.
    pub last_refreshed: Instant,

    /// Whether this peer is our upstream in the subscription tree.
    /// Internal routing hint, not exposed to protocol.
    pub is_upstream: bool,
}

impl PeerInterest {
    /// A `None` summary here is [`SummaryMissingReason::NeverPopulated`] by
    /// construction — this is the only constructor, so an entry cannot come
    /// into existence summaryless without carrying that tag.
    fn new(summary: Option<Arc<StateSummary<'static>>>, is_upstream: bool, now: Instant) -> Self {
        Self {
            summary,
            summary_absence: SummaryMissingReason::NeverPopulated,
            never_populated_origin: NeverPopulatedOrigin::New { recreated: false },
            never_populated_since: now,
            never_populated_send_starts: 0,
            last_refreshed: now,
            is_upstream,
        }
    }

    /// The peer's cached summary, if any.
    pub fn summary(&self) -> Option<&StateSummary<'static>> {
        self.summary.as_deref()
    }

    /// Refresh the TTL timestamp with the given current time.
    pub fn refresh(&mut self, now: Instant) {
        self.last_refreshed = now;
    }

    /// Check if this interest has expired relative to the given current time.
    pub fn is_expired_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_refreshed) > INTEREST_TTL
    }

    /// Why this peer has no cached summary, or `None` when one IS cached.
    pub fn summary_missing_reason(&self) -> Option<SummaryMissingReason> {
        self.summary.is_none().then_some(self.summary_absence)
    }

    /// Record why the summary is absent. The caller has already taken the
    /// summary out through [`ContractPeers`].
    ///
    /// Taking `reason` by value (rather than accepting an `Option` summary) is
    /// deliberate: it makes an untagged clear unrepresentable, so a future
    /// clear site cannot silently land in the `NeverPopulated` bucket and
    /// mis-aim the next fix.
    fn mark_cleared(&mut self, reason: SummaryMissingReason, now: Instant) {
        debug_assert!(self.summary.is_none());
        self.summary_absence = reason;
        if reason == SummaryMissingReason::NeverPopulated {
            self.never_populated_origin = NeverPopulatedOrigin::New { recreated: false };
            self.never_populated_since = now;
            self.never_populated_send_starts = 0;
        }
        self.refresh(now);
    }
}

/// Intern-table key: hashes and compares by the summary BYTES, so a lookup can
/// use a borrowed `&[u8]` without allocating.
#[derive(Debug)]
struct InternedBytes(Arc<StateSummary<'static>>);

impl Borrow<[u8]> for InternedBytes {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Hash for InternedBytes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Must match `<[u8] as Hash>` for the `Borrow<[u8]>` lookups.
        <[u8] as Hash>::hash(self.0.as_ref(), state);
    }
}

impl PartialEq for InternedBytes {
    fn eq(&self, other: &Self) -> bool {
        <[u8] as PartialEq>::eq(self.0.as_ref(), other.0.as_ref())
    }
}

impl Eq for InternedBytes {}

/// One contract's neighbour records plus the table that stores each distinct
/// summary among them once. See the module docs.
#[derive(Debug, Default)]
pub(super) struct ContractPeers {
    peers: HashMap<PeerKey, PeerInterest>,
    /// Distinct summary bytes held by `peers`, each with the number of records
    /// holding it. Never contains a zero count.
    summaries: HashMap<InternedBytes, Slot>,
    /// Length of this node's own summary of the contract, as last seen by a
    /// path that had it (#5647, #5781). Sizes [`Self::summary_cap`]. Lives
    /// here so it goes with the contract's records and needs no cleanup.
    own_len: Option<usize>,
    /// The allocation holding this node's own summary bytes, when a record
    /// holds them (a neighbour that is in sync with us). Such a summary costs
    /// nothing a neighbour caused: the per-peer share does not charge it, and
    /// the cap never drops it. A `Weak` keeps the allocation's address from
    /// being reused while it is compared, without keeping the bytes alive.
    own_shared: Weak<StateSummary<'static>>,
    /// Node-wide count of distinct neighbour-summary bytes, shared by every
    /// contract's records (#5781): this contract adds [`Self::counted`] to
    /// it. Read by the write-time node-wide budget check.
    counter: Arc<AtomicU64>,
    /// This contract's contribution to `counter`: its distinct summary bytes
    /// other than the allocation holding our own summary. Kept exact by
    /// [`Self::resync_neighbour_bytes`] after every change, and subtracted
    /// when the records are dropped.
    counted: u64,
    /// Calls to `acquire` and `release`, so tests can tell the same-bytes
    /// early returns apart from an acquire/release pair that nets to zero.
    #[cfg(test)]
    table_ops: usize,
}

/// One distinct summary and the number of records holding it.
#[derive(Debug)]
struct Slot {
    shared: Arc<StateSummary<'static>>,
    holders: usize,
}

impl Deref for ContractPeers {
    type Target = HashMap<PeerKey, PeerInterest>;

    fn deref(&self) -> &Self::Target {
        &self.peers
    }
}

impl Drop for ContractPeers {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.counted, Ordering::Relaxed);
    }
}

impl ContractPeers {
    /// Empty records for a contract, contributing to the node-wide
    /// neighbour-summary counter `counter` (#5781).
    pub(super) fn with_counter(counter: Arc<AtomicU64>) -> Self {
        let mut peers = Self::default();
        peers.counter = counter;
        peers
    }

    /// Distinct summary bytes other than our own summary's allocation.
    pub(super) fn neighbour_bytes(&self) -> u64 {
        self.summaries
            .values()
            .filter(|slot| !self.is_own(&slot.shared))
            .map(|slot| slot.shared.as_ref().as_ref().len() as u64)
            .sum()
    }

    /// Bring this contract's contribution to the node-wide counter up to date
    /// after a change (#5781). O(distinct summaries).
    fn resync_neighbour_bytes(&mut self) {
        let now = self.neighbour_bytes();
        if now >= self.counted {
            self.counter
                .fetch_add(now - self.counted, Ordering::Relaxed);
        } else {
            self.counter
                .fetch_sub(self.counted - now, Ordering::Relaxed);
        }
        self.counted = now;
    }

    /// Whether these exact bytes are already stored for this contract.
    /// Storing them again adds nothing, so no bound ever refuses them.
    pub(super) fn holds_bytes(&self, bytes: &[u8]) -> bool {
        self.summaries.contains_key(bytes)
    }

    /// The node-wide neighbour-summary bytes if `peer`'s summary became
    /// `bytes`, which are not already held here (#5781): the current count,
    /// plus the new bytes, minus `peer`'s current summary if only it holds it
    /// and it is not our own.
    pub(super) fn projected_node_neighbour_bytes(&self, peer: &PeerKey, bytes: &[u8]) -> u64 {
        let freed = self
            .peers
            .get(peer)
            .and_then(|r| r.summary.as_ref())
            .filter(|old| !self.is_own(old))
            .and_then(|old| {
                let old_bytes: &[u8] = old.as_ref().as_ref();
                self.summaries
                    .get(old_bytes)
                    .filter(|slot| slot.holders == 1)
                    .map(|_| old_bytes.len() as u64)
            })
            .unwrap_or(0);
        self.counter
            .load(Ordering::Relaxed)
            .saturating_sub(freed)
            .saturating_add(bytes.len() as u64)
    }

    /// Take one holder reference on `summary`'s bytes, returning the shared
    /// allocation. Identical bytes already held by another record are reused
    /// and `summary` is dropped.
    fn acquire(&mut self, summary: StateSummary<'static>) -> Arc<StateSummary<'static>> {
        #[cfg(test)]
        {
            self.table_ops += 1;
        }
        // One hash per acquire: the entry API needs an owned key, and moving
        // the summary into an `Arc` copies no bytes. On a hit the new `Arc`
        // (and the duplicate bytes it owns) is dropped.
        let candidate = Arc::new(summary);
        match self.summaries.entry(InternedBytes(Arc::clone(&candidate))) {
            Entry::Occupied(mut slot) => {
                slot.get_mut().holders += 1;
                Arc::clone(&slot.get().shared)
            }
            Entry::Vacant(vacant) => {
                vacant.insert(Slot {
                    shared: Arc::clone(&candidate),
                    holders: 1,
                });
                candidate
            }
        }
    }

    /// Drop one holder reference on `summary`'s bytes, removing the table entry
    /// when it was the last.
    ///
    /// This hashes the bytes again. Storing each record's hash to skip that
    /// would need a lookup by precomputed hash, which std's `HashMap` does not
    /// offer; the same-bytes early returns in [`Self::set_summary`] and
    /// [`Self::insert`] already keep the steady state (an in-sync neighbour
    /// resending identical bytes) from reaching here.
    fn release(&mut self, summary: &StateSummary<'static>) {
        #[cfg(test)]
        {
            self.table_ops += 1;
        }
        let bytes: &[u8] = summary.as_ref();
        match self.summaries.get_mut(bytes) {
            Some(slot) if slot.holders > 1 => slot.holders -= 1,
            Some(_) => {
                self.summaries.remove(bytes);
            }
            None => {
                // Unreachable while every summary write goes through this
                // type. Logged so an accounting bug is visible in release
                // builds, where the debug_assert is compiled out.
                tracing::warn!(
                    summary_len = bytes.len(),
                    "neighbour summary table: released a summary it does not hold"
                );
                debug_assert!(false, "released a summary the table does not hold");
            }
        }
    }

    /// Whether `record` already holds exactly `bytes`. Compares length first,
    /// so a changed summary of a different size costs no byte comparison.
    fn holds_same_bytes(record: Option<&PeerInterest>, bytes: &[u8]) -> bool {
        record
            .and_then(|r| r.summary.as_deref())
            .is_some_and(|held| {
                let held: &[u8] = held.as_ref();
                held.len() == bytes.len() && held == bytes
            })
    }

    /// Mutable access to a record's non-summary fields.
    pub(super) fn get_mut(&mut self, peer: &PeerKey) -> Option<&mut PeerInterest> {
        self.peers.get_mut(peer)
    }

    /// Insert a fresh record for `peer`, replacing any existing one (whose
    /// summary is released), and return it.
    pub(super) fn insert(
        &mut self,
        peer: PeerKey,
        summary: Option<StateSummary<'static>>,
        is_upstream: bool,
        now: Instant,
    ) -> &mut PeerInterest {
        // Re-registering with the bytes the record already holds keeps its
        // shared allocation and holder count as they are: no hashing, no
        // acquire/release.
        let reuse = summary
            .as_ref()
            .is_some_and(|s| Self::holds_same_bytes(self.peers.get(&peer), s.as_ref()));
        let shared = if reuse {
            self.peers.get(&peer).and_then(|p| p.summary.clone())
        } else {
            summary.map(|s| self.acquire(s))
        };
        let previous = self
            .peers
            .insert(peer.clone(), PeerInterest::new(shared, is_upstream, now));
        if !reuse && let Some(old) = previous.as_ref().and_then(|p| p.summary.as_deref()) {
            self.release(old);
        }
        self.resync_neighbour_bytes();
        self.peers
            .get_mut(&peer)
            .expect("record was inserted under this &mut borrow")
    }

    /// Remove `peer`'s record and release its summary. The returned record
    /// still carries its summary so callers can read why it was missing.
    pub(super) fn remove(&mut self, peer: &PeerKey) -> Option<PeerInterest> {
        let removed = self.peers.remove(peer)?;
        if let Some(summary) = removed.summary.as_deref() {
            self.release(summary);
        }
        self.resync_neighbour_bytes();
        Some(removed)
    }

    /// Cache `summary` for an existing record and refresh its TTL. Returns
    /// whether the record existed and whether it already held a summary.
    pub(super) fn set_summary(
        &mut self,
        peer: &PeerKey,
        summary: StateSummary<'static>,
        now: Instant,
    ) -> Option<bool> {
        let record = self.peers.get_mut(peer)?;
        // Steady state: an in-sync neighbour resends the bytes we hold. Only
        // the TTL changes; the table and the stored `Arc` are left alone.
        if Self::holds_same_bytes(Some(&*record), summary.as_ref()) {
            record.refresh(now);
            return Some(true);
        }
        // Acquire before releasing, so replacing a summary with identical
        // bytes never drops the table entry in between.
        let shared = self.acquire(summary);
        let interest = self
            .peers
            .get_mut(peer)
            .expect("presence checked above under the same &mut borrow");
        let previous = interest.summary.replace(shared);
        interest.refresh(now);
        let had_summary = previous.is_some();
        if let Some(old) = previous.as_deref() {
            self.release(old);
        }
        self.resync_neighbour_bytes();
        Some(had_summary)
    }

    /// Drop an existing record's cached summary, recording why, and refresh its
    /// TTL. Returns whether the record existed.
    pub(super) fn clear_summary(
        &mut self,
        peer: &PeerKey,
        reason: SummaryMissingReason,
        now: Instant,
    ) -> bool {
        let Some(interest) = self.peers.get_mut(peer) else {
            return false;
        };
        let previous = interest.summary.take();
        interest.mark_cleared(reason, now);
        if let Some(old) = previous.as_deref() {
            self.release(old);
        }
        self.resync_neighbour_bytes();
        true
    }

    /// Every record holding a summary other than this node's own, with the
    /// bytes charged to that peer for it and the allocation address (#5781):
    /// the summary's length divided by the number of records holding it,
    /// rounded up. A summary only one peer sent is charged to it in full;
    /// identical bytes from several peers split the charge, so peers sending
    /// the same bytes together still use up their shares.
    ///
    /// Identical bytes always share one allocation (the table's invariant,
    /// checked by [`Self::assert_consistent`]), so holders are counted by
    /// allocation without hashing bytes.
    pub(super) fn charged_summaries(&self) -> Vec<(PeerKey, u64, Weak<StateSummary<'static>>)> {
        let mut holders: HashMap<*const StateSummary<'static>, u64> = HashMap::new();
        for record in self.peers.values() {
            if let Some(shared) = &record.summary {
                *holders.entry(Arc::as_ptr(shared)).or_default() += 1;
            }
        }
        self.peers
            .iter()
            .filter_map(|(peer, record)| {
                let shared = record.summary.as_ref()?;
                if self.is_own(shared) {
                    return None;
                }
                let ptr = Arc::as_ptr(shared);
                let len = shared.as_ref().as_ref().len() as u64;
                let count = holders.get(&ptr).copied().unwrap_or(1).max(1);
                Some((peer.clone(), len.div_ceil(count), Arc::downgrade(shared)))
            })
            .collect()
    }

    /// The distinct summaries counted toward the node-wide budget (every one
    /// but our own allocation), for the node-wide trim (#5781).
    pub(super) fn counted_summaries(&self) -> Vec<Arc<StateSummary<'static>>> {
        self.summaries
            .values()
            .filter(|slot| !self.is_own(&slot.shared))
            .map(|slot| Arc::clone(&slot.shared))
            .collect()
    }

    /// Clear every record holding exactly `victim` (by allocation), in
    /// peer-key order, keeping the records (#5781). Returns the records
    /// cleared.
    pub(super) fn clear_allocation(
        &mut self,
        victim: &Arc<StateSummary<'static>>,
        now: Instant,
    ) -> u64 {
        let mut holders: Vec<PeerKey> = self
            .peers
            .iter()
            .filter(|(_, r)| r.summary.as_ref().is_some_and(|s| Arc::ptr_eq(s, victim)))
            .map(|(peer, _)| peer.clone())
            .collect();
        holders.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let mut cleared = 0;
        for peer in &holders {
            if self.clear_summary(peer, SummaryMissingReason::ClearedOverSizeBound, now) {
                cleared += 1;
            }
        }
        cleared
    }

    /// Whether `bytes` are this node's own summary as currently marked.
    pub(super) fn is_own_bytes(&self, bytes: &[u8]) -> bool {
        self.own_shared.upgrade().is_some_and(|marked| {
            let marked: &[u8] = marked.as_ref().as_ref();
            marked.len() == bytes.len() && marked == bytes
        })
    }

    /// Whether `shared` is the allocation holding this node's own summary.
    fn is_own(&self, shared: &Arc<StateSummary<'static>>) -> bool {
        std::ptr::eq(Weak::as_ptr(&self.own_shared), Arc::as_ptr(shared))
    }

    /// The length of this node's own summary of the contract, if known.
    pub(super) fn own_summary_len(&self) -> Option<usize> {
        self.own_len
    }

    /// Record this node's current own summary of the contract (#5781),
    /// computed fresh by the caller. The marked allocation is forgotten only
    /// if our bytes changed: the same bytes reported again keep it, so a
    /// neighbour that is in sync stays uncharged.
    pub(super) fn note_own_summary(&mut self, ours: &[u8]) {
        self.own_len = Some(ours.len());
        let unchanged = self.own_shared.upgrade().is_some_and(|marked| {
            let marked: &[u8] = marked.as_ref().as_ref();
            marked.len() == ours.len() && marked == ours
        });
        if !unchanged {
            self.own_shared = Weak::new();
            self.resync_neighbour_bytes();
        }
    }

    /// Record that `peer`'s stored summary is this node's own summary bytes
    /// (#5781). `authoritative` when the digests matched or the peer reported
    /// exactly the summary we just computed. A delivery is not: it can
    /// complete after our summary has moved on, so it marks our summary only
    /// when none is recorded yet ([`Self::delivery_marks_ours`]).
    pub(super) fn note_own_summary_held_by(&mut self, peer: &PeerKey, authoritative: bool) {
        let Some(shared) = self.peers.get(peer).and_then(|r| r.summary.clone()) else {
            return;
        };
        let len = shared.as_ref().as_ref().len();
        if !authoritative && !self.delivery_marks_ours() {
            return;
        }
        self.own_len = Some(len);
        self.own_shared = Arc::downgrade(&shared);
        self.resync_neighbour_bytes();
    }

    /// Whether a delivered summary (not authoritative) may be recorded as our
    /// own: only while no own summary is recorded yet.
    pub(super) fn delivery_marks_ours(&self) -> bool {
        self.own_len.is_none()
    }

    /// Most distinct summary bytes this contract may hold
    /// ([`contract_summary_cap`] of our own summary length).
    pub(super) fn summary_cap(&self) -> u64 {
        contract_summary_cap(self.own_len)
    }

    /// Distinct summary bytes held if `peer`'s summary became `bytes`
    /// (#5781). Bytes already in the table add nothing; a summary only `peer`
    /// held is released by the replacement.
    pub(super) fn projected_summary_bytes(&self, peer: &PeerKey, bytes: &[u8]) -> u64 {
        let held = self.held_summary_bytes();
        if self.holds_bytes(bytes) {
            return held;
        }
        let freed = self
            .peers
            .get(peer)
            .and_then(|r| r.summary.as_deref())
            .and_then(|old| {
                let old_bytes: &[u8] = old.as_ref();
                self.summaries
                    .get(old_bytes)
                    .filter(|slot| slot.holders == 1)
                    .map(|_| old_bytes.len() as u64)
            })
            .unwrap_or(0);
        held.saturating_sub(freed)
            .saturating_add(bytes.len() as u64)
    }

    /// Drop the largest distinct summaries, other than this node's own, until
    /// the contract holds at most [`Self::summary_cap`] (#5781). Every record
    /// holding a dropped summary keeps its record and loses the summary
    /// ([`SummaryMissingReason::ClearedOverSizeBound`]). Returns the records
    /// cleared and the distinct bytes freed.
    pub(super) fn trim_to_cap(&mut self, now: Instant) -> (u64, u64) {
        let cap = self.summary_cap();
        let mut held = self.held_summary_bytes();
        if held <= cap {
            return (0, 0);
        }
        let mut distinct: Vec<Arc<StateSummary<'static>>> = self
            .summaries
            .values()
            .filter(|slot| !self.is_own(&slot.shared))
            .map(|slot| Arc::clone(&slot.shared))
            .collect();
        // Largest first, then by the bytes themselves, so the order is the
        // same on every node and every run.
        distinct.sort_by(|a, b| {
            let (a, b): (&[u8], &[u8]) = (a.as_ref().as_ref(), b.as_ref().as_ref());
            b.len().cmp(&a.len()).then_with(|| a.cmp(b))
        });
        let mut records_cleared = 0;
        let mut bytes_freed = 0u64;
        for victim in distinct {
            if held <= cap {
                break;
            }
            let len = victim.as_ref().as_ref().len() as u64;
            records_cleared += self.clear_allocation(&victim, now);
            held = held.saturating_sub(len);
            bytes_freed = bytes_freed.saturating_add(len);
        }
        (records_cleared, bytes_freed)
    }

    /// Number of distinct summaries stored for this contract.
    #[cfg(test)]
    pub(super) fn distinct_summaries(&self) -> usize {
        self.summaries.len()
    }

    /// Bytes of summary data stored for this contract, counting each distinct
    /// summary once. See [`super::InterestManager::distinct_summary_bytes_for`].
    pub(super) fn held_summary_bytes(&self) -> u64 {
        self.summaries
            .values()
            .map(|slot| slot.shared.as_ref().as_ref().len() as u64)
            .sum()
    }

    /// Panics unless the intern table matches the records exactly: every
    /// record's summary is the table's allocation for those bytes, and every
    /// table entry's count is the number of records holding it.
    #[cfg(test)]
    pub(super) fn assert_consistent(&self) {
        let mut expected: HashMap<&[u8], usize> = HashMap::new();
        for interest in self.peers.values() {
            if let Some(summary) = interest.summary.as_ref() {
                let slot = self
                    .summaries
                    .get(summary.as_ref().as_ref())
                    .expect("a held summary is missing from the intern table");
                assert!(
                    Arc::ptr_eq(&slot.shared, summary),
                    "a record holds its own copy instead of the shared one"
                );
                *expected.entry(summary.as_ref().as_ref()).or_default() += 1;
            }
        }
        assert_eq!(
            expected.len(),
            self.summaries.len(),
            "intern table holds summaries no record holds"
        );
        for (bytes, holders) in expected {
            assert_eq!(
                self.summaries.get(bytes).map(|slot| slot.holders),
                Some(holders)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;

    fn peer() -> PeerKey {
        PeerKey(TransportKeypair::new().public().clone())
    }

    fn summary(bytes: &[u8]) -> StateSummary<'static> {
        StateSummary::from(bytes.to_vec())
    }

    fn shared(records: &ContractPeers, peer: &PeerKey) -> Arc<StateSummary<'static>> {
        Arc::clone(records.peers[peer].summary.as_ref().expect("summary held"))
    }

    #[test]
    fn identical_summaries_share_one_allocation() {
        let now = Instant::now();
        let mut records = ContractPeers::default();
        let (a, b, c) = (peer(), peer(), peer());
        records.insert(a.clone(), Some(summary(&[1; 64])), false, now);
        records.insert(b.clone(), None, false, now);
        assert_eq!(records.set_summary(&b, summary(&[1; 64]), now), Some(false));
        records.insert(c.clone(), Some(summary(&[2; 64])), false, now);

        assert!(Arc::ptr_eq(&shared(&records, &a), &shared(&records, &b)));
        assert!(!Arc::ptr_eq(&shared(&records, &a), &shared(&records, &c)));
        assert_eq!(records.distinct_summaries(), 2);
        assert_eq!(records.held_summary_bytes(), 128);
        records.assert_consistent();
    }

    #[test]
    fn replacing_a_summary_moves_its_holder_count() {
        let now = Instant::now();
        let mut records = ContractPeers::default();
        let (a, b) = (peer(), peer());
        records.insert(a.clone(), Some(summary(&[1])), false, now);
        records.insert(b.clone(), Some(summary(&[1])), false, now);

        // Same bytes: nothing changes, and the entry survives.
        assert_eq!(records.set_summary(&a, summary(&[1]), now), Some(true));
        assert_eq!(records.distinct_summaries(), 1);
        records.assert_consistent();

        // Different bytes: a second entry; the first still has one holder.
        assert_eq!(records.set_summary(&a, summary(&[2]), now), Some(true));
        assert_eq!(records.distinct_summaries(), 2);
        records.assert_consistent();

        // The last holder of [1] moves too: its entry goes.
        assert_eq!(records.set_summary(&b, summary(&[2]), now), Some(true));
        assert_eq!(records.distinct_summaries(), 1);
        assert!(Arc::ptr_eq(&shared(&records, &a), &shared(&records, &b)));
        records.assert_consistent();

        // Re-inserting a record over an existing one releases the old summary.
        records.insert(a.clone(), Some(summary(&[3])), false, now);
        records.insert(b.clone(), None, false, now);
        assert_eq!(records.distinct_summaries(), 1);
        records.assert_consistent();
    }

    #[test]
    fn resending_the_held_bytes_touches_neither_table_nor_arc() {
        let now = Instant::now();
        let later = now + std::time::Duration::from_secs(5);
        let mut records = ContractPeers::default();
        let (a, b) = (peer(), peer());
        records.insert(a.clone(), Some(summary(&[1; 32])), false, now);
        records.insert(b.clone(), Some(summary(&[1; 32])), false, now);
        let before = shared(&records, &a);
        let ops = records.table_ops;

        // set_summary with the bytes already held: TTL only.
        assert_eq!(
            records.set_summary(&a, summary(&[1; 32]), later),
            Some(true)
        );
        assert_eq!(records.table_ops, ops, "no acquire/release for same bytes");
        assert!(Arc::ptr_eq(&before, &shared(&records, &a)));
        assert_eq!(records.peers[&a].last_refreshed, later);

        // Re-inserting over a record with the same bytes keeps its allocation.
        records.insert(b.clone(), Some(summary(&[1; 32])), true, later);
        assert_eq!(records.table_ops, ops, "no acquire/release for same bytes");
        assert!(Arc::ptr_eq(&before, &shared(&records, &b)));
        assert!(
            records.peers[&b].is_upstream,
            "the record itself is replaced"
        );
        assert_eq!(records.distinct_summaries(), 1);
        records.assert_consistent();

        // Same length, different bytes: not taken as equal.
        assert_eq!(
            records.set_summary(&a, summary(&[2; 32]), later),
            Some(true)
        );
        assert_ne!(records.table_ops, ops);
        assert_eq!(records.distinct_summaries(), 2);
        records.assert_consistent();
    }

    #[test]
    fn clearing_and_removing_release_every_holder() {
        let now = Instant::now();
        let mut records = ContractPeers::default();
        let (a, b, c) = (peer(), peer(), peer());
        records.insert(a.clone(), Some(summary(&[1])), false, now);
        records.insert(b.clone(), Some(summary(&[1])), false, now);
        records.insert(c.clone(), Some(summary(&[2])), false, now);

        assert!(records.clear_summary(&a, SummaryMissingReason::ClearedByResync, now));
        assert_eq!(
            records.peers[&a].summary_missing_reason(),
            Some(SummaryMissingReason::ClearedByResync)
        );
        assert_eq!(records.distinct_summaries(), 2);
        records.assert_consistent();

        let removed = records.remove(&b).expect("record existed");
        assert_eq!(
            removed.summary().map(|s| s.as_ref().to_vec()),
            Some(vec![1])
        );
        assert_eq!(records.distinct_summaries(), 1);
        records.assert_consistent();

        records.remove(&c);
        records.remove(&a);
        assert!(records.is_empty());
        assert_eq!(records.distinct_summaries(), 0);

        // Operations on an absent record change nothing.
        assert_eq!(records.set_summary(&a, summary(&[9]), now), None);
        assert!(!records.clear_summary(&a, SummaryMissingReason::ClearedByResync, now));
        assert!(records.remove(&a).is_none());
        assert_eq!(records.distinct_summaries(), 0);
    }
}
