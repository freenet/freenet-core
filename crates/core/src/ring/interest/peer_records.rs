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
use std::sync::Arc;

use freenet_stdlib::prelude::StateSummary;
use tokio::time::Instant;

use super::{INTEREST_TTL, NeverPopulatedOrigin, PeerKey, SummaryMissingReason};

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

impl ContractPeers {
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
        true
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
