//! Scheduled-wakeup primitive for delegates (issue #3972).
//!
//! A delegate asks the host to wake it at an absolute time via
//! [`OutboundDelegateMsg::ScheduleWakeup`](freenet_stdlib::prelude::OutboundDelegateMsg::ScheduleWakeup);
//! the host delivers
//! [`InboundDelegateMsg::WakeupFired`](freenet_stdlib::prelude::InboundDelegateMsg::WakeupFired)
//! at (or after) that time. This lets an always-on delegate run periodic
//! background work (key rotation, TTL pruning, scheduled publication) without a
//! connected UI, instead of pushing that work into a client sync loop.
//!
//! This type owns the schedule:
//! - a **forward index** ordered by fire time so "what fires next" is O(1),
//! - a **reverse index** keyed by `(delegate, tag)` for cancel-by-tag, and
//! - **ReDb persistence** so pending wakeups survive a node restart.
//!
//! It is deliberately NOT a standalone background task. The `contract_handling`
//! event loop (the single task that also runs delegates) owns a
//! [`WakeupScheduler`], schedules into it when a delegate emits `ScheduleWakeup`,
//! and drains due wakeups from it each iteration. Because scheduling and firing
//! happen on the same task, there is no cross-task channel or lock, and a
//! newly-scheduled earlier wakeup is observed on the loop's next pass with no
//! explicit notification.
//!
//! ## Params capture
//!
//! A fired wakeup must invoke the delegate with the same `params` it was
//! registered with (the [`DelegateKey`] is a one-way hash of code+params, so the
//! host cannot recover them from the key). We therefore store the delegate's
//! params alongside each wakeup at schedule time and replay them on fire.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use freenet_stdlib::prelude::{CodeHash, DelegateKey};

use super::storages::Storage;

/// 32-byte `key` field + 32-byte `code_hash` field of a [`DelegateKey`].
const DELEGATE_KEY_LEN: usize = 64;
/// 8-byte seconds-since-epoch + 4-byte nanos prefix on a persisted value.
const TIME_PREFIX_LEN: usize = 12;
/// Max length of a wakeup `tag`. A tag only identifies a wakeup, so a small
/// bound is ample; it also bounds the `(delegate, tag)` ReDb key size.
const MAX_TAG_LEN: usize = 256;
/// Max pending wakeups a single delegate may hold. A delegate is untrusted
/// WASM: without this, one delegate could schedule unlimited distinct tags and
/// exhaust memory + ReDb. Re-scheduling an existing tag (cancel-by-tag) does not
/// count against this. Mirrors the per-actor caps `code-style.md` mandates for
/// externally-influenced collections (e.g. `MAX_SUBSCRIPTIONS_PER_CLIENT`).
const MAX_WAKEUPS_PER_DELEGATE: usize = 64;
/// Global ceiling on pending wakeups across all delegates — the absolute
/// memory/disk bound, in case many delegates each stay under the per-delegate
/// cap.
const MAX_TOTAL_WAKEUPS: usize = 100_000;

/// Identifies a pending wakeup: a delegate plus its opaque tag.
type WakeupId = (DelegateKey, Vec<u8>);

/// A wakeup that is due and has been removed from the schedule, ready to fire.
#[derive(Debug, Clone)]
pub(crate) struct DueWakeup {
    pub delegate_key: DelegateKey,
    pub tag: Vec<u8>,
    /// The delegate's params, captured when the wakeup was scheduled, so it
    /// replays in the same param context.
    pub params: Vec<u8>,
}

/// In-memory bookkeeping for one pending wakeup.
struct Entry {
    fire_at: SystemTime,
    /// Tie-breaker so two wakeups at the same instant get distinct forward-index
    /// keys (a `BTreeMap` key must be unique).
    seq: u64,
    params: Vec<u8>,
}

/// The per-node schedule of pending delegate wakeups.
pub(crate) struct WakeupScheduler {
    /// Persistent backing store. Every mutation writes through so the schedule
    /// survives restart. `None` disables persistence (used by handlers that
    /// have no durable store, e.g. simulation).
    storage: Option<Storage>,
    /// Forward index ordered by fire time: `(fire_at, seq) -> id`. The first
    /// entry is always the next wakeup to fire.
    by_time: BTreeMap<(SystemTime, u64), WakeupId>,
    /// Reverse index `id -> entry` for O(1) cancel-by-tag: re-scheduling the
    /// same `(delegate, tag)` finds and removes the prior forward slot.
    entries: HashMap<WakeupId, Entry>,
    /// Pending-wakeup count per delegate, kept in sync with `entries` so the
    /// per-delegate cap is O(1) to check without scanning. A delegate drops out
    /// of the map when its last wakeup fires.
    per_delegate: HashMap<DelegateKey, usize>,
    next_seq: u64,
}

impl WakeupScheduler {
    /// An empty scheduler with no persistence (for handlers without a durable
    /// store).
    #[cfg(test)]
    pub(crate) fn in_memory() -> Self {
        Self {
            storage: None,
            by_time: BTreeMap::new(),
            entries: HashMap::new(),
            per_delegate: HashMap::new(),
            next_seq: 0,
        }
    }

    /// Build a scheduler backed by `storage`, rehydrating any wakeups persisted
    /// before a restart. Malformed rows are skipped (logged), never fatal.
    pub(crate) fn load(storage: Storage) -> Self {
        let mut scheduler = Self {
            storage: Some(storage.clone()),
            by_time: BTreeMap::new(),
            entries: HashMap::new(),
            per_delegate: HashMap::new(),
            next_seq: 0,
        };

        let rows = match storage.load_all_wakeups() {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "failed to load persisted delegate wakeups; starting empty");
                return scheduler;
            }
        };

        let mut loaded = 0usize;
        for (key_bytes, value_bytes) in rows {
            let (Some((delegate_key, tag)), Some((fire_at, params))) =
                (decode_key(&key_bytes), decode_value(&value_bytes))
            else {
                tracing::warn!("skipping malformed persisted wakeup row");
                continue;
            };
            // Insert directly into the in-memory indexes WITHOUT re-persisting
            // (the row is already on disk). Persisted rows are trusted past the
            // caps: they were accepted under the caps when first scheduled, and
            // dropping already-durable wakeups on load would be a surprising
            // data loss on restart.
            let seq = scheduler.next_seq;
            scheduler.next_seq += 1;
            let id = (delegate_key, tag);
            scheduler.by_time.insert((fire_at, seq), id.clone());
            *scheduler.per_delegate.entry(id.0.clone()).or_insert(0) += 1;
            scheduler.entries.insert(
                id,
                Entry {
                    fire_at,
                    seq,
                    params,
                },
            );
            loaded += 1;
        }
        if loaded > 0 {
            tracing::info!(count = loaded, "restored persisted delegate wakeups");
        }
        scheduler
    }

    /// Schedule (or reschedule) a wakeup for `delegate_key` at `fire_at`.
    ///
    /// Re-scheduling with the same `(delegate_key, tag)` replaces the prior
    /// pending wakeup (cancel-by-tag), both in memory and in the persistent row
    /// (the ReDb key is `(delegate, tag)`, so the write overwrites).
    ///
    /// Returns `false` and drops the request (delegates are untrusted WASM) if
    /// the tag exceeds [`MAX_TAG_LEN`] or accepting a NEW `(delegate, tag)` would
    /// exceed [`MAX_WAKEUPS_PER_DELEGATE`] or [`MAX_TOTAL_WAKEUPS`]. A reschedule
    /// of an already-pending tag is always accepted (it doesn't grow the set).
    pub(crate) fn schedule(
        &mut self,
        delegate_key: DelegateKey,
        tag: Vec<u8>,
        fire_at: SystemTime,
        params: Vec<u8>,
    ) -> bool {
        if tag.len() > MAX_TAG_LEN {
            tracing::warn!(
                delegate = %delegate_key,
                tag_len = tag.len(),
                max = MAX_TAG_LEN,
                "dropping ScheduleWakeup with oversized tag"
            );
            return false;
        }

        let id: WakeupId = (delegate_key, tag);
        let is_reschedule = self.entries.contains_key(&id);

        // Caps apply only to NEW (delegate, tag) pairs; a reschedule replaces an
        // existing entry and cannot grow the set.
        if !is_reschedule {
            if self.entries.len() >= MAX_TOTAL_WAKEUPS {
                tracing::warn!(
                    delegate = %id.0,
                    total = self.entries.len(),
                    "dropping ScheduleWakeup: global pending-wakeup cap reached"
                );
                return false;
            }
            let per = self.per_delegate.get(&id.0).copied().unwrap_or(0);
            if per >= MAX_WAKEUPS_PER_DELEGATE {
                tracing::warn!(
                    delegate = %id.0,
                    pending = per,
                    max = MAX_WAKEUPS_PER_DELEGATE,
                    "dropping ScheduleWakeup: per-delegate pending-wakeup cap reached"
                );
                return false;
            }
        }

        // Cancel-by-tag: drop the prior forward-index slot for this id, if any.
        // (The per-delegate count is unchanged on a reschedule.)
        if let Some(prev) = self.entries.get(&id) {
            self.by_time.remove(&(prev.fire_at, prev.seq));
        } else {
            *self.per_delegate.entry(id.0.clone()).or_insert(0) += 1;
        }

        let seq = self.next_seq;
        self.next_seq += 1;
        self.by_time.insert((fire_at, seq), id.clone());
        self.entries.insert(
            id.clone(),
            Entry {
                fire_at,
                seq,
                params: params.clone(),
            },
        );

        if let Some(storage) = &self.storage {
            let key = encode_key(&id.0, &id.1);
            let value = encode_value(fire_at, &params);
            if let Err(e) = storage.store_wakeup(&key, &value) {
                tracing::warn!(
                    delegate = %id.0,
                    error = %e,
                    "failed to persist scheduled wakeup; it will fire this session \
                     but may not survive a restart"
                );
            }
        }
        true
    }

    /// The fire time of the earliest pending wakeup, if any. Used to compute how
    /// long the event loop should sleep before the next fire.
    pub(crate) fn next_due(&self) -> Option<SystemTime> {
        self.by_time.keys().next().map(|(fire_at, _)| *fire_at)
    }

    /// Remove and return every wakeup due at or before `now`, up to `max` (to
    /// bound work per event-loop iteration). Each returned wakeup is deleted
    /// from both indexes and the persistent store, so it fires exactly once and
    /// is not re-delivered after a restart.
    pub(crate) fn take_due(&mut self, now: SystemTime, max: usize) -> Vec<DueWakeup> {
        let mut due = Vec::new();
        while due.len() < max {
            // Peek the earliest slot; stop if it isn't due yet.
            let Some((&slot, _)) = self.by_time.iter().next() else {
                break;
            };
            if slot.0 > now {
                break;
            }
            let id = self
                .by_time
                .remove(&slot)
                .expect("slot came from iter().next()");
            let params = self
                .entries
                .remove(&id)
                .map(|e| e.params)
                .unwrap_or_default();
            // Keep the per-delegate count in sync; drop the delegate's map entry
            // when its last pending wakeup fires so the map doesn't leak keys.
            if let Some(count) = self.per_delegate.get_mut(&id.0) {
                *count -= 1;
                if *count == 0 {
                    self.per_delegate.remove(&id.0);
                }
            }

            if let Some(storage) = &self.storage {
                let key = encode_key(&id.0, &id.1);
                if let Err(e) = storage.remove_wakeup(&key) {
                    tracing::warn!(
                        delegate = %id.0,
                        error = %e,
                        "failed to delete fired wakeup from store; it may re-fire after a restart"
                    );
                }
            }

            due.push(DueWakeup {
                delegate_key: id.0,
                tag: id.1,
                params,
            });
        }
        due
    }

    /// Number of pending wakeups.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        debug_assert_eq!(self.by_time.len(), self.entries.len());
        debug_assert_eq!(
            self.entries.len(),
            self.per_delegate.values().sum::<usize>(),
            "per_delegate counts must sum to the number of pending wakeups"
        );
        self.entries.len()
    }

    /// The scheduled fire time for a specific `(delegate, tag)`, if pending.
    #[cfg(test)]
    pub(crate) fn scheduled_at(
        &self,
        delegate_key: &DelegateKey,
        tag: &[u8],
    ) -> Option<SystemTime> {
        self.entries
            .get(&(delegate_key.clone(), tag.to_vec()))
            .map(|e| e.fire_at)
    }
}

/// `DelegateKey` (64 bytes) || tag.
fn encode_key(delegate_key: &DelegateKey, tag: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(DELEGATE_KEY_LEN + tag.len());
    key.extend_from_slice(delegate_key.as_ref());
    key.extend_from_slice(delegate_key.code_hash().as_ref());
    key.extend_from_slice(tag);
    key
}

fn decode_key(bytes: &[u8]) -> Option<(DelegateKey, Vec<u8>)> {
    if bytes.len() < DELEGATE_KEY_LEN {
        return None;
    }
    let key32: [u8; 32] = bytes[..32].try_into().ok()?;
    let code32: [u8; 32] = bytes[32..DELEGATE_KEY_LEN].try_into().ok()?;
    let tag = bytes[DELEGATE_KEY_LEN..].to_vec();
    Some((DelegateKey::new(key32, CodeHash::from(&code32)), tag))
}

/// seconds-since-epoch (u64 LE) || nanos (u32 LE) || params.
fn encode_value(fire_at: SystemTime, params: &[u8]) -> Vec<u8> {
    let since_epoch = fire_at.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let mut value = Vec::with_capacity(TIME_PREFIX_LEN + params.len());
    value.extend_from_slice(&since_epoch.as_secs().to_le_bytes());
    value.extend_from_slice(&since_epoch.subsec_nanos().to_le_bytes());
    value.extend_from_slice(params);
    value
}

fn decode_value(bytes: &[u8]) -> Option<(SystemTime, Vec<u8>)> {
    if bytes.len() < TIME_PREFIX_LEN {
        return None;
    }
    let secs = u64::from_le_bytes(bytes[..8].try_into().ok()?);
    let nanos = u32::from_le_bytes(bytes[8..TIME_PREFIX_LEN].try_into().ok()?);
    // A corrupt/torn row must be SKIPPED, never crash startup (see `load`'s
    // contract). `Duration::new` panics if `nanos >= 1e9` carries and overflows
    // `secs`, and `SystemTime + Duration` panics on overflow — so validate nanos
    // and use `checked_add` instead of the panicking `+`.
    if nanos >= 1_000_000_000 {
        return None;
    }
    let fire_at = UNIX_EPOCH.checked_add(Duration::new(secs, nanos))?;
    let params = bytes[TIME_PREFIX_LEN..].to_vec();
    Some((fire_at, params))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn dkey(seed: u8) -> DelegateKey {
        DelegateKey::new([seed; 32], CodeHash::new([seed.wrapping_add(1); 32]))
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn key_value_roundtrip() {
        let k = dkey(7);
        let encoded = encode_key(&k, b"weekly");
        let (dk, tag) = decode_key(&encoded).unwrap();
        assert_eq!(dk, k);
        assert_eq!(tag, b"weekly");

        let v = encode_value(at(1_700_000_000), b"params-bytes");
        let (fire_at, params) = decode_value(&v).unwrap();
        assert_eq!(fire_at, at(1_700_000_000));
        assert_eq!(params, b"params-bytes");
    }

    #[test]
    fn decode_rejects_truncated_rows() {
        assert!(decode_key(&[0u8; 10]).is_none());
        assert!(decode_value(&[0u8; 4]).is_none());
    }

    #[test]
    fn decode_value_rejects_overflow_instead_of_panicking() {
        // A corrupt row must be SKIPPED (None), never panic and crash startup.
        // nanos >= 1e9 (would panic Duration::new on carry-overflow):
        let mut row = Vec::new();
        row.extend_from_slice(&0u64.to_le_bytes()); // secs
        row.extend_from_slice(&u32::MAX.to_le_bytes()); // nanos ~4.29e9
        assert!(decode_value(&row).is_none());
        // secs = u64::MAX (would panic UNIX_EPOCH + duration):
        let mut row = Vec::new();
        row.extend_from_slice(&u64::MAX.to_le_bytes());
        row.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_value(&row).is_none());
    }

    #[test]
    fn oversized_tag_is_rejected() {
        let mut s = WakeupScheduler::in_memory();
        let k = dkey(6);
        let ok = s.schedule(k.clone(), vec![0u8; MAX_TAG_LEN], at(100), vec![]);
        assert!(ok, "a tag exactly at the limit is accepted");
        let rejected = s.schedule(k, vec![0u8; MAX_TAG_LEN + 1], at(100), vec![]);
        assert!(!rejected, "a tag over the limit is dropped");
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn per_delegate_cap_rejects_excess_but_allows_reschedule() {
        let mut s = WakeupScheduler::in_memory();
        let k = dkey(8);
        for i in 0..MAX_WAKEUPS_PER_DELEGATE {
            assert!(s.schedule(k.clone(), i.to_le_bytes().to_vec(), at(100), vec![]));
        }
        assert_eq!(s.len(), MAX_WAKEUPS_PER_DELEGATE);
        // A NEW tag beyond the cap is rejected...
        assert!(!s.schedule(k.clone(), b"one-too-many".to_vec(), at(100), vec![]));
        assert_eq!(s.len(), MAX_WAKEUPS_PER_DELEGATE);
        // ...but re-scheduling an EXISTING tag is still allowed (no growth).
        assert!(s.schedule(k.clone(), 0usize.to_le_bytes().to_vec(), at(200), vec![]));
        assert_eq!(s.len(), MAX_WAKEUPS_PER_DELEGATE);
        assert_eq!(s.scheduled_at(&k, &0usize.to_le_bytes()), Some(at(200)));

        // A different delegate has its own budget.
        let k2 = dkey(9);
        assert!(s.schedule(k2, b"fresh".to_vec(), at(100), vec![]));
        assert_eq!(s.len(), MAX_WAKEUPS_PER_DELEGATE + 1);
    }

    #[test]
    fn per_delegate_count_freed_when_wakeups_fire() {
        // Firing all of a delegate's wakeups must free its budget so it can
        // schedule again (the periodic-rearm pattern).
        let mut s = WakeupScheduler::in_memory();
        let k = dkey(10);
        for i in 0..MAX_WAKEUPS_PER_DELEGATE {
            assert!(s.schedule(k.clone(), i.to_le_bytes().to_vec(), at(100), vec![]));
        }
        assert!(!s.schedule(k.clone(), b"blocked".to_vec(), at(100), vec![]));
        // Fire them all.
        let fired = s.take_due(at(1_000), MAX_WAKEUPS_PER_DELEGATE);
        assert_eq!(fired.len(), MAX_WAKEUPS_PER_DELEGATE);
        assert_eq!(s.len(), 0);
        // Budget freed → can schedule again.
        assert!(s.schedule(k, b"after".to_vec(), at(100), vec![]));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn cancel_by_tag_replaces_prior_entry() {
        let mut s = WakeupScheduler::in_memory();
        let k = dkey(1);
        s.schedule(k.clone(), b"rotate".to_vec(), at(100), vec![]);
        s.schedule(k.clone(), b"rotate".to_vec(), at(200), vec![]);
        // Same (delegate, tag) → single entry, replaced with the later time.
        assert_eq!(s.len(), 1);
        assert_eq!(s.scheduled_at(&k, b"rotate"), Some(at(200)));
        assert_eq!(s.next_due(), Some(at(200)));
    }

    #[test]
    fn distinct_tags_fire_independently() {
        let mut s = WakeupScheduler::in_memory();
        let k = dkey(2);
        s.schedule(k.clone(), b"a".to_vec(), at(100), vec![1]);
        s.schedule(k.clone(), b"b".to_vec(), at(200), vec![2]);
        assert_eq!(s.len(), 2);
        assert_eq!(s.next_due(), Some(at(100)));

        // Only "a" is due at t=150.
        let due = s.take_due(at(150), 16);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].tag, b"a");
        assert_eq!(due[0].params, vec![1]);
        assert_eq!(s.len(), 1);

        // "b" fires later.
        let due = s.take_due(at(250), 16);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].tag, b"b");
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn take_due_respects_fire_time_and_max() {
        let mut s = WakeupScheduler::in_memory();
        let k = dkey(3);
        for i in 0..5u64 {
            s.schedule(k.clone(), vec![i as u8], at(100 + i), vec![]);
        }
        // Nothing due before the earliest.
        assert!(s.take_due(at(50), 16).is_empty());
        // max bounds the batch even when more are due.
        let batch = s.take_due(at(1_000), 3);
        assert_eq!(batch.len(), 3);
        assert_eq!(s.len(), 2);
        // Remaining drain on a second call.
        let batch = s.take_due(at(1_000), 16);
        assert_eq!(batch.len(), 2);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn take_due_returns_earliest_first() {
        let mut s = WakeupScheduler::in_memory();
        let k = dkey(4);
        s.schedule(k.clone(), b"late".to_vec(), at(300), vec![]);
        s.schedule(k.clone(), b"early".to_vec(), at(100), vec![]);
        s.schedule(k.clone(), b"mid".to_vec(), at(200), vec![]);
        let due = s.take_due(at(1_000), 16);
        let order: Vec<&[u8]> = due.iter().map(|d| d.tag.as_slice()).collect();
        assert_eq!(order, vec![b"early".as_slice(), b"mid", b"late"]);
    }

    // Persistence tests require a real ReDb-backed `Storage`.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn persists_across_reload() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path()).await.unwrap();
        let k = dkey(9);
        {
            let mut s = WakeupScheduler::load(storage.clone());
            s.schedule(
                k.clone(),
                b"rotate".to_vec(),
                at(1_700_000_000),
                b"my-params".to_vec(),
            );
            assert_eq!(s.len(), 1);
        }
        // A fresh scheduler over the same storage rehydrates the entry
        // (simulates a node restart).
        let s2 = WakeupScheduler::load(storage.clone());
        assert_eq!(s2.len(), 1);
        assert_eq!(s2.scheduled_at(&k, b"rotate"), Some(at(1_700_000_000)));

        // Firing it removes it from the store too, so it does not re-fire after
        // a subsequent restart.
        let mut s3 = WakeupScheduler::load(storage.clone());
        let due = s3.take_due(at(1_800_000_000), 16);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].params, b"my-params");

        let s4 = WakeupScheduler::load(storage);
        assert_eq!(s4.len(), 0);
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn cancel_by_tag_persists_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path()).await.unwrap();
        let k = dkey(5);
        {
            let mut s = WakeupScheduler::load(storage.clone());
            s.schedule(k.clone(), b"rotate".to_vec(), at(100), vec![]);
            s.schedule(k.clone(), b"rotate".to_vec(), at(200), vec![]);
        }
        // On reload the persisted row is the replacement, not both.
        let reloaded = WakeupScheduler::load(storage);
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded.scheduled_at(&k, b"rotate"), Some(at(200)));
    }

    // Many independent schedules must all be retained and persisted — the
    // "concurrent schedule from many delegates does not lose entries" case.
    // The scheduler is only ever touched from the single `contract_handling`
    // task, so this exercises volume rather than thread-level concurrency.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn many_schedules_all_persist() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(dir.path()).await.unwrap();
        let mut s = WakeupScheduler::load(storage.clone());
        for i in 0..200u32 {
            let k = dkey((i % 199) as u8);
            s.schedule(k, i.to_le_bytes().to_vec(), at(1000 + i as u64), vec![]);
        }
        assert_eq!(s.len(), 200);
        let reloaded = WakeupScheduler::load(storage);
        assert_eq!(reloaded.len(), 200);
    }
}
