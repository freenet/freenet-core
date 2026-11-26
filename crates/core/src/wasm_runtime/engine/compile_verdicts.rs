//! Remembered refusals of WASM code, keyed by code hash.
//!
//! A module that was refused (outside the limits, or invalid) is refused again
//! for the same bytes without scanning it again. Only refusals are kept: each is
//! a property of the bytes, so it is permanent for them. A compile failure that
//! is not a property of the bytes (memory pressure, disk errors) is never kept.
//!
//! One cache is owned by each shared backend engine, so every executor of a pool
//! answers from the same entries and the memory bound does not multiply with the
//! pool size. Time comes from a [`DynTimeSource`], entries expire so a later
//! change to the limits is picked up, and a hit does not extend an entry's life.
//!
//! The cache is bounded in bytes, not only in entries: the code that builds a
//! [`CompileRefusal`] cuts its text to [`MAX_DIAGNOSTIC_BYTES`], so an entry is
//! at most `VERDICT_ENTRY_MAX_BYTES` and the cache at most
//! `VERDICT_CACHE_MAX_BYTES` (both checked by a test).

use std::time::Duration;

use dashmap::{DashMap, DashSet};
use tokio::time::Instant;

use super::compile_limits::CompileRefusal;
use crate::util::time_source::DynTimeSource;

/// How long a refusal is remembered.
pub(crate) const VERDICT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Most entries held. When full, the oldest `CAPACITY / EVICTION_DIVISOR` go.
pub(crate) const VERDICT_CAPACITY: usize = 4_096;
const EVICTION_DIVISOR: usize = 64;
/// Most bytes one entry holds: key, expiry, the refusal value, and the text
/// inside it.
#[cfg(test)]
pub(crate) const VERDICT_ENTRY_MAX_BYTES: usize = 32
    + std::mem::size_of::<Instant>()
    + std::mem::size_of::<CompileRefusal>()
    + super::compile_limits::MAX_DIAGNOSTIC_BYTES;
/// Most bytes the cache holds at its entry capacity.
#[cfg(test)]
pub(crate) const VERDICT_CACHE_MAX_BYTES: usize = VERDICT_CAPACITY * VERDICT_ENTRY_MAX_BYTES;

pub(crate) struct CompileVerdictCache {
    time: DynTimeSource,
    entries: DashMap<[u8; 32], (CompileRefusal, Instant)>,
    ttl: Duration,
    capacity: usize,
    /// Digest of the limits this cache's entries were recorded under; part of
    /// every key, so entries recorded under other limits are never found.
    limits: [u8; 32],
    /// Modules already reported as near a limit (see `first_near_limit_report`).
    reported: DashSet<[u8; 32]>,
}

/// Most code hashes remembered as already reported; the set is cleared when
/// full, which at worst repeats one log line per module.
const REPORTED_CAPACITY: usize = 4_096;

impl CompileVerdictCache {
    /// `engine_id` identifies the engine whose refusals are remembered (see
    /// `wasmtime_engine::engine_compat_id`).
    pub(crate) fn new(time: DynTimeSource, engine_id: u64) -> Self {
        Self::with_limits(time, VERDICT_TTL, VERDICT_CAPACITY, engine_id)
    }

    pub(crate) fn with_limits(
        time: DynTimeSource,
        ttl: Duration,
        capacity: usize,
        engine_id: u64,
    ) -> Self {
        Self {
            time,
            entries: DashMap::new(),
            ttl,
            capacity,
            limits: super::compile_limits::limits_fingerprint(engine_id),
            reported: DashSet::new(),
        }
    }

    /// The key under which `hash` is remembered: the code hash bound to the limits.
    fn key(&self, hash: &[u8; 32]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.limits);
        hasher.update(hash);
        *hasher.finalize().as_bytes()
    }

    /// True the first time it is called for `hash`: the caller logs a module
    /// that is near a limit once, not on every compile.
    pub(crate) fn first_near_limit_report(&self, hash: &[u8; 32]) -> bool {
        let key = self.key(hash);
        if self.reported.len() >= REPORTED_CAPACITY {
            self.reported.clear();
        }
        self.reported.insert(key)
    }

    /// The remembered refusal for `hash`, if one is held and not expired.
    pub(crate) fn get(&self, hash: &[u8; 32]) -> Option<CompileRefusal> {
        let hash = &self.key(hash);
        let now = self.time.now();
        let hit = self
            .entries
            .get(hash)
            .map(|e| (e.0.clone(), e.1))
            .filter(|(_, expires)| *expires > now);
        match hit {
            Some((refusal, _)) => Some(refusal),
            None => {
                // Drop an expired entry so it does not occupy capacity.
                self.entries
                    .remove_if(hash, |_, (_, expires)| *expires <= now);
                None
            }
        }
    }

    /// Remember `refusal` for `hash`. Does not extend an existing live entry.
    pub(crate) fn insert(&self, hash: [u8; 32], refusal: CompileRefusal) {
        let hash = self.key(&hash);
        let now = self.time.now();
        if self.entries.len() >= self.capacity {
            self.evict_oldest(now);
        }
        let expires = now + self.ttl;
        self.entries
            .entry(hash)
            .and_modify(|e| {
                if e.1 <= now {
                    *e = (refusal.clone(), expires);
                }
            })
            .or_insert((refusal, expires));
    }

    fn evict_oldest(&self, now: Instant) {
        self.entries.retain(|_, (_, expires)| *expires > now);
        if self.entries.len() < self.capacity {
            return;
        }
        let mut by_expiry: Vec<([u8; 32], Instant)> = self
            .entries
            .iter()
            .map(|e| (*e.key(), e.value().1))
            .collect();
        by_expiry.sort_by_key(|(_, expires)| *expires);
        let drop_count = (self.capacity / EVICTION_DIVISOR).max(1);
        for (key, _) in by_expiry.into_iter().take(drop_count) {
            self.entries.remove(&key);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use std::sync::Arc;

    fn refused() -> CompileRefusal {
        CompileRefusal::NestingTooDeep { limit: 1 }
    }

    const TEST_ENGINE_ID: u64 = 11;

    fn cache(time: &SharedMockTimeSource, ttl_secs: u64, capacity: usize) -> CompileVerdictCache {
        CompileVerdictCache::with_limits(
            Arc::new(time.clone()),
            Duration::from_secs(ttl_secs),
            capacity,
            TEST_ENGINE_ID,
        )
    }

    #[test]
    fn a_remembered_refusal_is_returned_until_it_expires() {
        let time = SharedMockTimeSource::new();
        let cache = cache(&time, 10, 16);
        let h = [1u8; 32];
        assert_eq!(cache.get(&h), None);
        cache.insert(h, refused());
        assert_eq!(cache.get(&h), Some(refused()));
        time.advance_time(Duration::from_secs(9));
        assert_eq!(
            cache.get(&h),
            Some(refused()),
            "a hit must not extend the entry"
        );
        time.advance_time(Duration::from_secs(2));
        assert_eq!(cache.get(&h), None, "the entry must expire after its ttl");
        assert_eq!(cache.len(), 0, "an expired entry must not keep its slot");
    }

    /// A refusal recorded under one set of limits is not found under another,
    /// so a release that raises a limit re-admits the module.
    #[test]
    fn a_refusal_recorded_under_other_limits_is_a_miss() {
        let time = SharedMockTimeSource::new();
        let mut cache = cache(&time, 1000, 16);
        let h = [3u8; 32];
        cache.insert(h, refused());
        assert_eq!(cache.get(&h), Some(refused()));
        let mut other = cache.limits;
        other[0] ^= 1;
        cache.limits = other;
        assert_eq!(cache.get(&h), None, "a refusal from version A hit under B");
        cache.limits = super::super::compile_limits::limits_fingerprint(TEST_ENGINE_ID);
        assert_eq!(cache.get(&h), Some(refused()));
    }

    /// A refusal recorded under one wasmtime (engine identity) is a miss under
    /// another, so a wasmtime bump never answers from the old compiler's verdict.
    #[test]
    fn a_refusal_recorded_under_another_engine_identity_is_a_miss() {
        let time = SharedMockTimeSource::new();
        let mut cache = cache(&time, 1000, 16);
        let h = [3u8; 32];
        cache.insert(h, refused());
        let before = cache.key(&h);
        cache.limits = super::super::compile_limits::limits_fingerprint(TEST_ENGINE_ID + 1);
        assert_ne!(
            cache.key(&h),
            before,
            "the engine identity is not in the key"
        );
        assert_eq!(cache.get(&h), None, "a refusal from engine A hit under B");
        cache.limits = super::super::compile_limits::limits_fingerprint(TEST_ENGINE_ID);
        assert_eq!(cache.get(&h), Some(refused()));
    }

    #[test]
    fn a_near_limit_report_is_made_once_per_code_hash() {
        let time = SharedMockTimeSource::new();
        let cache = cache(&time, 1000, 16);
        assert!(cache.first_near_limit_report(&[4u8; 32]));
        assert!(!cache.first_near_limit_report(&[4u8; 32]));
        assert!(cache.first_near_limit_report(&[5u8; 32]));
    }

    #[test]
    fn insert_over_a_live_entry_does_not_extend_it() {
        let time = SharedMockTimeSource::new();
        let cache = cache(&time, 10, 16);
        let h = [2u8; 32];
        cache.insert(h, refused());
        time.advance_time(Duration::from_secs(8));
        cache.insert(h, refused());
        time.advance_time(Duration::from_secs(3));
        assert_eq!(cache.get(&h), None);
    }

    #[test]
    fn a_full_cache_evicts_the_oldest_entries() {
        let time = SharedMockTimeSource::new();
        let cache = cache(&time, 1000, 64);
        for i in 0..64u8 {
            cache.insert([i; 32], refused());
            time.advance_time(Duration::from_secs(1));
        }
        assert_eq!(cache.len(), 64);
        cache.insert([200u8; 32], refused());
        assert_eq!(cache.len(), 64, "one entry out, one in");
        assert_eq!(
            cache.get(&[0u8; 32]),
            None,
            "the oldest entry is the one dropped"
        );
        assert_eq!(cache.get(&[200u8; 32]), Some(refused()));
    }

    /// The largest refusal the engine can build is within the per-entry bound, so
    /// the cache is within its byte bound at its entry capacity.
    #[test]
    fn the_largest_entry_fits_the_byte_bound() {
        let text = crate::wasm_runtime::engine::compile_limits::bounded_text(&"x".repeat(100_000));
        assert!(
            32 + std::mem::size_of::<Instant>()
                + std::mem::size_of::<CompileRefusal>()
                + text.len()
                <= VERDICT_ENTRY_MAX_BYTES,
        );
        const _: () = assert!(VERDICT_CACHE_MAX_BYTES < 4 * 1024 * 1024);
    }

    /// The cache never holds more than its entry capacity, however many
    /// distinct refused modules arrive, so the byte bound follows from the
    /// entry bound and the cut applied to every refusal's text.
    #[test]
    fn a_flood_of_distinct_refusals_stays_within_the_entry_capacity() {
        let time = SharedMockTimeSource::new();
        let cache = cache(&time, 1000, 128);
        for i in 0..1_000u32 {
            let mut h = [0u8; 32];
            h[..4].copy_from_slice(&i.to_le_bytes());
            cache.insert(h, refused());
            assert!(
                cache.len() <= 128,
                "{} entries after {i} inserts",
                cache.len()
            );
        }
    }
}
