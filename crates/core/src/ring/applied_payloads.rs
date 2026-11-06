//! Per-contract record of the payloads this node has already merged into its
//! current state lineage.
//!
//! ## Why
//!
//! A contract supplies its own merge (`update_state`). A lawful merge is
//! idempotent for full states and for deltas, so merging a payload that is
//! already part of the node's current state is a no-op: skipping it changes
//! nothing. Contracts whose merge does not converge can instead replay a small
//! fixed set of payloads between nodes indefinitely, each replay changing the
//! state and so causing another fan-out. The 60 s [`BroadcastDedupCache`] only
//! remembers a payload for 60 s, so a replay with a longer period passes it.
//!
//! This set remembers, for every hosted contract, which payloads were merged
//! into the state the node holds now, for up to [`APPLIED_PAYLOAD_MAX_AGE`]
//! (15 minutes) after each was last recorded. The broadcast receive paths skip
//! a payload whose hash is present and unexpired. There is no detector and no
//! verdict about the contract: the same rule applies to every contract.
//!
//! ## Precondition
//!
//! The skip is sound only for contracts whose merge forms a semilattice
//! (commutative, associative, idempotent) AND that absorbed the whole payload
//! when it was recorded. Neither is guaranteed:
//!
//! - A merge that CHANGED the state is not proof the payload was fully
//!   absorbed. A contract can apply part of a payload and silently drop the
//!   rest under a condition that later clears (an entry whose prerequisite has
//!   not arrived yet). A full-state heal from a quiet peer then carries the
//!   identical bytes every round, for example when the delta path is
//!   unavailable (full state judged more efficient, a missing summary, a
//!   failed delta).
//! - A contract whose merge is not a semilattice can legitimately send earlier
//!   bytes again as a new update (A, B, A).
//!
//! In both cases the skip would stop a needed payload one hop out. The age
//! bound turns that into a bounded delay. An expired hash only stops causing
//! skips; nothing re-sends the bytes. So the delay is 15 minutes plus the time
//! until those bytes next arrive, which can be the next anti-entropy revisit
//! of the contract: each heartbeat advertises a rotating window of the shared
//! set, so a contract is revisited within `2 * ceil(shared / 64) - 1`
//! heartbeats. Anti-entropy is not relied on to bring different bytes,
//! because a heal is computed from the sender's state and this node's summary
//! and can repeat exactly.
//!
//! For a merge that cycles among a fixed set of states, ending the replay is
//! the intended effect. The age bound costs such a contract at most one extra
//! replay cycle per 15 minutes, against a replay period of roughly 60 s.
//!
//! ## Rules
//!
//! - **Check before the merge** on the UPDATE broadcast receive drivers, which
//!   also carry anti-entropy heals. NOT on the `ResyncResponse` arm: this node
//!   requested that response because the states differ, and a quiet sender
//!   re-sends the same bytes every round.
//! - **Record only after a merge committed a changed state**, plus an incoming
//!   full state byte-identical to the stored one. The executor does this in
//!   `bridged_upsert_contract_state_inner`, the only place that knows whether a
//!   merge committed, was refused, or was suppressed for a contract flagged
//!   broken. A refused, failed, deferred or suppressed merge is never recorded,
//!   so it can be retried. Neither is a merge that ran and left the state
//!   unchanged, which may be a silent refusal. A changing merge is recorded
//!   even though it may have dropped part of the payload (see Precondition);
//!   the age bound is what limits the cost of that.
//! - **Reset whenever the state is replaced by something other than a merge**:
//!   an initial install on a node that held no copy, a corrupted-state
//!   recovery that replaces the stored state, a store through
//!   `verify_and_store_contract`, and eviction or reclamation of the hosted
//!   contract. Resets happen before the write where there is one. A restart
//!   resets everything because the set is in memory.
//!
//! Clearing is always safe (it only loses skips). Recording a payload whose
//! effect is not fully in the state is the unsafe case, which is why records
//! are made only at the executor's commit points and expire.
//!
//! A skipped payload never reaches the merge, so a skipped delta does not feed
//! the per-sender merge-backoff reset (`merge_backoff.record_success_from_sender`)
//! or the delta-compatibility memo (`delta_incompat.record_delta_success`).
//! Both are signals about a merge that actually ran; a skip carries none.
//!
//! ## Bound
//!
//! Values are fixed-size, so the per-contract count cap
//! ([`APPLIED_PAYLOADS_PER_CONTRACT`]) is a real memory bound. At the cap of
//! 1024 a contract holds:
//!
//! - the `VecDeque<u64>`: capacity 1024 x 8 B = 8 KiB. `record_absorbed`
//!   prunes BEFORE it pushes, so the queue never reaches 1025 entries and
//!   never doubles its capacity (pinned by `the_queue_never_grows_past_the_cap`);
//! - the `HashMap<u64, Instant>`: hashbrown sizes 1024 entries at 2048
//!   buckets (it keeps the load at or below 7/8), each bucket a 24-byte
//!   `(u64, Instant)` pair (pinned by the same test) plus one control byte:
//!   2048 x 25 B + 16 B = 51,216 B, about 50 KiB.
//!
//! About 58 KiB per contract at the cap, roughly 6% of the ~1 MB resident cost
//! of a hosted contract that the hosting budget already accounts for. The
//! number of contracts with an entry is bounded by the hosted set: entries are
//! removed at eviction, and the ring reaper drops entries for contracts that
//! are neither hosted nor in use, and for contracts whose newest hash has
//! expired, so a quiet contract does not keep up to 1024 expired hashes
//! ([`AppliedPayloads::retain_contracts`]).
//!
//! The cap only has to hold the replayed set (a handful of payloads) across
//! the unrelated traffic that arrives between replays. When it truncates, the
//! oldest hash is forgotten and a later re-delivery of that payload is merged
//! again. Expired hashes are dropped from the front of the queue the next time
//! the contract records a payload.
//!
//! ## Hash
//!
//! 64-bit `ahash` over the is-delta discriminant and the payload bytes, the
//! same input layout as [`BroadcastDedupCache`]. The hasher is a per-process
//! randomly keyed `ahash::RandomState` (`runtime-rng` is enabled by default),
//! so a remote peer cannot choose payloads that collide with a given hash. A
//! collision would skip one payload until anti-entropy delivers a different
//! one. The key never leaves the process and the hash values never order
//! anything, so the random key does not affect simulation determinism.
//!
//! [`BroadcastDedupCache`]: crate::operations::update::BroadcastDedupCache

use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use tokio::time::Instant;

use crate::util::time_source::DynTimeSource;

/// Hashes kept per contract, newest last. See the module docs, "Bound".
pub(crate) const APPLIED_PAYLOADS_PER_CONTRACT: usize = 1024;

/// How long a recorded hash causes a skip, measured from when it was last
/// recorded. See the module docs, "Precondition": three anti-entropy heartbeats,
/// well above the roughly 60 s replay period of a looping contract.
pub(crate) const APPLIED_PAYLOAD_MAX_AGE: Duration = Duration::from_secs(15 * 60);

struct ContractEntry {
    /// Kept so the reaper can ask the hosting layer about the contract, which
    /// takes a full key.
    key: ContractKey,
    /// Hashes in order of their last record, oldest first, for truncation at
    /// the cap and for dropping expired hashes from the front.
    hashes: VecDeque<u64>,
    /// The same hashes with the time each was last recorded, for O(1) lookup
    /// on every inbound payload. Kept in step with `hashes` by
    /// `record_absorbed`; `reset` drops both together.
    recorded_at: HashMap<u64, Instant>,
}

pub(crate) struct AppliedPayloads {
    entries: DashMap<ContractInstanceId, ContractEntry>,
    hasher: ahash::RandomState,
    cap_per_contract: usize,
    max_age: Duration,
    /// The node's injected clock: the same source the interest manager, and
    /// so the 60 s `BroadcastDedupCache`, read.
    time_source: DynTimeSource,
}

impl AppliedPayloads {
    pub(crate) fn new(time_source: DynTimeSource) -> Self {
        Self::with_limits(
            APPLIED_PAYLOADS_PER_CONTRACT,
            APPLIED_PAYLOAD_MAX_AGE,
            time_source,
        )
    }

    pub(crate) fn with_limits(
        cap_per_contract: usize,
        max_age: Duration,
        time_source: DynTimeSource,
    ) -> Self {
        Self {
            entries: DashMap::new(),
            hasher: ahash::RandomState::new(),
            cap_per_contract: cap_per_contract.max(1),
            max_age,
            time_source,
        }
    }

    /// Hash of a payload as both the receive paths and the executor compute
    /// it. A full state and a delta with the same bytes hash differently.
    pub(crate) fn payload_hash(&self, is_delta: bool, bytes: &[u8]) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        hasher.write_u8(u8::from(is_delta));
        hasher.write(bytes);
        hasher.finish()
    }

    /// Whether this payload was recorded as merged into `key`'s current state
    /// within the age bound, so the receive path should skip it. An expired
    /// hash does not cause a skip.
    pub(crate) fn contains(&self, key: &ContractKey, hash: u64) -> bool {
        let now = self.time_source.now();
        self.entries.get(key.id()).is_some_and(|entry| {
            entry
                .recorded_at
                .get(&hash)
                .is_some_and(|at| now.saturating_duration_since(*at) < self.max_age)
        })
    }

    /// Record that a merge of this payload committed a changed state, or that
    /// it equalled the stored state byte for byte. Call ONLY at those commit
    /// points, never for a refused, failed, deferred, suppressed or unchanged
    /// merge. Re-recording a hash refreshes its age.
    pub(crate) fn record_absorbed(&self, key: &ContractKey, hash: u64) {
        let now = self.time_source.now();
        let mut entry = self
            .entries
            .entry(*key.id())
            .or_insert_with(|| ContractEntry {
                key: *key,
                hashes: VecDeque::new(),
                recorded_at: HashMap::new(),
            });
        let entry = &mut *entry;
        if entry.recorded_at.contains_key(&hash) {
            // Already present: take it out of the queue so it moves to the
            // newest end below, matching its refreshed time. The linear scan
            // runs only on a re-record, never on lookup.
            if let Some(pos) = entry.hashes.iter().position(|h| *h == hash) {
                entry.hashes.remove(pos);
            }
            entry.recorded_at.remove(&hash);
        }
        // Prune BEFORE pushing, so neither the queue nor the map ever holds
        // more than `cap_per_contract` entries (and the queue never doubles its
        // capacity to fit a 1025th). Drop from the front: expired hashes (the
        // queue is ordered by record time, so they are all at the front) and
        // anything that would leave no room for this one.
        while let Some(front) = entry.hashes.front().copied() {
            let expired = entry
                .recorded_at
                .get(&front)
                .is_none_or(|at| now.saturating_duration_since(*at) >= self.max_age);
            if !expired && entry.hashes.len() < self.cap_per_contract {
                break;
            }
            entry.hashes.pop_front();
            entry.recorded_at.remove(&front);
        }
        entry.recorded_at.insert(hash, now);
        entry.hashes.push_back(hash);
        debug_assert_eq!(entry.hashes.len(), entry.recorded_at.len());
    }

    /// Forget everything recorded for `key`. Called when its state was
    /// replaced by something other than a merge, or it stopped being hosted.
    pub(crate) fn reset(&self, key: &ContractKey) {
        self.entries.remove(key.id());
    }

    /// Drop entries for contracts `keep` rejects, and for contracts whose
    /// NEWEST hash has expired (then every hash has, since the queue is
    /// ordered by record time), so a quiet contract does not keep up to 1024
    /// expired hashes. One check per contract. Returns how many were dropped.
    /// Keys are collected first so `keep` never runs under a shard guard of
    /// this map.
    pub(crate) fn retain_contracts(&self, keep: impl Fn(&ContractKey) -> bool) -> usize {
        let now = self.time_source.now();
        let candidates: Vec<(ContractKey, bool)> = self
            .entries
            .iter()
            .map(|e| {
                let entry = e.value();
                let all_expired = entry
                    .hashes
                    .back()
                    .and_then(|newest| entry.recorded_at.get(newest))
                    .is_none_or(|at| now.saturating_duration_since(*at) >= self.max_age);
                (entry.key, all_expired)
            })
            .collect();
        let mut dropped = 0;
        for (key, all_expired) in candidates {
            if (all_expired || !keep(&key)) && self.entries.remove(key.id()).is_some() {
                dropped += 1;
            }
        }
        dropped
    }

    #[cfg(test)]
    pub(crate) fn len_for(&self, key: &ContractKey) -> usize {
        self.entries.get(key.id()).map_or(0, |e| e.hashes.len())
    }

    /// Both views of one contract's record, for tests that check they agree.
    #[cfg(test)]
    pub(crate) fn views_for(
        &self,
        key: &ContractKey,
    ) -> (Vec<u64>, std::collections::HashSet<u64>) {
        self.entries.get(key.id()).map_or_else(
            || (Vec::new(), std::collections::HashSet::new()),
            |e| {
                (
                    e.hashes.iter().copied().collect(),
                    e.recorded_at.keys().copied().collect(),
                )
            },
        )
    }

    /// Allocated queue capacity for one contract, for the memory pin.
    #[cfg(test)]
    pub(crate) fn queue_capacity_for(&self, key: &ContractKey) -> usize {
        self.entries
            .get(key.id())
            .map_or(0, |e| e.hashes.capacity())
    }

    #[cfg(test)]
    pub(crate) fn contract_count(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn clock() -> SharedMockTimeSource {
        SharedMockTimeSource::new()
    }

    fn set_with(clock: &SharedMockTimeSource) -> AppliedPayloads {
        AppliedPayloads::new(std::sync::Arc::new(clock.clone()))
    }

    fn capped(cap: usize) -> AppliedPayloads {
        AppliedPayloads::with_limits(cap, APPLIED_PAYLOAD_MAX_AGE, std::sync::Arc::new(clock()))
    }

    fn key(n: u8) -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([n; 32]), CodeHash::new([n; 32]))
    }

    #[test]
    fn a_recorded_payload_is_found_and_an_unrecorded_one_is_not() {
        let set = set_with(&clock());
        let k = key(1);
        let a = set.payload_hash(false, b"state-a");
        let b = set.payload_hash(false, b"state-b");
        assert!(!set.contains(&k, a));
        set.record_absorbed(&k, a);
        assert!(set.contains(&k, a));
        assert!(!set.contains(&k, b));
        // Per contract: another contract has not absorbed it.
        assert!(!set.contains(&key(2), a));
    }

    #[test]
    fn delta_and_full_state_with_the_same_bytes_hash_differently() {
        let set = set_with(&clock());
        assert_ne!(
            set.payload_hash(true, b"same"),
            set.payload_hash(false, b"same")
        );
        assert_eq!(
            set.payload_hash(true, b"same"),
            set.payload_hash(true, b"same")
        );
    }

    #[test]
    fn two_instances_are_keyed_independently() {
        // Each RandomState::new() draws its own keys, so two sets disagree on
        // the hash of the same input with overwhelming probability. This is
        // what makes the hash unpredictable to a remote peer.
        let a = set_with(&clock());
        let b = set_with(&clock());
        let inputs: Vec<&[u8]> = vec![b"x", b"y", b"z", b"w"];
        assert!(
            inputs
                .iter()
                .any(|i| a.payload_hash(false, i) != b.payload_hash(false, i)),
            "two independently created sets produced identical hashes; the \
             hasher is not randomly keyed"
        );
    }

    #[test]
    fn reset_forgets_the_contract() {
        let set = set_with(&clock());
        let k = key(1);
        let a = set.payload_hash(true, b"d");
        set.record_absorbed(&k, a);
        set.reset(&k);
        assert!(!set.contains(&k, a));
        assert_eq!(set.contract_count(), 0);
    }

    #[test]
    fn the_cap_drops_the_oldest_and_keeps_the_newest() {
        let set = capped(3);
        let k = key(1);
        let hashes: Vec<u64> = (0u8..5).map(|i| set.payload_hash(true, &[i])).collect();
        for h in &hashes {
            set.record_absorbed(&k, *h);
        }
        assert_eq!(set.len_for(&k), 3);
        assert!(!set.contains(&k, hashes[0]));
        assert!(!set.contains(&k, hashes[1]));
        for h in &hashes[2..] {
            assert!(set.contains(&k, *h));
        }
    }

    #[test]
    fn re_recording_moves_a_hash_to_the_newest_end_without_duplicating() {
        let set = capped(3);
        let k = key(1);
        let h: Vec<u64> = (0u8..4).map(|i| set.payload_hash(true, &[i])).collect();
        set.record_absorbed(&k, h[0]);
        set.record_absorbed(&k, h[1]);
        set.record_absorbed(&k, h[2]);
        set.record_absorbed(&k, h[0]); // refresh: now newest
        assert_eq!(set.len_for(&k), 3);
        set.record_absorbed(&k, h[3]); // evicts h[1], the oldest
        assert!(set.contains(&k, h[0]));
        assert!(!set.contains(&k, h[1]));
        assert!(set.contains(&k, h[2]));
        assert!(set.contains(&k, h[3]));
    }

    #[test]
    fn the_default_cap_is_enforced() {
        let set = set_with(&clock());
        let k = key(1);
        for i in 0..(APPLIED_PAYLOADS_PER_CONTRACT as u32 + 10) {
            set.record_absorbed(&k, set.payload_hash(true, &i.to_le_bytes()));
        }
        assert_eq!(set.len_for(&k), APPLIED_PAYLOADS_PER_CONTRACT);
    }

    /// The lookup set and the ordered queue must hold the same hashes after
    /// every kind of change: insert, re-record, eviction at the cap, reset.
    #[test]
    fn the_lookup_set_stays_in_step_with_the_queue() {
        let clock = clock();
        let set = AppliedPayloads::with_limits(
            3,
            APPLIED_PAYLOAD_MAX_AGE,
            std::sync::Arc::new(clock.clone()),
        );
        let k = key(1);
        let check = |label: &str| {
            let (queue, members) = set.views_for(&k);
            assert_eq!(queue.len(), members.len(), "{label}: lengths differ");
            assert!(
                queue.iter().all(|h| members.contains(h)),
                "{label}: queue holds a hash the lookup set lacks"
            );
        };
        let h: Vec<u64> = (0u8..7).map(|i| set.payload_hash(true, &[i])).collect();
        set.record_absorbed(&k, h[0]);
        set.record_absorbed(&k, h[1]);
        check("insert");
        set.record_absorbed(&k, h[0]);
        check("re-record");
        set.record_absorbed(&k, h[2]);
        set.record_absorbed(&k, h[3]);
        set.record_absorbed(&k, h[4]);
        check("evict");
        assert!(!set.contains(&k, h[1]), "an evicted hash must not be found");
        clock.advance_time(APPLIED_PAYLOAD_MAX_AGE);
        set.record_absorbed(&k, h[5]);
        check("expiry pruning");
        assert_eq!(
            set.views_for(&k).0,
            vec![h[5]],
            "the record after expiry prunes every expired hash from both views"
        );
        set.reset(&k);
        check("reset");
        assert!(!set.contains(&k, h[5]));
    }

    /// Memory pin for the figures in the module doc ("Bound"): the queue never
    /// holds more than the cap and so never doubles its capacity, and the map
    /// bucket is the 24-byte pair the doc's arithmetic assumes.
    #[test]
    fn the_queue_never_grows_past_the_cap() {
        let set = set_with(&clock());
        let k = key(1);
        for i in 0..(APPLIED_PAYLOADS_PER_CONTRACT as u32 * 3) {
            set.record_absorbed(&k, set.payload_hash(true, &i.to_le_bytes()));
            assert!(set.len_for(&k) <= APPLIED_PAYLOADS_PER_CONTRACT);
        }
        assert!(
            set.queue_capacity_for(&k) <= APPLIED_PAYLOADS_PER_CONTRACT,
            "queue capacity grew to {} past the cap",
            set.queue_capacity_for(&k)
        );
        assert_eq!(std::mem::size_of::<(u64, Instant)>(), 24);
    }

    /// The reaper drops a still-hosted contract's entry once its newest hash
    /// has expired, and keeps it while the newest is fresh.
    #[test]
    fn retain_drops_a_contract_whose_newest_hash_expired() {
        let clock = clock();
        let set = set_with(&clock);
        let (quiet, busy) = (key(1), key(2));
        set.record_absorbed(&quiet, 1);
        set.record_absorbed(&busy, 2);
        clock.advance_time(APPLIED_PAYLOAD_MAX_AGE - Duration::from_secs(1));
        set.record_absorbed(&busy, 3);
        assert_eq!(set.retain_contracts(|_| true), 0, "nothing expired yet");
        clock.advance_time(Duration::from_secs(1));
        assert_eq!(
            set.retain_contracts(|_| true),
            1,
            "a hosted contract whose newest hash expired is dropped"
        );
        assert_eq!(set.len_for(&quiet), 0);
        assert!(
            set.contains(&busy, 3),
            "a contract with a fresh hash is kept"
        );
    }

    #[test]
    fn a_hash_expires_at_the_age_bound() {
        let clock = clock();
        let set = set_with(&clock);
        let k = key(1);
        let h = set.payload_hash(false, b"s");
        set.record_absorbed(&k, h);
        clock.advance_time(APPLIED_PAYLOAD_MAX_AGE - Duration::from_secs(1));
        assert!(set.contains(&k, h), "skipped just before the bound");
        clock.advance_time(Duration::from_secs(1));
        assert!(!set.contains(&k, h), "not skipped at the bound");
    }

    /// Re-recording a hash restarts its age, and moves it so that the
    /// front-of-queue expiry pass does not drop it early.
    #[test]
    fn re_recording_refreshes_the_age() {
        let clock = clock();
        let set = set_with(&clock);
        let k = key(1);
        let (a, b) = (set.payload_hash(false, b"a"), set.payload_hash(false, b"b"));
        set.record_absorbed(&k, a);
        set.record_absorbed(&k, b);
        clock.advance_time(APPLIED_PAYLOAD_MAX_AGE - Duration::from_secs(60));
        set.record_absorbed(&k, a); // refresh a, not b
        clock.advance_time(Duration::from_secs(120));
        assert!(
            set.contains(&k, a),
            "a refreshed hash is still within its bound"
        );
        assert!(!set.contains(&k, b), "an unrefreshed hash has expired");
        // The next record drops the expired hash from storage too.
        set.record_absorbed(&k, set.payload_hash(false, b"c"));
        let (queue, _) = set.views_for(&k);
        assert!(!queue.contains(&b), "expired hashes are pruned on record");
        assert!(queue.contains(&a));
    }

    /// Source pin: the ring reaper loop calls `reap_applied_payloads` at
    /// statement position, so the backstop that bounds the map by the hosted
    /// set actually runs.
    #[test]
    fn the_reaper_loop_calls_reap_applied_payloads() {
        let src = include_str!("../ring.rs");
        let start = src
            .find("async fn governance_reaper_loop(")
            .expect("reaper loop exists");
        let body = &src[start..];
        let body = &body[..body.find("\n    }\n").expect("reaper loop end")];
        assert!(
            body.lines()
                .any(|l| l.trim() == "ring.reap_applied_payloads();"),
            "the reaper loop must call ring.reap_applied_payloads()"
        );
    }

    #[test]
    fn retain_drops_only_rejected_contracts() {
        let set = set_with(&clock());
        let (k1, k2) = (key(1), key(2));
        set.record_absorbed(&k1, 1);
        set.record_absorbed(&k2, 2);
        let dropped = set.retain_contracts(|k| *k == k1);
        assert_eq!(dropped, 1);
        assert!(set.contains(&k1, 1));
        assert!(!set.contains(&k2, 2));
    }
}
