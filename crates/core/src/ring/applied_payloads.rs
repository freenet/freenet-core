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
//! into the state the node holds now, with no time limit. The network receive
//! paths skip a payload whose hash is already present. There is no detector and
//! no verdict about the contract: the same rule applies to every contract.
//!
//! ## Precondition
//!
//! The skip is sound for contracts whose merge forms a semilattice
//! (commutative, associative, idempotent): merging a payload whose effect is
//! already in the state cannot change it, so skipping it changes nothing.
//!
//! For a merge that is order-dependent the skip can delay a repair. If the
//! repair arrives as bytes identical to a payload recorded earlier, it is
//! skipped until the record for that contract is cleared by eviction, a
//! restart, or 1024 newer payloads pushing it out. Anti-entropy does not
//! necessarily bring different bytes: a heal is computed from the sender's
//! state and this node's summary, so for a quiet contract every round can
//! carry the same bytes.
//!
//! For a merge that cycles among a fixed set of states, ending the replay is
//! the intended effect.
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
//!   unchanged: that may be a silent refusal (a contract that drops part of a
//!   payload under a condition that later clears, such as an entry whose
//!   prerequisite has not arrived, and returns its old state), and the same
//!   bytes must be able to reach the merge again once the condition clears.
//! - **Reset whenever the state is replaced by something other than a merge**:
//!   an initial install on a node that held no copy, a corrupted-state
//!   recovery that replaces the stored state, a re-store from a fetched copy,
//!   and eviction or reclamation of the hosted contract. Resets happen before
//!   the write where there is one. A restart resets everything because the
//!   set is in memory.
//!
//! Clearing is always safe (it only loses skips). Recording a payload whose
//! effect is not in the state is the one unsafe operation, which is why
//! records are made only at the executor's commit points.
//!
//! ## Bound
//!
//! Values are fixed-size `u64` hashes, so the per-contract count cap
//! ([`APPLIED_PAYLOADS_PER_CONTRACT`]) is a real memory bound: 1024 hashes in
//! a `VecDeque` plus the same hashes in a `HashSet` for O(1) lookup, about
//! 30 KiB per contract at the cap, a few percent of the ~1 MB resident cost of
//! a hosted contract that the hosting budget already accounts for. The number
//! of contracts with an entry is bounded by the hosted set: entries are
//! removed at eviction, and the ring reaper drops entries for contracts that
//! are neither hosted nor in use ([`AppliedPayloads::retain_contracts`]).
//!
//! The cap only has to hold the replayed set (a handful of payloads) across
//! the unrelated traffic that arrives between replays. When it truncates, the
//! oldest hash is forgotten and a later re-delivery of that payload is merged
//! again.
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

use std::collections::{HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher};

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};

/// Hashes kept per contract, newest last. See the module docs, "Bound".
pub(crate) const APPLIED_PAYLOADS_PER_CONTRACT: usize = 1024;

struct ContractEntry {
    /// Kept so the reaper can ask the hosting layer about the contract, which
    /// takes a full key.
    key: ContractKey,
    /// Insertion order, oldest first, for truncation at the cap.
    hashes: VecDeque<u64>,
    /// The same hashes, for O(1) lookup on every inbound payload. Kept in
    /// step with `hashes` by `record_absorbed`; `reset` drops both together.
    members: HashSet<u64>,
}

pub(crate) struct AppliedPayloads {
    entries: DashMap<ContractInstanceId, ContractEntry>,
    hasher: ahash::RandomState,
    cap_per_contract: usize,
}

impl AppliedPayloads {
    pub(crate) fn new() -> Self {
        Self::with_cap(APPLIED_PAYLOADS_PER_CONTRACT)
    }

    pub(crate) fn with_cap(cap_per_contract: usize) -> Self {
        Self {
            entries: DashMap::new(),
            hasher: ahash::RandomState::new(),
            cap_per_contract: cap_per_contract.max(1),
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

    /// Whether this payload was already merged into the current state lineage
    /// of `key`.
    pub(crate) fn contains(&self, key: &ContractKey, hash: u64) -> bool {
        self.entries
            .get(key.id())
            .is_some_and(|entry| entry.members.contains(&hash))
    }

    /// Record that a payload was merged into `key`'s current state. Call ONLY
    /// after the merge committed (changed or unchanged), never for a refused,
    /// failed, deferred, or suppressed merge.
    pub(crate) fn record_absorbed(&self, key: &ContractKey, hash: u64) {
        let mut entry = self
            .entries
            .entry(*key.id())
            .or_insert_with(|| ContractEntry {
                key: *key,
                hashes: VecDeque::new(),
                members: HashSet::new(),
            });
        let entry = &mut *entry;
        if entry.members.contains(&hash) {
            // Already present (for example the same payload from a local
            // client and later from the network): move it to the newest end so
            // a payload that keeps arriving is the last one truncated. The
            // linear scan runs only on this rare re-record, never on lookup.
            if let Some(pos) = entry.hashes.iter().position(|h| *h == hash) {
                entry.hashes.remove(pos);
            }
        } else {
            while entry.hashes.len() >= self.cap_per_contract {
                match entry.hashes.pop_front() {
                    Some(evicted) => {
                        entry.members.remove(&evicted);
                    }
                    None => break,
                }
            }
            entry.members.insert(hash);
        }
        entry.hashes.push_back(hash);
        debug_assert_eq!(entry.hashes.len(), entry.members.len());
    }

    /// Forget everything recorded for `key`. Called when its state was
    /// replaced by something other than a merge, or it stopped being hosted.
    pub(crate) fn reset(&self, key: &ContractKey) {
        self.entries.remove(key.id());
    }

    /// Drop entries for contracts `keep` rejects. Returns how many were
    /// dropped. Keys are collected first so `keep` never runs under a shard
    /// guard of this map.
    pub(crate) fn retain_contracts(&self, keep: impl Fn(&ContractKey) -> bool) -> usize {
        let keys: Vec<ContractKey> = self.entries.iter().map(|e| e.value().key).collect();
        let mut dropped = 0;
        for key in keys {
            if !keep(&key) && self.entries.remove(key.id()).is_some() {
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
    pub(crate) fn views_for(&self, key: &ContractKey) -> (Vec<u64>, HashSet<u64>) {
        self.entries.get(key.id()).map_or_else(
            || (Vec::new(), HashSet::new()),
            |e| (e.hashes.iter().copied().collect(), e.members.clone()),
        )
    }

    #[cfg(test)]
    pub(crate) fn contract_count(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn key(n: u8) -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([n; 32]), CodeHash::new([n; 32]))
    }

    #[test]
    fn a_recorded_payload_is_found_and_an_unrecorded_one_is_not() {
        let set = AppliedPayloads::new();
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
        let set = AppliedPayloads::new();
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
        let a = AppliedPayloads::new();
        let b = AppliedPayloads::new();
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
        let set = AppliedPayloads::new();
        let k = key(1);
        let a = set.payload_hash(true, b"d");
        set.record_absorbed(&k, a);
        set.reset(&k);
        assert!(!set.contains(&k, a));
        assert_eq!(set.contract_count(), 0);
    }

    #[test]
    fn the_cap_drops_the_oldest_and_keeps_the_newest() {
        let set = AppliedPayloads::with_cap(3);
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
        let set = AppliedPayloads::with_cap(3);
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
        let set = AppliedPayloads::new();
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
        let set = AppliedPayloads::with_cap(3);
        let k = key(1);
        let check = |label: &str| {
            let (queue, members) = set.views_for(&k);
            assert_eq!(queue.len(), members.len(), "{label}: lengths differ");
            assert!(
                queue.iter().all(|h| members.contains(h)),
                "{label}: queue holds a hash the lookup set lacks"
            );
        };
        let h: Vec<u64> = (0u8..5).map(|i| set.payload_hash(true, &[i])).collect();
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
        set.reset(&k);
        check("reset");
        assert!(!set.contains(&k, h[4]));
    }

    #[test]
    fn retain_drops_only_rejected_contracts() {
        let set = AppliedPayloads::new();
        let (k1, k2) = (key(1), key(2));
        set.record_absorbed(&k1, 1);
        set.record_absorbed(&k2, 2);
        let dropped = set.retain_contracts(|k| *k == k1);
        assert_eq!(dropped, 1);
        assert!(set.contains(&k1, 1));
        assert!(!set.contains(&k2, 2));
    }
}
