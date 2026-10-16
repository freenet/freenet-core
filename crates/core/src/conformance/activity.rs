//! How busy each contract has recently been on THIS peer, for focus weighting.
//!
//! Focus selection used to draw uniformly from the hosted set (#5366). On a
//! production-shape peer that judged nothing for twelve hours (#5913): most hosted
//! contracts receive no UPDATE in a given two-hour focus period, a focused contract
//! only gains samples from merges it receives while focused, and so the focused pair
//! almost always had nothing to check. This module counts merges per contract so the
//! weighted focus slot ([`super::focus::FocusSelector::select_split`]) can prefer
//! contracts that will actually produce evidence. Only state-changing merges the
//! sampler could admit are counted (`capture::counts_as_activity`).
//!
//! # Cost, and where it is paid
//!
//! Nothing here runs on the merge path. Counting happens in the capture writer task,
//! when an observation leaves the queue (`capture::run_writer`), so a merge pays the
//! cost it already paid for capture and nothing more. Recording is a hash-map increment.
//! Once the tracked set is full, an insertion evicts a batch of the least active
//! entries in one linear pass, so the scan runs at most once per
//! `cap / EVICTION_BATCH_DIVISOR` new contracts rather than on every one.
//!
//! # What is counted, and what is not
//!
//! Only observations that reach the writer. One that capture dropped (queue full, or
//! larger than the queue's byte budget) is not counted. That under-counts a contract
//! during a burst, which is harmless, and never counts a contract whose observations
//! are always dropped, which is correct: such a contract cannot be sampled, so
//! focusing on it would judge nothing either.
//!
//! # Bounded
//!
//! At most [`MAX_TRACKED_ACTIVITY`] contracts, each a fixed-size id and a `u32`, so the
//! count cap is also the memory cap (roughly 60 KiB at the default). Decay is applied by
//! the caller once per focus period (`ShadowRunner::advance`), halving every count, so
//! a contract that stops merging drops out within a few periods.
//!
//! # Not persisted
//!
//! Activity is lost on restart, deliberately. It is a short-horizon estimate with a
//! half-life of one focus period, so a persisted copy would be mostly stale by the
//! time a restarted peer read it. A restarted peer's weighted slot draws uniformly
//! until it has observed state-changing merges, and weights by them from the next
//! draw on; the uniform slot is unaffected either way. See `ShadowRunner::advance`.

use std::collections::HashMap;

use freenet_stdlib::prelude::ContractInstanceId;

/// How many contracts' activity a peer tracks at once.
///
/// Several times the largest hosted set seen on a live peer (143 on 2026-10-16), so
/// eviction is not expected in normal operation. When it does happen it evicts the
/// LEAST active entries, so the contracts focus weighting actually favours are the
/// ones kept; an evicted contract simply reads as cold, which is a weight, not an
/// exclusion.
pub(crate) const MAX_TRACKED_ACTIVITY: usize = 1024;

/// An insertion at the cap evicts `cap / EVICTION_BATCH_DIVISOR` entries at once.
///
/// One victim per insertion would mean a full scan of the map on every new contract
/// once the map stayed full. Same shape as the UPDATE limiter's fix (#4997).
const EVICTION_BATCH_DIVISOR: usize = 64;

/// The most activity a contract can bank, as a multiple of the weight cap.
///
/// Weight saturates at [`super::focus::ACTIVITY_WEIGHT_CAP`], so anything above it buys
/// nothing now and only delays decay later. Without a clamp, a burst of 10,000 merges
/// would hold maximum weight for seven periods (fourteen hours) after the contract
/// went quiet. At twice the cap it holds it for at most one period after the burst.
pub(crate) const STORED_ACTIVITY_CAP: u32 = 2 * super::focus::ACTIVITY_WEIGHT_CAP;

/// Recent merge counts per contract.
#[derive(Debug, Clone)]
pub(crate) struct MergeActivity {
    counts: HashMap<ContractInstanceId, u32>,
    cap: usize,
    /// Per-peer secret that orders ties at eviction, so which equally-quiet contract
    /// loses its record is not a function of the id an author chose.
    salt: [u8; 32],
    /// Entries evicted to make room since this tracker was created.
    ///
    /// Reported on the shadow tick so a peer that outgrew the cap is visible in a
    /// release build. Without it, an evicted busy contract would read as a quiet one
    /// and nothing would say why.
    evicted: u64,
}

#[cfg(test)]
impl Default for MergeActivity {
    fn default() -> Self {
        Self::with_cap(MAX_TRACKED_ACTIVITY)
    }
}

impl MergeActivity {
    /// A tracker holding at most `cap` contracts, breaking eviction ties with `salt`.
    pub(crate) fn new(cap: usize, salt: [u8; 32]) -> Self {
        Self {
            counts: HashMap::new(),
            cap: cap.max(1),
            salt,
            evicted: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_cap(cap: usize) -> Self {
        Self::new(cap, [0; 32])
    }

    /// Count one merge observed for `contract`.
    pub(crate) fn record(&mut self, contract: ContractInstanceId) {
        if let Some(count) = self.counts.get_mut(&contract) {
            *count = count.saturating_add(1).min(STORED_ACTIVITY_CAP);
            return;
        }
        if self.counts.len() >= self.cap {
            self.evict_least_active();
        }
        self.counts.insert(contract, 1);
    }

    /// Halve every count and forget contracts that reach zero.
    ///
    /// Called once per focus period, which makes the count an exponentially-decayed
    /// merge total with a half-life of one period: a contract that merges steadily at
    /// `r` per period settles near `2r`, and one that goes quiet is forgotten after
    /// `log2(count)` periods.
    pub(crate) fn decay(&mut self) {
        self.counts.retain(|_, count| {
            *count /= 2;
            *count > 0
        });
    }

    /// Current activity for `contract`; zero if not tracked.
    pub(crate) fn count(&self, contract: &ContractInstanceId) -> u32 {
        self.counts.get(contract).copied().unwrap_or(0)
    }

    pub(crate) fn len(&self) -> usize {
        self.counts.len()
    }

    pub(crate) fn evicted(&self) -> u64 {
        self.evicted
    }

    /// Remove the least active batch of entries.
    ///
    /// Ties are broken by a salted hash of the id, so the result does not depend on
    /// hash-map iteration order and is not a fixed function of the id either.
    fn evict_least_active(&mut self) {
        let batch = (self.cap / EVICTION_BATCH_DIVISOR)
            .max(1)
            .min(self.counts.len());
        let salt = self.salt;
        let tie = |id: &ContractInstanceId| -> [u8; 32] {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"freenet-conformance-activity-evict-v1");
            hasher.update(&salt);
            hasher.update(id.as_bytes());
            *hasher.finalize().as_bytes()
        };
        let mut ranked: Vec<(u32, [u8; 32], ContractInstanceId)> = self
            .counts
            .iter()
            .map(|(id, count)| (*count, tie(id), *id))
            .collect();
        let order = |a: &(u32, [u8; 32], ContractInstanceId),
                     b: &(u32, [u8; 32], ContractInstanceId)| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.as_bytes().cmp(b.2.as_bytes()))
        };
        if batch < ranked.len() {
            ranked.select_nth_unstable_by(batch, order);
        }
        for (_, _, id) in ranked.into_iter().take(batch) {
            // Counted from what was actually removed, not from the batch size.
            if self.counts.remove(&id).is_some() {
                self.evicted += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u16) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[..2].copy_from_slice(&n.to_be_bytes());
        ContractInstanceId::new(bytes)
    }

    #[test]
    fn record_counts_merges_per_contract() {
        let mut activity = MergeActivity::default();
        for _ in 0..5 {
            activity.record(id(1));
        }
        activity.record(id(2));
        assert_eq!(activity.count(&id(1)), 5);
        assert_eq!(activity.count(&id(2)), 1);
        assert_eq!(
            activity.count(&id(3)),
            0,
            "an unseen contract must read as cold"
        );
    }

    /// The tracked set never exceeds its cap, however many distinct contracts merge.
    ///
    /// This is the memory bound: entries are fixed-size, so the count cap is the byte
    /// cap. A peer that saw an unbounded stream of distinct contracts must not grow
    /// this map with it.
    #[test]
    fn the_tracked_set_is_bounded_by_its_cap() {
        let cap = 128;
        let mut activity = MergeActivity::with_cap(cap);
        for n in 0..5_000u16 {
            activity.record(id(n));
            assert!(
                activity.len() <= cap,
                "tracked {} contracts with a cap of {cap}",
                activity.len()
            );
        }
        assert!(
            activity.evicted() > 0,
            "the cap was exceeded many times over and no eviction was counted, so \
             saturation would be invisible on the tick"
        );
        assert_eq!(
            activity.evicted() as usize + activity.len(),
            5_000,
            "every contract recorded is either still tracked or counted as evicted"
        );
    }

    /// Eviction keeps the busy contracts, which are the ones selection favours.
    #[test]
    fn eviction_removes_the_least_active_first() {
        let cap = 64;
        let mut activity = MergeActivity::with_cap(cap);
        // Eight busy contracts, then far more one-off contracts than the cap.
        for busy in 0..8u16 {
            for _ in 0..100 {
                activity.record(id(busy));
            }
        }
        for cold in 1_000..2_000u16 {
            activity.record(id(cold));
        }
        for busy in 0..8u16 {
            assert_eq!(
                activity.count(&id(busy)),
                100,
                "busy contract {busy} was evicted to make room for one-off contracts"
            );
        }
    }

    /// A newcomer is admitted even when the map is full of busy incumbents.
    ///
    /// Refusing at the cap would let whichever contracts got in first hold every slot
    /// for as long as they kept merging (the #4981 starvation shape), and a contract
    /// that only became busy later would read as cold forever.
    #[test]
    fn a_newcomer_is_admitted_when_the_map_is_full() {
        let cap = 64;
        let mut activity = MergeActivity::with_cap(cap);
        for incumbent in 0..cap as u16 {
            for _ in 0..10 {
                activity.record(id(incumbent));
            }
        }
        let newcomer = id(9_999);
        for _ in 0..50 {
            activity.record(newcomer);
        }
        assert_eq!(
            activity.count(&newcomer),
            50,
            "a contract arriving at a full map was refused or kept being evicted"
        );
        assert!(activity.len() <= cap);
    }

    /// Decay halves counts and forgets a contract that has gone quiet.
    #[test]
    fn decay_halves_and_forgets_quiet_contracts() {
        let mut activity = MergeActivity::default();
        for _ in 0..8 {
            activity.record(id(1));
        }
        activity.record(id(2));

        activity.decay();
        assert_eq!(activity.count(&id(1)), 4);
        assert_eq!(
            activity.count(&id(2)),
            0,
            "a single merge should not survive a period"
        );
        assert_eq!(activity.len(), 1, "a zero count must free its slot");

        activity.decay();
        activity.decay();
        activity.decay();
        assert_eq!(
            activity.len(),
            0,
            "a contract that stopped merging was never forgotten"
        );
    }

    #[test]
    fn counts_saturate_rather_than_wrap() {
        let mut activity = MergeActivity::default();
        activity.counts.insert(id(1), u32::MAX);
        activity.record(id(1));
        assert_eq!(activity.count(&id(1)), STORED_ACTIVITY_CAP);
    }

    /// A burst must not bank hours of maximum weight (#5914 review D3).
    ///
    /// Counts halve once per period, so an unclamped burst of 10,000 merges would
    /// stay above the weight cap for seven periods (fourteen hours) after the
    /// contract went quiet. Clamped at twice the weight cap, it stays at the cap for
    /// at most one period after the burst and then decays like anything else.
    #[test]
    fn a_burst_cannot_bank_more_than_the_stored_cap() {
        let mut activity = MergeActivity::default();
        for _ in 0..10_000 {
            activity.record(id(1));
        }
        assert_eq!(
            activity.count(&id(1)),
            STORED_ACTIVITY_CAP,
            "a burst banked more activity than the stored cap"
        );
        activity.decay();
        activity.decay();
        assert!(
            activity.count(&id(1)) < crate::conformance::focus::ACTIVITY_WEIGHT_CAP,
            "two quiet periods after a burst the contract still holds maximum weight"
        );
    }

    /// Batching at the production cap: one insertion past it evicts cap/64 entries,
    /// not one, so a persistently full map does not scan on every newcomer.
    #[test]
    fn eviction_batches_at_the_production_cap() {
        let mut activity = MergeActivity::new(MAX_TRACKED_ACTIVITY, [1; 32]);
        for n in 0..MAX_TRACKED_ACTIVITY as u16 {
            activity.record(id(n));
        }
        assert_eq!(activity.evicted(), 0);
        activity.record(id(60_000));
        let batch = MAX_TRACKED_ACTIVITY / EVICTION_BATCH_DIVISOR;
        assert_eq!(activity.evicted() as usize, batch);
        assert_eq!(activity.len(), MAX_TRACKED_ACTIVITY - batch + 1);
        // The next cap/64 - 1 newcomers fit without another scan.
        for n in 0..(batch - 1) as u16 {
            activity.record(id(61_000 + n));
        }
        assert_eq!(
            activity.evicted() as usize,
            batch,
            "evicted again before the batch was used up"
        );
    }

    /// Ties among equally quiet contracts are broken by a per-peer salt, not by id.
    ///
    /// Lowest-id-first is a systematic bias an author can aim at: grind an id that
    /// sorts high and your activity record is the last one evicted on every peer.
    #[test]
    fn eviction_ties_are_salted_not_lowest_id_first() {
        let cap = 64;
        let survivors_for = |salt: u8| {
            let mut activity = MergeActivity::new(cap, [salt; 32]);
            for n in 0..cap as u16 {
                activity.record(id(n));
            }
            activity.record(id(9_000));
            (0..cap as u16)
                .filter(|n| activity.counts.contains_key(&id(*n)))
                .collect::<Vec<_>>()
        };
        let lowest_evicted = (1..cap as u16).collect::<Vec<_>>();
        let outcomes: std::collections::HashSet<_> = (0..8u8).map(survivors_for).collect();
        assert!(
            outcomes.len() > 1,
            "eight salts evicted the same tied contract, so the salt plays no part"
        );
        assert!(
            outcomes
                .iter()
                .any(|survivors| *survivors != lowest_evicted),
            "ties are always broken lowest-id first"
        );
    }
}
