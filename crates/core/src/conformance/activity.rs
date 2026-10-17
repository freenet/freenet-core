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
//! cost it already paid for capture and nothing more. Per observation the writer pays
//! the size checks and state comparison in `capture::counts_as_activity`, one BLAKE3
//! hash of the result state, and here a lookup in a [`RECENT_STATES`]-entry ring plus
//! a hash-map increment.
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
//! At most [`MAX_TRACKED_ACTIVITY`] contracts, each a fixed-size id, a `u32` count and
//! [`RECENT_STATES`] 16-byte digests, so the count cap is also the memory cap (roughly
//! 150 KiB at the default, map overhead included). Decay is applied by
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

/// A 128-bit BLAKE3 digest of a merge's result state, for spotting repeats.
pub(crate) type StateDigest = u128;

/// How many recent result states per contract are remembered for deduplication.
///
/// A contract toggling between a few states (A -> B -> A ...) changes state on every
/// merge but gives the sampler nothing new after the first of each, so those merges
/// must not count as activity (#5914 review round 2, K2). Four covers the common
/// toggles; a contract cycling through more than four states does gain weight, and
/// does give the sampler distinct states to keep.
///
/// This filters BENIGN repetition only. It is not adversarial protection: any
/// contract whose state carries a counter or timestamp produces a new digest on
/// every merge, so a sender wanting weight for a contract gets it either way. The
/// weight cap and the uniform slot are what bound that.
pub(crate) const RECENT_STATES: usize = 4;

#[derive(Debug, Clone)]
struct Entry {
    count: u32,
    /// A counted merge since the last decay. Decides whether a count of one survives.
    merged_this_period: bool,
    /// Any merge offered since the last decay, counted or deduplicated. Keeps the
    /// digest ring alive for a contract that is still re-offering recent states.
    touched_this_period: bool,
    recent: [StateDigest; RECENT_STATES],
    filled: u8,
    next: u8,
}

impl Entry {
    fn new(digest: StateDigest) -> Self {
        let mut recent = [0; RECENT_STATES];
        recent[0] = digest;
        Self {
            count: 1,
            merged_this_period: true,
            touched_this_period: true,
            recent,
            filled: 1,
            next: 1 % RECENT_STATES as u8,
        }
    }

    /// Remember `digest`; false if it was already among the recent states.
    fn remember(&mut self, digest: StateDigest) -> bool {
        if self.recent[..usize::from(self.filled)].contains(&digest) {
            return false;
        }
        self.recent[usize::from(self.next)] = digest;
        self.next = (self.next + 1) % RECENT_STATES as u8;
        self.filled = (self.filled + 1).min(RECENT_STATES as u8);
        true
    }
}

/// Recent merge counts per contract.
#[derive(Debug, Clone)]
pub(crate) struct MergeActivity {
    counts: HashMap<ContractInstanceId, Entry>,
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

    /// Count one merge for `contract` whose result state hashes to `digest`, unless
    /// that state was among the contract's last [`RECENT_STATES`]. Returns whether it
    /// counted.
    pub(crate) fn record(&mut self, contract: ContractInstanceId, digest: StateDigest) -> bool {
        if let Some(entry) = self.counts.get_mut(&contract) {
            entry.touched_this_period = true;
            if !entry.remember(digest) {
                return false;
            }
            entry.count = entry.count.saturating_add(1).min(STORED_ACTIVITY_CAP);
            entry.merged_this_period = true;
            return true;
        }
        if self.counts.len() >= self.cap {
            self.evict_least_active();
        }
        self.counts.insert(contract, Entry::new(digest));
        true
    }

    /// Halve every count, rounding up, and forget a contract once it has gone a whole
    /// period without a counted merge and is down to one, unless it is still being
    /// offered (deduplicated) recent states, in which case it is kept at zero so its
    /// digest ring survives (#5914 review round 3, A5).
    ///
    /// Called once per focus period, so the count is an exponentially-decayed merge
    /// total with a half-life of one period: a contract merging steadily at `r` per
    /// period settles near `2r`, and one that goes quiet is forgotten after about
    /// `log2(count) + 1` periods. Rounding up is what keeps a contract that merges
    /// about once per period weighted: integer halving turned its one merge into zero
    /// at every rotation, so it read as cold whenever its next merge arrived.
    pub(crate) fn decay(&mut self) {
        self.counts.retain(|_, entry| {
            let merged = std::mem::replace(&mut entry.merged_this_period, false);
            let touched = std::mem::replace(&mut entry.touched_this_period, false);
            if !merged && entry.count <= 1 {
                if touched {
                    // Still re-offering states it produced recently (a toggler):
                    // keep the entry, and so its digest ring, at zero weight.
                    // Forgetting it let the same states count again two periods
                    // later, forever. Bounded like every entry: the slot goes to
                    // eviction first, since a zero count is the least active.
                    entry.count = 0;
                    return true;
                }
                return false;
            }
            entry.count = entry.count.div_ceil(2);
            true
        });
    }

    /// Current activity for `contract`; zero if not tracked.
    pub(crate) fn count(&self, contract: &ContractInstanceId) -> u32 {
        self.counts.get(contract).map_or(0, |entry| entry.count)
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
            .map(|(id, entry)| (entry.count, tie(id), *id))
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

    /// A digest no other call returns, for tests that mean "a merge with a new state".
    fn fresh() -> StateDigest {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        u128::from(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    fn id(n: u16) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[..2].copy_from_slice(&n.to_be_bytes());
        ContractInstanceId::new(bytes)
    }

    #[test]
    fn record_counts_merges_per_contract() {
        let mut activity = MergeActivity::default();
        for _ in 0..5 {
            activity.record(id(1), fresh());
        }
        activity.record(id(2), fresh());
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
            activity.record(id(n), fresh());
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
                activity.record(id(busy), fresh());
            }
        }
        for cold in 1_000..2_000u16 {
            activity.record(id(cold), fresh());
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
                activity.record(id(incumbent), fresh());
            }
        }
        let newcomer = id(9_999);
        for _ in 0..50 {
            activity.record(newcomer, fresh());
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
            activity.record(id(1), fresh());
        }
        activity.record(id(2), fresh());

        activity.decay();
        assert_eq!(activity.count(&id(1)), 4);
        assert_eq!(
            activity.count(&id(2)),
            1,
            "a merge in the period just ended must survive into the next one"
        );

        activity.decay();
        assert_eq!(
            activity.count(&id(2)),
            0,
            "a one-off merge outlived a quiet period"
        );
        assert_eq!(activity.len(), 1, "a forgotten contract must free its slot");

        activity.decay();
        activity.decay();
        assert_eq!(
            activity.len(),
            0,
            "a contract that stopped merging was never forgotten"
        );
    }

    /// A contract that merges about once per focus period stays weighted (found
    /// measuring #5914 round 2's realistic rates). Integer halving turned its single
    /// merge into zero at every rotation, so it was never weighted when its next merge
    /// arrived: in the real-runner simulation, 143 hosted with 4 such contracts got a
    /// new sample in 5% of periods.
    #[test]
    fn a_contract_merging_once_per_period_stays_weighted() {
        let mut activity = MergeActivity::default();
        for period in 0..6 {
            activity.record(id(1), fresh());
            activity.decay();
            assert!(
                activity.count(&id(1)) >= 1,
                "period {period}: a once-per-period contract read as cold after rotation"
            );
        }
    }

    #[test]
    fn counts_saturate_rather_than_wrap() {
        let mut activity = MergeActivity::default();
        activity.counts.insert(
            id(1),
            Entry {
                count: u32::MAX,
                ..Entry::new(0)
            },
        );
        activity.record(id(1), fresh());
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
            activity.record(id(1), fresh());
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
            activity.record(id(n), fresh());
        }
        assert_eq!(activity.evicted(), 0);
        activity.record(id(60_000), fresh());
        let batch = MAX_TRACKED_ACTIVITY / EVICTION_BATCH_DIVISOR;
        assert_eq!(activity.evicted() as usize, batch);
        assert_eq!(activity.len(), MAX_TRACKED_ACTIVITY - batch + 1);
        // The next cap/64 - 1 newcomers fit without another scan.
        for n in 0..(batch - 1) as u16 {
            activity.record(id(61_000 + n), fresh());
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
                activity.record(id(n), fresh());
            }
            activity.record(id(9_000), fresh());
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

    /// A toggle (A -> B -> A -> B ...) changes state on every merge and gives the
    /// sampler nothing new after the first of each, so it counts twice, not forty
    /// times (#5914 review round 2, K2).
    #[test]
    fn a_contract_toggling_between_states_counts_each_state_once() {
        let mut activity = MergeActivity::default();
        let (a, b) = (fresh(), fresh());
        let counted: usize = (0..40)
            .map(|i| usize::from(activity.record(id(1), if i % 2 == 0 { a } else { b })))
            .sum();
        assert_eq!(counted, 2);
        assert_eq!(activity.count(&id(1)), 2);
    }

    /// The window is the last RECENT_STATES states: a state that has fallen out of it
    /// counts again, and distinct states always count.
    #[test]
    fn the_dedup_window_is_the_last_few_states() {
        let mut activity = MergeActivity::default();
        let states: Vec<_> = (0..=RECENT_STATES).map(|_| fresh()).collect();
        for state in &states {
            assert!(
                activity.record(id(1), *state),
                "a distinct state was not counted"
            );
        }
        assert!(
            !activity.record(id(1), states[RECENT_STATES]),
            "the latest state counted twice"
        );
        assert!(
            activity.record(id(1), states[0]),
            "a state older than the window was still deduplicated"
        );
    }

    /// A toggler does not regain weight when its count decays away (#5914 review
    /// round 3, A5). Forgetting the entry also forgot its digest ring, so the same two
    /// states counted again two periods later, forever.
    #[test]
    fn a_toggler_does_not_regain_weight_after_decay() {
        let mut activity = MergeActivity::default();
        let (a, b) = (fresh(), fresh());
        for period in 0..10 {
            for _ in 0..4 {
                activity.record(id(1), a);
                activity.record(id(1), b);
            }
            if period >= 2 {
                assert_eq!(
                    activity.count(&id(1)),
                    0,
                    "period {period}: a contract toggling between two known states \
                     regained weight"
                );
            }
            activity.decay();
        }
    }

    /// A retained zero-count entry that produces a genuinely new state counts again
    /// and survives the next decay.
    #[test]
    fn a_kept_zero_entry_counts_a_genuinely_new_state() {
        let mut activity = MergeActivity::default();
        let (a, b) = (fresh(), fresh());
        for _ in 0..3 {
            activity.record(id(1), a);
            activity.record(id(1), b);
            activity.decay();
        }
        assert!(
            activity.record(id(1), fresh()),
            "a new state was not counted"
        );
        activity.decay();
        assert_eq!(
            activity.count(&id(1)),
            1,
            "a counted merge did not survive decay"
        );
    }

    /// A quiet contract down to two halves to one rather than being forgotten.
    #[test]
    fn a_quiet_count_of_two_halves_rather_than_vanishing() {
        let mut activity = MergeActivity::default();
        activity.record(id(1), fresh());
        activity.record(id(1), fresh());
        activity.decay(); // merged: 2 -> 1
        activity.record(id(1), fresh());
        activity.record(id(1), fresh());
        activity.record(id(1), fresh()); // 4
        activity.decay(); // merged: 4 -> 2
        activity.decay(); // quiet, 2 -> 1
        assert_eq!(activity.count(&id(1)), 1);
    }

    /// The digest keeps its full width: a thousand distinct states all count.
    #[test]
    fn distinct_states_never_collide_in_the_ring() {
        let mut activity = MergeActivity::default();
        let mut counted = 0;
        for n in 0..1_000u32 {
            let observation = crate::conformance::capture::Observation {
                contract: id(1),
                code_hash: [0; 32],
                parameters: vec![],
                base_state: vec![0],
                incoming_state: None,
                delta: Some(vec![1]),
                result_state: n.to_le_bytes().to_vec(),
                related: vec![],
            };
            let digest =
                crate::conformance::capture::activity_digest(&observation, 1024).expect("counts");
            counted += usize::from(activity.record(id(1), digest));
        }
        assert_eq!(counted, 1_000);
    }

    /// A three-state cycle counts each state once (the ring holds at least three).
    #[test]
    fn a_three_state_cycle_counts_each_state_once() {
        let mut activity = MergeActivity::default();
        let states = [fresh(), fresh(), fresh()];
        let counted: usize = (0..30)
            .map(|i| usize::from(activity.record(id(1), states[i % 3])))
            .sum();
        assert_eq!(counted, 3);
    }

    /// A four-state cycle still counts each state once: the ring is four wide.
    #[test]
    fn a_four_state_cycle_counts_each_state_once() {
        let mut activity = MergeActivity::default();
        let states = [fresh(), fresh(), fresh(), fresh()];
        let counted: usize = (0..40)
            .map(|i| usize::from(activity.record(id(1), states[i % 4])))
            .sum();
        assert_eq!(counted, 4);
    }

    /// A five-state cycle outruns the four-entry ring and keeps counting, which is
    /// the documented window, not a bug.
    #[test]
    fn a_five_state_cycle_outruns_the_ring() {
        let mut activity = MergeActivity::default();
        let states = [fresh(), fresh(), fresh(), fresh(), fresh()];
        let counted: usize = (0..30)
            .map(|i| usize::from(activity.record(id(1), states[i % 5])))
            .sum();
        assert_eq!(counted, 30);
    }
}
