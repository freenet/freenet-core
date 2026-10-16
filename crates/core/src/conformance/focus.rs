//! Choosing which contracts a peer watches, and when it moves on.
//!
//! Focus selection has one security requirement that shapes the whole design: **it
//! must not be controllable by the contract being tested.** A contract author picks
//! their own contract id, so any selection rule that is a pure function of the id
//! (nearest to the peer's ring location, lowest hash, most active) is a rule the
//! author can aim or dodge — they could grind ids until their contract is never
//! anyone's focus, or until it is *everyone's* focus and the checking cost becomes
//! an amplification vector.
//!
//! So selection is keyed on a per-peer secret salt. Scores are
//! `blake3(salt || epoch || contract_id)`, and the peer watches the lowest few.
//! Without knowing a peer's salt an author cannot predict, or influence, whether
//! that peer is looking at them; and because every peer has a different salt, the
//! network as a whole covers a broad spread of contracts while each peer watches
//! almost none.
//!
//! Rotation is just an epoch bump, which reshuffles every score at once.
//!
//! # Weighting by recent activity (#5913)
//!
//! A uniform draw over the hosted set judged nothing on a production-shape peer for
//! twelve hours: most hosted contracts receive no merge in a given focus period, and
//! a focused contract is only sampled from merges it receives while focused. So the
//! draw is weighted by how many merges this peer recently observed for each contract
//! ([`super::activity`]). It remains a salted random draw: the salted score supplies
//! the randomness and the weight only skews it (weighted sampling without replacement,
//! Efraimidis-Spirakis: each candidate's key is `-ln(u) / w` for a salted uniform
//! `u`, and the lowest keys win). Two bounds keep the weight from becoming a lever:
//!
//! - **Floor.** A contract with no activity keeps weight [`COLD_FOCUS_WEIGHT`], so
//!   quiet contracts are still explored (the RFC: "retain some random exploration
//!   even if activity weighting is later added"). Staying quiet lowers a contract's
//!   chance, but a quiet contract also has no merges to check.
//! - **Cap.** Activity beyond [`ACTIVITY_WEIGHT_CAP`] adds nothing. Without it, one
//!   contract flooded with UPDATEs would become every hosting peer's focus, and
//!   pumping decoys would pull the fleet's attention off a target. With it, a decoy
//!   competes with an active target as an equal. Per-peer cost is unchanged in any
//!   case: focus is still at most `max_focus` contracts.
//!
//! Activity is not a pure function of the contract id, and it is local to each peer,
//! so the "grind an id" argument above does not apply to it. What an actor CAN do is
//! send real updates to a contract to raise its weight on the peers hosting it, which
//! buys at most the cap and spends their own traffic to draw scrutiny.

use std::collections::HashSet;

use freenet_stdlib::prelude::ContractInstanceId;
use serde::{Deserialize, Serialize};

/// How many contracts one peer watches at a time.
///
/// Small on purpose. Discovery is meant to be sparse: coverage comes from many peers
/// each doing a little, not from any peer doing a lot. Operational tunable.
pub const DEFAULT_MAX_FOCUS_CONTRACTS: usize = 2;

/// Selection weight of a contract with no recent merges.
///
/// Non-zero so cold contracts keep a chance; see the module docs.
pub const COLD_FOCUS_WEIGHT: f64 = 1.0;

/// Recent merges beyond this count add no further weight.
///
/// So the busiest contract is at most `1 + 63 = 64` times as likely to be drawn first
/// as a cold one. Large enough that a handful of active contracts dominate a hosted
/// set of a hundred cold ones (the #5913 shape); small enough that activity cannot
/// make any single contract a near-certain pick on every peer hosting it. Since
/// counts halve once per focus period, a contract settles at the cap from about 32
/// merges per period. Operational tunable.
pub const ACTIVITY_WEIGHT_CAP: u32 = 63;

/// The selection weight for a contract with `activity` recent merges.
pub fn focus_weight(activity: u32) -> f64 {
    COLD_FOCUS_WEIGHT + f64::from(activity.min(ACTIVITY_WEIGHT_CAP))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusSelector {
    /// Per-peer secret. Persisted so focus does not reshuffle on every restart,
    /// which would keep resetting samples that need hours to accumulate.
    salt: [u8; 32],
    max_focus: usize,
    epoch: u64,
}

impl FocusSelector {
    pub fn new(salt: [u8; 32], max_focus: usize) -> Self {
        Self {
            salt,
            max_focus,
            epoch: 0,
        }
    }

    /// Resume at a known epoch.
    ///
    /// Rotation only means anything if it accumulates. A peer that restarted back to
    /// epoch zero would re-select the same first focus set every time, so a contract
    /// author able to cause restarts could hold a peer's attention on whatever epoch
    /// zero happens to pick — or keep it away from themselves forever.
    pub fn resuming_at(salt: [u8; 32], max_focus: usize, epoch: u64) -> Self {
        Self {
            salt,
            max_focus: max_focus.max(1),
            epoch,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn max_focus(&self) -> usize {
        self.max_focus
    }

    /// Move to the next focus period. Every score changes, so the set is redrawn.
    pub fn rotate(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// The contracts this peer should watch right now, given what it hosts,
    /// drawn uniformly.
    ///
    /// Deterministic for a given `(salt, epoch, candidates)`, so a peer that
    /// restarts mid-period resumes watching the same contracts instead of
    /// abandoning a half-built sample. [`select_weighted`](Self::select_weighted) with
    /// no activity anywhere; the shadow runner uses that form, whose activity is not
    /// persisted (see `ShadowRunner::advance` for what that costs a restart).
    pub fn select(&self, candidates: &[ContractInstanceId]) -> Vec<ContractInstanceId> {
        self.select_weighted(candidates, |_| 0)
    }

    /// The contracts to watch, drawn with probability weighted by recent activity.
    ///
    /// `activity` gives each candidate's recent merge count on this peer; it is turned
    /// into a weight by [`focus_weight`]. Deterministic for a given `(salt, epoch,
    /// candidates, activity)`, independent of candidate order and duplicates. Each
    /// candidate's key depends only on its own score and weight, so adding or removing
    /// some other candidate never reshuffles the rest: it can only take a slot by
    /// outranking them.
    pub fn select_weighted(
        &self,
        candidates: &[ContractInstanceId],
        activity: impl Fn(&ContractInstanceId) -> u32,
    ) -> Vec<ContractInstanceId> {
        let mut keyed: Vec<(f64, [u8; 32], ContractInstanceId)> = candidates
            .iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|id| {
                let score = self.score(id);
                (draw_key(&score, focus_weight(activity(id))), score, *id)
            })
            .collect();
        // Ties fall back to the full score, then the id, so the order is total even in
        // the astronomically unlikely case of a collision.
        keyed.sort_by(|(ka, sa, ia), (kb, sb, ib)| {
            ka.total_cmp(kb)
                .then_with(|| sa.cmp(sb))
                .then_with(|| ia.as_bytes().cmp(ib.as_bytes()))
        });
        keyed
            .into_iter()
            .take(self.max_focus)
            .map(|(_, _, id)| id)
            .collect()
    }

    fn score(&self, id: &ContractInstanceId) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"freenet-conformance-focus-v1");
        hasher.update(&self.salt);
        hasher.update(&self.epoch.to_le_bytes());
        hasher.update(id.as_bytes());
        *hasher.finalize().as_bytes()
    }
}

/// A candidate's sort key: `-ln(u) / weight`, lowest first.
///
/// `u` is uniform in `(0, 1]`, taken from the top 53 bits of the salted score and
/// arranged so that a LOWER score gives a LARGER `u` and so a smaller key. With equal
/// weights the order is therefore the score order the uniform draw always used, and a
/// peer with no recorded activity watches exactly what it watched before #5913. `u` is
/// never zero, so the logarithm is finite, and the weight is at least
/// [`COLD_FOCUS_WEIGHT`], so the key is never NaN.
fn draw_key(score: &[u8; 32], weight: f64) -> f64 {
    const SCALE: u64 = 1 << 53;
    let mut top = [0u8; 8];
    top.copy_from_slice(&score[..8]);
    let bits = u64::from_be_bytes(top) >> 11;
    // SCALE - bits is in [1, 2^53], exactly representable as an f64.
    let u = (SCALE - bits) as f64 / SCALE as f64;
    -u.ln() / weight
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn candidates(n: u8) -> Vec<ContractInstanceId> {
        (0..n).map(id).collect()
    }

    /// A peer watches at most `max_focus` contracts, however many it hosts.
    ///
    /// This is the bound the whole design rests on: discovery is meant to be sparse,
    /// and a peer that quietly watched everything it hosted would turn a diagnostic
    /// into an amplification vector on the busiest peers.
    #[test]
    fn selection_is_bounded_by_max_focus() {
        let selector = FocusSelector::new([1; 32], 2);
        assert_eq!(selector.select(&candidates(50)).len(), 2);

        // And it cannot exceed what is actually available.
        assert_eq!(selector.select(&candidates(1)).len(), 1);
        assert!(selector.select(&[]).is_empty());
    }

    /// The same peer, same period, same hosted set: the same answer.
    ///
    /// Sampling a contract usefully takes hours, so a peer that redrew its focus on
    /// every call — or on every restart — would keep abandoning half-built corpora
    /// and never finish one.
    #[test]
    fn selection_is_stable_within_an_epoch() {
        let selector = FocusSelector::new([2; 32], 3);
        let pool = candidates(20);
        let first = selector.select(&pool);
        assert_eq!(first, selector.select(&pool));

        // Order of the candidate list must not matter either: a peer's hosted set is
        // not an ordered thing, and iteration order should not change what it
        // watches.
        let mut shuffled = pool.clone();
        shuffled.reverse();
        assert_eq!(first, selector.select(&shuffled));

        // Nor should duplicates, which a hosted-set enumeration could produce.
        let mut duplicated = pool.clone();
        duplicated.extend(pool.iter().copied());
        assert_eq!(first, selector.select(&duplicated));
    }

    /// Rotation redraws the set rather than merely appending to it.
    #[test]
    fn rotation_redraws_the_selection() {
        let mut selector = FocusSelector::new([3; 32], 2);
        let pool = candidates(40);
        let before = selector.select(&pool);

        let mut changed = false;
        // Any single rotation could coincidentally reselect the same pair, so allow
        // a few before concluding rotation does nothing.
        for _ in 0..8 {
            selector.rotate();
            if selector.select(&pool) != before {
                changed = true;
                break;
            }
        }
        assert!(
            changed,
            "rotating did not change the focus set in eight epochs, so rotation is \
             not reshuffling anything"
        );
        assert!(selector.epoch() > 0, "rotate must advance the epoch");
    }

    /// The security property: selection is not controllable by the contract author.
    ///
    /// An author picks their own contract id, so any rule that is a pure function of
    /// the id can be aimed or dodged — grind ids until no peer ever watches you, or
    /// until every peer does and the checking cost becomes the attack. Keying on a
    /// per-peer secret is what removes that lever, and this test is what says the
    /// salt is actually load-bearing rather than decorative.
    #[test]
    fn different_peers_watch_different_contracts() {
        let pool = candidates(60);
        let a = FocusSelector::new([0xAA; 32], 2).select(&pool);
        let b = FocusSelector::new([0xBB; 32], 2).select(&pool);
        let c = FocusSelector::new([0xCC; 32], 2).select(&pool);

        assert!(
            !(a == b && b == c),
            "three peers with different salts chose identical focus sets, so the \
             salt is not affecting selection and an author could predict it"
        );
    }

    /// Changing the salt changes the answer for the SAME contract, which is the
    /// same property from the contract's point of view.
    #[test]
    fn a_contract_cannot_tell_whether_it_is_watched_without_the_salt() {
        let target = id(7);
        // A small pool and many peers, so the expected count sits far from both
        // bounds: each peer draws 2 of 10, so a given contract is watched by roughly
        // a fifth of them. With 60 candidates and 40 peers the expectation is ~1.3
        // and a result of zero says nothing about the salt — the first version of
        // this test was underpowered rather than wrong, and read as a real failure.
        let pool = candidates(10);
        let peers = 100u8;

        let watched = (0..peers)
            .filter(|i| {
                FocusSelector::new([*i; 32], 2)
                    .select(&pool)
                    .contains(&target)
            })
            .count();

        // BOTH bounds matter, and the lower one is the bound that bites. `watched
        // < 40` alone is satisfied by `watched == 0`, which is precisely the
        // outcome an author wants: a contract nobody ever checks. Removing the salt
        // from scoring makes selection a pure function of the id, and this contract
        // then lands in the same bucket for every peer — all of them or none.
        // Asserting only the upper bound passed under exactly that mutation.
        assert!(
            watched > 0 && watched < peers as usize,
            "this contract was watched by {watched} of {peers} peers; being watched by \
             none or by all means selection is decided by the id, and an author \
             could grind for either"
        );
    }

    // ------------------------------------------------------------ activity weighting

    /// A hosted set shaped like the #5913 peer: mostly cold, a few contracts merging.
    fn mostly_cold(
        active: &[u8],
        total: u8,
    ) -> (Vec<ContractInstanceId>, HashSet<ContractInstanceId>) {
        let pool = candidates(total);
        let busy = active.iter().map(|n| id(*n)).collect();
        (pool, busy)
    }

    /// How many of `trials` (salt, epoch) draws picked at least one busy contract.
    fn draws_hitting_busy(
        pool: &[ContractInstanceId],
        busy: &HashSet<ContractInstanceId>,
        weighted: bool,
    ) -> usize {
        let mut hits = 0;
        for salt in 0..=255u8 {
            let mut selector = FocusSelector::new([salt; 32], 2);
            for _ in 0..2 {
                let picked = if weighted {
                    selector.select_weighted(pool, |c| if busy.contains(c) { 50 } else { 0 })
                } else {
                    selector.select(pool)
                };
                if picked.iter().any(|c| busy.contains(c)) {
                    hits += 1;
                }
                selector.rotate();
            }
        }
        hits
    }

    /// The #5913 regression: uniform focus over a mostly-cold hosted set almost never
    /// lands on a contract that is receiving merges, so a production-shape peer judged
    /// nothing for twelve hours. Weighting by recent merges must fix that.
    ///
    /// 140 hosted, 4 busy, 2 focus slots: uniform picks a busy contract in about 6% of
    /// periods; the weighted draw must do so in most of them.
    #[test]
    fn weighted_focus_finds_the_busy_contracts_uniform_focus_misses() {
        let (pool, busy) = mostly_cold(&[3, 50, 97, 131], 140);
        let trials = 512;

        let uniform = draws_hitting_busy(&pool, &busy, false);
        assert!(
            uniform * 100 < trials * 15,
            "uniform selection hit a busy contract in {uniform}/{trials} periods; the \
             baseline this test contrasts against no longer holds"
        );

        let weighted = draws_hitting_busy(&pool, &busy, true);
        assert!(
            weighted * 100 > trials * 60,
            "weighted selection hit a busy contract in only {weighted}/{trials} periods \
             (uniform: {uniform}); focus is still dominated by contracts that cannot \
             produce evidence"
        );
    }

    /// Weighting must not starve quiet contracts entirely: the RFC asks to "retain
    /// some random exploration", and a contract must not be able to hide from every
    /// peer just by being quiet.
    #[test]
    fn a_cold_contract_keeps_a_small_nonzero_chance() {
        let pool = candidates(11);
        let cold = id(0);
        let mut picked_cold = 0usize;
        let mut trials = 0usize;
        for salt in 0..=255u8 {
            let mut selector = FocusSelector::new([salt; 32], 2);
            for _ in 0..16 {
                trials += 1;
                if selector
                    .select_weighted(&pool, |c| if *c == cold { 0 } else { 1_000 })
                    .contains(&cold)
                {
                    picked_cold += 1;
                }
                selector.rotate();
            }
        }
        assert!(
            picked_cold > 0,
            "a cold contract among busy ones was never selected in {trials} periods, \
             so weighting has removed random exploration"
        );
        // Uniform would pick it in 2/11 of periods. Weighted must be far below that.
        assert!(
            picked_cold * 11 < trials * 2 / 4,
            "a cold contract was picked {picked_cold}/{trials} times, close to the \
             uniform rate, so activity is not weighting anything"
        );
    }

    /// Past a cap, more activity buys nothing.
    ///
    /// Without a cap one contract flooded with UPDATEs would be every hosting peer's
    /// focus, and an adversary could pump decoys to pull the fleet's attention off a
    /// target. Capped, a decoy competes with an active target as an equal, not as a
    /// thousand-fold favourite.
    #[test]
    fn activity_beyond_the_cap_buys_no_extra_weight() {
        assert_eq!(focus_weight(ACTIVITY_WEIGHT_CAP), focus_weight(u32::MAX));
        assert!(
            focus_weight(1) > focus_weight(0),
            "one merge must count for something"
        );
        assert!(
            focus_weight(0) > 0.0,
            "a cold contract must keep a non-zero weight"
        );
    }

    /// Same salt, epoch, candidates and activity: the same answer, whatever the
    /// order or duplication of the candidate list.
    #[test]
    fn weighted_selection_is_deterministic() {
        let selector = FocusSelector::new([9; 32], 2);
        let pool = candidates(60);
        let activity = |c: &ContractInstanceId| u32::from(c.as_bytes()[0] % 7);
        let first = selector.select_weighted(&pool, activity);
        assert_eq!(first.len(), 2);
        assert_eq!(first, selector.select_weighted(&pool, activity));

        let mut shuffled = pool.clone();
        shuffled.reverse();
        shuffled.extend(pool.iter().copied());
        assert_eq!(first, selector.select_weighted(&shuffled, activity));
    }

    /// With no activity anywhere, weighting reduces to the pre-#5913 draw: the
    /// lowest salted scores.
    ///
    /// A restarted peer has no activity yet and must behave exactly as before rather
    /// than in some third way. Compared against the old rule written out here, not
    /// against `select`, which now delegates to `select_weighted` and would make this
    /// a tautology.
    #[test]
    fn no_activity_selects_exactly_what_uniform_selection_did() {
        for salt in 0..64u8 {
            let mut selector = FocusSelector::new([salt; 32], 2);
            for _ in 0..4 {
                let pool = candidates(40);
                let mut by_score: Vec<_> = pool.iter().map(|c| (selector.score(c), *c)).collect();
                by_score.sort_by(|(sa, a), (sb, b)| {
                    sa.cmp(sb).then_with(|| a.as_bytes().cmp(b.as_bytes()))
                });
                let lowest: Vec<_> = by_score.iter().take(2).map(|(_, c)| *c).collect();
                assert_eq!(
                    selector.select_weighted(&pool, |_| 0),
                    lowest,
                    "salt {salt}, epoch {}: zero activity changed the selection",
                    selector.epoch()
                );
                selector.rotate();
            }
        }
    }

    /// Weighting stays keyed on the salt: peers still spread out across the busy set
    /// rather than all converging on the same "busiest" pair.
    #[test]
    fn peers_spread_across_equally_busy_contracts() {
        let pool = candidates(60);
        let busy = |c: &ContractInstanceId| if c.as_bytes()[0] < 20 { 40 } else { 0 };
        let distinct: HashSet<_> = (0..12u8)
            .map(|salt| FocusSelector::new([salt; 32], 2).select_weighted(&pool, busy))
            .collect();
        assert!(
            distinct.len() > 6,
            "12 peers produced only {} distinct focus sets; activity has made selection \
             a function of the contracts rather than of each peer's salt",
            distinct.len()
        );
    }

    /// Activity recorded for a contract that is not a candidate cannot select it.
    ///
    /// Focus still comes from the hosted set (#5366); activity only weights it.
    #[test]
    fn activity_cannot_select_a_contract_outside_the_candidates() {
        let selector = FocusSelector::new([4; 32], 2);
        let pool = candidates(10);
        let outsider = id(200);
        let picked = selector.select_weighted(&pool, |c| if *c == outsider { u32::MAX } else { 0 });
        assert!(!picked.contains(&outsider));
        assert_eq!(picked.len(), 2);
    }
}
