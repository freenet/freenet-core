//! Choosing which contracts a peer watches, and when it moves on.
//!
//! Focus selection has one security requirement that shapes the whole design: **a
//! contract author must not be able to aim selection or dodge it.** An author picks
//! their own contract id, so any rule that is a pure function of the id (nearest to
//! the peer's ring location, lowest hash) is a rule they can grind ids against, until
//! their contract is never anyone's focus, or is *everyone's* focus and the checking
//! cost becomes an amplification vector.
//!
//! So every draw is keyed on a per-peer secret salt: scores are
//! `blake3(domain || salt || epoch || contract_id)`. Without a peer's salt an author
//! can neither predict whether that peer is watching them nor grind an id that
//! changes the answer, and because every peer has a different salt the network covers
//! a broad spread of contracts while each peer watches almost none. Rotation is an
//! epoch bump, which reshuffles every score at once.
//!
//! # Two slots: one uniform, one weighted by activity (#5913, #5914)
//!
//! A purely uniform draw judged nothing on a production-shape peer for twelve hours:
//! most hosted contracts receive no merge in a given focus period, and a focused
//! contract is only sampled from merges it receives while focused. A purely weighted
//! draw has the opposite failure: busy decoys, or anyone pumping updates into some
//! contract, crowd a quiet contract's chance down by up to the weight cap. So the
//! focus set is split ([`FocusSelector::select_split`]):
//!
//! - **The uniform slot** is the lowest salted score among the candidates. Activity
//!   plays no part, so every hosted contract is explored at about one in N per
//!   period whatever happens to activity anywhere. This is the RFC's "retain some
//!   random exploration even if activity weighting is later added", made absolute
//!   rather than a floor that pumping can dilute.
//! - **The weighted slot** is a salted draw weighted by how many state-changing
//!   merges this peer recently observed for each contract ([`super::activity`]),
//!   read at each probe tick so a contract that starts merging mid-period is seen at
//!   the next tick. Weighted sampling without replacement (Efraimidis-Spirakis: each
//!   candidate's key is `-ln(u) / w` for a salted uniform `u`, lowest keys win), on a
//!   separately domained score so it is independent of the uniform slot.
//!
//! Activity is a bounded lever, not a pure function of the id, and it is local to
//! each peer. What an actor CAN do is send real, state-changing updates to raise a
//! contract's weight in the weighted slot on the peers hosting it. That buys at most
//! [`ACTIVITY_WEIGHT_CAP`] (a contract with no activity keeps
//! [`COLD_FOCUS_WEIGHT`]), costs the actor their own traffic, draws scrutiny onto the
//! contract they pump, and cannot touch the uniform slot. Per-peer cost is unchanged:
//! focus is still at most `max_focus` contracts.

use std::collections::HashSet;

use freenet_stdlib::prelude::ContractInstanceId;
use serde::{Deserialize, Serialize};

/// How many contracts one peer watches at a time.
///
/// Small on purpose. Discovery is meant to be sparse: coverage comes from many peers
/// each doing a little, not from any peer doing a lot. Operational tunable.
pub const DEFAULT_MAX_FOCUS_CONTRACTS: usize = 2;

/// Weighted-slot weight of a contract with no recent merges.
///
/// A tenth of a contract with one merge (#5914 review round 2, K3). Exploring cold
/// contracts is the uniform slot's job; at weight 1 the weighted slot spent most of
/// its draws on cold contracts whenever a few busy ones sat among a hundred quiet
/// ones. Non-zero so the draw stays well defined and a hosted set with no activity
/// at all still fills the slot (uniformly).
pub const COLD_FOCUS_WEIGHT: f64 = 0.1;

/// Recent merges beyond this count add no further weight.
///
/// The busiest contract is then at most about 57 times as likely to win the weighted
/// slot as a contract with one merge (`63.1 / 1.1`), and about 630 times a cold one.
/// What the cap bounds is the advantage of flooding: past it, more updates buy
/// nothing, so a pumped decoy competes with an active target as an equal. It does not
/// stop one capped contract among many quiet ones from winning the weighted slot most
/// of the time on the peers hosting it; the uniform slot is unaffected either way.
/// Counts halve once per focus period, so a contract settles at the cap from about
/// 32 merges per period. Operational tunable.
pub const ACTIVITY_WEIGHT_CAP: u32 = 63;

/// The selection weight for a contract with `activity` recent merges.
pub fn focus_weight(activity: u32) -> f64 {
    COLD_FOCUS_WEIGHT + f64::from(activity.min(ACTIVITY_WEIGHT_CAP))
}

/// Which draw put a contract in focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FocusSlot {
    /// A uniform salted draw over the candidates. Activity plays no part.
    Uniform,
    /// A draw weighted by current activity.
    Weighted,
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

    /// The `max_focus` candidates with the lowest salted scores: a uniform draw.
    ///
    /// Deterministic for a given `(salt, epoch, candidates)`. This is the rule the
    /// uniform focus slot uses (see [`select_split`](Self::select_split)).
    pub fn select(&self, candidates: &[ContractInstanceId]) -> Vec<ContractInstanceId> {
        let mut scored: Vec<([u8; 32], ContractInstanceId)> = candidates
            .iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|id| (self.score(id), *id))
            .collect();
        // Tie-break on the id so the result is total-ordered even in the
        // astronomically unlikely case of a score collision.
        scored.sort_by(|(sa, ia), (sb, ib)| {
            sa.cmp(sb).then_with(|| ia.as_bytes().cmp(ib.as_bytes()))
        });
        scored
            .into_iter()
            .take(self.max_focus)
            .map(|(_, id)| id)
            .collect()
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
                let score = self.weighted_score(id);
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

    /// The focus set: one uniform slot, the rest weighted by current activity.
    ///
    /// - The **uniform slot** is the lowest salted score among the candidates, exactly
    ///   [`select`](Self::select)'s first pick. Activity plays no part, so every
    ///   hosted contract is explored at about one in N per period whatever anyone
    ///   does to activity elsewhere.
    /// - The **weighted slots** (`max_focus - 1` of them) are drawn by
    ///   [`select_weighted`](Self::select_weighted)'s rule from the remaining
    ///   candidates, using whatever `activity` says now. The caller re-draws at each
    ///   probe tick, so a contract that starts bursting mid-period is seen at the next
    ///   tick rather than the next period.
    ///
    /// `kept` are current focus contracts the caller would rather not displace (in
    /// the shadow runner: those the sampler admitted something new for in the last
    /// interval). A kept UNIFORM member keeps its slot while it is a candidate, so a
    /// newly hosted contract with a lower score does not displace a sample being
    /// built. A kept WEIGHTED member keeps its slot only while no challenger's
    /// current key beats it (#5914 review round 2, K1): otherwise contracts that
    /// stay busy, decoys included, would hold the weighted slot for the rest of the
    /// period against any burst elsewhere. Since keys are fixed for the epoch, that
    /// is the weighted draw's current winner, which only changes when activity
    /// reorders it. With `max_focus == 1` there is only the uniform slot.
    ///
    /// Deterministic for a given `(salt, epoch, candidates, activity, kept)`,
    /// independent of candidate order and duplicates. Returned uniform slot first,
    /// then weighted slots in draw order.
    pub fn select_split(
        &self,
        candidates: &[ContractInstanceId],
        activity: impl Fn(&ContractInstanceId) -> u32,
        kept: &[(ContractInstanceId, FocusSlot)],
    ) -> Vec<(ContractInstanceId, FocusSlot)> {
        let pool: HashSet<ContractInstanceId> = candidates.iter().copied().collect();
        let uniform_slots = self.max_focus.min(1);
        let weighted_slots = self.max_focus - uniform_slots;
        let mut chosen: Vec<(ContractInstanceId, FocusSlot)> = Vec::with_capacity(self.max_focus);

        // The uniform slot: a kept uniform member if there is one and it is still a
        // candidate (lowest id first, so the caller's order is irrelevant), otherwise
        // the lowest score. Kept WEIGHTED members are deliberately not consulted:
        // they compete in the weighted draw below and keep the slot exactly when they
        // still win it.
        if uniform_slots > 0 {
            let kept_uniform = kept
                .iter()
                .filter(|(id, slot)| *slot == FocusSlot::Uniform && pool.contains(id))
                .map(|(id, _)| *id)
                .min_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            let uniform = kept_uniform.or_else(|| {
                let all: Vec<ContractInstanceId> = pool.iter().copied().collect();
                self.select(&all).first().copied()
            });
            if let Some(id) = uniform {
                chosen.push((id, FocusSlot::Uniform));
            }
        }
        if weighted_slots > 0 {
            let remaining: Vec<ContractInstanceId> = pool
                .iter()
                .filter(|id| !chosen.iter().any(|(c, _)| c == *id))
                .copied()
                .collect();
            let drawn = self.select_weighted(&remaining, activity);
            chosen.extend(
                drawn
                    .into_iter()
                    .take(weighted_slots)
                    .map(|id| (id, FocusSlot::Weighted)),
            );
        }
        chosen.sort_by_key(|(_, slot)| *slot);
        chosen
    }

    fn score(&self, id: &ContractInstanceId) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"freenet-conformance-focus-v1");
        hasher.update(&self.salt);
        hasher.update(&self.epoch.to_le_bytes());
        hasher.update(id.as_bytes());
        *hasher.finalize().as_bytes()
    }

    /// The score the weighted draw uses: same inputs, separate domain, so the
    /// weighted slot is independent of the uniform one rather than being the uniform
    /// draw's runner-up.
    fn weighted_score(&self, id: &ContractInstanceId) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"freenet-conformance-focus-weighted-v1");
        hasher.update(&self.salt);
        hasher.update(&self.epoch.to_le_bytes());
        hasher.update(id.as_bytes());
        *hasher.finalize().as_bytes()
    }
}

/// A candidate's sort key: `-ln(u) / weight`, lowest first.
///
/// `u` is uniform in `(0, 1]`, taken from the top 53 bits of the salted score (a
/// lower score gives a larger `u` and so a smaller key, so with equal weights the
/// order is the score order). `u` is never zero, so the logarithm is finite, and the
/// weight is at least [`COLD_FOCUS_WEIGHT`], so the key is never NaN.
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

    /// The #5913 regression, on the weighted draw alone: uniform focus over a
    /// mostly-cold hosted set almost never lands on a contract that is receiving
    /// merges, so a production-shape peer judged nothing for twelve hours.
    ///
    /// 140 hosted, 4 busy, 2 slots both weighted: uniform picks a busy contract in
    /// about 6% of periods; the weighted draw must do so in most of them. The
    /// production split (one uniform slot) is measured in
    /// `hit_rates_at_realistic_activity_match_the_design`.
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
    /// peer just by being quiet. The weighted draw keeps a small non-zero chance; the
    /// split's uniform slot keeps the full uniform one.
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
                    .select_weighted(&pool, |c| if *c == cold { 0 } else { 10 })
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
        // A uniform pair would pick it in 2/11 of periods; weighted must be well
        // below that (a quarter of it), or activity is not weighting anything.
        let rate = picked_cold as f64 / trials as f64;
        let uniform_pair_rate = 2.0 / 11.0;
        assert!(
            rate < uniform_pair_rate / 4.0,
            "a cold contract was picked at {rate:.3}, close to the uniform rate \
             {uniform_pair_rate:.3}, so activity is not weighting anything"
        );

        // The split's uniform slot ignores activity, so the same cold contract keeps
        // at least its 1/11 uniform chance there.
        let mut split_picked = 0usize;
        for salt in 0..=255u8 {
            let mut selector = FocusSelector::new([salt; 32], 2);
            for _ in 0..16 {
                let picks =
                    selector.select_split(&pool, |c| if *c == cold { 0 } else { 1_000 }, &[]);
                split_picked += usize::from(picks.iter().any(|(c, _)| *c == cold));
                selector.rotate();
            }
        }
        let split_rate = split_picked as f64 / trials as f64;
        assert!(
            split_rate > 1.0 / 11.0 - 0.03,
            "the split picked a cold contract at {split_rate:.3}, below its uniform \
             slot's 1/11, so activity is leaking into the uniform slot"
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

    // ------------------------------------------------- uniform + weighted split (D1)

    /// The lowest uniform score among `pool`, i.e. the pre-#5913 first pick.
    fn lowest_score(selector: &FocusSelector, pool: &[ContractInstanceId]) -> ContractInstanceId {
        *pool
            .iter()
            .min_by(|a, b| {
                selector
                    .score(a)
                    .cmp(&selector.score(b))
                    .then_with(|| a.as_bytes().cmp(b.as_bytes()))
            })
            .expect("non-empty pool")
    }

    /// One slot is a pure uniform salted draw: no amount of activity anywhere moves
    /// it (#5914 review D1). That is what guarantees exploration of quiet contracts at
    /// an absolute rate of about one in N per period, which a pumped decoy cannot
    /// dilute, and it is exactly the pre-#5913 first pick.
    #[test]
    fn the_uniform_slot_ignores_activity() {
        let pool = candidates(40);
        for salt in 0..64u8 {
            let mut selector = FocusSelector::new([salt; 32], 2);
            for _ in 0..4 {
                let quiet = selector.select_split(&pool, |_| 0, &[]);
                let pumped = selector.select_split(
                    &pool,
                    |c| {
                        if c.as_bytes()[0] % 2 == 0 {
                            u32::MAX
                        } else {
                            0
                        }
                    },
                    &[],
                );
                let uniform_of = |picks: &[(ContractInstanceId, FocusSlot)]| {
                    picks
                        .iter()
                        .filter(|(_, slot)| *slot == FocusSlot::Uniform)
                        .map(|(id, _)| *id)
                        .collect::<Vec<_>>()
                };
                let expected = vec![lowest_score(&selector, &pool)];
                assert_eq!(uniform_of(&quiet), expected, "salt {salt}");
                assert_eq!(
                    uniform_of(&pumped),
                    expected,
                    "activity moved the uniform slot"
                );
                assert_eq!(quiet.len(), 2);
                assert_eq!(
                    pumped
                        .iter()
                        .filter(|(_, s)| *s == FocusSlot::Weighted)
                        .count(),
                    1
                );
                selector.rotate();
            }
        }
    }

    /// A kept UNIFORM contract keeps its slot when a lower-scoring contract joins
    /// the candidates; a kept WEIGHTED contract keeps its slot only while it still
    /// wins the draw, so a challenger whose current key beats it takes the slot.
    #[test]
    fn kept_contracts_keep_the_uniform_slot_but_must_win_the_weighted_one() {
        let pool = candidates(40);
        let mut challenged = 0;
        for salt in 0..32u8 {
            let selector = FocusSelector::new([salt; 32], 2);
            let uniform = lowest_score(&selector, &pool[1..]);
            // pool[0] joins later; when it outscores the kept uniform member it must
            // not displace it.
            let picks = selector.select_split(&pool, |_| 0, &[(uniform, FocusSlot::Uniform)]);
            assert!(
                picks.contains(&(uniform, FocusSlot::Uniform)),
                "salt {salt}: a kept uniform member was displaced: {picks:?}"
            );

            // A cold weighted member against a busy challenger.
            let kept = pool
                .iter()
                .copied()
                .find(|c| *c != uniform && c.as_bytes()[0] < 20)
                .expect("a cold contract");
            let busy = |c: &ContractInstanceId| if c.as_bytes()[0] >= 20 { 1_000 } else { 0 };
            let picks = selector.select_split(
                &pool,
                busy,
                &[(uniform, FocusSlot::Uniform), (kept, FocusSlot::Weighted)],
            );
            let fresh = selector.select_split(&pool, busy, &[(uniform, FocusSlot::Uniform)]);
            assert_eq!(
                picks, fresh,
                "salt {salt}: a kept weighted member beat the draw it lost"
            );
            challenged += usize::from(!picks.contains(&(kept, FocusSlot::Weighted)));
        }
        assert!(
            challenged > 0,
            "no kept weighted member was ever challenged"
        );
    }

    /// A kept contract the peer no longer hosts is not kept.
    #[test]
    fn a_kept_contract_outside_the_candidates_is_dropped() {
        let pool = candidates(10);
        let gone = id(200);
        let picks = FocusSelector::new([5; 32], 2).select_split(
            &pool,
            |_| 0,
            &[(gone, FocusSlot::Weighted)],
        );
        assert!(picks.iter().all(|(c, _)| *c != gone));
        assert_eq!(picks.len(), 2);
    }

    /// Deterministic for (salt, epoch, candidates, activity, kept), whatever the
    /// candidate order or duplication.
    #[test]
    fn the_split_is_deterministic() {
        let selector = FocusSelector::new([9; 32], 2);
        let pool = candidates(60);
        let activity = |c: &ContractInstanceId| u32::from(c.as_bytes()[0] % 7);
        let first = selector.select_split(&pool, activity, &[]);
        let mut shuffled = pool.clone();
        shuffled.reverse();
        shuffled.extend(pool.iter().copied());
        assert_eq!(first, selector.select_split(&shuffled, activity, &[]));
    }

    // ------------------------------------------- rates at realistic activity (T1-T3)

    /// The fraction of `trials` (salt, epoch) draws in which `draw` hits `busy`.
    fn hit_rate(
        busy: &HashSet<ContractInstanceId>,
        draw: impl Fn(&FocusSelector) -> Vec<ContractInstanceId>,
    ) -> (f64, usize) {
        let mut hits = 0usize;
        let mut trials = 0usize;
        for salt in 0..=255u8 {
            let mut selector = FocusSelector::new([salt; 32], 2);
            for _ in 0..4 {
                trials += 1;
                if draw(&selector).iter().any(|c| busy.contains(c)) {
                    hits += 1;
                }
                selector.rotate();
            }
        }
        (hits as f64 / trials as f64, trials)
    }

    /// Hit rates of the uniform+weighted split at realistic activity (#5914 review
    /// T1): 143 hosted (the #5913 peer's high-water mark), 4 to 20 of them busy, at
    /// 1, 3 and 10 recent merges each.
    ///
    /// Each rate is asserted against the closed form for this design: the uniform
    /// slot hits with `b/N`; otherwise the weighted slot's first draw over the other
    /// `N - 1` hits with `b*w / (b*w + 0.1*(N-1-b))`, `w = 0.1 + count` (a cold
    /// contract weighs 0.1 in the weighted slot). The point of
    /// printing them is the conclusion the PR has to state: at one or three merges per
    /// period and a handful of busy contracts the hit rate stays low. If a deployed
    /// peer shows `active_candidates` near zero, the cause is traffic, not selection.
    #[test]
    fn hit_rates_at_realistic_activity_match_the_design() {
        const HOSTED: u8 = 143;
        let pool = candidates(HOSTED);
        let n = f64::from(HOSTED);
        for busy_count in [4usize, 10, 20] {
            let stride = usize::from(HOSTED) / busy_count;
            let busy: HashSet<_> = (0..busy_count).map(|i| pool[i * stride]).collect();
            let b = busy_count as f64;
            let (uniform_pair, _) = hit_rate(&busy, |s| s.select(&pool));
            for count in [1u32, 3, 10] {
                let activity = |c: &ContractInstanceId| if busy.contains(c) { count } else { 0 };
                let (split, trials) = hit_rate(&busy, |s| {
                    s.select_split(&pool, activity, &[])
                        .into_iter()
                        .map(|(c, _)| c)
                        .collect()
                });
                // Literal weights, not `focus_weight`, so a change to the production
                // weights cannot move the expectation with it (S5): a cold contract
                // weighs 0.1 in the weighted slot, a busy one 0.1 + count.
                let (cold_w, w) = (0.1, 0.1 + f64::from(count));
                let expected = b / n + (n - b) / n * (b * w / (b * w + cold_w * (n - 1.0 - b)));
                println!(
                    "busy={busy_count:>2}/{HOSTED} merges={count:>2}: split={split:.3} \
                     expected={expected:.3} uniform_pair={uniform_pair:.3} ({trials} draws)"
                );
                assert!(
                    (split - expected).abs() < 0.05,
                    "busy={busy_count} merges={count}: measured {split:.3}, design says \
                     {expected:.3}"
                );
                assert!(
                    split + 0.03 >= uniform_pair,
                    "busy={busy_count} merges={count}: the split ({split:.3}) does worse \
                     than the uniform pair it replaced ({uniform_pair:.3})"
                );
                if count == 10 {
                    assert!(
                        split - uniform_pair > 0.3,
                        "busy={busy_count} at 10 merges: the split ({split:.3}) gains less \
                         than 0.3 over the uniform pair ({uniform_pair:.3})"
                    );
                }
            }
        }
    }

    /// The cap, exercised through the draw itself (T2): a contract at the cap and
    /// one at `u32::MAX` win the weighted slot about equally often.
    #[test]
    fn activity_past_the_cap_wins_no_more_often_than_activity_at_it() {
        let at_cap = id(1);
        let flooded = id(2);
        let pool = [at_cap, flooded];
        let mut flooded_first = 0usize;
        let mut trials = 0usize;
        for salt in 0..=255u8 {
            let mut selector = FocusSelector::new([salt; 32], 1);
            for _ in 0..16 {
                trials += 1;
                let picked = selector.select_weighted(&pool, |c| {
                    if *c == flooded {
                        u32::MAX
                    } else {
                        ACTIVITY_WEIGHT_CAP
                    }
                });
                flooded_first += usize::from(picked == [flooded]);
                selector.rotate();
            }
        }
        let rate = flooded_first as f64 / trials as f64;
        assert!(
            (rate - 0.5).abs() < 0.04,
            "a flooded contract won {rate:.3} of {trials} draws against one at the cap; \
             activity past the cap is buying weight"
        );
    }

    /// The weighted draw is proportional to weight (T3): activity 4 (weight 4.1)
    /// wins a two-way draw against activity 1 (weight 1.1) about 4.1/5.2 of the time.
    #[test]
    fn the_weighted_draw_is_proportional_to_weight() {
        let cold = id(1);
        let warm = id(2);
        let pool = [cold, warm];
        let mut warm_first = 0usize;
        let mut trials = 0usize;
        for salt in 0..=255u8 {
            let mut selector = FocusSelector::new([salt; 32], 1);
            for _ in 0..16 {
                trials += 1;
                // Activity 4 against activity 1: weight 4.1 against 1.1.
                let picked = selector.select_weighted(&pool, |c| if *c == warm { 4 } else { 1 });
                warm_first += usize::from(picked == [warm]);
                selector.rotate();
            }
        }
        let rate = warm_first as f64 / trials as f64;
        let expected = 4.1 / (4.1 + 1.1);
        assert!(
            (rate - expected).abs() < 0.04,
            "weight 4.1 against 1.1 won {rate:.3} of {trials} draws; expected {expected:.3}"
        );
    }

    /// In the weighted slot a cold contract weighs a tenth of a contract with one
    /// merge (#5914 review round 2, K3). Exploration of cold contracts is the uniform
    /// slot's job now, so at weight 1 the weighted slot spent most of its draws on
    /// cold contracts whenever a few busy ones sat among a hundred quiet ones.
    #[test]
    fn a_cold_contract_weighs_a_tenth_of_one_merge_in_the_weighted_slot() {
        assert!(
            (focus_weight(0) - 0.1).abs() < 1e-12,
            "cold weight {}",
            focus_weight(0)
        );
        assert!((focus_weight(1) - 1.1).abs() < 1e-12);
        assert!(
            (focus_weight(ACTIVITY_WEIGHT_CAP) - (0.1 + f64::from(ACTIVITY_WEIGHT_CAP))).abs()
                < 1e-12
        );
    }

    /// Two kept uniform members cannot both hold the single uniform slot.
    #[test]
    fn only_one_kept_contract_holds_the_uniform_slot() {
        let pool = candidates(20);
        let picks = FocusSelector::new([3; 32], 2).select_split(
            &pool,
            |_| 0,
            &[(id(4), FocusSlot::Uniform), (id(5), FocusSlot::Uniform)],
        );
        assert_eq!(
            picks
                .iter()
                .filter(|(_, s)| *s == FocusSlot::Uniform)
                .count(),
            1
        );
        assert_eq!(picks.len(), 2);
    }

    /// The weight cap is 63 merges: 62 < 63 == 64 (a literal pin, since every other
    /// test reads the constant and would follow any change to it).
    #[test]
    fn the_weight_cap_is_sixty_three_merges() {
        assert!(focus_weight(62) < focus_weight(63));
        assert_eq!(focus_weight(63), focus_weight(64));
    }
}
