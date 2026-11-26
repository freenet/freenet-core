//! TEST-ONLY next-hop policy switch for PUT and GET (experiment, not for merge).
//!
//! Compiled only under `cfg(any(test, feature = "testing"))`. The default is
//! [`HopPolicy::Today`], which leaves production selection untouched. The
//! switch is per thread because the deterministic simulation runs every node
//! on the test thread.

use std::cell::Cell;

use super::{Location, PeerKeyLocation};
use crate::node::network_status::OpType;

/// How a hop picks the candidate window handed to the router.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HopPolicy {
    /// T: today's behaviour (cost-ranked among the 25 closest).
    Today,
    /// G: always the geometrically closest connected peer.
    G,
    /// K2: cost-ranked among the 2 closest.
    K2,
    /// K3: cost-ranked among the 3 closest.
    K3,
    /// P1: cost-ranked among peers strictly closer to the key than this node;
    /// if none, today's choice.
    P1,
    /// P1+G: P1 on the way; at the last hops (two or fewer strictly-closer
    /// peers) the geometrically closest connected peer.
    P1G,
}

thread_local! {
    static EVENTS: std::cell::RefCell<Vec<(u64, u64, usize)>> = const { std::cell::RefCell::new(Vec::new()) };
    static PUT_POLICY: Cell<HopPolicy> = const { Cell::new(HopPolicy::Today) };
    static GET_POLICY: Cell<HopPolicy> = const { Cell::new(HopPolicy::Today) };
    static CHOICES: Cell<[u64; 3]> = const { Cell::new([0; 3]) };
    // [put_rank_sum, put_n, get_rank_sum, get_n]
    static RANKS: Cell<[u64; 4]> = const { Cell::new([0; 4]) };
}

/// Restores the previous policies when dropped.
#[must_use = "the policy reverts when the guard is dropped"]
pub struct HopPolicyGuard {
    put: HopPolicy,
    get: HopPolicy,
}

impl HopPolicyGuard {
    pub fn set(put: HopPolicy, get: HopPolicy) -> Self {
        let prev = Self {
            put: PUT_POLICY.with(|p| p.replace(put)),
            get: GET_POLICY.with(|p| p.replace(get)),
        };
        CHOICES.with(|c| c.set([0; 3]));
        RANKS.with(|c| c.set([0; 4]));
        EVENTS.with(|e| e.borrow_mut().clear());
        prev
    }
}

impl Drop for HopPolicyGuard {
    fn drop(&mut self) {
        PUT_POLICY.with(|p| p.set(self.put));
        GET_POLICY.with(|p| p.set(self.get));
    }
}

/// (decisions made under a non-default policy, decisions where the window was
/// narrowed, decisions that fell back to today's choice).
pub fn choice_counts() -> (u64, u64, u64) {
    let [a, b, c] = CHOICES.with(Cell::get);
    (a, b, c)
}

fn policy_for(op: OpType) -> HopPolicy {
    match op {
        OpType::Put => PUT_POLICY.with(Cell::get),
        OpType::Get => GET_POLICY.with(Cell::get),
        _ => HopPolicy::Today,
    }
}

/// Narrow `candidates` per the policy for `op`. The router then ranks what is
/// left (one candidate means no choice). `own` is this node's ring location.
pub(crate) fn narrow(
    op: OpType,
    own: Option<Location>,
    target: Location,
    candidates: &[PeerKeyLocation],
) -> Vec<PeerKeyLocation> {
    let policy = policy_for(op);
    if policy == HopPolicy::Today {
        return candidates.to_vec();
    }
    let dist = |p: &PeerKeyLocation| {
        p.location()
            .map(|l| l.distance(target).as_f64())
            .unwrap_or(f64::MAX)
    };
    let mut by_dist: Vec<&PeerKeyLocation> = candidates.iter().collect();
    by_dist.sort_by(|a, b| dist(a).total_cmp(&dist(b)));
    let first_n = |n: usize| by_dist.iter().take(n).map(|p| (*p).clone()).collect();
    let own_dist = own.map(|o| o.distance(target).as_f64());
    let closer = || -> Vec<PeerKeyLocation> {
        let Some(od) = own_dist else {
            return Vec::new();
        };
        by_dist
            .iter()
            .filter(|p| dist(p) < od)
            .map(|p| (*p).clone())
            .collect()
    };
    let (narrowed, fell_back): (Vec<PeerKeyLocation>, bool) = match policy {
        HopPolicy::Today => unreachable!(),
        HopPolicy::G => (first_n(1), false),
        HopPolicy::K2 => (first_n(2), false),
        HopPolicy::K3 => (first_n(3), false),
        HopPolicy::P1 => {
            let c = closer();
            if c.is_empty() {
                (candidates.to_vec(), true)
            } else {
                (c, false)
            }
        }
        HopPolicy::P1G => {
            let c = closer();
            if c.is_empty() {
                (candidates.to_vec(), true)
            } else if c.len() <= 2 {
                (c.into_iter().take(1).collect(), false)
            } else {
                (c, false)
            }
        }
    };
    CHOICES.with(|c| {
        let mut v = c.get();
        v[0] += 1;
        if narrowed.len() < candidates.len() {
            v[1] += 1;
        }
        if fell_back {
            v[2] += 1;
        }
        c.set(v);
    });
    narrowed
}

/// Mean distance rank (0 = closest connected candidate) of the chosen next
/// hop: `(put_mean, put_n, get_mean, get_n)`.
pub fn rank_stats() -> (f64, u64, f64, u64) {
    let [ps, pn, gs, gn] = RANKS.with(Cell::get);
    let m = |s: u64, n: u64| {
        if n == 0 {
            f64::NAN
        } else {
            s as f64 / n as f64
        }
    };
    (m(ps, pn), pn, m(gs, gn), gn)
}

/// Record the distance rank of `chosen` among all `candidates` for `op`.
pub(crate) fn record_rank(
    op: OpType,
    target: Location,
    candidates: &[PeerKeyLocation],
    chosen: Option<&PeerKeyLocation>,
) {
    let Some(chosen) = chosen else { return };
    let dist = |p: &PeerKeyLocation| {
        p.location()
            .map(|l| l.distance(target).as_f64())
            .unwrap_or(f64::MAX)
    };
    let cd = dist(chosen);
    let rank = candidates.iter().filter(|p| dist(p) < cd).count() as u64;
    let idx = match op {
        OpType::Put => 0,
        OpType::Get => 2,
        _ => return,
    };
    RANKS.with(|c| {
        let mut v = c.get();
        v[idx] += rank;
        v[idx + 1] += 1;
        c.set(v);
    });
}

/// Record the selecting node's router event count (`own` location bits, sim
/// time in ns) so a test can verify every router was warm before measuring.
pub(crate) fn record_events(own: Option<Location>, total_routing_events: usize) {
    let Some(own) = own else { return };
    let now = turmoil::sim_elapsed().map_or(0, |d| d.as_nanos() as u64);
    EVENTS.with(|e| {
        e.borrow_mut()
            .push((now, own.as_f64().to_bits(), total_routing_events))
    });
}

/// `(sim_time_ns, own_location_f64_bits, router_events)` for every logged selection.
pub fn event_timeline() -> Vec<(u64, u64, usize)> {
    EVENTS.with(|e| e.borrow().clone())
}
