//! Admission decision core — the pure decision logic for demand-driven hosting
//! ADMISSION (#4642 piece B / spec step "7-bis"; spec "Admission & Shedding").
//!
//! # What this is
//!
//! [`admission_decision`] is a **pure function** of an [`AdmissionInputs`]
//! snapshot: given everything the controller needs to know when a NEW
//! subscription routes to this peer and asks it to host a contract, it returns
//! one [`AdmissionOutcome`] — `Admit`, `EvictToAdmit(victim)`, or `Refuse`. It
//! reads no locks, touches no live maps, and performs no wire I/O; the caller
//! materializes the snapshot once, so the logic here is directly unit-testable
//! and any at-emission re-read (a hardening concern) stays a separable layer.
//!
//! This is the admission half of invariant 3 in `hosting-invariants.md`:
//! **admission and eviction are ONE demand-vs-capacity decision, and the demand
//! signal is SUBSCRIBER COUNT.** A peer retains the contracts with the most
//! **subscribers** (local client subscriptions + downstream subscribers — the
//! same genuine demand that makes a contract `contract_in_use`) that fit a
//! capability-relative budget; when a new subscription arrives at a full peer it
//! either makes room by displacing a **strictly-fewer-subscriber** incumbent or
//! refuses, never overcommitting and never demoting a real subscriber for
//! another.
//!
//! # Relationship to eviction (#4720) — one shared ranking
//!
//! Admission and the background over-budget eviction sweep are ONE demand-vs-
//! capacity decision (invariant 3), so they MUST rank victims the same way. They
//! do, by construction: both go through the single canonical
//! [`cache::victim_order`], which since the subscriber-primary rework (#4642)
//! orders victims ascending
//! `(local_subscription_count, downstream_subscriber_count, last_get_seq, key_bytes)`.
//! This inert admission model tracks only a single COMBINED `subscriber_count`, so
//! [`pick_displacement_victim`] and [`pick_oom_valve_victim`] call `victim_order`
//! with that combined count in the DOWNSTREAM slot and the local slot held at 0 —
//! `(0, n, seq, key)` compares exactly as the pre-split `(n, seq, key)` did, so no
//! second copy of the ordering drifts out of sync. The `admission_and_cache_agree_*`
//! pin tests in this module map their combined counts the same way and assert
//! admission's victim selection and `cache`'s over-budget eviction pick the SAME
//! victim (and the same full eviction order) from a shared fixture, so a change to
//! either side that broke the agreement fails CI. (When admission is reconciled
//! with the split model — or removed, per the demand-driven-hosting model that has
//! no separate admission decision — this shim goes away.)
//!
//! The subscriber signal is a raw `usize` count (local client subs + downstream
//! subs), NOT a distance-derived read-demand estimate and NOT ring distance. Both
//! axes the earlier draft of this core used (predicted demand and distance) are
//! gone: distance's causal pull on demand already flows through subscriber count
//! via keyward routing gravity (counting both double-counts), and locality is
//! delivered by routing, not by the admission/eviction decision (invariant 3).
//!
//! The `last_get_seq` recency tiebreak means the same thing on both sides: it is
//! stamped by a real **GET or PUT** (genuine client access), never by SUBSCRIBE or
//! subscription renewal — in both `cache.rs` (see `HostedContract::recency_seq`)
//! and here, so the shared tiebreak is consistent.
//!
//! # The decision (spec "Admission & Shedding"; task 7-bis)
//!
//! 1. **Fanout affordability (uplink).** The one variable cost weighed at
//!    ADMISSION, never at eviction (invariant 3): if the new subscription's
//!    ongoing per-update fanout exceeds the peer's uplink headroom, `Refuse` —
//!    the peer cannot afford to keep this contract updated regardless of bytes.
//!    Checked first: an unaffordable ongoing fanout can't be relieved by evicting
//!    bytes, so no eviction can rescue it.
//! 2. **Under budget → `Admit`.** Spare capacity, no eviction needed.
//! 3. **Displacement (strictly-fewer-subscriber evict-to-admit).** At/over
//!    budget, admit by displacing an incumbent with **strictly fewer subscribers**
//!    than the newcomer. For a fresh subscription (newcomer count 1) that means a
//!    zero-subscriber cached copy is displaced for the incoming one-subscriber
//!    contract. Among eligible victims, pick the fewest-subscriber, then
//!    least-recent real GET (`last_get_seq`), then a key-byte tiebreak — the
//!    canonical `cache::victim_order` ordering shared with the eviction sweep.
//!    Preferred even under `Overflow`: displacing a strictly-fewer-subscriber
//!    incumbent both relieves pressure and never pierces the in-use pin.
//! 4. **OOM valve.** ONLY under genuine memory overflow, if no strictly-fewer
//!    incumbent exists, shed the **fewest-subscriber** contract even though it has
//!    at least as many subscribers as the newcomer — piercing the in-use pin (its
//!    subscribers re-root). This is the sole path that sheds a pinned subscribed
//!    contract, and only at real OOM risk. Same canonical `cache::victim_order`,
//!    over ALL incumbents.
//! 5. **Otherwise `Refuse`.** At capacity and every incumbent has at least as many
//!    subscribers as the newcomer (nothing strictly-fewer to displace, and not
//!    genuine OOM) → the "node at capacity" NACK. Refuse the NEWCOMER (visible to
//!    the party that caused the pressure), never silently demote a settled
//!    incumbent (D1) and never demote a real subscriber for another.
//!
//! Both the displacement shed and the OOM valve **bypass the `min_ttl` filter**
//! that the background over-budget sweep (`cache::evict_over_budget`) applies:
//! admission-driven eviction is a placement decision, not the anti-thrash floor,
//! so a young-but-strictly-fewer-subscriber victim is still eligible (spec Fable
//! M9). This core does not see contract age, so the bypass is implicit — the
//! input-builder decides what to include.
//!
//! # Why raw `usize` subscriber count, not a coarsened class
//!
//! The old admission core coarsened subscriber count to a `Zero`/`One`/`Many`
//! class to express a "2+-subscriber contracts are protected" tier. The
//! subscriber-primary model has no such fixed threshold: the rule is purely
//! "strictly fewer subscribers than the newcomer" (a 3-sub newcomer could
//! displace a 2-sub incumbent), and the OOM valve sheds fewest-subscriber-first
//! across the raw counts. Coarsening would both lose the information the general
//! strictly-fewer comparison needs (3 and 5 both collapse to `Many`) and diverge
//! from the raw-`usize` ordering the shared `cache::victim_order` uses. So the
//! class is gone; the signal is a raw count, matching the shared eviction ordering.
//!
//! # Not yet wired
//!
//! This is the pure core + its types + table tests, **additive and inert**.
//! Nothing in production calls [`admission_decision`] yet — exactly like the
//! reconcile controller core (#4704) landed inert first. This PR stacks on the
//! subscriber-primary eviction rework (#4720), whose `cache::victim_order` it
//! reuses so the two share ONE ranking (above). The wiring that reads the live
//! hosting cache / ring to build [`AdmissionInputs`]
//! and that turns a `Refuse` into the on-wire "I'm full" NACK, an `EvictToAdmit`
//! into a cache eviction that pierces the pinned exemption, and an `Admit` into a
//! host, are all later steps. Because it is unwired, the items here are
//! `#[allow(dead_code)]` until that wiring lands. The
//! [`MemoryPressure::Overflow`] OOM-valve TRIGGER is deliberately unwired even
//! after the rest is wired (as in #4720): shedding a subscribed contract is the
//! single riskiest new behavior, so its genuine RSS/A1-resource trigger is a
//! separately-validated step.
//!
//! # Deliberately out of scope for THIS core (later parts of 7-bis)
//!
//! The following pieces of the full spec "Admission & Shedding" mechanism are
//! NOT modeled here and are called out so their absence is a conscious omission,
//! not a silent gap:
//! - **The client-pin exception** (spec Fable M6 / Codex #2): a local client's
//!   own subscription outranks a remote subscriber even at equal-or-fewer
//!   subscriber count, via a reserved client-pin budget slice. That is an
//!   *explicit* carve-out from the strictly-fewer rule and needs an
//!   `is_local_client` dimension + a reserved-slice input; left for a follow-up so
//!   this core stays the conservative general case.
//! - **Displacement-chain depth cap (D1)** and its wire-carried, increment-only,
//!   anti-spoof-clamped depth counter — a property of the multi-hop cascade, not
//!   of a single peer's local decision.
//! - **Make-before-break handoff** (spec Fable F8): the admitting peer must not
//!   drop the victim before its downstream re-homes — a driver-side sequencing
//!   concern, not part of the pure decision.

// Wired to production by a later 7-bis sub-task; the pure core + types + tests
// land here first, unused by any production path, so dead_code is expected and
// allowed until that wiring exists.
#![allow(dead_code)]

use super::cache;
use freenet_stdlib::prelude::ContractKey;

/// Coarse memory/capacity state of this peer at the moment of the admission
/// query, pre-resolved by the input-builder from the byte budget and the RSS/OOM
/// signal.
///
/// The eviction sweep's `cache::MemoryPressure` (this branch stacks on #4720)
/// carries the two OVER-budget states — `AtCapacity` and `Overflow` — with the
/// same meaning; the sweep only runs once already over budget, so it has no
/// `UnderBudget`. Admission needs that third state to distinguish "the newcomer
/// fits" from "must evict", so this stays admission's own enum. Only the victim
/// *ordering* is shared code (`cache::victim_order`); the pressure signal is read
/// consistently on both sides but modeled by each side's own type.
///
/// Pre-resolving the capacity state (rather than handing this module raw
/// `current_bytes` / `budget_bytes` / RSS) mirrors the reconcile core's
/// pre-resolved `computed_upstream`: it keeps [`admission_decision`] a clean
/// decision table and keeps the "does the newcomer fit / is RSS near OOM"
/// arithmetic — including which of memory/CPU/uplink is the scarce binding
/// resource (invariant 3) — in the builder where the live figures live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryPressure {
    /// The newcomer fits within the capability-relative budget — admit without
    /// eviction.
    UnderBudget,
    /// Admitting the newcomer would exceed the budget ("full"): the peer must
    /// displace a strictly-fewer-subscriber victim or refuse. NOT genuine RAM
    /// overflow — the in-use pin holds, so nothing with subscriber count >= the
    /// newcomer's is shed.
    AtCapacity,
    /// Genuine RAM/OOM overflow risk (RSS near the ceiling): the last-resort OOM
    /// valve may shed even a contract with subscriber count >= the newcomer's,
    /// piercing the in-use pin, to relieve pressure (its subscribers re-root).
    /// Distinct from `AtCapacity`, which is merely over the demand budget. Fires
    /// ONLY at real overflow. **Nothing constructs this in production yet** — the
    /// RSS/A1 trigger is a deliberately separate, separately-validated step (as in
    /// #4720), because shedding a subscribed contract is the riskiest new
    /// behavior. Only the valve-mechanism unit tests construct `Overflow`.
    Overflow,
}

/// The contract a new subscription just routed to this peer, which it is being
/// asked to host — the "newcomer" the admission decision is about.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AdmissionCandidate {
    /// Subscriber count the candidate would carry once admitted — the demand
    /// signal (local client subscriptions + downstream subscribers). A fresh
    /// subscription routing to this peer is **1** (the new subscription itself);
    /// the field generalizes so a candidate arriving with a known larger demand
    /// compares correctly. Displacement evicts an incumbent whose subscriber count
    /// is STRICTLY LESS than this (for the normal count-1 case, a zero-subscriber
    /// cached copy). [`admission_decision`] `debug_assert`s this is `>= 1`: a
    /// 0-subscriber newcomer could displace nothing (`0 < 0` is vacuous).
    pub subscriber_count: usize,

    /// The candidate subscription's ongoing per-update fanout cost, weighed
    /// against [`AdmissionInputs::fanout_headroom`] at admission. Interim proxy
    /// = sends-per-update × mean payload size while the egress meter is
    /// greenfield (spec Admission "Budget composition (uplink is greenfield)").
    /// This is the ONE variable cost charged at admission, never at eviction
    /// (invariant 3), because fanout is only incurred while a contract is pinned
    /// by active demand.
    pub fanout_cost: f64,
}

/// A contract this peer currently hosts, as the admission decision sees it — the
/// subscriber-ordering snapshot of the hosted set. Holds only plain values so
/// [`admission_decision`] stays pure.
///
/// Named `AdmissionIncumbent` (not `HostedContract`) deliberately: `cache.rs` has
/// its own, DIFFERENT `HostedContract` (size/last-accessed/keep-score cache
/// metadata). This is admission's view of an incumbent, not the cache entry.
#[derive(Debug, Clone)]
pub(crate) struct AdmissionIncumbent {
    /// Identity of the hosted contract; returned as the victim in an
    /// [`AdmissionOutcome::EvictToAdmit`] so the driver knows what to evict.
    pub key: ContractKey,

    /// Live subscriber count for this contract at this peer (local client
    /// subscriptions + downstream subscribers) — the primary demand signal, the
    /// same one the shared `cache::victim_order` orders eviction by. `== 0` iff
    /// `!contract_in_use`, so the in-use pin here matches the eviction pin.
    pub subscriber_count: usize,

    /// Monotonic real-access recency sequence for this contract: a smaller value is
    /// a less-recent access. The recency tiebreak in `cache::victim_order`. It is
    /// stamped by a real **GET or PUT** (genuine client access), never by SUBSCRIBE
    /// or subscription renewal (so renewal traffic doesn't make every subscribed
    /// contract look equally fresh) — matching `cache.rs`'s
    /// `HostedContract::recency_seq` semantics exactly, so the shared tiebreak
    /// means the same thing on both sides. An entry created without a real GET or
    /// PUT (e.g. a SUBSCRIBE-only entry) carries `0` and is the least-recent,
    /// evicted first within its subscriber class.
    pub last_get_seq: u64,
}

/// A pre-materialized snapshot of everything [`admission_decision`] needs when a
/// new subscription asks this peer to host a contract. Plain values only (no live
/// handles, no locks); the caller reads the live hosting cache / ring once to
/// build it.
#[derive(Debug, Clone)]
pub(crate) struct AdmissionInputs {
    /// The contract being offered for hosting.
    pub candidate: AdmissionCandidate,

    /// This peer's current hosted set, in no required order (the function sorts
    /// candidates itself). Its `subscriber_count` values ARE the demand ordering
    /// the decision reasons over.
    pub hosted: Vec<AdmissionIncumbent>,

    /// Coarse capacity/pressure state, pre-resolved by the builder.
    pub pressure: MemoryPressure,

    /// Available ongoing-update fanout capacity (uplink headroom) for a NEW
    /// subscription, same unit as [`AdmissionCandidate::fanout_cost`]. The
    /// candidate is admissible on the uplink axis iff its `fanout_cost` does not
    /// exceed this. While the egress meter is greenfield this is an interim
    /// proxy; the builder owns its derivation.
    pub fanout_headroom: f64,
}

/// The outcome of an admission query. The driver that acts on it (host / evict /
/// send the NACK) is a later step; here the outcome is a pure value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdmissionOutcome {
    /// Host the candidate; there is room (under budget and fanout affordable),
    /// no eviction needed.
    Admit,
    /// Host the candidate by first evicting the named victim. See
    /// [`Eviction::kind`] for whether this is the normal strictly-fewer-subscriber
    /// displacement or the last-resort OOM valve.
    EvictToAdmit(Eviction),
    /// Do not host the candidate. Expressed on the wire (a later step) as the
    /// "I'm full" NACK; here it carries only the reason.
    Refuse(RefuseReason),
}

/// The victim and rationale of an [`AdmissionOutcome::EvictToAdmit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Eviction {
    /// The contract to evict to make room for the candidate.
    pub victim: ContractKey,
    /// Which admission path chose this victim.
    pub kind: EvictionKind,
}

/// Which eviction path produced an [`Eviction`] — load-bearing because the two
/// carry different guarantees (the OOM valve is the ONLY one that may shed a
/// contract whose subscriber count is >= the newcomer's, piercing the in-use pin)
/// and are telemetered separately (the valve counter must stay 0 while the
/// Overflow trigger is unwired).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictionKind {
    /// Normal displacement evict-to-admit: the victim has STRICTLY FEWER
    /// subscribers than the candidate (for a fresh count-1 subscription, a
    /// zero-subscriber cached copy). A more-demanded contract replaces a
    /// less-demanded one; the in-use pin is never pierced (a contract with
    /// subscriber count >= the newcomer's is never the victim here).
    Displacement,
    /// Last-resort OOM valve: genuine memory overflow with no strictly-fewer
    /// victim available forced shedding the fewest-subscriber contract, which has
    /// subscriber count >= the newcomer's (they re-root). Pierces the in-use pin.
    /// Fires ONLY under [`MemoryPressure::Overflow`].
    OomValve,
}

/// Why an admission was refused (the "I'm full" NACK, spec "The 'I'm full'
/// backpressure").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefuseReason {
    /// The candidate's ongoing update-fanout exceeds this peer's uplink headroom:
    /// the peer cannot afford to keep this subscription updated. Independent of
    /// byte capacity — checked before the budget, since an unaffordable fanout
    /// cannot be relieved by evicting a byte-heavy contract.
    FanoutExceedsHeadroom,
    /// At/over budget, every incumbent has at least as many subscribers as the
    /// newcomer (nothing strictly-fewer to displace), and this is not genuine OOM
    /// overflow (so the valve does not fire). The peer refuses rather than
    /// overcommit or demote a real subscriber for another — the "node at capacity"
    /// NACK, surfaced to the newcomer, never a silent incumbent demote.
    AtCapacityNoVictim,
}

/// Decide whether to admit (host) the candidate contract, evict-to-admit, or
/// refuse.
///
/// Pure: no side effects, no locks, no I/O. See the module docs for the full
/// decision precedence and its grounding in invariant 3 + spec "Admission &
/// Shedding". Precedence (first match wins):
///
/// 1. **Fanout** unaffordable → [`AdmissionOutcome::Refuse`]
///    ([`RefuseReason::FanoutExceedsHeadroom`]).
/// 2. **Under budget** → [`AdmissionOutcome::Admit`].
/// 3. **Strictly-fewer-subscriber victim** exists (even under `Overflow` — it
///    doesn't pierce the pin) → [`AdmissionOutcome::EvictToAdmit`]
///    ([`EvictionKind::Displacement`]).
/// 4. **`Overflow`** and a fewest-subscriber victim exists →
///    [`AdmissionOutcome::EvictToAdmit`] ([`EvictionKind::OomValve`]).
/// 5. Otherwise → [`AdmissionOutcome::Refuse`]
///    ([`RefuseReason::AtCapacityNoVictim`]).
///
/// Contract: the newcomer must arrive with `candidate.subscriber_count >= 1` (a
/// fresh subscription is 1) — `debug_assert`ed. A 0-subscriber candidate could
/// displace nothing (`0 < 0` is vacuous) and would only ever `Admit` or `Refuse`.
pub(crate) fn admission_decision(inputs: &AdmissionInputs) -> AdmissionOutcome {
    let candidate = &inputs.candidate;

    debug_assert!(
        candidate.subscriber_count >= 1,
        "admission newcomer must arrive with >= 1 subscriber (a fresh subscription \
         is 1); a 0-subscriber candidate can displace nothing (0 < 0 is vacuous)"
    );

    // (1) Fanout affordability (uplink) — the one variable cost weighed at
    //     admission (invariant 3). Checked FIRST and independent of the byte
    //     budget: an unaffordable ongoing fanout cannot be relieved by evicting
    //     bytes, so no eviction can rescue it — this gates before BOTH the
    //     displacement path and the OOM valve. STRICT `>`: a fanout exactly equal
    //     to the headroom is affordable (fits).
    if candidate.fanout_cost > inputs.fanout_headroom {
        return AdmissionOutcome::Refuse(RefuseReason::FanoutExceedsHeadroom);
    }

    // (2) Under budget → admit, no eviction (fanout already fits).
    if inputs.pressure == MemoryPressure::UnderBudget {
        return AdmissionOutcome::Admit;
    }

    // At capacity or in overflow below: make room or refuse.

    // (3) Displacement evict-to-admit. Displace an incumbent with STRICTLY FEWER
    //     subscribers than the newcomer (for a fresh count-1 subscription, a
    //     zero-subscriber cached copy). Preferred even under `Overflow`, because a
    //     strictly-fewer-subscriber victim relieves pressure WITHOUT piercing the
    //     in-use pin. Bypasses `min_ttl` (spec Fable M9): admission-driven
    //     eviction is a placement decision, not the background sweep's anti-thrash
    //     floor.
    if let Some(victim) = pick_displacement_victim(&inputs.hosted, candidate.subscriber_count) {
        return AdmissionOutcome::EvictToAdmit(Eviction {
            victim,
            kind: EvictionKind::Displacement,
        });
    }

    // (4) Last-resort OOM valve — ONLY at genuine overflow, and only because no
    //     strictly-fewer victim was available above (so the fewest-subscriber
    //     incumbent has subscriber count >= the newcomer's; shedding it pierces
    //     the in-use pin and its subscribers re-root). Also bypasses `min_ttl`.
    if inputs.pressure == MemoryPressure::Overflow {
        if let Some(victim) = pick_oom_valve_victim(&inputs.hosted) {
            return AdmissionOutcome::EvictToAdmit(Eviction {
                victim,
                kind: EvictionKind::OomValve,
            });
        }
    }

    // (5) At capacity with nothing strictly-fewer to displace → the "node at
    //     capacity" NACK. Refuse the newcomer rather than overcommit or demote a
    //     settled incumbent / a real subscriber (D1).
    AdmissionOutcome::Refuse(RefuseReason::AtCapacityNoVictim)
}

/// Select the victim for a NORMAL displacement evict-to-admit, or `None` if no
/// incumbent has strictly fewer subscribers than the newcomer.
///
/// Eligible iff `subscriber_count` STRICTLY LESS than `candidate_subscriber_count`
/// (for a fresh count-1 subscription, exactly the zero-subscriber contracts).
/// Among eligible victims the preference order (worst evicted first) is the
/// canonical [`cache::victim_order`] key: fewest subscribers first, then
/// least-recent real GET (`last_get_seq`), then a deterministic key-byte tiebreak.
fn pick_displacement_victim(
    hosted: &[AdmissionIncumbent],
    candidate_subscriber_count: usize,
) -> Option<ContractKey> {
    hosted
        .iter()
        .filter(|c| c.subscriber_count < candidate_subscriber_count)
        .min_by(|a, b| {
            // The inert admission model (#4717) tracks only a single combined
            // `subscriber_count`; the split `(local, downstream)` `victim_order`
            // key degrades to ordering by that combined count when the first
            // (local) slot is held at 0 — `(0, n, seq, key)` compares exactly as
            // the old `(n, seq, key)` did. Shim kept until admission is reconciled
            // with the split model (or removed per the demand-driven-hosting model,
            // which has no separate admission decision).
            cache::victim_order(
                (0, a.subscriber_count, a.last_get_seq, &a.key),
                (0, b.subscriber_count, b.last_get_seq, &b.key),
            )
        })
        .map(|c| c.key)
}

/// Select the fewest-subscriber contract for the OOM valve, or `None` if the
/// hosted set is empty. Considers ALL incumbents — the valve is the sole path that
/// may shed a contract whose subscriber count is >= the newcomer's, piercing the
/// in-use pin, and only under genuine overflow.
///
/// "Fewest subscriber" uses the SAME canonical [`cache::victim_order`] key as the
/// displacement path: fewest subscribers first, then least-recent real GET, then a
/// deterministic key-byte tiebreak. (Reaching this path means no incumbent was
/// strictly-fewer than the newcomer, so the chosen victim necessarily has
/// subscriber count >= the newcomer's — a real pin pierce, which is why this is
/// telemetered separately.)
fn pick_oom_valve_victim(hosted: &[AdmissionIncumbent]) -> Option<ContractKey> {
    hosted
        .iter()
        .min_by(|a, b| {
            // See `pick_displacement_victim`: admission's inert combined-count
            // model maps onto the split `victim_order` key with the local slot 0.
            cache::victim_order(
                (0, a.subscriber_count, a.last_get_seq, &a.key),
                (0, b.subscriber_count, b.last_get_seq, &b.key),
            )
        })
        .map(|c| c.key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::hosting::cache::{AccessType, HostingCache, MemoryPressure as CachePressure};
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};
    use std::collections::HashMap;

    /// Deterministic distinct `ContractKey` from a seed byte (same construction
    /// as the `cache` module's tests).
    fn make_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    /// An incumbent with the given knobs. `key` is derived from `seed` so each
    /// incumbent is distinct and the victim assertion is unambiguous.
    fn hosted(seed: u8, subscriber_count: usize, last_get_seq: u64) -> AdmissionIncumbent {
        AdmissionIncumbent {
            key: make_key(seed),
            subscriber_count,
            last_get_seq,
        }
    }

    /// A newcomer with the normal fresh-subscription demand (1 subscriber) and
    /// generous fanout headroom by default (fanout not the axis under test unless
    /// overridden).
    fn candidate() -> AdmissionCandidate {
        AdmissionCandidate {
            subscriber_count: 1,
            fanout_cost: 1.0,
        }
    }

    /// A newcomer arriving with a specified subscriber count (for the general
    /// strictly-fewer tests).
    fn candidate_with_subs(subscriber_count: usize) -> AdmissionCandidate {
        AdmissionCandidate {
            subscriber_count,
            fanout_cost: 1.0,
        }
    }

    fn inputs(
        candidate: AdmissionCandidate,
        hosted: Vec<AdmissionIncumbent>,
        pressure: MemoryPressure,
        fanout_headroom: f64,
    ) -> AdmissionInputs {
        AdmissionInputs {
            candidate,
            hosted,
            pressure,
            fanout_headroom,
        }
    }

    // ------------------------------------------------------------------
    // (2) Under budget → Admit
    // ------------------------------------------------------------------

    #[test]
    fn under_budget_admits_without_eviction() {
        let got = admission_decision(&inputs(
            candidate(),
            vec![hosted(1, 0, 5)],
            MemoryPressure::UnderBudget,
            10.0,
        ));
        assert_eq!(got, AdmissionOutcome::Admit);
    }

    #[test]
    fn under_budget_empty_hosted_set_admits() {
        let got = admission_decision(&inputs(
            candidate(),
            vec![],
            MemoryPressure::UnderBudget,
            10.0,
        ));
        assert_eq!(got, AdmissionOutcome::Admit);
    }

    // ------------------------------------------------------------------
    // (1) Fanout affordability gates first, before byte capacity
    // ------------------------------------------------------------------

    #[test]
    fn fanout_over_headroom_refuses_even_under_budget() {
        // Byte headroom exists (UnderBudget) but the ongoing fanout does not fit
        // ⇒ Refuse on the uplink axis, proving fanout gates before the budget.
        let mut c = candidate();
        c.fanout_cost = 5.0;
        let got = admission_decision(&inputs(c, vec![], MemoryPressure::UnderBudget, 4.0));
        assert_eq!(
            got,
            AdmissionOutcome::Refuse(RefuseReason::FanoutExceedsHeadroom)
        );
    }

    #[test]
    fn fanout_over_headroom_refuses_at_capacity() {
        // At capacity WITH a would-be displacement victim present: the fanout gate
        // still fires first (an unaffordable fanout can't be relieved by evicting
        // bytes).
        let mut c = candidate();
        c.fanout_cost = 5.0;
        let got = admission_decision(&inputs(
            c,
            vec![hosted(1, 0, 5)],
            MemoryPressure::AtCapacity,
            4.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::Refuse(RefuseReason::FanoutExceedsHeadroom)
        );
    }

    #[test]
    fn fanout_over_headroom_refuses_under_overflow() {
        // Even at genuine overflow WITH a would-be valve victim present (subscriber
        // count >= the newcomer's), the fanout gate fires FIRST — pins that fanout
        // gates before the OOM valve, not just before displacement (an unaffordable
        // fanout can't be relieved by shedding bytes, valve or not).
        let mut c = candidate();
        c.fanout_cost = 5.0;
        let got = admission_decision(&inputs(
            c,
            vec![hosted(1, 2, 5)],
            MemoryPressure::Overflow,
            4.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::Refuse(RefuseReason::FanoutExceedsHeadroom)
        );
    }

    #[test]
    fn fanout_exactly_at_headroom_is_affordable() {
        // Boundary: fanout_cost == headroom is NOT over (strict `>`), so the
        // fanout gate does not fire and an under-budget candidate is admitted.
        let mut c = candidate();
        c.fanout_cost = 4.0;
        let got = admission_decision(&inputs(c, vec![], MemoryPressure::UnderBudget, 4.0));
        assert_eq!(got, AdmissionOutcome::Admit);
    }

    // ------------------------------------------------------------------
    // (3) Displacement evict-to-admit
    // ------------------------------------------------------------------

    #[test]
    fn at_capacity_with_zero_sub_incumbent_displaces_for_one_sub_newcomer() {
        // Newcomer: 1 subscriber (fresh subscription). Incumbent: 0 subscribers
        // (cached copy) ⇒ strictly fewer ⇒ displace it.
        let victim_key = make_key(7);
        let got = admission_decision(&inputs(
            candidate(),
            vec![AdmissionIncumbent {
                key: victim_key,
                subscriber_count: 0,
                last_get_seq: 3,
            }],
            MemoryPressure::AtCapacity,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: victim_key,
                kind: EvictionKind::Displacement,
            })
        );
    }

    #[test]
    fn at_capacity_higher_sub_newcomer_displaces_lower_sub_incumbent() {
        // Generality of the strictly-fewer rule: a 3-subscriber newcomer displaces
        // a 2-subscriber incumbent (2 < 3). Proves the rule is not hardcoded to
        // "displace only zero-subscriber".
        let victim_key = make_key(8);
        let got = admission_decision(&inputs(
            candidate_with_subs(3),
            vec![AdmissionIncumbent {
                key: victim_key,
                subscriber_count: 2,
                last_get_seq: 4,
            }],
            MemoryPressure::AtCapacity,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: victim_key,
                kind: EvictionKind::Displacement,
            })
        );
    }

    // ------------------------------------------------------------------
    // (5) At capacity, no strictly-fewer victim → Refuse.
    // ------------------------------------------------------------------

    #[test]
    fn at_capacity_no_incumbents_refuses() {
        let got = admission_decision(&inputs(
            candidate(),
            vec![],
            MemoryPressure::AtCapacity,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::Refuse(RefuseReason::AtCapacityNoVictim)
        );
    }

    #[test]
    fn at_capacity_equal_sub_incumbent_refuses() {
        // Incumbent has the SAME subscriber count as the newcomer (1) ⇒ not
        // strictly fewer ⇒ Refuse. Don't demote a real subscriber for another.
        let got = admission_decision(&inputs(
            candidate(),
            vec![hosted(1, 1, 3)],
            MemoryPressure::AtCapacity,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::Refuse(RefuseReason::AtCapacityNoVictim)
        );
    }

    #[test]
    fn at_capacity_higher_sub_incumbent_refuses() {
        // Incumbent has MORE subscribers than the newcomer (2 > 1) ⇒ protected in
        // normal operation ⇒ Refuse (not at genuine OOM, so no valve).
        let got = admission_decision(&inputs(
            candidate(),
            vec![hosted(1, 2, 3)],
            MemoryPressure::AtCapacity,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::Refuse(RefuseReason::AtCapacityNoVictim)
        );
    }

    // ------------------------------------------------------------------
    // Contract boundary: a degenerate 0-subscriber candidate displaces
    // nothing. admission_decision debug_asserts subscriber_count >= 1, so
    // exercise the victim-picker directly (the non-asserted path).
    // ------------------------------------------------------------------

    #[test]
    fn zero_subscriber_candidate_displaces_nothing() {
        // `0 < 0` is vacuously false, so even a zero-subscriber incumbent is not a
        // valid displacement victim for a zero-subscriber newcomer. Documents the
        // underlying picker contract without tripping the debug_assert in
        // admission_decision.
        let victim = pick_displacement_victim(&[hosted(1, 0, 3)], 0);
        assert_eq!(victim, None);
    }

    // ------------------------------------------------------------------
    // (3) Victim SELECTION among multiple eligible candidates —
    //     ascending (subscriber_count, last_get_seq, key).
    // ------------------------------------------------------------------

    #[test]
    fn victim_selection_prefers_fewest_subscribers() {
        // Newcomer has 3 subs; both incumbents are strictly fewer (1 and 2). The
        // FEWER-subscriber one (1) is displaced first even though the 2-sub one is
        // a less-recent GET.
        let fewest = make_key(2);
        let got = admission_decision(&inputs(
            candidate_with_subs(3),
            vec![
                // 2 subs, very stale GET
                hosted(1, 2, 1),
                // 1 sub, more-recent GET — still preferred: fewer subs wins
                AdmissionIncumbent {
                    key: fewest,
                    subscriber_count: 1,
                    last_get_seq: 99,
                },
            ],
            MemoryPressure::AtCapacity,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: fewest,
                kind: EvictionKind::Displacement,
            })
        );
    }

    #[test]
    fn victim_selection_prefers_least_recent_get_within_same_sub_count() {
        // Two zero-subscriber eligible victims; the least-recent real GET
        // (smallest last_get_seq) is displaced first.
        let stale = make_key(3);
        let got = admission_decision(&inputs(
            candidate(),
            vec![
                // fresher GET
                hosted(1, 0, 50),
                // staler GET (smaller seq) — displaced first
                AdmissionIncumbent {
                    key: stale,
                    subscriber_count: 0,
                    last_get_seq: 5,
                },
            ],
            MemoryPressure::AtCapacity,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: stale,
                kind: EvictionKind::Displacement,
            })
        );
    }

    // ------------------------------------------------------------------
    // (4) OOM valve
    // ------------------------------------------------------------------

    #[test]
    fn oom_valve_sheds_fewest_sub_when_no_strict_victim() {
        // Overflow, every incumbent has subscriber count >= the newcomer's 1 (no
        // strictly-fewer victim), so the valve sheds the FEWEST-subscriber one,
        // piercing the pin.
        let fewest = make_key(4);
        let got = admission_decision(&inputs(
            candidate(),
            vec![
                hosted(1, 3, 10),
                AdmissionIncumbent {
                    key: fewest,
                    subscriber_count: 1,
                    last_get_seq: 10,
                },
            ],
            MemoryPressure::Overflow,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: fewest,
                kind: EvictionKind::OomValve,
            })
        );
    }

    #[test]
    fn overflow_prefers_displacement_over_valve() {
        // Under Overflow, a strictly-fewer-subscriber victim is still preferred
        // over piercing the pin via the valve — it relieves pressure WITHOUT
        // shedding a pinned contract. The zero-sub victim wins, kind is
        // Displacement (NOT OomValve).
        let zero_sub = make_key(5);
        let got = admission_decision(&inputs(
            candidate(),
            vec![
                // pinned incumbent the valve COULD shed but shouldn't here
                hosted(1, 5, 2),
                // strictly-fewer (zero-sub) victim
                AdmissionIncumbent {
                    key: zero_sub,
                    subscriber_count: 0,
                    last_get_seq: 8,
                },
            ],
            MemoryPressure::Overflow,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: zero_sub,
                kind: EvictionKind::Displacement,
            })
        );
    }

    #[test]
    fn overflow_empty_hosted_set_refuses() {
        // Genuine overflow but nothing at all to shed ⇒ Refuse.
        let got = admission_decision(&inputs(candidate(), vec![], MemoryPressure::Overflow, 10.0));
        assert_eq!(
            got,
            AdmissionOutcome::Refuse(RefuseReason::AtCapacityNoVictim)
        );
    }

    #[test]
    fn oom_valve_tiebreak_by_least_recent_get_within_equal_sub_count() {
        // Overflow, all incumbents equal-subscriber (2) and >= the newcomer's 1
        // (no strictly-fewer), so the valve fires and breaks the tie by
        // least-recent real GET (smallest last_get_seq).
        let stale = make_key(6);
        let got = admission_decision(&inputs(
            candidate(),
            vec![
                hosted(1, 2, 40),
                AdmissionIncumbent {
                    key: stale,
                    subscriber_count: 2,
                    last_get_seq: 7,
                },
                hosted(2, 2, 60),
            ],
            MemoryPressure::Overflow,
            10.0,
        ));
        assert_eq!(
            got,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: stale,
                kind: EvictionKind::OomValve,
            })
        );
    }

    // ------------------------------------------------------------------
    // Determinism: equal-order victims resolve by key bytes, stably —
    // one test for the displacement path, one for the valve path.
    // ------------------------------------------------------------------

    #[test]
    fn victim_selection_is_deterministic_on_ties() {
        // Two identical eligible DISPLACEMENT victims (same subscriber count and
        // last_get_seq); the key-byte tiebreak makes the choice deterministic and
        // independent of input order. Whichever key is smaller by bytes wins both
        // ways.
        let k_a = make_key(10);
        let k_b = make_key(20);
        let mk = |first: ContractKey, second: ContractKey| {
            admission_decision(&inputs(
                candidate(),
                vec![
                    AdmissionIncumbent {
                        key: first,
                        subscriber_count: 0,
                        last_get_seq: 4,
                    },
                    AdmissionIncumbent {
                        key: second,
                        subscriber_count: 0,
                        last_get_seq: 4,
                    },
                ],
                MemoryPressure::AtCapacity,
                10.0,
            ))
        };
        let forward = mk(k_a, k_b);
        let reversed = mk(k_b, k_a);
        assert_eq!(forward, reversed, "victim choice must not depend on order");
        let expected = if k_a.as_bytes() <= k_b.as_bytes() {
            k_a
        } else {
            k_b
        };
        assert_eq!(
            forward,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: expected,
                kind: EvictionKind::Displacement,
            })
        );
    }

    #[test]
    fn oom_valve_selection_is_deterministic_on_ties() {
        // Two identical VALVE victims (same subscriber count and last_get_seq),
        // both >= the newcomer's 1 so no strictly-fewer victim exists and the valve
        // fires. The key-byte tiebreak makes the valve victim deterministic and
        // order-independent — the valve-path mirror of the displacement determinism
        // test.
        let k_a = make_key(11);
        let k_b = make_key(21);
        let mk = |first: ContractKey, second: ContractKey| {
            admission_decision(&inputs(
                candidate(),
                vec![
                    AdmissionIncumbent {
                        key: first,
                        subscriber_count: 2,
                        last_get_seq: 7,
                    },
                    AdmissionIncumbent {
                        key: second,
                        subscriber_count: 2,
                        last_get_seq: 7,
                    },
                ],
                MemoryPressure::Overflow,
                10.0,
            ))
        };
        let forward = mk(k_a, k_b);
        let reversed = mk(k_b, k_a);
        assert_eq!(forward, reversed, "valve victim must not depend on order");
        let expected = if k_a.as_bytes() <= k_b.as_bytes() {
            k_a
        } else {
            k_b
        };
        assert_eq!(
            forward,
            AdmissionOutcome::EvictToAdmit(Eviction {
                victim: expected,
                kind: EvictionKind::OomValve,
            })
        );
    }

    // ------------------------------------------------------------------
    // Cross-module consistency pin (#4717 stacked on #4720): admission's victim
    // selection and the cache's over-budget eviction MUST agree, because both go
    // through the single canonical `cache::victim_order`. These tests drive BOTH
    // real code paths — admission's pickers and the cache's `sweep_expired` —
    // over ONE shared fixture whose admission-side inputs are DERIVED from the
    // very cache state the eviction sweep ranks, so any order difference is
    // purely the ordering logic. If a future change forks either side onto a
    // different ranking, the two orders diverge and these fail — which is the
    // whole point of unifying on one `victim_order`.
    // ------------------------------------------------------------------

    /// Build the shared 4-contract fixture: seeds 3 & 2 are zero-subscriber,
    /// seed 5 has 1 subscriber, seed 4 has 2. Inserts via GET in `[3, 2, 5, 4]`
    /// order so seed 3 is a strictly-less-recent GET than seed 2 (each GET stamps
    /// an increasing `recency_seq`), making the zero-subscriber tiebreak
    /// deterministic and key-independent. Returns the populated cache, the
    /// subscriber-count map, and the admission-side view of the same set with
    /// each `last_get_seq` read back from the cache's actually-stamped value.
    ///
    /// All four are inserted under a GENEROUS budget so no insert auto-evicts (now
    /// that `min_ttl` is gone, the insert path sheds down to budget immediately);
    /// the caller-requested `budget` is then applied via `set_budget_for_test`, so
    /// a subsequent sweep sees all four resident with their distinct recency and
    /// sheds according to the tightened budget.
    fn cross_module_fixture(
        budget: u64,
    ) -> (
        HostingCache<SharedMockTimeSource>,
        SharedMockTimeSource,
        HashMap<ContractKey, usize>,
        Vec<AdmissionIncumbent>,
    ) {
        let subs: HashMap<ContractKey, usize> = HashMap::from([
            (make_key(2), 0),
            (make_key(3), 0),
            (make_key(4), 2),
            (make_key(5), 1),
        ]);
        // The cache orders by (local, downstream); admission's inert model tracks
        // only a combined count. Map every combined count to the DOWNSTREAM slot
        // (local = 0), exactly as admission's `victim_order` shim does, so the two
        // orderings remain comparable.
        let counts_of = |k: &ContractKey| (0usize, subs.get(k).copied().unwrap_or(0));

        let time = SharedMockTimeSource::new();
        // Seed under a generous budget so no insert auto-evicts, then tighten.
        let mut cache = HostingCache::new(100_000, time.clone());
        for seed in [3u8, 2, 5, 4] {
            let key = make_key(seed);
            let r = cache.record_access(key, 100, AccessType::Get, 0, counts_of);
            assert!(r.is_new, "fixture keys must be distinct fresh inserts");
        }
        cache.set_budget_for_test(budget);

        let hosted: Vec<AdmissionIncumbent> = subs
            .keys()
            .map(|k| AdmissionIncumbent {
                key: *k,
                subscriber_count: subs.get(k).copied().unwrap_or(0),
                last_get_seq: cache.get(k).expect("inserted above").recency_seq,
            })
            .collect();

        (cache, time, subs, hosted)
    }

    #[test]
    fn admission_and_cache_agree_on_full_eviction_order() {
        // Budget 0 so an Overflow sweep sheds EVERY entry, exposing the full
        // eviction order across all three ranking tiers. The fixture seeds all four
        // under a generous budget then tightens to 0 (see `cross_module_fixture`),
        // so all four are resident with distinct recency for the Overflow sweep.
        let (mut cache, _time, subs, hosted) = cross_module_fixture(0);
        // Combined counts mapped to the downstream slot (local = 0), matching the
        // admission `victim_order` shim, so the two orderings stay comparable.
        let counts_of = |k: &ContractKey| (0usize, subs.get(k).copied().unwrap_or(0));

        // Admission's full order: repeatedly pick the OOM-valve victim (ranks
        // over ALL incumbents by `victim_order`) and remove it.
        let mut remaining = hosted.clone();
        let mut admission_order = Vec::new();
        while let Some(victim) = pick_oom_valve_victim(&remaining) {
            admission_order.push(victim);
            remaining.retain(|c| c.key != victim);
        }

        // Cache's full order: Overflow sweep at budget 0 sheds all, fewest-
        // subscriber-then-least-recent-GET-first.
        let cache_order: Vec<ContractKey> = cache
            .sweep_expired(counts_of, CachePressure::Overflow)
            .into_iter()
            .map(|e| e.key)
            .collect();

        assert_eq!(
            admission_order, cache_order,
            "admission and cache must produce the same eviction order — both go \
             through cache::victim_order"
        );
        // Guard the fixture actually exercised the subscriber tie + recency
        // tiebreak (not only the subscriber-count primary): the two zero-sub
        // contracts lead, least-recent GET (seed 3) before seed 2.
        assert_eq!(
            cache_order,
            vec![make_key(3), make_key(2), make_key(5), make_key(4)],
            "expected fewest-subscriber-then-least-recent-GET order"
        );
    }

    #[test]
    fn admission_and_cache_agree_on_at_capacity_victim() {
        // The displacement / AtCapacity path: a 1-subscriber newcomer, so
        // admission's displacement picker is eligible to evict the strictly-fewer
        // (zero-subscriber) incumbents, while the cache's AtCapacity sweep evicts
        // the fewest-subscriber entries (no `min_ttl` gate). Both must pick seed 3.
        // Budget 350: three entries (300B) fit, four (400B) do not, so exactly
        // one entry is shed and the sweep's FRONT victim is observable.
        let (mut cache, _time, subs, hosted) = cross_module_fixture(350);
        // Combined counts mapped to the downstream slot (local = 0), matching the
        // admission `victim_order` shim.
        let counts_of = |k: &ContractKey| (0usize, subs.get(k).copied().unwrap_or(0));

        // No `min_ttl` gate (dropped 2026-07-08): every entry is eligible, so the
        // zero-subscriber seeds 2 & 3 sort ahead of the subscribed 4 & 5 and one
        // zero-subscriber entry is the front victim (subscribed ordered last).

        let admission_victim =
            pick_displacement_victim(&hosted, 1).expect("a zero-sub incumbent is displaceable");

        let evicted = cache.sweep_expired(counts_of, CachePressure::AtCapacity);
        assert_eq!(
            evicted.len(),
            1,
            "budget 350 sheds exactly one of four 100B entries"
        );
        let cache_victim = evicted[0].key;

        assert_eq!(
            admission_victim, cache_victim,
            "admission displacement and cache AtCapacity eviction must pick the \
             same victim — both go through cache::victim_order"
        );
        assert_eq!(
            cache_victim,
            make_key(3),
            "the least-recent-GET zero-subscriber contract is the first victim"
        );
    }
}
