//! Reconcile controller core — the pure decision logic of the demand-driven
//! hosting maintenance loop (#4642 piece 2, the keystone refactor; spec
//! "The maintenance / reconcile loop").
//!
//! # What this is
//!
//! [`reconcile`] is a **pure function** of a [`ReconcileInputs`] snapshot: given
//! everything the controller needs to know about one contract at one instant, it
//! returns the set of [`Action`]s that would bring the contract to its desired
//! hosting state. It reads no locks, touches no live maps, and performs no wire
//! I/O — the snapshot is materialized once by the caller, so the logic here is
//! directly unit-testable and the eventual at-emission re-read (a STEP-3 hardening
//! concern) stays a cleanly separable layer on top.
//!
//! Today the redesign's per-contract maintenance is scattered across event
//! handlers and a stored `is_upstream` interest flag that drifts under gossip
//! (#4671). This module is the level-triggered replacement: one function computes
//! the desired action set from current inputs, so a missed event is caught by the
//! next tick and the whole stale-flag bug class disappears.
//!
//! # Not yet wired
//!
//! This is **sub-task 1** of the keystone: the pure core + its types + unit tests,
//! additive and behavior-preserving. Nothing in production calls [`reconcile`]
//! yet. The next sub-task wires it in **shadow mode** at the ~6 on-`main` decision
//! sites (compute what it WOULD do, compare to what the current code does, record
//! divergence — current code still drives). The FLIP (controller actually drives)
//! is a later step. Because it is unwired, the items here are `#[allow(dead_code)]`
//! until that shadow wiring lands.
//!
//! # Notes for the shadow-compare wiring (next sub-task)
//!
//! - **`Collapse` is the LOCAL teardown** (drop our lease, `ring.unsubscribe`);
//!   **`Unsubscribe` is the WIRE message** to the computed upstream; **`Retract`
//!   withdraws the hosting advertisement** (on-`main` primitive
//!   `neighbor_hosting.on_contract_unhosted`, `node/neighbor_hosting.rs:131`,
//!   currently dead code a later flip must wire). The current code's
//!   `send_unsubscribe_upstream` does the first two together ("send Unsubscribe +
//!   `ring.unsubscribe`") in one call, so the shadow-compare must map it onto the
//!   `{Collapse, Unsubscribe}` PAIR by SET membership, not exact-`Vec` equality.
//!   `Retract` is INDEPENDENT of the lease: it can be emitted on its own (an
//!   advertised-but-not-subscribed host that loses demand → `[Retract]`, no
//!   `Collapse`/`Unsubscribe`), and maps to `on_contract_unhosted`.
//! - **`ReRootSearch` covers BOTH** "upstream lost" (we were a host and the
//!   closer co-host vanished) AND "never rooted / first formation" (in use, no
//!   upstream yet, not a root). Both route keyward via the same consult-equipped
//!   search, so shadow-mapping does not need to distinguish them.
//! - **`Renew` is level-triggered desired-state, not "renew now"** (see its action
//!   doc). The shadow-compare checks the PRESENCE of the renewal desire against
//!   the (renewing) current code; the driver later owns when a renewal is actually
//!   due, so do not shadow-map it onto an edge-timed send.
//!
//! # The `Distance` equality guard (load-bearing — read before editing)
//!
//! [`reconcile`] deliberately consumes an already-resolved
//! `computed_upstream: Option<PeerKeyLocation>` rather than raw ring distances.
//! That is not incidental: `Distance`'s `PartialEq` is **epsilon-fuzzy**
//! (`ring/location.rs:223`, `(a-b).abs() < f64::EPSILON`) while its `Ord`/`<`
//! (what `most_keyward_among` uses to SELECT the upstream) is **exact**. So the
//! same pair of distances can be classified "equal" by `==` and "strictly closer"
//! by `<` at once. Keeping every distance comparison inside `most_keyward_among`
//! (exact) and handing this module only the RESULT means the controller never
//! performs an epsilon `==` compare that could disagree with that selection.
//!
//! If a future edit ever adds a raw-`Distance` field here and needs to test it for
//! equality, it MUST use exact comparison — `a.cmp(&b) == Ordering::Equal`, never
//! `==` — to stay consistent with the strict-`<` ordering used for upstream
//! selection. See the pin test `distance_partialeq_is_fuzzy_but_cmp_is_exact` and
//! `ring/location.rs:223-243`.

// Wired to production (in shadow mode) by the next keystone sub-task; the pure
// core + types + tests land here first, unused by any production path, so
// dead_code is expected and allowed until that wiring exists.
#![allow(dead_code)]

use crate::ring::PeerKeyLocation;

/// A single maintenance action the reconcile controller can emit for one
/// contract. The driver that applies a `Vec<Action>` to the wire/state is a
/// later step; here the actions are pure values.
///
/// The set the on-`main` decision sites exercise is `Subscribe`, `Unsubscribe`,
/// `Collapse`, `Announce`, `Renew`, `ReRootSearch`, and `Retract`. (Evict-to-admit
/// is a separate later piece (7-bis) and is intentionally absent.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Desired to host via a known (computed) upstream, but we hold no active
    /// subscription yet → link to the upstream by subscribing toward the key.
    Subscribe,
    /// Level-triggered DESIRED-STATE that an in-use subscription lease we hold
    /// should be kept alive. This is NOT an edge "renew now" command — it means
    /// "this lease should stay alive"; the DRIVER owns the actual renewal timing
    /// (when a lease is due, its backoff, its dedup). Emitted whenever the
    /// contract is in use (a local client OR a strictly-farther downstream
    /// subscriber) AND we hold a lease (`is_subscribed`), and — crucially — NOT
    /// emitted once the last interest goes, so subscriptions track active demand
    /// rather than cache size. That interest-gating is the #3763 renewal-storm
    /// fix; without a `Renew` action the controller could not express it, so a
    /// shadow-compare against the (renewing) current code would show false
    /// divergence and a future flip would break-before-make.
    Renew,
    /// Send an `Unsubscribe` to the (computed) upstream, collapsing the chain one
    /// hop keyward. The WIRE message, distinct from `Collapse` (the local
    /// teardown). Emitted alongside `Collapse` when demand is gone AND a computed
    /// upstream exists to notify.
    Unsubscribe,
    /// Last interest is gone (interest-gated collapse): tear down our own
    /// subscription lease and stop hosting inward. The LOCAL teardown, distinct
    /// from `Unsubscribe` (the wire message to the upstream); `Collapse` fires
    /// even when there is no upstream to notify (e.g. a root whose demand lapsed).
    Collapse,
    /// We host (state present + a host role) but have not advertised it to our
    /// neighbors yet → advertise hosting so co-hosts can fan out updates and
    /// upstream selection can find us. Emitted only once the body is actually
    /// present, never before (reconcile-before-announce).
    Announce,
    /// Withdraw a hosting advertisement (on-`main` primitive:
    /// `neighbor_hosting.on_contract_unhosted`, currently dead code). Emitted on
    /// the teardown branch whenever we were advertising, INDEPENDENT of whether we
    /// held a lease — a verified root advertises without ever subscribing
    /// upstream, so a torn-down (not-in-use) advertised host must retract or its
    /// stale co-host advertisement poisons fan-out and upstream selection
    /// (hosting-iff-advertised, invariant 1: advertise iff a fresh in-mesh host).
    Retract,
    /// Demand is intact but our upstream vanished (or was never found) and we are
    /// not the verified root → search keyward to re-establish a place in the
    /// mesh. This is the partition-vs-collapse distinction: with demand present a
    /// lost upstream means re-root, NOT collapse a still-wanted chain. Covers both
    /// "upstream lost" and "never rooted / first formation". Suppressed while
    /// `actively_acquiring` (a search is already in flight).
    ReRootSearch,
}

/// A pure, already-materialized snapshot of everything [`reconcile`] needs about
/// one contract at one instant. Holds only plain values (no live handles, no
/// locks) so [`reconcile`] is a pure function; the caller reads the live maps
/// once to build this, and re-reading at emission time (for destructive actions)
/// is a separable STEP-3 concern.
#[derive(Debug, Clone)]
pub(crate) struct ReconcileInputs {
    /// The **computed upstream**: the most-keyward connected co-host STRICTLY
    /// closer to the contract key than this peer, from
    /// `Ring::most_keyward_hosting_neighbor` / `most_keyward_among` (#4693).
    ///
    /// `Some(p)` ⇒ `p` is our upstream (a live link toward the key). `None` ⇒ no
    /// strictly-closer connected co-host: either we are the terminus/root (see
    /// [`is_verified_root`](Self::is_verified_root)) or our upstream vanished
    /// (re-root). Pre-resolved on purpose — see the module-level `Distance`
    /// equality guard: all distance ORDERING stays in `most_keyward_among`
    /// (exact `<`/`cmp`), never re-derived here with epsilon `==`.
    pub computed_upstream: Option<PeerKeyLocation>,

    /// A local client is subscribed to this contract — real local demand.
    pub has_local_client: bool,

    /// At least one downstream peer STRICTLY FARTHER from the contract key than us
    /// holds a live (lease-valid) subscription to us — real forwarded demand from
    /// a peer we are the upstream of. Together with
    /// [`has_local_client`](Self::has_local_client) this is `contract_in_use`, the
    /// interest gate for renewal / collapse.
    ///
    /// # LOAD-BEARING NAMING — must count only STRICTLY-FARTHER subscribers
    ///
    /// Per `hosting-invariants.md` (piece-D converged model), the in-use / renewal
    /// gate MUST count only downstream subscribers **strictly farther** from the
    /// contract key (EXCLUDE the closer / upstream peer). If two mutual co-hosts
    /// each counted the OTHER as a downstream subscriber, each would keep the
    /// other's lease renewed forever and neither chain would ever collapse —
    /// breaking the strict distance-to-key total order that guarantees acyclicity
    /// and collapse termination (design §6 point 2, §4 point 2).
    ///
    /// The pure core cannot enforce the filter — it only consumes this bool. The
    /// **SHADOW-WIRING input-builder (next sub-task) MUST filter the
    /// downstream-subscriber set to peers strictly farther from the key before
    /// setting this**, and OWES a pin test on that builder asserting the
    /// closer/upstream peer is excluded. Named loudly so that obligation is not
    /// silently dropped when the builder is written.
    pub has_farther_downstream_subscriber: bool,

    /// We have the contract state locally (code + state present). Hosting requires
    /// state; a `Subscribe`/`Renew`/`ReRootSearch`/`Retract` may still be desired
    /// without it (they maintain the subscription/link/advertisement, not the
    /// body), but `Announce` never fires without it (reconcile-before-announce).
    pub state_present: bool,

    /// We hold an active upstream subscription lease for this contract (we are a
    /// host wired into the update mesh), as opposed to holding a cached-only copy
    /// or nothing at all.
    pub is_subscribed: bool,

    /// We currently advertise hosting this contract to our neighbors
    /// (`neighbor_hosting.is_hosted_locally`). Gates whether an `Announce` is
    /// still needed and whether a teardown must also `Retract`.
    pub is_advertised: bool,

    /// We are the **locally-verified root/terminus** for this key: a bounded
    /// search finds no strictly-closer host (`Ring::is_subscription_root` /
    /// `no_closer_routable_neighbor`). A LOCAL claim atop the accepted ~5-9%
    /// near-miss floor, not a global-freshness invariant. In well-formed inputs
    /// this is mutually exclusive with a `Some` `computed_upstream` (an upstream
    /// exists ⇒ a strictly-closer host exists ⇒ we are not root).
    pub is_verified_root: bool,

    /// We are actively acquiring an upstream / the post-merge body.
    ///
    /// STEP-3 / piece-D hook: no on-`main` source exists yet
    /// (`spawn_host_state_sync_retry` is a D addition), so this is always `false`
    /// in shadow mode. It counts toward the host role (hosting = state AND
    /// (upstream|acquiring|root)), so a state-present acquiring host still
    /// `Announce`s — while `Announce`'s separate `state_present` guard keeps a
    /// body-less acquiring host from announcing before its body arrives. It also
    /// suppresses a redundant `ReRootSearch` (a search is already in flight). D
    /// can wire it without touching [`reconcile`].
    pub actively_acquiring: bool,
}

/// Compute the desired maintenance actions for one contract from its snapshot.
///
/// Pure: no side effects, no locks, no I/O. Same function serves shadow mode
/// (compare against the current scattered decisions) now and the driver later.
///
/// Desired-state model (spec "Freshness & Propagation" + "The maintenance /
/// reconcile loop"):
/// - **Teardown when not in use runs first and unconditionally on what exists.**
///   `contract_in_use` = a local client OR a strictly-farther downstream
///   subscriber. When it is false we tear down WHATEVER we hold — a lease
///   (`Collapse` + `Unsubscribe` toward the upstream) and/or an advertisement
///   (`Retract`) — independent of `state_present` (teardown needs no body) and,
///   for `Retract`, independent of `is_subscribed` (a root advertises without ever
///   subscribing upstream). This is the interest-gated collapse / #3763 storm fix.
/// - **Otherwise reconcile maintains hosting we already have.** If in use but we
///   neither hold the state nor a lease, there is nothing to form or maintain —
///   initial acquisition is the client GET/PUT/SUBSCRIBE op path's job.
/// - **Renew** an in-use held lease; **Subscribe** when a known upstream exists
///   but we hold no lease; **ReRootSearch** (partition, not collapse) when demand
///   is intact but there is no upstream and we are not the root; **Announce** once
///   the body is present and we hold a host role.
///
/// Deterministic emission order: on teardown, `Collapse` → `Unsubscribe` →
/// `Retract`; otherwise `Renew` → `Subscribe` → `ReRootSearch` → `Announce`.
/// `Renew`/`Subscribe` and `Subscribe`/`ReRootSearch` are each mutually exclusive
/// by construction.
pub(crate) fn reconcile(inputs: &ReconcileInputs) -> Vec<Action> {
    let contract_in_use = inputs.has_local_client || inputs.has_farther_downstream_subscriber;

    // Interest-gated teardown (not in use) — runs FIRST and independent of
    // `state_present` (teardown needs no body). Tear down WHATEVER exists:
    //   - a held lease → `Collapse` (local) + `Unsubscribe` (wire, iff an upstream
    //     exists to notify);
    //   - an advertisement → `Retract`, gated ONLY on `is_advertised`, NOT on
    //     `is_subscribed`. A verified root advertises (`Announce`) without ever
    //     subscribing upstream, so gating `Retract` on `is_subscribed` would leave
    //     its advertisement stale after demand ends — and a stale co-host
    //     advertisement poisons fan-out and upstream selection
    //     (hosting-iff-advertised, invariant 1: advertise iff a fresh in-mesh
    //     host). Placing this ahead of the `!state_present` early return below is
    //     what stops that early return from swallowing a needed `Retract`.
    if !contract_in_use {
        let mut actions = Vec::new();
        if inputs.is_subscribed {
            actions.push(Action::Collapse);
            if inputs.computed_upstream.is_some() {
                actions.push(Action::Unsubscribe);
            }
        }
        if inputs.is_advertised {
            actions.push(Action::Retract);
        }
        return actions;
    }

    // `contract_in_use == true` below.

    // Nothing to form or maintain if we neither hold the state nor a subscription
    // lease — initial acquisition is driven by the client GET/PUT/SUBSCRIBE op
    // path, not this controller. (Runs AFTER the teardown branch above, so a stale
    // advertisement is never swallowed by this early return.)
    if !inputs.state_present && !inputs.is_subscribed {
        return Vec::new();
    }

    // A host role: an upstream link, the verified root, OR an acquisition in
    // flight. `actively_acquiring` is included because acquisition may have already
    // produced the body before the flag cleared, and hosting = state AND
    // (upstream|acquiring|root) — such a host IS hosting and must advertise. The
    // "don't announce before the body" rule is enforced NOT here but by the
    // separate `state_present` guard on the `Announce` arm below.
    let has_host_role =
        inputs.computed_upstream.is_some() || inputs.is_verified_root || inputs.actively_acquiring;

    let mut actions = Vec::new();

    // Level-triggered renewal: keep an in-use held lease alive. Desired-state, not
    // edge "renew now" — the driver owns the timing. Interest-gated: only while in
    // use, so the teardown branch above (not this one) fires once demand ends.
    if inputs.is_subscribed {
        actions.push(Action::Renew);
    }

    // Subscribe: a known upstream exists but we hold no lease yet → link to it.
    // Mutually exclusive with `Renew` (needs `!is_subscribed`) and with
    // `ReRootSearch` (needs a `Some` upstream).
    if inputs.computed_upstream.is_some() && !inputs.is_subscribed {
        actions.push(Action::Subscribe);
    }

    // Re-root (partition, not collapse): no strictly-closer connected co-host, not
    // the root, demand intact → search keyward to re-establish a place. Suppressed
    // while `actively_acquiring` so we do not kick a fresh search on top of one
    // already in flight. Covers both "upstream lost" and "never rooted / first
    // formation".
    if inputs.computed_upstream.is_none() && !inputs.is_verified_root && !inputs.actively_acquiring
    {
        actions.push(Action::ReRootSearch);
    }

    // Announce: only once the body is actually present (this `state_present` guard
    // is the "don't announce before the body" rule — reconcile-before-announce; a
    // body-less acquiring host therefore never announces) AND we hold a host role,
    // and we are not already advertising.
    if inputs.state_present && has_host_role && !inputs.is_advertised {
        actions.push(Action::Announce);
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::PeerKeyLocation;
    use crate::ring::location::Distance;
    use crate::transport::TransportKeypair;
    use std::cmp::Ordering;
    use std::net::SocketAddr;

    /// A dummy computed upstream. Its identity is irrelevant to `reconcile`, which
    /// only tests `computed_upstream.is_some()`.
    fn upstream() -> Option<PeerKeyLocation> {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let pk = TransportKeypair::new().public().clone();
        Some(PeerKeyLocation::new(pk, addr))
    }

    /// Baseline snapshot: state present, no upstream, no interest, not subscribed,
    /// not advertised, not root, not acquiring. Tests override the fields they
    /// care about via struct-update syntax.
    fn base() -> ReconcileInputs {
        ReconcileInputs {
            computed_upstream: None,
            has_local_client: false,
            has_farther_downstream_subscriber: false,
            state_present: true,
            is_subscribed: false,
            is_advertised: false,
            is_verified_root: false,
            actively_acquiring: false,
        }
    }

    #[test]
    fn reconcile_table() {
        use Action::*;
        let cases: Vec<(&str, ReconcileInputs, Vec<Action>)> = vec![
            // --- Empty / no-op edges ---
            (
                "no state, not subscribed, even with demand+upstream ⇒ [] \
                 (initial acquisition is the op path's job, not reconcile)",
                ReconcileInputs {
                    state_present: false,
                    has_local_client: true,
                    computed_upstream: upstream(),
                    ..base()
                },
                vec![],
            ),
            (
                "truly idle: no demand, not subscribed, NOT advertised ⇒ [] \
                 (nothing to tear down)",
                ReconcileInputs { ..base() },
                vec![],
            ),
            // --- P2-A: teardown must retract advertisements even without a lease ---
            (
                "P2-A: advertised, NOT subscribed, no demand ⇒ [Retract] \
                 (stale advertisement must be withdrawn)",
                ReconcileInputs {
                    is_advertised: true,
                    ..base()
                },
                vec![Retract],
            ),
            (
                "P2-A concrete: verified root that announced (advertised, not subscribed) \
                 loses its last client ⇒ [Retract]",
                ReconcileInputs {
                    is_advertised: true,
                    is_verified_root: true,
                    ..base()
                },
                vec![Retract],
            ),
            (
                "P2-A: advertised, not subscribed, no state, no demand ⇒ [Retract] \
                 (teardown needs no body)",
                ReconcileInputs {
                    state_present: false,
                    is_advertised: true,
                    ..base()
                },
                vec![Retract],
            ),
            // --- H1: subscribed but awaiting first state, demand ends ⇒ must collapse ---
            (
                "H1: no state, SUBSCRIBED, demand gone, has upstream ⇒ collapse + unsubscribe \
                 (do NOT leak the upstream subscription)",
                ReconcileInputs {
                    state_present: false,
                    is_subscribed: true,
                    computed_upstream: upstream(),
                    ..base()
                },
                vec![Collapse, Unsubscribe],
            ),
            // --- Collapse / Unsubscribe / Retract (interest-gated teardown) ---
            (
                "M3/L1: demand gone, subscribed, has upstream, advertised \
                 ⇒ collapse + unsubscribe + retract",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    is_subscribed: true,
                    is_advertised: true,
                    ..base()
                },
                vec![Collapse, Unsubscribe, Retract],
            ),
            (
                "demand gone, subscribed, has upstream, not advertised ⇒ collapse + unsubscribe",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    is_subscribed: true,
                    ..base()
                },
                vec![Collapse, Unsubscribe],
            ),
            (
                "demand gone, subscribed, no upstream (root lapsing), advertised \
                 ⇒ collapse + retract (no wire unsubscribe with no upstream)",
                ReconcileInputs {
                    is_subscribed: true,
                    is_verified_root: true,
                    is_advertised: true,
                    ..base()
                },
                vec![Collapse, Retract],
            ),
            // --- Renew: level-triggered, in-use held lease ---
            (
                "steady-state host: upstream, subscribed, advertised, in use ⇒ [Renew]",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    has_local_client: true,
                    is_subscribed: true,
                    is_advertised: true,
                    ..base()
                },
                vec![Renew],
            ),
            // --- Subscribe + Announce (host formation via upstream) ---
            (
                "host-with-upstream, not subscribed, not advertised ⇒ subscribe + announce",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    has_local_client: true,
                    ..base()
                },
                vec![Subscribe, Announce],
            ),
            (
                "host-with-upstream, not subscribed, already advertised ⇒ subscribe only",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    has_farther_downstream_subscriber: true,
                    is_advertised: true,
                    ..base()
                },
                vec![Subscribe],
            ),
            (
                "host-with-upstream, subscribed, not advertised ⇒ renew + announce",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    has_local_client: true,
                    is_subscribed: true,
                    ..base()
                },
                vec![Renew, Announce],
            ),
            // --- Root (no upstream, verified terminus) ---
            (
                "verified root, in use, subscribed, advertised ⇒ [Renew] \
                 (no subscribe, no collapse while in use)",
                ReconcileInputs {
                    has_local_client: true,
                    is_subscribed: true,
                    is_advertised: true,
                    is_verified_root: true,
                    ..base()
                },
                vec![Renew],
            ),
            (
                "verified root, in use, subscribed, not advertised ⇒ renew + announce",
                ReconcileInputs {
                    has_local_client: true,
                    is_subscribed: true,
                    is_verified_root: true,
                    ..base()
                },
                vec![Renew, Announce],
            ),
            (
                "L1: verified root, in use, NOT subscribed, not advertised ⇒ [Announce] \
                 (root has body + demand, advertises; no lease to renew)",
                ReconcileInputs {
                    has_local_client: true,
                    is_verified_root: true,
                    ..base()
                },
                vec![Announce],
            ),
            // --- ReRootSearch (partition vs. collapse) ---
            (
                "re-root: had upstream (subscribed), upstream now None, demand intact, not root \
                 ⇒ renew + re-root (serve-during: keep lease AND re-find, NOT collapse)",
                ReconcileInputs {
                    has_local_client: true,
                    is_subscribed: true,
                    ..base()
                },
                vec![Renew, ReRootSearch],
            ),
            (
                "re-root fresh: demand intact, no upstream, not subscribed, not root \
                 ⇒ [ReRootSearch] (first formation / never rooted)",
                ReconcileInputs {
                    has_farther_downstream_subscriber: true,
                    ..base()
                },
                vec![ReRootSearch],
            ),
            // --- P2-B: a state-present acquiring host still announces ---
            (
                "P2-B: acquiring, in use, no upstream/root, not subscribed, state present, \
                 not advertised ⇒ [Announce] (body arrived, acquiring flag not yet cleared)",
                ReconcileInputs {
                    has_local_client: true,
                    actively_acquiring: true,
                    ..base()
                },
                vec![Announce],
            ),
            (
                "P2-B: acquiring + subscribed, in use, no upstream/root, state present, \
                 not advertised ⇒ [Renew, Announce]",
                ReconcileInputs {
                    has_local_client: true,
                    is_subscribed: true,
                    actively_acquiring: true,
                    ..base()
                },
                vec![Renew, Announce],
            ),
            (
                "M1: acquiring, in use, NO state yet, not subscribed ⇒ [] \
                 (never announce before the body arrives)",
                ReconcileInputs {
                    state_present: false,
                    has_local_client: true,
                    actively_acquiring: true,
                    ..base()
                },
                vec![],
            ),
        ];

        for (name, inputs, expected) in cases {
            assert_eq!(reconcile(&inputs), expected, "case: {name}");
        }
    }

    /// Pin for the `Distance` Eq/Ord gotcha (`ring/location.rs:223-243`): two
    /// distances one ULP apart are epsilon-`==` yet cmp-unequal. `reconcile`
    /// consumes a pre-resolved `Option<PeerKeyLocation>` precisely so no epsilon
    /// `==` distance compare ever happens in this controller; any future
    /// distance-equality test here MUST use `a.cmp(&b) == Ordering::Equal`, never
    /// `==`. This test documents and pins that divergence so a regression that
    /// reaches for `==` on `Distance` is caught.
    #[test]
    fn distance_partialeq_is_fuzzy_but_cmp_is_exact() {
        // 0.3 <= 0.5, so `Distance::new` stores the value verbatim (see
        // `location.rs::Distance::new`). The next representable f64 above 0.3 is
        // one ULP (~5.5e-17) away, which is below `f64::EPSILON` (~2.2e-16).
        let x = Distance::new(0.3);
        let y = Distance::new(f64::from_bits(0.3_f64.to_bits() + 1));

        // Fuzzy PartialEq: within one EPSILON ⇒ treated as equal.
        assert_eq!(
            x, y,
            "epsilon PartialEq treats one-ULP-apart distances as equal"
        );

        // Exact Ord: the same pair is NOT equal — this is the ordering
        // `most_keyward_among` selects the upstream with.
        assert_ne!(
            x.cmp(&y),
            Ordering::Equal,
            "exact cmp does NOT — reconcile must never mix epsilon `==` with this ordering"
        );
    }
}
