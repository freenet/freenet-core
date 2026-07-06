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
/// The minimal set the on-`main` decision sites actually exercise is
/// `Subscribe`, `Unsubscribe`, `Collapse`, `Announce`, `ReRootSearch`.
/// `Retract` is kept for the enum's eventual shape (advertisement withdrawal),
/// but has no driving site on `main` — retraction is dead code per spec — so it
/// is never emitted in shadow mode. (Evict-to-admit is a separate later piece
/// (7-bis) and is intentionally absent.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Desired to host via a known (computed) upstream, but we hold no active
    /// subscription yet → link to the upstream by subscribing toward the key.
    Subscribe,
    /// Send an `Unsubscribe` to the (computed) upstream, collapsing the chain one
    /// hop keyward. Emitted alongside `Collapse` when demand is gone AND a
    /// computed upstream exists to notify.
    Unsubscribe,
    /// Last interest is gone (interest-gated collapse): tear down our own
    /// subscription lease and stop hosting inward. Distinct from `Unsubscribe`,
    /// which is the wire message to the upstream; `Collapse` is the local
    /// teardown and fires even when there is no upstream to notify (e.g. a root
    /// whose demand lapsed).
    Collapse,
    /// We host (state present + a reason to host) but have not advertised it to
    /// our neighbors yet → advertise hosting so co-hosts can fan out updates and
    /// upstream selection can find us.
    Announce,
    /// Withdraw a hosting advertisement. Kept for enum shape only — no on-`main`
    /// site drives retraction (dead code per spec), so it is never emitted in
    /// shadow mode.
    Retract,
    /// Demand is intact but our upstream vanished (or was never found) and we are
    /// not the verified root → search keyward to re-establish a place in the
    /// mesh. This is the partition-vs-collapse distinction: with demand present a
    /// lost upstream means re-root, NOT collapse a still-wanted chain.
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

    /// At least one downstream peer holds a live (lease-valid) subscription to us
    /// for this contract — real forwarded demand. Together with
    /// [`has_local_client`](Self::has_local_client) this is `contract_in_use`
    /// (`ring/hosting.rs::contract_in_use`), the interest gate for renewal /
    /// collapse.
    ///
    /// STEP-3 / piece-D hook: D refines this to "downstream STRICTLY farther from
    /// the key" (the §6 guard); modeled here as a plain bool so D can tighten the
    /// input without touching [`reconcile`].
    pub has_downstream_subscriber: bool,

    /// We have the contract state locally (code + state present). Hosting requires
    /// state; without it there is nothing to host, announce, or collapse, and the
    /// acquire/GET path — not this controller — is responsible for fetching it.
    pub state_present: bool,

    /// We hold an active upstream subscription lease for this contract (we are a
    /// host wired into the update mesh), as opposed to holding a cached-only copy.
    pub is_subscribed: bool,

    /// We currently advertise hosting this contract to our neighbors
    /// (`neighbor_hosting.is_hosted_locally`). Gates whether an `Announce` is
    /// still needed.
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
    /// in shadow mode. The desired-hosting predicate keeps the arm so D can wire
    /// it without touching [`reconcile`].
    pub actively_acquiring: bool,
}

/// Compute the desired maintenance actions for one contract from its snapshot.
///
/// Pure: no side effects, no locks, no I/O. Same function serves shadow mode
/// (compare against the current scattered decisions) now and the driver later.
///
/// Desired-state model (spec "Freshness & Propagation" + "The maintenance /
/// reconcile loop"):
/// - **Hosting** is desired iff we have the state AND one of {a computed upstream,
///   actively acquiring one, being the locally-verified root}.
/// - **Renewal / collapse** is interest-gated: `contract_in_use` = a local client
///   OR a downstream subscriber. When the last interest goes, the chain collapses
///   inward (this is the #3763 storm fix — subscriptions track active demand, not
///   cache size).
/// - **Partition vs. collapse**: with demand intact but no computed upstream and
///   not the root, the upstream vanished → re-root keyward, never collapse.
///
/// The returned actions are a diff of desired-vs-actual and are emitted in a
/// deterministic order (`Collapse` before `Unsubscribe`; `ReRootSearch` before
/// `Subscribe` before `Announce`). `ReRootSearch` and `Subscribe` are mutually
/// exclusive by construction (one needs `computed_upstream.is_none()`, the other
/// `is_some()`).
pub(crate) fn reconcile(inputs: &ReconcileInputs) -> Vec<Action> {
    // No local state ⇒ nothing to host, announce, or collapse. A missing body is
    // the acquire/GET path's job, not the reconcile controller's.
    if !inputs.state_present {
        return Vec::new();
    }

    let contract_in_use = inputs.has_local_client || inputs.has_downstream_subscriber;

    // Interest-gated collapse: the last interest is gone → tear down inward. Drop
    // our own subscription lease (`Collapse`); if a computed upstream exists, also
    // send it an `Unsubscribe` so the chain collapses one hop keyward. Nothing to
    // collapse if we never held a subscription — a cached idle copy is left to the
    // eviction path, not reconcile. This mirrors the on-`main`
    // `should_unsubscribe_upstream` + `send_unsubscribe_upstream` behavior, which
    // acts only when subscribed and only sends the wire message when an upstream
    // is located.
    if !contract_in_use {
        let mut actions = Vec::new();
        if inputs.is_subscribed {
            actions.push(Action::Collapse);
            if inputs.computed_upstream.is_some() {
                actions.push(Action::Unsubscribe);
            }
        }
        return actions;
    }

    // `contract_in_use == true` below.

    // Desired hosting: state present (guaranteed above) AND a reason to host.
    let desired_hosting =
        inputs.computed_upstream.is_some() || inputs.actively_acquiring || inputs.is_verified_root;

    let mut actions = Vec::new();

    // Upstream lost or never found, demand intact, and we are not the root →
    // re-root keyward (partition, not collapse). Mutually exclusive with
    // `Subscribe`, which requires a `Some` upstream.
    if inputs.computed_upstream.is_none() && !inputs.is_verified_root {
        actions.push(Action::ReRootSearch);
    }

    // Want to host via a known upstream but hold no subscription yet → subscribe.
    if inputs.computed_upstream.is_some() && !inputs.is_subscribed {
        actions.push(Action::Subscribe);
    }

    // We host (desired) but have not advertised it → announce. STEP-3 will order
    // this AFTER acquisition (reconcile-before-announce); here we only compute the
    // action SET, not its application order.
    if desired_hosting && !inputs.is_advertised {
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
            has_downstream_subscriber: false,
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
                "no state ⇒ no actions (nothing to host), even with demand + upstream",
                ReconcileInputs {
                    state_present: false,
                    has_local_client: true,
                    computed_upstream: upstream(),
                    ..base()
                },
                vec![],
            ),
            (
                "cached idle copy: no demand, not subscribed ⇒ no actions \
                 (left to eviction, not reconcile)",
                ReconcileInputs {
                    is_advertised: true,
                    ..base()
                },
                vec![],
            ),
            (
                "steady-state host: upstream, subscribed, advertised, in use ⇒ no actions",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    has_local_client: true,
                    is_subscribed: true,
                    is_advertised: true,
                    ..base()
                },
                vec![],
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
                    has_downstream_subscriber: true,
                    is_advertised: true,
                    ..base()
                },
                vec![Subscribe],
            ),
            (
                "host-with-upstream, subscribed, not advertised ⇒ announce only",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    has_local_client: true,
                    is_subscribed: true,
                    ..base()
                },
                vec![Announce],
            ),
            // --- Root (no upstream, verified terminus) ---
            (
                "verified root, in use, advertised ⇒ no actions \
                 (no subscribe, no collapse while in use)",
                ReconcileInputs {
                    has_local_client: true,
                    is_subscribed: true,
                    is_advertised: true,
                    is_verified_root: true,
                    ..base()
                },
                vec![],
            ),
            (
                "verified root, in use, not advertised ⇒ announce (root still advertises)",
                ReconcileInputs {
                    has_local_client: true,
                    is_subscribed: true,
                    is_verified_root: true,
                    ..base()
                },
                vec![Announce],
            ),
            // --- Collapse / Unsubscribe (interest-gated teardown) ---
            (
                "demand gone, subscribed, has upstream ⇒ collapse + unsubscribe",
                ReconcileInputs {
                    computed_upstream: upstream(),
                    is_subscribed: true,
                    ..base()
                },
                vec![Collapse, Unsubscribe],
            ),
            (
                "demand gone, subscribed, no upstream (root lapsing) ⇒ collapse only",
                ReconcileInputs {
                    is_subscribed: true,
                    is_verified_root: true,
                    ..base()
                },
                vec![Collapse],
            ),
            (
                "demand gone, not subscribed ⇒ no actions (nothing to tear down)",
                ReconcileInputs { ..base() },
                vec![],
            ),
            // --- ReRootSearch (partition vs. collapse) ---
            (
                "had upstream (subscribed), upstream now None, demand intact, not root \
                 ⇒ re-root (NOT collapse)",
                ReconcileInputs {
                    has_local_client: true,
                    is_subscribed: true,
                    ..base()
                },
                vec![ReRootSearch],
            ),
            (
                "demand intact, no upstream, not subscribed, not root ⇒ re-root (find a place)",
                ReconcileInputs {
                    has_downstream_subscriber: true,
                    ..base()
                },
                vec![ReRootSearch],
            ),
            // --- Piece-D hook: actively_acquiring keeps the host arm alive ---
            (
                "acquiring (D-hook), no upstream, not root, in use, not advertised \
                 ⇒ re-root + announce",
                ReconcileInputs {
                    has_local_client: true,
                    actively_acquiring: true,
                    ..base()
                },
                vec![ReRootSearch, Announce],
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
