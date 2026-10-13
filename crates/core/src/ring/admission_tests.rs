//! Tests for the two-budget CONNECT admission (#5858): the onboarding path,
//! the requester's bar, and the provisional-link lifecycle.

use super::*;
use crate::topology::rate::Rate;
use crate::transport::TransportKeypair;

/// A peer at location 0.5 with the lattice off (`max_connections` is below the
/// lattice floor and the force override is cleared), so the self-interest and
/// onboarding paths are observed without the lattice clause interfering.
fn manager(min: usize, max: usize) -> ConnectionManager {
    set_nn_lattice_force_active(false);
    let keypair = TransportKeypair::new();
    let own_location = AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes()));
    ConnectionManager::init(
        Rate::new_per_second(1_000_000.0),
        Rate::new_per_second(1_000_000.0),
        min,
        max,
        7,
        (keypair.public().clone(), Some(addr(1)), own_location),
        false,
        10,
        Duration::from_secs(60),
        0,
    )
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([10, 1, 0, 1], port))
}

fn connect(cm: &ConnectionManager, port: u16, loc: f64) -> PeerKeyLocation {
    let keypair = TransportKeypair::new();
    assert!(cm.add_connection(
        Location::new(loc),
        addr(port),
        keypair.public().clone(),
        false
    ));
    PeerKeyLocation::new(keypair.public().clone(), addr(port))
}

/// Fill the evaluator window with a winning score so every later legacy
/// candidate at a worse-or-equal location loses it.
fn saturate_window(cm: &ConnectionManager) {
    let first = cm.evaluate_admission(Location::new(0.75), addr(900), RequesterNeed::Unknown, None);
    assert_eq!(
        first,
        AdmissionCode::AcceptedSelfInterest,
        "empty window admits"
    );
    let second =
        cm.evaluate_admission(Location::new(0.75), addr(901), RequesterNeed::Unknown, None);
    assert_eq!(
        second,
        AdmissionCode::RejectedEvaluatorWindow,
        "an equal score does not beat the window"
    );
}

#[test]
fn short_requester_is_admitted_despite_a_saturated_evaluator_window() {
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    saturate_window(&cm);

    let legacy = cm.evaluate_admission(Location::new(0.75), addr(30), RequesterNeed::Unknown, None);
    assert_eq!(legacy, AdmissionCode::RejectedEvaluatorWindow);

    let short = cm.evaluate_admission(Location::new(0.75), addr(31), RequesterNeed::Short(0), None);
    assert_eq!(short, AdmissionCode::AcceptedOnboarding);
    assert!(cm.is_provisional(addr(31)));
    assert_eq!(cm.admission_stats().admitted_onboarding, 1);
}

/// Onboarding never records into the evaluator window: after short
/// requesters are admitted, a legacy candidate with the SAME score as one of
/// them is still judged against an empty window and admitted.
#[test]
fn onboarding_does_not_pollute_the_evaluator_window() {
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    for (port, loc) in [(40, 0.75), (41, 0.8)] {
        assert_eq!(
            cm.evaluate_admission(
                Location::new(loc),
                addr(port),
                RequesterNeed::Short(0),
                None
            ),
            AdmissionCode::AcceptedOnboarding
        );
    }
    let legacy = cm.evaluate_admission(Location::new(0.75), addr(42), RequesterNeed::Unknown, None);
    assert_eq!(
        legacy,
        AdmissionCode::AcceptedSelfInterest,
        "a needy score recorded into the window would have refused this equal-score candidate"
    );
}

#[test]
fn onboarding_budget_is_bounded_and_frees_on_a_failed_handshake() {
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    saturate_window(&cm);
    let start = Instant::now();
    let short_at = |port: u16, loc: f64, now: Instant| {
        cm.evaluate_admission_at(
            Location::new(loc),
            addr(port),
            RequesterNeed::Short(0),
            None,
            now,
        )
    };
    for (port, loc) in [(50, 0.8), (51, 0.85)] {
        assert_eq!(
            short_at(port, loc, start),
            AdmissionCode::AcceptedOnboarding
        );
    }
    // No slot left; the window (holding 0.75's score) also refuses the same
    // score, so the request is refused with the onboarding cause.
    assert_eq!(
        short_at(60, 0.75, start),
        AdmissionCode::RejectedOnboardingBudgetExhausted
    );
    assert_eq!(
        cm.admission_stats().provisional_in_use,
        ONBOARDING_PROVISIONAL_SLOTS as u64
    );

    // The first joiner's hole punch fails: its slot is freed at once, but the
    // token it spent is not refunded...
    cm.prune_in_transit_connection(addr(50));
    assert!(!cm.is_provisional(addr(50)));
    assert_eq!(
        short_at(61, 0.75, start + Duration::from_secs(1)),
        AdmissionCode::RejectedOnboardingRateLimited
    );
    // ...so the next admission waits for the refill.
    assert_eq!(
        short_at(
            62,
            0.75,
            start + crate::ring::admission::onboarding_refill_interval() + Duration::from_secs(1)
        ),
        AdmissionCode::AcceptedOnboarding
    );
}

/// Review M3: with no onboarding slot to give, a short requester still
/// competes on self-interest exactly as a legacy request would, so being short
/// can never make a joiner worse off than not saying so.
#[test]
fn short_requester_without_a_slot_falls_back_to_self_interest() {
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    for (port, loc) in [(63, 0.8), (64, 0.85)] {
        assert_eq!(
            cm.evaluate_admission(
                Location::new(loc),
                addr(port),
                RequesterNeed::Short(0),
                None
            ),
            AdmissionCode::AcceptedOnboarding
        );
    }
    // Slots full; the window is empty, so self-interest admits it.
    assert_eq!(
        cm.evaluate_admission(Location::new(0.75), addr(65), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedSelfInterest
    );
    assert!(
        !cm.is_provisional(addr(65)),
        "a self-interest admission is not provisional"
    );
}

/// A second slot is not given to the same masked location (one host using
/// several ports), and the request falls back to self-interest instead.
#[test]
fn one_onboarding_slot_per_location_at_the_acceptor() {
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    saturate_window(&cm);
    assert_eq!(
        cm.evaluate_admission(Location::new(0.75), addr(66), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedOnboarding
    );
    assert_eq!(
        cm.evaluate_admission(Location::new(0.75), addr(67), RequesterNeed::Short(0), None),
        AdmissionCode::RejectedOnboardingLocationTaken
    );
}

/// A short requester that already holds many connections may not take the
/// last slot, which is kept for the nearly isolated.
#[test]
fn the_last_slot_is_kept_for_nearly_isolated_requesters() {
    let cm = manager(20, 40);
    for i in 0..20u16 {
        connect(&cm, 100 + i, 0.02 + 0.045 * i as f64);
    }
    saturate_window(&cm);
    let topping_up = RequesterNeed::Short(18);
    assert_eq!(
        cm.evaluate_admission(Location::new(0.31), addr(68), topping_up, None),
        AdmissionCode::AcceptedOnboarding
    );
    assert_eq!(
        cm.evaluate_admission(Location::new(0.75), addr(69), topping_up, None),
        AdmissionCode::RejectedOnboardingBudgetExhausted
    );
    assert_eq!(
        cm.evaluate_admission(Location::new(0.33), addr(70), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedOnboarding
    );
}

/// Onboarding holds only while the acceptor is below `max_connections`.
#[test]
fn short_requester_is_refused_at_capacity() {
    let cm = manager(1, 4);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    // open 3 + this candidate = 4 = max.
    assert_eq!(
        cm.evaluate_admission(Location::new(0.8), addr(70), RequesterNeed::Short(0), None),
        AdmissionCode::RejectedAtCapacity
    );
    assert!(!cm.is_provisional(addr(70)));
}

/// Below `min_connections` a short requester takes the ordinary
/// (self-interest) admission when the soft filter allows it, keeping the
/// onboarding slots for acceptors that are full of their own choosing.
#[test]
fn below_min_short_requester_uses_self_interest_first() {
    let cm = manager(10, 20);
    connect(&cm, 10, 0.45);
    assert_eq!(
        cm.evaluate_admission(Location::new(0.8), addr(71), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedSelfInterest
    );
    assert!(!cm.is_provisional(addr(71)));
}

#[test]
fn requester_bar_rejects_an_acceptor_that_is_not_closer() {
    let _bar = BarEnforced::on();
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    let target = Some(Location::new(0.6)); // ~0.1 from this peer at 0.5
    assert_eq!(
        cm.evaluate_admission(
            Location::new(0.9),
            addr(80),
            RequesterNeed::Bar(0.08),
            target
        ),
        AdmissionCode::RejectedBarUnmet,
        "a farther acceptor does not improve the requester's coverage"
    );
    assert_eq!(cm.admission_stats().rejected_bar_unmet, 1);
    // A refused bar touched neither the window nor the onboarding budget: the
    // next legacy candidate meets an empty window.
    assert_eq!(
        cm.evaluate_admission(Location::new(0.9), addr(81), RequesterNeed::Unknown, None),
        AdmissionCode::AcceptedSelfInterest
    );
    // A bar this peer beats goes through self-interest (the window now
    // refuses an equal score) and never onboarding.
    assert_eq!(
        cm.evaluate_admission(
            Location::new(0.9),
            addr(82),
            RequesterNeed::Bar(0.2),
            target
        ),
        AdmissionCode::RejectedEvaluatorWindow
    );
    assert!(!cm.is_provisional(addr(82)));
}

#[test]
fn requester_need_is_short_below_min_and_a_side_bar_above() {
    let cm = manager(3, 20);
    assert_eq!(
        cm.requester_need(Location::new(0.7)),
        RequesterNeed::Short(0)
    );
    for (i, loc) in [0.45, 0.62, 0.9, 0.3].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    // Target 0.7 lies on the successor side; the successor-side connections
    // are 0.62 (0.08 away) and 0.9 (0.2 away).
    let RequesterNeed::Bar(bar) = cm.requester_need(Location::new(0.7)) else {
        panic!("a well-connected requester states a bar");
    };
    assert!((bar - 0.08).abs() < 1e-9, "bar {bar}");
    // Predecessor side: 0.45 and 0.3 against target 0.35.
    let RequesterNeed::Bar(bar) = cm.requester_need(Location::new(0.35)) else {
        panic!("bar expected");
    };
    assert!((bar - 0.05).abs() < 1e-9, "bar {bar}");
    // Aimed at own location (a lattice probe): the looser of the per-side
    // nearest distances, 0.12 (successor 0.62) vs 0.05 (predecessor 0.45).
    let RequesterNeed::Bar(bar) = cm.requester_need(Location::new(0.5)) else {
        panic!("bar expected");
    };
    assert!((bar - 0.12).abs() < 1e-9, "bar {bar}");
}

#[test]
fn requester_need_without_a_connection_on_the_target_side_is_unconstrained() {
    let cm = manager(2, 20);
    connect(&cm, 10, 0.4);
    connect(&cm, 11, 0.3);
    assert_eq!(
        cm.requester_need(Location::new(0.7)),
        RequesterNeed::Bar(UNCONSTRAINED_BAR)
    );
}

/// Admit `port` on an onboarding slot and establish its connection.
fn onboard(cm: &ConnectionManager, port: u16, loc: f64) {
    assert_eq!(
        cm.evaluate_admission(
            Location::new(loc),
            addr(port),
            RequesterNeed::Short(0),
            None
        ),
        AdmissionCode::AcceptedOnboarding
    );
    let keypair = TransportKeypair::new();
    assert!(cm.add_connection(
        Location::new(loc),
        addr(port),
        keypair.public().clone(),
        true
    ));
}

/// Six ordinary connections with routing traffic, above `min_connections`,
/// and a saturated evaluator window.
fn busy_manager() -> ConnectionManager {
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.3, 0.7, 0.15, 0.85].into_iter().enumerate() {
        let peer = connect(&cm, 10 + i as u16, loc);
        for _ in 0..5 {
            cm.topology_manager
                .write()
                .outbound_request_counter
                .record_request(peer.clone());
        }
    }
    saturate_window(&cm);
    cm
}

/// Put a score no candidate can beat into the evaluator window at `at`.
fn raise_window_bar(cm: &ConnectionManager, at: Instant) {
    cm.topology_manager
        .write()
        .evaluate_new_connection_with_score(f64::MAX, at);
}

fn dropped_addrs(drops: &[crate::ring::admission::ProbationDrop]) -> Vec<SocketAddr> {
    drops.iter().map(|d| d.addr).collect()
}

/// Review S1: at the end of probation the link is judged as a FRESH
/// candidate; one that would not beat the evaluator window is dropped.
#[test]
fn provisional_link_is_dropped_when_it_would_not_win_the_window() {
    let cm = busy_manager();
    let admitted = Instant::now();
    onboard(&cm, 90, 0.72);

    assert!(cm.review_provisional_links(admitted).is_empty());
    assert!(
        cm.review_provisional_links(admitted + ONBOARDING_PROBATION - Duration::from_secs(1))
            .is_empty(),
        "still on probation"
    );
    assert!(cm.is_provisional(addr(90)));

    let review_at = admitted + ONBOARDING_PROBATION + Duration::from_secs(1);
    raise_window_bar(&cm, review_at);
    let drops = cm.review_provisional_links(review_at);
    assert_eq!(dropped_addrs(&drops), vec![addr(90)]);
    assert!(
        !cm.is_provisional(addr(90)),
        "the drop decision holds the slot"
    );
    for drop in drops {
        cm.confirm_probation_drop(drop, true);
    }
    let stats = cm.admission_stats();
    assert_eq!(
        (stats.provisional_dropped, stats.provisional_graduated),
        (1, 0)
    );
}

/// A link that would win the window as a fresh candidate graduates, and its
/// score is recorded there like an arrival's.
#[test]
fn provisional_link_graduates_when_it_would_win_the_window() {
    let cm = busy_manager();
    let admitted = Instant::now();
    onboard(&cm, 91, 0.98);
    let review_at = admitted + ONBOARDING_PROBATION + Duration::from_secs(1);
    // The window from setup has expired by now: an empty window admits.
    let drops = cm.review_provisional_links(review_at);
    assert!(drops.is_empty(), "kept: {drops:?}");
    assert!(!cm.is_provisional(addr(91)), "graduation frees the slot");
    assert_eq!(cm.admission_stats().provisional_graduated, 1);
    // The graduate's score is now in the window: an equal one loses.
    assert_eq!(
        cm.evaluate_admission_at(
            Location::new(0.98),
            addr(98),
            RequesterNeed::Unknown,
            None,
            review_at
        ),
        AdmissionCode::RejectedEvaluatorWindow
    );
}

/// At most ONBOARDING_GRADUATIONS_PER_WINDOW links graduate on their merits
/// per window, even when each would win it.
#[test]
fn graduations_on_merit_are_capped_per_window() {
    let cm = busy_manager();
    let admitted = Instant::now();
    onboard(&cm, 91, 0.98);
    onboard(&cm, 92, 0.6);
    let review_at = admitted + ONBOARDING_PROBATION + Duration::from_secs(1);
    let drops = cm.review_provisional_links(review_at);
    assert_eq!(
        drops.len(),
        1,
        "one graduates, the other is capped: {drops:?}"
    );
    assert_eq!(cm.admission_stats().provisional_graduated, 1);
}

/// A drop that cannot be queued keeps the link on probation (review S4).
#[test]
fn an_unqueued_probation_drop_keeps_the_link_on_probation() {
    let cm = busy_manager();
    let admitted = Instant::now();
    onboard(&cm, 90, 0.72);
    let review_at = admitted + ONBOARDING_PROBATION + Duration::from_secs(1);
    raise_window_bar(&cm, review_at);
    let drops = cm.review_provisional_links(review_at);
    assert_eq!(drops.len(), 1);
    for drop in drops {
        cm.confirm_probation_drop(drop, false);
    }
    assert!(
        cm.is_provisional(addr(90)),
        "restored, reviewed again next tick"
    );
    assert_eq!(cm.admission_stats().provisional_dropped, 0);
}

/// When dropping it would take the acceptor below `min_connections`, its
/// self-interest keeps any link, so the provisional link graduates even though
/// it would not win the window.
#[test]
fn provisional_link_graduates_when_the_acceptor_needs_it() {
    let cm = busy_manager();
    let admitted = Instant::now();
    onboard(&cm, 92, 0.85);
    // Other links leave during probation: 7 -> 3 = min_connections.
    for port in [10, 11, 12, 13] {
        assert!(cm.prune_alive_connection(addr(port)).is_some());
    }
    assert_eq!(cm.connection_count(), cm.min_connections);
    let review_at = admitted + ONBOARDING_PROBATION + Duration::from_secs(1);
    raise_window_bar(&cm, review_at);
    assert!(cm.review_provisional_links(review_at).is_empty());
    assert_eq!(cm.admission_stats().provisional_graduated, 1);
}

/// A slot whose connection never establishes is freed once its reservation
/// has gone stale, well before the end of probation.
#[test]
fn unestablished_provisional_slot_is_released_after_the_reservation_ttl() {
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    let admitted = Instant::now();
    assert_eq!(
        cm.evaluate_admission(Location::new(0.8), addr(93), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedOnboarding
    );
    assert!(
        cm.review_provisional_links(admitted + Duration::from_secs(5))
            .is_empty()
    );
    assert!(cm.is_provisional(addr(93)), "handshake still in flight");
    let later = admitted + PENDING_RESERVATION_TTL + Duration::from_secs(5);
    assert!(
        cm.review_provisional_links(later).is_empty(),
        "nothing to drop"
    );
    assert!(!cm.is_provisional(addr(93)));
    assert_eq!(cm.admission_stats().provisional_released_unestablished, 1);
}

/// Old enough to be a prune substitute.
fn prunable_at(admitted: Instant) -> Instant {
    admitted + crate::ring::admission::ONBOARDING_MIN_AGE_BEFORE_PRUNE + Duration::from_secs(1)
}

#[test]
fn a_link_on_probation_is_first_in_line_for_pruning() {
    let cm = busy_manager();
    let recommended = cm.get_peer_by_addr(addr(10)).expect("connected");
    let admitted = Instant::now();
    assert!(
        cm.probation_prune_substitute(&recommended, prunable_at(admitted))
            .is_none(),
        "nothing on probation"
    );
    onboard(&cm, 94, 0.72);
    let substitute = cm
        .probation_prune_substitute(&recommended, prunable_at(admitted))
        .expect("the provisional link is shed instead");
    assert_eq!(substitute.socket_addr(), Some(addr(94)));
    // The slot is freed by the prune that executes the drop, not by the
    // choice (a swap's drop is deferred and may never happen).
    assert!(cm.is_provisional(addr(94)));
    cm.prune_alive_connection(addr(94));
    assert!(!cm.is_provisional(addr(94)));
    assert_eq!(cm.admission_stats().provisional_pruned, 1);
}

/// Review S5: a link that has just landed is not shed by the next prune.
#[test]
fn a_young_link_on_probation_is_not_substituted() {
    let cm = busy_manager();
    let recommended = cm.get_peer_by_addr(addr(10)).expect("connected");
    let admitted = Instant::now();
    onboard(&cm, 94, 0.72);
    assert!(
        cm.probation_prune_substitute(&recommended, admitted + Duration::from_secs(5))
            .is_none()
    );
}

/// Only an ESTABLISHED link on probation is a substitute: a slot whose
/// handshake is still in flight has no connection to shed.
#[test]
fn an_unestablished_slot_is_not_substituted() {
    let cm = busy_manager();
    let recommended = cm.get_peer_by_addr(addr(11)).expect("connected");
    let admitted = Instant::now();
    // At the location of an established link (0.45, port 10): an address
    // lookup by location alone would hand back THAT peer, not the joiner.
    assert_eq!(
        cm.evaluate_admission(Location::new(0.45), addr(99), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedOnboarding
    );
    assert!(
        cm.probation_prune_substitute(&recommended, prunable_at(admitted))
            .is_none()
    );
}

/// A substitute the prune had already named further down is dropped once.
#[test]
fn a_substitute_already_named_by_the_prune_is_dropped_once() {
    let a = PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr(1));
    let b = PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr(2));
    let s = PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr(3));
    let mut victims = vec![a, s.clone(), b.clone()];
    crate::ring::substitute_first_victim(&mut victims, s.clone());
    assert_eq!(victims, vec![s.clone(), b]);
    let mut empty = Vec::new();
    crate::ring::substitute_first_victim(&mut empty, s.clone());
    assert_eq!(empty, vec![s]);
}

/// A provisional link that is a per-side nearest lattice edge keeps the
/// lattice's retention protection: it is not the prune substitute.
#[test]
fn a_lattice_edge_on_probation_is_not_substituted_into_a_prune() {
    let _guard = LatticeGuard;
    let cm = busy_manager();
    let admitted = Instant::now();
    // Admitted for onboarding while the lattice is inactive; once it is
    // active, 0.52 is the nearest successor (closer than 0.55).
    onboard(&cm, 95, 0.52);
    set_nn_lattice_enabled(true);
    let recommended = cm.get_peer_by_addr(addr(10)).expect("connected");
    assert!(
        cm.probation_prune_substitute(&recommended, prunable_at(admitted))
            .is_none()
    );
    assert!(cm.is_provisional(addr(95)));
}

/// With the lattice active, a short requester that would be a new per-side
/// nearest neighbour is admitted by the lattice clause and does not use up
/// an onboarding slot.
#[test]
fn a_short_lattice_candidate_does_not_spend_an_onboarding_slot() {
    let _guard = LatticeGuard;
    let cm = busy_manager();
    set_nn_lattice_enabled(true);
    assert_eq!(
        cm.evaluate_admission(Location::new(0.52), addr(85), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedLattice
    );
    assert!(!cm.is_provisional(addr(85)));
}

/// The ConnectionManager-level rename hook: a corrected peer address keeps
/// its link on probation.
#[test]
fn an_identity_update_keeps_the_link_on_probation() {
    let cm = busy_manager();
    onboard(&cm, 86, 0.72);
    let keypair = TransportKeypair::new();
    assert!(cm.update_peer_identity(addr(86), addr(87), keypair.public().clone()));
    assert!(!cm.is_provisional(addr(86)));
    assert!(cm.is_provisional(addr(87)));
}

/// Enforce the requester's bar on this thread for the test's duration (it
/// ships off, `ENFORCE_REQUESTER_BAR`), restoring the default on drop.
struct BarEnforced;
impl BarEnforced {
    fn on() -> Self {
        crate::ring::admission::set_requester_bar_enforced(Some(true));
        BarEnforced
    }
}
impl Drop for BarEnforced {
    fn drop(&mut self) {
        crate::ring::admission::set_requester_bar_enforced(None);
    }
}

/// D1 (owner decision): the bar ships NOT enforced. A requester whose bar
/// this peer misses is decided on self-interest, and the refusal enforcement
/// would have made is counted instead.
#[test]
fn an_unenforced_bar_is_decided_on_self_interest_and_counted() {
    assert!(
        !crate::ring::admission::requester_bar_enforced(),
        "the bar ships unenforced until the simulation shows a net benefit (#5858)"
    );
    let cm = manager(3, 20);
    for (i, loc) in [0.45, 0.55, 0.2].into_iter().enumerate() {
        connect(&cm, 10 + i as u16, loc);
    }
    let target = Location::new(0.6); // ~0.1 from this peer at 0.5
    assert_eq!(
        cm.evaluate_admission(
            Location::new(0.9),
            addr(80),
            RequesterNeed::Bar(0.08),
            Some(target)
        ),
        AdmissionCode::AcceptedSelfInterest,
        "an empty window admits it on self-interest"
    );
    cm.record_bar_shadow(RequesterNeed::Bar(0.08), target);
    cm.record_bar_shadow(RequesterNeed::Bar(0.2), target);
    cm.record_bar_shadow(RequesterNeed::Short(0), target);
    let stats = cm.admission_stats();
    assert_eq!(
        stats.bar_unmet_not_enforced, 1,
        "only the bar this peer misses"
    );
    assert_eq!(stats.rejected_bar_unmet, 0);

    // With enforcement on, the shadow counter stays out of it.
    let _bar = BarEnforced::on();
    cm.record_bar_shadow(RequesterNeed::Bar(0.08), target);
    assert_eq!(cm.admission_stats().bar_unmet_not_enforced, 1);
}

struct LatticeGuard;
impl Drop for LatticeGuard {
    fn drop(&mut self) {
        set_nn_lattice_enabled(true);
        set_nn_lattice_force_active(false);
    }
}

/// The promotion path `handle_successful_connection` takes (#5858 review M1):
/// it consumes the reservation, then adds the connection. The onboarding slot
/// must survive that, or the link leaves probation the moment it connects and
/// the slot bound limits only handshakes in flight.
#[test]
fn successful_promotion_keeps_the_link_on_probation() {
    let cm = busy_manager();
    assert_eq!(
        cm.evaluate_admission(Location::new(0.72), addr(96), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedOnboarding
    );
    let loc = cm
        .take_reservation_for_promotion(addr(96))
        .expect("the reservation recorded a location");
    let keypair = TransportKeypair::new();
    assert!(cm.add_connection(loc, addr(96), keypair.public().clone(), true));
    assert!(
        cm.is_provisional(addr(96)),
        "still on probation after promotion"
    );
    assert_eq!(cm.admission_stats().onboarding_established, 1);
    assert_eq!(cm.admission_stats().provisional_released_unestablished, 0);

    // A real prune of the established link frees the slot, counted as pruned.
    cm.prune_alive_connection(addr(96));
    assert!(!cm.is_provisional(addr(96)));
    assert_eq!(cm.admission_stats().provisional_pruned, 1);
}

/// A failed handshake frees the slot and is counted as never established.
#[test]
fn failed_handshake_releases_the_slot_as_unestablished() {
    let cm = busy_manager();
    assert_eq!(
        cm.evaluate_admission(Location::new(0.72), addr(97), RequesterNeed::Short(0), None),
        AdmissionCode::AcceptedOnboarding
    );
    cm.prune_in_transit_connection(addr(97));
    assert!(!cm.is_provisional(addr(97)));
    assert_eq!(cm.admission_stats().provisional_released_unestablished, 1);
}

/// Source pin (review M1): NEITHER promotion path frees an onboarding slot
/// before adding the connection to the ring. `handle_successful_connection`
/// consumes its reservation with `take_reservation_for_promotion`; the
/// `handle_connect_peer` path (an existing transport reused, a transient
/// promoted) adds the connection without pruning. A prune on either path
/// takes the link off probation the moment it connects.
#[test]
fn neither_promotion_path_frees_the_onboarding_slot() {
    const LIFECYCLE_SRC: &str =
        include_str!("../node/network_bridge/p2p_protoc/connection_lifecycle.rs");
    fn region_to_add<'a>(src: &'a str, anchor: &str) -> Vec<&'a str> {
        let start = src
            .find(anchor)
            .unwrap_or_else(|| panic!("anchor {anchor}"));
        let end = start
            + src[start..]
                .find("add_connection_reporting(")
                .expect("the promotion adds the connection");
        src[start..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect()
    }
    let success = region_to_add(LIFECYCLE_SRC, "async fn handle_successful_connection(");
    let reuse = region_to_add(
        LIFECYCLE_SRC,
        "connect_peer: reusing existing transport / promoting transient if present",
    );
    for (name, code) in [
        ("handle_successful_connection", &success),
        ("handle_connect_peer", &reuse),
    ] {
        assert!(
            !code.iter().any(|l| {
                l.contains("prune_in_transit_connection(") || l.contains("prune_alive_connection(")
            }),
            "{name}: a prune before promotion takes onboarding links off probation"
        );
    }
    assert!(
        success
            .iter()
            .any(|l| l.contains("connection_manager.take_reservation_for_promotion(peer_addr)")),
        "the success path must consume its reservation without freeing the slot"
    );
}

/// Review S7: a relay may decide one request several times (after an uphill
/// rejection it re-decides). It is counted refused at most once and admitted
/// at most once, so the counters stay per request.
#[test]
fn a_relay_counts_each_request_once() {
    let cm = manager(3, 20);
    let mut recorded = false;
    let exhausted = Some(AdmissionCode::RejectedOnboardingBudgetExhausted);
    cm.record_relay_admission(exhausted, &mut recorded);
    cm.record_relay_admission(exhausted, &mut recorded);
    assert_eq!(cm.admission_stats().rejected_onboarding_budget_exhausted, 1);
    cm.record_relay_admission(Some(AdmissionCode::AcceptedOnboarding), &mut recorded);
    assert_eq!(cm.admission_stats().admitted_onboarding, 1);
    cm.record_relay_admission(None, &mut recorded);
    assert_eq!(cm.admission_stats().admitted_onboarding, 1);

    cm.record_request_seen(RequesterNeed::Short(0));
    cm.record_request_seen(RequesterNeed::Bar(0.1));
    cm.record_request_seen(RequesterNeed::Unknown);
    let stats = cm.admission_stats();
    assert_eq!((stats.requests_seen_short, stats.requests_seen_bar), (1, 1));
}

/// Source pin: the relay driver counts the need it saw and every admission
/// decision through `record_relay_admission` (initial, and both retry paths via
/// `settle_retry_admission`),
/// and the decision function itself no longer counts.
#[test]
fn the_relay_driver_counts_admissions_once_per_request() {
    const DRIVER_SRC: &str = include_str!("../operations/connect/op_ctx_task.rs");
    let code: String = DRIVER_SRC
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    assert_eq!(
        code.matches(".record_relay_admission(").count(),
        2,
        "the initial decision, and settle_retry_admission for the retries"
    );
    assert_eq!(
        code.matches("=settle_retry_admission(&op_manager.ring.connection_manager,")
            .count(),
        2,
        "both retry paths settle their admission"
    );
    assert_eq!(
        code.matches(".record_request_seen(state.request.need)")
            .count(),
        1
    );
}

/// Source pin (review S8): the maintenance tick reviews probation, reports
/// each drop's queueing back (so an unqueued drop keeps its slot), and routes
/// both prune paths through the probation substitute, the remove path via
/// `substitute_first_victim` so a substitute is never dropped twice.
#[test]
fn the_maintenance_tick_wires_probation_review_and_substitution() {
    const RING_SRC: &str = include_str!("../ring.rs");
    let start = RING_SRC
        .find("async fn connection_maintenance(")
        .expect("connection_maintenance");
    let end = start
        + RING_SRC[start..]
            .find("async fn acquire_new(")
            .expect("end of connection_maintenance");
    let code: String = RING_SRC[start..end]
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect::<String>()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    for needle in [
        ".review_provisional_links(tick_now)",
        ".confirm_probation_drop(drop,queued)",
        "substitute_first_victim(&mutshould_disconnect_peers,substitute)",
        ".probation_prune_substitute(first,tick_now)",
    ] {
        assert_eq!(
            code.matches(needle).count(),
            1,
            "missing or duplicated: {needle}"
        );
    }
    // Capacity prunes only: the swap arm must not substitute (#5858).
    let swap = &RING_SRC[RING_SRC
        .find("TopologyAdjustment::SwapConnection {")
        .expect("swap arm")..];
    let swap_arm: String = swap[..swap
        .find("TopologyAdjustment::NoChange")
        .expect("end of swap arm")]
        .lines()
        .filter(|l| !l.trim_start().starts_with("//"))
        .collect();
    assert!(
        !swap_arm.contains("probation_prune_substitute("),
        "an optional topology swap must not substitute a probation link"
    );
}
