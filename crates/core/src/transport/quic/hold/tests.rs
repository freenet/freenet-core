use std::sync::Arc;

use super::*;
use crate::transport::metrics::QuicDataCounters;

fn counters() -> &'static QuicDataCounters {
    Box::leak(Box::new(QuicDataCounters::new()))
}

fn detector(threshold_datagrams: u64) -> Arc<HoldDetector> {
    HoldDetector::new(threshold_datagrams * RETAINED_DATAGRAM_BYTES, counters())
}

/// With no stream in its in-order phase quinn holds nothing for one, so no
/// number of datagrams trips the detector.
#[test]
fn datagrams_with_no_phase_open_never_trip() {
    let d = detector(4);
    for _ in 0..1000 {
        d.on_datagram();
    }
    assert_eq!(d.node.snapshot(false).hold_trips, 0);
}

/// The count runs from when the oldest open phase began: past the threshold
/// it trips once, and the phase sees it.
#[test]
fn the_oldest_phase_trips_past_the_threshold_once() {
    let d = detector(4);
    for _ in 0..100 {
        d.on_datagram();
    }
    let phase = d.begin_phase();
    for _ in 0..4 {
        d.on_datagram();
    }
    assert!(
        futures::FutureExt::now_or_never(phase.tripped()).is_none(),
        "at the threshold, not past it"
    );
    for _ in 0..10 {
        d.on_datagram();
    }
    assert!(futures::FutureExt::now_or_never(phase.tripped()).is_some());
    assert_eq!(
        d.node.snapshot(false).hold_trips,
        1,
        "once per oldest phase"
    );
}

/// When the oldest phase ends the count restarts from the next oldest, so a
/// busy connection whose phases each end quickly never trips.
#[test]
fn the_count_restarts_from_the_next_oldest_phase() {
    let d = detector(7);
    let first = d.begin_phase();
    for _ in 0..4 {
        d.on_datagram();
    }
    let second = d.begin_phase();
    d.on_datagram();
    d.on_datagram();
    drop(first);
    for _ in 0..4 {
        d.on_datagram();
    }
    assert!(
        futures::FutureExt::now_or_never(second.tripped()).is_none(),
        "6 datagrams since the second began, 10 since the first: past 7 only if \
         counted from the first"
    );
    // Phases that each end before the threshold, back to back, forever.
    drop(second);
    for _ in 0..1000 {
        let phase = d.begin_phase();
        d.on_datagram();
        d.on_datagram();
        drop(phase);
    }
    assert_eq!(d.node.snapshot(false).hold_trips, 0);
    assert_eq!(d.open_phases(), 0);
}

/// A phase begun after a trip is not stopped by it.
#[test]
fn a_phase_begun_after_a_trip_is_not_stopped_by_it() {
    let d = detector(1);
    let old = d.begin_phase();
    d.on_datagram();
    d.on_datagram();
    assert!(futures::FutureExt::now_or_never(old.tripped()).is_some());
    drop(old);
    let new = d.begin_phase();
    assert!(futures::FutureExt::now_or_never(new.tripped()).is_none());
}

/// The registry counts a datagram only for the detector registered under its
/// source, and forgets the detector when its registration is dropped.
#[test]
fn the_registry_counts_by_source_while_registered() {
    let registry = Arc::new(HoldRegistry::default());
    let d = detector(1);
    let addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
    let other: SocketAddr = "10.0.0.2:5000".parse().unwrap();
    let registration = registry.register(addr, &d);
    let phase = d.begin_phase();
    registry.on_datagram(other);
    registry.on_datagram(other);
    assert!(futures::FutureExt::now_or_never(phase.tripped()).is_none());
    registry.on_datagram(addr);
    registry.on_datagram(addr);
    assert!(futures::FutureExt::now_or_never(phase.tripped()).is_some());
    drop(registration);
    assert!(registry.detectors.is_empty());
}

#[test]
fn the_threshold_and_charge_follow_the_credit() {
    for credit in [16 * 1024, 128 * 1024, 8 * 1024 * 1024] {
        let back = credit_for_hold_charge(hold_charge(credit));
        assert!(
            back <= credit && back + credit / 50 >= credit,
            "{credit}: {back}"
        );
    }
    assert_eq!(hold_threshold(100 * 1024), 220 * 1024);
    assert_eq!(
        hold_charge(100 * 1024),
        345 * 1024 + RETAINED_DATAGRAM_BYTES
    );
}
