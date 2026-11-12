use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::transport::metrics::QuicDataCounters;
use crate::transport::quic::path::inbound::{ConnRanges, ConnStaging};

const MS: u64 = 1_000_000;
const RTT: Duration = Duration::from_millis(10);
const FLOOR: u64 = MIN_WINDOW_BYTES;

/// A connection that records the windows set on it.
struct Fake {
    recv: Mutex<Vec<u64>>,
    send: Mutex<Vec<u64>>,
    cwnd: AtomicU64,
    rtt: Mutex<Duration>,
    /// When set, a send-window change takes a send epoch of this connection
    /// as it is applied, as a stream opened at that moment would.
    epoch_on_send: Mutex<Option<(Arc<QuicBudget>, u64)>>,
    taken: Mutex<Option<Arc<()>>>,
}

impl Default for Fake {
    fn default() -> Self {
        Self {
            recv: Mutex::default(),
            send: Mutex::default(),
            cwnd: AtomicU64::default(),
            rtt: Mutex::new(RTT),
            epoch_on_send: Mutex::default(),
            taken: Mutex::default(),
        }
    }
}

impl WindowControl for Fake {
    fn set_receive_window(&self, bytes: u64) {
        self.recv.lock().unwrap().push(bytes);
    }
    fn set_send_window(&self, bytes: u64) {
        self.send.lock().unwrap().push(bytes);
        if let Some((budget, id)) = self.epoch_on_send.lock().unwrap().as_ref() {
            *self.taken.lock().unwrap() = budget.send_epoch(*id);
        }
    }
    fn rtt(&self) -> Duration {
        *self.rtt.lock().unwrap()
    }
    fn congestion_window(&self) -> u64 {
        self.cwnd.load(Ordering::Relaxed)
    }
}

impl Fake {
    fn last_recv(&self) -> Option<u64> {
        self.recv.lock().unwrap().last().copied()
    }
}

fn counters() -> &'static QuicDataCounters {
    Box::leak(Box::new(QuicDataCounters::new()))
}

struct Conn {
    fake: Arc<Fake>,
    ranges: Arc<ConnRanges>,
    staging: Arc<ConnStaging>,
    meter: ConnMeter,
}

fn conn(budget: &Arc<QuicBudget>, now: u64) -> Conn {
    let fake = Arc::new(Fake::default());
    let ranges = Arc::new(ConnRanges::new(MIN_GAPS_PER_CONNECTION, 64));
    let staging = Arc::new(ConnStaging::new(0, counters()));
    let meter = budget.register(fake.clone(), ranges.clone(), staging.clone(), now);
    Conn {
        fake,
        ranges,
        staging,
        meter,
    }
}

/// Deliver enough contiguous bytes in one round trip to be window-limited at
/// `window`, and evaluate at the end of it.
fn deliver_window(conn: &Conn, window: u64, at: u64) -> Evaluation {
    conn.meter.on_read(window, window, at - 1);
    conn.meter.evaluate(at).expect("registered")
}

#[test]
fn a_new_path_is_charged_its_floor_windows() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    assert_eq!(
        a.meter.charge(),
        3 * FLOOR
            + FLOOR
            + FLOOR / 8
            + MIN_GAPS_PER_CONNECTION as u64 * GAP_BYTES
            + CONTROL_BUFFER_BYTES
    );
    assert_eq!(a.meter.charge(), floor_grant(FLOOR));
    assert_eq!(
        a.staging.cap(),
        2 * FLOOR as usize,
        "the staging cap follows the credit"
    );
    assert_eq!(
        budget.charged(),
        0,
        "the floor grant is the path's fixed charge, not the growth budget's"
    );
    assert_eq!(budget.running_total(), 0);
    drop(a);
    assert_eq!(budget.charged(), 0, "a dropped path frees its share");
}

/// A path whose contiguous delivery fills its window grows, doubling, and one
/// that does not stays at the floor.
#[test]
fn a_window_limited_path_grows_and_an_idle_one_does_not() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let busy = conn(&budget, 0);
    let idle = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..3 {
        let seen = deliver_window(&busy, window, at);
        assert!(seen.limited);
        assert_eq!(seen.recv_window, window * 2);
        window *= 2;
        at += 10 * MS;
    }
    assert_eq!(busy.fake.last_recv(), Some(FLOOR * 8));
    idle.meter.on_read(FLOOR / 4, FLOOR / 4, at - 1);
    let seen = idle.meter.evaluate(at).unwrap();
    assert!(!seen.limited);
    assert_eq!(seen.recv_window, FLOOR);
    assert_eq!(
        *idle.fake.recv.lock().unwrap(),
        vec![FLOOR],
        "an idle path keeps the floor its registration set"
    );
}

/// Bytes read above a gap are credit, not delivery: a peer that fills its
/// window with gapped data does not grow it.
#[test]
fn gapped_reads_do_not_grow_a_window() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    let mut at = 10 * MS;
    for _ in 0..5 {
        a.meter.on_read(FLOOR, 0, at - 1);
        let seen = a.meter.evaluate(at).unwrap();
        assert!(!seen.limited);
        assert_eq!(seen.recv_window, FLOOR);
        at += 10 * MS;
    }
}

/// Paths growing at once share the growth budget: none passes its share (its
/// floor grant plus an equal part of the budget), and the growth charged
/// stays within the budget.
#[test]
fn growing_paths_stay_within_their_share_and_the_budget() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let paths: Vec<Conn> = (0..4).map(|_| conn(&budget, 0)).collect();
    let mut windows = vec![FLOOR; paths.len()];
    let mut at = 10 * MS;
    for _ in 0..12 {
        for (path, window) in paths.iter().zip(windows.iter_mut()) {
            *window = deliver_window(path, *window, at).recv_window;
            at += MS;
        }
        assert!(
            budget.charged() <= total,
            "{} over {total}",
            budget.charged()
        );
    }
    let share = floor_grant(FLOOR) + total / 4;
    for path in &paths {
        assert!(
            path.meter.charge() <= share,
            "{} over {share}",
            path.meter.charge()
        );
        assert!(path.meter.charge() > floor_grant(FLOOR), "each grew");
    }
}

/// A path that grew alone is shrunk toward its share once another needs room.
/// The shrink frees nothing until the peer's data is read: W + debt stays
/// charged, and the gap cap follows W + debt.
#[test]
fn a_path_above_its_share_shrinks_and_keeps_its_debt_charged_until_read() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let big = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    while window < MAX_WINDOW_BYTES {
        let next = deliver_window(&big, window, at).recv_window;
        at += 10 * MS;
        if next == window {
            break;
        }
        window = next;
    }
    let grown = window;
    assert!(grown > 4 * FLOOR, "premise: it grew alone, to {grown}");
    let charged = big.meter.charge();

    let newcomer = conn(&budget, at);
    deliver_window(&newcomer, FLOOR, at + 10 * MS);
    assert_eq!(
        big.fake.last_recv(),
        Some(grown),
        "another connection's evaluation only asks for the shrink"
    );
    assert!(big.meter.pending_shrink().is_some());
    big.meter.evaluate(at + 11 * MS).unwrap();
    let shrunk = big.fake.last_recv().unwrap();
    assert!(
        shrunk < grown,
        "the big path shrank in its own evaluation: {shrunk}"
    );
    assert!(big.meter.pending_shrink().is_none());
    let debt = big.meter.debt();
    assert_eq!(debt, grown - shrunk);
    assert_eq!(
        big.meter.charge(),
        charged,
        "nothing freed before its data is read"
    );
    assert_eq!(
        big.ranges.cap(),
        gap_cap(shrunk + debt),
        "the gap cap follows W + debt"
    );
    assert_eq!(big.staging.cap(), staging_cap(shrunk + debt));

    // The peer's data is read: the debt is paid and the charge falls.
    big.meter.on_read(debt, 0, at + 19 * MS);
    big.meter.evaluate(at + 20 * MS).unwrap();
    assert_eq!(big.meter.debt(), 0);
    assert!(big.meter.charge() < charged);
}

/// A path with outstanding debt cannot grow past what its debt leaves: growth
/// is judged on W + debt, not on W alone.
#[test]
fn debt_counts_against_growth() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let a = conn(&budget, 0);
    let b = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..8 {
        window = deliver_window(&a, window, at).recv_window;
        at += 10 * MS;
    }
    // b's growth asks a to shrink.
    let mut bw = FLOOR;
    for _ in 0..8 {
        bw = deliver_window(&b, bw, at).recv_window;
        at += 10 * MS;
        assert!(budget.charged() <= total);
    }
    // a applies its shrink, then is limited again, but its debt is unread
    // (nothing read): it may not grow at all while the debt holds the share.
    a.meter.evaluate(at).unwrap();
    at += 10 * MS;
    let debt = a.meter.debt();
    assert!(debt > 0, "premise: a holds debt");
    let before = a.fake.last_recv().unwrap();
    a.meter.on_read(0, before, at - 1);
    let seen = a.meter.evaluate(at).unwrap();
    assert!(seen.limited, "premise: a is window-limited");
    assert_eq!(
        seen.recv_window, before,
        "no growth while {debt} B of debt is unread"
    );
    assert!(
        budget.charged() <= total,
        "{} over {total}",
        budget.charged()
    );
}

/// A send window lowered while a stream opened before it is still sending
/// stays charged until that stream ends.
#[test]
fn a_shrunk_send_window_stays_charged_while_its_streams_are_open() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    a.fake.cwnd.store(4 * FLOOR, Ordering::Relaxed);
    let seen = a.meter.evaluate(10 * MS).unwrap();
    assert_eq!(
        seen.send_window,
        8 * FLOOR,
        "the send window follows the cwnd"
    );
    let stream = a.meter.send_epoch().expect("an epoch");
    let charged = a.meter.charge();
    a.fake.cwnd.store(FLOOR / 4, Ordering::Relaxed);
    let seen = a.meter.evaluate(20 * MS).unwrap();
    assert_eq!(seen.send_window, FLOOR);
    assert_eq!(a.meter.charge(), charged, "held while the stream is open");
    drop(stream);
    assert!(a.meter.charge() < charged, "released when it ends");
}

/// A send window grown after a shrink is charged for a stream opened before
/// the shrink: the window is the connection's, so that stream may buffer up
/// to it, and a second shrink must not drop the charge to the window it was
/// opened at.
#[test]
fn a_send_window_grown_after_a_shrink_stays_charged_for_older_streams() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    a.fake.cwnd.store(4 * FLOOR, Ordering::Relaxed);
    assert_eq!(a.meter.evaluate(10 * MS).unwrap().send_window, 8 * FLOOR);
    let stream = a.meter.send_epoch().expect("an epoch");
    a.fake.cwnd.store(FLOOR / 4, Ordering::Relaxed);
    assert_eq!(a.meter.evaluate(20 * MS).unwrap().send_window, FLOOR);
    a.fake.cwnd.store(16 * FLOOR, Ordering::Relaxed);
    let grown = a.meter.evaluate(30 * MS).unwrap().send_window;
    assert!(grown > 8 * FLOOR, "premise: grown past the first window");
    let charged = a.meter.charge();
    a.fake.cwnd.store(FLOOR / 4, Ordering::Relaxed);
    assert_eq!(a.meter.evaluate(40 * MS).unwrap().send_window, FLOOR);
    assert_eq!(
        a.meter.charge(),
        charged,
        "the grown window stays charged while the older stream is open"
    );
    drop(stream);
    assert!(a.meter.charge() < charged, "released when it ends");
}

/// A stream opened while a send shrink is being applied, after the budget
/// decided it but before the connection has the lower window, may still
/// buffer up to the old window: it takes the epoch held at the old window,
/// and the old window stays charged until it ends.
#[test]
fn a_stream_opened_while_a_shrink_is_applied_keeps_the_old_window_charged() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    a.fake.cwnd.store(4 * FLOOR, Ordering::Relaxed);
    assert_eq!(a.meter.evaluate(10 * MS).unwrap().send_window, 8 * FLOOR);
    let charged = a.meter.charge();
    *a.fake.epoch_on_send.lock().unwrap() = Some((budget.clone(), a.meter.id));
    a.fake.cwnd.store(FLOOR / 4, Ordering::Relaxed);
    assert_eq!(a.meter.evaluate(20 * MS).unwrap().send_window, FLOOR);
    *a.fake.epoch_on_send.lock().unwrap() = None;
    let stream = a.fake.taken.lock().unwrap().take().expect("an epoch");
    assert_eq!(
        a.meter.charge(),
        charged,
        "the old window stays charged for the stream opened mid-shrink"
    );
    drop(stream);
    let low = a.meter.charge();
    assert!(low < charged, "released when it ends");
    let _after = a.meter.send_epoch().expect("an epoch");
    assert_eq!(
        a.meter.charge(),
        low,
        "a stream opened once the shrink is applied is charged at the new window"
    );
}

/// A send shrink with no stream open leaves the node's total at once, not at
/// the connection's next evaluation: the epoch renewal recounts it.
#[test]
fn a_send_shrink_with_no_stream_open_is_released_at_once() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    a.fake.cwnd.store(4 * FLOOR, Ordering::Relaxed);
    assert_eq!(a.meter.evaluate(10 * MS).unwrap().send_window, 8 * FLOOR);
    let before = budget.running_total();
    a.fake.cwnd.store(FLOOR / 4, Ordering::Relaxed);
    assert_eq!(a.meter.evaluate(20 * MS).unwrap().send_window, FLOOR);
    assert!(
        budget.running_total() < before,
        "released at once: {} vs {before}",
        budget.running_total()
    );
    assert_eq!(
        budget.running_total(),
        budget.charged(),
        "the running total is the recount"
    );
}

/// Paths arriving while one path holds the whole growth budget never push the
/// growth charged over it: a newcomer's floor grant is its fixed charge, so it
/// adds nothing here, and it grows only from room a squeeze frees.
#[test]
fn newcomers_never_push_the_growth_over_the_budget() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let big = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..12 {
        window = deliver_window(&big, window, at).recv_window;
        at += 10 * MS;
    }
    assert!(
        budget.running_total() + floor_grant(FLOOR) > total,
        "premise: one more floor grant would not fit, {} of {total}",
        budget.running_total()
    );
    let mut newcomers = Vec::new();
    let mut windows = Vec::new();
    let mut debt_paid = 0;
    for _ in 0..20 {
        newcomers.push(conn(&budget, at));
        windows.push(FLOOR);
        assert!(
            budget.running_total() <= total,
            "{} over {total} with {} newcomers",
            budget.running_total(),
            newcomers.len()
        );
        // Each newcomer wants to grow, which squeezes the big path.
        for (i, (newcomer, window)) in newcomers.iter().zip(windows.iter_mut()).enumerate() {
            *window = deliver_window(newcomer, *window, at + i as u64 + 1).recv_window;
        }
        big.meter.evaluate(at + MS).unwrap();
        assert!(budget.running_total() <= total);
        // The big path's peer sends what its credit allowed, and its reader
        // reads it: the debt is paid and the room is freed.
        let debt = big.meter.debt();
        debt_paid += debt;
        big.meter.on_read(debt, 0, at + 2 * MS);
        big.meter.evaluate(at + 3 * MS).unwrap();
        assert!(budget.running_total() <= total);
        assert_eq!(
            budget.running_total(),
            budget.charged(),
            "the total is the recount"
        );
        at += 10 * MS;
    }
    assert!(
        debt_paid > 0,
        "premise: the squeeze left the big path a debt"
    );
    let grown = windows.iter().filter(|window| **window > FLOOR).count();
    assert!(
        grown >= newcomers.len() / 2,
        "the newcomers grow into the room the big path freed: {grown} of {} grew",
        newcomers.len()
    );
}

#[test]
fn the_floor_and_the_gap_cap_follow_the_design() {
    assert_eq!(floor_window_bytes(1 << 30), MIN_WINDOW_SMALL_HOST_BYTES);
    assert_eq!(floor_window_bytes(2 << 30), MIN_WINDOW_BYTES);
    assert_eq!(gap_cap(MIN_WINDOW_BYTES), MIN_GAPS_PER_CONNECTION);
    assert_eq!(gap_cap(MAX_WINDOW_BYTES), 8192);
    // The design's floor grants with the receive side at 3 x the credit and
    // the waiting one-packet Shorts charged: 689.5 KiB at 2 GiB and up,
    // 425.5 KiB below.
    assert_eq!(CONTROL_BUFFER_BYTES, 147_456);
    assert_eq!(floor_grant(MIN_WINDOW_BYTES), 706_048);
    assert_eq!(floor_grant(MIN_WINDOW_SMALL_HOST_BYTES), 435_712);
    assert_eq!(receive_charge(MIN_WINDOW_BYTES), 3 * MIN_WINDOW_BYTES);
    assert_eq!(staging_cap(MIN_WINDOW_BYTES), 2 * MIN_WINDOW_BYTES as usize);
}

/// The debt mirror counts reads made before a shrink against the debt quinn
/// had then: a shrink asked for by another connection is applied in this
/// connection's own evaluation, after its pending reads are taken off, so the
/// debt equals the window it gave up and is paid exactly by later reads.
#[test]
fn reads_before_a_shrink_do_not_pay_its_debt() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let big = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..8 {
        window = deliver_window(&big, window, at).recv_window;
        at += 10 * MS;
    }
    let newcomer = conn(&budget, at);
    deliver_window(&newcomer, FLOOR, at + MS);
    assert!(
        big.meter.pending_shrink().is_some(),
        "premise: a shrink is asked for"
    );
    // Read before the shrink reaches quinn: quinn returns this credit
    // against the window it had, so it must not pay the new debt.
    big.meter.on_read(5000, 0, at + 2 * MS - 1);
    big.meter.evaluate(at + 2 * MS).unwrap();
    let shrunk = big.fake.last_recv().unwrap();
    assert_eq!(
        big.meter.debt(),
        window - shrunk,
        "the whole window given up"
    );
}

/// Evaluations are at least a round trip apart: with a long round trip, a
/// burst of reads within it does not evaluate again.
#[test]
fn evaluations_are_a_round_trip_apart() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    *a.fake.rtt.lock().unwrap() = Duration::from_millis(200);
    assert!(a.meter.evaluate(10 * MS).is_some());
    assert!(
        a.meter.maybe_evaluate(50 * MS).is_none(),
        "within the round trip"
    );
    assert!(
        a.meter.maybe_evaluate(150 * MS).is_none(),
        "within the round trip"
    );
    assert!(
        a.meter.maybe_evaluate(211 * MS).is_some(),
        "a round trip later"
    );
}

/// A connection that grew and then delivers little has its receive window
/// halved toward the floor once per IDLE_DECAY, and no further than the floor.
#[test]
fn an_idle_window_decays_toward_the_floor() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let a = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..3 {
        window = deliver_window(&a, window, at).recv_window;
        at += 10 * MS;
    }
    assert_eq!(window, 8 * FLOOR, "premise: it grew");
    let decay = nanos(IDLE_DECAY);
    assert_eq!(
        a.meter.evaluate(at + decay / 2).unwrap().recv_window,
        window
    );
    let mut seen = Vec::new();
    for i in 1..=5 {
        seen.push(a.meter.evaluate(at + i * decay + MS).unwrap().recv_window);
    }
    assert_eq!(
        seen,
        vec![4 * FLOOR, 2 * FLOOR, FLOOR, FLOOR, FLOOR],
        "halved once per period, down to the floor"
    );
}

/// A shrink never sets the gap cap below the gaps a connection already
/// holds, so another connection's growth cannot turn the next honest gap
/// into a trip.
#[test]
fn a_shrink_keeps_the_gap_cap_above_the_gaps_held() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let big = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..8 {
        window = deliver_window(&big, window, at).recv_window;
        at += 10 * MS;
    }
    let cap = big.ranges.cap();
    big.ranges.set_cap(usize::MAX);
    big.ranges.add_for_test(cap as isize);
    big.ranges.set_cap(cap);
    // Pay the debt so the credit falls below the gaps held.
    let newcomer = conn(&budget, at);
    deliver_window(&newcomer, FLOOR, at + MS);
    big.meter.evaluate(at + 2 * MS).unwrap();
    big.meter.on_read(big.meter.debt(), 0, at + 3 * MS);
    big.meter.evaluate(at + 4 * MS).unwrap();
    assert!(
        gap_cap(big.fake.last_recv().unwrap()) < cap,
        "premise: the credit alone would lower the cap"
    );
    assert!(big.ranges.cap() >= cap, "{} < {cap}", big.ranges.cap());
    // The gaps then close: the cap in force stays until the credit next
    // changes, and so does its charge.
    let charged = big.meter.charge();
    big.ranges.add_for_test(-(cap as isize));
    assert_eq!(big.ranges.held_now(), 0, "premise: the gaps closed");
    assert!(
        big.ranges.cap() >= cap,
        "premise: the cap is still in force"
    );
    assert_eq!(
        big.meter.charge(),
        charged,
        "the cap in force stays charged after its gaps close"
    );
}

/// Growth with a budget too small for every path to grow: paths still grow
/// within it, none past the budget, and none falls below the floor.
#[test]
fn growth_within_a_small_budget() {
    let total = 4 * floor_grant(FLOOR) + 2 * FLOOR;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let paths: Vec<Conn> = (0..4).map(|_| conn(&budget, 0)).collect();
    let mut windows = vec![FLOOR; paths.len()];
    let mut at = 10 * MS;
    for _ in 0..12 {
        for (path, window) in paths.iter().zip(windows.iter_mut()) {
            *window = deliver_window(path, *window, at).recv_window;
            at += MS;
            assert!(*window >= FLOOR);
        }
        assert!(
            budget.charged() <= total,
            "{} over {total}",
            budget.charged()
        );
    }
    assert!(
        windows.iter().any(|w| *w > FLOOR),
        "some room was used: {windows:?}"
    );
}

/// Paths evaluated from several threads at once keep the node within its
/// budget and the running total equal to a recount.
#[test]
fn concurrent_evaluations_stay_within_the_budget() {
    let total = 16 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let paths: Vec<Arc<Conn>> = (0..8).map(|_| Arc::new(conn(&budget, 0))).collect();
    let threads: Vec<_> = paths
        .iter()
        .map(|path| {
            let path = path.clone();
            let budget = budget.clone();
            std::thread::spawn(move || {
                let mut window = FLOOR;
                let mut at = 10 * MS;
                for _ in 0..200 {
                    window = deliver_window(&path, window, at).recv_window;
                    path.meter.on_read(path.meter.debt(), 0, at + 1);
                    at += 10 * MS;
                    assert!(budget.charged() <= total);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().expect("no panic");
    }
    let recount = budget.charged();
    assert!(recount <= total, "{recount} over {total}");
    assert_eq!(
        budget.running_total(),
        recount,
        "the running total is a recount"
    );
}

/// A path the budget holds below what it needs is throttled: a grow it needs
/// that does not fit, or a shrink another path's growth asked of it. The
/// throttle lasts `GROWING_FOR` past the last such evaluation and remembers
/// when it began; a path that grows freely is never throttled.
#[test]
fn a_held_back_path_is_throttled_until_it_is_not() {
    let total = 4 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let a = conn(&budget, 0);
    let b = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    let mut first = None;
    for _ in 0..12 {
        let seen = deliver_window(&a, window, at);
        window = seen.recv_window;
        if seen.throttled && first.is_none() {
            first = Some(at);
        }
        if first.is_none() {
            assert!(a.meter.throttled_since(at).is_none(), "grew freely");
        }
        at += 10 * MS;
    }
    let began = first.expect("a reached the budget and was held back");
    assert_eq!(a.meter.throttled_since(at - 10 * MS), Some(began));
    // b's growth squeezes a, which then counts as throttled too.
    let mut bw = FLOOR;
    for _ in 0..12 {
        bw = deliver_window(&b, bw, at).recv_window;
        at += 10 * MS;
    }
    assert!(
        a.meter.pending_shrink().is_some(),
        "premise: b asked a to shrink"
    );
    let later = at + nanos(GROWING_FOR) * 2;
    assert!(a.meter.throttled_since(later).is_none(), "premise: lapsed");
    let seen = a.meter.evaluate(later).unwrap();
    assert!(seen.throttled, "a shrink it was asked for throttles it");
    assert_eq!(a.meter.throttled_since(later + MS), Some(later));
    assert!(
        a.meter
            .throttled_since(later + nanos(GROWING_FOR) + MS)
            .is_none(),
        "the throttle lapses"
    );
}

/// A shrink whose debt is then read lowers the staging cap, but never below
/// what is staged, and the charge covers the cap as set: staged bytes are
/// never over the cap or uncharged.
#[test]
fn the_staging_cap_never_falls_below_what_is_staged() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let big = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..8 {
        window = deliver_window(&big, window, at).recv_window;
        at += 10 * MS;
    }
    assert!(window > 4 * FLOOR, "premise: it grew, to {window}");
    let staged = big.staging.cap() * 9 / 10;
    assert!(big.staging.add_for_test(staged), "premise: room to stage");
    // Another path's growth shrinks it; its debt is then read.
    let newcomer = conn(&budget, at);
    deliver_window(&newcomer, FLOOR, at + 10 * MS);
    big.meter.evaluate(at + 11 * MS).unwrap();
    let debt = big.meter.debt();
    assert!(debt > 0, "premise: it shrank");
    big.meter.on_read(debt, 0, at + 19 * MS);
    big.meter.evaluate(at + 30 * MS).unwrap();
    let credit = big.fake.last_recv().unwrap();
    assert!(
        staging_cap(credit) < staged,
        "premise: the credit alone would put the cap below what is staged"
    );
    assert!(
        big.staging.cap() >= staged,
        "the cap is floored at what is staged"
    );
    assert!(
        big.meter.charge() >= credit + staged as u64,
        "the staged bytes are charged"
    );
}

/// A squeeze asks the paths above the share to shrink largest first, and
/// only as many as cover what is needed: with three paths above the share
/// and a need one of them covers, only the largest is asked; with a need
/// none covers alone, more are.
#[test]
fn a_squeeze_asks_the_largest_first_and_only_as_many_as_needed() {
    let budget = QuicBudget::new(64 << 20, FLOOR, counters());
    let paths: Vec<Conn> = (0..3).map(|_| conn(&budget, 0)).collect();
    let mut at = 10 * MS;
    let mut windows = [FLOOR; 3];
    // Grow them to different sizes: 3, 4 and 5 doublings.
    for round in 0..6 {
        for (i, path) in paths.iter().enumerate() {
            if round < i + 3 {
                windows[i] = deliver_window(path, windows[i], at).recv_window;
            }
        }
        at += 10 * MS;
    }
    let charges: Vec<u64> = paths.iter().map(|p| p.meter.charge()).collect();
    assert!(
        charges[2] > charges[1] && charges[1] > charges[0],
        "premise: three sizes: {charges:?}"
    );
    let share = floor_grant(FLOOR);
    let asked = |need: u64, at: u64| {
        for path in &paths {
            budget
                .state
                .lock()
                .conns
                .get_mut(&path.meter.id)
                .unwrap()
                .shrink_to = None;
        }
        budget.squeeze_others(&mut budget.state.lock(), u64::MAX, share, need, at);
        paths
            .iter()
            .map(|p| p.meter.pending_shrink().is_some())
            .collect::<Vec<_>>()
    };
    assert_eq!(asked(1, at), vec![false, false, true], "the largest alone");
    let both = charges[2] - share + 1;
    assert_eq!(
        asked(both, at + 20 * MS),
        vec![false, true, true],
        "the two largest"
    );
}

/// Send at the full send window for one round trip, with a congestion window
/// that wants the largest send window, and evaluate at the end of it.
fn send_window(conn: &Conn, at: u64) -> Evaluation {
    conn.fake.cwnd.store(MAX_WINDOW_BYTES, Ordering::Relaxed);
    let window = conn
        .fake
        .send
        .lock()
        .unwrap()
        .last()
        .copied()
        .unwrap_or(FLOOR);
    conn.meter.note_send(window);
    conn.meter.evaluate(at).expect("registered")
}

/// Two paths that only send, arriving in turn: the first takes the growth
/// budget alone, and the second still reaches its share, because a send
/// window held back by the budget asks the first to make room as a receive
/// window does.
#[test]
fn a_later_sender_reaches_its_share() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let first = conn(&budget, 0);
    let mut at = 10 * MS;
    for _ in 0..12 {
        send_window(&first, at);
        at += 10 * MS;
    }
    assert!(
        budget.running_total() > total / 2,
        "premise: the first sender took most of the budget, {} of {total}",
        budget.running_total()
    );
    let second = conn(&budget, at);
    for _ in 0..40 {
        send_window(&second, at + MS);
        send_window(&first, at + 2 * MS);
        assert!(
            budget.running_total() <= total,
            "{} over {total}",
            budget.running_total()
        );
        at += 10 * MS;
    }
    let share = floor_grant(FLOOR) + total / 2;
    for (name, path) in [("first", &first), ("second", &second)] {
        let charge = path.meter.charge();
        assert!(charge <= share, "{name}: {charge} over its share {share}");
        assert!(
            charge + 16 * 1024 >= share,
            "{name}: {charge} short of its share {share}"
        );
    }
}

/// A path whose sends buffer less than most of its send window per round trip
/// is not held back by it, so a large congestion window alone neither grows
/// it toward the budget's limit nor squeezes others.
#[test]
fn a_sender_the_window_does_not_hold_back_asks_no_room() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let big = conn(&budget, 0);
    let mut at = 10 * MS;
    for _ in 0..12 {
        send_window(&big, at);
        at += 10 * MS;
    }
    let idle = conn(&budget, at);
    idle.fake.cwnd.store(MAX_WINDOW_BYTES, Ordering::Relaxed);
    for _ in 0..10 {
        at += 10 * MS;
        idle.meter.note_send(FLOOR / 4);
        idle.meter.evaluate(at).unwrap();
    }
    assert!(
        big.meter.pending_shrink().is_none(),
        "a sender with little to send squeezed another"
    );
}

/// As [`a_later_sender_reaches_its_share`], with a stream of the first
/// sender open across the squeeze: its old send window stays charged while
/// the stream is open, so the second sender waits for that room, with the
/// budget held throughout, and reaches its share once the stream ends.
#[test]
fn a_later_sender_reaches_its_share_once_the_squeezed_streams_end() {
    let total = 12 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let first = conn(&budget, 0);
    let mut at = 10 * MS;
    for _ in 0..12 {
        send_window(&first, at);
        at += 10 * MS;
    }
    let stream = first.meter.send_epoch().expect("an epoch");
    let grown = first.meter.charge();
    assert!(
        grown > floor_grant(FLOOR) + total / 2,
        "premise: the first sender took more than a share, {grown}"
    );
    let second = conn(&budget, at);
    let run = |rounds: usize, at: &mut u64| {
        for _ in 0..rounds {
            send_window(&second, *at + MS);
            send_window(&first, *at + 2 * MS);
            assert!(
                budget.running_total() <= total,
                "{} over {total}",
                budget.running_total()
            );
            *at += 10 * MS;
        }
    };
    run(40, &mut at);
    assert!(
        first
            .fake
            .send
            .lock()
            .unwrap()
            .iter()
            .any(|w| *w < MAX_WINDOW_BYTES),
        "premise: the first sender was asked to shrink"
    );
    assert_eq!(
        first.meter.charge(),
        grown,
        "the old window stays charged while the stream is open"
    );
    let share = floor_grant(FLOOR) + total / 2;
    assert!(
        second.meter.charge() + 16 * 1024 < share,
        "premise: the open stream held the room, the second has {}",
        second.meter.charge()
    );
    drop(stream);
    run(40, &mut at);
    for (name, path) in [("first", &first), ("second", &second)] {
        let charge = path.meter.charge();
        assert!(charge <= share, "{name}: {charge} over its share {share}");
        assert!(
            charge + 16 * 1024 >= share,
            "{name}: {charge} short of its share {share}"
        );
    }
}

/// A stream-limit raise needs the budget charged at most three quarters
/// (`QuicBudget::has_room`): a path growing into the budget takes the room
/// away once its growth passes three quarters, and it comes back when the
/// path's window decays.
#[test]
fn the_budget_has_room_for_a_stream_raise_up_to_three_quarters() {
    let total = 4 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let a = conn(&budget, 0);
    assert!(a.meter.budget_has_room(), "nothing charged");
    let mut at = 10 * MS;
    let mut window = FLOOR;
    let mut was_room = true;
    for _ in 0..12 {
        window = deliver_window(&a, window, at).recv_window;
        let charged = budget.running_total();
        let room = a.meter.budget_has_room();
        assert_eq!(
            room,
            charged * 4 <= total * 3,
            "charged {charged} of {total}"
        );
        was_room &= room;
        at += 10 * MS;
    }
    assert!(!was_room, "premise: the growth passed three quarters");
}
