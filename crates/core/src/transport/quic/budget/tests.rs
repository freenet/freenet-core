use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;
use crate::transport::metrics::QuicDataCounters;
use crate::transport::quic::path::inbound::ConnRanges;

const MS: u64 = 1_000_000;
const RTT: Duration = Duration::from_millis(10);
const FLOOR: u64 = MIN_WINDOW_BYTES;

/// A connection that records the windows set on it.
#[derive(Default)]
struct Fake {
    recv: Mutex<Vec<u64>>,
    send: Mutex<Vec<u64>>,
    cwnd: AtomicU64,
}

impl WindowControl for Fake {
    fn set_receive_window(&self, bytes: u64) {
        self.recv.lock().unwrap().push(bytes);
    }
    fn set_send_window(&self, bytes: u64) {
        self.send.lock().unwrap().push(bytes);
    }
    fn rtt(&self) -> Duration {
        RTT
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
    meter: ConnMeter,
}

fn conn(budget: &Arc<QuicBudget>, now: u64) -> Conn {
    let fake = Arc::new(Fake::default());
    let ranges = Arc::new(ConnRanges::new(MIN_GAPS_PER_CONNECTION, 64));
    let meter = budget.register(fake.clone(), ranges.clone(), now);
    Conn {
        fake,
        ranges,
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
        FLOOR + FLOOR + FLOOR / 8 + CONTROL_BUFFER_BYTES
    );
    assert!(a.meter.charge() <= floor_grant(FLOOR));
    assert_eq!(budget.charged(), a.meter.charge());
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

/// Paths growing at once share what is above their floors: none passes its
/// share, and the node's charge stays within the budget.
#[test]
fn growing_paths_stay_within_their_share_and_the_budget() {
    let total = 4 << 20;
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
    let share = floor_grant(FLOOR) + (total - 4 * floor_grant(FLOOR)) / 4;
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
    let total = 4 << 20;
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
    let shrunk = big.fake.last_recv().unwrap();
    assert!(shrunk < grown, "the big path was shrunk: {shrunk}");
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
    let total = 4 << 20;
    let budget = QuicBudget::new(total, FLOOR, counters());
    let a = conn(&budget, 0);
    let b = conn(&budget, 0);
    let mut at = 10 * MS;
    let mut window = FLOOR;
    for _ in 0..8 {
        window = deliver_window(&a, window, at).recv_window;
        at += 10 * MS;
    }
    // b's growth shrinks a; a now holds debt.
    let mut bw = FLOOR;
    for _ in 0..8 {
        bw = deliver_window(&b, bw, at).recv_window;
        at += 10 * MS;
        assert!(budget.charged() <= total);
    }
    assert!(a.meter.debt() > 0, "premise: a holds debt");
    // a is limited again, but its debt is unread (nothing read): it may not
    // grow past its share counting the debt.
    a.meter.on_read(0, a.fake.last_recv().unwrap(), at - 1);
    a.meter.evaluate(at).unwrap();
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

#[test]
fn the_floor_and_the_gap_cap_follow_the_design() {
    assert_eq!(floor_window_bytes(1 << 30), MIN_WINDOW_SMALL_HOST_BYTES);
    assert_eq!(floor_window_bytes(2 << 30), MIN_WINDOW_BYTES);
    assert_eq!(gap_cap(MIN_WINDOW_BYTES), MIN_GAPS_PER_CONNECTION);
    assert_eq!(gap_cap(MAX_WINDOW_BYTES), 8192);
    // The design's floor grants: 354 KiB at 2 GiB and up, 218 KiB below.
    assert_eq!(floor_grant(MIN_WINDOW_BYTES).div_ceil(1024), 354);
    assert_eq!(floor_grant(MIN_WINDOW_SMALL_HOST_BYTES).div_ceil(1024), 218);
}
