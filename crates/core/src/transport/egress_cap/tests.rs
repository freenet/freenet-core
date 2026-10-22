use super::*;
use crate::simulation::VirtualTime;

/// 1 MB/s: one byte per microsecond, a 25 000 byte burst, 1 200 bytes take
/// 1.2 ms. Round numbers so expected values can be written out.
const RATE: u64 = 1_000_000;
const BURST: u64 = 25_000;
const PACKET: usize = 1_200;

fn cap_at(rate: u64) -> (EgressCap, VirtualTime) {
    let time = VirtualTime::new();
    (EgressCap::new(rate, time.clone()), time)
}

fn count(cap: &EgressCap, decision: EgressDecision) -> u64 {
    cap.stats().packets[decision as usize]
}

#[test]
fn burst_is_a_window_of_the_rate_with_a_floor_of_four_packets() {
    let (cap, _) = cap_at(RATE);
    assert_eq!(cap.burst_bytes(), BURST);
    assert_eq!(cap.level_bytes(), BURST as i64, "a new cap starts full");

    // 10 KB/s would give a 250 byte burst; the floor keeps four packets.
    let (small, _) = cap_at(10_000);
    assert_eq!(small.burst_bytes(), 4 * MAX_PACKET_SIZE as u64);
}

#[test]
fn a_full_bucket_admits_the_burst_then_refuses_with_the_deficit() {
    let (cap, _) = cap_at(RATE);
    // 25 000 / 1 200 = 20 whole packets fit in the burst.
    for i in 0..20 {
        let take = cap
            .try_take(PACKET, Duration::ZERO)
            .expect("within the burst");
        take.sent(PACKET);
        assert_eq!(cap.level_bytes(), BURST as i64 - (i + 1) * PACKET as i64);
    }
    // 1 000 bytes left: a 1 200-byte take is refused, nothing is taken, and
    // the wait is the 200-byte deficit at 1 MB/s.
    let wait = cap.try_take(PACKET, Duration::ZERO).unwrap_err();
    assert_eq!(wait, Duration::from_micros(200));
    assert_eq!(cap.level_bytes(), 1_000, "a refused take takes nothing");
}

#[test]
fn refill_is_linear_in_time_and_stops_at_the_burst() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    assert_eq!(cap.level_bytes(), 0);
    time.advance(Duration::from_millis(10));
    assert_eq!(cap.level_bytes(), 10_000, "10 ms at 1 MB/s");
    time.advance(Duration::from_secs(60));
    assert_eq!(cap.level_bytes(), BURST as i64, "never above the burst");
}

#[test]
fn unpaced_sends_go_into_debt_and_paced_takes_wait_for_it() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    for _ in 0..100 {
        cap.charge_unpaced(1_000);
    }
    assert_eq!(cap.level_bytes(), -100_000);
    assert_eq!(count(&cap, EgressDecision::Unpaced), 101);
    assert_eq!(cap.stats().debt_hwm_bytes, 100_000);

    // A paced take waits for the debt plus its own bytes.
    let wait = cap.try_take(PACKET, Duration::ZERO).unwrap_err();
    assert_eq!(wait, Duration::from_micros(101_200));
    time.advance(wait);
    let take = cap.try_take(PACKET, wait).expect("due now");
    take.sent(PACKET);
    assert_eq!(cap.level_bytes(), 0);
    assert_eq!(count(&cap, EgressDecision::Waited), 1);
    assert_eq!(cap.stats().bytes_waited, PACKET as u64);
    assert_eq!(cap.stats().wait_us_max, 101_200);
}

#[test]
fn debt_beyond_the_limit_is_forgiven_and_counted() {
    let (cap, _) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    // The limit is one second at the rate: 1 000 000 bytes.
    cap.charge_unpaced(900_000);
    assert_eq!(cap.stats().forgiven_bytes, 0);
    cap.charge_unpaced(300_000);
    assert_eq!(cap.level_bytes(), -1_000_000, "debt stops at the limit");
    assert_eq!(cap.stats().forgiven_bytes, 200_000);
    // At the limit already, the whole charge is forgiven.
    cap.charge_unpaced(5_000);
    assert_eq!(cap.stats().forgiven_bytes, 205_000);
    // Every byte is still counted as sent.
    assert_eq!(cap.stats().bytes, BURST + 1_205_000);
}

/// A take is charged at the datagram's real size: what it did not use is
/// returned after the send.
#[test]
fn a_take_is_charged_the_bytes_really_sent() {
    let (cap, _) = cap_at(RATE);
    let take = cap.try_take(PACKET, Duration::ZERO).unwrap();
    assert_eq!(cap.level_bytes(), BURST as i64 - PACKET as i64);
    take.sent(700);
    assert_eq!(cap.level_bytes(), BURST as i64 - 700);
    let stats = cap.stats();
    assert_eq!(stats.bytes, 700);
    assert_eq!(count(&cap, EgressDecision::Immediate), 1);
    assert_eq!(stats.bytes_waited, 0);
}

/// A dropped take (the send failed or was abandoned) returns all of it, and
/// is not counted as sent.
#[test]
fn a_dropped_take_returns_its_bytes() {
    let (cap, _) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    cap.give_back(PACKET as u64);
    let take = cap.try_take(PACKET, Duration::ZERO).unwrap();
    assert_eq!(cap.level_bytes(), 0);
    drop(take);
    assert_eq!(cap.level_bytes(), PACKET as i64);
    assert_eq!(
        cap.stats().packets.iter().sum::<u64>(),
        1,
        "only the unpaced one"
    );
    // Returned bytes never lift the level above the burst.
    let (full, _) = cap_at(RATE);
    full.give_back(10 * BURST);
    assert_eq!(full.level_bytes(), BURST as i64);
}

#[test]
fn rounding_never_admits_more_than_the_rate() {
    // 3 B/s: one byte costs 333 333 333.33 ns, rounded up.
    let (cap, time) = cap_at(3);
    cap.charge_unpaced(cap.burst_bytes() as usize);
    let mut ns = 0u64;
    for _ in 0..30 {
        let wait = cap.try_take(1, Duration::ZERO).unwrap_err();
        time.advance(wait);
        ns += wait.as_nanos() as u64;
        cap.try_take(1, wait).expect("due").sent(1);
    }
    // 30 bytes at 3 B/s take at least 10 s.
    assert!(ns >= 10_000_000_000, "{ns} ns");
}

#[test]
fn a_zero_total_limit_builds_no_cap() {
    assert!(EgressCap::from_total_limit(0, VirtualTime::new()).is_none());
    let cap = EgressCap::from_total_limit(1, VirtualTime::new()).expect("1 B/s is a cap");
    assert_eq!(cap.burst_bytes(), 4 * MAX_PACKET_SIZE as u64);
}

#[test]
fn the_startup_floor_scales_with_max_connections() {
    let per = MIN_CAP_PER_CONNECTION;
    assert_eq!(cap_below_floor(per * 100 - 1, 100), Some(per * 100));
    assert_eq!(cap_below_floor(per * 100, 100), None);
    assert_eq!(cap_below_floor(per * 10, 100), Some(per * 100));
    assert_eq!(cap_below_floor(1, 0), None, "no connections, no floor");
}

/// No arithmetic panics, whatever the rate, the sizes or the clock: every
/// operation saturates.
#[test]
fn extreme_values_never_panic() {
    for start in [0u64, 1, i64::MAX as u64 - 1, u64::MAX] {
        for rate in [1u64, 3, u64::MAX] {
            let time = VirtualTime::with_initial_time(start);
            let cap = EgressCap::new(rate, time.clone());
            cap.charge_unpaced(usize::MAX);
            cap.charge_unpaced(0);
            drop(cap.try_take(usize::MAX, Duration::MAX));
            if let Ok(take) = cap.try_take(0, Duration::MAX) {
                take.sent(usize::MAX);
            }
            cap.give_back(u64::MAX);
            time.advance(Duration::from_secs(1));
            let _ = cap.level_bytes();
            let _ = jittered(Duration::MAX / 2);
        }
    }
}

/// A paced wait of at least `LONG_WAIT` is counted; a shorter one is not.
#[test]
fn a_long_paced_wait_is_counted() {
    let (cap, _) = cap_at(RATE);
    cap.try_take(PACKET, LONG_WAIT - Duration::from_millis(1))
        .unwrap()
        .sent(PACKET);
    cap.try_take(PACKET, LONG_WAIT).unwrap().sent(PACKET);
    assert_eq!(cap.stats().long_waits, 1);
}

/// The long-wait warning is rate-limited on the cap's clock.
#[test]
fn the_long_wait_warning_is_rate_limited() {
    let (cap, time) = cap_at(RATE);
    let now = |t: &VirtualTime| t.now_nanos() as i64;
    let warned = |cap: &EgressCap| cap.last_long_wait_warn_ns.load(Ordering::Relaxed);
    assert_eq!(warned(&cap), i64::MIN, "nothing logged yet");
    cap.warn_long_wait(LONG_WAIT, now(&time));
    let first = warned(&cap);
    assert_eq!(first, now(&time));
    time.advance(LONG_WAIT_WARN_INTERVAL - Duration::from_millis(1));
    cap.warn_long_wait(LONG_WAIT, now(&time));
    assert_eq!(warned(&cap), first, "suppressed inside the interval");
    time.advance(Duration::from_millis(1));
    cap.warn_long_wait(LONG_WAIT, now(&time));
    assert_eq!(warned(&cap), now(&time), "logged again after it");
}

#[test]
fn jitter_stays_within_twenty_percent() {
    GlobalRng::scoped_seed(11, || {
        let wait = Duration::from_millis(100);
        for _ in 0..1_000 {
            let j = jittered(wait);
            assert!(
                j >= Duration::from_millis(80) && j < Duration::from_millis(120),
                "{j:?}"
            );
        }
    });
}

struct Sink {
    fail: std::sync::atomic::AtomicBool,
}

impl Sink {
    fn new(fail: bool) -> Self {
        Self {
            fail: std::sync::atomic::AtomicBool::new(fail),
        }
    }
}

impl Socket for Sink {
    async fn bind(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
        Ok(Sink::new(false))
    }
    async fn recv_from(&self, _buf: &mut [u8]) -> std::io::Result<(usize, std::net::SocketAddr)> {
        std::future::pending().await
    }
    async fn send_to(&self, buf: &[u8], target: std::net::SocketAddr) -> std::io::Result<usize> {
        self.send_to_blocking(buf, target)
    }
    fn send_to_blocking(
        &self,
        buf: &[u8],
        _target: std::net::SocketAddr,
    ) -> std::io::Result<usize> {
        if self.fail.load(Ordering::Relaxed) {
            Err(std::io::ErrorKind::WouldBlock.into())
        } else {
            Ok(buf.len())
        }
    }
}

fn target() -> std::net::SocketAddr {
    "192.0.2.1:9".parse().unwrap()
}

/// A socket without a cap admits at once and takes nothing.
#[test]
fn try_take_without_a_cap_admits_at_once() {
    assert!(matches!(
        try_take(&Sink::new(false), Duration::ZERO),
        Ok(None)
    ));
}

/// The socket charges every datagram exactly once: unpaced sends at
/// `send_to`, paced ones at `send_to_paced`, at their real sizes.
#[tokio::test]
async fn the_socket_charges_every_datagram_once() {
    use super::super::DemuxSocket;
    let time = VirtualTime::new();
    let socket = DemuxSocket::wrap(Sink::new(false), Some(EgressCap::new(RATE, time.clone())));
    socket.send_to(&[1; 100], target()).await.unwrap();
    socket.send_to_blocking(&[1; 50], target()).unwrap();
    let take = try_take(&socket, Duration::ZERO).unwrap().expect("take");
    socket
        .send_to_paced(&[2; 700], target(), take)
        .await
        .unwrap();
    let cap = socket.egress_cap().expect("cap");
    assert_eq!(cap.stats().bytes, 850);
    assert_eq!(cap.level_bytes(), BURST as i64 - 850);
    assert_eq!(count(cap, EgressDecision::Unpaced), 2);
    assert_eq!(count(cap, EgressDecision::Immediate), 1);
}

/// Failed sends are not charged: an unpaced one charges nothing, a paced one
/// returns its take.
#[tokio::test]
async fn failed_sends_are_not_charged() {
    use super::super::DemuxSocket;
    let time = VirtualTime::new();
    let socket = DemuxSocket::wrap(Sink::new(true), Some(EgressCap::new(RATE, time.clone())));
    assert!(socket.send_to(&[1; 900], target()).await.is_err());
    assert!(socket.send_to_blocking(&[1; 900], target()).is_err());
    let take = try_take(&socket, Duration::ZERO).unwrap().expect("take");
    assert!(
        socket
            .send_to_paced(&[2; 700], target(), take)
            .await
            .is_err()
    );
    let cap = socket.egress_cap().expect("cap");
    assert_eq!(cap.level_bytes(), BURST as i64, "nothing charged");
    assert_eq!(cap.stats().packets.iter().sum::<u64>(), 0);
}

/// What a simulated run recorded: every send `(time ns, bytes, stream)`,
/// with `None` for an unpaced send.
type Sends = Vec<(u64, u64, Option<usize>)>;

/// Simulate `streams` paced senders on one cap for `duration`: each tries to
/// take a datagram as soon as its previous one is sent; a refused take
/// sleeps for the jittered deficit plus a wake-up lateness drawn from
/// `0..late_ns`, as a real timer can fire late. With `unpaced`, an unpaced
/// datagram of that size is sent every `unpaced.0`.
fn simulate(
    rate: u64,
    streams: usize,
    duration: Duration,
    unpaced: Option<(Duration, usize)>,
    late_ns: u64,
) -> (Sends, EgressStats) {
    let (cap, time) = cap_at(rate);
    let end = duration.as_nanos() as u64;
    // (next try ns, stream, slept so far for this datagram)
    let mut next: Vec<(u64, usize, Duration)> =
        (0..streams).map(|i| (0, i, Duration::ZERO)).collect();
    let mut next_unpaced = unpaced.map(|(every, _)| every.as_nanos() as u64);
    let mut sends = Sends::new();
    loop {
        let (i, &(at, stream, slept)) = next
            .iter()
            .enumerate()
            .min_by_key(|(_, n)| (n.0, n.1))
            .unwrap();
        if let (Some(u), Some((every, size))) = (next_unpaced, unpaced) {
            if u <= at {
                if u > end {
                    break;
                }
                time.advance_to(u);
                cap.charge_unpaced(size);
                sends.push((u, size as u64, None));
                next_unpaced = Some(u + every.as_nanos() as u64);
                continue;
            }
        }
        if at > end {
            break;
        }
        time.advance_to(at);
        match cap.try_take(PACKET, slept) {
            Ok(take) => {
                take.sent(PACKET);
                sends.push((at, PACKET as u64, Some(stream)));
                next[i] = (at, stream, Duration::ZERO);
            }
            Err(wait) => {
                let wait = jittered(wait);
                let late = if late_ns == 0 {
                    0
                } else {
                    GlobalRng::random_range(0..late_ns)
                };
                next[i] = (at + wait.as_nanos() as u64 + late, stream, slept + wait);
            }
        }
    }
    (sends, cap.stats())
}

/// Many concurrent paced streams with unpaced sends interleaved: over every
/// interval ending at a paced send, ALL bytes sent (paced and unpaced) are
/// at most the burst plus the rate times the interval. The burst is written
/// out (25 ms at 1 MB/s), and no forgiven bytes are allowed for: the
/// unpaced rate is low enough that nothing is forgiven, which is checked.
#[test]
fn every_interval_ending_at_a_paced_send_is_within_the_bucket_bound() {
    for seed in [1u64, 7, 42] {
        GlobalRng::scoped_seed(seed, || {
            let (sends, stats) = simulate(
                RATE,
                30,
                Duration::from_millis(400),
                Some((Duration::from_millis(1), 150)),
                1_000_000,
            );
            assert_eq!(stats.forgiven_bytes, 0, "premise: nothing forgiven");
            assert!(sends.iter().filter(|s| s.2.is_none()).count() > 300);
            for j in 0..sends.len() {
                if sends[j].2.is_none() {
                    continue;
                }
                let mut bytes = 0u128;
                for i in (0..=j).rev() {
                    bytes += sends[i].1 as u128;
                    let interval = (sends[j].0 - sends[i].0) as u128;
                    let allowed = 25_000 + interval * RATE as u128 / 1_000_000_000;
                    assert!(
                        bytes <= allowed,
                        "seed {seed}: {bytes} bytes in {interval} ns ending at a paced send, \
                         allowed {allowed}"
                    );
                }
            }
        });
    }
}

/// With many streams and no other limit, paced traffic reaches at least 95%
/// of the cap over a long interval, even when every timer wakes up to 1 ms
/// late. (A model that charged a datagram again for waking late fell to
/// about 50 to 65% of the cap here.)
#[test]
fn paced_streams_reach_the_cap_even_when_timers_wake_late() {
    for rate in [1_250_000u64, 12_500_000] {
        GlobalRng::scoped_seed(rate, || {
            let duration = Duration::from_secs(2);
            let (sends, _) = simulate(rate, 20, duration, None, 1_000_000);
            let bytes: u64 = sends.iter().map(|s| s.1).sum();
            let achieved = bytes as f64 / duration.as_secs_f64();
            assert!(
                achieved >= 0.95 * rate as f64,
                "{achieved:.0} B/s at a {rate} B/s cap"
            );
        });
    }
}

/// There is no order among waiting streams, but none starves: with jittered
/// waits, every one of N streams gets each next datagram within a bounded
/// multiple of the fair gap (N datagrams at the rate).
///
/// Without an order, each waiting stream wins a free datagram with
/// probability about 1/N, so its gaps are geometric: the worst of the ~1 600
/// gaps here is expected near ln(1 600), about 7 fair gaps, and measured at
/// up to 8.4 over these seeds. The bound is 12. Without the jitter, streams
/// wake together in a fixed order and the last ones in it never win.
#[test]
fn no_stream_starves() {
    const STREAMS: usize = 40;
    let rate = 200_000u64;
    let fair_gap_ns = STREAMS as u64 * PACKET as u64 * 1_000_000_000 / rate;
    for seed in [3u64, 9, 27, 81, 243, 729] {
        GlobalRng::scoped_seed(seed, || {
            let (sends, _) = simulate(rate, STREAMS, Duration::from_secs(10), None, 0);
            for stream in 0..STREAMS {
                let times: Vec<u64> = sends
                    .iter()
                    .filter(|s| s.2 == Some(stream))
                    .map(|s| s.0)
                    .collect();
                assert!(
                    times.len() > 10,
                    "seed {seed}: stream {stream} sent {}",
                    times.len()
                );
                let worst = times.windows(2).map(|w| w[1] - w[0]).max().unwrap();
                assert!(
                    worst <= 12 * fair_gap_ns,
                    "seed {seed}: stream {stream} waited {worst} ns, fair gap {fair_gap_ns} ns"
                );
            }
        });
    }
}
