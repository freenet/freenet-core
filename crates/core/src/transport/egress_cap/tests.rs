use super::*;
use crate::simulation::VirtualTime;
use crate::transport::metrics::WaitCause;

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

/// Run `fut` on the test runtime, moving `time` to its next wakeup whenever
/// everything is parked on it.
async fn drive_virtual<F>(time: &VirtualTime, fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let task = tokio::spawn(fut);
    let mut stuck = 0u32;
    while !task.is_finished() {
        tokio::task::yield_now().await;
        let before = time.now_nanos();
        time.advance_to_next_wakeup();
        stuck = if time.now_nanos() == before {
            stuck + 1
        } else {
            0
        };
        assert!(
            stuck < 100_000,
            "virtual time stopped advancing at {before} ns"
        );
    }
    task.await.unwrap()
}

/// A fragment that cannot be admitted fails its stream once the gap since
/// the stream's previous fragment reaches `PACED_WAIT_DEADLINE`, no later
/// (the last sleep is cut to the deadline), and the gap is reported once at
/// `LONG_WAIT` on the way, from the waiting loop, although nothing was sent.
/// Both are blamed on the cap, which took all the time.
#[tokio::test(flavor = "current_thread")]
async fn a_fragment_fails_its_stream_at_the_wait_deadline() {
    let time = VirtualTime::new();
    // 100 B/s with the bucket drained: a datagram needs 12 s, past the deadline.
    let cap = std::sync::Arc::new(EgressCap::new(100, time.clone()));
    cap.charge_unpaced(cap.burst_bytes() as usize);
    let gap = {
        let (cap, time) = (cap.clone(), time.clone());
        drive_virtual(&time.clone(), async move {
            let mut pacing = StreamPacing::new(&time);
            loop {
                assert!(
                    time.now_nanos() < 2 * PACED_WAIT_DEADLINE.as_nanos() as u64,
                    "no deadline"
                );
                if let Err(gap) = pacing.check(&time, &cap) {
                    break gap;
                }
                match cap.try_take(PACKET, pacing.slept()) {
                    Ok(_) => panic!("admitted"),
                    Err(deficit) => {
                        if let Err(gap) = pacing.sleep(&time, &cap, deficit).await {
                            break gap;
                        }
                    }
                }
            }
        })
        .await
    };
    assert_eq!(gap, PACED_WAIT_DEADLINE);
    assert_eq!(time.now_nanos(), PACED_WAIT_DEADLINE.as_nanos() as u64);
    let stats = cap.stats();
    assert_eq!(stats.streams_abandoned, [1, 0, 0], "blamed on the cap");
    assert_eq!(
        stats.long_waits,
        [1, 0, 0],
        "reported once, before the deadline"
    );
    assert_eq!(
        stats.packets.iter().sum::<u64>(),
        1,
        "only the unpaced drain"
    );
}

/// A gap that keeps growing past `LONG_WAIT` is reported once per fragment,
/// not once per sleep, and blamed on the limit that took the time (here the
/// connection's bucket, whose waits are cut at `LONG_WAIT` and resumed).
#[tokio::test(flavor = "current_thread")]
async fn a_long_gap_is_reported_once_per_fragment() {
    let time = VirtualTime::new();
    let cap = std::sync::Arc::new(EgressCap::new(RATE, time.clone()));
    let gap = {
        let (cap, time) = (cap.clone(), time.clone());
        drive_virtual(&time.clone(), async move {
            let mut pacing = StreamPacing::new(&time);
            pacing.sent(&time);
            // 200 ms waits for 4 s: about seven past LONG_WAIT.
            while pacing.gap(&time) < Duration::from_secs(4) {
                assert!(time.now_nanos() < 10_000_000_000, "gaps are not measured");
                pacing
                    .wait_for_connection(&time, &cap, Duration::from_millis(200))
                    .await
                    .unwrap();
            }
            // One wait across LONG_WAIT on the next fragment: reported once,
            // and the wait still lasts its whole length.
            pacing.sent(&time);
            pacing
                .wait_for_connection(&time, &cap, Duration::from_secs(4))
                .await
                .unwrap();
            pacing.gap(&time)
        })
        .await
    };
    assert_eq!(gap, Duration::from_secs(4), "the wait was not cut short");
    assert_eq!(cap.stats().long_waits, [0, 2, 0]);
    assert_eq!(cap.stats().streams_abandoned, [0, 0, 0]);
}

/// A connection wait longer than the deadline fails the stream AT the
/// deadline, blamed on the connection.
#[tokio::test(flavor = "current_thread")]
async fn a_connection_wait_fails_its_stream_at_the_deadline() {
    let time = VirtualTime::new();
    let cap = std::sync::Arc::new(EgressCap::new(RATE, time.clone()));
    let result = {
        let (cap, time) = (cap.clone(), time.clone());
        drive_virtual(&time.clone(), async move {
            let mut pacing = StreamPacing::new(&time);
            pacing.sent(&time);
            pacing
                .wait_for_connection(&time, &cap, Duration::from_secs(9))
                .await
        })
        .await
    };
    assert_eq!(result, Err(PACED_WAIT_DEADLINE));
    assert_eq!(time.now_nanos(), PACED_WAIT_DEADLINE.as_nanos() as u64);
    assert_eq!(cap.stats().streams_abandoned, [0, 1, 0]);
}

/// The gap is the time since the stream's previous fragment was sent, as
/// the receiver measures it: time spent outside the egress sleeps (waiting
/// for cwnd, for instance) counts, and blamed on "other"; a send starts a
/// new gap.
#[test]
fn the_gap_runs_from_the_previous_send_and_counts_all_time() {
    let (cap, time) = cap_at(RATE);
    let mut pacing = StreamPacing::new(&time);
    time.advance(Duration::from_secs(4));
    pacing.sent(&time);
    assert_eq!(pacing.gap(&time), Duration::ZERO, "a send starts a new gap");
    // Six seconds with no egress sleep, as in a cwnd wait.
    time.advance(Duration::from_secs(6));
    assert_eq!(pacing.check(&time, &cap), Err(Duration::from_secs(6)));
    assert_eq!(cap.stats().streams_abandoned, [0, 0, 1], "blamed on other");
}

/// A datagram held back only by its connection's bucket still counts as
/// waited: every egress sleep is counted, whichever bucket it was for.
#[tokio::test(flavor = "current_thread")]
async fn a_wait_on_the_connection_bucket_alone_counts_as_waited() {
    let time = VirtualTime::new();
    let cap = std::sync::Arc::new(EgressCap::new(RATE, time.clone()));
    let slept = {
        let (cap, time) = (cap.clone(), time.clone());
        drive_virtual(&time.clone(), async move {
            let mut pacing = StreamPacing::new(&time);
            // The connection's bucket reserved with a 5 ms deficit.
            pacing
                .wait_for_connection(&time, &cap, Duration::from_millis(5))
                .await
                .unwrap();
            let slept = pacing.slept();
            cap.try_take(PACKET, slept).unwrap().sent(PACKET);
            slept
        })
        .await
    };
    assert!(slept >= Duration::from_millis(5), "{slept:?}");
    assert_eq!(count(&cap, EgressDecision::Waited), 1);
    assert_eq!(count(&cap, EgressDecision::Immediate), 0);
    assert_eq!(cap.stats().wait_us_sum, slept.as_micros() as u64);
}

/// Long gaps and abandoned streams have separate rate-limited warnings, so
/// one cannot suppress the other.
#[test]
fn the_warnings_are_rate_limited_separately() {
    let (cap, time) = cap_at(RATE);
    assert!(cap.warn_due(&cap.last_long_wait_warn_ns));
    assert!(!cap.warn_due(&cap.last_long_wait_warn_ns), "suppressed");
    assert!(
        cap.warn_due(&cap.last_abandon_warn_ns),
        "a long-gap warning does not suppress the abandoned one"
    );
    // Abandoned streams inside the interval are counted for the next WARN.
    cap.record_abandoned(WaitCause::Cap, PACED_WAIT_DEADLINE);
    cap.record_abandoned(WaitCause::Cap, PACED_WAIT_DEADLINE);
    assert_eq!(cap.abandoned_since_warn.pending(WaitCause::Cap).0, 2);
    time.advance(LONG_WAIT_WARN_INTERVAL - Duration::from_millis(1));
    assert!(!cap.warn_due(&cap.last_long_wait_warn_ns));
    time.advance(Duration::from_millis(1));
    assert!(
        cap.warn_due(&cap.last_long_wait_warn_ns),
        "logged again after it"
    );
    // The next abandoned stream logs, with the count, and resets it.
    cap.record_abandoned(WaitCause::Cap, PACED_WAIT_DEADLINE);
    assert_eq!(cap.abandoned_since_warn.pending(WaitCause::Cap).0, 0);
}

#[test]
fn jitter_is_never_shorter_than_the_deficit() {
    GlobalRng::scoped_seed(11, || {
        let wait = Duration::from_millis(100);
        for _ in 0..1_000 {
            let j = jittered(wait);
            assert!(
                j >= Duration::from_millis(100) && j < Duration::from_millis(142),
                "{j:?}"
            );
        }
        // At a tiny deficit the additive part still spreads wakeups over
        // more than one 1 ms timer tick.
        let spread: std::collections::BTreeSet<u128> = (0..100)
            .map(|_| jittered(Duration::from_micros(10)).as_millis())
            .collect();
        assert!(spread.len() >= 2, "{spread:?}");
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
/// sleeps for the deficit (jittered, unless `jitter` is false) plus a wake-up
/// lateness drawn from `0..late_ns`, as a real timer can fire late. With
/// `unpaced`, an unpaced datagram of that size is sent every `unpaced.0`.
///
/// It fails at once, rather than spinning, if the cap admits more than its
/// bucket can give, if a refusal reports no deficit, or if many refusals in
/// a row leave time where it was.
fn simulate(
    rate: u64,
    streams: usize,
    duration: Duration,
    unpaced: Option<(Duration, usize)>,
    late_ns: u64,
    jitter: bool,
) -> (Sends, EgressStats) {
    let (cap, time) = cap_at(rate);
    let end = duration.as_nanos() as u64;
    // (next try ns, stream, slept so far for this datagram)
    let mut next: Vec<(u64, usize, Duration)> =
        (0..streams).map(|i| (0, i, Duration::ZERO)).collect();
    let mut next_unpaced = unpaced.map(|(every, _)| every.as_nanos() as u64);
    let mut sends = Sends::new();
    let mut refusals_without_progress = 0u32;
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
                // A cap that admits more than the bucket can give would
                // otherwise make the simulation spin at one instant.
                let admissible = (cap.burst_bytes() as u128
                    + at as u128 * rate as u128 / 1_000_000_000)
                    / PACKET as u128
                    + 1;
                assert!(
                    (sends.iter().filter(|s| s.2.is_some()).count() as u128) <= admissible,
                    "more paced sends than the cap admits by {at} ns"
                );
                next[i] = (at, stream, Duration::ZERO);
            }
            Err(wait) => {
                assert!(
                    !wait.is_zero(),
                    "a refused take reported no deficit at {at} ns"
                );
                let wait = if jitter { jittered(wait) } else { wait };
                let late = if late_ns == 0 {
                    0
                } else {
                    GlobalRng::random_range(0..late_ns)
                };
                let wake = at + wait.as_nanos() as u64 + late;
                if wake == at {
                    refusals_without_progress += 1;
                    assert!(
                        refusals_without_progress < 10_000,
                        "time does not advance across refusals at {at} ns"
                    );
                } else {
                    refusals_without_progress = 0;
                }
                next[i] = (wake, stream, slept + wait);
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
                true,
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

/// With many streams and no other limit, paced traffic gets at least 99% of
/// what the bucket allows over a long interval (the rate times the
/// interval; the initial burst is subtracted, since it would hide a refill
/// a few percent slow), even when every timer wakes up to 1 ms late. (A
/// model that charged a datagram again for waking late fell to about 50 to
/// 65% of the cap here.)
#[test]
fn paced_streams_reach_the_cap_even_when_timers_wake_late() {
    for rate in [1_250_000u64, 12_500_000] {
        GlobalRng::scoped_seed(rate, || {
            let duration = Duration::from_secs(2);
            let (sends, _) = simulate(rate, 20, duration, None, 1_000_000, true);
            let (cap, _) = cap_at(rate);
            let bytes = sends.iter().map(|s| s.1).sum::<u64>() - cap.burst_bytes();
            let allowed = rate as f64 * duration.as_secs_f64();
            assert!(
                bytes as f64 >= 0.99 * allowed,
                "{bytes} bytes beyond the burst in {duration:?} at a {rate} B/s cap \
                 ({:.1}% of the refill)",
                100.0 * bytes as f64 / allowed
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
/// up to 8.4 over these seeds. The bound is 12. The control run without
/// jitter shows why it is there: streams then wake together and are served
/// in a fixed order, and some stream exceeds the bound.
#[test]
fn no_stream_starves() {
    const STREAMS: usize = 40;
    let rate = 200_000u64;
    let fair_gap_ns = STREAMS as u64 * PACKET as u64 * 1_000_000_000 / rate;
    for seed in [3u64, 9, 27, 81, 243, 729] {
        GlobalRng::scoped_seed(seed, || {
            let (sends, _) = simulate(rate, STREAMS, Duration::from_secs(10), None, 0, true);
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
    // Control: the same run without jitter starves some stream.
    let (sends, _) = simulate(rate, STREAMS, Duration::from_secs(10), None, 0, false);
    let starved = (0..STREAMS).any(|stream| {
        let times: Vec<u64> = sends
            .iter()
            .filter(|s| s.2 == Some(stream))
            .map(|s| s.0)
            .collect();
        times.len() < 2 || times.windows(2).any(|w| w[1] - w[0] > 12 * fair_gap_ns)
    });
    assert!(starved, "control: without jitter some stream must starve");
}

/// A gap that goes from below `LONG_WAIT` to the deadline in one step (a long
/// cwnd or upstream wait between two checks) is still counted as a long gap
/// before the stream is abandoned, once.
#[test]
fn a_jump_straight_to_the_deadline_records_the_long_gap() {
    let (cap, time) = cap_at(RATE);
    let mut pacing = StreamPacing::new(&time);
    time.advance(Duration::from_secs(1));
    assert_eq!(pacing.check(&time, &cap), Ok(()));
    assert_eq!(
        cap.stats().long_waits,
        [0, 0, 0],
        "premise: below LONG_WAIT"
    );
    time.advance(PACED_WAIT_DEADLINE);
    assert_eq!(
        pacing.check(&time, &cap),
        Err(PACED_WAIT_DEADLINE + Duration::from_secs(1))
    );
    let stats = cap.stats();
    assert_eq!(stats.long_waits, [0, 0, 1], "all of it was not egress time");
    assert_eq!(stats.streams_abandoned, [0, 0, 1]);
}

/// Each WARN names every cause that happened since the last one, with that
/// cause's count and remedy, not the triggering cause's alone.
#[test]
fn the_warnings_report_each_cause_with_its_own_count_and_remedy() {
    let (cap, time) = cap_at(RATE);
    // Use up the first, immediate WARN of each kind, so later events pool.
    assert!(cap.warn_due(&cap.last_long_wait_warn_ns));
    assert!(cap.warn_due(&cap.last_abandon_warn_ns));
    let (logs, guard) = crate::util::test_log_capture::install();
    for _ in 0..3 {
        cap.record_abandoned(WaitCause::Connection, PACED_WAIT_DEADLINE);
        cap.record_long_wait(WaitCause::Connection, LONG_WAIT);
    }
    cap.record_abandoned(WaitCause::Other, PACED_WAIT_DEADLINE);
    cap.record_long_wait(WaitCause::Other, LONG_WAIT);
    assert!(logs.lock().unwrap().is_empty(), "premise: rate limited");
    time.advance(LONG_WAIT_WARN_INTERVAL);
    // The triggering event is a cap one; the other causes are still named.
    cap.record_abandoned(WaitCause::Cap, PACED_WAIT_DEADLINE);
    cap.record_long_wait(WaitCause::Cap, LONG_WAIT);
    drop(guard);
    let logs = logs.lock().unwrap().clone();
    for (field, text) in [
        ("streams_abandoned", "Outbound streams failed"),
        ("long_waits", "Stream fragments have waited"),
    ] {
        let lines: Vec<_> = logs.iter().filter(|l| l.contains(text)).collect();
        assert_eq!(lines.len(), 3, "one line per cause; captured: {logs:?}");
        for (cause, count) in [("cap", 1), ("connection", 3), ("other", 1)] {
            let line = lines
                .iter()
                .find(|l| l.contains(&format!("cause=\"{cause}\"")))
                .unwrap_or_else(|| panic!("no {field} line for {cause}: {logs:?}"));
            assert!(line.contains(&format!("{field}={count}")), "{line}");
            let cause_enum = WaitCause::ALL.iter().find(|c| c.label() == cause).unwrap();
            assert!(line.contains(remedy(*cause_enum)), "{line}");
        }
    }
}

/// A drain returns each cause's count with the longest gap of exactly those
/// events, and starts both afresh.
#[test]
fn a_warn_window_drains_count_and_longest_gap_together() {
    let window = WarnWindow::default();
    window.add(WaitCause::Cap, Duration::from_secs(3));
    window.add(WaitCause::Cap, Duration::from_secs(1));
    window.add(WaitCause::Other, Duration::from_secs(2));
    assert_eq!(window.drain(), [(2, 3_000), (0, 0), (1, 2_000)]);
    window.add(WaitCause::Cap, Duration::from_secs(1));
    assert_eq!(window.drain(), [(1, 1_000), (0, 0), (0, 0)], "max reset");
    assert_eq!(window.drain(), [(0, 0); WaitCause::COUNT]);
}

/// Adds racing a drain are never lost or split: every drained pair with a
/// count has the gap of an event, and the counts add up.
#[test]
fn a_warn_window_does_not_tear_under_concurrent_adds() {
    const THREADS: u64 = 4;
    const EACH: u64 = 20_000;
    let window = std::sync::Arc::new(WarnWindow::default());
    let done = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let adders: Vec<_> = (1..=THREADS)
        .map(|n| {
            let (window, done) = (window.clone(), done.clone());
            std::thread::spawn(move || {
                for _ in 0..EACH {
                    window.add(WaitCause::Cap, Duration::from_millis(n));
                }
                done.fetch_add(1, Ordering::SeqCst);
            })
        })
        .collect();
    let mut total = 0;
    let drain = |total: &mut u64| {
        let (count, longest) = window.drain()[WaitCause::Cap as usize];
        assert_eq!(count > 0, longest > 0, "torn: {count} events, {longest} ms");
        *total += count;
    };
    while done.load(Ordering::SeqCst) < THREADS {
        drain(&mut total);
    }
    drain(&mut total);
    for adder in adders {
        adder.join().unwrap();
    }
    assert_eq!(total, THREADS * EACH);
}

/// Counts how often it is woken.
#[derive(Default)]
struct CountingWaker(std::sync::atomic::AtomicUsize);

impl std::task::Wake for CountingWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

fn counting_waker() -> (std::task::Waker, std::sync::Arc<CountingWaker>) {
    let counter = std::sync::Arc::new(CountingWaker::default());
    (std::task::Waker::from(counter.clone()), counter)
}

fn woken(counter: &CountingWaker) -> usize {
    counter.0.load(std::sync::atomic::Ordering::SeqCst)
}

/// With the bucket empty, a datagram's admission is `Pending` until the
/// rate has made up exactly its deficit: still `Pending` 1 ns before, with
/// the waker not yet woken, and `Ready` at the deficit, after the cap's
/// timer woke the waker. Polling takes nothing.
#[test]
fn poll_admit_is_pending_until_the_deficit_and_ready_at_it() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    assert_eq!(cap.level_bytes(), 0);
    // 1 200 bytes at 1 MB/s.
    let deficit = Duration::from_micros(1_200);
    assert_eq!(cap.try_take(PACKET, Duration::ZERO).unwrap_err(), deficit);

    let (waker, counter) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    let mut waiter = AdmissionWaiter::default();
    assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_pending());
    let start = time.now_nanos();
    assert_eq!(waiter.wake_at_ns(), Some(start + deficit.as_nanos() as u64));

    time.advance(deficit - Duration::from_nanos(1));
    assert_eq!(
        woken(&counter),
        0,
        "the timer must not fire before the deficit"
    );
    assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_pending());
    assert_eq!(
        waiter.wake_at_ns(),
        Some(start + deficit.as_nanos() as u64),
        "a poll that needs the same wakeup keeps the armed timer"
    );

    time.advance(Duration::from_nanos(1));
    assert_eq!(woken(&counter), 1, "the timer fires at the deficit");
    assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_ready());
    assert_eq!(
        waiter.wake_at_ns(),
        None,
        "a Ready poll leaves no timer armed"
    );
    assert_eq!(cap.level_bytes(), PACKET as i64, "polling took nothing");
    cap.try_take(PACKET, Duration::ZERO)
        .expect("try_take agrees with the Ready poll")
        .sent(PACKET);
}

/// `poll_admit` and `try_take` decide alike at the same instant, over random
/// sequences of unpaced charges, paced takes, refills and datagram sizes,
/// and a `Pending` poll arms its wakeup exactly at `try_take`'s deficit.
#[test]
fn poll_admit_and_try_take_agree_at_the_same_instant() {
    let (waker, _) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    let (mut admitted, mut refused) = (0u32, 0u32);
    for seed in 0..32u64 {
        GlobalRng::scoped_seed(seed, || {
            let (cap, time) = cap_at(RATE);
            for _ in 0..500 {
                match GlobalRng::random_range(0..4u32) {
                    0 => cap.charge_unpaced(GlobalRng::random_range(1..4 * PACKET)),
                    1 => {
                        if let Ok(take) = cap.try_take(PACKET, Duration::ZERO) {
                            take.sent(GlobalRng::random_range(1..=PACKET));
                        }
                    }
                    _ => {
                        time.advance(Duration::from_nanos(GlobalRng::random_range(0..3_000_000)));
                    }
                }
                let bytes = GlobalRng::random_range(1..=2 * PACKET);
                let mut waiter = AdmissionWaiter::default();
                let polled = cap.poll_admit(bytes, &mut waiter, &mut cx);
                let wake_at = waiter.wake_at_ns();
                match cap.try_take(bytes, Duration::ZERO) {
                    Ok(take) => {
                        assert!(polled.is_ready(), "seed {seed}: try_take admitted {bytes}");
                        admitted += 1;
                        drop(take);
                    }
                    Err(deficit) => {
                        assert!(polled.is_pending(), "seed {seed}: try_take refused {bytes}");
                        assert_eq!(
                            wake_at,
                            Some(time.now_nanos() + deficit.as_nanos() as u64),
                            "seed {seed}: the wakeup is at try_take's deficit"
                        );
                        refused += 1;
                    }
                }
            }
        });
    }
    // Both branches must have been exercised, or the agreement is vacuous.
    assert!(
        admitted > 1_000 && refused > 1_000,
        "admitted {admitted}, refused {refused}"
    );
}

/// Bytes given back to the bucket wake a `Pending` `poll_admit` caller
/// before its timer once the bucket holds a full datagram again: a failed
/// paced send returns its whole take. A give-back that leaves less than a
/// datagram (a take that used most of its bytes) wakes nobody.
#[test]
fn a_give_back_wakes_a_waiter_only_when_a_datagram_fits() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize - 2 * PACKET);
    let first = cap.try_take(PACKET, Duration::ZERO).expect("fits");
    let second = cap.try_take(PACKET, Duration::ZERO).expect("fits");
    assert_eq!(cap.level_bytes(), 0);

    let (waker, counter) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    let mut waiter = AdmissionWaiter::default();
    assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_pending());
    let timer_at = waiter.wake_at_ns().expect("a timer is armed");

    // Used 1000 of 1200: 200 come back, not a datagram's worth.
    first.sent(1000);
    assert_eq!(woken(&counter), 0, "too little came back to wake anyone");
    assert_eq!(cap.admission_waker_count(), 1);

    // A failed send returns all 1200: now 1400, a datagram fits.
    drop(second);
    assert_eq!(woken(&counter), 1, "the give-back woke the waiter");
    assert!(time.now_nanos() < timer_at, "before its timer would have");
    assert_eq!(cap.admission_waker_count(), 0, "a woken waker is dropped");
    assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_ready());
}

/// A give-back wakes one waiter per datagram's worth the bucket then holds,
/// not every waiter: N waiting QUIC connections do not cost N wakeups per
/// legacy datagram.
#[test]
fn a_give_back_wakes_one_waiter_per_datagram_that_fits() {
    let (cap, _time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize - PACKET);
    let take = cap.try_take(PACKET, Duration::ZERO).expect("fits");
    // The waiters stay alive: dropping one removes its entry.
    let waiters: Vec<_> = (0..5)
        .map(|_| {
            let (waker, counter) = counting_waker();
            let mut cx = Context::from_waker(&waker);
            let mut waiter = AdmissionWaiter::default();
            assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_pending());
            (waiter, counter)
        })
        .collect();
    assert_eq!(cap.admission_waker_count(), 5);
    drop(take);
    let total: usize = waiters.iter().map(|(_, c)| woken(c)).sum();
    assert_eq!(total, 1, "one datagram came back, one waiter woken");
    assert_eq!(cap.admission_waker_count(), 4);
}

/// Waiters are woken in the order they registered: with more waiters than
/// datagrams given back, the first to register is woken first, and a waiter
/// that polls again keeps its place (its staggered timer depends on it).
/// On overflow the oldest entry is the one dropped.
#[test]
fn give_backs_wake_waiters_oldest_first() {
    let (cap, _time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize - 2 * PACKET);
    let first_take = cap.try_take(PACKET, Duration::ZERO).expect("fits");
    let second_take = cap.try_take(PACKET, Duration::ZERO).expect("fits");
    let mut waiters: Vec<_> = (0..3)
        .map(|_| {
            let (waker, counter) = counting_waker();
            (AdmissionWaiter::default(), waker, counter)
        })
        .collect();
    for (waiter, waker, _) in waiters.iter_mut() {
        let mut cx = Context::from_waker(waker);
        assert!(cap.poll_admit(PACKET, waiter, &mut cx).is_pending());
    }
    // Waiter 0 polls again: it keeps its place ahead of 1 and 2.
    {
        let (waiter, waker, _) = &mut waiters[0];
        let mut cx = Context::from_waker(waker);
        assert!(cap.poll_admit(PACKET, waiter, &mut cx).is_pending());
    }
    drop(first_take);
    let woken_now = |w: &[(
        AdmissionWaiter,
        std::task::Waker,
        std::sync::Arc<CountingWaker>,
    )]| { w.iter().map(|(_, _, c)| woken(c)).collect::<Vec<_>>() };
    assert_eq!(
        woken_now(&waiters),
        vec![1, 0, 0],
        "the oldest registration first"
    );
    // Waiter 0 took nothing, so the bucket now holds two datagrams' worth:
    // the two remaining waiters, 1 then 2.
    drop(second_take);
    assert_eq!(
        woken_now(&waiters),
        vec![1, 1, 1],
        "two datagrams, two wakes"
    );
}

/// The cap never holds more than `MAX_ADMISSION_WAKERS` wakers: past it an
/// entry is dropped WITHOUT being woken (its waiter still has its timer).
/// The waiters are kept alive, since dropping one removes its entry.
#[test]
fn admission_wakers_are_bounded_without_waking() {
    let (cap, _time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    let (waker, counter) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    let mut first_id = None;
    let mut held = Vec::with_capacity(MAX_ADMISSION_WAKERS);
    for _ in 0..MAX_ADMISSION_WAKERS {
        let mut waiter = AdmissionWaiter::default();
        first_id.get_or_insert(waiter.id);
        assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_pending());
        held.push(waiter);
    }
    let first_id = first_id.unwrap();
    assert_eq!(
        cap.admission.wakers.lock().front().map(|(id, _)| *id),
        Some(first_id)
    );
    assert_eq!(cap.admission_waker_count(), MAX_ADMISSION_WAKERS);
    let mut waiter = AdmissionWaiter::default();
    assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_pending());
    assert_eq!(cap.admission_waker_count(), MAX_ADMISSION_WAKERS, "bounded");
    assert_ne!(
        cap.admission.wakers.lock().front().map(|(id, _)| *id),
        Some(first_id),
        "the oldest entry was the one dropped"
    );
    assert_eq!(woken(&counter), 0, "an evicted entry is not woken");
    // The same waiter polling again holds one entry, not two.
    assert!(cap.poll_admit(PACKET, &mut waiter, &mut cx).is_pending());
    assert_eq!(cap.admission_waker_count(), MAX_ADMISSION_WAKERS);
}

/// `poll_admit` of a full datagram being `Ready` means a `try_take` of any
/// size up to it succeeds at the same instant, which is what the QUIC socket
/// relies on: its poller asks for `WAKE_DATAGRAM_BYTES`, then takes the
/// datagram's real size.
#[test]
fn a_ready_poll_for_a_datagram_admits_any_smaller_take() {
    let (waker, _) = counting_waker();
    let mut cx = Context::from_waker(&waker);
    let mut ready = 0u32;
    for seed in 0..32u64 {
        GlobalRng::scoped_seed(seed, || {
            let (cap, time) = cap_at(RATE);
            for _ in 0..300 {
                if GlobalRng::random_bool(0.5) {
                    cap.charge_unpaced(GlobalRng::random_range(1..3 * PACKET));
                } else {
                    time.advance(Duration::from_nanos(GlobalRng::random_range(0..2_000_000)));
                }
                let mut waiter = AdmissionWaiter::default();
                if cap
                    .poll_admit(WAKE_DATAGRAM_BYTES, &mut waiter, &mut cx)
                    .is_ready()
                {
                    ready += 1;
                    let n = GlobalRng::random_range(1..=WAKE_DATAGRAM_BYTES);
                    let take = cap
                        .try_take(n, Duration::ZERO)
                        .unwrap_or_else(|_| panic!("seed {seed}: Ready for 1200, refused {n}"));
                    drop(take);
                }
            }
        });
    }
    assert!(ready > 500, "the Ready branch ran {ready} times");
}

/// Waiters whose deficits are equal are staggered by one datagram of refill
/// each, so the cap does not wake them as a herd: with N waiters on an
/// empty bucket, each datagram's worth of refill wakes exactly one of them,
/// oldest first, and that one is admitted.
#[test]
fn waiting_connections_wake_one_per_datagram_of_refill() {
    const N: usize = 8;
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    let mut waiters: Vec<_> = (0..N)
        .map(|_| {
            let (waker, counter) = counting_waker();
            (AdmissionWaiter::default(), waker, counter)
        })
        .collect();
    for (waiter, waker, _) in waiters.iter_mut() {
        let mut cx = Context::from_waker(waker);
        assert!(cap.poll_admit(PACKET, waiter, &mut cx).is_pending());
    }
    let start = time.now_nanos();
    // One 1 200-byte datagram of refill at 1 MB/s.
    let datagram = Duration::from_micros(1_200);
    for (i, (waiter, _, _)) in waiters.iter().enumerate() {
        assert_eq!(
            waiter.wake_at_ns(),
            Some(start + (i as u64 + 1) * datagram.as_nanos() as u64),
            "waiter {i} is staggered by its place in the queue"
        );
    }
    for k in 0..N {
        time.advance(datagram);
        let woken_now: Vec<usize> = waiters.iter().map(|(_, _, c)| woken(c)).collect();
        assert_eq!(
            woken_now.iter().sum::<usize>(),
            k + 1,
            "after {} datagrams of refill: {woken_now:?}",
            k + 1
        );
        assert_eq!(woken_now[k], 1, "waiter {k} is the one woken");
        let (waiter, waker, _) = &mut waiters[k];
        let mut cx = Context::from_waker(waker);
        assert!(cap.poll_admit(PACKET, waiter, &mut cx).is_ready());
        cap.try_take(PACKET, Duration::ZERO)
            .expect("the woken waiter is admitted")
            .sent(PACKET);
    }
    assert_eq!(cap.admission_waker_count(), 0);
}

/// Dropping a `Pending` waiter (its connection closed) removes its entry,
/// so a give-back wakes a live waiter instead of being absorbed by the dead
/// one, and a waiter that outlives its cap drops cleanly.
#[test]
fn a_dropped_waiter_leaves_no_entry_behind() {
    let (cap, _time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize - PACKET);
    let take = cap.try_take(PACKET, Duration::ZERO).expect("fits");
    let (gone_waker, gone_counter) = counting_waker();
    let (live_waker, live_counter) = counting_waker();
    let mut gone = AdmissionWaiter::default();
    let mut live = AdmissionWaiter::default();
    assert!(
        cap.poll_admit(PACKET, &mut gone, &mut Context::from_waker(&gone_waker))
            .is_pending()
    );
    assert!(
        cap.poll_admit(PACKET, &mut live, &mut Context::from_waker(&live_waker))
            .is_pending()
    );
    assert_eq!(cap.admission_waker_count(), 2);
    drop(gone);
    assert_eq!(cap.admission_waker_count(), 1, "the dropped waiter's entry");
    // The drop woke the live waiter once, to move up a place.
    let moved_up = woken(&live_counter);
    assert_eq!(moved_up, 1);
    drop(take);
    assert_eq!(woken(&gone_counter), 0);
    assert_eq!(
        woken(&live_counter),
        moved_up + 1,
        "the live waiter got the give-back's wake"
    );

    assert!(
        cap.poll_admit(PACKET * 2, &mut live, &mut Context::from_waker(&live_waker))
            .is_pending()
    );
    drop(cap);
    drop(live);
}

/// Dropping queued waiters wakes those behind them, so each moves its
/// staggered timer to its new place instead of keeping one datagram of
/// delay per waiter that left: with 5 waiters queued, dropping the first 3
/// wakes the survivors, the first survivor re-polls to a timer at one
/// datagram (where the first waiter's was), fires there and is admitted.
#[test]
fn dropping_queued_waiters_moves_the_followers_up() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    let mut waiters: Vec<_> = (0..5)
        .map(|_| {
            let (waker, counter) = counting_waker();
            (AdmissionWaiter::default(), waker, counter)
        })
        .collect();
    for (waiter, waker, _) in waiters.iter_mut() {
        let mut cx = Context::from_waker(waker);
        assert!(cap.poll_admit(PACKET, waiter, &mut cx).is_pending());
    }
    let start = time.now_nanos();
    let datagram = Duration::from_micros(1_200).as_nanos() as u64;
    assert_eq!(waiters[3].0.wake_at_ns(), Some(start + 4 * datagram));
    let mut survivors = waiters.split_off(3);
    drop(waiters);
    assert_eq!(cap.admission_waker_count(), 2);
    for (i, (waiter, waker, counter)) in survivors.iter_mut().enumerate() {
        assert!(woken(counter) > 0, "survivor {i} was woken to move up");
        let mut cx = Context::from_waker(waker);
        assert!(cap.poll_admit(PACKET, waiter, &mut cx).is_pending());
        assert_eq!(
            waiter.wake_at_ns(),
            Some(start + (i as u64 + 1) * datagram),
            "survivor {i} is now staggered by its new place"
        );
    }
    let before = woken(&survivors[0].2);
    time.advance(Duration::from_nanos(datagram));
    assert_eq!(woken(&survivors[0].2), before + 1, "its timer fired");
    let (waiter, waker, _) = &mut survivors[0];
    assert!(
        cap.poll_admit(PACKET, waiter, &mut Context::from_waker(waker))
            .is_ready()
    );
}

/// Connections closing one by one in a long queue cost a bounded number of
/// wakes: each drop wakes at most `MAX_FOLLOWERS_WOKEN_PER_LEAVE` waiters
/// behind it, never the whole queue, so 64 drops from the front of 128
/// queued waiters wake at most 8 per drop.
#[test]
fn a_run_of_drops_wakes_a_bounded_number_of_followers() {
    const QUEUED: usize = 128;
    const DROPS: usize = 64;
    let (cap, _time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    let mut waiters: std::collections::VecDeque<_> = (0..QUEUED)
        .map(|_| {
            let (waker, counter) = counting_waker();
            (AdmissionWaiter::default(), waker, counter)
        })
        .collect();
    for (waiter, waker, _) in waiters.iter_mut() {
        let mut cx = Context::from_waker(waker);
        assert!(cap.poll_admit(PACKET, waiter, &mut cx).is_pending());
    }
    let counters: Vec<_> = waiters.iter().map(|(_, _, c)| c.clone()).collect();
    for drop_index in 0..DROPS {
        let before: usize = counters.iter().map(|c| woken(c)).sum();
        drop(waiters.pop_front());
        let after: usize = counters.iter().map(|c| woken(c)).sum();
        assert_eq!(
            after - before,
            MAX_FOLLOWERS_WOKEN_PER_LEAVE,
            "drop {drop_index} woke {} waiters",
            after - before
        );
    }
    let total: usize = counters.iter().map(|c| woken(c)).sum();
    assert!(total <= DROPS * MAX_FOLLOWERS_WOKEN_PER_LEAVE);
    // The last waiter was never close enough to the front to be woken.
    assert_eq!(woken(counters.last().unwrap()), 0);
    assert_eq!(cap.admission_waker_count(), QUEUED - DROPS);
}
