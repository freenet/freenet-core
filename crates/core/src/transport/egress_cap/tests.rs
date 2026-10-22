use super::*;
use crate::config::GlobalRng;
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

fn decisions(cap: &EgressCap) -> [u64; EgressDecision::COUNT] {
    cap.stats().packets
}

fn count(cap: &EgressCap, decision: EgressDecision) -> u64 {
    decisions(cap)[decision as usize]
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
fn a_full_bucket_admits_the_burst_then_paces_at_the_rate() {
    let (cap, _) = cap_at(RATE);
    let mut waits = Vec::new();
    for _ in 0..30 {
        let hold = cap.reserve(PACKET);
        waits.push(hold.wait());
        // Keep the reservation: the next one queues behind it.
        std::mem::forget(hold);
    }
    // 25 000 / 1 200 = 20 whole packets fit in the burst.
    assert!(waits[..20].iter().all(|w| w.is_zero()), "{waits:?}");
    // Packet 21 needs 21 * 1200 - 25 000 = 200 bytes more: 200 us. Each
    // later one waits one more packet time (1.2 ms).
    for (i, wait) in waits[20..].iter().enumerate() {
        let expected = Duration::from_micros(200 + 1_200 * i as u64);
        assert_eq!(*wait, expected, "packet {}", 21 + i);
    }
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
fn unpaced_sends_go_into_debt_and_paced_sends_wait_for_it() {
    let (cap, time) = cap_at(RATE);
    // An empty bucket, then 100 KB of unpaced sends (ACKs, retransmissions).
    cap.charge_unpaced(BURST as usize);
    for _ in 0..100 {
        cap.charge_unpaced(1_000);
    }
    assert_eq!(cap.level_bytes(), -100_000);
    assert_eq!(count(&cap, EgressDecision::Unpaced), 101);
    assert_eq!(cap.stats().backlog_hwm_bytes, 100_000);

    // A paced fragment now waits for the debt plus its own bytes.
    let hold = cap.reserve(PACKET);
    assert_eq!(hold.wait(), Duration::from_micros(101_200));

    // After the wait it is due exactly: the level is zero.
    time.advance(hold.wait());
    assert_eq!(cap.level_bytes(), 0);
    hold.settle(PACKET);
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

    // With no paced reservations queued, a paced sender waits for the debt
    // (at most the limit) plus its own bytes. Queued reservations are never
    // forgiven, so with many streams the wait is longer; see
    // `unpaced_charges_never_erase_queued_paced_reservations`.
    let hold = cap.reserve(PACKET);
    assert_eq!(hold.wait(), DEBT_LIMIT + Duration::from_micros(1_200));
}

#[test]
fn reservations_are_served_in_the_order_they_were_made() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    let first = cap.reserve(PACKET);
    let second = cap.reserve(300);
    let third = cap.reserve(PACKET);
    assert_eq!(first.wait(), Duration::from_micros(1_200));
    assert_eq!(second.wait(), Duration::from_micros(1_500));
    assert_eq!(third.wait(), Duration::from_micros(2_700));
    // Sending them as they come due keeps the level at zero each time.
    time.advance(Duration::from_micros(1_200));
    first.settle(PACKET);
    time.advance(Duration::from_micros(300));
    second.settle(300);
    time.advance(Duration::from_micros(1_200));
    assert_eq!(cap.level_bytes(), 0);
    third.settle(PACKET);
    assert_eq!(cap.level_bytes(), 0);
}

#[test]
fn a_dropped_hold_returns_its_reservation() {
    let (cap, _) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    let before = cap.level_bytes();
    let abandoned = cap.reserve(PACKET);
    assert_eq!(cap.level_bytes(), before - PACKET as i64);
    drop(abandoned);
    assert_eq!(cap.level_bytes(), before);
    // The next sender is not delayed by the abandoned one.
    let next = cap.reserve(PACKET);
    assert_eq!(next.wait(), Duration::from_micros(1_200));
    // An abandoned reservation was never sent, so it is not counted.
    assert_eq!(cap.stats().packets.iter().sum::<u64>(), 1);
}

#[test]
fn settle_charges_the_real_size_once() {
    let (cap, _) = cap_at(RATE);
    let hold = cap.reserve(PACKET);
    hold.settle(1_000);
    assert_eq!(cap.level_bytes(), BURST as i64 - 1_000);
    let stats = cap.stats();
    assert_eq!(stats.bytes, 1_000);
    assert_eq!(count(&cap, EgressDecision::Immediate), 1);
    assert_eq!(stats.bytes_waited, 0);
    assert_eq!(stats.wait_us_sum, 0);
}

/// A paced wait of at least `LONG_WAIT` (half the receiver's stream
/// inactivity timeout) is counted; a shorter one is not.
#[test]
fn a_long_paced_wait_is_counted() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    // 2.5 s at 1 MB/s is 2 500 000 bytes; queue 2 082 x 1 200 = 2 498 400
    // ahead, so the next reservation waits just under it and the one after
    // just over.
    let queued: Vec<_> = (0..2_082).map(|_| cap.reserve(PACKET)).collect();
    let short = cap.reserve(PACKET);
    assert!(short.wait() < LONG_WAIT, "{:?}", short.wait());
    let long = cap.reserve(PACKET);
    assert!(long.wait() >= LONG_WAIT, "{:?}", long.wait());
    time.advance(long.wait());
    for hold in queued {
        hold.settle(PACKET);
    }
    short.settle(PACKET);
    long.settle(PACKET);
    assert_eq!(cap.stats().long_waits, 1);
}

/// The long-wait warning is rate-limited on the cap's clock: the first long
/// wait logs, the next ones within the interval do not, and one after it
/// logs again.
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
fn rounding_never_admits_more_than_the_rate() {
    // 3 B/s: one byte costs 333 333 333.33 ns, rounded up.
    let (cap, time) = cap_at(3);
    cap.charge_unpaced(cap.burst_bytes() as usize);
    let mut sent = 0u64;
    let mut ns = 0u64;
    for _ in 0..30 {
        let hold = cap.reserve(1);
        ns += hold.wait().as_nanos() as u64;
        time.advance(hold.wait());
        hold.settle(1);
        sent += 1;
    }
    // 30 bytes at 3 B/s take at least 10 s.
    assert_eq!(sent, 30);
    assert!(ns >= 10_000_000_000, "{ns} ns");
}

#[test]
fn a_zero_total_limit_builds_no_cap() {
    assert!(EgressCap::from_total_limit(0, VirtualTime::new()).is_none());
    let cap = EgressCap::from_total_limit(1, VirtualTime::new()).expect("1 B/s is a cap");
    assert_eq!(cap.burst_bytes(), 4 * MAX_PACKET_SIZE as u64);
}

/// A settled datagram smaller than its reservation returns the difference,
/// which moves the next sender earlier, never later than the cap allows.
#[test]
fn settling_a_smaller_datagram_returns_the_difference() {
    let (cap, time) = cap_at(RATE);
    cap.charge_unpaced(BURST as usize);
    let small = cap.reserve(PACED_RESERVATION_BYTES);
    let next = cap.reserve(PACED_RESERVATION_BYTES);
    assert_eq!(next.wait(), Duration::from_micros(2 * 1_200));
    time.advance(small.wait());
    small.settle(200);
    // Without the refund the next reservation would be due at 3.6 ms; the
    // 1 000 bytes returned make it 2.6 ms, which is 1.4 ms from now.
    let after = cap.reserve(PACED_RESERVATION_BYTES);
    assert_eq!(after.wait(), Duration::from_micros(1_400));
    drop(next);
}

#[test]
fn the_startup_floor_scales_with_max_connections() {
    let per = MIN_CAP_PER_CONNECTION;
    assert_eq!(cap_below_floor(per * 100 - 1, 100), Some(per * 100));
    assert_eq!(cap_below_floor(per * 100, 100), None);
    assert_eq!(cap_below_floor(per * 10, 100), Some(per * 100));
    assert_eq!(cap_below_floor(1, 0), None, "no connections, no floor");
}

/// A socket with no cap paces nothing: `pace` returns at once with no hold.
#[tokio::test]
async fn pace_without_a_cap_returns_no_hold() {
    struct NoCap;
    impl Socket for NoCap {
        async fn bind(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
            Ok(NoCap)
        }
        async fn recv_from(
            &self,
            _buf: &mut [u8],
        ) -> std::io::Result<(usize, std::net::SocketAddr)> {
            std::future::pending().await
        }
        async fn send_to(
            &self,
            buf: &[u8],
            _target: std::net::SocketAddr,
        ) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn send_to_blocking(
            &self,
            buf: &[u8],
            _target: std::net::SocketAddr,
        ) -> std::io::Result<usize> {
            Ok(buf.len())
        }
    }
    let time = VirtualTime::new();
    assert!(pace(&NoCap, &time).await.is_none());
    assert_eq!(time.pending_wakeup_count(), 0, "it never slept");
}

/// Cancelling a paced sender while it sleeps returns its reservation.
#[tokio::test]
async fn a_cancelled_pace_returns_its_reservation() {
    use super::super::DemuxSocket;
    struct Sink;
    impl Socket for Sink {
        async fn bind(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
            Ok(Sink)
        }
        async fn recv_from(
            &self,
            _buf: &mut [u8],
        ) -> std::io::Result<(usize, std::net::SocketAddr)> {
            std::future::pending().await
        }
        async fn send_to(
            &self,
            buf: &[u8],
            _target: std::net::SocketAddr,
        ) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn send_to_blocking(
            &self,
            buf: &[u8],
            _target: std::net::SocketAddr,
        ) -> std::io::Result<usize> {
            Ok(buf.len())
        }
    }
    let time = VirtualTime::new();
    let socket = DemuxSocket::wrap(Sink, Some(EgressCap::new(RATE, time.clone())));
    let cap = socket.egress_cap().expect("cap");
    cap.charge_unpaced(BURST as usize);
    {
        let pacing = pace(&socket, &time);
        tokio::pin!(pacing);
        // Polled once: it reserves and parks on the clock.
        assert!(futures::poll!(pacing.as_mut()).is_pending());
        assert_eq!(cap.level_bytes(), -(PACKET as i64));
    }
    assert_eq!(
        cap.level_bytes(),
        0,
        "dropping the sleeping pace returned it"
    );
}

/// The socket charges every datagram exactly once: unpaced sends at
/// `send_to`, paced ones at `send_to_reserved`, at their real sizes.
#[tokio::test]
async fn the_socket_charges_every_datagram_once() {
    use super::super::DemuxSocket;
    struct Sink;
    impl Socket for Sink {
        async fn bind(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
            Ok(Sink)
        }
        async fn recv_from(
            &self,
            _buf: &mut [u8],
        ) -> std::io::Result<(usize, std::net::SocketAddr)> {
            std::future::pending().await
        }
        async fn send_to(
            &self,
            buf: &[u8],
            _target: std::net::SocketAddr,
        ) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn send_to_blocking(
            &self,
            buf: &[u8],
            _target: std::net::SocketAddr,
        ) -> std::io::Result<usize> {
            Ok(buf.len())
        }
    }
    let target: std::net::SocketAddr = "192.0.2.1:9".parse().unwrap();
    let time = VirtualTime::new();
    let socket = DemuxSocket::wrap(Sink, Some(EgressCap::new(RATE, time.clone())));
    socket.send_to(&[1; 100], target).await.unwrap();
    socket.send_to_blocking(&[1; 50], target).unwrap();
    let hold = pace(&socket, &time).await.expect("hold");
    socket
        .send_to_reserved(&[2; 700], target, hold)
        .await
        .unwrap();
    let cap = socket.egress_cap().expect("cap");
    let stats = cap.stats();
    assert_eq!(stats.bytes, 850);
    assert_eq!(cap.level_bytes(), BURST as i64 - 850);
    assert_eq!(count(cap, EgressDecision::Unpaced), 2);
    assert_eq!(count(cap, EgressDecision::Immediate), 1);
}

/// Seeded property over many concurrent paced senders with random sizes and
/// arrival times: in every window between two sends, the bytes sent are at
/// most the burst plus the rate times the window. This is the token-bucket
/// bound, and the property the cap exists for.
#[test]
fn paced_sends_never_exceed_the_bucket_bound_in_any_window() {
    for seed in [1u64, 7, 42, 0xC0FFEE] {
        GlobalRng::scoped_seed(seed, || check_window_bound(seed));
    }
}

fn check_window_bound(seed: u64) {
    let rate = 200_000 + GlobalRng::random_range(0..2_000_000u64);
    let (cap, time) = cap_at(rate);
    let burst = cap.burst_bytes();
    // Senders arrive over one simulated second; each reserves and is sent
    // when its wait ends. Pending sends are (due time, arrival order, hold).
    let mut arrivals: Vec<(u64, usize)> = (0..400)
        .map(|_| {
            (
                GlobalRng::random_range(0..1_000_000_000u64),
                GlobalRng::random_range(40..=MAX_PACKET_SIZE),
            )
        })
        .collect();
    arrivals.sort_unstable();
    let mut pending: Vec<(u64, usize, EgressHold<'_>, usize)> = Vec::new();
    let mut sent: Vec<(u64, u64)> = Vec::new();
    let mut next_arrival = 0;
    loop {
        let next_due = pending.iter().map(|p| p.0).min();
        let arrival_at = arrivals.get(next_arrival).map(|a| a.0);
        let step = match (next_due, arrival_at) {
            (None, None) => break,
            (Some(d), Some(a)) => d.min(a),
            (Some(d), None) => d,
            (None, Some(a)) => a,
        };
        time.advance_to(step);
        // Sends due now go first, in reservation order.
        pending.sort_by_key(|p| (p.0, p.1));
        while pending.first().is_some_and(|p| p.0 <= step) {
            let (_, _, hold, size) = pending.remove(0);
            hold.settle(size);
            sent.push((step, size as u64));
        }
        while arrivals.get(next_arrival).is_some_and(|a| a.0 <= step) {
            let size = arrivals[next_arrival].1;
            let hold = cap.reserve(size);
            let due = step + hold.wait().as_nanos() as u64;
            pending.push((due, next_arrival, hold, size));
            next_arrival += 1;
        }
    }
    assert_eq!(sent.len(), 400, "seed {seed}");
    for i in 0..sent.len() {
        let mut bytes = 0u64;
        for &(t, size) in &sent[i..] {
            bytes += size;
            let window_ns = (t - sent[i].0) as u128;
            let allowed = burst as u128 + (window_ns * rate as u128).div_ceil(1_000_000_000);
            assert!(
                bytes as u128 <= allowed,
                "seed {seed}: {bytes} bytes in {window_ns} ns, allowed {allowed} \
                 (rate {rate}, burst {burst})"
            );
        }
    }
}

/// Seeded property with unpaced sends mixed in: whatever the order, every
/// byte is charged, and the bytes sent stay within the burst plus the rate
/// times the elapsed time plus the forgiven debt plus the debt still owed.
/// That is the "holds on average" guarantee: unpaced bytes are not lost
/// from the account, they delay later paced sends.
#[test]
fn unpaced_debt_is_accounted_and_paid_by_paced_sends() {
    for seed in [3u64, 99, 1234] {
        GlobalRng::scoped_seed(seed, || {
            let rate = 500_000u64;
            let (cap, time) = cap_at(rate);
            let burst = cap.burst_bytes() as i128;
            let mut total = 0u64;
            for _ in 0..2_000 {
                time.advance(Duration::from_micros(GlobalRng::random_range(0..2_000u64)));
                let size = GlobalRng::random_range(40..=MAX_PACKET_SIZE);
                if GlobalRng::random_range(0..4u32) == 0 {
                    cap.charge_unpaced(size);
                } else {
                    let hold = cap.reserve(size);
                    time.advance(hold.wait());
                    hold.settle(size);
                }
                total += size as u64;
                let stats = cap.stats();
                assert_eq!(stats.bytes, total, "seed {seed}: every byte is charged");
                let elapsed = time.now_nanos() as i128;
                let refilled = elapsed * rate as i128 / 1_000_000_000;
                let owed = (-cap.level_bytes()).max(0) as i128;
                assert!(
                    total as i128 <= burst + refilled + stats.forgiven_bytes as i128 + owed + 3,
                    "seed {seed}: {total} sent after {elapsed} ns"
                );
            }
        });
    }
}

/// Regression: an unpaced charge must never move `empty_at` backwards. With
/// more paced reservations queued than `DEBT_LIMIT` covers, the old
/// `min(refilled + cost, now + limit)` pulled the queue end back to
/// `now + limit` on every ACK, erasing queued reservations; their holders
/// still sent at their given times while new reservations reused the same
/// slots, so paced traffic alone ran above the cap.
///
/// The bound checked here is on PACED bytes only, with no allowance for
/// forgiven bytes: forgiveness may only ever drop unpaced charges, never
/// paced ones.
#[test]
fn unpaced_charges_never_erase_queued_paced_reservations() {
    for seed in [5u64, 17, 2024] {
        GlobalRng::scoped_seed(seed, || {
            // 100 KB/s and 150 streams each keeping one reservation queued:
            // 150 x 1 200 bytes is 1.8 s of the rate, beyond the 1 s limit.
            let rate = 100_000u64;
            let (cap, time) = cap_at(rate);
            let burst = cap.burst_bytes() as u128;
            let streams = 150usize;
            let per_stream = 6usize;
            // (due ns, stream, hold); each stream reserves its next datagram
            // when the previous one is sent.
            let mut pending: Vec<(u64, usize, EgressHold<'_>)> = (0..streams)
                .map(|i| {
                    let hold = cap.reserve(PACKET);
                    (hold.wait().as_nanos() as u64, i, hold)
                })
                .collect();
            let mut remaining = vec![per_stream - 1; streams];
            let mut paced_sends: Vec<(u64, u64)> = Vec::new();
            let mut next_unpaced = 0u64;
            while !pending.is_empty() {
                let next_due = pending.iter().map(|p| p.0).min().unwrap();
                if next_unpaced <= next_due {
                    time.advance_to(next_unpaced);
                    // ACKs and Pongs, unpaced, every 2 ms.
                    cap.charge_unpaced(GlobalRng::random_range(40..200));
                    next_unpaced += 2_000_000;
                    continue;
                }
                time.advance_to(next_due);
                pending.sort_by_key(|p| (p.0, p.1));
                while pending.first().is_some_and(|p| p.0 <= next_due) {
                    let (_, stream, hold) = pending.remove(0);
                    hold.settle(PACKET);
                    paced_sends.push((next_due, PACKET as u64));
                    if remaining[stream] > 0 {
                        remaining[stream] -= 1;
                        let hold = cap.reserve(PACKET);
                        pending.push((next_due + hold.wait().as_nanos() as u64, stream, hold));
                    }
                }
            }
            assert_eq!(paced_sends.len(), streams * per_stream);
            for i in 0..paced_sends.len() {
                let mut bytes = 0u128;
                for &(t, size) in &paced_sends[i..] {
                    bytes += size as u128;
                    let window_ns = (t - paced_sends[i].0) as u128;
                    let allowed = burst + (window_ns * rate as u128).div_ceil(1_000_000_000);
                    assert!(
                        bytes <= allowed,
                        "seed {seed}: {bytes} paced bytes in {window_ns} ns, allowed {allowed}"
                    );
                }
            }
        });
    }
}

/// An inner socket whose sends fail or succeed on command.
struct Flaky {
    fail: std::sync::atomic::AtomicBool,
}

impl Socket for Flaky {
    async fn bind(_addr: std::net::SocketAddr) -> std::io::Result<Self> {
        Ok(Flaky {
            fail: std::sync::atomic::AtomicBool::new(false),
        })
    }
    async fn recv_from(&self, _buf: &mut [u8]) -> std::io::Result<(usize, std::net::SocketAddr)> {
        std::future::pending().await
    }
    async fn send_to(&self, buf: &[u8], _target: std::net::SocketAddr) -> std::io::Result<usize> {
        self.send_to_blocking(buf, _target)
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

/// A failed unpaced send is not charged; only datagrams that were sent are.
#[tokio::test]
async fn a_failed_unpaced_send_is_not_charged() {
    use super::super::DemuxSocket;
    let target: std::net::SocketAddr = "192.0.2.1:9".parse().unwrap();
    let time = VirtualTime::new();
    let socket = DemuxSocket::wrap(
        Flaky {
            fail: std::sync::atomic::AtomicBool::new(true),
        },
        Some(EgressCap::new(RATE, time.clone())),
    );
    assert!(socket.send_to(&[1; 900], target).await.is_err());
    assert!(socket.send_to_blocking(&[1; 900], target).is_err());
    let cap = socket.egress_cap().expect("cap");
    assert_eq!(cap.level_bytes(), BURST as i64, "nothing charged");
    assert_eq!(cap.stats().packets.iter().sum::<u64>(), 0);
    socket.inner_for_test().fail.store(false, Ordering::Relaxed);
    socket.send_to(&[1; 900], target).await.unwrap();
    assert_eq!(cap.level_bytes(), BURST as i64 - 900);
}

/// A failed paced send returns its reservation: the next paced sender is
/// not delayed by a datagram that never left.
#[tokio::test]
async fn a_failed_paced_send_returns_its_reservation() {
    use super::super::DemuxSocket;
    let target: std::net::SocketAddr = "192.0.2.1:9".parse().unwrap();
    let time = VirtualTime::new();
    let socket = DemuxSocket::wrap(
        Flaky {
            fail: std::sync::atomic::AtomicBool::new(true),
        },
        Some(EgressCap::new(RATE, time.clone())),
    );
    let cap = socket.egress_cap().expect("cap");
    cap.charge_unpaced(BURST as usize);
    let hold = cap.reserve(PACED_RESERVATION_BYTES);
    assert_eq!(cap.level_bytes(), -(PACED_RESERVATION_BYTES as i64));
    time.advance(hold.wait());
    assert!(
        socket
            .send_to_reserved(&[2; 700], target, hold)
            .await
            .is_err()
    );
    // 1.2 ms of refill from an empty bucket, as if nothing was reserved.
    assert_eq!(
        cap.level_bytes(),
        PACED_RESERVATION_BYTES as i64,
        "the reservation was returned"
    );
    assert_eq!(cap.stats().packets[EgressDecision::Immediate as usize], 0);
    assert_eq!(cap.stats().packets[EgressDecision::Waited as usize], 0);
}
