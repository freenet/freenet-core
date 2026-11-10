use std::time::{Duration, Instant};

use super::{GateState, LOSS_SHARE_CEILING, QUEUE_FLOOR, SEND_BUCKETS, Verdict, queue_threshold};

const MS: Duration = Duration::from_millis(1);

/// Feed `gate` `rounds` round trips of `per_round` acknowledged datagrams
/// on a path of `rtt`, each sample `rtt + queue`, from `start`. Returns the
/// time the next round would start and the send time of the last datagram.
fn steady(
    gate: &mut GateState,
    start: Instant,
    rtt: Duration,
    queue: Duration,
    rounds: u32,
    per_round: u32,
) -> (Instant, Instant) {
    let mut now = start;
    let mut last = start;
    for _ in 0..rounds {
        for i in 0..per_round {
            let at = now + rtt.mul_f64(f64::from(i) / f64::from(per_round));
            let sample = rtt + queue;
            gate.on_ack(at + sample, at, 1200, sample);
            last = at;
        }
        now += rtt + queue;
    }
    (now, last)
}

#[test]
fn the_threshold_is_a_tenth_of_the_rtt_with_a_floor() {
    assert_eq!(queue_threshold(5 * MS), QUEUE_FLOOR);
    assert_eq!(queue_threshold(25 * MS), Duration::from_micros(2500));
    assert_eq!(queue_threshold(800 * MS), 80 * MS);
}

/// The threshold for the 100 ms paths below.
const THRESHOLD_100: Duration = Duration::from_millis(10);

#[test]
fn a_loss_with_no_queue_is_ignored() {
    let mut gate = GateState::default();
    let (now, last) = steady(&mut gate, Instant::now(), 100 * MS, Duration::ZERO, 4, 40);
    assert_eq!(gate.queue_at(last), Some(Duration::ZERO));
    assert_eq!(gate.on_loss(now, last, 1200, false), Verdict::Ignored);
    assert_eq!((gate.passed, gate.ignored), (0, 1));
}

#[test]
fn a_loss_with_a_standing_queue_counts() {
    let mut gate = GateState::default();
    // The minimum is set by the first rounds with no queue, then a queue of
    // 10 ms builds (a tenth of the path's RTT).
    let (now, _) = steady(&mut gate, Instant::now(), 100 * MS, Duration::ZERO, 2, 40);
    let (now, last) = steady(&mut gate, now, 100 * MS, THRESHOLD_100, 3, 40);
    assert_eq!(gate.queue_at(last), Some(THRESHOLD_100));
    assert_eq!(gate.on_loss(now, last, 1200, false), Verdict::Queue);
    assert_eq!((gate.passed, gate.ignored), (1, 0));
}

#[test]
fn a_queue_just_below_the_threshold_does_not_count() {
    let mut gate = GateState::default();
    let (now, _) = steady(&mut gate, Instant::now(), 100 * MS, Duration::ZERO, 2, 40);
    let (now, last) = steady(&mut gate, now, 100 * MS, THRESHOLD_100 - MS / 10, 3, 40);
    assert_eq!(gate.on_loss(now, last, 1200, false), Verdict::Ignored);
}

/// One flow on a shallow buffer fills it and drains it every round: the
/// smallest RTT of the round shows no queue, but the packets sent next to
/// the one the full queue dropped were delayed by it. Judged at the lost
/// packet's send time, the loss counts; judged by the round minimum, as the
/// first version of the gate did, it read as random.
#[test]
fn a_loss_where_an_oscillating_queue_peaked_counts() {
    let mut gate = GateState::default();
    let rtt = 100 * MS;
    let step = rtt / 40;
    let start = Instant::now();
    let (mut peak_sent, mut trough_sent) = (start, start);
    // A datagram every 2.5 ms for 8 round trips. The queue rises from 0 to
    // 20 ms over one round trip and drains over the next, the sawtooth of
    // one Cubic flow on a buffer it overflows at the top.
    for n in 0..320u32 {
        let at = start + step * n;
        let phase = n % 80;
        let rising = if phase < 40 { phase } else { 80 - phase };
        let queue = (20 * MS) * rising / 40;
        gate.on_ack(at + rtt + queue, at, 1200, rtt + 10 * MS);
        if phase == 40 {
            peak_sent = at;
        }
        if phase == 0 {
            trough_sent = at;
        }
    }
    let now = start + step * 320 + rtt;
    assert!(
        gate.queue() < Some(QUEUE_FLOOR),
        "the round minimum shows no queue: {:?}",
        gate.queue()
    );
    assert!(gate.queue_at(peak_sent) >= Some(THRESHOLD_100));
    assert_eq!(gate.on_loss(now, peak_sent, 1200, false), Verdict::Queue);
    // A loss where the same queue was empty is still random.
    assert_eq!(gate.queue_at(trough_sent), Some(Duration::ZERO));
    assert_eq!(
        gate.on_loss(now, trough_sent, 1200, false),
        Verdict::Ignored
    );
}

/// Jitter raises some samples, not all: the smallest RTT near the lost
/// packet is the one the gate reads, so random delay on a path with no
/// queue does not read as a queue.
#[test]
fn jitter_on_some_samples_is_not_a_queue() {
    let mut gate = GateState::default();
    let mut now = Instant::now();
    let rtt = 50 * MS;
    let mut sends = Vec::new();
    for round in 0..6u32 {
        for i in 0..40u32 {
            let at = now + rtt.mul_f64(f64::from(i) / 40.0);
            // Most samples carry 0 to 30 ms of jitter; every fourth none.
            let jitter = if (i + round) % 4 == 0 {
                Duration::ZERO
            } else {
                MS * ((i * 7 + round * 13) % 30)
            };
            gate.on_ack(at + rtt + jitter, at, 1200, rtt + 15 * MS);
            sends.push(at);
        }
        now += rtt + 15 * MS;
    }
    for &sent in &sends[sends.len() - 80..] {
        assert_eq!(gate.queue_at(sent), Some(Duration::ZERO), "{sent:?}");
    }
    assert_eq!(
        gate.on_loss(now, sends[sends.len() - 1], 1200, false),
        Verdict::Ignored
    );
}

/// A lost packet sent before every span the gate keeps is judged by the
/// round minimum instead.
#[test]
fn a_loss_older_than_the_kept_spans_falls_back_to_the_round_minimum() {
    let mut gate = GateState::default();
    let start = Instant::now();
    let (now, _) = steady(&mut gate, start, 100 * MS, Duration::ZERO, 2, 40);
    let (now, _) = steady(&mut gate, now, 100 * MS, THRESHOLD_100, 8, 40);
    assert_eq!(gate.sends.len(), SEND_BUCKETS, "the spans are bounded");
    assert_eq!(gate.queue_at(start), None);
    assert_eq!(gate.on_loss(now, start, 1200, false), Verdict::Queue);
}

/// An acknowledgement that arrives out of order lowers the span its packet
/// was sent in, not the newest one.
#[test]
fn an_out_of_order_acknowledgement_goes_to_its_own_span() {
    let mut gate = GateState::default();
    let rtt = 80 * MS;
    let (now, _) = steady(&mut gate, Instant::now(), rtt, Duration::ZERO, 2, 40);
    let (now, _) = steady(&mut gate, now, rtt, 10 * MS, 3, 40);
    let early = now - 2 * rtt;
    let before = gate.queue_at(early);
    gate.on_ack(early + rtt, early, 1200, rtt);
    assert!(gate.queue_at(early) < before, "{before:?}");
    assert_eq!(gate.sends.back().map(|s| s.1), Some(rtt + 10 * MS));
}

#[test]
fn persistent_congestion_always_counts() {
    let mut gate = GateState::default();
    let (now, last) = steady(&mut gate, Instant::now(), 100 * MS, Duration::ZERO, 4, 40);
    assert_eq!(gate.on_loss(now, last, 1200, true), Verdict::Persistent);
}

#[test]
fn a_loss_before_any_rtt_sample_counts() {
    let mut gate = GateState::default();
    let now = Instant::now();
    assert_eq!(gate.on_loss(now, now, 1200, false), Verdict::NoSample);
}

/// A path losing a quarter of what it sends is pushed too hard, or
/// policed, whatever the queue says: every loss then counts.
#[test]
fn a_heavy_loss_share_counts_with_no_queue() {
    let mut gate = GateState::default();
    let rtt = 100 * MS;
    let mut now = Instant::now();
    let mut verdicts = Vec::new();
    for _ in 0..12 {
        for i in 0..30u32 {
            let at = now + rtt.mul_f64(f64::from(i) / 30.0);
            gate.on_ack(at + rtt, at, 1200, rtt);
        }
        // 15 datagrams lost against 30 acknowledged: a third.
        for _ in 0..15 {
            verdicts.push(gate.on_loss(now + rtt, now, 1200, false));
        }
        now += rtt;
    }
    assert!(
        gate.loss_share() >= LOSS_SHARE_CEILING,
        "{}",
        gate.loss_share()
    );
    assert_eq!(verdicts.last(), Some(&Verdict::LossShare));
    assert_eq!(verdicts.first(), Some(&Verdict::Ignored));
}

/// 15% random loss stays below the share ceiling, so it stays ignored.
#[test]
fn fifteen_percent_loss_stays_under_the_share_ceiling() {
    let mut gate = GateState::default();
    let rtt = 100 * MS;
    let mut now = Instant::now();
    for _ in 0..40 {
        for i in 0..85u32 {
            let at = now + rtt.mul_f64(f64::from(i) / 85.0);
            gate.on_ack(at + rtt, at, 1200, rtt);
        }
        for _ in 0..15 {
            assert_eq!(gate.on_loss(now + rtt, now, 1200, false), Verdict::Ignored);
        }
        now += rtt;
    }
    assert!(
        (gate.loss_share() - 0.15).abs() < 0.02,
        "{}",
        gate.loss_share()
    );
}

/// A round with too few datagrams does not move the share: one loss in a
/// window of 8 is not a 12% loss rate.
#[test]
fn small_rounds_do_not_move_the_loss_share() {
    let mut gate = GateState::default();
    let rtt = 100 * MS;
    let mut now = Instant::now();
    for _ in 0..20 {
        for i in 0..6u32 {
            let at = now + rtt.mul_f64(f64::from(i) / 6.0);
            gate.on_ack(at + rtt, at, 1200, rtt);
        }
        gate.on_loss(now + rtt, now, 2400, false);
        now += rtt;
    }
    assert_eq!(gate.loss_share(), 0.0);
}

/// A route that grows longer reads as a queue, so the gate passes every
/// loss: it falls back to Cubic, not to ignoring congestion.
#[test]
fn a_longer_route_falls_back_to_passing_losses() {
    let mut gate = GateState::default();
    let (now, _) = steady(&mut gate, Instant::now(), 50 * MS, Duration::ZERO, 3, 40);
    let (now, last) = steady(&mut gate, now, 80 * MS, Duration::ZERO, 3, 40);
    assert_eq!(gate.on_loss(now, last, 1200, false), Verdict::Queue);
}
