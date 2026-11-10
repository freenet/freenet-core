use std::time::{Duration, Instant};

use super::{GateState, LOSS_SHARE_CEILING, QUEUE_CEILING, QUEUE_FLOOR, Verdict, queue_threshold};

const MS: Duration = Duration::from_millis(1);

/// A gate that has seen `rounds` round trips of `per_round` acknowledged
/// datagrams on a path of `rtt`, each sample `rtt + queue`, from `start`.
/// Returns the time after the last round.
fn steady(
    gate: &mut GateState,
    start: Instant,
    rtt: Duration,
    queue: Duration,
    rounds: u32,
    per_round: u32,
) -> Instant {
    let mut now = start;
    for _ in 0..rounds {
        for i in 0..per_round {
            let at = now + rtt.mul_f64(f64::from(i) / f64::from(per_round));
            let sample = rtt + queue;
            gate.on_ack(at + sample, at, 1200, sample);
        }
        now += rtt + queue;
    }
    now + rtt + queue
}

#[test]
fn the_threshold_is_a_tenth_of_the_rtt_within_the_floor_and_ceiling() {
    assert_eq!(queue_threshold(5 * MS), QUEUE_FLOOR);
    assert_eq!(queue_threshold(25 * MS), Duration::from_micros(2500));
    assert_eq!(queue_threshold(300 * MS), QUEUE_CEILING);
}

#[test]
fn a_loss_with_no_queue_is_ignored() {
    let mut gate = GateState::default();
    let start = Instant::now();
    let now = steady(&mut gate, start, 100 * MS, Duration::ZERO, 4, 40);
    assert_eq!(gate.queue(), Some(Duration::ZERO));
    assert_eq!(gate.on_loss(now, 1200, false), Verdict::Ignored);
    assert_eq!((gate.passed, gate.ignored), (0, 1));
}

#[test]
fn a_loss_with_a_standing_queue_counts() {
    let mut gate = GateState::default();
    let start = Instant::now();
    // The minimum is set by the first rounds with no queue, then a queue of
    // 3 ms builds (the ceiling for a 100 ms path, whose tenth is 10 ms).
    let now = steady(&mut gate, start, 100 * MS, Duration::ZERO, 2, 40);
    let now = steady(&mut gate, now, 100 * MS, QUEUE_CEILING, 3, 40);
    assert_eq!(gate.queue(), Some(QUEUE_CEILING));
    assert_eq!(gate.on_loss(now, 1200, false), Verdict::Queue);
}

#[test]
fn a_queue_just_below_the_threshold_does_not_count() {
    let mut gate = GateState::default();
    let start = Instant::now();
    let now = steady(&mut gate, start, 100 * MS, Duration::ZERO, 2, 40);
    let now = steady(&mut gate, now, 100 * MS, QUEUE_CEILING - MS / 10, 3, 40);
    assert_eq!(gate.on_loss(now, 1200, false), Verdict::Ignored);
}

/// Jitter raises some samples, not all: the smallest sample of a round is
/// the one the gate reads, so random delay on a path with no queue does not
/// read as a queue.
#[test]
fn jitter_on_some_samples_is_not_a_queue() {
    let mut gate = GateState::default();
    let mut now = Instant::now();
    let rtt = 50 * MS;
    for round in 0..6u32 {
        for i in 0..40u32 {
            let at = now + rtt.mul_f64(f64::from(i) / 40.0);
            // Every sample but one per round carries 0 to 30 ms of jitter.
            let jitter = if i == round % 40 {
                Duration::ZERO
            } else {
                MS * ((i * 7 + round * 13) % 30)
            };
            gate.on_ack(at + rtt + jitter, at, 1200, rtt + 15 * MS);
        }
        now += rtt + 15 * MS;
    }
    assert_eq!(gate.queue(), Some(Duration::ZERO));
    assert_eq!(gate.on_loss(now, 1200, false), Verdict::Ignored);
}

#[test]
fn persistent_congestion_always_counts() {
    let mut gate = GateState::default();
    let start = Instant::now();
    let now = steady(&mut gate, start, 100 * MS, Duration::ZERO, 4, 40);
    assert_eq!(gate.on_loss(now, 1200, true), Verdict::Persistent);
}

#[test]
fn a_loss_before_any_rtt_sample_counts() {
    let mut gate = GateState::default();
    assert_eq!(gate.on_loss(Instant::now(), 1200, false), Verdict::NoSample);
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
            verdicts.push(gate.on_loss(now + rtt, 1200, false));
        }
        now += rtt;
    }
    assert!(gate.loss_share() >= LOSS_SHARE_CEILING, "{}", gate.loss_share());
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
            assert_eq!(gate.on_loss(now + rtt, 1200, false), Verdict::Ignored);
        }
        now += rtt;
    }
    assert!((gate.loss_share() - 0.15).abs() < 0.02, "{}", gate.loss_share());
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
        gate.on_loss(now + rtt, 2400, false);
        now += rtt;
    }
    assert_eq!(gate.loss_share(), 0.0);
}

/// A route that grows longer reads as a queue, so the gate passes every
/// loss: it falls back to Cubic, not to ignoring congestion.
#[test]
fn a_longer_route_falls_back_to_passing_losses() {
    let mut gate = GateState::default();
    let start = Instant::now();
    let now = steady(&mut gate, start, 50 * MS, Duration::ZERO, 3, 40);
    let now = steady(&mut gate, now, 80 * MS, Duration::ZERO, 3, 40);
    assert_eq!(gate.on_loss(now, 1200, false), Verdict::Queue);
}
