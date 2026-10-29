//! Whether this node may upgrade a UDP session to QUIC, and how many QUIC
//! paths it may hold (#5940, step e5a-2).
//!
//! [`QuicPolicy::allowed`] is true only when all three hold:
//!
//! - `transport-quic` is not `off` ([`QuicMode`]; `prefer` runs as
//!   `shadow` in this release);
//! - the endpoint is [`QuicAvailability::Up`]. Every other availability is
//!   final, so once the endpoint is down the node never upgrades again
//!   before it restarts;
//! - the crash-loop breaker is not keeping QUIC off
//!   ([`breaker`](super::super::breaker)). Today a suppressing breaker also
//!   stops the endpoint being built, so the availability is `Disabled`
//!   anyway; the policy checks the breaker itself so that it holds without
//!   relying on that.
//!
//! [`QuicPathPermits`] bounds the paths the node holds, from offering or
//! dialing through active, at `quic-max-paths`. A permit is released when it
//! is dropped, so a path cannot keep one past its own end.
//!
//! Nothing in the node builds a policy yet: the upgrade negotiation that
//! reads it comes in later steps (e5a-3 to e5a-5).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::transport::quic::QuicMode;
use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};

/// The node's QUIC upgrade policy. See the module docs.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) struct QuicPolicy {
    /// The mode this release runs ([`QuicMode::effective`]): never `Prefer`.
    mode: QuicMode,
    availability: QuicAvailabilityWatch,
    breaker_suppressed: bool,
    permits: QuicPathPermits,
}

/// Why [`QuicPolicy::admit`] refused a new path.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathRefusal {
    /// [`QuicPolicy::allowed`] is false.
    Disabled,
    /// Every permit is held.
    Busy,
}

/// A bounded count of the QUIC paths the node holds. Clones share the count.
#[derive(Debug, Clone)]
pub(crate) struct QuicPathPermits(Arc<PermitCount>);

#[derive(Debug)]
struct PermitCount {
    limit: usize,
    in_use: AtomicUsize,
}

/// One held path slot, released when dropped.
#[derive(Debug)]
pub(crate) struct QuicPathPermit(Arc<PermitCount>);

impl Drop for QuicPathPermit {
    fn drop(&mut self) {
        self.0.in_use.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl QuicPolicy {
    /// The policy for a node configured with `configured`, whose endpoint
    /// reports `availability`, with `breaker_suppressed` set when the
    /// crash-loop breaker keeps QUIC off for this run, holding at most
    /// `max_paths` paths (`quic-max-paths`). Logs at INFO when `configured`
    /// is `Prefer` ([`QuicMode::effective`]).
    pub(crate) fn new(
        configured: QuicMode,
        availability: QuicAvailabilityWatch,
        breaker_suppressed: bool,
        max_paths: usize,
    ) -> Self {
        Self {
            mode: configured.effective(),
            availability,
            breaker_suppressed,
            permits: QuicPathPermits::new(max_paths),
        }
    }

    /// The mode the node runs: `Off` or `Shadow`.
    pub(crate) fn mode(&self) -> QuicMode {
        self.mode
    }

    /// Whether the node may upgrade a session to QUIC now. Read it again for
    /// each decision: it turns false for good when the endpoint goes down.
    pub(crate) fn allowed(&self) -> bool {
        self.mode != QuicMode::Off
            && self.availability.get() == QuicAvailability::Up
            && !self.breaker_suppressed
    }

    /// A permit for one new path, or why there is none. A disallowed policy
    /// refuses without touching the permits.
    pub(crate) fn admit(&self) -> Result<QuicPathPermit, PathRefusal> {
        if !self.allowed() {
            return Err(PathRefusal::Disabled);
        }
        self.permits.try_acquire().ok_or(PathRefusal::Busy)
    }

    /// The path permits.
    pub(crate) fn permits(&self) -> &QuicPathPermits {
        &self.permits
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl QuicPathPermits {
    /// Permits for at most `limit` paths at once. A limit of 0 admits none.
    pub(crate) fn new(limit: usize) -> Self {
        Self(Arc::new(PermitCount {
            limit,
            in_use: AtomicUsize::new(0),
        }))
    }

    /// A permit, or `None` when `limit` are held.
    pub(crate) fn try_acquire(&self) -> Option<QuicPathPermit> {
        let count = &self.0;
        count
            .in_use
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held < count.limit).then_some(held + 1)
            })
            .ok()
            .map(|_| QuicPathPermit(Arc::clone(&self.0)))
    }

    /// The permits held now.
    pub(crate) fn in_use(&self) -> usize {
        self.0.in_use.load(Ordering::Acquire)
    }

    /// The most permits that can be held at once.
    pub(crate) fn limit(&self) -> usize {
        self.0.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const AVAILABILITIES: [QuicAvailability; 4] = [
        QuicAvailability::Disabled,
        QuicAvailability::Up,
        QuicAvailability::Down,
        QuicAvailability::Released,
    ];

    fn policy(
        mode: QuicMode,
        availability: QuicAvailability,
        breaker_suppressed: bool,
        max_paths: usize,
    ) -> QuicPolicy {
        let (_sender, watch) = QuicAvailabilityWatch::for_test(availability);
        QuicPolicy::new(mode, watch, breaker_suppressed, max_paths)
    }

    /// `allowed()` over every combination of its three inputs: true only for
    /// a mode other than off, an endpoint that is up, and no breaker
    /// suppression. Each rule has a case where it alone decides, so dropping
    /// any one of them fails this test.
    #[test]
    fn allowed_only_when_on_up_and_not_suppressed() {
        let mut allowed_cases = 0;
        for mode in [QuicMode::Off, QuicMode::Shadow, QuicMode::Prefer] {
            for availability in AVAILABILITIES {
                for suppressed in [false, true] {
                    let expected = mode != QuicMode::Off
                        && availability == QuicAvailability::Up
                        && !suppressed;
                    let policy = policy(mode, availability, suppressed, 4);
                    assert_eq!(
                        policy.allowed(),
                        expected,
                        "mode {mode:?}, availability {availability:?}, suppressed {suppressed}"
                    );
                    let admitted = policy.admit();
                    assert_eq!(admitted.is_ok(), expected);
                    if !expected {
                        assert_eq!(admitted.unwrap_err(), PathRefusal::Disabled);
                        assert_eq!(policy.permits().in_use(), 0, "a refusal holds nothing");
                    }
                    allowed_cases += usize::from(expected);
                }
            }
        }
        assert_eq!(allowed_cases, 2, "shadow and prefer, up, not suppressed");
    }

    /// The policy reads the availability at each call: an endpoint that goes
    /// down after the policy was built turns it off, and paths admitted
    /// before keep their permits until dropped.
    #[test]
    fn endpoint_going_down_turns_the_policy_off() {
        let (sender, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let policy = QuicPolicy::new(QuicMode::Shadow, watch, false, 2);
        let held = policy.admit().expect("allowed while up");
        sender.send_replace(QuicAvailability::Down);
        assert!(!policy.allowed());
        assert_eq!(policy.admit().unwrap_err(), PathRefusal::Disabled);
        assert_eq!(policy.permits().in_use(), 1);
        drop(held);
        assert_eq!(policy.permits().in_use(), 0);
    }

    /// Permits stop at the limit, a dropped permit frees its slot, and the
    /// count returns to zero when every permit is gone. Clones share it.
    #[test]
    fn permits_respect_the_limit_and_release_on_drop() {
        let permits = QuicPathPermits::new(2);
        let first = permits.try_acquire().expect("first");
        let second = permits.clone().try_acquire().expect("second, via a clone");
        assert!(permits.try_acquire().is_none(), "at the limit");
        assert_eq!(permits.in_use(), 2);
        drop(first);
        assert_eq!(permits.in_use(), 1);
        let third = permits.try_acquire().expect("a released slot is reusable");
        assert!(permits.try_acquire().is_none());
        drop((second, third));
        assert_eq!(permits.in_use(), 0);
        assert_eq!(permits.limit(), 2);
    }

    /// A limit of 0 admits nothing, and an allowed policy at its limit
    /// refuses with `Busy`, not `Disabled`.
    #[test]
    fn zero_limit_and_busy_refusal() {
        assert!(QuicPathPermits::new(0).try_acquire().is_none());
        let policy = policy(QuicMode::Shadow, QuicAvailability::Up, false, 1);
        let held = policy.admit().expect("one slot");
        assert_eq!(policy.admit().unwrap_err(), PathRefusal::Busy);
        drop(held);
        assert!(policy.admit().is_ok());
        let none = self::policy(QuicMode::Shadow, QuicAvailability::Up, false, 0);
        assert_eq!(none.admit().unwrap_err(), PathRefusal::Busy);
    }

    /// Threads racing for permits never hold more than the limit, and every
    /// permit is returned.
    #[test]
    fn concurrent_acquires_never_exceed_the_limit() {
        const LIMIT: usize = 3;
        let permits = QuicPathPermits::new(LIMIT);
        let peak = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let permits = permits.clone();
                let peak = Arc::clone(&peak);
                std::thread::spawn(move || {
                    for _ in 0..2_000 {
                        if let Some(permit) = permits.try_acquire() {
                            peak.fetch_max(permits.in_use(), Ordering::AcqRel);
                            drop(permit);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(peak.load(Ordering::Acquire) <= LIMIT);
        assert_eq!(permits.in_use(), 0);
    }

    /// `prefer` runs as `shadow` and says so at INFO; `off` and `shadow` are
    /// themselves and log nothing.
    #[test]
    fn prefer_runs_as_shadow_with_an_info_log() {
        const NEEDLE: &str = "runs as \"shadow\" in this release";
        for (configured, effective, logs_it) in [
            (QuicMode::Off, QuicMode::Off, false),
            (QuicMode::Shadow, QuicMode::Shadow, false),
            (QuicMode::Prefer, QuicMode::Shadow, true),
        ] {
            let (logs, guard) = crate::util::test_log_capture::install();
            let policy = policy(configured, QuicAvailability::Up, false, 1);
            drop(guard);
            assert_eq!(policy.mode(), effective, "{configured:?}");
            let logs = logs.lock().unwrap().clone();
            let lines: Vec<_> = logs.iter().filter(|line| line.contains(NEEDLE)).collect();
            assert_eq!(
                lines.len(),
                usize::from(logs_it),
                "{configured:?}; captured: {logs:?}"
            );
            if let Some(line) = lines.first() {
                assert!(line.starts_with("INFO"), "{line}");
            }
        }
    }
}
