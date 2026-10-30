//! Whether this node may upgrade a UDP session to QUIC, and how many QUIC
//! paths it may hold (#5940, step e5a-2).
//!
//! [`QuicPolicy::disabled_by`] names the first rule that stops upgrades, and
//! [`QuicPolicy::allowed`] is true when none does:
//!
//! - `transport-quic` is not `off` ([`QuicMode`]; `prefer` runs as `shadow`
//!   in this release);
//! - the crash-loop breaker is not keeping QUIC off ([`BreakerOutcome`]).
//!   Today a suppressing breaker also stops the endpoint being built, so the
//!   availability is `Disabled` anyway; the policy checks the breaker itself
//!   so the result does not depend on that, and so a refusal names the cause;
//! - `quic-max-paths` is not 0, which is a decision to turn upgrades off;
//! - the endpoint is [`QuicAvailability::Up`]. Every other availability is
//!   final, so once the endpoint is down the node does not upgrade again
//!   before it restarts.
//!
//! [`QuicPathPermits`] bounds the paths the node holds, from offering or
//! dialing through active, at `quic-max-paths`. A permit is released when it
//! is dropped, so a path cannot keep one past its own end.
//!
//! Nothing in the node builds a policy yet: the upgrade negotiation that
//! reads it comes in later steps (e5a-3 to e5a-5). [`log_startup`] already
//! runs at node startup.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::config::{NetworkApiConfig, QuicMaxPathsSetting};
use crate::transport::quic::QuicMode;
use crate::transport::quic::breaker::{BreakerOutcome, Verdict};
use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};

/// The node's QUIC upgrade policy. See the module docs.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) struct QuicPolicy {
    /// The mode this release runs ([`QuicMode::effective`]): never `Prefer`.
    mode: QuicMode,
    breaker: BreakerOutcome,
    availability: QuicAvailabilityWatch,
    permits: QuicPathPermits,
}

/// Why the policy refuses upgrades.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DisabledBy {
    /// `transport-quic` is off.
    Mode,
    /// The crash-loop breaker keeps QUIC off for this run, with this
    /// verdict.
    Breaker(Verdict),
    /// `quic-max-paths` is 0.
    NoPaths,
    /// The endpoint is not up: never built, down, or released.
    Endpoint(QuicAvailability),
}

/// Why [`QuicPolicy::admit`] refused a new path.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathRefusal {
    /// The policy refuses upgrades at all.
    Disabled(DisabledBy),
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
    /// The policy for a node configured by `config`, whose crash-loop
    /// breaker decided `breaker` ([`breaker::gate`]) and whose endpoint
    /// reports `availability`. Logs nothing: [`log_startup`] reports the
    /// configuration once.
    ///
    /// [`breaker::gate`]: crate::transport::quic::breaker::gate
    pub(crate) fn new(
        config: &NetworkApiConfig,
        breaker: BreakerOutcome,
        availability: QuicAvailabilityWatch,
    ) -> Self {
        Self {
            mode: configured_mode(config).effective(),
            breaker,
            availability,
            permits: QuicPathPermits::new(config.quic_max_paths()),
        }
    }

    /// The mode the node runs: `Off` or `Shadow`.
    pub(crate) fn mode(&self) -> QuicMode {
        self.mode
    }

    /// The first rule that stops upgrades now, in the order of the module
    /// docs, or `None` when upgrades are allowed. Read it again for each
    /// decision: it changes for good when the endpoint goes down.
    pub(crate) fn disabled_by(&self) -> Option<DisabledBy> {
        if self.mode == QuicMode::Off {
            return Some(DisabledBy::Mode);
        }
        if let Some(verdict) = self.breaker.suppressing_verdict() {
            return Some(DisabledBy::Breaker(verdict));
        }
        if self.permits.limit() == 0 {
            return Some(DisabledBy::NoPaths);
        }
        match self.availability.get() {
            QuicAvailability::Up => None,
            other @ (QuicAvailability::Disabled
            | QuicAvailability::Down
            | QuicAvailability::Released) => Some(DisabledBy::Endpoint(other)),
        }
    }

    /// Whether the node may upgrade a session to QUIC now.
    pub(crate) fn allowed(&self) -> bool {
        self.disabled_by().is_none()
    }

    /// A permit for one new path, or why there is none. A disabled policy
    /// refuses without touching the permits.
    ///
    /// `Ok` does not prove the endpoint is still up: it can go down just
    /// after the check, so a path must still handle a down endpoint.
    pub(crate) fn admit(&self) -> Result<QuicPathPermit, PathRefusal> {
        if let Some(cause) = self.disabled_by() {
            return Err(PathRefusal::Disabled(cause));
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

/// The configured `transport-quic` mode. An unknown value is off, and
/// `quic_settings` has already warned about it.
fn configured_mode(config: &NetworkApiConfig) -> QuicMode {
    QuicMode::from_config_value(config.transport_quic.as_deref())
}

/// Report the QUIC upgrade configuration once, at node startup: that
/// `prefer` runs as `shadow` (INFO), and, with QUIC on, a `quic-max-paths`
/// that cannot be parsed (WARN) or is lowered to `max-number-of-connections`
/// (INFO). In a build that aborts on panic QUIC stays off whatever the
/// configuration (`QuicEndpoint::bind` refuses), and only that is reported
/// (WARN).
pub(crate) fn log_startup(config: &NetworkApiConfig) {
    if configured_mode(config) != QuicMode::Off && !crate::transport::quic::UNWINDS {
        tracing::warn!(
            transport_quic = ?config.transport_quic,
            "This build aborts on panic (panic = \"abort\"), so QUIC stays off: the QUIC \
             transport needs panics to unwind to contain them"
        );
        return;
    }
    if configured_mode(config) == QuicMode::Prefer {
        tracing::info!(
            "transport-quic = \"prefer\" runs as \"shadow\" in this release: QUIC paths \
             carry only control frames and all application traffic stays on UDP"
        );
    }
    // `quic-max-paths` matters only with QUIC on.
    if configured_mode(config) == QuicMode::Off {
        return;
    }
    match config.quic_max_paths_setting() {
        QuicMaxPathsSetting::Unset => {}
        QuicMaxPathsSetting::Invalid(raw) => tracing::warn!(
            quic_max_paths = %raw,
            default = config.max_connections,
            "quic-max-paths is not a whole number and is ignored; using \
             max-number-of-connections"
        ),
        QuicMaxPathsSetting::Set(limit) if limit > config.max_connections => tracing::info!(
            quic_max_paths = limit,
            max_connections = config.max_connections,
            "quic-max-paths is above max-number-of-connections and is lowered to it"
        ),
        QuicMaxPathsSetting::Set(_) => {}
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

    const BREAKERS: [BreakerOutcome; 3] = [
        BreakerOutcome::NotConsulted,
        BreakerOutcome::Decided(Verdict::Arm),
        BreakerOutcome::Decided(Verdict::Trip),
    ];

    fn config(transport_quic: &str, max_paths: Option<&str>) -> NetworkApiConfig {
        // Every key has a default, so an empty document is the default config.
        let mut config: NetworkApiConfig = toml::from_str("").expect("default network config");
        config.transport_quic = Some(transport_quic.to_string());
        config.quic_max_paths = max_paths.map(str::to_string);
        config
    }

    fn policy(
        mode: &str,
        breaker: BreakerOutcome,
        availability: QuicAvailability,
        max_paths: usize,
    ) -> QuicPolicy {
        let (_sender, watch) = QuicAvailabilityWatch::for_test(availability);
        QuicPolicy::new(&config(mode, Some(&max_paths.to_string())), breaker, watch)
    }

    /// `disabled_by()` over every combination of its inputs names the first
    /// rule that applies, in the documented order, and `allowed()` and
    /// `admit()` agree with it. Each rule has a case where it alone decides,
    /// so dropping or reordering any one of them fails this test.
    #[test]
    fn disabled_by_names_the_first_rule_that_applies() {
        let mut allowed_cases = 0;
        for mode in ["off", "shadow", "prefer"] {
            for breaker in BREAKERS {
                for max_paths in [0, 4] {
                    for availability in AVAILABILITIES {
                        let expected = if mode == "off" {
                            Some(DisabledBy::Mode)
                        } else if let BreakerOutcome::Decided(Verdict::Trip) = breaker {
                            Some(DisabledBy::Breaker(Verdict::Trip))
                        } else if max_paths == 0 {
                            Some(DisabledBy::NoPaths)
                        } else if availability != QuicAvailability::Up {
                            Some(DisabledBy::Endpoint(availability))
                        } else {
                            None
                        };
                        let case = format!(
                            "mode {mode}, breaker {breaker:?}, max_paths {max_paths}, \
                             availability {availability:?}"
                        );
                        let policy = policy(mode, breaker, availability, max_paths);
                        assert_eq!(policy.disabled_by(), expected, "{case}");
                        assert_eq!(policy.allowed(), expected.is_none(), "{case}");
                        match (policy.admit(), expected) {
                            (Ok(_), None) => allowed_cases += 1,
                            (Err(refusal), Some(cause)) => {
                                assert_eq!(refusal, PathRefusal::Disabled(cause), "{case}");
                                assert_eq!(policy.permits().in_use(), 0, "{case}");
                            }
                            (outcome, _) => panic!("{case}: admit gave {outcome:?}"),
                        }
                    }
                }
            }
        }
        // shadow and prefer, with a breaker that did not run or armed.
        assert_eq!(allowed_cases, 4);
    }

    /// The policy reads the availability at each call: an endpoint that goes
    /// down after the policy was built turns it off, and paths admitted
    /// before keep their permits until dropped.
    #[test]
    fn endpoint_going_down_turns_the_policy_off() {
        let (sender, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let policy = QuicPolicy::new(
            &config("shadow", Some("2")),
            BreakerOutcome::NotConsulted,
            watch,
        );
        let held = policy.admit().expect("allowed while up");
        sender.send_replace(QuicAvailability::Down);
        assert_eq!(
            policy.admit().unwrap_err(),
            PathRefusal::Disabled(DisabledBy::Endpoint(QuicAvailability::Down))
        );
        assert_eq!(policy.permits().in_use(), 1);
        drop(held);
        assert_eq!(policy.permits().in_use(), 0);
    }

    /// The policy takes its mode and limit from the config: `prefer` runs as
    /// shadow, the limit is `quic-max-paths` lowered to
    /// `max-number-of-connections`, and an unparsable value means the
    /// default.
    #[test]
    fn the_policy_reads_mode_and_limit_from_the_config() {
        let up = || QuicAvailabilityWatch::for_test(QuicAvailability::Up).1;
        let build = |mode, max_paths| {
            QuicPolicy::new(&config(mode, max_paths), BreakerOutcome::NotConsulted, up())
        };
        let max_connections = config("off", None).max_connections;
        assert!(max_connections > 3, "premise");
        for (mode, effective) in [
            ("off", QuicMode::Off),
            ("shadow", QuicMode::Shadow),
            ("prefer", QuicMode::Shadow),
            ("a-later-mode", QuicMode::Off),
        ] {
            assert_eq!(build(mode, None).mode(), effective, "{mode}");
        }
        for (max_paths, limit) in [
            (None, max_connections),
            (Some("3"), 3),
            (Some(" 3 "), 3),
            (Some("0"), 0),
            (Some("lots"), max_connections),
            (Some("-1"), max_connections),
        ] {
            assert_eq!(
                build("shadow", max_paths).permits().limit(),
                limit,
                "{max_paths:?}"
            );
        }
        let above = (max_connections + 1).to_string();
        assert_eq!(
            build("shadow", Some(&above)).permits().limit(),
            max_connections,
            "lowered to max-number-of-connections"
        );
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
        assert!(QuicPathPermits::new(0).try_acquire().is_none());
    }

    /// An allowed policy at its limit refuses with `Busy`, and a limit of 0
    /// is reported as disabled, not busy.
    #[test]
    fn busy_at_the_limit_and_disabled_at_zero() {
        let up = QuicAvailability::Up;
        let policy = policy("shadow", BreakerOutcome::NotConsulted, up, 1);
        let held = policy.admit().expect("one slot");
        assert_eq!(policy.admit().unwrap_err(), PathRefusal::Busy);
        drop(held);
        assert!(policy.admit().is_ok());
        let none = self::policy("shadow", BreakerOutcome::NotConsulted, up, 0);
        assert_eq!(
            none.admit().unwrap_err(),
            PathRefusal::Disabled(DisabledBy::NoPaths)
        );
    }

    /// Threads released together race for permits while each holds its
    /// permit across a yield. The count never passes the limit, reaches it
    /// (so the race really happened), and returns to zero.
    #[test]
    fn concurrent_acquires_never_exceed_the_limit() {
        const LIMIT: usize = 3;
        const THREADS: usize = 8;
        let permits = QuicPathPermits::new(LIMIT);
        let peak = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(std::sync::Barrier::new(THREADS));
        let threads: Vec<_> = (0..THREADS)
            .map(|_| {
                let permits = permits.clone();
                let peak = Arc::clone(&peak);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..5_000 {
                        if let Some(permit) = permits.try_acquire() {
                            peak.fetch_max(permits.in_use(), Ordering::AcqRel);
                            std::thread::yield_now();
                            drop(permit);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(peak.load(Ordering::Acquire), LIMIT);
        assert_eq!(permits.in_use(), 0);
    }

    /// Startup says that `prefer` runs as `shadow` (INFO), warns about a
    /// `quic-max-paths` it cannot parse, and says when it lowers one, the
    /// last two only with QUIC on; a plain configuration logs none of it.
    #[test]
    fn startup_reports_prefer_and_max_paths() {
        const PREFER: &str = "runs as \"shadow\" in this release";
        const INVALID: &str = "quic-max-paths is not a whole number";
        const LOWERED: &str = "quic-max-paths is above max-number-of-connections";
        let above = (config("off", None).max_connections + 1).to_string();
        for (mode, max_paths, expected) in [
            ("off", None, None),
            ("off", Some("lots"), None),
            ("off", Some(above.as_str()), None),
            ("shadow", Some("3"), None),
            ("prefer", None, Some(("INFO", PREFER))),
            ("shadow", Some("lots"), Some(("WARN", INVALID))),
            ("shadow", Some(above.as_str()), Some(("INFO", LOWERED))),
        ] {
            let config = config(mode, max_paths);
            let (logs, guard) = crate::util::test_log_capture::install();
            log_startup(&config);
            drop(guard);
            let logs = logs.lock().unwrap().clone();
            let ours: Vec<_> = logs
                .iter()
                .filter(|line| [PREFER, INVALID, LOWERED].iter().any(|n| line.contains(n)))
                .collect();
            match expected {
                None => assert!(ours.is_empty(), "{mode} {max_paths:?}: {logs:?}"),
                Some((level, needle)) => {
                    assert_eq!(ours.len(), 1, "{mode} {max_paths:?}: {logs:?}");
                    assert!(ours[0].starts_with(level), "{}", ours[0]);
                    assert!(ours[0].contains(needle), "{}", ours[0]);
                }
            }
        }
    }
}
