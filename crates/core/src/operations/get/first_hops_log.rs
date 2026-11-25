//! One rate-limited INFO line naming the first hops a client GET tried before
//! it ended `NotFound` (retry loop exhausted).
//!
//! Why: the per-attempt peer of a GET is only logged at DEBUG, which a release
//! build compiles out, so a production node reporting repeated fast
//! `NotFound`s for a contract that other peers hold gives no way to see which
//! peers it asked or how far they were from the contract. This records, per
//! attempt, the hop's address, its ring location, its distance to the key and
//! whether it answered `NotFound` or gave no usable reply (timeout, dropped
//! connection), and emits them once when the search ends.
//!
//! Log volume: lines are admitted by a process-global token bucket
//! ([`FIRST_HOPS_LIMITER`]) so a node with many failing GETs cannot flood its
//! log. Lines the bucket refused are counted and the next admitted line
//! reports that count as `suppressed_since_last`.

use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::message::Transaction;
use crate::ring::{Location, PeerKeyLocation};

/// Most hops remembered per GET. The retry budget allows 4 first hops
/// (`MAX_RETRIES` + 1) and the fallback re-asks add a few more; the cap only
/// bounds a pathological loop.
const MAX_HOPS_RECORDED: usize = 8;

/// Lines admitted back to back after a quiet period.
const BURST: u32 = 5;
/// One more line is admitted per this interval once the burst is spent, so the
/// sustained rate is at most 3 lines a minute (about 4300 a day, roughly
/// 1.5 MB).
const REFILL_INTERVAL: Duration = Duration::from_secs(20);

/// Process-global limiter shared by every client GET on the node.
static FIRST_HOPS_LIMITER: LogLimiter = LogLimiter::new(BURST, REFILL_INTERVAL);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HopOutcome {
    /// The hop replied `NotFound`.
    NotFound,
    /// The attempt ended without a usable reply from the hop: timeout,
    /// dropped connection or a send failure.
    NoReply,
}

impl HopOutcome {
    fn label(self) -> &'static str {
        match self {
            HopOutcome::NotFound => "not_found",
            HopOutcome::NoReply => "no_reply",
        }
    }
}

#[derive(Debug, Clone)]
struct HopAttempt {
    /// `None` when the loopback relay recorded no forward for the attempt.
    addr: Option<SocketAddr>,
    loc: Option<Location>,
    outcome: HopOutcome,
}

/// Hops a client GET was forwarded to, in attempt order.
#[derive(Debug, Default)]
pub(super) struct FirstHopsLog {
    hops: Vec<HopAttempt>,
    /// Attempts past [`MAX_HOPS_RECORDED`].
    omitted: usize,
}

impl FirstHopsLog {
    /// An attempt got a verdict or ended; `hop` is where it was forwarded.
    /// Starts as `NoReply`; [`Self::mark_not_found`] upgrades it.
    pub(super) fn record_attempt(&mut self, hop: Option<&PeerKeyLocation>) {
        if self.hops.len() >= MAX_HOPS_RECORDED {
            self.omitted += 1;
            return;
        }
        self.hops.push(HopAttempt {
            addr: hop.and_then(|h| h.socket_addr()),
            loc: hop.and_then(|h| h.location()),
            outcome: HopOutcome::NoReply,
        });
    }

    /// The most recently recorded attempt replied `NotFound`.
    pub(super) fn mark_not_found(&mut self) {
        // An omitted attempt has no entry to upgrade.
        if self.omitted == 0 {
            if let Some(last) = self.hops.last_mut() {
                last.outcome = HopOutcome::NotFound;
            }
        }
    }

    /// `[addr@loc(d=dist):outcome, ...]`, with `?` where a value is unknown.
    fn render(&self, key_loc: Location) -> String {
        let mut out = String::from("[");
        for (i, hop) in self.hops.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            match hop.addr {
                Some(addr) => out.push_str(&addr.to_string()),
                None => out.push('?'),
            }
            match hop.loc {
                Some(loc) => out.push_str(&format!(
                    "@{:.4}(d={:.4})",
                    loc.as_f64(),
                    loc.distance(key_loc).as_f64()
                )),
                None => out.push_str("@?"),
            }
            out.push(':');
            out.push_str(hop.outcome.label());
        }
        if self.omitted > 0 {
            out.push_str(&format!(", +{} more", self.omitted));
        }
        out.push(']');
        out
    }
}

struct LimiterState {
    tokens: u32,
    last_refill: Option<Instant>,
    suppressed: u64,
}

/// Token bucket with a suppressed-line counter. Time is passed in so tests can
/// drive it without sleeping.
pub(super) struct LogLimiter {
    burst: u32,
    refill: Duration,
    state: Mutex<LimiterState>,
}

impl LogLimiter {
    pub(super) const fn new(burst: u32, refill: Duration) -> Self {
        Self {
            burst,
            refill,
            state: Mutex::new(LimiterState {
                tokens: burst,
                last_refill: None,
                suppressed: 0,
            }),
        }
    }

    /// `Some(n)` admits one line, `n` being the lines refused since the last
    /// admitted one; `None` refuses it and counts it.
    pub(super) fn admit(&self, now: Instant) -> Option<u64> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match st.last_refill {
            // A full bucket has no refill in progress: the timer starts when
            // the first token is taken, not at some earlier quiet moment.
            Some(last) if st.tokens < self.burst => {
                let elapsed = now.saturating_duration_since(last);
                let intervals = elapsed.as_nanos() / self.refill.as_nanos().max(1);
                if intervals > 0 {
                    let add = u32::try_from(intervals).unwrap_or(u32::MAX);
                    st.tokens = st.tokens.saturating_add(add).min(self.burst);
                    st.last_refill = Some(if st.tokens == self.burst {
                        now
                    } else {
                        // Keep the remainder so slow refill is not lost to
                        // rounding. `add` < burst here, so no overflow.
                        last + self.refill * add
                    });
                }
            }
            _ => st.last_refill = Some(now),
        }
        if st.tokens == 0 {
            st.suppressed += 1;
            return None;
        }
        st.tokens -= 1;
        Some(std::mem::take(&mut st.suppressed))
    }
}

/// Everything the line reports besides the hops.
pub(super) struct FirstHopsContext<'a> {
    pub tx: Transaction,
    pub instance_id: ContractInstanceId,
    pub own_loc: Option<Location>,
    pub cause: &'a str,
}

/// Emit the line for the node's process-global limiter.
pub(super) fn report_notfound_first_hops(ctx: &FirstHopsContext<'_>, log: &FirstHopsLog) {
    report_with(&FIRST_HOPS_LIMITER, Instant::now(), ctx, log);
}

fn report_with(limiter: &LogLimiter, now: Instant, ctx: &FirstHopsContext<'_>, log: &FirstHopsLog) {
    let Some(suppressed_since_last) = limiter.admit(now) else {
        return;
    };
    let key_loc = Location::from(&ctx.instance_id);
    let own_loc = ctx
        .own_loc
        .map_or_else(|| "?".to_string(), |l| format!("{:.4}", l.as_f64()));
    tracing::info!(
        tx = %ctx.tx,
        instance_id = %ctx.instance_id,
        key_loc = format!("{:.4}", key_loc.as_f64()),
        own_loc,
        hops_tried = log.hops.len() + log.omitted,
        hops = log.render(key_loc),
        cause = ctx.cause,
        suppressed_since_last,
        "GET client: NotFound, first hops tried"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn hop(port: u16) -> PeerKeyLocation {
        PeerKeyLocation::new(
            crate::transport::TransportKeypair::new().public().clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)), port),
        )
    }

    fn ctx(cause: &str) -> FirstHopsContext<'_> {
        FirstHopsContext {
            tx: Transaction::new::<super::super::GetMsg>(),
            instance_id: ContractInstanceId::new([7u8; 32]),
            own_loc: Some(Location::new(0.25)),
            cause,
        }
    }

    fn log_of(hops: &[(PeerKeyLocation, HopOutcome)]) -> FirstHopsLog {
        let mut log = FirstHopsLog::default();
        for (h, outcome) in hops {
            log.record_attempt(Some(h));
            if *outcome == HopOutcome::NotFound {
                log.mark_not_found();
            }
        }
        log
    }

    fn info_lines(logs: &crate::util::test_log_capture::CapturedLogs) -> Vec<String> {
        logs.lock()
            .unwrap()
            .iter()
            .filter(|l| l.starts_with("INFO") && l.contains("first hops tried"))
            .cloned()
            .collect()
    }

    /// The line carries each hop's address, location, distance to the key and
    /// outcome, in attempt order, plus the key and own locations.
    #[test]
    fn line_lists_the_hops_with_location_distance_and_outcome() {
        let (a, b, c) = (hop(4001), hop(4002), hop(4003));
        let log = log_of(&[
            (a.clone(), HopOutcome::NotFound),
            (b.clone(), HopOutcome::NoReply),
            (c.clone(), HopOutcome::NotFound),
        ]);
        let limiter = LogLimiter::new(5, Duration::from_secs(20));
        let c0 = ctx("stopped advancing");
        let key_loc = Location::from(&c0.instance_id);

        let (logs, guard) = crate::util::test_log_capture::install();
        report_with(&limiter, Instant::now(), &c0, &log);
        drop(guard);

        let lines = info_lines(&logs);
        assert_eq!(lines.len(), 1, "exactly one line; got {lines:?}");
        let line = &lines[0];
        for (h, outcome) in [(&a, "not_found"), (&b, "no_reply"), (&c, "not_found")] {
            let want = format!(
                "{}@{:.4}(d={:.4}):{outcome}",
                h.socket_addr().unwrap(),
                h.location().unwrap().as_f64(),
                h.location().unwrap().distance(key_loc).as_f64()
            );
            assert!(line.contains(&want), "missing {want} in {line}");
        }
        // Attempt order is preserved.
        let (ia, ib, ic) = (
            line.find(":4001").unwrap(),
            line.find(":4002").unwrap(),
            line.find(":4003").unwrap(),
        );
        assert!(ia < ib && ib < ic, "order: {line}");
        assert!(
            line.contains(&format!("key_loc=\"{:.4}\"", key_loc.as_f64())),
            "{line}"
        );
        assert!(line.contains("own_loc=\"0.2500\""), "{line}");
        assert!(line.contains("hops_tried=3"), "{line}");
        assert!(line.contains("suppressed_since_last=0"), "{line}");
    }

    /// A burst beyond the bucket is refused and counted; the next admitted
    /// line reports the count; the bucket refills with time.
    #[test]
    fn rate_limit_caps_lines_and_reports_the_suppressed_count() {
        let log = log_of(&[(hop(4001), HopOutcome::NotFound)]);
        let limiter = LogLimiter::new(3, Duration::from_secs(20));
        let c0 = ctx("x");
        let t0 = Instant::now();

        let (logs, guard) = crate::util::test_log_capture::install();
        for _ in 0..10 {
            report_with(&limiter, t0, &c0, &log);
        }
        assert_eq!(info_lines(&logs).len(), 3, "burst of 3, rest refused");
        // 19 s later: still no token.
        report_with(&limiter, t0 + Duration::from_secs(19), &c0, &log);
        assert_eq!(info_lines(&logs).len(), 3);
        // 20 s: one token. 7 refused in the burst + 1 at 19 s = 8.
        report_with(&limiter, t0 + Duration::from_secs(20), &c0, &log);
        let lines = info_lines(&logs);
        assert_eq!(lines.len(), 4, "{lines:?}");
        assert!(lines[3].contains("suppressed_since_last=8"), "{}", lines[3]);
        // And the count resets.
        report_with(&limiter, t0 + Duration::from_secs(40), &c0, &log);
        let lines = info_lines(&logs);
        assert_eq!(lines.len(), 5);
        assert!(lines[4].contains("suppressed_since_last=0"), "{}", lines[4]);
        drop(guard);
    }

    /// A long quiet period refills only up to the burst, never beyond.
    #[test]
    fn refill_is_capped_at_the_burst() {
        let limiter = LogLimiter::new(2, Duration::from_secs(20));
        let t0 = Instant::now();
        assert!(limiter.admit(t0).is_some());
        let later = t0 + Duration::from_secs(3600);
        assert!(limiter.admit(later).is_some());
        assert!(limiter.admit(later).is_some());
        assert!(
            limiter.admit(later).is_none(),
            "cap is the burst, not the elapsed time"
        );
    }

    /// Hops past the cap are counted, not stored.
    #[test]
    fn hops_past_the_cap_are_counted() {
        let mut log = FirstHopsLog::default();
        for p in 0..(MAX_HOPS_RECORDED as u16 + 3) {
            log.record_attempt(Some(&hop(5000 + p)));
            log.mark_not_found();
        }
        assert_eq!(log.hops.len(), MAX_HOPS_RECORDED);
        assert!(log.render(Location::new(0.5)).contains("+3 more"));
    }
    /// A hop the loopback relay never recorded renders as unknowns, and an
    /// unknown own location falls back to `?` on an empty log.
    #[test]
    fn unknown_hop_and_empty_log_render_placeholders() {
        let mut log = FirstHopsLog::default();
        log.record_attempt(None);
        assert_eq!(log.render(Location::new(0.5)), "[?@?:no_reply]");
        log.mark_not_found();
        assert_eq!(log.render(Location::new(0.5)), "[?@?:not_found]");

        let empty = FirstHopsLog::default();
        assert_eq!(empty.render(Location::new(0.5)), "[]");
        let limiter = LogLimiter::new(5, Duration::from_secs(20));
        let mut c0 = ctx("x");
        c0.own_loc = None;
        let (logs, guard) = crate::util::test_log_capture::install();
        report_with(&limiter, Instant::now(), &c0, &empty);
        drop(guard);
        let lines = info_lines(&logs);
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("own_loc=\"?\""), "{}", lines[0]);
        assert!(lines[0].contains("hops_tried=0"), "{}", lines[0]);
        assert!(lines[0].contains("hops=\"[]\""), "{}", lines[0]);
    }

    /// Past the cap, a later NotFound has no entry to upgrade and must not be
    /// credited to the last stored hop.
    #[test]
    fn mark_not_found_after_the_cap_keeps_stored_outcomes() {
        let mut log = FirstHopsLog::default();
        for p in 0..MAX_HOPS_RECORDED as u16 {
            log.record_attempt(Some(&hop(5000 + p)));
        }
        // All stored hops are NoReply; the omitted attempt answers NotFound.
        log.record_attempt(Some(&hop(6000)));
        log.mark_not_found();
        assert_eq!(log.omitted, 1);
        assert!(
            log.hops.iter().all(|h| h.outcome == HopOutcome::NoReply),
            "an omitted attempt's NotFound must not upgrade a stored hop"
        );
    }

    /// Production source of `op_ctx_task.rs`, cut before its test module so a
    /// pin cannot match its own string literals.
    fn production_source() -> &'static str {
        let src = include_str!("op_ctx_task.rs");
        let cut = src
            .find("\n#[cfg(test)]\nmod ")
            .expect("test module not located: this guard cannot verify anything");
        &src[..cut]
    }

    /// Body of the function (free or method) starting at `signature`: from the
    /// signature to the first closing brace at the same indentation. Panics if
    /// the signature is absent or not unique.
    fn fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
        assert_eq!(
            src.matches(signature).count(),
            1,
            "`{signature}` must occur exactly once"
        );
        let at = src.find(signature).unwrap();
        let line_start = src[..at].rfind('\n').map_or(0, |i| i + 1);
        let indent = &src[line_start..at];
        assert!(
            indent.chars().all(|c| c == ' '),
            "`{signature}` must start its line (got prefix {indent:?})"
        );
        let end = format!("\n{indent}}}\n");
        let after = &src[at..];
        let (body, _) = after
            .split_once(end.as_str())
            .unwrap_or_else(|| panic!("end of `{signature}` not found"));
        body
    }

    fn compact(s: &str) -> String {
        s.chars().filter(|c| !c.is_whitespace()).collect()
    }

    /// The driver hooks feed the log from inside the two `RetryDriver` methods.
    #[test]
    fn driver_hooks_feed_the_log() {
        let src = production_source();
        let attempt = compact(fn_body(
            src,
            "fn on_attempt_hop(&mut self, hop: Option<&PeerKeyLocation>) {",
        ));
        assert!(
            attempt.contains("self.first_hops.record_attempt(hop);"),
            "on_attempt_hop must record"
        );
        let not_found = compact(fn_body(
            src,
            "fn on_not_found_hop(&mut self, hop: Option<&PeerKeyLocation>) {",
        ));
        assert!(
            not_found.contains("self.first_hops.mark_not_found();"),
            "on_not_found_hop must mark"
        );
    }

    /// The `Exhausted` arm of `drive_client_get_inner` emits the line, only for
    /// client-visible (non sub-operation) GETs, from that arm and not another.
    #[test]
    fn exhausted_arm_emits_for_client_gets_only() {
        let inner = fn_body(production_source(), "async fn drive_client_get_inner(");
        let start = inner
            .find("RetryLoopOutcome::Exhausted(cause) => {")
            .expect("Exhausted arm not found");
        let rest = &inner[start..];
        // The arm ends at the next sibling arm, or the end of the function.
        let arm = rest
            .find("\n        RetryLoopOutcome::")
            .map_or(rest, |i| &rest[..i]);
        let arm = compact(arm);
        assert!(
            arm.contains(
                "if!client_tx.is_sub_operation(){super::first_hops_log::report_notfound_first_hops(&super::first_hops_log::FirstHopsContext{"
            ),
            "the Exhausted arm must emit the line behind the sub-op guard"
        );
        assert!(
            arm.contains("&driver.first_hops"),
            "the emit must pass the driver's log"
        );
        assert_eq!(
            compact(inner)
                .matches("report_notfound_first_hops(")
                .count(),
            1,
            "emitted from exactly one place"
        );
    }
}
