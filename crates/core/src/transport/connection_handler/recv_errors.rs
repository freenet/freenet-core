//! How the UDP listener reacts to a failed `recv_from`.
//!
//! The listener is the node's only reader of its UDP socket, and when its task
//! ends the node force-exits (`node/p2p_impl.rs`). So a receive error ends it
//! only when the error says the socket itself is unusable. Everything else
//! (one bad datagram, an ICMP report about some remote host, a network blip)
//! drops that read and carries on.
//!
//! The classification is an allow-list of FATAL conditions, not of transient
//! ones. The old shape listed the transient kinds and treated every other kind
//! as fatal, and Rust std reports several routine Windows socket errors as
//! `Uncategorized` (e.g. `WSAEMSGSIZE`, `WSAENETRESET`), so routine traffic
//! restarted Windows nodes. Unknown errors therefore default to non-fatal, and
//! a socket that keeps failing with them is caught by [`RecvErrorTracker`]
//! instead, which ends the listener once the failure has lasted long enough
//! that it cannot be a burst. Leaving a dead socket running is its own failure:
//! the node stays up but deaf and is never restarted (#4549). Transient errors
//! never count toward that exit (see [`RecvErrorClass::Transient`]).

use std::io;
use std::time::Duration;

/// Windows error codes (`winerror.h`). Matched on every platform: Unix errno
/// values are all far below 1000, so they cannot collide, and matching them
/// everywhere lets the classifier be tested off Windows.
mod win {
    pub const WSAEINTR: i32 = 10004;
    pub const WSAEBADF: i32 = 10009;
    pub const WSAEWOULDBLOCK: i32 = 10035;
    pub const WSAENOTSOCK: i32 = 10038;
    pub const WSAEMSGSIZE: i32 = 10040;
    pub const WSAENETDOWN: i32 = 10050;
    pub const WSAENETUNREACH: i32 = 10051;
    pub const WSAENETRESET: i32 = 10052;
    pub const WSAECONNABORTED: i32 = 10053;
    pub const WSAECONNRESET: i32 = 10054;
    pub const WSAENOBUFS: i32 = 10055;
    pub const WSAESHUTDOWN: i32 = 10058;
    pub const WSAETIMEDOUT: i32 = 10060;
    pub const WSAECONNREFUSED: i32 = 10061;
    pub const WSAEHOSTDOWN: i32 = 10064;
    pub const WSAEHOSTUNREACH: i32 = 10065;
    pub const WSANOTINITIALISED: i32 = 10093;
    // Win32 (not WSA) codes that some Windows socket paths report for an
    // ICMP-driven failure. Listed so they cannot fall through to a kind.
    pub const ERROR_CONNECTION_REFUSED: i32 = 1225;
    pub const ERROR_PORT_UNREACHABLE: i32 = 1234;
    pub const ERROR_CONNECTION_ABORTED: i32 = 1236;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecvErrorClass {
    /// The socket is closed or invalid; no later read can succeed.
    Fatal,
    /// A per-datagram condition caused by the remote end or the path to it.
    /// A remote host can cause these at any rate, so they never count toward
    /// an exit, however many there are or however long they go on.
    Transient,
    /// Anything else, including conditions that describe the local socket or
    /// network (buffers, interface down). Non-fatal, but a long unbroken run
    /// of these ends the listener (see [`RecvErrorTracker`]).
    Unclassified,
}

// The trailing wildcard is the point: every kind not named here, including
// ones std adds later, must default to Unclassified rather than Fatal.
#[allow(clippy::wildcard_enum_match_arm)]
pub(super) fn classify_recv_error(err: &io::Error) -> RecvErrorClass {
    if let Some(code) = err.raw_os_error() {
        match code {
            // Socket handle closed, not a socket, shut down, or Winsock gone.
            win::WSAEBADF | win::WSAENOTSOCK | win::WSAESHUTDOWN | win::WSANOTINITIALISED => {
                return RecvErrorClass::Fatal;
            }
            // Message size, ICMP-driven resets and unreachable reports, and
            // interrupted or would-block reads.
            win::WSAEINTR
            | win::WSAEWOULDBLOCK
            | win::WSAEMSGSIZE
            | win::WSAENETUNREACH
            | win::WSAENETRESET
            | win::WSAECONNRESET
            | win::WSAECONNREFUSED
            | win::WSAEHOSTDOWN
            | win::WSAEHOSTUNREACH
            | win::ERROR_CONNECTION_REFUSED
            | win::ERROR_PORT_UNREACHABLE
            | win::ERROR_CONNECTION_ABORTED => return RecvErrorClass::Transient,
            // Local socket or network state (MSDN describes these as
            // conditions of the socket, not of one datagram), so they must
            // stay able to end a listener whose socket never recovers.
            win::WSAENETDOWN | win::WSAECONNABORTED | win::WSAENOBUFS | win::WSAETIMEDOUT => {
                return RecvErrorClass::Unclassified;
            }
            _ => {}
        }
        #[cfg(unix)]
        match code {
            libc::EBADF | libc::ENOTSOCK => return RecvErrorClass::Fatal,
            libc::EMSGSIZE | libc::EHOSTDOWN | libc::ENETRESET => {
                return RecvErrorClass::Transient;
            }
            _ => {}
        }
    }
    match err.kind() {
        // An unconnected UDP socket never reports these for a datagram; they
        // mean the socket was shut down or closed under us.
        io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe => RecvErrorClass::Fatal,
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::HostUnreachable
        | io::ErrorKind::NetworkUnreachable
        | io::ErrorKind::Interrupted
        | io::ErrorKind::WouldBlock => RecvErrorClass::Transient,
        _ => RecvErrorClass::Unclassified,
    }
}

/// An unbroken run of unclassified errors (no successful read in between)
/// ends the listener only once it has lasted this long...
pub(super) const UNCLASSIFIED_RUN_FATAL_AFTER: Duration = Duration::from_secs(60);
/// ...and contains at least this many of them, so a handful of stray errors on
/// an otherwise idle socket cannot add up to an exit.
pub(super) const UNCLASSIFIED_RUN_FATAL_MIN_ERRORS: u32 = 1000;
/// After this many consecutive unclassified errors, each further one waits
/// [`UNCLASSIFIED_BACKOFF`] so a socket failing instantly cannot spin a core.
pub(super) const UNCLASSIFIED_BACKOFF_AFTER: u32 = 1024;
pub(super) const UNCLASSIFIED_BACKOFF: Duration = Duration::from_millis(1);
/// After this many consecutive transient errors, each further one yields to
/// the runtime, in case a platform reports one repeatedly without consuming a
/// datagram. Never a sleep: a wait in this loop would delay every read behind
/// it, while a yield costs nothing when there is a datagram to read.
pub(super) const TRANSIENT_YIELD_AFTER: u32 = 1024;
/// At most [`LOG_BURST`] errors per class are logged per [`LOG_WINDOW`]; the
/// rest are counted and reported with that class's next logged error. Classes
/// have separate budgets so a burst of transient errors cannot hide a local
/// one.
pub(super) const LOG_WINDOW: Duration = Duration::from_secs(60);
pub(super) const LOG_BURST: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecvErrorAction {
    /// End the listen task with this error.
    Exit,
    /// Drop this read, pause as given, and receive again.
    Continue { pause: RecvErrorPause },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecvErrorPause {
    None,
    /// Yield to the runtime once (`tokio::task::yield_now`). Touches no clock.
    Yield,
    /// Sleep on the listener's `TimeSource`.
    Sleep(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecvErrorLog {
    /// Log this error; `suppressed` errors of the same class went unlogged
    /// since the last one.
    Emit {
        suppressed: u64,
    },
    Suppress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RecvErrorVerdict {
    pub class: RecvErrorClass,
    pub action: RecvErrorAction,
    pub log: RecvErrorLog,
}

#[derive(Debug, Default)]
struct LogBudget {
    window_start_nanos: Option<u64>,
    logged_in_window: u32,
    suppressed: u64,
}

impl LogBudget {
    fn decide(&mut self, now_nanos: u64) -> RecvErrorLog {
        let window_start = *self.window_start_nanos.get_or_insert(now_nanos);
        if now_nanos.saturating_sub(window_start) >= LOG_WINDOW.as_nanos() as u64 {
            self.window_start_nanos = Some(now_nanos);
            self.logged_in_window = 0;
        }
        if self.logged_in_window < LOG_BURST {
            self.logged_in_window += 1;
            RecvErrorLog::Emit {
                suppressed: std::mem::take(&mut self.suppressed),
            }
        } else {
            self.suppressed += 1;
            RecvErrorLog::Suppress
        }
    }
}

/// Per-listener state behind the receive-error decision. Time is passed in
/// (from the listener's `TimeSource`) so the decision is deterministic.
#[derive(Debug, Default)]
pub(super) struct RecvErrorTracker {
    /// Transient errors since the last successful read. Drives only the
    /// yield; transient errors never count toward an exit.
    transient_in_run: u32,
    /// Unclassified errors since the last successful read, and when the first
    /// of them happened. Transient errors neither extend nor reset this run.
    unclassified_in_run: u32,
    unclassified_run_start_nanos: Option<u64>,
    transient_log: LogBudget,
    unclassified_log: LogBudget,
}

impl RecvErrorTracker {
    pub(super) fn on_success(&mut self) {
        self.transient_in_run = 0;
        self.unclassified_in_run = 0;
        self.unclassified_run_start_nanos = None;
    }

    pub(super) fn on_error(&mut self, err: &io::Error, now_nanos: u64) -> RecvErrorVerdict {
        let class = classify_recv_error(err);
        let exit = RecvErrorVerdict {
            class,
            action: RecvErrorAction::Exit,
            log: RecvErrorLog::Emit {
                suppressed: self.transient_log.suppressed + self.unclassified_log.suppressed,
            },
        };
        if class == RecvErrorClass::Fatal {
            return exit;
        }

        let (pause, log) = match class {
            RecvErrorClass::Unclassified => {
                self.unclassified_in_run = self.unclassified_in_run.saturating_add(1);
                let start = *self.unclassified_run_start_nanos.get_or_insert(now_nanos);
                if self.unclassified_in_run >= UNCLASSIFIED_RUN_FATAL_MIN_ERRORS
                    && now_nanos.saturating_sub(start)
                        >= UNCLASSIFIED_RUN_FATAL_AFTER.as_nanos() as u64
                {
                    return exit;
                }
                let pause = if self.unclassified_in_run > UNCLASSIFIED_BACKOFF_AFTER {
                    RecvErrorPause::Sleep(UNCLASSIFIED_BACKOFF)
                } else {
                    RecvErrorPause::None
                };
                (pause, self.unclassified_log.decide(now_nanos))
            }
            RecvErrorClass::Transient | RecvErrorClass::Fatal => {
                self.transient_in_run = self.transient_in_run.saturating_add(1);
                let pause = if self.transient_in_run > TRANSIENT_YIELD_AFTER {
                    RecvErrorPause::Yield
                } else {
                    RecvErrorPause::None
                };
                (pause, self.transient_log.decide(now_nanos))
            }
        };
        RecvErrorVerdict {
            class,
            action: RecvErrorAction::Continue { pause },
            log,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(code: i32) -> io::Error {
        io::Error::from_raw_os_error(code)
    }

    fn unknown() -> io::Error {
        io::Error::other("unknown")
    }

    fn is_continue(v: RecvErrorVerdict) -> bool {
        matches!(v.action, RecvErrorAction::Continue { .. })
    }

    const SEC: u64 = 1_000_000_000;
    const UNCLASSIFIED_LIMIT: u64 = UNCLASSIFIED_RUN_FATAL_AFTER.as_nanos() as u64;

    /// The Windows errors behind the restart loops, as raw codes. On Windows
    /// std decodes 10040 and 10052 to `Uncategorized` and 10051/10065 to the
    /// unreachable kinds; off Windows the kinds differ, so the raw-code match
    /// is what is under test and must hold on every platform.
    #[test]
    fn windows_per_datagram_recv_errors_are_transient() {
        for code in [
            win::WSAEMSGSIZE,
            win::WSAENETRESET,
            win::WSAEHOSTUNREACH,
            win::WSAENETUNREACH,
            win::WSAECONNRESET,
            win::WSAECONNREFUSED,
            win::WSAEHOSTDOWN,
            win::WSAEINTR,
            win::WSAEWOULDBLOCK,
            win::ERROR_CONNECTION_REFUSED,
            win::ERROR_PORT_UNREACHABLE,
            win::ERROR_CONNECTION_ABORTED,
        ] {
            assert_eq!(
                classify_recv_error(&raw(code)),
                RecvErrorClass::Transient,
                "raw {code}"
            );
        }
    }

    /// Conditions of the local socket or network: not fatal on their own, but
    /// not exempt from the run-based exit either.
    #[test]
    fn windows_socket_level_recv_errors_are_unclassified() {
        for code in [
            win::WSAENETDOWN,
            win::WSAECONNABORTED,
            win::WSAENOBUFS,
            win::WSAETIMEDOUT,
        ] {
            assert_eq!(
                classify_recv_error(&raw(code)),
                RecvErrorClass::Unclassified,
                "raw {code}"
            );
        }
    }

    #[test]
    fn windows_closed_socket_errors_are_fatal() {
        for code in [
            win::WSAENOTSOCK,
            win::WSAESHUTDOWN,
            win::WSAEBADF,
            win::WSANOTINITIALISED,
        ] {
            assert_eq!(
                classify_recv_error(&raw(code)),
                RecvErrorClass::Fatal,
                "raw {code}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_recv_errors_classify() {
        for code in [
            libc::ECONNREFUSED,
            libc::ECONNRESET,
            libc::EHOSTUNREACH,
            libc::ENETUNREACH,
            libc::EMSGSIZE,
            libc::EINTR,
            libc::EAGAIN,
            libc::EHOSTDOWN,
            libc::ENETRESET,
        ] {
            assert_eq!(
                classify_recv_error(&raw(code)),
                RecvErrorClass::Transient,
                "errno {code}"
            );
        }
        for code in [
            libc::ENOBUFS,
            libc::ENOMEM,
            libc::ENETDOWN,
            libc::ECONNABORTED,
            libc::ETIMEDOUT,
            libc::EINVAL,
        ] {
            assert_eq!(
                classify_recv_error(&raw(code)),
                RecvErrorClass::Unclassified,
                "errno {code}"
            );
        }
        for code in [libc::EBADF, libc::ENOTSOCK, libc::ENOTCONN, libc::EPIPE] {
            assert_eq!(
                classify_recv_error(&raw(code)),
                RecvErrorClass::Fatal,
                "errno {code}"
            );
        }
    }

    /// Kind-only errors (no OS code), as a wrapper socket or the simulation
    /// may produce them.
    #[test]
    fn error_kinds_classify() {
        use io::ErrorKind::*;
        for kind in [
            ConnectionReset,
            ConnectionRefused,
            HostUnreachable,
            NetworkUnreachable,
            Interrupted,
            WouldBlock,
        ] {
            assert_eq!(
                classify_recv_error(&kind.into()),
                RecvErrorClass::Transient,
                "{kind:?}"
            );
        }
        for kind in [NotConnected, BrokenPipe] {
            assert_eq!(
                classify_recv_error(&kind.into()),
                RecvErrorClass::Fatal,
                "{kind:?}"
            );
        }
        for kind in [
            ConnectionAborted,
            TimedOut,
            NetworkDown,
            OutOfMemory,
            Other,
            InvalidInput,
            InvalidData,
            UnexpectedEof,
            PermissionDenied,
        ] {
            assert_eq!(
                classify_recv_error(&kind.into()),
                RecvErrorClass::Unclassified,
                "{kind:?}"
            );
        }
        // An OS code nobody listed falls through to its kind, then to
        // Unclassified: never fatal by default.
        assert_eq!(
            classify_recv_error(&raw(10_999)),
            RecvErrorClass::Unclassified
        );
    }

    #[test]
    fn fatal_error_exits_immediately() {
        let mut t = RecvErrorTracker::default();
        let v = t.on_error(&raw(win::WSAENOTSOCK), 0);
        assert_eq!(v.action, RecvErrorAction::Exit);
        assert!(matches!(v.log, RecvErrorLog::Emit { .. }));
    }

    /// Transient errors never end the listener, even on a node that reads
    /// nothing else: 20,000 of them over more than ten minutes, with no
    /// successful read at all.
    #[test]
    fn transient_errors_never_exit_without_any_successful_read() {
        for code in [win::WSAEMSGSIZE, win::WSAECONNRESET, win::WSAENETUNREACH] {
            let mut t = RecvErrorTracker::default();
            for i in 0..20_000u64 {
                assert!(
                    is_continue(t.on_error(&raw(code), i * SEC / 30)),
                    "raw {code}, error {i}"
                );
            }
        }
    }

    /// Both bounds of the unclassified exit are inclusive: the run must reach
    /// exactly `UNCLASSIFIED_RUN_FATAL_MIN_ERRORS` errors and span exactly
    /// `UNCLASSIFIED_RUN_FATAL_AFTER`, and it exits on the error that reaches
    /// the second of the two.
    #[test]
    fn unclassified_exit_count_boundary() {
        let n = UNCLASSIFIED_RUN_FATAL_MIN_ERRORS;
        let mut t = RecvErrorTracker::default();
        assert!(is_continue(t.on_error(&unknown(), 0)));
        for i in 2..n {
            assert!(
                is_continue(t.on_error(&unknown(), UNCLASSIFIED_LIMIT)),
                "error {i}"
            );
        }
        assert_eq!(
            t.on_error(&unknown(), UNCLASSIFIED_LIMIT).action,
            RecvErrorAction::Exit,
            "error {n} at the time limit must exit"
        );
    }

    #[test]
    fn unclassified_exit_time_boundary() {
        let n = u64::from(UNCLASSIFIED_RUN_FATAL_MIN_ERRORS);
        let mut t = RecvErrorTracker::default();
        for i in 0..2 * n {
            assert!(is_continue(t.on_error(&unknown(), i % 2)), "error {i}");
        }
        assert!(is_continue(t.on_error(&unknown(), UNCLASSIFIED_LIMIT - 1)));
        assert_eq!(
            t.on_error(&unknown(), UNCLASSIFIED_LIMIT).action,
            RecvErrorAction::Exit
        );
    }

    /// Few errors over a long time (an idle socket's odd failure) never add
    /// up to an exit.
    #[test]
    fn sparse_unclassified_errors_do_not_exit() {
        let mut t = RecvErrorTracker::default();
        for i in 0..u64::from(UNCLASSIFIED_RUN_FATAL_MIN_ERRORS - 1) {
            assert!(is_continue(t.on_error(&unknown(), i * 3600 * SEC)));
        }
    }

    #[test]
    fn successful_read_resets_the_unclassified_run() {
        let mut t = RecvErrorTracker::default();
        for i in 0..u64::from(UNCLASSIFIED_RUN_FATAL_MIN_ERRORS) * 2 {
            t.on_error(&unknown(), i);
        }
        t.on_success();
        // Past the limit measured from the first error, but the run restarted.
        assert!(is_continue(t.on_error(&unknown(), 2 * UNCLASSIFIED_LIMIT)));
    }

    /// A transient error in the middle of an unclassified run neither counts
    /// toward the exit nor resets it.
    #[test]
    fn transient_errors_do_not_count_toward_or_reset_unclassified_exit() {
        let n = UNCLASSIFIED_RUN_FATAL_MIN_ERRORS;
        let mut t = RecvErrorTracker::default();
        assert!(is_continue(t.on_error(&unknown(), 0)));
        for _ in 2..n - 1 {
            assert!(is_continue(t.on_error(&unknown(), UNCLASSIFIED_LIMIT)));
        }
        assert!(is_continue(
            t.on_error(&raw(win::WSAEMSGSIZE), UNCLASSIFIED_LIMIT)
        ));
        // Unclassified error n-1: would be error n, and exit, if the transient
        // error had counted.
        assert!(
            is_continue(t.on_error(&unknown(), UNCLASSIFIED_LIMIT)),
            "the transient error must not count toward the run"
        );
        assert_eq!(
            t.on_error(&unknown(), UNCLASSIFIED_LIMIT).action,
            RecvErrorAction::Exit,
            "the transient error must not have reset the run"
        );
    }

    /// A long transient run yields, never sleeps, and a read clears it.
    #[test]
    fn long_transient_run_yields_and_never_sleeps() {
        let mut t = RecvErrorTracker::default();
        for i in 0..TRANSIENT_YIELD_AFTER {
            assert_eq!(
                t.on_error(&raw(win::WSAEMSGSIZE), 0).action,
                RecvErrorAction::Continue {
                    pause: RecvErrorPause::None
                },
                "error {i}"
            );
        }
        for i in 0..5 * TRANSIENT_YIELD_AFTER {
            assert_eq!(
                t.on_error(&raw(win::WSAEMSGSIZE), 0).action,
                RecvErrorAction::Continue {
                    pause: RecvErrorPause::Yield
                },
                "error {i} past the yield threshold"
            );
        }
        t.on_success();
        assert_eq!(
            t.on_error(&raw(win::WSAEMSGSIZE), 0).action,
            RecvErrorAction::Continue {
                pause: RecvErrorPause::None
            }
        );
    }

    #[test]
    fn long_unclassified_run_backs_off_and_success_clears_it() {
        let mut t = RecvErrorTracker::default();
        for i in 0..UNCLASSIFIED_BACKOFF_AFTER {
            assert_eq!(
                t.on_error(&unknown(), 0).action,
                RecvErrorAction::Continue {
                    pause: RecvErrorPause::None
                },
                "error {i}"
            );
        }
        assert_eq!(
            t.on_error(&unknown(), 0).action,
            RecvErrorAction::Continue {
                pause: RecvErrorPause::Sleep(UNCLASSIFIED_BACKOFF)
            }
        );
        t.on_success();
        assert_eq!(
            t.on_error(&unknown(), 0).action,
            RecvErrorAction::Continue {
                pause: RecvErrorPause::None
            }
        );
    }

    /// Transient errors do not advance the unclassified backoff, and
    /// unclassified errors do not advance the transient yield.
    #[test]
    fn transient_and_unclassified_pauses_are_counted_separately() {
        let mut t = RecvErrorTracker::default();
        for _ in 0..5 * TRANSIENT_YIELD_AFTER {
            t.on_error(&raw(win::WSAEMSGSIZE), 0);
        }
        assert_eq!(
            t.on_error(&unknown(), 0).action,
            RecvErrorAction::Continue {
                pause: RecvErrorPause::None
            }
        );
        let mut t = RecvErrorTracker::default();
        for _ in 0..UNCLASSIFIED_BACKOFF_AFTER {
            t.on_error(&unknown(), 0);
        }
        assert_eq!(
            t.on_error(&raw(win::WSAEMSGSIZE), 0).action,
            RecvErrorAction::Continue {
                pause: RecvErrorPause::None
            }
        );
    }

    #[test]
    fn logging_is_rate_limited_and_reports_suppressed_count() {
        let mut t = RecvErrorTracker::default();
        let err = raw(win::WSAEMSGSIZE);
        for _ in 0..LOG_BURST {
            assert_eq!(
                t.on_error(&err, 0).log,
                RecvErrorLog::Emit { suppressed: 0 }
            );
        }
        for _ in 0..1000 {
            assert_eq!(t.on_error(&err, SEC).log, RecvErrorLog::Suppress);
        }
        let next_window = LOG_WINDOW.as_nanos() as u64;
        assert_eq!(
            t.on_error(&err, next_window).log,
            RecvErrorLog::Emit { suppressed: 1000 }
        );
        assert_eq!(
            t.on_error(&err, next_window).log,
            RecvErrorLog::Emit { suppressed: 0 }
        );
    }

    /// A flood of transient errors that exhausts their log budget must not
    /// silence an unclassified one.
    #[test]
    fn unclassified_errors_have_their_own_log_budget() {
        let mut t = RecvErrorTracker::default();
        for _ in 0..1000 {
            t.on_error(&raw(win::WSAEMSGSIZE), 0);
        }
        assert_eq!(
            t.on_error(&raw(win::WSAEMSGSIZE), 0).log,
            RecvErrorLog::Suppress
        );
        assert_eq!(
            t.on_error(&unknown(), 0).log,
            RecvErrorLog::Emit { suppressed: 0 }
        );
    }
}
