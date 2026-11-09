//! Test builds only (the `quic-panic-injection` feature, which refuses to
//! build without debug assertions, so a release build cannot carry it):
//! inject a panic in QUIC code from the environment, so the Windows
//! crash-loop breaker (`breaker/short_runs.rs`, #6031) can be exercised
//! through the real binary. Compiled in unit tests too, for its parser.
//!
//! `FREENET_TEST_QUIC_PANIC` is `<mode>:<seconds>`, read once when the
//! breaker's marker is armed (so on Windows the panic hook is already in
//! place); `<seconds>` later a thread in this file panics:
//!
//! - `caught`: a plain panic that ends only its thread, as a panic contained
//!   by the socket adapter would.
//! - `abort`: a panic whose unwinding drops a value that panics again, so the
//!   process aborts with "panic in a destructor during cleanup", the way a
//!   panic inside quinn's endpoint driver does (#6027).

use std::time::Duration;

#[cfg(all(feature = "quic-panic-injection", not(debug_assertions)))]
compile_error!(
    "`quic-panic-injection` is for test builds only and needs debug assertions; \
     a release build must not be able to inject a panic"
);

/// The variable read when the marker is armed.
#[cfg(feature = "quic-panic-injection")]
pub(crate) const ENV: &str = "FREENET_TEST_QUIC_PANIC";

/// What [`ENV`] asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Injection {
    Caught(Duration),
    Abort(Duration),
}

fn parse(value: &str) -> Option<Injection> {
    let (mode, secs) = value.trim().split_once(':')?;
    let after = Duration::from_secs(secs.trim().parse().ok()?);
    match mode.trim() {
        "caught" => Some(Injection::Caught(after)),
        "abort" => Some(Injection::Abort(after)),
        _ => None,
    }
}

/// Start the injection [`ENV`] asks for, if any.
#[cfg(feature = "quic-panic-injection")]
pub(crate) fn inject_from_env() {
    let Some(injection) = std::env::var(ENV).ok().as_deref().and_then(parse) else {
        return;
    };
    tracing::warn!(?injection, "Test build: a QUIC panic will be injected");
    let spawned = std::thread::Builder::new()
        .name("quic-panic-injection".into())
        .spawn(move || match injection {
            Injection::Caught(after) => {
                std::thread::sleep(after);
                panic!("injected QUIC panic (caught)");
            }
            Injection::Abort(after) => {
                struct PanicsOnDrop;
                impl Drop for PanicsOnDrop {
                    fn drop(&mut self) {
                        panic!("injected QUIC panic in a destructor during unwinding");
                    }
                }
                std::thread::sleep(after);
                let _armed = PanicsOnDrop;
                panic!("injected QUIC panic (abort)");
            }
        });
    if let Err(error) = spawned {
        tracing::warn!(%error, "Test build: could not start the QUIC panic injection");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_variable_parses() {
        assert_eq!(
            parse("caught:5"),
            Some(Injection::Caught(Duration::from_secs(5)))
        );
        assert_eq!(parse(" abort : 0 "), Some(Injection::Abort(Duration::ZERO)));
        for bad in ["", "caught", "abort:x", "other:1", "caught:-1"] {
            assert_eq!(parse(bad), None, "{bad}");
        }
    }
}
