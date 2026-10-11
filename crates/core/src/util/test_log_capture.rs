//! Order-independent `tracing` capture for unit tests.
//!
//! A test that asserts on emitted `tracing` events installs a capturing
//! subscriber with [`tracing::subscriber::set_default`], which is **thread
//! local**: only events emitted inline on the test's own thread are recorded.
//! That part is fine — libtest gives every test its own thread. What is *not*
//! fine is that whether an event reaches the capture at all depends on a
//! **process-global** decision made by whichever thread happens to touch that
//! event's callsite first.
//!
//! # The failure this module exists to prevent (#4927)
//!
//! `tracing` caches an [`Interest`] per callsite, once, the first time that
//! callsite is reached (`tracing_core::callsite::register`). It is never
//! re-evaluated afterwards unless some thread creates a new `Dispatch` (which
//! rebuilds the cache for every *already-registered* callsite). Two details of
//! `tracing_core` 0.1.36 turn that into a cross-test race:
//!
//! 1. `Callsites::rebuild_interest` takes a `Rebuilder::JustOne` fast path when
//!    at most one dispatcher is registered, and that path resolves interest
//!    against `dispatcher::get_default()` — **the registering thread's**
//!    dispatcher, not the registered one. A test thread that has installed no
//!    subscriber resolves to `NoSubscriber`, whose `register_callsite` returns
//!    [`Interest::never`].
//! 2. `rebuild_callsite_interest` falls back to `Interest::never()` when no
//!    dispatcher answers at all.
//!
//! So while test A holds a thread-local capture, concurrently-running test B on
//! another thread can be the first to reach a callsite A asserts on, pin it to
//! `never` process-wide, and A then observes *nothing* from that callsite — no
//! matter that its capture is installed and working for every other callsite.
//! The symptom is an assertion failure whose "captured:" list holds a
//! *different subset* of the expected events on each run, at roughly 1% per run
//! locally. It cannot be fixed by re-running, and `cargo nextest --profile ci`
//! (`retries = 2`) absorbs it, so CI never sees it.
//!
//! A third variant of the same root cause: `#[test_log::test]` (used widely in
//! this crate) installs a **global** default subscriber filtered at `INFO`.
//! Once it exists, a `DEBUG` callsite first reached from a thread with no
//! scoped subscriber resolves against that `INFO` filter and is likewise pinned
//! to `never`, and the global max-level hint drops to `INFO`.
//!
//! # The fix
//!
//! Keep two permanently-registered, never-defaulted [`InterestKeeper`]
//! dispatchers alive for the life of the test process. They report interest in
//! every callsite and are enabled for none, which removes every route to a
//! cached `never`:
//!
//! * With two of them always live, the registered-dispatcher count is never
//!   `<= 1`, so `Rebuilder::JustOne` is never taken again and interest is
//!   always resolved against the **full set** of live dispatchers — which
//!   includes the per-test capture.
//! * `Interest::and` of differing kinds is `sometimes`, so the combination is
//!   at worst `sometimes`: enabled-ness is then decided per event against the
//!   *emitting thread's* dispatcher, which is exactly the correct behaviour for
//!   both the capture and every filtered subscriber in the binary.
//! * Their absent max-level hint keeps the global max level at `TRACE`, so
//!   `test_log`'s `INFO` global cannot compile out `DEBUG` capture.
//!
//! Two keepers rather than one is load-bearing: `has_just_one` is
//! `live_dispatchers <= 1`, so a single keeper would still leave the
//! `JustOne` path armed whenever it is the only live dispatcher. Do not
//! "simplify" this to one.
//!
//! The keepers are never installed as the *global default*, deliberately:
//! taking that slot would make `test_log`'s `try_init()` fail and silently stop
//! `RUST_LOG=... cargo test` from printing anything.

use std::sync::{Arc, Mutex, OnceLock};

use tracing::subscriber::DefaultGuard;

/// Formatted events recorded by an installed capture, newest last.
pub(crate) type CapturedLogs = Arc<Mutex<Vec<String>>>;

/// A subscriber that is interested in every callsite and enabled for none.
///
/// It exists purely to keep callsite `Interest` resolvable; it is never set as
/// any thread's (or the process's) default, so its `enabled` is only ever
/// consulted if some future change does install it — in which case answering
/// `false` keeps it inert.
struct InterestKeeper;

impl tracing::Subscriber for InterestKeeper {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        // NOT `always`: `always` tells `tracing` that no per-event `enabled`
        // check is needed, which would bypass other subscribers' filters when
        // this interest wins the `Interest::and` combination. `sometimes` keeps
        // every subscriber's own filtering intact.
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, _: &tracing::Event<'_>) {}

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

/// Register (and keep alive forever) the two interest keepers, then re-resolve
/// every callsite already cached — an earlier test may have pinned one to
/// `never` before the keepers existed.
fn ensure_interest_keepers() {
    static KEEPERS: OnceLock<[tracing::Dispatch; 2]> = OnceLock::new();
    KEEPERS.get_or_init(|| {
        // Creating a `Dispatch` is what registers it with the callsite
        // registry (`tracing_core::dispatcher::Dispatch::new` ->
        // `callsite::register_dispatch`). Holding them in this `OnceLock` is
        // what keeps the registry's `Weak`s upgradeable for the whole process.
        let keepers = [
            tracing::Dispatch::new(InterestKeeper),
            tracing::Dispatch::new(InterestKeeper),
        ];
        tracing::callsite::rebuild_interest_cache();
        keepers
    });
}

/// Install a thread-local capture that records every event reaching this thread
/// as a `"<LEVEL> field=value ..."` string.
///
/// Returns the buffer and the subscriber guard; drop the guard to detach the
/// capture (dropping it before reading the buffer also guarantees no further
/// writes race the assertions).
pub(crate) fn install() -> (CapturedLogs, DefaultGuard) {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Default, Clone)]
    struct Capture(CapturedLogs);
    impl<S: tracing::Subscriber> Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct V(String);
            impl tracing::field::Visit for V {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write;
                    // Writing into a String is infallible; ignore the Result.
                    write!(self.0, " {}={value:?}", field.name()).ok();
                }
            }
            let mut v = V(String::new());
            event.record(&mut v);
            self.0
                .lock()
                .unwrap()
                .push(format!("{}{}", event.metadata().level(), v.0));
        }
    }

    // MUST precede the capture: the keepers' registration is what stops a
    // concurrent thread from pinning a callsite this capture needs to `never`.
    ensure_interest_keepers();

    let capture = Capture::default();
    let messages = capture.0.clone();
    let subscriber = tracing_subscriber::registry().with(capture);
    let guard = tracing::subscriber::set_default(subscriber);
    (messages, guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canary callsite for the regression test below. Reached from nowhere
    /// else, so the test controls which thread registers it first.
    fn emit_canary() {
        tracing::error!(phase = "log_capture_canary", "log capture canary");
    }

    const CHILD_ENV: &str = "FREENET_LOG_CAPTURE_CANARY_CHILD";
    const CHILD_TEST: &str =
        "util::test_log_capture::tests::callsite_first_reached_by_another_thread_is_still_captured";

    /// Regression test for #4927: an event must reach the capture even when its
    /// callsite was first reached by a *different*, subscriber-less thread
    /// while the capture was installed.
    ///
    /// This runs its assertions in a **child process** (a re-exec of the test
    /// binary filtered to this one test) on purpose. In-process the outcome
    /// depends on how many other dispatchers happen to be alive — any second
    /// live dispatcher disarms `tracing`'s `JustOne` path and the bug hides. A
    /// child that runs this test alone has a known dispatcher population, so
    /// the check is deterministic in both directions: it fails every time if
    /// `ensure_interest_keepers` is removed from `install`, and passes every
    /// time with it.
    #[test]
    fn callsite_first_reached_by_another_thread_is_still_captured() {
        if std::env::var_os(CHILD_ENV).is_some() {
            let (messages, guard) = install();

            // First touch of the canary callsite happens here, on a thread with
            // no subscriber of its own. Without the interest keepers this pins
            // the callsite's process-global Interest to `never`, and the
            // emission below is dropped before it can reach the capture.
            std::thread::spawn(emit_canary)
                .join()
                .expect("canary thread panicked");

            emit_canary();
            drop(guard);

            let logs = messages.lock().unwrap();
            let seen = logs
                .iter()
                .filter(|l| l.contains("log capture canary"))
                .count();
            assert_eq!(
                seen, 1,
                "the capture must record the emission on its own thread (and only \
                 that one) even though another thread registered the callsite \
                 first; captured: {logs:?}"
            );
            return;
        }

        let exe = std::env::current_exe().expect("test binary path");
        let output = std::process::Command::new(exe)
            .args(["--exact", "--test-threads=1", "--nocapture", CHILD_TEST])
            .env(CHILD_ENV, "1")
            .output()
            .expect("re-exec the test binary");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "child run of {CHILD_TEST} failed.\nstdout:\n{stdout}\nstderr:\n{stderr}",
        );
        // Fail CLOSED on a rename: libtest exits 0 when its filter matches
        // nothing, so without this the whole regression check would silently
        // become vacuous the moment this function is renamed.
        assert!(
            stdout.contains("1 passed"),
            "the child must actually have run {CHILD_TEST} — if this function was \
             renamed, update CHILD_TEST.\nstdout:\n{stdout}\nstderr:\n{stderr}",
        );
    }

    /// `always` would let this keeper's interest suppress other subscribers'
    /// per-event filtering (see `register_callsite`), and `never` would
    /// reinstate the bug outright. Probed against a real callsite's metadata,
    /// grabbed from a live event rather than synthesised.
    #[test]
    fn interest_keeper_is_interested_in_every_callsite_but_enabled_for_none() {
        use tracing::Subscriber;
        use tracing_subscriber::Layer;
        use tracing_subscriber::layer::SubscriberExt;

        static META: OnceLock<&'static tracing::Metadata<'static>> = OnceLock::new();

        struct Grab;
        impl<S: tracing::Subscriber> Layer<S> for Grab {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if META.set(event.metadata()).is_err() {
                    // The first event wins; later ones have nothing to add.
                }
            }
        }

        // This test captures an event itself, so it needs the same protection
        // `install` gives its callers — otherwise it is flaky in exactly the
        // way this module exists to prevent.
        ensure_interest_keepers();

        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(Grab));
        emit_canary();
        drop(guard);

        let meta = *META
            .get()
            .expect("the canary event must reach the grabbing layer");
        assert!(
            InterestKeeper.register_callsite(meta).is_sometimes(),
            "the keeper must report `sometimes` so per-event filtering is preserved"
        );
        assert!(
            !InterestKeeper.enabled(meta),
            "the keeper must never enable a callsite for itself"
        );
    }
}
