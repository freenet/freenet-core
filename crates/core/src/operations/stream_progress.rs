//! Stream-progress liveness handle for the streaming-PUT retry loop (#4001).
//!
//! # Why this exists
//!
//! A client-initiated large (streaming) PUT runs as **two** concurrent tasks
//! on the originator node that share only a [`Transaction`] id:
//!
//! 1. **Task A** — `start_client_put` → `drive_retry_loop`
//!    ([`crate::operations::op_ctx`]). It emits a `PutMsg::Request` via
//!    `send_and_await` and parks on the reply, owning the per-attempt timeout
//!    decision.
//! 2. **Task B** — the originator-loopback relay driver `drive_relay_put`
//!    ([`crate::operations::put::op_ctx_task`]). The `PutMsg::Request` loops
//!    back through the local event loop (`source_addr=None` →
//!    `upstream_addr=own_addr`) and is dispatched to this *separate* spawned
//!    task, which actually fragments and streams the payload to the next peer
//!    via `conn_manager.send_stream(...)` →
//!    [`crate::transport::peer_connection::outbound_stream::send_stream`].
//!
//! Because the per-fragment send (`sent_so_far += packet_size`) happens in
//! Task B's downstream transport task while the timeout decision lives in Task
//! A, there is no shared per-tx object to thread a handle through directly.
//! The [`StreamProgressHandle`] bridges them via a `Transaction`-keyed registry
//! ([`StreamProgressRegistry`]): Task A inserts a handle before sending and
//! removes it on every loop exit; Task B looks it up by `Transaction` and
//! records a tick per fragment. This is a *scoped* lookup, not a free-floating
//! global — the registry is owned by `OpManager` and keyed by `Transaction`,
//! mirroring the existing `pending_op_results` / `OrphanStreamRegistry`
//! patterns.
//!
//! # Liveness primitive
//!
//! The handle holds:
//! - an `AtomicU64` (`last_progress`, milliseconds) updated relaxed on each
//!   fragment from the handle's OWN clock, and
//! - a [`tokio::sync::Notify`] pinged on each fragment.
//!
//! The handle owns its clock (a `now_millis` closure over the originator's
//! `TimeSource`). Crucially, `record()` reads THAT clock — it takes no
//! caller-supplied timestamp — so the value the writer (transport send task)
//! stores and the value the reader (retry loop) compares against come from one
//! epoch. Threading the transport's connection clock into `record()` instead
//! would silently defeat the timeout, because `TimeSource::now()` is
//! epoch-relative per-instance (see the single-epoch invariant on
//! [`StreamProgressHandle`]).
//!
//! The retry loop waits on `notify.notified()` racing a `TimeSource::sleep`.
//! Notify alone is racy (a fragment that lands in the wakeup window can be
//! missed — lost-wakeup), and polling `last_progress` alone is wasteful, so
//! the loop uses **both**: it is woken promptly by `Notify`, and on each
//! inactivity-sleep expiry it re-reads `last_progress` and only declares a
//! stall if the atomic confirms no fragment landed in the race window. This
//! handle deliberately has **no dependency on transport or op types** — it is
//! a neutral handle the transport layer can hold opaquely.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::Notify;

use crate::message::Transaction;
use crate::simulation::TimeSource;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A neutral per-stream liveness handle: records a monotonic "last fragment
/// dispatched at" timestamp (millis) plus a `Notify` to wake an observer.
///
/// # The handle owns its clock (single-epoch invariant)
///
/// Both [`Self::record`] (writer, transport send task) and [`Self::since_last`]
/// (reader, the retry loop) read time from the **same** clock — the
/// `now_millis` closure captured at construction over the originator's
/// `TimeSource`. This is load-bearing: `TimeSource::now()` is epoch-relative
/// per-instance (`RealTime::elapsed()` from a per-instance epoch), so if the
/// writer recorded against the connection's `RealTime` (epoch = connection
/// establishment) while the reader measured against the op's `RealTime` (epoch
/// = op start, strictly later), the stored "last progress" would exceed the
/// reader's clock, `since_last` would `saturating_sub` to 0 forever, and the
/// inactivity stall would NEVER fire — silently degrading to the 600 s
/// ceiling. Owning the clock here makes `record()` take **no caller timestamp**,
/// guaranteeing one epoch for both sides and preserving VirtualTime/DST
/// correctness (one injected clock).
///
/// Cheap to clone (two `Arc`s + a cloned `Arc<dyn Fn>`). The recording side
/// calls [`Self::record`] once per fragment (one clock read + one relaxed store
/// plus one `notify_one`); the observing side (the retry loop) reads
/// [`Self::since_last`] and awaits [`Self::notified`].
#[derive(Clone)]
pub(crate) struct StreamProgressHandle {
    last_progress_millis: Arc<AtomicU64>,
    notify: Arc<Notify>,
    /// The single shared clock both writer and reader read from. See the
    /// single-epoch invariant in the type docs.
    now_millis: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl std::fmt::Debug for StreamProgressHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamProgressHandle")
            .field(
                "last_progress_millis",
                &self.last_progress_millis.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl StreamProgressHandle {
    /// Construct a handle bound to `time_source`. Its initial progress is the
    /// current time, so the inactivity clock starts at construction (op-send
    /// time), not the epoch.
    pub(crate) fn new<T: TimeSource>(time_source: T) -> Self {
        let now_ts = time_source;
        let now_millis: Arc<dyn Fn() -> u64 + Send + Sync> =
            Arc::new(move || now_ts.now().as_millis() as u64);
        let initial = now_millis();
        Self {
            last_progress_millis: Arc::new(AtomicU64::new(initial)),
            notify: Arc::new(Notify::new()),
            now_millis,
        }
    }

    /// Record a fragment dispatch. Reads the handle's OWN clock (never a caller-
    /// supplied timestamp — see the single-epoch invariant), then a single
    /// relaxed store plus `notify_one()`. Non-blocking and lock-free (no
    /// `.send().await`, safe per `channel-safety.md`).
    pub(crate) fn record(&self) {
        let now = (self.now_millis)();
        self.last_progress_millis.store(now, Ordering::Relaxed);
        self.notify.notify_one();
    }

    /// Duration since the last recorded progress, against the handle's own
    /// clock. Saturates at zero if a fragment landed at or after now (race
    /// window), which is exactly the tiebreak the retry loop relies on to
    /// suppress a false stall.
    pub(crate) fn since_last(&self) -> Duration {
        let now = (self.now_millis)();
        let last = self.last_progress_millis.load(Ordering::Relaxed);
        Duration::from_millis(now.saturating_sub(last))
    }

    /// Await the next `record()` ping.
    ///
    /// `Notify::notified()` is a future; a single pending permit stored by a
    /// `notify_one()` that arrives before this is awaited is consumed on the
    /// next await, which (together with the `since_last` atomic re-check) closes
    /// the lost-wakeup race.
    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

/// Streaming-attempt liveness bundle returned by
/// [`crate::operations::op_ctx::RetryDriver::stream_progress`].
///
/// Bundles the per-fragment [`StreamProgressHandle`] (which owns the shared
/// clock — see its single-epoch invariant) with a type-erased `sleep` over the
/// driver's [`TimeSource`] (so the retry loop's sleeps obey VirtualTime under
/// DST without `drive_retry_loop` taking a `TimeSource` type parameter).
/// [`TimeSource`] is `Clone` and therefore not `dyn`-safe, so `sleep` is
/// captured as a cloneable closure. The handle and this `sleep` are built from
/// the SAME `TimeSource`, so liveness reads and the retry loop's sleeps share
/// one time basis.
#[derive(Clone)]
pub(crate) struct StreamProgress {
    handle: StreamProgressHandle,
    sleep: Arc<dyn Fn(Duration) -> BoxFuture + Send + Sync>,
}

impl StreamProgress {
    /// Build a bundle from a concrete [`TimeSource`]. The handle and the `sleep`
    /// closure share this one clock, guaranteeing the single-epoch invariant.
    pub(crate) fn new<T: TimeSource>(time_source: T) -> Self {
        let handle = StreamProgressHandle::new(time_source.clone());
        let sleep_ts = time_source;
        Self {
            handle,
            sleep: Arc::new(move |d| sleep_ts.sleep(d)),
        }
    }

    /// The progress handle (cloned).
    pub(crate) fn handle(&self) -> StreamProgressHandle {
        self.handle.clone()
    }

    /// Sleep `duration` via the driver's `TimeSource` (VirtualTime-aware).
    pub(crate) fn sleep(&self, duration: Duration) -> BoxFuture {
        (self.sleep)(duration)
    }
}

/// `Transaction`-keyed registry of in-flight [`StreamProgressHandle`]s.
///
/// Owned by `OpManager`. Task A (`drive_retry_loop`) registers a handle keyed
/// by the attempt `Transaction` via a [`StreamProgressGuard`] (RAII) before
/// emitting the streaming request, so the entry is removed on EVERY exit path
/// AND on cancellation/panic; Task B (`drive_relay_put` originator loopback)
/// looks it up by the same `Transaction` and threads it into the transport send
/// so per-fragment progress is recorded. A non-streaming PUT, a non-originator
/// relay, or any op without a registered handle is a no-op on the lookup side.
pub(crate) struct StreamProgressRegistry {
    handles: DashMap<Transaction, StreamProgressHandle>,
}

impl StreamProgressRegistry {
    pub(crate) fn new() -> Self {
        Self {
            handles: DashMap::new(),
        }
    }

    /// Insert (or replace) the handle for `tx`. Called by the observing side
    /// before the streaming request is emitted.
    pub(crate) fn insert(&self, tx: Transaction, handle: StreamProgressHandle) {
        self.handles.insert(tx, handle);
    }

    /// Remove the handle for `tx`. Idempotent. Production callers go through
    /// [`StreamProgressGuard`]'s `Drop` so the entry cannot leak even under
    /// cancellation/panic (the GC concern the registry must satisfy, since it
    /// has no TTL/sweep).
    pub(crate) fn remove(&self, tx: &Transaction) {
        self.handles.remove(tx);
    }

    /// Look up the handle for `tx`, if one is registered. Called by the
    /// recording side; `None` means "no progress tracking for this op" and the
    /// caller records nothing.
    pub(crate) fn get(&self, tx: &Transaction) -> Option<StreamProgressHandle> {
        self.handles.get(tx).map(|h| h.clone())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.handles.len()
    }
}

impl Default for StreamProgressRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that registers a [`StreamProgressHandle`] for a `Transaction` on
/// construction and removes it on `Drop`.
///
/// Using a Drop guard (rather than an explicit `insert` … `remove` pair) makes
/// the "cannot leak" claim TRUE even if the holder is cancelled mid-`await` or
/// panics: the registry has no TTL/sweep, so a skipped `remove` would leak the
/// entry permanently. Mirrors `RelayPutInflightGuard`
/// (`put/op_ctx_task.rs`). The guard holds an `Arc<StreamProgressRegistry>` so
/// it can clean up without borrowing the `OpManager`.
pub(crate) struct StreamProgressGuard {
    registry: Arc<StreamProgressRegistry>,
    tx: Transaction,
}

impl StreamProgressGuard {
    /// Register `handle` for `tx`; the entry is removed when the guard drops.
    pub(crate) fn new(
        registry: Arc<StreamProgressRegistry>,
        tx: Transaction,
        handle: StreamProgressHandle,
    ) -> Self {
        registry.insert(tx, handle);
        Self { registry, tx }
    }
}

impl Drop for StreamProgressGuard {
    fn drop(&mut self) {
        self.registry.remove(&self.tx);
    }
}

/// Op-level stream-inactivity timeout for the streaming-PUT retry loop (#4001).
///
/// Distinct from the transport-layer `STREAM_INACTIVITY_TIMEOUT` (5 s in
/// `transport::peer_connection::streaming`), which bounds the gap between
/// individual transport fragments on one hop. The op-level timeout is larger on
/// purpose: it must absorb whole transport retransmit cycles (a hop can stall
/// for several seconds and recover) without the op layer prematurely declaring
/// the stream dead and firing a version-conflicting retry.
///
/// # Sizing this against the phases that record nothing (#5432)
///
/// The original 30 s was chosen as ~6x the transport inactivity window, which
/// sizes it for **one** of the three phases of an attempt: the stretch during
/// which fragments are actually being dispatched. [`StreamProgressHandle::record`]
/// is called only by the originator's outbound stream send loop, once per
/// fragment, so an attempt has two further phases in which nothing can record
/// and the clock runs unopposed:
///
/// - **Before the first fragment.** The clock starts at construction, before
///   the request is even emitted. The originator-loopback relay then runs
///   `relay_put_store_locally` — a wait on the single-threaded contract-handling
///   queue, then WASM `validate_state`/`update_state`, then a disk persist of
///   the whole state, then `announce_contract_hosted` (a DELIBERATELY blocking
///   `notify_node_event`, itself bounded by
///   `OpManager::NOTIFICATION_SEND_TIMEOUT` at 30 s) — all before a single byte
///   is streamed.
/// - **After the last fragment.** Remote reassembly, the remote's own WASM
///   execution, further downstream relay hops (each re-streaming the payload),
///   and the terminal reply's return trip all happen with the send loop
///   finished.
///
/// A multi-MiB payload routinely spends more than 30 s in either. Every
/// `fdev website publish`/`update` is such a payload (the embedded website
/// contract WASM alone exceeds the 64 KiB streaming threshold), so healthy
/// publishes were being abandoned as `stream_stall` at exactly 30 s — and since
/// streaming PUTs get a single attempt (`MAX_PEER_ADVANCEMENTS_STREAMING = 0`),
/// that was an immediate, deterministic client-visible failure.
///
/// So the window must be sized for the longest silence the pre-fragment phase
/// can plausibly take, and bounded above by how long the client will wait to
/// hear the answer. Both ends are real constraints, and they conflict:
///
/// - **Lower.** The pre-fragment phase is a wait on the contract handler, and
///   the honest measured floor for it is only **">30 s"**: in both #4912 and
///   the nova log on #5432 the stall fired at exactly 30 s while the local
///   store was still running, which tells us the phase outlasts 30 s and
///   nothing more. 120 s is an **extrapolation**, not a measurement of this
///   phase — #4912's ~120 s was queue starvation BEFORE its attempt began (see
///   the `TimeoutCause` rustdoc in `op_ctx.rs`, which says so explicitly), so
///   it is outside the span this constant governs. The inference is that the
///   same saturated node that starved the dispatch path for ~120 s will
///   equally delay the in-window store, which waits on a handler permitted up
///   to 300 s. Reasonable, and stated as inference so the next person tuning
///   this does not treat it as a measured floor.
/// - **Upper.** `fdev`'s default client-side wait is 300 s. Past that the user
///   stops seeing the node's specific `stream_stall` diagnosis and gets fdev's
///   generic "may have succeeded, verify out-of-band" instead. Precisely: the
///   verdict fires at `last_fragment + window`, not `attempt_start + window`,
///   so staying under 300 s buys the node the last word only in the
///   **no-progress** case — which is the common one, and the one #5432 is
///   about. A stream that uploads for 300 s and then dies is declared at 540 s
///   and fdev still speaks first; the ceiling, not this bound, is what limits
///   that. Do not read the bound as an unconditional guarantee.
///
/// **This does NOT cover the contract handler's full permitted budget.**
/// `CH_EV_RESPONSE_TIME_OUT` allows a `PutQuery` up to 300 s, so in the extreme
/// where the handler spends all of it, this watchdog still fires first. That is
/// a deliberate bound, not an oversight: covering it would require a window
/// above 300 s, which is precisely the upper limit above. Given a live path
/// where the terminal reply is never delivered at all even though the store
/// succeeded (`crates/core/tests/fdev_publish_e2e.rs` documents it, and
/// measures the client hanging to its full 300 s once the window is raised past
/// it), a long hang is a worse outcome than a fast wrong answer. The extreme
/// handler case is rarer than the lost-reply case, so the bound goes to the
/// common one.
///
/// The real fix for both is to make the terminal reply arrive; until then this
/// constant is a compromise between two bad outcomes rather than a derivation
/// with a single right answer. Do not "tighten" it toward either end without
/// re-reading this block — each end has a demonstrated failure behind it.
///
/// It also stays well below the 600 s
/// [`crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP`] ceiling, so a dead
/// stream is still reported as a stall rather than swallowed by the ceiling
/// (the #4912 misattribution class). All of these relationships are pinned by
/// `tests::inactivity_window_is_bounded_at_both_ends`.
pub(crate) const STREAM_OP_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(240);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::put::PutMsg;
    use crate::simulation::VirtualTime;

    /// The window must sit strictly inside both of its real bounds (#5432).
    ///
    /// Neither bound is local to this file, and each has a demonstrated failure
    /// behind it, which is why they are pinned rather than left to prose:
    ///
    /// - **Too short kills healthy PUTs.** The pre-fragment phase is a wait on
    ///   the contract handler. Measured floor is only ">30 s"; the 120 s here
    ///   is extrapolated from #4912 (see the constant's rustdoc — that 120 s
    ///   was PRE-attempt starvation, not this phase). A window under it keeps
    ///   declaring a stall on work that was about to succeed — the #5432 defect.
    /// - **Too long hands the answer to the client.** `fdev`'s default wait is
    ///   300 s. Above that the node's specific diagnosis never reaches the
    ///   user; they get fdev's generic message instead. Measured: raising the
    ///   window to 330 s -- a candidate this PR REJECTED for exactly this
    ///   reason -- made `fdev_publish_e2e` hang to fdev's full 300 s.
    /// - **Above the ceiling, a stall can never fire at all**, and every dead
    ///   stream is misreported as `stream_ceiling` (#4912's class again). The
    ///   band assertion keeps the correctly-attributed span meaningful rather
    ///   than letting it shrink to nothing.
    ///
    /// `fdev`'s constant cannot be referenced from core (fdev depends on core,
    /// not the reverse), so that bound is asserted against a local copy. A
    /// mirrored constant is a guard whose two inputs can drift apart from one
    /// edit, so `crates/fdev`'s
    /// `commands::tests::default_response_timeout_matches_the_core_stall_pin`
    /// asserts the other side of the mirror and names this test. Change either
    /// and BOTH fail, rather than one silently going stale.
    #[test]
    fn inactivity_window_is_bounded_at_both_ends() {
        let ceiling = crate::operations::STREAMING_ATTEMPT_TIMEOUT_CAP;

        // The pre-fragment phase budget. NOT a measurement of that phase:
        // the measured floor is only ">30 s" (see the constant's rustdoc);
        // 120 s is extrapolated from #4912's pre-attempt starvation.
        const PRE_FRAGMENT_PHASE_BUDGET: Duration = Duration::from_secs(120);
        // Strict inequality is not enough: 121 s would "clear" a 120 s figure
        // with no headroom, and 120 s is itself an extrapolation rather than a
        // measurement of this phase (see the constant's rustdoc). The design
        // margin is 2x, so pin the margin, not just the inequality.
        assert!(
            STREAM_OP_INACTIVITY_TIMEOUT >= 2 * PRE_FRAGMENT_PHASE_BUDGET,
            "the stream watchdog ({STREAM_OP_INACTIVITY_TIMEOUT:?}) must clear \
             the ~{PRE_FRAGMENT_PHASE_BUDGET:?} pre-fragment phase budget with \
             the 2x margin the sizing argument claims, or it keeps stalling \
             PUTs that were about to succeed"
        );

        // Mirror of fdev's DEFAULT_RESPONSE_TIMEOUT; see the doc note above.
        const FDEV_DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);
        // Same again at the top: 299 s is "under" 300 s and buys the node no
        // room at all to get its verdict out. Require a real gap.
        const CLIENT_MARGIN: Duration = Duration::from_secs(30);
        assert!(
            STREAM_OP_INACTIVITY_TIMEOUT + CLIENT_MARGIN <= FDEV_DEFAULT_RESPONSE_TIMEOUT,
            "the stream watchdog ({STREAM_OP_INACTIVITY_TIMEOUT:?}) must fire at \
             least {CLIENT_MARGIN:?} before fdev gives up \
             ({FDEV_DEFAULT_RESPONSE_TIMEOUT:?}), or the user sees fdev's \
             generic timeout instead of the node's reason"
        );

        assert!(
            STREAM_OP_INACTIVITY_TIMEOUT < ceiling,
            "the stream watchdog ({STREAM_OP_INACTIVITY_TIMEOUT:?}) must stay \
             below the hard ceiling ({ceiling:?}), or a stall can never fire"
        );

        // The span of upload during which a mid-stream death is still reported
        // as the stall it is. `< ceiling` alone passed at 599 s.
        let attributable_upload_span = ceiling - STREAM_OP_INACTIVITY_TIMEOUT;
        assert!(
            attributable_upload_span >= Duration::from_secs(120),
            "only {attributable_upload_span:?} of upload would have its stalls \
             attributed correctly; the window has crowded the ceiling"
        );
    }

    #[test]
    fn record_advances_last_progress_and_since_last_tracks_it() {
        let clock = VirtualTime::new();
        let handle = StreamProgressHandle::new(clock.clone());
        // Constructed at t=0 → since_last is 0; advancing 5 s without a record
        // grows since_last.
        assert_eq!(handle.since_last(), Duration::ZERO);
        clock.advance(Duration::from_secs(5));
        assert_eq!(handle.since_last(), Duration::from_secs(5));

        // record() resets the clock to "now".
        handle.record();
        assert_eq!(handle.since_last(), Duration::ZERO);
        clock.advance(Duration::from_millis(2_500));
        assert_eq!(handle.since_last(), Duration::from_millis(2_500));
    }

    /// The single-epoch invariant: writer and reader must read the SAME clock.
    ///
    /// Regression for the cross-epoch `RealTime` bug — the writer recorded
    /// against the connection's epoch (earlier than the op's), so the stored
    /// value exceeded the reader's clock and `since_last` saturated to 0
    /// forever, defeating the stall entirely. Because `record()` now reads the
    /// handle's OWN clock (no caller timestamp), a clone used by the "writer"
    /// shares the reader's basis: after a record, then a full window of silence,
    /// `since_last` reflects the REAL elapsed time and the stall fires.
    ///
    /// This test fails on the pre-fix code (where `record(now_millis)` took an
    /// external, later-epoch timestamp): the stored value would be > the
    /// reader's now and `since_last()` would be 0 here, not 30 s.
    #[test]
    fn writer_and_reader_share_one_clock_so_stall_is_detectable() {
        // Reader clock, already advanced (simulating an op that started after
        // the connection was established — the skew that caused the bug).
        let clock = VirtualTime::new();
        clock.advance(Duration::from_secs(120));
        let reader = StreamProgressHandle::new(clock.clone());

        // The "writer" is a clone handed to the transport. It records using the
        // handle's own clock — NOT a separate, earlier-epoch connection clock.
        let writer = reader.clone();
        writer.record();
        assert_eq!(reader.since_last(), Duration::ZERO, "fresh record → 0");

        // Stream goes silent for 30 s. since_last MUST reflect the real elapsed
        // time (not saturate to 0), so the inactivity stall can fire.
        clock.advance(STREAM_OP_INACTIVITY_TIMEOUT);
        assert_eq!(
            reader.since_last(),
            STREAM_OP_INACTIVITY_TIMEOUT,
            "after the writer's record + 30 s of silence, since_last must show \
             the true elapsed time so the stall fires; a cross-epoch clock would \
             saturate this to 0 and defeat the whole fix"
        );
    }

    #[test]
    fn since_last_saturates_when_progress_is_in_the_future() {
        // With a single shared clock, since_last is never negative; sanity-check
        // that a fresh record yields zero elapsed (the race-window tiebreak),
        // never an underflow panic.
        let clock = VirtualTime::new();
        let handle = StreamProgressHandle::new(clock);
        handle.record();
        assert_eq!(handle.since_last(), Duration::ZERO);
    }

    #[tokio::test]
    async fn notified_wakes_after_record() {
        let handle = StreamProgressHandle::new(VirtualTime::new());
        let observer = handle.clone();
        // notify_one() before notified() leaves a permit; the await returns
        // immediately, proving the lost-wakeup-avoidance permit semantics.
        handle.record();
        tokio::time::timeout(Duration::from_secs(1), observer.notified())
            .await
            .expect("a record() before notified() must leave a permit");
    }

    #[test]
    fn registry_insert_get_remove_roundtrip_and_is_leak_free() {
        let registry = StreamProgressRegistry::new();
        let tx = Transaction::new::<PutMsg>();
        assert_eq!(registry.len(), 0);
        assert!(registry.get(&tx).is_none());

        registry.insert(tx, StreamProgressHandle::new(VirtualTime::new()));
        assert_eq!(registry.len(), 1);
        assert!(registry.get(&tx).is_some());

        registry.remove(&tx);
        assert_eq!(registry.len(), 0, "remove must leave the registry empty");
        // Idempotent: a second remove on an absent key is a no-op.
        registry.remove(&tx);
        assert_eq!(registry.len(), 0);
    }

    /// The `StreamProgressGuard` registers on construction and removes on Drop,
    /// so the registry entry cannot leak — including when the holder is dropped
    /// early (cancellation) rather than reaching an explicit cleanup line. This
    /// is the property that makes the retry loop's "cannot leak under
    /// cancel/panic" claim true.
    #[test]
    fn guard_registers_on_new_and_removes_on_drop_including_early_drop() {
        let registry = Arc::new(StreamProgressRegistry::new());
        let tx = Transaction::new::<PutMsg>();
        let handle = StreamProgressHandle::new(VirtualTime::new());

        // Normal scope: registered while alive, removed when the guard drops.
        {
            let _guard = StreamProgressGuard::new(registry.clone(), tx, handle.clone());
            assert_eq!(registry.len(), 1, "guard must register on construction");
            assert!(registry.get(&tx).is_some());
        }
        assert_eq!(registry.len(), 0, "guard must remove on Drop");

        // Early drop (simulates the future being cancelled mid-await before any
        // explicit remove line runs): the entry is still cleaned up.
        let guard = StreamProgressGuard::new(registry.clone(), tx, handle);
        assert_eq!(registry.len(), 1);
        drop(guard); // cancellation drops the guard without running a remove stmt
        assert_eq!(
            registry.len(),
            0,
            "an early/cancellation drop of the guard must still remove the entry"
        );
    }
}
