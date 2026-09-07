//! The V2 delegate broadcast drain (#5479), lifted out of the network event
//! loop's `select!` so that it is reachable from a test.
//!
//! # Why this is a module and not a `match` arm
//!
//! This PR already applied exactly this refactor twice, and said why each
//! time: `classify_drain_read` was split out of `read_state_for_broadcast_drain`
//! because "the mapping can be tested exhaustively without a live contract
//! handler", and `plan_v2_drain_retry` was split out of the dispatch arm
//! because "inline in a `match` arm of the select loop it is unreachable from
//! any test".
//!
//! Both observations were right and neither went far enough. What remained in
//! the arm was the part that decides whether a committed write is announced at
//! all, and a mutation-testing pass over `4ef5823c0` found that region had NO
//! behavioural coverage whatsoever: six separate mutations to it — including
//! deleting the fan-out call outright, which reinstates #5479 in full — each
//! left the entire 5440-test suite green. The three source-order pins written
//! to compensate all passed while the call each one guards was commented out.
//!
//! Source-order pins can assert that a call appears in a block. They cannot
//! assert that it runs, that it runs for the right read outcome, or that it
//! runs at all when the line is commented rather than deleted. That is the gap
//! this module closes.
//!
//! # The seam
//!
//! [`V2DrainCtx`] is the whole trick. The drain's *decisions* live in
//! [`handle_v2_delegate_state_changed`]; its three *side effects* — touching
//! the retry map, fanning out, scheduling a retry — go through the trait. The
//! production implementor is `P2pConnManager`; the tests use a recording
//! double, so they observe what the drain decided to do without standing up a
//! transport, a bridge, or an event register.
//!
//! The retry body is [`run_v2_drain_retry`], a free function rather than an
//! inline `tokio::spawn` closure, for the same reason: a closure inside a
//! spawn inside a match arm is not addressable, and the thing it does — re-queue
//! through the marker-respecting API rather than a raw emit — is load-bearing.

use super::*;
use crate::node::op_state_manager::{DrainStateRead, V2BroadcastQueued};
use freenet_stdlib::prelude::{ContractKey, WrappedState};
use std::collections::HashMap;

/// The side effects the V2 drain performs, behind a trait so the drain's
/// control flow can be driven by a test.
///
/// Deliberately narrow: three methods, each one an effect the drain must
/// perform for exactly one read outcome. A test double implementing these
/// records which were called and with what, which is what turns "the fan-out
/// call is present in the source" into "the fan-out happened, for `Found`, with
/// the state that was read".
///
/// The retry map is reached through the trait rather than passed as a separate
/// `&mut` argument on purpose: `P2pConnManager` owns both the map and the
/// fan-out, so two independent `&mut` borrows of it would not type-check at the
/// call site. One receiver, three methods.
pub(super) trait V2DrainCtx {
    /// Per-contract retry counter for drains whose state read came back
    /// `Unavailable`. Cleared on every terminal outcome.
    fn v2_drain_retries_mut(&mut self) -> &mut HashMap<ContractKey, u8>;

    /// Announce `new_state` for `key` to every advertised co-host.
    ///
    /// This is the leg whose absence IS #5479: without it a V2 delegate write
    /// commits locally, returns success to the delegate, and is never seen off
    /// this node.
    fn fan_out(
        &mut self,
        op_manager: &Arc<OpManager>,
        key: ContractKey,
        new_state: WrappedState,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Re-attempt the drain for `key` after `delay`.
    fn schedule_retry(&mut self, op_manager: &Arc<OpManager>, key: ContractKey, delay: Duration);
}

/// Drain one `NodeEvent::V2DelegateStateChanged`.
///
/// `jitter_pct` is a parameter rather than a `GlobalRng` call inside, matching
/// `plan_v2_drain_retry`, so the retry arithmetic is deterministic under test
/// while production still supplies a random factor.
///
/// # The marker is cleared BEFORE the read, and that ordering is the fix
///
/// A write landing during the read then queues a fresh event instead of being
/// folded into this drain, which is already past the point where it could
/// observe it. Clearing afterwards would drop that write's fan-out — #5479
/// again, one layer up.
///
/// The marker also stays clear for the whole fan-out, so a write landing
/// mid-fan-out queues a fresh event and re-sends bytes already going out.
/// Correct but wasteful, on the sink #5147/#5153 exist to shrink. Deliberate:
/// holding the marker until the fan-out completed would trade a duplicate send
/// for a DROPPED one, and a drop here is unrecoverable until the next write.
pub(super) async fn handle_v2_delegate_state_changed<C: V2DrainCtx>(
    ctx: &mut C,
    op_manager: &Arc<OpManager>,
    key: ContractKey,
    jitter_pct: u64,
) {
    op_manager.clear_v2_delegate_broadcast_pending(&key);

    // The event carries no state; read what is stored now. BOUNDED (#4549 —
    // this runs inline on the event loop) and three-way, because "we do not
    // hold it" and "the read failed" need opposite handling.
    match op_manager
        .read_state_for_broadcast_drain(&key, V2_BROADCAST_DRAIN_READ_TIMEOUT)
        .await
    {
        DrainStateRead::Found(new_state) => {
            ctx.v2_drain_retries_mut().remove(&key);
            ctx.fan_out(op_manager, key, new_state).await;
        }
        DrainStateRead::NotHeld => {
            // Definitive: retrying cannot change it.
            ctx.v2_drain_retries_mut().remove(&key);
            tracing::debug!(
                contract = %key,
                "V2 delegate broadcast drained for a contract we do not hold; nothing to send"
            );
        }
        DrainStateRead::Unavailable => {
            // The read is a `GetQuery` to the serial `contract_handling` loop,
            // and the event being drained was queued by a delegate write that
            // ran ON that loop — so this read waits for the delegate that
            // caused it. A bounded retry recovers a delegate that was merely
            // busy.
            //
            // Re-queueing is safe: the marker was cleared before the read, so a
            // fresh write is free to queue its own event and at worst we fan
            // out twice.
            let attempts_so_far = *ctx.v2_drain_retries_mut().entry(key).or_insert(0);
            if let Some(delay) = P2pConnManager::plan_v2_drain_retry(attempts_so_far, jitter_pct) {
                *ctx.v2_drain_retries_mut().entry(key).or_insert(0) += 1;
                ctx.schedule_retry(op_manager, key, delay);
            } else {
                // Retries exhausted. A write that already committed and already
                // returned success to the delegate now goes unannounced.
                //
                // WARN, not debug: `release_max_level_info` compiles debug out,
                // and a drop that leaves no evidence in a release build is the
                // #4981 shape.
                ctx.v2_drain_retries_mut().remove(&key);
                note_v2_broadcast_drain_dropped(&key);
            }
        }
    }
}

/// The body of a scheduled V2 drain retry.
///
/// A free function rather than an inline `tokio::spawn` closure so a test can
/// call it with a zero delay and observe the re-queue.
///
/// Interruptible: a >=1s plain sleep in a retry loop would keep this task alive
/// past shutdown (`code-style.md`).
pub(super) async fn run_v2_drain_retry(
    op_manager: Arc<OpManager>,
    key: ContractKey,
    delay: Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::select! {
        _ = shutdown.cancelled() => return,
        _ = tokio::time::sleep(delay) => {}
    }
    // Through the marker-respecting API, not a raw emit: if a fresh write has
    // already queued its own drain for this contract, this retry coalesces into
    // it instead of adding a second event.
    //
    // NOTHING IS COUNTED HERE. This site used to count every non-`Queued`
    // outcome as a dropped broadcast, which was wrong in both directions — see
    // the arms. The drop WARN fires at powers of two, so inflating the counter
    // with benign coalesces pushes the next milestone exponentially out of
    // reach and a genuine drop then logs nothing at all.
    match op_manager.queue_v2_delegate_broadcast(key) {
        // The retry did its job.
        V2BroadcastQueued::Queued => {}
        // A fresh write queued its own drain while we slept. That drain
        // re-reads stored state, so this write is announced by it. Nothing was
        // lost and nothing is counted.
        V2BroadcastQueued::Coalesced => {}
        // A real drop, but already counted and WARNed inside the call. Counting
        // it here too would report one event twice across two separate
        // counters.
        V2BroadcastQueued::EnqueueFailed => {}
    }
}

/// Spawn a V2 drain retry for `key` after `delay`.
///
/// A free function rather than an inline body in
/// `<P2pConnManager as V2DrainCtx>::schedule_retry`, so that the WIRING — the
/// `OpManager` clone, the shutdown token taken from the ring, and the spawn
/// itself — is reachable from a test. Emptying `schedule_retry`'s body was the
/// one mutation the recording-double tests could not see, because they replace
/// the very impl that holds it.
pub(super) fn spawn_v2_drain_retry(op_manager: &Arc<OpManager>, key: ContractKey, delay: Duration) {
    let op_mgr = op_manager.clone();
    let shutdown = op_manager.ring.shutdown_token();
    tokio::spawn(run_v2_drain_retry(op_mgr, key, delay, shutdown));
}

/// The PRODUCTION side effects.
///
/// # What tests can and cannot reach here
///
/// The nine behavioural tests drive `RecordingCtx`, which REPLACES this impl —
/// so they say nothing about it. That is the seam's cost, and it is the same
/// shape as the mutation that started this work: emptying `schedule_retry` to
/// `{}` stops every `Unavailable` drain retrying while every test stays green.
///
/// Two mitigations, and the honest limit:
///
/// * Each method is now a ONE-LINE delegation to something that is itself
///   tested — `spawn_v2_drain_retry` behaviourally, `handle_broadcast_state_change`
///   by its own suite. There is no logic left in here to get wrong.
/// * `production_drain_ctx_delegates_rather_than_reimplementing` pins those
///   three delegations as REAL CODE through the comment-stripping scanner, so
///   an emptied body or a commented-out call fails.
///
/// LIMIT, stated rather than implied: that pin is a source scrape, and this
/// file's own history is why that is worth distrusting. Genuinely executing
/// this impl needs a constructed `P2pConnManager` — 25 fields including a
/// transport, a bridge and an event register — and, worse, `fan_out` would
/// still need a populated ring before a broadcast attempt were observable. That
/// fixture is a larger piece of work than this change and is NOT done here.
impl V2DrainCtx for P2pConnManager {
    fn v2_drain_retries_mut(&mut self) -> &mut HashMap<ContractKey, u8> {
        &mut self.v2_drain_retries
    }

    async fn fan_out(
        &mut self,
        op_manager: &Arc<OpManager>,
        key: ContractKey,
        new_state: WrappedState,
    ) {
        self.handle_broadcast_state_change(op_manager, key, new_state, false, false)
            .await;
    }

    fn schedule_retry(&mut self, op_manager: &Arc<OpManager>, key: ContractKey, delay: Duration) {
        spawn_v2_drain_retry(op_manager, key, delay);
    }
}

#[cfg(test)]
mod tests {
    //! Behavioural coverage for the V2 drain.
    //!
    //! Every test here was written against a mutation that survived the full
    //! 5440-test suite on `4ef5823c0`, and each is verified by re-applying that
    //! mutation and watching this test fail. A test that has only been watched
    //! to pass is not a verified test — that is the finding this module exists
    //! to answer, so it would be incoherent not to hold these to it.
    //!
    //! The mutation each test kills is named in its doc comment.

    use super::*;
    use crate::config::ConfigArgs;
    use crate::contract::{ContractHandlerEvent, OperationMode, StoreResponse};
    use crate::message::NodeEvent;
    use crate::node::EventLoopNotificationsReceiver;
    use freenet_stdlib::prelude::{ContractCode, Parameters};
    use std::sync::Mutex;

    /// What the stub contract handler answers a drain's `GetQuery` with.
    #[derive(Clone, Copy)]
    enum StubReply {
        /// State present → `DrainStateRead::Found`.
        Found(&'static [u8]),
        /// Answered, no state → `DrainStateRead::NotHeld`.
        NoState,
        /// Executor error → `DrainStateRead::Unavailable`.
        ///
        /// Used in preference to simply not answering: a silent handler would
        /// make every `Unavailable` test wait out the full
        /// `V2_BROADCAST_DRAIN_READ_TIMEOUT`, and the bound already has its own
        /// dedicated test in `pool_tests`. Here we want the classification, not
        /// the timeout.
        ExecutorError,
    }

    /// Records what the drain decided to do, in place of performing it.
    ///
    /// This is the seam that makes the drain testable: production wires these
    /// three methods to `P2pConnManager`, and the assertions below read them
    /// back.
    #[derive(Default)]
    struct RecordingCtx {
        retries: HashMap<ContractKey, u8>,
        fanned_out: Vec<(ContractKey, Vec<u8>)>,
        retries_scheduled: Vec<(ContractKey, Duration)>,
    }

    impl V2DrainCtx for RecordingCtx {
        fn v2_drain_retries_mut(&mut self) -> &mut HashMap<ContractKey, u8> {
            &mut self.retries
        }

        async fn fan_out(
            &mut self,
            _op_manager: &Arc<OpManager>,
            key: ContractKey,
            new_state: WrappedState,
        ) {
            self.fanned_out.push((key, new_state.as_ref().to_vec()));
        }

        fn schedule_retry(
            &mut self,
            _op_manager: &Arc<OpManager>,
            key: ContractKey,
            delay: Duration,
        ) {
            self.retries_scheduled.push((key, delay));
        }
    }

    /// Return `src` between `start` with all COMMENTS stripped — both `//` line
    /// comments and `/* */` block comments.
    ///
    /// Stripping BOTH is the point. The pins this replaces stripped only lines
    /// whose trimmed start was `//`, and a reviewer defeated all three of them
    /// by block-commenting the guarded call: the needle stayed in the text, the
    /// pin stayed green, and the call was inert. A scraper that skips one kind
    /// of comment is not skipping comments.
    fn strip_comments(src: &str) -> String {
        let mut out = String::with_capacity(src.len());
        let mut rest = src;
        loop {
            let (idx, is_line) = match (rest.find("//"), rest.find("/*")) {
                (None, None) => {
                    out.push_str(rest);
                    return out;
                }
                (Some(l), None) => (l, true),
                (None, Some(b)) => (b, false),
                (Some(l), Some(b)) => {
                    if l < b {
                        (l, true)
                    } else {
                        (b, false)
                    }
                }
            };
            out.push_str(&rest[..idx]);
            let tail = &rest[idx..];
            if is_line {
                match tail.find('\n') {
                    Some(nl) => rest = &tail[nl..],
                    None => return out,
                }
            } else {
                match tail.find("*/") {
                    Some(e) => rest = &tail[e + 2..],
                    None => return out,
                }
            }
        }
    }

    /// The body of `handle_v2_delegate_state_changed`, comments stripped.
    ///
    /// Bounded with `.expect(..)` rather than `unwrap_or(SOURCE.len())`. The
    /// pin this replaces used the latter, so losing its end anchor silently
    /// widened the window to the whole file — including the pin's own assertion
    /// prose, which contained the needle twice. Measured on `4ef5823c0`: the
    /// bounded window saw 3 occurrences and the unbounded one saw 5. Failing
    /// loudly on a moved anchor is the only safe behaviour.
    fn drain_fn_body() -> String {
        const SOURCE: &str = include_str!("v2_drain.rs");
        let start = SOURCE
            .find("pub(super) async fn handle_v2_delegate_state_changed")
            .expect("handle_v2_delegate_state_changed must exist — re-anchor this pin");
        let rest = &SOURCE[start..];
        let end = rest
            .find("\n/// The body of a scheduled V2 drain retry.")
            .expect(
                "the retry-body fn no longer follows the drain fn — this pin bounds its \
                 search on it, and without a bound it would scan its own assertion text \
                 and pass vacuously. Re-anchor it.",
            );
        strip_comments(&rest[..end])
    }

    /// Every terminal outcome must drop its retry-map entry.
    ///
    /// `v2_drain_retries` has no size cap, so an arm that forgets to remove
    /// leaks one entry per contract that reaches it. The three behavioural
    /// tests above each assert this for their own outcome; this is the cheap
    /// structural backstop that also catches a FOURTH terminal arm being added
    /// without one.
    #[test]
    fn every_terminal_outcome_drops_its_retry_entry() {
        let body = drain_fn_body();
        let removals = body.matches("v2_drain_retries_mut().remove(&key)").count();
        assert_eq!(
            removals, 3,
            "expected exactly 3 `v2_drain_retries_mut().remove(&key)` sites in the drain \
             (Found, NotHeld, retries-exhausted); found {removals}. Fewer means a terminal \
             outcome leaks a map entry; more means a still-retrying path drops its own \
             counter, making the retry unbounded."
        );
    }

    /// All THREE `V2BroadcastQueued` outcomes must be distinguishable.
    ///
    /// KILLS MUTATION N6. Returning `Coalesced` where `EnqueueFailed` belongs
    /// survived the full suite: nothing anywhere observed the difference, so a
    /// two-valued signal was replaced by a three-valued one that no test could
    /// tell apart. The arms are not cosmetic -- `run_v2_drain_retry` branches on
    /// them, and the branch exists because conflating them mis-counts drops:
    /// `Coalesced` is benign (a fresh write queued its own drain, so this write
    /// IS announced) while `EnqueueFailed` is a real lost broadcast. Counting a
    /// coalesce as a drop pushes the power-of-two WARN milestone exponentially
    /// out of reach, so a genuine drop then logs nothing at all.
    ///
    /// Each arm is checked together with its marker side effect, because the
    /// return value and the marker have to agree: a `Coalesced` that released
    /// the marker, or an `EnqueueFailed` that held it, is the latch bug.
    #[tokio::test(flavor = "current_thread")]
    async fn queue_v2_delegate_broadcast_distinguishes_its_three_outcomes() {
        let (op_manager, rx, _probe, _guards) =
            harness("v2drain-queue-outcomes", StubReply::NoState).await;
        let first = test_key(41);

        // 1. Nothing queued for this contract yet -> Queued, marker taken.
        assert!(
            matches!(
                op_manager.queue_v2_delegate_broadcast(first),
                V2BroadcastQueued::Queued
            ),
            "the first write for a contract must report Queued"
        );
        assert!(
            op_manager
                .v2_delegate_broadcast_pending
                .contains(first.id()),
            "a Queued broadcast must hold the coalescing marker until the drain clears it"
        );

        // 2. Same contract, still undrained -> Coalesced. NOT a drop: the
        //    outstanding drain re-reads stored state, so this write is announced
        //    by it.
        assert!(
            matches!(
                op_manager.queue_v2_delegate_broadcast(first),
                V2BroadcastQueued::Coalesced
            ),
            "a repeat write while a broadcast is undrained must report Coalesced, not \
             Queued and not EnqueueFailed -- it is benign and must not be counted as a \
             dropped broadcast"
        );
        assert!(
            op_manager
                .v2_delegate_broadcast_pending
                .contains(first.id()),
            "coalescing must LEAVE the marker set; releasing it here would let the next \
             write queue a second event for a broadcast that has not drained"
        );

        // 3. Channel closed -> EnqueueFailed, marker released. A fresh contract,
        //    so the insert succeeds and the failure is genuinely the enqueue.
        drop(rx);
        let second = test_key(42);
        assert!(
            matches!(
                op_manager.queue_v2_delegate_broadcast(second),
                V2BroadcastQueued::EnqueueFailed
            ),
            "a failed enqueue must report EnqueueFailed and NOT Coalesced. Reporting a \
             real lost broadcast as a benign coalesce is mutation N6: the drop goes \
             uncounted, and because the WARN fires at powers of two, an inflated or \
             deflated counter means a genuine drop logs nothing at all"
        );
        assert!(
            !op_manager
                .v2_delegate_broadcast_pending
                .contains(second.id()),
            "a failed enqueue must RELEASE the marker. Left set it latches the contract: \
             every later write coalesces into a broadcast that was never queued and will \
             never drain, so the contract stops propagating for the process lifetime"
        );
    }

    /// The production `schedule_retry` wiring, driven for real.
    ///
    /// KILLS the seam review's Medium at one remove: `schedule_retry`'s body is
    /// now a single call to this, and this is exercised end-to-end — the
    /// `OpManager` clone, the shutdown token pulled from the ring, the spawn,
    /// and the re-queue the spawned task performs.
    ///
    /// The recording double cannot reach any of that, because it REPLACES the
    /// impl that holds it. This is the closest a test gets without constructing
    /// a whole `P2pConnManager`.
    #[tokio::test(flavor = "current_thread")]
    async fn spawn_v2_drain_retry_requeues_after_the_delay() {
        let (op_manager, mut rx, _probe, _guards) =
            harness("v2drain-spawnwire", StubReply::NoState).await;
        let key = test_key(37);

        spawn_v2_drain_retry(&op_manager, key, Duration::ZERO);

        // Let the spawned task run. `current_thread` means it cannot progress
        // until we yield.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut queued = Vec::new();
        while let Ok(event) = rx.notifications_receiver.try_recv() {
            if let either::Either::Right(NodeEvent::V2DelegateStateChanged { key }) = event {
                queued.push(key);
            }
        }
        assert_eq!(
            queued,
            vec![key],
            "spawning a drain retry must actually re-queue the drain. If this is empty the \
             spawn wiring is broken -- the clone, the shutdown token, or the spawn itself \
             -- and every Unavailable drain silently stops retrying while the \
             RecordingCtx tests stay green"
        );
    }

    /// The production `V2DrainCtx` impl must DELEGATE, not reimplement.
    ///
    /// The nine behavioural tests drive `RecordingCtx`, which replaces this
    /// impl, so none of them can see it. The seam review found exactly that:
    /// empty `schedule_retry`'s body to `{}` and every `Unavailable` drain
    /// stops retrying with the whole suite green.
    ///
    /// So each method is reduced to one delegation, and this asserts those
    /// delegations exist AS REAL CODE — through the comment-stripping scanner,
    /// because a `//`-only filter is what let four earlier pins in this
    /// workstream pass against commented-out calls.
    ///
    /// This is a source scrape and is therefore the weaker half. It is here
    /// because the alternative -- constructing a `P2pConnManager` and a
    /// populated ring so `fan_out` is observable -- is a bigger piece of work
    /// than this change. See the impl's own rustdoc for that limit.
    #[test]
    fn production_drain_ctx_delegates_rather_than_reimplementing() {
        const SOURCE: &str = include_str!("v2_drain.rs");
        let start = SOURCE
            .find("impl V2DrainCtx for P2pConnManager {")
            .expect("the production V2DrainCtx impl is gone — re-anchor this pin");
        let rest = &SOURCE[start..];
        let end = rest.find("\n#[cfg(test)]").expect(
            "the test module no longer follows the impl — this pin bounds its scan \
                     on it, and unbounded it would read its own assertion text and pass \
                     vacuously. Re-anchor it.",
        );
        let body = strip_comments(&rest[..end]);

        for (needle, why) in [
            (
                "&mut self.v2_drain_retries",
                "the retry map must be the manager's own field; a fresh map per call makes \
                 every drain look like a first attempt and the retry never bounds",
            ),
            (
                "self.handle_broadcast_state_change(op_manager, key, new_state, false, false)",
                "fan_out must hand the read state to the real fan-out with the same five \
                 arguments the pre-extraction arm used; this is the leg whose absence is \
                 #5479",
            ),
            (
                "spawn_v2_drain_retry(op_manager, key, delay)",
                "schedule_retry must actually spawn the retry. An empty body here stops \
                 every Unavailable drain retrying, and no RecordingCtx test can see it \
                 because the double replaces this impl",
            ),
        ] {
            assert!(
                body.contains(needle),
                "the production V2DrainCtx impl must contain `{needle}` as real code \
                 (not commented out, not in a string): {why}"
            );
        }
    }

    /// The marker clear must precede the state read, in source order too.
    ///
    /// `drain_clears_the_coalescing_marker_before_reading_state` above asserts
    /// this BEHAVIOURALLY and is the real guard. This is kept as a second,
    /// cheaper lens that also fails if the clear is deleted outright — and,
    /// unlike its predecessor, it cannot be satisfied by a comment.
    #[test]
    fn marker_clear_precedes_the_read_in_source_order() {
        let body = drain_fn_body();
        let clear = body
            .find("clear_v2_delegate_broadcast_pending(")
            .expect("the drain no longer clears the coalescing marker");
        let read = body
            .find("read_state_for_broadcast_drain(")
            .expect("the drain no longer reads state — re-anchor this pin");
        assert!(
            clear < read,
            "the marker must be cleared BEFORE the read. Clearing afterwards means a write \
             landing during the read coalesces into a drain that has already read past it, \
             and that write is never announced."
        );
    }

    fn test_key(seed: u8) -> ContractKey {
        ContractKey::from_params_and_code(
            Parameters::from(vec![seed]),
            ContractCode::from(vec![seed, seed, seed]),
        )
    }

    /// Build a real `OpManager` plus a stub contract handler that answers every
    /// `GetQuery` with `reply`.
    ///
    /// The returned `Mutex<Option<bool>>` records whether the coalescing marker
    /// was still set at the moment the handler served the read — which is how
    /// `drain_clears_the_coalescing_marker_before_reading_state` checks an
    /// ORDERING that the source-order pin can only scrape.
    ///
    /// It is THREE-valued on purpose. `None` means the read never reached the
    /// handler at all, and that case must be distinguishable from "the read
    /// happened and the marker was clear". With a plain `AtomicBool` starting
    /// `false`, a drain that stopped reading entirely would leave the probe
    /// `false` and satisfy an assertion of "the marker was clear at the read" —
    /// passing because the event it describes never occurred. A guard asserting
    /// the ABSENCE of something must also assert that the thing EXISTS to be
    /// absent from.
    async fn harness(
        id: &str,
        reply: StubReply,
    ) -> (
        Arc<OpManager>,
        EventLoopNotificationsReceiver,
        Arc<Mutex<Option<bool>>>,
        Box<dyn std::any::Any>,
    ) {
        let config_args = ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");

        let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, mut ch_channel, wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();

        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);

        let marker_at_read: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
        let marker_probe = marker_at_read.clone();
        let op_for_stub = op_manager.clone();

        // The stub handler. Answers GetQuery per `reply`, and snapshots the
        // coalescing marker as it does so.
        tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                if let ContractHandlerEvent::GetQuery { instance_id, .. } = ev {
                    *marker_probe.lock().unwrap() = Some(
                        op_for_stub
                            .v2_delegate_broadcast_pending
                            .contains(&instance_id),
                    );
                    let response = match reply {
                        StubReply::Found(bytes) => Ok(StoreResponse {
                            state: Some(WrappedState::new(bytes.to_vec())),
                            contract: None,
                        }),
                        StubReply::NoState => Ok(StoreResponse {
                            state: None,
                            contract: None,
                        }),
                        StubReply::ExecutorError => Err(crate::contract::ExecutorError::other(
                            anyhow::anyhow!("stub executor failure"),
                        )),
                    };
                    // The drain may already have timed out and stopped waiting; a
                    // failed reply is expected in that case and is not the stub's problem.
                    let _reply_sent = ch_channel
                        .send_to_sender(
                            id,
                            ContractHandlerEvent::GetResponse {
                                key: None,
                                response,
                            },
                        )
                        .await;
                }
            }
        });

        let guards: Box<dyn std::any::Any> =
            Box::new((wait_for_event, result_router_rx, task_monitor));
        (op_manager, notification_rx, marker_at_read, guards)
    }

    /// `Found` must fan the state out. KILLS MUTATION S1.
    ///
    /// S1 deleted `ctx.handle_broadcast_state_change(...)` from the dispatch
    /// arm and the entire suite stayed green — #5479 reintroduced in full, with
    /// no detector anywhere in `--lib`. This is that detector.
    #[tokio::test(flavor = "current_thread")]
    async fn found_fans_out_the_state_that_was_read() {
        let (op_manager, _rx, _probe, _guards) =
            harness("v2drain-found", StubReply::Found(&[7, 7, 7])).await;
        let key = test_key(31);
        let mut ctx = RecordingCtx::default();

        handle_v2_delegate_state_changed(&mut ctx, &op_manager, key, 100).await;

        assert_eq!(
            ctx.fanned_out,
            vec![(key, vec![7, 7, 7])],
            "a drain that reads state MUST fan it out; without this the write is \
             committed locally, reported successful to the delegate, and never seen \
             off this node — which is #5479 itself"
        );
        assert!(
            ctx.retries_scheduled.is_empty(),
            "a successful drain must not also schedule a retry"
        );
        assert!(
            !ctx.retries.contains_key(&key),
            "a successful drain is terminal and must drop its retry-map entry"
        );
    }

    /// `NotHeld` is final: nothing sent, nothing retried. KILLS MUTATION S7.
    ///
    /// S7 swapped the `NotHeld` and `Unavailable` arms. `classify_drain_read`
    /// has a good exhaustive test of the MAPPING; nothing tested the CONSUMER
    /// of that mapping, so the swap survived. Treating `NotHeld` as retryable
    /// spins forever against a contract we will never hold.
    #[tokio::test(flavor = "current_thread")]
    async fn not_held_sends_nothing_and_does_not_retry() {
        let (op_manager, _rx, _probe, _guards) =
            harness("v2drain-notheld", StubReply::NoState).await;
        let key = test_key(32);
        let mut ctx = RecordingCtx::default();
        ctx.retries.insert(key, 1);

        handle_v2_delegate_state_changed(&mut ctx, &op_manager, key, 100).await;

        assert!(
            ctx.fanned_out.is_empty(),
            "we hold no state for this contract; there is nothing to broadcast"
        );
        assert!(
            ctx.retries_scheduled.is_empty(),
            "NotHeld is DEFINITIVE — retrying cannot change it, and a retry here is an \
             unbounded spin against a contract this node will never hold"
        );
        assert!(
            !ctx.retries.contains_key(&key),
            "NotHeld is terminal and must drop its retry-map entry; the map has no size \
             cap, so a terminal arm that forgets leaks one entry per contract"
        );
    }

    /// `Unavailable` retries, and the attempt counter advances.
    /// KILLS MUTATIONS S7 AND S8.
    ///
    /// S8 deleted `*attempt += 1`, making the retry loop unbounded against a
    /// contract handler that was already too busy to answer — the retry then
    /// amplifies the congestion it exists to recover from. The counter is only
    /// observable across successive drains, which is why one call is not enough.
    #[tokio::test(flavor = "current_thread")]
    async fn unavailable_schedules_a_retry_and_advances_the_attempt_counter() {
        let (op_manager, _rx, _probe, _guards) =
            harness("v2drain-unavail", StubReply::ExecutorError).await;
        let key = test_key(33);
        let mut ctx = RecordingCtx::default();

        for expected_attempts in 1..=P2pConnManager::MAX_V2_DRAIN_RETRIES {
            handle_v2_delegate_state_changed(&mut ctx, &op_manager, key, 100).await;
            assert_eq!(
                ctx.retries.get(&key).copied(),
                Some(expected_attempts),
                "each Unavailable drain must ADVANCE the attempt counter. If this stays \
                 at 1 the counter is not incrementing and the retry is unbounded"
            );
        }
        assert_eq!(
            ctx.retries_scheduled.len() as u8,
            P2pConnManager::MAX_V2_DRAIN_RETRIES,
            "every attempt below the cap must schedule exactly one retry"
        );
        assert!(
            ctx.fanned_out.is_empty(),
            "a failed read has nothing to fan out — broadcasting here would put \
             whatever was last in hand on the wire"
        );

        // One more: the cap is reached, so this must GIVE UP rather than retry.
        handle_v2_delegate_state_changed(&mut ctx, &op_manager, key, 100).await;
        assert_eq!(
            ctx.retries_scheduled.len() as u8,
            P2pConnManager::MAX_V2_DRAIN_RETRIES,
            "at the cap the drain must stop retrying"
        );
        assert!(
            !ctx.retries.contains_key(&key),
            "giving up is terminal and must drop its retry-map entry"
        );
    }

    /// The retry body must re-queue through the marker-respecting API.
    /// KILLS MUTATION S10.
    ///
    /// S10 made the spawned retry sleep and then do nothing. Retries fired,
    /// cost a task spawn each, and never re-queued — and the suite stayed
    /// green because the body was an anonymous closure inside a `tokio::spawn`
    /// inside a `match` arm, addressable by nothing.
    #[tokio::test(flavor = "current_thread")]
    async fn retry_body_requeues_through_the_marker_respecting_api() {
        let (op_manager, mut rx, _probe, _guards) =
            harness("v2drain-retrybody", StubReply::NoState).await;
        let key = test_key(34);

        run_v2_drain_retry(
            op_manager.clone(),
            key,
            Duration::ZERO,
            tokio_util::sync::CancellationToken::new(),
        )
        .await;

        let mut queued = Vec::new();
        while let Ok(event) = rx.notifications_receiver.try_recv() {
            if let either::Either::Right(NodeEvent::V2DelegateStateChanged { key }) = event {
                queued.push(key);
            }
        }
        assert_eq!(
            queued,
            vec![key],
            "the retry must actually re-queue the drain. If this is empty the retry is a \
             sleep that does nothing, and the committed write is never announced"
        );
    }

    /// A cancelled shutdown token must abandon the retry.
    ///
    /// The `select!` is why the sleep is interruptible at all: a >=1s plain
    /// sleep in a retry loop keeps the task alive past shutdown.
    #[tokio::test(flavor = "current_thread")]
    async fn retry_body_abandons_on_shutdown() {
        let (op_manager, mut rx, _probe, _guards) =
            harness("v2drain-retrycancel", StubReply::NoState).await;
        let key = test_key(35);
        let shutdown = tokio_util::sync::CancellationToken::new();
        shutdown.cancel();

        run_v2_drain_retry(op_manager.clone(), key, Duration::from_secs(3600), shutdown).await;

        let mut queued = 0;
        while let Ok(event) = rx.notifications_receiver.try_recv() {
            if let either::Either::Right(NodeEvent::V2DelegateStateChanged { .. }) = event {
                queued += 1;
            }
        }
        assert_eq!(
            queued, 0,
            "a cancelled retry must not re-queue; it must also not have waited out the \
             3600s delay, which is what this test finishing at all demonstrates"
        );

        // THE POSITIVE HALF. Everything above is an assertion of absence, and an
        // absence is satisfied by a harness that could never have observed the
        // thing: a broken notification channel, a receiver already drained, a
        // key that never matches. So prove the observation was POSSIBLE by
        // running the same call with a live token and watching it queue.
        run_v2_drain_retry(
            op_manager.clone(),
            key,
            Duration::ZERO,
            tokio_util::sync::CancellationToken::new(),
        )
        .await;
        let mut queued_after = 0;
        while let Ok(event) = rx.notifications_receiver.try_recv() {
            if let either::Either::Right(NodeEvent::V2DelegateStateChanged { .. }) = event {
                queued_after += 1;
            }
        }
        assert_eq!(
            queued_after, 1,
            "an UNCANCELLED retry through the same harness must queue exactly one event. \
             If this is 0 the harness cannot observe a re-queue at all, and the \
             assertion above passed because nothing could ever have been seen — not \
             because cancellation worked"
        );
    }

    /// The marker is cleared BEFORE the read — asserted behaviourally.
    ///
    /// `v2_drain_clears_marker_before_reading_state` scrapes source order for
    /// this, and a reviewer defeated it with a block comment: the needle stayed
    /// present, the pin stayed green, and the marker latched permanently. This
    /// checks the actual ordering instead, by having the stub handler snapshot
    /// the marker at the instant it serves the read.
    ///
    /// Why the order matters: between the read and the fan-out a delegate may
    /// write again. If the marker were still set, that write would coalesce
    /// into THIS drain — which has already read and cannot see it — and its
    /// fan-out would never happen.
    #[tokio::test(flavor = "current_thread")]
    async fn drain_clears_the_coalescing_marker_before_reading_state() {
        let (op_manager, _rx, marker_at_read, _guards) =
            harness("v2drain-order", StubReply::Found(&[1])).await;
        let key = test_key(36);

        // Put the marker in the state a queued broadcast leaves it in.
        op_manager.v2_delegate_broadcast_pending.insert(*key.id());
        assert!(op_manager.v2_delegate_broadcast_pending.contains(key.id()));

        let mut ctx = RecordingCtx::default();
        handle_v2_delegate_state_changed(&mut ctx, &op_manager, key, 100).await;

        // Some(false) = the read WAS served, and the marker was already clear.
        // None would mean the read never happened, in which case this test proves
        // nothing about ordering — the positive half of the assertion.
        assert_eq!(
            *marker_at_read.lock().unwrap(),
            Some(false),
            "expected the read to be SERVED (not None) and the coalescing marker to be \
             already CLEAR when it was. None means the drain never issued the read, so \
             the ordering this test exists to check did not occur and a bare \
             'marker was not set' assertion would have passed vacuously. Some(true) \
             means the marker was still set at the read: a write landing during the read \
             then coalesces into a drain that has already read past it, and that write is \
             never announced (#5479)"
        );
        assert!(
            !op_manager.v2_delegate_broadcast_pending.contains(key.id()),
            "and it must stay clear through the fan-out"
        );
    }
}
