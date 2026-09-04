//! Parking a delegate's round-trip OFF the serial `contract_handling` loop
//! (#5544).
//!
//! # The problem
//!
//! A delegate round-trip is already a two-invocation protocol at the runtime
//! level: `RequestUserInput` breaks out of `process_outbound`, the WASM
//! `process()` call returns, and the delegate's continuation lives in the
//! [`DelegateContextCache`](crate::wasm_runtime::native_api::DelegateContextCache)
//! until the executor re-enters with the matching `UserResponse`.
//!
//! What made that round-trip look atomic was that
//! `handle_delegate_with_contract_requests` awaited the slow half INLINE, on
//! the single serial loop — so nothing else on the node ran until it finished.
//! For a permission prompt that is up to `USER_INPUT_TIMEOUT` (60 s) during
//! which no GET, PUT, UPDATE, subscribe or delegate notification is serviced
//! anywhere on this node.
//!
//! Parking replaces that: the loop hands the slow half to a spawned task and
//! returns immediately, and the delegate is re-entered on a later iteration
//! when the result arrives.
//!
//! # Why this needs per-delegate exclusion
//!
//! The context cache is keyed by `DelegateKey` alone and is last-write-wins.
//! It is only sound while at most ONE `process()` per delegate is in flight,
//! and — see that type's rustdoc, corrected in this same change — that
//! property is supplied ENTIRELY by the serial loop, not by the runtime. There
//! is no per-delegate lock in `prepare_delegate_call`, no per-delegate
//! affinity in `RuntimePool::execute_delegate_request`, and no mutex anywhere
//! in `wasm_runtime::delegate`.
//!
//! So the moment a round-trip spans two loop iterations, that protection is
//! gone: a second request for the same delegate would run `process()`, write
//! the shared context, and the parked continuation would resume reading
//! someone else's bytes. Silent state corruption, not a crash.
//!
//! [`DelegateParkCtx`] therefore keeps its own per-delegate exclusion: while a
//! delegate is parked, further requests for it are QUEUED rather than run, and
//! drained when it resumes. That preserves the invariant the delegate author
//! already relies on, and converts a node-wide stall into a per-delegate one —
//! which is the correct semantics, not a compromise. Everything else on the
//! node keeps running.
//!
//! # Ownership
//!
//! Loop-owned (`contract_handling` holds it and passes `&mut` down), NOT a
//! process global. It holds this node's client responders and gates this
//! node's loop, and an in-process multi-node simulation must not share either.
//! Same reasoning as `client_events::user_op_rate_limit`, and deliberately
//! unlike the older `DELEGATE_SUBSCRIPTIONS` global.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use freenet_stdlib::client_api::DelegateRequest;
use either::Either;
use freenet_stdlib::prelude::{
    ContractContainer, ContractInstanceId, ContractKey, DelegateContext, DelegateKey,
    InboundDelegateMsg, OutboundDelegateMsg, Parameters, RelatedContracts, StateDelta,
    WrappedState,
};

use super::executor::ExecutorError;

use super::handler::{EventId, StashedResponder};
use crate::client_events::ConnectionScope;
use crate::wasm_runtime::UserSecretContext;

/// Node-wide cap on simultaneously parked delegates.
///
/// Each park holds a continuation, at most one stashed client responder and up
/// to [`MAX_PENDING_PER_DELEGATE`] queued requests, so this bounds the whole
/// structure's footprint. 64 is far above any realistic concurrent count — a
/// node runs a handful of registered delegates and a prompt needs a human — and
/// well below anything that would matter for memory.
///
/// At the cap a new park is REFUSED and the caller falls back to answering the
/// delegate inline (the pre-#5544 behaviour, stall included) rather than
/// dropping the round-trip. Degrading to the old behaviour under an
/// implausible flood is strictly better than losing a user's prompt.
pub(super) const MAX_PARKED_DELEGATES: usize = 64;

/// Cap on requests queued behind a single parked delegate.
///
/// Overflow is REJECTED with a visible error, never silently dropped: the
/// caller gets `ContractHandlerEvent::DelegateResponse` carrying nothing, which
/// surfaces to the client as an empty response rather than a hang. A delegate
/// with 8 requests already queued behind a prompt is not going to be helped by
/// a ninth.
pub(super) const MAX_PENDING_PER_DELEGATE: usize = 8;

/// Backstop lifetime for a park.
///
/// The [`ParkGuard`] already guarantees exactly-one resume per park even if
/// the spawned task is dropped, panics or is cancelled, and both parkable
/// waits are internally bounded (`USER_INPUT_TIMEOUT` = 60 s for a prompt,
/// `DEFERRED_RELATED_FETCH_TIMEOUT` = `OPERATION_TTL` + 2 s for a related
/// fetch). This TTL is the third layer: it covers a task that neither
/// completes nor drops, which no current path can produce but which a future
/// one could. On expiry the park is force-resumed — pending drained, responder
/// answered — so a wedged delegate can never be wedged forever.
///
/// Set above BOTH inner budgets so the real timeout always wins and this never
/// fires first on a merely-slow operation; a park cut short at its own TTL
/// would report a spurious failure for work that was about to succeed.
pub(super) const PARK_TTL: Duration = Duration::from_secs(90);

/// Cap on the off-loop task's own runtime, kept BELOW [`PARK_TTL`].
///
/// The task runs this iteration's prompts and related-contract fetches
/// concurrently, but several prompts (each up to `USER_INPUT_TIMEOUT`) could
/// still sum past the TTL. If that happened the loop's backstop sweep would
/// force-resume the park while the task was still working, and the task's own
/// result would then arrive for a park that no longer exists and be discarded.
/// Bounding the task below the TTL means the guard always wins that race, so
/// the TTL stays what it is meant to be — unreachable in practice.
pub(super) const PARK_WORK_BUDGET: Duration = Duration::from_secs(75);

/// A delegate request that arrived while its delegate was parked.
///
/// Held here rather than requeued into the fair queue: every delegate request
/// shares the single `QueueKey::Default` lane (`fair_queue.rs`), so a
/// pop-see-parked-repush cycle would busy-spin the loop.
pub(super) struct PendingRequest {
    pub id: EventId,
    pub req: DelegateRequest<'static>,
    pub origin_contract: Option<ContractInstanceId>,
    pub connection_scope: ConnectionScope,
    pub user_context: Option<UserSecretContext>,
}

/// Where a resumed run's residual `ApplicationMessage`s must go.
///
/// The two entry points differ: a client-driven run answers the parked client
/// responder (so the client sees ONE response covering the whole round-trip,
/// exactly as it does today when the loop blocks), while a
/// notification-driven run has no client and fans out to the apps registered
/// with the delegate — the same route `handle_delegate_notification` already
/// uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Delivery {
    Client,
    Apps,
}

/// Everything needed to re-enter a parked delegate on a later loop iteration.
///
/// `params` is carried explicitly and is NOT optional: `DelegateKey` identity
/// covers `BLAKE3(code_hash ‖ params)` and the params are threaded into the
/// WASM env, so resuming with empty params (as the notification path does, a
/// known v1 limitation) would run a different delegate instance.
pub(super) struct Continuation {
    pub params: Parameters<'static>,
    pub origin_contract: Option<ContractInstanceId>,
    pub connection_scope: ConnectionScope,
    pub user_context: Option<UserSecretContext>,
    pub inter_delegate: super::InterDelegateDispatch,
    /// Outbound messages the delegate produced before it parked. Carried so the
    /// client sees ONE response covering the whole round-trip rather than a
    /// partial one now and the rest out-of-band.
    pub accumulated: Vec<OutboundDelegateMsg>,
    /// Responses already computed for this iteration, awaiting the parked one.
    pub inbound_so_far: Vec<InboundDelegateMsg<'static>>,
    /// The parked client's responder, if this run descends from a client
    /// request. Attached by the caller immediately after parking (it owns the
    /// channel the responder is taken from), and re-attached by the resume
    /// handler if the resumed run parks again — a delegate that prompts twice
    /// in a row must not strand its client.
    pub responder: Option<StashedResponder>,
    pub delivery: Delivery,
}

/// One parked delegate.
struct ParkEntry {
    continuation: Continuation,
    parked_at: tokio::time::Instant,
    pending: VecDeque<PendingRequest>,
}

/// Why a park ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResumeCause {
    /// The awaited work delivered a result.
    Completed,
    /// The park outlived [`PARK_TTL`] (see its rustdoc — a backstop, not an
    /// expected path).
    TimedOut,
}

/// A delegate PUT/UPDATE that could not complete because the contract asked for
/// related contracts this node does not hold.
///
/// The fetch is off-loaded (it is a network GET, and awaiting it on the loop is
/// the second #5544 stall); the upsert itself must be RE-RUN on the loop,
/// because it runs WASM and WASM stays serial. So the park carries everything
/// needed to re-run it.
pub(super) struct PendingUpsert {
    pub key: ContractKey,
    pub update: Either<WrappedState, StateDelta<'static>>,
    pub related_contracts: RelatedContracts<'static>,
    pub code: Option<ContractContainer>,
    /// `true` builds a `PutContractResponse`, `false` an
    /// `UpdateContractResponse`.
    pub is_put: bool,
    /// Echoed back to the delegate so it can match the response to its request.
    pub context: DelegateContext,
    /// The related contracts to fetch off-loop.
    pub missing: Vec<ContractInstanceId>,
}

/// A [`PendingUpsert`] whose off-loop fetch has finished, one way or the other.
pub(super) struct ResolvedUpsert {
    pub pending: PendingUpsert,
    pub fetched: Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>,
}

/// Sent from an off-loop task back to the `contract_handling` loop.
pub(super) struct DelegateResume {
    pub delegate_key: DelegateKey,
    pub cause: ResumeCause,
    /// Messages that are ready to feed straight back into the delegate (the
    /// prompt path). Empty on a dropped or timed-out park, which still resumes
    /// so the continuation terminates.
    pub inbound: Vec<InboundDelegateMsg<'static>>,
    /// Upserts whose related contracts were fetched off-loop and which must be
    /// RE-RUN on the loop before their responses can be built.
    pub upserts: Vec<ResolvedUpsert>,
}

/// RAII guard guaranteeing an off-loop task delivers EXACTLY ONE
/// [`DelegateResume`] for its park — on success, or on drop / panic /
/// cancellation before it got there.
///
/// Same load-bearing invariant as #4391's `ResumeGuard`: because every park is
/// answered exactly once, the loop needs no stale-resume guard, a parked client
/// responder can never be stranded, and a delegate's pending queue is always
/// drained. Never zero (Drop covers early exit), never twice (the success path
/// takes the payload, so Drop sees `None`).
///
/// The resume channel is unbounded, so both sends are non-blocking. Producers
/// are bounded by [`MAX_PARKED_DELEGATES`], the receiver is the loop (which
/// drains every iteration), and the task never reads what the loop produces —
/// no cycle, per `channel-safety.md`'s carve-out.
pub(super) struct ParkGuard {
    payload: Option<ParkGuardPayload>,
}

struct ParkGuardPayload {
    resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
    delegate_key: DelegateKey,
}

impl ParkGuard {
    pub(super) fn new(
        resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
        delegate_key: DelegateKey,
    ) -> Self {
        Self {
            payload: Some(ParkGuardPayload {
                resume_tx,
                delegate_key,
            }),
        }
    }

    /// Deliver the resolved work. Consumes the payload so a later `Drop` is a
    /// no-op (exactly-once).
    pub(super) fn send(
        mut self,
        inbound: Vec<InboundDelegateMsg<'static>>,
        upserts: Vec<ResolvedUpsert>,
    ) {
        if let Some(p) = self.payload.take() {
            Self::deliver(p, ResumeCause::Completed, inbound, upserts);
        }
    }

    fn deliver(
        p: ParkGuardPayload,
        cause: ResumeCause,
        inbound: Vec<InboundDelegateMsg<'static>>,
        upserts: Vec<ResolvedUpsert>,
    ) {
        let ParkGuardPayload {
            resume_tx,
            delegate_key,
        } = p;
        if resume_tx
            .send(DelegateResume {
                delegate_key: delegate_key.clone(),
                cause,
                inbound,
                upserts,
            })
            .is_err()
        {
            tracing::debug!(
                delegate = %delegate_key,
                "Delegate resume channel closed; contract-handling loop gone"
            );
        }
    }
}

impl Drop for ParkGuard {
    fn drop(&mut self) {
        if let Some(p) = self.payload.take() {
            tracing::warn!(
                delegate = %p.delegate_key,
                "Off-loop delegate task dropped before sending — delivering an \
                 empty resume so the park terminates, its pending queue drains \
                 and the parked client is answered exactly once (#5544)"
            );
            Self::deliver(p, ResumeCause::TimedOut, Vec::new(), Vec::new());
        }
    }
}

/// Outcome of asking to park a delegate.
pub(super) enum ParkAdmission {
    /// Parked. The caller must spawn the off-loop work with a [`ParkGuard`].
    Admitted,
    /// Refused (node-wide cap). The caller keeps the old inline behaviour;
    /// the continuation is handed back so nothing is lost.
    Refused(Box<Continuation>),
}

/// Outcome of offering a request for an already-parked delegate.
pub(super) enum QueueOutcome {
    /// Queued behind the park; it will run when the delegate resumes.
    Queued,
    /// The delegate's queue is full. The caller must answer this request with a
    /// visible error rather than dropping it.
    Rejected(Box<PendingRequest>),
}

/// Loop-owned per-delegate park state.
pub(super) struct DelegateParkCtx {
    parked: HashMap<DelegateKey, ParkEntry>,
    /// Handed to each [`ParkGuard`] so an off-loop task can deliver its resume.
    /// Kept here rather than threaded separately so every call site needs only
    /// a single `&mut DelegateParkCtx`.
    resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
}

impl DelegateParkCtx {
    pub(super) fn new(resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>) -> Self {
        Self {
            parked: HashMap::new(),
            resume_tx,
        }
    }

    pub(super) fn resume_tx(&self) -> &tokio::sync::mpsc::UnboundedSender<DelegateResume> {
        &self.resume_tx
    }

    /// `true` if this delegate currently has a parked continuation, and so must
    /// not be re-entered by a fresh request.
    pub(super) fn is_parked(&self, key: &DelegateKey) -> bool {
        self.parked.contains_key(key)
    }

    #[cfg(test)]
    pub(super) fn parked_count(&self) -> usize {
        self.parked.len()
    }

    /// Park `key`, or refuse at the node-wide cap.
    ///
    /// A delegate that is already parked cannot park again — the exclusion
    /// guarantees only one round-trip per delegate is ever in flight, so this
    /// is unreachable by construction and is treated as a refusal rather than
    /// silently clobbering the live continuation.
    pub(super) fn park(
        &mut self,
        key: DelegateKey,
        continuation: Continuation,
    ) -> ParkAdmission {
        if self.parked.len() >= MAX_PARKED_DELEGATES || self.parked.contains_key(&key) {
            tracing::warn!(
                delegate = %key,
                parked = self.parked.len(),
                limit = MAX_PARKED_DELEGATES,
                already_parked = self.parked.contains_key(&key),
                "Refusing to park delegate; falling back to the inline path"
            );
            return ParkAdmission::Refused(Box::new(continuation));
        }
        self.parked.insert(
            key,
            ParkEntry {
                continuation,
                parked_at: tokio::time::Instant::now(),
                pending: VecDeque::new(),
            },
        );
        ParkAdmission::Admitted
    }

    /// Queue a request that arrived for a parked delegate.
    ///
    /// Caller must have checked [`is_parked`](Self::is_parked); queueing for an
    /// unparked delegate is a caller bug and is reported back as a rejection so
    /// the request is still answered.
    pub(super) fn queue_pending(&mut self, key: &DelegateKey, req: PendingRequest) -> QueueOutcome {
        let Some(entry) = self.parked.get_mut(key) else {
            return QueueOutcome::Rejected(Box::new(req));
        };
        if entry.pending.len() >= MAX_PENDING_PER_DELEGATE {
            tracing::warn!(
                delegate = %key,
                queued = entry.pending.len(),
                limit = MAX_PENDING_PER_DELEGATE,
                "Delegate pending queue full while parked — rejecting request"
            );
            return QueueOutcome::Rejected(Box::new(req));
        }
        entry.pending.push_back(req);
        QueueOutcome::Queued
    }

    /// Hand the parked client's responder to a live park.
    ///
    /// Separate from [`park`](Self::park) because the responder is taken from
    /// the contract-handler channel, which the caller owns and this registry
    /// deliberately knows nothing about. A `None` responder (client already
    /// gone) is stored as-is: the park still has to terminate.
    pub(super) fn attach_responder(
        &mut self,
        key: &DelegateKey,
        responder: Option<StashedResponder>,
    ) {
        match self.parked.get_mut(key) {
            Some(entry) => entry.continuation.responder = responder,
            None => {
                // Unreachable: the caller attaches immediately after a
                // successful park, on the same loop iteration, and nothing
                // else can end a park in between. Log rather than panic — a
                // dropped response is recoverable, a panicked loop is not.
                tracing::error!(
                    delegate = %key,
                    "attach_responder for a delegate that is not parked; the \
                     client for this run will not be answered"
                );
            }
        }
    }

    /// End a park, returning its continuation and everything queued behind it.
    pub(super) fn take(
        &mut self,
        key: &DelegateKey,
    ) -> Option<(Continuation, VecDeque<PendingRequest>)> {
        self.parked
            .remove(key)
            .map(|entry| (entry.continuation, entry.pending))
    }

    /// Keys whose park has outlived [`PARK_TTL`].
    ///
    /// Returned rather than acted on so the caller (which owns the executor and
    /// the channel) performs the force-resume; this keeps the registry a pure
    /// data structure and unit-testable without a loop.
    pub(super) fn expired(&self, now: tokio::time::Instant) -> Vec<DelegateKey> {
        self.parked
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.parked_at) >= PARK_TTL)
            .map(|(key, _)| key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> DelegateKey {
        DelegateKey::new(
            [byte; 32],
            freenet_stdlib::prelude::CodeHash::new([byte; 32]),
        )
    }

    fn continuation() -> Continuation {
        Continuation {
            params: Parameters::from(Vec::new()),
            origin_contract: None,
            connection_scope: ConnectionScope::Local,
            user_context: None,
            inter_delegate: super::super::InterDelegateDispatch::Allowed,
            accumulated: Vec::new(),
            inbound_so_far: Vec::new(),
            responder: None,
            delivery: Delivery::Client,
        }
    }

    fn pending(byte: u8) -> PendingRequest {
        PendingRequest {
            id: EventId { id: byte as u64 },
            req: DelegateRequest::ApplicationMessages {
                key: key(byte),
                params: Parameters::from(Vec::new()),
                inbound: Vec::new(),
            },
            origin_contract: None,
            connection_scope: ConnectionScope::Local,
            user_context: None,
        }
    }

    fn ctx() -> (
        DelegateParkCtx,
        tokio::sync::mpsc::UnboundedReceiver<DelegateResume>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (DelegateParkCtx::new(tx), rx)
    }

    #[tokio::test]
    async fn park_then_take_round_trips() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        assert!(!ctx.is_parked(&k));
        assert!(matches!(
            ctx.park(k.clone(), continuation()),
            ParkAdmission::Admitted
        ));
        assert!(ctx.is_parked(&k));
        let (_cont, pend) = ctx.take(&k).expect("park must be takeable");
        assert!(pend.is_empty());
        assert!(!ctx.is_parked(&k), "take must end the park");
    }

    #[tokio::test]
    async fn node_wide_cap_refuses_and_hands_the_continuation_back() {
        let (mut ctx, _rx) = ctx();
        for i in 0..MAX_PARKED_DELEGATES {
            assert!(matches!(
                ctx.park(key(i as u8), continuation()),
                ParkAdmission::Admitted
            ));
        }
        assert_eq!(ctx.parked_count(), MAX_PARKED_DELEGATES);
        // Over the cap: refused, and the continuation comes back so the caller
        // can fall back inline rather than losing the round-trip.
        assert!(matches!(
            ctx.park(key(200), continuation()),
            ParkAdmission::Refused(_)
        ));
        assert!(!ctx.is_parked(&key(200)));
    }

    #[tokio::test]
    async fn double_park_is_refused_not_clobbered() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        assert!(matches!(
            ctx.park(k.clone(), continuation()),
            ParkAdmission::Admitted
        ));
        // The live continuation must survive: clobbering it would strand the
        // first round-trip's client responder.
        assert!(matches!(
            ctx.park(k.clone(), continuation()),
            ParkAdmission::Refused(_)
        ));
        assert!(ctx.take(&k).is_some());
    }

    #[tokio::test]
    async fn pending_queue_is_capped_and_overflow_is_returned_not_dropped() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation());
        for i in 0..MAX_PENDING_PER_DELEGATE {
            assert!(matches!(
                ctx.queue_pending(&k, pending(i as u8)),
                QueueOutcome::Queued
            ));
        }
        // Overflow must hand the request BACK so the caller can answer it.
        // Silently dropping it would hang that client forever.
        assert!(matches!(
            ctx.queue_pending(&k, pending(99)),
            QueueOutcome::Rejected(_)
        ));
        let (_cont, pend) = ctx.take(&k).expect("park present");
        assert_eq!(pend.len(), MAX_PENDING_PER_DELEGATE);
    }

    #[tokio::test]
    async fn queueing_for_an_unparked_delegate_returns_the_request() {
        let (mut ctx, _rx) = ctx();
        assert!(matches!(
            ctx.queue_pending(&key(1), pending(1)),
            QueueOutcome::Rejected(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn park_expires_only_after_the_ttl() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation());

        tokio::time::advance(PARK_TTL - Duration::from_secs(1)).await;
        assert!(
            ctx.expired(tokio::time::Instant::now()).is_empty(),
            "must not expire early — a park cut short would report a spurious \
             failure for work that was about to succeed"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(ctx.expired(tokio::time::Instant::now()), vec![k]);
    }

    #[tokio::test]
    async fn guard_delivers_exactly_one_resume_on_success() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let guard = ParkGuard::new(tx, key(1));
        guard.send(Vec::new(), Vec::new());
        let resume = rx.recv().await.expect("one resume");
        assert_eq!(resume.cause, ResumeCause::Completed);
        assert!(rx.try_recv().is_err(), "must not deliver twice");
    }

    #[tokio::test]
    async fn guard_delivers_a_resume_when_dropped_before_sending() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        // Simulates the spawned task being cancelled/panicking before it
        // finished its work. Without this the park would never end: the
        // pending queue would never drain and the client would hang.
        drop(ParkGuard::new(tx, key(1)));
        let resume = rx.recv().await.expect("drop must still resume the park");
        assert_eq!(resume.cause, ResumeCause::TimedOut);
        assert!(resume.inbound.is_empty());
    }
}
