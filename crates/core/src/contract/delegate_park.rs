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
//! # What parking does NOT relax: `process()` stays globally serial
//!
//! Parking releases the loop while a delegate is **suspended**, never while it
//! is **running**. That distinction is load-bearing for code outside this
//! module, so state it as an invariant:
//!
//! > **At most one delegate `process()` executes node-wide at any instant, and
//! > it always executes on the `contract_handling` loop.**
//!
//! Two separate properties depend on it:
//!
//! * `DelegateContextCache` needs one `process()` **per delegate** — that is
//!   the narrower guarantee, and it is the one [`DelegateParkCtx`]'s exclusion
//!   supplies, because parking genuinely does let a delegate's round-trip span
//!   loop iterations.
//! * `native_api::state_content_changed` (V2 delegate writes, #5490) needs one
//!   write **per contract**. Its read-then-write pair is not atomic, and its
//!   racing pair is two DIFFERENT delegates writing the SAME contract — which
//!   per-delegate exclusion permits by construction. It is safe only because of
//!   the global property above, not because of anything in this module.
//!
//! Why the global property still holds after #5544, by construction rather than
//! by convention:
//!
//! 1. `execute_delegate_request` is reached ONLY through
//!    `handle_delegate_with_contract_requests`.
//! 2. Every route into that function is awaited, directly or one hop removed,
//!    from `contract_handling` — a single task per node:
//!
//!    ```text
//!    contract_handling
//!      ├─ handle_contract_event ─────────► dispatch_delegate_request ─┐
//!      ├─ handle_delegate_notification ────────────────────────────────┤
//!      └─ handle_delegate_resume ──┬───────────────────────────────────┤
//!                                  ├─► dispatch_delegate_request ──────┤
//!                                  └─► run_queued_notification ────────┘
//!                                                                      │
//!                          handle_delegate_with_contract_requests ◄─────┘
//!                                      └─► execute_delegate_request
//!    ```
//!
//!    Stated as the PROPERTY — every route is awaited from the one loop — and
//!    not as a count. An exact tally is a fact with an expiry date: this list
//!    said "four call sites" until `run_queued_notification` was added for the
//!    notification-coalescing fix, and was wrong the moment it was. The
//!    property survives a new caller; the number does not. If you add a route,
//!    it must be awaited from this loop or the invariant is gone.
//! 3. The off-loop task this module spawns captures a `ParkGuard`, an
//!    `Arc<P: UserInputPrompter>`, an `Option<Arc<OpManager>>` and plain data.
//!    It does **not** capture the `ContractHandler` or an executor, so it
//!    cannot invoke a delegate even by mistake. Its two jobs — waiting on a
//!    human and driving a sub-op GET — need neither.
//! 4. A resume re-enters the delegate from `handle_delegate_resume`, which runs
//!    **on the loop**. The spawned task only ships a result back down a
//!    channel; it never runs the continuation itself.
//!
//! So the window parking opens is a window in which a *different* delegate may
//! **start**, not one in which two may **run**. #5490's TOCTOU stays
//! unreachable, and its atomic compare-and-write (folding the comparison into
//! the same ReDb write transaction as the store, the way `update_state_sync`
//! already does) is a follow-up rather than a prerequisite for this change.
//!
//! **What would break it.** Spawning any work that holds the
//! `ContractHandler`, or resuming a continuation anywhere other than the loop.
//! If you are about to do either, #5490's gate must become atomic first. The
//! nearest existing precedent is deliberately NOT a counter-example: #4531's
//! hosted-secret export does run off-loop holding a pooled executor, but it
//! enumerates and seals secrets and never invokes a delegate.
//!
//! **The pattern worth noticing.** This is the second documented-but-unenforced
//! invariant found to be resting on the serial loop by accident rather than by
//! design — the context cache was the first. Neither said so where it was
//! relied upon. When touching this loop, assume there is a third.
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

use either::Either;
use freenet_stdlib::client_api::DelegateRequest;
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

/// Cap on DISTINCT contracts with a coalesced notification pending behind one
/// park.
///
/// Notifications coalesce per contract, so this bounds the map by the number of
/// contracts a delegate subscribes to rather than by message rate. In principle
/// #5493 bounds subscriptions separately; this cap does not assume that has
/// landed, because "bounded somewhere else" is the assumption that produced
/// three wrong-scope bounds on this change already. Over the cap the NEW
/// notification is dropped — the delegate will see that contract's next state
/// change, which is the pipeline's standing contract.
///
/// 16 rather than something larger because this lane also sets the worst-case
/// burst when a park is torn down: one resume runs `1 +
/// MAX_PENDING_PER_DELEGATE + MAX_PENDING_NOTIFICATION_CONTRACTS` delegate runs
/// before the fair queue gets a turn (#5544 M6). At 16 that is 25, against a
/// `MAX_RESUME_DRAIN_BATCH` of 16 — one over-long batch at park tear-down,
/// which cannot repeat until another park forms. At 64 it was 73.
pub(super) const MAX_PENDING_NOTIFICATION_CONTRACTS: usize = 16;

/// Cap on deferred related-contract fetches a single park may carry (#5544 S3).
///
/// The client-driven path bounds its off-loop fetches with
/// `MAX_INFLIGHT_DEFERRALS` (256) as explicit anti-amplification. The delegate
/// path cannot consult that counter — it has no `DeferralCtx` — and nothing
/// caps how many `PutContractRequest`s one `process()` may emit, each able to
/// name up to `MAX_RELATED_CONTRACTS_PER_REQUEST` (10) missing contracts.
///
/// 4 is chosen so the node-wide worst case matches the client path rather than
/// exceeding it: MAX_PARKED_DELEGATES (64) x 4 x 10 = 2560 ids in flight,
/// against the client path's 256 x 10 = 2560. Over the cap the excess upserts
/// fall back to the inline fetch, which stalls the loop for those specific
/// operations — the same deliberate trade as the park-cap fallback: degrading
/// to the old behaviour beats dropping a delegate's write.
pub(super) const MAX_DEFERRED_UPSERTS_PER_PARK: usize = 4;

/// Node-wide cap on the bytes a park may hold (#5544 S4).
///
/// `MAX_PARKED_DELEGATES` bounds the NUMBER of parks, which is not the same as
/// bounding their footprint: `Continuation::inbound_so_far` carries
/// `GetContractResponse`s holding full `WrappedState`s, so a count cap reads
/// like a memory bound and is not one. This is the fourth instance of that
/// pattern found on this change alone (see #5551), and `code-style.md` rule 4
/// requires the cap be on the quantity actually consumed.
///
/// 64 MiB: generous beside the 50 MB single-state ceiling the runtime already
/// allows, while bounding the aggregate a flood of parked delegates can pin.
pub(super) const MAX_PARKED_BYTES: usize = 64 * 1024 * 1024;

/// Approximate heap footprint of the payloads a continuation pins.
///
/// Counts the large, contract-controlled parts — inbound states and payloads —
/// and ignores fixed-size bookkeeping. The point is to bound what an attacker
/// can grow, not to be exact.
pub(super) fn continuation_bytes(continuation: &Continuation) -> usize {
    continuation
        .inbound_so_far
        .iter()
        .map(inbound_bytes)
        .sum::<usize>()
        + continuation
            .accumulated
            .iter()
            .map(outbound_bytes)
            .sum::<usize>()
        // `params` is delegate-supplied and retained for the life of the park.
        // Omitting it was one of three ways this "byte bound" failed to bound.
        + continuation.params.as_ref().len()
}

/// Approximate bytes an off-loop task retains for one park: the prompts it is
/// driving and the upserts whose related contracts it is fetching.
///
/// Charged at admission because the task holds these for exactly as long as the
/// park exists, and a single `PendingUpsert` can own a full state plus related
/// contracts plus contract code. `MAX_DEFERRED_UPSERTS_PER_PARK` caps the
/// COUNT of those, which is the same unit mismatch one level down.
pub(super) fn task_bytes(
    prompts: &[freenet_stdlib::prelude::UserInputRequest<'static>],
    upserts: &[PendingUpsert],
) -> usize {
    let prompt_bytes: usize = prompts
        .iter()
        .map(|r| r.message.bytes().len() + r.responses.iter().map(|resp| resp.len()).sum::<usize>())
        .sum();
    let upsert_bytes: usize = upserts
        .iter()
        .map(|u| {
            let update = match &u.update {
                Either::Left(state) => state.as_ref().len(),
                Either::Right(delta) => delta.as_ref().len(),
            };
            let code = u
                .code
                .as_ref()
                .map_or(0, |c| c.data().len() + c.params().as_ref().len());
            let related: usize = u
                .related_contracts
                .clone()
                .into_owned()
                .states()
                .map(|(_, st)| st.as_ref().map_or(0, |s| s.as_ref().len()))
                .sum();
            update + code + related
        })
        .sum();
    prompt_bytes + upsert_bytes
}

/// Approximate bytes a queued delegate request pins.
pub(super) fn request_bytes(req: &DelegateRequest<'static>) -> usize {
    // The registration variants are NOT free: `RegisterDelegate` carries a whole
    // `DelegateContainer`, i.e. the delegate's WASM, and `DelegateRequest::key()`
    // returns that delegate's own key — so a re-registration really does queue
    // behind that delegate's park. An earlier version of this comment asserted
    // the opposite of the type definition and charged them zero, which is 8 per
    // park x 64 parks = 512 delegate modules at a counted cost of nothing.
    match req {
        DelegateRequest::ApplicationMessages {
            inbound, params, ..
        } => inbound.iter().map(inbound_bytes).sum::<usize>() + params.as_ref().len(),
        DelegateRequest::RegisterDelegate { delegate, .. } => delegate_container_bytes(delegate),
        DelegateRequest::RegisterDelegateWithPredecessors {
            delegate,
            predecessors,
            ..
        } => delegate_container_bytes(delegate) + predecessors.len() * 64,
        DelegateRequest::UnregisterDelegate(_) | _ => 0,
    }
}

fn delegate_container_bytes(delegate: &freenet_stdlib::prelude::DelegateContainer) -> usize {
    // `DelegateContainer` exposes the code but not the parameters directly;
    // the code is the large part (the WASM) and is what matters for the bound.
    delegate.code().as_ref().len()
}

fn inbound_bytes(msg: &InboundDelegateMsg<'static>) -> usize {
    match msg {
        InboundDelegateMsg::ApplicationMessage(m) => m.payload.len(),
        InboundDelegateMsg::GetContractResponse(r) => {
            r.state.as_ref().map_or(0, |s| s.as_ref().len())
        }
        InboundDelegateMsg::ContractNotification(n) => n.new_state.as_ref().len(),
        InboundDelegateMsg::UserResponse(r) => r.response.len(),
        InboundDelegateMsg::DelegateMessage(m) => m.payload.len(),
        // Put/Update/Subscribe responses carry only a small Result. The
        // wildcard is required by `#[non_exhaustive]`; a future variant
        // carrying bytes must be added above or it goes uncounted.
        InboundDelegateMsg::PutContractResponse(_)
        | InboundDelegateMsg::UpdateContractResponse(_)
        | InboundDelegateMsg::SubscribeContractResponse(_)
        | _ => 0,
    }
}

fn outbound_bytes(msg: &OutboundDelegateMsg) -> usize {
    // The request variants never appear in `accumulated` — they are consumed by
    // the run that produced them — and the rest carry no contract-controlled
    // payload. Listed rather than wildcarded so a future payload-bearing
    // variant has to be considered here.
    match msg {
        OutboundDelegateMsg::ApplicationMessage(m) => m.payload.len(),
        OutboundDelegateMsg::SendDelegateMessage(m) => m.payload.len(),
        // NOT zero: `DelegateContext::MAX_SIZE` is 409,600 bytes, and these
        // accumulate through park -> resume -> re-park via `RunSeed` for up to
        // MAX_CONTRACT_REQUEST_ITERATIONS, which is ~40 MB per park if
        // uncounted.
        OutboundDelegateMsg::ContextUpdated(ctx) => ctx.as_ref().len(),
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_) => 0,
    }
}

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

/// The budget/TTL ordering above is load-bearing, so it is CHECKED rather than
/// merely described. Tune one of these and the compiler makes you tune the
/// other — prose in two rustdoc blocks is exactly the kind of coupling that
/// rots the first time someone adjusts a timeout in isolation.
const _: () = assert!(
    PARK_WORK_BUDGET.as_secs() < PARK_TTL.as_secs(),
    "PARK_WORK_BUDGET must stay below PARK_TTL: the off-loop task has to \
     finish and deliver its resume before the loop's backstop sweep would \
     force-resume the park, or the task's result is discarded"
);

/// A delegate invocation that arrived while its delegate was parked.
///
/// Held here rather than requeued into the fair queue: every delegate request
/// shares the single `QueueKey::Default` lane (`fair_queue.rs`), so a
/// pop-see-parked-repush cycle would busy-spin the loop.
///
/// Two variants because the two entry points differ in how their result is
/// delivered, and a queued run must resume through the SAME path it would have
/// taken had it not been queued. Collapsing them would route a notification's
/// residual messages to a client responder that does not exist.
pub(super) enum PendingRun {
    /// Client-driven (`ContractHandlerEvent::DelegateRequest`). Answers `id`.
    Client {
        id: EventId,
        req: DelegateRequest<'static>,
        origin_contract: Option<ContractInstanceId>,
        connection_scope: ConnectionScope,
        user_context: Option<UserSecretContext>,
    },
    /// Contract-notification-driven. No client; residual `ApplicationMessage`s
    /// fan out to the apps registered with the delegate.
    ///
    /// Queued rather than dropped, but note the delivered state may be STALE by
    /// the time it drains — up to `PARK_TTL`. That is within the notification
    /// pipeline's documented contract, which is explicitly best-effort and
    /// lossy (`send_delegate_contract_notifications`: "Delegates that require
    /// guaranteed delivery should poll contract state periodically"). Running
    /// it immediately is NOT an option: it would clobber the parked
    /// continuation's context, which is the whole reason for the exclusion.
    ///
    /// COALESCED per contract rather than capped, and not counted against
    /// [`MAX_PENDING_PER_DELEGATE`]. Rejecting the 9th notification would be a
    /// silent loss landing on exactly the wrong population: ghostkeys parks on
    /// prompts, so the rejection window is precisely when a user is
    /// interacting, and Harvest with many address contracts subscribed would
    /// lose payment notifications there. Coalescing is sound because the
    /// notification contract is already "you will see the next state", so a
    /// superseded notification carries nothing its successor does not.
    Notification {
        /// The contract whose change triggered this. Carried explicitly so
        /// queued notifications can be COALESCED per contract.
        contract_id: ContractInstanceId,
        req: DelegateRequest<'static>,
    },
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
    /// Iterations this round-trip has already consumed, so
    /// `MAX_CONTRACT_REQUEST_ITERATIONS` bounds the WHOLE round-trip rather
    /// than each leg of it (#5544 S1).
    ///
    /// Without this the counter is a call-frame local that every park resets,
    /// so a delegate emitting `RequestUserInput` on every re-entry loops
    /// park -> resume -> park forever, holding its exclusion open the whole
    /// time and rejecting every other request for it. Before parking existed
    /// the same delegate stopped after 100 iterations.
    ///
    /// Scope boundary: this covers PARKS, not contract NOTIFICATIONS. A
    /// notification is a genuinely new invocation and resets the count, which
    /// is correct — and is also why #5558 (a delegate notified of its own
    /// writes) is a separate unbounded loop that this does not close.
    pub iterations: usize,
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
    /// Identity of THIS park; see [`DelegateResume::epoch`].
    epoch: u64,
    parked_at: tokio::time::Instant,
    /// Bytes retained by the OFF-LOOP TASK for this park — the prompts and the
    /// deferred upserts it is holding. Not part of the continuation, but
    /// retained for exactly as long, and each `PendingUpsert` can own a full
    /// state plus related contracts and code. Charged so the byte cap bounds
    /// what is actually held rather than only what this struct points at.
    task_bytes: usize,
    /// Client requests, FIFO, capped by [`MAX_PENDING_PER_DELEGATE`]. Rejection
    /// is acceptable here precisely because the caller can be TOLD.
    pending_clients: VecDeque<PendingRun>,
    /// Newest pending notification per contract. Superseded ones are dropped,
    /// which loses nothing the successor does not carry.
    pending_notifications: HashMap<ContractInstanceId, DelegateRequest<'static>>,
    /// Bytes held by the queued items above, so they count toward
    /// [`MAX_PARKED_BYTES`] like the continuation does. Without this the
    /// coalescing map would be a fresh count-bounded-but-not-byte-bounded hole
    /// of exactly the kind #5551 tracks: 64 contracts of 50 MB state is 3.2 GB.
    pending_bytes: usize,
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
    /// Which PARK this resume belongs to (#5544 H1).
    ///
    /// A park is identified by `(key, epoch)`, not by key alone. The TTL
    /// backstop ends a park by force-resuming it WITHOUT consuming the off-loop
    /// task's `ParkGuard`, so that guard still owes a resume. If the delegate
    /// has re-parked by the time it arrives, matching on key alone would hand
    /// the OLD continuation's messages to the NEW park — the cross-round-trip
    /// context corruption this whole mechanism exists to prevent, reached
    /// through the backstop itself. The epoch makes the stale resume
    /// identifiable and droppable.
    pub epoch: u64,
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
    epoch: u64,
}

impl ParkGuard {
    pub(super) fn new(
        resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
        delegate_key: DelegateKey,
        epoch: u64,
    ) -> Self {
        Self {
            payload: Some(ParkGuardPayload {
                resume_tx,
                delegate_key,
                epoch,
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
            epoch,
        } = p;
        if resume_tx
            .send(DelegateResume {
                delegate_key: delegate_key.clone(),
                epoch,
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
    /// Parked. The caller must spawn the off-loop work with a [`ParkGuard`]
    /// carrying this `epoch`, so a stale resume can be told from a live one.
    Admitted { epoch: u64 },
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
    Rejected(Box<PendingRun>),
}

/// Loop-owned per-delegate park state.
pub(super) struct DelegateParkCtx {
    parked: HashMap<DelegateKey, ParkEntry>,
    /// Running total of every retained payload across live parks (#5544 S4):
    /// continuations, off-loop task work, and queued pending runs.
    parked_bytes: usize,
    /// Source of park identities; see [`DelegateResume::epoch`].
    next_epoch: u64,
    /// Refusal counters, per cause (L9). A refusal that is only logged is a
    /// clean zero to anything reading metrics — the same pattern this branch
    /// fixed for the over-cap client request.
    refused: RefusalCounts,
    /// Handed to each [`ParkGuard`] so an off-loop task can deliver its resume.
    /// Kept here rather than threaded separately so every call site needs only
    /// a single `&mut DelegateParkCtx`.
    resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
}

/// Why parked work was turned away, counted rather than only logged (L9).
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RefusalCounts {
    /// Parks refused at the node-wide count or byte cap.
    pub parks: u64,
    /// Client requests refused because the pending queue was full or over budget.
    pub client_requests: u64,
    /// Notifications dropped at the distinct-contract cap or the byte budget.
    pub notifications: u64,
}

impl DelegateParkCtx {
    pub(super) fn new(resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>) -> Self {
        Self {
            parked: HashMap::new(),
            parked_bytes: 0,
            next_epoch: 0,
            refused: RefusalCounts::default(),
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

    /// The live epoch for `key`, for tests that need to end a park they did not
    /// capture the epoch from. Deliberately test-only: production code always
    /// has the epoch from `ParkAdmission::Admitted` or the resume itself, and a
    /// helper that looked one up by key would defeat the identity check.
    /// Snapshot of what has been turned away, by cause.
    ///
    /// The running totals also ride on each refusal's own `warn!`/`info!`, so
    /// production observability does not depend on anything calling this; this
    /// accessor is what lets a test pin that the counting happens at all.
    #[cfg(test)]
    pub(super) fn refusals(&self) -> RefusalCounts {
        self.refused
    }

    #[cfg(test)]
    pub(super) fn epoch_of(&self, key: &DelegateKey) -> Option<u64> {
        self.parked.get(key).map(|e| e.epoch)
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
        // Bytes the off-loop task will retain for this park (see [`task_bytes`]).
        task_bytes: usize,
    ) -> ParkAdmission {
        let bytes = continuation_bytes(&continuation).saturating_add(task_bytes);
        let over_bytes = self.parked_bytes.saturating_add(bytes) > MAX_PARKED_BYTES;
        if self.parked.len() >= MAX_PARKED_DELEGATES || self.parked.contains_key(&key) || over_bytes
        {
            tracing::warn!(
                delegate = %key,
                parked = self.parked.len(),
                limit = MAX_PARKED_DELEGATES,
                parked_bytes = self.parked_bytes,
                adding_bytes = bytes,
                byte_limit = MAX_PARKED_BYTES,
                over_bytes,
                already_parked = self.parked.contains_key(&key),
                total_refused_parks = self.refused.parks.saturating_add(1),
                "Refusing to park delegate; falling back to the inline path"
            );
            self.refused.parks = self.refused.parks.saturating_add(1);
            return ParkAdmission::Refused(Box::new(continuation));
        }
        self.parked_bytes = self.parked_bytes.saturating_add(bytes);
        let epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.parked.insert(
            key,
            ParkEntry {
                continuation,
                epoch,
                task_bytes,
                parked_at: tokio::time::Instant::now(),
                pending_clients: VecDeque::new(),
                pending_notifications: HashMap::new(),
                pending_bytes: 0,
            },
        );
        ParkAdmission::Admitted { epoch }
    }

    /// Queue a request that arrived for a parked delegate.
    ///
    /// Caller must have checked [`is_parked`](Self::is_parked); queueing for an
    /// unparked delegate is a caller bug and is reported back as a rejection so
    /// the request is still answered.
    pub(super) fn queue_pending(&mut self, key: &DelegateKey, req: PendingRun) -> QueueOutcome {
        let parked_bytes = self.parked_bytes;
        let Some(entry) = self.parked.get_mut(key) else {
            return QueueOutcome::Rejected(Box::new(req));
        };

        match req {
            // COALESCE, do not reject. A superseded notification carries
            // nothing its successor does not, so replacing is lossless in a way
            // rejecting is not — and rejecting would land on ghostkeys and
            // Harvest exactly when they are most active.
            PendingRun::Notification { contract_id, req } => {
                let bytes = request_bytes(&req);
                let superseded = entry
                    .pending_notifications
                    .get(&contract_id)
                    .map_or(0, request_bytes);
                // `contains_key` alone is the question. An earlier
                // `superseded == 0 &&` conjunct was dead weight that also read
                // as if a zero-byte entry were no entry (L11).
                let is_new_contract = !entry.pending_notifications.contains_key(&contract_id);

                if is_new_contract
                    && entry.pending_notifications.len() >= MAX_PENDING_NOTIFICATION_CONTRACTS
                {
                    tracing::info!(
                        delegate = %key,
                        contract = %contract_id,
                        limit = MAX_PENDING_NOTIFICATION_CONTRACTS,
                        total_dropped = self.refused.notifications.saturating_add(1),
                        "Dropped a notification: too many distinct contracts already \
                         queued behind this park"
                    );
                    self.refused.notifications = self.refused.notifications.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(PendingRun::Notification {
                        contract_id,
                        req,
                    }));
                }

                let projected = parked_bytes
                    .saturating_add(bytes)
                    .saturating_sub(superseded);
                if projected > MAX_PARKED_BYTES {
                    tracing::info!(
                        delegate = %key,
                        contract = %contract_id,
                        parked_bytes,
                        adding_bytes = bytes,
                        byte_limit = MAX_PARKED_BYTES,
                        total_dropped = self.refused.notifications.saturating_add(1),
                        "Dropped a notification: queueing it would exceed the parked \
                         byte budget"
                    );
                    self.refused.notifications = self.refused.notifications.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(PendingRun::Notification {
                        contract_id,
                        req,
                    }));
                }

                entry.pending_bytes = entry.pending_bytes.saturating_add(bytes);
                if let Some(old) = entry.pending_notifications.insert(contract_id, req) {
                    let freed = request_bytes(&old);
                    entry.pending_bytes = entry.pending_bytes.saturating_sub(freed);
                    self.parked_bytes = self.parked_bytes.saturating_sub(freed);
                    tracing::debug!(
                        delegate = %key,
                        contract = %contract_id,
                        "Coalesced a superseded notification behind a park"
                    );
                }
                self.parked_bytes = self.parked_bytes.saturating_add(bytes);
                QueueOutcome::Queued
            }
            // Client requests keep the cap: over it, the caller is TOLD.
            client => {
                if entry.pending_clients.len() >= MAX_PENDING_PER_DELEGATE {
                    tracing::warn!(
                        delegate = %key,
                        queued = entry.pending_clients.len(),
                        limit = MAX_PENDING_PER_DELEGATE,
                        total_refused = self.refused.client_requests.saturating_add(1),
                        "Delegate pending queue full while parked — rejecting request"
                    );
                    self.refused.client_requests = self.refused.client_requests.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(client));
                }
                let bytes = match &client {
                    PendingRun::Client { req, .. } => request_bytes(req),
                    PendingRun::Notification { .. } => 0,
                };
                // Check the PROJECTED total BEFORE inserting. Adding first and
                // checking never was worse than an overshoot: once
                // `parked_bytes` passed the cap, `park()` refused EVERY delegate
                // node-wide and everything fell back to inline stalls, so one
                // local app pushing large ApplicationMessages behind a single
                // park could disable parking for the whole node — reinstating
                // the exact stall this change removes.
                if parked_bytes.saturating_add(bytes) > MAX_PARKED_BYTES {
                    tracing::warn!(
                        delegate = %key,
                        parked_bytes,
                        adding_bytes = bytes,
                        byte_limit = MAX_PARKED_BYTES,
                        total_refused = self.refused.client_requests.saturating_add(1),
                        "Refusing to queue a delegate request: it would exceed the \
                         parked byte budget"
                    );
                    self.refused.client_requests = self.refused.client_requests.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(client));
                }
                entry.pending_bytes = entry.pending_bytes.saturating_add(bytes);
                self.parked_bytes = self.parked_bytes.saturating_add(bytes);
                entry.pending_clients.push_back(client);
                QueueOutcome::Queued
            }
        }
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
    /// End the park identified by `(key, epoch)`.
    ///
    /// Returns `None` when the epoch does not match — a STALE resume, from an
    /// off-loop task whose park was already ended by the TTL backstop and whose
    /// delegate has since re-parked. Matching on key alone would feed the old
    /// continuation's messages into the new park (#5544 H1).
    pub(super) fn take_matching(
        &mut self,
        key: &DelegateKey,
        epoch: u64,
    ) -> Option<(Continuation, VecDeque<PendingRun>)> {
        match self.parked.get(key) {
            Some(entry) if entry.epoch == epoch => {}
            Some(entry) => {
                tracing::warn!(
                    delegate = %key,
                    stale_epoch = epoch,
                    live_epoch = entry.epoch,
                    "Dropping a STALE park resume: this delegate re-parked after \
                     its previous park was force-resumed by the TTL backstop. \
                     Absorbing it would feed the old continuation's messages to \
                     the new park (#5544 H1)"
                );
                return None;
            }
            None => return None,
        }
        self.parked.remove(key).map(|entry| {
            self.parked_bytes = self
                .parked_bytes
                .saturating_sub(continuation_bytes(&entry.continuation))
                .saturating_sub(entry.task_bytes)
                .saturating_sub(entry.pending_bytes);
            // Client requests first, then coalesced notifications. Clients have
            // a caller waiting on a response; notifications do not, and their
            // ordering is already approximate because coalescing drops
            // superseded ones.
            let mut pending: VecDeque<PendingRun> = entry.pending_clients;
            pending.extend(
                entry
                    .pending_notifications
                    .into_iter()
                    .map(|(contract_id, req)| PendingRun::Notification { contract_id, req }),
            );
            (entry.continuation, pending)
        })
    }

    /// The earliest instant at which some park will reach [`PARK_TTL`], or
    /// `None` when nothing is parked.
    ///
    /// The loop uses this to arm a timer in its idle `select!`. Without it the
    /// backstop sweep only runs when some UNRELATED event happens to wake the
    /// loop, so on a quiet node — the normal state for a background peer, and
    /// exactly the condition under which a prompt goes unanswered because no
    /// dashboard tab is open — a wedged park would never be swept. A backstop
    /// whose firing depends on other traffic is not a backstop.
    pub(super) fn next_sweep_deadline(&self) -> Option<tokio::time::Instant> {
        self.parked
            .values()
            .map(|entry| entry.parked_at + PARK_TTL)
            .min()
    }

    /// Keys whose park has outlived [`PARK_TTL`].
    ///
    /// Returned rather than acted on so the caller (which owns the executor and
    /// the channel) performs the force-resume; this keeps the registry a pure
    /// data structure and unit-testable without a loop.
    pub(super) fn expired(&self, now: tokio::time::Instant) -> Vec<(DelegateKey, u64)> {
        let mut out: Vec<(DelegateKey, u64)> = self
            .parked
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.parked_at) >= PARK_TTL)
            .map(|(key, entry)| (key.clone(), entry.epoch))
            .collect();
        // Deterministic order: `HashMap` iteration is arbitrary, and a sweep
        // that force-resumes several parks should not do so in a different
        // order run to run (L7).
        out.sort_by_key(|(_, epoch)| *epoch);
        out
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
            iterations: 0,
        }
    }

    fn pending(byte: u8) -> PendingRun {
        PendingRun::Client {
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
            ctx.park(k.clone(), continuation(), 0),
            ParkAdmission::Admitted { .. }
        ));
        assert!(ctx.is_parked(&k));
        let (_cont, pend) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park must be takeable");
        assert!(pend.is_empty());
        assert!(!ctx.is_parked(&k), "take must end the park");
    }

    #[tokio::test]
    async fn node_wide_cap_refuses_and_hands_the_continuation_back() {
        let (mut ctx, _rx) = ctx();
        for i in 0..MAX_PARKED_DELEGATES {
            assert!(matches!(
                ctx.park(key(i as u8), continuation(), 0),
                ParkAdmission::Admitted { .. }
            ));
        }
        assert_eq!(ctx.parked_count(), MAX_PARKED_DELEGATES);
        // Over the cap: refused, and the continuation comes back so the caller
        // can fall back inline rather than losing the round-trip.
        assert!(matches!(
            ctx.park(key(200), continuation(), 0),
            ParkAdmission::Refused(_)
        ));
        assert!(!ctx.is_parked(&key(200)));
    }

    #[tokio::test]
    async fn double_park_is_refused_not_clobbered() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        assert!(matches!(
            ctx.park(k.clone(), continuation(), 0),
            ParkAdmission::Admitted { .. }
        ));
        // The live continuation must survive: clobbering it would strand the
        // first round-trip's client responder.
        assert!(matches!(
            ctx.park(k.clone(), continuation(), 0),
            ParkAdmission::Refused(_)
        ));
        assert!(
            ctx.take_matching(&k, ctx.epoch_of(&k).expect("parked"))
                .is_some()
        );
    }

    #[tokio::test]
    async fn pending_queue_is_capped_and_overflow_is_returned_not_dropped() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), 0);
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
        let (_cont, pend) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park present");
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

    fn notification(contract: u8, state: &[u8]) -> PendingRun {
        let contract_id = ContractInstanceId::new([contract; 32]);
        PendingRun::Notification {
            contract_id,
            req: DelegateRequest::ApplicationMessages {
                key: key(1),
                params: Parameters::from(Vec::new()),
                inbound: vec![InboundDelegateMsg::ContractNotification(
                    freenet_stdlib::prelude::ContractNotification {
                        contract_id,
                        new_state: WrappedState::new(state.to_vec()),
                        context: DelegateContext::default(),
                    },
                )],
            },
        }
    }

    fn queued_state(run: &PendingRun) -> Option<Vec<u8>> {
        let PendingRun::Notification { req, .. } = run else {
            return None;
        };
        let DelegateRequest::ApplicationMessages { inbound, .. } = req else {
            return None;
        };
        #[allow(clippy::wildcard_enum_match_arm)]
        inbound.iter().find_map(|m| match m {
            InboundDelegateMsg::ContractNotification(n) => Some(n.new_state.as_ref().to_vec()),
            _ => None,
        })
    }

    /// Notifications COALESCE per contract instead of being rejected at the
    /// client queue's cap.
    ///
    /// Rejecting them would be a silent loss — a notification has no caller to
    /// return an error to — and it would land on exactly the wrong population:
    /// ghostkeys parks on prompts, so the window is precisely when a user is
    /// interacting, and Harvest with many address contracts subscribed would
    /// lose payment notifications there.
    #[tokio::test]
    async fn notifications_coalesce_per_contract_rather_than_being_rejected() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), 0);

        // Far more than MAX_PENDING_PER_DELEGATE, all for ONE contract.
        for i in 0..(MAX_PENDING_PER_DELEGATE as u8 + 12) {
            assert!(
                matches!(
                    ctx.queue_pending(&k, notification(7, &[i])),
                    QueueOutcome::Queued
                ),
                "a notification must never be rejected for queue depth; \
                 superseded ones coalesce"
            );
        }

        let (_cont, pending) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park present");
        assert_eq!(
            pending.len(),
            1,
            "notifications for one contract must collapse to a single pending run"
        );
        assert_eq!(
            queued_state(&pending[0]),
            Some(vec![MAX_PENDING_PER_DELEGATE as u8 + 11]),
            "the NEWEST notification must win; a superseded one carries nothing \
             its successor does not"
        );
    }

    /// A full client queue must not block notifications: the two lanes are
    /// separate, because only one of them has a caller that can be told.
    #[tokio::test]
    async fn a_full_client_queue_does_not_reject_notifications() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), 0);

        for i in 0..MAX_PENDING_PER_DELEGATE {
            assert!(matches!(
                ctx.queue_pending(&k, pending(i as u8)),
                QueueOutcome::Queued
            ));
        }
        assert!(
            matches!(
                ctx.queue_pending(&k, pending(99)),
                QueueOutcome::Rejected(_)
            ),
            "client requests still hit the cap — the caller can be told"
        );
        assert!(
            matches!(
                ctx.queue_pending(&k, notification(3, b"x")),
                QueueOutcome::Queued
            ),
            "a notification must still be accepted with the client queue full"
        );

        let (_cont, pending_runs) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park present");
        assert_eq!(
            pending_runs.len(),
            MAX_PENDING_PER_DELEGATE + 1,
            "clients plus the coalesced notification"
        );
    }

    /// Distinct contracts are capped, so the coalescing map cannot grow without
    /// bound if subscriptions are not limited elsewhere.
    /// L9: refusals are COUNTED, not only logged. A refusal that increments
    /// nothing renders as a clean zero to anything reading metrics — the same
    /// pattern this branch fixed for the over-cap client request.
    #[tokio::test]
    async fn refusals_are_counted_per_cause() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), 0);

        for i in 0..MAX_PENDING_PER_DELEGATE {
            ctx.queue_pending(&k, pending(i as u8));
        }
        ctx.queue_pending(&k, pending(99));
        for i in 0..MAX_PENDING_NOTIFICATION_CONTRACTS {
            ctx.queue_pending(&k, notification(i as u8, b"s"));
        }
        ctx.queue_pending(&k, notification(250, b"s"));

        let counts = ctx.refusals();
        assert_eq!(counts.client_requests, 1, "the over-cap client request");
        assert_eq!(
            counts.notifications, 1,
            "the over-cap notification contract"
        );
        assert_eq!(counts.parks, 0, "no park was refused here");
    }

    #[tokio::test]
    async fn distinct_notification_contracts_are_capped() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), 0);

        for i in 0..MAX_PENDING_NOTIFICATION_CONTRACTS {
            assert!(matches!(
                ctx.queue_pending(&k, notification(i as u8, b"s")),
                QueueOutcome::Queued
            ));
        }
        assert!(
            matches!(
                ctx.queue_pending(&k, notification(250, b"s")),
                QueueOutcome::Rejected(_)
            ),
            "a NEW contract past the cap is refused; the delegate will see that \
             contract's next state change"
        );
        // An already-queued contract still coalesces at the cap.
        assert!(matches!(
            ctx.queue_pending(&k, notification(0, b"newer")),
            QueueOutcome::Queued
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn park_expires_only_after_the_ttl() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), 0);

        tokio::time::advance(PARK_TTL - Duration::from_secs(1)).await;
        assert!(
            ctx.expired(tokio::time::Instant::now()).is_empty(),
            "must not expire early — a park cut short would report a spurious \
             failure for work that was about to succeed"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            ctx.expired(tokio::time::Instant::now()),
            vec![(k.clone(), ctx.epoch_of(&k).expect("parked"))]
        );
    }

    /// H1: a resume from a park the TTL backstop already ended must be REJECTED,
    /// not absorbed by whatever park exists now.
    ///
    /// The sweep force-resumes a park without consuming the off-loop task's
    /// `ParkGuard`, so that guard still owes a resume. If the delegate has
    /// re-parked by the time it lands, matching on key alone hands the OLD
    /// continuation's `UserResponse`/`PutContractResponse` messages to the NEW
    /// park — the cross-round-trip corruption the whole exclusion exists to
    /// prevent, arriving through the backstop I was asked to add.
    ///
    /// FALSIFY by making `take_matching` ignore the epoch: the stale resume is
    /// then absorbed and this returns `Some`.
    #[tokio::test]
    async fn a_stale_resume_from_a_force_resumed_park_is_rejected() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);

        let ParkAdmission::Admitted { epoch: first } = ctx.park(k.clone(), continuation(), 0)
        else {
            panic!("first park must be admitted");
        };

        // The TTL backstop ends park #1 WITHOUT consuming its guard.
        assert!(
            ctx.take_matching(&k, first).is_some(),
            "the sweep ends the park it observed"
        );

        // The delegate re-parks: a new round-trip, a new continuation.
        let ParkAdmission::Admitted { epoch: second } = ctx.park(k.clone(), continuation(), 0)
        else {
            panic!("second park must be admitted");
        };
        assert_ne!(first, second, "each park must have its own identity");

        // Park #1's guard finally fires. It must NOT take park #2.
        assert!(
            ctx.take_matching(&k, first).is_none(),
            "a stale resume must be rejected; absorbing it would feed park #1's \
             messages into park #2 (#5544 H1)"
        );
        assert_eq!(
            ctx.epoch_of(&k),
            Some(second),
            "the live park must survive the stale resume untouched"
        );
    }

    /// P1a: the byte cap must charge what is actually RETAINED, including the
    /// payloads the off-loop task holds, not just what `Continuation` points at.
    ///
    /// FALSIFY by reverting any of the three: dropping `task_bytes` from
    /// `park`, omitting `params` from `continuation_bytes`, or removing the
    /// projected-total check on the client queue lane.
    #[tokio::test]
    async fn the_byte_cap_charges_retained_payloads_not_just_the_continuation() {
        let (mut ctx, _rx) = ctx();

        // A continuation carrying a large inbound state, as a real parked GET
        // response would.
        let big = vec![0u8; 8 * 1024 * 1024];
        let mut cont = continuation();
        cont.inbound_so_far = vec![InboundDelegateMsg::ContractNotification(
            freenet_stdlib::prelude::ContractNotification {
                contract_id: ContractInstanceId::new([1; 32]),
                new_state: WrappedState::new(big.clone()),
                context: DelegateContext::default(),
            },
        )];
        assert!(
            continuation_bytes(&cont) >= big.len(),
            "the continuation's inbound state must be charged"
        );

        // `params` is delegate-supplied and retained; omitting it was one of the
        // three ways this bound failed to bound.
        let mut with_params = continuation();
        with_params.params = Parameters::from(vec![7u8; 4096]);
        assert!(
            continuation_bytes(&with_params) >= 4096,
            "`params` must be charged: it is retained for the life of the park"
        );

        // Fill the budget with parks that each carry a large task payload, and
        // confirm admission is refused rather than the total silently growing.
        let per_park = MAX_PARKED_BYTES / 4;
        let mut admitted = 0usize;
        for i in 0..MAX_PARKED_DELEGATES {
            match ctx.park(key(i as u8), continuation(), per_park) {
                ParkAdmission::Admitted { .. } => admitted += 1,
                ParkAdmission::Refused(_) => break,
            }
        }
        assert!(
            admitted <= 4,
            "the byte cap must refuse once the RETAINED total is reached; \
             admitted {admitted} parks of {per_park} bytes each against a \
             {MAX_PARKED_BYTES} byte budget"
        );
    }

    #[tokio::test]
    async fn guard_delivers_exactly_one_resume_on_success() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let guard = ParkGuard::new(tx, key(1), 0);
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
        drop(ParkGuard::new(tx, key(1), 0));
        let resume = rx.recv().await.expect("drop must still resume the park");
        assert_eq!(resume.cause, ResumeCause::TimedOut);
        assert!(resume.inbound.is_empty());
    }
}
