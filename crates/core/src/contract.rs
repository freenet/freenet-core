//! Handling of contracts and delegates, including storage, execution, caching, etc.
//!
//! Internally uses the wasm_runtime module to execute contract and/or delegate instructions.

use std::sync::Arc;

use either::Either;
use freenet_stdlib::prelude::*;

pub(crate) mod delegate_app_registry;
mod delegate_park;
mod executor;
mod fair_queue;
pub(crate) use fair_queue::Priority;
/// Fair-queue occupancy for the status snapshot (#4917). Re-exported rather
/// than widening `fair_queue` itself, which is otherwise private to this
/// module.
pub(crate) use fair_queue::{FairQueueStats, global_stats as fair_queue_stats};
pub(crate) mod governance;
mod handler;
mod state_size_metrics;
pub mod storages;
pub(crate) mod user_input;

pub(crate) use executor::{
    ContractExecutor, ExecutorTransactionStream, ExportBusy, MAX_CREATED_DELEGATES_PER_NODE,
    MAX_DELEGATE_CREATION_DEPTH, MAX_DELEGATE_CREATIONS_PER_CALL,
    SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE, UpsertOutcome, UpsertResult, declared_cache_ceiling,
    mock_runtime::MockRuntime,
};

// Re-export CRDT emulation functions for testing
pub use executor::mock_runtime::{clear_crdt_contracts, is_crdt_contract, register_crdt_contract};
pub(crate) use handler::{
    BundleKeyKind, ClientResponsesReceiver, ClientResponsesSender, ContractHandler,
    ContractHandlerChannel, ContractHandlerEvent, ImportBundle, ImportTargetScope,
    NetworkContractHandler, RedactedToken, SenderHalve, SessionMessage, StashedResponder,
    StoreResponse, WaitingResolution, WaitingTransaction, client_responses_channel,
    contract_handler_channel,
    in_memory::{
        MemoryContractHandler, MockWasmContractHandler, MockWasmHandlerBuilder,
        SimulationContractHandler, SimulationHandlerBuilder,
    },
};

pub use executor::{ContractQueueFull, Executor, ExecutorError, OperationMode};
pub use handler::reset_event_id_counter;
pub(crate) use state_size_metrics::{
    StateSizeRejectionStage, record_state_size_rejection, state_size_rejection_snapshot,
};

use freenet_stdlib::client_api::DelegateRequest;
use tracing::Instrument;

use self::executor::DelegateNotificationReceiver;
use self::user_input::{CallerIdentity, UserInputPrompter};
use crate::config::GlobalExecutor;
use crate::wasm_runtime::UserSecretContext;

/// Maximum iterations when handling contract requests to prevent infinite loops
const MAX_CONTRACT_REQUEST_ITERATIONS: usize = 100;

/// Maximum delegate notifications to drain per iteration.
/// Matches the same bounded-drain pattern as MAX_DRAIN_BATCH for contract events,
/// preventing delegate notification channel growth under sustained contract load.
const MAX_DELEGATE_DRAIN_BATCH: usize = 16;

/// Timeout for the off-loop related-contract fetch that backs a deferred
/// PUT/UPDATE (#4391).
///
/// This matches the executor's `RELATED_FETCH_TIMEOUT` (10s) — the ONLY timeout
/// that was ever on the serial `contract_handling` loop's upsert path. The
/// inline related-fetch sites the loop hits (validate-side
/// `fetch_related_for_validation` and the `update_state requires()` branch) are
/// all bounded by that 10s budget, so off-loading them under the same budget
/// preserves the client-visible timeout exactly.
///
/// (The separate 120s `SUB_OP_FETCH_TIMEOUT` in `local_state_or_from_network`
/// is on the `OperationMode::Local`-only GET path, NOT on the serial loop's
/// `upsert_contract_state` path, so it is unrelated to this fix and unchanged.)
/// Upper bound on the OFF-LOOP related-contract fetch. The fetch runs in a
/// spawned waiter task (NOT on the serial `contract_handling` loop), so it does
/// not block other contract ops — there is therefore no reason to cut it shorter
/// than the GET sub-op's own budget. `start_sub_op_get` self-terminates at
/// `OPERATION_TTL` (60s); this is that budget plus a small margin so the GET's
/// own outcome (including a successful late Found) wins over this backstop. An
/// earlier 10s value (inherited from the inline-fetch era, where a shorter cap
/// kept the loop from stalling) prematurely failed slow-but-successful related
/// GETs on lossy/cross-node paths — the exact #4391 / freenet-mail scenario.
const DEFERRED_RELATED_FETCH_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(crate::config::OPERATION_TTL.as_secs() + 2);

/// Maximum number of PUT/UPDATE operations that may have an in-flight
/// off-loop related-contract fetch at once.
///
/// Bounds the number of concurrently spawned waiter tasks so a flood of
/// related-needing PUTs cannot spawn unbounded background work. When the cap
/// is reached, a new related-needing op falls back to surfacing
/// `MissingRelated` to the client rather than deferring — backpressure, not
/// unbounded growth (see code-style.md "per-key/per-client caps").
const MAX_INFLIGHT_DEFERRALS: usize = 256;

/// Maximum number of deferred resumes applied per loop iteration before
/// yielding back to the fair queue.
///
/// Each resume is a full WASM upsert; draining all of them back-to-back would
/// re-introduce head-of-line latency for cached GETs (the exact thing #4391
/// fixes). Capping the per-iteration resume work and interleaving it with the
/// fair queue keeps both moving. Mirrors `fair_queue::MAX_DRAIN_BATCH`'s
/// bounded-batch rationale.
const MAX_RESUME_DRAIN_BATCH: usize = 16;

/// A PUT/UPDATE that deferred its related-contract fetch off the serial loop,
/// carrying everything needed to re-run the upsert once the fetch resolves.
///
/// Built by the off-loop waiter task and sent back to the `contract_handling`
/// loop via the resume channel; the loop re-runs the upsert ON the loop (so
/// WASM stays serial) with `fetched` supplied, then answers the stashed client
/// responder. See issue #4391.
struct DeferredResume {
    /// Key into the loop's stashed-responder map (which doubles as the
    /// in-flight-deferral accounting for the `MAX_INFLIGHT_DEFERRALS` cap).
    deferral_id: u64,
    /// The contract being PUT/UPDATEd.
    key: ContractKey,
    /// The original update payload (full state for PUT, delta/state for UPDATE).
    update: Either<WrappedState, StateDelta<'static>>,
    /// Caller-supplied related contracts from the original event.
    related_contracts: RelatedContracts<'static>,
    /// Contract code (Some for PUT, None for UPDATE).
    code: Option<ContractContainer>,
    /// `true` for PUT (build `PutResponse`), `false` for UPDATE
    /// (build `UpdateResponse`).
    is_put: bool,
    /// The states fetched off-loop, or `Err` if the fetch failed/timed out
    /// (→ resume surfaces `MissingRelated`).
    fetched: Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>,
}

/// RAII guard that guarantees the off-loop waiter delivers EXACTLY ONE terminal
/// [`DeferredResume`] for its deferral — whether the fetch completes, or the
/// waiter task is dropped/panics/cancelled before sending.
///
/// This is the load-bearing invariant of the #4391 deferral lifecycle: because
/// every deferral is answered by exactly one resume, the loop never needs a TTL
/// sweep to recover a wedged waiter, and `handle_deferred_resume` never needs a
/// stale-resume guard (a resume is always for the live deferral identified by
/// its unique `deferral_id`).
///
/// - On the success path the waiter calls [`send`](Self::send) with the real
///   fetch result; this takes the payload so `Drop` becomes a no-op.
/// - On any early exit (drop / panic / cancellation) before `send`, `Drop`
///   delivers a terminal resume carrying a `MissingRelated` fetch error, which
///   `handle_deferred_resume` surfaces to the client (answering the parked
///   responder exactly once).
///
/// Exactly once: never zero (Drop covers the early-exit path), never twice (the
/// success send `take`s the payload, so Drop sees `None` and does nothing).
///
/// The resume channel is unbounded, so both sends are non-blocking
/// `unbounded_send` (no `.await`, no back-pressure). A send failure means the
/// loop has shut down — nothing left to answer.
struct ResumeGuard {
    /// `None` once a resume has been sent (success or drop). Holds everything a
    /// [`DeferredResume`] needs except its `fetched` result.
    payload: Option<ResumePayload>,
}

struct ResumePayload {
    resume_tx: tokio::sync::mpsc::UnboundedSender<DeferredResume>,
    deferral_id: u64,
    key: ContractKey,
    update: Either<WrappedState, StateDelta<'static>>,
    related_contracts: RelatedContracts<'static>,
    code: Option<ContractContainer>,
    is_put: bool,
}

impl ResumeGuard {
    fn new(payload: ResumePayload) -> Self {
        Self {
            payload: Some(payload),
        }
    }

    /// Deliver the terminal resume with the real fetch result. Consumes the
    /// payload so a subsequent `Drop` is a no-op (exactly-once).
    fn send(mut self, fetched: Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>) {
        if let Some(p) = self.payload.take() {
            Self::deliver(p, fetched);
        }
    }

    fn deliver(
        p: ResumePayload,
        fetched: Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>,
    ) {
        let ResumePayload {
            resume_tx,
            deferral_id,
            key,
            update,
            related_contracts,
            code,
            is_put,
        } = p;
        if resume_tx
            .send(DeferredResume {
                deferral_id,
                key,
                update,
                related_contracts,
                code,
                is_put,
                fetched,
            })
            .is_err()
        {
            tracing::debug!(
                contract = %key,
                "Deferred resume channel closed; contract-handling loop gone"
            );
        }
    }
}

impl Drop for ResumeGuard {
    fn drop(&mut self) {
        // Early exit before `send` (waiter dropped / panicked / cancelled):
        // deliver a terminal MissingRelated resume so the deferral is still
        // answered exactly once (the parked client responder gets a response
        // instead of hanging).
        if let Some(p) = self.payload.take() {
            let key_id = *p.key.id();
            tracing::warn!(
                contract = %p.key,
                deferral_id = p.deferral_id,
                "Off-loop waiter dropped before sending — delivering MissingRelated \
                 resume so the deferral terminates and the client is answered (#4391)"
            );
            Self::deliver(p, Err(ExecutorError::missing_related(key_id)));
        }
    }
}

/// A hosted-mode export that ran OFF the contract loop, carrying its result
/// (and the executor to return to the pool) back to the loop (#4531 / #4381 P5).
///
/// Built by the off-loop export task and sent back to the `contract_handling`
/// loop via the export-resume channel; the loop returns/replaces the executor
/// (`finish_export`) and answers the stashed client responder. Mirrors the
/// #4391 [`DeferredResume`] flow, but the work is the export itself (not a
/// network wait), so there is nothing to re-run on the loop — just deliver the
/// precomputed result and reclaim the executor.
struct ExportResume {
    /// The completed export: the executor to return (or `None` if the export
    /// task panicked) and the result for the client.
    done: crate::contract::executor::runtime::ExportDone,
    /// The parked client responder (the HTTP export handler's oneshot). `None`
    /// only if the client disconnected before the export finished.
    responder: Option<StashedResponder>,
}

/// RAII guard guaranteeing the off-loop export task delivers EXACTLY ONE
/// [`ExportResume`] — whether the export completes, or the task is
/// dropped/panics/cancelled before sending.
///
/// Same load-bearing invariant as the #4391 [`ResumeGuard`]: every admitted
/// export is answered exactly once, so the parked client responder never hangs
/// and the checked-out executor is always reclaimed. On the success path
/// [`send`](Self::send) takes the payload (Drop becomes a no-op); on any early
/// exit, `Drop` delivers an `ExportResume` carrying a panicked-style
/// [`ExportDone`] (`executor: None`) so the loop replaces the lost executor and
/// answers the client with an error.
struct ExportGuard {
    payload: Option<ExportGuardPayload>,
}

struct ExportGuardPayload {
    export_resume_tx: tokio::sync::mpsc::UnboundedSender<ExportResume>,
    responder: Option<StashedResponder>,
}

impl ExportGuard {
    fn new(
        export_resume_tx: tokio::sync::mpsc::UnboundedSender<ExportResume>,
        responder: Option<StashedResponder>,
    ) -> Self {
        Self {
            payload: Some(ExportGuardPayload {
                export_resume_tx,
                responder,
            }),
        }
    }

    /// Deliver the completed export. Consumes the payload so a subsequent `Drop`
    /// is a no-op (exactly-once).
    fn send(mut self, done: crate::contract::executor::runtime::ExportDone) {
        if let Some(p) = self.payload.take() {
            Self::deliver(p, done);
        }
    }

    fn deliver(p: ExportGuardPayload, done: crate::contract::executor::runtime::ExportDone) {
        let ExportGuardPayload {
            export_resume_tx,
            responder,
        } = p;
        // Unbounded channel, non-blocking send (channel-safety.md carve-out: the
        // producer count is bounded by MAX_CONCURRENT_EXPORTS, the receiver is
        // this loop which drains every iteration, and the waiter never reads what
        // the loop produces — no cycle). A send failure means the loop is gone.
        if export_resume_tx
            .send(ExportResume { done, responder })
            .is_err()
        {
            tracing::debug!("export resume channel closed; contract-handling loop gone");
        }
    }
}

impl Drop for ExportGuard {
    fn drop(&mut self) {
        if let Some(p) = self.payload.take() {
            // Early exit before `send` (task dropped / panicked / cancelled).
            // Deliver a panicked-style ExportDone so the loop replaces the lost
            // executor and answers the parked client exactly once.
            tracing::warn!(
                "Off-loop export task dropped before sending — delivering an error \
                 resume so the export terminates and the client is answered (#4531)"
            );
            Self::deliver(
                p,
                crate::contract::executor::runtime::ExportDone::lost_to_drop(),
            );
        }
    }
}

/// Loop-side state for off-loading deferred related-contract fetches (#4391).
///
/// Owned by `contract_handling`; passed by `&mut` into `handle_contract_event`
/// so the PUT/UPDATE arms can defer. When this context is absent (e.g. the
/// delegate-driven PUT path, or direct unit-test calls to
/// `handle_contract_event`), the arms keep the legacy inline-fetch behavior.
///
/// NOTE: there is deliberately NO per-contract ordering/blocking here. A GET or
/// UPDATE for a contract whose PUT is mid-deferral runs immediately in normal
/// fair-queue order and may observe the pre-PUT state; the PUT issuer's own
/// response is still delivered only on commit (via its stashed responder), so
/// its causality holds. Two same-key ops can also defer concurrently (bounded
/// globally by `MAX_INFLIGHT_DEFERRALS`); each resumes independently and
/// re-runs its upsert against CURRENT state, so the contract's CRDT merge
/// reconciles them exactly as it would two concurrent same-key UPDATEs — no
/// lost update. This reordering is the intended, accepted behavior (the
/// per-contract-FIFO machinery was removed in #4391 round 5).
struct DeferralCtx {
    /// Off-loop waiters send completed [`DeferredResume`]s back to the loop here.
    resume_tx: tokio::sync::mpsc::UnboundedSender<DeferredResume>,
    /// Client responders parked while their op's related fetch runs off-loop,
    /// keyed by `deferral_id`. Drained by the loop when the resume arrives.
    stashed: std::collections::HashMap<u64, StashedResponder>,
    /// Monotonic id generator for `deferral_id`.
    next_deferral_id: u64,
    /// Off-loop export tasks send their completed [`ExportResume`]s back to the
    /// loop here (#4531 / #4381 P5). Separate from `resume_tx` because the export
    /// resume carries an executor + precomputed bytes, not an upsert to re-run.
    export_resume_tx: tokio::sync::mpsc::UnboundedSender<ExportResume>,
}

impl DeferralCtx {
    fn new(
        resume_tx: tokio::sync::mpsc::UnboundedSender<DeferredResume>,
        export_resume_tx: tokio::sync::mpsc::UnboundedSender<ExportResume>,
    ) -> Self {
        Self {
            resume_tx,
            stashed: std::collections::HashMap::new(),
            next_deferral_id: 0,
            export_resume_tx,
        }
    }

    /// `true` when the in-flight deferral cap is reached and a new op must NOT
    /// defer (it falls back to surfacing `MissingRelated`).
    fn at_capacity(&self) -> bool {
        self.stashed.len() >= MAX_INFLIGHT_DEFERRALS
    }
}

#[cfg(test)]
pub(crate) type OffLoopFetchStub = Arc<
    dyn Fn(
            Vec<ContractInstanceId>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>,
                    > + Send,
            >,
        > + Send
        + Sync,
>;

#[cfg(test)]
static OFF_LOOP_FETCH_OVERRIDE: std::sync::Mutex<Option<OffLoopFetchStub>> =
    std::sync::Mutex::new(None);

/// Install (or clear) the test override for [`fetch_related_off_loop`].
///
/// Unlike the executor's `TEST_NETWORK_FETCH_OVERRIDE` (a thread-local, sync
/// stub), this override is `Arc`-based and async so a test running on a
/// multi-thread runtime can make the off-loop fetch *hang* until the test
/// releases it — the deterministic way to reproduce the head-of-line stall
/// the fix removes (#4391).
#[cfg(test)]
pub(crate) fn set_off_loop_fetch_override(stub: Option<OffLoopFetchStub>) {
    *OFF_LOOP_FETCH_OVERRIDE.lock().unwrap() = stub;
}

/// Map a sub-op GET outcome to a related-contract fetch result: `Found` yields
/// the fetched state; `NotFound`/`Infra` map to `MissingRelated` for `id`.
/// Factored out so the mapping is unit-testable without an `OpManager`.
fn map_sub_op_outcome(
    outcome: crate::operations::get::op_ctx_task::SubOpGetOutcome,
    id: ContractInstanceId,
) -> Result<WrappedState, ExecutorError> {
    use crate::operations::get::op_ctx_task::SubOpGetOutcome;
    match outcome {
        SubOpGetOutcome::Found(get_result) => {
            Ok(WrappedState::from(get_result.state.as_ref().to_vec()))
        }
        SubOpGetOutcome::NotFound(_) | SubOpGetOutcome::Infra(_) => {
            Err(ExecutorError::missing_related(id))
        }
    }
}

/// Drive the related-contract GET(s) for a deferred upsert OFF the serial
/// `contract_handling` loop.
///
/// Each missing id races its own background sub-op GET (`start_sub_op_get`
/// already spawns the GET on its own task); we await the oneshots under a
/// single [`DEFERRED_RELATED_FETCH_TIMEOUT`] budget. Any miss/timeout/infra
/// error fails the whole fetch with `MissingRelated`, mirroring the inline
/// validate path's all-or-nothing semantics. Runs in a `GlobalExecutor::spawn`
/// task; nothing here touches the executor or the serial loop.
async fn fetch_related_off_loop(
    op_manager: Option<Arc<crate::node::OpManager>>,
    missing: Vec<ContractInstanceId>,
) -> Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError> {
    #[cfg(test)]
    {
        let stub = OFF_LOOP_FETCH_OVERRIDE.lock().unwrap().clone();
        if let Some(stub) = stub {
            return stub(missing).await;
        }
    }

    let Some(op_manager) = op_manager else {
        // No network available (local-only executor): surface the first
        // missing id as MissingRelated, matching the legacy local-only path.
        let id = missing
            .first()
            .copied()
            .unwrap_or_else(|| ContractInstanceId::new([0u8; 32]));
        return Err(ExecutorError::missing_related(id));
    };

    let fetch_all = async {
        let results: Vec<(ContractInstanceId, Result<WrappedState, ExecutorError>)> =
            futures::future::join_all(missing.iter().map(|id| {
                let id = *id;
                let op_manager = op_manager.clone();
                async move {
                    let (_tx, rx) = crate::operations::get::op_ctx_task::start_sub_op_get(
                        &op_manager,
                        id,
                        false,
                    );
                    let mapped = match rx.await {
                        Ok(outcome) => map_sub_op_outcome(outcome, id),
                        Err(_) => Err(ExecutorError::missing_related(id)),
                    };
                    (id, mapped)
                }
            }))
            .await;
        let mut fetched = Vec::with_capacity(results.len());
        for (id, res) in results {
            fetched.push((id, res?));
        }
        Ok::<_, ExecutorError>(fetched)
    };

    match tokio::time::timeout(DEFERRED_RELATED_FETCH_TIMEOUT, fetch_all).await {
        Ok(result) => result,
        Err(_) => {
            let id = missing
                .first()
                .copied()
                .unwrap_or_else(|| ContractInstanceId::new([0u8; 32]));
            Err(ExecutorError::missing_related(id))
        }
    }
}

/// Whether a delegate run may deliver delegate-to-delegate messages.
///
/// This exists because the two callers of
/// [`handle_delegate_with_contract_requests`] differ in a security-relevant way:
/// one descends from a client connection whose scope is known, the other does
/// not descend from a connection at all (GHSA-824h-7x5x-wfmf).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InterDelegateDispatch {
    /// Client-driven run. The hop carries the ORIGINATING connection scope, so a
    /// non-local caller cannot obtain an attested `MessageOrigin::Delegate`.
    Allowed,
    /// Contract-notification-driven run. Suppressed — see the rationale at the
    /// dispatch site.
    Suppressed,
}

/// Build the delegate-facing response for a PUT/UPDATE upsert.
///
/// Shared by the inline path and the post-fetch resume path so a deferred
/// upsert reports exactly what an inline one would.
fn upsert_response_msg(
    is_put: bool,
    contract_id: ContractInstanceId,
    context: DelegateContext,
    result: Result<UpsertResult, ExecutorError>,
) -> InboundDelegateMsg<'static> {
    let outcome = match result {
        Ok(_) => Ok(()),
        Err(err) => {
            tracing::warn!(
                contract = %contract_id,
                error = %err,
                is_put,
                "Failed to upsert contract for a delegate contract request"
            );
            Err(format!("{err}"))
        }
    };
    if is_put {
        InboundDelegateMsg::PutContractResponse(PutContractResponse {
            contract_id,
            result: outcome,
            context,
        })
    } else {
        InboundDelegateMsg::UpdateContractResponse(UpdateContractResponse {
            contract_id,
            result: outcome,
            context,
        })
    }
}

/// Run a deferred upsert INLINE, related fetch and all.
///
/// The fallback when there is no parking context, or when the node-wide park
/// cap refused this one. It reinstates the pre-#5544 stall for that single
/// operation, which is the deliberate trade: degrading to the old behaviour
/// beats dropping a delegate's write.
async fn run_deferred_upsert_inline<CH>(
    contract_handler: &mut CH,
    pending: delegate_park::PendingUpsert,
) -> InboundDelegateMsg<'static>
where
    CH: ContractHandler + Send + 'static,
{
    let contract_id = *pending.key.id();
    let result = contract_handler
        .executor()
        .upsert_contract_state(
            pending.key,
            pending.update,
            pending.related_contracts,
            pending.code,
        )
        .await;
    upsert_response_msg(pending.is_put, contract_id, pending.context, result)
}

/// Re-run a deferred upsert ON the loop now that its related contracts have
/// been fetched off it.
///
/// The fetched states are injected so the resumed upsert resolves them locally
/// and does NOT make a second network round trip — the same trick
/// `handle_deferred_resume` uses for the client-driven path (#4391).
async fn apply_resolved_upsert<CH>(
    contract_handler: &mut CH,
    resolved: delegate_park::ResolvedUpsert,
) -> InboundDelegateMsg<'static>
where
    CH: ContractHandler + Send + 'static,
{
    let delegate_park::ResolvedUpsert { pending, fetched } = resolved;
    let contract_id = *pending.key.id();
    let mut related_contracts = pending.related_contracts;
    let result = match fetched {
        Ok(states) => {
            for (id, state) in states {
                inject_related_state(
                    &mut related_contracts,
                    id,
                    freenet_stdlib::prelude::State::from(state.as_ref().to_vec()),
                );
            }
            // DEFERRABLE, not the plain upsert (#5544 B4).
            //
            // This runs from `handle_delegate_resume`, ON the serial loop. If
            // the contract asks for a SECOND or DIFFERENT related contract now
            // that the first set is injected, the plain `upsert_contract_state`
            // would do that network GET inline and re-create the exact
            // node-wide stall this change removes.
            //
            // A repeated `DeferRelated` becomes `MissingRelated` rather than
            // deferring again: the same depth=1 / one-deferral cap
            // `handle_deferred_resume` applies on the client path, and the
            // pattern `contracts.md` requires for work on the serial loop.
            // Without the cap a contract could defer indefinitely and hold this
            // delegate's exclusion open.
            let outcome = contract_handler
                .executor()
                .upsert_contract_state_deferrable(
                    pending.key,
                    pending.update,
                    related_contracts,
                    pending.code,
                )
                .await;
            match outcome {
                Ok(UpsertOutcome::Completed(r)) => Ok(r),
                Ok(UpsertOutcome::DeferRelated(missing)) => {
                    let id = missing.first().copied().unwrap_or(contract_id);
                    tracing::warn!(
                        contract = %contract_id,
                        "Resumed delegate upsert requested a further related \
                         contract; refusing to defer twice (depth=1 cap)"
                    );
                    Err(ExecutorError::missing_related(id))
                }
                Err(err) => Err(err),
            }
        }
        // The fetch failed or timed out: surface it to the delegate rather than
        // retrying, matching the all-or-nothing semantics of the inline path.
        Err(err) => Err(err),
    };
    upsert_response_msg(pending.is_put, contract_id, pending.context, result)
}

/// Drive the permission prompts for one delegate iteration and build the
/// `UserResponse` messages to feed back.
///
/// Extracted so the PARKED path (spawned off the loop) and the inline fallback
/// (taken only when the park cap is hit) run byte-identical logic. Inlining it
/// twice is how the denied/timed-out branch silently diverges.
async fn run_user_input_prompts<P>(
    prompter: &P,
    requests: Vec<UserInputRequest<'static>>,
    delegate_key: &DelegateKey,
    delegate_key_str: &str,
    caller: CallerIdentity,
) -> Vec<InboundDelegateMsg<'static>>
where
    P: UserInputPrompter,
{
    let mut out = Vec::with_capacity(requests.len());
    for req in requests {
        let request_id = req.request_id;
        let response = match prompter
            .prompt(&req, delegate_key_str, caller.clone())
            .await
        {
            Some((_, response)) => response,
            None => {
                tracing::warn!(
                    request_id,
                    delegate = %delegate_key,
                    "User input request timed out or was denied"
                );
                // Send an empty response so the delegate knows the request
                // was denied/timed out, rather than leaving it waiting forever.
                ClientResponse::new(Vec::new())
            }
        };
        out.push(InboundDelegateMsg::UserResponse(UserInputResponse {
            request_id,
            response,
            // UserInputRequest has no context field, so we use default.
            // The delegate's actual context is maintained separately in
            // the process_outbound loop in delegate.rs.
            context: DelegateContext::default(),
        }));
    }
    out
}

/// Loop-side context that lets a delegate run PARK instead of blocking.
///
/// `None` for callers with no loop behind them (direct unit-test calls), which
/// keep the legacy inline behaviour exactly. Mirrors the
/// `deferral: Option<&mut DeferralCtx>` shape `handle_contract_event` already
/// uses for #4391.
struct ParkingCtx<'a> {
    park: &'a mut delegate_park::DelegateParkCtx,
    /// Where the residual messages go once the (possibly resumed) run finishes.
    delivery: delegate_park::Delivery,
    /// The client responder a RESUMED run is already holding (it was taken from
    /// the channel when the run first parked, so it cannot be re-taken by event
    /// id). Borrowed, not owned: if the run parks again the value moves into the
    /// new continuation, and if it completes the caller reads what is left here
    /// and answers the client. A fresh run passes `&mut None`.
    carried_responder: &'a mut Option<StashedResponder>,
}

/// Outcome of one delegate run.
enum DelegateRunOutcome {
    /// The run finished; these are the residual outbound messages.
    Completed(Vec<OutboundDelegateMsg>),
    /// The run PARKED awaiting off-loop work and will be resumed on a later
    /// loop iteration. The caller must NOT answer the client — it must hand the
    /// client's responder to the park entry (`attach_responder`) and return, so
    /// the client sees one response covering the whole round-trip.
    Parked,
}

/// Handle a delegate request, including any contract request messages in the response.
///
/// When a delegate emits contract request messages, this function:
/// 1. For GET: Fetches the contract state and sends GetContractResponse back
/// 2. For PUT: Upserts the contract state via `upsert_contract_state` (which automatically
///    propagates to the network via `BroadcastStateChange`), sends PutContractResponse back
/// 3. For UPDATE: Applies a state update via `upsert_contract_state` (same propagation),
///    sends UpdateContractResponse back
/// 4. For SUBSCRIBE: Registers in subscription registry, sends SubscribeContractResponse back
/// 5. Repeats until no more contract request messages
///
/// Returns the final response with contract request messages filtered out.
#[allow(clippy::too_many_arguments)]
async fn handle_delegate_with_contract_requests<CH, P>(
    contract_handler: &mut CH,
    initial_req: DelegateRequest<'static>,
    origin_contract: Option<&ContractInstanceId>,
    connection_scope: crate::client_events::ConnectionScope,
    inter_delegate: InterDelegateDispatch,
    user_context: Option<&UserSecretContext>,
    delegate_key: &DelegateKey,
    prompter: &std::sync::Arc<P>,
    mut parking: Option<ParkingCtx<'_>>,
    // Messages this delegate emitted BEFORE an earlier park, carried in so a
    // round-trip that parks more than once still yields one complete response.
    // `Vec::new()` for a fresh run; `continuation.accumulated` for a resume.
    seed_accumulated: Vec<OutboundDelegateMsg>,
) -> DelegateRunOutcome
where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter + 'static,
{
    // Extract initial params from the request (only ApplicationMessages has params we need).
    // The registration variants (including RegisterDelegateWithPredecessors,
    // #4117) carry no params relevant here — registration's empty response exits
    // the loop before params are read — but the new variant is listed EXPLICITLY
    // for local consistency with this match's style; the wildcard remains only to
    // satisfy `#[non_exhaustive]`.
    // GATE THE ORIGIN BEFORE ANY CONSUMER IN THIS FUNCTION
    // (GHSA-824h-7x5x-wfmf).
    //
    // Two consumers below read `origin_contract`: the executor call (which
    // gates again internally, so it is safe either way) and
    // `caller_identity_from_origin`, which names the calling app to the HUMAN in
    // the consent prompt. The prompt was the one that mattered: a remote caller
    // could mint a token for a well-known contract id, drive a delegate that
    // emits `RequestUserInput` with attacker-chosen text, and have the prompt
    // render that contract's identity as the requester to a local user who then
    // approves it.
    //
    // Gating here rather than at the prompt call site keeps the two consumers on
    // one value, so a future third consumer cannot be added ungated by accident.
    let origin_contract = if connection_scope.is_local() {
        origin_contract
    } else {
        None
    };

    let initial_params = match &initial_req {
        DelegateRequest::ApplicationMessages { params, .. } => params.clone(),
        DelegateRequest::RegisterDelegate { .. }
        | DelegateRequest::RegisterDelegateWithPredecessors { .. }
        | DelegateRequest::UnregisterDelegate(_)
        | _ => Parameters::from(Vec::new()),
    };

    let mut current_req = initial_req;
    let current_params = initial_params;
    let mut iterations = 0;
    // Accumulate non-contract-request messages across iterations
    let mut accumulated_messages: Vec<OutboundDelegateMsg> = seed_accumulated;

    loop {
        iterations += 1;
        if iterations > MAX_CONTRACT_REQUEST_ITERATIONS {
            tracing::error!(
                delegate_key = %delegate_key,
                iterations = iterations,
                "Exceeded maximum contract request iterations, possible infinite loop"
            );
            // Return whatever we accumulated so far
            return DelegateRunOutcome::Completed(accumulated_messages);
        }

        // Execute the delegate request
        let values = match contract_handler
            .executor()
            .execute_delegate_request(
                current_req,
                origin_contract,
                None,
                connection_scope,
                user_context,
            )
            .await
        {
            Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse { key: _, values }) => {
                values
            }
            Ok(freenet_stdlib::client_api::HostResponse::Ok) => Vec::new(),
            Ok(_other) => {
                tracing::error!(
                    delegate_key = %delegate_key,
                    phase = "unexpected_response",
                    "Unexpected response type from delegate request"
                );
                // Return whatever we accumulated so far
                return DelegateRunOutcome::Completed(accumulated_messages);
            }
            Err(err) => {
                // Downgrade "not found" to warn — expected during legacy
                // migration probes when old delegate WASM isn't on this node
                if err.is_missing_delegate() {
                    tracing::warn!(
                        delegate_key = %delegate_key,
                        "Delegate not found in store (expected for migration probes)"
                    );
                } else {
                    tracing::error!(
                        delegate_key = %delegate_key,
                        error = %err,
                        phase = "execution_failed",
                        "Failed executing delegate request"
                    );
                }
                // Return whatever we accumulated so far
                return DelegateRunOutcome::Completed(accumulated_messages);
            }
        };

        // Check for contract request messages (GET, PUT, UPDATE, SUBSCRIBE), delegate messages,
        // and user input requests
        let mut get_requests: Vec<GetContractRequest> = Vec::new();
        let mut put_requests: Vec<PutContractRequest> = Vec::new();
        let mut update_requests: Vec<UpdateContractRequest> = Vec::new();
        let mut subscribe_requests: Vec<SubscribeContractRequest> = Vec::new();
        let mut delegate_messages: Vec<DelegateMessage> = Vec::new();
        let mut user_input_requests: Vec<UserInputRequest<'static>> = Vec::new();

        for msg in values {
            match msg {
                OutboundDelegateMsg::GetContractRequest(req) => {
                    get_requests.push(req);
                }
                OutboundDelegateMsg::PutContractRequest(req) => {
                    put_requests.push(req);
                }
                OutboundDelegateMsg::UpdateContractRequest(req) => {
                    update_requests.push(req);
                }
                OutboundDelegateMsg::SubscribeContractRequest(req) => {
                    subscribe_requests.push(req);
                }
                OutboundDelegateMsg::SendDelegateMessage(msg) => {
                    delegate_messages.push(msg);
                }
                OutboundDelegateMsg::RequestUserInput(req) => {
                    user_input_requests.push(req);
                }
                other @ OutboundDelegateMsg::ApplicationMessage(_)
                | other @ OutboundDelegateMsg::ContextUpdated(_) => {
                    // Accumulate non-request messages
                    accumulated_messages.push(other);
                }
            }
        }

        // If no contract requests, delegate messages, or user input requests, we're done
        if get_requests.is_empty()
            && put_requests.is_empty()
            && update_requests.is_empty()
            && subscribe_requests.is_empty()
            && delegate_messages.is_empty()
            && user_input_requests.is_empty()
        {
            return DelegateRunOutcome::Completed(accumulated_messages);
        }

        let mut inbound_responses: Vec<InboundDelegateMsg<'static>> = Vec::new();
        // Delegate PUT/UPDATEs whose contract asked for related contracts this
        // node does not hold. Their network fetch is off-loaded with the rest of
        // this iteration's slow work rather than awaited here (#5544 stall 1).
        let mut deferred_upserts: Vec<delegate_park::PendingUpsert> = Vec::new();

        // Process PUT requests (fire-and-forget: upsert state, send result back).
        // This calls upsert_contract_state which stores locally AND automatically
        // propagates to the network via BroadcastStateChange — the same mechanism
        // used by normal client PUT operations (ContractHandlerEvent::PutQuery).
        if !put_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = put_requests.len(),
                "Processing PutContractRequest messages from delegate"
            );

            for req in put_requests {
                let contract_key = req.contract.key();
                let context = req.context;

                // Deferrable: on a missing related contract this returns
                // immediately with the ids instead of awaiting a network GET on
                // the serial loop (#5544 stall 1). Where parking is
                // unavailable the fetch still happens, just inline, below.
                let update = Either::Left(req.state);
                let outcome = contract_handler
                    .executor()
                    .upsert_contract_state_deferrable(
                        contract_key,
                        update.clone(),
                        req.related_contracts.clone(),
                        Some(req.contract.clone()),
                    )
                    .await;

                let result = match outcome {
                    Ok(UpsertOutcome::Completed(r)) => Ok(r),
                    Ok(UpsertOutcome::DeferRelated(missing)) if parking.is_some() => {
                        deferred_upserts.push(delegate_park::PendingUpsert {
                            key: contract_key,
                            update,
                            related_contracts: req.related_contracts,
                            code: Some(req.contract),
                            is_put: true,
                            context,
                            missing,
                        });
                        continue;
                    }
                    Ok(UpsertOutcome::DeferRelated(_)) => {
                        // No parking context (direct unit-test calls): keep the
                        // legacy inline fetch rather than failing the upsert.
                        contract_handler
                            .executor()
                            .upsert_contract_state(
                                contract_key,
                                update,
                                req.related_contracts,
                                Some(req.contract),
                            )
                            .await
                    }
                    Err(err) => Err(err),
                };

                let put_result = match result {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        tracing::warn!(
                            contract = %contract_key,
                            error = %err,
                            "Failed to upsert contract for delegate PutContractRequest"
                        );
                        Err(format!("{err}"))
                    }
                };

                inbound_responses.push(InboundDelegateMsg::PutContractResponse(
                    PutContractResponse {
                        contract_id: *contract_key.id(),
                        result: put_result,
                        context,
                    },
                ));
            }
        }

        // Process GET requests
        if !get_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = get_requests.len(),
                "Processing GetContractRequest messages from delegate"
            );

            for req in get_requests {
                let contract_id = req.contract_id;
                let context = req.context;

                // Look up the full key from the instance id
                let state = match contract_handler.executor().lookup_key(&contract_id) {
                    Some(full_key) => {
                        // Fetch the contract state
                        match contract_handler
                            .executor()
                            .fetch_contract(full_key, false)
                            .await
                        {
                            Ok((state, _)) => state,
                            Err(err) => {
                                tracing::warn!(
                                    contract = %contract_id,
                                    error = %err,
                                    "Failed to fetch contract for delegate GetContractRequest"
                                );
                                None
                            }
                        }
                    }
                    None => {
                        tracing::debug!(
                            contract = %contract_id,
                            "Contract not found locally for delegate GetContractRequest"
                        );
                        None
                    }
                };

                inbound_responses.push(InboundDelegateMsg::GetContractResponse(
                    GetContractResponse {
                        contract_id,
                        state,
                        context,
                    },
                ));
            }
        }

        // Process UPDATE requests (fire-and-forget: apply update, send result back).
        // Like PUT, this calls upsert_contract_state which propagates to the network
        // automatically via BroadcastStateChange when the state actually changes.
        // Only UpdateData::State and UpdateData::Delta are supported; compound variants
        // (StateAndDelta, Related*) are rejected because the delegate API doesn't
        // currently provide the related contract information needed to resolve them.
        if !update_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = update_requests.len(),
                "Processing UpdateContractRequest messages from delegate"
            );

            for req in update_requests {
                let contract_id = req.contract_id;
                let context = req.context;

                // Look up the full key from the instance id
                let result = match contract_handler.executor().lookup_key(&contract_id) {
                    Some(full_key) => {
                        let update_value: Either<WrappedState, StateDelta<'static>> = match req
                            .update
                        {
                            freenet_stdlib::prelude::UpdateData::State(state) => {
                                Either::Left(WrappedState::from(state.into_bytes()))
                            }
                            freenet_stdlib::prelude::UpdateData::Delta(delta) => {
                                Either::Right(delta)
                            }
                            // StateAndDelta, RelatedState, RelatedDelta, RelatedStateAndDelta
                            // are not supported because the delegate API doesn't provide the
                            // related contract context needed to resolve them. Future
                            // `UpdateData` variants (the enum is `#[non_exhaustive]` since
                            // stdlib 0.6.0) fall through the same "unsupported" branch and
                            // are similarly rejected with a warn-level log.
                            other @ freenet_stdlib::prelude::UpdateData::StateAndDelta {
                                ..
                            }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedState { .. }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedDelta { .. }
                            | other @ freenet_stdlib::prelude::UpdateData::RelatedStateAndDelta {
                                ..
                            }
                            | other => {
                                tracing::warn!(
                                    contract = %contract_id,
                                    variant = ?std::mem::discriminant(&other),
                                    "Unsupported UpdateData variant in delegate UpdateContractRequest \
                                     (only State and Delta are supported)"
                                );
                                inbound_responses.push(InboundDelegateMsg::UpdateContractResponse(
                                    UpdateContractResponse {
                                        contract_id,
                                        result: Err("Unsupported UpdateData variant".to_string()),
                                        context,
                                    },
                                ));
                                continue;
                            }
                        };

                        // Deferrable, exactly as the PUT arm above (#5544
                        // stall 1): a missing related contract off-loads its
                        // network GET instead of pinning the serial loop.
                        let outcome = contract_handler
                            .executor()
                            .upsert_contract_state_deferrable(
                                full_key,
                                update_value.clone(),
                                RelatedContracts::default(),
                                None,
                            )
                            .await;
                        match outcome {
                            Ok(UpsertOutcome::Completed(r)) => Ok(r),
                            Ok(UpsertOutcome::DeferRelated(missing)) if parking.is_some() => {
                                deferred_upserts.push(delegate_park::PendingUpsert {
                                    key: full_key,
                                    update: update_value,
                                    related_contracts: RelatedContracts::default(),
                                    code: None,
                                    is_put: false,
                                    context,
                                    missing,
                                });
                                continue;
                            }
                            Ok(UpsertOutcome::DeferRelated(_)) => {
                                // No parking context: keep the legacy inline
                                // fetch rather than failing the update.
                                contract_handler
                                    .executor()
                                    .upsert_contract_state(
                                        full_key,
                                        update_value,
                                        RelatedContracts::default(),
                                        None,
                                    )
                                    .await
                            }
                            Err(err) => Err(err),
                        }
                    }
                    None => {
                        tracing::debug!(
                            contract = %contract_id,
                            "Contract not found locally for delegate UpdateContractRequest"
                        );
                        inbound_responses.push(InboundDelegateMsg::UpdateContractResponse(
                            UpdateContractResponse {
                                contract_id,
                                result: Err("Contract not found".to_string()),
                                context,
                            },
                        ));
                        continue;
                    }
                };

                let update_result = match result {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        tracing::warn!(
                            contract = %contract_id,
                            error = %err,
                            "Failed to update contract for delegate UpdateContractRequest"
                        );
                        Err(format!("{err}"))
                    }
                };

                inbound_responses.push(InboundDelegateMsg::UpdateContractResponse(
                    UpdateContractResponse {
                        contract_id,
                        result: update_result,
                        context,
                    },
                ));
            }
        }

        // Process SUBSCRIBE requests
        // There are two registration paths that converge on DELEGATE_SUBSCRIPTIONS:
        // 1. V2 delegates: subscribe_contract() host function (native_api.rs) registers
        //    during WASM execution and returns success/error synchronously.
        // 2. V1 delegates: emit SubscribeContractRequest in process() outbound, handled here.
        // Both paths are idempotent — inserting the same (contract_id, delegate_key) twice
        // is a no-op on the HashSet. After registration, the delegate receives
        // ContractNotification messages when the subscribed contract's state changes.
        //
        // TODO(#2830): UnsubscribeContractRequest is not yet handled. Delegates can
        // only unsubscribe implicitly via UnregisterDelegate cleanup.
        if !subscribe_requests.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = subscribe_requests.len(),
                "Processing SubscribeContractRequest messages from delegate"
            );

            for req in subscribe_requests {
                let contract_id = req.contract_id;
                let context = req.context;

                // Validate contract existence before registering (matches V2 host function behavior)
                let result = if contract_handler
                    .executor()
                    .lookup_key(&contract_id)
                    .is_some()
                {
                    // Register subscription in the global registry
                    crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS
                        .entry(contract_id)
                        .or_default()
                        .insert(delegate_key.clone());
                    Ok(())
                } else {
                    tracing::debug!(
                        contract = %contract_id,
                        "Contract not found locally for delegate SubscribeContractRequest"
                    );
                    Err("Contract not found".to_string())
                };

                inbound_responses.push(InboundDelegateMsg::SubscribeContractResponse(
                    SubscribeContractResponse {
                        contract_id,
                        result,
                        context,
                    },
                ));
            }
        }

        // Deliver delegate-to-delegate messages (fire-and-forget, single-hop).
        //
        // Design notes:
        // - Delivery is intentionally single-hop: if target delegate B emits its own
        //   SendDelegateMessage in response, it is filtered out (not delivered). This
        //   prevents infinite recursion between delegates. We use execute_delegate_request
        //   (not the full delivery loop) for the same reason.
        // - Target delegate receives empty params because params are not stored in the
        //   delegate registry — they are passed per-request at the API layer. If a target
        //   delegate's process() relies on params, the caller must use ApplicationMessages
        //   directly. This is a known v1 limitation.
        // - The caller's delegate key is passed as `caller_delegate` so the receiver
        //   sees `MessageOrigin::Delegate(caller_key)` and can authorize on it
        //   (issue #3860). The runtime attests this identity — the calling delegate
        //   cannot forge it.
        // - Inter-delegate delivery runs ONLY for a client-driven run
        //   (`InterDelegateDispatch::Allowed`). It is SUPPRESSED for a
        //   contract-notification-driven run — see the guard below. An earlier
        //   version of this comment asserted that notification-driven runs did
        //   not reach this code; that was false, because
        //   `handle_delegate_notification` calls THIS function, and the guard is
        //   what finally makes the statement true.
        if !delegate_messages.is_empty() && inter_delegate == InterDelegateDispatch::Allowed {
            tracing::debug!(
                delegate_key = %delegate_key,
                count = delegate_messages.len(),
                "Delivering delegate-to-delegate messages"
            );

            for msg in delegate_messages {
                let target_key = msg.target.clone();
                let inbound = vec![InboundDelegateMsg::DelegateMessage(msg)];
                let target_req = DelegateRequest::ApplicationMessages {
                    key: target_key.clone(),
                    params: Parameters::from(Vec::new()),
                    inbound,
                };
                // PER-DELEGATE EXCLUSION, second door (#5544 B2).
                //
                // This is the SECOND call site of `execute_delegate_request`,
                // and it invokes the TARGET delegate, not the calling one — so
                // `dispatch_delegate_request`'s check upstream says nothing
                // about it. Delivering here while the target is parked would
                // run its `process()` mid-round-trip and clobber the parked
                // continuation's `DelegateContextCache` entry: the exact
                // corruption the exclusion exists to prevent, reached through a
                // different door.
                //
                // DROPPED rather than queued, deliberately. The hop is already
                // single-hop, fire-and-forget, and entirely suppressed on
                // notification-driven runs, so callers cannot rely on delivery.
                // Queueing it would mean replaying an ATTESTED caller identity
                // (`Some(delegate_key)`, the control that replaced the deleted
                // registration-record refusal — GHSA-824h-7x5x-wfmf) at an
                // arbitrarily later time under a scope captured earlier, and
                // deferring an attestation is not something to introduce as a
                // side effect of a stall fix. Losing a fire-and-forget message
                // beats corrupting the target's state.
                //
                // `info!`, not `debug!`: the crate sets `release_max_level_info`
                // so a `debug!` compiles out of shipped binaries and a delegate
                // author would have no way to see why their message vanished —
                // the same reasoning as the suppression log below.
                if parking
                    .as_ref()
                    .is_some_and(|ctx| ctx.park.is_parked(&target_key))
                {
                    tracing::info!(
                        from_delegate = %delegate_key,
                        target_delegate = %target_key,
                        "Dropped an inter-delegate message: the target is parked \
                         mid-round-trip and running it now would clobber its \
                         parked continuation (#5544)"
                    );
                    continue;
                }

                match contract_handler
                    .executor()
                    // Inter-delegate hop: `user_context = None`. A
                    // delegate-to-delegate message does NOT carry the
                    // originating connection's per-user secret namespace — the
                    // target delegate's secrets are its own, scoped to whatever
                    // (if any) user context its own connections present. This
                    // keeps the per-user namespace bound to the connection
                    // boundary, not propagated transitively through delegate
                    // messages (part of the #4381 unforgeability invariant).
                    // The ORIGINATING connection's scope rides along, never a
                    // literal `Local`: a hop driven by an off-host caller must
                    // not manufacture an attested `MessageOrigin::Delegate` that
                    // the caller could not have obtained directly
                    // (GHSA-824h-7x5x-wfmf). This forwarding is the control that
                    // REPLACED the deleted registration-record refusal, so it is
                    // source-pinned by
                    // `inter_delegate_hop_forwards_the_originating_scope`.
                    //
                    // Scope nulls the `origin` ARGUMENT only. `msg.sender` still
                    // rides through unchanged on every path, so the target can
                    // always see which delegate sent it — that field is
                    // runtime-attested (`execution.rs` overwrites it with the key
                    // of the delegate that actually ran) and is a hash of that
                    // delegate's own code, so it is self-authenticating rather
                    // than borrowed authority.
                    .execute_delegate_request(
                        target_req,
                        None,
                        Some(delegate_key),
                        connection_scope,
                        None,
                    )
                    .await
                {
                    Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                        values,
                        ..
                    }) => {
                        // Filter out SendDelegateMessage from target's output to enforce
                        // single-hop delivery. Only accumulate client-visible messages.
                        for value in values {
                            if !matches!(value, OutboundDelegateMsg::SendDelegateMessage(_)) {
                                accumulated_messages.push(value);
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(
                            target_delegate = %target_key,
                            error = %err,
                            "Failed to deliver delegate message (fire-and-forget)"
                        );
                    }
                }
            }
        } else if !delegate_messages.is_empty() {
            // SCOPE-LAUNDERING GUARD (GHSA-824h-7x5x-wfmf).
            //
            // `handle_delegate_notification` invokes this function for a run that
            // descends from a contract state change rather than from any client
            // connection, so it has no connection scope to pass and hardcodes
            // `Local`. That made the notification path a laundry: a non-local
            // caller drives its own delegate (correctly resolving to NO attested
            // origin), the delegate emits `SubscribeContractRequest`, and on the
            // next state change of that contract the SAME delegate re-executes
            // under `Local` — at which point its `SendDelegateMessage` would
            // reach a victim carrying a fully attested
            // `MessageOrigin::Delegate(caller)`. The scope the caller was
            // refused would have been handed back to it one hop later.
            //
            // Suppressing the hop on this path removes the laundering route
            // outright, which is stronger than propagating a scope and is what
            // the function's own design notes always claimed happened.
            //
            // TEST COVERAGE, precisely: this suppression has NO behavioural
            // test. It is covered only by the source scrape
            // `inter_delegate_hop_forwards_the_originating_scope`, which asserts
            // that this call site passes `InterDelegateDispatch::Suppressed` and
            // that the hop forwards a scope variable rather than a literal.
            // Both halves were checked non-vacuous by mutation. What that does
            // NOT cover is the runtime behaviour — that a delegate emitting
            // `SendDelegateMessage` from a notification run really is not
            // delivered to. Exercising that needs a delegate that subscribes,
            // then messages, under a notification-driven run; if you add one,
            // this comment should shrink.
            //
            // Safe to suppress because nothing first-party emits here, verified
            // by reading the delegates rather than by observing tests: River's
            // chat delegate returns only empty or `UpdateContractRequest` from
            // `ContractNotification` and never constructs a `DelegateMessage`,
            // ghostkeys rejects notifications in its catch-all, and the only
            // in-tree emitter of `SendDelegateMessage` sits in the
            // `ApplicationMessage` arm and errors on notifications. "No test
            // relied on it" is deliberately NOT the grounds here — that is the
            // same reasoning that let the tokenless-CLI break through.
            //
            // If
            // notification-driven inter-delegate messaging is ever wanted, it
            // must come back with the originating scope RECORDED ON THE
            // SUBSCRIPTION (both `DELEGATE_SUBSCRIPTIONS` registration paths) and
            // propagated here — never with a hardcoded `Local`.
            //
            // `info!`, not `debug!`: the crate sets `release_max_level_info`, so
            // a `debug!` would compile out of shipped binaries and a delegate
            // author would have no way to see why their message vanished.
            tracing::info!(
                delegate_key = %delegate_key,
                count = delegate_messages.len(),
                "Suppressed delegate-to-delegate messages from a \
                 notification-driven run (GHSA-824h-7x5x-wfmf): this path carries \
                 no client connection scope, so it must not confer an attested \
                 caller identity"
            );
        }

        // Off-load this iteration's SLOW work rather than awaiting it on the
        // serial `contract_handling` loop (#5544). Two kinds, both of which
        // used to pin the loop:
        //
        //   * permission prompts — `prompter.prompt()` waits on a HUMAN for up
        //     to USER_INPUT_TIMEOUT (60s), and production wires the real
        //     DashboardPrompter, so any delegate that prompts froze every
        //     GET/PUT/UPDATE/subscribe on the node for a minute. ghostkeys
        //     prompts in four places.
        //   * deferred related-contract fetches — a delegate PUT/UPDATE whose
        //     contract asks for a related contract this node lacks used to
        //     await a network GET inline, up to RELATED_FETCH_TIMEOUT (10s).
        //
        // Both are parked together under ONE continuation: an iteration can
        // produce both, and two park points would mean the second kind's work
        // was silently dropped when the first parked.
        //
        // Nothing about the delegate requires us to stay here. It is already
        // suspended at the runtime level — `RequestUserInput` breaks out of
        // `process_outbound` and the WASM `process()` call has returned — and
        // the deferrable upsert has already rolled back its partial work. All
        // that is needed is to re-enter the delegate when the results arrive.
        if !user_input_requests.is_empty() || !deferred_upserts.is_empty() {
            tracing::debug!(
                delegate_key = %delegate_key,
                prompts = user_input_requests.len(),
                deferred_upserts = deferred_upserts.len(),
                "Off-loading slow delegate work from the contract-handling loop"
            );

            // The caller identity passed to the prompter is built from the
            // executor's runtime context, NOT from anything the delegate could
            // influence, and `origin_contract` is already gated on
            // `connection_scope.is_local()` at the top of this function
            // (GHSA-824h-7x5x-wfmf). See `caller_identity_from_origin`.
            let caller = caller_identity_from_origin(origin_contract);
            let delegate_key_str = delegate_key.to_string();

            if let Some(ctx) = parking.as_mut() {
                let continuation = delegate_park::Continuation {
                    params: current_params.clone(),
                    origin_contract: origin_contract.copied(),
                    connection_scope,
                    user_context: user_context.cloned(),
                    inter_delegate,
                    accumulated: std::mem::take(&mut accumulated_messages),
                    inbound_so_far: std::mem::take(&mut inbound_responses),
                    // Attached by the caller right after we return `Parked` (it
                    // owns the channel), or carried straight through if this run
                    // is itself a resume that is parking again.
                    responder: ctx.carried_responder.take(),
                    delivery: ctx.delivery,
                };
                match ctx.park.park(delegate_key.clone(), continuation) {
                    delegate_park::ParkAdmission::Admitted => {
                        let guard = delegate_park::ParkGuard::new(
                            ctx.park.resume_tx().clone(),
                            delegate_key.clone(),
                        );
                        let prompter = std::sync::Arc::clone(prompter);
                        let key = delegate_key.clone();
                        let op_manager = contract_handler.executor().op_manager_handle();
                        let prompts = std::mem::take(&mut user_input_requests);
                        let upserts = std::mem::take(&mut deferred_upserts);
                        // Fire-and-forget is safe precisely because of the
                        // guard: it delivers exactly one resume even if this
                        // task is dropped, panics or is cancelled, so the park
                        // always ends, its pending queue always drains and the
                        // parked client is always answered.
                        //
                        // NOTE, load-bearing: this task deliberately captures
                        // NO `ContractHandler` and no executor — only the
                        // prompter, an OpManager handle and plain data. That is
                        // what keeps delegate `process()` globally serial on the
                        // loop even though the loop is now released mid-
                        // round-trip, which #5490's non-atomic
                        // `state_content_changed` gate depends on. Do not hand
                        // this task the handler. See `delegate_park`'s module
                        // docs, "What parking does NOT relax".
                        GlobalExecutor::spawn(async move {
                            // Prompts and fetches run CONCURRENTLY, and the whole
                            // body is capped by PARK_WORK_BUDGET. Sequentially
                            // they could sum past PARK_TTL, at which point the
                            // loop's backstop sweep would force-resume while this
                            // task was still running and its result would be
                            // discarded. Keeping the budget below the TTL means
                            // the guard always wins the race.
                            let fetches = async {
                                futures::future::join_all(upserts.into_iter().map(|pending| {
                                    let op_manager = op_manager.clone();
                                    async move {
                                        let fetched = fetch_related_off_loop(
                                            op_manager,
                                            pending.missing.clone(),
                                        )
                                        .await;
                                        delegate_park::ResolvedUpsert { pending, fetched }
                                    }
                                }))
                                .await
                            };
                            let answers = async {
                                if prompts.is_empty() {
                                    Vec::new()
                                } else {
                                    run_user_input_prompts(
                                        prompter.as_ref(),
                                        prompts,
                                        &key,
                                        &delegate_key_str,
                                        caller,
                                    )
                                    .await
                                }
                            };
                            let done =
                                tokio::time::timeout(delegate_park::PARK_WORK_BUDGET, async {
                                    tokio::join!(answers, fetches)
                                })
                                .await;
                            match done {
                                Ok((responses, resolved)) => guard.send(responses, resolved),
                                Err(_) => {
                                    tracing::warn!(
                                        delegate = %key,
                                        "Off-loop delegate work exceeded PARK_WORK_BUDGET; \
                                         resuming empty so the round-trip terminates"
                                    );
                                    guard.send(Vec::new(), Vec::new());
                                }
                            }
                        });
                        return DelegateRunOutcome::Parked;
                    }
                    delegate_park::ParkAdmission::Refused(continuation) => {
                        // Node-wide park cap reached. Fall through to the
                        // pre-#5544 inline waits: they stall the loop, which is
                        // what this change exists to stop, but silently losing a
                        // user's permission prompt or a delegate's write would
                        // be worse. Restore what the refused continuation held.
                        let continuation = *continuation;
                        accumulated_messages = continuation.accumulated;
                        inbound_responses = continuation.inbound_so_far;
                        *ctx.carried_responder = continuation.responder;
                    }
                }
            }

            // Inline fallback: no parking context (direct unit-test calls), or
            // the park cap was hit.
            for pending in std::mem::take(&mut deferred_upserts) {
                inbound_responses.push(run_deferred_upsert_inline(contract_handler, pending).await);
            }
            if !user_input_requests.is_empty() {
                inbound_responses.extend(
                    run_user_input_prompts(
                        prompter.as_ref(),
                        std::mem::take(&mut user_input_requests),
                        delegate_key,
                        &delegate_key_str,
                        caller,
                    )
                    .await,
                );
            }
        }

        // Create a new request to send the responses back to the delegate
        current_req = DelegateRequest::ApplicationMessages {
            key: delegate_key.clone(),
            inbound: inbound_responses,
            params: current_params.clone(),
        };
    }
}

/// Receive a delegate notification from the optional channel.
/// Returns `std::future::pending()` if the channel is `None` or closed,
/// so the `tokio::select!` branch is effectively disabled.
async fn recv_delegate_notification(
    rx: &mut Option<DelegateNotificationReceiver>,
) -> executor::DelegateNotification {
    match rx {
        Some(rx) => match rx.recv().await {
            Some(n) => n,
            None => std::future::pending().await,
        },
        None => std::future::pending().await,
    }
}

/// Try to receive a delegate notification without blocking.
/// Returns `None` if the channel is `None`, closed, or has no pending notifications.
fn try_recv_delegate_notification(
    rx: &mut Option<DelegateNotificationReceiver>,
) -> Option<executor::DelegateNotification> {
    match rx {
        Some(rx) => rx.try_recv().ok(),
        None => None,
    }
}

/// Build a `CallerIdentity` for the permission prompt UI from the executor's
/// runtime origin context (see #3857).
///
/// The mapping is intentionally narrow: today the only attested non-`None`
/// caller is a web app via `MessageOrigin::WebApp(..)`. Every other path
/// (delegate-to-delegate, local client, missing attestation) collapses to
/// `CallerIdentity::None` and renders as `"No app caller"` in the UI.
/// Issue #3860 tracks adding a `MessageOrigin::Delegate(DelegateKey)` variant
/// and the corresponding `CallerIdentity::Delegate` row; until that lands,
/// inter-delegate calls are not distinguishable here from "no caller at all".
///
/// Extracted as a free function (rather than inlined at the call site) so
/// the mapping can be unit-tested in isolation. Without this, swapping the
/// `Some` and `None` arms — or accidentally wiring a delegate-controlled
/// value into the prompter — would not fail any test.
fn caller_identity_from_origin(origin: Option<&ContractInstanceId>) -> CallerIdentity {
    match origin {
        Some(id) => CallerIdentity::WebApp(id.to_string()),
        None => CallerIdentity::None,
    }
}

/// Inject a related contract's full state into `related_contracts` for
/// downstream propagation to the contract WASM as a `RelatedState` entry.
///
/// `RelatedContracts` exposes no public `insert`, so we use the documented
/// `missing()` + `update()` pattern: `missing()` registers the id with a
/// `None` slot, and `update()` returns a mutable iterator we can use to set
/// the slot. The slot is guaranteed to exist after `missing()`; if a caller
/// previously supplied a state for the same id via the bare
/// `related_contracts` argument on `UpdateQuery`, we overwrite it — the
/// inline `RelatedStateAndDelta` payload is the atomic package the sender
/// committed to and is preferred over any out-of-band hint.
///
/// Extracted as a free function so the conversion can be unit-tested in
/// isolation. Without that, regressions in `RelatedContracts::missing()` or
/// `update()` semantics — for instance if `missing()` ever stopped
/// guaranteeing the slot exists — would silently drop the inline state and
/// surface only as missing-message bugs in higher-level integration tests.
fn inject_related_state(
    related_contracts: &mut RelatedContracts<'static>,
    related_to: ContractInstanceId,
    state: freenet_stdlib::prelude::State<'static>,
) {
    related_contracts.missing(vec![related_to]);
    let mut state_holder = Some(state);
    for (id, slot) in related_contracts.update() {
        if id == &related_to {
            *slot = state_holder.take();
            break;
        }
    }
    debug_assert!(
        state_holder.is_none(),
        "RelatedContracts::missing() must register the slot it just declared, \
         so update() should always have a slot for the requested id"
    );
}

pub(crate) async fn contract_handling<CH, P>(
    mut contract_handler: CH,
    prompter: P,
) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter + 'static,
{
    // `Arc` so a parked delegate's off-loop prompt task can hold the prompter
    // while the loop keeps running (#5544). `DashboardPrompter` is not `Clone`
    // (it owns a `Mutex`), and the task outlives this call frame.
    let prompter = std::sync::Arc::new(prompter);
    let mut delegate_rx = contract_handler.executor().take_delegate_notification_rx();
    let mut fair_queue = fair_queue::FairEventQueue::new();

    // Resume channel for PUT/UPDATE operations whose related-contract fetch was
    // off-loaded from this serial loop (#4391). Off-loop waiter tasks send the
    // fetched states back here; the loop re-runs the upsert ON the loop so WASM
    // stays serial. Unbounded is acceptable per channel-safety.md's carve-out:
    // the producer count is bounded by MAX_INFLIGHT_DEFERRALS, the receiver is
    // this loop (which drains every iteration), and there is no cycle (the
    // waiter never reads what the loop produces). The waiter sends with a
    // non-blocking `unbounded_send` (no `.await`), so it can never back-pressure
    // anything.
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel::<DeferredResume>();
    // Resume channel for hosted-mode EXPORTS off-loaded from this serial loop
    // (#4531 / #4381 P5). The off-loop export task sends back the executor (to
    // return to the pool) + the result (for the client). Unbounded for the same
    // channel-safety carve-out as `resume_tx`: producers bounded by
    // MAX_CONCURRENT_EXPORTS, the receiver is this loop (drains every iteration),
    // no cycle, non-blocking `unbounded_send`.
    let (export_resume_tx, mut export_resume_rx) =
        tokio::sync::mpsc::unbounded_channel::<ExportResume>();
    let mut deferral_ctx = DeferralCtx::new(resume_tx, export_resume_tx);
    // Resume channel for delegates PARKED off this loop (#5544). Same
    // channel-safety carve-out as the two above: producers are bounded by
    // MAX_PARKED_DELEGATES, the receiver is this loop (drains every
    // iteration), the off-loop task never reads what the loop produces, and
    // the send is a non-blocking `unbounded_send`.
    let (delegate_resume_tx, mut delegate_resume_rx) =
        tokio::sync::mpsc::unbounded_channel::<delegate_park::DelegateResume>();
    let mut park_ctx = delegate_park::DelegateParkCtx::new(delegate_resume_tx);

    loop {
        // Drain resumed (deferred) upserts so a completed off-loop fetch is
        // applied promptly and its client answered, ahead of new queued work —
        // but cap the batch (each resume is a full WASM upsert) and interleave
        // with the fair queue so resumes can't head-of-line-block cached GETs.
        for _ in 0..MAX_RESUME_DRAIN_BATCH {
            match resume_rx.try_recv() {
                Ok(resume) => {
                    handle_deferred_resume(&mut contract_handler, &mut deferral_ctx, resume)
                        .await?;
                }
                Err(_) => break,
            }
        }

        // Resume delegates whose parked work has landed (#5544), ahead of new
        // queued work: a parked delegate is holding a client responder and,
        // possibly, requests queued behind it, so finishing it first keeps
        // latency down and releases the exclusion sooner. Bounded per
        // iteration for the same reason as the deferred-upsert drain — each
        // resume re-enters WASM — and interleaved with the fair queue so
        // resumes cannot head-of-line-block ordinary contract ops.
        for _ in 0..MAX_RESUME_DRAIN_BATCH {
            match delegate_resume_rx.try_recv() {
                Ok(resume) => {
                    handle_delegate_resume(&mut contract_handler, &mut park_ctx, &prompter, resume)
                        .await;
                }
                Err(_) => break,
            }
        }

        // Backstop sweep for parks that neither completed nor were dropped
        // (see `PARK_TTL`). Force-resume them so a wedged delegate cannot stay
        // wedged: the resume drains its pending queue and answers its client.
        for delegate_key in park_ctx.expired(tokio::time::Instant::now()) {
            tracing::warn!(
                delegate = %delegate_key,
                "Delegate park exceeded PARK_TTL — force-resuming"
            );
            handle_delegate_resume(
                &mut contract_handler,
                &mut park_ctx,
                &prompter,
                delegate_park::DelegateResume {
                    delegate_key,
                    cause: delegate_park::ResumeCause::TimedOut,
                    inbound: Vec::new(),
                    upserts: Vec::new(),
                },
            )
            .await;
        }

        // Drain completed off-loop EXPORTS (#4531 / #4381 P5): return/replace the
        // executor and answer the parked client. Bounded by MAX_CONCURRENT_EXPORTS
        // (the export semaphore), so this batch is tiny; each iteration is a pool
        // return + a oneshot send (microseconds).
        for _ in 0..executor::runtime::MAX_CONCURRENT_EXPORTS {
            match export_resume_rx.try_recv() {
                Ok(resume) => {
                    handle_export_resume(&mut contract_handler, resume).await;
                }
                Err(_) => break,
            }
        }

        // Drain pending events from the channel into the fair queue (bounded batch).
        // Capped at MAX_DRAIN_BATCH (256) to bound the synchronous work per iteration
        // before yielding back to the Tokio scheduler. Each iteration of this loop is
        // a non-blocking try_recv + HashMap insert, so 256 iterations complete in
        // single-digit microseconds even at peak load.
        for _ in 0..fair_queue::MAX_DRAIN_BATCH {
            match contract_handler.channel().try_recv_from_sender()? {
                Some((id, event, priority)) => {
                    // Process ClientDisconnect inline — it's a lightweight cleanup
                    // operation that should never be delayed or compete with
                    // DelegateRequest events for the default queue's limited capacity.
                    if let ContractHandlerEvent::ClientDisconnect { client_id } = &event {
                        let client_id = *client_id;
                        contract_handler.executor().remove_client(client_id);
                        // Drop any delegate-notification routing registrations
                        // for this client (#3275).
                        delegate_app_registry::remove_client(client_id);
                        contract_handler.channel().drop_waiting_response(id);
                        continue;
                    }
                    push_with_background_eviction(
                        &mut contract_handler,
                        &mut fair_queue,
                        id,
                        event,
                        priority,
                    )
                    .await;
                }
                None => break,
            }
        }

        // Drain delegate notifications (non-blocking, bounded) even when the fair queue
        // has work. This prevents delegate starvation under sustained contract load.
        // We drain up to MAX_DELEGATE_DRAIN_BATCH to handle bursts (e.g., a contract
        // update triggering many delegate notifications simultaneously).
        for _ in 0..MAX_DELEGATE_DRAIN_BATCH {
            match try_recv_delegate_notification(&mut delegate_rx) {
                Some(notification) => {
                    handle_delegate_notification(
                        &mut contract_handler,
                        notification,
                        &prompter,
                        Some(&mut park_ctx),
                    )
                    .await;
                }
                None => break,
            }
        }

        // Process one event from the fair queue in normal round-robin order.
        // There is NO per-contract holding: an event for a contract whose
        // PUT/UPDATE is mid-deferral runs immediately and may observe the
        // pre-deferral state. The deferring op's own response is still delivered
        // only on commit (its responder is parked in `deferral_ctx.stashed`), so
        // its causality is preserved; concurrent same-key ops reconcile via the
        // contract's CRDT merge on resume. This reordering is intended (#4391).
        if let Some((id, event)) = fair_queue.pop() {
            handle_contract_event(
                &mut contract_handler,
                id,
                event,
                &prompter,
                Some(&mut deferral_ctx),
                Some(&mut park_ctx),
            )
            .await?;
            continue;
        }

        // Fair queue is empty. Block-wait for a new event, a resumed deferral,
        // a delegate notification — or the park backstop deadline.
        //
        // That last arm is load-bearing (#5544 B6). Without it the sweep at the
        // top of this loop only runs when some UNRELATED event happens to wake
        // the select, so on a quiet node a wedged park would be swept late or
        // never — and a quiet node is the normal state for a background peer,
        // and precisely the condition under which a prompt goes unanswered
        // because no dashboard tab is open. A backstop whose firing depends on
        // other traffic is not a backstop.
        //
        // `pending()` when nothing is parked, so an idle node with no parks
        // still blocks indefinitely rather than spinning on a timer.
        let park_deadline = park_ctx.next_sweep_deadline();
        tokio::select! {
            result = contract_handler.channel().recv_from_sender() => {
                let (id, event, priority) = result?;
                push_with_background_eviction(
                    &mut contract_handler,
                    &mut fair_queue,
                    id,
                    event,
                    priority,
                )
                .await;
            }
            Some(resume) = resume_rx.recv() => {
                handle_deferred_resume(&mut contract_handler, &mut deferral_ctx, resume).await?;
            }
            Some(export_resume) = export_resume_rx.recv() => {
                handle_export_resume(&mut contract_handler, export_resume).await;
            }
            () = async {
                match park_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                // Wake only; the sweep itself runs at the top of the next
                // iteration, so there is exactly one sweep implementation.
            }
            Some(delegate_resume) = delegate_resume_rx.recv() => {
                handle_delegate_resume(
                    &mut contract_handler,
                    &mut park_ctx,
                    &prompter,
                    delegate_resume,
                )
                .await;
            }
            notification = recv_delegate_notification(&mut delegate_rx) => {
                handle_delegate_notification(
                    &mut contract_handler,
                    notification,
                    &prompter,
                    Some(&mut park_ctx),
                )
                .await;
            }
        }
    }
}

/// Re-run a deferred PUT/UPDATE upsert ON the serial loop once its related
/// contracts have been fetched off-loop, then answer the original client.
///
/// The fetched related states are injected into `related_contracts` so the
/// resumed upsert finds them locally and does NOT re-fetch. The upsert runs
/// via the deferrable path (`upsert_contract_state_deferrable`) again, so if the
/// contract *still* requests a DIFFERENT related contract it cannot defer again
/// — the second `DeferRelated` is converted to `MissingRelated` (the depth=1 /
/// one-deferral cap of #4391).
///
/// The upsert re-reads CURRENT state, so concurrent same-key deferrals (or any
/// same-key op that ran while this one was deferred) are reconciled by the
/// contract's CRDT merge — there is no lost update. The client is answered
/// exactly once via its stashed responder (the `ResumeGuard` guarantees this
/// resume runs exactly once per deferral, even if the waiter was dropped).
/// Send an `ExportUserSecretsResponse` to a still-waiting client inline (the
/// rejection paths: no deferral ctx, busy, unsupported). Logs at debug if the
/// client already disconnected.
async fn send_export_response_inline<CH>(
    contract_handler: &mut CH,
    id: handler::EventId,
    result: Result<Vec<u8>, ExecutorError>,
) where
    CH: ContractHandler + Send + 'static,
{
    if let Err(error) = contract_handler
        .channel()
        .send_to_sender(id, ContractHandlerEvent::ExportUserSecretsResponse(result))
        .await
    {
        tracing::debug!(
            error = %error,
            "Failed to send EXPORT response (client may have disconnected)"
        );
    }
}

/// Handle a completed off-loop export (#4531 / #4381 P5): return/replace the
/// executor in the pool and answer the parked client. Runs ON the loop (the
/// pool return needs `&mut contract_handler`), but the work is just a pool
/// slot-return + a oneshot send — microseconds, never the export itself.
async fn handle_export_resume<CH>(contract_handler: &mut CH, resume: ExportResume)
where
    CH: ContractHandler + Send + 'static,
{
    let ExportResume { done, responder } = resume;
    // Return/replace the executor and recover the result for the client.
    let result = contract_handler.executor().finish_export(done).await;
    // Answer the parked client. `responder` is `None` only if the client
    // disconnected before the export finished (we still reclaimed the executor).
    if let Some(responder) = responder {
        if let Err(error) =
            responder.respond(ContractHandlerEvent::ExportUserSecretsResponse(result))
        {
            tracing::debug!(
                error = %error,
                "Failed to deliver EXPORT resume response (client may have disconnected)"
            );
        }
    }
}

/// Send the `ImportSecretsResponse` to the waiting client. Because the live
/// import (#4592) runs fully ON the contract loop (no off-loop deferral, unlike
/// the export), this is the SOLE response path for the import and carries EVERY
/// outcome — success and failure alike — not just rejections. Logs at debug if
/// the client already disconnected.
async fn send_import_response_inline<CH>(
    contract_handler: &mut CH,
    id: handler::EventId,
    result: Result<crate::wasm_runtime::secret_export::ImportReport, ExecutorError>,
) where
    CH: ContractHandler + Send + 'static,
{
    if let Err(error) = contract_handler
        .channel()
        .send_to_sender(id, ContractHandlerEvent::ImportSecretsResponse(result))
        .await
    {
        tracing::debug!(
            error = %error,
            "Failed to send IMPORT response (client may have disconnected)"
        );
    }
}

/// Resume a PUT/UPDATE upsert whose related-contract fetch was deferred off-loop
/// (#4391). Runs ON the contract loop. Reclaims the parked client responder,
/// injects the off-loop-fetched related states so the re-run resolves them
/// locally (no second network round trip), re-runs the upsert in DEFERRABLE mode
/// (a further `DeferRelated` is converted to `MissingRelated` — the one-deferral
/// cap), then answers the parked client exactly once. This is the related-contract
/// upsert resume path, NOT a secrets-import path: the live import (#4592) runs
/// fully on-loop and never defers (see the `ImportSecrets` arm and
/// [`send_import_response_inline`]).
async fn handle_deferred_resume<CH>(
    contract_handler: &mut CH,
    deferral_ctx: &mut DeferralCtx,
    resume: DeferredResume,
) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    let DeferredResume {
        deferral_id,
        key,
        update,
        mut related_contracts,
        code,
        is_put,
        fetched,
    } = resume;

    // Reclaim the parked client responder. It may legitimately be `None` only if
    // the client disconnected (its receiver dropped); we still run the upsert for
    // its side effects.
    let responder = deferral_ctx.stashed.remove(&deferral_id);
    if responder.is_none() {
        tracing::debug!(contract = %key, "Deferred resume has no stashed responder");
    }

    let event_result = match fetched {
        Ok(states) => {
            // Inject the off-loop-fetched related states so the resumed upsert
            // resolves them locally (no second network round trip).
            for (id, state) in states {
                inject_related_state(
                    &mut related_contracts,
                    id,
                    freenet_stdlib::prelude::State::from(state.as_ref().to_vec()),
                );
            }
            // Preserve the original incoming full state (PUT only) for the
            // NoChange response, since `update` is consumed by the upsert.
            let incoming_state = match &update {
                Either::Left(state) => Some(state.clone()),
                Either::Right(_) => None,
            };
            // Re-run the upsert ON the loop in DEFERRABLE mode again. With the
            // related states supplied, validation/merge resolves locally and
            // returns `Completed`. The one-deferral cap (#4391): if the contract
            // now requests a DIFFERENT related contract not held locally, the
            // deferrable path returns `DeferRelated` again — we convert that to
            // MissingRelated rather than spawning a second off-loop fetch, so a
            // misbehaving contract cannot defer indefinitely and the network
            // wait never re-enters the loop.
            let result = match contract_handler
                .executor()
                .upsert_contract_state_deferrable(key, update, related_contracts, code)
                .instrument(tracing::info_span!("upsert_contract_state_resumed", %key))
                .await
            {
                Ok(UpsertOutcome::Completed(result)) => Ok(result),
                Ok(UpsertOutcome::DeferRelated(missing)) => {
                    tracing::debug!(
                        contract = %key,
                        missing = missing.len(),
                        "Resumed upsert still needs a related contract — surfacing \
                         MissingRelated (one-deferral cap, #4391)"
                    );
                    Err(ExecutorError::missing_related(
                        missing.first().copied().unwrap_or_else(|| *key.id()),
                    ))
                }
                Err(err) => Err(err),
            };
            if is_put {
                let incoming_state =
                    incoming_state.unwrap_or_else(|| WrappedState::new(Vec::new()));
                put_response_from_result(contract_handler, key, incoming_state, result).await?
            } else {
                update_response_from_result(contract_handler, key, result).await?
            }
        }
        Err(err) => {
            // Off-loop fetch failed/timed out: surface the related-fetch error
            // to the client (do NOT defer again).
            tracing::debug!(
                contract = %key,
                error = %err,
                "Deferred related-contract fetch failed; surfacing to client"
            );
            if is_put {
                ContractHandlerEvent::PutResponse {
                    new_value: Err(err),
                    state_changed: false,
                }
            } else {
                ContractHandlerEvent::UpdateResponse {
                    new_value: Err(err),
                    state_changed: false,
                }
            }
        }
    };

    // The resumed upsert has committed (or errored). Answer the parked client
    // exactly once.
    if let Some(responder) = responder {
        if let Err(error) = responder.respond(event_result) {
            tracing::debug!(
                error = %error,
                contract = %key,
                "Failed to deliver deferred upsert response (client may have disconnected)"
            );
        }
    }
    Ok(())
}

/// Result of attempting a deferrable upsert in the PUT/UPDATE arms.
enum DeferStep {
    /// The upsert ran to completion (no related fetch needed, or it was
    /// resolvable locally). Build the response from this result.
    Completed(Result<UpsertResult, ExecutorError>),
    /// The upsert deferred its related-contract fetch off-loop. The client
    /// responder has been parked; the arm must return immediately and let the
    /// resume path answer the client later.
    Deferred,
}

/// Run an upsert in deferrable mode when a [`DeferralCtx`] is available,
/// off-loading the related-contract network fetch from the serial loop (#4391).
///
/// When `deferral` is `None` (delegate-driven PUTs, direct unit tests), this
/// uses the legacy inline `upsert_contract_state` and always returns
/// `Completed` — preserving existing behavior for callers that don't drive the
/// serial loop.
///
/// When the in-flight deferral cap is reached, it does NOT fall back to an
/// inline network fetch (that would re-introduce the head-of-line stall on the
/// loop for the over-cap op). Instead it surfaces `MissingRelated` immediately
/// — backpressure that fails fast rather than blocking the loop (#4391 SF#4).
#[allow(clippy::too_many_arguments)]
async fn maybe_defer_upsert<CH>(
    contract_handler: &mut CH,
    deferral: Option<&mut DeferralCtx>,
    id: &handler::EventId,
    key: ContractKey,
    update: Either<WrappedState, StateDelta<'static>>,
    related_contracts: RelatedContracts<'static>,
    code: Option<ContractContainer>,
    is_put: bool,
) -> DeferStep
where
    CH: ContractHandler + Send + 'static,
{
    // No deferral context → legacy inline path (delegate-driven PUTs, direct
    // unit-test calls). Behavior is unchanged for these callers.
    let Some(deferral) = deferral else {
        return DeferStep::Completed(
            contract_handler
                .executor()
                .upsert_contract_state(key, update, related_contracts, code)
                .instrument(tracing::info_span!("upsert_contract_state", %key))
                .await,
        );
    };

    let outcome = contract_handler
        .executor()
        .upsert_contract_state_deferrable(
            key,
            update.clone(),
            related_contracts.clone(),
            code.clone(),
        )
        .instrument(tracing::info_span!("upsert_contract_state_deferrable", %key))
        .await;

    let missing = match outcome {
        Ok(UpsertOutcome::Completed(result)) => return DeferStep::Completed(Ok(result)),
        Ok(UpsertOutcome::DeferRelated(missing)) => missing,
        Err(err) => return DeferStep::Completed(Err(err)),
    };

    // At the in-flight cap: fail fast with MissingRelated instead of doing an
    // inline network fetch on the loop (which would re-introduce the HoL stall
    // for the over-cap op). The deferrable upsert already rolled back any
    // partial work, so this is a clean rejection. (#4391 SF#4.)
    if deferral.at_capacity() {
        tracing::warn!(
            contract = %key,
            inflight = deferral.stashed.len(),
            limit = MAX_INFLIGHT_DEFERRALS,
            "Deferral capacity reached — failing fast with MissingRelated (not deferring)"
        );
        let id = missing.first().copied().unwrap_or_else(|| *key.id());
        return DeferStep::Completed(Err(ExecutorError::missing_related(id)));
    }

    // Park the client responder so it survives the off-loop wait.
    let Some(responder) = contract_handler.channel().take_waiting_response(id) else {
        // Fire-and-forget event (no responder): nothing to answer, and there's
        // no client awaiting the resume. Fall back to the inline path so the
        // upsert's side effects (store/broadcast) still happen. (Fire-and-forget
        // upserts have no per-contract-FIFO concern: there is no client whose
        // ordering we must preserve.)
        return DeferStep::Completed(
            contract_handler
                .executor()
                .upsert_contract_state(key, update, related_contracts, code)
                .instrument(tracing::info_span!("upsert_contract_state", %key))
                .await,
        );
    };

    // Park the client responder keyed by deferral_id; the (exactly-once) resume
    // answers it on commit. There is NO per-contract block: same-key events keep
    // running in normal fair-queue order and reconcile via CRDT merge on resume
    // (see DeferralCtx docs). (#4391 round 5: per-contract-FIFO machinery removed.)
    let deferral_id = deferral.next_deferral_id;
    deferral.next_deferral_id = deferral.next_deferral_id.wrapping_add(1);
    deferral.stashed.insert(deferral_id, responder);

    let op_manager = contract_handler.executor().op_manager_handle();
    let resume_tx = deferral.resume_tx.clone();

    tracing::debug!(
        contract = %key,
        missing = missing.len(),
        deferral_id,
        "Off-loading related-contract fetch from the contract-handling loop (#4391)"
    );

    // Spawn the off-loop waiter. It drives the related GET(s), then delivers a
    // DeferredResume back to the loop via the `ResumeGuard`. Fire-and-forget is
    // acceptable per code-style.md ("short-lived work"): the task is bounded by
    // MAX_INFLIGHT_DEFERRALS and self-terminates at DEFERRED_RELATED_FETCH_TIMEOUT.
    // The `ResumeGuard` guarantees EXACTLY ONE terminal resume even if the task
    // is dropped/panics before sending (its Drop delivers a MissingRelated
    // resume) — so a wedged/dropped waiter cannot leak the stashed responder or
    // permanently wedge the contract. This exactly-once delivery is what lets
    // the loop omit the old TTL sweep and the stale-resume guard entirely.
    let guard = ResumeGuard::new(ResumePayload {
        resume_tx,
        deferral_id,
        key,
        update,
        related_contracts,
        code,
        is_put,
    });
    GlobalExecutor::spawn(async move {
        let fetched = fetch_related_off_loop(op_manager, missing).await;
        guard.send(fetched);
    });

    DeferStep::Deferred
}

/// Build the `PutResponse` (or fatal error) for a PUT upsert result. Shared by
/// the inline PUT arm and the deferred-resume path so both produce identical
/// responses.
///
/// `incoming_state` is the full state the PUT carried; it is returned on the
/// `NoChange` outcome (the incoming state was identical to / older than the
/// stored state), matching the legacy inline behavior exactly.
async fn put_response_from_result<CH>(
    contract_handler: &mut CH,
    key: ContractKey,
    incoming_state: WrappedState,
    put_result: Result<UpsertResult, ExecutorError>,
) -> Result<ContractHandlerEvent, ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    let _ = contract_handler;
    Ok(match put_result {
        Ok(UpsertResult::NoChange) => ContractHandlerEvent::PutResponse {
            new_value: Ok(incoming_state),
            state_changed: false,
        },
        Ok(UpsertResult::Updated(new_state)) => ContractHandlerEvent::PutResponse {
            new_value: Ok(new_state),
            state_changed: true,
        },
        Ok(UpsertResult::CurrentWon(current_state)) => {
            // Merge resulted in no change (incoming state was old/already incorporated).
            ContractHandlerEvent::PutResponse {
                new_value: Ok(current_state),
                state_changed: false,
            }
        }
        Err(err) => {
            if err.is_fatal() {
                tracing::error!(
                    contract = %key,
                    error = %err,
                    phase = "fatal_error",
                    "Fatal executor error during put query"
                );
                return Err(ContractError::FatalExecutorError { key, error: err });
            }
            ContractHandlerEvent::PutResponse {
                new_value: Err(err),
                state_changed: false,
            }
        }
    })
}

/// Build the `UpdateResponse`/`UpdateNoChange` (or fatal error) for an UPDATE
/// upsert result. Shared by the inline UPDATE arm and the deferred-resume path.
async fn update_response_from_result<CH>(
    contract_handler: &mut CH,
    key: ContractKey,
    update_result: Result<UpsertResult, ExecutorError>,
) -> Result<ContractHandlerEvent, ContractError>
where
    CH: ContractHandler + Send + 'static,
{
    Ok(match update_result {
        Ok(UpsertResult::NoChange) => {
            tracing::debug!(
                contract = %key,
                phase = "update_no_change",
                "UPDATE resulted in NoChange, fetching current state to return UpdateResponse"
            );
            // When there's no change, we still need to return the current state
            // so the client gets a proper response
            match contract_handler.executor().fetch_contract(key, false).await {
                Ok((Some(current_state), _)) => {
                    tracing::debug!(
                        contract = %key,
                        phase = "fetch_complete",
                        "Successfully fetched current state for NoChange update"
                    );
                    ContractHandlerEvent::UpdateResponse {
                        new_value: Ok(current_state),
                        state_changed: false,
                    }
                }
                Ok((None, _)) => {
                    tracing::warn!(
                        contract = %key,
                        phase = "fetch_failed",
                        "No state found when fetching for NoChange update"
                    );
                    // Fallback to the old behavior if we can't fetch the state
                    ContractHandlerEvent::UpdateNoChange { key }
                }
                Err(err) => {
                    tracing::error!(
                        contract = %key,
                        error = %err,
                        phase = "fetch_error",
                        "Error fetching state for NoChange update"
                    );
                    // Fallback to the old behavior if we can't fetch the state
                    ContractHandlerEvent::UpdateNoChange { key }
                }
            }
        }
        Ok(UpsertResult::Updated(state)) => ContractHandlerEvent::UpdateResponse {
            new_value: Ok(state),
            state_changed: true,
        },
        Ok(UpsertResult::CurrentWon(current_state)) => {
            // Merge resulted in no change (incoming state was old/already incorporated).
            // Return current state with state_changed=false.
            ContractHandlerEvent::UpdateResponse {
                new_value: Ok(current_state),
                state_changed: false,
            }
        }
        Err(err) => {
            if err.is_fatal() {
                tracing::error!(
                    contract = %key,
                    error = %err,
                    phase = "fatal_error",
                    "Fatal executor error during update query"
                );
                return Err(ContractError::FatalExecutorError { key, error: err });
            }
            ContractHandlerEvent::UpdateResponse {
                new_value: Err(err),
                state_changed: false,
            }
        }
    })
}

/// Push an event into the fair queue, shedding queued `Background` work to make
/// room when a higher-priority (foreground) event is rejected *for lack of
/// global capacity* (#4534).
///
/// Eviction is attempted ONLY when ALL of:
/// - the rejected event is foreground (`> Background`), and
/// - the rejection reason is [`RejectReason::GlobalCapacity`] — a
///   [`RejectReason::PerContract`] rejection cannot be helped by evicting
///   Background (a different tier/contract), so we skip the wasted shed and the
///   identical-failure retry (review of #4534), and
/// - there is queued `Background` to shed.
///
/// The evicted background events get best-effort queue-full responses (they
/// re-emit on their next cycle), then the foreground event is retried once. If
/// it still cannot be admitted (or eviction did not apply), it gets the normal
/// queue-full response.
async fn push_with_background_eviction<CH>(
    contract_handler: &mut CH,
    fair_queue: &mut fair_queue::FairEventQueue,
    id: handler::EventId,
    event: ContractHandlerEvent,
    priority: fair_queue::Priority,
) where
    CH: ContractHandler + Send + 'static,
{
    let rejected = match fair_queue.try_push(id, event, priority) {
        Ok(()) => return,
        Err(rejected) => rejected,
    };

    let eviction_can_help = priority > fair_queue::Priority::Background
        && rejected.reason == fair_queue::RejectReason::GlobalCapacity
        && fair_queue.background_queued() > 0;

    if eviction_can_help {
        // Reclaim at most the reserve's worth of slots in one shot — enough to
        // admit a burst of client work without unbounded eviction churn.
        let evicted = fair_queue.evict_background(fair_queue::CLIENT_LOCAL_RESERVE);
        for shed in evicted {
            track_pending_reclamation_if_evict(contract_handler, &shed);
            send_queue_full_response(contract_handler.channel(), Box::new(shed)).await;
        }
        // Retry the foreground push now that space was freed.
        match fair_queue.try_push(rejected.id, rejected.event, rejected.priority) {
            Ok(()) => return,
            Err(still_rejected) => {
                // Terminal: eviction did not free enough room (#4917).
                fair_queue.record_terminal_rejection(still_rejected.reason);
                track_pending_reclamation_if_evict(contract_handler, &still_rejected);
                send_queue_full_response(contract_handler.channel(), still_rejected).await;
                return;
            }
        }
    }

    // Terminal: eviction could not help (wrong priority, wrong reason, or no
    // Background queued), so this event is refused (#4917).
    fair_queue.record_terminal_rejection(rejected.reason);
    track_pending_reclamation_if_evict(contract_handler, &rejected);
    send_queue_full_response(contract_handler.channel(), rejected).await;
}

/// When the fair queue rejects an `EvictContract` event (queue-full),
/// the hosting-cache entry is already gone — no later sweep would
/// re-emit on its own. Record the key in the pending-reclamation retry
/// queue so the periodic sweep retries via `reclaim_evicted_contract`.
///
/// This closes disk-leak edge case #1 in PR #4212 review round 7
/// (other variants are no-ops here — they have their own error paths).
fn track_pending_reclamation_if_evict<CH>(
    contract_handler: &mut CH,
    rejected: &fair_queue::RejectedEvent,
) where
    CH: ContractHandler + Send + 'static,
{
    if let ContractHandlerEvent::EvictContract {
        key,
        expected_generation,
    } = &rejected.event
    {
        contract_handler
            .executor()
            .track_pending_reclamation(*key, *expected_generation);
    }
}

/// Send an error response to the event sender when the per-contract queue is full.
///
/// Logs at DEBUG (per-event backpressure is not user-actionable; see #4251)
/// and sends the appropriate error response variant for the rejected event.
/// For fire-and-forget events (delegates, disconnects), no response is sent.
async fn send_queue_full_response(
    channel: &mut handler::ContractHandlerChannel<handler::ContractHandlerHalve>,
    rejected: Box<fair_queue::RejectedEvent>,
) {
    // Per-event backpressure is normal under sustained load on a hot contract
    // and is not user-actionable. Logging at WARN once per rejection drowns
    // node logs (millions of lines/day on a saturated contract). Aggregate
    // backpressure visibility belongs in metrics, not per-event logs.
    // See issue #4251 for the underlying queue-saturation tracking.
    tracing::debug!(
        event = %rejected.event,
        "Rejected event due to per-contract queue capacity limit"
    );
    // Typed `ContractQueueFull` marker powers `is_contract_queue_full` at
    // every caller; the UPDATE relay drivers gate amplification on it. #4251.
    let make_err = || ExecutorError::other(ContractQueueFull);
    let response = match &rejected.event {
        ContractHandlerEvent::PutQuery { .. } => ContractHandlerEvent::PutResponse {
            new_value: Err(make_err()),
            state_changed: false,
        },
        ContractHandlerEvent::UpdateQuery { .. } => ContractHandlerEvent::UpdateResponse {
            new_value: Err(make_err()),
            state_changed: false,
        },
        ContractHandlerEvent::GetQuery { .. } => ContractHandlerEvent::GetResponse {
            key: None,
            response: Err(make_err()),
        },
        ContractHandlerEvent::GetSummaryQuery { key, .. } => {
            ContractHandlerEvent::GetSummaryResponse {
                key: *key,
                summary: Err(make_err()),
            }
        }
        ContractHandlerEvent::GetDeltaQuery { key, .. } => ContractHandlerEvent::GetDeltaResponse {
            key: *key,
            delta: Err(make_err()),
        },
        // Export has a typed response variant, so deliver the queue-full error
        // through it (the HTTP export handler awaits this exact variant) rather
        // than dropping the sender and surfacing a generic NoEvHandlerResponse.
        ContractHandlerEvent::ExportUserSecrets { .. } => {
            ContractHandlerEvent::ExportUserSecretsResponse(Err(make_err()))
        }
        // Import likewise has a typed response variant the HTTP handler awaits.
        ContractHandlerEvent::ImportSecrets { .. } => {
            ContractHandlerEvent::ImportSecretsResponse(Err(make_err()))
        }
        // Events without error response variants: drop the oneshot sender to
        // unblock the caller. The caller's receiver will get a RecvError, which
        // maps to ContractError::NoEvHandlerResponse. This prevents leaking
        // entries in the waiting_response map.
        ContractHandlerEvent::DelegateRequest { .. }
        | ContractHandlerEvent::DelegateResponse(_)
        | ContractHandlerEvent::ExportUserSecretsResponse(_)
        | ContractHandlerEvent::ImportSecretsResponse(_)
        | ContractHandlerEvent::PutResponse { .. }
        | ContractHandlerEvent::GetResponse { .. }
        | ContractHandlerEvent::UpdateResponse { .. }
        | ContractHandlerEvent::UpdateNoChange { .. }
        | ContractHandlerEvent::RegisterSubscriberListener { .. }
        | ContractHandlerEvent::RegisterSubscriberListenerResponse { .. }
        | ContractHandlerEvent::QuerySubscriptions { .. }
        | ContractHandlerEvent::QuerySubscriptionsResponse
        | ContractHandlerEvent::GetSummaryResponse { .. }
        | ContractHandlerEvent::GetDeltaResponse { .. }
        | ContractHandlerEvent::ClientDisconnect { .. }
        | ContractHandlerEvent::EvictContract { .. } => {
            channel.drop_waiting_response(rejected.id);
            return;
        }
    };
    if let Err(error) = channel.send_to_sender(rejected.id, response).await {
        tracing::warn!(
            error = %error,
            "Failed to send queue-full response (client may have disconnected)"
        );
    }
}

async fn handle_delegate_notification<CH, P>(
    contract_handler: &mut CH,
    notification: executor::DelegateNotification,
    prompter: &std::sync::Arc<P>,
    mut park: Option<&mut delegate_park::DelegateParkCtx>,
) where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter + 'static,
{
    let executor::DelegateNotification {
        delegate_key,
        contract_id,
        new_state,
    } = notification;

    tracing::debug!(
        delegate = %delegate_key,
        contract = %contract_id,
        "Delivering contract notification to delegate"
    );

    // Unwrap the Arc — if this is the last subscriber the state moves without cloning,
    // otherwise a clone is made (unavoidable since ContractNotification owns the state).
    let owned_state = Arc::try_unwrap(new_state).unwrap_or_else(|arc| (*arc).clone());

    let inbound = vec![InboundDelegateMsg::ContractNotification(
        ContractNotification {
            contract_id,
            new_state: owned_state,
            context: DelegateContext::default(),
        },
    )];

    let req = DelegateRequest::ApplicationMessages {
        key: delegate_key.clone(),
        params: Parameters::from(vec![]),
        inbound,
    };

    // PER-DELEGATE EXCLUSION, first door (#5544 B1).
    //
    // A notification is a THIRD way into a delegate, alongside the client
    // request and the inter-delegate hop, and it is driven by a contract's
    // state changing rather than by anything this delegate did. So a delegate
    // that is parked mid-round-trip — waiting up to `USER_INPUT_TIMEOUT` on a
    // permission prompt — AND subscribed to a contract can be re-entered here
    // the moment that contract changes, clobbering the parked continuation's
    // `DelegateContextCache` entry.
    //
    // That combination is not exotic: holding a durable subscription while
    // prompting the user is the shape the delegate epic is heading for, and a
    // V2 delegate registers its subscription with `ctx.subscribe_contract()`.
    //
    // Note this hazard is about INTERLEAVING, not simultaneity: this path also
    // runs on the serial loop, so nothing runs concurrently, and the global
    // one-`process()`-at-a-time property does NOT save us. A serially executed
    // notification landing INSIDE the park window corrupts the continuation
    // just as thoroughly as a concurrent one would.
    if let Some(park) = park.as_deref_mut()
        && park.is_parked(&delegate_key)
    {
        match park.queue_pending(
            &delegate_key,
            delegate_park::PendingRun::Notification { req },
        ) {
            delegate_park::QueueOutcome::Queued => {
                tracing::debug!(
                    delegate = %delegate_key,
                    contract = %contract_id,
                    "Delegate is parked; queued this notification behind it"
                );
            }
            delegate_park::QueueOutcome::Rejected(_) => {
                // The pending queue is full. Dropping is within the
                // notification pipeline's documented best-effort contract (see
                // `send_delegate_contract_notifications`), and running it would
                // corrupt the parked continuation. `info!` so it is visible in
                // shipped binaries — a silently vanishing notification is the
                // hardest kind of bug to chase from a user report.
                tracing::info!(
                    delegate = %delegate_key,
                    contract = %contract_id,
                    "Dropped a contract notification: the delegate is parked and \
                     its pending queue is full (#5544)"
                );
            }
        }
        return;
    }

    // A notification-driven run has no client responder to carry; the slot
    // exists only to satisfy the shared `ParkingCtx` shape.
    let mut no_responder = None;
    let outcome = handle_delegate_with_contract_requests(
        contract_handler,
        req,
        None,
        // Node-internal invocation: this descends from a contract state change,
        // not from any client connection, so there is NO scope to classify and
        // this hardcodes `Local` to keep the delegate's own inherited
        // attestation intact exactly as before.
        //
        // That hardcoded `Local` is precisely why the inter-delegate hop must be
        // SUPPRESSED below: a non-local caller can steer WHICH delegate runs here
        // (drive it once, have it subscribe, then trigger a state change), so a
        // hop under this scope would hand back the attestation the caller was
        // refused. See the guard in `handle_delegate_with_contract_requests`.
        crate::client_events::ConnectionScope::Local,
        InterDelegateDispatch::Suppressed,
        // Contract-state-change notification: not driven by a client
        // connection, so there is no user token and no per-user secret
        // namespace. Secrets stay `SecretScope::Local`.
        None,
        &delegate_key,
        prompter,
        park.map(|park| ParkingCtx {
            park,
            // No client behind a notification-driven run; residual messages fan
            // out to registered apps instead.
            delivery: delegate_park::Delivery::Apps,
            carried_responder: &mut no_responder,
        }),
        Vec::new(),
    )
    .await;

    match outcome {
        // Notification-driven run: no client responder to stash. The residual
        // messages are routed by `handle_delegate_resume` when the park
        // resolves.
        DelegateRunOutcome::Parked => {}
        DelegateRunOutcome::Completed(outbound) => {
            route_notification_outbound(&delegate_key, outbound);
        }
    }
}

/// Fan a notification-driven run's residual `ApplicationMessage`s out to the
/// apps registered with the delegate.
///
/// Split out of `handle_delegate_notification` because a run that PARKS
/// finishes on a later loop iteration inside `handle_delegate_resume`, by which
/// point the original call frame is gone — both paths must route identically.
fn route_notification_outbound(delegate_key: &DelegateKey, outbound: Vec<OutboundDelegateMsg>) {
    // Route outbound ApplicationMessages to the apps registered with this
    // delegate (#3275). handle_delegate_with_contract_requests already
    // processed the contract requests (GET/PUT/UPDATE/SUBSCRIBE) internally;
    // the residual ApplicationMessages are the delegate's replies meant for
    // connected apps. A notification-driven invocation has no originating
    // client request to answer, so we fan them out via the delegate->apps
    // registry (delegate_app_registry) — the mirror of DELEGATE_SUBSCRIPTIONS,
    // populated when an app talks to the delegate over its WS connection.
    let app_messages: Vec<OutboundDelegateMsg> = outbound
        .into_iter()
        .filter(|msg| match msg {
            OutboundDelegateMsg::ApplicationMessage(_) => true,
            OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_)
            | OutboundDelegateMsg::SendDelegateMessage(_) => {
                tracing::warn!(
                    delegate = %delegate_key,
                    msg_type = ?std::mem::discriminant(msg),
                    "Delegate produced unexpected outbound message from contract notification — dropped"
                );
                false
            }
        })
        .collect();

    // Opportunistic TTL sweep of the delegate->apps registry, mirroring how
    // prune_expired_contexts sweeps on delegate activity rather than via a
    // background task. Bounds stale registrations for apps that vanished
    // without a clean disconnect (AGENTS.md GC-exemption rule).
    delegate_app_registry::sweep_expired();

    if app_messages.is_empty() {
        return;
    }

    // One HostResponse per ApplicationMessage so an app sees each reply as a
    // distinct DelegateResponse, exactly as it would for a client-initiated
    // ApplicationMessages request.
    for app_msg in app_messages {
        let response: crate::client_events::HostResult =
            Ok(freenet_stdlib::client_api::HostResponse::DelegateResponse {
                key: delegate_key.clone(),
                values: vec![app_msg],
            });
        let delivered = delegate_app_registry::route_to_apps(delegate_key, response);
        if delivered == 0 {
            // `debug!` on purpose. This fires once per notification, so at
            // `release_max_level_info` it would ship one line per contract-state
            // change on any node where nobody happens to be registered — a
            // headless node, or simply a closed browser tab. That is the same
            // per-room-update amplification removed from the executor's audit
            // line, and "nobody is listening" is a normal state rather than a
            // security event.
            //
            // The case that IS a security event — a registration that exists but
            // is withheld from delivery — is reported at INFO by
            // `route_to_apps`, once per registration.
            tracing::debug!(
                delegate = %delegate_key,
                "Delegate notification produced an ApplicationMessage but no \
                 local app is registered with this delegate — nothing to route to"
            );
        }
    }
}

/// Run one client-driven delegate request, honouring per-delegate exclusion.
///
/// Shared by the `ContractHandlerEvent::DelegateRequest` arm and by the drain
/// of requests that queued behind a park, so the exclusion check cannot be
/// present on one path and missing on the other.
#[allow(clippy::too_many_arguments)]
async fn dispatch_delegate_request<CH, P>(
    contract_handler: &mut CH,
    park: Option<&mut delegate_park::DelegateParkCtx>,
    prompter: &std::sync::Arc<P>,
    id: handler::EventId,
    req: DelegateRequest<'static>,
    origin_contract: Option<ContractInstanceId>,
    connection_scope: crate::client_events::ConnectionScope,
    user_context: Option<UserSecretContext>,
) where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter + 'static,
{
    let delegate_key = req.key().clone();

    // INVARIANT this function is the chokepoint for, load-bearing for TWO
    // separate changes and enforced by nothing but the shape of the call graph:
    //
    //   At most one delegate `process()` executes node-wide at any instant, and
    //   it always executes on the `contract_handling` loop.
    //
    // #5544 parks a delegate mid-round-trip, which releases the loop while that
    // delegate is SUSPENDED (its WASM call has already returned) — never while
    // it is RUNNING. So the window parking opens is one in which a DIFFERENT
    // delegate may START, not one in which two may RUN:
    //
    //   * `execute_delegate_request` is reached only via
    //     `handle_delegate_with_contract_requests`, whose every caller — this
    //     function, `handle_delegate_notification` and `handle_delegate_resume`
    //     — is awaited from `contract_handling`, one task per node.
    //   * The off-loop task #5544 spawns captures no `ContractHandler` and no
    //     executor, so it cannot invoke a delegate. It waits on a human and
    //     drives a sub-op GET; neither needs one.
    //   * A resume re-enters the delegate from `handle_delegate_resume`, ON the
    //     loop. The spawned task only ships a result back down a channel.
    //
    // Who depends on this, and why per-delegate exclusion is NOT enough:
    // `native_api::state_content_changed` (V2 delegate writes, #5490) does a
    // read-then-write that is not atomic. Its racing pair is two DIFFERENT
    // delegates writing the SAME contract — per-CONTRACT, which the per-delegate
    // exclusion below permits by construction. It is safe only because of the
    // global property above. The durable fix is on #5490's side: fold the
    // comparison into the same ReDb write transaction as the store, as
    // `update_state_sync` already does.
    //
    // BREAKING IT: spawning any work that holds the `ContractHandler`, or
    // resuming a continuation anywhere other than the loop. Do either and
    // #5490's gate must become atomic first. (#4531's off-loop secret export is
    // not a counter-example — it holds a pooled executor but never invokes a
    // delegate.) See `delegate_park`'s module docs for the full argument.
    let Some(park) = park else {
        // No loop behind us (direct unit-test calls): legacy inline behaviour,
        // stalls and all.
        let outcome = handle_delegate_with_contract_requests(
            contract_handler,
            req,
            origin_contract.as_ref(),
            connection_scope,
            InterDelegateDispatch::Allowed,
            user_context.as_ref(),
            &delegate_key,
            prompter,
            None,
            Vec::new(),
        )
        .await;
        let DelegateRunOutcome::Completed(response) = outcome else {
            // Unreachable: a run given no `ParkingCtx` cannot park.
            tracing::error!(
                delegate_key = %delegate_key,
                "delegate run parked without a parking context"
            );
            return;
        };
        send_delegate_response(contract_handler, id, &delegate_key, response).await;
        return;
    };

    // PER-DELEGATE EXCLUSION (#5544). While this delegate has a parked
    // continuation we must NOT run `process()` for it again: the context cache
    // is keyed by delegate and last-write-wins, so a second run would overwrite
    // the parked continuation and it would resume reading the wrong bytes.
    // Queue instead, and drain on resume.
    if park.is_parked(&delegate_key) {
        let pending = delegate_park::PendingRun::Client {
            id,
            req,
            origin_contract,
            connection_scope,
            user_context,
        };
        match park.queue_pending(&delegate_key, pending) {
            delegate_park::QueueOutcome::Queued => {
                tracing::debug!(
                    delegate_key = %delegate_key,
                    "Delegate is parked; queued this request behind it"
                );
            }
            delegate_park::QueueOutcome::Rejected(pending) => {
                // A REFUSAL, and it must not render as a successful empty run.
                //
                // `DelegateResponse(Vec::new())` is what a delegate that ran and
                // said nothing returns, so answering with it would report
                // success for work `process()` never performed, and log as
                // executed. Drop the responder instead — the client's oneshot
                // closes and it surfaces an error, the same shape the fair
                // queue's own rejection path produces. (code-style.md: "a
                // refusal that is not counted renders as a clean zero".)
                let delegate_park::PendingRun::Client { id, .. } = *pending else {
                    // A notification never reaches this arm — `queue_pending`
                    // only returns `Rejected` for what it was handed, and the
                    // notification path handles its own rejection.
                    tracing::error!(
                        delegate = %delegate_key,
                        "non-client run rejected on the client queue path"
                    );
                    return;
                };
                tracing::warn!(
                    delegate_key = %delegate_key,
                    "Refused a delegate request: the delegate is parked and its \
                     pending queue is full. Dropping the responder so the client \
                     sees an error rather than a successful empty response (#5544)"
                );
                contract_handler.channel().drop_waiting_response(id);
            }
        }
        return;
    }

    let mut carried_responder = None;
    let outcome = handle_delegate_with_contract_requests(
        contract_handler,
        req,
        origin_contract.as_ref(),
        connection_scope,
        // Client-driven: the hop forwards THIS connection's scope, so a
        // non-local caller cannot obtain an attested caller identity
        // through it.
        InterDelegateDispatch::Allowed,
        user_context.as_ref(),
        &delegate_key,
        prompter,
        Some(ParkingCtx {
            park: &mut *park,
            delivery: delegate_park::Delivery::Client,
            carried_responder: &mut carried_responder,
        }),
        Vec::new(),
    )
    .await;

    match outcome {
        DelegateRunOutcome::Parked => {
            // Move the client's responder into the park so the resumed run can
            // answer it. The client then sees ONE response covering the whole
            // round-trip, exactly as it does today when the loop blocks.
            let responder = contract_handler.channel().take_waiting_response(&id);
            park.attach_responder(&delegate_key, responder);
        }
        DelegateRunOutcome::Completed(response) => {
            send_delegate_response(contract_handler, id, &delegate_key, response).await;
        }
    }
}

/// Answer a client-driven delegate request. A dropped channel means the client
/// disconnected, which is not fatal — the delegate already ran.
async fn send_delegate_response<CH>(
    contract_handler: &mut CH,
    id: handler::EventId,
    delegate_key: &DelegateKey,
    response: Vec<OutboundDelegateMsg>,
) where
    CH: ContractHandler + Send + 'static,
{
    if let Err(error) = contract_handler
        .channel()
        .send_to_sender(id, ContractHandlerEvent::DelegateResponse(response))
        .await
    {
        tracing::debug!(
            error = %error,
            delegate_key = %delegate_key,
            "Failed to send DELEGATE response (client may have disconnected)"
        );
    }
}

/// Re-enter a delegate whose park has resolved, then drain whatever queued
/// behind it.
///
/// Runs ON the serial loop (the delegate's WASM must stay serial); only the
/// WAIT happened off it. This is the counterpart to #4391's
/// `handle_deferred_resume`.
async fn handle_delegate_resume<CH, P>(
    contract_handler: &mut CH,
    park: &mut delegate_park::DelegateParkCtx,
    prompter: &std::sync::Arc<P>,
    resume: delegate_park::DelegateResume,
) where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter + 'static,
{
    let delegate_park::DelegateResume {
        delegate_key,
        cause,
        inbound,
        upserts,
    } = resume;

    let Some((continuation, pending)) = park.take(&delegate_key) else {
        // Unreachable: the ParkGuard delivers exactly one resume per park, and
        // only a resume ends a park.
        tracing::error!(
            delegate = %delegate_key,
            "Resume for a delegate that is not parked — dropping"
        );
        return;
    };

    if cause == delegate_park::ResumeCause::TimedOut {
        tracing::warn!(
            delegate = %delegate_key,
            "Delegate park ended without a result (task dropped, or past \
             PARK_TTL); resuming with no inbound so the round-trip terminates \
             and the client is answered"
        );
    }

    let delegate_park::Continuation {
        params,
        origin_contract,
        connection_scope,
        user_context,
        inter_delegate,
        accumulated,
        inbound_so_far,
        responder,
        delivery,
    } = continuation;

    let mut all_inbound = inbound_so_far;
    // Re-run any deferred upserts ON the loop (WASM stays serial; only the
    // fetch happened off it), then feed their responses back with the rest.
    for resolved in upserts {
        all_inbound.push(apply_resolved_upsert(contract_handler, resolved).await);
    }
    all_inbound.extend(inbound);

    let mut carried_responder = responder;
    #[allow(clippy::if_not_else)]
    let outcome = if all_inbound.is_empty() {
        // Nothing to feed back — a dropped park that had computed no responses
        // before it. Do NOT re-enter the delegate with an empty inbound; that
        // would run `process()` for no reason. Terminate with what we had.
        DelegateRunOutcome::Completed(accumulated)
    } else {
        let req = DelegateRequest::ApplicationMessages {
            key: delegate_key.clone(),
            params,
            inbound: all_inbound,
        };
        handle_delegate_with_contract_requests(
            contract_handler,
            req,
            origin_contract.as_ref(),
            connection_scope,
            inter_delegate,
            user_context.as_ref(),
            &delegate_key,
            prompter,
            Some(ParkingCtx {
                park: &mut *park,
                delivery,
                carried_responder: &mut carried_responder,
            }),
            // SEED, not append-afterwards (#5544 B3). What the delegate emitted
            // before the earlier park has to be inside the run, so that if this
            // run parks AGAIN it lands in the new continuation. Appending to the
            // `Completed` arm only — as this did — silently dropped every
            // pre-park message on the second park.
            accumulated,
        )
        .await
    };

    match outcome {
        DelegateRunOutcome::Parked => {
            // Parked again (a delegate that prompts twice). `carried_responder`
            // already moved into the new continuation.
        }
        DelegateRunOutcome::Completed(messages) => match delivery {
            delegate_park::Delivery::Client => {
                if let Some(responder) = carried_responder {
                    if responder
                        .respond(ContractHandlerEvent::DelegateResponse(messages))
                        .is_err()
                    {
                        tracing::debug!(
                            delegate = %delegate_key,
                            "Parked client disconnected before its delegate \
                             round-trip finished"
                        );
                    }
                }
            }
            delegate_park::Delivery::Apps => route_notification_outbound(&delegate_key, messages),
        },
    }

    // Drain what queued behind the park, in arrival order. Re-check the park on
    // every item: the resumed run (or an earlier drained request) may have
    // parked this delegate again, and exclusion must still hold.
    for queued in pending {
        match queued {
            delegate_park::PendingRun::Client {
                id,
                req,
                origin_contract,
                connection_scope,
                user_context,
            } => {
                dispatch_delegate_request(
                    contract_handler,
                    Some(&mut *park),
                    prompter,
                    id,
                    req,
                    origin_contract,
                    connection_scope,
                    user_context,
                )
                .await;
            }
            // A queued notification resumes through the SAME path it would have
            // taken had it not been queued, so its residual messages still fan
            // out to registered apps rather than to a client responder that does
            // not exist. `run_queued_notification` re-checks the park, so a
            // delegate that parked again during this drain re-queues instead of
            // being re-entered.
            delegate_park::PendingRun::Notification { req } => {
                run_queued_notification(
                    contract_handler,
                    Some(&mut *park),
                    prompter,
                    &delegate_key,
                    req,
                )
                .await;
            }
        }
    }
}

/// Re-run a notification-driven delegate invocation that had been queued behind
/// a park.
///
/// Split from `handle_delegate_notification` because that function owns
/// building the `ContractNotification` from a `DelegateNotification`; by the
/// time a queued run drains, the request has already been built and the
/// notification consumed.
async fn run_queued_notification<CH, P>(
    contract_handler: &mut CH,
    mut park: Option<&mut delegate_park::DelegateParkCtx>,
    prompter: &std::sync::Arc<P>,
    delegate_key: &DelegateKey,
    req: DelegateRequest<'static>,
) where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter + 'static,
{
    // The delegate may have parked again while this drain was running.
    if let Some(park) = park.as_deref_mut()
        && park.is_parked(delegate_key)
    {
        match park.queue_pending(
            delegate_key,
            delegate_park::PendingRun::Notification { req },
        ) {
            delegate_park::QueueOutcome::Queued => {}
            delegate_park::QueueOutcome::Rejected(_) => {
                tracing::info!(
                    delegate = %delegate_key,
                    "Dropped a queued contract notification: the delegate parked \
                     again and its pending queue is full (#5544)"
                );
            }
        }
        return;
    }

    let mut no_responder = None;
    let outcome = handle_delegate_with_contract_requests(
        contract_handler,
        req,
        None,
        crate::client_events::ConnectionScope::Local,
        InterDelegateDispatch::Suppressed,
        None,
        delegate_key,
        prompter,
        park.map(|park| ParkingCtx {
            park,
            delivery: delegate_park::Delivery::Apps,
            carried_responder: &mut no_responder,
        }),
        Vec::new(),
    )
    .await;

    match outcome {
        DelegateRunOutcome::Parked => {}
        DelegateRunOutcome::Completed(outbound) => {
            route_notification_outbound(delegate_key, outbound);
        }
    }
}

async fn handle_contract_event<CH, P>(
    contract_handler: &mut CH,
    id: handler::EventId,
    event: ContractHandlerEvent,
    prompter: &std::sync::Arc<P>,
    deferral: Option<&mut DeferralCtx>,
    park: Option<&mut delegate_park::DelegateParkCtx>,
) -> Result<(), ContractError>
where
    CH: ContractHandler + Send + 'static,
    P: UserInputPrompter + 'static,
{
    tracing::debug!(
        event = %event,
        "Received contract handling event"
    );
    match event {
        ContractHandlerEvent::GetQuery {
            instance_id,
            return_contract_code,
        } => {
            // Look up the full key from the instance_id
            let key = contract_handler.executor().lookup_key(&instance_id);
            match key {
                Some(key) => {
                    match contract_handler
                        .executor()
                        .fetch_contract(key, return_contract_code)
                        .instrument(
                            tracing::info_span!("fetch_contract", %key, %return_contract_code),
                        )
                        .await
                    {
                        Ok((state, contract)) => {
                            tracing::debug!(
                                contract = %key,
                                with_contract_code = return_contract_code,
                                has_contract = contract.is_some(),
                                has_state = state.is_some(),
                                phase = "get_complete",
                                "Fetched contract"
                            );
                            // Send response back to caller. If the caller disconnected, just log and continue.
                            if let Err(error) = contract_handler
                                .channel()
                                .send_to_sender(
                                    id,
                                    ContractHandlerEvent::GetResponse {
                                        key: Some(key),
                                        response: Ok(StoreResponse { state, contract }),
                                    },
                                )
                                .await
                            {
                                tracing::debug!(
                                    error = %error,
                                    contract = %key,
                                    "Failed to send GET response (client may have disconnected)"
                                );
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                contract = %key,
                                error = %err,
                                phase = "get_failed",
                                "Error executing get contract query"
                            );
                            if err.is_fatal() {
                                tracing::error!(
                                    contract = %key,
                                    error = %err,
                                    phase = "fatal_error",
                                    "Fatal executor error during get query"
                                );
                                return Err(ContractError::FatalExecutorError { key, error: err });
                            }
                            // Send error response back to caller. If the caller disconnected, just log and continue.
                            if let Err(error) = contract_handler
                                .channel()
                                .send_to_sender(
                                    id,
                                    ContractHandlerEvent::GetResponse {
                                        key: Some(key),
                                        response: Err(err),
                                    },
                                )
                                .await
                            {
                                tracing::debug!(
                                    error = %error,
                                    contract = %key,
                                    "Failed to send GET error response (client may have disconnected)"
                                );
                            }
                        }
                    }
                }
                None => {
                    // Contract not found locally - return None for key
                    tracing::debug!(
                        instance_id = %instance_id,
                        phase = "not_found",
                        "Contract not found in local store"
                    );
                    if let Err(error) = contract_handler
                        .channel()
                        .send_to_sender(
                            id,
                            ContractHandlerEvent::GetResponse {
                                key: None,
                                response: Ok(StoreResponse {
                                    state: None,
                                    contract: None,
                                }),
                            },
                        )
                        .await
                    {
                        tracing::debug!(
                            error = %error,
                            instance_id = %instance_id,
                            "Failed to send GET not-found response (client may have disconnected)"
                        );
                    }
                }
            }
        }
        ContractHandlerEvent::PutQuery {
            key,
            state,
            related_contracts,
            contract,
        } => {
            // DEBUG: Log contract details in PutQuery handler
            if let Some(ref contract_container) = contract {
                tracing::debug!(
                    contract = %key,
                    key_code_hash = ?key.code_hash(),
                    container_key = %contract_container.key(),
                    container_code_hash = ?contract_container.key().code_hash(),
                    data_len = contract_container.data().len(),
                    phase = "put_query_debug",
                    "DEBUG PUT: In PutQuery handler with contract"
                );
            } else {
                tracing::debug!(
                    contract = %key,
                    phase = "put_query_debug",
                    "DEBUG PUT: In PutQuery handler - contract is None"
                );
            }

            // Run the upsert in deferrable mode when a deferral context is
            // present (the production loop). If the contract needs a related
            // contract that isn't held locally, the executor returns
            // `DeferRelated` instead of awaiting the network GET inline; we
            // off-load that fetch so the serial loop keeps draining other
            // events (e.g. cached GETs) — issue #4391. Without a deferral
            // context (delegate-driven PUTs, direct unit tests), this falls
            // back to the inline-fetch `upsert_contract_state`.
            let put_result = match maybe_defer_upsert(
                contract_handler,
                deferral,
                &id,
                key,
                Either::Left(state.clone()),
                related_contracts,
                contract,
                true,
            )
            .await
            {
                DeferStep::Deferred => return Ok(()),
                DeferStep::Completed(result) => result,
            };

            let event_result =
                put_response_from_result(contract_handler, key, state, put_result).await?;

            // Send response back to caller. If the caller disconnected (e.g., WebSocket closed),
            // the response channel may be dropped. This is not fatal - the contract has already
            // been stored, so we just log and continue processing other events.
            if let Err(error) = contract_handler
                .channel()
                .send_to_sender(id, event_result)
                .await
            {
                tracing::debug!(
                    error = %error,
                    contract = %key,
                    "Failed to send PUT response (client may have disconnected)"
                );
            }
        }
        ContractHandlerEvent::UpdateQuery {
            key,
            data,
            mut related_contracts,
        } => {
            let update_value: Either<WrappedState, StateDelta<'static>> = match data {
                freenet_stdlib::prelude::UpdateData::State(state) => {
                    Either::Left(WrappedState::from(state.into_bytes()))
                }
                freenet_stdlib::prelude::UpdateData::Delta(delta) => Either::Right(delta),
                freenet_stdlib::prelude::UpdateData::StateAndDelta { state, .. } => {
                    // Prefer the full state — it's authoritative and avoids
                    // an extra delta-merge round trip in the executor. The
                    // delta is dropped because `upsert_contract_state` only
                    // accepts one of the two via the `Either` argument; the
                    // full state is a strict superset of the delta's effect,
                    // so callers cannot observe a stale result. If a caller
                    // ever needs strict delta-only semantics (e.g. a write
                    // that intentionally relies on merge-time validation
                    // rejecting a full-state replacement), they should send
                    // `Delta` directly.
                    Either::Left(WrappedState::from(state.into_bytes()))
                }
                freenet_stdlib::prelude::UpdateData::RelatedStateAndDelta {
                    related_to,
                    state,
                    delta,
                } => {
                    // Bundle the related contract's state into
                    // `related_contracts` so the executor's
                    // related-state-injection bridge surfaces it to the
                    // contract WASM as a `UpdateData::RelatedState` entry.
                    // The delta on this variant targets `key` itself.
                    // `RelatedContracts` exposes no public `insert`, so use
                    // `update()` to mutate the inner slot. If the caller
                    // also supplied a state for the same id via the bare
                    // `related_contracts` field, prefer the inline one —
                    // it arrived with the delta as an atomic package.
                    inject_related_state(&mut related_contracts, related_to, state);
                    Either::Right(delta)
                }
                freenet_stdlib::prelude::UpdateData::RelatedState { .. }
                | freenet_stdlib::prelude::UpdateData::RelatedDelta { .. } => {
                    // These variants carry a payload only for a related
                    // contract, with no main-contract state or delta. The
                    // runtime's `RequestRelated → GET → re-update` flow
                    // populates `related_contracts` itself rather than
                    // routing these variants through `UpdateQuery`, so
                    // hitting this branch indicates a misuse from a
                    // client-facing surface. Reject explicitly instead of
                    // panicking — see freenet/freenet-core#4003.
                    let err = ExecutorError::other(anyhow::anyhow!(
                        "RelatedState / RelatedDelta UpdateData variants are not \
                         accepted directly via ContractRequest::Update — they are \
                         reserved for the runtime's request-related orchestration"
                    ));
                    if let Err(send_err) = contract_handler
                        .channel()
                        .send_to_sender(
                            id,
                            ContractHandlerEvent::UpdateResponse {
                                new_value: Err(err),
                                state_changed: false,
                            },
                        )
                        .await
                    {
                        tracing::debug!(error = %send_err, contract = %key, "Failed to send rejection");
                    }
                    return Ok(());
                }
                // `UpdateData` is `#[non_exhaustive]` since stdlib 0.6.0.
                // Reject unknown future variants explicitly instead of
                // panicking the executor task; the producer at the edge
                // should validate variants before routing them here.
                _ => {
                    let err = ExecutorError::other(anyhow::anyhow!(
                        "Unknown UpdateData variant reached \
                         ContractHandlerEvent::UpdateQuery; add explicit \
                         handling before landing the new variant"
                    ));
                    if let Err(send_err) = contract_handler
                        .channel()
                        .send_to_sender(
                            id,
                            ContractHandlerEvent::UpdateResponse {
                                new_value: Err(err),
                                state_changed: false,
                            },
                        )
                        .await
                    {
                        tracing::debug!(error = %send_err, contract = %key, "Failed to send rejection");
                    }
                    return Ok(());
                }
            };
            // Deferrable upsert: off-load the related-contract fetch from the
            // serial loop when the contract needs a related contract not held
            // locally (#4391). See the PUT arm above for the rationale.
            let update_result = match maybe_defer_upsert(
                contract_handler,
                deferral,
                &id,
                key,
                update_value,
                related_contracts,
                None,
                false,
            )
            .await
            {
                DeferStep::Deferred => return Ok(()),
                DeferStep::Completed(result) => result,
            };

            let event_result =
                update_response_from_result(contract_handler, key, update_result).await?;

            // Send response back to caller. If the caller disconnected, the response channel
            // may be dropped. This is not fatal - the update has already been applied.
            if let Err(error) = contract_handler
                .channel()
                .send_to_sender(id, event_result)
                .await
            {
                tracing::debug!(
                    error = %error,
                    contract = %key,
                    "Failed to send UPDATE response (client may have disconnected)"
                );
            }
        }
        ContractHandlerEvent::DelegateRequest {
            req,
            origin_contract,
            connection_scope,
            user_context,
        } => {
            let delegate_key = req.key().clone();
            tracing::debug!(
                delegate_key = %delegate_key,
                ?origin_contract,
                // `user_context`'s Debug redacts the dek_secret; logging the
                // (non-secret) user_id here is safe and useful for tracing
                // which namespace a hosted-mode request touched.
                ?user_context,
                "Processing delegate request"
            );

            // Execute the delegate and handle any contract request messages.
            // Routed through the shared dispatcher so the #5544 per-delegate
            // exclusion applies identically here and on the post-resume drain.
            dispatch_delegate_request(
                contract_handler,
                park,
                prompter,
                id,
                req,
                origin_contract,
                connection_scope,
                user_context,
            )
            .await;
        }
        ContractHandlerEvent::ExportUserSecrets {
            user_context,
            token,
        } => {
            // Hosted-mode export (P3-live of #4381). DEFERRED OFF this serial loop
            // (#4531 / #4381 P5): the enumerate+decrypt+seal is potentially long,
            // so running it inline would park the loop for its whole duration and
            // make every queued GET/PUT/UPDATE/delegate event wait behind it —
            // the authenticated DoS. Instead we check out a pooled executor
            // (`try_begin_export`) and run the export on a SPAWNED background task,
            // returning from this arm IMMEDIATELY so the loop drains the next
            // event. The task sends its result + the executor back via the
            // export-resume channel; the loop returns/replaces the executor and
            // answers the client (`handle_export_resume`).
            //
            // `user_context` is the forge-proof per-user namespace derived at the
            // connection boundary; the export reads ONLY that user's scope. Log
            // only the non-secret user_id — never the token or bundle bytes.
            tracing::debug!(
                user_id = ?user_context.user_id(),
                "Processing hosted-mode secret export request"
            );

            // Without a DeferralCtx (delegate-driven path / direct unit-test
            // calls) there is no export-resume channel to off-load onto. The
            // production hosted HTTP path always provides one; anything else
            // cannot export. Answer with the not-supported error inline.
            let Some(deferral) = deferral else {
                let err = ExecutorError::other(anyhow::anyhow!(
                    "secret export is not supported in this context"
                ));
                send_export_response_inline(contract_handler, id, Err(err)).await;
                return Ok(());
            };

            match contract_handler
                .executor()
                .try_begin_export(&user_context, token.expose())
            {
                executor::runtime::ExportAdmission::Admitted(job) => {
                    // Park the client responder so the off-loop task can answer it
                    // later via the resume channel; the arm returns now.
                    let responder = contract_handler.channel().take_waiting_response(&id);
                    let guard = ExportGuard::new(deferral.export_resume_tx.clone(), responder);
                    // Fire-and-forget background task. Bounded by
                    // MAX_CONCURRENT_EXPORTS (the export semaphore the job holds);
                    // the ExportGuard guarantees exactly-one resume even if the
                    // task is dropped/panics, so the executor is always reclaimed
                    // and the client always answered. (Same fire-and-forget shape
                    // as the #4391 deferral spawn — short-lived, capacity-bounded.)
                    GlobalExecutor::spawn(async move {
                        let done = job.run().await;
                        guard.send(done);
                    });
                }
                executor::runtime::ExportAdmission::Busy => {
                    // At MAX_CONCURRENT_EXPORTS already; do not queue on the loop.
                    let err = ExecutorError::other(ExportBusy);
                    send_export_response_inline(contract_handler, id, Err(err)).await;
                }
                executor::runtime::ExportAdmission::Unsupported => {
                    let err = ExecutorError::other(anyhow::anyhow!(
                        "secret export is not supported by this executor"
                    ));
                    send_export_response_inline(contract_handler, id, Err(err)).await;
                }
            }
        }
        ContractHandlerEvent::ImportSecrets {
            target_scope,
            bundle,
            key_material,
            key_kind,
            overwrite,
        } => {
            // Live secret import (P3-live of #4592). Runs ON this serial contract
            // loop — DELIBERATELY, unlike the hosted EXPORT which is off-loop.
            //
            // The import is the FIRST writer to the `SecretsStore` outside the
            // serial loop's existing on-loop write path (delegate `store_secret`).
            // The store's write path (fixed sibling `.tmp` + check-then-write
            // collision handling) assumes node-wide write serialization; running
            // the import off-loop on a separate pooled executor would let it race
            // an on-loop delegate write (or another import) on the same secret
            // file and corrupt it. Running it on the loop keeps every secret WRITE
            // in one serialization domain, matching that assumption (Codex P1).
            //
            // Blocking the loop for the import's duration is acceptable here
            // because — unlike the export — this endpoint is gated to LOOPBACK +
            // the node's own dashboard origin (`hosted_import`): only the node's
            // operator, over loopback, in a one-shot migration, can trigger it.
            // It is NOT the authenticated-REMOTE token-holder DoS surface that
            // justified moving export off-loop (#4381 P5). The request body is
            // also bounded (`MAX_IMPORT_BUNDLE_BYTES`).
            //
            // Log only non-secret shape (bytes/scope/overwrite); never the key.
            tracing::debug!(
                bytes = bundle.expose().len(),
                ?target_scope,
                overwrite,
                "Processing live secret import request"
            );
            let result = contract_handler
                .executor()
                .import_secrets(
                    target_scope,
                    bundle.expose(),
                    key_material.expose(),
                    key_kind,
                    overwrite,
                )
                .await;
            send_import_response_inline(contract_handler, id, result).await;
        }
        ContractHandlerEvent::RegisterSubscriberListener {
            key,
            client_id,
            summary,
            subscriber_listener,
        } => {
            // #4681: surface a registration failure to the client instead of
            // returning a success response that client_events.rs would convert
            // into a fake successful subscribe. The error rides back on the
            // response variant so the subscribing client sees a real failure.
            let result = contract_handler.executor().register_contract_notifier(
                key,
                client_id,
                subscriber_listener,
                summary,
            );
            if let Err(err) = &result {
                tracing::warn!(
                    contract = %key,
                    client = %client_id,
                    error = %err,
                    phase = "registration_failed",
                    "Error registering subscriber listener"
                );
            }

            // If the caller disconnected, just log and continue.
            if let Err(error) = contract_handler
                .channel()
                .send_to_sender(
                    id,
                    ContractHandlerEvent::RegisterSubscriberListenerResponse { result },
                )
                .await
            {
                tracing::debug!(
                    error = %error,
                    contract = %key,
                    "Failed to send RegisterSubscriberListener response (client may have disconnected)"
                );
            }
        }
        ContractHandlerEvent::QuerySubscriptions { callback } => {
            // Get subscription information from the executor and send it through the callback
            let subscriptions = contract_handler.executor().get_subscription_info();
            let connections = vec![]; // For now, we'll populate this from the calling context
            let network_debug = crate::message::NetworkDebugInfo {
                application_subscriptions: subscriptions,
                network_subscriptions: vec![], // Contract handler only tracks application subscriptions
                connected_peers: connections,
            };
            if let Err(e) = callback
                .send(crate::message::QueryResult::NetworkDebug(network_debug))
                .await
            {
                tracing::debug!(error = %e, "failed to send network debug info via callback");
            }

            // If the caller disconnected, just log and continue.
            if let Err(error) = contract_handler
                .channel()
                .send_to_sender(id, ContractHandlerEvent::QuerySubscriptionsResponse)
                .await
            {
                tracing::debug!(
                    error = %error,
                    "Failed to send QuerySubscriptions response (client may have disconnected)"
                );
            }
        }
        ContractHandlerEvent::GetSummaryQuery { key } => {
            let summary = contract_handler
                .executor()
                .summarize_contract_state(key)
                .instrument(tracing::info_span!("summarize_contract_state", %key))
                .await;

            if let Err(error) = contract_handler
                .channel()
                .send_to_sender(
                    id,
                    ContractHandlerEvent::GetSummaryResponse { key, summary },
                )
                .await
            {
                tracing::debug!(
                    error = %error,
                    contract = %key,
                    "Failed to send GetSummary response (client may have disconnected)"
                );
            }
        }
        ContractHandlerEvent::GetDeltaQuery { key, their_summary } => {
            let delta = contract_handler
                .executor()
                .get_contract_state_delta(key, their_summary)
                .instrument(tracing::info_span!("get_contract_state_delta", %key))
                .await;

            if let Err(error) = contract_handler
                .channel()
                .send_to_sender(id, ContractHandlerEvent::GetDeltaResponse { key, delta })
                .await
            {
                tracing::debug!(
                    error = %error,
                    contract = %key,
                    "Failed to send GetDelta response (client may have disconnected)"
                );
            }
        }
        ContractHandlerEvent::ClientDisconnect { client_id } => {
            contract_handler.executor().remove_client(client_id);
            contract_handler.channel().drop_waiting_response(id);
        }
        ContractHandlerEvent::EvictContract {
            key,
            expected_generation,
        } => {
            // Reclaim the contract's on-disk storage after it was evicted
            // from the hosting cache. Routed here (not inline-fast-pathed in
            // the drain loop) so it goes through the fair queue and is
            // serialized per-contract with any other in-flight ops on the
            // same key. Fire-and-forget: no response is sent.
            //
            // `expected_generation` is consulted by
            // `RuntimePool::remove_contract` to skip deletion if the
            // contract's state was rewritten since eviction (re-host race).
            match contract_handler
                .executor()
                .remove_contract(&key, expected_generation)
                .await
            {
                Ok(()) => {
                    tracing::info!(contract = %key, "Reclaimed on-disk storage for evicted contract");
                }
                Err(error) => {
                    tracing::warn!(
                        contract = %key,
                        error = %error,
                        "Failed to reclaim on-disk storage for evicted contract"
                    );
                }
            }
            contract_handler.channel().drop_waiting_response(id);
        }
        ContractHandlerEvent::DelegateResponse(_)
        | ContractHandlerEvent::ExportUserSecretsResponse(_)
        | ContractHandlerEvent::ImportSecretsResponse(_)
        | ContractHandlerEvent::PutResponse { .. }
        | ContractHandlerEvent::GetResponse { .. }
        | ContractHandlerEvent::UpdateResponse { .. }
        | ContractHandlerEvent::UpdateNoChange { .. }
        | ContractHandlerEvent::RegisterSubscriberListenerResponse { .. }
        | ContractHandlerEvent::QuerySubscriptionsResponse
        | ContractHandlerEvent::GetSummaryResponse { .. }
        | ContractHandlerEvent::GetDeltaResponse { .. } => {
            unreachable!("response events should not be received by the handler")
        }
    }
    Ok(())
}

/// Prefix shared by all DWARF debug custom sections (`.debug_info`,
/// `.debug_str`, ...). Release builds (`--release`) emit none of these.
const DEBUG_SECTION_PREFIX: &str = ".debug_";

/// Scan a WASM module's custom sections and collect the names of any
/// DWARF debug sections (`.debug_*`) it contains.
///
/// Debug-compiled contracts are typically 10-100x larger than release
/// builds (e.g. ~12MB vs ~186KB for the ping contract) and can exceed
/// WebSocket message-size limits, surfacing as a confusing "Message too
/// long" transport error rather than an actionable one. We detect them
/// at PUT time by looking for the `.debug_*` custom sections the Rust
/// debug profile leaves in the module. See #2257.
///
/// Returns the matching section names in encounter order, or an empty
/// vec when the module is a release build OR cannot be fully parsed.
///
/// A module that fails to parse is treated as "no debug sections" —
/// including the case where some `.debug_*` sections were collected
/// before the parse error: we discard the partial result and return
/// empty. Malformed-WASM rejection is a separate concern handled
/// downstream by the WASM runtime with a precise error; we must not
/// short-circuit it here with a misleading "recompile with --release"
/// message, and a partial scan of a module we can't fully parse is not
/// trustworthy evidence of a debug build. We only trust the `.debug_*`
/// signal on a module that parses cleanly end to end.
pub(crate) fn debug_sections(wasm: &[u8]) -> Vec<String> {
    let mut found = Vec::new();
    for payload in wasmparser::Parser::new(0).parse_all(wasm) {
        match payload {
            Ok(wasmparser::Payload::CustomSection(reader)) => {
                let name = reader.name();
                if name.starts_with(DEBUG_SECTION_PREFIX) {
                    found.push(name.to_string());
                }
            }
            // A parse error anywhere voids the whole scan: section
            // boundaries past it can't be trusted, and any `.debug_*`
            // names collected BEFORE it are not reliable evidence on a
            // module we can't fully parse. Treat the module as "no debug
            // sections" (see doc comment) so the runtime surfaces the real
            // malformed-WASM error instead of "recompile with --release".
            Err(_) => return Vec::new(),
            // Non-custom sections are irrelevant to debug detection.
            Ok(_) => {}
        }
    }
    found
}

/// Returns true if the WASM module contains any DWARF debug section,
/// i.e. it was compiled in debug mode. See [`debug_sections`].
pub(crate) fn contains_debug_sections(wasm: &[u8]) -> bool {
    !debug_sections(wasm).is_empty()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractError {
    #[error("handler channel dropped")]
    ChannelDropped(Box<ContractHandlerEvent>),
    #[error("{0}")]
    IOError(#[from] std::io::Error),
    #[error("no response received from handler")]
    NoEvHandlerResponse,
    #[error("fatal executor error for contract {key}: {error}")]
    FatalExecutorError {
        key: ContractKey,
        error: ExecutorError,
    },
    /// The contract WASM contains DWARF debug sections (`.debug_*`),
    /// indicating it was compiled in debug mode. Debug builds are
    /// typically 10-100x larger than release builds and can exceed
    /// WebSocket message-size limits, producing confusing "Message too
    /// long" errors. Rejected at PUT time so the problem surfaces with
    /// an actionable message instead of a transport failure. See #2257.
    #[error(
        "contract appears to be compiled in debug mode \
         (contains {sections} section(s)). Debug WASM is typically \
         10-100x larger than release builds and may exceed message-size \
         limits. Recompile the contract with `--release` before publishing."
    )]
    DebugWasmRejected { sections: String },
}

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::config::GlobalExecutor;
    use std::time::Duration;

    /// Pin the rejection log site to DEBUG.
    ///
    /// Demoting `Rejected event due to per-contract queue capacity limit`
    /// from WARN to DEBUG was the user-visible point of the #4251 fix —
    /// at WARN it produced millions of lines/day on saturated contracts.
    /// A future refactor that re-promotes it would silently restore that
    /// regression. This is a source-level pin (per
    /// `.claude/rules/bug-prevention-patterns.md`'s precedent for
    /// regression-guard pins) rather than a tracing-subscriber capture,
    /// because the level is the *only* thing being asserted and a string
    /// scan over our own source is more robust than runtime-subscriber
    /// state-machine plumbing.
    #[test]
    fn send_queue_full_response_logs_at_debug_not_warn_pin_test() {
        // file!() returns a workspace-rooted path but tests run from
        // CARGO_MANIFEST_DIR. Anchor explicitly.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/contract.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        let needle = "Rejected event due to per-contract queue capacity limit";
        let idx = source
            .find(needle)
            .expect("rejection log message must still exist in source");
        // Anchor on the closest preceding `tracing::` macro (rfind) rather
        // than a fixed byte window, so the assertion is immune to refactors
        // that move this site relative to other nearby tracing macros.
        // Adopted from the #4272 pin tests; see
        // `operations/update.rs::no_targets_propagation_logs_at_debug_pin_test`.
        let preceding = &source[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .expect("a tracing macro must precede the rejection log site");
        let line_start = preceding[..macro_idx].rfind('\n').map_or(0, |n| n + 1);
        let line_prefix = &preceding[line_start..macro_idx];
        assert!(
            line_prefix.chars().all(char::is_whitespace),
            "rfind matched `tracing::` inside a string literal or comment, \
             not a macro invocation. Prefix on its line: {line_prefix:?}"
        );
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        // Char-boundary-safe last-200-bytes window: a raw byte slice could
        // start mid-UTF-8-char and panic while building the failure message.
        let tail_start = preceding
            .char_indices()
            .map(|(i, _)| i)
            .find(|&i| preceding.len() - i <= 200)
            .unwrap_or(0);
        let context = &preceding[tail_start..];
        // Equality against "debug" rejects WARN/INFO (and any other level)
        // implicitly: re-promotion is exactly the issue #4251 regression.
        assert_eq!(
            macro_name, "debug",
            "Rejected-event log site must be at DEBUG, not WARN/INFO \
             (closest preceding macro is `tracing::{macro_name}!`). \
             Re-promotion restores the issue #4251 log-volume regression.\n\
             Preceding source (last 200 bytes):\n{context}"
        );
    }

    /// Pin: the PUT and UPDATE arms of `handle_contract_event` route their
    /// upsert through `maybe_defer_upsert` (the #4391 off-load path) rather than
    /// calling `upsert_contract_state` directly inline. A refactor that
    /// re-inlined the blocking upsert into the arm would silently re-introduce
    /// the head-of-line stall the fix removes. Anchored on the two arm bodies.
    #[test]
    fn put_update_arms_route_through_maybe_defer_upsert_pin_test() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/contract.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));

        // Extract the body of `handle_contract_event` (the dispatch fn), then
        // check each arm delegates to maybe_defer_upsert.
        let body = source
            .split("async fn handle_contract_event")
            .nth(1)
            .expect("handle_contract_event must exist");

        // PutQuery arm.
        let put_arm = body
            .split("ContractHandlerEvent::PutQuery {")
            .nth(1)
            .expect("PutQuery arm must exist");
        let put_arm = put_arm
            .split("ContractHandlerEvent::UpdateQuery {")
            .next()
            .expect("PutQuery arm bounded by UpdateQuery arm");
        assert!(
            put_arm.contains("maybe_defer_upsert"),
            "PUT arm must route its upsert through maybe_defer_upsert (#4391); \
             re-inlining upsert_contract_state restores the HoL stall"
        );

        // UpdateQuery arm.
        let update_arm = body
            .split("ContractHandlerEvent::UpdateQuery {")
            .nth(1)
            .expect("UpdateQuery arm must exist");
        let update_arm = update_arm
            .split("ContractHandlerEvent::DelegateRequest {")
            .next()
            .expect("UpdateQuery arm bounded by DelegateRequest arm");
        assert!(
            update_arm.contains("maybe_defer_upsert"),
            "UPDATE arm must route its upsert through maybe_defer_upsert (#4391)"
        );
    }

    fn make_contract_key() -> ContractKey {
        let code = ContractCode::from(vec![42u8; 32]);
        let params = Parameters::from(vec![7u8; 8]);
        ContractKey::from_params_and_code(&params, &code)
    }

    /// H2 — the control that REPLACED the deleted registration-record gate.
    ///
    /// Removing a control is only safe if the thing standing in for it is
    /// itself pinned. The inter-delegate hop is safe because it forwards the
    /// ORIGINATING connection scope, so a non-local caller cannot launder its
    /// lack of attestation through a delegate. Nothing enforced that: passing a
    /// literal `ConnectionScope::Local` at the hop compiled and broke nothing.
    ///
    /// Two properties, because the hop has two callers with different scope
    /// provenance:
    ///  1. the hop forwards the `connection_scope` VARIABLE, never a literal; and
    ///  2. the notification path — which has no connection and therefore
    ///     hardcodes `Local` — is `InterDelegateDispatch::Suppressed`, so that
    ///     hardcoded value can never reach the hop.
    #[test]
    fn inter_delegate_hop_forwards_the_originating_scope() {
        let src = include_str!("contract.rs");
        let body = src
            .split("async fn handle_delegate_with_contract_requests")
            .nth(1)
            .expect("handle_delegate_with_contract_requests must exist");
        let body = body
            .split("\nasync fn ")
            .next()
            .expect("bounded by the next free function");

        // Isolate the hop's own executor call.
        let hop = body
            .split(".execute_delegate_request(")
            .nth(2)
            .expect("the inter-delegate hop is the SECOND execute_delegate_request call");
        // Bound on `.await`, not on the first `)` — the argument list itself
        // contains parentheses (`Some(delegate_key)`).
        let hop_args = hop
            .split(".await")
            .next()
            .expect("the call is bounded by its .await");
        assert!(
            hop_args.contains("connection_scope"),
            "the hop must forward the originating connection scope; got args: {hop_args}"
        );
        assert!(
            !hop_args.contains("ConnectionScope::Local"),
            "the hop must NOT hardcode a scope — that is the laundering bug this \
             replaced the removed registration-record gate to avoid; got args: {hop_args}"
        );

        // And the scopeless caller must not be able to reach it at all.
        let notification = src
            .split("async fn handle_delegate_notification")
            .nth(1)
            .expect("handle_delegate_notification must exist");
        let notification = notification
            .split("\nasync fn ")
            .next()
            .expect("bounded by the next free function");
        assert!(
            notification.contains("InterDelegateDispatch::Suppressed"),
            "the notification path hardcodes ConnectionScope::Local, so it MUST \
             suppress inter-delegate dispatch or that literal reaches the hop"
        );
    }

    /// GHSA-824h-7x5x-wfmf: the identity rendered to the HUMAN in a consent
    /// prompt must come from the GATED origin.
    ///
    /// `caller_identity_from_origin` feeds `CallerIdentity` to
    /// `DashboardPrompter`, which names the requesting app to the user deciding
    /// whether to approve. Fed the raw `origin_contract`, a caller the node
    /// cannot prove is local could mint a token for a well-known contract id,
    /// drive a delegate that emits `RequestUserInput` with attacker-chosen text,
    /// and have the prompt attribute the request to that app.
    ///
    /// This is a source pin rather than a behavioural test because reaching the
    /// prompt needs a full delegate + prompter harness; what it must catch is
    /// the gate being deleted or moved below its consumer, which it does. The
    /// region is bounded to the function so the pin cannot match its own
    /// assertion strings further down the file.
    #[test]
    fn consent_prompt_identity_comes_from_the_gated_origin() {
        let src = include_str!("contract.rs");
        let body = src
            .split("async fn handle_delegate_with_contract_requests")
            .nth(1)
            .expect("handle_delegate_with_contract_requests must exist");
        let body = body
            .split("\nasync fn ")
            .next()
            .expect("bounded by the next free function");

        let gate = body
            .find("let origin_contract = if connection_scope.is_local()")
            .expect(
                "handle_delegate_with_contract_requests must gate `origin_contract` on \
                 the connection scope before using it",
            );
        let prompt_use = body
            .find("caller_identity_from_origin(origin_contract)")
            .expect("the prompter's caller identity must be built from origin_contract");
        assert!(
            gate < prompt_use,
            "the connection-scope gate must run BEFORE the origin reaches the \
             consent prompt, or a non-local caller's forged app identity is \
             rendered to the user"
        );
    }

    // Regression test for issue #3857: the executor's origin context must be
    // mapped onto the prompter's CallerIdentity correctly. Without this test,
    // accidentally swapping the Some/None arms — or wiring the wrong
    // variable into the prompter call — would not fail any other test in
    // the suite (the prompter unit tests pass `CallerIdentity` in directly).
    #[test]
    fn test_caller_identity_from_origin() {
        // None origin (delegate-to-delegate, local client, missing
        // attestation) collapses to CallerIdentity::None.
        assert_eq!(caller_identity_from_origin(None), CallerIdentity::None);

        // Some(ContractInstanceId) becomes CallerIdentity::WebApp(<id>),
        // with the id stringified via its Display impl.
        let key = make_contract_key();
        let id = *key.id();
        let mapped = caller_identity_from_origin(Some(&id));
        assert_eq!(mapped, CallerIdentity::WebApp(id.to_string()));

        // Sanity check: the produced string must not be empty (would
        // render as "Freenet app " with a dangling space in the UI).
        match mapped {
            CallerIdentity::WebApp(s) => assert!(!s.is_empty()),
            other => panic!("expected WebApp, got {other:?}"),
        }
    }

    // Regression tests for the `UpdateQuery` arm rewrite that landed in
    // PR #4004: previously every variant other than `State` and `Delta`
    // hit `unreachable!()` and panicked the executor task. The conversion
    // now decomposes `RelatedStateAndDelta` and `StateAndDelta` instead;
    // these tests exercise the `inject_related_state` helper directly so
    // a regression in the conversion (silent state drop, wrong slot, lost
    // related id) fails fast without needing the full runtime stack.

    #[test]
    fn inject_related_state_writes_inline_state_to_slot() {
        // When `RelatedStateAndDelta` arrives with an inline `state` for
        // `related_to`, the inline state must end up in the
        // `RelatedContracts` slot for that id so the executor's
        // related-state-injection bridge surfaces it as a
        // `UpdateData::RelatedState` entry to the contract WASM.
        let related_id = *make_contract_key().id();
        let inline_state = freenet_stdlib::prelude::State::from(vec![1, 2, 3, 4]);

        let mut related = RelatedContracts::default();
        inject_related_state(&mut related, related_id, inline_state.clone());

        let states: Vec<_> = related.states().collect();
        assert_eq!(states.len(), 1, "expected exactly one related entry");
        let (id, slot) = states[0];
        assert_eq!(id, &related_id);
        let stored = slot.as_ref().expect("slot must hold the inline state");
        assert_eq!(stored.as_ref(), inline_state.as_ref());
    }

    #[test]
    fn inject_related_state_overrides_existing_slot() {
        // If the caller already populated `related_contracts` with a state
        // for the same id (e.g. via a separate `related_contracts` arg on
        // `UpdateQuery`), the inline state from `RelatedStateAndDelta`
        // wins. The inline payload arrived alongside the delta as an
        // atomic package and is the sender's authoritative intent.
        let related_id = *make_contract_key().id();

        let prior = freenet_stdlib::prelude::State::from(vec![9, 9]);
        let inline = freenet_stdlib::prelude::State::from(vec![1, 2, 3]);

        let mut map = std::collections::HashMap::new();
        map.insert(related_id, Some(prior));
        let mut related = RelatedContracts::from(map);

        inject_related_state(&mut related, related_id, inline.clone());

        let states: Vec<_> = related.states().collect();
        assert_eq!(states.len(), 1);
        let stored = states[0].1.as_ref().expect("slot must be Some");
        assert_eq!(
            stored.as_ref(),
            inline.as_ref(),
            "inline state must override pre-existing slot"
        );
    }

    /// Helper: send an event through the sender halve and receive it on the handler side,
    /// then wrap it as a RejectedEvent. This simulates the normal drain path where
    /// try_recv_from_sender inserts into waiting_response, followed by queue rejection.
    async fn setup_rejected_event(
        send_halve: handler::ContractHandlerChannel<handler::SenderHalve>,
        rcv_halve: &mut handler::ContractHandlerChannel<handler::ContractHandlerHalve>,
        event: ContractHandlerEvent,
    ) -> (
        Box<fair_queue::RejectedEvent>,
        tokio::task::JoinHandle<Result<ContractHandlerEvent, anyhow::Error>>,
    ) {
        // Spawn the sender — it blocks until the response arrives
        let handle = GlobalExecutor::spawn(async move {
            send_halve
                .send_to_handler(event)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))
        });

        // Receive on the handler side (populates waiting_response)
        let (id, received_event, priority) =
            tokio::time::timeout(Duration::from_millis(200), rcv_halve.recv_from_sender())
                .await
                .expect("timeout waiting for event")
                .expect("channel should be open");

        let rejected = Box::new(fair_queue::RejectedEvent {
            id,
            event: received_event,
            priority,
            reason: fair_queue::RejectReason::GlobalCapacity,
        });

        (rejected, handle)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_put_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::PutQuery {
                key,
                state: WrappedState::new(vec![1, 2, 3]),
                related_contracts: RelatedContracts::default(),
                contract: None,
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::PutResponse {
                new_value,
                state_changed,
            } => {
                assert!(new_value.is_err(), "should be an error response");
                assert!(!state_changed);
            }
            other => panic!("expected PutResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_get_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::GetQuery {
                instance_id: *key.id(),
                return_contract_code: false,
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::GetResponse { response, .. } => {
                assert!(response.is_err(), "should be an error response");
            }
            other => panic!("expected GetResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_update_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::UpdateQuery {
                key,
                data: UpdateData::Delta(StateDelta::from(vec![1])),
                related_contracts: RelatedContracts::default(),
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::UpdateResponse {
                new_value,
                state_changed,
            } => {
                assert!(new_value.is_err(), "should be an error response");
                assert!(!state_changed);
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_get_summary_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::GetSummaryQuery { key },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::GetSummaryResponse {
                key: resp_key,
                summary,
            } => {
                assert_eq!(resp_key, key);
                assert!(summary.is_err(), "should be an error response");
            }
            other => panic!("expected GetSummaryResponse, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_get_delta_query() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::GetDeltaQuery {
                key,
                their_summary: StateSummary::from(vec![1, 2]),
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::GetDeltaResponse {
                key: resp_key,
                delta,
            } => {
                assert_eq!(resp_key, key);
                assert!(delta.is_err(), "should be an error response");
            }
            other => panic!("expected GetDeltaResponse, got {other}"),
        }
    }

    /// Issue #4251: the queue-full response MUST carry the typed
    /// `ContractQueueFull` marker, not just an opaque anyhow error, so
    /// callers can suppress amplification side effects (auto-fetch,
    /// ResyncRequest, ERROR logs) via
    /// `ExecutorError::is_contract_queue_full`. A regression here would
    /// silently bring back the storm-on-saturation behavior.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_carries_typed_queue_full_marker() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::UpdateQuery {
                key,
                data: UpdateData::Delta(StateDelta::from(vec![1])),
                related_contracts: RelatedContracts::default(),
            },
        )
        .await;

        send_queue_full_response(&mut rcv_halve, rejected).await;

        let response = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete")
            .expect("should get response");

        match response {
            ContractHandlerEvent::UpdateResponse { new_value, .. } => {
                let err = new_value.expect_err("UpdateResponse should carry an error");
                assert!(
                    err.is_contract_queue_full(),
                    "queue-full response MUST be classified by is_contract_queue_full; \
                     got err = {err:?}. If you removed this classification, you have also \
                     re-enabled the auto-fetch / ResyncRequest amplification storm — see \
                     issue #4251."
                );
                // Disjointness from other predicates is covered by
                // `test_contract_queue_full_disjoint_from_other_predicates`
                // in executor.rs.
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    /// Verify that fire-and-forget events (RegisterSubscriberListener, delegates, etc.)
    /// have their waiting_response entry cleaned up via drop_waiting_response when rejected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_queue_full_response_fire_and_forget_cleans_waiting_response() {
        let (send_halve, mut rcv_halve, _) = handler::contract_handler_channel();
        let key = make_contract_key();

        let (rejected, handle) = setup_rejected_event(
            send_halve,
            &mut rcv_halve,
            ContractHandlerEvent::RegisterSubscriberListener {
                key: *key.id(),
                client_id: crate::client_events::ClientId::next(),
                summary: None,
                subscriber_listener: tokio::sync::mpsc::channel(64).0,
            },
        )
        .await;

        // Verify waiting_response was populated
        assert!(
            rcv_halve.has_waiting_response(&rejected.id),
            "waiting_response should contain the event"
        );

        let rejected_id = handler::EventId { id: rejected.id.id };
        send_queue_full_response(&mut rcv_halve, rejected).await;

        // Verify waiting_response was cleaned up
        assert!(
            !rcv_halve.has_waiting_response(&rejected_id),
            "waiting_response should be cleaned up after rejection"
        );

        // The sender should get a RecvError (NoEvHandlerResponse) since we dropped the oneshot
        let result = tokio::time::timeout(Duration::from_millis(200), handle)
            .await
            .expect("timeout")
            .expect("task should complete");
        assert!(
            result.is_err(),
            "fire-and-forget rejection should produce an error"
        );
    }

    // ======================================================================
    // End-to-end UpdateQuery dispatch tests (issue #4005)
    // ======================================================================
    //
    // These drive an `UpdateData` variant through the *full* dispatch arm in
    // `handle_contract_event` via the `ContractHandlerChannel`, then assert on
    // the `UpdateResponse` that flows back to the sender. Unlike the
    // `inject_related_state_*` unit tests (which call the helper in isolation)
    // and `send_queue_full_response_update_query` (which only exercises the
    // rejection path), these tests would catch a regression that reintroduced
    // `unreachable!()` in the dispatch arm itself — the exact failure mode
    // PR #4004 fixed. A revert of the `StateAndDelta` or `RelatedStateAndDelta`
    // conversion arms now fails CI here.

    /// Drive a single `UpdateQuery` event end-to-end through
    /// `handle_contract_event` over a real `ContractHandlerChannel` backed by a
    /// `MemoryContractHandler` (mock executor), returning the response the
    /// sender observes.
    ///
    /// Mirrors the production driver loop (`contract_handling`): the sender's
    /// `send_to_handler` blocks awaiting the response; the handler side
    /// `recv_from_sender`s to register the waiting-response slot and obtain the
    /// `EventId`, then `handle_contract_event` runs the dispatch arm and
    /// fulfils the sender's oneshot via `send_to_sender`.
    async fn dispatch_update_query_e2e(
        handler: &mut MemoryContractHandler,
        send_halve: &handler::ContractHandlerChannel<handler::SenderHalve>,
        key: ContractKey,
        data: UpdateData<'static>,
        related_contracts: RelatedContracts<'static>,
    ) -> ContractHandlerEvent {
        let event = ContractHandlerEvent::UpdateQuery {
            key,
            data,
            related_contracts,
        };
        // Run the sender concurrently with the handler-side dispatch via
        // `tokio::join!` (the handler is borrowed, so it cannot move into a
        // spawned task). `send_to_handler` awaits the oneshot until the
        // dispatch arm replies via `send_to_sender`; the dispatch never blocks,
        // so the two futures make progress cooperatively to completion.
        let send_fut = send_halve.send_to_handler(event);
        let recv_fut = async {
            let (id, received, _priority) = handler
                .channel()
                .recv_from_sender()
                .await
                .expect("handler channel should be open");
            handle_contract_event(
                handler,
                id,
                received,
                &std::sync::Arc::new(user_input::AutoApprovePrompter),
                None,
                None,
            )
            .await
            .expect("dispatch must not error for a well-formed UpdateQuery");
        };
        let (send_res, ()) = tokio::join!(send_fut, recv_fut);
        send_res.expect("sender must receive a response")
    }

    /// Seed an initial state for `contract` via a `PutQuery` so subsequent
    /// `UpdateQuery`s have an existing state to merge against (the mock
    /// executor rejects an update with no prior state via `MissingContract`).
    async fn seed_contract_state(
        handler: &mut MemoryContractHandler,
        send_halve: &handler::ContractHandlerChannel<handler::SenderHalve>,
        contract: ContractContainer,
        initial: WrappedState,
    ) {
        let key = contract.key();
        let event = ContractHandlerEvent::PutQuery {
            key,
            state: initial,
            related_contracts: RelatedContracts::default(),
            contract: Some(contract),
        };
        let send_fut = send_halve.send_to_handler(event);
        let recv_fut = async {
            let (id, received, _priority) = handler
                .channel()
                .recv_from_sender()
                .await
                .expect("handler channel should be open");
            handle_contract_event(
                handler,
                id,
                received,
                &std::sync::Arc::new(user_input::AutoApprovePrompter),
                None,
                None,
            )
            .await
            .expect("seed PutQuery must not error");
        };
        let (send_res, ()) = tokio::join!(send_fut, recv_fut);
        match send_res.expect("seed PutQuery must respond") {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                new_value.expect("seed PutQuery must store state");
            }
            other => panic!("expected PutResponse from seed, got {other}"),
        }
    }

    /// `StateAndDelta` must survive dispatch (not panic) AND the conversion
    /// must prefer the full `state` over the `delta`. We seed a contract, then
    /// send `StateAndDelta { state, delta }` where `state` is chosen to win the
    /// mock's hash-comparison merge and `delta` is distinct, non-empty bytes.
    /// If the dispatch arm wrongly routed the delta into the executor, the
    /// stored result would be `seed ++ delta` (the mock's delta-concat merge);
    /// asserting the response equals the full `state` bytes proves the
    /// full-state path was taken.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_query_state_and_delta_dispatch_prefers_full_state_e2e() {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MemoryContractHandler::new(
            rcv_halve,
            None,
            "update_query_state_and_delta_dispatch_prefers_full_state",
        )
        .await;

        let contract = executor::mock_runtime::test::create_test_contract(b"sad_e2e");
        let key = contract.key();

        // Seed with bytes whose hash is the smaller of the two, so the update's
        // full state deterministically wins the mock's "larger hash wins"
        // merge and we observe `Updated(full_state)`.
        let candidate_a = WrappedState::new(vec![0xAA; 16]);
        let candidate_b = WrappedState::new(vec![0xBB; 16]);
        let (seed_state, winning_full_state) = if blake3::hash(candidate_a.as_ref()).as_bytes()
            < blake3::hash(candidate_b.as_ref()).as_bytes()
        {
            (candidate_a, candidate_b)
        } else {
            (candidate_b, candidate_a)
        };

        seed_contract_state(&mut handler, &send_halve, contract, seed_state.clone()).await;

        // A delta distinct from the full state. If the dispatch arm used this
        // delta instead of the state, the mock would store `seed ++ delta`,
        // which is neither `winning_full_state` nor equal in length to it.
        let distinct_delta = StateDelta::from(vec![0xCC; 8]);
        let data = UpdateData::StateAndDelta {
            state: freenet_stdlib::prelude::State::from(winning_full_state.as_ref().to_vec()),
            delta: distinct_delta,
        };

        let response = dispatch_update_query_e2e(
            &mut handler,
            &send_halve,
            key,
            data,
            RelatedContracts::default(),
        )
        .await;

        match response {
            ContractHandlerEvent::UpdateResponse {
                new_value,
                state_changed,
            } => {
                let stored = new_value.expect("StateAndDelta update must succeed");
                assert_eq!(
                    stored.as_ref(),
                    winning_full_state.as_ref(),
                    "dispatch must store the full state, not seed++delta — \
                     a regression here means the conversion arm routed the \
                     delta instead of the authoritative full state"
                );
                assert!(
                    state_changed,
                    "full state differs from seed, so the merge changed state"
                );
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    /// `RelatedStateAndDelta` must survive dispatch (not panic) and route
    /// through `inject_related_state` + the delta path. We seed a contract,
    /// then send `RelatedStateAndDelta { related_to, state, delta }`. The
    /// dispatch arm injects the related state into `related_contracts` and
    /// feeds the `delta` to the executor for `key`. The mock applies the delta
    /// (seed ++ delta), so the response is a successful `UpdateResponse`
    /// reflecting the delta application — proving the arm reaches the executor
    /// rather than panicking. (The injection itself is unit-tested in
    /// `inject_related_state_*`; this test covers the wiring.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_query_related_state_and_delta_dispatch_routes_through_helper_e2e() {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MemoryContractHandler::new(
            rcv_halve,
            None,
            "update_query_related_state_and_delta_dispatch_routes_through_helper",
        )
        .await;

        let contract = executor::mock_runtime::test::create_test_contract(b"rsad_e2e");
        let key = contract.key();
        let related_contract = executor::mock_runtime::test::create_test_contract(b"rsad_related");
        let related_id = *related_contract.key().id();

        let seed_state = WrappedState::new(vec![1, 2, 3]);
        seed_contract_state(&mut handler, &send_halve, contract, seed_state.clone()).await;

        // The mock applies a delta by concatenating `seed ++ delta` and only
        // stores the result if its hash beats the current state's hash
        // (otherwise `NoChange`). Pick a delta whose concatenation
        // deterministically wins, so the test is independent of incidental
        // hash ordering and reliably observes `Updated(seed ++ delta)`.
        let seed_hash = *blake3::hash(seed_state.as_ref()).as_bytes();
        let delta_bytes = (0u8..=255)
            .map(|n| vec![n; 4])
            .find(|candidate| {
                let mut merged = seed_state.as_ref().to_vec();
                merged.extend_from_slice(candidate);
                *blake3::hash(&merged).as_bytes() > seed_hash
            })
            .expect("some 4-byte delta must produce a winning concatenation hash");
        let data = UpdateData::RelatedStateAndDelta {
            related_to: related_id,
            state: freenet_stdlib::prelude::State::from(vec![9, 9, 9]),
            delta: StateDelta::from(delta_bytes.clone()),
        };

        let response = dispatch_update_query_e2e(
            &mut handler,
            &send_halve,
            key,
            data,
            RelatedContracts::default(),
        )
        .await;

        match response {
            ContractHandlerEvent::UpdateResponse { new_value, .. } => {
                let stored = new_value.expect(
                    "RelatedStateAndDelta must reach the executor and apply the \
                     delta — an Err here (or a panic) means the dispatch arm \
                     regressed to rejecting/unreachable!() the variant",
                );
                // The mock concatenates the delta onto the seed; the result must
                // therefore contain the delta bytes as a suffix, confirming the
                // delta (not the related state) drove the `key` update.
                assert!(
                    stored.as_ref().ends_with(&delta_bytes),
                    "stored state {:?} must end with the delta bytes {:?}, \
                     proving the delta arm of RelatedStateAndDelta was used",
                    stored.as_ref(),
                    delta_bytes
                );
            }
            other => panic!("expected UpdateResponse, got {other}"),
        }
    }

    /// The `RelatedState` and `RelatedDelta` variants carry a payload only for
    /// a *related* contract, with no main-contract state/delta. They used to
    /// hit `unreachable!()`; the fix returns a structured
    /// `UpdateResponse { new_value: Err, state_changed: false }`. This test
    /// drives both through dispatch end-to-end and asserts the structured
    /// rejection (no panic, error returned via the channel, no state change).
    ///
    /// The unknown-`_` arm cannot be exercised from test code: `UpdateData` is
    /// `#[non_exhaustive]` *outside* its defining crate, but within freenet-core
    /// every existing variant is matched explicitly, so there is no constructible
    /// value that reaches the `_` arm. It remains as defensive handling for a
    /// future stdlib variant; the two reachable rejection variants are covered
    /// here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn update_query_rejection_arms_return_structured_error_e2e() {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MemoryContractHandler::new(
            rcv_halve,
            None,
            "update_query_rejection_arms_return_structured_error",
        )
        .await;

        let contract = executor::mock_runtime::test::create_test_contract(b"reject_e2e");
        let key = contract.key();
        let related_id = *executor::mock_runtime::test::create_test_contract(b"reject_related")
            .key()
            .id();

        // Seed so a *correct* main-contract update would succeed — this rules
        // out "rejected because the contract was missing" and isolates the
        // rejection to the variant itself.
        seed_contract_state(
            &mut handler,
            &send_halve,
            contract,
            WrappedState::new(vec![7, 7, 7]),
        )
        .await;

        let rejection_variants = [
            UpdateData::RelatedState {
                related_to: related_id,
                state: freenet_stdlib::prelude::State::from(vec![1, 1]),
            },
            UpdateData::RelatedDelta {
                related_to: related_id,
                delta: StateDelta::from(vec![2, 2]),
            },
        ];

        for data in rejection_variants {
            let label = format!("{data:?}");
            let response = dispatch_update_query_e2e(
                &mut handler,
                &send_halve,
                key,
                data,
                RelatedContracts::default(),
            )
            .await;

            match response {
                ContractHandlerEvent::UpdateResponse {
                    new_value,
                    state_changed,
                } => {
                    assert!(
                        new_value.is_err(),
                        "{label} must be rejected with a structured error, \
                         not applied as an update"
                    );
                    assert!(
                        !state_changed,
                        "{label} rejection must report state_changed = false"
                    );
                }
                other => panic!("expected UpdateResponse for {label}, got {other}"),
            }
        }
    }

    /// Anti-rot pin for #4917: every TERMINAL admission rejection in
    /// `push_with_background_eviction` must record itself.
    ///
    /// The counter deliberately does not live in `try_push`, because a
    /// capacity failure there is often recovered by shedding Background and
    /// retrying — counting it would report a client-visible reject for an
    /// operation that succeeded. That correctness depends on the terminal arms
    /// remembering to call `record_terminal_rejection`, so pin it: a new
    /// terminal path that forgets would silently under-report queue-full
    /// failures on the dashboard.
    #[test]
    fn terminal_rejection_paths_record_the_rejection() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/contract.rs");
        let source = std::fs::read_to_string(&path).expect("read own source");
        let start = source
            .find("async fn push_with_background_eviction")
            .expect("push_with_background_eviction must exist");
        let rest = &source[start..];
        let end = rest
            .find("\n/// ")
            .map(|o| start + o)
            .unwrap_or(source.len());
        let body = &source[start..end];

        let records = body.matches("record_terminal_rejection(").count();
        assert_eq!(
            records, 2,
            "expected exactly two terminal rejection sites (eviction retry \
             exhausted, and eviction-cannot-help), found {records}. If you \
             added a terminal path, record the rejection there too; if you \
             removed one, update this pin deliberately. See #4917."
        );

        // The shed loop also calls send_queue_full_response, for Background
        // work that is re-emitted on its next cycle. That is a shed, not an
        // admission rejection, and must NOT be counted as one.
        let shed_loop = body
            .find("for shed in evicted {")
            .expect("the Background shed loop must exist");
        let shed_end = body[shed_loop..]
            .find('}')
            .map(|o| shed_loop + o)
            .expect("shed loop must close");
        assert!(
            !body[shed_loop..shed_end].contains("record_terminal_rejection"),
            "a shed Background event is not an admission rejection; counting \
             it would inflate the client-visible failure count (#4917)"
        );
    }
}

// ============================================================================
// Head-of-line blocking regression tests (issue #4391)
// ============================================================================
//
// The serial `contract_handling` loop used to await a PUT/UPDATE's related-
// contract NETWORK fetch INLINE, pinning the loop for up to
// RELATED_FETCH_TIMEOUT and blocking every queued event behind it — including
// local-store-hit GETs that need no network at all. These tests drive the REAL
// `contract_handling` loop and assert the fix off-loads that wait so unrelated
// work keeps draining.
#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
// These tests intentionally discard `JoinHandle`/`timeout` results in cleanup
// paths (the assertions that matter run before cleanup).
#[allow(clippy::let_underscore_must_use)]
mod hol_4391_tests {
    use super::*;
    use crate::config::GlobalExecutor;
    use crate::contract::executor::mock_wasm_runtime::{ScriptedRun, ValidateOverride};
    use std::sync::Arc;
    use std::time::Duration;

    /// Serializes these tests: `OFF_LOOP_FETCH_OVERRIDE` is a process-global
    /// hook, so two of these tests running concurrently would clobber each
    /// other's stub. Each test holds this guard for its duration. An
    /// async-aware mutex so the guard can be held across the tests' `.await`s.
    static TEST_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// RAII wrapper around `set_off_loop_fetch_override`: installs a stub on
    /// construction and clears it on `Drop`, so a panicking test can't leak a
    /// stale stub into the next test even though they share the process-global
    /// hook. (#4391 SF#5.)
    struct OverrideGuard;
    impl OverrideGuard {
        fn install(stub: OffLoopFetchStub) -> Self {
            set_off_loop_fetch_override(Some(stub));
            OverrideGuard
        }
    }
    impl Drop for OverrideGuard {
        fn drop(&mut self) {
            set_off_loop_fetch_override(None);
        }
    }

    fn make_contract(code_bytes: &[u8]) -> ContractContainer {
        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![]);
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(code),
            params,
        )))
    }

    /// Build a handler over a `MockWasmRuntime` executor (no `op_manager`), with
    /// the given per-contract validate overrides installed, plus the sender
    /// halve used to drive it.
    async fn build_handler(
        overrides: Vec<(ContractInstanceId, ValidateOverride)>,
    ) -> (
        MockWasmContractHandler,
        handler::ContractHandlerChannel<handler::SenderHalve>,
    ) {
        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MockWasmContractHandler::new_test(rcv_halve, None, "hol_4391").await;
        for (id, ov) in overrides {
            handler.runtime_mut().validate_overrides.insert(id, ov);
        }
        (handler, send_halve)
    }

    /// PUT a contract with no related dependencies (a plain local store).
    async fn put_local(
        send: &handler::ContractHandlerChannel<handler::SenderHalve>,
        contract: ContractContainer,
        state: WrappedState,
    ) -> ContractHandlerEvent {
        send.send_to_handler(ContractHandlerEvent::PutQuery {
            key: contract.key(),
            state,
            related_contracts: RelatedContracts::default(),
            contract: Some(contract),
        })
        .await
        .expect("PUT must respond")
    }

    /// The core regression: a PUT whose related fetch hangs must NOT block a
    /// subsequent local-store-hit GET on the serial loop. Before the fix the
    /// GET would wait the full related-fetch timeout; after the fix the fetch
    /// is off-loaded and the GET returns promptly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferred_related_fetch_does_not_block_local_get() {
        let _guard = TEST_GUARD.lock().await;
        // Contract A requests related contract B (which is never local). The
        // off-loop fetch hangs until we release `gate`, simulating a slow /
        // unavailable network GET.
        let contract_a = make_contract(b"hol_a_blocking");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_b_missing").key().id();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        // Off-loop fetch hangs on this gate, then reports B as missing so A
        // ultimately fails with MissingRelated (we only care that it does NOT
        // block the loop while it hangs).
        let gate = Arc::new(tokio::sync::Notify::new());
        let gate_for_stub = gate.clone();
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            let gate = gate_for_stub.clone();
            Box::pin(async move {
                gate.notified().await;
                Err(ExecutorError::missing_related(missing[0]))
            })
        })));

        // Seed a locally-stored contract C (no related, plain local store).
        let contract_c = make_contract(b"hol_c_cached");
        let key_c = contract_c.key();

        // Share the sender across the test task and the background PUT task
        // (the channel itself is not `Clone`).
        let send = Arc::new(send);

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Store C so a later GET for it is a pure local-store hit.
        let put_c = put_local(
            send.as_ref(),
            contract_c,
            WrappedState::new(b"c_state".to_vec()),
        )
        .await;
        assert!(
            matches!(
                put_c,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(_),
                    ..
                }
            ),
            "seed PUT for C must succeed, got {put_c}"
        );

        // Fire the related-needing PUT for A on a background task: it will defer
        // (off-loop fetch hangs on the gate) and not respond until we release it.
        let send_a = send.clone();
        let a_state = WrappedState::new(b"a_state".to_vec());
        let a_task: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(async move {
                send_a
                    .send_to_handler(ContractHandlerEvent::PutQuery {
                        key: key_a,
                        state: a_state,
                        related_contracts: RelatedContracts::default(),
                        contract: Some(contract_a),
                    })
                    .await
            });

        // Give the loop a beat to pick up A's PUT and enter the deferral path.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The crux: a local-store-hit GET for C must complete PROMPTLY while
        // A's fetch is still hanging. Before the fix this would block on A's
        // inline network wait. Use a tight timeout — well under the deferred
        // fetch timeout — so a regression (inline blocking) fails the test.
        let get_c = tokio::time::timeout(
            Duration::from_secs(2),
            send.send_to_handler(ContractHandlerEvent::GetQuery {
                instance_id: *key_c.id(),
                return_contract_code: false,
            }),
        )
        .await
        .expect("GET for cached C must NOT block behind A's deferred network fetch (#4391)")
        .expect("GET for C must respond");

        match get_c {
            ContractHandlerEvent::GetResponse { response, .. } => {
                let store = response.expect("GET for C must succeed");
                assert_eq!(
                    store.state.expect("C has state").as_ref(),
                    b"c_state",
                    "GET must return C's stored state"
                );
            }
            other => panic!("expected GetResponse for C, got {other}"),
        }

        // Release the gate: A's deferred fetch reports B missing, so A's PUT
        // resolves (with a MissingRelated error) and its client is answered.
        gate.notify_one();
        let a_resp = tokio::time::timeout(Duration::from_secs(5), a_task)
            .await
            .expect("A's PUT must eventually resolve after the fetch completes")
            .expect("A task join")
            .expect("A's PUT must respond");
        match a_resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "A's PUT must surface MissingRelated once the off-loop fetch fails"
                );
            }
            other => panic!("expected PutResponse for A, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// The OFF-LOOP related fetch must not be cut shorter than the GET sub-op's
    /// own budget. The fetch runs in a spawned waiter (not on the serial loop),
    /// so a short cap no longer serves a "don't stall the loop" purpose; an
    /// inline-era 10s value prematurely failed slow-but-successful related GETs
    /// on lossy/cross-node paths — the exact #4391 / freenet-mail scenario.
    #[test]
    fn off_loop_related_fetch_budget_is_at_least_operation_ttl() {
        assert!(
            DEFERRED_RELATED_FETCH_TIMEOUT >= crate::config::OPERATION_TTL,
            "off-loop related-fetch budget {:?} must be >= GET OPERATION_TTL {:?} \
             so a slow-but-successful related GET is not failed prematurely (#4391)",
            DEFERRED_RELATED_FETCH_TIMEOUT,
            crate::config::OPERATION_TTL,
        );
    }

    /// Round 5 (no FIFO block): TWO same-key PUTs can both defer CONCURRENTLY
    /// (the stash is keyed by unique `deferral_id`, not by contract key), and
    /// BOTH clients must receive their own response — neither responder is lost
    /// or evicted by the other. This is the headline behavior the per-contract
    /// FIFO deletion enables; the deleted machinery used to serialize same-key
    /// ops, which would have masked a lost-responder bug class. (#4391)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_same_key_deferrals_both_clients_answered() {
        let _guard = TEST_GUARD.lock().await;

        let key_seed = b"concurrent_a";
        let key_a = make_contract(key_seed).key();
        let id_b = *make_contract(b"concurrent_b").key().id();
        let b_state = WrappedState::new(b"concurrent_b_state".to_vec());

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        // Off-loop fetch returns B's state, gated by a semaphore so BOTH PUTs
        // park before either resumes — two same-key deferrals live at once.
        // (A semaphore avoids the Notify registration race for two waiters.)
        let sem = Arc::new(tokio::sync::Semaphore::new(0));
        let sem_for_stub = sem.clone();
        let b_for_stub = b_state.clone();
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            let sem = sem_for_stub.clone();
            let b = b_for_stub.clone();
            Box::pin(async move {
                let _permit = sem.acquire_owned().await.expect("sem open");
                Ok(vec![(missing[0], b.clone())])
            })
        })));

        let send = Arc::new(send);
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Fire TWO same-key PUTs; each defers and parks its own responder.
        let send1 = send.clone();
        let contract1 = make_contract(key_seed);
        let put1 = GlobalExecutor::spawn(async move {
            send1
                .send_to_handler(ContractHandlerEvent::PutQuery {
                    key: contract1.key(),
                    state: WrappedState::new(b"a_state_1".to_vec()),
                    related_contracts: RelatedContracts::default(),
                    contract: Some(contract1),
                })
                .await
        });
        let send2 = send.clone();
        let contract2 = make_contract(key_seed);
        let put2 = GlobalExecutor::spawn(async move {
            send2
                .send_to_handler(ContractHandlerEvent::PutQuery {
                    key: contract2.key(),
                    state: WrappedState::new(b"a_state_2".to_vec()),
                    related_contracts: RelatedContracts::default(),
                    contract: Some(contract2),
                })
                .await
        });

        // Let the loop pick up both PUTs and enter the deferral path for each.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Release BOTH parked fetches at once.
        sem.add_permits(2);

        // BOTH clients must be answered (neither responder lost) — the core
        // invariant of deferral_id-keyed stashing. (We assert each gets a
        // PutResponse, not a specific Ok/Err, so the test is robust to the
        // mock's merge semantics; the lost-responder failure class would show
        // up as one of these hanging.)
        let r1 = tokio::time::timeout(Duration::from_secs(5), put1)
            .await
            .expect("put1 must not hang")
            .expect("put1 join")
            .expect("put1 responds");
        let r2 = tokio::time::timeout(Duration::from_secs(5), put2)
            .await
            .expect("put2 must not hang")
            .expect("put2 join")
            .expect("put2 responds");
        assert!(
            matches!(r1, ContractHandlerEvent::PutResponse { .. }),
            "put1 client must get its own PutResponse, got {r1}"
        );
        assert!(
            matches!(r2, ContractHandlerEvent::PutResponse { .. }),
            "put2 client must get its own PutResponse, got {r2}"
        );

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// Round 5 (accepted reordering): a SAME-KEY GET issued while a PUT for that
    /// key is mid-deferral runs PROMPTLY — it is NOT held behind the deferring
    /// PUT. This is the intended behavior after the per-contract-FIFO machinery
    /// was removed: the GET observes the pre-PUT state (here: not-found, since
    /// the contract was never stored before the deferring PUT), while the PUT's
    /// own response is still delivered only on commit. The point of this test is
    /// the absence of holding — the GET must not wait for the gated fetch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_key_get_during_deferred_put_runs_promptly() {
        let _guard = TEST_GUARD.lock().await;

        // Contract A requests related B (never local); its off-loop fetch hangs
        // on the gate, so A's PUT stays mid-deferral.
        let contract_a = make_contract(b"reorder_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"reorder_b").key().id();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        let gate = Arc::new(tokio::sync::Notify::new());
        let gate_for_stub = gate.clone();
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            let gate = gate_for_stub.clone();
            Box::pin(async move {
                gate.notified().await;
                Err(ExecutorError::missing_related(missing[0]))
            })
        })));

        let send = Arc::new(send);
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Fire A's related-needing PUT in the background: it defers and parks.
        let send_a = send.clone();
        let a_task: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(async move {
                send_a
                    .send_to_handler(ContractHandlerEvent::PutQuery {
                        key: key_a,
                        state: WrappedState::new(b"a_state".to_vec()),
                        related_contracts: RelatedContracts::default(),
                        contract: Some(contract_a),
                    })
                    .await
            });

        // Let the loop pick up A's PUT and enter the deferral path.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // A SAME-KEY GET for A must resolve PROMPTLY (NOT held behind the gated
        // PUT). With no holding, it runs immediately and observes the pre-PUT
        // state — the contract isn't stored yet, so a not-found GetResponse.
        let get_a = tokio::time::timeout(
            Duration::from_secs(2),
            send.send_to_handler(ContractHandlerEvent::GetQuery {
                instance_id: *key_a.id(),
                return_contract_code: false,
            }),
        )
        .await
        .expect("same-key GET must NOT be held behind the deferring PUT (#4391 round 5)")
        .expect("GET for A must respond");
        match get_a {
            ContractHandlerEvent::GetResponse { .. } => {
                // Either a not-found response (contract not stored yet) — the
                // expected pre-PUT observation. We only assert it RAN promptly.
            }
            other => panic!("expected GetResponse for A, got {other}"),
        }

        // Release the gate so A's PUT resolves and its client is answered.
        gate.notify_one();
        let a_resp = tokio::time::timeout(Duration::from_secs(5), a_task)
            .await
            .expect("A's PUT must resolve after the fetch completes")
            .expect("A task join")
            .expect("A's PUT must respond");
        assert!(
            matches!(a_resp, ContractHandlerEvent::PutResponse { .. }),
            "A's PUT must still receive its own response on resume"
        );

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// Happy path: a deferred PUT whose off-loop fetch SUCCEEDS resumes and
    /// stores the contract, answering the original client.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferred_related_fetch_success_resumes_and_completes() {
        let _guard = TEST_GUARD.lock().await;
        let contract_a = make_contract(b"hol_resume_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_resume_b").key().id();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        // Off-loop fetch returns a synthetic state for B immediately.
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            Box::pin(async move {
                Ok(missing
                    .into_iter()
                    .map(|id| (id, WrappedState::new(b"fetched_b".to_vec())))
                    .collect())
            })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("deferred PUT must resume and respond")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                new_value.expect("deferred PUT must succeed once B is supplied off-loop");
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// Failure path: a deferred PUT whose off-loop fetch FAILS surfaces
    /// MissingRelated to the client (and does not hang or defer again).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferred_related_fetch_failure_surfaces_missing_related() {
        let _guard = TEST_GUARD.lock().await;
        let contract_a = make_contract(b"hol_fail_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_fail_b").key().id();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            Box::pin(async move { Err(ExecutorError::missing_related(missing[0])) })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("deferred PUT must resolve (not hang) when the fetch fails")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "a failed off-loop fetch must surface an error to the client"
                );
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// A PUT whose related contract IS already local must NOT defer — it
    /// behaves exactly as before (no off-loop fetch invoked). We prove this by
    /// installing an override that PANICS if called, then PUTting a contract
    /// whose only related dependency was stored locally first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_related_local_does_not_defer() {
        let _guard = TEST_GUARD.lock().await;
        // B is stored locally first, then A requests B — validation must
        // resolve B from the local store and never reach the off-loop fetch.
        let contract_b = make_contract(b"hol_local_b");
        let key_b = contract_b.key();
        let contract_a = make_contract(b"hol_local_a");
        let key_a = contract_a.key();

        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![*key_b.id()]),
        )])
        .await;

        // Override panics if the off-loop fetch is ever invoked — proving the
        // all-local path never defers.
        set_off_loop_fetch_override(Some(Arc::new(|_missing| {
            Box::pin(async move {
                panic!("off-loop fetch must NOT be invoked when all related contracts are local");
            })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Store B locally so A's RequestRelated([B]) resolves without network.
        let put_b = put_local(&send, contract_b, WrappedState::new(b"b_state".to_vec())).await;
        assert!(
            matches!(
                put_b,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(_),
                    ..
                }
            ),
            "seed PUT for B must succeed, got {put_b}"
        );

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("local-resolvable PUT must complete inline")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                new_value.expect("A must store successfully when B is already local");
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    /// One-deferral cap: a contract that requests a related contract on EVERY
    /// validate call (even after the first round's states are supplied) must
    /// surface MissingRelated after a single deferral — it must NOT spawn a
    /// second off-loop fetch or loop forever. We prove the off-loop fetch runs
    /// at most once by counting stub invocations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn second_related_request_after_resume_does_not_defer_again() {
        let _guard = TEST_GUARD.lock().await;
        let contract_a = make_contract(b"hol_cap_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"hol_cap_b").key().id();

        // AlwaysRequestRelated: returns RequestRelated([B]) on EVERY call, so
        // even after B is supplied on resume the contract asks again — the
        // depth>1 / one-deferral cap must kick in.
        let (handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::AlwaysRequestRelated(vec![id_b]),
        )])
        .await;

        // Count how many times the off-loop fetch is invoked. The cap requires
        // it be invoked AT MOST once (the first/only deferral); the resume must
        // not spawn a second fetch.
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_stub = calls.clone();
        set_off_loop_fetch_override(Some(Arc::new(move |missing| {
            calls_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                Ok(missing
                    .into_iter()
                    .map(|id| (id, WrappedState::new(b"fetched".to_vec())))
                    .collect())
            })
        })));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: key_a,
                state: WrappedState::new(b"a_state".to_vec()),
                related_contracts: RelatedContracts::default(),
                contract: Some(contract_a),
            }),
        )
        .await
        .expect("deferred-then-resumed PUT must resolve, not loop forever")
        .expect("PUT must respond");

        match resp {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "a contract that keeps requesting related contracts must be \
                     rejected after one deferral (depth=1 / one-deferral cap)"
                );
            }
            other => panic!("expected PutResponse, got {other}"),
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the off-loop fetch must run exactly once — the resume must NOT \
             defer again (#4391 one-deferral cap)"
        );

        set_off_loop_fetch_override(None);
        handle.abort();
    }

    // ========================================================================
    // SHOULD-FIX #5: UPDATE-path deferral, real off-loop fetch, cap, disconnect
    // ========================================================================

    /// An UPDATE whose `update_state` returns `requires(missing)` for a related
    /// contract not held locally must defer off-loop and, on success, resume and
    /// apply the merge — exercising the UPDATE response path through the loop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn update_path_deferral_resumes_and_applies() {
        use crate::contract::executor::mock_wasm_runtime::UpdateOverride;
        let _guard = TEST_GUARD.lock().await;

        let contract_k = make_contract(b"upd_defer_k");
        let key_k = contract_k.key();
        let id_b = *make_contract(b"upd_defer_related").key().id();

        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MockWasmContractHandler::new_test(rcv_halve, None, "upd_defer").await;
        handler
            .runtime_mut()
            .update_overrides
            .insert(*key_k.id(), UpdateOverride::RequiresRelated(vec![id_b]));
        let send = Arc::new(send_halve);

        let _override =
            OverrideGuard::install(Arc::new(move |missing: Vec<ContractInstanceId>| {
                Box::pin(async move {
                    Ok(missing
                        .into_iter()
                        .map(|id| (id, WrappedState::new(b"b_state".to_vec())))
                        .collect())
                })
            }));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Seed K's initial state (no override fires for a State PUT, only the
        // delta UPDATE triggers RequiresRelated).
        let put = put_local(send.as_ref(), contract_k, WrappedState::new(vec![0])).await;
        assert!(matches!(
            put,
            ContractHandlerEvent::PutResponse {
                new_value: Ok(_),
                ..
            }
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::UpdateQuery {
                key: key_k,
                data: freenet_stdlib::prelude::UpdateData::Delta(StateDelta::from(vec![1])),
                related_contracts: RelatedContracts::default(),
            }),
        )
        .await
        .expect("deferred UPDATE must resume and respond")
        .expect("UPDATE responds");
        match resp {
            ContractHandlerEvent::UpdateResponse { new_value, .. } => {
                new_value.expect("UPDATE must succeed once B is supplied off-loop");
            }
            ContractHandlerEvent::UpdateNoChange { .. } => {}
            other => panic!("expected UpdateResponse, got {other}"),
        }

        handle.abort();
    }

    /// An UPDATE-path deferral whose off-loop fetch FAILS surfaces an
    /// UpdateResponse error (not a hang, not a PUT response).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn update_path_deferral_failure_surfaces_update_error() {
        use crate::contract::executor::mock_wasm_runtime::UpdateOverride;
        let _guard = TEST_GUARD.lock().await;

        let contract_k = make_contract(b"upd_fail_k");
        let key_k = contract_k.key();
        let id_b = *make_contract(b"upd_fail_related").key().id();

        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MockWasmContractHandler::new_test(rcv_halve, None, "upd_fail").await;
        handler
            .runtime_mut()
            .update_overrides
            .insert(*key_k.id(), UpdateOverride::RequiresRelated(vec![id_b]));
        let send = Arc::new(send_halve);

        let _override =
            OverrideGuard::install(Arc::new(move |missing: Vec<ContractInstanceId>| {
                Box::pin(async move { Err(ExecutorError::missing_related(missing[0])) })
            }));

        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let put = put_local(send.as_ref(), contract_k, WrappedState::new(vec![0])).await;
        assert!(matches!(
            put,
            ContractHandlerEvent::PutResponse {
                new_value: Ok(_),
                ..
            }
        ));

        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(ContractHandlerEvent::UpdateQuery {
                key: key_k,
                data: freenet_stdlib::prelude::UpdateData::Delta(StateDelta::from(vec![1])),
                related_contracts: RelatedContracts::default(),
            }),
        )
        .await
        .expect("deferred UPDATE must resolve (not hang) when the fetch fails")
        .expect("UPDATE responds");
        match resp {
            ContractHandlerEvent::UpdateResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "a failed off-loop fetch must surface an UpdateResponse error"
                );
            }
            other => panic!("expected UpdateResponse error, got {other}"),
        }

        handle.abort();
    }

    /// Unit-test the production `SubOpGetOutcome → Result` mapping that backs
    /// the REAL `fetch_related_off_loop` (the path the stubbed loop tests skip).
    /// `Found` → the fetched state; `NotFound`/`Infra` → `MissingRelated`. This
    /// is deterministic (no OpManager / network timing), covering the mapping
    /// without a flaky real-network harness. (#4391 SF#5.)
    #[test]
    fn off_loop_sub_op_outcome_mapping() {
        use crate::operations::get::op_ctx_task::SubOpGetOutcome;
        let id = *make_contract(b"map_id").key().id();

        let found = SubOpGetOutcome::Found(crate::operations::get::GetResult::new(
            WrappedState::new(b"fetched".to_vec()),
            None,
        ));
        match map_sub_op_outcome(found, id) {
            Ok(state) => assert_eq!(state.as_ref(), b"fetched"),
            Err(e) => panic!("Found must map to the fetched state, got {e}"),
        }

        let not_found = SubOpGetOutcome::NotFound("nope".to_string());
        assert!(
            map_sub_op_outcome(not_found, id).is_err(),
            "NotFound must map to MissingRelated"
        );

        let infra = SubOpGetOutcome::Infra(crate::operations::OpError::UnexpectedOpState);
        assert!(
            map_sub_op_outcome(infra, id).is_err(),
            "Infra must map to MissingRelated"
        );
    }

    /// A client that disconnects mid-deferral (drops its response receiver) must
    /// NOT leak the stashed responder or wedge the contract. Proven by: after the
    /// client drops, a fresh same-key op resolves promptly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn client_disconnect_during_deferral_does_not_wedge() {
        let _guard = TEST_GUARD.lock().await;

        let contract_k = make_contract(b"disc_k");
        let key_k = contract_k.key();
        let id_b = *make_contract(b"disc_related").key().id();

        let (handler, send) = build_handler(vec![(
            *key_k.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        // Off-loop fetch resolves quickly so the deferral completes shortly
        // after the client has gone away.
        let _override =
            OverrideGuard::install(Arc::new(move |missing: Vec<ContractInstanceId>| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Ok(missing
                        .into_iter()
                        .map(|id| (id, WrappedState::new(b"b_state".to_vec())))
                        .collect())
                })
            }));

        let send = Arc::new(send);
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Fire K's PUT with a SHORT client-side timeout so the client gives up
        // (drops its receiver) while the fetch is still in flight.
        let send_k = send.clone();
        let contract_k_owned = contract_k;
        let disconnect_task = GlobalExecutor::spawn(async move {
            let _ = tokio::time::timeout(
                Duration::from_millis(50),
                send_k.send_to_handler(ContractHandlerEvent::PutQuery {
                    key: key_k,
                    state: WrappedState::new(vec![9]),
                    related_contracts: RelatedContracts::default(),
                    contract: Some(contract_k_owned),
                }),
            )
            .await;
            // On timeout, the future is dropped → response receiver dropped →
            // the client has "disconnected".
        });
        let _ = disconnect_task.await;

        // Let the deferral resume and discover the dropped responder.
        tokio::time::sleep(Duration::from_millis(400)).await;

        // A fresh same-key GET must resolve promptly — proving the disconnected
        // deferral didn't wedge the contract or leak its responder.
        let resp = tokio::time::timeout(
            DEFERRED_RELATED_FETCH_TIMEOUT + Duration::from_secs(2),
            send.send_to_handler(ContractHandlerEvent::GetQuery {
                instance_id: *key_k.id(),
                return_contract_code: false,
            }),
        )
        .await
        .expect("same-key op after a disconnected deferral must not hang")
        .expect("GET responds");
        assert!(
            matches!(resp, ContractHandlerEvent::GetResponse { .. }),
            "expected a GetResponse"
        );

        handle.abort();
    }

    /// The in-flight deferral cap is enforced: once `MAX_INFLIGHT_DEFERRALS`
    /// PUTs are deferred (gated open), the next related-needing PUT must FAIL
    /// FAST with MissingRelated rather than (a) deferring a 257th waiter or
    /// (b) falling back to an inline network fetch that would re-stall the loop.
    /// (#4391 MUST-FIX #2 boundary / SHOULD-FIX #4.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deferral_cap_fails_fast_with_missing_related() {
        let _guard = TEST_GUARD.lock().await;

        // All deferrals hang until `released` is set, so they stay in-flight,
        // saturating the cap. Poll the flag (release-all) so every parked stub
        // drains on cleanup, not just one.
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let released_stub = released.clone();
        let _override =
            OverrideGuard::install(Arc::new(move |missing: Vec<ContractInstanceId>| {
                let released = released_stub.clone();
                Box::pin(async move {
                    while !released.load(std::sync::atomic::Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(ExecutorError::missing_related(missing[0]))
                })
            }));

        // Build a handler whose contracts each request a distinct related id.
        // Use MAX_INFLIGHT_DEFERRALS distinct contracts to saturate the cap,
        // plus one more to probe the over-cap behavior.
        let n = MAX_INFLIGHT_DEFERRALS;
        let mut overrides = Vec::with_capacity(n + 1);
        let mut put_keys = Vec::with_capacity(n + 1);
        let mut put_contracts = Vec::with_capacity(n + 1);
        for i in 0..=n {
            let c = make_contract(format!("cap_{i}").as_bytes());
            let related = *make_contract(format!("cap_rel_{i}").as_bytes()).key().id();
            overrides.push((
                *c.key().id(),
                ValidateOverride::RequestRelated(vec![related]),
            ));
            put_keys.push(c.key());
            put_contracts.push(c);
        }

        let (handler, send) = build_handler(overrides).await;
        let send = Arc::new(send);
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        // Fire the first N PUTs in the background; each defers and parks (gate
        // held), saturating the cap. We don't await them.
        let mut tasks = Vec::with_capacity(n);
        for i in 0..n {
            let send_i = send.clone();
            let key_i = put_keys[i];
            let contract_i = put_contracts[i].clone();
            tasks.push(GlobalExecutor::spawn(async move {
                let _ = send_i
                    .send_to_handler(ContractHandlerEvent::PutQuery {
                        key: key_i,
                        state: WrappedState::new(vec![i as u8]),
                        related_contracts: RelatedContracts::default(),
                        contract: Some(contract_i),
                    })
                    .await;
            }));
        }

        // Wait until the cap is saturated (all N deferrals registered). Poll by
        // giving the loop time to process all N PUTs into deferrals.
        tokio::time::sleep(Duration::from_millis(800)).await;

        // The (N+1)th related-needing PUT must FAIL FAST: at capacity, the loop
        // surfaces MissingRelated immediately instead of deferring or doing an
        // inline fetch. It must respond well within the off-loop timeout
        // (which would apply if it had wrongly deferred behind the gate).
        let over = tokio::time::timeout(
            Duration::from_secs(3),
            send.send_to_handler(ContractHandlerEvent::PutQuery {
                key: put_keys[n],
                state: WrappedState::new(vec![0xFF]),
                related_contracts: RelatedContracts::default(),
                contract: Some(put_contracts[n].clone()),
            }),
        )
        .await
        .expect("over-cap PUT must FAIL FAST, not block behind the gated cap")
        .expect("over-cap PUT responds");
        match over {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "over-cap related-needing PUT must surface MissingRelated"
                );
            }
            other => panic!("expected PutResponse error at cap, got {other}"),
        }

        // Release all gated deferrals so the saturating PUTs drain, then clean up.
        released.store(true, std::sync::atomic::Ordering::SeqCst);
        for t in tasks {
            let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
        }
        handle.abort();
    }

    // ========================================================================
    // ResumeGuard exactly-once delivery + off-loop fetch mapping (unit)
    // ========================================================================

    fn instance_id(seed: &[u8]) -> ContractInstanceId {
        *make_contract(seed).key().id()
    }

    /// Round-4 (unit): the `ResumeGuard` delivers a terminal MissingRelated
    /// resume when the off-loop waiter is DROPPED before sending (task
    /// cancelled / panicked). Driving that resume through `handle_deferred_resume`
    /// must answer the parked client with MissingRelated — the wedge-prevention
    /// comes from the drop-guard's exactly-once delivery.
    #[tokio::test]
    async fn dropped_waiter_guard_answers_client_missing_related() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DeferredResume>();
        // This test exercises the related-fetch resume path only; the export
        // resume channel is unused here.
        let (export_tx, _export_rx) = tokio::sync::mpsc::unbounded_channel::<ExportResume>();
        let mut ctx = DeferralCtx::new(tx.clone(), export_tx);
        let mut handler =
            MockWasmContractHandler::new_test(handler::contract_handler_channel().1, None, "drop")
                .await;

        let contract = make_contract(b"drop_guard_k");
        let key = contract.key();

        // Stash the client responder (as `maybe_defer_upsert` would) for an
        // in-flight deferral whose waiter is about to be dropped.
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        ctx.stashed
            .insert(7, StashedResponder::for_test(7, resp_tx));

        // Build the waiter's guard and DROP it WITHOUT calling `send` (simulating
        // the waiter task being cancelled/dropped before the fetch reports).
        {
            let guard = ResumeGuard::new(ResumePayload {
                resume_tx: tx,
                deferral_id: 7,
                key,
                update: Either::Left(WrappedState::new(vec![1])),
                related_contracts: RelatedContracts::default(),
                code: Some(contract),
                is_put: true,
            });
            drop(guard);
        }

        // The guard's Drop must have delivered exactly one terminal resume.
        let resume = rx.try_recv().expect("Drop must deliver one resume");
        assert_eq!(resume.deferral_id, 7);
        assert!(
            resume.fetched.is_err(),
            "dropped-waiter resume must carry a MissingRelated fetch error"
        );
        assert!(rx.try_recv().is_err(), "exactly one resume — no extra");

        // Process it on the loop: the parked client is answered with a PUT error.
        handle_deferred_resume(&mut handler, &mut ctx, resume)
            .await
            .expect("resume handled");
        assert!(
            !ctx.stashed.contains_key(&7),
            "the stashed responder must be consumed (no leak)"
        );

        let (_id, ev) = resp_rx.await.expect("client must be answered");
        match ev {
            ContractHandlerEvent::PutResponse { new_value, .. } => {
                assert!(
                    new_value.is_err(),
                    "dropped-waiter PUT must surface MissingRelated"
                );
            }
            other => panic!("expected PutResponse error, got {other}"),
        }
    }

    /// Round-4 (unit): the success path delivers EXACTLY ONE resume — the
    /// explicit `send` consumes the payload, so the guard's `Drop` is a no-op
    /// (never a double-send).
    #[tokio::test]
    async fn resume_guard_success_sends_exactly_one_resume() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DeferredResume>();
        let key = make_contract(b"once_k").key();

        let guard = ResumeGuard::new(ResumePayload {
            resume_tx: tx,
            deferral_id: 3,
            key,
            update: Either::Left(WrappedState::new(vec![1])),
            related_contracts: RelatedContracts::default(),
            code: None,
            is_put: false,
        });
        // Success send, then the guard is dropped at end of scope.
        guard.send(Ok(vec![]));

        let resume = rx.try_recv().expect("success path must deliver one resume");
        assert_eq!(resume.deferral_id, 3);
        assert!(
            resume.fetched.is_ok(),
            "success resume must carry the real fetch result"
        );
        assert!(
            rx.try_recv().is_err(),
            "exactly one resume — Drop must NOT send again after a success send"
        );
    }

    /// SHOULD-FIX (unit): with no op_manager, the real `fetch_related_off_loop`
    /// (override cleared) surfaces MissingRelated for the missing ids — covers
    /// the orchestration's `op_manager.is_none()` branch.
    #[tokio::test]
    async fn fetch_related_off_loop_no_op_manager_is_missing_related() {
        let _guard = TEST_GUARD.lock().await;
        set_off_loop_fetch_override(None);
        let id = instance_id(b"off_loop_none");
        let result = fetch_related_off_loop(None, vec![id]).await;
        assert!(
            result.is_err(),
            "no op_manager must surface MissingRelated, got {result:?}"
        );
    }

    /// Regression for #4681 (partial hardening): when `register_contract_notifier`
    /// fails (here: the per-contract subscriber limit is exhausted), the
    /// `RegisterSubscriberListener` handler MUST return a
    /// `RegisterSubscriberListenerResponse { result: Err(_) }` so the subscribing
    /// client sees a real failure. Before the fix the handler always returned a
    /// success response, which `client_events.rs` converted into a fake
    /// successful subscribe.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn register_subscriber_listener_surfaces_registration_failure() {
        use crate::contract::executor::{ContractExecutor, MAX_SUBSCRIBERS_PER_CONTRACT};

        let (send_halve, rcv_halve, _) = handler::contract_handler_channel();
        let mut handler = MockWasmContractHandler::new_test(rcv_halve, None, "reg_fail_4681").await;

        let instance_id = *make_contract(b"reg_fail_4681").key().id();

        // Saturate the per-contract subscriber cap directly on the executor so
        // the next registration through the handler event path is rejected.
        for _ in 0..MAX_SUBSCRIBERS_PER_CONTRACT {
            let (tx, _rx) = tokio::sync::mpsc::channel(64);
            handler
                .executor()
                .register_contract_notifier(
                    instance_id,
                    crate::client_events::ClientId::next(),
                    tx,
                    None,
                )
                .expect("registration within limit should succeed");
        }

        // One more registration, driven through `handle_contract_event`, must
        // fail — and that failure must ride back on the response variant.
        let (tx, _rx) = tokio::sync::mpsc::channel(64);
        let event = ContractHandlerEvent::RegisterSubscriberListener {
            key: instance_id,
            client_id: crate::client_events::ClientId::next(),
            summary: None,
            subscriber_listener: tx,
        };
        let send_fut = send_halve.send_to_handler(event);
        let recv_fut = async {
            let (id, received, _priority) = handler
                .channel()
                .recv_from_sender()
                .await
                .expect("handler channel should be open");
            handle_contract_event(
                &mut handler,
                id,
                received,
                &std::sync::Arc::new(user_input::AutoApprovePrompter),
                None,
                None,
            )
            .await
            .expect("dispatch must not error");
        };
        let (send_res, ()) = tokio::join!(send_fut, recv_fut);
        match send_res.expect("must receive a response") {
            ContractHandlerEvent::RegisterSubscriberListenerResponse { result } => {
                assert!(
                    result.is_err(),
                    "a registration rejected by the subscriber limit must surface \
                     an error to the client, not a fake success"
                );
            }
            other => panic!("expected RegisterSubscriberListenerResponse, got {other}"),
        }
    }

    // ---- #5544: a parked delegate must not stall the loop, and must not be
    // ---- re-entered while parked.

    /// A prompter that hangs until released, standing in for a human who has
    /// not clicked yet. The real `DashboardPrompter` waits up to
    /// `USER_INPUT_TIMEOUT` (60s) for exactly this.
    struct GatedPrompter {
        /// One permit per prompt the test lets through. A `Notify` will not do:
        /// it stores at most ONE permit, so a test that has to release two
        /// prompts could silently lose a release.
        gate: Arc<tokio::sync::Semaphore>,
    }

    impl user_input::UserInputPrompter for GatedPrompter {
        async fn prompt(
            &self,
            _request: &UserInputRequest<'static>,
            _delegate_key: &str,
            _caller: user_input::CallerIdentity,
        ) -> Option<(usize, ClientResponse<'static>)> {
            self.gate
                .acquire()
                .await
                .expect("prompt gate closed")
                .forget();
            Some((0, ClientResponse::new(b"approved".to_vec())))
        }
    }

    fn test_delegate_key() -> DelegateKey {
        DelegateKey::new([7u8; 32], freenet_stdlib::prelude::CodeHash::new([7u8; 32]))
    }

    /// An `ApplicationMessage` carrying `payload`, followed by the prompt that
    /// makes the delegate park. Used to prove pre-park output survives a park —
    /// see `a_delegate_that_parks_twice_still_answers_its_client_once`.
    fn payload_then_prompt(payload: &[u8]) -> Vec<OutboundDelegateMsg> {
        let mut out = vec![OutboundDelegateMsg::ApplicationMessage(
            freenet_stdlib::prelude::ApplicationMessage::new(payload.to_vec()),
        )];
        out.extend(prompt_outbound());
        out
    }

    /// One scripted `RequestUserInput`, which is what makes the delegate park.
    fn prompt_outbound() -> Vec<OutboundDelegateMsg> {
        let message = freenet_stdlib::prelude::NotificationMessage::try_from(&serde_json::json!({
            "message": "allow?"
        }))
        .expect("notification message");
        vec![OutboundDelegateMsg::RequestUserInput(UserInputRequest {
            request_id: 1,
            message,
            responses: vec![ClientResponse::new(b"yes".to_vec())],
        })]
    }

    fn delegate_event(key: &DelegateKey) -> ContractHandlerEvent {
        ContractHandlerEvent::DelegateRequest {
            req: DelegateRequest::ApplicationMessages {
                key: key.clone(),
                params: Parameters::from(Vec::new()),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    freenet_stdlib::prelude::ApplicationMessage::new(b"go".to_vec()),
                )],
            },
            origin_contract: None,
            connection_scope: crate::client_events::ConnectionScope::Local,
            user_context: None,
        }
    }

    /// THE #5544 REGRESSION TEST: unrelated contract work must keep draining
    /// while a delegate sits parked on a permission prompt.
    ///
    /// Before the fix, `prompter.prompt(...)` was awaited on the serial
    /// `contract_handling` loop, so a delegate awaiting a human froze every
    /// GET/PUT/UPDATE/subscribe on the node for up to `USER_INPUT_TIMEOUT`
    /// (60s). Asserting only that the delegate still works would NOT catch a
    /// regression to inline blocking — the delegate works either way. What
    /// distinguishes the two is whether anything ELSE can run meanwhile, so
    /// that is what this asserts, under a timeout far below 60s.
    #[tokio::test]
    async fn parked_delegate_prompt_does_not_block_the_loop() {
        let _guard = TEST_GUARD.lock().await;
        let (mut handler, send) = build_handler(vec![]).await;
        let script = handler.runtime_mut().delegate_script.clone();
        script.lock().unwrap().push_back(prompt_outbound().into());

        // A locally stored contract, so the GET below needs no network and its
        // only possible source of delay is the loop being blocked.
        let contract_c = make_contract(b"park_c_cached");
        let key_c = contract_c.key();

        let send = Arc::new(send);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            GatedPrompter { gate: gate.clone() },
        ));

        let put_c = put_local(
            send.as_ref(),
            contract_c,
            WrappedState::new(b"c_state".to_vec()),
        )
        .await;
        assert!(
            matches!(
                put_c,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(_),
                    ..
                }
            ),
            "seed PUT for C must succeed, got {put_c}"
        );

        // Drive the delegate on a background task: it emits RequestUserInput,
        // parks, and stays parked until the gate opens.
        let send_d = send.clone();
        let key_d = test_delegate_key();
        let delegate_task: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(
                async move { send_d.send_to_handler(delegate_event(&key_d)).await },
            );

        // Let the loop pick up the delegate request and park it.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // The crux. Inline blocking makes this time out.
        let get_c = tokio::time::timeout(
            Duration::from_secs(2),
            send.send_to_handler(ContractHandlerEvent::GetQuery {
                instance_id: *key_c.id(),
                return_contract_code: false,
            }),
        )
        .await
        .expect(
            "a local-store GET must NOT block behind a delegate parked on a \
             user prompt (#5544)",
        )
        .expect("GET for C must respond");
        match get_c {
            ContractHandlerEvent::GetResponse { response, .. } => {
                let store = response.expect("GET for C must succeed");
                assert_eq!(
                    store.state.expect("C has state").as_ref(),
                    b"c_state",
                    "GET must return C's stored state"
                );
            }
            other => panic!("expected GetResponse for C, got {other}"),
        }

        // Release the human: the delegate resumes and its client is answered.
        gate.add_permits(1);
        let resp = tokio::time::timeout(Duration::from_secs(5), delegate_task)
            .await
            .expect("the parked delegate must resolve once the prompt is answered")
            .expect("delegate task join")
            .expect("delegate request must be answered");
        assert!(
            matches!(resp, ContractHandlerEvent::DelegateResponse(_)),
            "parked delegate must answer its original client exactly once, got {resp}"
        );

        handle.abort();
    }

    /// A delegate that prompts TWICE parks, resumes, and parks again. Its
    /// client must still be answered exactly once, at the end.
    ///
    /// This exercises the path where the parked client responder is carried
    /// FORWARD into the second continuation rather than being answered early
    /// or dropped. Getting it wrong gives either a client that hangs forever
    /// or one that receives a truncated first response — neither of which the
    /// single-prompt tests can see.
    #[tokio::test]
    async fn a_delegate_that_parks_twice_still_answers_its_client_once() {
        let _guard = TEST_GUARD.lock().await;
        let (mut handler, send) = build_handler(vec![]).await;
        let script = handler.runtime_mut().delegate_script.clone();
        let calls = handler.runtime_mut().delegate_calls.clone();
        // Emit a payload BEFORE each prompt. Without a payload this test could
        // not see B3 — the bug where a second park silently discarded
        // everything the delegate had emitted before the first one — because
        // `accumulated` was empty in both parks and the only assertion was that
        // *some* DelegateResponse came back.
        script
            .lock()
            .unwrap()
            .push_back(payload_then_prompt(b"before-park-1").into());
        script
            .lock()
            .unwrap()
            .push_back(payload_then_prompt(b"before-park-2").into());

        let send = Arc::new(send);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            GatedPrompter { gate: gate.clone() },
        ));

        let send_d = send.clone();
        let key_d = test_delegate_key();
        let delegate_task: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(
                async move { send_d.send_to_handler(delegate_event(&key_d)).await },
            );

        // First park.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "first entry into the delegate"
        );
        assert!(!delegate_task.is_finished(), "must be parked on prompt 1");

        // Release prompt 1 -> resume -> the delegate prompts again -> re-park.
        gate.add_permits(1);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "the resume must have re-entered the delegate exactly once"
        );
        assert!(
            !delegate_task.is_finished(),
            "must be parked AGAIN on prompt 2; answering the client here would \
             truncate the round-trip"
        );

        // Release prompt 2: the delegate finishes and the ORIGINAL client
        // responder, carried across both parks, is answered.
        gate.add_permits(1);
        let resp = tokio::time::timeout(Duration::from_secs(5), delegate_task)
            .await
            .expect("a twice-parked delegate must still resolve")
            .expect("join")
            .expect("the original client must be answered after the second park");
        let ContractHandlerEvent::DelegateResponse(msgs) = resp else {
            panic!("expected one DelegateResponse covering the whole round-trip, got {resp}");
        };

        // THE B3 ASSERTION. Both pre-park payloads must survive to the single
        // final response. Before the fix, the resumed run's `accumulated` was
        // dropped on the `Parked` arm, so `before-park-1` vanished.
        let payloads: Vec<Vec<u8>> = msgs
            .iter()
            .filter_map(|m| match m {
                OutboundDelegateMsg::ApplicationMessage(am) => Some(am.payload.clone()),
                _ => None,
            })
            .collect();
        assert!(
            payloads.iter().any(|p| p == b"before-park-1"),
            "output emitted before the FIRST park must survive the second park \
             (#5544 B3); got {payloads:?}"
        );
        assert!(
            payloads.iter().any(|p| p == b"before-park-2"),
            "output emitted before the second park must survive; got {payloads:?}"
        );

        handle.abort();
    }

    /// B5: the request refused because the pending queue is full must surface an
    /// ERROR, not a successful empty response.
    ///
    /// `DelegateResponse(Vec::new())` is exactly what a delegate that ran and
    /// said nothing returns, so answering a refusal with it reports success for
    /// work `process()` never performed — the repo's "a refusal that is not
    /// counted renders as a clean zero" pattern. Dropping the responder makes
    /// the client's channel close, which surfaces as an error.
    ///
    /// FALSIFY by restoring `send_delegate_response(.., Vec::new())` in
    /// `dispatch_delegate_request`'s `Rejected` arm: the ninth request then
    /// returns Ok and this fails.
    #[tokio::test]
    async fn a_request_refused_behind_a_full_pending_queue_errors_not_succeeds() {
        let _guard = TEST_GUARD.lock().await;
        let (mut handler, send) = build_handler(vec![]).await;
        let script = handler.runtime_mut().delegate_script.clone();
        script.lock().unwrap().push_back(prompt_outbound().into());

        let send = Arc::new(send);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            GatedPrompter { gate: gate.clone() },
        ));

        let key_d = test_delegate_key();

        // Park it.
        let send_0 = send.clone();
        let k0 = key_d.clone();
        let parked: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(async move { send_0.send_to_handler(delegate_event(&k0)).await });
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Fill the pending queue to exactly its cap.
        let mut queued = Vec::new();
        for _ in 0..delegate_park::MAX_PENDING_PER_DELEGATE {
            let s = send.clone();
            let k = key_d.clone();
            queued.push(GlobalExecutor::spawn(async move {
                s.send_to_handler(delegate_event(&k)).await
            }));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // One more than the cap: must be REFUSED with an error.
        let over = tokio::time::timeout(
            Duration::from_secs(2),
            send.send_to_handler(delegate_event(&key_d)),
        )
        .await
        .expect("a refused request must be answered promptly, not left hanging");
        assert!(
            over.is_err(),
            "over-cap request must surface an error, not a successful empty \
             DelegateResponse that is indistinguishable from a delegate that \
             ran and said nothing (#5544 B5); got {over:?}"
        );

        gate.add_permits(1);
        let _ = tokio::time::timeout(Duration::from_secs(5), parked).await;
        handle.abort();
    }

    /// B6: the park backstop must be able to fire on an otherwise idle node.
    ///
    /// The sweep runs at the top of a loop iteration, and iterations only happen
    /// when something wakes the `select!`. Without a timer arm the backstop
    /// fires whenever unrelated traffic next arrives — on a quiet node, which is
    /// the normal state for a background peer and exactly the condition under
    /// which a prompt goes unanswered, effectively never.
    ///
    /// LIMITATION, stated rather than papered over: this asserts the DEADLINE
    /// computation and that the loop consults it, not an end-to-end sweep on an
    /// idle node. A decisive end-to-end test is not reachable today, because
    /// `PARK_WORK_BUDGET` (75s) is deliberately below `PARK_TTL` (90s) so the
    /// `ParkGuard` always resumes the park first — which is what makes the TTL a
    /// backstop rather than a timeout. Reaching the sweep would need a park with
    /// no live guard, which no production path can currently produce. The value
    /// of the fix is that IF that ever becomes reachable, the backstop works.
    #[test]
    fn park_sweep_deadline_is_armed_and_the_loop_waits_on_it() {
        // The loop must consult the deadline, not sweep only on other traffic.
        let src = include_str!("contract.rs");
        let body = src
            .split("pub(crate) async fn contract_handling")
            .nth(1)
            .expect("contract_handling must exist");
        assert!(
            body.contains("park_ctx.next_sweep_deadline()"),
            "contract_handling must compute the park sweep deadline"
        );
        assert!(
            body.contains("tokio::time::sleep_until(deadline)"),
            "the idle select! must WAIT on the park sweep deadline, or the \
             backstop cannot fire without unrelated traffic (#5544 B6)"
        );
    }

    /// B1: a contract notification arriving for a delegate that is currently
    /// parked must be QUEUED, not run.
    ///
    /// The notification path is a third door into a delegate, alongside the
    /// client request and the inter-delegate hop, and it is driven by a
    /// contract changing rather than by anything the delegate did. Running it
    /// mid-park re-enters `process()` and clobbers the parked continuation's
    /// context entry — the corruption the exclusion exists to prevent, reached
    /// through a door the exclusion originally did not guard.
    ///
    /// NOTE: this is about INTERLEAVING, not simultaneity. The notification
    /// path also runs on the serial loop, so the global
    /// one-`process()`-at-a-time property is untouched and does not help. A
    /// serially executed notification landing inside the park window corrupts
    /// just as thoroughly as a concurrent one would.
    ///
    /// FALSIFY by deleting the `is_parked` gate in `handle_delegate_notification`:
    /// `delegate_calls` then records the notification's run.
    #[tokio::test]
    async fn a_notification_for_a_parked_delegate_is_queued_not_run() {
        let _guard = TEST_GUARD.lock().await;
        let (mut handler, _send) = build_handler(vec![]).await;
        let calls = handler.runtime_mut().delegate_calls.clone();
        let script = handler.runtime_mut().delegate_script.clone();
        let contexts = handler.runtime_mut().delegate_contexts.clone();
        // Present so that, if the gate is missing, the delegate really does run
        // rather than erroring out for an unrelated reason — and writes a
        // DIFFERENT context, which is what makes the corruption visible.
        script.lock().unwrap().push_back(ScriptedRun {
            outbound: Vec::new(),
            writes_context: Some(b"clobbered-by-notification".to_vec()),
        });

        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut park = delegate_park::DelegateParkCtx::new(tx);
        let key = test_delegate_key();
        // What the parked round-trip left behind, as its pre-park `ctx.write()`
        // would have.
        contexts
            .lock()
            .unwrap()
            .insert(key.clone(), b"parked-continuation".to_vec());
        assert!(
            matches!(
                park.park(key.clone(), parked_continuation()),
                delegate_park::ParkAdmission::Admitted
            ),
            "the delegate must start out parked"
        );

        handle_delegate_notification(
            &mut handler,
            executor::DelegateNotification {
                delegate_key: key.clone(),
                contract_id: ContractInstanceId::new([9u8; 32]),
                new_state: Arc::new(WrappedState::new(b"changed".to_vec())),
            },
            &std::sync::Arc::new(crate::contract::user_input::AutoApprovePrompter),
            Some(&mut park),
        )
        .await;

        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "a notification must NOT re-enter a parked delegate (#5544 B1)"
        );
        // The assertion that shows the CONSEQUENCE, not just the symptom
        // (#5544 S7): the parked continuation's context must be untouched. An
        // extra `process()` is only a problem because it clobbers this.
        assert_eq!(
            contexts.lock().unwrap().get(&key).map(Vec::as_slice),
            Some(b"parked-continuation".as_slice()),
            "the parked continuation's context must be intact; a notification \
             that ran would have overwritten it and the delegate would resume \
             on someone else's bytes (#5544 B1)"
        );
        let (_continuation, pending) = park.take(&key).expect("still parked");
        assert_eq!(
            pending.len(),
            1,
            "the notification must be queued, not lost"
        );
        assert!(
            matches!(pending[0], delegate_park::PendingRun::Notification { .. }),
            "queued as a notification so it resumes through the app-routing path"
        );
    }

    /// B2 via the context model: an inter-delegate message must not re-enter a
    /// parked TARGET delegate.
    ///
    /// Delegate A is driven by a client and emits `SendDelegateMessage` at B.
    /// B is parked on a prompt. Delivering the hop would run B's `process()`
    /// mid-round-trip and overwrite the context its parked continuation
    /// depends on.
    ///
    /// This is the test the mock could not express before S7. Modelling call
    /// counts alone showed only that an extra invocation happened; modelling
    /// the context store shows that the invocation CORRUPTED something, which
    /// is the reason the extra invocation matters.
    ///
    /// FALSIFY by deleting the `is_parked(&target_key)` gate on the hop: B's
    /// context becomes `clobbered-by-hop`.
    #[tokio::test]
    async fn an_inter_delegate_message_does_not_re_enter_a_parked_target() {
        let _guard = TEST_GUARD.lock().await;
        let (mut handler, send) = build_handler(vec![]).await;
        let script = handler.runtime_mut().delegate_script.clone();
        let contexts = handler.runtime_mut().delegate_contexts.clone();

        let key_b = test_delegate_key();
        let key_a = DelegateKey::new([8u8; 32], freenet_stdlib::prelude::CodeHash::new([8u8; 32]));

        // Run 1 is B's: it parks on a prompt, having written its continuation.
        script.lock().unwrap().push_back(ScriptedRun {
            outbound: prompt_outbound(),
            writes_context: Some(b"b-parked-continuation".to_vec()),
        });
        // Run 2 is A's: it fires a message at B.
        script.lock().unwrap().push_back(ScriptedRun {
            outbound: vec![OutboundDelegateMsg::SendDelegateMessage(
                freenet_stdlib::prelude::DelegateMessage::new(
                    key_b.clone(),
                    key_a.clone(),
                    b"ping".to_vec(),
                ),
            )],
            writes_context: None,
        });
        // Run 3 would be B's, if the hop were wrongly delivered.
        script.lock().unwrap().push_back(ScriptedRun {
            outbound: Vec::new(),
            writes_context: Some(b"clobbered-by-hop".to_vec()),
        });

        let send = Arc::new(send);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            GatedPrompter { gate: gate.clone() },
        ));

        // Park B.
        let send_b = send.clone();
        let kb = key_b.clone();
        let parked: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(async move { send_b.send_to_handler(delegate_event(&kb)).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            contexts.lock().unwrap().get(&key_b).map(Vec::as_slice),
            Some(b"b-parked-continuation".as_slice()),
            "B must have parked with its continuation written"
        );

        // Now drive A, which messages the parked B.
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            send.send_to_handler(delegate_event(&key_a)),
        )
        .await
        .expect("A's own request must complete; only the hop to B is dropped");

        assert_eq!(
            contexts.lock().unwrap().get(&key_b).map(Vec::as_slice),
            Some(b"b-parked-continuation".as_slice()),
            "an inter-delegate message must NOT re-enter a parked target; B's \
             parked continuation was overwritten (#5544 B2)"
        );

        gate.add_permits(1);
        let _ = tokio::time::timeout(Duration::from_secs(5), parked).await;
        handle.abort();
    }

    /// A continuation standing in for a delegate parked on a prompt.
    fn parked_continuation() -> delegate_park::Continuation {
        delegate_park::Continuation {
            params: Parameters::from(Vec::new()),
            origin_contract: None,
            connection_scope: crate::client_events::ConnectionScope::Local,
            user_context: None,
            inter_delegate: InterDelegateDispatch::Allowed,
            accumulated: Vec::new(),
            inbound_so_far: Vec::new(),
            responder: None,
            delivery: delegate_park::Delivery::Client,
        }
    }

    /// The second half of #5544: a delegate PUT whose contract asks for a
    /// related contract this node lacks used to await a network GET INLINE, up
    /// to RELATED_FETCH_TIMEOUT (10s), on the same serial loop. Smaller than
    /// the prompt stall but reachable with no user involvement at all.
    ///
    /// Same falsification shape as the prompt test: with the deferral removed
    /// the local GET below times out.
    #[tokio::test]
    async fn deferred_delegate_upsert_does_not_block_the_loop() {
        let _guard = TEST_GUARD.lock().await;

        // Contract A requests related contract B, which is never local.
        let contract_a = make_contract(b"park_put_a");
        let key_a = contract_a.key();
        let id_b = *make_contract(b"park_put_b_missing").key().id();

        let (mut handler, send) = build_handler(vec![(
            *key_a.id(),
            ValidateOverride::RequestRelated(vec![id_b]),
        )])
        .await;

        let script = handler.runtime_mut().delegate_script.clone();
        script.lock().unwrap().push_back(
            vec![OutboundDelegateMsg::PutContractRequest(
                freenet_stdlib::prelude::PutContractRequest {
                    contract: contract_a.clone(),
                    state: WrappedState::new(b"a_state".to_vec()),
                    related_contracts: RelatedContracts::default(),
                    context: DelegateContext::default(),
                    processed: false,
                },
            )]
            .into(),
        );

        // B's fetch hangs until released, standing in for a slow network GET.
        // The counter records that the OFF-LOOP path was the one taken.
        let gate = Arc::new(tokio::sync::Notify::new());
        let gate_for_stub = gate.clone();
        let off_loop_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_stub = off_loop_calls.clone();
        let _override =
            OverrideGuard::install(Arc::new(move |missing: Vec<ContractInstanceId>| {
                let gate = gate_for_stub.clone();
                calls_for_stub.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async move {
                    gate.notified().await;
                    Err(ExecutorError::missing_related(missing[0]))
                })
            }));

        // A locally stored contract whose GET needs no network at all.
        let contract_c = make_contract(b"park_put_c_cached");
        let key_c = contract_c.key();

        let send = Arc::new(send);
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            crate::contract::user_input::AutoApprovePrompter,
        ));

        let put_c = put_local(
            send.as_ref(),
            contract_c,
            WrappedState::new(b"c_state".to_vec()),
        )
        .await;
        assert!(
            matches!(
                put_c,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(_),
                    ..
                }
            ),
            "seed PUT for C must succeed, got {put_c}"
        );

        let send_d = send.clone();
        let key_d = test_delegate_key();
        let delegate_task: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(
                async move { send_d.send_to_handler(delegate_event(&key_d)).await },
            );

        tokio::time::sleep(Duration::from_millis(150)).await;

        // The two DECISIVE assertions. Remove the deferral (drop the
        // `DeferRelated if parking.is_some()` arm) and both fail: the upsert
        // then fetches inline and the delegate answers straight away.
        //
        // Note for reviewers on why these carry the weight rather than the
        // GET-promptness check below: this harness has no op_manager, so the
        // INLINE related fetch fails immediately instead of hanging. The GET
        // would therefore look prompt even with the deferral removed, which
        // makes it corroborating rather than decisive HERE. (In the prompt
        // test it is decisive — `prompter.prompt()` really does hang inline.)
        assert_eq!(
            off_loop_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the related fetch must have been routed OFF the loop, not awaited \
             inline in the upsert (#5544)"
        );
        assert!(
            !delegate_task.is_finished(),
            "the delegate must still be PARKED while its related fetch is in \
             flight; finishing already means the fetch was awaited inline"
        );

        let get_c = tokio::time::timeout(
            Duration::from_secs(2),
            send.send_to_handler(ContractHandlerEvent::GetQuery {
                instance_id: *key_c.id(),
                return_contract_code: false,
            }),
        )
        .await
        .expect(
            "a local-store GET must NOT block behind a delegate PUT whose \
             related-contract fetch is still in flight (#5544)",
        )
        .expect("GET for C must respond");
        match get_c {
            ContractHandlerEvent::GetResponse { response, .. } => {
                let store = response.expect("GET for C must succeed");
                assert_eq!(
                    store.state.expect("C has state").as_ref(),
                    b"c_state",
                    "GET must return C's stored state"
                );
            }
            other => panic!("expected GetResponse for C, got {other}"),
        }

        // Release the fetch: B is reported missing, so A's upsert fails and the
        // delegate is told so — the point is that it terminates and answers.
        gate.notify_one();
        let resp = tokio::time::timeout(Duration::from_secs(5), delegate_task)
            .await
            .expect("the parked delegate must resolve once its fetch completes")
            .expect("delegate task join")
            .expect("delegate request must be answered");
        assert!(
            matches!(resp, ContractHandlerEvent::DelegateResponse(_)),
            "a delegate parked on a related fetch must still answer its client, got {resp}"
        );

        handle.abort();
    }

    /// THE EXCLUSION TEST. Pins the invariant that makes parking sound.
    ///
    /// `DelegateContextCache` is keyed by `DelegateKey` and last-write-wins, so
    /// it is only correct while at most one `process()` per delegate is in
    /// flight. Before #5544 the serial loop supplied that for free; parking
    /// removes it, so `DelegateParkCtx` has to supply it instead. If it did
    /// not, a second request would run `process()` and overwrite the parked
    /// continuation, and the delegate would resume reading someone else's
    /// bytes — silent state corruption, not a crash, which is why it needs a
    /// test rather than trust.
    ///
    /// NOTE FOR REVIEWERS: this test CANNOT fail on pre-#5544 `main`, because
    /// there the blocked loop prevents the interleave by construction. It is a
    /// pin against *this* change regressing: delete the `is_parked` check in
    /// `dispatch_delegate_request` and it fails, because the second request
    /// reaches the executor while the first is still parked. That is the
    /// falsification to run if you want to confirm it is not vacuous.
    #[tokio::test]
    async fn a_parked_delegate_is_not_re_entered_until_it_resumes() {
        let _guard = TEST_GUARD.lock().await;
        let (mut handler, send) = build_handler(vec![]).await;
        let script = handler.runtime_mut().delegate_script.clone();
        let calls = handler.runtime_mut().delegate_calls.clone();
        // Only the FIRST invocation prompts; later ones return nothing, so the
        // round-trip terminates instead of parking forever.
        script.lock().unwrap().push_back(prompt_outbound().into());

        let send = Arc::new(send);
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let handle = GlobalExecutor::spawn(contract_handling(
            handler,
            GatedPrompter { gate: gate.clone() },
        ));

        let key_d = test_delegate_key();

        // Request 1 parks the delegate.
        let send_1 = send.clone();
        let k1 = key_d.clone();
        let first: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(async move { send_1.send_to_handler(delegate_event(&k1)).await });
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "the first request must have entered the delegate exactly once"
        );

        // Request 2 for the SAME delegate arrives while it is parked.
        let send_2 = send.clone();
        let k2 = key_d.clone();
        let second: tokio::task::JoinHandle<Result<ContractHandlerEvent, ContractError>> =
            GlobalExecutor::spawn(async move { send_2.send_to_handler(delegate_event(&k2)).await });

        // Give the loop ample opportunity to (wrongly) run it.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "a parked delegate must NOT be re-entered: the queued request would \
             overwrite the parked continuation's context (#5544)"
        );

        // Release the prompt: the parked run resumes, then the queued request
        // is drained — both reach the delegate, in order.
        gate.add_permits(1);
        let r1 = tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("first delegate request must resolve")
            .expect("join")
            .expect("first must be answered");
        assert!(matches!(r1, ContractHandlerEvent::DelegateResponse(_)));
        let r2 = tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("the queued request must be drained once the park ends")
            .expect("join")
            .expect("second must be answered");
        assert!(
            matches!(r2, ContractHandlerEvent::DelegateResponse(_)),
            "a request queued behind a park must still be answered, never dropped"
        );
        assert!(
            calls.lock().unwrap().len() >= 3,
            "expected the resume plus the drained request to enter the delegate, \
             got {} entries",
            calls.lock().unwrap().len()
        );

        handle.abort();
    }
}
