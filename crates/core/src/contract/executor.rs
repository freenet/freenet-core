//! Executes WASM contract and delegate code within a sandboxed environment (`WasmRuntime`).
//! Communicates with the `ContractHandler`.
//! See `architecture.md`.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::future::Future;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use either::Either;
use freenet_stdlib::client_api::{
    ClientError as WsClientError, ClientRequest, ContractError as StdContractError,
    ContractRequest, ContractResponse, DelegateError as StdDelegateError, DelegateRequest,
    HostResponse::{self, DelegateResponse},
    RequestError,
};
use freenet_stdlib::prelude::*;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::storages::Storage;
use crate::config::Config;
use crate::node::OpManager;
use crate::operations::get::GetResult;
use crate::wasm_runtime::{
    ContractRuntimeInterface, ContractStore, DelegateRuntimeInterface, DelegateStore, Runtime,
    SecretsStore, StateStorage, StateStore, StateStoreError, UserSecretContext,
};
use crate::{
    client_events::{ClientId, HostResult},
    operations,
};

pub(super) mod init_tracker;
pub(super) mod mock_runtime;
pub(super) mod mock_wasm_runtime;
#[cfg(test)]
mod pool_tests;
pub(super) mod runtime;

/// Notification sent when a subscribed contract's state changes.
/// Delivered from `commit_state_update()` to the `contract_handling()` loop.
/// Uses `Arc<WrappedState>` so multiple subscribers share one allocation.
pub(crate) struct DelegateNotification {
    pub delegate_key: DelegateKey,
    pub contract_id: ContractInstanceId,
    pub new_state: Arc<WrappedState>,
}

/// Buffer size for the delegate notification channel. Notifications that exceed
/// this limit are dropped with a warning — the delegate will see the next state
/// change instead. This prevents unbounded memory growth under load.
pub(crate) const DELEGATE_NOTIFICATION_CHANNEL_SIZE: usize = 1000;

/// Maximum number of subscriber clients per contract.
/// Prevents unbounded WASM amplification and memory growth from notification fan-out.
pub(crate) const MAX_SUBSCRIBERS_PER_CONTRACT: usize = 256;

/// Maximum total subscriptions a single client may hold across all contracts.
/// Prevents a single client from spreading thin across many contracts to exhaust resources.
pub(crate) const MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 50;

/// Buffer size for per-subscriber notification channels.
/// When full, notifications are dropped (lossy) rather than blocking the executor.
pub(crate) const SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE: usize = 64;

/// Maximum WASM `get_state_delta()` calls per notification fan-out.
/// Beyond this limit, remaining subscribers receive full state instead of a computed delta.
pub(crate) const MAX_DELTA_COMPUTATIONS_PER_FANOUT: usize = 32;

/// Subscriber count above which a warning is logged during notification fan-out.
/// This is below `MAX_SUBSCRIBERS_PER_CONTRACT` to provide early visibility into
/// contracts with high fan-out before they hit the hard cap.
pub(crate) const FANOUT_WARNING_THRESHOLD: usize = 50;

/// Maximum delegate creation chain depth (A creates B creates C...).
/// Prevents recursive fork-bomb attacks via delegate spawning.
pub(crate) const MAX_DELEGATE_CREATION_DEPTH: u32 = 4;

/// Maximum delegates a single delegate can create within one process() call.
pub(crate) const MAX_DELEGATE_CREATIONS_PER_CALL: u32 = 8;

/// Maximum total delegates that can be created via the create_delegate host function
/// across the lifetime of this node. Prevents unbounded memory growth in the
/// delegate store and secret store. Enforced via `CREATED_DELEGATES_COUNT` atomic counter.
pub(crate) const MAX_CREATED_DELEGATES_PER_NODE: usize = 1024;

pub(crate) type DelegateNotificationSender = mpsc::Sender<DelegateNotification>;
pub(crate) type DelegateNotificationReceiver = mpsc::Receiver<DelegateNotification>;

pub(crate) use init_tracker::{
    ContractInitTracker, InitCheckResult, MAX_CONCURRENT_INITIALIZATIONS,
    MAX_QUEUED_OPS_PER_CONTRACT, SLOW_INIT_THRESHOLD, STALE_INIT_THRESHOLD, now_nanos,
};
pub(crate) use runtime::RuntimePool;

/// Typed marker for queue-full errors so callers can downcast and
/// distinguish transient per-contract queue saturation from real
/// executor failures (OOG, traps, missing parameters, storage errors).
///
/// Constructed by `send_queue_full_response`; recognized by
/// `ExecutorError::is_contract_queue_full` (see that predicate for the
/// platform-resilience invariant it enforces). Issue #4251.
#[derive(Debug, Clone, Copy)]
pub struct ContractQueueFull;

impl std::fmt::Display for ContractQueueFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("contract queue full, try again later")
    }
}

impl std::error::Error for ContractQueueFull {}

/// Typed marker carried by an [`ExecutorError`] when an upsert was invoked
/// in *deferrable* mode (see [`ContractExecutor::upsert_contract_state_deferrable`])
/// and discovered it needs to fetch related contracts from the network to
/// finish validation/merge.
///
/// Instead of awaiting that network GET inline — which would pin the serial
/// `contract_handling` loop for up to `RELATED_FETCH_TIMEOUT`, blocking every
/// queued event behind it (including local-store-hit GETs) — the executor
/// aborts the upsert cleanly (running the same init-tracker/contract-store
/// rollback the error paths use) and surfaces the missing related ids here.
/// The caller off-loads the GET to a background task and re-runs the upsert
/// once the states arrive. See issue #4391.
#[derive(Debug, Clone)]
pub struct DeferRelatedFetch {
    /// Related contract instance ids that are not held locally and must be
    /// fetched from the network before the upsert can complete.
    pub missing: Vec<ContractInstanceId>,
}

impl std::fmt::Display for DeferRelatedFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "upsert needs {} related contract(s) fetched from the network",
            self.missing.len()
        )
    }
}

impl std::error::Error for DeferRelatedFetch {}

#[derive(Debug)]
pub struct ExecutorError {
    inner: Either<Box<RequestError>, anyhow::Error>,
    fatal: bool,
}

enum InnerOpError {
    Upsert(ContractKey),
    Delegate(DelegateKey),
}

impl std::error::Error for ExecutorError {}

impl ExecutorError {
    pub fn other(error: impl Into<anyhow::Error>) -> Self {
        Self {
            inner: Either::Right(error.into()),
            fatal: false,
        }
    }

    /// Call this when an unreachable path is reached but need to avoid panics.
    fn internal_error() -> Self {
        Self {
            inner: Either::Right(anyhow::anyhow!("internal error")),
            fatal: false,
        }
    }

    fn request(error: impl Into<RequestError>) -> Self {
        Self {
            inner: Either::Left(Box::new(error.into())),
            fatal: false,
        }
    }

    fn execution(
        outer_error: crate::wasm_runtime::ContractError,
        op: Option<InnerOpError>,
    ) -> Self {
        use crate::wasm_runtime::RuntimeInnerError;
        let error = outer_error.deref();

        if let RuntimeInnerError::ContractExecError(e) = error {
            if let Some(InnerOpError::Upsert(key)) = &op {
                return ExecutorError::request(StdContractError::update_exec_error(*key, e));
            }
        }

        if let RuntimeInnerError::DelegateNotFound(key) = error {
            return ExecutorError::request(StdDelegateError::Missing(key.clone()));
        }

        if let RuntimeInnerError::DelegateExecError(e) = error {
            return ExecutorError::request(StdDelegateError::ExecutionError(format!("{e}").into()));
        }

        if let (
            RuntimeInnerError::SecretStoreError(
                crate::wasm_runtime::SecretStoreError::MissingSecret(secret),
            ),
            Some(InnerOpError::Delegate(key)),
        ) = (error, &op)
        {
            return ExecutorError::request(StdDelegateError::MissingSecret {
                key: key.clone(),
                secret: secret.clone(),
            });
        }

        if let RuntimeInnerError::WasmError(e) = error {
            match op {
                Some(InnerOpError::Upsert(key)) => {
                    return ExecutorError::request(StdContractError::update_exec_error(key, e));
                }
                _ => return ExecutorError::other(anyhow::anyhow!("execution error: {e}")),
            }
        }

        ExecutorError::other(outer_error)
    }

    pub fn is_request(&self) -> bool {
        matches!(self.inner, Either::Left(_))
    }

    /// Returns true if this error indicates the contract's WASM merge function
    /// ran and rejected the update (e.g., stale version). This means the contract
    /// code IS present locally, so no auto-fetch is needed.
    ///
    /// This is BROADER than `is_invalid_update_rejection`: it ALSO returns true
    /// for runtime failures like out-of-gas, max-compute-time, traps, etc.,
    /// because those still mean the contract code is present : only the
    /// execution itself failed. Use this for auto-fetch decisions, NOT for log
    /// severity (a contract that runs out of gas is a real bug operators must
    /// see at ERROR level : see `is_invalid_update_rejection` for the
    /// log-severity discriminator).
    ///
    /// Only matches errors created via `StdContractError::update_exec_error()`
    /// (cause starts with "execution error:"), NOT other `Update` variants like
    /// "missing contract parameters" where auto-fetch IS appropriate.
    pub fn is_contract_exec_rejection(&self) -> bool {
        match &self.inner {
            Either::Left(req_err) => matches!(
                req_err.as_ref(),
                RequestError::ContractError(StdContractError::Update { cause, .. })
                    if cause.starts_with("execution error")
            ),
            Either::Right(_) => false,
        }
    }

    /// Narrow discriminator for the specific failure that the
    /// originator-side UPDATE auto-fetch heals: contract code/params
    /// are not present in the local `state_store`, so
    /// `update_contract` cannot run the merge.
    ///
    /// Distinct from `is_contract_exec_rejection` (which negates a
    /// broader set including other contract-side validation errors
    /// like `Deser`/`InvalidState`/`InvalidDelta`/`Other`/`DoublePut`/
    /// `InvalidArrayLength` and storage errors). Auto-fetching on
    /// those broader failures is wasted work — the contract IS
    /// present, the input is bad. Use this narrow predicate at
    /// originator UPDATE call sites so a malformed delta or a disk
    /// error never triggers auto-fetch storms.
    ///
    /// Discriminator: stdlib's `ContractError::Update` with a cause
    /// containing the literal "missing contract parameters" string
    /// (produced by `runtime.rs`'s `get_params` failure path,
    /// approx. lines 920–953). Any other `Update` variant cause
    /// returns false.
    pub fn is_missing_contract_parameters(&self) -> bool {
        match &self.inner {
            Either::Left(req_err) => matches!(
                req_err.as_ref(),
                RequestError::ContractError(StdContractError::Update { cause, .. })
                    if cause.contains("missing contract parameters")
            ),
            Either::Right(_) => false,
        }
    }

    /// Returns true ONLY when the contract WASM merge function ran to completion
    /// and returned a typed `InvalidUpdate` / `InvalidUpdateWithInfo` rejection
    /// (e.g., "New state version N must be higher than current version N"). This
    /// is the precise case that production gateways hit on every re-broadcast
    /// missed by the dedup cache (issue #3914) and the only case where ERROR-
    /// level logging is operationally noise.
    ///
    /// Excluded by design (these remain real failures and keep their ERROR/WARN
    /// log levels):
    /// - Out-of-gas / max-compute-time-exceeded
    /// - WASM traps (stack overflow, division by zero, etc.)
    /// - Compilation errors, instantiation errors, internal runtime errors
    /// - Other contract-side `ContractError` variants (`Deser`, `InvalidState`,
    ///   `InvalidDelta`, `Other`, `DoublePut`, `InvalidArrayLength`, etc.)
    ///
    /// Discriminator: stdlib's `ContractError::InvalidUpdate{,WithInfo}` Display
    /// impls produce strings beginning with "invalid contract update", which
    /// `update_exec_error` then prefixes with "execution error: ". Any other
    /// flavor of execution error has a different prefix and falls through.
    pub fn is_invalid_update_rejection(&self) -> bool {
        match &self.inner {
            Either::Left(req_err) => matches!(
                req_err.as_ref(),
                RequestError::ContractError(StdContractError::Update { cause, .. })
                    if cause.starts_with("execution error: invalid contract update")
            ),
            Either::Right(_) => false,
        }
    }

    /// Returns true if this error is the typed `ContractQueueFull` marker.
    ///
    /// Produced by:
    /// - `send_queue_full_response` (per-contract fair queue at capacity),
    /// - the `InitCheckResult::QueueFull` arm in `executor/runtime.rs` (per-contract
    ///   initialization queue at capacity).
    ///
    /// **Platform-resilience invariant**: queue-full is transient
    /// backpressure, not a contract-level fault, missing-contract condition,
    /// or WASM failure. Callers in paths that have amplification side effects
    /// (today: UPDATE relay's `try_auto_fetch_contract` and `ResyncRequest`)
    /// MUST gate those branches on this predicate so a saturated contract
    /// doesn't induce a network-wide storm. Paths without amplification
    /// (today: PUT, GET, SUBSCRIBE) only need to gate **ERROR-level logging**
    /// off this predicate, since on a hot contract the volume otherwise drowns
    /// real failures. See issue #4251.
    pub fn is_contract_queue_full(&self) -> bool {
        match &self.inner {
            Either::Left(_) => false,
            Either::Right(err) => err.downcast_ref::<ContractQueueFull>().is_some(),
        }
    }

    /// Construct a `MissingRelated` request error for `id`. Used by the
    /// off-loop related-fetch path (#4391) to surface a fetch failure to the
    /// client with the same error shape the inline path produces.
    pub(crate) fn missing_related(id: ContractInstanceId) -> Self {
        Self::request(StdContractError::MissingRelated { key: id })
    }

    /// Construct the typed [`DeferRelatedFetch`] signal. Only produced by the
    /// deferrable-upsert path when the missing related contracts must be
    /// fetched from the network. See `DeferRelatedFetch`.
    pub(crate) fn defer_related_fetch(missing: Vec<ContractInstanceId>) -> Self {
        Self {
            inner: Either::Right(anyhow::Error::new(DeferRelatedFetch { missing })),
            fatal: false,
        }
    }

    /// If this error is the typed [`DeferRelatedFetch`] signal, return the
    /// missing related contract ids; otherwise `None`. Consuming the error so
    /// the caller can re-run the upsert with the fetched states.
    pub(crate) fn into_defer_related_fetch(self) -> Result<Vec<ContractInstanceId>, Self> {
        match self.inner {
            Either::Right(err) => match err.downcast::<DeferRelatedFetch>() {
                Ok(defer) => Ok(defer.missing),
                Err(err) => Err(Self {
                    inner: Either::Right(err),
                    fatal: self.fatal,
                }),
            },
            inner @ Either::Left(_) => Err(Self {
                inner,
                fatal: self.fatal,
            }),
        }
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal
    }

    /// Returns true if the error is due to a missing delegate (not found in store).
    /// This is expected during legacy migration probes and should be logged at
    /// warn level rather than error.
    pub fn is_missing_delegate(&self) -> bool {
        matches!(
            &self.inner,
            Either::Left(err) if matches!(
                err.as_ref(),
                RequestError::DelegateError(StdDelegateError::Missing(_))
            )
        )
    }

    pub fn unwrap_request(self) -> RequestError {
        match self.inner {
            Either::Left(err) => *err,
            Either::Right(_) => unreachable!("called unwrap_request on a non-request error"),
        }
    }
}

impl From<RequestError> for ExecutorError {
    fn from(value: RequestError) -> Self {
        Self {
            inner: Either::Left(Box::new(value)),
            fatal: false,
        }
    }
}

impl Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Either::Left(l) => write!(f, "{}", &**l),
            Either::Right(r) => write!(f, "{}", &**r),
        }
    }
}

impl From<Box<RequestError>> for ExecutorError {
    fn from(value: Box<RequestError>) -> Self {
        Self {
            inner: Either::Left(value),
            fatal: false,
        }
    }
}

type Response = Result<HostResponse, ExecutorError>;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationMode {
    /// Run the node in local-only mode. Useful for development purposes.
    Local,
    /// Standard operation mode.
    Network,
}

impl Display for OperationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationMode::Local => write!(f, "local"),
            OperationMode::Network => write!(f, "network"),
        }
    }
}

// Executor auto-subscribe calls `subscribe::run_executor_subscribe`
// directly; UPDATEs flow through `start_client_update`.

/// Empty stream used to fill the executor-transaction slot in
/// `priority_select::PrioritySelectStream`. Never yields.
pub(crate) struct ExecutorTransactionStream;

impl futures::Stream for ExecutorTransactionStream {
    type Item = crate::message::Transaction;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}

#[derive(Debug)]
pub(crate) enum UpsertResult {
    /// The incoming state was identical to the current state (same hash).
    NoChange,
    /// The incoming state won the CRDT merge and is now stored.
    Updated(WrappedState),
    /// The current state won the CRDT merge - incoming was rejected.
    /// Contains the winning current state which should be propagated.
    CurrentWon(WrappedState),
}

/// Outcome of a *deferrable* upsert (see
/// [`ContractExecutor::upsert_contract_state_deferrable`]).
///
/// A normal upsert either completes (`Completed`) or, when it needs related
/// contracts that aren't held locally, signals that the network fetch should
/// be off-loaded from the serial event loop (`DeferRelated`) instead of being
/// awaited inline. See issue #4391 for why the inline wait is harmful.
#[derive(Debug)]
pub(crate) enum UpsertOutcome {
    /// The upsert ran to completion (all related contracts were resolvable
    /// locally, or none were needed). Carries the same result a plain
    /// `upsert_contract_state` would return.
    Completed(UpsertResult),
    /// The upsert needs these related contracts fetched from the network
    /// before it can finish. No state was committed and any in-progress
    /// initialization was rolled back; the caller must fetch the listed
    /// contracts off-loop and re-run the upsert with them supplied.
    DeferRelated(Vec<ContractInstanceId>),
}

pub(crate) trait ContractExecutor: Send + 'static {
    /// Look up the full ContractKey from a ContractInstanceId.
    /// Returns None if the contract is not known to this node.
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey>;

    fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> impl Future<
        Output = Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError>,
    > + Send;

    /// Upsert contract state.
    ///
    /// # Arguments
    /// * `key` - The contract key
    /// * `update` - Either a full state or a delta to apply
    /// * `related_contracts` - Related contracts needed for validation
    /// * `code` - Optional contract code (for PUT operations)
    fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> impl Future<Output = Result<UpsertResult, ExecutorError>> + Send;

    /// Like [`upsert_contract_state`](Self::upsert_contract_state), but when the
    /// upsert needs related contracts that are not held locally, it does NOT
    /// await the network GET inline. Instead it rolls back any partial work and
    /// returns [`UpsertOutcome::DeferRelated`] with the missing ids so the
    /// caller can off-load the fetch from the serial event loop and re-run the
    /// upsert once the states are available.
    ///
    /// The default implementation never defers — it simply forwards to
    /// `upsert_contract_state` and wraps the result in
    /// [`UpsertOutcome::Completed`]. Only executors that perform network
    /// related-contract fetches (the production `RuntimePool` / `Executor<Runtime>`
    /// and the `MockWasmRuntime` test executor) override it to defer. Local-only
    /// and mock executors keep their existing inline behavior. See issue #4391.
    fn upsert_contract_state_deferrable(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> impl Future<Output = Result<UpsertOutcome, ExecutorError>> + Send {
        async move {
            self.upsert_contract_state(key, update, related_contracts, code)
                .await
                .map(UpsertOutcome::Completed)
        }
    }

    fn register_contract_notifier(
        &mut self,
        key: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::Sender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>>;

    /// Execute a delegate request.
    ///
    /// `origin_contract` carries the WebApp attestation when a contract-backed
    /// web app dispatches a request to a delegate.
    ///
    /// `caller_delegate` carries the runtime-attested identity of a calling
    /// delegate when one delegate sends a message to another via
    /// [`OutboundDelegateMsg::SendDelegateMessage`]. When `Some`, it takes
    /// precedence over `origin_contract` (and over inherited origins) for
    /// the receiver's `MessageOrigin`. At most one of these two arguments is
    /// expected to be `Some` at a given call site, and only `caller_delegate`
    /// is used for inter-delegate dispatch (issue #3860).
    ///
    /// `user_context` carries the per-connection per-user secret namespace
    /// (hosted mode, P2 of #4381). It is derived ONCE at the WS connection
    /// boundary from the connection's user token and is `None` outside hosted
    /// mode or when no token was presented. When `Some`, the delegate's secret
    /// host functions operate on that user's namespace; when `None` they use
    /// the single-user [`crate::wasm_runtime::SecretScope::Local`] path,
    /// byte-for-byte today's behavior. Crucially this is a SEPARATE channel
    /// from `origin_contract`/`caller_delegate` and the request body: nothing
    /// the delegate or client can put in a message can set or forge it. The
    /// inter-delegate dispatch path passes `None` (a delegate-to-delegate hop
    /// does not inherit the originating connection's user namespace).
    fn execute_delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
        user_context: Option<&UserSecretContext>,
    ) -> impl Future<Output = Response> + Send;

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo>;

    /// Remove all subscriptions for a disconnected client.
    ///
    /// Default implementation is a no-op (for mock executors that don't track subscriptions).
    fn remove_client(&self, _client_id: ClientId) {}

    /// Reclaim a contract's on-disk storage (persisted state + parameters and
    /// the WASM code blob) after the contract was evicted from the hosting
    /// cache. Best-effort and idempotent: a double eviction is a no-op.
    ///
    /// `expected_generation` is the state-write generation captured
    /// atomically with the eviction decision. Implementations that wire
    /// through to a real `Ring`/`HostingManager` re-read the current
    /// generation and skip reclamation if it has advanced (closing the
    /// EvictContract re-host race). Implementations without a `Ring` may
    /// ignore the argument.
    ///
    /// Default implementation is a no-op (for mock executors that keep state
    /// in memory and have no on-disk storage to reclaim).
    fn remove_contract(
        &mut self,
        _key: &ContractKey,
        _expected_generation: u64,
    ) -> impl Future<Output = Result<(), ExecutorError>> + Send {
        async { Ok(()) }
    }

    /// Record that an `EvictContract` event was dropped before it could
    /// complete (queue-full rejection in `contract_handling`), so the
    /// periodic sweep can retry it via `reclaim_evicted_contract`.
    ///
    /// Default implementation is a no-op (for mock executors with no
    /// `Ring` to record into). The real implementation on `RuntimePool`
    /// forwards to `op_manager.ring.pending_reclamation_add`.
    fn track_pending_reclamation(&self, _key: ContractKey, _expected_generation: u64) {}

    /// Compute the state summary for a contract using the contract's summarize_state method.
    fn summarize_contract_state(
        &mut self,
        key: ContractKey,
    ) -> impl Future<Output = Result<StateSummary<'static>, ExecutorError>> + Send;

    /// Compute a state delta for a contract given a peer's state summary.
    ///
    /// Uses the contract's get_state_delta method to compute the minimal changes
    /// needed for a peer at `their_summary` to reach our current state.
    fn get_contract_state_delta(
        &mut self,
        key: ContractKey,
        their_summary: StateSummary<'static>,
    ) -> impl Future<Output = Result<StateDelta<'static>, ExecutorError>> + Send;

    /// Take the delegate notification receiver, if available.
    ///
    /// Returns `Some(rx)` for pool-based executors that support delegate subscription
    /// notifications. Returns `None` for mock/test executors.
    /// This can only be called once — subsequent calls return `None`.
    fn take_delegate_notification_rx(&mut self) -> Option<DelegateNotificationReceiver> {
        None
    }

    /// Clone the executor's [`OpManager`] handle, if it has one.
    ///
    /// The `contract_handling` loop uses this to drive an off-loop related-
    /// contract GET (via `start_sub_op_get`) when a PUT/UPDATE deferred its
    /// network fetch (#4391): the loop cannot move the `&mut` executor into a
    /// background task, but it can clone this `Arc<OpManager>` into one.
    /// Returns `None` for local-only / mock executors with no network.
    fn op_manager_handle(&self) -> Option<Arc<OpManager>> {
        None
    }
}

/// Tracks contracts that have undergone corrupted-state recovery.
///
/// When a contract's stored state is corrupted (e.g., WASM can't deserialize it),
/// the executor replaces it with a valid incoming state. This guard prevents infinite
/// recovery loops: if the replacement state also causes failures, the contract is
/// considered broken and no further recovery is attempted.
///
/// Entries are removed on subsequent successful updates, allowing future recovery
/// if corruption happens again later.
pub(crate) type CorruptedStateRecoveryGuard = Arc<std::sync::Mutex<HashSet<ContractKey>>>;

// Type alias for shared notification storage (used by RuntimePool).
// Uses DashMap for fine-grained per-key locking instead of a global RwLock.
type SharedNotifications =
    Arc<dashmap::DashMap<ContractInstanceId, Vec<(ClientId, mpsc::Sender<HostResult>)>>>;

// Type alias for shared subscriber summaries (used by RuntimePool).
type SharedSummaries =
    Arc<dashmap::DashMap<ContractInstanceId, HashMap<ClientId, Option<StateSummary<'static>>>>>;

// Per-client subscription counts for O(1) limit enforcement (used by RuntimePool).
type SharedClientCounts = Arc<dashmap::DashMap<ClientId, usize>>;

/// Consumers of the executor are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
///
/// The type parameters are:
/// - `R`: The runtime type (default: `Runtime` for production, `MockRuntime` for testing)
/// - `S`: The state storage type (default: `Storage` for disk-based, can use `MockStateStorage` for in-memory)
pub struct Executor<R = Runtime, S: StateStorage = Storage> {
    mode: OperationMode,
    runtime: R,
    pub state_store: StateStore<S>,
    /// Notification channels for any clients subscribed to updates for a given contract.
    /// Used when executor is standalone (not in a pool).
    update_notifications: HashMap<ContractInstanceId, Vec<(ClientId, mpsc::Sender<HostResult>)>>,
    /// Per-client subscription counts for O(1) limit enforcement (standalone executor).
    client_subscription_counts: HashMap<ClientId, usize>,
    /// Summaries of the state of all clients subscribed to a given contract.
    /// Used when executor is standalone (not in a pool).
    subscriber_summaries:
        HashMap<ContractInstanceId, HashMap<ClientId, Option<StateSummary<'static>>>>,
    /// Origin contract instances for a given delegate.
    delegate_origin_ids: HashMap<DelegateKey, Vec<ContractInstanceId>>,
    /// Tracks contracts that are being initialized and operations queued for them
    init_tracker: ContractInitTracker,

    /// Reference to the operation manager for initiating operations.
    op_manager: Option<Arc<OpManager>>,

    /// Shared notification storage at pool level (when running in a pool).
    /// When present, this is used instead of per-executor update_notifications
    /// to ensure subscriptions registered while an executor is checked out are
    /// still notified when that executor processes updates.
    shared_notifications: Option<SharedNotifications>,
    /// Shared subscriber summaries at pool level (when running in a pool).
    shared_summaries: Option<SharedSummaries>,
    /// Per-client subscription counts at pool level for O(1) limit enforcement.
    shared_client_counts: Option<SharedClientCounts>,
    /// Shared guard for corrupted-state recovery, preventing infinite recovery loops.
    /// See [`CorruptedStateRecoveryGuard`] for details.
    pub(crate) recovery_guard: CorruptedStateRecoveryGuard,

    /// Cache of contract summaries keyed by ContractKey, storing (state_hash, summary).
    /// Avoids redundant WASM instantiations during the 5-minute interest heartbeat
    /// which calls summarize_state() for every matching contract.
    /// LRU-bounded to prevent unbounded growth on gateways that see many contracts over time.
    summary_cache: LruCache<ContractKey, (u64, StateSummary<'static>)>,

    /// Cache of delta results keyed by (ContractKey, state_hash, their_summary_hash).
    /// Avoids redundant WASM instantiations for get_state_delta() calls.
    delta_cache: LruCache<(ContractKey, u64, u64), StateDelta<'static>>,

    /// Channel to send delegate notifications when subscribed contracts change state.
    /// Set when running in a pool via `set_delegate_notification_tx()`.
    delegate_notification_tx: Option<DelegateNotificationSender>,
}

impl<R, S> Executor<R, S>
where
    S: StateStorage + Send + Sync + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    /// Create a new Executor with optional network operation support.
    /// This is `pub(crate)` because the parameters involve crate-internal types.
    pub(crate) async fn new(
        state_store: StateStore<S>,
        ctrl_handler: impl FnOnce() -> anyhow::Result<()>,
        mode: OperationMode,
        runtime: R,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        ctrl_handler()?;

        Ok(Self {
            mode,
            runtime,
            state_store,
            update_notifications: HashMap::default(),
            client_subscription_counts: HashMap::default(),
            subscriber_summaries: HashMap::default(),
            delegate_origin_ids: HashMap::default(),
            init_tracker: ContractInitTracker::new(),
            op_manager,
            shared_notifications: None,
            shared_summaries: None,
            shared_client_counts: None,
            recovery_guard: Arc::new(std::sync::Mutex::new(HashSet::new())),
            summary_cache: LruCache::new(NonZeroUsize::new(1024).unwrap()),
            delta_cache: LruCache::new(NonZeroUsize::new(1024).unwrap()),
            delegate_notification_tx: None,
        })
    }

    pub fn test_data_dir(identifier: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "freenet-executor-{identifier}-{}-{unique_id}",
            std::process::id()
        ))
    }

    /// Set shared notification storage for pool-based operation.
    /// When set, notifications will be sent via shared storage instead of per-executor storage.
    /// This ensures subscriptions registered while this executor is checked out are still notified.
    pub(crate) fn set_shared_notifications(
        &mut self,
        notifications: SharedNotifications,
        summaries: SharedSummaries,
        client_counts: SharedClientCounts,
    ) {
        self.shared_notifications = Some(notifications);
        self.shared_summaries = Some(summaries);
        self.shared_client_counts = Some(client_counts);
    }

    /// Set a shared recovery guard for pool-based operation.
    /// All executors in a pool should share the same guard so that recovery
    /// tracking is consistent regardless of which executor handles a request.
    pub(crate) fn set_recovery_guard(&mut self, guard: CorruptedStateRecoveryGuard) {
        self.recovery_guard = guard;
    }

    /// Set the delegate notification sender for pool-based operation.
    /// When set, `commit_state_update()` will send notifications to subscribed delegates.
    pub(crate) fn set_delegate_notification_tx(&mut self, tx: DelegateNotificationSender) {
        self.delegate_notification_tx = Some(tx);
    }

    /// Create all stores including StateStore. Used when creating a standalone executor.
    pub(crate) async fn get_stores(
        config: &Config,
    ) -> Result<
        (
            ContractStore,
            DelegateStore,
            SecretsStore,
            StateStore<Storage>,
        ),
        anyhow::Error,
    > {
        const MAX_MEM_CACHE: u32 = 10_000_000;

        let db = Storage::new(&config.db_dir()).await?;
        let state_store = StateStore::new(db.clone(), MAX_MEM_CACHE).unwrap();
        let (contract_store, delegate_store, secret_store) = Self::get_runtime_stores(config, db)?;

        Ok((contract_store, delegate_store, secret_store, state_store))
    }

    /// Create only the Runtime stores (contract, delegate, secrets) without StateStore.
    /// Used by RuntimePool to create executors that share a StateStore.
    /// The Storage (ReDb) is shared across all stores for index persistence.
    pub(crate) fn get_runtime_stores(
        config: &Config,
        db: Storage,
    ) -> Result<(ContractStore, DelegateStore, SecretsStore), anyhow::Error> {
        const MAX_SIZE: u64 = 10 * 1024 * 1024;

        let contract_store = ContractStore::new(config.contracts_dir(), MAX_SIZE, db.clone())?;
        let delegate_store = DelegateStore::new(config.delegates_dir(), MAX_SIZE, db.clone())?;
        let secret_store = SecretsStore::new(config.secrets_dir(), config.secrets.clone(), db)?;

        Ok((contract_store, delegate_store, secret_store))
    }

    pub fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        let mut subscriptions = Vec::new();
        for (instance_id, client_list) in &self.update_notifications {
            for (client_id, _channel) in client_list {
                subscriptions.push(crate::message::SubscriptionInfo {
                    instance_id: *instance_id,
                    client_id: *client_id,
                    last_update: None,
                });
            }
        }
        subscriptions
    }
}

/// Test fixtures for creating contract-related test data.
///
/// These helpers make it easier to write unit tests for contract module code
/// by providing convenient constructors for common types.
#[cfg(test)]
pub(crate) mod test_fixtures {
    use freenet_stdlib::prelude::*;

    /// Create a test contract key with arbitrary but consistent data
    pub fn make_contract_key() -> ContractKey {
        let code = ContractCode::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let params = Parameters::from(vec![10, 20, 30, 40]);
        ContractKey::from_params_and_code(&params, &code)
    }

    /// Create a test contract key with custom code bytes
    pub fn make_contract_key_with_code(code_bytes: &[u8]) -> ContractKey {
        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![10, 20, 30, 40]);
        ContractKey::from_params_and_code(&params, &code)
    }

    /// Create a test wrapped state from raw bytes
    pub fn make_state(data: &[u8]) -> WrappedState {
        WrappedState::new(data.to_vec())
    }

    /// Create test parameters from raw bytes
    pub fn make_params(data: &[u8]) -> Parameters<'static> {
        Parameters::from(data.to_vec())
    }

    /// Create a test state delta from raw bytes
    pub fn make_delta(data: &[u8]) -> StateDelta<'static> {
        StateDelta::from(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod executor_error_tests {
        use super::*;

        #[test]
        fn test_executor_error_other_is_not_request() {
            let err = ExecutorError::other(anyhow::anyhow!("some error"));
            assert!(!err.is_request());
            assert!(!err.is_fatal());
        }

        #[test]
        fn test_executor_error_request_is_request() {
            let err = ExecutorError::request(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "test".into(),
            });
            assert!(err.is_request());
            assert!(!err.is_fatal());
        }

        #[test]
        fn test_executor_error_internal_error() {
            let err = ExecutorError::internal_error();
            assert!(!err.is_request());
            assert!(!err.is_fatal());
            assert!(err.to_string().contains("internal error"));
        }

        #[test]
        fn test_executor_error_display_left() {
            let err = ExecutorError::request(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "test cause".into(),
            });
            let display = err.to_string();
            assert!(display.contains("test cause") || display.contains("Put"));
        }

        #[test]
        fn test_executor_error_display_right() {
            let err = ExecutorError::other(anyhow::anyhow!("custom error message"));
            assert!(err.to_string().contains("custom error message"));
        }

        #[test]
        fn test_executor_error_from_request_error() {
            let request_err = RequestError::ContractError(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "from conversion".into(),
            });
            let err: ExecutorError = request_err.into();
            assert!(err.is_request());
        }

        #[test]
        fn test_executor_error_from_boxed_request_error() {
            let request_err = Box::new(RequestError::ContractError(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "boxed".into(),
            }));
            let err: ExecutorError = request_err.into();
            assert!(err.is_request());
        }

        #[test]
        fn test_unwrap_request_succeeds_for_request_error() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Put {
                key,
                cause: "unwrap test".into(),
            });
            let _unwrapped = err.unwrap_request(); // Should not panic
        }

        #[test]
        #[should_panic]
        fn test_unwrap_request_panics_for_other_error() {
            let err = ExecutorError::other(anyhow::anyhow!("not a request"));
            let _unwrapped = err.unwrap_request(); // Should panic
        }

        #[test]
        fn test_contract_exec_rejection_for_update_error() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "New state version 100 must be higher than current version 100",
            ));
            assert!(
                err.is_contract_exec_rejection(),
                "Update exec errors should be recognized as contract exec rejections"
            );
        }

        #[test]
        fn test_contract_exec_rejection_false_for_missing_parameters() {
            // This is the "missing contract parameters" case from runtime.rs:2681
            // where auto-fetch IS needed — must return false.
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Update {
                key,
                cause: "missing contract parameters".into(),
            });
            assert!(
                !err.is_contract_exec_rejection(),
                "Missing parameters errors should NOT be recognized as exec rejections"
            );
        }

        #[test]
        fn test_contract_exec_rejection_false_for_missing_contract() {
            let key = test_fixtures::make_contract_key();
            let err =
                ExecutorError::request(StdContractError::MissingContract { key: (*key.id()) });
            assert!(
                !err.is_contract_exec_rejection(),
                "MissingContract errors should NOT be recognized as exec rejections"
            );
        }

        #[test]
        fn test_contract_exec_rejection_false_for_other_error() {
            let err = ExecutorError::other(anyhow::anyhow!("some other error"));
            assert!(
                !err.is_contract_exec_rejection(),
                "Non-request errors should NOT be recognized as exec rejections"
            );
        }

        // Tests for `is_invalid_update_rejection` : the tighter predicate that
        // gates log severity (issue #3914). It must match ONLY the contract's
        // typed `InvalidUpdate{,WithInfo}` rejections, NOT runtime failures
        // like OOG/timeout/traps even though those flow through the same
        // `update_exec_error` wrapper.

        #[test]
        fn test_invalid_update_rejection_for_invalid_update() {
            let key = test_fixtures::make_contract_key();
            // Production cause string from issue #3914.
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "invalid contract update, reason: New state version 100 must be higher than current version 100",
            ));
            assert!(
                err.is_invalid_update_rejection(),
                "Contract InvalidUpdateWithInfo rejection MUST be recognized as benign"
            );
            assert!(
                err.is_contract_exec_rejection(),
                "The benign case must also satisfy the broader predicate (auto-fetch gate)"
            );
        }

        #[test]
        fn test_invalid_update_rejection_false_for_out_of_gas() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation.",
            ));
            assert!(
                !err.is_invalid_update_rejection(),
                "Out-of-gas MUST NOT be classified as a benign invalid-update rejection (it's a real WASM fault)"
            );
            assert!(
                err.is_contract_exec_rejection(),
                "Out-of-gas IS a contract-exec error (broader predicate matches), so auto-fetch is correctly skipped"
            );
        }

        #[test]
        fn test_invalid_update_rejection_false_for_max_compute_time() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "The operation exceeded the maximum allowed compute time",
            ));
            assert!(
                !err.is_invalid_update_rejection(),
                "Max-compute-time MUST NOT be classified as a benign invalid-update rejection"
            );
        }

        #[test]
        fn test_invalid_update_rejection_false_for_double_put() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                format!(
                    "Attempted to perform a put for an already put contract ({key}), use update instead"
                ),
            ));
            assert!(
                !err.is_invalid_update_rejection(),
                "DoublePut MUST NOT be classified as a benign invalid-update rejection"
            );
        }

        #[test]
        fn test_invalid_update_rejection_false_for_missing_parameters() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Update {
                key,
                cause: "missing contract parameters".into(),
            });
            assert!(
                !err.is_invalid_update_rejection(),
                "Missing parameters is a real failure, not a benign rejection"
            );
        }

        #[test]
        fn test_max_compute_time_exceeded_is_not_fatal() {
            use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
            let contract_err: ContractError =
                RuntimeInnerError::ContractExecError(ContractExecError::MaxComputeTimeExceeded)
                    .into();
            // op: None simulates validate_state() path where the bug manifested
            let err = ExecutorError::execution(contract_err, None);
            assert!(
                !err.is_fatal(),
                "MaxComputeTimeExceeded must not be fatal - it would kill the entire contract handler"
            );
        }

        // ── ContractQueueFull predicate (issue #4251) ─────────────────────
        //
        // The marker must be cleanly distinguishable from every other error
        // class so amplification suppression fires only on queue-full.

        #[test]
        fn test_contract_queue_full_true_for_marker_error() {
            let err = ExecutorError::other(ContractQueueFull);
            assert!(
                err.is_contract_queue_full(),
                "ContractQueueFull marker MUST be recognized by is_contract_queue_full"
            );
            // Display message preserved for human-readable surface (logs, etc.)
            assert!(err.to_string().contains("contract queue full"));
        }

        #[test]
        fn test_contract_queue_full_false_for_anyhow_string_lookalike() {
            // Predicate is typed (downcast), not string-matched: a similarly-
            // worded anyhow error must not inherit queue-full semantics.
            let err = ExecutorError::other(anyhow::anyhow!("contract queue full, try again later"));
            assert!(
                !err.is_contract_queue_full(),
                "Anyhow string with matching prose must NOT satisfy the typed predicate"
            );
        }

        #[test]
        fn test_contract_queue_full_false_for_invalid_update() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "invalid contract update, reason: stale",
            ));
            assert!(
                !err.is_contract_queue_full(),
                "Benign WASM invalid-update rejection is not queue-full"
            );
        }

        #[test]
        fn test_contract_queue_full_false_for_missing_parameters() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Update {
                key,
                cause: "missing contract parameters".into(),
            });
            assert!(
                !err.is_contract_queue_full(),
                "Missing contract parameters is not queue-full"
            );
        }

        #[test]
        fn test_contract_queue_full_disjoint_from_other_predicates() {
            // Load-bearing property used by the gating in op_ctx_task.rs:
            // the queue-full marker must trip its own predicate and no other.
            let err = ExecutorError::other(ContractQueueFull);
            assert!(err.is_contract_queue_full());
            assert!(!err.is_request());
            assert!(!err.is_contract_exec_rejection());
            assert!(!err.is_invalid_update_rejection());
            assert!(!err.is_missing_contract_parameters());
            assert!(!err.is_missing_delegate());
            assert!(!err.is_fatal());
        }
    }

    mod test_fixtures_tests {
        use super::*;

        #[test]
        fn test_make_contract_key_is_consistent() {
            let key1 = test_fixtures::make_contract_key();
            let key2 = test_fixtures::make_contract_key();
            assert_eq!(key1, key2);
        }

        #[test]
        fn test_make_contract_key_with_different_code() {
            let key1 = test_fixtures::make_contract_key_with_code(&[1, 2, 3]);
            let key2 = test_fixtures::make_contract_key_with_code(&[4, 5, 6]);
            assert_ne!(key1, key2);
        }

        #[test]
        fn test_make_state() {
            let state = test_fixtures::make_state(&[1, 2, 3, 4]);
            assert_eq!(state.as_ref(), &[1, 2, 3, 4]);
        }

        #[test]
        fn test_make_params() {
            let params = test_fixtures::make_params(&[10, 20]);
            assert_eq!(params.as_ref(), &[10, 20]);
        }

        #[test]
        fn test_make_delta() {
            let delta = test_fixtures::make_delta(&[100, 200]);
            assert_eq!(delta.as_ref(), &[100, 200]);
        }
    }
}
