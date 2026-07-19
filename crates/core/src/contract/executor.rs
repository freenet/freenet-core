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
    SecretsStore, SharedContractIndex, StateStorage, StateStore, StateStoreError,
    UserSecretContext,
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
/// delegate store and secret store. Enforced via the per-node
/// `wasm_runtime::SharedDelegateCounter` the runtime carries.
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

/// Typed marker carried by an [`ExecutorError`] when a hosted-mode secret
/// export was rejected for exceeding the per-user export bound (too many
/// secrets, or too much total plaintext). Lets the hosted-export HTTP layer
/// downcast and return a 413 (Payload Too Large) instead of a generic 500.
///
/// Constructed by `Executor::export_user_secrets` from a
/// [`crate::wasm_runtime::secret_export::ExportError::TooLarge`]; recognized by
/// [`ExecutorError::is_export_too_large`]. The `Display` text is non-secret
/// (sizes only, no token / secret bytes), so it is safe to log/return. See
/// #4381 P5.
#[derive(Debug, Clone)]
pub struct ExportTooLarge {
    pub message: String,
}

impl std::fmt::Display for ExportTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ExportTooLarge {}

/// Typed marker carried by an [`ExecutorError`] when a hosted-mode export was
/// rejected because `MAX_CONCURRENT_EXPORTS` exports are already running
/// off-loop. Lets the hosted-export HTTP layer downcast and return a 503
/// (Service Unavailable, "retry later") rather than a generic 500 — the export
/// was not attempted and is not queued. See #4531 / #4381 P5.
#[derive(Debug, Clone, Copy)]
pub struct ExportBusy;

impl std::fmt::Display for ExportBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("too many concurrent exports, try again later")
    }
}

impl std::error::Error for ExportBusy {}

/// Typed marker carried by an [`ExecutorError`] when a live secret import failed
/// on CLIENT-supplied input: a wrong decryption key, bad magic, a truncated or
/// unsupported bundle, an unknown KDF, a malformed entry, or a post-decrypt CBOR
/// parse failure. Lets the import HTTP layer downcast and return a 4xx (the
/// client uploaded the wrong bundle/key) instead of a generic 500. The `message`
/// is non-secret (it never echoes the key or any plaintext), so it is safe to
/// surface. See #4592.
#[derive(Debug, Clone)]
pub struct ImportBadBundle {
    pub message: String,
}

impl std::fmt::Display for ImportBadBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ImportBadBundle {}

/// Classify a [`crate::wasm_runtime::secret_export::ExportError`] from an import
/// as CLIENT-input (the uploaded bundle/key is wrong → 4xx) vs NODE-side (store
/// / IO / internal → 500).
///
/// Exhaustive match (no wildcard) so a future `ExportError` variant fails to
/// COMPILE here until it is explicitly classified — the catch-all would
/// otherwise silently misclassify a new variant (and trip
/// `clippy::wildcard_enum_match_arm`).
pub(crate) fn is_bad_bundle_input(e: &crate::wasm_runtime::secret_export::ExportError) -> bool {
    use crate::wasm_runtime::secret_export::ExportError;
    match e {
        // Client uploaded the wrong bundle or presented the wrong key.
        ExportError::AuthFailed
        | ExportError::BadMagic
        | ExportError::UnsupportedVersion(_)
        | ExportError::UnknownKdf(_)
        | ExportError::Truncated(_)
        | ExportError::BadEntryField { .. }
        | ExportError::CborDe(_) => true,
        // Node-side faults (or not reachable on the import path): a 500.
        ExportError::TooLarge { .. }
        | ExportError::Store(_)
        | ExportError::Runtime(_)
        | ExportError::CborSer(_)
        | ExportError::Argon2(_)
        | ExportError::EncryptFailed
        | ExportError::Io(_) => false,
    }
}

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
    /// Discriminator: stdlib's `ContractError::Update` OR `ContractError::Put`
    /// with a cause containing the literal "missing contract parameters"
    /// string. BOTH variants must be matched (issue #3279):
    ///
    /// - The delta / update-only path
    ///   (`executor/runtime/contract_ops.rs`) raises the `Update` variant.
    /// - The full-state upsert path
    ///   (`executor/runtime/executor_impl.rs::upsert_contract_state`) raises
    ///   the `Put` variant when `state_store.get_params` returns `None`.
    ///
    /// A cross-node **full-state (non-delta)** UPDATE takes the upsert path,
    /// so it surfaces as `Put`. Matching only `Update` (the pre-#3279
    /// behavior) silently misclassified that case: the auto-fetch recovery
    /// gated on this predicate (the originator self-heal and the no-remote
    /// hosting-divergence branch in `update/op_ctx_task.rs`) never fired, so
    /// a subscriber that received a full-state broadcast without local params
    /// stayed permanently stuck on "missing contract parameters" — exactly
    /// the #3279 regression. Any other cause string on either variant returns
    /// false.
    pub fn is_missing_contract_parameters(&self) -> bool {
        match &self.inner {
            Either::Left(req_err) => matches!(
                req_err.as_ref(),
                RequestError::ContractError(
                    StdContractError::Update { cause, .. } | StdContractError::Put { cause, .. },
                ) if cause.contains("missing contract parameters")
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

    /// Returns true ONLY when the contract's WASM merge/validate exceeded the
    /// execution time limit — the #4861 poison-contract case where every apply
    /// runs past `max_execution_seconds`. Both the wall-clock timeout and the
    /// epoch-deadline interrupt (#4861) converge on `WasmError::Timeout` →
    /// `ContractExecError::MaxComputeTimeExceeded` → `update_exec_error`, whose
    /// cause is "execution error: The operation exceeded the maximum allowed
    /// compute time".
    ///
    /// Used by the per-contract merge-failure backoff (#4861) to select the
    /// longer *Timeout*-class cooldown: a runaway merge is far more expensive to
    /// re-attempt than a cheap `InvalidUpdate` rejection, so a contract that
    /// times out should be quarantined harder than one that merely rejects a
    /// stale delta. Narrow by design — matched on the same stable
    /// "execution error:" prefix as `is_invalid_update_rejection`, so out-of-gas,
    /// traps, and other exec errors return false and keep the base
    /// (Invalid-class) cooldown.
    ///
    /// String-matched (not a typed downcast like `is_contract_queue_full`)
    /// deliberately: the timeout must KEEP its `Update{cause}` representation so
    /// `is_contract_exec_rejection` still returns true for it (a timeout means
    /// the contract IS present locally, so no auto-fetch), and so a client-local
    /// UPDATE timeout still surfaces a typed `ContractError::Update` to the
    /// client. The compute-time message is defined in this crate
    /// (`ContractExecError::MaxComputeTimeExceeded`), so the string is stable.
    pub fn is_wasm_timeout(&self) -> bool {
        match &self.inner {
            Either::Left(req_err) => matches!(
                req_err.as_ref(),
                RequestError::ContractError(StdContractError::Update { cause, .. })
                    if cause.starts_with("execution error:")
                        && cause.contains("maximum allowed compute time")
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

    /// Returns true if this error is the typed [`ExportTooLarge`] marker (a
    /// hosted-mode export rejected for exceeding the per-user export bound).
    /// The hosted-export HTTP handler gates a 413 response on this. See #4381 P5.
    pub fn is_export_too_large(&self) -> bool {
        match &self.inner {
            Either::Left(_) => false,
            Either::Right(err) => err.downcast_ref::<ExportTooLarge>().is_some(),
        }
    }

    /// Returns true if this error is the typed [`ExportBusy`] marker (a
    /// hosted-mode export rejected because the node is at its concurrent-export
    /// cap). The hosted-export HTTP handler gates a 503 response on this so the
    /// caller can distinguish "retry later" from a real failure. See #4531 P5.
    pub fn is_export_busy(&self) -> bool {
        match &self.inner {
            Either::Left(_) => false,
            Either::Right(err) => err.downcast_ref::<ExportBusy>().is_some(),
        }
    }

    /// Returns true if this error is the typed [`ImportBadBundle`] marker (a live
    /// import rejected because the client-supplied bundle/key was wrong: wrong
    /// key, bad magic, truncated/unsupported bundle, or a malformed entry). The
    /// import HTTP handler gates a 4xx on this — a client-input fault, NOT a node
    /// fault, so it must not read as a 500. See #4592.
    pub fn is_import_bad_bundle(&self) -> bool {
        match &self.inner {
            Either::Left(_) => false,
            Either::Right(err) => err.downcast_ref::<ImportBadBundle>().is_some(),
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

    /// Export the per-user delegate secrets named by `user_context` into an
    /// encrypted bundle, sealed under the user's `token` (hosted-mode export,
    /// P3-live of #4381). The bundle round-trips through
    /// [`crate::wasm_runtime::secret_export::import_bundle`] with
    /// `BundleKeyMaterial::Token(token)`, so the user re-imports on their own
    /// peer with the same token they already hold.
    ///
    /// `user_context` MUST come from the connection boundary (the same
    /// forge-proof channel that scopes delegate secrets), never from a request
    /// body — see [`UserSecretContext`]'s security invariant. The export is
    /// strictly per-user: it reads only `user_context.scope()`
    /// ([`crate::wasm_runtime::SecretScope::User`]), never the node-local
    /// (`Local`) namespace.
    ///
    /// Admit a hosted-mode export to run OFF the contract loop (#4531 / #4381
    /// P5). Instead of running the (potentially long) enumerate+decrypt+seal
    /// inline — which would park the single-threaded contract loop for its whole
    /// duration, so queued GET/PUT/UPDATE/delegate events wait behind it — this
    /// checks out a pooled executor and returns an opaque
    /// `ExportJob`. The caller (the contract loop) moves the job into
    /// a background task, calls `ExportJob::run` there, and hands the
    /// resulting [`runtime::ExportDone`] back to [`Self::finish_export`] on the
    /// loop to return/replace the executor.
    ///
    /// Concurrency is bounded ([`runtime::MAX_CONCURRENT_EXPORTS`]); over the cap
    /// returns [`runtime::ExportAdmission::Busy`]. `user_context` MUST come from
    /// the connection boundary (the forge-proof per-user namespace), never a
    /// request body; the export reads only `user_context.scope()`, never `Local`.
    ///
    /// The default implementation returns
    /// [`runtime::ExportAdmission::Unsupported`]: only the production
    /// `RuntimePool` (which owns real `SecretsStore`-backed executors) supports
    /// export. Mock executors keep no on-disk secrets.
    /// NON-BLOCKING: runs on the contract loop, so it must never await/park (a
    /// blocking executor checkout here is the #4531 deadlock). Returns `Busy`
    /// when the node is at its export-concurrency cap OR no executor is
    /// immediately free; the loop answers a 503 and never queues the export.
    fn try_begin_export(
        &mut self,
        _user_context: &UserSecretContext,
        _token: &[u8],
    ) -> runtime::ExportAdmission {
        runtime::ExportAdmission::Unsupported
    }

    /// Return (or, on a panicked export task, replace) the executor an
    /// `ExportJob` borrowed, and yield the export RESULT for the
    /// client. Called on the contract loop once the background export task
    /// delivers its [`runtime::ExportDone`]. Default returns the carried result
    /// without touching a pool (mock executors never admit, so this is only
    /// reached via the `ExportDone` carried in an [`runtime::ExportDone`] the
    /// default path never builds).
    fn finish_export(
        &mut self,
        done: runtime::ExportDone,
    ) -> impl Future<Output = Result<Vec<u8>, ExecutorError>> + Send {
        async move { done.into_result() }
    }

    /// Import delegate secrets from an encrypted `bundle` into the node's secrets
    /// store at `target_scope`, LIVE (#4592). Runs ON the contract loop
    /// (serialized with delegate `store_secret`) — DELIBERATELY not off-loop like
    /// the export, because the import WRITES and the store write path assumes
    /// node-wide write serialization (see `RuntimePool::import_secrets` and the
    /// `ImportSecrets` arm in `contract.rs`).
    ///
    /// Default returns a not-supported error: only the production `RuntimePool`
    /// (which owns real `SecretsStore`-backed executors) supports import; mock
    /// executors keep no on-disk secrets.
    fn import_secrets(
        &mut self,
        _target_scope: crate::contract::handler::ImportTargetScope,
        _bundle: &[u8],
        _key: &[u8],
        _key_kind: crate::contract::handler::BundleKeyKind,
        _overwrite: bool,
    ) -> impl Future<
        Output = Result<crate::wasm_runtime::secret_export::ImportReport, ExecutorError>,
    > + Send {
        async move {
            Err(ExecutorError::other(anyhow::anyhow!(
                "secret import is not supported by this executor"
            )))
        }
    }

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

// ============================================================================
// Summary / delta fast-path cache sizing
//
// The summary/delta caches memoize the (expensive) WASM `summarize_state` /
// `get_state_delta` calls that the ~5-min interest heartbeat runs for every
// hosted contract. Two INDEPENDENT bounds govern each cache (see
// `ByteBoundedLruCache`):
//
//   1. COUNT target (coverage): grown to the node's live hosted-contract count
//      (`Ring::hosting_contracts_count()`, via `ensure_cache_covers_hosted_set`)
//      so the heartbeat's whole hosted working set stays cached across cycles and
//      never recompiles a cold module. Tied to the REAL hosted count, not a
//      contract-size assumption.
//   2. BYTE budget (safety): a hard ceiling on total retained bytes, INDEPENDENT
//      of how large the contract-controlled `StateSummary`/`StateDelta` values
//      are. The count target alone cannot bound RAM — a contract that emits large
//      summaries/deltas, cached at up to the count MAX across every pool worker,
//      could otherwise pin gigabytes and OOM the node (#4565 class; the code-style
//      "per-key collections influenced by external actors MUST be size-bounded"
//      amplification rule). The pre-#4802 flat count cap bounded this only by
//      accident of being small; the count-resize removed that incidental bound.
//
// In the normal case (summaries are small digests) the count target binds and the
// byte budget has ample headroom, so coverage holds. Only a large-value contract
// makes the byte budget bind, holding fewer entries but never OOMing.
// ============================================================================

/// Lower clamp for the summary/delta cache COUNT target (entries). Keeps
/// small/mock nodes at the historical fixed size so behavior is unchanged there.
pub(crate) const SUMMARY_CACHE_COUNT_MIN: usize = 1024;

/// Slack added to the live hosted count before clamping, so contracts hosted
/// mid-cycle (between the resize and the heartbeat) still land inside the cache.
pub(crate) const SUMMARY_CACHE_COUNT_MARGIN: usize = 256;

/// Upper clamp for the summary/delta cache COUNT target (entries). Bounds the LRU
/// node count for pathologically tiny contracts; the byte budget (below) is the
/// real RAM bound, so this only caps bookkeeping overhead. Chosen as a power of
/// two so `next_power_of_two()` never overshoots it.
pub(crate) const SUMMARY_CACHE_COUNT_MAX: usize = 65_536;

/// Count target for the summary/delta cache given the live hosted-contract
/// count: cover the hosted set (plus margin), clamped to [MIN, MAX] and rounded
/// up to a power of two (LruCache sizing), never exceeding MAX.
///
/// Pure, so the boundary math (zero/one/min/mid/max/overflow) is unit-testable
/// without the OpManager/Ring integration path. `saturating_add` keeps a
/// `usize::MAX` hosted count from overflowing, and clamping before
/// `next_power_of_two` keeps that call from panicking on a huge input.
pub(crate) fn summary_cache_count_target(hosted: usize) -> usize {
    hosted
        .saturating_add(SUMMARY_CACHE_COUNT_MARGIN)
        .clamp(SUMMARY_CACHE_COUNT_MIN, SUMMARY_CACHE_COUNT_MAX)
        .next_power_of_two()
        // Defensive: `next_power_of_two` of a value already clamped to
        // `COUNT_MAX` (a power of two) cannot exceed `COUNT_MAX`, so this is a
        // no-op today — kept to stay correct if `COUNT_MAX` is ever set to a
        // non-power-of-two.
        .min(SUMMARY_CACHE_COUNT_MAX)
}

/// Per-entry structural-overhead allowance (bytes) added to every summary/delta
/// value's payload length when accounting for the byte budget.
///
/// Two jobs:
///   - It covers the real per-entry overhead the payload length ignores — the key
///     (`ContractKey` / `(ContractKey, u64, u64)`), the `LruCache` node's
///     prev/next pointers and boxed entry, and the map slot (~100-250 B combined).
///     Adding it on TOP of the payload makes the counted total a genuine upper
///     bound on retained RAM, so the byte budget is a true hard cap (not the
///     ~1.5x-of-budget story an un-floored weigher would give).
///   - It floors each entry's weight so even EMPTY values still count. A contract
///     legitimately returns an empty delta once a peer is current; without a
///     floor an unbounded stream of distinct zero-weight keys would never be
///     evicted and the entry count (with its uncounted overhead) would grow
///     without bound — the #4565 OOM class this budget exists to close (same
///     failure the closed PR #4794 fixed with its delta-cache floor). With the
///     floor the entry COUNT is capped at `byte_budget / CACHE_ENTRY_OVERHEAD_BYTES`.
pub(crate) const CACHE_ENTRY_OVERHEAD_BYTES: usize = 512;

/// Fraction of "memory the node may use" that sizes the per-executor SUMMARY
/// cache byte budget. Summaries are small digests, so a modest share holds far
/// more small entries than any realistic hosted count while capping worst-case
/// bytes. See `summary_cache_budget_bytes`.
const SUMMARY_CACHE_RAM_DIVISOR: usize = 64;

/// Lower clamp for the summary-cache byte budget (16 MiB). At the ~512 B per-entry
/// floor this holds ~32k small summaries — far above any realistic hosted count on
/// a small node — so the count target (coverage) binds, never this floor.
const SUMMARY_CACHE_MIN_BYTES: usize = 16 * 1024 * 1024;

/// Upper clamp for the summary-cache byte budget (32 MiB). At the ~512 B floor
/// this holds ~65k small summaries (≈ the count MAX), so coverage still holds at
/// the count cap for small digests; a large-summary contract instead evicts down
/// to whatever fits in 32 MiB. Per-executor × up to 16 pool workers → ≤ 512 MiB
/// aggregate summary-cache RAM.
const SUMMARY_CACHE_MAX_BYTES: usize = 32 * 1024 * 1024;

/// Fraction of "memory the node may use" that sizes the per-executor DELTA cache
/// byte budget. Deltas are larger and higher-cardinality than summaries (the key
/// includes a per-peer summary hash, so a contract can hold >1 delta entry during
/// fan-out), so the delta cache gets a bigger share. Mirrors the closed PR #4794's
/// `hosting_budget / 16` intent (host RAM and the hosting budget are both
/// RAM-scaled). See `delta_cache_budget_bytes`.
const DELTA_CACHE_RAM_DIVISOR: usize = 16;

/// Lower clamp for the delta-cache byte budget (8 MiB). Matches PR #4794's floor:
/// enough for a small node's fan-out working set.
const DELTA_CACHE_MIN_BYTES: usize = 8 * 1024 * 1024;

/// Upper clamp for the delta-cache byte budget (64 MiB). Matches PR #4794's
/// ceiling. Per-executor × up to 16 pool workers → ≤ 1 GiB aggregate delta-cache
/// RAM. Combined with the summary cache (≤ 512 MiB aggregate) and the shared
/// module cache (≤ 1.5 GiB) the total cache commitment stays a safe fraction of a
/// production gateway's memory (≈ 3 GiB worst case, well under the 7600 MiB (= 7.42 GiB)
/// gateway limit). NOTE: this budget is PER-EXECUTOR; aggregate RAM is
/// `pool_size × (summary_budget + delta_budget)`, not a single node-wide figure.
/// `pool_size` tracks CPU count (core count), so a RAM-scaled per-executor budget
/// is multiplied by cores: on an exotic high-core/low-RAM box the aggregate is a
/// larger fraction of that box's RAM; production gateways have modest core counts
/// and stay safe (asserted by `cache_byte_budgets_are_aggregate_safe`).
const DELTA_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Per-executor SUMMARY-cache byte budget, scaled to the memory the node may use
/// (host RAM, or a smaller cgroup limit when containerized) and clamped to a sane
/// floor/ceiling: `clamp(total_ram / SUMMARY_CACHE_RAM_DIVISOR,
/// SUMMARY_CACHE_MIN_BYTES, SUMMARY_CACHE_MAX_BYTES)`. Reuses the module cache's
/// `read_total_ram_bytes()` so there is a single "memory the node may use" source.
pub(crate) fn summary_cache_budget_bytes() -> usize {
    let total_ram = crate::wasm_runtime::read_total_ram_bytes()
        .unwrap_or(SUMMARY_CACHE_FALLBACK_TOTAL_RAM_BYTES);
    (total_ram / SUMMARY_CACHE_RAM_DIVISOR).clamp(SUMMARY_CACHE_MIN_BYTES, SUMMARY_CACHE_MAX_BYTES)
}

/// Per-executor DELTA-cache byte budget, derived the same way as
/// `summary_cache_budget_bytes` but with the delta divisor/clamps (deltas are
/// larger and higher-cardinality): `clamp(total_ram / DELTA_CACHE_RAM_DIVISOR,
/// DELTA_CACHE_MIN_BYTES, DELTA_CACHE_MAX_BYTES)`.
pub(crate) fn delta_cache_budget_bytes() -> usize {
    let total_ram = crate::wasm_runtime::read_total_ram_bytes()
        .unwrap_or(SUMMARY_CACHE_FALLBACK_TOTAL_RAM_BYTES);
    (total_ram / DELTA_CACHE_RAM_DIVISOR).clamp(DELTA_CACHE_MIN_BYTES, DELTA_CACHE_MAX_BYTES)
}

/// Fallback total-RAM estimate (1 GiB) when the OS query fails — mirrors the
/// module cache's `FALLBACK_TOTAL_RAM_BYTES`, yielding a mid-range budget.
const SUMMARY_CACHE_FALLBACK_TOTAL_RAM_BYTES: usize = 1024 * 1024 * 1024;

/// A count-capped LRU cache with a hard total-byte backstop.
///
/// Wraps [`lru::LruCache`] with running byte accounting so eviction is driven by
/// EITHER bound, whichever binds first:
///
///   - the LRU's own COUNT cap (`inner.cap()`, grown via [`Self::grow`] to the
///     live hosted count for coverage), and
///   - a fixed BYTE budget (`byte_budget`): after every insert, LRU entries are
///     popped until `total_bytes <= byte_budget`.
///
/// The values (`StateSummary` / `StateDelta`) are contract-controlled and
/// variable-size, so the count cap ALONE cannot bound RAM and the byte budget
/// ALONE would make coverage a contract-size assumption. Both together: small
/// digests → count binds (coverage); large values → bytes bind (safety). See the
/// module-level cache-sizing comment above.
///
/// A single value whose accounted weight alone exceeds the whole budget is NOT
/// cached: [`Self::put`] returns early without inserting it. The values are
/// contract-controlled and can reach the WASM memory limit, so retaining even one
/// oversized entry (times every pool worker times both caches) would defeat the
/// hard cap and is a real OOM vector (#4565). This deliberately does NOT match
/// [`crate::wasm_runtime::ModuleCache`]'s "keep one oversized entry" handling:
/// that cache's values are trusted operator-supplied modules; these are not, so
/// they get no oversized exemption. Net: `total_bytes <= byte_budget` holds
/// STRICTLY after every put.
struct ByteBoundedLruCache<K: std::hash::Hash + Eq, V> {
    inner: LruCache<K, V>,
    /// Running sum of every resident entry's weight
    /// (`weigh(value) + CACHE_ENTRY_OVERHEAD_BYTES`). Invariant: equals
    /// the sum over all entries.
    total_bytes: usize,
    /// Hard eviction threshold in bytes.
    byte_budget: usize,
    /// Payload byte size of a value; the per-entry structural overhead is added
    /// on top in [`Self::entry_weight`].
    weigh: fn(&V) -> usize,
}

impl<K: std::hash::Hash + Eq, V> ByteBoundedLruCache<K, V> {
    fn new(count_cap: NonZeroUsize, byte_budget: usize, weigh: fn(&V) -> usize) -> Self {
        Self {
            inner: LruCache::new(count_cap),
            total_bytes: 0,
            byte_budget: byte_budget.max(1),
            weigh,
        }
    }

    /// Counted weight of one entry: payload length plus the per-entry structural
    /// overhead allowance (which also floors empty values above zero).
    fn entry_weight(&self, value: &V) -> usize {
        (self.weigh)(value).saturating_add(CACHE_ENTRY_OVERHEAD_BYTES)
    }

    /// Look up a key, marking it most-recently-used on a hit.
    fn get(&mut self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    /// Insert (or replace) a value, then evict LRU entries until within the byte
    /// budget. A value whose accounted weight alone exceeds the budget is NOT
    /// cached (early return): the values are contract-controlled, so caching one
    /// would defeat the hard cap, and the caller already owns its own copy of the
    /// result, so caching buys nothing. Any pre-existing entry under the same key
    /// is left untouched (it was already within budget, so the invariant holds).
    fn put(&mut self, key: K, value: V) {
        let added = self.entry_weight(&value);
        // Skip-oversized guard (#4565): a single value larger than the whole
        // budget would otherwise stay resident (the pop-loop below keeps the MRU
        // entry), leaving total_bytes > byte_budget and breaking the hard cap.
        // StateSummary/StateDelta are contract-controlled and can reach the WASM
        // memory limit, so refuse to cache such a value at all. The caller already
        // owns its result; a later cache miss simply recomputes. Result:
        // total_bytes <= byte_budget holds STRICTLY after every put.
        if added > self.byte_budget {
            return;
        }
        // `push` returns the displaced entry: the OLD value when `key` already
        // existed, OR the LRU entry evicted to honor the COUNT cap. In BOTH cases
        // subtract its weight so the running total stays exact (a replace does not
        // grow the count, so it never also evicts — exactly one of the two).
        if let Some((_, displaced)) = self.inner.push(key, value) {
            self.total_bytes = self
                .total_bytes
                .saturating_sub(self.entry_weight(&displaced));
        }
        self.total_bytes = self.total_bytes.saturating_add(added);
        // Byte backstop: pop LRU entries until within budget. With the
        // skip-oversized guard above, the just-inserted entry alone is always
        // within budget, so this converges to total_bytes <= byte_budget every
        // time. The len() > 1 guard is kept as defense-in-depth but is no longer
        // what bounds the total (no entry can alone exceed the budget now).
        while self.total_bytes > self.byte_budget && self.inner.len() > 1 {
            match self.inner.pop_lru() {
                Some((_, evicted)) => {
                    self.total_bytes = self.total_bytes.saturating_sub(self.entry_weight(&evicted));
                }
                None => break,
            }
        }
    }

    /// The current COUNT cap.
    fn cap(&self) -> NonZeroUsize {
        self.inner.cap()
    }

    /// Grow the COUNT cap. Only ever grows (callers guard on `new > cap`), so this
    /// never evicts and the byte total stays exact. A shrink WOULD evict entries
    /// via `lru::resize` WITHOUT byte accounting, drifting `total_bytes` high, so a
    /// non-growing request is made a no-op in RELEASE too (not just a debug
    /// assert): the early return below is the real guard against a future
    /// non-monotonic caller.
    fn grow(&mut self, cap: NonZeroUsize) {
        debug_assert!(
            cap >= self.inner.cap(),
            "ByteBoundedLruCache::grow must not shrink (would leak byte accounting)"
        );
        // Release-safe guard: a shrink (or a no-op re-grow to the same cap) must
        // not reach `lru::resize`, which would evict without updating `total_bytes`.
        if cap <= self.inner.cap() {
            return;
        }
        self.inner.resize(cap);
    }

    #[cfg(test)]
    fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

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
    /// which calls summarize_state() for every matching contract. Bounded by BOTH a
    /// count target grown to the live hosted count (coverage, so the heartbeat
    /// stays warm) AND a hard byte budget (safety, so a large-summary contract
    /// cannot OOM the node). See [`ByteBoundedLruCache`].
    summary_cache: ByteBoundedLruCache<ContractKey, (u64, StateSummary<'static>)>,

    /// Cache of delta results keyed by (ContractKey, state_hash, their_summary_hash).
    /// Avoids redundant WASM instantiations for get_state_delta() calls. Byte-bounded
    /// like the summary cache (deltas are larger + the per-peer summary hash in the
    /// key means >1 entry per contract during fan-out). See [`ByteBoundedLruCache`].
    delta_cache: ByteBoundedLruCache<(ContractKey, u64, u64), StateDelta<'static>>,

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
            summary_cache: ByteBoundedLruCache::new(
                NonZeroUsize::new(SUMMARY_CACHE_COUNT_MIN).unwrap(),
                summary_cache_budget_bytes(),
                |(_, summary)| summary.as_ref().len(),
            ),
            delta_cache: ByteBoundedLruCache::new(
                NonZeroUsize::new(SUMMARY_CACHE_COUNT_MIN).unwrap(),
                delta_cache_budget_bytes(),
                |delta| delta.as_ref().len(),
            ),
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
        // Standalone executor: no pool, so its ContractStore owns a fresh
        // (unshared) instance index.
        let (contract_store, delegate_store, secret_store) =
            Self::get_runtime_stores(config, db, None)?;

        Ok((contract_store, delegate_store, secret_store, state_store))
    }

    /// Create only the Runtime stores (contract, delegate, secrets) without StateStore.
    /// Used by RuntimePool to create executors that share a StateStore.
    /// The Storage (ReDb) is shared across all stores for index persistence.
    ///
    /// `shared_contract_index` is `Some` for pool executors so every executor's
    /// `ContractStore` shares one live `ContractInstanceId -> CodeHash` map
    /// (#4218); `None` for standalone executors, which get a fresh index.
    pub(crate) fn get_runtime_stores(
        config: &Config,
        db: Storage,
        shared_contract_index: Option<SharedContractIndex>,
    ) -> Result<(ContractStore, DelegateStore, SecretsStore), anyhow::Error> {
        const MAX_SIZE: u64 = 10 * 1024 * 1024;

        let contract_store = match shared_contract_index {
            Some(index) => ContractStore::new_with_shared_index(
                config.contracts_dir(),
                MAX_SIZE,
                db.clone(),
                index,
            )?,
            None => ContractStore::new(config.contracts_dir(), MAX_SIZE, db.clone())?,
        };
        let delegate_store = DelegateStore::new(config.delegates_dir(), MAX_SIZE, db.clone())?;
        // Thread the operator-configured per-user secret quota (#4561, P5 of
        // #4381) into the store at construction. `0` = disabled (the default).
        // Every pooled executor passes the SAME limit (same Config), while the
        // per-user byte counters they enforce against live in the process-global
        // tracker inside SecretsStore — so accounting is shared even though each
        // executor builds its own store.
        let secret_store = SecretsStore::new(config.secrets_dir(), config.secrets.clone(), db)?
            .with_user_quota(config.per_user_secret_quota_bytes);

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

    /// Tests for [`ByteBoundedLruCache`] — the count-target + byte-backstop wrapper
    /// that bounds the summary/delta fast-path caches.
    mod byte_bounded_lru_cache_tests {
        use super::*;

        fn vec_len(v: &Vec<u8>) -> usize {
            v.len()
        }

        /// P1 regression: a contract-controlled cache VALUE is variable-size, so the
        /// COUNT cap alone cannot bound RAM. With a huge count cap but a modest byte
        /// budget, inserting many LARGE values must keep total retained bytes under
        /// the byte budget (holding far fewer than the count cap) — otherwise a
        /// large-summary/large-delta contract could pin gigabytes and OOM the node
        /// (#4565 class). Without the byte backstop the count cap would let all 200
        /// one-MiB values (~200 MiB) stay resident.
        #[test]
        fn byte_budget_bounds_ram_for_large_values() {
            let byte_budget = 8 * 1024 * 1024; // 8 MiB
            let count_cap = NonZeroUsize::new(65_536).unwrap(); // effectively unbounded here
            let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
                ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

            // Insert 200 distinct 1-MiB values (200 MiB total if unbounded).
            for i in 0..200u64 {
                cache.put(i, vec![0u8; 1024 * 1024]);
                assert!(
                    cache.total_bytes() <= byte_budget,
                    "total_bytes {} exceeded byte_budget {} after insert {}",
                    cache.total_bytes(),
                    byte_budget,
                    i
                );
            }

            // At ~1 MiB/entry an 8 MiB budget holds <= 8 entries — nowhere near the
            // 65_536 count cap. The byte backstop, not the count cap, bound the RAM.
            assert!(
                cache.len() <= 8,
                "byte budget must hold far fewer than the count cap; held {}",
                cache.len()
            );
            assert!(
                cache.total_bytes() <= byte_budget,
                "final total_bytes {} must be within byte_budget {}",
                cache.total_bytes(),
                byte_budget
            );
        }

        /// P1 skip-oversized: a value whose accounted weight alone exceeds the
        /// byte budget is NOT cached at all, so `total_bytes` stays 0 and the
        /// cache stays empty. A within-budget value inserted afterward caches
        /// normally and stays within budget. Pins the skip-oversized fix (#4565).
        #[test]
        fn oversized_value_is_not_cached() {
            let byte_budget = 8 * 1024 * 1024; // 8 MiB
            let count_cap = NonZeroUsize::new(65_536).unwrap();
            let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
                ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

            // A 16-MiB value alone exceeds the 8-MiB budget, so it must be refused.
            cache.put(1, vec![0u8; 16 * 1024 * 1024]);
            assert!(
                cache.get(&1).is_none(),
                "an over-budget value must not be cached"
            );
            assert_eq!(
                cache.len(),
                0,
                "cache must stay empty after an over-budget put"
            );
            assert_eq!(
                cache.total_bytes(),
                0,
                "total_bytes must stay 0 when nothing was cached"
            );

            // A normal-sized value still caches and stays within budget.
            cache.put(2, vec![0u8; 1024]);
            assert!(cache.get(&2).is_some(), "a within-budget value must cache");
            assert_eq!(cache.len(), 1);
            assert!(
                cache.total_bytes() <= byte_budget,
                "total_bytes {} must stay within budget {}",
                cache.total_bytes(),
                byte_budget
            );
        }

        /// The per-entry overhead floor means even ZERO-length values count toward
        /// the budget, so an unbounded stream of distinct empty-value keys cannot
        /// accumulate without bound (the empty-delta failure PR #4794 fixed). Entry
        /// count is capped at `byte_budget / CACHE_ENTRY_OVERHEAD_BYTES`.
        #[test]
        fn empty_values_stay_entry_bounded() {
            // Budget for exactly 32 entries at the overhead floor.
            let byte_budget = 32 * CACHE_ENTRY_OVERHEAD_BYTES;
            let count_cap = NonZeroUsize::new(65_536).unwrap();
            let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
                ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

            for i in 0..2000u64 {
                cache.put(i, Vec::new()); // empty value → weigh == 0, floored to overhead
            }
            assert!(
                cache.len() <= 32,
                "empty values must still be evicted at the overhead floor; held {} (expected <= 32)",
                cache.len()
            );
        }

        /// In the normal case (small values, generous budget) the COUNT cap binds —
        /// this is the coverage guarantee at the unit level. With small values that
        /// never approach the byte budget, the cache holds exactly up to its count
        /// cap and evicting-by-count keeps the byte total exact.
        #[test]
        fn count_cap_binds_for_small_values() {
            let byte_budget = 32 * 1024 * 1024; // ample
            let count_cap = NonZeroUsize::new(4).unwrap();
            let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
                ByteBoundedLruCache::new(count_cap, byte_budget, vec_len);

            for i in 0..10u64 {
                cache.put(i, vec![7u8; 8]);
            }
            assert_eq!(cache.len(), 4, "count cap must bind for small values");
            // Byte total must match the 4 resident entries exactly (each 8 + overhead).
            assert_eq!(
                cache.total_bytes(),
                4 * (8 + CACHE_ENTRY_OVERHEAD_BYTES),
                "byte accounting must stay exact across count-cap evictions"
            );
            // Only the 4 most-recently-inserted keys survive.
            for i in 0..6u64 {
                assert!(cache.get(&i).is_none(), "key {i} should have been evicted");
            }
            for i in 6..10u64 {
                assert!(cache.get(&i).is_some(), "key {i} should be resident");
            }
        }

        /// Replacing an existing key must not double-count its bytes: the running
        /// total reflects the NEW value's size, not old + new.
        #[test]
        fn replacing_a_key_keeps_byte_total_exact() {
            let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
                ByteBoundedLruCache::new(NonZeroUsize::new(16).unwrap(), 32 * 1024 * 1024, vec_len);
            cache.put(1, vec![0u8; 100]);
            cache.put(1, vec![0u8; 300]);
            assert_eq!(cache.len(), 1);
            assert_eq!(
                cache.total_bytes(),
                300 + CACHE_ENTRY_OVERHEAD_BYTES,
                "replace must account only the new value, not old + new"
            );
        }

        /// Growing the count cap must not disturb byte accounting (it never evicts).
        #[test]
        fn grow_preserves_byte_total() {
            let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
                ByteBoundedLruCache::new(NonZeroUsize::new(2).unwrap(), 32 * 1024 * 1024, vec_len);
            cache.put(1, vec![0u8; 10]);
            cache.put(2, vec![0u8; 20]);
            let before = cache.total_bytes();
            cache.grow(NonZeroUsize::new(1024).unwrap());
            assert_eq!(cache.cap().get(), 1024);
            assert_eq!(
                cache.total_bytes(),
                before,
                "grow must not change byte total"
            );
            assert_eq!(cache.len(), 2, "grow must not evict");
        }

        /// The `cap <= inner.cap()` no-shrink guard in [`Self::grow`] makes a
        /// repeated grow to the SAME cap a no-op: it returns before
        /// `lru::resize`, so no entry is evicted and byte accounting is
        /// untouched. Pins the guard that stops a non-monotonic (equal-cap)
        /// caller from corrupting `total_bytes` via an un-accounted resize
        /// eviction. An equal cap also satisfies the no-shrink `debug_assert`,
        /// so this exercises the early return without tripping it.
        #[test]
        fn grow_with_equal_cap_is_noop() {
            let count_cap = NonZeroUsize::new(4).unwrap();
            let mut cache: ByteBoundedLruCache<u64, Vec<u8>> =
                ByteBoundedLruCache::new(count_cap, 32 * 1024 * 1024, vec_len);
            cache.put(1, vec![0u8; 10]);
            cache.put(2, vec![0u8; 20]);
            cache.put(3, vec![0u8; 30]);
            let len_before = cache.len();
            let bytes_before = cache.total_bytes();
            let cap_before = cache.cap().get();

            // Grow to the SAME cap the cache already has: the `cap <= inner.cap()`
            // guard returns early (equal cap also satisfies the no-shrink
            // debug_assert), so this is a pure no-op (no resize, hence no
            // un-accounted eviction).
            cache.grow(count_cap);

            assert_eq!(cache.len(), len_before, "equal-cap grow must not evict");
            assert_eq!(
                cache.total_bytes(),
                bytes_before,
                "equal-cap grow must not change the byte total"
            );
            assert_eq!(
                cache.cap().get(),
                cap_before,
                "equal-cap grow must not change the count cap"
            );
        }
    }

    /// The default per-executor byte budgets stay within their documented clamps,
    /// and the worst-case aggregate across the max pool size (16 workers) plus the
    /// shared module-cache ceiling stays a safe fraction of a production gateway's
    /// memory (the 7600 MiB = 7.42 GiB gateway limit the review flagged).
    #[test]
    fn cache_byte_budgets_are_aggregate_safe() {
        let summary = summary_cache_budget_bytes();
        let delta = delta_cache_budget_bytes();
        assert!(
            (SUMMARY_CACHE_MIN_BYTES..=SUMMARY_CACHE_MAX_BYTES).contains(&summary),
            "summary budget {summary} must be within [{SUMMARY_CACHE_MIN_BYTES}, {SUMMARY_CACHE_MAX_BYTES}]"
        );
        assert!(
            (DELTA_CACHE_MIN_BYTES..=DELTA_CACHE_MAX_BYTES).contains(&delta),
            "delta budget {delta} must be within [{DELTA_CACHE_MIN_BYTES}, {DELTA_CACHE_MAX_BYTES}]"
        );

        // Worst case: every one of the (up to 16) pool workers at the MAX budget.
        const MAX_POOL_SIZE: usize = 16;
        let summary_aggregate = MAX_POOL_SIZE * SUMMARY_CACHE_MAX_BYTES; // <= 512 MiB
        let delta_aggregate = MAX_POOL_SIZE * DELTA_CACHE_MAX_BYTES; // <= 1 GiB
        // Reference gateway RAM (7600 MiB ≈ 7.42 GiB; 7_600 * 1024 * 1024 is
        // 7600 MiB, not 7.6 GiB).
        let gateway_limit: usize = 7_600 * 1024 * 1024;
        // Shared module cache ceiling (single, not per-worker), sized to the RAM
        // this gateway actually has — NOT the absolute MAX clamp. The 4 GiB MAX
        // only binds on hosts with >32 GiB RAM; on a 7.6 GiB gateway the
        // `total_ram / 8` divisor binds at ~950 MiB, so using the MAX here would
        // model an impossible 4 GiB module cache on a 7.6 GiB box. (MAX-clamp
        // aggregate safety on a >32 GiB host is guarded separately by
        // `module_cache::tests::max_clamp_combined_ceiling_is_safe_at_binding_host`.)
        let module_ceiling = crate::wasm_runtime::budget_for_ram(gateway_limit);
        let total = summary_aggregate + delta_aggregate + module_ceiling;
        // Keep total cache commitment under half the gateway RAM.
        assert!(
            total <= gateway_limit / 2,
            "worst-case cache aggregate {total} (summary {summary_aggregate} + delta \
             {delta_aggregate} + module {module_ceiling}) must stay under half the \
             gateway limit {gateway_limit}"
        );
    }

    /// Boundary coverage for [`summary_cache_count_target`], the clamp + round-up
    /// math that sizes the summary/delta caches from the live hosted count. Covers
    /// the edges testing.md requires (zero, one, the clamp bounds, a mid value that
    /// rounds up, and overflow) that the integration test (fixed 1024/3000/5000
    /// hosted counts) never reaches.
    #[test]
    fn summary_cache_count_target_boundaries() {
        // Zero / one (cold start): margin (256) < MIN (1024), so both clamp up to MIN.
        assert_eq!(summary_cache_count_target(0), SUMMARY_CACHE_COUNT_MIN);
        assert_eq!(summary_cache_count_target(1), SUMMARY_CACHE_COUNT_MIN);

        // Largest hosted count whose +margin still lands exactly at MIN stays at MIN;
        // one more rounds up to the next power of two. Margin is added BEFORE the
        // clamp, so feeding MIN itself does NOT yield MIN (asserted just below).
        assert_eq!(
            summary_cache_count_target(SUMMARY_CACHE_COUNT_MIN - SUMMARY_CACHE_COUNT_MARGIN),
            SUMMARY_CACHE_COUNT_MIN
        ); // 768 + 256 == 1024 == MIN, already a power of two
        assert_eq!(
            summary_cache_count_target(SUMMARY_CACHE_COUNT_MIN - SUMMARY_CACHE_COUNT_MARGIN + 1),
            2048
        ); // 769 + 256 == 1025 -> next_power_of_two == 2048

        // Feeding MIN itself: 1024 + 256 == 1280 -> next_power_of_two == 2048.
        assert_eq!(summary_cache_count_target(SUMMARY_CACHE_COUNT_MIN), 2048);

        // A mid value rounds up to the enclosing power of two:
        // 2600 + 256 == 2856, and next_power_of_two(2856) == 4096.
        assert_eq!(summary_cache_count_target(2600), 4096);

        // At and above MAX everything clamps to MAX; usize::MAX must not overflow or
        // panic (saturating_add plus clamp-before-next_power_of_two guarantee this).
        assert_eq!(
            summary_cache_count_target(SUMMARY_CACHE_COUNT_MAX),
            SUMMARY_CACHE_COUNT_MAX
        );
        assert_eq!(
            summary_cache_count_target(SUMMARY_CACHE_COUNT_MAX + 10_000),
            SUMMARY_CACHE_COUNT_MAX
        );
        assert_eq!(
            summary_cache_count_target(usize::MAX),
            SUMMARY_CACHE_COUNT_MAX
        );
    }

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

        // ── is_wasm_timeout predicate (#4861) ─────────────────────────────
        //
        // Selects the longer Timeout-class merge-failure backoff. Must match
        // ONLY the compute-time-exceeded case, distinct from OOG / invalid
        // update / queue-full, all of which flow through similar wrappers.

        #[test]
        fn test_wasm_timeout_true_for_max_compute_time_update_error() {
            let key = test_fixtures::make_contract_key();
            // Exact representation the UPDATE merge path produces on a timeout
            // (both wall-clock and epoch-deadline converge here).
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "The operation exceeded the maximum allowed compute time",
            ));
            assert!(
                err.is_wasm_timeout(),
                "max-compute-time update error MUST be recognized as a wasm timeout"
            );
            // A timeout keeps its exec-rejection classification (contract IS
            // present ⇒ auto-fetch must stay suppressed) ...
            assert!(
                err.is_contract_exec_rejection(),
                "a timeout is a contract-exec rejection (auto-fetch stays gated)"
            );
            // ... and is NOT a benign invalid-update rejection.
            assert!(!err.is_invalid_update_rejection());
        }

        #[test]
        fn test_wasm_timeout_via_execution_conversion_on_upsert() {
            use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
            let key = test_fixtures::make_contract_key();
            let contract_err: ContractError =
                RuntimeInnerError::ContractExecError(ContractExecError::MaxComputeTimeExceeded)
                    .into();
            // The UPDATE merge path constructs the ExecutorError with
            // op = Some(Upsert(key)), routing through update_exec_error.
            let err = ExecutorError::execution(
                contract_err,
                Some(super::super::InnerOpError::Upsert(key)),
            );
            assert!(
                err.is_wasm_timeout(),
                "MaxComputeTimeExceeded on the Upsert path MUST classify as a wasm timeout"
            );
        }

        #[test]
        fn test_wasm_timeout_false_for_out_of_gas() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation.",
            ));
            assert!(
                !err.is_wasm_timeout(),
                "out-of-gas MUST NOT be classified as a wasm timeout"
            );
        }

        #[test]
        fn test_wasm_timeout_false_for_invalid_update() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "invalid contract update, reason: stale",
            ));
            assert!(
                !err.is_wasm_timeout(),
                "benign invalid-update rejection MUST NOT be a wasm timeout"
            );
        }

        #[test]
        fn test_wasm_timeout_false_for_queue_full() {
            let err = ExecutorError::other(ContractQueueFull);
            assert!(
                !err.is_wasm_timeout(),
                "queue-full backpressure MUST NOT be a wasm timeout"
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

        // ── is_missing_contract_parameters over BOTH variants (issue #3279) ──
        //
        // The predicate MUST match "missing contract parameters" whether it
        // arrives as a `ContractError::Update` (delta / update-only path) or a
        // `ContractError::Put` (full-state upsert path in executor_impl.rs). A
        // cross-node non-delta UPDATE surfaces as `Put`; matching only `Update`
        // silently suppressed the auto-fetch recovery for that case — the exact
        // #3279 regression.

        #[test]
        fn test_missing_contract_parameters_true_for_update_variant() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Update {
                key,
                cause: "missing contract parameters".into(),
            });
            assert!(
                err.is_missing_contract_parameters(),
                "Update-variant missing-params MUST be recognized"
            );
        }

        #[test]
        fn test_missing_contract_parameters_true_for_put_variant() {
            // Regression guard for issue #3279: the full-state upsert path
            // (executor_impl.rs) raises the `Put` variant. Before the fix the
            // predicate only matched `Update`, so this returned false and the
            // full-state cross-node UPDATE stayed permanently stuck.
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Put {
                key,
                cause: "missing contract parameters".into(),
            });
            assert!(
                err.is_missing_contract_parameters(),
                "Put-variant missing-params MUST be recognized (issue #3279); \
                 the full-state cross-node UPDATE path raises this variant and \
                 the auto-fetch recovery gates on this predicate"
            );
        }

        #[test]
        fn test_missing_contract_parameters_false_for_other_put_cause() {
            // A Put failure with a DIFFERENT cause must NOT trip the predicate:
            // broadening to the Put variant must stay scoped to the exact
            // missing-params cause, or unrelated PUT failures would spuriously
            // trigger auto-fetch storms.
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Put {
                key,
                cause: "state size 999 bytes exceeds maximum allowed".into(),
            });
            assert!(
                !err.is_missing_contract_parameters(),
                "Only the missing-params cause may match, not every Put error"
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
