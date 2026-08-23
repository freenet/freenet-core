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
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::storages::Storage;
use crate::config::Config;
use crate::node::OpManager;
use crate::operations::get::GetResult;
use crate::util::byte_bounded_lru::ByteBoundedLruCache;
use crate::wasm_runtime::{
    ContractRuntimeInterface, ContractStore, DelegateRuntimeInterface, DelegateStore, Runtime,
    SecretsStore, SharedStores, StateStorage, StateStore, StateStoreError, UserSecretContext,
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
///
/// This is a hard-coded network-wide constant, not a per-node config option, and that is
/// deliberate: a configurable cap would mean a dApp works on some peers and not others,
/// which is exactly the non-uniformity Freenet must avoid (every node must enforce the
/// same limit so client behavior is predictable network-wide). Do not make this
/// configurable — that has been proposed and explicitly rejected (Ian, 2026-08-22).
///
/// Raised from 50 to 500 (2026-08-22): 50 was hit almost immediately by apps that
/// subscribe to one contract per discoverable peer/user (e.g. Freebird's discovery
/// pattern), and the cap was trivially bypassable by opening a second websocket
/// connection (each connection mints a fresh `ClientId` with its own budget), so it
/// penalized well-behaved clients while stopping no determined abuser.
///
/// This constant does not bound per-subscription memory, so raising it is safe by the
/// same argument at any value: each subscription's notification channel
/// (`SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE`) is bounded by message COUNT, not bytes, and
/// is drained lossily (`try_send`, dropped when full) rather than growing unbounded. A
/// queued message can itself carry a full contract-state clone up to `MAX_STATE_SIZE`
/// (see `wasm_runtime::state_store`), so the per-subscription worst case is already
/// governed by that channel depth and state-size cap, not by this constant — raising
/// this value only scales an exposure that exists independently of it. See the PR that
/// raised this constant to 500 for the full numeric worked example.
///
/// Note: the tokio mpsc channel behind each subscription eagerly allocates its first
/// block (32 slots by default) at creation and allocates further blocks on demand — it
/// is not fully preallocated to capacity, but an idle subscription is not literally
/// zero-cost either. The order-of-magnitude conclusion (idle cost is negligible, on the
/// order of a few hundred bytes per subscription) still holds.
pub(crate) const MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 500;

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
    /// Typed provenance for a HOST-originated execution timeout (#4864 round-9).
    /// Set ONLY by [`ExecutorError::execution`] when it sees a
    /// `ContractExecError::{MaxComputeTimeExceeded, SchedulerOverloaded}` — the
    /// two variants the host's `classify_result` (wasm_runtime/contract.rs) alone
    /// constructs from a `WasmError`, NEVER a contract via its own return text.
    ///
    /// This is the security-critical fix: `is_wasm_timeout` / `is_scheduler_timeout`
    /// gate on THIS field, not on the `Update{cause}` substring. `update_exec_error`
    /// flattens the typed variant into a cause string ("… maximum allowed compute
    /// time …"), and a malicious `update_state`/`validate_state` can RETURN a
    /// rejection whose text contains that same phrase — which would otherwise be
    /// misclassified as a real host timeout and earn the contract-wide, trip-at-one
    /// Timeout quarantine, suppressing honest peers for up to 2h. The typed field
    /// is unforgeable through contract return text.
    host_timeout: Option<HostTimeoutClass>,
}

/// Provenance class for a host-originated execution timeout (#4864 round-9). See
/// [`ExecutorError::host_timeout`]. Constructed only on the `classify_result` →
/// `ContractExecError` → `ExecutorError::execution` path, so a contract cannot
/// forge either class by returning text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostTimeoutClass {
    /// The guest RAN and exceeded its compute deadline (`WasmError::Timeout` →
    /// `ContractExecError::MaxComputeTimeExceeded`). A real, contract-intrinsic
    /// timeout — earns the Timeout-class merge-failure backoff.
    ComputeExceeded,
    /// The guest NEVER ran — it sat queued on a saturated blocking pool past the
    /// deadline (`WasmError::SchedulerOverloaded`). Transient load, not a contract
    /// fault — EXCLUDED from the backoff.
    SchedulerOverloaded,
}

enum InnerOpError {
    /// UPDATE / re-PUT merge: a contract-exec failure surfaces as
    /// `StdContractError::Update{cause}` (so `is_contract_exec_rejection` matches
    /// and the client sees an UpdateResponse-shaped error).
    Upsert(ContractKey),
    /// Fresh PUT (#4864 round-9 item 4): a contract-exec failure surfaces as
    /// `StdContractError::Put{cause}` so a fresh PUT's validation error keeps
    /// PutResponse semantics instead of masquerading as an UPDATE error.
    Put(ContractKey),
    Delegate(DelegateKey),
}

/// Which op a shared validation helper is running for (#4864 round-9 item 4), so
/// it can classify a `validate_state` exec failure as the RIGHT client error:
/// `Update` for the UPDATE / re-PUT callers (keeps the merge-backoff
/// classification), `Put` for the fresh-PUT `verify_and_store_contract` path.
#[derive(Clone, Copy)]
pub(crate) enum ValidationOpKind {
    Update,
    Put,
}

impl ValidationOpKind {
    fn op_for(self, key: ContractKey) -> InnerOpError {
        match self {
            ValidationOpKind::Update => InnerOpError::Upsert(key),
            ValidationOpKind::Put => InnerOpError::Put(key),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl ExecutorError {
    pub fn other(error: impl Into<anyhow::Error>) -> Self {
        Self {
            inner: Either::Right(error.into()),
            fatal: false,
            host_timeout: None,
        }
    }

    /// Call this when an unreachable path is reached but need to avoid panics.
    fn internal_error() -> Self {
        Self {
            inner: Either::Right(anyhow::anyhow!("internal error")),
            fatal: false,
            host_timeout: None,
        }
    }

    fn request(error: impl Into<RequestError>) -> Self {
        Self {
            inner: Either::Left(Box::new(error.into())),
            fatal: false,
            host_timeout: None,
        }
    }

    fn execution(
        outer_error: crate::wasm_runtime::ContractError,
        op: Option<InnerOpError>,
    ) -> Self {
        use crate::wasm_runtime::{ContractExecError, RuntimeInnerError};
        // TYPED PROVENANCE (#4864 round-9): capture the host timeout class from
        // the TYPED `ContractExecError` variant BEFORE `update_exec_error` flattens
        // it into a contract-forgeable string cause. These two variants originate
        // ONLY from the host `classify_result` (wasm_runtime/contract.rs), so a
        // contract cannot set this field by returning text. Op-INDEPENDENT: a
        // timeout is a host timeout whether or not the op maps it to `Update`.
        // `matches!` (not a wildcard `match`) so a new ContractExecError variant
        // stays a compile-clean "not a host timeout" without a catch-all arm.
        let host_timeout = if matches!(
            outer_error.deref(),
            RuntimeInnerError::ContractExecError(ContractExecError::MaxComputeTimeExceeded)
        ) {
            Some(HostTimeoutClass::ComputeExceeded)
        } else if matches!(
            outer_error.deref(),
            RuntimeInnerError::ContractExecError(ContractExecError::SchedulerOverloaded)
        ) {
            Some(HostTimeoutClass::SchedulerOverloaded)
        } else {
            None
        };
        let mut err = Self::execution_classified(outer_error, op);
        err.host_timeout = host_timeout;
        err
    }

    /// The op-based routing half of [`ExecutorError::execution`] (produces the
    /// `Update{cause}` request error / delegate error / `other`). The typed
    /// `host_timeout` provenance is stamped by the `execution` wrapper above; keep
    /// them separate so the string cause never becomes the timeout signal.
    fn execution_classified(
        outer_error: crate::wasm_runtime::ContractError,
        op: Option<InnerOpError>,
    ) -> Self {
        use crate::wasm_runtime::RuntimeInnerError;
        let error = outer_error.deref();

        if let RuntimeInnerError::ContractExecError(e) = error {
            match &op {
                Some(InnerOpError::Upsert(key)) => {
                    return ExecutorError::request(StdContractError::update_exec_error(*key, e));
                }
                // #4864 round-9 item 4: fresh PUT → Put{cause}, same
                // "execution error:" prefix as update_exec_error so log-severity /
                // is_wasm_timeout provenance behave identically, but a Put variant
                // for PutResponse semantics (is_contract_exec_rejection matches only
                // Update, so a fresh-PUT exec failure correctly does NOT look like
                // an UPDATE-side auto-fetchable rejection).
                Some(InnerOpError::Put(key)) => {
                    return ExecutorError::request(StdContractError::Put {
                        key: *key,
                        cause: format!("execution error: {e}").into(),
                    });
                }
                _ => {}
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
                // #4864 round-9 item 4: fresh PUT → Put{cause} (see the
                // ContractExecError branch above for rationale).
                Some(InnerOpError::Put(key)) => {
                    return ExecutorError::request(StdContractError::Put {
                        key,
                        cause: format!("execution error: {e}").into(),
                    });
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

    /// Returns true ONLY when the contract's WASM merge/validate ran and exceeded
    /// the execution time limit — the #4861 poison-contract case where every apply
    /// runs past `max_execution_seconds`. Both the wall-clock timeout and the
    /// epoch-deadline interrupt (#4861) converge on `WasmError::Timeout` →
    /// `ContractExecError::MaxComputeTimeExceeded` in the host `classify_result`.
    ///
    /// Used by the per-contract merge-failure backoff (#4861) to select the
    /// longer *Timeout*-class cooldown: a runaway merge is far more expensive to
    /// re-attempt than a cheap `InvalidUpdate` rejection, so a contract that
    /// times out is quarantined harder (contract-wide, trip-at-ONE) than one that
    /// merely rejects a stale delta.
    ///
    /// TYPED PROVENANCE (#4864 round-9): gated on the unforgeable
    /// [`ExecutorError::host_timeout`] field, NOT on the `Update{cause}` substring.
    /// The earlier substring form (`cause.contains("maximum allowed compute time")`)
    /// was contract-controllable: a malicious `update_state`/`validate_state` can
    /// RETURN a rejection whose text contains that phrase, and `update_exec_error`
    /// embeds it into the same cause — so the substring form would have let a
    /// contract forge the harsh Timeout-class quarantine and suppress honest peers
    /// for up to 2h. The typed field is set only by the host classification path.
    /// Op-independent (a timeout is a host timeout even when the op leaves it on
    /// the `other`/`Either::Right` path); `is_contract_exec_rejection` still keys
    /// off the `Update{cause}` string for the (harmless-if-forged) auto-fetch gate.
    pub fn is_wasm_timeout(&self) -> bool {
        self.host_timeout == Some(HostTimeoutClass::ComputeExceeded)
    }

    /// Returns true ONLY when the contract's WASM merge/validate call never ran
    /// because it sat QUEUED on a saturated blocking pool past the wall-clock
    /// deadline — the #4864 round-6 scheduler-overload case, where the guest
    /// never entered execution (distinct from a runaway guest that DID run and
    /// blew the deadline, which is `is_wasm_timeout`). The engine classifies it as
    /// `WasmError::SchedulerOverloaded` → `ContractExecError::SchedulerOverloaded`
    /// in the host `classify_result`.
    ///
    /// Used by the per-contract merge-failure backoff record site to EXCLUDE a
    /// scheduler timeout (exactly like `is_contract_queue_full`): the guest never
    /// executed, so the failure is transient load, not a contract fault, and
    /// quarantining the contract for a delta it never applied would be wrong.
    /// Deliberately DISJOINT from `is_wasm_timeout`.
    ///
    /// TYPED PROVENANCE (#4864 round-9): like `is_wasm_timeout`, gated on the
    /// unforgeable [`ExecutorError::host_timeout`] field rather than the
    /// contract-controllable cause substring, so a contract cannot forge the
    /// scheduler class either.
    pub fn is_scheduler_timeout(&self) -> bool {
        self.host_timeout == Some(HostTimeoutClass::SchedulerOverloaded)
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
            host_timeout: None,
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
                    host_timeout: self.host_timeout,
                }),
            },
            inner @ Either::Left(_) => Err(Self {
                inner,
                fatal: self.fatal,
                host_timeout: self.host_timeout,
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

    /// Test-only faithful constructor for a HOST compute-time timeout (#4864
    /// round-9): mirrors the production path
    /// (`classify_result` → `ContractExecError::MaxComputeTimeExceeded` →
    /// `execution`), so the typed `host_timeout` provenance is set exactly as in
    /// production. Lets tests in OTHER modules build a real timeout that
    /// `is_wasm_timeout()` recognizes — a plain `update_exec_error(..)` string
    /// does NOT (that is precisely the contract-forge case the typing rejects).
    #[cfg(test)]
    pub(crate) fn test_host_compute_timeout(key: ContractKey) -> Self {
        use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
        let ce: ContractError =
            RuntimeInnerError::ContractExecError(ContractExecError::MaxComputeTimeExceeded).into();
        Self::execution(ce, Some(InnerOpError::Upsert(key)))
    }

    /// Test-only faithful constructor for a HOST scheduler-overload timeout
    /// (#4864 round-9). See [`ExecutorError::test_host_compute_timeout`].
    #[cfg(test)]
    pub(crate) fn test_host_scheduler_timeout(key: ContractKey) -> Self {
        use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
        let ce: ContractError =
            RuntimeInnerError::ContractExecError(ContractExecError::SchedulerOverloaded).into();
        Self::execution(ce, Some(InnerOpError::Upsert(key)))
    }
}

impl From<RequestError> for ExecutorError {
    fn from(value: RequestError) -> Self {
        Self {
            inner: Either::Left(Box::new(value)),
            fatal: false,
            host_timeout: None,
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
            host_timeout: None,
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
    /// `connection_scope` says whether the client connection this request
    /// descends from is entitled to an ATTESTED application identity
    /// (GHSA-824h-7x5x-wfmf). When it is
    /// [`crate::client_events::ConnectionScope::Remote`] the executor resolves
    /// NO `MessageOrigin` at all — not from `origin_contract`, not from
    /// `caller_delegate`, not from the node's inherited-origins map — so an
    /// off-host caller sees exactly what a tokenless local caller has always
    /// seen. Like `user_context` it travels beside the request, never inside
    /// it. Node-internal invocations (contract-notification callbacks) pass
    /// `Local`: they descend from no client connection at all.
    fn execute_delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
        connection_scope: crate::client_events::ConnectionScope,
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
//
// Both byte budgets are PER EXECUTOR, and the pool size is derived from CPU count
// — which `MemoryMax` does not constrain. So the RAM-scaled clamps alone were not
// a bound on what the node commits: a 20-core laptop inside the shipped 2 GiB
// cgroup got 16 workers × (32 MiB summary + 64 MiB delta) = 1.5 GiB of declared
// ceiling out of a 2 GiB limit, and no code anywhere composed the two (#5268
// defect 3). Each budget is therefore additionally capped by its share of
// `per_executor_cache_envelope_bytes`, the node-wide envelope divided by the pool
// size. That cap binds only on memory-constrained many-core hosts; a single-worker
// peer and a large gateway keep exactly the budgets they had.
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

/// Fraction of "memory the node may use" that sizes the per-executor SUMMARY
/// cache byte budget. Summaries are small digests, so a modest share holds far
/// more small entries than any realistic hosted count while capping worst-case
/// bytes. See `summary_cache_budget_bytes`.
const SUMMARY_CACHE_RAM_DIVISOR: usize = 64;

/// Lower clamp for the summary-cache byte budget (16 MiB). At the ~512 B per-entry
/// floor this holds ~32k small summaries — far above any realistic hosted count on
/// a small node — so the count target (coverage) binds, never this floor.
const SUMMARY_CACHE_MIN_BYTES: usize = 16 * 1024 * 1024;

/// Upper clamp for the summary-cache byte budget (32 MiB), which binds on a host
/// with room for it: an unconstrained gateway, or any node whose pool is small
/// enough that the envelope share is wider. At the ~512 B floor 32 MiB holds
/// ~65k small summaries (≈ the count MAX), so coverage holds at the count cap for
/// small digests; a large-summary contract instead evicts down to what fits.
///
/// This is a CEILING, not the resolved budget. On a memory-constrained many-core
/// host `summary_budget_for` composes it down to a share of the node-wide
/// envelope (#5268 defect 3): a 2 GiB peer with 16 workers resolves to ~5 MiB,
/// which at the entry floor still holds ~10k small summaries. `pool_size ×
/// (summary + delta)` is what the node actually commits, and that product is
/// what `cache_byte_budgets_are_aggregate_safe` bounds.
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
/// ceiling.
///
/// Like [`SUMMARY_CACHE_MAX_BYTES`] this is a CEILING, not the resolved budget:
/// it binds where the node has room for it, and `delta_budget_for` composes it
/// down to a share of the node-wide envelope otherwise (#5268 defect 3). The
/// budget is PER-EXECUTOR and `pool_size` tracks CPU COUNT, which `MemoryMax`
/// does not constrain — multiplying a RAM-scaled per-executor budget by cores is
/// exactly how a 2 GiB peer ended up declaring 1.5 GiB of summary+delta ceiling.
/// `cache_byte_budgets_are_aggregate_safe` bounds the product, together with the
/// module caches, the redb page cache, the Store arenas and the source-byte
/// caches, on both a 2 GiB peer and a production gateway.
const DELTA_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Per-executor SUMMARY-cache byte budget, scaled to the memory the node may use
/// (host RAM, or a smaller cgroup limit when containerized) and clamped to a sane
/// floor/ceiling: `clamp(total_ram / SUMMARY_CACHE_RAM_DIVISOR,
/// SUMMARY_CACHE_MIN_BYTES, SUMMARY_CACHE_MAX_BYTES)`. Reuses the module cache's
/// `read_total_ram_bytes()` so there is a single "memory the node may use" source.
pub(crate) fn summary_cache_budget_bytes() -> usize {
    summary_budget_for(live_total_ram_bytes(), live_pool_size())
}

/// Pure sizing math behind [`summary_cache_budget_bytes`], split out so
/// aggregate-commitment tests can ask what a hypothetical host would get instead
/// of depending on the test machine's own RAM and core count.
pub(crate) fn summary_budget_for(total_ram: usize, pool_size: usize) -> usize {
    let ram_scaled = (total_ram / SUMMARY_CACHE_RAM_DIVISOR)
        .clamp(SUMMARY_CACHE_MIN_BYTES, SUMMARY_CACHE_MAX_BYTES);
    compose_against_envelope(
        ram_scaled,
        SUMMARY_CACHE_ENVELOPE_SHARE,
        total_ram,
        pool_size,
    )
}

fn live_total_ram_bytes() -> usize {
    crate::wasm_runtime::read_total_ram_bytes().unwrap_or(SUMMARY_CACHE_FALLBACK_TOTAL_RAM_BYTES)
}

/// The pool size the node will actually create, from
/// [`crate::config::runtime_pool_size`] — the same function `RuntimePool::new`
/// sizes the pool with, so the divisor here can never drift from the multiplier.
fn live_pool_size() -> usize {
    crate::config::runtime_pool_size().into()
}

/// Cap a RAM-scaled per-executor budget at `share` of this executor's slice of
/// the node-wide cache envelope, never dropping below
/// [`CACHE_ABSOLUTE_FLOOR_BYTES`].
fn compose_against_envelope(
    ram_scaled: usize,
    share: (usize, usize),
    total_ram: usize,
    pool_size: usize,
) -> usize {
    let (numerator, denominator) = share;
    let per_executor_envelope =
        total_ram / SUMMARY_DELTA_ENVELOPE_RAM_DIVISOR / pool_size.max(1) / denominator * numerator;
    ram_scaled
        .min(per_executor_envelope)
        .max(CACHE_ABSOLUTE_FLOOR_BYTES)
}

/// Per-executor DELTA-cache byte budget, derived the same way as
/// `summary_cache_budget_bytes` but with the delta divisor/clamps (deltas are
/// larger and higher-cardinality): `clamp(total_ram / DELTA_CACHE_RAM_DIVISOR,
/// DELTA_CACHE_MIN_BYTES, DELTA_CACHE_MAX_BYTES)`.
pub(crate) fn delta_cache_budget_bytes() -> usize {
    delta_budget_for(live_total_ram_bytes(), live_pool_size())
}

/// Pure sizing math behind [`delta_cache_budget_bytes`]; see
/// [`summary_budget_for`].
pub(crate) fn delta_budget_for(total_ram: usize, pool_size: usize) -> usize {
    let ram_scaled =
        (total_ram / DELTA_CACHE_RAM_DIVISOR).clamp(DELTA_CACHE_MIN_BYTES, DELTA_CACHE_MAX_BYTES);
    compose_against_envelope(ram_scaled, DELTA_CACHE_ENVELOPE_SHARE, total_ram, pool_size)
}

/// Sum of every cache ceiling a node with `memory_limit` bytes and `pool_size`
/// workers declares: the per-executor summary, delta, and Store-arena caches
/// times the pool, plus the single shared contract and delegate module caches,
/// the shared source-WASM byte caches, and the redb page cache.
///
/// Module ceilings are sized to the RAM the host actually has, NOT the
/// absolute MAX clamp: the 4 GiB module-cache MAX only binds above 32 GiB of
/// RAM, so using it would model an impossible cache on a smaller box. (MAX-clamp
/// safety on a >32 GiB host is guarded separately by
/// `module_cache::tests::max_clamp_combined_ceiling_is_safe_at_binding_host`.)
///
/// Promoted from a `#[cfg(test)]`-only helper (originally written purely to
/// verify [`cache_byte_budgets_are_aggregate_safe`] below) to a real
/// production function (#5333 review): the resident-overhead hosting budget
/// (`ring::hosting::cache::resident_overhead_budget_for`) needs the SAME
/// real figure — what every OTHER memory consumer has already declared — to
/// derive its own budget as a residual rather than an independently-clamped
/// guess. Using this one function in both places means the aggregate-safety
/// test now checks the ACTUAL formula the resident-overhead budget composes
/// against, not a second, potentially-drifting re-derivation of it.
pub(crate) fn declared_cache_ceiling(memory_limit: usize, pool_size: usize) -> usize {
    // PER-EXECUTOR — multiplied by the pool.
    let summary = summary_budget_for(memory_limit, pool_size);
    let delta = delta_budget_for(memory_limit, pool_size);
    // One wasmtime Store per executor, each holding retired-instance bytes up
    // to its arena budget between refreshes.
    let arena = crate::wasm_runtime::engine::store_arena_budget_for(memory_limit, pool_size);

    // NODE-WIDE — one of each, shared by every executor.
    let contract_modules = crate::wasm_runtime::budget_for_ram(memory_limit);
    let delegate_modules =
        contract_modules / crate::wasm_runtime::DELEGATE_MODULE_CACHE_BUDGET_DIVISOR;
    // The two source-WASM byte caches. Node-wide because #5268 made them
    // shared; each executor used to build its own, so this term was
    // `pool_size × 2 × 10 MiB` and counted nowhere.
    let source_code = 2 * SOURCE_CODE_CACHE_MAX_BYTES as usize;
    #[cfg(feature = "redb")]
    let page_cache = crate::contract::storages::redb::page_cache_size_for(memory_limit);
    #[cfg(not(feature = "redb"))]
    let page_cache = 0;

    pool_size * (summary + delta + arena)
        + contract_modules
        + delegate_modules
        + source_code
        + page_cache
}

/// Fallback total-RAM estimate (1 GiB) when the OS query fails — mirrors the
/// module cache's `FALLBACK_TOTAL_RAM_BYTES`, yielding a mid-range budget.
const SUMMARY_CACHE_FALLBACK_TOTAL_RAM_BYTES: usize = 1024 * 1024 * 1024;

/// Fraction of the memory the node may use that ALL summary and delta caches
/// together may declare. An eighth leaves the other seven eighths for the module
/// cache (also an eighth), the redb page cache, the state store, transport
/// buffers, the WASM instance arenas and the base runtime — the aggregate that
/// `cache_byte_budgets_are_aggregate_safe` checks.
const SUMMARY_DELTA_ENVELOPE_RAM_DIVISOR: usize = 8;

/// Summary's share of a worker's envelope slice, as `(numerator, denominator)`.
/// A third, matching the 32:64 MiB ratio of the two caches' ceilings — summaries
/// are small digests, deltas are larger and higher-cardinality.
const SUMMARY_CACHE_ENVELOPE_SHARE: (usize, usize) = (1, 3);

/// Delta's share of a worker's envelope slice — the remaining two thirds.
const DELTA_CACHE_ENVELOPE_SHARE: (usize, usize) = (2, 3);

/// Byte ceiling for each of the two source-WASM caches (`ContractStore`'s and
/// `DelegateStore`'s), which memoize the raw `.wasm` bytes read off disk.
///
/// Node-wide, not per-executor: pool executors share one cache each (see
/// [`SharedStores`]). Before #5268 each executor built its own, so the node's
/// real commitment was `pool_size × 10 MiB` per cache — 320 MiB across both at
/// 16 workers, holding up to 16 copies of the same bytes, and counted by no
/// budget anywhere.
pub(crate) const SOURCE_CODE_CACHE_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// Hard floor for either cache's byte budget once the envelope has been applied.
///
/// At the `CACHE_ENTRY_OVERHEAD_BYTES` (512 B) per-entry floor this still holds
/// ~2k small entries, above the `SUMMARY_CACHE_COUNT_MIN` count target, so even
/// the most constrained node keeps a cache that covers its working set of small
/// digests. It exists so an extreme memory limit cannot compose the budget down
/// to something that caches nothing at all.
const CACHE_ABSOLUTE_FLOOR_BYTES: usize = 1024 * 1024;

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
        shared: Option<SharedStores>,
    ) -> Result<(ContractStore, DelegateStore, SecretsStore), anyhow::Error> {
        // Tell the conformance machinery where contract WASM lives. Shadow-mode
        // probing replays captured samples against the real contract, and the only
        // component that knows the path is the one building the store. Idempotent and
        // free when capture is off: nothing reads it unless a probe runs.
        crate::conformance::capture::set_contract_store(config.contracts_dir());

        let (contract_store, delegate_store) = match shared {
            // Pool executors: adopt the node's shared indexes AND byte caches, so
            // one contract/delegate registered anywhere is visible everywhere and
            // the node holds ONE copy of each WASM blob rather than `pool_size`
            // copies (#4218 for the index, #5268 for the byte cache).
            Some(shared) => (
                ContractStore::new_with_shared(
                    config.contracts_dir(),
                    db.clone(),
                    shared.contract_index,
                    shared.contract_code,
                )?,
                DelegateStore::new_with_shared(
                    config.delegates_dir(),
                    db.clone(),
                    shared.delegate_index,
                    shared.delegate_code,
                )?,
            ),
            // Standalone / mock executors own their state.
            None => (
                ContractStore::new(
                    config.contracts_dir(),
                    SOURCE_CODE_CACHE_MAX_BYTES,
                    db.clone(),
                )?,
                DelegateStore::new(
                    config.delegates_dir(),
                    SOURCE_CODE_CACHE_MAX_BYTES,
                    db.clone(),
                )?,
            ),
        };
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

    /// The per-executor byte budgets stay within their documented clamps, and the
    /// aggregate of EVERY declared cache ceiling stays a safe fraction of the
    /// node's memory — on a production gateway AND on a peer running under the
    /// shipped 2 GiB `MemoryMax`.
    ///
    /// REGRESSION (issue #5268 defect 3): this test previously modelled only a
    /// 7600 MiB gateway and multiplied the per-executor budgets by a hardcoded
    /// worst-case pool size, so it never asked what a 2 GiB peer commits. It
    /// commits ~2.9 GiB: 16 workers (a 20-core laptop, since `MemoryMax` does not
    /// constrain CPU count) × (32 MiB summary + 64 MiB delta) = 1.5 GiB, plus
    /// 256 MiB + 64 MiB of module cache, plus redb's uncounted 1 GiB default page
    /// cache — against a 2 GiB hard limit. Nothing in the code composed the
    /// CPU-derived multiplier against the memory limit.
    #[test]
    fn cache_byte_budgets_are_aggregate_safe() {
        let summary = summary_cache_budget_bytes();
        let delta = delta_cache_budget_bytes();
        assert!(
            (CACHE_ABSOLUTE_FLOOR_BYTES..=SUMMARY_CACHE_MAX_BYTES).contains(&summary),
            "summary budget {summary} must be within \
             [{CACHE_ABSOLUTE_FLOOR_BYTES}, {SUMMARY_CACHE_MAX_BYTES}]"
        );
        assert!(
            (CACHE_ABSOLUTE_FLOOR_BYTES..=DELTA_CACHE_MAX_BYTES).contains(&delta),
            "delta budget {delta} must be within \
             [{CACHE_ABSOLUTE_FLOOR_BYTES}, {DELTA_CACHE_MAX_BYTES}]"
        );

        // Worst case is the MAXIMUM pool size, because every per-executor budget
        // is multiplied by it.
        const MAX_POOL_SIZE: usize = 16;
        // The shipped systemd `MemoryMax=2G`, which 87.5% of peers report.
        let peer_limit: usize = 2 * 1024 * 1024 * 1024;
        // Reference gateway RAM (7600 MiB ≈ 7.42 GiB; 7_600 * 1024 * 1024 is
        // 7600 MiB, not 7.6 GiB).
        let gateway_limit: usize = 7_600 * 1024 * 1024;

        // A 1 GiB VPS is the smallest limit at which the node's own floors still
        // leave room; below that the pre-existing 64 MiB module-cache MINIMUM
        // dominates any budget this PR composes (see the small-host note below).
        let small_vps: usize = 1024 * 1024 * 1024;

        for (label, limit) in [
            ("1 GiB VPS", small_vps),
            ("2 GiB peer", peer_limit),
            ("gateway", gateway_limit),
        ] {
            for pool_size in [1, 4, MAX_POOL_SIZE] {
                let total = declared_cache_ceiling(limit, pool_size);
                assert!(
                    total <= limit / 2,
                    "{label} with {pool_size} workers declares {total} bytes of cache \
                     ceiling, which must stay under half its {limit}-byte limit"
                );
            }
        }
    }

    /// `declared_cache_ceiling` must keep naming EVERY declared ceiling.
    ///
    /// The bound it feeds is an inequality, so dropping a term does not
    /// necessarily break it — at the 2 GiB / 16-worker shape the total sits at
    /// roughly 45% of the limit, with room to lose a 256 MiB term and still pass.
    /// That is exactly how the original version of this test came to omit the
    /// Store arena and the per-executor source-byte caches while its doc comment
    /// claimed to sum "every declared ceiling" (#5268 review, Must Fix 2).
    ///
    /// So pin the composition itself: every budget that consumes node memory has
    /// to appear in the sum, and a new one is added here at the same time it is
    /// added there.
    #[test]
    fn declared_cache_ceiling_names_every_budget() {
        const FULL: &str = include_str!("executor.rs");
        // Built by concatenation so this pin's own copy of the anchor is NOT a
        // verbatim match for it. A scrape whose anchor can match its own source
        // silently re-scopes to a later occurrence and passes vacuously — the
        // failure mode `.claude/rules/bug-prevention-patterns.md` records twice
        // (#5102). The uniqueness assertion below is what makes that fail loudly
        // instead: if the signature ever appears twice, or not at all, this stops.
        let anchor = format!(
            "fn declared_cache_ceiling(memory_limit: usize, {}",
            "pool_size: usize) -> usize {"
        );
        assert_eq!(
            FULL.matches(&anchor).count(),
            1,
            "the scrape anchor must occur EXACTLY once in this file; a second \
             occurrence would let the pin scope itself to the wrong region"
        );
        let after = FULL.split(&anchor).nth(1).expect("anchor just counted");
        // `declared_cache_ceiling` is a top-level (0-indent) function — #5333
        // promoted it out of `mod tests` into production code — so it closes
        // with `\n}` at column 0, not a nested method's `\n    }`. Require it,
        // rather than letting a missing end anchor widen the region to EOF.
        let body = after
            .split_once("\n}")
            .expect("could not locate the end of declared_cache_ceiling")
            .0;
        assert!(
            !body.contains("\npub") && !body.contains("\nfn ") && !body.contains("\nconst "),
            "the scoped region escaped past declared_cache_ceiling into a \
             sibling item — this pin would pass vacuously"
        );

        for required in [
            // per-executor, multiplied by the pool
            "summary_budget_for",
            "delta_budget_for",
            "store_arena_budget_for",
            // node-wide
            "budget_for_ram",
            "DELEGATE_MODULE_CACHE_BUDGET_DIVISOR",
            "SOURCE_CODE_CACHE_MAX_BYTES",
            "page_cache_size_for",
        ] {
            assert!(
                body.contains(required),
                "declared_cache_ceiling must include `{required}` in the aggregate \
                 it claims to sum; without it the bound passes while measuring less \
                 than the node actually commits"
            );
        }
        assert!(
            body.contains("pool_size * (summary + delta + arena)"),
            "the per-executor terms must be multiplied by the pool size — that \
             product IS defect 3"
        );
    }

    /// Below roughly a 1 GiB limit the aggregate is dominated by floors this PR
    /// does not own — chiefly the 64 MiB module-cache MINIMUM, which alone is the
    /// whole budget on a 128 MiB host.
    ///
    /// Asserted rather than omitted so the boundary is a stated property instead
    /// of an untested gap: a future change that lowers those floors should see
    /// this test and extend the matrix above, and one that RAISES a per-worker
    /// floor will trip the multiplier bound here.
    #[test]
    fn small_host_ceiling_is_dominated_by_floors_this_pr_does_not_own() {
        let tiny: usize = 128 * 1024 * 1024;
        let module_floor = crate::wasm_runtime::budget_for_ram(tiny);
        assert!(
            module_floor >= tiny / 2,
            "the module-cache floor ({module_floor}) is already half a {tiny}-byte              host — no composition of the budgets around it can bring the              aggregate under half the limit"
        );

        // What this PR DOES own on such a host stays proportionate: the
        // per-worker terms it introduces or composes must not, multiplied by the
        // pool, exceed the memory limit on their own.
        for pool_size in [1, 4, 16] {
            let per_worker = summary_budget_for(tiny, pool_size)
                + delta_budget_for(tiny, pool_size)
                + crate::wasm_runtime::engine::store_arena_budget_for(tiny, pool_size);
            assert!(
                pool_size * per_worker <= tiny,
                "per-worker terms at {pool_size} workers total {} bytes, which must                  not exceed the {tiny}-byte limit by themselves",
                pool_size * per_worker
            );
        }
    }

    /// The per-executor budgets must shrink as the pool grows, because the pool
    /// size is CPU-derived and the memory limit does not constrain CPU count.
    ///
    /// Pins the actual mechanism rather than only its aggregate consequence: a
    /// future change that restored a flat per-executor budget would still satisfy
    /// `cache_byte_budgets_are_aggregate_safe` if the envelope happened to be
    /// generous, but would fail here.
    #[test]
    fn per_executor_budgets_shrink_as_the_pool_grows() {
        let peer_limit: usize = 2 * 1024 * 1024 * 1024;
        let one = summary_budget_for(peer_limit, 1) + delta_budget_for(peer_limit, 1);
        let sixteen = summary_budget_for(peer_limit, 16) + delta_budget_for(peer_limit, 16);
        assert!(
            sixteen < one,
            "a 16-worker 2 GiB peer must get a smaller per-executor budget ({sixteen}) \
             than a 1-worker one ({one})"
        );

        // An unconstrained host keeps exactly the ceilings it had: the envelope
        // is wide enough there that the RAM-scaled clamps still bind.
        let big: usize = 125 * 1024 * 1024 * 1024;
        assert_eq!(summary_budget_for(big, 16), SUMMARY_CACHE_MAX_BYTES);
        assert_eq!(delta_budget_for(big, 16), DELTA_CACHE_MAX_BYTES);
        // So does a single-worker 2 GiB peer (vega's shape: 2 cores → 1 worker).
        assert_eq!(summary_budget_for(peer_limit, 1), SUMMARY_CACHE_MAX_BYTES);
        assert_eq!(delta_budget_for(peer_limit, 1), DELTA_CACHE_MAX_BYTES);

        // Never composed away to nothing.
        assert!(summary_budget_for(64 * 1024 * 1024, 16) >= CACHE_ABSOLUTE_FLOOR_BYTES);
        assert!(delta_budget_for(64 * 1024 * 1024, 16) >= CACHE_ABSOLUTE_FLOOR_BYTES);
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

        /// #4864 round-9 (Codex P1, the load-bearing fix): a contract CANNOT forge
        /// the Timeout classification by RETURNING a rejection whose cause text
        /// contains "maximum allowed compute time". `is_wasm_timeout` now gates on
        /// the unforgeable host-provenance marker, not the `Update{cause}` string.
        /// Only a HOST-originated timeout (classify_result → MaxComputeTimeExceeded
        /// → execution) is a wasm timeout; a contract-supplied string that happens
        /// to contain the phrase is NOT (else it could earn the contract-wide,
        /// trip-at-one, up-to-2h Timeout quarantine and suppress honest peers).
        #[test]
        fn contract_cannot_forge_wasm_timeout_via_cause_string() {
            let key = test_fixtures::make_contract_key();

            // FORGED: a contract-returned rejection whose text contains the
            // compute-time phrase (this is exactly what a malicious update_state
            // could produce through update_exec_error). MUST NOT be a wasm timeout.
            let forged = ExecutorError::request(StdContractError::update_exec_error(
                key,
                "rejected: your update mentioned the maximum allowed compute time, nice try",
            ));
            assert!(
                !forged.is_wasm_timeout(),
                "a contract-forged cause string MUST NOT classify as a host wasm timeout \
                 (#4864 round-9) — otherwise a contract could self-inflict the harsh \
                 Timeout-class quarantine on honest peers"
            );
            assert!(
                !forged.is_scheduler_timeout(),
                "nor forge the scheduler-timeout class"
            );
            // It is still a contract-exec rejection (the string gate is harmless —
            // it only suppresses auto-fetch, correct for a contract that IS present)
            // and not a benign invalid-update rejection.
            assert!(forged.is_contract_exec_rejection());
            assert!(!forged.is_invalid_update_rejection());

            // HOST-originated: the real timeout the merge path produces. This IS a
            // wasm timeout, and keeps its exec-rejection classification.
            let real = ExecutorError::test_host_compute_timeout(key);
            assert!(
                real.is_wasm_timeout(),
                "a host-originated MaxComputeTimeExceeded MUST classify as a wasm timeout"
            );
            assert!(real.is_contract_exec_rejection());
            assert!(!real.is_invalid_update_rejection());
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

        /// #4864 round-6: a SCHEDULER timeout (the merge closure sat queued on a
        /// saturated blocking pool past the deadline and the guest NEVER ran)
        /// must be a distinct classification from a real compute-time timeout.
        /// It flows `ContractExecError::SchedulerOverloaded` → `update_exec_error`
        /// (op = Some(Upsert(key))), so it carries the "execution error:" prefix
        /// and stays a contract-exec rejection (contract IS present ⇒ no
        /// auto-fetch), yet the op_ctx_task record site excludes it from the
        /// merge-failure backoff via `!err.is_scheduler_timeout()`. Crucially it
        /// is DISJOINT from `is_wasm_timeout`: the guest never executed, so it
        /// must NOT earn the Timeout-class quarantine a runaway merge does.
        #[test]
        fn test_scheduler_timeout_is_distinct_transient_classification() {
            use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
            let key = test_fixtures::make_contract_key();
            let contract_err: ContractError =
                RuntimeInnerError::ContractExecError(ContractExecError::SchedulerOverloaded).into();
            let err = ExecutorError::execution(
                contract_err,
                Some(super::super::InnerOpError::Upsert(key)),
            );
            assert!(
                err.is_scheduler_timeout(),
                "SchedulerOverloaded on the Upsert path MUST classify as a scheduler timeout"
            );
            assert!(
                !err.is_wasm_timeout(),
                "a scheduler timeout MUST be disjoint from a real wasm timeout — the \
                 guest never ran, so it must not pick the Timeout-class quarantine"
            );
            assert!(
                err.is_contract_exec_rejection(),
                "a scheduler timeout IS an exec rejection (contract present ⇒ no auto-fetch); \
                 the backoff record site excludes it via the && !is_scheduler_timeout() gate"
            );

            // The two are disjoint from the OTHER side too: a genuine
            // MaxComputeTimeExceeded (guest ran and blew the deadline) is a real
            // wasm timeout and is NOT a scheduler timeout. Built via the HOST path
            // (#4864 round-9) — a plain update_exec_error string would no longer
            // classify, by design.
            let real_timeout = ExecutorError::test_host_compute_timeout(key);
            assert!(
                real_timeout.is_wasm_timeout(),
                "a real compute-time timeout stays a wasm timeout"
            );
            assert!(
                !real_timeout.is_scheduler_timeout(),
                "a real compute-time timeout MUST NOT be misclassified as a scheduler timeout"
            );
        }

        /// #4864 round-5 (Codex P1): a GUEST-ENTRY epoch interrupt (a runaway
        /// module start function in `create_instance`, or the allocator in
        /// `initiate_buffer`) surfaces as `WasmError::Timeout` at the engine layer
        /// (the engine test `epoch_interrupt_during_instantiation_classifies_as_timeout`
        /// asserts that). The runtime routes it through `classify_result`, which
        /// normalizes it to the SAME `MaxComputeTimeExceeded` the blocking merge
        /// path produces — so the resulting `ExecutorError::is_wasm_timeout()` is
        /// true and the merge-failure backoff picks the contract-wide Timeout
        /// class, not per-sender Invalid. Before the fix the guest-entry
        /// `WasmError::Timeout` went through the generic conversion ("execution
        /// timeout"), which `is_wasm_timeout` does NOT match.
        #[test]
        fn guest_entry_timeout_classifies_as_wasm_timeout() {
            let key = test_fixtures::make_contract_key();
            // The runtime's classify_result normalizes a guest-entry Timeout.
            let contract_err = crate::wasm_runtime::classify_result::<i64>(Err(
                crate::wasm_runtime::engine::WasmError::Timeout,
            ))
            .expect_err("a WasmError::Timeout must classify as an error");
            let err = ExecutorError::execution(
                contract_err,
                Some(super::super::InnerOpError::Upsert(key)),
            );
            assert!(
                err.is_wasm_timeout(),
                "a guest-entry epoch-interrupt timeout, normalized by classify_result, \
                 MUST classify as a wasm timeout (contract-wide Timeout backoff, not Invalid)"
            );
        }

        /// Source-scrape pin (#4864 round-5): EVERY guest-entry engine call in the
        /// runtime (`create_instance`, `initiate_buffer`) must be wrapped by
        /// `classify_result`, so an epoch-interrupt Timeout normalizes to the
        /// Timeout class. A future refactor that adds an unwrapped guest-entry
        /// call would silently reintroduce the misclassification.
        #[test]
        fn guest_entry_calls_route_through_classify_result() {
            let src = include_str!("../wasm_runtime/runtime.rs");
            let create_calls = src.matches("engine.create_instance(").count();
            let create_wrapped = src
                .matches("classify_result(engine.create_instance(")
                .count();
            assert!(
                create_calls > 0 && create_calls == create_wrapped,
                "every engine.create_instance call in runtime.rs must be wrapped in \
                 classify_result ({create_wrapped}/{create_calls} wrapped)"
            );
            let buffer_calls = src.matches("engine.initiate_buffer(").count();
            let buffer_wrapped = src
                .matches("classify_result(self.engine.initiate_buffer(")
                .count();
            assert!(
                buffer_calls > 0 && buffer_calls == buffer_wrapped,
                "every initiate_buffer call in runtime.rs must be wrapped in \
                 classify_result ({buffer_wrapped}/{buffer_calls} wrapped)"
            );
        }

        /// #4864 round-7 (Codex P1): a validation-phase WASM error (a runaway
        /// `validate_state` that blows the deadline, or a queue-saturation
        /// scheduler timeout) MUST classify exactly like a merge-phase error. The
        /// UPDATE-path validation helpers previously wrapped these with `op = None`,
        /// which routes a `WasmError`/`ContractExecError` to `ExecutorError::other`
        /// (`Either::Right`) — so `is_contract_exec_rejection`, `is_wasm_timeout`,
        /// and `is_scheduler_timeout` all returned false and the UPDATE driver
        /// recorded NOTHING, letting a contract whose runaway half is
        /// `validate_state` burn the full budget per broadcast without ever backing
        /// off. With `op = Some(Upsert(key))` (the fix) the SAME error routes
        /// through `update_exec_error` and classifies.
        #[test]
        fn validation_phase_exec_errors_with_upsert_op_classify() {
            use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
            let key = test_fixtures::make_contract_key();
            let mk = |exec: ContractExecError| -> ContractError {
                RuntimeInnerError::ContractExecError(exec).into()
            };

            // FIXED wrapper (op = Some(Upsert(key))): a runaway validate_state that
            // blew the deadline classifies as a real wasm timeout, and stays a
            // contract-exec rejection (contract present ⇒ no auto-fetch).
            let timeout_fixed = ExecutorError::execution(
                mk(ContractExecError::MaxComputeTimeExceeded),
                Some(super::super::InnerOpError::Upsert(key)),
            );
            assert!(
                timeout_fixed.is_wasm_timeout(),
                "validation-phase MaxComputeTimeExceeded on the Upsert path MUST be a wasm timeout"
            );
            assert!(timeout_fixed.is_contract_exec_rejection());

            // A queue-saturation scheduler timeout during validation classifies as
            // the transient scheduler class (excluded from the backoff), NOT a real
            // timeout — same as the merge-phase scheduler timeout.
            let sched_fixed = ExecutorError::execution(
                mk(ContractExecError::SchedulerOverloaded),
                Some(super::super::InnerOpError::Upsert(key)),
            );
            assert!(
                sched_fixed.is_scheduler_timeout(),
                "validation-phase SchedulerOverloaded on the Upsert path MUST be a scheduler timeout"
            );
            assert!(!sched_fixed.is_wasm_timeout());
            assert!(sched_fixed.is_contract_exec_rejection());

            // The bug the round-7 fix closes: with op = None the error stays on the
            // `other`/Either::Right path, so it is NOT a contract-exec rejection →
            // the backoff RECORD gate (is_contract_exec_rejection && !is_scheduler_
            // timeout) skips it. THAT is why the validation sites must pass Upsert.
            let timeout_bug =
                ExecutorError::execution(mk(ContractExecError::MaxComputeTimeExceeded), None);
            assert!(
                !timeout_bug.is_contract_exec_rejection(),
                "op = None leaves the error off the Update path, so the backoff record \
                 gate (is_contract_exec_rejection) skips it — the reason validation \
                 must pass Upsert"
            );
            // Post-#4864-round-9, is_wasm_timeout is HOST-provenance and
            // op-INDEPENDENT, so the typed timeout class is set even for op=None.
            // (That alone does not make the driver record — the exec-rejection gate
            // does, and it needs the Upsert path above.)
            assert!(
                timeout_bug.is_wasm_timeout(),
                "is_wasm_timeout is host-provenance (op-independent) post round-9"
            );
        }

        /// #4864 round-9 item 4: a shared validation helper serves both UPDATE and
        /// fresh-PUT callers. A fresh PUT's validate_state exec failure must surface
        /// as a `Put` variant (PutResponse semantics), NOT an `Update` — while an
        /// UPDATE's stays `Update` and keeps its is_wasm_timeout classification. The
        /// host-timeout provenance is set for BOTH (op-independent, round-9 item 1).
        #[test]
        fn put_op_validation_error_is_put_variant_update_op_is_update() {
            use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
            let key = test_fixtures::make_contract_key();
            let mk = || -> ContractError {
                RuntimeInnerError::ContractExecError(ContractExecError::MaxComputeTimeExceeded)
                    .into()
            };

            // Fresh PUT op → Put variant. NOT a contract-exec rejection (which matches
            // only Update), so a fresh PUT does not look like an UPDATE-side
            // auto-fetchable rejection. Host-timeout provenance still classifies.
            let put_err =
                ExecutorError::execution(mk(), Some(super::super::InnerOpError::Put(key)));
            assert!(
                put_err.is_wasm_timeout(),
                "host-timeout provenance classifies even on the Put op"
            );
            assert!(
                !put_err.is_contract_exec_rejection(),
                "a Put variant is NOT an UPDATE-side exec rejection"
            );
            let put_request_err = put_err.unwrap_request();
            let RequestError::ContractError(StdContractError::Put { .. }) = &put_request_err else {
                panic!("fresh-PUT validation error must be a Put variant, got {put_request_err:?}");
            };

            // UPDATE op → Update variant, is_contract_exec_rejection true, timeout true.
            let upd_err =
                ExecutorError::execution(mk(), Some(super::super::InnerOpError::Upsert(key)));
            assert!(upd_err.is_wasm_timeout());
            assert!(upd_err.is_contract_exec_rejection());
            let upd_request_err = upd_err.unwrap_request();
            let RequestError::ContractError(StdContractError::Update { .. }) = &upd_request_err
            else {
                panic!(
                    "UPDATE validation error must be an Update variant, got {upd_request_err:?}"
                );
            };
        }

        /// Source-scrape pin (#4864 round-7 Codex P1, updated round-9 item 4): the
        /// UPDATE-path validation helpers must CLASSIFY `validate_state` failures
        /// (never `op = None`, which would make the driver record nothing for a
        /// contract whose runaway half is `validate_state`). The base
        /// `fetch_related_for_validation` still hardcodes the Upsert op; the shared
        /// `fetch_related_for_validation_network` now classifies via the
        /// caller-supplied op kind (`validation_op.op_for`) so a fresh PUT gets a
        /// `Put` variant — but STILL never `op = None`.
        #[test]
        fn update_path_validation_sites_classify_never_op_none() {
            // (source, fn-signature-needle, expected-classification-needle).
            let cases: [(&str, &str, &str); 2] = [
                (
                    include_str!("executor/runtime/executor_impl.rs"),
                    "async fn fetch_related_for_validation(",
                    "Some(InnerOpError::Upsert(*key))",
                ),
                (
                    include_str!("executor/runtime/contract_ops.rs"),
                    "async fn fetch_related_for_validation_network(",
                    "Some(validation_op.op_for(*key))",
                ),
            ];
            for (src, sig, expected) in cases {
                let start = src
                    .find(sig)
                    .unwrap_or_else(|| panic!("validation helper `{sig}` not found"));
                // Bound the body at the NEXT method in the impl block (any
                // visibility), so the negative assertion below can't overrun into a
                // different function that legitimately uses `execution(e, None)`.
                let rest = &src[start + sig.len()..];
                let end = [
                    "\n    fn ",
                    "\n    async fn ",
                    "\n    pub(super) fn ",
                    "\n    pub(super) async fn ",
                    "\n    pub(crate) fn ",
                    "\n    pub(crate) async fn ",
                    "\n    pub(in crate::contract::executor) async fn ",
                ]
                .iter()
                .filter_map(|needle| rest.find(needle))
                .min()
                .map(|i| start + sig.len() + i)
                .unwrap_or(src.len());
                let body = &src[start..end];

                assert!(
                    body.contains("validate_state"),
                    "`{sig}` must call validate_state (scrape anchor)"
                );
                assert!(
                    body.contains(expected),
                    "`{sig}` must classify validate_state failures via `{expected}` \
                     so a validation-phase timeout reaches the merge-failure backoff \
                     (#4864 round-7/9)"
                );
                assert!(
                    !body.contains("execution(e, None)"),
                    "`{sig}` must NOT wrap a validate_state failure with op = None — \
                     that escapes classification and the UPDATE driver records \
                     nothing (#4864 round-7)"
                );
            }
        }

        /// #4864 round-9 item 4 pin: the fresh-PUT path
        /// (`verify_and_store_contract`) must validate with `ValidationOpKind::Put`
        /// so its validation error keeps PutResponse semantics (a Put variant), not
        /// Update. A regression to Update would mislabel a fresh PUT's validation
        /// failure as an UPDATE error.
        #[test]
        fn fresh_put_validation_uses_put_op_kind() {
            let src = include_str!("executor/runtime/contract_ops.rs");
            let start = src
                .find("async fn verify_and_store_contract(")
                .expect("verify_and_store_contract not found");
            let rest = &src[start + 1..];
            let end = rest
                .find("\n    async fn ")
                .or_else(|| rest.find("\n    fn "))
                .map(|i| i + 1)
                .unwrap_or(rest.len());
            let body = &src[start..start + 1 + end];
            assert!(
                body.contains("ValidationOpKind::Put"),
                "verify_and_store_contract (fresh PUT) must validate with \
                 ValidationOpKind::Put — a fresh PUT's validation error must be a Put \
                 variant, not Update (#4864 round-9 item 4)"
            );
        }

        /// Pin (#4861 review): the DESERIALIZATION-poison class — a contract's
        /// `update_state` returning `ContractError::Deser` for a malformed
        /// circulating delta (the `FBFRDjxV…` production case) — MUST reach the
        /// merge-failure backoff. It routes `ContractExecError::ContractError`
        /// through `update_exec_error`, so the cause carries the
        /// "execution error:" prefix and `is_contract_exec_rejection` (the
        /// backoff recording gate) matches; it is neither a timeout nor an
        /// invalid-update rejection.
        #[test]
        fn test_deser_poison_classifies_as_exec_rejection_for_backoff() {
            use crate::wasm_runtime::{ContractError, ContractExecError, RuntimeInnerError};
            let key = test_fixtures::make_contract_key();
            let deser = freenet_stdlib::prelude::ContractError::Deser(
                "Semantic(None, \"invalid type: null, expected map\")".to_string(),
            );
            let contract_err: ContractError =
                RuntimeInnerError::ContractExecError(ContractExecError::from(deser)).into();
            let err = ExecutorError::execution(
                contract_err,
                Some(super::super::InnerOpError::Upsert(key)),
            );
            assert!(
                err.is_contract_exec_rejection(),
                "a contract-returned Deser error on the Upsert path must satisfy \
                 the backoff recording gate"
            );
            assert!(!err.is_wasm_timeout(), "deser poison is not a timeout");
            assert!(
                !err.is_invalid_update_rejection(),
                "deser poison is not the benign invalid-update rejection"
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
