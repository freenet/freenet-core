//! Handles interactions with contracts, managing their state and execution requests.
//!
//! It receives events via `ContractHandlerChannel` from the main node event loop (`node::Node`)
//! and interacts with the `ContractExecutor` to perform actions. Results are sent back
//! to the node loop.
//!
//! See [`../architecture.md`](../architecture.md) for its role and communication patterns.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;

use freenet_stdlib::client_api::DelegateRequest;
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use super::ExecutorError;
use super::executor::RuntimePool;
use super::{ContractError, executor::ContractExecutor};
use crate::client_events::ClientId;
use crate::client_events::{AuthToken, HostResult, RequestId};
use crate::config::Config;
use crate::message::{QueryResult, Transaction};
use crate::node::OpManager;
use crate::wasm_runtime::UserSecretContext;
use std::num::NonZeroUsize;

pub(crate) struct ClientResponsesReceiver(UnboundedReceiver<(ClientId, RequestId, HostResult)>);

pub(crate) fn client_responses_channel() -> (ClientResponsesReceiver, ClientResponsesSender) {
    let (tx, rx) = mpsc::unbounded_channel();
    (ClientResponsesReceiver(rx), ClientResponsesSender(tx))
}

impl std::ops::Deref for ClientResponsesReceiver {
    type Target = UnboundedReceiver<(ClientId, RequestId, HostResult)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ClientResponsesReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone)]
pub(crate) struct ClientResponsesSender(UnboundedSender<(ClientId, RequestId, HostResult)>);

impl std::ops::Deref for ClientResponsesSender {
    type Target = UnboundedSender<(ClientId, RequestId, HostResult)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) trait ContractHandler {
    type Builder;
    type ContractExecutor: ContractExecutor;

    fn build(
        contract_handler_channel: ContractHandlerChannel<ContractHandlerHalve>,
        op_manager: Arc<OpManager>,
        builder: Self::Builder,
    ) -> impl Future<Output = anyhow::Result<Self>> + Send
    where
        Self: Sized + 'static;

    fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve>;

    fn executor(&mut self) -> &mut Self::ContractExecutor;
}

pub(crate) struct NetworkContractHandler {
    executor: RuntimePool,
    channel: ContractHandlerChannel<ContractHandlerHalve>,
}

impl ContractHandler for NetworkContractHandler {
    type Builder = Arc<Config>;
    type ContractExecutor = RuntimePool;

    async fn build(
        channel: ContractHandlerChannel<ContractHandlerHalve>,
        op_manager: Arc<OpManager>,
        config: Self::Builder,
    ) -> anyhow::Result<Self>
    where
        Self: Sized + 'static,
    {
        // Reserve one logical core for the Tokio event loop and OS scheduling.
        // WASM execution is CPU-bound, so the pool naturally can't exceed useful parallelism.
        // Cap at 16 to stay well within the max_blocking_threads limit (default: 2x cores,
        // clamped to [4, 32]), preventing the executor pool from exhausting the blocking pool.
        let parallelism = std::thread::available_parallelism()
            .unwrap_or(NonZeroUsize::new(4).unwrap())
            .get()
            .saturating_sub(1)
            .max(1);

        // Pool size configurable via FREENET_RUNTIME_POOL_SIZE env var (useful for tests)
        let pool_size = std::env::var("FREENET_RUNTIME_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .and_then(|n| NonZeroUsize::new(n.clamp(1, 16)))
            .unwrap_or_else(|| NonZeroUsize::new(parallelism.clamp(1, 16)).unwrap());

        tracing::info!(pool_size = %pool_size, "Creating RuntimePool");

        let executor = RuntimePool::new(config.clone(), op_manager.clone(), pool_size).await?;

        // Set up hosting storage reference for eviction cleanup
        // This must be done before loading the cache so evictions work correctly
        let storage = executor.state_store().inner().clone();
        op_manager.ring.set_hosting_storage(storage.clone());
        // Hydrate broken-invariants flags from the same backing store so a
        // node that previously detected a non-idempotent contract doesn't
        // re-engage its broadcast storm after restart.
        op_manager
            .ring
            .set_broken_invariants_storage(storage.clone());

        // Load hosting cache from persisted storage
        // This restores contracts that were hosted before restart, and also
        // migrates legacy contracts (state exists but no hosting metadata).
        // We pass a closure that uses RuntimePool's code_hash_from_id for
        // looking up CodeHash from ContractInstanceId during legacy migration.
        #[cfg(feature = "redb")]
        {
            if let Err(e) = op_manager.ring.load_hosting_cache(&storage, |instance_id| {
                executor.code_hash_from_id(instance_id)
            }) {
                tracing::warn!(error = %e, "Failed to load hosting cache from storage");
            }
        }
        #[cfg(all(feature = "sqlite", not(feature = "redb")))]
        {
            if let Err(e) = op_manager
                .ring
                .load_hosting_cache(&storage, |instance_id| {
                    executor.code_hash_from_id(instance_id)
                })
                .await
            {
                tracing::warn!(error = %e, "Failed to load hosting cache from storage");
            }
        }

        // Populate neighbor hosting from hosted contracts so HostingStateResponse
        // reports our full contract set when ring connections establish.
        let hosted_keys = op_manager.ring.hosting_contract_keys();
        let hosted_ids = hosted_keys.iter().map(|k| *k.id());
        op_manager
            .neighbor_hosting
            .initialize_from_hosting_cache(hosted_ids);

        Ok(Self { executor, channel })
    }

    fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve> {
        &mut self.channel
    }

    fn executor(&mut self) -> &mut Self::ContractExecutor {
        &mut self.executor
    }
}

#[derive(Debug, Eq)]
pub(crate) struct EventId {
    pub(crate) id: u64,
}

impl PartialEq for EventId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for EventId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// A bidirectional channel which keeps track of the initiator half
/// and sends the corresponding response to the listener of the operation.
pub(crate) struct ContractHandlerChannel<End: sealed::ChannelHalve> {
    end: End,
    /// Optional session actor communication
    session_adapter_tx: Option<mpsc::Sender<SessionMessage>>,
}

pub(crate) struct ContractHandlerHalve {
    event_receiver: mpsc::UnboundedReceiver<InternalCHEvent>,
    waiting_response: BTreeMap<u64, tokio::sync::oneshot::Sender<(EventId, ContractHandlerEvent)>>,
}

/// Ownership of a client's response oneshot, removed from `waiting_response`
/// so it can outlive a single `handle_contract_event` call.
///
/// Created by [`ContractHandlerChannel::take_waiting_response`] when a PUT/UPDATE
/// defers its related-contract fetch off the serial loop (#4391). The loop holds
/// this until the deferred upsert resumes, then calls [`respond`](Self::respond)
/// to deliver the result to the original client — the same delivery
/// `send_to_sender` performs for the inline path.
pub(crate) struct StashedResponder {
    id: u64,
    sender: tokio::sync::oneshot::Sender<(EventId, ContractHandlerEvent)>,
}

impl StashedResponder {
    /// Deliver `ev` to the original client. Returns `Err` if the client
    /// already disconnected (its receiver was dropped) — non-fatal, the
    /// upsert side effects have already been applied.
    pub fn respond(self, ev: ContractHandlerEvent) -> Result<(), ContractError> {
        self.sender
            .send((EventId { id: self.id }, ev))
            .map_err(|_| ContractError::NoEvHandlerResponse)
    }

    /// Construct a `StashedResponder` directly from a oneshot sender, for unit
    /// tests of the deferral machinery (e.g. the drop-guard delivering a
    /// `MissingRelated` resume) that need to observe the response a stranded
    /// client receives without standing up the full channel + loop.
    #[cfg(test)]
    pub(crate) fn for_test(
        id: u64,
        sender: tokio::sync::oneshot::Sender<(EventId, ContractHandlerEvent)>,
    ) -> Self {
        Self { id, sender }
    }
}

pub(crate) struct SenderHalve {
    event_sender: mpsc::UnboundedSender<InternalCHEvent>,
    wait_for_res_tx: mpsc::Sender<(ClientId, WaitingTransaction)>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WaitingTransaction {
    Transaction(Transaction),
    Subscription { contract_key: ContractInstanceId },
}

/// Session message definitions for actor-based client management
#[allow(dead_code)]
#[derive(Debug)]
pub enum SessionMessage {
    #[allow(dead_code)]
    RegisterClient {
        client_id: ClientId,
        request_id: RequestId,
        transport_tx: UnboundedSender<HostResult>,
        token: Option<AuthToken>,
    },
    #[allow(dead_code)]
    RegisterTransaction {
        #[allow(dead_code)]
        tx: Transaction,
        #[allow(dead_code)]
        client_id: ClientId,
        #[allow(dead_code)]
        request_id: RequestId,
    },
    #[allow(dead_code)]
    DeliverResult {
        tx: Transaction,
        result: Box<QueryResult>,
    },
    #[allow(dead_code)]
    DeliverHostResponse {
        tx: Transaction,
        response: Arc<HostResult>,
    },
    #[allow(dead_code)]
    DeliverHostResponseWithRequestId {
        tx: Transaction,
        response: Arc<HostResult>,
        request_id: RequestId,
    },
    ClientDisconnect {
        client_id: ClientId,
    },
}

impl From<Transaction> for WaitingTransaction {
    fn from(tx: Transaction) -> Self {
        WaitingTransaction::Transaction(tx)
    }
}

/// Communicates that a client is waiting for a transaction resolution
/// to continue processing this event.
pub(crate) struct WaitingResolution {
    wait_for_res_rx: mpsc::Receiver<(ClientId, WaitingTransaction)>,
}

mod sealed {
    use super::{ContractHandlerHalve, SenderHalve, WaitingResolution};
    pub(crate) trait ChannelHalve {}
    impl ChannelHalve for ContractHandlerHalve {}
    impl ChannelHalve for SenderHalve {}
    impl ChannelHalve for WaitingResolution {}
}

pub(crate) fn contract_handler_channel() -> (
    ContractHandlerChannel<SenderHalve>,
    ContractHandlerChannel<ContractHandlerHalve>,
    ContractHandlerChannel<WaitingResolution>,
) {
    let (event_sender, event_receiver) = mpsc::unbounded_channel();
    // Capacity sized to comfortably absorb concurrent multi-WebSocket bursts
    // (e.g. freenet-git's 8-way parallel chunked PUT pool) while the event
    // loop's anti-starvation force-poll drains it. Smaller values caused
    // 30s producer-side timeouts on the production gateway under realistic
    // mirror traffic. See issue #4056.
    let (wait_for_res_tx, wait_for_res_rx) = mpsc::channel(1000);
    (
        ContractHandlerChannel {
            end: SenderHalve {
                event_sender,
                wait_for_res_tx,
            },
            session_adapter_tx: None,
        },
        ContractHandlerChannel {
            end: ContractHandlerHalve {
                event_receiver,
                waiting_response: BTreeMap::new(),
            },
            session_adapter_tx: None,
        },
        ContractHandlerChannel {
            end: WaitingResolution { wait_for_res_rx },
            session_adapter_tx: None,
        },
    )
}

const EV_ID_BLOCK: u64 = 1_000_000;

thread_local! {
    static EV_ID: Cell<u64> = {
        let idx = crate::config::GlobalRng::thread_index();
        Cell::new(idx * EV_ID_BLOCK)
    };
}

/// Reset the event ID counter to initial state for this thread.
/// Thread-local, so safe for parallel test execution.
pub fn reset_event_id_counter() {
    let idx = crate::config::GlobalRng::thread_index();
    EV_ID.with(|c| c.set(idx * EV_ID_BLOCK));
}

impl Stream for ContractHandlerChannel<WaitingResolution> {
    type Item = (ClientId, WaitingTransaction);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.end.wait_for_res_rx).poll_recv(cx)
    }
}

impl ContractHandlerChannel<SenderHalve> {
    // TODO: the timeout should be derived from whatever is the worst
    // case we are willing to accept for waiting out for an event;
    // have to double check all events to see if any depend on external
    // responses and go from there, also this may very well depend on the
    // kind of event and can be optimized on a case basis
    const CH_EV_RESPONSE_TIME_OUT: Duration = Duration::from_secs(300);

    /// Send an event to the contract handler and receive a response event if successful.
    ///
    /// Defaults to [`Priority::DEFAULT`] (`NetworkRelay`). Callers on a
    /// local-client path should use [`send_to_handler_prioritized`] with
    /// [`Priority::ClientLocal`] (#4534).
    pub async fn send_to_handler(
        &self,
        ev: ContractHandlerEvent,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.send_to_handler_prioritized(ev, super::fair_queue::Priority::DEFAULT)
            .await
    }

    /// Send an event at an explicit priority class and await its response.
    pub async fn send_to_handler_prioritized(
        &self,
        ev: ContractHandlerEvent,
        priority: super::fair_queue::Priority,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.send_to_handler_with_timeout(ev, Self::CH_EV_RESPONSE_TIME_OUT, priority)
            .await
    }

    /// Send an event to the contract handler with a custom timeout.
    ///
    /// Use shorter timeouts for calls from the event loop path (e.g., broadcast
    /// state change) to avoid blocking the event loop for extended periods.
    pub async fn send_to_handler_with_timeout(
        &self,
        ev: ContractHandlerEvent,
        timeout: Duration,
        priority: super::fair_queue::Priority,
    ) -> Result<ContractHandlerEvent, ContractError> {
        let id = EV_ID.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        });
        let (result, result_receiver) = tokio::sync::oneshot::channel();
        self.end
            .event_sender
            .send(InternalCHEvent {
                ev,
                id,
                priority,
                result,
            })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))?;
        match tokio::time::timeout(timeout, result_receiver).await {
            Ok(Ok((_, res))) => Ok(res),
            Ok(Err(_)) | Err(_) => Err(ContractError::NoEvHandlerResponse),
        }
    }

    /// Send an event to the contract handler without waiting for a response.
    ///
    /// Used for fire-and-forget events like `ClientDisconnect` that don't
    /// produce a response. Defaults to [`Priority::DEFAULT`].
    pub fn send_to_handler_fire_and_forget(
        &self,
        ev: ContractHandlerEvent,
    ) -> Result<(), ContractError> {
        self.send_to_handler_fire_and_forget_prioritized(ev, super::fair_queue::Priority::DEFAULT)
    }

    /// Fire-and-forget send at an explicit priority class.
    pub fn send_to_handler_fire_and_forget_prioritized(
        &self,
        ev: ContractHandlerEvent,
        priority: super::fair_queue::Priority,
    ) -> Result<(), ContractError> {
        let id = EV_ID.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        });
        // Create a oneshot but immediately drop the receiver — the handler
        // won't send a response, and if it does, the send will harmlessly fail.
        let (result, _) = tokio::sync::oneshot::channel();
        self.end
            .event_sender
            .send(InternalCHEvent {
                ev,
                id,
                priority,
                result,
            })
            .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))?;
        Ok(())
    }

    /// Install session adapter for migration
    pub fn with_session_adapter(&mut self, session_tx: mpsc::Sender<SessionMessage>) {
        self.session_adapter_tx = Some(session_tx);
    }

    /// Timeout for registering a transaction with the event loop's client-result delivery system.
    /// If the channel is full for this long, the event loop is not draining client transactions
    /// (polled at Priority 7) and the operation should fail rather than block forever.
    const WAIT_FOR_RES_SEND_TIMEOUT: Duration = Duration::from_secs(30);

    pub async fn waiting_for_transaction_result(
        &self,
        transaction: impl Into<WaitingTransaction>,
        client_id: ClientId,
        request_id: RequestId,
    ) -> Result<(), ContractError> {
        let waiting_tx = transaction.into();

        match tokio::time::timeout(
            Self::WAIT_FOR_RES_SEND_TIMEOUT,
            self.end.wait_for_res_tx.send((client_id, waiting_tx)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(ContractError::NoEvHandlerResponse),
            Err(_) => {
                tracing::error!(
                    client = %client_id,
                    request_id = %request_id,
                    channel_capacity = self.end.wait_for_res_tx.capacity(),
                    timeout_secs = Self::WAIT_FOR_RES_SEND_TIMEOUT.as_secs(),
                    "Timed out registering transaction for result delivery — \
                     event loop is not draining client transactions (Priority 7 starvation)"
                );
                return Err(ContractError::NoEvHandlerResponse);
            }
        }

        if let WaitingTransaction::Transaction(tx) = waiting_tx {
            self.notify_session_actor(tx, client_id, request_id).await;
        }

        Ok(())
    }

    pub async fn waiting_for_subscription_result(
        &self,
        tx: Transaction,
        contract_key: ContractInstanceId,
        client_id: ClientId,
        request_id: RequestId,
    ) -> Result<(), ContractError> {
        match tokio::time::timeout(
            Self::WAIT_FOR_RES_SEND_TIMEOUT,
            self.end
                .wait_for_res_tx
                .send((client_id, WaitingTransaction::Subscription { contract_key })),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(ContractError::NoEvHandlerResponse),
            Err(_) => {
                tracing::error!(
                    tx = %tx,
                    client = %client_id,
                    request_id = %request_id,
                    contract = %contract_key,
                    channel_capacity = self.end.wait_for_res_tx.capacity(),
                    timeout_secs = Self::WAIT_FOR_RES_SEND_TIMEOUT.as_secs(),
                    "Timed out registering subscription for result delivery — \
                     event loop is not draining client transactions (Priority 7 starvation)"
                );
                return Err(ContractError::NoEvHandlerResponse);
            }
        }

        self.notify_session_actor(tx, client_id, request_id).await;

        Ok(())
    }

    /// Notify the session actor about a transaction registration, if installed.
    ///
    /// Uses `.send().await` with a timeout to ensure the registration is not
    /// silently dropped (which would cause the client to timeout waiting for a
    /// response that will never arrive). If the session actor channel is full,
    /// this waits up to `WAIT_FOR_RES_SEND_TIMEOUT` for capacity.
    async fn notify_session_actor(
        &self,
        tx: Transaction,
        client_id: ClientId,
        request_id: RequestId,
    ) {
        let Some(session_tx) = &self.session_adapter_tx else {
            tracing::warn!(
                client = %client_id,
                request_id = %request_id,
                "Session adapter not installed — session actor will not track transaction"
            );
            return;
        };
        let msg = SessionMessage::RegisterTransaction {
            tx,
            client_id,
            request_id,
        };
        match tokio::time::timeout(Self::WAIT_FOR_RES_SEND_TIMEOUT, session_tx.send(msg)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!(
                    tx = %tx,
                    client = %client_id,
                    request_id = %request_id,
                    error = %e,
                    "Failed to notify session actor — receiver dropped"
                );
            }
            Err(_) => {
                tracing::error!(
                    tx = %tx,
                    client = %client_id,
                    request_id = %request_id,
                    timeout_secs = Self::WAIT_FOR_RES_SEND_TIMEOUT.as_secs(),
                    "Timed out notifying session actor — channel full, client will not receive response"
                );
            }
        }
    }
}

impl ContractHandlerChannel<ContractHandlerHalve> {
    pub async fn send_to_sender(
        &mut self,
        id: EventId,
        ev: ContractHandlerEvent,
    ) -> Result<(), ContractError> {
        if let Some(response) = self.end.waiting_response.remove(&id.id) {
            response
                .send((id, ev))
                .map_err(|_| ContractError::NoEvHandlerResponse)
        } else {
            Err(ContractError::NoEvHandlerResponse)
        }
    }

    /// Drop the waiting-response entry for a fire-and-forget event.
    ///
    /// The sender side already dropped the oneshot receiver, so there is
    /// nothing to send back. This just prevents the entry from leaking in
    /// the `waiting_response` map.
    pub fn drop_waiting_response(&mut self, id: EventId) {
        self.end.waiting_response.remove(&id.id);
    }

    /// Remove and return the response oneshot for `id` so the caller can hold
    /// it across an off-loop wait and answer the original client later.
    ///
    /// Used by the `contract_handling` loop when a PUT/UPDATE must defer its
    /// related-contract network fetch off the serial loop (#4391): the loop
    /// takes ownership of the oneshot here, drives the fetch on a background
    /// task, re-runs the upsert, and finally fires the stashed oneshot via
    /// [`StashedResponder::respond`]. Returns `None` if no entry exists (e.g. a
    /// fire-and-forget event whose receiver was already dropped).
    pub fn take_waiting_response(&mut self, id: &EventId) -> Option<StashedResponder> {
        self.end
            .waiting_response
            .remove(&id.id)
            .map(|sender| StashedResponder { id: id.id, sender })
    }

    /// Check if a waiting_response entry exists for the given event ID.
    #[cfg(test)]
    pub fn has_waiting_response(&self, id: &EventId) -> bool {
        self.end.waiting_response.contains_key(&id.id)
    }

    pub async fn recv_from_sender(
        &mut self,
    ) -> Result<(EventId, ContractHandlerEvent, super::fair_queue::Priority), ContractError> {
        if let Some(InternalCHEvent {
            ev,
            id,
            priority,
            result,
        }) = self.end.event_receiver.recv().await
        {
            self.end.waiting_response.insert(id, result);
            return Ok((EventId { id }, ev, priority));
        }
        Err(ContractError::NoEvHandlerResponse)
    }

    /// Try to receive an event without blocking.
    ///
    /// Returns `Ok(None)` if the channel is empty, `Err` if the channel is closed.
    /// Used by the fair-queue drain loop to batch-fill the fair queue before processing.
    #[allow(clippy::type_complexity)]
    pub fn try_recv_from_sender(
        &mut self,
    ) -> Result<Option<(EventId, ContractHandlerEvent, super::fair_queue::Priority)>, ContractError>
    {
        match self.end.event_receiver.try_recv() {
            Ok(InternalCHEvent {
                ev,
                id,
                priority,
                result,
            }) => {
                self.end.waiting_response.insert(id, result);
                Ok(Some((EventId { id }, ev, priority)))
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                Err(ContractError::NoEvHandlerResponse)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoreResponse {
    pub state: Option<WrappedState>,
    pub contract: Option<ContractContainer>,
}

struct InternalCHEvent {
    ev: ContractHandlerEvent,
    id: u64,
    // client_id: Option<ClientId>,
    /// Scheduling/admission priority class for the fair queue (#4534).
    priority: super::fair_queue::Priority,
    result: tokio::sync::oneshot::Sender<(EventId, ContractHandlerEvent)>,
}

/// A user token carried on a [`ContractHandlerEvent::ExportUserSecrets`] event,
/// held in `Zeroizing` so it is wiped on drop. Its `Debug` redacts the bytes:
/// `ContractHandlerEvent` derives `Debug` and is logged, and the token is a
/// high-value, durable credential that must NEVER reach a log line (mirrors how
/// `UserSecretContext`'s `Debug` redacts its `dek_secret`).
pub(crate) struct RedactedToken(zeroize::Zeroizing<Vec<u8>>);

impl RedactedToken {
    pub(crate) fn new(token: Vec<u8>) -> Self {
        Self(zeroize::Zeroizing::new(token))
    }

    /// Borrow the raw token bytes (for deriving the bundle key only).
    pub(crate) fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for RedactedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RedactedToken(<redacted>)")
    }
}

/// Which key-derivation the import bundle was sealed with. The bundle header
/// also records the KDF, and [`crate::wasm_runtime::secret_export::open_bundle`]
/// rejects a mismatch with a clean auth failure — so a wrong `kind` surfaces as
/// a 4xx (bad key/bundle), never a panic or a 500.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BundleKeyKind {
    /// HKDF-SHA256 over the opaque per-user token (the hosted-export default).
    Token,
    /// Argon2id over a user passphrase (the offline `freenet secrets export
    /// --passphrase` form, supported for the file-upload fallback).
    Passphrase,
}

/// Where a live import places the incoming secrets. v1 supports only `Local`
/// (a normal single-user node taking its data home, #4592); the `User` re-host
/// target is reserved for a future hosted-side flow and is not constructed by
/// any current endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportTargetScope {
    /// Place imported secrets at the node-local / single-user scope.
    Local,
}

/// The raw bundle bytes carried on a [`ContractHandlerEvent::ImportSecrets`]
/// event. Wraps `Vec<u8>` only so its `Debug` prints the byte LENGTH instead of
/// dumping the (up-to-256-MiB) ciphertext into a log line — `ContractHandlerEvent`
/// derives `Debug` and is logged. The bytes are encrypted-at-rest ciphertext
/// (the plaintext is only ever materialized inside `import_bundle`'s `Zeroizing`
/// buffers), not a secret; the wrapper exists purely to keep a multi-hundred-
/// megabyte `?event` log line from ever forming.
pub(crate) struct ImportBundle(Vec<u8>);

impl ImportBundle {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Borrow the raw bundle bytes (handed to `import_bundle`).
    pub(crate) fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for ImportBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ImportBundle({} bytes)", self.0.len())
    }
}

#[derive(Debug)]
pub(crate) enum ContractHandlerEvent {
    DelegateRequest {
        req: DelegateRequest<'static>,
        origin_contract: Option<ContractInstanceId>,
        /// Per-connection per-user secret namespace (hosted mode, P2 of #4381),
        /// derived once at the WS connection boundary from the connection's
        /// user token. `None` outside hosted mode or when no token was
        /// presented. Carried here on a separate channel from `req`/`origin_contract`
        /// so nothing in the request body can forge it. The owned
        /// `UserSecretContext` holds the `dek_secret` in `Zeroizing`; its
        /// `Debug` impl redacts the secret, so logging this event is safe.
        user_context: Option<UserSecretContext>,
    },
    DelegateResponse(Vec<OutboundDelegateMsg>),
    /// Export a hosted user's per-user delegate secrets into an encrypted
    /// bundle (hosted-mode export, P3-live of #4381). Carries the
    /// connection-derived `user_context` (the forge-proof per-user namespace,
    /// same channel as `DelegateRequest`) and the raw user `token` used as the
    /// bundle-key material so the user can re-import with the token they hold.
    ///
    /// Like `DelegateRequest::user_context`, `user_context`'s `Debug` redacts
    /// the dek_secret. The `token` is sensitive (high-value, durable
    /// credential): it is NOT derived from `Debug`/`Display` (this struct has
    /// neither field rendered) and must never be logged.
    ExportUserSecrets {
        user_context: UserSecretContext,
        /// Raw user token bytes, redacted in `Debug` and wiped on drop; used
        /// only to derive the bundle encryption key.
        token: RedactedToken,
    },
    /// Response to an `ExportUserSecrets` request: the encrypted bundle bytes,
    /// or an executor error.
    ExportUserSecretsResponse(Result<Vec<u8>, ExecutorError>),
    /// Import delegate secrets from an encrypted bundle into this node's local
    /// (single-user) `SecretsStore`, LIVE — without stopping the node (P3-live
    /// of #4592, the durable counterpart of [`Self::ExportUserSecrets`]). The
    /// raw `bundle` bytes are decrypted with `key_material` (interpreted per
    /// `key_kind`); the decrypt is all-or-nothing and runs BEFORE any write, so
    /// a wrong key / corrupt bundle fails with nothing written.
    ///
    /// `key_material` is the high-value bundle decryption key, redacted in
    /// `Debug` and wiped on drop; `bundle`'s `Debug` prints only its length.
    /// Neither is ever rendered by the `Display` impl.
    ImportSecrets {
        /// Where the imported secrets land. v1 is always `Local`.
        target_scope: ImportTargetScope,
        /// The encrypted bundle bytes (`FNSX` format).
        bundle: ImportBundle,
        /// Raw bundle-key material, redacted in `Debug` and wiped on drop.
        key_material: RedactedToken,
        /// How to interpret `key_material` (token-HKDF vs passphrase-Argon2id).
        key_kind: BundleKeyKind,
        /// Collision policy: `false` skips+reports an entry whose secret already
        /// exists; `true` overwrites it (the prior value is snapshotted first).
        overwrite: bool,
    },
    /// Response to an `ImportSecrets` request: the per-entry import accounting,
    /// or an executor error.
    ImportSecretsResponse(Result<crate::wasm_runtime::secret_export::ImportReport, ExecutorError>),
    /// Try to push/put a new value into the contract
    PutQuery {
        key: ContractKey,
        state: WrappedState,
        related_contracts: RelatedContracts<'static>,
        contract: Option<ContractContainer>,
    },
    /// The response to a push query
    PutResponse {
        new_value: Result<WrappedState, ExecutorError>,
        /// True if the stored state actually changed (old state != new state).
        /// Used to determine whether PUT should trigger UPDATE propagation.
        state_changed: bool,
    },
    /// Fetch a supposedly existing contract value in this node, and optionally the contract itself
    GetQuery {
        instance_id: ContractInstanceId,
        return_contract_code: bool,
    },
    /// The response to a get query event
    GetResponse {
        key: Option<ContractKey>,
        response: Result<StoreResponse, ExecutorError>,
    },
    /// Updates a supposedly existing contract in this node
    UpdateQuery {
        key: ContractKey,
        data: UpdateData<'static>,
        related_contracts: RelatedContracts<'static>,
    },
    /// The response to an update query
    UpdateResponse {
        new_value: Result<WrappedState, ExecutorError>,
        /// True if the stored state actually changed after the merge.
        state_changed: bool,
    },
    // The response to an update query where the state has not changed
    UpdateNoChange {
        key: ContractKey,
    },
    RegisterSubscriberListener {
        key: ContractInstanceId,
        client_id: ClientId,
        summary: Option<StateSummary<'static>>,
        subscriber_listener: mpsc::Sender<HostResult>,
    },
    RegisterSubscriberListenerResponse,
    #[allow(dead_code)]
    QuerySubscriptions {
        callback: tokio::sync::mpsc::Sender<QueryResult>,
    },
    #[allow(dead_code)]
    QuerySubscriptionsResponse,
    /// Get the state summary for a contract using its summarize_state method
    GetSummaryQuery {
        key: ContractKey,
    },
    /// Response to a GetSummaryQuery
    GetSummaryResponse {
        key: ContractKey,
        summary: Result<StateSummary<'static>, ExecutorError>,
    },
    /// Get a state delta for a contract given a peer's state summary.
    /// Used for delta-based synchronization.
    GetDeltaQuery {
        key: ContractKey,
        their_summary: StateSummary<'static>,
    },
    /// Response to a GetDeltaQuery
    GetDeltaResponse {
        key: ContractKey,
        delta: Result<StateDelta<'static>, ExecutorError>,
    },
    /// A client has disconnected — clean up its entries in shared_summaries
    /// and shared_notifications. Fire-and-forget: no response is sent.
    ClientDisconnect {
        client_id: ClientId,
    },
    /// Reclaim a contract's on-disk storage (state + WASM code) after it was
    /// evicted from the hosting cache. Fire-and-forget: no response is sent.
    ///
    /// `expected_generation` is the state-write generation captured
    /// atomically when the contract was evicted (see
    /// `HostingCache::record_access` / `sweep_expired`). The eviction
    /// handler in `RuntimePool::remove_contract` re-reads the current
    /// generation and skips disk reclamation if it has advanced — that
    /// means a state write (PUT/UPDATE) occurred between eviction and
    /// this handler running, and the contract has been re-hosted with
    /// fresh state we must not delete. This closes the re-host TOCTOU
    /// window described on `RuntimePool::remove_contract`.
    EvictContract {
        key: ContractKey,
        expected_generation: u64,
    },
}

impl std::fmt::Display for ContractHandlerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContractHandlerEvent::DelegateRequest {
                req,
                origin_contract,
                // `user_context` deliberately omitted from Display: it names a
                // per-user namespace and carries a (redacted-in-Debug) secret;
                // it has no place in a human-facing event description.
                ..
            } => {
                write!(
                    f,
                    "delegate request {{ key: {:?}, origin: {:?} }}",
                    req.key(),
                    origin_contract
                )
            }
            ContractHandlerEvent::DelegateResponse(_) => {
                write!(f, "delegate response")
            }
            ContractHandlerEvent::PutQuery { key, contract, .. } => {
                if let Some(contract) = contract {
                    use std::fmt::Write;
                    let mut params = String::new();
                    params.push_str("0x");
                    for b in contract.params().as_ref().iter().take(8) {
                        write!(&mut params, "{b:02x}")?;
                    }
                    params.push_str("...");
                    write!(f, "put query {{ {key}, params: {params} }}",)
                } else {
                    write!(f, "put query {{ {key} }}")
                }
            }
            ContractHandlerEvent::PutResponse {
                new_value,
                state_changed,
            } => match new_value {
                Ok(v) => {
                    write!(
                        f,
                        "put query response {{ {v}, state_changed: {state_changed} }}",
                    )
                }
                Err(e) => {
                    write!(f, "put query failed {{ {e} }}",)
                }
            },
            ContractHandlerEvent::GetQuery {
                instance_id,
                return_contract_code,
                ..
            } => {
                write!(
                    f,
                    "get query {{ {instance_id}, return contract code: {return_contract_code} }}",
                )
            }
            ContractHandlerEvent::GetResponse { key, response } => match response {
                Ok(_) => {
                    write!(f, "get query response {{ {key:?} }}",)
                }
                Err(_) => {
                    write!(f, "get query failed {{ {key:?} }}",)
                }
            },
            ContractHandlerEvent::UpdateQuery { key, .. } => {
                write!(f, "update query {{ {key} }}")
            }
            ContractHandlerEvent::UpdateResponse { new_value, .. } => match new_value {
                Ok(v) => {
                    write!(f, "update query response {{ {v} }}",)
                }
                Err(e) => {
                    write!(f, "update query failed {{ {e} }}",)
                }
            },
            ContractHandlerEvent::UpdateNoChange { key } => {
                write!(f, "update query no change {{ {key} }}",)
            }
            ContractHandlerEvent::RegisterSubscriberListener { key, client_id, .. } => {
                write!(
                    f,
                    "register subscriber listener {{ {key}, client_id: {client_id} }}",
                )
            }
            ContractHandlerEvent::RegisterSubscriberListenerResponse => {
                write!(f, "register subscriber listener response")
            }
            ContractHandlerEvent::QuerySubscriptions { .. } => {
                write!(f, "query subscriptions")
            }
            ContractHandlerEvent::QuerySubscriptionsResponse => {
                write!(f, "query subscriptions response")
            }
            ContractHandlerEvent::GetSummaryQuery { key } => {
                write!(f, "get summary query {{ {key} }}")
            }
            ContractHandlerEvent::GetSummaryResponse { key, summary } => match summary {
                Ok(_) => write!(f, "get summary response {{ {key} }}"),
                Err(e) => write!(f, "get summary failed {{ {key}, error: {e} }}"),
            },
            ContractHandlerEvent::GetDeltaQuery { key, .. } => {
                write!(f, "get delta query {{ {key} }}")
            }
            ContractHandlerEvent::GetDeltaResponse { key, delta } => match delta {
                Ok(_) => write!(f, "get delta response {{ {key} }}"),
                Err(e) => write!(f, "get delta failed {{ {key}, error: {e} }}"),
            },
            ContractHandlerEvent::ClientDisconnect { client_id } => {
                write!(f, "client disconnect {{ {client_id} }}")
            }
            ContractHandlerEvent::EvictContract {
                key,
                expected_generation,
            } => {
                write!(
                    f,
                    "evict contract {{ {key}, expected_generation: {expected_generation} }}"
                )
            }
            // `token` deliberately omitted (high-value credential). `user_id`
            // is a non-secret namespace tag, safe to render.
            ContractHandlerEvent::ExportUserSecrets { user_context, .. } => {
                write!(
                    f,
                    "export user secrets {{ user_id: {:?} }}",
                    user_context.user_id()
                )
            }
            ContractHandlerEvent::ExportUserSecretsResponse(result) => match result {
                Ok(bundle) => write!(
                    f,
                    "export user secrets response {{ {} bytes }}",
                    bundle.len()
                ),
                Err(e) => write!(f, "export user secrets failed {{ {e} }}"),
            },
            // `key_material` deliberately omitted (high-value credential); the
            // bundle is rendered as a byte count only (never its contents).
            ContractHandlerEvent::ImportSecrets {
                target_scope,
                bundle,
                key_kind,
                overwrite,
                ..
            } => write!(
                f,
                "import secrets {{ scope: {target_scope:?}, {} bytes, kind: {key_kind:?}, overwrite: {overwrite} }}",
                bundle.expose().len()
            ),
            ContractHandlerEvent::ImportSecretsResponse(result) => match result {
                Ok(report) => write!(
                    f,
                    "import secrets response {{ imported: {}, skipped: {} }}",
                    report.imported,
                    report.skipped.len()
                ),
                Err(e) => write!(f, "import secrets failed {{ {e} }}"),
            },
        }
    }
}

#[cfg(test)]
pub mod test {
    use freenet_stdlib::prelude::*;

    use super::*;
    use crate::config::GlobalExecutor;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn channel_test() -> anyhow::Result<()> {
        let (send_halve, mut rcv_halve, _) = contract_handler_channel();

        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2, 3])),
            Parameters::from(vec![4, 5]),
        )));

        let h = GlobalExecutor::spawn(async move {
            send_halve
                .send_to_handler(ContractHandlerEvent::PutQuery {
                    key: contract.key(),
                    state: vec![6, 7, 8].into(),
                    related_contracts: RelatedContracts::default(),
                    contract: Some(contract),
                })
                .await
        });
        let (id, ev, _priority) =
            tokio::time::timeout(Duration::from_millis(100), rcv_halve.recv_from_sender())
                .await??;

        let ContractHandlerEvent::PutQuery { state, .. } = ev else {
            anyhow::bail!("invalid event");
        };
        assert_eq!(state.as_ref(), &[6, 7, 8]);

        tokio::time::timeout(
            Duration::from_millis(100),
            rcv_halve.send_to_sender(
                id,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(vec![0, 7].into()),
                    state_changed: true,
                },
            ),
        )
        .await??;
        let ContractHandlerEvent::PutResponse {
            new_value,
            state_changed,
        } = h.await??
        else {
            anyhow::bail!("invalid event!");
        };
        let new_value = new_value.map_err(|e| anyhow::anyhow!(e))?;
        assert_eq!(new_value.as_ref(), &[0, 7]);
        assert!(state_changed);

        Ok(())
    }

    /// Regression test for issue #2278: Verifies that send_to_sender returns
    /// an error when the response receiver is dropped, but does NOT crash or
    /// break the channel.
    ///
    /// This tests that the channel infrastructure supports the fix in
    /// contract.rs where we changed `send_to_sender()?` to non-propagating
    /// error handling, so the handler loop can continue even when a response
    /// can't be delivered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_to_sender_fails_gracefully_when_receiver_dropped() -> anyhow::Result<()> {
        let (send_halve, mut rcv_halve, _) = contract_handler_channel();

        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2, 3])),
            Parameters::from(vec![4, 5]),
        )));
        let key = contract.key();

        // Send a request
        let h = GlobalExecutor::spawn({
            async move {
                send_halve
                    .send_to_handler(ContractHandlerEvent::PutQuery {
                        key,
                        state: vec![6, 7, 8].into(),
                        related_contracts: RelatedContracts::default(),
                        contract: None,
                    })
                    .await
            }
        });

        // Receive the event from the handler side
        let (id, ev, _priority) =
            tokio::time::timeout(Duration::from_millis(100), rcv_halve.recv_from_sender())
                .await??;

        // Verify it's a PutQuery
        let ContractHandlerEvent::PutQuery { state, .. } = ev else {
            anyhow::bail!("expected PutQuery event");
        };
        assert_eq!(state.as_ref(), &[6, 7, 8]);

        // Abort the request handler task - this drops the oneshot receiver
        // simulating a client disconnect
        h.abort();
        // Wait a bit for the abort to take effect
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Try to send the response - this should fail because the receiver was dropped
        // when we aborted the task. In the actual contract_handling loop (after the fix),
        // this would just log and continue rather than propagating the error.
        let send_result = rcv_halve
            .send_to_sender(
                id,
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(vec![0, 7].into()),
                    state_changed: true,
                },
            )
            .await;

        // send_to_sender should fail because receiver was dropped
        assert!(
            send_result.is_err(),
            "send_to_sender should fail when receiver is dropped, got {:?}",
            send_result
        );

        // Verify the error is the expected NoEvHandlerResponse
        // Use super:: to disambiguate from freenet_stdlib::prelude::ContractError
        assert!(
            matches!(send_result, Err(super::ContractError::NoEvHandlerResponse)),
            "Expected NoEvHandlerResponse error, got {:?}",
            send_result
        );

        Ok(())
    }

    /// Regression test for issue #3071: Verifies that `waiting_for_transaction_result`
    /// times out instead of blocking indefinitely when the `wait_for_res_tx` channel
    /// is full (the consumer at Priority 7 in the event loop is starved).
    #[tokio::test(start_paused = true)]
    async fn waiting_for_transaction_result_times_out_when_channel_full() {
        use crate::client_events::RequestId;
        use crate::message::Transaction;
        use crate::operations::put::PutMsg;

        let (send_halve, _rcv_halve, _wait_res) = contract_handler_channel();

        // Fill the channel to capacity. We hold _wait_res so
        // the receiver isn't dropped (which would give a different error).
        let cap = send_halve.end.wait_for_res_tx.max_capacity();
        for _ in 0..cap {
            let tx = Transaction::new::<PutMsg>();
            send_halve
                .end
                .wait_for_res_tx
                .send((ClientId::FIRST, WaitingTransaction::Transaction(tx)))
                .await
                .expect("channel should accept items up to capacity");
        }

        // The next send should block, and with paused time we can
        // advance past the timeout instantly.
        let tx = Transaction::new::<PutMsg>();
        let result = send_halve
            .waiting_for_transaction_result(tx, ClientId::FIRST, RequestId::new())
            .await;

        assert!(
            matches!(result, Err(super::ContractError::NoEvHandlerResponse)),
            "Expected NoEvHandlerResponse timeout error, got {:?}",
            result
        );
    }

    /// Regression test for issue #3246: Verifies that `notify_session_actor` delivers
    /// the `RegisterTransaction` message even when the session actor channel has
    /// backpressure, instead of silently dropping it via `try_send`.
    #[tokio::test]
    async fn notify_session_actor_delivers_under_backpressure() {
        use crate::client_events::RequestId;
        use crate::contract::SessionMessage;
        use crate::message::Transaction;
        use crate::operations::put::PutMsg;

        let (mut send_halve, _rcv_halve, _wait_res) = contract_handler_channel();

        // Use a tiny channel (capacity 1) to simulate backpressure.
        let (session_tx, mut session_rx) = tokio::sync::mpsc::channel::<SessionMessage>(1);
        send_halve.with_session_adapter(session_tx);

        // Fill the session channel to capacity.
        let filler_tx = Transaction::new::<PutMsg>();
        send_halve
            .notify_session_actor(filler_tx, ClientId::FIRST, RequestId::new())
            .await;

        // Channel is now full. Spawn a concurrent drain so .send().await unblocks.
        let drain_handle = tokio::spawn(async move {
            let mut received = Vec::new();
            while let Some(msg) = session_rx.recv().await {
                received.push(msg);
                if received.len() == 2 {
                    break;
                }
            }
            received
        });

        // This would have been silently dropped by the old try_send.
        let test_tx = Transaction::new::<PutMsg>();
        let test_client = ClientId::FIRST;
        let test_request_id = RequestId::new();
        send_halve
            .notify_session_actor(test_tx, test_client, test_request_id)
            .await;

        let received = drain_handle.await.expect("drain task should complete");
        assert_eq!(received.len(), 2, "Both messages should be delivered");

        // Verify the second message is our test registration.
        match &received[1] {
            SessionMessage::RegisterTransaction {
                tx,
                client_id,
                request_id,
            } => {
                assert_eq!(*tx, test_tx);
                assert_eq!(*client_id, test_client);
                assert_eq!(*request_id, test_request_id);
            }
            other @ SessionMessage::RegisterClient { .. }
            | other @ SessionMessage::DeliverResult { .. }
            | other @ SessionMessage::DeliverHostResponse { .. }
            | other @ SessionMessage::DeliverHostResponseWithRequestId { .. }
            | other @ SessionMessage::ClientDisconnect { .. } => panic!(
                "Expected RegisterTransaction, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    #[tokio::test]
    async fn try_recv_from_sender_empty_channel() {
        let (_send_halve, mut rcv_halve, _) = contract_handler_channel();

        // Empty channel should return Ok(None)
        let result = rcv_halve.try_recv_from_sender();
        assert!(matches!(result, Ok(None)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_recv_from_sender_receives_event() {
        let (send_halve, mut rcv_halve, _) = contract_handler_channel();

        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![10, 11, 12])),
            Parameters::from(vec![13]),
        )));
        let key = contract.key();

        // Send an event from another task
        let _h = GlobalExecutor::spawn(async move {
            send_halve
                .send_to_handler(ContractHandlerEvent::PutQuery {
                    key,
                    state: vec![20, 21].into(),
                    related_contracts: RelatedContracts::default(),
                    contract: None,
                })
                .await
        });

        // Wait briefly for the event to arrive
        tokio::time::sleep(Duration::from_millis(50)).await;

        // try_recv should succeed
        let result = rcv_halve.try_recv_from_sender();
        assert!(result.is_ok());
        let (id, event, _priority) = result.unwrap().expect("should have received an event");
        // Verify we got a valid EventId (any u64 is valid)

        let ContractHandlerEvent::PutQuery { state, .. } = event else {
            panic!("expected PutQuery event");
        };
        assert_eq!(state.as_ref(), &[20, 21]);

        // The waiting_response entry should be populated
        assert!(rcv_halve.end.waiting_response.contains_key(&id.id));
    }

    #[tokio::test]
    async fn try_recv_from_sender_disconnected() {
        let (send_halve, mut rcv_halve, _) = contract_handler_channel();

        // Drop the sender to disconnect the channel
        drop(send_halve);

        let result = rcv_halve.try_recv_from_sender();
        assert!(
            matches!(result, Err(super::ContractError::NoEvHandlerResponse)),
            "Expected NoEvHandlerResponse, got {:?}",
            result
        );
    }
}

pub(super) mod in_memory {
    use super::{
        super::{
            Executor, MockRuntime, executor::mock_wasm_runtime::MockWasmRuntime, storages::Storage,
        },
        ContractHandler, ContractHandlerChannel, ContractHandlerHalve,
    };
    use crate::node::OpManager;
    use crate::wasm_runtime::MockStateStorage;
    use std::sync::Arc;

    /// In-memory contract handler for testing with disk-based storage (default).
    pub(crate) struct MemoryContractHandler {
        channel: ContractHandlerChannel<ContractHandlerHalve>,
        runtime: Executor<MockRuntime, Storage>,
    }

    impl MemoryContractHandler {
        /// Create a new handler with disk-based storage (default, for backward compatibility).
        pub async fn new(
            channel: ContractHandlerChannel<ContractHandlerHalve>,
            op_manager: Option<Arc<OpManager>>,
            identifier: &str,
        ) -> Self {
            MemoryContractHandler {
                channel,
                runtime: Executor::new_mock(identifier, op_manager)
                    .await
                    .expect("should start mock executor"),
            }
        }
    }

    /// In-memory contract handler with shared in-memory storage for deterministic simulation.
    ///
    /// Unlike `MemoryContractHandler` which uses disk-based storage, this handler stores
    /// all state in memory using `MockStateStorage`. The Arc-based storage can be cloned
    /// and shared across node restarts to preserve state.
    pub(crate) struct SimulationContractHandler {
        channel: ContractHandlerChannel<ContractHandlerHalve>,
        runtime: Executor<MockRuntime, MockStateStorage>,
    }

    impl SimulationContractHandler {
        /// Create a new handler with shared in-memory storage for deterministic simulation.
        ///
        /// The `shared_storage` is an Arc-backed storage that persists across node restarts.
        /// Clone the same `MockStateStorage` instance to share state between restarts.
        pub async fn new(
            channel: ContractHandlerChannel<ContractHandlerHalve>,
            op_manager: Option<Arc<OpManager>>,
            identifier: &str,
            shared_storage: MockStateStorage,
        ) -> Self {
            SimulationContractHandler {
                channel,
                runtime: Executor::new_mock_in_memory(identifier, shared_storage, op_manager)
                    .await
                    .expect("should start mock in-memory executor"),
            }
        }
    }

    impl ContractHandler for MemoryContractHandler {
        type Builder = String;
        type ContractExecutor = Executor<MockRuntime, Storage>;

        async fn build(
            channel: ContractHandlerChannel<ContractHandlerHalve>,
            op_manager: Arc<OpManager>,
            identifier: Self::Builder,
        ) -> anyhow::Result<Self>
        where
            Self: Sized + 'static,
        {
            Ok(MemoryContractHandler::new(channel, Some(op_manager), &identifier).await)
        }

        fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve> {
            &mut self.channel
        }

        fn executor(&mut self) -> &mut Self::ContractExecutor {
            &mut self.runtime
        }
    }

    /// Builder for simulation contract handler that includes shared storage.
    pub struct SimulationHandlerBuilder {
        pub identifier: String,
        pub shared_storage: MockStateStorage,
    }

    impl ContractHandler for SimulationContractHandler {
        type Builder = SimulationHandlerBuilder;
        type ContractExecutor = Executor<MockRuntime, MockStateStorage>;

        async fn build(
            channel: ContractHandlerChannel<ContractHandlerHalve>,
            op_manager: Arc<OpManager>,
            builder: Self::Builder,
        ) -> anyhow::Result<Self>
        where
            Self: Sized + 'static,
        {
            Ok(SimulationContractHandler::new(
                channel,
                Some(op_manager),
                &builder.identifier,
                builder.shared_storage,
            )
            .await)
        }

        fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve> {
            &mut self.channel
        }

        fn executor(&mut self) -> &mut Self::ContractExecutor {
            &mut self.runtime
        }
    }

    /// Contract handler using MockWasmRuntime — exercises the production ContractExecutor
    /// code path (init_tracker, validation, notification pipeline, corrupted state recovery)
    /// without requiring real WASM binaries.
    #[allow(dead_code)]
    pub(crate) struct MockWasmContractHandler {
        channel: ContractHandlerChannel<ContractHandlerHalve>,
        runtime: Executor<MockWasmRuntime, MockStateStorage>,
    }

    #[cfg(test)]
    impl MockWasmContractHandler {
        /// Construct a handler over a `MockWasmRuntime` executor with an
        /// optional `op_manager`. Used by the head-of-line-blocking regression
        /// test (#4391) to drive the real `contract_handling` loop without
        /// standing up a full network `OpManager`.
        pub(crate) async fn new_test(
            channel: ContractHandlerChannel<ContractHandlerHalve>,
            op_manager: Option<Arc<OpManager>>,
            identifier: &str,
        ) -> Self {
            let runtime =
                Executor::new_mock_wasm(identifier, MockStateStorage::new(), None, op_manager)
                    .await
                    .expect("create MockWasmRuntime executor for test");
            Self { channel, runtime }
        }

        /// Mutable access to the executor's `MockWasmRuntime` so a test can
        /// install per-contract `validate_overrides` / `update_overrides`.
        pub(crate) fn runtime_mut(&mut self) -> &mut MockWasmRuntime {
            self.runtime.mock_runtime_mut()
        }
    }

    /// Builder for MockWasmContractHandler.
    #[allow(dead_code)]
    pub struct MockWasmHandlerBuilder {
        pub identifier: String,
        pub shared_storage: MockStateStorage,
        /// Pre-seeded contract store. If `None`, a fresh empty store is created.
        pub contract_store: Option<crate::wasm_runtime::InMemoryContractStore>,
    }

    impl ContractHandler for MockWasmContractHandler {
        type Builder = MockWasmHandlerBuilder;
        type ContractExecutor = Executor<MockWasmRuntime, MockStateStorage>;

        async fn build(
            channel: ContractHandlerChannel<ContractHandlerHalve>,
            op_manager: Arc<OpManager>,
            builder: Self::Builder,
        ) -> anyhow::Result<Self>
        where
            Self: Sized + 'static,
        {
            let runtime = Executor::new_mock_wasm(
                &builder.identifier,
                builder.shared_storage,
                builder.contract_store,
                Some(op_manager),
            )
            .await?;
            Ok(Self { channel, runtime })
        }

        fn channel(&mut self) -> &mut ContractHandlerChannel<ContractHandlerHalve> {
            &mut self.channel
        }

        fn executor(&mut self) -> &mut Self::ContractExecutor {
            &mut self.runtime
        }
    }

    #[test]
    fn serialization() -> anyhow::Result<()> {
        use freenet_stdlib::prelude::WrappedContract;
        let bytes = crate::util::test::random_bytes_1kb();
        let mut unstructured = arbitrary::Unstructured::new(&bytes);
        let contract: WrappedContract = unstructured.arbitrary()?;

        let serialized = bincode::serialize(&contract)?;
        let deser: WrappedContract = bincode::deserialize(&serialized)?;
        assert_eq!(deser.code(), contract.code());
        assert_eq!(deser.key(), contract.key());
        Ok(())
    }
}
