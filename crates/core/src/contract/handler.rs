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

use freenet_stdlib::client_api::{DelegateRequest, RequestError};
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
        // One shared source (`config::runtime_pool_size`) so the per-worker cache
        // budgets, which must divide by this to stay within the node's memory
        // limit, can never disagree with the pool actually created (#5268).
        let pool_size = crate::config::runtime_pool_size();

        tracing::info!(pool_size = %pool_size, "Creating RuntimePool");

        let executor = RuntimePool::new(config.clone(), op_manager.clone(), pool_size).await?;

        // Set up hosting storage reference for eviction cleanup
        // This must be done before loading the cache so evictions work correctly
        let storage = executor.state_store().inner().clone();
        op_manager.ring.set_hosting_storage(storage.clone());
        // Aggregate disk-usage tracker paths (#4683, #5007): the mode-resolved
        // contracts dir (WASM blobs) and the relocated wasmtime compile-cache
        // dir, which the disk budget counts, plus the database dir and the
        // webapp cache, which are measured and reported only. This is the only
        // place the tracker learns these paths. Seeded lazily on the first
        // sweep tick. Pinned by
        // `hosting_disk_paths_are_installed_from_the_node_config`.
        op_manager.ring.set_hosting_disk_paths(
            crate::ring::HostingDiskPaths {
                contracts_dir: config.contracts_dir(),
                wasmtime_cache_dir: config.wasmtime_cache_dir(),
                db_dir: config.db_dir(),
                webapp_cache_dir: config.ws_api.webapp_cache_dir.clone(),
            },
            config.hosting_disk_pct,
            config.max_hosting_disk,
        );
        // Share of the memory limit hosted contracts may hold in RAM (#5647).
        op_manager
            .ring
            .configure_resident_overhead_mem_share(config.hosting_mem_share);
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
        op_manager
            .neighbor_hosting
            .initialize_from_hosting_cache(hosted_keys.into_iter());

        // #4780: also rehydrate InterestManager local-hosting for every restored
        // hosted contract, so a client GET for a cached contract serves LOCALLY
        // (serve-DURING gate) instead of routing to the network, and the copy
        // re-joins InterestSync anti-entropy. The two restore steps above rebuild
        // the hosting cache and the advertised-host set but NOT the
        // InterestManager, which both the serve gate and the interest-sync
        // heartbeat key on. State-gated inside the helper (register-after-state,
        // #4782) so no phantom interested-but-stateless entry is created — the
        // hosting storage handle is set above (`set_hosting_storage`), so the
        // state-presence check is real here.
        //
        // Do NOT "fix" this instead by re-adding is_hosting_contract to the serve
        // gate (`client_events::should_serve_local_copy`) — that would serve
        // copies with no freshening path and re-introduce the #3698 stale-serve
        // bug. See .claude/rules/hosting-invariants.md (invariant 1).
        op_manager.rehydrate_local_hosting_interest();

        // Restore persisted delegate subscriptions (#5493). AFTER the hosting
        // cache and interest rehydration above, so a restored subscribe that
        // hits a locally hosted contract sees it as local; BEFORE this
        // function returns, which is before the event loop exists, so no
        // delegate can run while its subscriptions are still missing. The
        // registry half is synchronous; the network half is paced in the
        // background (`delegate_restore`). Inert when nothing was persisted.
        let restored = crate::wasm_runtime::delegate_subscriptions::restore_from_storage(&storage);
        super::delegate_restore::spawn_reestablish(op_manager.clone(), restored);

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
    /// Shared with the [`SenderHalve`]: `contract_handling` installs the
    /// delegate lane's intake here once it has started the lane.
    delegate_lane: DelegateLaneSlot,
    /// A WEAK handle on this channel's own event sender, for the delegate
    /// lane to forward requests into ([`LoopForwarder`]). Weak, so that it
    /// does not keep the channel open: the loop still sees the channel close
    /// when every client-side sender is gone.
    event_sender: mpsc::WeakUnboundedSender<InternalCHEvent>,
}

/// Where client delegate requests go instead of the contract loop, once the
/// loop has started a delegate lane (`contract::delegate_lane`). Empty until
/// then, and for executors that do not provide a delegate runner.
type DelegateLaneSlot = Arc<std::sync::OnceLock<DelegateLaneIntake>>;

/// A started delegate lane's intake channel.
pub(crate) struct DelegateLaneIntake {
    pub(crate) tx: mpsc::Sender<super::delegate_lane::LaneRequest>,
}

/// Puts a request the delegate lane does not run back on the loop's
/// ordinary path: the same event channel, with the same id, priority and
/// responder the handler would have sent it with before the lane existed.
pub(crate) struct LoopForwarder {
    event_sender: mpsc::WeakUnboundedSender<InternalCHEvent>,
}

impl super::delegate_lane::LoopForward for LoopForwarder {
    fn forward(
        &self,
        request: super::delegate_lane::LaneRequest,
        ticket: super::delegate_lane::LaneTicket,
    ) {
        let super::delegate_lane::LaneRequest {
            id,
            responder,
            req,
            origin_contract,
            connection_scope,
            user_context,
            priority,
        } = request;
        let event = InternalCHEvent {
            ev: ContractHandlerEvent::DelegateRequest {
                req,
                origin_contract,
                connection_scope,
                user_context,
                lane_ticket: ticket,
            },
            id,
            priority,
            result: responder,
        };
        // No loop left (node shutdown): the responder drops with the event,
        // so the client sees an error, and the loop-owned mark is moot.
        let sent = self
            .event_sender
            .upgrade()
            .is_some_and(|tx| tx.send(event).is_ok());
        if !sent {
            tracing::debug!(
                id,
                "Delegate lane: the contract loop is gone; dropping a request"
            );
        }
    }
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
    delegate_lane: DelegateLaneSlot,
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
    let delegate_lane: DelegateLaneSlot = Arc::new(std::sync::OnceLock::new());
    let weak_event_sender = event_sender.downgrade();
    (
        ContractHandlerChannel {
            end: SenderHalve {
                event_sender,
                wait_for_res_tx,
                delegate_lane: delegate_lane.clone(),
            },
            session_adapter_tx: None,
        },
        ContractHandlerChannel {
            end: ContractHandlerHalve {
                event_receiver,
                waiting_response: BTreeMap::new(),
                delegate_lane,
                event_sender: weak_event_sender,
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
        let internal = InternalCHEvent {
            ev,
            id,
            priority,
            result,
        };
        // Client delegate requests go to the delegate lane when one is
        // running, so they do not wait behind contract WASM on the loop.
        if let Some(internal) = self.offer_to_delegate_lane(internal) {
            self.end
                .event_sender
                .send(internal)
                .map_err(|err| ContractError::ChannelDropped(Box::new(err.0.ev)))?;
        }
        match tokio::time::timeout(timeout, result_receiver).await {
            Ok(Ok((_, res))) => Ok(res),
            Ok(Err(_)) | Err(_) => Err(ContractError::NoEvHandlerResponse),
        }
    }

    /// Hand a `DelegateRequest` to the delegate lane. Returns the event back
    /// when it is not a delegate request, when no lane is installed, or when
    /// the lane has exited, so the caller sends it to the loop as before.
    ///
    /// A FULL lane refuses the request: the responder is dropped, so the
    /// client sees an error at once, the same shape the fair queue's own
    /// refusal produces. Falling back to the loop instead would let a later
    /// request for the same delegate overtake it on the lane.
    fn offer_to_delegate_lane(&self, internal: InternalCHEvent) -> Option<InternalCHEvent> {
        use super::delegate_lane::LaneRequest;
        let Some(lane) = self.end.delegate_lane.get() else {
            return Some(internal);
        };
        let InternalCHEvent {
            ev,
            id,
            priority,
            result,
        } = internal;
        let ContractHandlerEvent::DelegateRequest {
            req,
            origin_contract,
            connection_scope,
            user_context,
            lane_ticket: _,
        } = ev
        else {
            return Some(InternalCHEvent {
                ev,
                id,
                priority,
                result,
            });
        };
        // The LOCAL reserve (`delegate_lane::LOCAL_INTAKE_RESERVE`): a
        // request that is not from the node's own local client may not take
        // the last slots. Check-then-act against concurrent senders: N remote
        // senders racing here can overshoot the reserve by up to N - 1 slots,
        // so the reserve is approximate, not exact.
        if !super::delegate_lane::is_local_client(connection_scope, user_context.as_ref())
            && lane.tx.capacity() <= super::delegate_lane::LOCAL_INTAKE_RESERVE
        {
            let total = super::delegate_lane::record_refusal();
            if super::delegate_lane::log_this_refusal(total) {
                tracing::warn!(
                    delegate_key = %req.key(),
                    total_refused = total,
                    "Refused a non-local delegate request: the delegate lane's intake \
                     is full apart from its local reserve. Logged at powers of two \
                     of the running total"
                );
            }
            // `result` (the responder) drops here: the client sees an error.
            return None;
        }
        let request = LaneRequest {
            id,
            responder: result,
            req,
            origin_contract,
            connection_scope,
            user_context,
            priority,
        };
        match lane.tx.try_send(request) {
            Ok(()) => None,
            Err(mpsc::error::TrySendError::Full(request)) => {
                let total = super::delegate_lane::record_refusal();
                if super::delegate_lane::log_this_refusal(total) {
                    tracing::warn!(
                        delegate_key = %request.req.key(),
                        capacity = super::delegate_lane::LANE_INTAKE_CAPACITY,
                        total_refused = total,
                        "Refused a delegate request: the delegate lane is full. \
                         Logged at powers of two of the running total"
                    );
                }
                None
            }
            Err(mpsc::error::TrySendError::Closed(request)) => {
                // No lane (its task exited): the request goes to the loop.
                // Counted, so traffic after a lane death does not look flat.
                // The count also ticks while the node shuts down, when the
                // lane is stopped with the loop. No loop-owned mark: the mark
                // only tells the lane to forward, and no lane is left to
                // check it.
                super::delegate_lane::record_forward(super::delegate_lane::ForwardReason::LaneDown);
                Some(InternalCHEvent {
                    ev: ContractHandlerEvent::DelegateRequest {
                        req: request.req,
                        origin_contract: request.origin_contract,
                        connection_scope: request.connection_scope,
                        user_context: request.user_context,
                        lane_ticket: Default::default(),
                    },
                    id: request.id,
                    priority: request.priority,
                    result: request.responder,
                })
            }
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

    /// Register a responder the delegate lane received directly, so the loop
    /// can answer a request the lane handed it exactly as it answers one it
    /// received itself.
    pub(crate) fn insert_waiting_response(
        &mut self,
        id: u64,
        responder: tokio::sync::oneshot::Sender<(EventId, ContractHandlerEvent)>,
    ) -> EventId {
        self.end.waiting_response.insert(id, responder);
        EventId { id }
    }

    /// Route client delegate requests to `lane` from now on. Called once by
    /// `contract_handling`; a second call is ignored.
    pub(crate) fn install_delegate_lane(&mut self, lane: DelegateLaneIntake) {
        if self.end.delegate_lane.set(lane).is_err() {
            tracing::warn!("A delegate lane is already installed; ignoring the second one");
        }
    }

    /// The handle the delegate lane forwards requests with.
    pub(crate) fn loop_forwarder(&self) -> LoopForwarder {
        LoopForwarder {
            event_sender: self.end.event_sender.clone(),
        }
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
        /// Whether the issuing connection may receive an ATTESTED application
        /// identity (GHSA-824h-7x5x-wfmf). Carried on the same forge-proof
        /// channel as `user_context`; the executor withholds every attested
        /// [`freenet_stdlib::prelude::MessageOrigin`] when this is
        /// [`ConnectionScope::Remote`].
        connection_scope: crate::client_events::ConnectionScope,
        /// Per-connection per-user secret namespace (hosted mode, P2 of #4381),
        /// derived once at the WS connection boundary from the connection's
        /// user token. `None` outside hosted mode or when no token was
        /// presented. Carried here on a separate channel from `req`/`origin_contract`
        /// so nothing in the request body can forge it. The owned
        /// `UserSecretContext` holds the `dek_secret` in `Zeroizing`; its
        /// `Debug` impl redacts the secret, so logging this event is safe.
        user_context: Option<UserSecretContext>,
        /// The delegate lane's loop-owned mark, when the lane forwarded this
        /// request (`delegate_lane::LaneTicket`); empty otherwise. Released
        /// wherever this event is dropped.
        lane_ticket: super::delegate_lane::LaneTicket,
    },
    /// `Err` on a genuine delegate execution failure (#5263 — previously
    /// every failure here silently became an empty successful
    /// `DelegateResponse`, so the client had no way to distinguish
    /// "the delegate answered nothing" from "the delegate failed").
    DelegateResponse(Result<Vec<OutboundDelegateMsg>, ExecutorError>),
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
    RegisterSubscriberListenerResponse {
        /// `Ok(())` when the subscriber notifier was registered, `Err` when
        /// registration was rejected (e.g. subscriber-limit exceeded). Carrying
        /// the result lets the client-facing caller surface a real failure
        /// instead of a fake successful subscribe (#4681).
        result: Result<(), Box<RequestError>>,
    },
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
    /// Read a contract's stored state and its summary as one snapshot.
    ///
    /// Both come from a single turn of the contract-handling loop, which is
    /// also where every state write runs, so no write can land between them.
    /// A `GetQuery` followed by a `GetSummaryQuery` gives no such guarantee:
    /// the summary can describe a state committed after the one returned.
    /// The broadcast fan-out needs the pair to agree, because it records the
    /// summary as what the peer now holds (#5842).
    GetStateAndSummaryQuery {
        key: ContractKey,
    },
    /// Response to a GetStateAndSummaryQuery
    GetStateAndSummaryResponse {
        key: ContractKey,
        snapshot: Result<(WrappedState, StateSummary<'static>), ExecutorError>,
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
            ContractHandlerEvent::RegisterSubscriberListenerResponse { result } => match result {
                Ok(()) => write!(f, "register subscriber listener response"),
                Err(e) => write!(f, "register subscriber listener failed {{ {e} }}"),
            },
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
            ContractHandlerEvent::GetStateAndSummaryQuery { key } => {
                write!(f, "get state and summary query {{ {key} }}")
            }
            ContractHandlerEvent::GetStateAndSummaryResponse { key, snapshot } => match snapshot {
                Ok(_) => write!(f, "get state and summary response {{ {key} }}"),
                Err(e) => write!(f, "get state and summary failed {{ {key}, error: {e} }}"),
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

    fn lane_delegate_request() -> ContractHandlerEvent {
        let key = DelegateKey::new([9u8; 32], CodeHash::new([9u8; 32]));
        ContractHandlerEvent::DelegateRequest {
            req: freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                key,
                params: Parameters::from(Vec::new()),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(b"payload".to_vec()),
                )],
            },
            origin_contract: None,
            connection_scope: crate::client_events::ConnectionScope::Local,
            user_context: None,
            lane_ticket: Default::default(),
        }
    }

    fn lane_filler() -> crate::contract::delegate_lane::LaneRequest {
        let ContractHandlerEvent::DelegateRequest {
            req,
            origin_contract,
            connection_scope,
            user_context,
            lane_ticket: _,
        } = lane_delegate_request()
        else {
            unreachable!()
        };
        let (responder, rx) = tokio::sync::oneshot::channel();
        std::mem::forget(rx);
        crate::contract::delegate_lane::LaneRequest {
            id: u64::MAX,
            responder,
            req,
            origin_contract,
            connection_scope,
            user_context,
            priority: super::super::fair_queue::Priority::ClientLocal,
        }
    }

    /// A FULL lane intake refuses a delegate request at once (the client gets
    /// an error) and does NOT fall back to the loop, which would let a later
    /// request for the same delegate overtake it on the lane.
    ///
    /// FALSIFY: in `offer_to_delegate_lane`, return `Some(..)` (fall back) from
    /// the `Full` arm; the request reaches the loop's channel.
    #[tokio::test]
    async fn a_full_delegate_lane_refuses_without_falling_back() -> anyhow::Result<()> {
        let (send, mut rcv, _) = contract_handler_channel();
        let (tx, mut lane_rx) = mpsc::channel(1);
        tx.try_send(lane_filler()).expect("room for the filler");
        rcv.install_delegate_lane(DelegateLaneIntake { tx });
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            send.send_to_handler(lane_delegate_request()),
        )
        .await?;
        assert!(
            matches!(result, Err(super::ContractError::NoEvHandlerResponse)),
            "a full lane refuses the request with an error, got {result:?}"
        );
        assert!(
            rcv.try_recv_from_sender()?.is_none(),
            "a refused request must not fall back to the loop"
        );
        assert_eq!(
            lane_rx.try_recv()?.id,
            u64::MAX,
            "only the filler is queued"
        );
        assert!(lane_rx.try_recv().is_err());
        Ok(())
    }

    /// A CLOSED lane (its task died) sends the request to the loop instead,
    /// with the same event id, the same priority and the client's own
    /// responder, so the loop's answer reaches the client.
    ///
    /// The fallback is counted as `LaneDown`, so traffic after a lane death
    /// does not read as no traffic. The counter is process-global and only
    /// increases, so the assertion is on a delta.
    ///
    /// FALSIFY: refuse in the `Closed` arm (return `None`), or rebuild the
    /// event with a fresh id/responder (the client is not answered); or drop
    /// the `record_forward(LaneDown)` call (the count does not move).
    #[tokio::test]
    async fn a_closed_delegate_lane_falls_back_to_the_loop() -> anyhow::Result<()> {
        use crate::contract::delegate_lane::{ForwardReason, routing_counts};
        let down_before = routing_counts().forwarded(ForwardReason::LaneDown);
        let (send, mut rcv, _) = contract_handler_channel();
        let (tx, lane_rx) = mpsc::channel(8);
        drop(lane_rx);
        rcv.install_delegate_lane(DelegateLaneIntake { tx });
        let client = GlobalExecutor::spawn(async move {
            send.send_to_handler_prioritized(
                lane_delegate_request(),
                crate::contract::fair_queue::Priority::ClientLocal,
            )
            .await
        });
        let (id, ev, priority) =
            tokio::time::timeout(Duration::from_secs(5), rcv.recv_from_sender()).await??;
        assert!(
            matches!(ev, ContractHandlerEvent::DelegateRequest { .. }),
            "the loop receives the delegate request, got {ev}"
        );
        assert!(
            routing_counts().forwarded(ForwardReason::LaneDown) > down_before,
            "the fallback is counted as LaneDown"
        );
        assert_eq!(priority, crate::contract::fair_queue::Priority::ClientLocal);
        rcv.send_to_sender(id, ContractHandlerEvent::DelegateResponse(Ok(Vec::new())))
            .await?;
        let answer = tokio::time::timeout(Duration::from_secs(5), client).await???;
        assert!(
            matches!(answer, ContractHandlerEvent::DelegateResponse(Ok(_))),
            "the loop's answer reaches the client through its own responder"
        );
        Ok(())
    }

    /// A flood of requests that are not from the node's own local client
    /// (remote connections, or hosted users arriving through the loopback
    /// proxy) fills the lane's intake only up to its local reserve, so a
    /// local request is still admitted.
    ///
    /// The flood is sent ONE request at a time, each settled (queued in the
    /// intake or refused) before the next is sent. The reserve check is
    /// check-then-act, so concurrent senders may overshoot it (see
    /// `offer_to_delegate_lane`); sequential senders make the boundary
    /// exact, which is what lets this test assert it exactly.
    ///
    /// FALSIFY: drop the local-reserve refusal in `offer_to_delegate_lane`;
    /// no remote request is refused and the local one is refused instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_remote_flood_leaves_the_local_reserve() -> anyhow::Result<()> {
        use crate::contract::delegate_lane::{LANE_INTAKE_CAPACITY, LOCAL_INTAKE_RESERVE};
        for hosted in [false, true] {
            let (send, mut rcv, _) = contract_handler_channel();
            let (tx, lane_rx) = mpsc::channel(LANE_INTAKE_CAPACITY);
            rcv.install_delegate_lane(DelegateLaneIntake { tx });
            let send = Arc::new(send);
            let refused = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut tasks = Vec::new();
            for sent in 1..=LANE_INTAKE_CAPACITY {
                let (s, r) = (send.clone(), refused.clone());
                let ContractHandlerEvent::DelegateRequest {
                    req,
                    origin_contract,
                    ..
                } = lane_delegate_request()
                else {
                    panic!("lane_delegate_request builds a DelegateRequest");
                };
                let ev = ContractHandlerEvent::DelegateRequest {
                    req,
                    origin_contract,
                    connection_scope: if hosted {
                        crate::client_events::ConnectionScope::Local
                    } else {
                        crate::client_events::ConnectionScope::Remote
                    },
                    user_context: hosted.then(|| UserSecretContext::from_token(b"hosted")),
                    lane_ticket: Default::default(),
                };
                tasks.push(GlobalExecutor::spawn(async move {
                    if s.send_to_handler(ev).await.is_err() {
                        r.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }));
                // Settle this request before sending the next.
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                while refused.load(std::sync::atomic::Ordering::SeqCst) + lane_rx.len() < sent {
                    anyhow::ensure!(
                        tokio::time::Instant::now() < deadline,
                        "remote request {sent} neither queued nor refused (hosted={hosted})"
                    );
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            assert_eq!(
                refused.load(std::sync::atomic::Ordering::SeqCst),
                LOCAL_INTAKE_RESERVE,
                "exactly the reserve is refused to non-local requests (hosted={hosted})"
            );
            assert_eq!(lane_rx.len(), LANE_INTAKE_CAPACITY - LOCAL_INTAKE_RESERVE);
            let s = send.clone();
            let local =
                GlobalExecutor::spawn(
                    async move { s.send_to_handler(lane_delegate_request()).await },
                );
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while lane_rx.len() < LANE_INTAKE_CAPACITY - LOCAL_INTAKE_RESERVE + 1 {
                anyhow::ensure!(
                    tokio::time::Instant::now() < deadline && !local.is_finished(),
                    "the local request must be queued, not refused (hosted={hosted})"
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            local.abort();
            for t in tasks {
                t.abort();
            }
        }
        Ok(())
    }

    /// The lane's forwarder puts a request on the loop's ordinary path
    /// exactly as the handler would have: same id, same priority, and the
    /// client's own responder, so the loop's answer reaches the client.
    ///
    /// The lane's loop-owned mark (`LaneTicket`) rides in the event, and is
    /// released only when the event is dropped.
    ///
    /// FALSIFY: in `LoopForwarder::forward`, send with `Priority::DEFAULT`
    /// instead of the request's priority, or a fresh responder, or
    /// `lane_ticket: Default::default()`; the assertions on priority / the
    /// client's answer / the mark fail.
    #[tokio::test]
    async fn a_forwarded_request_reaches_the_loop_as_the_client_sent_it() -> anyhow::Result<()> {
        use crate::contract::delegate_lane::LoopForward;
        let (_send, mut rcv, _) = contract_handler_channel();
        let forwarder = rcv.loop_forwarder();
        let (mut request, answer) = {
            let (responder, rx) = tokio::sync::oneshot::channel();
            let mut r = lane_filler();
            r.responder = responder;
            (r, rx)
        };
        request.id = 4242;
        request.priority = crate::contract::fair_queue::Priority::Background;
        let key = request.req.key().clone();
        let gate = crate::contract::delegate_lane::DelegateGate::new();
        forwarder.forward(request, gate.ticket_for_test(&key, None));
        let (id, ev, priority) =
            tokio::time::timeout(Duration::from_secs(5), rcv.recv_from_sender()).await??;
        assert_eq!(id.id, 4242);
        assert_eq!(priority, crate::contract::fair_queue::Priority::Background);
        let ContractHandlerEvent::DelegateRequest { lane_ticket, .. } = &ev else {
            panic!("the loop receives the delegate request, got {ev}");
        };
        assert!(lane_ticket.is_held(), "the mark rides in the event");
        assert!(gate.is_tracking(&key));
        drop(ev);
        assert!(
            !gate.is_tracking(&key),
            "dropping the event releases the mark"
        );
        rcv.send_to_sender(id, ContractHandlerEvent::DelegateResponse(Ok(Vec::new())))
            .await?;
        let (_, answer) = tokio::time::timeout(Duration::from_secs(5), answer).await??;
        assert!(matches!(
            answer,
            ContractHandlerEvent::DelegateResponse(Ok(_))
        ));
        Ok(())
    }

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

    /// Call-site pin: the disk-usage tracker must be installed with all four of
    /// the node's own directories (#4683, #5007).
    ///
    /// The tracker cannot walk a directory it was never given, so a field
    /// dropped here, or wired to the wrong accessor, silently reproduces the
    /// blind spots #5007 closes (a database whose dead space nothing reports, a
    /// webapp cache nothing measures), or mis-sizes the budgeted WASM and
    /// compile-cache gauges, while every other test stays green.
    ///
    /// Nothing else can catch it. The `HostingDiskPaths` fields are all
    /// `PathBuf`, so the type system permits both omission (via struct-update
    /// syntax) and transposition; and a behavioral test would have to boot a
    /// real node with a real store, which is what this whole file's `build` path
    /// does only in integration tests that do not assert on disk accounting.
    ///
    /// Read against the comment-stripped, whitespace-collapsed production half
    /// of this file so a rustfmt re-wrap of the call cannot rot the needles, and
    /// the needles are assembled from fragments so this test cannot match its
    /// own source through `include_str!`.
    #[test]
    fn hosting_disk_paths_are_installed_from_the_node_config() {
        let src = production_source();

        let call = concat!("set_hosting_disk", "_paths(");
        assert_eq!(
            src.matches(call).count(),
            1,
            "pin guard: expected exactly one disk-paths install in this file, so \
             this test is asserting against the wrong thing"
        );
        let args = call_arguments(&src, call);

        for (field, accessor) in [
            // Pre-#5007 pair. Present so the pin covers the whole struct, not
            // only the fields the issue added.
            (
                concat!("contracts", "_dir:"),
                concat!("config.contracts", "_dir()"),
            ),
            (
                concat!("wasmtime_cache", "_dir:"),
                concat!("config.wasmtime_cache", "_dir()"),
            ),
            // #5007 blind spot 1: the storage backend's database directory. Its
            // measured size is the only term that can see redb's dead space.
            (concat!("db", "_dir:"), concat!("config.db", "_dir()")),
            // #5007 blind spot 2: the unpacked-webapp cache. Reported, not
            // budgeted — but it has to be measured to be reported at all, and it
            // must come from the node's OWN config, never the process-wide
            // default, for the same reason `server.rs` threads it.
            (
                concat!("webapp_cache", "_dir:"),
                concat!("config.ws_api.webapp_cache", "_dir"),
            ),
        ] {
            // Field and accessor must be ADJACENT, so a transposition (two
            // fields swapping accessors) fails here as well as an omission.
            // Whitespace is already stripped from `args`.
            let pair = format!("{field}{accessor}");
            assert!(
                args.contains(&pair),
                "the disk tracker must be installed with `{field}` resolved from \
                 the node's config via `{accessor}`; found arguments `{args}`"
            );
        }

        // The substitution template must not exist on this path at all: the
        // process-wide webapp-cache default would measure some other node's
        // directory (or the developer's) instead of this one's.
        // Checked against the RAW production text, not the comment-stripped
        // one: stripping can only remove text, which an absence check would
        // read as a pass. A mention in a comment fails this, which is the safe
        // direction.
        const FULL: &str = include_str!("handler.rs");
        let raw_production = &FULL[..FULL
            .find(concat!("pub mod ", "test {"))
            .expect("handler.rs must have a test module")];
        assert!(
            !raw_production.contains(concat!("default_webapp", "_cache_dir")),
            "the node's disk accounting must never resolve the process-wide \
             webapp cache default — it has to measure the directory this node \
             actually serves from"
        );
    }

    /// Production half of this file, comment-stripped and whitespace-collapsed,
    /// for the source pin above.
    ///
    /// Cut at the test module's `mod` line, NOT at `#[cfg(test)]`: that
    /// attribute also sits on individual test-only items earlier in this file,
    /// so cutting there truncates the slice the pin reads and turns it vacuous
    /// rather than red. Comments are dropped before collapsing so a `//` line
    /// cannot run into the code below it, and whitespace is collapsed so a
    /// rustfmt re-wrap of a call's arguments cannot rot a needle. The needle for
    /// the cut is split so it cannot match its own source.
    fn production_source() -> String {
        const FULL: &str = include_str!("handler.rs");
        let cutoff = FULL
            .find(concat!("pub mod ", "test {"))
            .expect("handler.rs must have a test module");
        // Line comments first, so a `/*` inside one cannot open a block that
        // swallows code; then `/* */` blocks, so a call wrapped in one cannot
        // satisfy the pin. Both can only remove text, which turns a presence
        // check red; an exactly-once count could miss a second call hidden
        // behind a `"//"` or `"/*"` inside a string literal (none exists in
        // this file's production half).
        let no_line = FULL[..cutoff]
            .lines()
            .map(|line| line.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let mut production = String::with_capacity(no_line.len());
        let mut rest = no_line.as_str();
        while let Some(start) = rest.find(concat!("/", "*")) {
            production.push_str(&rest[..start]);
            rest = match rest[start..].find(concat!("*", "/")) {
                Some(end) => &rest[start + end + 2..],
                None => "",
            };
        }
        production.push_str(rest);
        production.chars().filter(|c| !c.is_whitespace()).collect()
    }

    /// The balanced-parenthesis argument list of the (single) call to
    /// `call_prefix` within `src`.
    fn call_arguments(src: &str, call_prefix: &str) -> String {
        let start = src
            .find(call_prefix)
            .unwrap_or_else(|| panic!("could not find {call_prefix}"))
            + call_prefix.len();
        let mut depth: i32 = 1;
        for (offset, ch) in src[start..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return src[start..start + offset].to_string();
                    }
                }
                _ => {}
            }
        }
        panic!("unterminated call to {call_prefix}");
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

        /// As [`Self::new_test`], over a caller-supplied `MockStateStorage` so the
        /// test can count the writes the executor makes (`store_count`).
        pub(crate) async fn new_test_with_storage(
            channel: ContractHandlerChannel<ContractHandlerHalve>,
            op_manager: Option<Arc<OpManager>>,
            identifier: &str,
            storage: MockStateStorage,
        ) -> Self {
            let runtime = Executor::new_mock_wasm(identifier, storage, None, op_manager)
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
