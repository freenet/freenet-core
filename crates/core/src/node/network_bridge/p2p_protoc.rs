use anyhow::anyhow;
use dashmap::DashSet;
use either::{Either, Left, Right};
use futures::FutureExt;
use futures::StreamExt;
use std::convert::Infallible;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::time::Duration;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TryRecvError};
use tokio::time::Instant;
use tokio::time::{sleep, timeout};
use tracing::Instrument;

use super::{ConnectionError, EventLoopNotificationsReceiver, NetworkBridge};
use crate::contract::{ContractHandlerEvent, WaitingTransaction};
use crate::message::{NetMessageV1, QueryResult};
use crate::node::network_bridge::handshake::{
    Command as HandshakeCommand, CommandSender as HandshakeCommandSender, Event as HandshakeEvent,
    HandshakeHandler,
};
use crate::node::network_bridge::priority_select;
use crate::operations::connect::ConnectMsg;
#[cfg(feature = "simulation_tests")]
use crate::ring::PeerKey;
use crate::ring::{Distance, Location};
use crate::transport::{
    BroadcastDeliveryOutcome, CongestionControlConfig, ExpectedInboundTracker, PeerConnectionApi,
    Socket, TransportError, TransportKeypair, TransportPublicKey, create_connection_handler,
    global_bandwidth::GlobalBandwidthManager, peer_connection::StreamId,
};
use crate::{
    client_events::ClientId,
    config::GlobalExecutor,
    contract::{ContractHandlerChannel, ExecutorTransactionStream, SenderHalve, WaitingResolution},
    message::{MessageStats, NetMessage, NodeEvent, Transaction, TransactionType},
    node::{
        NetEventRegister, NodeConfig, OpManager, PeerId, WaiterReply, process_message_decoupled,
    },
    ring::{KnownPeerKeyLocation, PeerConnectionBackoff, PeerKeyLocation},
    tracing::NetEventLog,
};
use freenet_stdlib::client_api::{ContractResponse, HostResponse};

mod broadcast;
mod connection_lifecycle;
mod dispatch;
mod migration;

/// Returns the process RSS (Resident Set Size) in bytes by reading /proc/self/statm.
/// Returns None on non-Linux platforms or if the read fails.
fn get_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let rss_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        // SAFETY: `sysconf(_SC_PAGESIZE)` is a POSIX-defined, signal-safe
        // call that reads a system constant and has no preconditions.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size > 0 {
            Some(rss_pages * page_size as u64)
        } else {
            None
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Represents the different ways the event loop can exit.
///
/// This enum is used instead of string-based error matching to provide
/// type-safe handling of shutdown conditions.
#[derive(Debug)]
pub enum EventLoopExitReason {
    /// Graceful shutdown via NodeEvent::Disconnect or ClosedChannel
    GracefulShutdown,
    /// Unexpected stream termination (priority_select returned None)
    UnexpectedStreamEnd,
}

impl std::fmt::Display for EventLoopExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventLoopExitReason::GracefulShutdown => write!(f, "Graceful shutdown"),
            EventLoopExitReason::UnexpectedStreamEnd => {
                write!(f, "Network event stream ended unexpectedly")
            }
        }
    }
}

impl std::error::Error for EventLoopExitReason {}

/// Upper bound on how long the network event listener will wait for the contract
/// handler to answer a *diagnostics* `QuerySubscriptions` query (#4549).
///
/// These queries run inline on the event loop. The handler's own response timeout
/// is `CH_EV_RESPONSE_TIME_OUT` (300s); without an outer bound a saturated handler
/// would stall the entire network event loop for up to 5 minutes per diagnostics
/// poll. A diagnostics query is best-effort, so cap the wait and serve an empty
/// result on timeout rather than freezing (or, previously, killing) the listener.
const QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT: Duration = Duration::from_secs(5);

/// Best-effort, bounded query to the contract handler for application-level
/// subscriptions, used by the diagnostics arms of the network event loop (#4549).
///
/// This runs INLINE on the network event loop, so it must be both bounded and
/// non-fatal: a saturated contract handler previously made the bare
/// `notify_contract_handler(QuerySubscriptions ...).await?` return
/// `NoEvHandlerResponse` only after the 300s handler timeout, which killed the
/// whole network event listener ("Network event listener exited: no response
/// received from handler") and took the gateway network-dead. Here the wait is
/// capped at [`QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT`] and ANY failure (timeout,
/// channel error, missing response) yields an empty list. A genuinely-dead
/// contract handler causes the `contract_executor_task` to exit, which the
/// `run_node` supervisor observes via its `deterministic_select!` and brings the
/// node down — so de-fatalizing this diagnostics query does not mask a dead
/// handler.
async fn query_app_subscriptions_bounded(
    ch_outbound: &ContractHandlerChannel<SenderHalve>,
) -> Vec<crate::message::SubscriptionInfo> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    match timeout(
        QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT,
        ch_outbound.send_to_handler(ContractHandlerEvent::QuerySubscriptions { callback: tx }),
    )
    .await
    {
        Ok(Ok(_)) => match timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(QueryResult::NetworkDebug(info))) => info.application_subscriptions,
            _ => Vec::new(),
        },
        Ok(Err(err)) => {
            tracing::warn!(
                error = %err,
                "QuerySubscriptions diagnostics: contract handler error; \
                 serving empty app subscriptions"
            );
            Vec::new()
        }
        Err(_elapsed) => {
            tracing::warn!(
                "QuerySubscriptions diagnostics: contract handler did not respond within \
                 timeout; serving empty app subscriptions"
            );
            Vec::new()
        }
    }
}

pub(crate) enum P2pBridgeEvent {
    Message(PeerKeyLocation, Box<NetMessage>),
    NodeAction(NodeEvent),
    StreamSend {
        target_addr: SocketAddr,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        /// Optional completion signal for broadcast queue concurrency control.
        /// Signaled when the actual stream transfer finishes (not just enqueue),
        /// carrying a [`BroadcastDeliveryOutcome`] so the queue distinguishes a
        /// real delivery from a drop (#4235).
        completion_tx: Option<tokio::sync::oneshot::Sender<BroadcastDeliveryOutcome>>,
        /// Optional per-fragment progress handle for the streaming-PUT retry
        /// loop (#4001). Threaded through to `send_stream` so the originator's
        /// retry loop sees liveness ticks. `None` for every non-originator path.
        progress: Option<crate::operations::stream_progress::StreamProgressHandle>,
    },
    /// Pipe an inbound stream to a target, forwarding fragments incrementally.
    PipeStream {
        target_addr: SocketAddr,
        outbound_stream_id: StreamId,
        inbound_handle: crate::transport::peer_connection::streaming::StreamHandle,
        metadata: Option<bytes::Bytes>,
    },
}

impl std::fmt::Debug for P2pBridgeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(peer, msg) => f.debug_tuple("Message").field(peer).field(msg).finish(),
            Self::NodeAction(action) => f.debug_tuple("NodeAction").field(action).finish(),
            Self::StreamSend {
                target_addr,
                stream_id,
                data,
                metadata,
                ..
            } => f
                .debug_struct("StreamSend")
                .field("target_addr", target_addr)
                .field("stream_id", stream_id)
                .field("data_len", &data.len())
                .field("has_metadata", &metadata.is_some())
                .finish(),
            Self::PipeStream {
                target_addr,
                outbound_stream_id,
                ..
            } => f
                .debug_struct("PipeStream")
                .field("target_addr", target_addr)
                .field("outbound_stream_id", outbound_stream_id)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct P2pBridge {
    #[allow(dead_code)]
    accepted_peers: Arc<DashSet<SocketAddr>>,
    ev_listener_tx: Sender<P2pBridgeEvent>,
    op_manager: Arc<OpManager>,
    log_register: Arc<dyn NetEventRegister>,
    /// Configured gateways with their public keys, used for key lookup during reconnection.
    gateways: Arc<Vec<PeerKeyLocation>>,
}

impl P2pBridge {
    fn new<EL>(
        sender: Sender<P2pBridgeEvent>,
        op_manager: Arc<OpManager>,
        event_register: EL,
        gateways: Arc<Vec<PeerKeyLocation>>,
    ) -> Self
    where
        EL: NetEventRegister,
    {
        Self {
            accepted_peers: Arc::new(DashSet::new()),
            ev_listener_tx: sender,
            op_manager,
            log_register: Arc::new(event_register),
            gateways,
        }
    }

    /// Wake any drivers whose downstream peer just disconnected.
    ///
    /// For each orphaned tx, emit `NodeEvent::TransactionOrphaned`. The
    /// event-loop handler delivers `WaiterReply::PeerDisconnected` into the
    /// waiter channel *before* dropping the sender, so the parked driver
    /// reads the cause deterministically and surfaces
    /// `OpError::PeerDisconnected`, which the retry loop advances past to
    /// the next peer (#4313).
    ///
    /// Without the wake at all, a GET issued just before its peer
    /// disconnected stalls for `OPERATION_TTL` (60 s) — see #4154.
    pub(crate) async fn handle_orphaned_transactions(
        &self,
        transactions: Vec<Transaction>,
        disconnected_peer_addr: SocketAddr,
    ) {
        if transactions.is_empty() {
            return;
        }
        tracing::debug!(
            count = transactions.len(),
            peer = %disconnected_peer_addr,
            "Orphaned transactions from pruned connection — waking parked drivers"
        );
        for tx in transactions {
            self.op_manager
                .notify_orphaned_transaction(tx, disconnected_peer_addr);
        }
    }

    /// Send stream data with an explicit completion signal.
    ///
    /// Like `NetworkBridge::send_stream` but allows passing a `completion_tx`
    /// that will be signaled when the actual stream transfer finishes at the
    /// transport layer. Used by the broadcast queue for concurrency control.
    pub(super) async fn send_stream_with_completion(
        &self,
        target_addr: SocketAddr,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        completion_tx: Option<tokio::sync::oneshot::Sender<BroadcastDeliveryOutcome>>,
        progress: Option<crate::operations::stream_progress::StreamProgressHandle>,
    ) -> super::ConnResult<()> {
        // channel-safety: ok — sole caller is the broadcast-queue task
        // (broadcast_to_single_peer), a detached spawned task, not the event
        // loop; the StreamSend it enqueues is drained by the loop, so it cannot
        // self-stall it. Grandfathered enqueue; the #4001 `progress` field that
        // brought this statement into the diff does not change the send path.
        self.ev_listener_tx
            .send(P2pBridgeEvent::StreamSend {
                target_addr,
                stream_id,
                data,
                metadata,
                completion_tx,
                progress,
            })
            .await
            .map_err(|_| ConnectionError::SendNotCompleted(target_addr))?;
        Ok(())
    }

    /// Resolve the `PeerKeyLocation` to address a message to `target_addr`,
    /// preferring the connection-manager record, then a configured gateway key,
    /// then (as a normal transient fallback during handshake) our own key.
    fn resolve_target(&self, target_addr: SocketAddr) -> PeerKeyLocation {
        if let Some(target) = self
            .op_manager
            .ring
            .connection_manager
            .get_peer_by_addr(target_addr)
        {
            return target;
        }
        if let Some(gw_peer) = self
            .gateways
            .iter()
            .find(|gw| gw.socket_addr() == Some(target_addr))
            .cloned()
        {
            tracing::debug!(
                peer_addr = %target_addr,
                phase = "send",
                "Using gateway public key for reconnection"
            );
            return gw_peer;
        }
        // Truly unknown peer - use own key as last resort. This is a normal
        // transient state during connection handshakes when the peer's public
        // key hasn't been established yet.
        tracing::debug!(
            peer_addr = %target_addr,
            phase = "send",
            "Sending to unknown peer address with no known public key"
        );
        PeerKeyLocation::new(
            (*self.op_manager.ring.connection_manager.pub_key).clone(),
            target_addr,
        )
    }

    /// Non-blocking variant of [`NetworkBridge::send`] for callers that run
    /// INLINE on the network event-loop task.
    ///
    /// `send` does `ev_listener_tx.send(..).await` on the cap-512 bridge channel
    /// that the event loop itself drains; awaiting it from an on-loop caller is a
    /// self-deadlock cycle under fan-out back-pressure (#4145). This variant uses
    /// `try_send`: on a full or closed channel it drops the message and returns
    /// `SendNotCompleted` so the caller can log it. The dropped message is
    /// recovered by the caller's own path (op retry, or the periodic
    /// InterestSync / subscription-renewal re-sync for interest/cache fan-out) —
    /// see each call site's rationale. Returns `Ok(())` on a successful enqueue.
    fn try_send(&self, target_addr: SocketAddr, msg: NetMessage) -> super::ConnResult<()> {
        let target = self.resolve_target(target_addr);
        self.op_manager.sending_transaction(&target, &msg);
        match self
            .ev_listener_tx
            .try_send(P2pBridgeEvent::Message(target, Box::new(msg)))
        {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    peer_addr = %target_addr,
                    phase = "backpressure",
                    "Event-loop bridge channel full; dropped outbound message from an \
                     on-loop caller (will be recovered by op retry / periodic re-sync). \
                     Sustained occurrences indicate fan-out / migration load."
                );
                Err(ConnectionError::SendNotCompleted(target_addr))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!(
                    peer_addr = %target_addr,
                    phase = "send",
                    "Event-loop bridge channel closed; dropping outbound message"
                );
                Err(ConnectionError::SendNotCompleted(target_addr))
            }
        }
    }
}

impl NetworkBridge for P2pBridge {
    async fn drop_connection(&mut self, peer_addr: SocketAddr) -> super::ConnResult<()> {
        if self.accepted_peers.remove(&peer_addr).is_some() {
            self.ev_listener_tx
                .send(P2pBridgeEvent::NodeAction(NodeEvent::DropConnection(
                    peer_addr,
                )))
                .await
                .map_err(|_| ConnectionError::SendNotCompleted(peer_addr))?;
            // Log disconnect with PeerKeyLocation if we can construct one
            if let Some(peer_loc) = self
                .op_manager
                .ring
                .connection_manager
                .get_peer_by_addr(peer_addr)
            {
                // Peer from connection manager should have known address
                if let Ok(known_loc) = KnownPeerKeyLocation::try_from(&peer_loc) {
                    use crate::tracing::DisconnectReason;
                    // Capture connection duration before the connection is removed
                    let connection_duration_ms = self
                        .op_manager
                        .ring
                        .connection_manager
                        .get_connection_duration_ms(peer_addr);
                    if let Some(event) = NetEventLog::disconnected_with_context(
                        &self.op_manager.ring,
                        &PeerId::new(peer_loc.pub_key().clone(), known_loc.socket_addr()),
                        DisconnectReason::RemoteDropped,
                        connection_duration_ms,
                        None, // bytes_sent not tracked yet
                        None, // bytes_received not tracked yet
                    ) {
                        self.log_register.register_events(Either::Left(event)).await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn send(&self, target_addr: SocketAddr, msg: NetMessage) -> super::ConnResult<()> {
        // Note: outbound event tracing (UnsubscribeSent, etc.) is handled in the
        // OutboundMessageWithTarget handler in the event loop, not here. All messages
        // from P2pBridge::send() flow through handle_bridge_msg → OutboundMessageWithTarget
        // → the shared handler which calls register_events(from_outbound_msg(...)).
        // Tracing here would cause duplicate events.
        let target = self.resolve_target(target_addr);
        self.op_manager.sending_transaction(&target, &msg);
        // channel-safety: ok — NetworkBridge::send is the OFF-loop producer API
        // (op-driver / contract / broadcast tasks producing INTO the event loop);
        // blocking here is the intended back-pressure on those callers. On-loop
        // callers must use Self::try_send instead (#4145).
        self.ev_listener_tx
            .send(P2pBridgeEvent::Message(target, Box::new(msg)))
            .await
            .map_err(|_| ConnectionError::SendNotCompleted(target_addr))?;
        Ok(())
    }

    async fn send_stream(
        &self,
        target_addr: SocketAddr,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
    ) -> super::ConnResult<()> {
        // channel-safety: ok — grandfathered streaming enqueue; callers are
        // off-loop spawned op/relay tasks, not the event loop (which drains
        // StreamSend). The #4001 `progress: None` field that brought this
        // statement into the diff does not change the send path.
        self.ev_listener_tx
            .send(P2pBridgeEvent::StreamSend {
                target_addr,
                stream_id,
                data,
                metadata,
                completion_tx: None,
                progress: None,
            })
            .await
            .map_err(|_| ConnectionError::SendNotCompleted(target_addr))?;
        Ok(())
    }

    async fn send_stream_with_progress(
        &self,
        target_addr: SocketAddr,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        progress: Option<crate::operations::stream_progress::StreamProgressHandle>,
    ) -> super::ConnResult<()> {
        // Enqueues the streaming send carrying the #4001 progress handle so
        // `outbound_stream::send_stream` records a tick per fragment.
        // channel-safety: ok — sole caller is the originator-loopback PUT relay
        // (drive_relay_put), which runs on a spawned op task, not the event
        // loop; the StreamSend it enqueues is drained by the loop, so it cannot
        // self-stall it. Identical send path to the grandfathered `send_stream`
        // above; completion_tx stays None (no broadcast-queue concurrency on
        // the originator PUT path).
        self.ev_listener_tx
            .send(P2pBridgeEvent::StreamSend {
                target_addr,
                stream_id,
                data,
                metadata,
                completion_tx: None,
                progress,
            })
            .await
            .map_err(|_| ConnectionError::SendNotCompleted(target_addr))?;
        Ok(())
    }

    async fn pipe_stream(
        &self,
        target_addr: SocketAddr,
        outbound_stream_id: StreamId,
        inbound_handle: crate::transport::peer_connection::streaming::StreamHandle,
        metadata: Option<bytes::Bytes>,
    ) -> super::ConnResult<()> {
        self.ev_listener_tx
            .send(P2pBridgeEvent::PipeStream {
                target_addr,
                outbound_stream_id,
                inbound_handle,
                metadata,
            })
            .await
            .map_err(|_| ConnectionError::SendNotCompleted(target_addr))?;
        Ok(())
    }
}

type PeerConnChannelSender = Sender<Either<NetMessage, ConnEvent>>;
type PeerConnChannelRecv = Receiver<Either<NetMessage, ConnEvent>>;

/// Entry in the connections HashMap, keyed by SocketAddr.
/// The pub_key is learned from the first message received on this connection.
#[derive(Debug)]
struct ConnectionEntry {
    sender: PeerConnChannelSender,
    /// The peer's public key, learned from the first message.
    /// None for transient connections before identity is established.
    pub_key: Option<TransportPublicKey>,
    /// Unique ID for this connection entry. Used to distinguish stale
    /// TransportClosed events from replaced connections (e.g., after identity change).
    connection_id: u64,
    /// When this transport connection was established.
    /// Used for zombie detection: connections not promoted to the ring
    /// within a timeout are considered zombies and dropped.
    created_at: Instant,
    /// The remote peer's negotiated protocol version, if known.
    /// `None` when the version wasn't exchanged (e.g. joiner->gateway path).
    /// Used to gate version-dependent message types (e.g. SubscribeHint).
    remote_version: Option<(u8, u8, u16)>,
}

/// Check whether a transport connection is a zombie: old enough but not
/// promoted to ring, not pending reservation, and not a gateway.
///
/// Gateway connections are exempt below a 1-hour absolute cap because they
/// are intentionally transient (never promoted to ring) but actively needed
/// for routing (#3595).
///
/// Two thresholds for non-gateway connections:
/// Zombie thresholds are derived from `transient_ttl` (configurable, default 30s):
///
/// - `zombie_threshold` = `transient_ttl * 3`: catches connections with no pending
///   reservation. Must be greater than `PENDING_RESERVATION_TTL` (60s) so that a
///   connection isn't immediately killed after its reservation expires. Previous
///   hardcoded value of 300s caused gateways to accumulate ~250 zombie transports,
///   overwhelming the packet processing channel and dropping keepalive packets.
/// - `absolute_zombie_threshold` = `transient_ttl * 6`: overrides `has_pending` to
///   break the refresh cycle where `connection_maintenance()` perpetually renews
///   pending reservations on gateway transports. Previous hardcoded value of 600s
///   allowed zombie transports to linger far too long.
fn is_zombie(
    created_at_elapsed: Duration,
    in_ring: bool,
    has_pending: bool,
    is_gateway: bool,
    transient_ttl: Duration,
) -> bool {
    let zombie_threshold = transient_ttl * 3;
    let absolute_zombie_threshold = transient_ttl * 6;

    if in_ring {
        return false;
    }
    // Gateway transient connections are intentionally not promoted to ring,
    // but the node needs them for routing. Without this exemption, gateways
    // enter a zombie→prune→reconnect→zombie death spiral that breaks all
    // streaming transfers (#3595).
    //
    // The exemption is time-bounded: truly dead gateway connections (no
    // traffic for 1 hour) are still cleaned up. The transport-level idle
    // timeout is the primary backstop, but this ensures no permanent leaks.
    /// Gateway connections get a generous exemption window (1 hour) because they
    /// are intentionally transient but needed for routing. The transport-level
    /// idle timeout (120s keepalive) is the primary cleanup mechanism for dead
    /// gateways; this threshold is the safety net.
    const GATEWAY_ZOMBIE_EXEMPTION: Duration = Duration::from_secs(3600);
    if is_gateway && created_at_elapsed < GATEWAY_ZOMBIE_EXEMPTION {
        return false;
    }
    if created_at_elapsed > zombie_threshold && !has_pending {
        return true;
    }
    if created_at_elapsed > absolute_zombie_threshold {
        return true;
    }
    false
}

/// Monotonically increasing counter for generating unique connection IDs.
static NEXT_CONNECTION_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn next_connection_id() -> u64 {
    NEXT_CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// The local-presence questions the `QueryNodeDiagnostics` handler asks the
/// ring when building `contract_states`. Abstracted into a trait so the
/// presence-filter logic in [`collect_contract_states`] can be unit-tested
/// without standing up a full `Ring`/`OpManager` — the production handler
/// passes `&op_manager.ring`, tests pass a fake oracle.
trait ContractPresenceOracle {
    fn is_hosting_contract(&self, key: &freenet_stdlib::prelude::ContractKey) -> bool;
    fn is_subscribed(&self, key: &freenet_stdlib::prelude::ContractKey) -> bool;
    fn hosting_contract_keys(&self) -> Vec<freenet_stdlib::prelude::ContractKey>;
    fn hosting_contract_size(&self, key: &freenet_stdlib::prelude::ContractKey) -> u64;
}

impl ContractPresenceOracle for crate::ring::Ring {
    fn is_hosting_contract(&self, key: &freenet_stdlib::prelude::ContractKey) -> bool {
        crate::ring::Ring::is_hosting_contract(self, key)
    }
    fn is_subscribed(&self, key: &freenet_stdlib::prelude::ContractKey) -> bool {
        crate::ring::Ring::is_subscribed(self, key)
    }
    fn hosting_contract_keys(&self) -> Vec<freenet_stdlib::prelude::ContractKey> {
        crate::ring::Ring::hosting_contract_keys(self)
    }
    fn hosting_contract_size(&self, key: &freenet_stdlib::prelude::ContractKey) -> u64 {
        crate::ring::Ring::hosting_contract_size(self, key)
    }
}

/// Build the `contract_states` map for a `NodeDiagnostics` response.
///
/// When `contract_keys` is empty, returns ALL hosting contracts. When specific
/// keys are provided, returns ONLY those the node has local presence for —
/// hosts in its store, or holds an active subscription lease for. A requested
/// key with neither is omitted (the `if !is_hosting && !is_subscribed`
/// guard): the explicit-keys branch must NOT echo every requested key back as
/// "present", or the response is useless as a local-presence signal and the
/// web-subresource DoS gate (#3945, see `path_handlers::is_locally_known`)
/// would treat every queried key as locally known. Absence here MUST mean
/// "not locally present".
fn collect_contract_states<O: ContractPresenceOracle + ?Sized>(
    oracle: &O,
    contract_keys: &[freenet_stdlib::prelude::ContractKey],
) -> std::collections::HashMap<String, freenet_stdlib::client_api::ContractState> {
    use freenet_stdlib::client_api::ContractState;

    let mut states = std::collections::HashMap::new();
    // stdlib 0.8.0 keyed contract_states by String
    // (`fix!: stringify NodeDiagnosticsResponse.contract_states keys`).
    if contract_keys.is_empty() {
        for contract_key in oracle.hosting_contract_keys() {
            let is_subscribed = oracle.is_subscribed(&contract_key);
            let subscriber_count = if is_subscribed { 1 } else { 0 };
            states.insert(
                contract_key.to_string(),
                ContractState {
                    subscribers: subscriber_count as u32,
                    subscriber_peer_ids: Vec::new(),
                    size_bytes: oracle.hosting_contract_size(&contract_key),
                },
            );
        }
    } else {
        for contract_key in contract_keys {
            let is_hosting = oracle.is_hosting_contract(contract_key);
            let is_subscribed = oracle.is_subscribed(contract_key);
            // Only report a state entry for a contract the node actually has
            // local presence for. Previously this branch inserted an entry for
            // EVERY requested key unconditionally, which made `contract_states`
            // claim the node hosted contracts it had never seen — and made the
            // response useless as a local-presence signal. The web subresource
            // DoS gate (#3945) relies on this being an accurate "do we know
            // this contract locally" answer, so absence here must mean "not
            // locally present".
            if !is_hosting && !is_subscribed {
                continue;
            }
            let subscriber_count = if is_subscribed { 1 } else { 0 };
            states.insert(
                contract_key.to_string(),
                ContractState {
                    subscribers: subscriber_count as u32,
                    subscriber_peer_ids: Vec::new(),
                    size_bytes: oracle.hosting_contract_size(contract_key),
                },
            );
        }
    }
    states
}

pub(in crate::node) struct P2pConnManager {
    pub(in crate::node) gateways: Vec<PeerKeyLocation>,
    pub(in crate::node) bridge: P2pBridge,
    conn_bridge_rx: Receiver<P2pBridgeEvent>,
    event_listener: Box<dyn NetEventRegister>,
    /// Connections indexed by socket address (the transport-level identifier).
    /// This is the source of truth for active connections.
    /// Uses BTreeMap for deterministic iteration order in simulation tests.
    connections: BTreeMap<SocketAddr, ConnectionEntry>,
    /// Reverse lookup: public key -> socket address.
    /// Used to find connections when we only know the peer's identity.
    /// Must be kept in sync with `connections`.
    /// Uses BTreeMap for deterministic iteration order in simulation tests.
    addr_by_pub_key: BTreeMap<TransportPublicKey, SocketAddr>,
    conn_event_tx: Option<Sender<ConnEvent>>,
    key_pair: TransportKeypair,
    listening_ip: IpAddr,
    listening_port: u16,
    is_gateway: bool,
    /// If set, will sent the location over network messages.
    ///
    /// It will also determine whether to trust the location of peers sent in network messages or derive them from IP.
    ///
    /// This is used for testing deterministically with given location. In production this should always be none
    /// and locations should be derived from IP addresses.
    this_location: Option<Location>,
    check_version: bool,
    bandwidth_limit: Option<usize>,
    /// Global bandwidth manager for fair sharing across all connections.
    global_bandwidth: Option<Arc<GlobalBandwidthManager>>,
    /// Minimum ssthresh floor for LEDBAT timeout recovery.
    ledbat_min_ssthresh: Option<usize>,
    /// Congestion control configuration.
    congestion_config: CongestionControlConfig,
    blocked_addresses: Option<HashSet<SocketAddr>>,
    /// Per-contract retry count for broadcasts that found no targets yet.
    broadcast_retries: HashMap<freenet_stdlib::prelude::ContractKey, u8>,
    /// Tracks how many consecutive broadcast cycles found zero targets per contract.
    /// Used to suppress repetitive WARN logs after the first few failures.
    /// Bounded to MAX_BROADCAST_STREAK_ENTRIES to prevent unbounded growth from
    /// network-influenced contract keys.
    broadcast_no_target_streak: HashMap<freenet_stdlib::prelude::ContractKey, u32>,
    /// Global broadcast queue that serializes outbound broadcast streams
    /// with bounded concurrency to prevent uplink saturation (issue #3337).
    #[cfg(not(feature = "simulation_tests"))]
    broadcast_queue: super::broadcast_queue::BroadcastQueue,
}

/// Minimum negotiated protocol version a peer must report before we send it a
/// [`SubscribeHint`](crate::message::NetMessageV1::SubscribeHint).
///
/// DISABLED at `(0, 3, 0)` — above every shipped 0.2.x release — so the
/// SubscribeHint placement migration is OFF for the entire live network. The
/// gate (`remote.is_some_and(|v| v >= floor)`, fail-closed on unknown) can
/// never be satisfied by any 0.2.x peer, so no node sends or acts on hints.
///
/// WHY DISABLED — the #4404 placement migration is net-negative in production.
/// It imposes real, measurable cost: it drives the #4610 full-state summarize
/// storm, the #4440 subscription-renewal load, and #4534 module-cache thrash —
/// while delivering NO measurable placement benefit. Production telemetry over
/// the re-enabled window showed `hosted_key_distance` FLAT (placement did not
/// tighten at all) and the inbound-hint act-rate collapsed to ~0.05%
/// (received-but-not-acted), so the network paid the storm / renewal / thrash
/// cost for essentially zero migrations. Disabling removes that cost with no
/// placement regression. Authorized by Ian + overseer (2026-06).
///
/// REVERSIBLE — this is a single const. Lowering the floor (e.g. back to a
/// shipped 0.2.x version) re-enables BOTH the SEND gate
/// (`select_migration_target` → `peer_supports_subscribe_hint`) and the RECEIVE
/// gate (`node.rs` inbound `SubscribeHint` handler, which gates on this node's
/// own version) together.
///
/// BAR FOR RE-ENABLING — do NOT lower this floor on the strength of a passing
/// simulation test alone. A sim can show hints flowing, but the production
/// failure was precisely that hints flowed, cost a lot, and moved placement
/// nowhere. Any future re-enable MUST be gated on a CANARY showing
/// `hosted_key_distance` measurably TIGHTENING under the migration (i.e. real
/// placement benefit on real peers), with the #4610 / #4440 / #4534 costs
/// bounded — not just a green test. If you cannot show placement improving on
/// production peers, leave it off. This is a deliberate re-disable, NOT a stale
/// "forgot to lower the floor" — do not reflex-revert it.
///
/// Wire-compat note: a peer is only sent SubscribeHint if its negotiated
/// version is `>=` this floor; older peers cannot deserialize the wire variant
/// and would drop the connection. With the floor parked above every 0.2.x
/// release this is moot today, but if the floor is ever lowered to re-enable,
/// pick a value no lower than the first release that can deserialize the variant
/// (0.2.73). None (unknown, e.g. joiner->gateway) is treated as unsupported.
///
/// This is a single non-cfg constant: deliberately NOT lowered under any test
/// feature, because `testing` is pulled into the SHIPPED release binary (`fdev`
/// depends on `freenet` with `features = ["testing"]` and the release builds
/// `-p freenet -p fdev` together, unifying `testing` onto the freenet binary),
/// so a cfg-gated test floor would defeat the wire-compat gate in production.
/// Simulation tests that want the cascade lower the floor per-node at runtime via
/// `NodeConfig::subscribe_hint_floor_override` (set by
/// `SimNetwork::enable_placement_migration`), which never touches production — so
/// the migration-on sim tests still run the cascade with this production floor
/// parked.
///
/// `pub(crate)` so the INBOUND `SubscribeHint` receive handler in
/// [`crate::node`] can share the same floor: the send-side gate alone is not
/// enough, because a node would otherwise still ACT on hints sent by a peer and
/// keep the (disabled) migration load alive.
///
/// History: the migration first shipped in v0.2.73, where the placement-migration
/// path (directed subscribes plus the `ConsiderContractMigration` emits) amplified
/// the #4145/#4231 broadcast-backpressure surface and drove a network-wide
/// UPDATE-broadcast degradation. v0.2.74 disabled it by parking this floor at
/// `(0, 3, 0)`. It was RE-ENABLED at `(0, 2, 80)` (#4511) after #4145 was fixed
/// (#4499) and validated at incident-scale fan-out — but production telemetry then
/// showed it net-negative (the storm / renewal / thrash costs above, with flat
/// `hosted_key_distance` and a ~0.05% act-rate), so it is RE-DISABLED here by
/// parking the floor at `(0, 3, 0)` again.
pub(crate) const SUBSCRIBE_HINT_MIN_VERSION: (u8, u8, u16) = (0, 3, 0);

/// This node's own crate version as a `(major, minor, patch)` tuple, parsed at
/// compile time from `CARGO_PKG_VERSION`.
///
/// Used by the INBOUND `SubscribeHint` receive gate (see [`crate::node`]) to ask
/// "is the placement migration active for a node running THIS version?" via
/// [`version_supports_subscribe_hint`]. With the floor parked at `(0, 3, 0)` the
/// migration is DISABLED, so no shipped 0.2.x node acts on inbound hints; the
/// send and receive sides share this floor so they enable/disable together.
pub(crate) const fn own_crate_version() -> (u8, u8, u16) {
    parse_crate_version(env!("CARGO_PKG_VERSION"))
}

/// `const` parser for `"X.Y.Z"` / `"X.Y.Z-pre"` into `(major, minor, patch)`.
///
/// Mirrors `connection_handler::parse_semver` (kept local so this gate does not
/// reach into the transport module). Pre-release suffixes are ignored.
const fn parse_crate_version(version: &str) -> (u8, u8, u16) {
    let bytes = version.as_bytes();
    let mut major = 0u8;
    let mut minor = 0u8;
    let mut patch = 0u16;
    let mut state = 0u8; // 0: major, 1: minor, 2: patch
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'.' {
            state += 1;
        } else if c.is_ascii_digit() {
            let digit = (c - b'0') as u16;
            match state {
                0 => major = (major as u16 * 10 + digit) as u8,
                1 => minor = (minor as u16 * 10 + digit) as u8,
                2 => patch = patch * 10 + digit,
                _ => {}
            }
        } else {
            break; // Pre-release suffix — ignored.
        }
        i += 1;
    }
    (major, minor, patch)
}

/// Pure version-gate decision, factored out so the comparison and the
/// fail-closed-on-unknown-version (`None`) behavior are unit-testable with an
/// explicit floor rather than the production [`SUBSCRIBE_HINT_MIN_VERSION`].
///
/// Returns `true` iff `remote` is known (`Some`) and at least `floor`. An
/// unknown remote version is treated as unsupported: an older peer that cannot
/// deserialize the appended `SubscribeHint` wire variant would drop the
/// connection, so when in doubt we do not send the hint.
///
/// `pub(crate)` so the inbound `SubscribeHint` receive gate in [`crate::node`]
/// can reuse the identical predicate (kept symmetric with the send side).
pub(crate) fn version_supports_subscribe_hint(
    remote: Option<(u8, u8, u16)>,
    floor: (u8, u8, u16),
) -> bool {
    remote.is_some_and(|v| v >= floor)
}

/// Upper bound on the number of hosted contracts examined per new-peer
/// migration trigger. Each examined contract may emit a best-effort
/// non-blocking `try_send` (the SubscribeHint nudge), so an unbounded scan
/// would still put O(hosted-contracts) work inline on the connection-handling
/// path per connection (see `.claude/rules/channel-safety.md`). 32 keeps the
/// per-event work bounded; remaining contracts are reconsidered on the next
/// hosting/peer event.
const MIGRATION_SCAN_CAP_PER_NEW_PEER: usize = 32;

/// Pure selection core for directed-subscribe placement (#4404).
///
/// Given the contract's ring location, OUR distance to it (`my_dist`), the set
/// of peers already known to host it, and an iterator over candidate
/// neighbors, return the single best neighbor to nudge — or `None`.
///
/// A candidate qualifies iff it has a known socket address, a known ring
/// location, is not already hosting (per `hosting`), and is STRICTLY closer to
/// the contract than we are (`dist < my_dist`). Among qualifying candidates the
/// closest wins; ties are broken deterministically by public-key bytes
/// (`TransportPublicKey: Ord` compares `as_bytes()`), so the choice is
/// reproducible regardless of iteration order.
///
/// Factored out of [`P2pConnManager::select_migration_target`] so the decision
/// logic is unit-testable without constructing a full connection manager.
fn pick_closest_migration_target<'a, I>(
    contract_loc: Location,
    my_dist: Distance,
    hosting: &HashSet<TransportPublicKey>,
    candidates: I,
) -> Option<PeerKeyLocation>
where
    I: IntoIterator<Item = &'a PeerKeyLocation>,
{
    let mut best: Option<(Distance, &'a TransportPublicKey, &'a PeerKeyLocation)> = None;
    for pkl in candidates {
        if pkl.socket_addr().is_none() {
            continue;
        }
        if hosting.contains(pkl.pub_key()) {
            continue;
        }
        let Some(pkl_loc) = pkl.location() else {
            continue;
        };
        let dist = contract_loc.distance(pkl_loc);
        if dist >= my_dist {
            continue;
        }
        let candidate_pk = pkl.pub_key();
        let replace = match &best {
            None => true,
            Some((best_dist, best_pk, _)) => {
                dist < *best_dist || (dist == *best_dist && candidate_pk < *best_pk)
            }
        };
        if replace {
            best = Some((dist, candidate_pk, pkl));
        }
    }
    best.map(|(_, _, pkl)| pkl.clone())
}

impl P2pConnManager {
    pub(in crate::node) fn listening_port(&self) -> u16 {
        self.listening_port
    }

    pub async fn build(
        config: &NodeConfig,
        op_manager: Arc<OpManager>,
        event_listener: impl NetEventRegister + Clone,
    ) -> anyhow::Result<Self> {
        let listen_port = config.network_listener_port;
        let listener_ip = config.network_listener_ip;

        let gateways = config.get_gateways()?;
        let gateways_arc = Arc::new(gateways.clone());
        let key_pair = config.key_pair.clone();

        // Bridge channel capacity: must exceed the max number of connected peers
        // to avoid self-deadlock when the event loop broadcasts inline. The primary
        // mitigation is spawning fan-out via broadcast_to_peers(), but this provides
        // defense-in-depth. Do NOT fan-out more than this many sends inline on the
        // event loop — use broadcast_to_peers() instead.
        let (tx_bridge_cmd, rx_bridge_cmd) = mpsc::channel(512);
        let bridge = P2pBridge::new(
            tx_bridge_cmd,
            op_manager,
            event_listener.clone(),
            gateways_arc,
        );

        // Initialize our peer identity before any connection attempts so join requests can
        // reference the correct address.
        // Note: For peers behind NAT, this initial address is a fallback - the correct
        // external address is learned via ObservedAddress and updated via set_own_addr().
        // The ring location should be computed from the observed external address,
        // not from this initial fallback.
        let advertised_addr = {
            let advertised_ip = config
                .own_addr
                .map(|addr| addr.ip())
                .or(config.config.network_api.public_address)
                .unwrap_or_else(|| {
                    if listener_ip.is_unspecified() {
                        IpAddr::V4(Ipv4Addr::LOCALHOST)
                    } else {
                        listener_ip
                    }
                });
            let advertised_port = config
                .own_addr
                .map(|addr| addr.port())
                .or(config.config.network_api.public_port)
                .unwrap_or(listen_port);
            SocketAddr::new(advertised_ip, advertised_port)
        };
        bridge
            .op_manager
            .ring
            .connection_manager
            .try_set_own_addr(advertised_addr);

        Ok(P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx: rx_bridge_cmd,
            event_listener: Box::new(event_listener),
            connections: BTreeMap::new(),
            addr_by_pub_key: BTreeMap::new(),
            conn_event_tx: None,
            key_pair,
            listening_ip: listener_ip,
            listening_port: listen_port,
            is_gateway: config.is_gateway,
            this_location: config.location,
            check_version: !config.config.network_api.ignore_protocol_version,
            bandwidth_limit: config.config.network_api.bandwidth_limit,
            global_bandwidth: config
                .config
                .network_api
                .total_bandwidth_limit
                .map(|total| {
                    let manager = Arc::new(GlobalBandwidthManager::new(
                        total,
                        config.config.network_api.min_bandwidth_per_connection,
                    ));
                    // Phase 1.6 shadow telemetry (#4074): expose the
                    // effective aggregate rate R to the demand aggregator.
                    // Observation only — the manager is read, never written,
                    // by the shadow side. See transport/shadow_demand.rs.
                    crate::transport::shadow_demand::register_global_bandwidth(&manager);
                    manager
                }),
            ledbat_min_ssthresh: config.config.network_api.ledbat_min_ssthresh,
            congestion_config: config.config.network_api.build_congestion_config(),
            blocked_addresses: config.blocked_addresses.clone(),
            broadcast_retries: HashMap::new(),
            broadcast_no_target_streak: HashMap::new(),
            #[cfg(not(feature = "simulation_tests"))]
            broadcast_queue: super::broadcast_queue::BroadcastQueue::new(),
        })
    }

    /// Runs the event listener with `UdpSocket` (production mode).
    ///
    /// This is a convenience wrapper around `run_event_listener_with_socket<UdpSocket>`.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(name = "network_event_listener", fields(peer = %self.bridge.op_manager.ring.connection_manager.pub_key), skip_all)]
    pub async fn run_event_listener(
        self,
        op_manager: Arc<OpManager>,
        client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
        notification_channel: EventLoopNotificationsReceiver,
        node_controller: Receiver<NodeEvent>,
    ) -> anyhow::Result<Infallible> {
        self.run_event_listener_with_socket::<UdpSocket>(
            op_manager,
            client_wait_for_transaction,
            notification_channel,
            node_controller,
        )
        .await
    }

    /// Runs the event listener with a configurable socket type.
    ///
    /// This is the generic version that allows using different socket implementations:
    /// - `UdpSocket` for production
    /// - `InMemorySocket` for testing
    ///
    /// The socket type only affects connection establishment. Once connections are
    /// established, they are type-erased via `PeerConnectionApi` trait objects.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(name = "network_event_listener", fields(peer = %self.bridge.op_manager.ring.connection_manager.pub_key), skip_all)]
    pub async fn run_event_listener_with_socket<S: Socket>(
        self,
        op_manager: Arc<OpManager>,
        client_wait_for_transaction: ContractHandlerChannel<WaitingResolution>,
        notification_channel: EventLoopNotificationsReceiver,
        node_controller: Receiver<NodeEvent>,
    ) -> anyhow::Result<Infallible> {
        // Destructure self to avoid partial move issues
        let P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx,
            event_listener,
            connections,
            addr_by_pub_key,
            conn_event_tx: _,
            key_pair,
            listening_ip,
            listening_port,
            is_gateway,
            this_location,
            check_version,
            bandwidth_limit,
            global_bandwidth,
            ledbat_min_ssthresh,
            congestion_config,
            blocked_addresses,
            broadcast_retries,
            broadcast_no_target_streak,
            #[cfg(not(feature = "simulation_tests"))]
            broadcast_queue,
        } = self;

        let (outbound_conn_handler, inbound_conn_handler, mut listen_task_handle) =
            create_connection_handler::<S>(
                key_pair.clone(),
                listening_ip,
                listening_port,
                is_gateway,
                bandwidth_limit,
                global_bandwidth,
                ledbat_min_ssthresh,
                Some(congestion_config.clone()),
            )
            .await?;

        tracing::info!(
            listening_port,
            listening_ip = %listening_ip,
            is_gateway,
            peer_key = %key_pair.public(),
            phase = "startup",
            "Network listener opened - ready to receive connections"
        );

        let expected_inbound = outbound_conn_handler.expected_inbound_tracker();
        let mut state = EventListenerState::new(expected_inbound);

        let (conn_event_tx, conn_event_rx) = mpsc::channel(1024);

        // For non-gateway peers, pass the peer_ready flag so it can be set after first handshake
        // For gateways, pass None (they're always ready)
        let peer_ready = if !is_gateway {
            Some(bridge.op_manager.peer_ready.clone())
        } else {
            None
        };

        let (handshake_handler, handshake_cmd_sender) = HandshakeHandler::new(
            inbound_conn_handler,
            outbound_conn_handler.clone(),
            bridge.op_manager.ring.connection_manager.clone(),
            bridge.op_manager.ring.router.clone(),
            this_location,
            is_gateway,
            peer_ready,
        );

        let select_stream = priority_select::ProductionPrioritySelectStream::new(
            notification_channel.notifications_receiver,
            notification_channel.op_execution_receiver,
            conn_bridge_rx,
            handshake_handler,
            node_controller,
            client_wait_for_transaction,
            ExecutorTransactionStream,
            conn_event_rx,
        );

        // Pin the stream on the stack
        tokio::pin!(select_stream);

        // Reconstruct a P2pConnManager-like structure for use in the loop
        // We can't use the original self because we moved conn_bridge_rx
        let mut ctx = P2pConnManager {
            gateways,
            bridge,
            conn_bridge_rx: tokio::sync::mpsc::channel(1).1, // Dummy, won't be used
            event_listener,
            connections,
            addr_by_pub_key,
            conn_event_tx: Some(conn_event_tx.clone()),
            key_pair,
            listening_ip,
            listening_port,
            is_gateway,
            this_location,
            check_version,
            bandwidth_limit,
            global_bandwidth: None, // Already used for connection handler, not needed in ctx
            ledbat_min_ssthresh,
            congestion_config, // Already used for connection handler, kept for struct completeness
            blocked_addresses,
            broadcast_retries,
            broadcast_no_target_streak,
            #[cfg(not(feature = "simulation_tests"))]
            broadcast_queue: broadcast_queue.clone(),
        };

        // Start the broadcast queue worker (production only).
        // In simulation tests, broadcasts are sent inline for deterministic ordering.
        #[cfg(not(feature = "simulation_tests"))]
        let _broadcast_worker_handle =
            broadcast_queue.start_worker(ctx.bridge.clone(), op_manager.clone());

        // Track whether we exit via graceful shutdown (Disconnect or ClosedChannel)
        // vs unexpected stream end
        let mut graceful_shutdown = false;

        // Event loop instrumentation
        let mut loop_iteration_count = 0u64;
        let mut slow_event_count = 0u64;
        let mut last_stats_log = Instant::now();
        const STATS_LOG_INTERVAL: Duration = Duration::from_secs(30);
        const SLOW_EVENT_THRESHOLD: Duration = Duration::from_millis(100);

        // Monitor both the event stream AND the UDP listen task.
        // If the listen task exits for any reason, the transport layer is dead
        // and we must propagate the error to trigger node shutdown.
        loop {
            let result = tokio::select! {
                biased;

                listen_result = &mut listen_task_handle => {
                    // The UDP listener task has exited — this is fatal.
                    // Without the listener, no UDP packets are read from the socket,
                    // connections will time out, and the node becomes unresponsive.
                    match listen_result {
                        Ok(e) => {
                            tracing::error!(
                                error = %e,
                                "CRITICAL: UDP listen task exited with transport error"
                            );
                        }
                        Err(join_err) => {
                            let cause = if join_err.is_panic() { "panicked" } else { "cancelled" };
                            tracing::error!(
                                cause,
                                "CRITICAL: UDP listen task failed: {join_err}"
                            );
                        }
                    }
                    return Err(anyhow!(
                        "UDP listen task exited unexpectedly — \
                         transport layer is dead, node must restart"
                    ));
                },

                maybe_result = StreamExt::next(&mut select_stream) => {
                    let Some(result) = maybe_result else {
                        break;
                    };
                    result
                },
            };

            loop_iteration_count += 1;

            let event_type = match &result {
                priority_select::SelectResult::Notification(_) => "notification",
                priority_select::SelectResult::OpExecution(_) => "op_execution",
                priority_select::SelectResult::PeerConnection(_) => "peer_connection",
                priority_select::SelectResult::ConnBridge(_) => "conn_bridge",
                priority_select::SelectResult::Handshake(_) => "handshake",
                priority_select::SelectResult::NodeController(_) => "node_controller",
                priority_select::SelectResult::ClientTransaction(_) => "client_transaction",
                priority_select::SelectResult::ExecutorTransaction(_) => "executor_transaction",
            };

            let process_start = Instant::now();

            // Process the result using the existing handler
            let event = ctx
                .process_select_result(result, &mut state, &handshake_cmd_sender)
                .await?;

            let elapsed = process_start.elapsed();
            if elapsed > SLOW_EVENT_THRESHOLD {
                slow_event_count += 1;
                tracing::warn!(
                    event_type,
                    elapsed_ms = elapsed.as_millis(),
                    "Slow event loop iteration"
                );
            }

            // Periodic stats logging
            if last_stats_log.elapsed() > STATS_LOG_INTERVAL {
                let notifier = &op_manager.to_event_listener;
                let transport_connections = ctx.connections.len();
                let ring_connections = op_manager.ring.connection_manager.connection_count();
                tracing::info!(
                    iterations = loop_iteration_count,
                    slow_events = slow_event_count,
                    notification_channel_pending = notifier.notification_channel_pending(),
                    notification_channel_capacity = notifier.notifications_sender.capacity(),
                    active_connections = transport_connections,
                    ring_connections,
                    "Event loop stats"
                );
                // Detect transport/ring divergence: transport has connections but ring doesn't
                if transport_connections > 0 && ring_connections == 0 {
                    tracing::error!(
                        transport_connections,
                        ring_connections,
                        "RING_TRANSPORT_DESYNC: transport has connections but ring topology is empty - \
                         connections are not being promoted or are being immediately pruned"
                    );
                }

                // Periodic ReadyState re-broadcast: if we are ready and have readiness
                // gating enabled, re-broadcast our ReadyState to all peers every 30s.
                // This ensures lost ReadyState messages are recovered within one tick.
                if op_manager.ring.connection_manager.min_ready_connections > 0
                    && op_manager.ring.connection_manager.is_self_ready()
                {
                    ctx.handle_broadcast_ready_state(true).await;
                }

                #[cfg(all(unix, feature = "jemalloc-prof"))]
                {
                    use tikv_jemalloc_ctl::{epoch, stats};
                    epoch::advance().ok();
                    let allocated = stats::allocated::read().unwrap_or(0);
                    let resident = stats::resident::read().unwrap_or(0);
                    tracing::info!(
                        allocated_mb = allocated / (1024 * 1024),
                        resident_mb = resident / (1024 * 1024),
                        "jemalloc memory stats"
                    );
                }

                loop_iteration_count = 0;
                slow_event_count = 0;
                last_stats_log = Instant::now();

                // Zombie transport cleanup: remove connections older than 3× transient_ttl
                // that haven't been promoted to ring and have no pending reservation.
                // An absolute threshold of 6× transient_ttl overrides pending reservations
                // to catch gateway transports stuck in a pending-refresh cycle.
                //
                // IMPORTANT: We use drop_zombie_connection (non-blocking try_send)
                // instead of drop_connection_by_addr to avoid a circular deadlock
                // with the handshake driver (#3519). We also cap the batch size to
                // limit event loop latency — each zombie cleanup involves topology
                // pruning and orphaned transaction handling. With a 100ms timeout
                // per zombie, 64 zombies = ~6.4s worst case per cycle.
                // Remaining zombies will be cleaned up in the next 30s cycle.
                const MAX_ZOMBIE_CLEANUP_PER_CYCLE: usize = 64;
                let transient_ttl = op_manager.ring.connection_manager.transient_ttl();
                let zombie_addrs: Vec<SocketAddr> = ctx
                    .connections
                    .iter()
                    .filter(|(addr, entry)| {
                        let is_gateway = ctx
                            .gateways
                            .iter()
                            .any(|gw| gw.socket_addr() == Some(**addr));
                        is_zombie(
                            entry.created_at.elapsed(),
                            op_manager.ring.connection_manager.is_in_ring(**addr),
                            op_manager
                                .ring
                                .connection_manager
                                .has_connection_or_pending(**addr),
                            is_gateway,
                            transient_ttl,
                        )
                    })
                    .map(|(addr, _)| *addr)
                    .take(MAX_ZOMBIE_CLEANUP_PER_CYCLE)
                    .collect();
                if !zombie_addrs.is_empty() {
                    tracing::info!(
                        zombie_count = zombie_addrs.len(),
                        "Cleaning up zombie transports (not promoted to ring)"
                    );
                }
                for addr in &zombie_addrs {
                    ctx.drop_zombie_connection(*addr, &handshake_cmd_sender)
                        .await;
                }

                // Periodic cleanup of pending_op_results: remove entries where the
                // receiver has been dropped (closed sender). This is a safety net for
                // transactions that complete without emitting TransactionCompleted/TimedOut
                // events, preventing unbounded HashMap growth.
                const PENDING_OP_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
                if state.last_pending_op_cleanup.elapsed() > PENDING_OP_CLEANUP_INTERVAL {
                    let before = state.pending_op_results.len();
                    state
                        .pending_op_results
                        .retain(|_tx, sender| !sender.is_closed());
                    let removed = before - state.pending_op_results.len();
                    if removed > 0 {
                        for _ in 0..removed {
                            crate::config::GlobalTestMetrics::record_pending_op_remove();
                        }
                        crate::config::GlobalTestMetrics::record_pending_op_size(
                            state.pending_op_results.len() as u64,
                        );
                        tracing::info!(
                            removed,
                            remaining = state.pending_op_results.len(),
                            "Cleaned up closed pending_op_results senders"
                        );
                    }
                    state.last_pending_op_cleanup = Instant::now();
                }
            }

            match event {
                EventResult::Continue => continue,
                EventResult::Event(event) => {
                    match *event {
                        ConnEvent::InboundMessage(inbound) => {
                            let remote = inbound.remote_addr;
                            let msg = inbound.msg;
                            tracing::debug!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                peer_addr = ?remote,
                                direction = "inbound",
                                phase = "receive",
                                "Received inbound message from peer"
                            );
                            // NOTE: Do NOT fill in the joiner's address here!
                            // The connect::handle_request() function handles address discovery
                            // and emits ObservedAddress to inform the joiner of their external
                            // address. If we fill it in here, handle_request() won't know that
                            // WE discovered the address and won't send ObservedAddress.
                            // The source_addr is passed to handle_inbound_message and propagates
                            // to the connect operation via source_addr parameter.
                            // Pass the source address through to operations for routing.
                            // This replaces the old rewrite_sender_addr hack - instead of mutating
                            // message contents, we pass the observed transport address separately.
                            ctx.handle_inbound_message(msg, remote, &op_manager, &mut state)
                                .await?;
                        }
                        ConnEvent::OutboundMessage(NetMessage::V1(NetMessageV1::Aborted(tx))) => {
                            // TODO: handle aborted transaction as internal message
                            tracing::warn!(tx = %tx, phase = "abort", "Transaction aborted");
                        }
                        ConnEvent::OutboundMessage(msg) => {
                            // With hop-by-hop routing, messages should go through OutboundMessageWithTarget.
                            // This path should not be reached - if we get here, it means a message was sent
                            // without proper target extraction from operation state.
                            tracing::error!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                phase = "send",
                                "Outbound message missing target - bug in routing logic, processing locally as fallback"
                            );
                            // Process locally as a fallback
                            ctx.handle_inbound_message(msg, None, &op_manager, &mut state)
                                .await?;
                        }
                        ConnEvent::OutboundMessageWithTarget { target_addr, msg } => {
                            // This variant uses an explicit target address from P2pBridge::send(),
                            // which is critical for NAT scenarios where the address in the message
                            // differs from the actual transport address we should send to.
                            tracing::debug!(
                                tx = %msg.id(),
                                msg_type = %msg,
                                peer_addr = %target_addr,
                                direction = "outbound",
                                phase = "send",
                                "Sending outbound message to peer"
                            );

                            // Trace outbound events (UnsubscribeSent, etc.) for telemetry.
                            // Without this, messages routed via handle_notification_msg are
                            // invisible to the event aggregator.
                            ctx.bridge
                                .log_register
                                .register_events(NetEventLog::from_outbound_msg(
                                    &msg,
                                    &ctx.bridge.op_manager.ring,
                                    Some(target_addr),
                                ))
                                .await;

                            // Look up the connection using the explicit target address
                            let peer_connection = ctx.connections.get(&target_addr);

                            match peer_connection {
                                Some(peer_connection) => {
                                    // Short timeout avoids head-of-line blocking: if
                                    // the channel doesn't drain within 500ms the peer
                                    // is congested/dead, so we drop the message and
                                    // let the op timeout + retry. 500ms gives transient
                                    // bursts and slow CI runners time to clear while
                                    // preventing the event loop from stalling on dead
                                    // peers (which would block indefinitely).
                                    const SEND_TIMEOUT: std::time::Duration =
                                        std::time::Duration::from_millis(500);
                                    let msg_type = match &msg {
                                        NetMessage::V1(v1) => match v1 {
                                            NetMessageV1::Connect(_) => "Connect",
                                            NetMessageV1::Put(_) => "Put",
                                            NetMessageV1::Get(_) => "Get",
                                            NetMessageV1::Subscribe(_) => "Subscribe",
                                            NetMessageV1::Update(_) => "Update",
                                            NetMessageV1::Aborted(_) => "Aborted",
                                            NetMessageV1::NeighborHosting { .. } => {
                                                "NeighborHosting"
                                            }
                                            NetMessageV1::InterestSync { .. } => "InterestSync",
                                            NetMessageV1::ReadyState { .. } => "ReadyState",
                                            NetMessageV1::SubscribeHint(_) => "SubscribeHint",
                                        },
                                    };
                                    match tokio::time::timeout(
                                        SEND_TIMEOUT,
                                        peer_connection.sender.send(Left(msg.clone())),
                                    )
                                    .await
                                    {
                                        Ok(Ok(())) => {
                                            tracing::trace!(
                                                tx = %msg.id(),
                                                peer_addr = %target_addr,
                                                phase = "send",
                                                "Message sent to peer connection"
                                            );
                                        }
                                        Ok(Err(_closed)) => {
                                            tracing::error!(
                                                tx = %msg.id(),
                                                peer_addr = %target_addr,
                                                msg_type,
                                                phase = "error",
                                                "Peer connection channel closed"
                                            );
                                        }
                                        Err(_timeout) => {
                                            tracing::warn!(
                                                tx = %msg.id(),
                                                peer_addr = %target_addr,
                                                msg_type,
                                                phase = "backpressure",
                                                "Outbound channel full after {SEND_TIMEOUT:?}, \
                                                 dropping message to avoid event loop stall"
                                            );
                                        }
                                    }
                                }
                                None => {
                                    // No existing connection - need to establish one first.
                                    // This happens for initial Connect requests to a gateway.
                                    let tx = *msg.id();

                                    // Try to get full peer info from operation state (needed for handshake)
                                    // Uses peek method to avoid pop/push overhead
                                    let target_peer = ctx.bridge.op_manager.peek_target_peer(&tx);

                                    // If peek_target_peer returns None (e.g., for Put/Get operations which
                                    // only store addresses, not full peer info), try looking up the peer
                                    // in the connection_manager by address. This enables response routing
                                    // when the connection still exists but operation state lacks full peer info.
                                    let target_peer = target_peer.or_else(|| {
                                        ctx.bridge
                                            .op_manager
                                            .ring
                                            .connection_manager
                                            .get_peer_location_by_addr(target_addr)
                                    });

                                    // If still not found, check if this is a known gateway.
                                    // Gateways have their public keys configured at startup.
                                    let target_peer = target_peer.or_else(|| {
                                        ctx.gateways
                                            .iter()
                                            .find(|gw| gw.socket_addr() == Some(target_addr))
                                            .cloned()
                                    });

                                    let Some(target_peer) = target_peer else {
                                        // Can't establish connection without peer info.
                                        // This happens when the connection was dropped before we could
                                        // route the response. Since the peer is no longer in
                                        // connection_manager, we can't re-establish the connection.
                                        // Log at warn level since this can legitimately happen with
                                        // transient connections that time out before response routing.
                                        tracing::warn!(
                                            tx = %tx,
                                            peer_addr = %target_addr,
                                            active_connections = ctx.connections.len(),
                                            phase = "connect",
                                            "Cannot establish connection - peer not found in connection_manager. \
                                             Connection likely dropped before response could be routed."
                                        );
                                        ctx.bridge.op_manager.completed(tx);
                                        continue;
                                    };

                                    tracing::debug!(
                                        tx = %tx,
                                        peer_addr = %target_addr,
                                        peer = %target_peer,
                                        active_connections = ctx.connections.len(),
                                        phase = "connect",
                                        "No existing connection - establishing connection before sending message"
                                    );

                                    // Queue the message for sending after connection is established
                                    let (callback, mut result) = mpsc::channel(10);
                                    let msg_clone = msg.clone();
                                    let bridge_sender = ctx.bridge.ev_listener_tx.clone();
                                    let gateways = ctx.gateways.clone();

                                    // Mark gateway connections as transient to prevent
                                    // premature ring promotion (should only happen via
                                    // ConnectResponse).
                                    let is_gw = gateways
                                        .iter()
                                        .any(|gw| gw.socket_addr() == Some(target_addr));

                                    // Non-blocking enqueue to avoid self-deadlocking the
                                    // event loop (#4145). ev_listener_tx is drained by THIS
                                    // task's own select loop, so a `.send().await` here that
                                    // back-pressures would stall the very loop that drains it
                                    // — a true self-deadlock cycle. Under the SubscribeHint
                                    // placement migration's load this is the ConnectPeer arm
                                    // the nudge drives, so a full channel is the load signal
                                    // we must shed cleanly rather than wedge on.
                                    //
                                    // On Full/Closed we drop THIS outbound attempt: the
                                    // message is not sent, the op driver's retry loop / op
                                    // TTL re-routes it (same recovery as the congested-peer
                                    // 500ms-timeout drop above). We must NOT spawn the
                                    // resend-waiter in that case — its `callback` would never
                                    // be registered with any connect op, so dropping the
                                    // callback here closes `result` and the waiter would
                                    // observe `Ok(None)` immediately rather than hang the
                                    // full 20s timeout on a connection that was never
                                    // requested.
                                    if let Err(err) = ctx.bridge.ev_listener_tx.try_send(
                                        P2pBridgeEvent::NodeAction(NodeEvent::ConnectPeer {
                                            peer: target_peer.clone(),
                                            tx,
                                            callback,
                                            is_gw,
                                        }),
                                    ) {
                                        // `err` owns the ConnectPeer event (and thus the
                                        // callback Sender); dropping it here is what
                                        // short-circuits the would-be waiter.
                                        let reason = match err {
                                            mpsc::error::TrySendError::Full(_) => "full",
                                            mpsc::error::TrySendError::Closed(_) => "closed",
                                        };
                                        tracing::warn!(
                                            tx = %tx,
                                            peer_addr = %target_addr,
                                            reason,
                                            phase = "backpressure",
                                            "Event-loop bridge channel {reason} while dispatching \
                                             ConnectPeer; dropping outbound message (op will retry). \
                                             Sustained occurrences indicate fan-out / migration load \
                                             overwhelming the event loop."
                                        );
                                        continue;
                                    }

                                    tracing::debug!(
                                        tx = %tx,
                                        peer_addr = %target_addr,
                                        phase = "connect",
                                        "Dispatched ConnectPeer event - waiting for connection establishment"
                                    );

                                    // Spawn a task to wait for connection and then send the message
                                    let target_peer_for_resend = target_peer.clone();
                                    GlobalExecutor::spawn(async move {
                                        match timeout(Duration::from_secs(20), result.recv()).await
                                        {
                                            Ok(Some(Ok((connected_addr, _)))) => {
                                                tracing::debug!(
                                                    tx = %tx,
                                                    peer_addr = %connected_addr,
                                                    phase = "connect",
                                                    "Connection established - rescheduling message send"
                                                );
                                                // Construct PeerKeyLocation with the connected address
                                                // and the original public key
                                                let connected_peer = PeerKeyLocation::new(
                                                    target_peer_for_resend.pub_key().clone(),
                                                    connected_addr,
                                                );
                                                // Re-send via P2pBridge which will route correctly
                                                if let Err(e) = bridge_sender
                                                    .send(P2pBridgeEvent::Message(
                                                        connected_peer,
                                                        Box::new(msg_clone),
                                                    ))
                                                    .await
                                                {
                                                    tracing::error!(
                                                        tx = %tx,
                                                        peer_addr = %connected_addr,
                                                        error = ?e,
                                                        phase = "error",
                                                        "Failed to reschedule message after connection established"
                                                    );
                                                }
                                            }
                                            Ok(Some(Err(e))) => {
                                                tracing::debug!(
                                                    tx = %tx,
                                                    peer_addr = %target_addr,
                                                    error = ?e,
                                                    phase = "error",
                                                    "Connection attempt failed"
                                                );
                                            }
                                            Ok(None) => {
                                                tracing::error!(
                                                    tx = %tx,
                                                    peer_addr = %target_addr,
                                                    phase = "error",
                                                    "Response channel closed before connection result received"
                                                );
                                            }
                                            Err(_) => {
                                                tracing::error!(
                                                    tx = %tx,
                                                    peer_addr = %target_addr,
                                                    phase = "timeout",
                                                    "Timeout waiting for connection establishment"
                                                );
                                            }
                                        }
                                    });
                                }
                            }
                        }
                        ConnEvent::TransportClosed {
                            remote_addr,
                            error,
                            connection_id,
                        } => {
                            tracing::debug!(
                                peer_addr = %remote_addr,
                                error = ?error,
                                connection_id,
                                phase = "disconnect",
                                "Transport connection closed"
                            );
                        }
                        ConnEvent::StreamSend {
                            target_addr,
                            stream_id,
                            data,
                            metadata,
                            completion_tx,
                            progress,
                        } => {
                            // The event loop must never block indefinitely on a single
                            // congested peer. Reserve a slot with the same 500 ms
                            // ceiling as OutboundMessageWithTarget; if the peer's
                            // channel cannot drain within that window we drop the
                            // stream fragment and fire `completion_tx` so the
                            // broadcast queue releases its semaphore permit
                            // immediately instead of waiting STREAM_COMPLETION_TIMEOUT
                            // (120 s). Issue #4145. Using `reserve()` (rather than
                            // `timeout(send())`) lets us keep ownership of
                            // `completion_tx` when the timeout fires, so we can
                            // signal it manually on the drop path.
                            const SEND_TIMEOUT: std::time::Duration =
                                std::time::Duration::from_millis(500);
                            if let Some(peer_connection) = ctx.connections.get(&target_addr) {
                                match tokio::time::timeout(
                                    SEND_TIMEOUT,
                                    peer_connection.sender.reserve(),
                                )
                                .await
                                {
                                    Ok(Ok(permit)) => {
                                        permit.send(Right(ConnEvent::StreamSend {
                                            target_addr,
                                            stream_id,
                                            data,
                                            metadata,
                                            completion_tx,
                                            progress,
                                        }));
                                    }
                                    Ok(Err(_closed)) => {
                                        tracing::error!(
                                            stream_id = %stream_id,
                                            peer_addr = %target_addr,
                                            phase = "error",
                                            "Peer connection channel closed; \
                                             cannot route stream fragment"
                                        );
                                        if let Some(tx) = completion_tx {
                                            let _ignored =
                                                tx.send(BroadcastDeliveryOutcome::Dropped);
                                        }
                                    }
                                    Err(_timeout) => {
                                        tracing::warn!(
                                            stream_id = %stream_id,
                                            peer_addr = %target_addr,
                                            timeout_ms = SEND_TIMEOUT.as_millis() as u64,
                                            phase = "backpressure",
                                            "Peer congested; dropping stream fragment \
                                             to keep event loop responsive"
                                        );
                                        if let Some(tx) = completion_tx {
                                            let _ignored =
                                                tx.send(BroadcastDeliveryOutcome::Dropped);
                                        }
                                    }
                                }
                            } else {
                                tracing::warn!(
                                    stream_id = %stream_id,
                                    peer_addr = %target_addr,
                                    phase = "error",
                                    "No connection found for stream send target"
                                );
                                // Signal completion so the broadcast queue permit
                                // is released immediately. No connection means the
                                // fragment was dropped, not delivered (#4235).
                                if let Some(tx) = completion_tx {
                                    let _ignored = tx.send(BroadcastDeliveryOutcome::Dropped);
                                }
                            }
                        }
                        ConnEvent::PipeStream {
                            target_addr,
                            outbound_stream_id,
                            inbound_handle,
                            metadata,
                        } => {
                            // Same 500 ms reserve ceiling as StreamSend /
                            // OutboundMessageWithTarget. If the peer is congested
                            // we drop the pipe rather than stall the loop; the
                            // associated stream-level error path will eventually
                            // surface a timeout to the originating op. Issue #4145.
                            const SEND_TIMEOUT: std::time::Duration =
                                std::time::Duration::from_millis(500);
                            if let Some(peer_connection) = ctx.connections.get(&target_addr) {
                                match tokio::time::timeout(
                                    SEND_TIMEOUT,
                                    peer_connection.sender.reserve(),
                                )
                                .await
                                {
                                    Ok(Ok(permit)) => {
                                        permit.send(Right(ConnEvent::PipeStream {
                                            target_addr,
                                            outbound_stream_id,
                                            inbound_handle,
                                            metadata,
                                        }));
                                    }
                                    Ok(Err(_closed)) => {
                                        tracing::error!(
                                            stream_id = %outbound_stream_id,
                                            peer_addr = %target_addr,
                                            phase = "error",
                                            "Peer connection channel closed; cannot pipe stream"
                                        );
                                    }
                                    Err(_timeout) => {
                                        tracing::warn!(
                                            stream_id = %outbound_stream_id,
                                            peer_addr = %target_addr,
                                            timeout_ms = SEND_TIMEOUT.as_millis() as u64,
                                            phase = "backpressure",
                                            "Peer congested; dropping stream pipe \
                                             to keep event loop responsive"
                                        );
                                    }
                                }
                            } else {
                                tracing::warn!(
                                    stream_id = %outbound_stream_id,
                                    peer_addr = %target_addr,
                                    phase = "error",
                                    "No connection found for pipe stream target"
                                );
                            }
                        }
                        ConnEvent::ClosedChannel(reason) => {
                            match reason {
                                ChannelCloseReason::Bridge
                                | ChannelCloseReason::Controller
                                | ChannelCloseReason::Notification
                                | ChannelCloseReason::OpExecution => {
                                    // All ClosedChannel events are critical - the transport is unable to establish
                                    // more connections, rendering this peer useless. Perform cleanup and shutdown.
                                    let is_gateway = ctx.bridge.op_manager.ring.is_gateway();
                                    let active_conns = ctx.connections.len();
                                    // Write to stderr directly to ensure this survives process exit
                                    // (tracing output may be buffered and lost during shutdown)
                                    eprintln!(
                                        "CRITICAL: Channel closed reason={reason:?} is_gateway={is_gateway} active_connections={active_conns} - shutting down"
                                    );
                                    tracing::error!(
                                        reason = ?reason,
                                        is_gateway,
                                        active_connections = active_conns,
                                        phase = "shutdown",
                                        "Critical channel closed - performing cleanup and shutting down"
                                    );

                                    // Clean up all active connections
                                    let peers_to_cleanup: Vec<_> = ctx
                                        .connections
                                        .iter()
                                        .map(|(addr, entry)| (*addr, entry.pub_key.clone()))
                                        .collect();
                                    for (peer_addr, pub_key_opt) in peers_to_cleanup {
                                        tracing::debug!(
                                            peer_addr = %peer_addr,
                                            phase = "cleanup",
                                            "Cleaning up connection due to channel closure"
                                        );

                                        // Clean up ring state - construct PeerKeyLocation with pub_key if available
                                        let peer = if let Some(pub_key) = pub_key_opt.clone() {
                                            PeerKeyLocation::new(pub_key, peer_addr)
                                        } else {
                                            // Use our own pub_key as placeholder if we don't know the peer's
                                            PeerKeyLocation::new(
                                                (*ctx
                                                    .bridge
                                                    .op_manager
                                                    .ring
                                                    .connection_manager
                                                    .pub_key)
                                                    .clone(),
                                                peer_addr,
                                            )
                                        };
                                        let prune_result = ctx
                                            .bridge
                                            .op_manager
                                            .ring
                                            .prune_connection(PeerId::new(
                                                peer.pub_key().clone(),
                                                peer_addr,
                                            ))
                                            .await;

                                        // Note: In the simplified architecture (2026-01), subscriptions are lease-based
                                        // and don't require explicit pruning notifications. Just handle orphaned transactions.

                                        // Handle orphaned transactions immediately (retry via alternate routes).
                                        ctx.bridge
                                            .handle_orphaned_transactions(
                                                prune_result.orphaned_transactions,
                                                peer_addr,
                                            )
                                            .await;

                                        if prune_result.became_unready {
                                            ctx.handle_broadcast_ready_state(false).await;
                                        }

                                        if let Some(ref pub_key) = pub_key_opt {
                                            ctx.bridge.op_manager.on_ring_connection_lost(pub_key);
                                        }

                                        // Remove from connection map
                                        tracing::trace!(
                                            peer_addr = %peer_addr,
                                            conn_map_size = ctx.connections.len(),
                                            reason = "channel_closed",
                                            "Removing connection from tracking map"
                                        );
                                        ctx.connections.remove(&peer_addr);
                                        if let Some(pub_key) = pub_key_opt {
                                            ctx.addr_by_pub_key.remove(&pub_key);
                                        }

                                        // Notify handshake handler to clean up.
                                        // Non-blocking (#4145): runs on the event-loop task,
                                        // so a `.send().await` that back-pressures on the
                                        // handshake command channel (cap 128) would stall the
                                        // loop. Drop on full — the DropConnection is redundant
                                        // cleanup: the connection has already been removed from
                                        // ctx.connections / addr_by_pub_key and pruned from the
                                        // ring above. The only residue is the handshake driver's
                                        // ExpectedInboundTracker entry (if any). That tracker is
                                        // a per-SocketAddr HashMap with no TTL sweep, but the
                                        // entry is benign and bounded: it is overwritten on the
                                        // next register() for the same addr and consumed when a
                                        // matching inbound arrives, so a missed DropConnection
                                        // leaves at most one stale per-addr expectation, not an
                                        // unbounded leak.
                                        if !handshake_cmd_sender.try_send(
                                            HandshakeCommand::DropConnection { peer: peer.clone() },
                                        ) {
                                            tracing::warn!(
                                                peer = %peer,
                                                phase = "cleanup",
                                                "Handshake command channel full/closed during \
                                                 cleanup; skipping redundant drop notification"
                                            );
                                        }
                                    }

                                    // Clean up reservations for in-progress connections
                                    // These are connections that started handshake but haven't completed yet
                                    // Notifying the callbacks will trigger the calling code to clean up reservations
                                    tracing::debug!(
                                        awaiting_count = state.awaiting_connection.len(),
                                        phase = "cleanup",
                                        "Cleaning up in-progress connection reservations"
                                    );

                                    for (addr, callbacks) in state.awaiting_connection.drain() {
                                        tracing::debug!(
                                            peer_addr = %addr,
                                            callbacks = callbacks.len(),
                                            phase = "cleanup",
                                            "Notifying awaiting connection of shutdown"
                                        );
                                        // Best effort notification during shutdown - receiver may already be dropped
                                        for mut callback in callbacks {
                                            #[allow(clippy::let_underscore_must_use)]
                                            let _ = callback.send_result(Err(())).await;
                                        }
                                    }

                                    // Clean up pending operation result callbacks
                                    // Drop all waiting senders so callers see channel closure
                                    let pending_count = state.pending_op_results.len();
                                    if pending_count > 0 {
                                        tracing::debug!(
                                            pending_count,
                                            phase = "cleanup",
                                            "Draining pending_op_results"
                                        );
                                        for _ in 0..pending_count {
                                            crate::config::GlobalTestMetrics::record_pending_op_remove();
                                        }
                                        state.pending_op_results.clear();
                                    }

                                    tracing::info!(
                                        phase = "shutdown",
                                        "Cleanup complete - exiting event loop"
                                    );
                                    graceful_shutdown = true;
                                    break;
                                }
                            }
                        }
                        ConnEvent::NodeAction(action) => match action {
                            NodeEvent::DropConnection(peer_addr) => {
                                if !ctx
                                    .drop_connection_by_addr(peer_addr, &handshake_cmd_sender)
                                    .await
                                {
                                    tracing::debug!(
                                        peer_addr = %peer_addr,
                                        phase = "disconnect",
                                        "Drop connection requested for unknown address - ignoring"
                                    );
                                }
                            }
                            NodeEvent::DropAllConnections => {
                                let all_addrs: Vec<SocketAddr> =
                                    ctx.connections.keys().copied().collect();
                                tracing::warn!(
                                    count = all_addrs.len(),
                                    "DropAllConnections: closing all connections (suspend/resume recovery)"
                                );
                                // Use drop_connection_by_addr (blocking, 1s timeout) here
                                // rather than drop_zombie_connection because these may be
                                // healthy connections that need proper close notification.
                                // The deadlock risk is low: DropAllConnections fires only
                                // during suspend/resume recovery (rare), and during resume
                                // there are few inbound connections to fill the events channel.
                                for peer_addr in all_addrs {
                                    ctx.drop_connection_by_addr(peer_addr, &handshake_cmd_sender)
                                        .await;
                                }
                            }
                            NodeEvent::ConnectPeer {
                                peer,
                                tx,
                                callback,
                                is_gw: transient,
                            } => {
                                tracing::debug!(
                                    tx = %tx,
                                    peer = %peer,
                                    peer_addr = ?peer.socket_addr(),
                                    transient,
                                    phase = "connect",
                                    "Received connect peer request"
                                );
                                ctx.handle_connect_peer(
                                    peer,
                                    Box::new(callback),
                                    tx,
                                    &handshake_cmd_sender,
                                    &mut state,
                                    transient,
                                )
                                .await?;
                            }
                            NodeEvent::ExpectPeerConnection { addr } => {
                                tracing::debug!(
                                    peer_addr = %addr,
                                    direction = "inbound",
                                    phase = "connect",
                                    "Expecting peer connection - registering with handshake handler"
                                );
                                state.expected_inbound.expect_incoming(addr);
                                // We don't know the peer's public key yet for expected inbound connections,
                                // so use our own pub_key as a placeholder. The handshake will update it.
                                let peer_placeholder = PeerKeyLocation::new(
                                    (*ctx.bridge.op_manager.ring.connection_manager.pub_key)
                                        .clone(),
                                    addr,
                                );
                                // Non-blocking (#4145): this ExpectPeerConnection
                                // handler runs on the event-loop task, so a
                                // `.send().await` that back-pressures on the
                                // handshake command channel (cap 128) would stall the
                                // loop. Dropping the ExpectInbound registration is
                                // recoverable: when the inbound connection actually
                                // arrives without a matching expectation it is still
                                // accepted provisionally (see handle_handshake_action,
                                // "accepting provisionally"), and the originating
                                // connect op retries if its handshake does not
                                // complete. We `warn!` because a dropped expectation
                                // makes admission less reliable under load.
                                if !handshake_cmd_sender.try_send(HandshakeCommand::ExpectInbound {
                                    peer: peer_placeholder,
                                    transaction: None,
                                    transient: false,
                                }) {
                                    tracing::warn!(
                                        peer_addr = %addr,
                                        phase = "connect",
                                        "Handshake command channel full/closed; dropped \
                                         ExpectInbound registration. Inbound connection will \
                                         be accepted provisionally; connect op retries if the \
                                         handshake does not complete."
                                    );
                                }
                            }
                            NodeEvent::QueryConnections { callback } => {
                                // Reconstruct PeerKeyLocations from stored connections
                                let total_connections = ctx.connections.len();
                                let connections: Vec<PeerKeyLocation> = ctx
                                    .connections
                                    .iter()
                                    .filter_map(|(addr, entry)| {
                                        // Only include connections where we know the public key
                                        entry.pub_key.as_ref().map(|pub_key| {
                                            PeerKeyLocation::new(pub_key.clone(), *addr)
                                        })
                                    })
                                    .collect();
                                let with_pub_key = connections.len();
                                tracing::debug!(
                                    total_connections,
                                    with_pub_key,
                                    phase = "query",
                                    "Returning query connections result"
                                );
                                match timeout(
                                    Duration::from_secs(1),
                                    callback.send(QueryResult::Connections(connections)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(send_error)) => {
                                        tracing::error!(
                                            error = ?send_error,
                                            phase = "error",
                                            "Failed to send connections query result"
                                        );
                                    }
                                    Err(elapsed) => {
                                        tracing::error!(
                                            error = ?elapsed,
                                            phase = "timeout",
                                            "Timeout sending connections query result"
                                        );
                                    }
                                }
                            }
                            NodeEvent::QuerySubscriptions { callback } => {
                                // Get network subscriptions from OpManager
                                let network_subs = op_manager.get_network_subscriptions();
                                // Convert PeerKeyLocations to SocketAddrs for NetworkDebugInfo
                                let network_subs: Vec<(
                                    freenet_stdlib::prelude::ContractKey,
                                    Vec<SocketAddr>,
                                )> = network_subs
                                    .into_iter()
                                    .map(|(key, peers)| {
                                        let addrs = peers
                                            .into_iter()
                                            .filter_map(|p| p.socket_addr())
                                            .collect();
                                        (key, addrs)
                                    })
                                    .collect();

                                // Get application subscriptions from the contract handler.
                                // #4549: bounded + non-fatal (see
                                // `query_app_subscriptions_bounded`) — a saturated handler
                                // must never stall or kill the network event listener.
                                let app_subscriptions =
                                    query_app_subscriptions_bounded(&op_manager.ch_outbound).await;

                                // Log network subscription details for debugging
                                for (contract_key, peers) in &network_subs {
                                    if !peers.is_empty() {
                                        tracing::trace!(
                                            contract = %contract_key,
                                            subscriber_count = peers.len(),
                                            phase = "query",
                                            "Network subscription found"
                                        );
                                    }
                                }

                                // Reconstruct PeerKeyLocations from stored connections
                                let connections: Vec<PeerKeyLocation> = ctx
                                    .connections
                                    .iter()
                                    .filter_map(|(addr, entry)| {
                                        // Only include connections where we know the public key
                                        entry.pub_key.as_ref().map(|pub_key| {
                                            PeerKeyLocation::new(pub_key.clone(), *addr)
                                        })
                                    })
                                    .collect();
                                let debug_info = crate::message::NetworkDebugInfo {
                                    application_subscriptions: app_subscriptions,
                                    network_subscriptions: network_subs,
                                    connected_peers: connections,
                                };

                                match timeout(
                                    Duration::from_secs(1),
                                    callback.send(QueryResult::NetworkDebug(debug_info)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(send_error)) => {
                                        tracing::error!(
                                            error = ?send_error,
                                            phase = "error",
                                            "Failed to send subscriptions query result"
                                        );
                                    }
                                    Err(elapsed) => {
                                        tracing::error!(
                                            error = ?elapsed,
                                            phase = "timeout",
                                            "Timeout sending subscriptions query result"
                                        );
                                    }
                                }
                            }
                            NodeEvent::QueryNodeDiagnostics { config, callback } => {
                                use freenet_stdlib::client_api::{
                                    NetworkInfo, NodeDiagnosticsResponse, NodeInfo, SystemMetrics,
                                };
                                use std::collections::HashMap;

                                let mut response = NodeDiagnosticsResponse {
                                    node_info: None,
                                    network_info: None,
                                    subscriptions: Vec::new(),
                                    contract_states: HashMap::new(),
                                    system_metrics: None,
                                    connected_peers_detailed: Vec::new(),
                                };

                                // Collect node information
                                if config.include_node_info {
                                    // Get stored location (set by ObservedAddress handler) and address
                                    // IMPORTANT: Use get_stored_location() rather than computing from
                                    // get_own_addr() because the address may be a fallback value for
                                    // peers behind NAT, while the stored location comes from the
                                    // externally observed address.
                                    let addr = op_manager.ring.connection_manager.get_own_addr();
                                    let location =
                                        op_manager.ring.connection_manager.get_stored_location();

                                    // Always include basic node info, but only include address/location if available
                                    response.node_info = Some(NodeInfo {
                                        peer_id: ctx.key_pair.public().to_string(),
                                        is_gateway: self.is_gateway,
                                        location: location.map(|loc| format!("{:.6}", loc.0)),
                                        listening_address: addr
                                            .map(|peer_addr| peer_addr.to_string()),
                                        uptime_seconds: 0, // TODO: implement actual uptime tracking
                                    });
                                }

                                // Collect network information
                                if config.include_network_info {
                                    let cm = &op_manager.ring.connection_manager;
                                    let connections_by_loc = cm.get_connections_by_location();
                                    let mut connected_peers = Vec::new();
                                    for conns in connections_by_loc.values() {
                                        for conn in conns {
                                            connected_peers.push((
                                                conn.location.pub_key().to_string(),
                                                conn.location
                                                    .socket_addr()
                                                    .expect("connection should have address")
                                                    .to_string(),
                                            ));
                                        }
                                    }
                                    connected_peers.sort_by(|a, b| a.0.cmp(&b.0));
                                    connected_peers.dedup_by(|a, b| a.0 == b.0);

                                    response.network_info = Some(NetworkInfo {
                                        active_connections: connected_peers.len(),
                                        connected_peers,
                                    });
                                }

                                // Collect subscription information
                                if config.include_subscriptions {
                                    // Get network subscriptions from OpManager
                                    let _network_subs = op_manager.get_network_subscriptions();

                                    // Get application subscriptions from the contract handler.
                                    // #4549: bounded + non-fatal (see
                                    // `query_app_subscriptions_bounded`). On any failure this
                                    // serves an empty list rather than stalling or killing the
                                    // network event listener.
                                    response.subscriptions =
                                        query_app_subscriptions_bounded(&op_manager.ch_outbound)
                                            .await
                                            .into_iter()
                                            .map(|sub| {
                                                freenet_stdlib::client_api::SubscriptionInfo {
                                                    contract_key: sub.instance_id,
                                                    client_id: sub.client_id.into(),
                                                }
                                            })
                                            .collect();
                                }

                                // Collect contract states.
                                // When contract_keys is empty, return ALL hosting contracts.
                                // When specific keys are provided, return only those the
                                // node has local presence for (see `collect_contract_states`
                                // for the #3945 presence-filter rationale).
                                // Note: subscriber_peer_ids is always empty because we use
                                // lease-based subscriptions rather than explicit subscriber tracking.
                                response.contract_states = collect_contract_states(
                                    op_manager.ring.as_ref(),
                                    &config.contract_keys,
                                );

                                // Collect topology-backed connection info (exclude transient transports).
                                let cm = &op_manager.ring.connection_manager;
                                let connections_by_loc = cm.get_connections_by_location();
                                let mut connected_peer_ids = Vec::new();
                                if config.include_detailed_peer_info {
                                    use freenet_stdlib::client_api::ConnectedPeerInfo;
                                    for conns in connections_by_loc.values() {
                                        for conn in conns {
                                            connected_peer_ids
                                                .push(conn.location.pub_key().to_string());
                                            response.connected_peers_detailed.push(
                                                ConnectedPeerInfo {
                                                    peer_id: conn.location.pub_key().to_string(),
                                                    address: conn
                                                        .location
                                                        .socket_addr()
                                                        .expect("connection should have address")
                                                        .to_string(),
                                                },
                                            );
                                        }
                                    }
                                } else {
                                    for conns in connections_by_loc.values() {
                                        connected_peer_ids.extend(
                                            conns.iter().map(|c| c.location.pub_key().to_string()),
                                        );
                                    }
                                }
                                connected_peer_ids.sort();
                                connected_peer_ids.dedup();

                                // Collect system metrics
                                if config.include_system_metrics {
                                    // Use the actual hosting cache count, not the subscriber count.
                                    // The hosting cache tracks contracts this node is actively hosting,
                                    // while subscribers tracks remote peers subscribed to updates.
                                    let hosting_contracts =
                                        op_manager.ring.hosting_contracts_count() as u32;
                                    // Log memory/operation metrics for debugging (#2928)
                                    let pending_ops = op_manager.pending_op_counts();
                                    let contract_waiters_count =
                                        op_manager.contract_waiters_count();
                                    let memory_rss_bytes = get_rss_bytes();
                                    tracing::info!(
                                        pending_connect = pending_ops[0],
                                        pending_put = pending_ops[1],
                                        pending_get = pending_ops[2],
                                        pending_subscribe = pending_ops[3],
                                        pending_update = pending_ops[4],
                                        contract_waiters = contract_waiters_count,
                                        memory_rss_bytes = ?memory_rss_bytes,
                                        "Node diagnostics: operation & memory metrics"
                                    );
                                    response.system_metrics = Some(SystemMetrics {
                                        active_connections: connected_peer_ids.len() as u32,
                                        hosting_contracts,
                                    });
                                }

                                match timeout(
                                    Duration::from_secs(2),
                                    callback.send(QueryResult::NodeDiagnostics(response)),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(send_error)) => {
                                        tracing::error!(
                                            error = ?send_error,
                                            phase = "error",
                                            "Failed to send node diagnostics query result"
                                        );
                                    }
                                    Err(elapsed) => {
                                        tracing::error!(
                                            error = ?elapsed,
                                            phase = "timeout",
                                            "Timeout sending node diagnostics query result"
                                        );
                                    }
                                }
                            }
                            NodeEvent::TransactionTimedOut(tx) => {
                                // Clean up client subscription to prevent memory leak
                                // Clients are not notified - transactions simply expire silently
                                if let Some(clients) = state.tx_to_client.remove(&tx) {
                                    tracing::debug!(
                                        tx = %tx,
                                        client_count = clients.len(),
                                        phase = "timeout",
                                        "Cleaned up client subscriptions for timed out transaction"
                                    );
                                }
                                // Clean up executor callback sender to prevent unbounded
                                // HashMap growth (entries were inserted by handle_op_execution
                                // but never removed — see #2941)
                                if state.pending_op_results.remove(&tx).is_some() {
                                    crate::config::GlobalTestMetrics::record_pending_op_remove();
                                }
                            }
                            NodeEvent::TransactionCompleted(tx) => {
                                // Clean up client subscription after successful completion
                                state.tx_to_client.remove(&tx);
                                // Clean up executor callback sender
                                if state.pending_op_results.remove(&tx).is_some() {
                                    crate::config::GlobalTestMetrics::record_pending_op_remove();
                                }
                                // Clean up the `live_tx_tracker` entry registered
                                // by `handle_op_execution` for messages with an
                                // explicit target (#4154); otherwise per-attempt
                                // entries leak past op completion. Idempotent with
                                // the `op_manager.completed(client_tx)` that
                                // drivers call on terminal exit.
                                ctx.bridge
                                    .op_manager
                                    .ring
                                    .live_tx_tracker
                                    .remove_finished_transaction(tx);
                            }
                            NodeEvent::TransactionOrphaned { tx, peer } => {
                                // The awaited peer was pruned mid-flight (#4313).
                                // Deliver the cause THROUGH the waiter channel
                                // before the sender drops, so the parked driver's
                                // recv() yields PeerDisconnected before None —
                                // race-free, no side registry. Then clean up like
                                // TransactionCompleted.
                                state.tx_to_client.remove(&tx);
                                if let Some(sender) = state.pending_op_results.remove(&tx) {
                                    // Best-effort: a full channel means a real
                                    // reply is already queued, a closed one means
                                    // the driver already exited — both benign.
                                    #[allow(clippy::let_underscore_must_use)]
                                    let _ = sender.try_send(WaiterReply::PeerDisconnected { peer });
                                    crate::config::GlobalTestMetrics::record_pending_op_remove();
                                }
                                ctx.bridge
                                    .op_manager
                                    .ring
                                    .live_tx_tracker
                                    .remove_finished_transaction(tx);
                            }
                            NodeEvent::LocalSubscribeComplete {
                                tx,
                                key,
                                subscribed,
                                is_renewal,
                            } => {
                                // This event is only fired for STANDALONE subscriptions (no remote peers).
                                // Normal subscribe flow now goes through handle_op_result which sends
                                // results via result_router_tx directly.
                                tracing::debug!(
                                    tx = %tx,
                                    contract = %key,
                                    is_renewal,
                                    phase = "complete",
                                    "Standalone subscribe operation completed"
                                );

                                // If this is a child operation (e.g., Subscribe spawned by PUT),
                                // just mark it complete - parent operation handles client response.
                                // Uses the structural Transaction::is_sub_operation() check
                                // (parent field set at creation via new_child_of).
                                if tx.is_sub_operation() {
                                    tracing::debug!(
                                        tx = %tx,
                                        contract = %key,
                                        phase = "complete",
                                        "Completing standalone child subscribe operation"
                                    );
                                    op_manager.completed(tx);
                                    continue;
                                }

                                // Subscription renewals are node-internal operations with no
                                // client waiting for results. Skip client notification. See #2891.
                                if is_renewal {
                                    tracing::debug!(
                                        tx = %tx,
                                        contract = %key,
                                        phase = "complete",
                                        "Skipping client notification for standalone subscription renewal"
                                    );
                                    op_manager.completed(tx);
                                    continue;
                                }

                                // Standalone parent operation - send response to client
                                let response = Ok(HostResponse::ContractResponse(
                                    ContractResponse::SubscribeResponse { key, subscribed },
                                ));

                                // Use try_send to avoid blocking the event loop.
                                // If the result router channel is full, the client
                                // will see a timeout rather than deadlocking the node.
                                match op_manager.result_router_tx.try_send((tx, response)) {
                                    Ok(()) => {
                                        tracing::debug!(
                                            tx = %tx,
                                            phase = "response",
                                            "Sent standalone subscribe response to client"
                                        );
                                        if let Some(clients) = state.tx_to_client.remove(&tx) {
                                            tracing::trace!(
                                                tx = %tx,
                                                client_count = clients.len(),
                                                "Removed waiting clients for completed transaction"
                                            );
                                        } else if let Some(pos) = state
                                            .client_waiting_transaction
                                            .iter()
                                            .position(|(waiting, _)| match waiting {
                                                WaitingTransaction::Subscription {
                                                    contract_key,
                                                } => contract_key == key.id(),
                                                _ => false,
                                            })
                                        {
                                            let (_, clients) =
                                                state.client_waiting_transaction.remove(pos);
                                            tracing::trace!(
                                                tx = %tx,
                                                contract = %key,
                                                waiter_count = clients.len(),
                                                "Matched subscription waiters by contract"
                                            );
                                        } else {
                                            tracing::warn!(
                                                tx = %tx,
                                                phase = "complete",
                                                "Standalone subscribe complete but no waiting clients found"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            tx = %tx,
                                            error = %e,
                                            phase = "error",
                                            "Failed to send standalone subscribe response to client \
                                             (result router channel full or closed)"
                                        );
                                    }
                                }
                            }
                            NodeEvent::BroadcastHostingUpdate { message } => {
                                ctx.handle_hosting_broadcast(message).await;
                            }
                            NodeEvent::BroadcastChangeInterests { added, removed } => {
                                ctx.handle_broadcast_change_interests(added, removed).await;
                            }
                            NodeEvent::SendInterestMessage { target, message } => {
                                ctx.handle_send_interest_message(target, message).await;
                            }
                            NodeEvent::SendNetMessage { target, msg } => {
                                ctx.handle_send_net_message(target, *msg).await;
                            }
                            NodeEvent::Disconnect { cause } => {
                                tracing::info!(
                                    cause = ?cause,
                                    phase = "shutdown",
                                    "Disconnecting from network"
                                );
                                graceful_shutdown = true;
                                break;
                            }
                            NodeEvent::BroadcastStateChange {
                                key,
                                new_state,
                                is_retry,
                                is_reemit,
                            } => {
                                ctx.handle_broadcast_state_change(
                                    &op_manager,
                                    key,
                                    new_state,
                                    is_retry,
                                    is_reemit,
                                )
                                .await;
                            }
                            NodeEvent::SyncStateToPeer {
                                key,
                                new_state,
                                target,
                            } => {
                                ctx.handle_sync_state_to_peer(&op_manager, key, new_state, target)
                                    .await;
                            }
                            NodeEvent::ConsiderContractMigration { key } => {
                                ctx.consider_contract_migration(key);
                            }
                        },
                    }
                }
            }
        }
        if graceful_shutdown {
            // Return typed error for graceful shutdown - callers can use downcast_ref()
            // to identify this as a clean exit rather than an actual error
            Err(EventLoopExitReason::GracefulShutdown.into())
        } else {
            Err(EventLoopExitReason::UnexpectedStreamEnd.into())
        }
    }
}

trait ConnectResultSender: Send {
    fn send_result(
        &mut self,
        result: Result<(SocketAddr, Option<usize>), ()>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>>;
}

impl ConnectResultSender for mpsc::Sender<Result<(SocketAddr, Option<usize>), ()>> {
    fn send_result(
        &mut self,
        result: Result<(SocketAddr, Option<usize>), ()>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ()>> + Send + '_>> {
        async move { self.send(result).await.map_err(|_| ()) }.boxed()
    }
}

/// Fail every callback awaiting a connection to `peer_addr` after a connect
/// attempt was abandoned (e.g. its `HandshakeCommand::Connect` enqueue was
/// dropped on a full/closed handshake command channel, #4145).
///
/// Extracted as a free function so its callback/state invariants are
/// unit-testable without standing up a full `P2pConnManager`:
///
/// 1. Every awaiting callback for `peer_addr` is resolved with `Err(())` — never
///    left parked — so callers (including the SubscribeHint resend-waiter) fail
///    fast and the op can retry.
/// 2. `awaiting_connection` AND `awaiting_connection_txs` are both drained for
///    `peer_addr` (kept in lock-step; a stranded tx entry would mislead later
///    diagnostics).
/// 3. NO peer backoff is recorded: a full LOCAL channel is resource contention,
///    not a remote connection failure, so backing the peer off would wrongly
///    delay a legitimate retry. (This function simply never touches
///    `state.peer_backoff`; the test asserts it stays clear.)
///
/// The caller is responsible for the connection-manager side effect
/// (`prune_in_transit_connection`), which is the one piece that needs the live
/// ring — see the call site in `connect_peer`.
async fn fail_awaiting_connection(
    state: &mut EventListenerState,
    peer_addr: SocketAddr,
    tx: Transaction,
    transient: bool,
) {
    // (2) drain both maps in lock-step.
    let pending_txs = state.awaiting_connection_txs.remove(&peer_addr);

    // (1) fail every awaiting callback — never leave one parked.
    if let Some(callbacks) = state.awaiting_connection.remove(&peer_addr) {
        tracing::debug!(
            tx = %tx,
            remote = %peer_addr,
            callbacks = callbacks.len(),
            transient,
            "Cleaning up callbacks after connect command failure"
        );
        for mut cb in callbacks {
            cb.send_result(Err(()))
                .await
                .inspect_err(|send_err| {
                    tracing::debug!(
                        remote = %peer_addr,
                        ?send_err,
                        "Failed to deliver connect command failure to awaiting callback"
                    );
                })
                .ok();
        }
    }
    if let Some(pending_txs) = pending_txs {
        tracing::debug!(
            remote = %peer_addr,
            pending_txs = ?pending_txs,
            "Removed pending transactions after connect command failure"
        );
    }
    // (3) Deliberately NO state.peer_backoff.record_failure() here.
}

struct EventListenerState {
    expected_inbound: ExpectedInboundTracker,
    pending_from_executor: HashSet<Transaction>,
    /// Maps transactions to the set of clients waiting for their results.
    /// Cleaned up via `TransactionTimedOut` and `TransactionCompleted` handlers.
    tx_to_client: HashMap<Transaction, HashSet<ClientId>>,
    client_waiting_transaction: Vec<(WaitingTransaction, HashSet<ClientId>)>,
    awaiting_connection: HashMap<SocketAddr, Vec<Box<dyn ConnectResultSender>>>,
    awaiting_connection_txs: HashMap<SocketAddr, Vec<Transaction>>,
    pending_op_results: HashMap<Transaction, Sender<WaiterReply>>,
    /// Last time pending_op_results was scanned for closed senders.
    last_pending_op_cleanup: Instant,
    /// Per-peer backoff tracking for failed connection attempts.
    /// Prevents rapid repeated connection attempts to the same peer.
    peer_backoff: PeerConnectionBackoff,
    /// Last time peer_backoff was cleaned up.
    last_backoff_cleanup: Instant,
}

impl EventListenerState {
    fn new(expected_inbound: ExpectedInboundTracker) -> Self {
        Self {
            expected_inbound,
            pending_from_executor: HashSet::new(),
            tx_to_client: HashMap::new(),
            client_waiting_transaction: Vec::new(),
            awaiting_connection: HashMap::new(),
            pending_op_results: HashMap::new(),
            last_pending_op_cleanup: Instant::now(),
            awaiting_connection_txs: HashMap::new(),
            peer_backoff: PeerConnectionBackoff::new(),
            last_backoff_cleanup: Instant::now(),
        }
    }
}

impl Drop for EventListenerState {
    fn drop(&mut self) {
        // Balance pending_op_results accounting on every event-loop exit path
        // (graceful Shutdown, simulation `handle.abort()`, Disconnect,
        // unexpected stream end). Memory is freed regardless; only the metric
        // backing #3100's regression guard `test_pending_op_results_bounded`
        // depends on this, so a missed remove looks like a phantom leak (#4057).
        //
        // No double-counting on graceful Shutdown: the explicit drain at the
        // ClosedChannel(ChannelCloseReason::Shutdown) branch above already
        // records `pending_count` removes and then `pending_op_results.drain()`
        // empties the map, so the loop below sees `len() == 0`.
        for _ in 0..self.pending_op_results.len() {
            crate::config::GlobalTestMetrics::record_pending_op_remove();
        }
    }
}

enum EventResult {
    Continue,
    Event(Box<ConnEvent>),
}

#[derive(Debug)]
pub(super) enum ConnEvent {
    InboundMessage(IncomingMessage),
    OutboundMessage(NetMessage),
    /// Outbound message with explicit target address from P2pBridge::send().
    /// Used when the target address differs from what's in the message (NAT scenarios).
    /// The target_addr is the actual transport address to send to.
    OutboundMessageWithTarget {
        target_addr: SocketAddr,
        msg: NetMessage,
    },
    NodeAction(NodeEvent),
    ClosedChannel(ChannelCloseReason),
    TransportClosed {
        remote_addr: SocketAddr,
        error: TransportError,
        /// ID of the connection entry that spawned the listener reporting this closure.
        /// Used to ignore stale events from replaced connections.
        connection_id: u64,
    },
    /// Send raw stream data to a peer via the transport's stream mechanism.
    /// Used by operations-level streaming to send large payloads as stream fragments.
    StreamSend {
        target_addr: SocketAddr,
        stream_id: StreamId,
        data: bytes::Bytes,
        metadata: Option<bytes::Bytes>,
        /// Optional completion signal for broadcast queue concurrency control,
        /// carrying a [`BroadcastDeliveryOutcome`] (#4235).
        completion_tx: Option<tokio::sync::oneshot::Sender<BroadcastDeliveryOutcome>>,
        /// Optional per-fragment progress handle for the streaming-PUT retry
        /// loop (#4001). `None` for every non-originator path.
        progress: Option<crate::operations::stream_progress::StreamProgressHandle>,
    },
    /// Pipe an inbound stream to a peer, forwarding fragments as they arrive.
    /// Used for low-latency forwarding at intermediate nodes.
    PipeStream {
        target_addr: SocketAddr,
        outbound_stream_id: StreamId,
        inbound_handle: crate::transport::peer_connection::streaming::StreamHandle,
        metadata: Option<bytes::Bytes>,
    },
}

#[derive(Debug)]
pub(super) struct IncomingMessage {
    pub msg: NetMessage,
    pub remote_addr: Option<SocketAddr>,
}

impl IncomingMessage {
    fn with_remote(msg: NetMessage, remote_addr: SocketAddr) -> Self {
        Self {
            msg,
            remote_addr: Some(remote_addr),
        }
    }
}

impl From<NetMessage> for IncomingMessage {
    fn from(msg: NetMessage) -> Self {
        Self {
            msg,
            remote_addr: None,
        }
    }
}

#[derive(Debug)]
pub(super) enum ChannelCloseReason {
    /// Internal bridge channel closed - critical, must shutdown gracefully
    Bridge,
    /// Node controller channel closed - critical, must shutdown gracefully
    Controller,
    /// Notification channel closed - critical, must shutdown gracefully
    Notification,
    /// Op execution channel closed - critical, must shutdown gracefully
    OpExecution,
}

#[allow(dead_code)]
enum ProtocolStatus {
    Unconfirmed,
    Confirmed,
    Reported,
    Failed,
}

async fn handle_peer_channel_message(
    conn: &mut Box<dyn PeerConnectionApi>,
    msg: Either<NetMessage, ConnEvent>,
) -> Result<(), TransportError> {
    match msg {
        Left(msg) => {
            tracing::debug!(to=%conn.remote_addr() ,"Sending message to peer. Msg: {msg}");
            if let Err(error) = conn.send_message(msg).await {
                tracing::error!(
                    to = %conn.remote_addr(),
                    ?error,
                    "[CONN_LIFECYCLE] Failed to send message to peer"
                );
                return Err(error);
            }
            tracing::debug!(
                to = %conn.remote_addr(),
                "[CONN_LIFECYCLE] Message enqueued on transport socket"
            );
        }
        Right(action) => {
            tracing::debug!(to=%conn.remote_addr(), "Received action from channel");
            match action {
                ConnEvent::NodeAction(NodeEvent::DropConnection(peer)) => {
                    tracing::info!(
                        to = %conn.remote_addr(),
                        peer = %peer,
                        "[CONN_LIFECYCLE] Closing connection per DropConnection action"
                    );
                    return Err(TransportError::ConnectionClosed(conn.remote_addr()));
                }
                ConnEvent::ClosedChannel(reason) => {
                    tracing::info!(
                        to = %conn.remote_addr(),
                        reason = ?reason,
                        "[CONN_LIFECYCLE] Closing connection due to ClosedChannel action"
                    );
                    return Err(TransportError::ConnectionClosed(conn.remote_addr()));
                }
                ConnEvent::StreamSend {
                    stream_id,
                    data,
                    metadata,
                    completion_tx,
                    progress,
                    ..
                } => {
                    tracing::debug!(
                        to = %conn.remote_addr(),
                        stream_id = %stream_id,
                        data_len = data.len(),
                        has_metadata = metadata.is_some(),
                        "[CONN_LIFECYCLE] Sending operations stream data to peer"
                    );
                    if let Err(error) = conn
                        .send_stream_data(stream_id, data, metadata, completion_tx, progress)
                        .await
                    {
                        tracing::error!(
                            to = %conn.remote_addr(),
                            stream_id = %stream_id,
                            ?error,
                            "[CONN_LIFECYCLE] Failed to send stream data to peer"
                        );
                        return Err(error);
                    }
                }
                ConnEvent::PipeStream {
                    outbound_stream_id,
                    inbound_handle,
                    metadata,
                    ..
                } => {
                    tracing::debug!(
                        to = %conn.remote_addr(),
                        stream_id = %outbound_stream_id,
                        total_bytes = inbound_handle.total_bytes(),
                        has_metadata = metadata.is_some(),
                        "[CONN_LIFECYCLE] Piping stream to peer"
                    );
                    if let Err(error) = conn
                        .pipe_stream_data(outbound_stream_id, inbound_handle, metadata, None)
                        .await
                    {
                        tracing::error!(
                            to = %conn.remote_addr(),
                            stream_id = %outbound_stream_id,
                            ?error,
                            "[CONN_LIFECYCLE] Failed to pipe stream to peer"
                        );
                        return Err(error);
                    }
                }
                other @ ConnEvent::InboundMessage(_)
                | other @ ConnEvent::OutboundMessage(_)
                | other @ ConnEvent::OutboundMessageWithTarget { .. }
                | other @ ConnEvent::NodeAction(_)
                | other @ ConnEvent::TransportClosed { .. } => {
                    unreachable!(
                        "Unexpected action from peer_connection_listener channel: {:?}",
                        other
                    );
                }
            }
        }
    }
    Ok(())
}

async fn notify_transport_closed(
    sender: &Sender<ConnEvent>,
    remote_addr: SocketAddr,
    error: TransportError,
    connection_id: u64,
) {
    if sender
        .send(ConnEvent::TransportClosed {
            remote_addr,
            error,
            connection_id,
        })
        .await
        .is_err()
    {
        tracing::debug!(
            remote = %remote_addr,
            "[CONN_LIFECYCLE] conn_events receiver dropped before handling closure event"
        );
    }
}

/// Drains and sends all pending outbound messages before shutting down.
///
/// This is critical for preventing message loss: when we detect an error condition
/// (transport error, channel closed, etc.), there may be messages that were queued
/// to the channel AFTER we started waiting in select! but BEFORE we detected the error.
/// Without this drain, those messages would be silently lost.
async fn drain_pending_before_shutdown(
    rx: &mut PeerConnChannelRecv,
    conn: &mut Box<dyn PeerConnectionApi>,
    remote_addr: SocketAddr,
) -> usize {
    let mut drained = 0;
    while let Ok(msg) = rx.try_recv() {
        // Best-effort send - connection may already be degraded
        if let Err(e) = handle_peer_channel_message(conn, msg).await {
            tracing::debug!(
                to = %remote_addr,
                ?e,
                drained,
                "[CONN_LIFECYCLE] Error during shutdown drain (expected if connection closed)"
            );
            break;
        }
        drained += 1;
    }
    if drained > 0 {
        tracing::info!(
            to = %remote_addr,
            drained,
            "[CONN_LIFECYCLE] Drained pending messages before shutdown"
        );
    }
    drained
}

/// Listens for messages on a peer connection using drain-then-select pattern.
///
/// On each iteration, drains all pending outbound messages via `try_recv()` before
/// waiting for either new outbound or inbound messages. This approach:
/// 1. Ensures queued outbound messages are sent promptly
/// 2. Avoids starving inbound (which would happen with biased select)
/// 3. No messages are lost due to poll ordering
///
/// IMPORTANT: Before exiting on any error path, we drain pending messages to prevent
/// loss of messages that arrived during the select! wait.
async fn peer_connection_listener(
    mut rx: PeerConnChannelRecv,
    mut conn: Box<dyn PeerConnectionApi>,
    peer_addr: SocketAddr,
    conn_events: Sender<ConnEvent>,
    connection_id: u64,
) {
    let remote_addr = conn.remote_addr();
    tracing::debug!(
        to = %remote_addr,
        %peer_addr,
        "[CONN_LIFECYCLE] Starting peer_connection_listener task"
    );

    loop {
        // Yield to allow the tokio runtime to schedule other tasks.
        // This is critical for cooperative scheduling - without it, tokio's coop
        // budget can be exhausted and channel recv() will return Pending even when
        // messages are available. See https://github.com/tokio-rs/tokio/issues/7108
        //
        // We cannot add this yield inside deterministic_select! because it would
        // break determinism guarantees for simulation testing.
        tokio::task::yield_now().await;

        // Drain pending outbound messages before checking inbound, but with a
        // bounded limit to prevent outbound from starving inbound packet processing.
        // Remaining messages are picked up by the fair select's outbound branch.
        // 8 messages ≈ 0.8ms of sends (8 × ~100µs per encrypt+send).
        const MAX_OUTBOUND_DRAIN: usize = 8;
        let mut drained = 0;
        loop {
            match rx.try_recv() {
                Ok(msg) => {
                    drained += 1;
                    if let Err(error) = handle_peer_channel_message(&mut conn, msg).await {
                        if error.is_transient_send_failure() {
                            tracing::warn!(
                                to = %remote_addr,
                                ?error,
                                "[CONN_LIFECYCLE] Transient send failure, continuing"
                            );
                        } else {
                            tracing::debug!(
                                to = %remote_addr,
                                ?error,
                                "[CONN_LIFECYCLE] Connection closed after channel command"
                            );
                            // Drain any messages that arrived after our try_recv() but before
                            // handle_peer_channel_message returned an error
                            drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                            notify_transport_closed(
                                &conn_events,
                                remote_addr,
                                error,
                                connection_id,
                            )
                            .await;
                            return;
                        }
                    }
                    if drained >= MAX_OUTBOUND_DRAIN {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    tracing::warn!(
                        to = %remote_addr,
                        "[CONN_LIFECYCLE] peer_connection_listener channel closed without explicit DropConnection"
                    );
                    // Channel disconnected means no more messages can arrive
                    notify_transport_closed(
                        &conn_events,
                        remote_addr,
                        TransportError::ConnectionClosed(remote_addr),
                        connection_id,
                    )
                    .await;
                    return;
                }
            }
        }

        // Now wait for either new outbound or inbound messages fairly
        // Uses deterministic_select! for consistent test behavior under DST
        crate::deterministic_select! {
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        if let Err(error) = handle_peer_channel_message(&mut conn, msg).await {
                            if error.is_transient_send_failure() {
                                tracing::warn!(
                                    to = %remote_addr,
                                    ?error,
                                    "[CONN_LIFECYCLE] Transient send failure, continuing"
                                );
                            } else {
                                tracing::debug!(
                                    to = %remote_addr,
                                    ?error,
                                    "[CONN_LIFECYCLE] Connection closed after channel command"
                                );
                                // Drain any messages that arrived while we were processing this one
                                drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                                notify_transport_closed(&conn_events, remote_addr, error, connection_id).await;
                                return;
                            }
                        }
                    }
                    None => {
                        tracing::warn!(
                            to = %remote_addr,
                            "[CONN_LIFECYCLE] peer_connection_listener channel closed without explicit DropConnection"
                        );
                        // Channel closed means no more messages can arrive
                        notify_transport_closed(
                            &conn_events,
                            remote_addr,
                            TransportError::ConnectionClosed(remote_addr),
                            connection_id,
                        )
                        .await;
                        return;
                    }
                }
            },
            msg = conn.recv() => {
                match msg {
                    Ok(msg) => match decode_msg(&msg) {
                        Ok(net_message) => {
                            let tx = *net_message.id();
                            tracing::debug!(
                                from = %remote_addr,
                                %tx,
                                tx_type = ?tx.transaction_type(),
                                msg_type = %net_message,
                                "[CONN_LIFECYCLE] Received inbound NetMessage from peer"
                            );
                            if conn_events
                                .send(ConnEvent::InboundMessage(IncomingMessage::with_remote(
                                    net_message,
                                    remote_addr,
                                )))
                                .await
                                .is_err()
                            {
                                tracing::debug!(
                                    from = %remote_addr,
                                    "[CONN_LIFECYCLE] conn_events receiver dropped; stopping listener"
                                );
                                // Drain pending messages - they may still be sendable even if
                                // the conn_events channel is closed
                                drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                                return;
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                from = %remote_addr,
                                ?error,
                                "[CONN_LIFECYCLE] Failed to deserialize inbound message; closing connection"
                            );
                            // Drain pending outbound messages before closing - they may still succeed
                            drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                            let transport_error = TransportError::Other(anyhow!(
                                "Failed to deserialize inbound message from {remote_addr}: {error:?}"
                            ));
                            notify_transport_closed(&conn_events, remote_addr, transport_error, connection_id).await;
                            return;
                        }
                    },
                    Err(error) if error.is_transient_send_failure() => {
                        // Transient send failure from internal sends (ACKs, etc.) that
                        // bubbled up through recv(). Don't kill the connection — the idle
                        // timeout is the sole authority on liveness.
                        tracing::warn!(
                            from = %remote_addr,
                            ?error,
                            "[CONN_LIFECYCLE] Transient send failure during recv, continuing"
                        );
                    }
                    Err(error) => {
                        tracing::debug!(
                            from = %remote_addr,
                            ?error,
                            "[CONN_LIFECYCLE] peer_connection_listener terminating after recv error"
                        );
                        // CRITICAL: Drain pending outbound messages before exiting.
                        // Messages may have been queued to the channel while we were
                        // waiting in select!, and they would be lost without this drain.
                        drain_pending_before_shutdown(&mut rx, &mut conn, remote_addr).await;
                        notify_transport_closed(&conn_events, remote_addr, error, connection_id).await;
                        return;
                    }
                }
            },
        }
    }
}

#[inline(always)]
fn decode_msg(data: &[u8]) -> Result<NetMessage, ConnectionError> {
    bincode::deserialize(data).map_err(|err| ConnectionError::Serialization(Some(err)))
}

/// Extract sender information from various message types.
/// Most message types use connection-based routing (sender determined from socket),
/// but ConnectMsg::Request contains the joiner's identity (pub_key) which we need
/// to associate with the transport-layer connection entry.
fn extract_sender_from_message(msg: &NetMessage) -> Option<PeerKeyLocation> {
    match msg {
        NetMessage::V1(msg_v1) => match msg_v1 {
            // ConnectMsg::Request contains the joiner's identity in payload.joiner.
            // The address may be Unknown (peer behind NAT), but the pub_key is always present.
            // We return the joiner so that the transport layer can update its connection entry
            // with the peer's identity, enabling QueryConnections to return the peer.
            NetMessageV1::Connect(ConnectMsg::Request { payload, .. }) => {
                Some(payload.joiner.clone())
            }
            // Other Connect messages (Response, ObservedAddress) and all other operations
            // use hop-by-hop routing via upstream_addr in operation state.
            // No sender/target is embedded - routing is determined by transport layer.
            NetMessageV1::Connect(_)
            | NetMessageV1::Get(_)
            | NetMessageV1::Put(_)
            | NetMessageV1::Update(_)
            | NetMessageV1::Subscribe(_) => None,
            // Other message types don't have sender info
            NetMessageV1::Aborted(_)
            | NetMessageV1::NeighborHosting { .. }
            | NetMessageV1::InterestSync { .. }
            | NetMessageV1::ReadyState { .. }
            | NetMessageV1::SubscribeHint(_) => None,
        },
    }
}

fn extract_sender_from_message_mut(msg: &mut NetMessage) -> Option<&mut PeerKeyLocation> {
    match msg {
        NetMessage::V1(msg_v1) => match msg_v1 {
            // All operations now use hop-by-hop routing via upstream_addr in operation state.
            // No sender/target is embedded in messages - routing is determined by transport layer.
            NetMessageV1::Connect(_)
            | NetMessageV1::Get(_)
            | NetMessageV1::Put(_)
            | NetMessageV1::Update(_)
            | NetMessageV1::Subscribe(_) => None,
            NetMessageV1::Aborted(_)
            | NetMessageV1::NeighborHosting { .. }
            | NetMessageV1::InterestSync { .. }
            | NetMessageV1::ReadyState { .. }
            | NetMessageV1::SubscribeHint(_) => None,
        },
    }
}

// TODO: add testing for the network loop, now it should be possible to do since we don't depend upon having real connections

#[cfg(test)]
mod tests {
    use crate::config::GlobalExecutor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, Instant, sleep, timeout};

    /// A fake [`super::ContractPresenceOracle`] for testing
    /// [`super::collect_contract_states`] without standing up a real `Ring`.
    /// `hosting` and `subscribed` are the sets the node "knows"; everything
    /// else is locally unknown.
    struct FakePresence {
        hosting: std::collections::HashSet<freenet_stdlib::prelude::ContractKey>,
        subscribed: std::collections::HashSet<freenet_stdlib::prelude::ContractKey>,
    }

    impl super::ContractPresenceOracle for FakePresence {
        fn is_hosting_contract(&self, key: &freenet_stdlib::prelude::ContractKey) -> bool {
            self.hosting.contains(key)
        }
        fn is_subscribed(&self, key: &freenet_stdlib::prelude::ContractKey) -> bool {
            self.subscribed.contains(key)
        }
        fn hosting_contract_keys(&self) -> Vec<freenet_stdlib::prelude::ContractKey> {
            self.hosting.iter().copied().collect()
        }
        fn hosting_contract_size(&self, _key: &freenet_stdlib::prelude::ContractKey) -> u64 {
            42
        }
    }

    fn contract_key_from_seed(seed: u8) -> freenet_stdlib::prelude::ContractKey {
        use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        ContractKey::from_id_and_code(ContractInstanceId::new(bytes), CodeHash::new([0u8; 32]))
    }

    /// Regression for the `QueryNodeDiagnostics` explicit-`contract_keys`
    /// presence filter (companion to the #3945 web-subresource DoS gate).
    ///
    /// When the handler is asked about specific keys, `contract_states` must
    /// contain ONLY the keys the node actually has local presence for (hosts
    /// or subscribes) and must OMIT a requested key it neither hosts nor
    /// subscribes to. This is the `if !is_hosting && !is_subscribed { continue }`
    /// guard inside [`super::collect_contract_states`], which
    /// `path_handlers::is_locally_known` depends on: without it the handler
    /// would echo EVERY requested key back as "present", so the gate would
    /// treat every queried key as locally known and the DoS protection would
    /// be defeated. Load-bearing: reverting the guard to an unconditional
    /// insert makes the `unknown` assertion below fail.
    #[test]
    fn collect_contract_states_filters_unknown_explicit_keys() {
        let hosted = contract_key_from_seed(0x01);
        let subscribed = contract_key_from_seed(0x02);
        let unknown = contract_key_from_seed(0x03);

        let oracle = FakePresence {
            hosting: std::iter::once(hosted).collect(),
            subscribed: std::iter::once(subscribed).collect(),
        };

        // Ask explicitly about all three: one hosted, one subscribed, one
        // neither. The neither-key must be filtered out of the response.
        let states = super::collect_contract_states(&oracle, &[hosted, subscribed, unknown]);

        assert!(
            states.contains_key(&hosted.to_string()),
            "a hosted key must be reported as present (positive control)"
        );
        assert!(
            states.contains_key(&subscribed.to_string()),
            "a subscribed key must be reported as present (positive control)"
        );
        assert!(
            !states.contains_key(&unknown.to_string()),
            "a key the node neither hosts nor subscribes to MUST be omitted — \
             reporting it would defeat the #3945 local-presence gate"
        );
        assert_eq!(
            states.len(),
            2,
            "only the two locally-present keys may appear in contract_states"
        );
    }

    /// The empty-`contract_keys` branch of [`super::collect_contract_states`]
    /// still enumerates ALL hosting contracts (the pre-existing "dump
    /// everything" behavior the presence filter must NOT regress). Guards
    /// against a fix that over-filters and drops the empty-query enumeration.
    #[test]
    fn collect_contract_states_empty_query_returns_all_hosting() {
        let a = contract_key_from_seed(0x11);
        let b = contract_key_from_seed(0x12);
        let oracle = FakePresence {
            hosting: [a, b].into_iter().collect(),
            subscribed: std::collections::HashSet::new(),
        };

        let states = super::collect_contract_states(&oracle, &[]);
        assert_eq!(
            states.len(),
            2,
            "empty query must dump all hosting contracts"
        );
        assert!(states.contains_key(&a.to_string()));
        assert!(states.contains_key(&b.to_string()));
    }

    mod version_gate {
        use super::super::{SUBSCRIBE_HINT_MIN_VERSION, version_supports_subscribe_hint};

        const FLOOR: (u8, u8, u16) = (0, 2, 73);

        // NOTE: there is deliberately NO test asserting the floor relative to
        // CARGO_PKG_VERSION. The floor must be set to the EXACT release that
        // first ships SubscribeHint and then FROZEN; the crate version keeps
        // climbing afterward, so any `floor >= crate_version` / `floor <=
        // crate_version` assertion would false-fail on a later release and
        // invite the wrong "fix" (bumping the floor above the ship version,
        // silently cutting off capable peers). The release coupling is enforced
        // by docs/RELEASING.md + the const's "UPDATE AT RELEASE TIME" doc, not a
        // unit test.

        #[test]
        fn unknown_remote_version_fails_closed() {
            // A peer whose negotiated version we never captured (e.g. the
            // joiner->gateway path) must NOT be sent the appended wire variant.
            assert!(!version_supports_subscribe_hint(None, FLOOR));
        }

        #[test]
        fn older_remote_version_is_rejected() {
            assert!(!version_supports_subscribe_hint(Some((0, 2, 72)), FLOOR));
            assert!(!version_supports_subscribe_hint(Some((0, 1, 99)), FLOOR));
        }

        #[test]
        fn equal_or_newer_remote_version_is_accepted() {
            // Exactly at the floor qualifies.
            assert!(version_supports_subscribe_hint(Some((0, 2, 73)), FLOOR));
            // Strictly newer (patch, minor, major) qualifies.
            assert!(version_supports_subscribe_hint(Some((0, 2, 74)), FLOOR));
            assert!(version_supports_subscribe_hint(Some((0, 3, 0)), FLOOR));
            assert!(version_supports_subscribe_hint(Some((1, 0, 0)), FLOOR));
        }

        #[test]
        fn tuple_ordering_is_major_then_minor_then_patch() {
            // (0,3,0) >= (0,2,73) even though its patch is smaller: minor wins.
            assert!(version_supports_subscribe_hint(Some((0, 3, 0)), FLOOR));
            // A zero floor accepts any known version but still fails closed on None
            // (this is the simulation/test floor behavior).
            assert!(version_supports_subscribe_hint(Some((0, 0, 0)), (0, 0, 0)));
            assert!(!version_supports_subscribe_hint(None, (0, 0, 0)));
        }

        /// The placement migration is DISABLED for every shipped 0.2.x peer: the
        /// floor is parked at `(0, 3, 0)`, so no 0.2.x version — and no unknown
        /// version — is ever sent or acts on a hint. Re-disabled because the
        /// migration is net-negative in production (#4610 / #4440 / #4534 costs
        /// with flat `hosted_key_distance` and a ~0.05% act-rate, i.e. no
        /// placement benefit). See the `SUBSCRIBE_HINT_MIN_VERSION` doc comment.
        #[test]
        fn placement_migration_deactivated_for_all_0_2_x() {
            assert!(!version_supports_subscribe_hint(
                Some((0, 2, 73)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            assert!(!version_supports_subscribe_hint(
                Some((0, 2, 255)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            assert!(!version_supports_subscribe_hint(
                None,
                SUBSCRIBE_HINT_MIN_VERSION
            ));
        }

        /// Pin the PRODUCTION constant (not the local `FLOOR`): the placement
        /// migration is DISABLED by parking the floor at `(0, 3, 0)`, above every
        /// shipped 0.2.x release, because it is net-negative in production
        /// (#4610 / #4440 / #4534 costs, flat `hosted_key_distance`, ~0.05%
        /// act-rate). The `!supported(0,2,255)` + `supported(0,3,0)` pair pins the
        /// floor to EXACTLY `(0, 3, 0)`: lowering it (a re-enable) trips these
        /// asserts — the intended tripwire, since re-enabling must be a deliberate,
        /// canary-gated change (see docs/RELEASING.md and the const doc comment).
        /// See issues #4404 / #4610 / #4440.
        #[test]
        fn placement_migration_disabled_above_all_0_2_x() {
            // Highest possible 0.2.x patch is still below the floor: disabled.
            assert!(!version_supports_subscribe_hint(
                Some((0, 2, 255)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            assert!(!version_supports_subscribe_hint(
                Some((0, 2, 80)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            // Exactly at the disabled floor `(0, 3, 0)` qualifies — this pins the
            // floor value so an accidental lowering trips the assert above.
            assert!(version_supports_subscribe_hint(
                Some((0, 3, 0)),
                SUBSCRIBE_HINT_MIN_VERSION
            ));
            // Unknown version fails closed.
            assert!(!version_supports_subscribe_hint(
                None,
                SUBSCRIBE_HINT_MIN_VERSION
            ));
        }

        /// Regression pin for the deliberate re-disable: the production floor MUST
        /// stay at the disabled value `(0, 3, 0)` (above every shipped 0.2.x
        /// release). The #4404 placement migration is net-negative in production
        /// (#4610 / #4440 / #4534 costs with no placement benefit — flat
        /// `hosted_key_distance`, ~0.05% act-rate), so it is intentionally OFF.
        /// This direct equality is the loudest tripwire against a reflex-revert to
        /// `(0, 2, 80)` (the prior re-enable value): re-enabling must be a
        /// deliberate, canary-gated change (see docs/RELEASING.md and the const
        /// doc comment), NOT an accidental edit. See #4404 / #4610 / #4440.
        #[test]
        fn subscribe_hint_floor_is_parked_at_disabled_value() {
            assert_eq!(
                SUBSCRIBE_HINT_MIN_VERSION,
                (0, 3, 0),
                "SubscribeHint placement migration must stay DISABLED — the floor \
                 must remain parked at (0, 3, 0). Lowering it re-enables a \
                 net-negative migration and must only be done via a deliberate, \
                 canary-gated change."
            );
        }
    }

    mod migration_selection {
        use super::super::pick_closest_migration_target;
        use crate::ring::{Location, PeerKeyLocation};
        use crate::transport::TransportPublicKey;
        use std::collections::HashSet;
        use std::net::SocketAddr;

        /// Build a `PeerKeyLocation` with a deterministic public key (so the
        /// tie-break is reproducible) and the given socket address (which
        /// determines its ring `location()`).
        fn pkl(pk_byte: u8, addr: &str) -> PeerKeyLocation {
            let pub_key = TransportPublicKey::from_bytes([pk_byte; 32]);
            let addr: SocketAddr = addr.parse().expect("valid socket addr");
            PeerKeyLocation::new(pub_key, addr)
        }

        /// Distance from the contract location to a candidate's ring location.
        fn dist_to(contract: Location, p: &PeerKeyLocation) -> crate::ring::Distance {
            contract.distance(p.location().expect("known location"))
        }

        #[test]
        fn picks_strictly_closest_non_hosting_candidate() {
            let contract = Location::new(0.5);
            // Three candidates at distinct ring locations.
            let a = pkl(1, "10.0.0.1:1000");
            let b = pkl(2, "10.0.0.2:1000");
            let c = pkl(3, "10.0.0.3:1000");
            let candidates = [a.clone(), b.clone(), c.clone()];

            // Identify which candidate is actually closest to `contract`.
            let closest = candidates
                .iter()
                .min_by_key(|p| dist_to(contract, p))
                .unwrap()
                .clone();

            // `my_dist` is larger than every candidate's distance, so all
            // qualify and the single closest must be chosen.
            let my_dist = candidates
                .iter()
                .map(|p| dist_to(contract, p))
                .max()
                .unwrap();
            // Make `my_dist` strictly larger than all candidates so each is
            // strictly closer than us.
            let my_dist = crate::ring::Distance::new(my_dist.as_f64() + 0.01);

            let hosting = HashSet::new();
            let chosen = pick_closest_migration_target(contract, my_dist, &hosting, &candidates)
                .expect("a qualifying candidate exists");
            assert_eq!(
                chosen.pub_key(),
                closest.pub_key(),
                "must pick the candidate closest to the contract location"
            );
        }

        #[test]
        fn excludes_candidates_not_closer_than_us() {
            let contract = Location::new(0.5);
            let a = pkl(1, "10.0.0.1:1000");
            let candidates = [a.clone()];
            // `my_dist` smaller than the candidate's distance → it does NOT
            // qualify (not strictly closer than us).
            let my_dist = crate::ring::Distance::new(dist_to(contract, &a).as_f64() - 0.0001);
            let hosting = HashSet::new();
            assert!(
                pick_closest_migration_target(contract, my_dist, &hosting, &candidates).is_none(),
                "a candidate no closer than us must be excluded"
            );
        }

        #[test]
        fn excludes_already_hosting_candidates() {
            let contract = Location::new(0.5);
            let a = pkl(1, "10.0.0.1:1000");
            let candidates = [a.clone()];
            let my_dist = crate::ring::Distance::new(dist_to(contract, &a).as_f64() + 0.01);
            let mut hosting = HashSet::new();
            hosting.insert(a.pub_key().clone());
            assert!(
                pick_closest_migration_target(contract, my_dist, &hosting, &candidates).is_none(),
                "a candidate already hosting the contract must be excluded"
            );
        }

        #[test]
        fn excludes_unknown_address_candidates() {
            let contract = Location::new(0.5);
            let unknown =
                PeerKeyLocation::with_unknown_addr(TransportPublicKey::from_bytes([7; 32]));
            let candidates = [unknown];
            let my_dist = crate::ring::Distance::new(1.0);
            let hosting = HashSet::new();
            assert!(
                pick_closest_migration_target(contract, my_dist, &hosting, &candidates).is_none(),
                "a candidate without a known socket address must be excluded"
            );
        }

        #[test]
        fn breaks_ties_deterministically_by_pubkey() {
            // Two candidates at the SAME ring location (same address) but
            // different pub keys: the smaller pub-key bytes must win,
            // regardless of input order.
            let contract = Location::new(0.5);
            let low = pkl(1, "10.0.0.9:1000");
            let high = pkl(9, "10.0.0.9:1000");
            assert_eq!(
                dist_to(contract, &low),
                dist_to(contract, &high),
                "test setup: both candidates must be equidistant"
            );
            let my_dist = crate::ring::Distance::new(dist_to(contract, &low).as_f64() + 0.01);
            let hosting = HashSet::new();

            let forward = [low.clone(), high.clone()];
            let reverse = [high.clone(), low.clone()];
            let chosen_fwd =
                pick_closest_migration_target(contract, my_dist, &hosting, &forward).unwrap();
            let chosen_rev =
                pick_closest_migration_target(contract, my_dist, &hosting, &reverse).unwrap();
            assert_eq!(chosen_fwd.pub_key(), low.pub_key());
            assert_eq!(chosen_rev.pub_key(), low.pub_key());
        }

        #[test]
        fn returns_none_when_no_candidates() {
            let contract = Location::new(0.5);
            let candidates: [PeerKeyLocation; 0] = [];
            let hosting = HashSet::new();
            assert!(
                pick_closest_migration_target(
                    contract,
                    crate::ring::Distance::new(1.0),
                    &hosting,
                    &candidates
                )
                .is_none()
            );
        }
    }

    /// Regression test for message loss during shutdown.
    ///
    /// This test validates the drain-before-shutdown pattern that prevents message loss
    /// when an error occurs during select! wait. The bug scenario:
    ///
    /// 1. Listener enters select! waiting for inbound/outbound
    /// 2. Message is queued to the channel while select! is waiting
    /// 3. Error occurs (e.g., connection timeout, deserialize failure)
    /// 4. select! returns with the error
    /// 5. BUG: Without drain, we return immediately and the queued message is lost
    /// 6. FIX: Drain pending messages before returning
    ///
    /// This test simulates the pattern by:
    /// - Having a "listener" that waits on select! then encounters an error
    /// - Sending messages during the wait period
    /// - Verifying all messages are processed (drained) before exit
    #[tokio::test]
    async fn test_drain_before_shutdown_prevents_message_loss() {
        let (tx, mut rx) = mpsc::channel::<String>(10);
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_clone = processed.clone();

        // Simulate peer_connection_listener pattern
        let listener = GlobalExecutor::spawn(async move {
            // Drain-then-select loop (simplified)
            loop {
                // Phase 1: Drain pending messages (like the loop at start of peer_connection_listener)
                loop {
                    match rx.try_recv() {
                        Ok(msg) => {
                            tracing::debug!("Drained message: {}", msg);
                            processed_clone.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => return,
                    }
                }

                // Phase 2: Wait for new messages or "error" (simulated by timeout)
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Some(m) => {
                                tracing::debug!("Received via select: {}", m);
                                processed_clone.fetch_add(1, Ordering::SeqCst);
                            }
                            None => return, // Channel closed
                        }
                    }
                    // Simulate error condition (e.g., connection timeout)
                    _ = sleep(Duration::from_millis(50)) => {
                        tracing::debug!("Simulated error occurred");

                        // CRITICAL: This is the fix - drain before shutdown
                        // Without this, messages queued during the 50ms wait would be lost
                        let mut drained = 0;
                        while let Ok(msg) = rx.try_recv() {
                            tracing::debug!("Drain before shutdown: {}", msg);
                            processed_clone.fetch_add(1, Ordering::SeqCst);
                            drained += 1;
                        }
                        tracing::debug!("Drained {} messages before shutdown", drained);
                        return;
                    }
                }
            }
        });

        // Send messages with timing that causes them to arrive DURING the select! wait
        // This is the race condition that causes message loss without the drain
        GlobalExecutor::spawn(async move {
            // Wait for listener to enter select!
            sleep(Duration::from_millis(10)).await;

            // Send messages while listener is in select! waiting
            for i in 0..5 {
                tx.send(format!("msg{}", i)).await.unwrap();
                sleep(Duration::from_millis(5)).await;
            }
            // Don't close the channel - let the timeout trigger the "error"
        });

        // Wait for listener to complete
        let result = timeout(Duration::from_millis(200), listener).await;
        assert!(result.is_ok(), "Listener should complete");

        // All 5 messages should have been processed
        let count = processed.load(Ordering::SeqCst);
        assert_eq!(
            count, 5,
            "All messages should be processed (drained before shutdown). Got {} instead of 5. \
             If this fails, messages are being lost during the error-exit drain.",
            count
        );
    }

    /// Test that demonstrates what happens WITHOUT the drain (the bug we're preventing).
    /// This test intentionally shows the failure mode when drain is missing.
    ///
    /// The key is to ensure messages are queued AFTER select! starts but the error
    /// fires immediately, so select! returns with error before processing the messages.
    #[tokio::test]
    async fn test_message_loss_without_drain() {
        let (tx, mut rx) = mpsc::channel::<String>(10);
        let processed = Arc::new(AtomicUsize::new(0));
        let processed_clone = processed.clone();

        // Signal when listener is ready for messages
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();

        // Buggy listener - NO drain before shutdown (demonstrates the bug)
        let listener = GlobalExecutor::spawn(async move {
            // Drain at loop start (the original PR #2255 fix)
            loop {
                match rx.try_recv() {
                    Ok(msg) => {
                        tracing::debug!("Drained message: {}", msg);
                        processed_clone.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
            }

            // Signal that we're about to enter select! and then immediately timeout
            assert!(ready_tx.send(()).is_ok(), "ready_tx receiver dropped");

            // Immediate timeout to simulate error - messages sent after this starts are lost
            tokio::select! {
                biased; // Ensure timeout wins if both are ready
                _ = sleep(Duration::from_millis(1)) => {
                    tracing::debug!("Error occurred - BUG: returning without drain!");
                    // BUG: No drain here! Messages queued during this 1ms window are lost.
                }
                msg = rx.recv() => {
                    if let Some(m) = msg {
                        tracing::debug!("Received via select: {}", m);
                        processed_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        // Wait for listener to be ready, then immediately send messages
        let _ready = ready_rx.await;

        // Send messages - these arrive while select! is running with a very short timeout
        // Some or all will be lost because we return without draining
        for i in 0..5 {
            let _sent = tx.send(format!("msg{}", i)).await;
        }

        let _listener_result = timeout(Duration::from_millis(100), listener).await;

        // Without the drain fix, messages are lost
        let count = processed.load(Ordering::SeqCst);
        // The listener exits immediately due to timeout, so no messages should be processed
        assert!(
            count < 5,
            "Without drain, messages should be lost. Got {} (expected < 5). \
             This test validates the bug exists when drain is missing.",
            count
        );
    }

    /// Regression guard for #4057: `EventListenerState::Drop` must record one
    /// `pending_op_remove` per resident entry, otherwise non-Shutdown exit paths
    /// (simulation `handle.abort()`, etc.) leave `inserts - removes` inflated
    /// and trip `test_pending_op_results_bounded` (#3100).
    #[test]
    fn event_listener_state_drop_records_remaining_pending_op_removes() {
        use crate::config::GlobalTestMetrics;
        use crate::message::Transaction;
        use crate::transport::ExpectedInboundTracker;

        let removes_before = GlobalTestMetrics::pending_op_removes();

        let mut state = super::EventListenerState::new(ExpectedInboundTracker::empty_for_test());
        for _ in 0..3 {
            let (callback, _rx) = mpsc::channel(1);
            state
                .pending_op_results
                .insert(Transaction::ttl_transaction(), callback);
        }
        drop(state);

        assert_eq!(
            GlobalTestMetrics::pending_op_removes() - removes_before,
            3,
            "Drop must record one pending_op_remove per resident entry"
        );
    }

    /// Regression guard for #4154: `handle_orphaned_transactions` must not
    /// revert to a logging-only stub. If a future refactor renames the
    /// helper, update both the call site and this guard together.
    #[test]
    fn handle_orphaned_transactions_wakes_parked_drivers() {
        let src = include_str!("p2p_protoc.rs");
        let handler_start = src
            .find("pub(crate) async fn handle_orphaned_transactions(")
            .expect("handle_orphaned_transactions must exist");
        // Bound the body at the next `\n    }\n` — `async fn` impl methods
        // close at indent level 4.
        let body_after = &src[handler_start..];
        let end = body_after
            .find("\n    }\n")
            .expect("handle_orphaned_transactions must close cleanly");
        let body = &body_after[..end];
        assert!(
            body.contains("notify_orphaned_transaction"),
            "handle_orphaned_transactions must call \
             notify_orphaned_transaction to wake parked drivers (#4154/#4313). \
             Found body:\n{body}"
        );
    }

    /// Boundary case: dropping an empty state must not record spurious removes.
    #[test]
    fn event_listener_state_drop_empty_records_nothing() {
        use crate::config::GlobalTestMetrics;
        use crate::transport::ExpectedInboundTracker;

        let removes_before = GlobalTestMetrics::pending_op_removes();
        drop(super::EventListenerState::new(
            ExpectedInboundTracker::empty_for_test(),
        ));
        assert_eq!(GlobalTestMetrics::pending_op_removes(), removes_before);
    }

    /// Test that connection_id generation produces unique, monotonically increasing IDs.
    #[test]
    fn test_connection_id_uniqueness() {
        let id1 = super::next_connection_id();
        let id2 = super::next_connection_id();
        let id3 = super::next_connection_id();
        assert!(id2 > id1, "IDs must be monotonically increasing");
        assert!(id3 > id2, "IDs must be monotonically increasing");
    }

    /// Validates the stale TransportClosed detection pattern: when a connection is
    /// replaced (peer reconnected with new identity), a TransportClosed from the old
    /// listener (mismatched connection_id) must not remove the replacement entry.
    #[test]
    fn test_stale_transport_closed_detection() {
        use std::collections::BTreeMap;
        use std::net::SocketAddr;

        let addr: SocketAddr = "1.2.3.4:5678".parse().unwrap();
        let mut connections: BTreeMap<SocketAddr, super::ConnectionEntry> = BTreeMap::new();

        // Insert initial connection (id=10), then replace with new one (id=20)
        let (tx1, _rx1) = mpsc::channel(1);
        connections.insert(
            addr,
            super::ConnectionEntry {
                sender: tx1,
                pub_key: None,
                connection_id: 10,
                created_at: Instant::now(),
                remote_version: None,
            },
        );
        let (tx2, _rx2) = mpsc::channel(1);
        connections.insert(
            addr,
            super::ConnectionEntry {
                sender: tx2,
                pub_key: None,
                connection_id: 20,
                created_at: Instant::now(),
                remote_version: None,
            },
        );

        // Stale TransportClosed (id=10) should not match the current entry (id=20)
        let current = connections.get(&addr).unwrap();
        assert_ne!(current.connection_id, 10, "stale event must not match");
        assert_eq!(current.connection_id, 20);

        // Current TransportClosed (id=20) should match and allow removal
        assert_eq!(current.connection_id, 20, "current event must match");
        connections.remove(&addr);
        assert!(!connections.contains_key(&addr));
    }

    // ============ Zombie detection tests ============

    const TEST_TRANSIENT_TTL: Duration = Duration::from_secs(30);
    // With TTL=30s: zombie_threshold=90s, absolute_zombie_threshold=180s

    #[test]
    fn test_zombie_detection_ignores_young_connections() {
        // Connection younger than zombie threshold (90s) should never be a zombie
        let elapsed = Duration::from_secs(60);
        assert!(
            !super::is_zombie(elapsed, false, false, false, TEST_TRANSIENT_TTL),
            "Young connection should not be zombie"
        );
    }

    #[test]
    fn test_zombie_detection_ignores_ring_connections() {
        // In-ring connection should never be a zombie regardless of age
        let elapsed = Duration::from_secs(400);
        assert!(
            !super::is_zombie(elapsed, true, false, false, TEST_TRANSIENT_TTL),
            "Ring connection should not be zombie"
        );
    }

    #[test]
    fn test_zombie_detection_catches_old_unpromoted() {
        // Old connection that isn't in ring, no pending → zombie
        let elapsed = Duration::from_secs(91); // > 90s
        assert!(
            super::is_zombie(elapsed, false, false, false, TEST_TRANSIENT_TTL),
            "Old unpromoted connection should be zombie"
        );
    }

    #[test]
    fn test_zombie_detection_ignores_pending_reservation() {
        // Old connection not in ring, but has pending reservation → not zombie (under absolute)
        let elapsed = Duration::from_secs(120); // > 90s but < 180s
        assert!(
            !super::is_zombie(elapsed, false, true, false, TEST_TRANSIENT_TTL),
            "Connection with pending reservation below absolute threshold should not be zombie"
        );
    }

    #[test]
    fn test_zombie_absolute_threshold_overrides_pending() {
        // Connection 181s old, has_pending=true, not in ring → IS zombie
        // The absolute threshold (180s) overrides has_pending
        let elapsed = Duration::from_secs(181);
        assert!(
            super::is_zombie(elapsed, false, true, false, TEST_TRANSIENT_TTL),
            "Absolute threshold should override has_pending"
        );
    }

    #[test]
    fn test_zombie_absolute_threshold_spares_ring() {
        // Connection 181s old, in_ring=true → NOT zombie
        let elapsed = Duration::from_secs(181);
        assert!(
            !super::is_zombie(elapsed, true, true, false, TEST_TRANSIENT_TTL),
            "Ring connection should never be zombie even past absolute threshold"
        );
    }

    #[test]
    fn test_zombie_boundary_exactly_at_threshold() {
        // Exactly 90s, no pending, not in ring → NOT zombie (uses > not >=)
        assert!(!super::is_zombie(
            Duration::from_secs(90),
            false,
            false,
            false,
            TEST_TRANSIENT_TTL
        ));
    }

    #[test]
    fn test_zombie_boundary_exactly_at_absolute() {
        // Exactly 180s, has_pending, not in ring → NOT zombie (uses > not >=)
        assert!(!super::is_zombie(
            Duration::from_secs(180),
            false,
            true,
            false,
            TEST_TRANSIENT_TTL
        ));
    }

    #[test]
    fn test_zombie_detection_ignores_gateway_connections() {
        // Gateway connections are intentionally transient and should not be
        // classified as zombies within the 1-hour gateway exemption (#3595).
        let elapsed = Duration::from_secs(400); // Well past normal absolute threshold (180s)
        assert!(
            !super::is_zombie(elapsed, false, false, true, TEST_TRANSIENT_TTL),
            "Gateway connection should not be zombie within exemption window"
        );

        // But truly stale gateway connections (>1 hour) ARE cleaned up.
        let stale = Duration::from_secs(3601);
        assert!(
            super::is_zombie(stale, false, false, true, TEST_TRANSIENT_TTL),
            "Gateway connection past 1-hour cap should be zombie"
        );
    }

    #[test]
    fn test_zombie_thresholds_scale_with_ttl() {
        // With larger TTL (120s, as used in tests), thresholds scale:
        // zombie_threshold=360s, absolute=720s
        let large_ttl = Duration::from_secs(120);
        // 200s: not zombie even without pending (< 360s)
        assert!(!super::is_zombie(
            Duration::from_secs(200),
            false,
            false,
            false,
            large_ttl
        ));
        // 400s: zombie without pending (> 360s)
        assert!(super::is_zombie(
            Duration::from_secs(400),
            false,
            false,
            false,
            large_ttl
        ));
        // 400s with pending: not zombie (< 720s)
        assert!(!super::is_zombie(
            Duration::from_secs(400),
            false,
            true,
            false,
            large_ttl
        ));
        // 721s: zombie even with pending (> 720s)
        assert!(super::is_zombie(
            Duration::from_secs(721),
            false,
            true,
            false,
            large_ttl
        ));
    }

    // ──────────────────────────────────────────────────────────
    // Regression tests for #4145: the event loop must not stall
    // indefinitely on a single congested peer when dispatching
    // StreamSend / PipeStream. Pre-fix these branches called
    // `peer_connection.sender.send(...).await` with no timeout, so
    // a wedged peer's full per-connection mpsc could freeze the
    // whole p2p_protoc event loop — exactly the symptom seen on
    // the nova / vega gateways on 2026-05-24.
    // ──────────────────────────────────────────────────────────

    /// Behavioral pin: the `reserve()` + `timeout(500 ms)` pattern
    /// used in the StreamSend / PipeStream dispatch branches must
    /// return `Err(_timeout)` when the per-peer channel is saturated,
    /// and must do so within a bounded window (timeout + slack).
    ///
    /// If this fails, either:
    ///   - someone reverted the timeout in p2p_protoc.rs::StreamSend / PipeStream
    ///   - or tokio's mpsc reserve semantics changed
    /// Either way, the event loop is once again exposed to the wedge.
    #[tokio::test]
    async fn peer_send_timeout_drops_when_channel_saturated() {
        // Use the same 500 ms ceiling encoded in the SEND_TIMEOUT
        // constant of StreamSend / PipeStream (kept in sync by
        // `stream_send_branch_uses_timed_reserve_pattern` below).
        const SEND_TIMEOUT: Duration = Duration::from_millis(500);
        // 1-deep channel so we can saturate it with a single message.
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.try_send(0).expect("first send must succeed");

        let start = Instant::now();
        let result = timeout(SEND_TIMEOUT, tx.reserve()).await;
        let elapsed = start.elapsed();

        assert!(
            result.is_err(),
            "reserve must time out when channel is saturated"
        );
        assert!(
            elapsed >= SEND_TIMEOUT,
            "must wait the full {SEND_TIMEOUT:?} before giving up, waited {elapsed:?}"
        );
        // Generous slack for slow CI runners (release_pending_op_slot test
        // allows a similar margin). The key invariant is "bounded", not
        // "exactly N ms".
        assert!(
            elapsed < SEND_TIMEOUT + Duration::from_millis(500),
            "must not block significantly past {SEND_TIMEOUT:?}, took {elapsed:?}"
        );
    }

    /// Behavioral pin: when `reserve()` succeeds before the timeout
    /// fires (transient congestion that clears in <500 ms), the
    /// dispatch proceeds normally — we don't accidentally drop the
    /// stream fragment in the fast path.
    #[tokio::test]
    async fn peer_send_succeeds_when_consumer_drains_in_time() {
        const SEND_TIMEOUT: Duration = Duration::from_millis(500);
        let (tx, mut rx) = mpsc::channel::<u32>(1);
        tx.try_send(0).expect("first send must succeed");

        // Drain the slot after a short delay so reserve completes
        // before the timeout fires.
        let consumer = GlobalExecutor::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            rx.recv().await.expect("must drain pre-filled entry");
            rx.recv().await.expect("must receive reserved value")
        });

        let result = timeout(SEND_TIMEOUT, tx.reserve()).await;
        let permit = result
            .expect("reserve must complete within timeout")
            .expect("channel must be open");
        permit.send(42u32);

        let received = consumer.await.expect("consumer task must succeed");
        assert_eq!(received, 42, "reserved permit must deliver our value");
    }

    /// Load-bearing invariant for #4145: when the per-peer channel is
    /// saturated and the dispatch times out, the StreamSend branch MUST
    /// signal `completion_tx` so the broadcast queue's semaphore permit
    /// releases immediately instead of waiting `STREAM_COMPLETION_TIMEOUT`
    /// (120 s). This is the entire reason the dispatch uses `reserve()`
    /// (so we retain ownership of `completion_tx`) instead of
    /// `timeout(send())` (which moves it into the dropped future).
    ///
    /// This test pins the *pattern* — production code is exercised by
    /// the source-scrape pin `stream_send_branch_uses_timed_reserve_pattern`.
    /// A behavioural mismatch between the two is a CI failure here, not
    /// a 120 s production stall.
    #[tokio::test]
    async fn stream_send_pattern_fires_completion_tx_on_timeout() {
        use crate::transport::BroadcastDeliveryOutcome;
        const SEND_TIMEOUT: Duration = Duration::from_millis(100);
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.try_send(0).expect("first send must succeed");

        let (completion_tx, completion_rx) =
            tokio::sync::oneshot::channel::<BroadcastDeliveryOutcome>();

        // Mirror the StreamSend dispatch pattern:
        //   reserve() → permit OR closed OR timeout
        // The timeout branch MUST signal completion_tx (we retain
        // ownership because reserve() never consumed it). Post-#4235 the
        // signal carries `Dropped` because the fragment was NOT delivered.
        match tokio::time::timeout(SEND_TIMEOUT, tx.reserve()).await {
            Ok(Ok(_permit)) => {
                // Wouldn't happen in this test (channel saturated), but
                // the production happy path passes completion_tx into
                // the message and the receiver task fires it.
                panic!("reserve unexpectedly succeeded on saturated channel");
            }
            Ok(Err(_closed)) => {
                let _ignored = completion_tx.send(BroadcastDeliveryOutcome::Dropped);
            }
            Err(_timeout) => {
                let _ignored = completion_tx.send(BroadcastDeliveryOutcome::Dropped);
            }
        }

        let signal = timeout(Duration::from_millis(50), completion_rx).await;
        assert!(
            matches!(signal, Ok(Ok(BroadcastDeliveryOutcome::Dropped))),
            "completion_tx must be fired on the dispatch timeout path so the \
             broadcast queue releases its permit immediately (not after \
             120s STREAM_COMPLETION_TIMEOUT). Got {signal:?}"
        );
    }

    /// Source-scrape regression guard for #4145. If a future refactor
    /// removes the `tokio::time::timeout(SEND_TIMEOUT, … .reserve())`
    /// pattern from the StreamSend dispatch branch, fail loudly.
    /// Pin the *dispatch pattern*, not the surrounding logic, so that
    /// reasonable refactors (renaming locals, moving the match arm)
    /// still pass.
    ///
    /// Also pins that the branch contains a `completion_tx` fire path
    /// outside the success arm — the load-bearing invariant tested
    /// behaviourally by `stream_send_pattern_fires_completion_tx_on_timeout`.
    #[test]
    fn stream_send_branch_uses_timed_reserve_pattern() {
        let src = include_str!("p2p_protoc.rs");
        // Locate the StreamSend dispatch branch in the event loop.
        let needle = "ConnEvent::StreamSend {";
        let mut occurrences = src.match_indices(needle);
        // The first occurrence is the event-loop dispatch branch we
        // care about (the second is inside the re-wrap when we
        // forward the message to the peer connection). Take the first.
        let (idx, _) = occurrences
            .next()
            .expect("StreamSend branch must exist in p2p_protoc.rs");
        // Tight window: bound at the next `ConnEvent::PipeStream {`
        // declaration (the immediately-following match arm). Falling
        // back to a generous fixed window prevents the test from
        // breaking on unrelated refactors that move PipeStream around,
        // but the tight bound is the primary scoping mechanism.
        let pipe_stream_needle = "ConnEvent::PipeStream {";
        let pipe_stream_rel = src[idx..]
            .find(pipe_stream_needle)
            .expect("PipeStream branch must follow StreamSend in p2p_protoc.rs");
        let window_end = idx + pipe_stream_rel;
        let body = &src[idx..window_end];
        assert!(
            body.contains("tokio::time::timeout("),
            "StreamSend dispatch must wrap the peer send in a timeout — \
             see #4145 (gateway wedge). Body:\n{body}"
        );
        assert!(
            body.contains(".reserve()"),
            "StreamSend dispatch must use `reserve()` (not `send().await`) \
             so we keep ownership of completion_tx on the timeout path. \
             See #4145. Body:\n{body}"
        );
        assert!(
            body.contains("SEND_TIMEOUT"),
            "StreamSend dispatch must define a named SEND_TIMEOUT constant \
             so the bound is greppable. See #4145. Body:\n{body}"
        );
        // Load-bearing invariant: ALL THREE non-success arms (closed /
        // timeout / no-connection) MUST signal `completion_tx` so the
        // broadcast queue's semaphore permit releases immediately.
        // Per-arm count instead of a single match: a refactor that
        // drops just the timeout arm's fire (the one #4145 added)
        // would otherwise silently reintroduce the 120 s
        // STREAM_COMPLETION_TIMEOUT stall.
        //
        // Post-#4235 the signal carries a `BroadcastDeliveryOutcome` instead
        // of `()`, and every drop arm reports `Dropped` so the broadcast queue
        // releases the permit WITHOUT treating the drop as a delivery. Counting
        // `Dropped` fires here pins both invariants at once: a regression that
        // dropped an arm OR that mislabeled a drop arm as `Delivered` (the
        // exact conflation #4235 fixed) trips this assertion. Tested
        // behaviourally by `stream_send_pattern_fires_completion_tx_on_timeout`.
        let fire_count = body
            .matches("tx.send(BroadcastDeliveryOutcome::Dropped)")
            .count();
        assert_eq!(
            fire_count, 3,
            "StreamSend dispatch must signal completion_tx with \
             BroadcastDeliveryOutcome::Dropped in all three non-success arms \
             (closed / timeout / no-connection) — found {fire_count} such \
             occurrences, expected exactly 3. If you REMOVED one, a refactor \
             reintroduced the 120 s STREAM_COMPLETION_TIMEOUT stall (#4145). \
             If you relabeled a drop arm as Delivered, you reintroduced the \
             delivery/permit conflation (#4235). If you ADDED a new arm with \
             its own fire, bump this count to match. Body:\n{body}"
        );
    }

    /// Source-scrape regression guard for #4145 — PipeStream variant.
    /// Same rationale as `stream_send_branch_uses_timed_reserve_pattern`.
    /// Tight scoping bounded at the next match arm (`ConnEvent::ClosedChannel`)
    /// so the assertion can't bleed into unrelated code if PipeStream is
    /// later reordered or expanded.
    #[test]
    fn pipe_stream_branch_uses_timed_reserve_pattern() {
        let src = include_str!("p2p_protoc.rs");
        let needle = "ConnEvent::PipeStream {";
        let mut occurrences = src.match_indices(needle);
        let (idx, _) = occurrences
            .next()
            .expect("PipeStream branch must exist in p2p_protoc.rs");
        // Bound at the next match arm — `ConnEvent::ClosedChannel(...)`
        // immediately follows PipeStream in the event-loop dispatch.
        let closed_channel_rel = src[idx..]
            .find("ConnEvent::ClosedChannel(")
            .expect("ClosedChannel branch must follow PipeStream in p2p_protoc.rs");
        let window_end = idx + closed_channel_rel;
        let body = &src[idx..window_end];
        assert!(
            body.contains("tokio::time::timeout("),
            "PipeStream dispatch must wrap the peer send in a timeout — see #4145. Body:\n{body}"
        );
        assert!(
            body.contains(".reserve()"),
            "PipeStream dispatch must use `reserve()` instead of `send().await` — \
             see #4145. Body:\n{body}"
        );
    }

    /// Source-scrape regression guard for #4549. The inline diagnostics
    /// `QuerySubscriptions` queries on the network event loop must be BOUNDED by
    /// `QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT` and must NOT propagate the contract
    /// handler's result with `?`. The original bug was a bare
    /// `notify_contract_handler(QuerySubscriptions ...).await?`: under
    /// contract-executor saturation it returned `NoEvHandlerResponse` after the
    /// 300s handler timeout, which killed the entire network event listener
    /// ("Network event listener exited: no response received from handler") and
    /// took the gateway network-dead with no self-recovery.
    #[test]
    fn query_subscriptions_diagnostics_bounded_and_non_fatal() {
        let src = include_str!("p2p_protoc.rs");
        // Built at runtime so this test's own source can't self-match the needle.
        let needle = concat!("ContractHandlerEvent::", "QuerySubscriptions {");
        let sites: Vec<usize> = src.match_indices(needle).map(|(i, _)| i).collect();
        // The handler query now lives in exactly ONE place — the bounded helper
        // `query_app_subscriptions_bounded`. Re-inlining it into an event-loop arm
        // (the #4549 regression shape) would add a site and fail this assert.
        assert_eq!(
            sites.len(),
            1,
            "the QuerySubscriptions handler query must live only in \
             query_app_subscriptions_bounded, found {} sites",
            sites.len()
        );
        let idx = sites[0];
        // Non-fatal: no `?`-propagation on the handler result (char-based window so
        // a multi-byte char in nearby comments can't panic the slice).
        let forward: String = src[idx..].chars().take(220).collect();
        assert!(
            !forward.contains(".await?"),
            "the QuerySubscriptions handler query must not use the fatal `.await?` — a per-op \
             handler timeout must never kill the network event listener (#4549). Near:\n{forward}"
        );
        // Bounded: wrapped in `timeout(QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT, ..)`.
        let before = &src[..idx];
        let bounded = before
            .rfind("timeout(")
            .is_some_and(|tp| src[tp..idx].contains("QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT"));
        assert!(
            bounded,
            "the QuerySubscriptions handler query must be wrapped in \
             timeout(QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT, ..) so a saturated contract handler \
             can neither stall nor kill the network event loop (#4549)."
        );
        // Both diagnostics arms (and the helper definition) must reference the helper.
        assert!(
            src.matches("query_app_subscriptions_bounded(").count() >= 3,
            "both diagnostics arms must delegate to query_app_subscriptions_bounded (#4549)"
        );
        // Keep the bound small so a future bump can't reopen the event-loop stall window.
        assert!(
            super::QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT <= Duration::from_secs(30),
            "QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT must stay small to bound the loop stall (#4549)"
        );
    }

    /// Behavioral pin for #4549: a stalled contract handler must make the bounded
    /// diagnostics query return an EMPTY list within the timeout — never error (the
    /// old fatal `.await?`) and never stall for the handler's 300s response timeout.
    /// This exercises the production `query_app_subscriptions_bounded` directly, so
    /// it fails if the stall-survival behavior regresses by ANY route, not just the
    /// one syntactic revert the source-scrape pin catches.
    #[tokio::test(start_paused = true)]
    async fn query_app_subscriptions_bounded_yields_empty_when_handler_stalls() {
        // Keep the handler (receiver) half alive but NEVER respond — the saturation
        // condition that killed the listener pre-#4549. The unbounded send succeeds;
        // the response oneshot never resolves, so the outer bound must fire.
        let (sender, _handler_half, _waiting) = crate::contract::contract_handler_channel();
        let bound = super::QUERY_SUBSCRIPTIONS_HANDLER_TIMEOUT;
        let start = Instant::now();
        let subs = super::query_app_subscriptions_bounded(&sender).await;
        let elapsed = start.elapsed();
        assert!(
            subs.is_empty(),
            "a stalled handler must yield empty app subscriptions, got {} entries",
            subs.len()
        );
        // start_paused advances the clock deterministically: we wait the bound but
        // NOT the handler's full 300s CH_EV_RESPONSE_TIME_OUT.
        assert!(
            elapsed >= bound,
            "must wait the bounded timeout ({elapsed:?} < {bound:?})"
        );
        assert!(
            elapsed < Duration::from_secs(60),
            "must not stall for the 300s handler timeout ({elapsed:?})"
        );
    }

    /// Pin: the `TransactionOrphaned` handler must deliver the cause
    /// THROUGH the waiter channel (`try_send(WaiterReply::PeerDisconnected ...)`)
    /// before dropping the sender via `pending_op_results.remove`. A future
    /// refactor that drops the sender without sending the signal re-opens the
    /// #4313 race (driver wakes on close with no cause → FORBIDDEN_MARKER).
    #[test]
    fn transaction_orphaned_handler_sends_cause_before_dropping_sender() {
        const SOURCE: &str = include_str!("p2p_protoc.rs");

        let arm_anchor = "NodeEvent::TransactionOrphaned { tx, peer } => {";
        let arm_start = SOURCE
            .find(arm_anchor)
            .expect("TransactionOrphaned handler renamed or removed");
        let arm_end = SOURCE[arm_start..]
            .find("\n                            }\n")
            .map(|p| arm_start + p)
            .expect("TransactionOrphaned arm closer not found");
        let body = &SOURCE[arm_start..arm_end];

        let send_pos = body
            .find("WaiterReply::PeerDisconnected")
            .expect("missing PeerDisconnected delivery — #4313 cause is dropped");
        let remove_pos = body
            .find("pending_op_results.remove(&tx)")
            .expect("missing pending_op_results.remove — sender never dropped");
        assert!(
            send_pos > remove_pos,
            "the PeerDisconnected send must happen on the sender taken out by \
             pending_op_results.remove (send-before-drop). Arm body:\n{body}"
        );
    }

    /// #4473 path-B summarize-storm pin (sim-inline counterpart of
    /// `broadcast_queue::broadcast_single_peer_gates_summarize_on_hosted_or_in_use_pin`).
    ///
    /// The simulation-only `broadcast_state_to_peers` has its own inline
    /// `get_contract_summary` (computed once for all targets). It must skip the
    /// whole fan-out via `should_broadcast_contract` BEFORE that call, so the
    /// sim path mirrors the production `broadcast_to_single_peer` gate and the
    /// behavioral broadcast simulation tests can't silently regress the storm.
    #[test]
    fn broadcast_state_to_peers_gates_summarize_on_hosted_or_in_use() {
        // Lives in the `broadcast` submodule after the p2p_protoc.rs split.
        const SOURCE: &str = include_str!("p2p_protoc/broadcast.rs");

        let fn_anchor = "async fn broadcast_state_to_peers(";
        let fn_start = SOURCE
            .find(fn_anchor)
            .expect("broadcast_state_to_peers renamed or removed");
        // Bound the slice at the next method (tolerating a `pub(super)` prefix)
        // so the assertion can't match a later function's body.
        let after_header = &SOURCE[fn_start + fn_anchor.len()..];
        let body_end = [
            after_header.find("\n    async fn "),
            after_header.find("\n    pub(super) async fn "),
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|p| fn_start + fn_anchor.len() + p)
        .unwrap_or(SOURCE.len());
        let body = &SOURCE[fn_start..body_end];

        let gate_pos = body.find("should_broadcast_contract(op_manager").expect(
            "broadcast_state_to_peers must gate on should_broadcast_contract — a \
             bare get_contract_summary here reintroduces the #4473 summarize storm \
             on the sim-inline broadcast path",
        );
        let summarize_pos = body
            .find("get_contract_summary(")
            .expect("broadcast_state_to_peers get_contract_summary call not found");
        assert!(
            gate_pos < summarize_pos,
            "broadcast_state_to_peers must call should_broadcast_contract (offset \
             {gate_pos}) BEFORE get_contract_summary (offset {summarize_pos}) so the \
             fan-out is skipped for a contract we hold no state for. Body:\n{body}"
        );
    }

    /// Phase 7 egress self-block pin (#4300). `handle_broadcast_state_change`
    /// MUST skip the fan-out for a banned contract — the egress
    /// `contract_ban_list.is_banned(key.id())` check must appear BEFORE
    /// the target resolution (`get_broadcast_targets_update`) so a
    /// delegate-driven UPDATE for a banned contract is not fanned out to
    /// subscribers who don't know about our ban, and so the no-target
    /// retry re-emission is skipped too. Mirrors the receive-side
    /// `update_dispatch_gates_banned_contracts` and the other egress
    /// pins in the `start_client_*` entry points.
    #[test]
    fn handle_broadcast_state_change_gates_banned_egress() {
        // Lives in the `broadcast` submodule after the p2p_protoc.rs split.
        const SOURCE: &str = include_str!("p2p_protoc/broadcast.rs");

        let fn_anchor = "async fn handle_broadcast_state_change(";
        let fn_start = SOURCE
            .find(fn_anchor)
            .expect("handle_broadcast_state_change renamed or removed");
        // Bound the slice at the next method (tolerating the `pub(super)` prefix
        // the sibling methods carry in the submodule) so the assertion can't
        // accidentally match a later function's body.
        let after_header = &SOURCE[fn_start + fn_anchor.len()..];
        let body_end = [
            after_header.find("\n    async fn "),
            after_header.find("\n    pub(super) async fn "),
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|p| fn_start + fn_anchor.len() + p)
        .unwrap_or(SOURCE.len());
        let body = &SOURCE[fn_start..body_end];

        let gate_pos = body.find("contract_ban_list.is_banned").expect(
            "handle_broadcast_state_change is missing the ban-list egress gate — \
             banned contracts would continue to be fanned out to subscribers \
             (defeats the Phase 7 egress self-block, #4300)",
        );
        let target_pos = body
            .find("get_broadcast_targets_update")
            .expect("handle_broadcast_state_change is missing get_broadcast_targets_update");
        assert!(
            gate_pos < target_pos,
            "BroadcastStateChange ban-list gate (offset {gate_pos}) must run \
             BEFORE target resolution (offset {target_pos}) so a banned \
             contract's broadcast is skipped — including the no-target retry \
             re-emission. Body:\n{body}"
        );

        // Regression guard for the `fix(governance): clear broadcast retry
        // state on banned-egress skip` commit: the banned-skip branch must
        // also clear the per-contract retry/streak bookkeeping, otherwise a
        // contract banned mid-retry-cycle leaves a stale entry that nothing
        // clears until it is unbanned and broadcast again. We scope the
        // assertion to the banned-skip branch (between the gate and its
        // early `return;`) so deleting either `remove()` call fails this
        // test — the gate-ordering assertion above passes even without
        // them.
        let banned_branch = &body[gate_pos..];
        let return_pos = banned_branch
            .find("return;")
            .expect("banned-egress branch must early-return");
        let banned_branch = &banned_branch[..return_pos];
        assert!(
            banned_branch.contains("self.broadcast_retries.remove(&key)"),
            "banned-egress skip must clear broadcast_retries for the contract \
             so a ban mid-retry-cycle doesn't leave a stale entry. Branch:\n{banned_branch}"
        );
        assert!(
            banned_branch.contains("self.broadcast_no_target_streak.remove(&key)"),
            "banned-egress skip must clear broadcast_no_target_streak for the \
             contract so a ban mid-retry-cycle doesn't leave a stale entry. \
             Branch:\n{banned_branch}"
        );
    }

    /// #4359 wiring pin (MUST-FIX 2). The only behavioral unit tests for the
    /// deferred-broadcast feature exercise the emit-core in isolation; they do
    /// NOT drive `handle_broadcast_state_change`, so deleting the production
    /// stash/take calls would re-open the bug with zero failing behavioral
    /// tests. This source-grep pin (mirroring
    /// `handle_broadcast_state_change_gates_banned_egress`) locks the wiring:
    /// the give-up (retries-exhausted) branch MUST stash, and the targets-found
    /// branch MUST take. Driving the full async handler needs a complete
    /// OpManager + contract handler, hence the grep floor.
    #[test]
    fn handle_broadcast_state_change_stashes_on_giveup_and_takes_on_targets() {
        // Lives in the `broadcast` submodule after the p2p_protoc.rs split.
        const SOURCE: &str = include_str!("p2p_protoc/broadcast.rs");

        let fn_anchor = "async fn handle_broadcast_state_change(";
        let fn_start = SOURCE
            .find(fn_anchor)
            .expect("handle_broadcast_state_change renamed or removed (#4359 wiring pin)");
        // Bound at the next method, tolerating the `pub(super)` prefix the
        // sibling methods carry in the submodule.
        let after_header = &SOURCE[fn_start + fn_anchor.len()..];
        let body_end = [
            after_header.find("\n    async fn "),
            after_header.find("\n    pub(super) async fn "),
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|p| fn_start + fn_anchor.len() + p)
        .unwrap_or(SOURCE.len());
        let body = &SOURCE[fn_start..body_end];

        // The targets-empty branch (`if target_result.targets.is_empty() {`)
        // ends at the `return;` that precedes the targets-found code. Everything
        // after that return is the targets-found path.
        let empty_branch_anchor = "if target_result.targets.is_empty() {";
        let empty_start = body.find(empty_branch_anchor).expect(
            "#4359: targets-empty branch anchor missing — give-up stash wiring cannot be located",
        );
        let empty_branch = &body[empty_start..];
        let empty_return = empty_branch
            .find("\n            return;")
            .expect("#4359: targets-empty branch must early-return before the targets-found path");
        let giveup_branch = &empty_branch[..empty_return];
        let targets_found_branch = &empty_branch[empty_return..];

        assert!(
            giveup_branch.contains("pending_broadcasts\n                    .stash(")
                || giveup_branch.contains("pending_broadcasts.stash("),
            "#4359: the broadcast give-up (retries-exhausted) branch MUST stash the state in \
             op_manager.pending_broadcasts — otherwise a fresh-contract PUT that loses the \
             broadcast/interest-resolve race is permanently abandoned (locally-hosted only). \
             Give-up branch:\n{giveup_branch}"
        );
        assert!(
            giveup_branch.contains("pending_broadcast_stash_recheck"),
            "#4359: the give-up branch MUST re-resolve targets after stashing (the \
             pending_broadcast_stash_recheck guard) to close the stash-after-flush \
             lost-wakeup race. Give-up branch:\n{giveup_branch}"
        );
        assert!(
            targets_found_branch.contains("pending_broadcasts.take("),
            "#4359: the targets-found branch MUST take()/clear any stashed deferred broadcast \
             so a recovered contract is not later re-emitted with superseded state. \
             Targets-found branch:\n{targets_found_branch}"
        );
    }

    /// Strip every `#[cfg(test)]` region (single item *or* brace-delimited
    /// module/block) from a Rust source string, leaving only production code.
    ///
    /// This is deliberately a small ad-hoc scanner rather than a real parser:
    /// it only needs to be precise enough that test-only
    /// `register_peer_interest(` calls (which all live under `#[cfg(test)]`)
    /// don't count toward the production tally walked by
    /// `pending_broadcast_flush_wired_at_all_interest_sites`. It handles the
    /// two cfg(test) shapes that occur in this crate:
    ///
    /// * single item — `#[cfg(test)]\npub(crate) use ...;` / `mod foo;`
    ///   (removed up to and including the terminating `;`), and
    /// * block — `#[cfg(test)]\nmod tests { ... }` (removed with brace
    ///   balancing, so nested `{}` inside the block don't end it early).
    ///
    /// Braces inside string/char literals or comments are ignored well enough
    /// for this purpose because the only thing we measure afterward is the
    /// count of two specific identifiers, and those never appear inside a
    /// literal in production source.
    fn strip_cfg_test_regions(src: &str) -> String {
        const MARKER: &str = "#[cfg(test)]";
        let bytes = src.as_bytes();
        let mut out = String::with_capacity(src.len());
        let mut i = 0usize;
        while i < src.len() {
            if src[i..].starts_with(MARKER) {
                // Advance past the marker and any following attributes/whitespace
                // until the first significant token of the gated item.
                let mut j = i + MARKER.len();
                // Skip whitespace and additional `#[...]` attribute lines.
                loop {
                    while j < src.len() && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if src[j..].starts_with("#[") {
                        // Skip a balanced `#[ ... ]` attribute.
                        let mut depth = 0i32;
                        while j < src.len() {
                            match bytes[j] {
                                b'[' => depth += 1,
                                b']' => {
                                    depth -= 1;
                                    if depth == 0 {
                                        j += 1;
                                        break;
                                    }
                                }
                                _ => {}
                            }
                            j += 1;
                        }
                        continue;
                    }
                    break;
                }
                // From `j`, find the end of this gated item: either the matching
                // `}` of the first `{ ... }` block, or the terminating `;`.
                let mut k = j;
                let mut found_brace = false;
                while k < src.len() {
                    match bytes[k] {
                        b'{' => {
                            found_brace = true;
                            break;
                        }
                        b';' => break,
                        _ => {}
                    }
                    k += 1;
                }
                if found_brace {
                    // Brace-balance to the matching close.
                    let mut depth = 0i32;
                    while k < src.len() {
                        match bytes[k] {
                            b'{' => depth += 1,
                            b'}' => {
                                depth -= 1;
                                if depth == 0 {
                                    k += 1;
                                    break;
                                }
                            }
                            _ => {}
                        }
                        k += 1;
                    }
                } else if k < src.len() {
                    // Consume the terminating `;`.
                    k += 1;
                }
                i = k;
                continue;
            }
            // Copy one byte of production source. `src` is valid UTF-8 and we
            // only ever split on ASCII boundaries (markers/braces/`;`), so
            // pushing the char at this index is safe.
            let ch = src[i..].chars().next().unwrap();
            out.push(ch);
            i += ch.len_utf8();
        }
        out
    }

    /// Recursively collect every `*.rs` file under `dir`.
    fn collect_rs_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_rs_files(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }

    /// #4359 trigger-completeness pin (MUST-FIX 1). The deferred broadcast only
    /// drains when a viable target appears, signalled by
    /// `flush_pending_broadcast_on_interest`. The interest-manager (Source 2)
    /// path makes a peer a target via `register_peer_interest`; EVERY such call
    /// site that can be the first viable target for a never-seen id must flush.
    ///
    /// Rather than trusting a static allow-list of files (the brittle shape the
    /// re-review flagged — a NEW production `register_peer_interest` added in
    /// some other module would silently escape the pin and re-open the
    /// unflushed-target bug), this pin WALKS the entire crate source tree
    /// (`$CARGO_MANIFEST_DIR/src/**/*.rs`), strips `#[cfg(test)]` regions, and
    /// asserts that:
    ///
    ///   1. the SET of production files that contain a `register_peer_interest(`
    ///      call equals the known-wired set (so a brand-new file with such a
    ///      call trips the pin even before we reach the per-file count), and
    ///   2. within each such file, production
    ///      `flush_pending_broadcast_on_interest(` calls >= production
    ///      `register_peer_interest(` calls (the pairing convention the fix
    ///      established).
    ///
    /// Source 1 (proximity) is guarded by the node.rs assertion at the end.
    #[test]
    fn pending_broadcast_flush_wired_at_all_interest_sites() {
        // The set of production files (relative to `src/`) that are known to
        // make a peer a first-viable-target via `register_peer_interest` and
        // are correctly flush-paired. A NEW production call site in any other
        // file must FAIL this pin — see the set-equality assertion below.
        let known_wired: std::collections::BTreeSet<&str> = [
            "node.rs",
            "operations.rs",
            "operations/subscribe.rs",
            "operations/get/op_ctx_task.rs",
        ]
        .into_iter()
        .collect();

        let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        collect_rs_files(&src_root, &mut files);
        assert!(
            !files.is_empty(),
            "#4359 MUST-FIX 1: source walk found no .rs files under {} — the pin cannot \
             guarantee completeness if it can't read the crate source.",
            src_root.display()
        );

        let mut found_with_register: std::collections::BTreeSet<String> = Default::default();
        // Per-file pairing failures: (relative path, reg_count, flush_count).
        let mut pairing_failures: Vec<(String, usize, usize)> = Vec::new();

        for path in &files {
            let rel = path
                .strip_prefix(&src_root)
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            let src = match std::fs::read_to_string(path) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let prod = strip_cfg_test_regions(&src);
            let reg_count = prod.matches("register_peer_interest(").count();
            if reg_count == 0 {
                continue;
            }
            // The interest-manager method *definition* itself lives in
            // ring/interest.rs and is not a call site; ignore it. Detect by the
            // presence of the `pub fn register_peer_interest` definition.
            if prod.contains("fn register_peer_interest(") {
                continue;
            }
            found_with_register.insert(rel.clone());
            let flush_count = prod.matches("flush_pending_broadcast_on_interest(").count();
            if flush_count < reg_count {
                pairing_failures.push((rel, reg_count, flush_count));
            }
        }

        // (1) Set equality: no NEW production file may carry a
        // register_peer_interest call without being deliberately wired here.
        let found_refs: std::collections::BTreeSet<&str> =
            found_with_register.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            found_refs, known_wired,
            "#4359 MUST-FIX 1: the set of production files that call \
             register_peer_interest changed. Found {found_refs:?}, expected {known_wired:?}. \
             A NEW register_peer_interest call site can be the first viable broadcast target \
             for a cold id; it MUST flush the deferred broadcast (call \
             flush_pending_broadcast_on_interest) or the #4359 fix silently fails for that \
             path. Wire the flush, then add the file to `known_wired` in this pin. If the new \
             site genuinely cannot be a first viable target, document why and adjust this pin."
        );

        // (2) Per-file pairing: every wired file's flush count covers its
        // register count.
        assert!(
            pairing_failures.is_empty(),
            "#4359 MUST-FIX 1: interest registration without a paired broadcast flush: \
             {pairing_failures:?} (file, register_peer_interest count, \
             flush_pending_broadcast_on_interest count). Every interest registration that can \
             be the first viable broadcast target for a cold id must flush the deferred \
             broadcast, or the fix silently fails for that path."
        );

        // Source 1 (proximity): the NeighborHosting overlap path must flush too,
        // since a neighbor newly announcing it hosts our contract is a viable
        // target with no interest_manager entry.
        let node_path = src_root.join("node.rs");
        let node_src = std::fs::read_to_string(&node_path).expect("node.rs must be readable");
        let node_prod = strip_cfg_test_regions(&node_src);
        assert!(
            node_prod.contains("Source 1 / proximity")
                && node_prod.contains("flush_pending_broadcast_on_interest(&key)"),
            "#4359 MUST-FIX 1: the NeighborHosting proximity overlap path in node.rs must flush \
             the deferred broadcast (Source 1 first-viable-target signal), or a cold-id PUT whose \
             first target arrives via proximity announcement never drains."
        );
    }

    // ----------------------------------------------------------------------
    // #4145: blocking .send().await on event-loop-reachable channels.
    //
    // The behavioral tests below pin the two properties the SITE 1 fix
    // depends on. The source-scrape pins guard against a future revert of
    // any of the eight converted sites.
    // ----------------------------------------------------------------------

    /// SITE 1 liveness: when the event-loop bridge channel is full, the
    /// ConnectPeer enqueue must NOT block the event loop. `try_send` on a
    /// full bounded channel returns `Full` immediately rather than parking,
    /// which is what lets the loop `continue` and drop the outbound attempt.
    #[tokio::test]
    async fn full_bridge_channel_try_send_does_not_block() {
        // Cap 1, then fill it so the next send would block a `.send().await`.
        let (tx, _rx) = mpsc::channel::<u32>(1);
        tx.try_send(1).expect("first send fills the single slot");

        // A blocking send would hang here forever (receiver is parked, never
        // draining). try_send must return immediately with Full.
        let res = timeout(Duration::from_millis(500), async { tx.try_send(2) })
            .await
            .expect("try_send must return immediately, never block the event loop");

        match res {
            Err(mpsc::error::TrySendError::Full(v)) => assert_eq!(v, 2),
            other => panic!("expected TrySendError::Full, got {other:?}"),
        }
    }

    /// SITE 1 resend-waiter safety: the waiter does `timeout(20s,
    /// result.recv())`. If the ConnectPeer enqueue is dropped on a full
    /// channel, the `callback` Sender (carried inside the dropped event) is
    /// dropped too, so the waiter's `result.recv()` must observe `None`
    /// IMMEDIATELY — never hang the full 20s on a connection that was never
    /// requested. This reproduces the drop path: build the callback/result
    /// pair exactly as the production code does, then drop the callback (as
    /// dropping the TrySendError::Full(event) does) without registering it.
    #[tokio::test]
    async fn dropped_connect_peer_does_not_hang_resend_waiter() {
        // Mirror the production pair: `let (callback, mut result) = mpsc::channel(10);`
        let (callback, mut result) = mpsc::channel::<u32>(10);

        // Simulate the Full path: the event (and thus `callback`) is dropped
        // without ever being delivered to a connect op.
        drop(callback);

        // The real waiter awaits up to 20s. Bound the test well under that:
        // if the callback drop did NOT close `result`, this recv would hang
        // and the timeout would fire. A correct drop closes the channel so
        // recv() resolves to None immediately.
        let recv = timeout(Duration::from_secs(1), result.recv())
            .await
            .expect("result.recv() must resolve immediately when callback dropped, not hang");
        assert!(
            recv.is_none(),
            "dropping the callback must close the result channel (recv -> None), \
             so the resend-waiter short-circuits instead of waiting 20s"
        );
    }

    /// Production source ONLY (everything before the `mod tests` block).
    /// We deliberately do not use `strip_cfg_test_regions` here: these pins
    /// reference the very `.send(...)` substrings they forbid, inside their
    /// own assertion strings, so scanning the test module (whose brace-based
    /// strip can be thrown off by `{`/`}` in earlier test string literals)
    /// would self-trip. Slicing at the single top-level `mod tests {` is
    /// robust and unambiguous.
    fn production_src() -> String {
        // The `P2pConnManager` impl is split across this module root and its
        // `p2p_protoc/` submodules, so the source-scrape pins below must see
        // ALL of them concatenated — otherwise a `try_send` / `send` site that
        // moved into a submodule would silently drop out of the scan.
        let full = include_str!("p2p_protoc.rs");
        let cut = full
            .find("\nmod tests {")
            .expect("p2p_protoc.rs must have a `mod tests {` block");
        [
            &full[..cut],
            include_str!("p2p_protoc/migration.rs"),
            include_str!("p2p_protoc/broadcast.rs"),
            include_str!("p2p_protoc/dispatch.rs"),
            include_str!("p2p_protoc/connection_lifecycle.rs"),
        ]
        .join("\n")
    }

    /// Source-scrape pin: SITE 1. The ConnectPeer enqueue in the
    /// OutboundMessageWithTarget no-connection branch must use the
    /// non-blocking `ev_listener_tx.try_send(` — never a blocking
    /// `.send(...).await` — or the event loop can self-deadlock (#4145).
    #[test]
    fn connect_peer_enqueue_is_non_blocking() {
        let src = production_src();
        // Anchor on the SITE-1 ConnectPeer enqueue specifically. The broader
        // "no blocking send on a watched channel" guarantee is enforced by the
        // CI linter (.github/scripts/check_blocking_sends.py); here we pin the
        // SITE-1 shape and the resend-waiter ordering it depends on.
        let try_send_at = src.find("ev_listener_tx.try_send(").expect(
            "#4145 SITE 1: the ConnectPeer enqueue must use ev_listener_tx.try_send(), \
             not a blocking .send().await (self-deadlock on the channel the loop drains).",
        );

        // Spawn-ordering invariant (review item 10): the resend-waiter must be
        // spawned STRICTLY AFTER the try_send. The waiter does
        // `timeout(20s, result.recv())`; it is sound only because on a Full
        // enqueue we `continue` BEFORE spawning, so a dropped callback closes
        // `result` and the waiter would short-circuit. If a future refactor
        // spawned the waiter before (or unconditionally regardless of) the
        // try_send, a dropped ConnectPeer would leave the waiter parked the full
        // 20s on a connection that was never requested. We anchor on the waiter's
        // 20s timeout, which is unique to this site.
        let waiter_at = src[try_send_at..]
            .find("Duration::from_secs(20)")
            .map(|rel| try_send_at + rel)
            .expect(
                "#4145 SITE 1: the resend-waiter (timeout 20s) must appear AFTER the \
                 ev_listener_tx.try_send — found it before, which risks a 20s hang on a \
                 dropped ConnectPeer.",
            );
        // And the `continue` that short-circuits the Full path must sit between
        // the try_send and the waiter spawn.
        let between = &src[try_send_at..waiter_at];
        assert!(
            between.contains("continue"),
            "#4145 SITE 1: the Full-path `continue` must short-circuit BEFORE the \
             resend-waiter spawn, so a dropped ConnectPeer never spawns a waiter."
        );
    }

    /// Source-scrape pin (review item 2): the four inline-on-event-loop
    /// `P2pBridge::send` callers — both connection-establishment interest/cache
    /// fan-out loops, plus the two unicast NodeEvent handlers — must use the
    /// non-blocking `bridge.try_send(`, never `bridge.send(...).await`. A
    /// blocking send there awaits the cap-512 ev_listener_tx the event loop
    /// itself drains (#4145). The off-loop callers (`broadcast_to_peers`'
    /// spawned task, the sim-only `broadcast_state_to_peers`) and the
    /// op-driver/contract-task callers stay blocking and are NOT matched here.
    #[test]
    fn inline_bridge_sends_are_non_blocking() {
        let src = production_src();
        // The four converted call sites all read `self.bridge.try_send(`.
        let converted = src.matches("self.bridge.try_send(").count();
        assert!(
            converted >= 4,
            "#4145 item 2: expected at least four inline-on-loop self.bridge.try_send(...) \
             call sites (2 fan-out loops + 2 unicast handlers), found {converted}."
        );
        // The blocking `self.bridge.send(...).await` form must be GONE from
        // production (the remaining blocking bridge.send callers use the bare
        // `bridge.send(` receiver — broadcast_to_peers' spawned task and the
        // sim-only broadcast_state_to_peers — not `self.bridge.send(`).
        assert!(
            !src.contains("self.bridge.send("),
            "#4145 item 2: a blocking self.bridge.send(...).await remains on an \
             inline-on-event-loop path — must be self.bridge.try_send()."
        );
    }

    /// Source-scrape pin: SITES 2-7. Every HandshakeCommand send from the
    /// event-loop task must go through `try_send` (the bare blocking
    /// `.send(HandshakeCommand::` shape stalls the loop, #4145). The blocking
    /// `CommandSender::send` method was removed so it cannot be reintroduced.
    #[test]
    fn handshake_commands_are_non_blocking() {
        let src = production_src();
        assert!(
            !src.contains(".send(HandshakeCommand::"),
            "#4145 SITES 2-7: a blocking .send(HandshakeCommand::...).await remains — \
             every handshake command from the event loop must use try_send()."
        );
        // Sanity: every place that constructs a HandshakeCommand for sending
        // goes through try_send. The conversions plus the original zombie
        // precedent give seven handshake try_send sites; some use the
        // same-line `try_send(HandshakeCommand::` form and one uses the
        // multi-line `try_send(\n    HandshakeCommand::` form, so we count
        // both rather than pin a brittle single-form number.
        let same_line = src.matches("try_send(HandshakeCommand::").count();
        let multi_line = src.matches("try_send(").count().saturating_sub(same_line);
        assert!(
            same_line >= 6,
            "#4145 SITES 2-7: expected at least six same-line \
             try_send(HandshakeCommand::...) call sites, found {same_line}."
        );
        // The multi-line try_send( count includes the ev_listener_tx site too,
        // so we only assert it is non-zero as a smoke check that the
        // multi-line conversion (SITE 2 cleanup) survives.
        assert!(
            multi_line >= 1,
            "#4145: expected at least one multi-line try_send( call site."
        );
    }

    /// Behavioral test (review item 8) for the SITE 6 connect-command-drop
    /// cleanup. When the handshake command channel is full/closed and the
    /// `HandshakeCommand::Connect` enqueue is dropped, `connect_peer` must:
    ///   (a) prune the in-transit connection slot — covered by the source pin
    ///       `connect_peer_full_handshake_channel_prunes_in_transit` below,
    ///       since the prune touches the live ring;
    ///   (b) resolve EVERY awaiting callback with Err(()) — never park it;
    ///   (c) drain BOTH awaiting_connection and awaiting_connection_txs;
    ///   (d) NOT record peer backoff (local channel contention, not a remote
    ///       failure).
    /// This drives the extracted `fail_awaiting_connection` helper directly,
    /// which owns (b)-(d).
    #[tokio::test]
    async fn connect_command_drop_fails_callbacks_without_backoff() {
        use crate::dev_tool::Transaction;
        use crate::transport::ExpectedInboundTracker;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let mut state = super::EventListenerState::new(ExpectedInboundTracker::empty_for_test());
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 45678);
        let tx = Transaction::new::<crate::operations::connect::ConnectMsg>();

        // Two callers queued on this peer (mirrors the Occupied-entry append).
        let (cb1, mut rx1) = mpsc::channel::<Result<(SocketAddr, Option<usize>), ()>>(1);
        let (cb2, mut rx2) = mpsc::channel::<Result<(SocketAddr, Option<usize>), ()>>(1);
        state.awaiting_connection.insert(
            peer_addr,
            vec![
                Box::new(cb1) as Box<dyn super::ConnectResultSender>,
                Box::new(cb2) as Box<dyn super::ConnectResultSender>,
            ],
        );
        state.awaiting_connection_txs.insert(peer_addr, vec![tx]);

        // Precondition: peer is NOT in backoff.
        assert!(
            !state.peer_backoff.is_in_backoff(peer_addr),
            "precondition: peer must not be in backoff before the drop"
        );

        super::fail_awaiting_connection(&mut state, peer_addr, tx, false).await;

        // (b) every callback resolved with Err(()), immediately (not parked).
        let r1 = timeout(Duration::from_secs(1), rx1.recv())
            .await
            .expect("callback 1 must resolve immediately, not park");
        let r2 = timeout(Duration::from_secs(1), rx2.recv())
            .await
            .expect("callback 2 must resolve immediately, not park");
        assert_eq!(r1, Some(Err(())), "callback 1 must receive Err(())");
        assert_eq!(r2, Some(Err(())), "callback 2 must receive Err(())");

        // (c) both maps drained for this peer.
        assert!(
            !state.awaiting_connection.contains_key(&peer_addr),
            "awaiting_connection must be drained for the peer"
        );
        assert!(
            !state.awaiting_connection_txs.contains_key(&peer_addr),
            "awaiting_connection_txs must be drained for the peer (lock-step with callbacks)"
        );

        // (d) no peer backoff recorded.
        assert!(
            !state.peer_backoff.is_in_backoff(peer_addr),
            "#4145 SITE 6: a LOCAL handshake-channel-full drop must NOT record peer \
             backoff — that would wrongly delay a legitimate retry."
        );
    }

    /// Source pin (review item 8, invariant a): the connect-command-drop branch
    /// in `connect_peer` must prune the in-transit connection slot, and must NOT
    /// record peer backoff on this path. (The prune touches the live ring, so it
    /// is verified by source-scrape rather than the behavioral test above.)
    #[test]
    fn connect_peer_full_handshake_channel_prunes_in_transit() {
        let src = production_src();
        // Locate the Connect-command try_send and bound a window over its
        // failure branch (the `fail_awaiting_connection` call is the tail).
        let from = src
            .find("try_send(HandshakeCommand::Connect {")
            .expect("connect_peer must enqueue HandshakeCommand::Connect via try_send");
        let end = (from + 1200).min(src.len());
        let branch = &src[from..end];
        assert!(
            branch.contains("prune_in_transit_connection(peer_addr)"),
            "#4145 SITE 6: the connect-command-drop branch must prune the in-transit \
             connection slot so the dropped attempt does not leak a reservation."
        );
        assert!(
            branch.contains("fail_awaiting_connection("),
            "#4145 SITE 6: the connect-command-drop branch must fail awaiting callbacks \
             via fail_awaiting_connection()."
        );
        // Must NOT record backoff on this local-contention path.
        assert!(
            !branch.contains("record_failure"),
            "#4145 SITE 6: the connect-command-drop branch must NOT record peer backoff \
             (local channel contention is not a remote connection failure)."
        );
    }
}
