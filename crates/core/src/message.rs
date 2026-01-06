//! Network messaging between peers.
//! Defines the `NetMessage` enum, the standard format for all peer-to-peer communication within the Freenet network.
//! See `architecture.md`.

#[cfg(feature = "trace-ot")]
use std::time::SystemTime;
use std::{borrow::Cow, fmt::Display, net::SocketAddr, time::Duration};

use crate::{
    client_events::{ClientId, HostResult},
    operations::{
        connect::ConnectMsg, get::GetMsg, put::PutMsg, subscribe::SubscribeMsg, update::UpdateMsg,
    },
    ring::{Location, PeerKeyLocation},
};
use freenet_stdlib::prelude::{
    ContractContainer, ContractInstanceId, ContractKey, DelegateKey, WrappedState,
};
pub(crate) use sealed_msg_type::{TransactionType, TransactionTypeId};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// An transaction is a unique, universal and efficient identifier for any
/// roundtrip transaction as it is broadcasted around the Freenet network.
///
/// The identifier conveys all necessary information to identify and classify the
/// transaction:
/// - The unique identifier itself.
/// - The type of transaction being performed.
/// - If the transaction has been finalized, this allows for the connection manager
///   to sweep any garbage left by a finished (or timed out) transaction.
///
/// A transaction may span different messages sent across the network.
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub struct Transaction {
    id: Ulid,
    /// Parent transaction ID for child operations spawned by this transaction.
    /// Enables atomicity tracking for composite operations (e.g., PUT with SUBSCRIBE).
    parent: Option<Ulid>,
}

impl Transaction {
    pub const NULL: &'static Transaction = &Transaction {
        id: Ulid(0),
        parent: None,
    };

    pub(crate) fn new<T: TxType>() -> Self {
        let ty = <T as TxType>::tx_type_id();
        let id = crate::config::GlobalSimulationTime::new_ulid();
        Self::update(ty.0, id, None)
    }

    /// Creates a child transaction with the specified type, linked to the parent
    /// for atomicity tracking in composite operations.
    pub(crate) fn new_child_of<T: TxType>(parent: &Transaction) -> Self {
        let ty = <T as TxType>::tx_type_id();
        let id = crate::config::GlobalSimulationTime::new_ulid();
        Self::update(ty.0, id, Some(parent.id))
    }

    /// Returns the parent transaction ID for child operations.
    pub fn parent_id(&self) -> Option<&Ulid> {
        self.parent.as_ref()
    }

    /// Returns true if this transaction is a child operation.
    pub fn is_sub_operation(&self) -> bool {
        self.parent.is_some()
    }

    pub(crate) fn transaction_type(&self) -> TransactionType {
        let id_byte = (self.id.0 & 0xFFu128) as u8;
        match id_byte {
            0 => TransactionType::Connect,
            1 => TransactionType::Put,
            2 => TransactionType::Get,
            3 => TransactionType::Subscribe,
            4 => TransactionType::Update,
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }

    pub fn timed_out(&self) -> bool {
        self.elapsed() >= crate::config::OPERATION_TTL
    }

    #[cfg(feature = "trace-ot")]
    pub fn started(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.id.timestamp_ms())
    }

    #[cfg(feature = "trace-ot")]
    pub fn as_bytes(&self) -> [u8; 16] {
        self.id.0.to_le_bytes()
    }

    /// Returns the transaction ID as raw bytes.
    /// Used for deriving hash keys in bloom filters.
    pub fn id_bytes(&self) -> [u8; 16] {
        self.id.0.to_le_bytes()
    }

    /// Returns the elapsed time since this transaction was created.
    ///
    /// Uses simulation time when in simulation mode, otherwise system time.
    /// This ensures deterministic elapsed time calculations in DST tests.
    pub fn elapsed(&self) -> Duration {
        use crate::config::GlobalSimulationTime;
        let current_unix_epoch_ts = GlobalSimulationTime::read_time_ms();
        let this_tx_creation = self.id.timestamp_ms();
        if current_unix_epoch_ts < this_tx_creation {
            Duration::new(0, 0)
        } else {
            let ms_elapsed = current_unix_epoch_ts - this_tx_creation;
            Duration::from_millis(ms_elapsed)
        }
    }

    /// Generate a random transaction which has the implicit TTL cutoff.
    ///
    /// This will allow, for example, to compare against any older transactions,
    /// in order to remove them.
    pub fn ttl_transaction() -> Self {
        let id = crate::config::GlobalSimulationTime::new_ulid();
        let ts = id.timestamp_ms();
        const TTL_MS: u64 = crate::config::OPERATION_TTL.as_millis() as u64;
        let ttl_epoch: u64 = ts - TTL_MS;

        // Clear the ts significant bits of the ULID and replace them with the new cutoff ts.
        const TIMESTAMP_MASK: u128 = 0x00000000000000000000FFFFFFFFFFFFFFFF;
        let new_ulid = (id.0 & TIMESTAMP_MASK) | ((ttl_epoch as u128) << 80);
        Self {
            id: Ulid(new_ulid),
            parent: None,
        }
    }

    fn update(ty: TransactionType, id: Ulid, parent: Option<Ulid>) -> Self {
        const TYPE_MASK: u128 = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00u128;
        // Clear the last byte
        let cleared = id.0 & TYPE_MASK;
        // Set the last byte with the transaction type
        let updated = cleared | (ty as u8) as u128;

        // 2 words size for 64-bits platforms
        Self {
            id: Ulid(updated),
            parent,
        }
    }
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for Transaction {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let ty: TransactionTypeId = u.arbitrary()?;
        let bytes: u128 = Ulid::new().0;
        Ok(Self::update(ty.0, Ulid(bytes), None))
    }
}

impl Display for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl std::fmt::Debug for Transaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl PartialOrd for Transaction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Transaction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

/// Get the transaction type associated to a given message type.
pub trait TxType: sealed_msg_type::SealedTxType {
    fn tx_type_id() -> TransactionTypeId;
}

impl<T> TxType for T
where
    T: sealed_msg_type::SealedTxType,
{
    fn tx_type_id() -> TransactionTypeId {
        <Self as sealed_msg_type::SealedTxType>::tx_type_id()
    }
}

mod sealed_msg_type {
    use super::*;
    use crate::operations::connect::ConnectMsg;

    pub trait SealedTxType {
        fn tx_type_id() -> TransactionTypeId;
    }

    #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
    #[cfg_attr(test, derive(arbitrary::Arbitrary))]
    pub struct TransactionTypeId(pub(super) TransactionType);

    #[repr(u8)]
    #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
    #[cfg_attr(test, derive(arbitrary::Arbitrary))]
    pub enum TransactionType {
        Connect = 0,
        Put = 1,
        Get = 2,
        Subscribe = 3,
        Update = 4,
    }

    impl TransactionType {
        pub fn description(&self) -> &'static str {
            match self {
                TransactionType::Connect => "connect",
                TransactionType::Put => "put",
                TransactionType::Get => "get",
                TransactionType::Subscribe => "subscribe",
                TransactionType::Update => "update",
            }
        }
    }

    impl Display for TransactionType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.description())
        }
    }

    macro_rules! transaction_type_enumeration {
        ($variant:ident, $enum_type:ident, decl struct { $( $var:ident -> $ty:ty ),+ }) => {
            $(
                impl From<$ty> for NetMessage {
                    fn from(msg: $ty) -> Self {
                        Self::$variant($enum_type::$var(msg))
                    }
                }

                impl SealedTxType for $ty {
                    fn tx_type_id() -> TransactionTypeId {
                        TransactionTypeId(TransactionType::$var)
                    }
                }
            )+
        };
    }

    transaction_type_enumeration!(V1, NetMessageV1, decl struct {
        Connect -> ConnectMsg,
        Put -> PutMsg,
        Get -> GetMsg,
        Subscribe -> SubscribeMsg,
        Update -> UpdateMsg
    });
}

pub(crate) trait MessageStats {
    fn id(&self) -> &Transaction;

    fn requested_location(&self) -> Option<Location>;
}

/// Wrapper for inbound messages that carries the source address from the transport layer.
/// This separates routing concerns from message content - the source address is determined by
/// the network layer (from the packet), not embedded in the serialized message.
///
/// Generic over the message type so it can wrap:
/// - `NetMessage` at the network layer (p2p_protoc.rs)
/// - Specific operation messages (GetMsg, PutMsg, etc.) at the operation layer
///
/// Note: Currently unused but prepared for Phase 4 of #2164.
/// Will be used to thread source addresses to operations for routing.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InboundMessage<M> {
    /// The message content
    pub msg: M,
    /// The socket address this message was received from (from UDP packet source)
    pub source_addr: SocketAddr,
}

#[allow(dead_code)]
impl<M> InboundMessage<M> {
    /// Create a new inbound message wrapper
    pub fn new(msg: M, source_addr: SocketAddr) -> Self {
        Self { msg, source_addr }
    }

    /// Transform the inner message while preserving source_addr
    pub fn map<N>(self, f: impl FnOnce(M) -> N) -> InboundMessage<N> {
        InboundMessage {
            msg: f(self.msg),
            source_addr: self.source_addr,
        }
    }

    /// Get a reference to the inner message
    pub fn inner(&self) -> &M {
        &self.msg
    }
}

#[allow(dead_code)]
impl InboundMessage<NetMessage> {
    /// Get the transaction ID from the wrapped network message
    pub fn id(&self) -> &Transaction {
        self.msg.id()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum NetMessage {
    V1(NetMessageV1),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum NetMessageV1 {
    Connect(ConnectMsg),
    Put(PutMsg),
    Get(GetMsg),
    Subscribe(SubscribeMsg),
    Unsubscribed {
        transaction: Transaction,
        key: ContractKey,
        from: PeerKeyLocation,
    },
    Update(UpdateMsg),
    Aborted(Transaction),
    /// Proximity cache protocol message for tracking which neighbors cache which contracts.
    ProximityCache {
        message: ProximityCacheMessage,
    },
}

/// Messages for the proximity cache protocol.
///
/// This protocol allows neighbors to inform each other which contracts they have cached,
/// enabling UPDATE forwarding to seeders who may not be explicitly subscribed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum ProximityCacheMessage {
    /// Announce changes to our cached contracts.
    CacheAnnounce {
        /// Contracts we've started caching.
        added: Vec<ContractInstanceId>,
        /// Contracts we've stopped caching.
        removed: Vec<ContractInstanceId>,
    },
    /// Request a neighbor's full cache state (used on new connections).
    CacheStateRequest,
    /// Response with the neighbor's full cache state.
    CacheStateResponse { contracts: Vec<ContractInstanceId> },
}

trait Versioned {
    fn version(&self) -> semver::Version;
}

impl Versioned for NetMessage {
    fn version(&self) -> semver::Version {
        match self {
            NetMessage::V1(inner) => inner.version(),
        }
    }
}

impl Versioned for NetMessageV1 {
    fn version(&self) -> semver::Version {
        match self {
            NetMessageV1::Connect(_) => semver::Version::new(1, 1, 0),
            NetMessageV1::Put(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Get(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Subscribe(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Unsubscribed { .. } => semver::Version::new(1, 0, 0),
            NetMessageV1::Update(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Aborted(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::ProximityCache { .. } => semver::Version::new(1, 0, 0),
        }
    }
}

impl From<NetMessage> for semver::Version {
    fn from(msg: NetMessage) -> Self {
        msg.version()
    }
}

pub trait InnerMessage: Into<NetMessage> {
    fn id(&self) -> &Transaction;

    fn requested_location(&self) -> Option<Location>;
}

type RemainingChecks = Option<usize>;
type ConnectResult = Result<(SocketAddr, RemainingChecks), ()>;

/// Internal node events emitted to the event loop.
#[derive(Debug, Clone)]
pub(crate) enum NodeEvent {
    /// Drop the given peer connection by socket address.
    DropConnection(SocketAddr),
    // Try connecting to the given peer.
    ConnectPeer {
        peer: PeerKeyLocation,
        tx: Transaction,
        callback: tokio::sync::mpsc::Sender<ConnectResult>,
        is_gw: bool,
    },
    Disconnect {
        cause: Option<Cow<'static, str>>,
    },
    QueryConnections {
        callback: tokio::sync::mpsc::Sender<QueryResult>,
    },
    QuerySubscriptions {
        callback: tokio::sync::mpsc::Sender<QueryResult>,
    },
    QueryNodeDiagnostics {
        config: freenet_stdlib::client_api::NodeDiagnosticsConfig,
        callback: tokio::sync::mpsc::Sender<QueryResult>,
    },
    TransactionTimedOut(Transaction),
    /// Transaction completed successfully - cleanup client subscription
    TransactionCompleted(Transaction),
    /// **Standalone** subscription completed - deliver SubscribeResponse to client via result router.
    ///
    /// **IMPORTANT:** This event is ONLY used for standalone subscriptions (no remote peers available).
    /// Normal network subscriptions go through `handle_op_result`, which sends results via
    /// `result_router_tx` directly without needing this event.
    ///
    /// **Architecture Note (Issue #2075):**
    /// Local client subscriptions are handled separately from network peer subscriptions:
    /// - Subsequent contract updates are delivered via the executor's `update_notifications`
    ///   channels (see `send_update_notification` in runtime.rs)
    /// - Network peer subscriptions use the `seeding_manager.subscribers` for UPDATE propagation
    LocalSubscribeComplete {
        tx: Transaction,
        key: ContractKey,
        subscribed: bool,
    },
    /// Register expectation for an inbound connection from the given peer.
    ExpectPeerConnection {
        addr: SocketAddr,
    },
    /// Broadcast a proximity cache message to all connected peers.
    BroadcastProximityCache {
        message: ProximityCacheMessage,
    },
    /// A WebSocket client disconnected - clean up its subscriptions and trigger tree pruning.
    ClientDisconnected {
        client_id: ClientId,
    },
}

#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub instance_id: ContractInstanceId,
    pub client_id: ClientId,
    pub last_update: Option<std::time::SystemTime>,
}

#[derive(Debug, Clone)]
pub struct NetworkDebugInfo {
    /// Application-level subscriptions (WebSocket clients subscribed to contracts)
    pub application_subscriptions: Vec<SubscriptionInfo>,
    /// Network-level subscriptions (nodes subscribing to contracts for routing)
    #[allow(dead_code)] // Used for debugging purposes, not exposed via stdlib API yet
    pub network_subscriptions: Vec<(ContractKey, Vec<SocketAddr>)>,
    pub connected_peers: Vec<PeerKeyLocation>,
}

#[derive(Debug)]
pub(crate) enum QueryResult {
    Connections(Vec<PeerKeyLocation>),
    GetResult {
        key: ContractKey,
        state: WrappedState,
        contract: Option<ContractContainer>,
    },
    DelegateResult {
        #[allow(dead_code)]
        key: DelegateKey,
        response: HostResult,
    },
    NetworkDebug(NetworkDebugInfo),
    NodeDiagnostics(freenet_stdlib::client_api::NodeDiagnosticsResponse),
}

impl Display for NodeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeEvent::DropConnection(peer) => {
                write!(f, "DropConnection (from {peer})")
            }
            NodeEvent::ConnectPeer { peer, .. } => {
                write!(f, "ConnectPeer (to {peer})")
            }
            NodeEvent::Disconnect { cause: Some(cause) } => {
                write!(f, "Disconnect node, reason: {cause}")
            }
            NodeEvent::Disconnect { cause: None } => {
                write!(f, "Disconnect node, reason: unknown")
            }
            NodeEvent::QueryConnections { .. } => {
                write!(f, "QueryConnections")
            }
            NodeEvent::QuerySubscriptions { .. } => {
                write!(f, "QuerySubscriptions")
            }
            NodeEvent::QueryNodeDiagnostics { .. } => {
                write!(f, "QueryNodeDiagnostics")
            }
            NodeEvent::TransactionTimedOut(transaction) => {
                write!(f, "Transaction timed out ({transaction})")
            }
            NodeEvent::TransactionCompleted(transaction) => {
                write!(f, "Transaction completed ({transaction})")
            }
            NodeEvent::LocalSubscribeComplete {
                tx,
                key,
                subscribed,
            } => {
                write!(
                    f,
                    "Local subscribe complete (tx: {tx}, key: {key}, subscribed: {subscribed})"
                )
            }
            NodeEvent::ExpectPeerConnection { addr } => {
                write!(f, "ExpectPeerConnection (from {addr})")
            }
            NodeEvent::BroadcastProximityCache { message } => {
                write!(f, "BroadcastProximityCache ({message:?})")
            }
            NodeEvent::ClientDisconnected { client_id } => {
                write!(f, "ClientDisconnected (client: {client_id})")
            }
        }
    }
}

impl MessageStats for NetMessage {
    fn id(&self) -> &Transaction {
        match self {
            NetMessage::V1(msg) => msg.id(),
        }
    }

    fn requested_location(&self) -> Option<Location> {
        match self {
            NetMessage::V1(msg) => msg.requested_location(),
        }
    }
}

impl MessageStats for NetMessageV1 {
    fn id(&self) -> &Transaction {
        match self {
            NetMessageV1::Connect(op) => op.id(),
            NetMessageV1::Put(op) => op.id(),
            NetMessageV1::Get(op) => op.id(),
            NetMessageV1::Subscribe(op) => op.id(),
            NetMessageV1::Update(op) => op.id(),
            NetMessageV1::Aborted(tx) => tx,
            NetMessageV1::Unsubscribed { transaction, .. } => transaction,
            NetMessageV1::ProximityCache { .. } => Transaction::NULL,
        }
    }

    fn requested_location(&self) -> Option<Location> {
        match self {
            NetMessageV1::Connect(op) => op.requested_location(),
            NetMessageV1::Put(op) => op.requested_location(),
            NetMessageV1::Get(op) => op.requested_location(),
            NetMessageV1::Subscribe(op) => op.requested_location(),
            NetMessageV1::Update(op) => op.requested_location(),
            NetMessageV1::Aborted(_) => None,
            NetMessageV1::Unsubscribed { .. } => None,
            NetMessageV1::ProximityCache { .. } => None,
        }
    }
}

impl Display for NetMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use NetMessageV1::*;
        write!(f, "Message {{")?;
        match self {
            NetMessage::V1(msg) => match msg {
                Connect(msg) => msg.fmt(f)?,
                Put(msg) => msg.fmt(f)?,
                Get(msg) => msg.fmt(f)?,
                Subscribe(msg) => msg.fmt(f)?,
                Update(msg) => msg.fmt(f)?,
                Aborted(msg) => msg.fmt(f)?,
                Unsubscribed { key, from, .. } => {
                    write!(f, "Unsubscribed {{  key: {key}, from: {from} }}")?;
                }
                ProximityCache { message } => {
                    write!(f, "ProximityCache {{ {message:?} }}")?;
                }
            },
        };
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_transaction_type() {
        let ts_0 = Ulid::new();
        std::thread::sleep(Duration::from_millis(1));
        let tx = Transaction::update(TransactionType::Connect, Ulid::new(), None);
        assert_eq!(tx.transaction_type(), TransactionType::Connect);
        let tx = Transaction::update(TransactionType::Subscribe, Ulid::new(), None);
        assert_eq!(tx.transaction_type(), TransactionType::Subscribe);
        std::thread::sleep(Duration::from_millis(1));
        let ts_1 = Ulid::new();
        assert!(
            tx.id.timestamp_ms() > ts_0.timestamp_ms(),
            "{:?} <= {:?}",
            tx.id.datetime(),
            ts_0.datetime()
        );
        assert!(
            tx.id.timestamp_ms() < ts_1.timestamp_ms(),
            "{:?} >= {:?}",
            tx.id.datetime(),
            ts_1.datetime()
        );
    }

    #[test]
    fn get_ttl_cutoff_transaction() {
        let ttl_tx = Transaction::ttl_transaction();
        let original_tx = Transaction::new::<crate::operations::get::GetMsg>();

        assert!(original_tx > ttl_tx);
        assert!(ttl_tx.timed_out());
        assert!(
            original_tx.id.timestamp_ms() - ttl_tx.id.timestamp_ms()
                >= crate::config::OPERATION_TTL.as_millis() as u64
        );
        assert!(
            original_tx.id.timestamp_ms() - ttl_tx.id.timestamp_ms()
                < crate::config::OPERATION_TTL.as_millis() as u64 + 5
        );
    }
}
