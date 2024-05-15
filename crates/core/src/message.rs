//! Network messaging between peers.

use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
    time::{Duration, SystemTime},
};

use freenet_stdlib::prelude::ContractKey;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    node::PeerId,
    operations::{
        connect::ConnectMsg, get::GetMsg, put::PutMsg, subscribe::SubscribeMsg, update::UpdateMsg,
    },
    ring::{Location, PeerKeyLocation},
};
pub(crate) use sealed_msg_type::{TransactionType, TransactionTypeId};

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
}

impl Transaction {
    pub const NULL: &'static Transaction = &Transaction { id: Ulid(0) };

    pub(crate) fn new<T: TxType>() -> Self {
        let ty = <T as TxType>::tx_type_id();
        let id = Ulid::new();
        Self::update(ty.0, id)
        // Self { id }
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

    fn elapsed(&self) -> Duration {
        let current_unix_epoch_ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("now should be always be later than unix epoch")
            .as_millis() as u64;
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
        let id = Ulid::new();
        let ts = id.timestamp_ms();
        const TTL_MS: u64 = crate::config::OPERATION_TTL.as_millis() as u64;
        let ttl_epoch: u64 = ts - TTL_MS;

        // Clear the ts significant bits of the ULID and replace them with the new cutoff ts.
        const TIMESTAMP_MASK: u128 = 0x00000000000000000000FFFFFFFFFFFFFFFF;
        let new_ulid = (id.0 & TIMESTAMP_MASK) | ((ttl_epoch as u128) << 80);
        Self { id: Ulid(new_ulid) }
    }

    fn update(ty: TransactionType, id: Ulid) -> Self {
        const TYPE_MASK: u128 = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00u128;
        // Clear the last byte
        let cleared = id.0 & TYPE_MASK;
        // Set the last byte with the transaction type
        let updated = cleared | (ty as u8) as u128;

        // 2 words size for 64-bits platforms
        Self { id: Ulid(updated) }
    }
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for Transaction {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let ty: TransactionTypeId = u.arbitrary()?;
        let bytes: u128 = Ulid::new().0;
        Ok(Self::update(ty.0, Ulid(bytes)))
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

    fn target(&self) -> Option<PeerKeyLocation>;

    fn terminal(&self) -> bool;

    fn requested_location(&self) -> Option<Location>;

    fn track_stats(&self) -> bool;
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum NetMessage {
    V1(NetMessageV1),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum NetMessageV1 {
    Connect(ConnectMsg),
    Put(PutMsg),
    Get(GetMsg),
    Subscribe(SubscribeMsg),
    Unsubscribed {
        transaction: Transaction,
        key: ContractKey,
        from: PeerId,
    },
    Update(UpdateMsg),
    Aborted(Transaction),
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
            NetMessageV1::Connect(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Put(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Get(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Subscribe(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Unsubscribed { .. } => semver::Version::new(1, 0, 0),
            NetMessageV1::Update(_) => semver::Version::new(1, 0, 0),
            NetMessageV1::Aborted(_) => semver::Version::new(1, 0, 0),
        }
    }
}

impl From<NetMessage> for semver::Version {
    fn from(msg: NetMessage) -> Self {
        msg.version()
    }
}

pub(crate) trait InnerMessage: Into<NetMessage> {
    fn id(&self) -> &Transaction;

    fn target(&self) -> Option<impl Borrow<PeerKeyLocation>>;

    fn terminal(&self) -> bool;

    fn requested_location(&self) -> Option<Location>;
}

/// Internal node events emitted to the event loop.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum NodeEvent {
    /// For unspecified reasons the node is gracefully shutting down.
    ShutdownNode,
    /// Drop the given peer connection.
    DropConnection(PeerId),
    Disconnect {
        cause: Option<Cow<'static, str>>,
    },
}

impl Display for NodeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeEvent::ShutdownNode => f.write_str("ShutdownNode"),
            NodeEvent::DropConnection(peer) => {
                write!(f, "DropConnection (from {peer})")
            }
            NodeEvent::Disconnect { cause: Some(cause) } => {
                write!(f, "Disconnect node, reason: {cause}")
            }
            NodeEvent::Disconnect { cause: None } => {
                write!(f, "Disconnect node, reason: unknown")
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

    fn target(&self) -> Option<PeerKeyLocation> {
        match self {
            NetMessage::V1(msg) => msg.target(),
        }
    }

    fn terminal(&self) -> bool {
        match self {
            NetMessage::V1(msg) => msg.terminal(),
        }
    }

    fn requested_location(&self) -> Option<Location> {
        match self {
            NetMessage::V1(msg) => msg.requested_location(),
        }
    }

    fn track_stats(&self) -> bool {
        match self {
            NetMessage::V1(msg) => msg.track_stats(),
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
        }
    }

    fn target(&self) -> Option<PeerKeyLocation> {
        match self {
            NetMessageV1::Connect(op) => op.target().as_ref().map(|b| b.borrow().clone()),
            NetMessageV1::Put(op) => op.target().as_ref().map(|b| b.borrow().clone()),
            NetMessageV1::Get(op) => op.target().as_ref().map(|b| b.borrow().clone()),
            NetMessageV1::Subscribe(op) => op.target().as_ref().map(|b| b.borrow().clone()),
            NetMessageV1::Update(op) => op.target().as_ref().map(|b| b.borrow().clone()),
            NetMessageV1::Aborted(_) => None,
            NetMessageV1::Unsubscribed { .. } => None,
        }
    }

    fn terminal(&self) -> bool {
        match self {
            NetMessageV1::Connect(op) => op.terminal(),
            NetMessageV1::Put(op) => op.terminal(),
            NetMessageV1::Get(op) => op.terminal(),
            NetMessageV1::Subscribe(op) => op.terminal(),
            NetMessageV1::Update(op) => op.terminal(),
            NetMessageV1::Aborted(_) => true,
            NetMessageV1::Unsubscribed { .. } => true,
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
        }
    }

    fn track_stats(&self) -> bool {
        !matches!(
            self,
            NetMessageV1::Connect(_) | NetMessageV1::Subscribe(_) | NetMessageV1::Aborted(_)
        )
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
                    write!(f, "Unsubscribed {{  key: {}, from: {} }}", key, from)?;
                }
            },
        };
        write!(f, "}}")
    }
}

/// The result of a connection attempt.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum ConnectionResult {
    /// The target node for connection is valid
    Accepted,
    /// The target node for connection is not valid
    Connection,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_transaction_type() {
        let ts_0 = Ulid::new();
        std::thread::sleep(Duration::from_millis(1));
        let tx = Transaction::update(TransactionType::Connect, Ulid::new());
        assert_eq!(tx.transaction_type(), TransactionType::Connect);
        let tx = Transaction::update(TransactionType::Subscribe, Ulid::new());
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
