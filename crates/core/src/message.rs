//! Network messaging between peers.

use std::{
    fmt::Display,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    node::{ConnectionError, PeerKey},
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
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct Transaction {
    id: Ulid,
}

impl Transaction {
    pub const NULL: &'static Transaction = &Transaction { id: Ulid(0) };

    pub fn new<T: TxType>() -> Self {
        let ty = <T as TxType>::tx_type_id();
        let id = Ulid::new();
        Self::update(ty.0, id)
        // Self { id }
    }

    pub fn transaction_type(&self) -> TransactionType {
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
        let bytes: u128 = u.arbitrary()?;
        Ok(Self::update(ty.0, Ulid(bytes)))
    }
}

impl Display for Transaction {
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
pub(crate) trait TxType: sealed_msg_type::SealedTxType {
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
    use crate::operations::update::UpdateMsg;

    use super::*;

    pub(crate) trait SealedTxType {
        fn tx_type_id() -> TransactionTypeId;
    }

    #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
    #[cfg_attr(test, derive(arbitrary::Arbitrary))]
    pub(crate) struct TransactionTypeId(pub(super) TransactionType);

    #[repr(u8)]
    #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
    #[cfg_attr(test, derive(arbitrary::Arbitrary))]
    pub(crate) enum TransactionType {
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
        (decl struct { $( $var:tt -> $ty:tt),+ }) => {
            $(
                impl From<$ty> for NetMessage {
                    fn from(msg: $ty) -> Self {
                        Self::$var(msg)
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

    transaction_type_enumeration!(decl struct {
        Connect -> ConnectMsg,
        Put -> PutMsg,
        Get -> GetMsg,
        Subscribe -> SubscribeMsg,
        Update -> UpdateMsg
    });
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum NetMessage {
    Connect(ConnectMsg),
    Put(PutMsg),
    Get(GetMsg),
    Subscribe(SubscribeMsg),
    Update(UpdateMsg),
    /// Failed a transaction, informing of abortion.
    Aborted(Transaction),
}

pub(crate) trait InnerMessage: Into<NetMessage> {
    fn id(&self) -> &Transaction;

    fn target(&self) -> Option<&PeerKeyLocation>;

    fn terminal(&self) -> bool;

    fn requested_location(&self) -> Option<Location>;
}

/// Internal node events emitted to the event loop.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum NodeEvent {
    /// For unspecified reasons the node is gracefully shutting down.
    ShutdownNode,
    /// Received a confirmation from a peer that a physical connection was established.
    ConfirmedInbound,
    /// Drop the given peer connection.
    DropConnection(PeerKey),
    /// Accept the connections from the given peer.
    AcceptConnection(PeerKey),
    /// Error while sending a message by the connection bridge from within the ops.
    #[serde(skip)]
    Error(ConnectionError),
    Disconnect {
        cause: Option<String>,
    },
}

impl Display for NodeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeEvent::ShutdownNode => f.write_str("ShutdownNode"),
            NodeEvent::ConfirmedInbound => f.write_str("ConfirmedInbound"),
            NodeEvent::DropConnection(peer) => {
                write!(f, "DropConnection (from {peer})")
            }
            NodeEvent::AcceptConnection(peer) => {
                write!(f, "AcceptConnection (from {peer})")
            }
            NodeEvent::Error(err) => write!(f, "{err}"),
            NodeEvent::Disconnect { cause: Some(cause) } => {
                write!(f, "Disconnect node, reason: {cause}")
            }
            NodeEvent::Disconnect { cause: None } => {
                write!(f, "Disconnect node, reason: unknown")
            }
        }
    }
}

impl NetMessage {
    pub fn id(&self) -> &Transaction {
        use NetMessage::*;
        match self {
            Connect(op) => op.id(),
            Put(op) => op.id(),
            Get(op) => op.id(),
            Subscribe(op) => op.id(),
            Update(op) => op.id(),
            Aborted(tx) => tx,
        }
    }

    pub fn target(&self) -> Option<&PeerKeyLocation> {
        use NetMessage::*;
        match self {
            Connect(op) => op.target(),
            Put(op) => op.target(),
            Get(op) => op.target(),
            Subscribe(op) => op.target(),
            Update(op) => op.target(),
            Aborted(_) => None,
        }
    }

    /// Is the last expected message for this chain of messages.
    pub fn terminal(&self) -> bool {
        use NetMessage::*;
        match self {
            Connect(op) => op.terminal(),
            Put(op) => op.terminal(),
            Get(op) => op.terminal(),
            Subscribe(op) => op.terminal(),
            Update(op) => op.terminal(),
            Aborted(_) => true,
        }
    }

    pub fn requested_location(&self) -> Option<Location> {
        use NetMessage::*;
        match self {
            Connect(op) => op.requested_location(),
            Put(op) => op.requested_location(),
            Get(op) => op.requested_location(),
            Subscribe(op) => op.requested_location(),
            Update(op) => op.requested_location(),
            Aborted(_) => None,
        }
    }

    pub fn track_stats(&self) -> bool {
        use NetMessage::*;
        !matches!(self, Connect(_) | Subscribe(_) | Aborted(_))
    }
}

impl Display for NetMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use NetMessage::*;
        write!(f, "Message {{")?;
        match self {
            Connect(msg) => msg.fmt(f)?,
            Put(msg) => msg.fmt(f)?,
            Get(msg) => msg.fmt(f)?,
            Subscribe(msg) => msg.fmt(f)?,
            Update(msg) => msg.fmt(f)?,
            Aborted(msg) => msg.fmt(f)?,
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
        let tx = Transaction::update(TransactionType::Connect, Ulid::new());
        assert_eq!(tx.transaction_type(), TransactionType::Connect);
        let tx = Transaction::update(TransactionType::Subscribe, Ulid::new());
        assert_eq!(tx.transaction_type(), TransactionType::Subscribe);
        std::thread::sleep(Duration::from_millis(1));
        let ts_1 = Ulid::new();
        assert!(
            tx.id.timestamp_ms() > ts_0.timestamp_ms(),
            "{} <= {}",
            tx.id.datetime(),
            ts_0.datetime()
        );
        assert!(
            tx.id.timestamp_ms() < ts_1.timestamp_ms(),
            "{} >= {}",
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
